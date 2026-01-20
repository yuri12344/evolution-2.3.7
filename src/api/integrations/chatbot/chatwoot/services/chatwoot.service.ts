/* eslint-disable @typescript-eslint/no-explicit-any */
import { InstanceDto } from '@api/dto/instance.dto';
import { Options, Quoted, SendAudioDto, SendMediaDto, SendTextDto } from '@api/dto/sendMessage.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { postgresClient } from '@api/integrations/chatbot/chatwoot/libs/postgres.client';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import { PrismaRepository } from '@api/repository/repository.service';
import { CacheService } from '@api/services/cache.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { Events } from '@api/types/wa.types';
import { Chatwoot, ConfigService, Database, HttpServer } from '@config/env.config';
import { Logger } from '@config/logger.config';
import i18next from '@utils/i18n';
import { sendTelemetry } from '@utils/sendTelemetry';
import axios, { AxiosError, AxiosRequestConfig } from 'axios';
import { WAMessageContent, WAMessageKey } from 'baileys';
import dayjs from 'dayjs';
import FormData from 'form-data';
import { Jimp, JimpMime } from 'jimp';
import Long from 'long';
import mimeTypes from 'mime-types';
import path from 'path';
import { Readable } from 'stream';

/**
 * =========================================================
 * Chatwoot REST adapter (NO SDK)
 * =========================================================
 * - Mantém a assinatura mental do antigo "client.*" mas via REST
 * - Header: api_access_token: <PAT>
 * - basePath: "https://chatwoot.suaempresa.com" (SEM /api/v1)
 */

type ApiRequestOptions = {
  readonly method: 'GET' | 'PUT' | 'POST' | 'DELETE' | 'OPTIONS' | 'HEAD' | 'PATCH';
  readonly url: string; // ex: '/api/v1/accounts/3/contacts/filter'
  readonly headers?: Record<string, any>;
  readonly query?: Record<string, any>;
  readonly body?: any;
  readonly mediaType?: string; // ex: 'application/json'
};

export type ChatwootAPIConfig = {
  basePath: string; // host puro; ex: 'https://chatwoot.especialista.pro' (sem /api/v1)
  with_credentials?: boolean;
  credentials?: 'include' | 'omit' | 'same-origin';
  token?: string | ((o: ApiRequestOptions) => Promise<string> | string);
  headers?: Record<string, string> | ((o: ApiRequestOptions) => Promise<Record<string, string>> | Record<string, string>);
  encode_path?: (p: string) => string;
  timeout_ms?: number;
  max_retries?: number;
  onLog?: (ev: { phase: 'req' | 'res' | 'err'; data: any }) => void;
};

function trimEndSlash(s: string) {
  return (s || '').replace(/\/+$/, '');
}
function joinUrl(base: string, p: string) {
  const b = trimEndSlash(base);
  const pathPart = (p || '').replace(/^\/+/, '');
  return `${b}/${pathPart}`;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function resolveHeaders(config: ChatwootAPIConfig, options: ApiRequestOptions) {
  const extraHeaders = typeof config.headers === 'function' ? await (config.headers as any)(options) : config.headers || {};
  const tok = typeof config.token === 'function' ? await (config.token as any)(options) : config.token;

  return {
    Accept: 'application/json',
    'Content-Type': options.mediaType || 'application/json',
    ...extraHeaders,
    ...(options.headers || {}),
    ...(tok ? { api_access_token: String(tok) } : {}),
  };
}

/**
 * Drop-in request() com retries p/ 429/5xx e erro estilo "ApiError".
 */
export const chatwootRequest = async <T>(config: ChatwootAPIConfig, options: ApiRequestOptions): Promise<T> => {
  const base = trimEndSlash(config.basePath || '');
  if (!base) throw new Error('Chatwoot basePath is empty');

  const url = joinUrl(base, options.url);
  const headers = await resolveHeaders(config, options);

  const axiosCfg: AxiosRequestConfig = {
    method: options.method,
    url,
    headers,
    params: options.query,
    data: options.body,
    maxRedirects: 0,
    withCredentials: !!config.with_credentials,
    timeout: config.timeout_ms ?? 15000,
    transformRequest: (data, hdrs) => {
      const ct = (hdrs as any)['Content-Type'] || (hdrs as any)['content-type'];
      if (ct && String(ct).includes('application/json') && data && typeof data === 'object') return JSON.stringify(data);
      return data;
    },
  };

  const maxRetries = Math.max(0, config.max_retries ?? 2);
  let attempt = 0;
  let lastErr: any;

  while (attempt <= maxRetries) {
    try {
      config.onLog?.({ phase: 'req', data: { url: axiosCfg.url, method: axiosCfg.method, attempt } });
      const resp = await axios(axiosCfg);
      config.onLog?.({ phase: 'res', data: { status: resp.status, url: axiosCfg.url, attempt } });
      return resp.data as T;
    } catch (err: any) {
      lastErr = err;
      const ae = err as AxiosError;
      const status = ae?.response?.status;

      if (ae?.response) {
        const e: any = new Error('ApiError: ' + (ae.response.statusText || 'HTTP Error'));
        e.url = ae.config?.url;
        e.status = ae.response.status;
        e.statusText = ae.response.statusText;
        e.body = ae.response.data;
        e.request = {
          method: String(ae.config?.method || '').toUpperCase(),
          url: (ae.config?.url?.replace(base, '') || options.url) ?? options.url,
          body: (() => {
            try {
              return ae.config?.data ? JSON.parse(ae.config.data as any) : undefined;
            } catch {
              return ae.config?.data;
            }
          })(),
        };

        if ([429, 502, 503, 504].includes(status || 0) && attempt < maxRetries) {
          const retryAfterHeader = (ae.response.headers as any)?.['retry-after'];
          const retryAfter = retryAfterHeader ? Number(retryAfterHeader) * 1000 : undefined;
          const backoff = retryAfter ?? Math.min(2000 * Math.pow(2, attempt), 8000);
          config.onLog?.({ phase: 'err', data: { status, retrying_in_ms: backoff, attempt } });
          await sleep(backoff);
          attempt += 1;
          continue;
        }

        config.onLog?.({ phase: 'err', data: { status, url: e.url, attempt, body: e.body } });
        throw e;
      }

      config.onLog?.({ phase: 'err', data: { kind: 'network', message: ae?.message, attempt } });
      if (attempt < maxRetries) {
        await sleep(Math.min(2000 * Math.pow(2, attempt), 8000));
        attempt += 1;
        continue;
      }
      throw err;
    }
  }

  throw lastErr;
};

// ====================== Helpers / DTO normalize ======================

export function normalizeChatwootDto(data: any) {
  const rawId = data?.accountId;
  const cleanedId = typeof rawId === 'string' ? rawId.replace(/"/g, '').trim() : rawId;

  return {
    ...data,
    accountId: cleanedId !== undefined && cleanedId !== null && cleanedId !== '' ? Number(cleanedId) : null,
    token: data?.token ?? data?.instance_token ?? data?.api_access_token ?? null,
    url: String(data?.url || '').replace(/\/+$/, ''),
  };
}

// ====================== Tipos mínimos (sem SDK) ======================

export type cw_inbox = { id: number; name: string; inbox_identifier?: string };
export type cw_conversation = { id: number; inbox_id: number; status: string; meta?: any };
export type cw_conversation_show = any;
export type cw_contact = { id: number; name?: string; phone_number?: string; identifier?: string; thumbnail?: string; payload?: any };

// ====================== Service ======================

interface ChatwootMessage {
  messageId?: number;
  inboxId?: number;
  conversationId?: number;
  contactInboxSourceId?: string;
  isRead?: boolean;
}

export class ChatwootService {
  private readonly logger = new Logger('ChatwootService');
  private readonly LOCK_POLLING_DELAY_MS = 300;

  private provider: any;
  private pgClient = postgresClient.getChatwootConnection();

  constructor(
    private readonly waMonitor: WAMonitoringService,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly cache: CacheService,
  ) {}

  // ------------------ Provider & REST config ------------------

  private async getProvider(instance: InstanceDto): Promise<any | null> {
    const cacheKey = `${instance.instanceName}:getProvider`;
    if (await this.cache.has(cacheKey)) return (await this.cache.get(cacheKey)) as any;

    const provider = await this.waMonitor.waInstances[instance.instanceName]?.findChatwoot();
    if (!provider) {
      this.logger.warn('provider not found');
      return null;
    }

    this.cache.set(cacheKey, provider);
    return provider;
  }

// 1) clientCw: permite token override (opcional)
private async clientCw(instance: InstanceDto, tokenOverride?: string, ..._unused: any[]) {
  const provider = await this.getProvider(instance);
  if (!provider) return null;

  const token =
    tokenOverride ??
    provider?.token ??
    (provider as any)?.instance_token ??
    (provider as any)?.api_access_token ??
    null;

  this.provider = {
    ...provider,
    token,
    url: String(provider?.url || '').replace(/\/+$/, ''),
    accountId: provider?.accountId != null ? Number(provider.accountId) : undefined,
  };

  if (!this.provider?.url || !this.provider?.token || !this.provider?.accountId) {
    this.logger.error(
      `[ChatwootService:clientCw] provider config incompleta | ` +
        `hasUrl=${!!this.provider?.url} ` +
        `hasToken=${!!this.provider?.token} ` +
        `hasAccountId=${!!this.provider?.accountId}`,
    );
  }

  return { ok: true };
}
public getClientCwConfig(): ChatwootAPIConfig & { nameInbox: string; mergeBrazilContacts: boolean } {
  const token =
    this.provider?.token ??
    (this.provider as any)?.instance_token ??
    (this.provider as any)?.api_access_token ??
    null;

  const basePath = trimEndSlash(String(this.provider?.url || ''));

  const cfg: ChatwootAPIConfig & { nameInbox: string; mergeBrazilContacts: boolean } = {
    basePath,
    with_credentials: false,
    credentials: 'omit',
    token,

    // deixa só o essencial aqui; resolveHeaders já injeta api_access_token também :contentReference[oaicite:2]{index=2}
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      api_access_token: String(token || ''), // redundante mas OK
    },

    timeout_ms: 15000,
    max_retries: 2,

    nameInbox: this.provider?.nameInbox,
    mergeBrazilContacts: this.provider?.mergeBrazilContacts,

    // ✅ LOG CENTRALIZADO DE TODAS AS CHAMADAS
    onLog: (ev) => {
      const safe = (obj: any) => {
        try {
          return JSON.stringify(obj);
        } catch {
          return String(obj);
        }
      };

      // nunca loga token inteiro
      const tokenStr = token ? String(token) : '';
      const tokenHint = tokenStr ? `${tokenStr.slice(0, 4)}…${tokenStr.slice(-4)}` : 'EMPTY';

      if (ev.phase === 'req') {
        this.logger.verbose(
          `[Chatwoot:REQ] ${safe({
            method: ev.data?.method,
            url: ev.data?.url,
            attempt: ev.data?.attempt,
            basePath,
            accountId: this.provider?.accountId,
            hasToken: !!token,
            tokenHint,
          })}`,
        );
      }

      if (ev.phase === 'res') {
        this.logger.verbose(
          `[Chatwoot:RES] ${safe({
            status: ev.data?.status,
            url: ev.data?.url,
            attempt: ev.data?.attempt,
          })}`,
        );
      }

      if (ev.phase === 'err') {
        this.logger.error(
          `[Chatwoot:ERR] ${safe({
            status: ev.data?.status,
            url: ev.data?.url,
            attempt: ev.data?.attempt,
            body: ev.data?.body,
            retrying_in_ms: ev.data?.retrying_in_ms,
            hasToken: !!token,
            tokenHint,
            accountId: this.provider?.accountId,
            basePath,
          })}`,
        );
      }
    },
  };

  return cfg;
}

// 3) cw: ponto único de verdade; SEMPRE injeta api_access_token por argumento
private cw<T>(options: ApiRequestOptions, tokenOverride?: string): Promise<T> {
  const token =
    tokenOverride ??
    this.provider?.token ??
    (this.provider as any)?.instance_token ??
    (this.provider as any)?.api_access_token ??
    null;

  if (!token) {
    // se você preferir “não quebrar”, troca por logger.warn e deixa passar,
    // mas o certo é falhar cedo pra não mascarar 401.
    throw new Error('[ChatwootService:cw] token ausente (api_access_token) — vai dar Unauthorized');
  }

  return chatwootRequest<T>(this.getClientCwConfig(), {
    ...options,
    headers: {
      ...(options.headers || {}),
      api_access_token: String(token), // garante no request mesmo se algum lugar sobrescrever headers
    },
  });
}


  public getCache() {
    return this.cache;
  }

  // ------------------ REST helpers ------------------

  private async cwListInboxes(): Promise<{ payload: cw_inbox[] }> {
    return this.cw({ method: 'GET', url: `/api/v1/accounts/${this.provider.accountId}/inboxes`, query: { page: 1, per_page: 100 } });
  }

  private async cwCreateInbox(data: any): Promise<cw_inbox> {
    return this.cw({ method: 'POST', url: `/api/v1/accounts/${this.provider.accountId}/inboxes`, body: data });
  }

  private async cwContactCreate(data: any): Promise<any> {
    return this.cw({ method: 'POST', url: `/api/v1/accounts/${this.provider.accountId}/contacts`, body: data });
  }

  private async cwContactUpdate(id: number, data: any): Promise<any> {
    return this.cw({ method: 'PUT', url: `/api/v1/accounts/${this.provider.accountId}/contacts/${id}`, body: data });
  }

  private async cwContactSearch(q: string): Promise<any> {
    // Chatwoot: GET /contacts/search?q=...
    return this.cw({ method: 'GET', url: `/api/v1/accounts/${this.provider.accountId}/contacts/search`, query: { q } });
  }

  private async cwContactFilter(payload: any): Promise<any> {
    return this.cw({ method: 'POST', url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`, body: { payload } });
  }

  private async cwGetContact(id: number): Promise<any> {
    return this.cw({ method: 'GET', url: `/api/v1/accounts/${this.provider.accountId}/contacts/${id}` });
  }

  private async cwListContactConversations(contactId: number): Promise<{ payload: cw_conversation[] }> {
    return this.cw({ method: 'GET', url: `/api/v1/accounts/${this.provider.accountId}/contacts/${contactId}/conversations` });
  }

  private async cwConversationCreate(data: any): Promise<cw_conversation> {
    return this.cw({ method: 'POST', url: `/api/v1/accounts/${this.provider.accountId}/conversations`, body: data });
  }

  private async cwConversationGet(conversationId: number): Promise<any> {
    return this.cw({ method: 'GET', url: `/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}` });
  }

  private async cwConversationToggleStatus(conversationId: number, status: string): Promise<any> {
    // Doc: POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/toggle_status
    // OBS: em algumas versões, status só alterna open/resolved. Se falhar, a gente só loga.
    return this.cw({
      method: 'POST',
      url: `/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/toggle_status`,
      body: { status },
    });
  }

  private async cwMessageCreate(conversationId: number, data: any): Promise<any> {
    return this.cw({
      method: 'POST',
      url: `/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/messages`,
      body: data,
    });
  }

  private async cwMessageDelete(conversationId: number, messageId: number): Promise<any> {
    return this.cw({
      method: 'DELETE',
      url: `/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/messages/${messageId}`,
    });
  }

  // ------------------ Public API ------------------

  public async create(instance: InstanceDto, data: ChatwootDto) {
    // normaliza antes de guardar/propagar
    const normalized = normalizeChatwootDto(data);

    await this.waMonitor.waInstances[instance.instanceName].setChatwoot(normalized);

    if (normalized.autoCreate) {
      this.logger.log('Auto create chatwoot instance');
      const urlServer = this.configService.get<HttpServer>('SERVER').URL;

      await this.initInstanceChatwoot(
        instance,
        normalized.nameInbox ?? instance.instanceName.split('-cwId-')[0],
        `${urlServer}/chatwoot/webhook/${encodeURIComponent(instance.instanceName)}`,
        true,
        normalized.number,
        normalized.organization,
        normalized.logo,
      );
    }
    return normalized;
  }

  public async find(instance: InstanceDto): Promise<ChatwootDto> {
    try {
      return await this.waMonitor.waInstances[instance.instanceName].findChatwoot();
    } catch {
      this.logger.error('chatwoot not found');
      return { enabled: null, url: '' } as any;
    }
  }

  public async getContact(instance: InstanceDto, id: number) {
    await this.clientCw(instance);
    if (!id) return null;
    try {
      return await this.cwGetContact(id);
    } catch {
      return null;
    }
  }

  public async initInstanceChatwoot(
    instance: InstanceDto,
    inboxName: string,
    webhookUrl: string,
    qrcode: boolean,
    number: string,
    organization?: string,
    logo?: string,
  ) {
    await this.clientCw(instance);

    const findInbox: any = await this.cwListInboxes();
    const checkDuplicate = (findInbox?.payload || []).map((inb: any) => inb.name).includes(inboxName);

    let inboxId: number;

    this.logger.log('Creating chatwoot inbox');
    if (!checkDuplicate) {
      const inbox = await this.cwCreateInbox({
        name: inboxName,
        channel: { type: 'api', webhook_url: webhookUrl },
      });

      if (!inbox) return null;
      inboxId = inbox.id;
    } else {
      const inbox = (findInbox?.payload || []).find((inb: any) => inb.name === inboxName);
      if (!inbox) return null;
      inboxId = inbox.id;
    }
    this.logger.log(`Inbox created - inboxId: ${inboxId}`);

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');
      return true;
    }

    this.logger.log('Creating chatwoot bot contact');
    const contact =
      (await this.findContact(instance, '123456')) ||
      (await this.createContact(
        instance,
        '123456',
        inboxId,
        false,
        organization ? organization : 'EvolutionAPI',
        logo ? logo : 'https://evolution-api.com/files/evolution-api-favicon.png',
      ));

    if (!contact) return null;

    const contactId = (contact as any)?.id || (contact as any)?.payload?.contact?.id || (contact as any)?.payload?.id;
    this.logger.log(`Contact created - contactId: ${contactId}`);

    if (qrcode) {
      this.logger.log('QR code enabled');

      const conversation = await this.cwConversationCreate({
        contact_id: contactId.toString(),
        inbox_id: inboxId.toString(),
      });

      if (!conversation) return null;

      let contentMsg = 'init';
      if (number) contentMsg = `init:${number}`;

      await this.cwMessageCreate(conversation.id, {
        content: contentMsg,
        message_type: 'outgoing',
      });

      this.logger.log('Init message sent');
    }

    return true;
  }

  public async createContact(
    instance: InstanceDto,
    phoneNumber: string,
    inboxId: number,
    isGroup: boolean,
    name?: string,
    avatar_url?: string,
    jid?: string,
  ) {
    try {
      await this.clientCw(instance);

      let data: any = {};
      if (!isGroup) {
        data = {
          inbox_id: inboxId,
          name: name || phoneNumber,
          identifier: jid,
          avatar_url: avatar_url,
        };

        if ((jid && jid.includes('@')) || !jid) {
          data['phone_number'] = `+${phoneNumber}`;
        }
      } else {
        data = {
          inbox_id: inboxId,
          name: name || phoneNumber,
          identifier: phoneNumber,
          avatar_url: avatar_url,
        };
      }

      const contact = await this.cwContactCreate(data);

      const findContact = await this.findContact(instance, phoneNumber);
      const contactId = (findContact as any)?.id;

      await this.addLabelToContact(this.provider.nameInbox, contactId);

      return contact;
    } catch (error) {
      this.logger.error('Error creating contact');
      this.logger.error(error as any);
      return null;
    }
  }

  public async updateContact(instance: InstanceDto, id: number, data: any) {
    await this.clientCw(instance);
    if (!id) return null;

    try {
      return await this.cwContactUpdate(id, data);
    } catch {
      return null;
    }
  }

  public async addLabelToContact(nameInbox: string, contactId: number) {
    try {
      const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI;
      if (!uri) return false;

      const sqlTags = `SELECT id, taggings_count FROM tags WHERE name = $1 LIMIT 1`;
      const tagData = (await this.pgClient.query(sqlTags, [nameInbox]))?.rows[0];
      let tagId = tagData?.id;
      const taggingsCount = tagData?.taggings_count || 0;

      const sqlTag = `INSERT INTO tags (name, taggings_count)
                      VALUES ($1, $2)
                      ON CONFLICT (name)
                      DO UPDATE SET taggings_count = tags.taggings_count + 1
                      RETURNING id`;

      tagId = (await this.pgClient.query(sqlTag, [nameInbox, taggingsCount + 1]))?.rows[0]?.id;

      const sqlCheckTagging = `SELECT 1 FROM taggings
                               WHERE tag_id = $1 AND taggable_type = 'Contact' AND taggable_id = $2 AND context = 'labels' LIMIT 1`;

      const taggingExists = (await this.pgClient.query(sqlCheckTagging, [tagId, contactId]))?.rowCount > 0;

      if (!taggingExists) {
        const sqlInsertLabel = `INSERT INTO taggings (tag_id, taggable_type, taggable_id, context, created_at)
                                VALUES ($1, 'Contact', $2, 'labels', NOW())`;

        await this.pgClient.query(sqlInsertLabel, [tagId, contactId]);
      }

      return true;
    } catch {
      return false;
    }
  }

  public async findContact(instance: InstanceDto, phoneNumber: string) {
    await this.clientCw(instance);

    const isGroup = phoneNumber.includes('@g.us');
    const query = isGroup ? phoneNumber : `+${phoneNumber}`;

    let contact: any;

    if (isGroup) {
      contact = await this.cwContactSearch(query);
    } else {
      contact = await this.cwContactFilter(this.getFilterPayload(query));
    }

    if (!contact && contact?.payload?.length === 0) return null;

    if (!isGroup) {
      return contact.payload.length > 1 ? this.findContactInContactList(contact.payload, query) : contact.payload[0];
    }
    return contact.payload.find((c: any) => c.identifier === query);
  }

  private async mergeContacts(baseId: number, mergeId: number) {
    try {
      return await this.cw({
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/actions/contact_merge`,
        body: { base_contact_id: baseId, mergee_contact_id: mergeId },
      });
    } catch {
      this.logger.error('Error merging contacts');
      return null;
    }
  }

  private async mergeBrazilianContacts(contacts: any[]) {
    try {
      return await this.cw({
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/actions/contact_merge`,
        body: {
          base_contact_id: contacts.find((c) => c.phone_number.length === 14)?.id,
          mergee_contact_id: contacts.find((c) => c.phone_number.length === 13)?.id,
        },
      });
    } catch {
      this.logger.error('Error merging contacts');
      return null;
    }
  }

  private findContactInContactList(contacts: any[], query: string) {
    const phoneNumbers = this.getNumbers(query);
    const searchableFields = this.getSearchableFields();

    if (contacts.length === 2 && this.getClientCwConfig().mergeBrazilContacts && query.startsWith('+55')) {
      const contact = this.mergeBrazilianContacts(contacts);
      if (contact) return contact;
    }

    const phone = phoneNumbers.reduce((savedNumber, number) => (number.length > savedNumber.length ? number : savedNumber), '');

    const contact_with9 = contacts.find((c) => c.phone_number === phone);
    if (contact_with9) return contact_with9;

    for (const c of contacts) {
      for (const field of searchableFields) {
        if (c[field] && phoneNumbers.includes(c[field])) return c;
      }
    }

    return null;
  }

  private getNumbers(query: string) {
    const numbers: string[] = [];
    numbers.push(query);

    if (query.startsWith('+55') && query.length === 14) {
      const withoutNine = query.slice(0, 5) + query.slice(6);
      numbers.push(withoutNine);
    } else if (query.startsWith('+55') && query.length === 13) {
      const withNine = query.slice(0, 5) + '9' + query.slice(5);
      numbers.push(withNine);
    }

    return numbers;
  }

  private getSearchableFields() {
    return ['phone_number'];
  }

  private getFilterPayload(query: string) {
    const filterPayload: any[] = [];
    const numbers = this.getNumbers(query);
    const fieldsToSearch = this.getSearchableFields();

    fieldsToSearch.forEach((field, index1) => {
      numbers.forEach((number, index2) => {
        const queryOperator = fieldsToSearch.length - 1 === index1 && numbers.length - 1 === index2 ? null : 'OR';
        filterPayload.push({
          attribute_key: field,
          filter_operator: 'equal_to',
          values: [number.replace('+', '')],
          query_operator: queryOperator,
        });
      });
    });

    return filterPayload;
  }

  // ------------------ Inbox ------------------

  public async getInbox(instance: InstanceDto): Promise<cw_inbox | null> {
    const cacheKey = `${instance.instanceName}:getInbox`;
    if (await this.cache.has(cacheKey)) return (await this.cache.get(cacheKey)) as cw_inbox;

    await this.clientCw(instance);

    const inboxes = await this.cwListInboxes();
    const findByName = (inboxes?.payload || []).find((i: any) => i.name === this.getClientCwConfig().nameInbox);

    if (!findByName) return null;

    this.cache.set(cacheKey, findByName);
    return findByName;
  }

  // ------------------ Conversation ------------------

  public async createConversation(instance: InstanceDto, body: any) {
    const J = (v: any) => {
      try {
        return JSON.stringify(v, null, 2);
      } catch {
        return String(v);
      }
    };
    const L = (lvl: 'verbose' | 'warn' | 'error', msg: string, extra?: any) => {
      const line = `[ChatwootService:createConversation] ${msg}${extra !== undefined ? ` :: ${J(extra)}` : ''}`;
      if (lvl === 'verbose') this.logger.verbose(line);
      if (lvl === 'warn') this.logger.warn(line);
      if (lvl === 'error') this.logger.error(line);
    };

    const isLid = !!(body?.key?.addressingMode === 'lid');
    const isGroup = !!(body?.key?.remoteJid && String(body.key.remoteJid).endsWith('@g.us'));
    const remoteJid = body?.key?.remoteJid;
    const phoneNumber = isLid && !isGroup ? body?.key?.remoteJidAlt : remoteJid;
    const cacheKey = `${instance?.instanceName}:createConversation-${remoteJid}`;
    const lockKey = `${instance?.instanceName}:lock:createConversation-${remoteJid}`;
    const maxWaitTime = 5000;

    await this.clientCw(instance);

    try {
      // Atualização de contato @lid se aplicável
      if (phoneNumber && remoteJid && !isGroup) {
        const pn = String(phoneNumber).split('@')[0];
        const contact = await this.findContact(instance, pn);

        if (contact && contact.identifier !== remoteJid) {
          const updateContact = await this.updateContact(instance, contact.id, { identifier: remoteJid, phone_number: `+${pn}` });

          if (updateContact === null) {
            const baseContact = await this.findContact(instance, pn);
            if (baseContact) await this.mergeContacts(baseContact.id, contact.id);
          }
        }
      }

      // Cache short-circuit
      if (await this.cache.has(cacheKey)) {
        const conversationId = (await this.cache.get(cacheKey)) as number;

        let conversationExists: any | boolean;
        try {
          conversationExists = await this.cwConversationGet(conversationId);
        } catch (error) {
          L('error', 'Error calling conversations.get', { error: String(error) });
          conversationExists = false;
        }

        if (!conversationExists) {
          this.cache.delete(cacheKey);
          return await this.createConversation(instance, body);
        }
        return conversationId;
      }

      // Espera lock se já existir
      if (await this.cache.has(lockKey)) {
        const start = Date.now();
        while (await this.cache.has(lockKey)) {
          if (Date.now() - start > maxWaitTime) break;
          await new Promise((res) => setTimeout(res, this.LOCK_POLLING_DELAY_MS));
          if (await this.cache.has(cacheKey)) {
            return (await this.cache.get(cacheKey)) as number;
          }
        }
      }

      // Adquire lock
      await this.cache.set(lockKey, true, 30);

      try {
        // Double-check pós-lock
        if (await this.cache.has(cacheKey)) return (await this.cache.get(cacheKey)) as number;

        const chatId = isGroup ? remoteJid : String(phoneNumber).split('@')[0].split(':')[0];
        let nameContact = !body?.key?.fromMe ? body?.pushName : chatId;

        const filterInbox = await this.getInbox(instance);
        if (!filterInbox) return null;

        const waCtx = this?.waMonitor?.waInstances?.[instance?.instanceName];

        if (isGroup) {
          const group = await waCtx?.client?.groupMetadata?.(chatId);
          const participantJid = isLid && !body?.key?.fromMe ? body?.key?.participantAlt : body?.key?.participant;

          nameContact = `${group?.subject ?? 'UNKNOWN_GROUP'} (GROUP)`;

          const picture_url = await waCtx?.profilePicture?.(String(participantJid || '').split('@')[0]);
          const findParticipant = await this.findContact(instance, String(participantJid || '').split('@')[0]);

          if (findParticipant) {
            const needsName = !findParticipant.name || findParticipant.name === chatId;
            if (needsName) {
              await this.updateContact(instance, findParticipant.id, {
                name: body?.pushName,
                avatar_url: picture_url?.profilePictureUrl || null,
              });
            }
          } else {
            await this.createContact(
              instance,
              String(participantJid || '').split('@')[0],
              filterInbox.id,
              false,
              body?.pushName,
              picture_url?.profilePictureUrl || null,
              participantJid,
            );
          }
        }

        const picture_url = await waCtx?.profilePicture?.(chatId);

        let contact = await this.findContact(instance, chatId);

        if (contact) {
          if (!body?.key?.fromMe) {
            const waFile = picture_url?.profilePictureUrl?.split('#')[0]?.split('?')[0]?.split('/')?.pop() || '';
            const cwFile = contact?.thumbnail?.split('#')[0]?.split('?')[0]?.split('/')?.pop() || '';
            const pictureNeedsUpdate = waFile !== cwFile;

            const nameNeedsUpdate =
              !contact.name ||
              contact.name === chatId ||
              ((`+${chatId}`).startsWith('+55')
                ? this.getNumbers(`+${chatId}`).some(
                    (v) => contact!.name === v || contact!.name === v.substring(3) || contact!.name === v.substring(1),
                  )
                : false);

            if (pictureNeedsUpdate || nameNeedsUpdate) {
              contact = await this.updateContact(instance, contact.id, {
                ...(nameNeedsUpdate && { name: nameContact }),
                ...(waFile === '' && { avatar: null }),
                ...(pictureNeedsUpdate && { avatar_url: picture_url?.profilePictureUrl }),
              });
            }
          }
        } else {
          contact = await this.createContact(
            instance,
            chatId,
            filterInbox.id,
            isGroup,
            nameContact,
            picture_url?.profilePictureUrl || null,
            remoteJid,
          );
        }

        if (!contact) return null;

        const contactId = contact?.payload?.id || contact?.payload?.contact?.id || contact?.id;

        const contactConversations = await this.cwListContactConversations(Number(contactId));

        let inboxConversation = (contactConversations?.payload || []).find((c: any) => c?.inbox_id == filterInbox.id);

        if (inboxConversation) {
          if (this.provider.reopenConversation) {
            if (this.provider.conversationPending && inboxConversation.status !== 'open') {
              try {
                await this.cwConversationToggleStatus(inboxConversation.id, 'pending');
              } catch (e) {
                this.logger.warn(`toggleStatus failed: ${(e as any)?.toString?.() || String(e)}`);
              }
            }
          } else {
            inboxConversation = (contactConversations?.payload || []).find(
              (c: any) => c && c.status !== 'resolved' && c.inbox_id == filterInbox.id,
            );
          }

          if (inboxConversation) {
            this.cache.set(cacheKey, inboxConversation.id, 8 * 3600);
            return inboxConversation.id;
          }
        }

        const data: any = {
          contact_id: String(contactId),
          inbox_id: String(filterInbox.id),
        };
        if (this.provider.conversationPending) data.status = 'pending';

        if (await this.cache.has(cacheKey)) return (await this.cache.get(cacheKey)) as number;

        const conversation = await this.cwConversationCreate(data);
        if (!conversation) return null;

        this.cache.set(cacheKey, conversation.id, 8 * 3600);
        return conversation.id;
      } finally {
        await this.cache.delete(lockKey);
      }
    } catch (error) {
      L('error', 'Top-level catch in createConversation', { error: String(error), stack: (error as any)?.stack });
      return null;
    }
  }

  // ------------------ Messages ------------------

  public async createMessage(
    instance: InstanceDto,
    conversationId: number,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    privateMessage?: boolean,
    attachments?: { content: unknown; encoding: string; filename: string }[],
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: any,
  ) {
    await this.clientCw(instance);

    const replyToIds = await this.getReplyToIds(messageBody, instance);
    const sourceReplyId = quotedMsg?.chatwootMessageId || null;

    const message = await this.cwMessageCreate(conversationId, {
      content,
      message_type: messageType,
      attachments,
      private: privateMessage || false,
      source_id: sourceId,
      content_attributes: { ...replyToIds },
      source_reply_id: sourceReplyId ? sourceReplyId.toString() : null,
    });

    if (!message) return null;
    return message;
  }

  public async getOpenConversationByContact(instance: InstanceDto, inbox: cw_inbox, contact: any): Promise<cw_conversation> {
    await this.clientCw(instance);
    const conversations = await this.cwListContactConversations(contact.id);
    return (conversations.payload || []).find((c: any) => c.inbox_id === inbox.id && c.status === 'open') || undefined;
  }

  public async createBotMessage(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    attachments?: { content: unknown; encoding: string; filename: string }[],
  ) {
    await this.clientCw(instance);

    const contact = await this.findContact(instance, '123456');
    if (!contact) return null;

    const filterInbox = await this.getInbox(instance);
    if (!filterInbox) return null;

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);
    if (!conversation) return;

    const message = await this.cwMessageCreate(conversation.id, { content, message_type: messageType, attachments });

    if (!message) return null;
    return message;
  }

  private async sendData(
    conversationId: number,
    fileStream: Readable,
    fileName: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    content?: string,
    instance?: InstanceDto,
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: any,
  ) {
    if (sourceId && this.isImportHistoryAvailable()) {
      const messageAlreadySaved = await chatwootImport.getExistingSourceIds([sourceId], conversationId);
      if (messageAlreadySaved && messageAlreadySaved.size > 0) {
        this.logger.warn('Message already saved on chatwoot');
        return null;
      }
    }

    const data = new FormData();
    if (content) data.append('content', content);
    data.append('message_type', messageType as any);
    data.append('attachments[]', fileStream, { filename: fileName });

    const sourceReplyId = quotedMsg?.chatwootMessageId || null;

    if (messageBody && instance) {
      const replyToIds = await this.getReplyToIds(messageBody, instance);
      if (replyToIds.in_reply_to || replyToIds.in_reply_to_external_id) {
        data.append('content_attributes', JSON.stringify({ ...replyToIds }));
      }
    }

    if (sourceReplyId) data.append('source_reply_id', sourceReplyId.toString());
    if (sourceId) data.append('source_id', sourceId);

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/messages`,
      headers: { api_access_token: this.provider.token, ...data.getHeaders() },
      data,
    };

    try {
      const { data: resp } = await axios.request(config as any);
      return resp;
    } catch (error) {
      this.logger.error(error as any);
    }
  }

  public async createBotQr(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    fileStream?: Readable,
    fileName?: string,
  ) {
    await this.clientCw(instance);

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');
      return true;
    }

    const contact = await this.findContact(instance, '123456');
    if (!contact) return null;

    const filterInbox = await this.getInbox(instance);
    if (!filterInbox) return null;

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);
    if (!conversation) return;

    const data = new FormData();
    if (content) data.append('content', content);
    data.append('message_type', messageType as any);
    if (fileStream && fileName) data.append('attachments[]', fileStream, { filename: fileName });

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversation.id}/messages`,
      headers: { api_access_token: this.provider.token, ...data.getHeaders() },
      data,
    };

    try {
      const { data: resp } = await axios.request(config as any);
      return resp;
    } catch (error) {
      this.logger.error(error as any);
    }
  }

  public async sendAttachment(waInstance: any, number: string, media: any, caption?: string, options?: Options) {
    try {
      const parsedMedia = path.parse(decodeURIComponent(media));
      let mimeType = mimeTypes.lookup(parsedMedia?.ext) || '';
      let fileName = parsedMedia?.name + parsedMedia?.ext;

      if (!mimeType) {
        const parts = media.split('/');
        fileName = decodeURIComponent(parts[parts.length - 1]);

        const response = await axios.get(media, { responseType: 'arraybuffer' });
        mimeType = response.headers['content-type'];
      }

      let type = 'document';
      switch (mimeType.split('/')[0]) {
        case 'image':
          type = 'image';
          break;
        case 'video':
          type = 'video';
          break;
        case 'audio':
          type = 'audio';
          break;
        default:
          type = 'document';
      }

      if (type === 'audio') {
        const data: SendAudioDto = {
          number,
          audio: media,
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
          quoted: options?.quoted,
        };

        sendTelemetry('/message/sendWhatsAppAudio');
        return await waInstance?.audioWhatsapp(data, null, true);
      }

      const documentExtensions = ['.gif', '.svg', '.tiff', '.tif', '.dxf', '.dwg'];
      if (type === 'image' && parsedMedia && documentExtensions.includes(parsedMedia?.ext)) type = 'document';

      const data: SendMediaDto = {
        number,
        mediatype: type as any,
        fileName,
        media,
        delay: 1200,
        quoted: options?.quoted,
      };

      sendTelemetry('/message/sendMedia');
      if (caption) data.caption = caption;

      return await waInstance?.mediaMessage(data, null, true);
    } catch (error) {
      this.logger.error(error as any);
      throw error;
    }
  }

  public async onSendMessageError(instance: InstanceDto, conversation: number, error?: any) {
    this.logger.verbose(`onSendMessageError ${JSON.stringify(error)}`);

    await this.clientCw(instance);

    if (error && error?.status === 400 && error?.message[0]?.exists === false) {
      await this.cwMessageCreate(conversation, {
        content: `${i18next.t('cw.message.numbernotinwhatsapp')}`,
        message_type: 'outgoing',
        private: true,
      });
      return;
    }

    await this.cwMessageCreate(conversation, {
      content: i18next.t('cw.message.notsent', { error: error ? `_${error.toString()}_` : '' }),
      message_type: 'outgoing',
      private: true,
    });
  }

  // =========================================================
  // A partir daqui: seu código original (WA<->CW) permanece
  // O que mudou foi só o "client.*" => REST helpers acima.
  // =========================================================

  public async receiveWebhook(instance: InstanceDto, body: any) {
    try {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await this.clientCw(instance);

      if (
        this.provider.reopenConversation === false &&
        body.event === 'conversation_status_changed' &&
        body.status === 'resolved' &&
        body.meta?.sender?.identifier
      ) {
        const keyToDelete = `${instance.instanceName}:createConversation-${body.meta.sender.identifier}`;
        this.cache.delete(keyToDelete);
      }

      if (!body?.conversation || body.private || (body.event === 'message_updated' && !body.content_attributes?.deleted)) {
        return { message: 'bot' };
      }

      const chatId = body.conversation.meta.sender?.identifier || body.conversation.meta.sender?.phone_number.replace('+', '');

      const messageReceived = body.content
        ? body.content
            .replaceAll(/(?<!\*)\*((?!\s)([^\n*]+?)(?<!\s))\*(?!\*)/g, '_$1_')
            .replaceAll(/\*{2}((?!\s)([^\n*]+?)(?<!\s))\*{2}/g, '*$1*')
            .replaceAll(/~{2}((?!\s)([^\n*]+?)(?<!\s))~{2}/g, '~$1~')
            .replaceAll(/(?<!`)`((?!\s)([^`*]+?)(?<!\s))`(?!`)/g, '```$1```')
        : body.content;

      const senderName = body?.conversation?.messages[0]?.sender?.available_name || body?.sender?.name;
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      instance.instanceId = waInstance.instanceId;

      if (body.event === 'message_updated' && body.content_attributes?.deleted) {
        const message = await this.prismaRepository.message.findFirst({
          where: { chatwootMessageId: body.id, instanceId: instance.instanceId },
        });

        if (message) {
          const key = message.key as WAMessageKey;

          await waInstance?.client.sendMessage(key.remoteJid, { delete: key });

          await this.prismaRepository.message.deleteMany({
            where: { instanceId: instance.instanceId, chatwootMessageId: body.id },
          });
        }
        return { message: 'bot' };
      }

      const cwBotContact = this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT;

      if (chatId === '123456' && body.message_type === 'outgoing') {
        const command = messageReceived.replace('/', '');

        if (cwBotContact && (command.includes('init') || command.includes('iniciar'))) {
          const state = waInstance?.connectionStatus?.state;

          if (state !== 'open') {
            const number = command.split(':')[1];
            await waInstance.connectToWhatsapp(number);
          } else {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.alreadyConnected', { inboxName: body.inbox.name }),
              'incoming',
            );
          }
        }

        if (command === 'clearcache') {
          waInstance.clearCacheChatwoot();
          await this.createBotMessage(instance, i18next.t('cw.inbox.clearCache', { inboxName: body.inbox.name }), 'incoming');
        }

        if (command === 'status') {
          const state = waInstance?.connectionStatus?.state;

          if (!state) {
            await this.createBotMessage(instance, i18next.t('cw.inbox.notFound', { inboxName: body.inbox.name }), 'incoming');
          }

          if (state) {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.status', { inboxName: body.inbox.name, state: state }),
              'incoming',
            );
          }
        }

        if (cwBotContact && (command === 'disconnect' || command === 'desconectar')) {
          const msgLogout = i18next.t('cw.inbox.disconnect', { inboxName: body.inbox.name });

          await this.createBotMessage(instance, msgLogout, 'incoming');

          await waInstance?.client?.logout('Log out instance: ' + instance.instanceName);
          await waInstance?.client?.ws?.close();
        }
      }

      if (body.message_type === 'outgoing' && body?.conversation?.messages?.length && chatId !== '123456') {
        if (body?.conversation?.messages[0]?.source_id?.substring(0, 5) === 'WAID:' && body?.conversation?.messages[0]?.id === body?.id) {
          return { message: 'bot' };
        }

        if (!waInstance && body.conversation?.id) {
          this.onSendMessageError(instance, body.conversation?.id, 'Instance not found');
          return { message: 'bot' };
        }

        let formatText: string;
        if (senderName === null || senderName === undefined) {
          formatText = messageReceived;
        } else {
          const formattedDelimiter = this.provider.signDelimiter ? this.provider.signDelimiter.replaceAll('\\n', '\n') : '\n';
          const textToConcat = this.provider.signMsg ? [`*${senderName}:*`] : [];
          textToConcat.push(messageReceived);
          formatText = textToConcat.join(formattedDelimiter);
        }

        for (const message of body.conversation.messages) {
          if (message.attachments && message.attachments.length > 0) {
            for (const attachment of message.attachments) {
              if (!messageReceived) formatText = null as any;

              const options: Options = { quoted: await this.getQuotedMessage(body, instance) };

              const messageSent = await this.sendAttachment(waInstance, chatId, attachment.data_url, formatText, options);
              if (!messageSent && body.conversation?.id) this.onSendMessageError(instance, body.conversation?.id);

              await this.updateChatwootMessageId(
                { ...(messageSent as any) },
                {
                  messageId: body.id,
                  inboxId: body.inbox?.id,
                  conversationId: body.conversation?.id,
                  contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
                },
                instance,
              );
            }
          } else {
            const data: SendTextDto = {
              number: chatId,
              text: formatText,
              delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
              quoted: await this.getQuotedMessage(body, instance),
            };

            sendTelemetry('/message/sendText');

            let messageSent: any;
            try {
              messageSent = await waInstance?.textMessage(data, true);
              if (!messageSent) throw new Error('Message not sent');

              if (Long.isLong(messageSent?.messageTimestamp)) messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();

              await this.updateChatwootMessageId(
                { ...messageSent },
                {
                  messageId: body.id,
                  inboxId: body.inbox?.id,
                  conversationId: body.conversation?.id,
                  contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
                },
                instance,
              );
            } catch (error) {
              if (!messageSent && body.conversation?.id) this.onSendMessageError(instance, body.conversation?.id, error);
              throw error;
            }
          }
        }

        const chatwootRead = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_READ;
        if (chatwootRead) {
          const lastMessage = await this.prismaRepository.message.findFirst({
            where: { key: { path: ['fromMe'], equals: false }, instanceId: instance.instanceId },
          });
          if (lastMessage && !lastMessage.chatwootIsRead) {
            const key = lastMessage.key as WAMessageKey;

            waInstance?.markMessageAsRead({
              readMessages: [{ id: key.id, fromMe: key.fromMe, remoteJid: key.remoteJid }],
            });
            const updateMessage = {
              chatwootMessageId: lastMessage.chatwootMessageId,
              chatwootConversationId: lastMessage.chatwootConversationId,
              chatwootInboxId: lastMessage.chatwootInboxId,
              chatwootContactInboxSourceId: lastMessage.chatwootContactInboxSourceId,
              chatwootIsRead: true,
            };

            await this.prismaRepository.message.updateMany({
              where: { instanceId: instance.instanceId, key: { path: ['id'], equals: key.id } },
              data: updateMessage,
            });
          }
        }
      }

      if (body.message_type === 'template' && body.event === 'message_created') {
        const data: SendTextDto = {
          number: chatId,
          text: body.content.replace(/\\\r\n|\\\n|\n/g, '\n'),
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
        };

        sendTelemetry('/message/sendText');
        await waInstance?.textMessage(data);
      }

      return { message: 'bot' };
    } catch (error) {
      this.logger.error(error as any);
      return { message: 'bot' };
    }
  }

  private async updateChatwootMessageId(message: any, chatwootMessageIds: ChatwootMessage, instance: InstanceDto) {
    const key = message.key as WAMessageKey;

    if (!chatwootMessageIds.messageId || !key?.id) return;

    const result = await this.prismaRepository.$executeRaw`
      UPDATE "Message"
      SET
        "chatwootMessageId" = ${chatwootMessageIds.messageId},
        "chatwootConversationId" = ${chatwootMessageIds.conversationId},
        "chatwootInboxId" = ${chatwootMessageIds.inboxId},
        "chatwootContactInboxSourceId" = ${chatwootMessageIds.contactInboxSourceId},
        "chatwootIsRead" = ${chatwootMessageIds.isRead || false}
      WHERE "instanceId" = ${instance.instanceId}
      AND "key"->>'id' = ${key.id}
    `;

    this.logger.verbose(`Update result: ${result} rows affected`);

    if (this.isImportHistoryAvailable()) {
      chatwootImport.updateMessageSourceID(chatwootMessageIds.messageId as any, key.id);
    }
  }

  private async getMessageByKeyId(instance: InstanceDto, keyId: string): Promise<any> {
    const messages = await this.prismaRepository.$queryRaw`
      SELECT * FROM "Message"
      WHERE "instanceId" = ${instance.instanceId}
      AND "key"->>'id' = ${keyId}
      LIMIT 1
    `;

    return (messages as any[])[0] || null;
  }

  private async getReplyToIds(msg: any, instance: InstanceDto): Promise<{ in_reply_to: string; in_reply_to_external_id: string }> {
    let inReplyTo: any = null;
    let inReplyToExternalId: any = null;

    if (msg) {
      inReplyToExternalId = msg.message?.extendedTextMessage?.contextInfo?.stanzaId ?? msg.contextInfo?.stanzaId;
      if (inReplyToExternalId) {
        const message = await this.getMessageByKeyId(instance, inReplyToExternalId);
        if (message?.chatwootMessageId) inReplyTo = message.chatwootMessageId;
      }
    }

    return { in_reply_to: inReplyTo, in_reply_to_external_id: inReplyToExternalId };
  }

  private async getQuotedMessage(msg: any, instance: InstanceDto): Promise<Quoted> {
    if (msg?.content_attributes?.in_reply_to) {
      const message = await this.prismaRepository.message.findFirst({
        where: { chatwootMessageId: msg?.content_attributes?.in_reply_to, instanceId: instance.instanceId },
      });

      const key = message?.key as WAMessageKey;
      const messageContent = message?.message as WAMessageContent;

      if (messageContent && key?.id) {
        return { key, message: messageContent };
      }
    }

    return null as any;
  }

  private isMediaMessage(message: any) {
    const media = [
      'imageMessage',
      'documentMessage',
      'documentWithCaptionMessage',
      'audioMessage',
      'videoMessage',
      'stickerMessage',
      'viewOnceMessageV2',
    ];
    const messageKeys = Object.keys(message);
    return messageKeys.some((key) => media.includes(key));
  }

  private getAdsMessage(msg: any) {
    interface AdsMessage {
      title: string;
      body: string;
      thumbnailUrl: string;
      sourceUrl: string;
    }

    const adsMessage: AdsMessage | undefined = {
      title: msg.extendedTextMessage?.contextInfo?.externalAdReply?.title || msg.contextInfo?.externalAdReply?.title,
      body: msg.extendedTextMessage?.contextInfo?.externalAdReply?.body || msg.contextInfo?.externalAdReply?.body,
      thumbnailUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.thumbnailUrl || msg.contextInfo?.externalAdReply?.thumbnailUrl,
      sourceUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.sourceUrl || msg.contextInfo?.externalAdReply?.sourceUrl,
    };

    return adsMessage;
  }

  private getReactionMessage(msg: any) {
    interface ReactionMessage {
      key: { id: string; fromMe: boolean; remoteJid: string; participant?: string };
      text: string;
    }
    const reactionMessage: ReactionMessage | undefined = msg?.reactionMessage;
    return reactionMessage;
  }

  private getTypeMessage(msg: any) {
    const types = {
      conversation: msg.conversation,
      imageMessage: msg.imageMessage?.caption,
      videoMessage: msg.videoMessage?.caption,
      extendedTextMessage: msg.extendedTextMessage?.text,
      messageContextInfo: msg.messageContextInfo?.stanzaId,
      stickerMessage: undefined,
      documentMessage: msg.documentMessage?.caption,
      documentWithCaptionMessage: msg.documentWithCaptionMessage?.message?.documentMessage?.caption,
      audioMessage: msg.audioMessage ? (msg.audioMessage.caption ?? '') : undefined,
      contactMessage: msg.contactMessage?.vcard,
      contactsArrayMessage: msg.contactsArrayMessage,
      locationMessage: msg.locationMessage,
      liveLocationMessage: msg.liveLocationMessage,
      listMessage: msg.listMessage,
      listResponseMessage: msg.listResponseMessage,
      viewOnceMessageV2:
        msg?.message?.viewOnceMessageV2?.message?.imageMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.videoMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.audioMessage?.url,
    };

    return types;
  }

  private getMessageContent(types: any) {
    const typeKey = Object.keys(types).find((key) => types[key] !== undefined);
    let result = typeKey ? types[typeKey] : undefined;

    if (result && typeof result === 'string' && result.includes('externalAdReplyBody|')) {
      result = result.split('externalAdReplyBody|').filter(Boolean).join('');
    }

    if (typeKey === 'locationMessage' || typeKey === 'liveLocationMessage') {
      const latitude = result.degreesLatitude;
      const longitude = result.degreesLongitude;

      const locationName = result?.name;
      const locationAddress = result?.address;

      const formattedLocation =
        `*${i18next.t('cw.locationMessage.location')}:*\n\n` +
        `_${i18next.t('cw.locationMessage.latitude')}:_ ${latitude} \n` +
        `_${i18next.t('cw.locationMessage.longitude')}:_ ${longitude} \n` +
        (locationName ? `_${i18next.t('cw.locationMessage.locationName')}:_ ${locationName}\n` : '') +
        (locationAddress ? `_${i18next.t('cw.locationMessage.locationAddress')}:_ ${locationAddress} \n` : '') +
        `_${i18next.t('cw.locationMessage.locationUrl')}:_ ` +
        `https://www.google.com/maps/search/?api=1&query=${latitude},${longitude}`;

      return formattedLocation;
    }

    if (typeKey === 'contactMessage') {
      const vCardData = result.split('\n');
      const contactInfo: any = {};

      vCardData.forEach((line: string) => {
        const [key, value] = line.split(':');
        if (key && value) contactInfo[key] = value;
      });

      let formattedContact =
        `*${i18next.t('cw.contactMessage.contact')}:*\n\n` + `_${i18next.t('cw.contactMessage.name')}:_ ${contactInfo['FN']}`;

      let numberCount = 1;
      Object.keys(contactInfo).forEach((key) => {
        if (key.startsWith('item') && key.includes('TEL')) {
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${contactInfo[key]}`;
          numberCount++;
        } else if (key.includes('TEL')) {
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${contactInfo[key]}`;
          numberCount++;
        }
      });

      return formattedContact;
    }

    if (typeKey === 'contactsArrayMessage') {
      const formattedContacts = result.contacts.map((contact: any) => {
        const vCardData = contact.vcard.split('\n');
        const contactInfo: any = {};

        vCardData.forEach((line: string) => {
          const [key, value] = line.split(':');
          if (key && value) contactInfo[key] = value;
        });

        let formattedContact = `*${i18next.t('cw.contactMessage.contact')}:*\n\n_${i18next.t('cw.contactMessage.name')}:_ ${
          contact.displayName
        }`;

        let numberCount = 1;
        Object.keys(contactInfo).forEach((key) => {
          if (key.startsWith('item') && key.includes('TEL')) {
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${contactInfo[key]}`;
            numberCount++;
          } else if (key.includes('TEL')) {
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${contactInfo[key]}`;
            numberCount++;
          }
        });

        return formattedContact;
      });

      return formattedContacts.join('\n\n');
    }

    if (typeKey === 'listMessage') {
      const listTitle = result?.title || 'Unknown';
      const listDescription = result?.description || 'Unknown';
      const listFooter = result?.footerText || 'Unknown';

      let formattedList = '*List Menu:*\n\n' + '_Title_: ' + listTitle + '\n' + '_Description_: ' + listDescription + '\n' + '_Footer_: ' + listFooter;

      if (result.sections && result.sections.length > 0) {
        result.sections.forEach((section: any, sectionIndex: number) => {
          formattedList += '\n\n*Section ' + (sectionIndex + 1) + ':* ' + (section.title || 'Unknown\n');

          if (section.rows && section.rows.length > 0) {
            section.rows.forEach((row: any, rowIndex: number) => {
              formattedList += '\n*Line ' + (rowIndex + 1) + ':*\n';
              formattedList += '_▪️ Title:_ ' + (row.title || 'Unknown') + '\n';
              formattedList += '_▪️ Description:_ ' + (row.description || 'Unknown') + '\n';
              formattedList += '_▪️ ID:_ ' + (row.rowId || 'Unknown') + '\n';
            });
          } else {
            formattedList += '\nNo lines found in this section.\n';
          }
        });
      } else {
        formattedList += '\nNo sections found.\n';
      }

      return formattedList;
    }

    if (typeKey === 'listResponseMessage') {
      const responseTitle = result?.title || 'Unknown';
      const responseDescription = result?.description || 'Unknown';
      const responseRowId = result?.singleSelectReply?.selectedRowId || 'Unknown';

      const formattedResponseList =
        '*List Response:*\n\n' + '_Title_: ' + responseTitle + '\n' + '_Description_: ' + responseDescription + '\n' + '_ID_: ' + responseRowId;
      return formattedResponseList;
    }

    return result;
  }

  public getConversationMessage(msg: any) {
    const types = this.getTypeMessage(msg);
    return this.getMessageContent(types);
  }

  // =========================================================
  // eventWhatsapp (mantido; só trocas internas p/ REST)
  // =========================================================

  public async eventWhatsapp(event: string, instance: InstanceDto, body: any) {
    try {
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      if (!waInstance) {
        this.logger.warn('wa instance not found');
        return null;
      }

      await this.clientCw(instance);

      if (this.provider?.ignoreJids && this.provider?.ignoreJids.length > 0) {
        const ignoreJids: any = this.provider?.ignoreJids;

        let ignoreGroups = false;
        let ignoreContacts = false;

        if (ignoreJids.includes('@g.us')) ignoreGroups = true;
        if (ignoreJids.includes('@s.whatsapp.net')) ignoreContacts = true;

        if (ignoreGroups && body?.key?.remoteJid.endsWith('@g.us')) {
          this.logger.warn('Ignoring message from group: ' + body?.key?.remoteJid);
          return;
        }

        if (ignoreContacts && body?.key?.remoteJid.endsWith('@s.whatsapp.net')) {
          this.logger.warn('Ignoring message from contact: ' + body?.key?.remoteJid);
          return;
        }

        if (ignoreJids.includes(body?.key?.remoteJid)) {
          this.logger.warn('Ignoring message from jid: ' + body?.key?.remoteJid);
          return;
        }
      }

      // ======== messages.upsert / send.message ========
      if (event === 'messages.upsert' || event === 'send.message') {
        this.logger.info(`[${event}] New message received - Instance: ${JSON.stringify(body, null, 2)}`);
        if (body.key.remoteJid === 'status@broadcast') return;

        if (body.message?.ephemeralMessage?.message) {
          body.message = { ...body.message?.ephemeralMessage?.message };
        }

        const originalMessage = await this.getConversationMessage(body.message);
        const bodyMessage = originalMessage
          ? originalMessage
              .replaceAll(/\*((?!\s)([^\n*]+?)(?<!\s))\*/g, '**$1**')
              .replaceAll(/_((?!\s)([^\n_]+?)(?<!\s))_/g, '*$1*')
              .replaceAll(/~((?!\s)([^\n~]+?)(?<!\s))~/g, '~~$1~~')
          : originalMessage;

        if (bodyMessage && bodyMessage.includes('/survey/responses/') && bodyMessage.includes('http')) return;

        const quotedId = body.contextInfo?.stanzaId || body.message?.contextInfo?.stanzaId;

        let quotedMsg: any = null;

        if (quotedId) {
          quotedMsg = await this.prismaRepository.message.findFirst({
            where: { key: { path: ['id'], equals: quotedId }, chatwootMessageId: { not: null } },
          });
        }

        const isMedia = this.isMediaMessage(body.message);
        const adsMessage = this.getAdsMessage(body);
        const reactionMessage = this.getReactionMessage(body.message);

        if (!bodyMessage && !isMedia && !reactionMessage) {
          this.logger.warn('no body message found');
          return;
        }

        const getConversation = await this.createConversation(instance, body);
        if (!getConversation) {
          this.logger.warn('conversation not found');
          return;
        }

        const messageType = body.key.fromMe ? 'outgoing' : 'incoming';

        if (isMedia) {
          const downloadBase64 = await waInstance?.getBase64FromMediaMessage({ message: { ...body } });

          let nameFile: string | undefined;
          const messageBody = body?.message[body?.messageType];
          const originalFilename = messageBody?.fileName || messageBody?.filename || messageBody?.message?.documentMessage?.fileName;
          if (originalFilename) {
            const parsedFile = path.parse(originalFilename);
            if (parsedFile.name && parsedFile.ext) nameFile = `${parsedFile.name}-${Math.floor(Math.random() * (99 - 10 + 1) + 10)}${parsedFile.ext}`;
          }

          if (!nameFile) nameFile = `${Math.random().toString(36).substring(7)}.${mimeTypes.extension(downloadBase64.mimetype) || ''}`;

          const fileData = Buffer.from(downloadBase64.base64, 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          if (body.key.remoteJid.includes('@g.us')) {
            const participantName = body.pushName;
            const rawPhoneNumber =
              body.key.addressingMode === 'lid' && !body.key.fromMe ? body.key.participantAlt.split('@')[0] : body.key.participant.split('@')[0];
            const phoneMatch = rawPhoneNumber.match(/^(\d{2})(\d{2})(\d{4})(\d{4})$/);

            let formattedPhoneNumber: string;
            if (phoneMatch) formattedPhoneNumber = `+${phoneMatch[1]} (${phoneMatch[2]}) ${phoneMatch[3]}-${phoneMatch[4]}`;
            else formattedPhoneNumber = `+${rawPhoneNumber}`;

            let content: string;
            if (!body.key.fromMe) content = bodyMessage ? `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}` : `**${formattedPhoneNumber} - ${participantName}:**`;
            else content = bodyMessage || '';

            const send = await this.sendData(getConversation, fileStream, nameFile, messageType, content, instance, body, 'WAID:' + body.key.id, quotedMsg);
            if (!send) {
              this.logger.warn('message not sent');
              return;
            }
            return send;
          } else {
            const send = await this.sendData(getConversation, fileStream, nameFile, messageType, bodyMessage, instance, body, 'WAID:' + body.key.id, quotedMsg);
            if (!send) {
              this.logger.warn('message not sent');
              return;
            }
            return send;
          }
        }

        if (reactionMessage) {
          if (reactionMessage.text) {
            const send = await this.createMessage(
              instance,
              getConversation,
              reactionMessage.text,
              messageType,
              false,
              [],
              { message: { extendedTextMessage: { contextInfo: { stanzaId: reactionMessage.key.id } } } },
              'WAID:' + body.key.id,
              quotedMsg,
            );
            if (!send) {
              this.logger.warn('message not sent');
              return;
            }
          }
          return;
        }

        const isAdsMessage = (adsMessage && adsMessage.title) || adsMessage.body || adsMessage.thumbnailUrl;
        if (isAdsMessage) {
          const imgBuffer = await axios.get(adsMessage.thumbnailUrl, { responseType: 'arraybuffer' });
          const extension = mimeTypes.extension(imgBuffer.headers['content-type']);
          const mimeType = extension && mimeTypes.lookup(extension);

          if (!mimeType) {
            this.logger.warn('mimetype of Ads message not found');
            return;
          }

          const random = Math.random().toString(36).substring(7);
          const nameFile = `${random}.${mimeTypes.extension(mimeType)}`;
          const fileData = Buffer.from(imgBuffer.data, 'binary');

          const img = await Jimp.read(fileData);
          await img.cover({ w: 320, h: 180 });
          const processedBuffer = await img.getBuffer(JimpMime.png);

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(processedBuffer);
          fileStream.push(null);

          const truncStr = (str: string, len: number) => (!str ? '' : str.length > len ? str.substring(0, len) + '...' : str);

          const title = truncStr(adsMessage.title, 40);
          const description = truncStr(adsMessage?.body, 75);

          const send = await this.sendData(
            getConversation,
            fileStream,
            nameFile,
            messageType,
            `${bodyMessage}\n\n\n**${title}**\n${description}\n${adsMessage.sourceUrl}`,
            instance,
            body,
            'WAID:' + body.key.id,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }
          return send;
        }

        if (body.key.remoteJid.includes('@g.us')) {
          const participantName = body.pushName;
          const rawPhoneNumber =
            body.key.addressingMode === 'lid' && !body.key.fromMe ? body.key.participantAlt.split('@')[0] : body.key.participant.split('@')[0];
          const phoneMatch = rawPhoneNumber.match(/^(\d{2})(\d{2})(\d{4})(\d{4})$/);

          let formattedPhoneNumber: string;
          if (phoneMatch) formattedPhoneNumber = `+${phoneMatch[1]} (${phoneMatch[2]}) ${phoneMatch[3]}-${phoneMatch[4]}`;
          else formattedPhoneNumber = `+${rawPhoneNumber}`;

          const content = !body.key.fromMe ? `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}` : `${bodyMessage}`;

          const send = await this.createMessage(instance, getConversation, content, messageType, false, [], body, 'WAID:' + body.key.id, quotedMsg);
          if (!send) {
            this.logger.warn('message not sent');
            return;
          }
          return send;
        } else {
          const send = await this.createMessage(instance, getConversation, bodyMessage, messageType, false, [], body, 'WAID:' + body.key.id, quotedMsg);
          if (!send) {
            this.logger.warn('message not sent');
            return;
          }
          return send;
        }
      }

      // ======== delete ========
      if (event === Events.MESSAGES_DELETE) {
        const chatwootDelete = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_DELETE;

        if (chatwootDelete === true) {
          if (!body?.key?.id) {
            this.logger.warn('message id not found');
            return;
          }

          const message = await this.getMessageByKeyId(instance, body.key.id);

          if (message?.chatwootMessageId && message?.chatwootConversationId) {
            await this.prismaRepository.message.deleteMany({
              where: { key: { path: ['id'], equals: body.key.id }, instanceId: instance.instanceId },
            });

            return await this.cwMessageDelete(message.chatwootConversationId, message.chatwootMessageId);
          }
        }
      }

      // ======== edit ========
      if (event === 'messages.edit' || event === 'send.message.update') {
        const editedMessageContent = body?.editedMessage?.conversation || body?.editedMessage?.extendedTextMessage?.text;
        const message = await this.getMessageByKeyId(instance, body?.key?.id);

        if (!message) {
          this.logger.warn('Message not found for edit event');
          return;
        }

        const key = message.key as WAMessageKey;
        const messageType = key?.fromMe ? 'outgoing' : 'incoming';

        if (message && message.chatwootConversationId && message.chatwootMessageId) {
          const editedText = `\n\n\`${i18next.t('cw.message.edited')}:\`\n\n${editedMessageContent}`;

          const send = await this.createMessage(
            instance,
            message.chatwootConversationId,
            editedText,
            messageType,
            false,
            [],
            { message: { extendedTextMessage: { contextInfo: { stanzaId: key.id } } } },
            'WAID:' + body.key.id,
            null as any,
          );
          if (!send) {
            this.logger.warn('edited message not sent');
            return;
          }
        }
        return;
      }

      // ======== read ========
      if (event === 'messages.read') {
        if (!body?.key?.id || !body?.key?.remoteJid) {
          this.logger.warn('message id not found');
          return;
        }

        const message = await this.getMessageByKeyId(instance, body.key.id);
        const conversationId = message?.chatwootConversationId;
        const contactInboxSourceId = message?.chatwootContactInboxSourceId;

        if (conversationId) {
          let sourceId = contactInboxSourceId;
          const inbox = (await this.getInbox(instance)) as cw_inbox;

          if (!sourceId && inbox) {
            const conversation = (await this.cwConversationGet(conversationId)) as any;
            sourceId = conversation.last_non_activity_message?.conversation?.contact_inbox?.source_id;
          }

          if (sourceId && (inbox as any)?.inbox_identifier) {
            const url =
              `/public/api/v1/inboxes/${(inbox as any).inbox_identifier}/contacts/${sourceId}` +
              `/conversations/${conversationId}/update_last_seen`;
            this.cw({ method: 'POST', url });
          }
        }
        return;
      }

      // ======== status.instance ========
      if (event === 'status.instance') {
        const data = body;
        const inbox = await this.getInbox(instance);

        if (!inbox) {
          this.logger.warn('inbox not found');
          return;
        }

        const msgStatus = i18next.t('cw.inbox.status', { inboxName: inbox.name, state: data.status });
        await this.createBotMessage(instance, msgStatus, 'incoming');
      }

      // ======== connection.update ========
      if (event === 'connection.update' && body.status === 'open') {
        const waInstance = this.waMonitor.waInstances[instance.instanceName];
        if (!waInstance) return;

        const now = Date.now();
        const timeSinceLastNotification = now - (waInstance.lastConnectionNotification || 0);

        if (waInstance.qrCode && waInstance.qrCode.count > 0) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.qrCode.count = 0;
          waInstance.lastConnectionNotification = now;
          chatwootImport.clearAll(instance);
        } else if (timeSinceLastNotification >= 30000) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.lastConnectionNotification = now;
        } else {
          this.logger.warn(
            `Connection notification skipped for ${instance.instanceName} - too frequent (${timeSinceLastNotification}ms since last)`,
          );
        }
      }

      // ======== qrcode.updated ========
      if (event === 'qrcode.updated') {
        if (body.statusCode === 500) {
          const erroQRcode = `🚨 ${i18next.t('qrlimitreached')}`;
          return await this.createBotMessage(instance, erroQRcode, 'incoming');
        } else {
          const fileData = Buffer.from(body?.qrcode.base64.replace('data:image/png;base64,', ''), 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          await this.createBotQr(instance, i18next.t('qrgeneratedsuccesfully'), 'incoming', fileStream, `${instance.instanceName}.png`);

          let msgQrCode = `⚡️${i18next.t('qrgeneratedsuccesfully')}\n\n${i18next.t('scanqr')}`;

          if (body?.qrcode?.pairingCode) {
            msgQrCode =
              msgQrCode +
              `\n\n*Pairing Code:* ${body.qrcode.pairingCode.substring(0, 4)}-${body.qrcode.pairingCode.substring(4, 8)}`;
          }

          await this.createBotMessage(instance, msgQrCode, 'incoming');
        }
      }
    } catch (error) {
      this.logger.error(error as any);
    }
  }

  public getNumberFromRemoteJid(remoteJid: string) {
    return remoteJid.replace(/:\d+/, '').split('@')[0];
  }

  public startImportHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) return;
    this.createBotMessage(instance, i18next.t('cw.import.startImport'), 'incoming');
  }

  public isImportHistoryAvailable() {
    const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI;
    return uri && uri !== 'postgres://user:password@hostname:port/dbname';
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: any[]) {
    if (!this.isImportHistoryAvailable()) return;
    chatwootImport.addHistoryMessages(instance, messagesRaw as any);
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: any[]) {
    if (!this.isImportHistoryAvailable()) return;
    return chatwootImport.addHistoryContacts(instance, contactsRaw as any);
  }

  public async importHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) return;

    this.createBotMessage(instance, i18next.t('cw.import.importingMessages'), 'incoming');

    const totalMessagesImported = await chatwootImport.importHistoryMessages(instance, this as any, await this.getInbox(instance), this.provider);
    this.updateContactAvatarInRecentConversations(instance);

    const msg = Number.isInteger(totalMessagesImported)
      ? i18next.t('cw.import.messagesImported', { totalMessagesImported })
      : i18next.t('cw.import.messagesException');

    this.createBotMessage(instance, msg, 'incoming');

    return totalMessagesImported;
  }

  public async updateContactAvatarInRecentConversations(instance: InstanceDto, limitContacts = 100) {
    try {
      if (!this.isImportHistoryAvailable()) return;

      await this.clientCw(instance);

      const inbox = await this.getInbox(instance);
      if (!inbox) return null;

      const recentContacts = await chatwootImport.getContactsOrderByRecentConversations(inbox as any, this.provider, limitContacts);

      const contactIdentifiers = recentContacts.map((c: any) => c.identifier).filter((identifier: any) => identifier !== null);

      const contactsWithProfilePicture = (
        await this.prismaRepository.contact.findMany({
          where: { instanceId: instance.instanceId, id: { in: contactIdentifiers }, profilePicUrl: { not: null } },
        })
      ).reduce((acc: Map<string, any>, c: any) => acc.set(c.id, c), new Map());

      recentContacts.forEach(async (contact: any) => {
        if (contactsWithProfilePicture.has(contact.identifier)) {
          await this.cwContactUpdate(contact.id, { avatar_url: contactsWithProfilePicture.get(contact.identifier).profilePictureUrl || null });
        }
      });
    } catch (error) {
      this.logger.error(`Error on update avatar in recent conversations: ${(error as any).toString()}`);
    }
  }

  public async syncLostMessages(instance: InstanceDto, chatwootConfig: ChatwootDto, prepareMessage: (message: any) => any) {
    try {
      if (!this.isImportHistoryAvailable()) return;
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) return;

      await this.clientCw(instance);

      const inbox = await this.getInbox(instance);

      const sqlMessages = `select * from messages m
      where account_id = ${chatwootConfig.accountId}
      and inbox_id = ${(inbox as any).id}
      and created_at >= now() - interval '6h'
      order by created_at desc`;

      const messagesData = (await this.pgClient.query(sqlMessages))?.rows;
      const ids: string[] = messagesData.filter((m: any) => !!m.source_id).map((m: any) => m.source_id.replace('WAID:', ''));

      const savedMessages = await this.prismaRepository.message.findMany({
        where: {
          Instance: { name: instance.instanceName },
          messageTimestamp: { gte: Number(dayjs().subtract(6, 'hours').unix()) },
          AND: ids.map((id) => ({ key: { path: ['id'], not: id } })),
        },
      });

      const filteredMessages = savedMessages.filter((msg: any) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid));
      const messagesRaw: any[] = [];
      for (const m of filteredMessages) {
        if (!m.message || !m.key || !m.messageTimestamp) continue;
        if (Long.isLong(m?.messageTimestamp)) m.messageTimestamp = m.messageTimestamp?.toNumber();
        messagesRaw.push(prepareMessage(m as any));
      }

      this.addHistoryMessages(instance, messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)));

      await chatwootImport.importHistoryMessages(instance, this as any, inbox as any, this.provider);
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      waInstance.clearCacheChatwoot();
    } catch {
      return;
    }
  }
}
