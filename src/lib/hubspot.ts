import { Env, QueueMessageBody } from './types';

interface HubSpotResult {
  ok: boolean;
  responseCode: number;
  responseBody: string;
}

class HubSpotRequestError extends Error {
  status: number;
  body: string;

  constructor(status: number, body: string) {
    super(`HubSpot error ${status}: ${body}`);
    this.status = status;
    this.body = body;
  }
}

const HS_BASE = 'https://api.hubapi.com';

function isDryRunEnabled(env: Env): boolean {
  return (env.DRY_RUN_HUBSPOT ?? '').toLowerCase() === 'true';
}

function pickString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  const normalized = String(value).trim();
  return normalized.length ? normalized : undefined;
}

function normalizeString(value: unknown): string | null {
  return pickString(value) ?? null;
}

function normalizeEmail(value: unknown): string | null {
  const normalized = pickString(value);
  if (!normalized) return null;
  const email = normalized.toLowerCase();
  return email.includes('@') ? email : null;
}

function normalizeOccurredAt(value: unknown): string | null {
  const normalized = pickString(value);
  if (!normalized) return null;

  let candidate = normalized.trim();
  candidate = candidate.replace(/(\.\d{3})\d+/, '$1');

  const hasTimezone = /z$|[+-]\d{2}:\d{2}$/i.test(candidate);
  if (!hasTimezone) candidate += 'Z';

  const parsed = new Date(candidate);
  if (Number.isNaN(parsed.getTime())) return null;

  return parsed.toISOString();
}

function centsToDollarsString(value: unknown): string | null {
  if (value === null || value === undefined || value === '') return null;
  const cents = Number(value);
  if (!Number.isFinite(cents)) return null;

  const fixed = (cents / 100).toFixed(2);
  return fixed.replace(/\.00$/, '').replace(/(\.\d)0$/, '$1');
}

async function sha256Hex(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

function uuidFromHex(hex: string): string {
  const normalized = hex.replace(/[^0-9a-f]/gi, '').slice(0, 32).padEnd(32, '0');
  return `${normalized.slice(0, 8)}-${normalized.slice(8, 12)}-${normalized.slice(12, 16)}-${normalized.slice(16, 20)}-${normalized.slice(20, 32)}`;
}

function getPayload(event: QueueMessageBody): Record<string, unknown> {
  if (event.payload && typeof event.payload === 'object' && !Array.isArray(event.payload)) {
    return event.payload;
  }
  return {};
}

function endpointPath(value: string | undefined, fallback: string): string {
  if (!value) return fallback;

  try {
    return new URL(value).pathname;
  } catch {
    return value.startsWith('/') ? value : fallback;
  }
}

function getEventPath(env: Env): string {
  return endpointPath(env.HUBSPOT_EVENT_ENDPOINT, '/events/v3/send');
}

function getFinancingObjectType(env: Env): string {
  const explicit = normalizeString(env.HS_FINANCING_OBJECT_TYPE_ID);
  if (explicit) return explicit;

  const endpoint = normalizeString(env.HUBSPOT_OBJECT_ENDPOINT);
  if (endpoint) {
    try {
      const pathname = new URL(endpoint).pathname;
      const parts = pathname.split('/').filter(Boolean);
      return decodeURIComponent(parts[parts.length - 1]);
    } catch {
      const parts = endpoint.split('/').filter(Boolean);
      return decodeURIComponent(parts[parts.length - 1]);
    }
  }

  throw new Error('HS_FINANCING_OBJECT_TYPE_ID is not configured.');
}

async function hsRequest<T>(env: Env, method: string, path: string, body?: unknown): Promise<T | null> {
  const token = env.HUBSPOT_ACCESS_TOKEN;
  if (!token) {
    throw new Error('HUBSPOT_ACCESS_TOKEN is not configured.');
  }

  const response = await fetch(path.startsWith('http') ? path : `${HS_BASE}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
      accept: 'application/json'
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  });

  if (!response.ok) {
    throw new HubSpotRequestError(response.status, await response.text());
  }

  if (response.status === 204) return null;

  const text = await response.text();
  if (!text) return null;
  return JSON.parse(text) as T;
}

async function getCachedContactId(env: Env, email: string): Promise<string | null> {
  try {
    const row = await env.DB.prepare(
      'SELECT hs_contact_id FROM contact_index WHERE email = ?'
    )
      .bind(email)
      .first<{ hs_contact_id: string }>();

    return row?.hs_contact_id ?? null;
  } catch {
    return null;
  }
}

async function cacheContactId(env: Env, email: string, contactId: string): Promise<void> {
  try {
    await env.DB.prepare(
      `INSERT INTO contact_index (email, hs_contact_id, updated_at)
       VALUES (?, ?, ?)
       ON CONFLICT(email) DO UPDATE SET
         hs_contact_id=excluded.hs_contact_id,
         updated_at=excluded.updated_at`
    )
      .bind(email, contactId, new Date().toISOString())
      .run();
  } catch {
    // best-effort cache only
  }
}

async function getCachedFinancingId(env: Env, checkoutToken: string): Promise<string | null> {
  try {
    const row = await env.DB.prepare(
      'SELECT hs_financing_id FROM financing_index WHERE checkout_token = ?'
    )
      .bind(checkoutToken)
      .first<{ hs_financing_id: string }>();

    return row?.hs_financing_id ?? null;
  } catch {
    return null;
  }
}

async function cacheFinancingId(env: Env, checkoutToken: string, financingId: string): Promise<void> {
  try {
    await env.DB.prepare(
      `INSERT INTO financing_index (checkout_token, hs_financing_id, updated_at)
       VALUES (?, ?, ?)
       ON CONFLICT(checkout_token) DO UPDATE SET
         hs_financing_id=excluded.hs_financing_id,
         updated_at=excluded.updated_at`
    )
      .bind(checkoutToken, financingId, new Date().toISOString())
      .run();
  } catch {
    // best-effort cache only
  }
}

async function hsFindOrCreateContact(env: Env, email: string): Promise<string> {
  const cachedId = await getCachedContactId(env, email);
  if (cachedId) return cachedId;

  const found = await hsRequest<{ results: Array<{ id: string }> }>(
    env,
    'POST',
    '/crm/v3/objects/contacts/search',
    {
      filterGroups: [
        {
          filters: [{ propertyName: 'email', operator: 'EQ', value: email }]
        }
      ],
      properties: ['email'],
      limit: 1
    }
  );

  const existingId = found?.results?.[0]?.id;
  if (existingId) {
    await cacheContactId(env, email, existingId);
    return existingId;
  }

  const created = await hsRequest<{ id: string }>(
    env,
    'POST',
    '/crm/v3/objects/contacts',
    { properties: { email } }
  );

  if (!created?.id) {
    throw new Error('HubSpot contact create returned no id.');
  }

  await cacheContactId(env, email, created.id);
  return created.id;
}

async function hsSendPrequalEvent(env: Env, args: {
  contactId: string;
  email: string;
  occurredAt: string | null;
  uuid: string;
  decision: string;
  event: string;
  amount: string;
  platform: string;
}): Promise<void> {
  if (!env.HS_EVENT_PREQUAL) {
    throw new Error('HS_EVENT_PREQUAL is not configured.');
  }

  const body: Record<string, unknown> = {
    eventName: env.HS_EVENT_PREQUAL,
    objectId: args.contactId,
    email: args.email,
    uuid: args.uuid,
    properties: {
      decision: args.decision,
      event: args.event,
      amount: args.amount,
      platform: args.platform
    }
  };

  if (args.occurredAt) {
    body.occurredAt = args.occurredAt;
  }

  await hsRequest(env, 'POST', getEventPath(env), body);
}

async function hsSendOpenEvent(env: Env, args: {
  contactId: string;
  email: string;
  occurredAt: string | null;
  uuid: string;
  checkoutToken: string;
  amount: string;
  platform: string;
}): Promise<void> {
  if (!env.HS_EVENT_OPEN) {
    throw new Error('HS_EVENT_OPEN is not configured.');
  }

  const body: Record<string, unknown> = {
    eventName: env.HS_EVENT_OPEN,
    objectId: args.contactId,
    email: args.email,
    uuid: args.uuid,
    properties: {
      checkout_token: args.checkoutToken,
      amount: args.amount,
      platform: args.platform
    }
  };

  if (args.occurredAt) {
    body.occurredAt = args.occurredAt;
  }

  await hsRequest(env, 'POST', getEventPath(env), body);
}

async function hsUpsertFinancing(env: Env, args: {
  contactId: string;
  email: string;
  checkoutToken: string;
  event: string;
  totalDollars: string | null;
  decision: string | null;
}): Promise<string> {
  const objectType = getFinancingObjectType(env);
  const uniqueProp = normalizeString(env.HS_FINANCING_UNIQUE_PROPERTY) || 'checkout_id';

  let financingId = await getCachedFinancingId(env, args.checkoutToken);

  if (!financingId) {
    const found = await hsRequest<{ results: Array<{ id: string }> }>(
      env,
      'POST',
      `/crm/v3/objects/${objectType}/search`,
      {
        filterGroups: [
          {
            filters: [{ propertyName: uniqueProp, operator: 'EQ', value: args.checkoutToken }]
          }
        ],
        properties: [uniqueProp],
        limit: 1
      }
    );

    financingId = found?.results?.[0]?.id ?? null;
  }

  const properties: Record<string, string> = {
    [uniqueProp]: args.checkoutToken,
    event: args.event,
    email_address: args.email,
    platform: env.DEFAULT_PLATFORM || 'Affirm',
    financing_type: 'Applications'
  };

  if (args.totalDollars) properties.total = args.totalDollars;
  if (args.decision) properties.decision = args.decision;

  if (financingId) {
    await hsRequest(env, 'PATCH', `/crm/v3/objects/${objectType}/${financingId}`, { properties });
  } else {
    const created = await hsRequest<{ id: string }>(
      env,
      'POST',
      `/crm/v3/objects/${objectType}`,
      { properties }
    );

    if (!created?.id) {
      throw new Error('HubSpot financing create returned no id.');
    }

    financingId = created.id;
  }

  await hsRequest(
    env,
    'PUT',
    `/crm/v4/objects/contact/${args.contactId}/associations/default/${objectType}/${financingId}`
  );

  await cacheFinancingId(env, args.checkoutToken, financingId);
  return financingId;
}

export async function sendToHubSpot(
  event: QueueMessageBody,
  env: Env
): Promise<HubSpotResult> {
  const payload = getPayload(event);
  const email = normalizeEmail(payload.email_address ?? payload.email);
  const checkoutToken = normalizeString(payload.checkout_token);
  const eventName = normalizeString(payload.event) || '';
  const occurredAt = normalizeOccurredAt(payload.occurred_at ?? payload.event_timestamp ?? payload.created);
  const decision = normalizeString(payload.decision) || '';
  const totalDollars = normalizeString(payload.total_dollars) || centsToDollarsString(payload.total);
  const approvedAmountDollars =
    normalizeString(payload.approved_amount_dollars) || centsToDollarsString(payload.approved_amount);
  const platform = normalizeString(payload.platform) || env.DEFAULT_PLATFORM || 'Affirm';
  const uuid = uuidFromHex(await sha256Hex(`${event.eventType}|${event.eventId}`));

  if (!email) {
    return {
      ok: false,
      responseCode: 400,
      responseBody: 'Missing email_address.'
    };
  }

  if ((event.eventType === 'open' || event.eventType === 'financing') && !checkoutToken) {
    return {
      ok: false,
      responseCode: 400,
      responseBody: 'Missing checkout_token.'
    };
  }

  if (isDryRunEnabled(env)) {
    return {
      ok: true,
      responseCode: 202,
      responseBody: JSON.stringify({
        dry_run: true,
        eventType: event.eventType,
        email,
        checkoutToken,
        occurredAt,
        payload
      })
    };
  }

  try {
    const contactId = await hsFindOrCreateContact(env, email);

    if (event.eventType === 'prequal') {
      await hsSendPrequalEvent(env, {
        contactId,
        email,
        occurredAt,
        uuid,
        decision,
        event: eventName,
        amount: approvedAmountDollars || '',
        platform
      });

      return {
        ok: true,
        responseCode: 200,
        responseBody: JSON.stringify({ eventType: event.eventType, contactId })
      };
    }

    if (event.eventType === 'open') {
      await hsSendOpenEvent(env, {
        contactId,
        email,
        occurredAt,
        uuid,
        checkoutToken: checkoutToken || '',
        amount: totalDollars || '',
        platform
      });

      return {
        ok: true,
        responseCode: 200,
        responseBody: JSON.stringify({ eventType: event.eventType, contactId })
      };
    }

    const financingId = await hsUpsertFinancing(env, {
      contactId,
      email,
      checkoutToken: checkoutToken || '',
      event: eventName,
      totalDollars,
      decision: decision || null
    });

    return {
      ok: true,
      responseCode: 200,
      responseBody: JSON.stringify({ eventType: event.eventType, contactId, financingId })
    };
  } catch (error) {
    if (error instanceof HubSpotRequestError) {
      return {
        ok: false,
        responseCode: error.status,
        responseBody: error.body
      };
    }

    return {
      ok: false,
      responseCode: 500,
      responseBody: error instanceof Error ? error.message : 'unknown_error'
    };
  }
}
