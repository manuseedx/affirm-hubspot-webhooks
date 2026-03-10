import { AffirmEventType, NormalizedAffirmEvent, WebhookCategory } from './types';

const defaultSource = 'affirm';

function parseAffirmPayload(rawBody: string, contentType: string): Record<string, unknown> {
  const ct = (contentType || '').toLowerCase();

  if (ct.includes('application/json') || rawBody.trim().startsWith('{')) {
    const parsed = JSON.parse(rawBody || '{}');
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('invalid_payload');
    }
    return parsed as Record<string, unknown>;
  }

  const params = new URLSearchParams(rawBody);a
  const out: Record<string, unknown> = {};
  for (const [key, value] of params) {
    out[key] = value;
  }
  return out;
}

function normalizeTopLevelKeys(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    out[normalizeKey(key)] = value;
  }
  return out;
}

function normalizeKey(key: string): string {
  return String(key).trim().toLowerCase().replace(/[\s-]+/g, '_');
}

function pickString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  const normalized = String(value).trim();
  return normalized.length ? normalized : undefined;
}

function normalizeString(value: unknown): string | null {
  const normalized = pickString(value);
  return normalized ?? null;
}

function normalizeEmail(value: unknown): string | null {
  const normalized = pickString(value);
  if (!normalized) return null;
  const email = normalized.toLowerCase();
  return email.includes('@') ? email : null;
}

function normalizeOccurredAt(value: string | null): string | null {
  if (!value) return null;

  let candidate = value.trim();
  candidate = candidate.replace(/(\.\d{3})\d+/, '$1');

  const hasTimezone = /z$|[+-]\d{2}:\d{2}$/i.test(candidate);
  if (!hasTimezone) candidate += 'Z';

  const parsed = new Date(candidate);
  if (Number.isNaN(parsed.getTime())) return null;

  return parsed.toISOString();
}

function toFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function centsToDollarsString(value: unknown): string | null {
  const cents = typeof value === 'number' ? value : toFiniteNumber(value);
  if (cents === null) return null;

  const fixed = (cents / 100).toFixed(2);
  return fixed.replace(/\.00$/, '').replace(/(\.\d)0$/, '$1');
}

function classifyCategory(checkoutToken: string | null, event: string | null): WebhookCategory {
  if (!checkoutToken) return 'PREQUAL';

  const normalizedEvent = (event || '').toLowerCase();
  if (normalizedEvent === 'opened' || normalizedEvent === 'open') {
    return 'OPEN';
  }

  return 'FINANCING';
}

function toEventType(category: WebhookCategory): AffirmEventType {
  if (category === 'PREQUAL') return 'prequal';
  if (category === 'OPEN') return 'open';
  return 'financing';
}

async function sha256Hex(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

async function computeDedupeSeed(args: {
  category: WebhookCategory;
  id: string | null;
  email: string | null;
  checkoutToken: string | null;
  event: string | null;
  occurredAt: string | null;
  rawBody: string;
}): Promise<string> {
  const { category, id, email, checkoutToken, event, occurredAt, rawBody } = args;

  if (id) return sha256Hex(`${category}|id:${id}`);
  if (checkoutToken && event && occurredAt) {
    return sha256Hex(`${category}|${checkoutToken}|${event}|${occurredAt}|${email || ''}`);
  }
  if (occurredAt) {
    return sha256Hex(`${category}|${occurredAt}|${email || ''}|${checkoutToken || ''}|${event || ''}`);
  }
  return sha256Hex(rawBody);
}

function extractExternalId(normalized: Record<string, unknown>, checkoutToken: string | null): string | null {
  const candidates = [
    normalized.id,
    normalized.external_id,
    normalized.application_id,
    normalized.prequalification_id,
    checkoutToken,
    normalized.uuid,
    normalized.lead_id
  ];

  for (const candidate of candidates) {
    const value = normalizeString(candidate);
    if (value) return value;
  }

  return null;
}

export async function normalizeAffirmEvent(
  rawBody: string,
  contentType: string,
  sourceName?: string
): Promise<NormalizedAffirmEvent> {
  const source = sourceName ?? defaultSource;
  const parsed = parseAffirmPayload(rawBody, contentType);
  const normalized = normalizeTopLevelKeys(parsed);

  const email = normalizeEmail(normalized.email_address ?? normalized.email);
  const checkoutToken = normalizeString(normalized.checkout_token);
  const event = normalizeString(normalized.event);
  const occurredAt = normalizeOccurredAt(
    normalizeString(normalized.event_timestamp) || normalizeString(normalized.created)
  );
  const category = classifyCategory(checkoutToken, event);
  const eventType = toEventType(category);
  const dedupeSeed = await computeDedupeSeed({
    category,
    id: normalizeString(normalized.id),
    email,
    checkoutToken,
    event,
    occurredAt,
    rawBody
  });

  const payload: Record<string, unknown> = {
    ...normalized,
    category,
    email_address: email,
    checkout_token: checkoutToken,
    event,
    occurred_at: occurredAt,
    total_dollars: centsToDollarsString(normalized.total),
    approved_amount_dollars: centsToDollarsString(normalized.approved_amount)
  };

  const externalId = extractExternalId(normalized, checkoutToken) ?? `evt_${dedupeSeed}`;

  return {
    eventId: crypto.randomUUID(),
    source,
    eventType,
    externalId,
    dedupeKey: `${source}:${dedupeSeed}`,
    receivedAt: new Date().toISOString(),
    payload
  };
}
