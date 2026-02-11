type LogLevel = "debug" | "info" | "warn" | "error";
type WebhookCategory = "PREQUAL" | "OPEN" | "FINANCING";

interface Env {
  // Bindings
  DB: D1Database;
  AFFIRM_QUEUE: Queue;

  // Secrets (set in Cloudflare dashboard)
  HS_ACCESS_TOKEN: string;
  WEBHOOK_TOKEN: string;

  // Vars (from wrangler.jsonc)
  HS_EVENT_PREQUAL: string;
  HS_EVENT_OPEN: string;

  HS_FINANCING_OBJECT_TYPE_ID: string;   // e.g. "2-37647069"
  HS_FINANCING_UNIQUE_PROPERTY: string;  // e.g. "checkout_id"

  DEFAULT_PLATFORM: string;              // "Affirm"
  LOG_LEVEL?: LogLevel;
}

interface QueuePayload {
  eventId: string;
}

interface ParsedAffirmPayload {
  id?: string;
  email_address?: string;
  checkout_token?: string;
  event?: string;
  event_timestamp?: string;
  created?: string;

  total?: unknown;
  approved_amount?: unknown;
  decision?: string;
}

const HS_BASE = "https://api.hubapi.com";
const DEFAULT_LOG_LEVEL: LogLevel = "info";

let schemaInit: Promise<void> | null = null;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight (helps when testing in browser tools)
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true }, 200);
    }

    if (request.method !== "POST") {
      return json({ ok: false, error: "Method Not Allowed" }, 405);
    }

    // Token auth (query string or header)
    const provided =
      request.headers.get("x-webhook-token") ||
      url.searchParams.get("token") ||
      "";

    if (!provided || !safeEqual(provided, env.WEBHOOK_TOKEN || "")) {
      return json({ ok: false, error: "Unauthorized" }, 401);
    }

    await ensureSchema(env);

    const contentType = request.headers.get("content-type") || "";
    const rawBody = await request.text();

    const payload = parseAffirmPayload(rawBody, contentType);

    const email = normalizeEmail(payload.email_address);
    const checkoutToken = normalizeString(payload.checkout_token);
    const event = normalizeString(payload.event);

    const occurredAt = normalizeOccurredAt(
      normalizeString(payload.event_timestamp) || normalizeString(payload.created)
    );

    const category = classifyCategory(checkoutToken, event);

    const eventId = await computeEventId({
      category,
      id: normalizeString(payload.id),
      email,
      checkoutToken,
      event,
      occurredAt,
      rawBody
    });

    // Idempotency insert: if it already exists, ignore and return 204
    const nowIso = new Date().toISOString();
    const insert = await env.DB.prepare(
      `INSERT OR IGNORE INTO webhook_events
       (event_id, received_at, content_type, category, email, checkout_token, event, event_timestamp, raw_body, status, attempts)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED', 0)`
    )
      .bind(
        eventId,
        nowIso,
        contentType,
        category,
        email || null,
        checkoutToken || null,
        event || null,
        occurredAt || null,
        rawBody
      )
      .run();

    const changes = (insert as any)?.meta?.changes ?? 0;

    if (changes === 0) {
      // Duplicate delivery (retry or re-send) -> treat as success
      return new Response(null, { status: 204 });
    }

    log(env, "info", "Webhook received", {
      category,
      email,
      checkoutToken,
      event
    });

    // Push to queue for async processing
    ctx.waitUntil(env.AFFIRM_QUEUE.send({ eventId }));

    // Affirm only needs a 2xx fast response
    return new Response(null, { status: 204 });
  },

  async queue(batch: MessageBatch<QueuePayload>, env: Env, ctx: ExecutionContext): Promise<void> {
    await ensureSchema(env);

    for (const message of batch.messages) {
      const eventId = message?.body?.eventId;

      if (!eventId) {
        message.ack();
        continue;
      }

      try {
        await processEvent(env, eventId);
        message.ack();
      } catch (err) {
        const retryable = isRetryableError(err);
        const delaySeconds = retryDelaySeconds(message.attempts);

        log(env, "error", "Queue processing error", {
          eventId,
          retryable,
          attempts: message.attempts,
          delaySeconds,
          error: String(err)
        });

        if (retryable) {
          message.retry({ delaySeconds });
        } else {
          message.ack();
        }
      }
    }
  }
};

// --------------------- DB schema ---------------------

async function ensureSchema(env: Env): Promise<void> {
  if (!schemaInit) {
    schemaInit = (async () => {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS webhook_events (
          event_id TEXT PRIMARY KEY,
          received_at TEXT NOT NULL,
          content_type TEXT NOT NULL,
          category TEXT NOT NULL,
          email TEXT,
          checkout_token TEXT,
          event TEXT,
          event_timestamp TEXT,
          raw_body TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          processed_at TEXT,
          last_error TEXT,
          hs_contact_id TEXT,
          hs_financing_id TEXT
        );
      `).run();

      await env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_webhook_events_status
        ON webhook_events(status);
      `).run();

      await env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_webhook_events_email
        ON webhook_events(email);
      `).run();

      await env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_webhook_events_checkout
        ON webhook_events(checkout_token);
      `).run();

      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS contact_index (
          email TEXT PRIMARY KEY,
          hs_contact_id TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
      `).run();

      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS financing_index (
          checkout_token TEXT PRIMARY KEY,
          hs_financing_id TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
      `).run();
    })();
  }

  await schemaInit;
}

// --------------------- Main processing ---------------------

async function processEvent(env: Env, eventId: string): Promise<void> {
  const row = await env.DB.prepare(
    `SELECT event_id, content_type, category, email, checkout_token, event, event_timestamp, raw_body, status
     FROM webhook_events
     WHERE event_id = ?`
  )
    .bind(eventId)
    .first<any>();

  if (!row) return;
  if (row.status === "PROCESSED") return;

  await env.DB.prepare(
    `UPDATE webhook_events
     SET status='PROCESSING', attempts=attempts+1, last_error=NULL
     WHERE event_id = ?`
  )
    .bind(eventId)
    .run();

  const nowIso = new Date().toISOString();

  try {
    const parsed = parseAffirmPayload(row.raw_body, row.content_type);

    const email = normalizeEmail(parsed.email_address || row.email || "");
    if (!email) throw new Error("Missing email_address");

    const checkoutToken = normalizeString(parsed.checkout_token || row.checkout_token);
    const event = normalizeString(parsed.event || row.event);
    const occurredAt = normalizeOccurredAt(
      normalizeString(parsed.event_timestamp || row.event_timestamp || parsed.created)
    );

    const category = (row.category as WebhookCategory) || classifyCategory(checkoutToken, event);

    const contactId = await hsFindOrCreateContact(env, email);

    let financingId: string | null = null;

    if (category === "PREQUAL") {
      await hsSendPrequalEvent(env, {
        contactId,
        email,
        occurredAt,
        uuid: uuidFromHex(await sha256Hex(`prequal|${eventId}`)),
        decision: normalizeString(parsed.decision) || "",
        event: event || "",
        amount: centsToDollarsString(parsed.approved_amount) || "",
        platform: env.DEFAULT_PLATFORM || "Affirm"
      });
    } else if (category === "OPEN") {
      await hsSendOpenEvent(env, {
        contactId,
        email,
        occurredAt,
        uuid: uuidFromHex(await sha256Hex(`open|${eventId}`)),
        checkoutToken: checkoutToken || "",
        amount: centsToDollarsString(parsed.total) || "",
        platform: env.DEFAULT_PLATFORM || "Affirm"
      });
    } else if (category === "FINANCING") {
      if (!checkoutToken) throw new Error("Missing checkout_token for FINANCING");

      financingId = await hsUpsertFinancing(env, {
        contactId,
        email,
        checkoutToken,
        event: event || "",
        totalDollars: centsToDollarsString(parsed.total),
        decision: normalizeString(parsed.decision)
      });
    }

    await env.DB.prepare(
      `UPDATE webhook_events
       SET status='PROCESSED', processed_at=?, last_error=NULL, hs_contact_id=?, hs_financing_id=?
       WHERE event_id=?`
    )
      .bind(nowIso, contactId, financingId, eventId)
      .run();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await env.DB.prepare(
      `UPDATE webhook_events
       SET status='FAILED', processed_at=?, last_error=?
       WHERE event_id=?`
    )
      .bind(nowIso, msg, eventId)
      .run();
    throw err;
  }
}

// --------------------- Parsing + classification ---------------------

function classifyCategory(checkoutToken: string | null, event: string | null): WebhookCategory {
  if (!checkoutToken) return "PREQUAL";

  const ev = (event || "").toLowerCase();
  if (ev === "opened" || ev === "open") return "OPEN";

  return "FINANCING";
}

function parseAffirmPayload(rawBody: string, contentType: string): ParsedAffirmPayload {
  const ct = (contentType || "").toLowerCase();
  let obj: Record<string, unknown> = {};

  if (ct.includes("application/json") || rawBody.trim().startsWith("{")) {
    try {
      const parsed = JSON.parse(rawBody);
      if (parsed && typeof parsed === "object") obj = parsed as Record<string, unknown>;
    } catch {
      obj = {};
    }
  } else {
    // Default to form encoding
    const params = new URLSearchParams(rawBody);
    for (const [k, v] of params.entries()) obj[k] = v;
  }

  const n = normalizeTopLevelKeys(obj);

  return {
    id: pickString(n["id"]),
    email_address: pickString(n["email_address"] ?? n["email"]),
    checkout_token: pickString(n["checkout_token"]),
    event: pickString(n["event"]),
    event_timestamp: pickString(n["event_timestamp"]),
    created: pickString(n["created"]),
    total: n["total"],
    approved_amount: n["approved_amount"],
    decision: pickString(n["decision"])
  };
}

function normalizeTopLevelKeys(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    out[normalizeKey(k)] = v;
  }
  return out;
}

function normalizeKey(key: string): string {
  return String(key).trim().toLowerCase().replace(/[\s-]+/g, "_");
}

function pickString(v: unknown): string | undefined {
  if (v === null || v === undefined) return undefined;
  const s = String(v).trim();
  return s.length ? s : undefined;
}

function normalizeString(v: unknown): string | null {
  const s = pickString(v);
  return s ? s : null;
}

function normalizeEmail(v: unknown): string | null {
  const s = pickString(v);
  if (!s) return null;
  const e = s.toLowerCase();
  return e.includes("@") ? e : null;
}

function normalizeOccurredAt(v: string | null): string | null {
  if (!v) return null;

  let s = v.trim();

  // If microseconds exist, trim to milliseconds (3 digits)
  s = s.replace(/(\.\d{3})\d+/, "$1");

  const hasTZ = /z$|[+-]\d{2}:\d{2}$/i.test(s);
  if (!hasTZ) s += "Z";

  // Validate
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;

  return d.toISOString();
}

function centsToDollarsString(v: unknown): string | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;

  const dollars = n / 100;
  const fixed = dollars.toFixed(2);
  return fixed.replace(/\.00$/, "").replace(/(\.\d)0$/, "$1");
}

// --------------------- Idempotency ID + UUID helpers ---------------------

async function computeEventId(args: {
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
    return sha256Hex(`${category}|${checkoutToken}|${event}|${occurredAt}|${email || ""}`);
  }
  if (occurredAt) {
    return sha256Hex(`${category}|${occurredAt}|${email || ""}|${checkoutToken || ""}|${event || ""}`);
  }
  return sha256Hex(rawBody);
}

async function sha256Hex(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, "0")).join("");
}

function uuidFromHex(hex: string): string {
  // Use first 32 hex chars and format as UUID-like string
  const h = hex.replace(/[^0-9a-f]/gi, "").slice(0, 32).padEnd(32, "0");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

// --------------------- HubSpot client ---------------------

class HubSpotError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string) {
    super(`HubSpot error ${status}: ${body}`);
    this.status = status;
    this.body = body;
  }
}

async function hsRequest<T>(env: Env, method: string, path: string, body?: unknown): Promise<T | null> {
  const res = await fetch(`${HS_BASE}${path}`, {
    method,
    headers: {
      "authorization": `Bearer ${env.HS_ACCESS_TOKEN}`,
      "content-type": "application/json",
      "accept": "application/json"
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  });

  if (!res.ok) {
    const text = await res.text();
    throw new HubSpotError(res.status, text);
  }

  if (res.status === 204) return null;

  const text = await res.text();
  if (!text) return null;

  return JSON.parse(text) as T;
}

async function hsFindOrCreateContact(env: Env, email: string): Promise<string> {
  // Cache first
  const cached = await env.DB.prepare(
    "SELECT hs_contact_id FROM contact_index WHERE email = ?"
  )
    .bind(email)
    .first<{ hs_contact_id: string }>();

  if (cached?.hs_contact_id) return cached.hs_contact_id;

  // Search
  const searchBody = {
    filterGroups: [
      { filters: [{ propertyName: "email", operator: "EQ", value: email }] }
    ],
    properties: ["email"],
    limit: 1
  };

  const found = await hsRequest<{ results: Array<{ id: string }> }>(
    env,
    "POST",
    "/crm/v3/objects/contacts/search",
    searchBody
  );

  const existingId = found?.results?.[0]?.id;
  if (existingId) {
    await upsertContactCache(env, email, existingId);
    return existingId;
  }

  // Create
  const created = await hsRequest<{ id: string }>(
    env,
    "POST",
    "/crm/v3/objects/contacts",
    { properties: { email } }
  );

  if (!created?.id) throw new Error("HubSpot contact create returned no id");

  await upsertContactCache(env, email, created.id);
  return created.id;
}

async function upsertContactCache(env: Env, email: string, contactId: string): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO contact_index (email, hs_contact_id, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(email) DO UPDATE SET
       hs_contact_id=excluded.hs_contact_id,
       updated_at=excluded.updated_at`
  )
    .bind(email, contactId, new Date().toISOString())
    .run();
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
  const body: any = {
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

  if (args.occurredAt) body.occurredAt = args.occurredAt;

  await hsRequest(env, "POST", "/events/v3/send", body);
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
  const body: any = {
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

  if (args.occurredAt) body.occurredAt = args.occurredAt;

  await hsRequest(env, "POST", "/events/v3/send", body);
}

async function hsUpsertFinancing(env: Env, args: {
  contactId: string;
  email: string;
  checkoutToken: string;
  event: string;
  totalDollars: string | null;
  decision: string | null;
}): Promise<string> {
  const objectType = env.HS_FINANCING_OBJECT_TYPE_ID;
  const uniqueProp = env.HS_FINANCING_UNIQUE_PROPERTY || "checkout_id";

  // D1 cache first
  const cached = await env.DB.prepare(
    "SELECT hs_financing_id FROM financing_index WHERE checkout_token = ?"
  )
    .bind(args.checkoutToken)
    .first<{ hs_financing_id: string }>();

  let financingId = cached?.hs_financing_id ?? null;

  // If not cached, search HubSpot
  if (!financingId) {
    const searchBody = {
      filterGroups: [
        { filters: [{ propertyName: uniqueProp, operator: "EQ", value: args.checkoutToken }] }
      ],
      properties: [uniqueProp],
      limit: 1
    };

    const found = await hsRequest<{ results: Array<{ id: string }> }>(
      env,
      "POST",
      `/crm/v3/objects/${objectType}/search`,
      searchBody
    );

    financingId = found?.results?.[0]?.id ?? null;
  }

  const properties: Record<string, string> = {
    [uniqueProp]: args.checkoutToken,
    event: args.event,
    email_address: args.email,
    platform: env.DEFAULT_PLATFORM || "Affirm",
    financing_type: "Applications"
  };

  if (args.totalDollars) properties.total = args.totalDollars;
  if (args.decision) properties.decision = args.decision;

  if (financingId) {
    await hsRequest(env, "PATCH", `/crm/v3/objects/${objectType}/${financingId}`, { properties });
  } else {
    const created = await hsRequest<{ id: string }>(
      env,
      "POST",
      `/crm/v3/objects/${objectType}`,
      { properties }
    );
    if (!created?.id) throw new Error("HubSpot financing create returned no id");
    financingId = created.id;
  }

  // Associate to contact (default association)
  await hsRequest(
    env,
    "PUT",
    `/crm/v4/objects/contact/${args.contactId}/associations/default/${objectType}/${financingId}`
  );

  // Update cache
  await env.DB.prepare(
    `INSERT INTO financing_index (checkout_token, hs_financing_id, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(checkout_token) DO UPDATE SET
       hs_financing_id=excluded.hs_financing_id,
       updated_at=excluded.updated_at`
  )
    .bind(args.checkoutToken, financingId, new Date().toISOString())
    .run();

  return financingId;
}

// --------------------- Retry logic ---------------------

function isRetryableError(err: unknown): boolean {
  if (err instanceof HubSpotError) {
    if (err.status === 429) return true;
    if (err.status >= 500) return true;
    return false; // 4xx -> likely permanent config/property error
  }
  // Non-HubSpot errors (D1 / runtime) treat as retryable
  return true;
}

function retryDelaySeconds(attempts: number): number {
  // attempts starts at 1, exponential backoff capped to 15 minutes
  const a = Math.max(1, Number(attempts) || 1);
  const delay = Math.min(15 * 60, Math.pow(2, a) * 5);
  return Math.floor(delay);
}

// --------------------- Helpers ---------------------

function corsHeaders(): Record<string, string> {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,x-webhook-token"
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...corsHeaders()
    }
  });
}

function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

function log(env: Env, level: LogLevel, message: string, meta?: unknown): void {
  const current = env.LOG_LEVEL || DEFAULT_LOG_LEVEL;

  const rank: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
  if (rank[level] < rank[current]) return;

  const line = meta ? `${message} ${JSON.stringify(meta)}` : message;

  // eslint-disable-next-line no-console
  if (level === "debug") console.log(line);
  else if (level === "info") console.log(line);
  else if (level === "warn") console.warn(line);
  else console.error(line);
}
