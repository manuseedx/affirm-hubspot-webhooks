type LogLevel = "debug" | "info" | "warn" | "error";

interface Env {
  DB: D1Database;
  AFFIRM_QUEUE: Queue;

  // Cloudflare Secrets (set in Cloudflare dashboard)
  HS_ACCESS_TOKEN: string;
  WEBHOOK_TOKEN: string;

  // Non-secret vars (from wrangler.jsonc)
  HS_FINANCING_OBJECT_TYPE_ID: string;   // e.g. "2-37647069"
  HS_FINANCING_UNIQUE_PROPERTY: string;  // "checkout_id"
  DEFAULT_PLATFORM: string;              // "Affirm"
  LOG_LEVEL: LogLevel;
}

type IncomingAny = Record<string, unknown>;

type NormalizedAffirmPayload = {
  event?: string;
  event_timestamp?: string;
  created?: string;

  email_address?: string;

  checkout_token?: string;

  total?: unknown;
  approved_amount?: unknown;
  decision?: string;
  expiration?: string;

  // keep original keys (normalized) for debugging
  _all?: Record<string, unknown>;
};

type WebhookCategory = "OPEN" | "PREQUAL" | "FINANCING" | "UNKNOWN";

type QueueMessage = {
  received_at: string;
  category: WebhookCategory;
  payload: NormalizedAffirmPayload;
  raw_truncated?: string; // for debugging, capped to keep under Queues message size
};

const HS_BASE = "https://api.hubapi.com";
const MAX_QUEUE_MESSAGE_BYTES = 110 * 1024; // Queues message size limit is 128KB :contentReference[oaicite:15]{index=15}

let schemaReady: Promise<void> | null = null;

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,x-webhook-token",
  };
}

function jsonResponse(body: unknown, init: ResponseInit = {}) {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  Object.entries(corsHeaders()).forEach(([k, v]) => headers.set(k, v));
  return new Response(JSON.stringify(body, null, 2), { ...init, headers });
}

function textResponse(text: string, init: ResponseInit = {}) {
  const headers = new Headers(init.headers);
  headers.set("content-type", "text/plain; charset=utf-8");
  Object.entries(corsHeaders()).forEach(([k, v]) => headers.set(k, v));
  return new Response(text, { ...init, headers });
}

function safeEqual(a: string, b: string) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function normalizeKey(k: string) {
  return k.trim().toLowerCase().replace(/[\s-]+/g, "_");
}

function normalizeTopLevelKeys(obj: IncomingAny): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[normalizeKey(k)] = v;
  return out;
}

function pickString(v: unknown): string | undefined {
  if (v === null || v === undefined) return undefined;
  const s = String(v).trim();
  return s.length ? s : undefined;
}

function centsToDollarsString(v: unknown): string | undefined {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  if (!Number.isFinite(n)) return undefined;
  const dollars = n / 100;
  // keep it simple: trim trailing .00
  const fixed = dollars.toFixed(2);
  return fixed.replace(/\.00$/, "").replace(/(\.\d)0$/, "$1");
}

function normalizeTimestamp(ts?: string): string | undefined {
  if (!ts) return undefined;

  // If it already has timezone (Z or +/-), keep it; else assume UTC and add Z
  let out = ts.trim();
  const hasTZ = /z$|[+-]\d{2}:\d{2}$/i.test(out);

  // If fractional seconds has > 3 digits, truncate to ms
  out = out.replace(
    /(\.\d{3})\d+(?=(z$|[+-]\d{2}:\d{2}$|$))/i,
    "$1"
  );

  if (!hasTZ) out += "Z";
  return out;
}

async function parseBody(request: Request): Promise<IncomingAny> {
  const ct = request.headers.get("content-type") || "";

  if (ct.includes("application/json")) {
    const parsed = (await request.json()) as unknown;
    if (parsed && typeof parsed === "object") return parsed as IncomingAny;
    return { value: parsed };
  }

  if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
    const form = await request.formData();
    const out: Record<string, unknown> = {};
    for (const [k, v] of form.entries()) {
      const key = k;
      const val = typeof v === "string" ? v : (v as File).name;

      // handle repeated keys -> array
      if (key in out) {
        const existing = out[key];
        out[key] = Array.isArray(existing) ? [...existing, val] : [existing, val];
      } else {
        out[key] = val;
      }
    }
    return out;
  }

  // fallback: try text -> json
  const text = await request.text();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object") return parsed as IncomingAny;
    return { value: parsed };
  } catch {
    return { raw: text };
  }
}

function categorize(p: NormalizedAffirmPayload): WebhookCategory {
  const ev = (p.event || "").toLowerCase();
  const hasCheckout = !!p.checkout_token;

  if (!hasCheckout && ev === "prequal_decision") return "PREQUAL";
  if (hasCheckout && ev === "opened") return "OPEN";
  if (hasCheckout) return "FINANCING";
  return "UNKNOWN";
}

function log(env: Env, level: LogLevel, message: string, meta?: unknown) {
  const order: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
  const current = order[env.LOG_LEVEL || "info"] ?? 20;
  if (order[level] < current) return;

  const line = meta ? `${message} ${JSON.stringify(meta)}` : message;
  // eslint-disable-next-line no-console
  console[level === "debug" ? "log" : level](line);
}

async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function ensureSchema(env: Env) {
  if (!schemaReady) {
    schemaReady = (async () => {
      // Idempotency table
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS webhook_events (
          id TEXT PRIMARY KEY,
          payload_hash TEXT NOT NULL,
          status TEXT NOT NULL,
          category TEXT,
          event TEXT,
          checkout_token TEXT,
          email TEXT,
          event_timestamp TEXT,
          first_seen_at TEXT NOT NULL,
          processed_at TEXT,
          error TEXT
        );
      `).run();

      await env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_webhook_events_checkout_token
        ON webhook_events(checkout_token);
      `).run();

      await env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_webhook_events_email
        ON webhook_events(email);
      `).run();

      // Map checkout_token -> HubSpot financing object ID
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS financing_index (
          checkout_token TEXT PRIMARY KEY,
          hs_financing_id TEXT NOT NULL,
          hs_contact_id TEXT,
          last_event TEXT,
          last_event_timestamp TEXT,
          updated_at TEXT NOT NULL
        );
      `).run();
    })();
  }
  await schemaReady;
}

async function hsFetch(env: Env, path: string, init: RequestInit): Promise<Response> {
  const url = `${HS_BASE}${path}`;
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${env.HS_ACCESS_TOKEN}`);
  headers.set("accept", "application/json");

  return fetch(url, { ...init, headers });
}

async function hsJson<T>(env: Env, method: string, path: string, body?: unknown): Promise<T | null> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }

  const res = await hsFetch(env, path, init);

  if (res.status === 204) return null;

  if (!res.ok) {
    const text = await res.text();
    throw new HubSpotError(res.status, text);
  }

  return (await res.json()) as T;
}

class HubSpotError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function hsFindOrCreateContactId(env: Env, email: string, maybeFirstName?: string, maybeLastName?: string): Promise<string> {
  // Search by email
  const searchBody = {
    filterGroups: [
      { filters: [{ propertyName: "email", operator: "EQ", value: email }] }
    ],
    properties: ["email"],
    limit: 1
  };

  const found = await hsJson<{ results: Array<{ id: string }> }>(env, "POST", "/crm/v3/objects/contacts/search", searchBody);
  const id = found?.results?.[0]?.id;
  if (id) return id;

  // Create if not found
  const props: Record<string, string> = { email };
  if (maybeFirstName) props.firstname = maybeFirstName;
  if (maybeLastName) props.lastname = maybeLastName;

  const created = await hsJson<{ id: string }>(env, "POST", "/crm/v3/objects/contacts", { properties: props });
  if (!created?.id) throw new Error("HubSpot contact create returned no id");
  return created.id;
}

async function hsSendCustomEvent(env: Env, eventName: string, objectId: string, email: string | undefined, occurredAt: string | undefined, properties: Record<string, string>) {
  // HubSpot custom event occurrences endpoint :contentReference[oaicite:16]{index=16}
  const body: Record<string, unknown> = {
    eventName,
    objectId,
    properties
  };

  // occurredAt is optional; if not provided HubSpot uses send time :contentReference[oaicite:17]{index=17}
  if (occurredAt) body.occurredAt = occurredAt;

  // email is a supported identifier for contacts; objectId is recommended :contentReference[oaicite:18]{index=18}
  if (email) body.email = email;

  await hsJson(env, "POST", "/events/v3/send", body);
}

async function hsFindFinancingIdByCheckout(env: Env, checkoutToken: string): Promise<string | null> {
  const objectType = env.HS_FINANCING_OBJECT_TYPE_ID;
  const uniqueProp = env.HS_FINANCING_UNIQUE_PROPERTY;

  const searchBody = {
    filterGroups: [
      { filters: [{ propertyName: uniqueProp, operator: "EQ", value: checkoutToken }] }
    ],
    properties: [uniqueProp],
    limit: 1
  };

  const found = await hsJson<{ results: Array<{ id: string }> }>(
    env,
    "POST",
    `/crm/v3/objects/${objectType}/search`,
    searchBody
  );

  return found?.results?.[0]?.id ?? null;
}

async function hsCreateFinancing(env: Env, properties: Record<string, string>): Promise<string> {
  const objectType = env.HS_FINANCING_OBJECT_TYPE_ID;
  const created = await hsJson<{ id: string }>(
    env,
    "POST",
    `/crm/v3/objects/${objectType}`,
    { properties }
  );
  if (!created?.id) throw new Error("HubSpot financing create returned no id");
  return created.id;
}

async function hsUpdateFinancing(env: Env, financingId: string, properties: Record<string, string>): Promise<void> {
  const objectType = env.HS_FINANCING_OBJECT_TYPE_ID;
  await hsJson(env, "PATCH", `/crm/v3/objects/${objectType}/${financingId}`, { properties });
}

async function hsAssociateContactToFinancing(env: Env, contactId: string, financingId: string): Promise<void> {
  // Default association endpoint for v4 associations :contentReference[oaicite:19]{index=19}
  const toObjectType = env.HS_FINANCING_OBJECT_TYPE_ID;
  await hsJson(env, "PUT", `/crm/v4/objects/contact/${contactId}/associations/default/${toObjectType}/${financingId}`);
}

function buildFinancingProperties(env: Env, p: NormalizedAffirmPayload): Record<string, string> {
  // Mirrors the fields used in your Zap’s financing object create step 
  const props: Record<string, string> = {};

  const checkout = pickString(p.checkout_token);
  if (checkout) props["checkout_id"] = checkout;

  const email = pickString(p.email_address);
  if (email) props["email_address"] = email;

  const ev = pickString(p.event);
  if (ev) props["event"] = ev;

  // Zap divided total by 100 before storing in HS 
  const totalDollars = centsToDollarsString(p.total);
  if (totalDollars) props["total"] = totalDollars;

  // Optional fields if they come in later
  const decision = pickString(p.decision);
  if (decision) props["decision"] = decision;

  // constant fields
  props["platform"] = env.DEFAULT_PLATFORM || "Affirm";
  props["financing_type"] = "Applications";

  return props;
}

async function processQueueMessage(env: Env, msg: QueueMessage) {
  await ensureSchema(env);

  const now = new Date().toISOString();
  const payloadHash = await sha256Hex(JSON.stringify(msg));

  // Idempotency: skip if already processed
  const existing = await env.DB.prepare(
    "SELECT status FROM webhook_events WHERE id = ?"
  ).bind(payloadHash).first<{ status: string }>();

  if (existing?.status === "processed") {
    return;
  }

  // Mark attempt
  await env.DB.prepare(`
    INSERT INTO webhook_events (id, payload_hash, status, category, event, checkout_token, email, event_timestamp, first_seen_at)
    VALUES (?, ?, 'processin:contentReference[oaicite:22]{index=22}
    ON CONFLICT(id) DO UPDATE SET
      status='processing',
      processed_at=NULL,
      error=NULL
  `).bind(
    payloadHash,
    payloadHash,
    msg.category,
    msg.payload.event ?? null,
    msg.payload.checkout_token ?? null,
    msg.payload.email_address ?? null,
    msg.payload.event_timestamp ?? null,
    now
  ).run();

  try {
    con:contentReference[oaicite:23]{index=23}   const email = pickString(p.email_address);
    if (!email) throw new Error("Missing email_address");

    const contactId = await hsFindOrCreateContactId(env, email);

    const occurredAt = normalizeTimestamp(p.event_timestamp || p.created) || undefined;

    if (msg.category === "PREQUAL") {
      // Your custom event internal name
      const eventName = "pe39758416_prequalified_affirm_application";

      const amount = centsToDollarsString(p.approved_amount);
      const decision = pickString(p.decision) || "";
      const ev = pickString(p.event) || "";
      const platform = env.DEFAULT_PLATFORM || "Affirm";

      // Send only properties you used in Zap code 
      await hsSendCustomEvent(env, eventName, contactId, email, occurredAt, {
        decision,
        event: ev,
        amount: amount ?? "",
        platform
      });
    }

    if (msg.category === "OPEN") {
      const eventName = "pe39758416_open_financing_v2";

      const checkoutToken = pickString(p.checkout_token) || "";
      const amount = centsToDollarsString(p.total);
      const platform = env.DEFAULT_PLATFORM || "Affirm";

      await hsSendCustomEvent(env, eventName, contactId, email, occurredAt, {
        checkout_token: checkoutToken,
        amount: amount ?? "",
        platform
      });
    }

    if (msg.category === "FINANCING") {
      const checkoutToken = pickString(p.checkout_token);
      if (!checkoutToken) throw new Error("FINANCING message missing checkout_token");

      // Try D1 cache
      const cached = await env.DB.prepare(
        "SELECT hs_financing_id FROM financing_index WHERE checkout_token = ?"
      ).bind(checkoutToken).first<{ hs_financing_id: string }>();

      let financingId = cached?.hs_financing_id ?? null;

      if (!financingId) {
        financingId = await hsFindFinancingIdByCheckout(env, checkoutToken);
      }

      const props = buildFinancingProperties(env, p);

      if (!financingId) {
        financingId = await hsCreateFinancing(env, props);
      } else {
        await hsUpdateFinancing(env, financingId, p:contentReference[oaicite:25]{index=25} // Associate to contact
      await hsAssociateContactToFinancing(env, contactId, financingId);

      // Update cache
      await env.DB.prepare(`
        INSERT INTO financing_index (checkout_token, hs_financing_id, hs_contact_id, last_event, last_event_timestamp, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(checkout_token) DO UPDATE SET
          hs_financing_id=excluded.hs_financing_id,
          hs_contact_id=excluded.hs_contact_id,
          last_event=excluded.last_event,
          last_event_timestamp=excluded.last_event_timestamp,
          updated_at=excluded.updated_at
      `).bind(
        checkoutToken,
        financingId,
        contactId,
        p.event ?? null,
        p.event_timestamp ?? null,
        now
      ).run();
    }

    await env.DB.prepare(
      "UPDATE webhook_events SET status='processed', processed_at=? WHERE id=?"
    ).bind(now, payloadHash).run();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await env.DB.prepare(
      "UPDATE webhook_events SET status='error', processed_at=?, error=? WHERE id=?"
    ).bind(now, now, message, payloadHash).run();
    throw err;
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // CORS preflight for browser testing
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders() });

    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return jsonResponse({ ok: true });
    }

    if (request.method !== "POST") {
      return textResponse("Use POST for webhooks (or GET /health).", { status: 405 });
    }

    // Auth: allow either header or ?token= query param
    const provided = request.headers.get("x-webhook-token") || url.searchParams.get("token") || "";
    if (!env.WEBHOOK_TOKEN || !safeEqual(provided, env.WEBHOOK_TOKEN)) {
      return jsonResponse({ ok: false, error: "Unauthorized" }, { status: 401 });
    }

    const raw = await parseBody(request);
    const normalizedTop = normalizeTopLevelKeys(raw);

    const payload: NormalizedAffirmPayload = {
      event: pickString(normalizedTop["event"]),
      event_timestamp: pickString(normalizedTop["event_timestamp"]),
      created: pickString(normalizedTop["created"]),

      email_address: pickString(normalizedTop["email_address"]),

      checkout_token: pickString(normalizedTop["checkout_token"]),

      total: normalizedTop["total"],
      approved_amount: normalizedTop["approved_amount"],
      decision: pickString(normalizedTop["decision"]),
      expiration: pickString(normalizedTop["expiration"]),

      _all: normalizedTop
    };

    const category = categorize(payload);

    // Truncate raw json for queue message safety (Queues message size 128KB) :contentReference[oaicite:26]{index=26}
    const rawJson = JSON.stringify(normalizedTop);
    let raw_truncated: string | undefined = undefined;
    const rawBytes = new TextEncoder().encode(rawJson).byteLength;
    if (rawBytes <= MAX_QUEUE_MESSAGE_BYTES) raw_truncated = rawJson;

    const message: QueueMessage = {
      received_at: new Date().toISOString(),
      category,
      payload,
      raw_truncated
    };

    log(env, "info", "Webhook received", { category, email: payload.email_address, event: payload.event });

    await env.AFFIRM_QUEUE.send(message);

    return jsonResponse({ ok: true, queued: true, category });
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env, ctx: ExecutionContext): Promise<void> {
    for (const msg of batch.messages) {
      try {
        await processQueueMessage(env, msg.body);
        msg.ack();
      } catch (err) {
        // Retry transient HubSpot errors with backoff
        const attempts = msg.attempts ?? 1;
        const delay = Math.min(60 * 60, Math.pow(2, attempts) * 5); // up to 1 hour
        log(env, "error", "Queue message failed; retrying", { attempts, delay, error: String(err) });
        msg.retry({ delaySeconds: delay });
      }
    }
  }
};
