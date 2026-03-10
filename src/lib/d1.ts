import { AffirmEventType, NormalizedAffirmEvent } from './types';

export type EventStatus = 'received' | 'enqueueing' | 'enqueued' | 'delivered' | 'failed';

export interface ExistingEventState {
  eventId: string;
  status: EventStatus;
  source: string;
  eventType: AffirmEventType;
  externalId: string;
  dedupeKey: string;
  payload: Record<string, unknown>;
}

interface ExistingEventRow {
  eventId: string;
  status: EventStatus;
  source: string;
  eventType: AffirmEventType;
  externalId: string;
  dedupeKey: string;
  payloadJson: string;
}

export async function insertInboundEvent(
  db: D1Database,
  event: NormalizedAffirmEvent
): Promise<{ inserted: boolean }> {
  const statement = db
    .prepare(
      `INSERT OR IGNORE INTO affirm_events
        (event_id, source, event_type, external_id, dedupe_key, payload_json, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, 'received', ?, ?)`
    )
    .bind(
      event.eventId,
      event.source,
      event.eventType,
      event.externalId,
      event.dedupeKey,
      JSON.stringify(event.payload),
      event.receivedAt,
      event.receivedAt
    );

  const result = await statement.run();
  const inserted = (result.meta.changes ?? 0) > 0;
  return { inserted };
}

export async function findEventByDedupeKey(
  db: D1Database,
  dedupeKey: string
): Promise<ExistingEventState | null> {
  const result = await db
    .prepare(
      `SELECT event_id AS eventId,
              status,
              source,
              event_type AS eventType,
              external_id AS externalId,
              dedupe_key AS dedupeKey,
              payload_json AS payloadJson
       FROM affirm_events
       WHERE dedupe_key = ?
       LIMIT 1`
    )
    .bind(dedupeKey)
    .first<ExistingEventRow>();

  if (!result) {
    return null;
  }

  let payload: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(result.payloadJson);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      payload = parsed as Record<string, unknown>;
    }
  } catch {
  }

  return {
    eventId: result.eventId,
    status: result.status,
    source: result.source,
    eventType: result.eventType,
    externalId: result.externalId,
    dedupeKey: result.dedupeKey,
    payload
  };
}

export async function markEnqueued(db: D1Database, eventId: string): Promise<void> {
  await db
    .prepare(
      `UPDATE affirm_events
         SET status = 'enqueued',
             updated_at = ?
       WHERE event_id = ?`
    )
    .bind(new Date().toISOString(), eventId)
    .run();
}

export async function markEnqueueing(db: D1Database, eventId: string): Promise<void> {
  await db
    .prepare(
      `UPDATE affirm_events
         SET status = 'enqueueing',
             updated_at = ?
       WHERE event_id = ?`
    )
    .bind(new Date().toISOString(), eventId)
    .run();
}

export async function recordEnqueueStatusError(
  db: D1Database,
  eventId: string,
  lastError: string
): Promise<void> {
  await db
    .prepare(
      `UPDATE affirm_events
         SET last_error = ?,
             updated_at = ?
       WHERE event_id = ?`
    )
    .bind(lastError, new Date().toISOString(), eventId)
    .run();
}

export async function updateDeliveryStatus(
  db: D1Database,
  eventId: string,
  status: 'delivered' | 'failed',
  options?: { responseCode?: number; lastError?: string }
): Promise<void> {
  await db
    .prepare(
      `UPDATE affirm_events
         SET status = ?,
             hubspot_response_code = ?,
             last_error = ?,
             updated_at = ?
       WHERE event_id = ?`
    )
    .bind(
      status,
      options?.responseCode ?? null,
      options?.lastError ?? null,
      new Date().toISOString(),
      eventId
    )
    .run();
}
