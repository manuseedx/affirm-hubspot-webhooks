export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type WebhookCategory = 'PREQUAL' | 'OPEN' | 'FINANCING';
export type AffirmEventType = 'prequal' | 'open' | 'financing';

export interface NormalizedAffirmEvent {
  eventId: string;
  source: string;
  eventType: AffirmEventType;
  externalId: string;
  dedupeKey: string;
  receivedAt: string;
  payload: Record<string, unknown>;
}

export interface QueueMessageBody {
  eventId: string;
  source: string;
  eventType: AffirmEventType;
  externalId: string;
  dedupeKey: string;
  payload: Record<string, unknown>;
}

export interface Env {
  DB: D1Database;
  EVENT_QUEUE: Queue<QueueMessageBody>;
  SHARED_SECRET: string;
  HUBSPOT_ACCESS_TOKEN?: string;
  HUBSPOT_EVENT_ENDPOINT?: string;
  HUBSPOT_OBJECT_ENDPOINT?: string;
  HS_EVENT_PREQUAL?: string;
  HS_EVENT_OPEN?: string;
  HS_FINANCING_OBJECT_TYPE_ID?: string;
  HS_FINANCING_UNIQUE_PROPERTY?: string;
  DEFAULT_PLATFORM?: string;
  LOG_LEVEL?: LogLevel;
  SOURCE_NAME?: string;
  DRY_RUN_HUBSPOT?: string;
}
