CREATE TABLE IF NOT EXISTS affirm_events (
  event_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  event_type TEXT NOT NULL,
  external_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'received',
  hubspot_response_code INTEGER,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_affirm_events_external_id
  ON affirm_events (external_id);

CREATE INDEX IF NOT EXISTS idx_affirm_events_event_type
  ON affirm_events (event_type);

CREATE INDEX IF NOT EXISTS idx_affirm_events_status
  ON affirm_events (status);
