CREATE TABLE IF NOT EXISTS ingest_idempotency (
    key_hash bytea PRIMARY KEY,
    record_id uuid NOT NULL,
    content_sha256 bytea NOT NULL,
    state text NOT NULL CHECK (state IN ('accepted', 'indexed', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ingest_idempotency_created_at_idx
    ON ingest_idempotency (created_at);

CREATE TABLE IF NOT EXISTS download_jobs (
    id uuid PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'failed')),
    filter jsonb NOT NULL,
    format text NOT NULL,
    record_count bigint NOT NULL DEFAULT 0,
    result_object_key text,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS download_jobs_state_created_idx
    ON download_jobs (state, created_at)
    WHERE state IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS download_jobs_expires_at_idx
    ON download_jobs (expires_at);

CREATE TABLE IF NOT EXISTS api_keys (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    key_hash bytea NOT NULL UNIQUE,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);

