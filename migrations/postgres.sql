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

-- rskafka has no consumer groups and no offset commit API, so the pack worker
-- stores its own position here. One row per (topic, partition), advanced in a
-- single upsert only after the records up to next_offset have been packed,
-- uploaded and indexed.
CREATE TABLE IF NOT EXISTS kafka_offsets (
    topic text NOT NULL,
    partition_id integer NOT NULL,
    next_offset bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition_id)
);

-- One row per one-off data fix that has already been run to completion. The
-- fixes themselves are applied by the API at startup, the same way this file is,
-- and without a marker each of them would re-scan the whole index on every boot:
-- the metadata rewrite reads the bytes of every record ever collected, which on
-- the historical corpus is hours of object reads to discover there is nothing
-- left to do. A row appears only after the pass finished, so a pass that died
-- partway runs again.
CREATE TABLE IF NOT EXISTS completed_backfills (
    name text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    key_hash bytea NOT NULL UNIQUE,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);

