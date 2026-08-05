CREATE TABLE IF NOT EXISTS ingest_idempotency (
    key_hash bytea PRIMARY KEY,
    record_id uuid NOT NULL,
    content_sha256 bytea NOT NULL,
    state text NOT NULL CHECK (state IN ('accepted', 'indexed', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Whether this claim is a retry guard or the record of a game's identity.
--
-- A caller-supplied key expires with the window in which a collector may retry.
-- A key derived from the record's own game uuid does not: it is the only thing
-- anywhere that says this game has already been stored, because the uuid is not
-- a column of the index, and deleting it is what makes a later re-import store
-- the game a second time under a fresh record id that nothing can collapse.
-- One row per game, forever, is the price of deduplication that does not expire,
-- and it is the cheapest form of it — the 810,004 games collected so far are
-- about 130MB here, against 25GiB of records they protect.
--
-- Non-volatile default, so this is a catalogue update rather than a rewrite of
-- every existing row. Existing claims stay expiring, which is correct for the
-- caller-scoped ones and is repaired for the game-scoped ones by the
-- `game_scoped_claims` backfill.
ALTER TABLE ingest_idempotency
    ADD COLUMN IF NOT EXISTS expires boolean NOT NULL DEFAULT true;

-- Partial, because the prune only ever looks at expiring claims and at the
-- scale this is sized for the permanent ones outnumber them by orders of
-- magnitude: a full index on created_at would have every boot walk all of them
-- to find the handful that are due.
CREATE INDEX IF NOT EXISTS ingest_idempotency_expiring_idx
    ON ingest_idempotency (created_at) WHERE expires;

DROP INDEX IF EXISTS ingest_idempotency_created_at_idx;

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


-- Where the 牌谱屋 sync has got to, one row per mode. The catalogue itself is
-- in ClickHouse; this is only the bookmark, and it belongs here for the same
-- reason `kafka_offsets` does — it is a single small row that has to be updated
-- transactionally after the page it describes has actually landed.
--
-- `next_from` is exclusive of nothing: the page is re-requested from the start
-- time of its own last game, so a game sharing that second with the boundary is
-- read twice rather than skipped. The catalogue collapses the duplicate.
CREATE TABLE IF NOT EXISTS paipuya_cursor (
    mode_id integer PRIMARY KEY,
    next_from timestamptz NOT NULL,
    synced_games bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- How far the re-fetch pool has walked the 牌谱屋 catalogue. Separate from
-- `paipuya_cursor` above, which belongs to the sync: one says how much of the
-- catalogue has been *learned about*, this says how much of it has been *looked
-- for*, and the two move at wildly different speeds — a page of five hundred
-- listings arrives in one request and takes hours to fetch through a pool of
-- rate-limited accounts.
--
-- It is a keyset position in `mjai.paipuya_games`'s own sorting key, both halves
-- of it, because at a couple of games a second a whole page inside one second is
-- ordinary and a cursor of seconds alone would either skip games or re-read the
-- same ones forever. Keyed by walk name so a second walk does not need a
-- migration; the row is deleted when a walk reaches the end of the catalogue.
CREATE TABLE IF NOT EXISTS refetch_cursor (
    walk text PRIMARY KEY,
    started_at timestamptz NOT NULL,
    uuid text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
