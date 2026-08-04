CREATE DATABASE IF NOT EXISTS mjai;

CREATE TABLE IF NOT EXISTS mjai.records
(
    record_id UUID,
    source LowCardinality(String),
    sha256 FixedString(64),
    received_at DateTime64(3, 'UTC'),
    played_at Nullable(DateTime64(3, 'UTC')),
    players Array(String),
    rule LowCardinality(String),
    event_count UInt16,

    pack_key String,
    pack_offset UInt64,
    compressed_size UInt32,
    raw_size UInt32,
    codec Enum8('zstd' = 1),

    -- The Mahjong Soul protobuf the record was converted from, as a second
    -- zstd frame in the same pack. No `pb_pack_key`: both frames are appended
    -- to one writer inside a single `append_batch`, which never seals between
    -- them, so a record's protobuf is always in the record's own pack.
    --
    -- `pb_size = 0` means no protobuf was stored — every record indexed before
    -- this column existed, and every record that did not come from the live
    -- collector (a historical import is already mjai and never had one).
    pb_offset UInt64 DEFAULT 0,
    pb_compressed_size UInt32 DEFAULT 0,
    pb_size UInt32 DEFAULT 0,

    indexed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX players_bloom players TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX sha256_bloom sha256 TYPE bloom_filter(0.001) GRANULARITY 4,
    -- record_id is last in ORDER BY, so GET /api/v1/records/{id} and its /raw
    -- twin — the primary read path — cannot prune on the sorting key alone.
    INDEX record_id_bloom record_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(indexed_at)
PARTITION BY toYYYYMM(received_at)
ORDER BY (toDate(received_at), source, received_at, record_id)
SETTINGS index_granularity = 8192;

-- The API applies this file at startup, so the CREATE above is a no-op wherever
-- the table already exists; the ALTER is what reaches those installations.
-- Declaring the index is only half of it: ClickHouse writes a skip index into
-- parts created after the ALTER and leaves existing parts scanning. The
-- matching MATERIALIZE INDEX is issued by Catalog::connect rather than living
-- here, because it must run once — on the boot that introduces the index — and
-- not on every restart, and only the application can tell those apart.
ALTER TABLE mjai.records
    ADD INDEX IF NOT EXISTS record_id_bloom record_id TYPE bloom_filter(0.01) GRANULARITY 4;

-- Same reason as the index above: the CREATE cannot reach a table that already
-- exists. Defaulting to zero rather than Nullable keeps the read path a plain
-- comparison and costs nothing — an offset of zero is not a valid location,
-- because every pack begins with its own magic header.
ALTER TABLE mjai.records
    ADD COLUMN IF NOT EXISTS pb_offset UInt64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pb_compressed_size UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pb_size UInt32 DEFAULT 0;

