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

