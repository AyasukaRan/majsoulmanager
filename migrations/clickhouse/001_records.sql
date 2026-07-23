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
    INDEX sha256_bloom sha256 TYPE bloom_filter(0.001) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(indexed_at)
PARTITION BY toYYYYMM(received_at)
ORDER BY (toDate(received_at), source, received_at, record_id)
SETTINGS index_granularity = 8192;

