-- What 牌谱屋 (amae-koromo) says exists. Not records — a catalogue of games,
-- most of which this deployment does not have and some of which it does.
--
-- It is here rather than in PostgreSQL because it is append-only and large: the
-- site lists on the order of half a billion games, against 1.6 million records
-- in `mjai.records`. Nothing ever updates a row; the only questions asked of it
-- are "how many are there in this window" and "which of them is this corpus
-- missing", and both are scans of a time range.
--
-- No per-row state, deliberately. Whether a game has been fetched is not stored
-- here — it is the presence of a matching row in `mjai.records`, which is the
-- one answer that cannot go stale. A `state` column would be a second copy of
-- that fact, and the two would disagree the first time an import ran.
CREATE TABLE IF NOT EXISTS mjai.paipuya_games
(
    uuid String,
    mode_id Int32,
    -- The game's own start, which is the same clock `mjai.records.played_at`
    -- carries: both come from Mahjong Soul's `start_time`, a unix second.
    started_at DateTime64(3, 'UTC'),
    ended_at DateTime64(3, 'UTC'),
    -- Seat-ordered, like `mjai.records.players`. Compared sorted anyway, so
    -- that a difference in seat convention cannot make every game look missing.
    players Array(String),
    account_ids Array(UInt64),
    scores Array(Int32),
    synced_at DateTime64(3, 'UTC') DEFAULT now64(3),

    INDEX uuid_bloom uuid TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(synced_at)
-- By year, not by month. A sync pulls one time window at a time and would be
-- fine either way, but a bulk load of the whole catalogue spans about eighty
-- months against ClickHouse's default limit of a hundred partitions per insert
-- block — close enough to the ceiling that it would start failing partway
-- through some future year. Eight partitions cost nothing here: every read is a
-- range on `started_at`, which leads the sorting key.
PARTITION BY toYear(started_at)
ORDER BY (started_at, uuid)
SETTINGS index_granularity = 8192;
