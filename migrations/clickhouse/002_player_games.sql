-- One row per seat per game: the counters `src/replay.rs` reads off an mjai
-- event stream, stored unaggregated.
--
-- Counters and not ratios, because a ratio cannot be summed. Every question the
-- console asks — this player's win rate in jade south over the last 30 days —
-- is a sum over a filtered set of these rows with the division done last, and a
-- stored ratio would answer none of them.
--
-- `player` leads the sorting key because every read filters by it first. The
-- bloom filter is for the search box, which does the opposite: it looks for a
-- substring across every player and cannot prune on the key at all.
CREATE TABLE IF NOT EXISTS mjai.player_games
(
    record_id UUID,
    seat UInt8,
    player String,
    -- When the game was played. Falls back to the ingest time for a record
    -- carrying no `majsoul.start_time`, because this is a partition key and a
    -- Nullable one would put every such row in the same partition forever.
    played_at DateTime64(3, 'UTC'),
    rule LowCardinality(String),
    seats UInt8,
    -- Whether the record carried the scoring detail the last four counters
    -- need. Records converted before the converter stopped discarding it have
    -- zeroes there that mean "not known", not "did not happen", so those four
    -- have to be divided by the sum of this column rather than by `count()`.
    detailed UInt8,

    placement UInt8,
    final_score Int32,
    settled_point Int32,
    grading_score Int32,
    level_id UInt32,
    busted UInt8,

    hands UInt16,
    hands_as_dealer UInt16,
    max_dealer_streak UInt16,
    net_points Int32,

    wins UInt16,
    wins_tsumo UInt16,
    win_points Int32,
    win_turns UInt32,
    deal_ins UInt16,
    deal_in_points Int32,

    riichi UInt16,
    riichi_wins UInt16,
    riichi_deal_ins UInt16,
    riichi_turns UInt32,
    riichi_first UInt16,
    riichi_chasing UInt16,
    riichi_chased UInt16,
    riichi_net Int32,

    called UInt16,
    called_wins UInt16,
    draws UInt16,

    draws_tenpai UInt16,
    riichi_ippatsu UInt16,
    riichi_ura_hits UInt16,
    yakuman UInt16,
    max_han UInt16,

    indexed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX player_bloom player TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(indexed_at)
PARTITION BY toYYYYMM(played_at)
ORDER BY (player, toDate(played_at), record_id, seat)
SETTINGS index_granularity = 8192;
