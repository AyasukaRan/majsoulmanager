-- Full Mahjong Soul game uuids that are known to exist: what `majsoul2mjai`
-- enumerated from 牌谱屋 and then resolved. The re-fetch pool's work list.
--
-- Separate from `mjai.paipuya_games` because the two answer different
-- questions, and the same column cannot hold both answers:
--
--   paipuya_games  what 牌谱屋 lists — matched against `mjai.records` on
--                  players and start time to say how much live collection
--                  missed. Its uuids are 11-character short ids: the site's
--                  API returns `"_masked": true` for every game this
--                  deployment's key can see, and a short id is not something
--                  Mahjong Soul will accept.
--   game_uuids     uuids that can be handed to `fetchGameRecord`. No players,
--                  no comparison — a work list, and nothing else.
--
-- Both were in `paipuya_games` for a while, and neither failure was loud: the
-- walk was refused on every short id it fed to Mahjong Soul, which reads as
-- "the game is no longer served", and the comparison card counted 191 million
-- rows with empty `players` as missing, which reads as a 100% gap.
--
-- No per-row state, for the reason `003` gives: whether a game has been fetched
-- is the presence of a row in `mjai.records`, and a second copy of that fact
-- would disagree with it the first time an import ran.
CREATE TABLE IF NOT EXISTS mjai.game_uuids
(
    uuid String,
    mode_id Int32,
    -- Mahjong Soul's `start_time`, the same clock `mjai.records.played_at` and
    -- `mjai.paipuya_games.started_at` carry. Leads the sorting key because the
    -- walk pages through it by keyset and resumes from a position in it.
    started_at DateTime64(3, 'UTC'),
    imported_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- Re-importing an overlapping range is expected — the enumerator is resumed by
-- date range — so a repeat of the same game collapses instead of being walked
-- twice.
ENGINE = ReplacingMergeTree(imported_at)
-- By year, like `paipuya_games`: a bulk load spanning eighty months would
-- otherwise be close to ClickHouse's hundred-partitions-per-block default.
PARTITION BY toYear(started_at)
ORDER BY (started_at, uuid)
SETTINGS index_granularity = 8192;
