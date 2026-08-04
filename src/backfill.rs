use crate::{
    AppState,
    catalog::{PlayerGame, Record, RecordFilter},
    mjai::{self, Metadata},
    replay,
    watch_log::WatchLogLevel,
};

/// Names this fix in `completed_backfills`. Fixing the parser only reaches
/// records ingested after it, so every row indexed before it still carries the
/// empty rule the converter's records always produced, and every row the live
/// collector wrote still carries a `played_at` rounded down to the midnight of
/// the game's uuid. Those rows have to be rewritten once, and it is worth doing
/// before the historical import rather than after: the pass reads the bytes of
/// every record in the index, so its cost scales with the corpus.
pub const NAME: &str = "records_rule_played_at";

/// What the console's log panel attributes these lines to.
const LOG_SOURCE: &str = "backfill";

/// One page of the keyset walk, the size the export path already walks with. It
/// bounds what is resident, not what the pass costs: every row on a page is a
/// read of that record's bytes, and for a pack that is no longer on local disk
/// that is one Range GET, taken one at a time. Reading a page's rows
/// concurrently is the upgrade path if the wall time on the historical corpus
/// ever matters more than staying out of the live ingest path's way.
const PAGE_SIZE: usize = 1_000;

/// How many rows pass between progress lines. The console's log buffer holds 500
/// entries, so a line per page would push every other line out of it long before
/// an operator looked at it.
const PROGRESS_EVERY: usize = 20_000;

#[derive(Default)]
struct Progress {
    scanned: usize,
    rewritten: usize,
    /// Rows whose bytes could not be read. Counted apart from `unparsable`
    /// because only this one is usually about the object store rather than
    /// about the record.
    unreadable: usize,
    unparsable: usize,
}

impl Progress {
    /// Whether this pass earned the right to write the one-shot marker.
    ///
    /// A row whose bytes could not be read was never examined, and the reason is
    /// far more often an object store that is unreachable or misconfigured than
    /// a record that is gone — a state that ends. Marking the pass done from
    /// inside one of those boots would spend the marker on a pass that rewrote
    /// nothing, leave the corpus stale permanently, and leave one info line as
    /// the only trace. A row that could not be *parsed* is the opposite: those
    /// bytes will not parse on the next boot either, so waiting for them would
    /// mean never finishing.
    ///
    /// Being strict costs a full re-scan on every boot for as long as a pack is
    /// genuinely unreadable, and that is the intended pressure: an indexed row
    /// pointing at bytes nobody can fetch is an incident, not a rounding error.
    /// The message that reports this names the one statement that accepts it.
    fn is_complete(&self) -> bool {
        self.unreadable == 0
    }
}

/// The row to write back for one already-indexed record, or `None` when the
/// parser now derives exactly what the row already holds.
///
/// The rewritten row is built from the old one with struct update syntax rather
/// than field by field, and that is what carries every column of the sorting key
/// — `toDate(received_at)`, `source`, `received_at`, `record_id`, the first of
/// which is a function of the third — back unchanged by construction. A row
/// re-inserted under a key that differs in any one of them is not collapsed onto
/// the old row by ReplacingMergeTree; it sits beside it, and the index doubles
/// without anything failing or anyone noticing.
///
/// Only `rule` and `played_at` are looked at. They are the two fields the ingest
/// path lost, and re-deriving `players` and `event_count` as well would buy
/// nothing while adding a way for a row to differ from its own bytes forever,
/// since `event_count` is clamped to `u16::MAX` on the way into the index.
///
/// Neither field is ever cleared, which is the guard that keeps a rerun from
/// destroying something: a record whose bytes carry no `majsoul.start_time` —
/// any mjai log that did not come from the converter — leaves the stored
/// `played_at` as the only one anybody has. What this cannot do is tell such a
/// value apart from one an `X-Mjai-Played-At` header set, because the override
/// is not stored anywhere: if a record's own header says one thing and a
/// collector once claimed another, the record wins here. Nothing sets that
/// header now that the managed collector has stopped passing it, so the only way
/// this loses anything is if a future caller starts using it again on records
/// that also carry a `start_time`.
fn rewritten(row: &Record, metadata: &Metadata) -> Option<Record> {
    let rule = metadata.rule.clone().or_else(|| row.rule.clone());
    let played_at = metadata.played_at.or(row.played_at);
    if rule == row.rule && played_at == row.played_at {
        return None;
    }
    Some(Record {
        rule,
        played_at,
        ..row.clone()
    })
}

/// Rewrites the metadata of every row already in the index, once per
/// deployment. Spawned behind the listener like the legacy pack upload: it reads
/// the bytes of every record ever collected, and an API that would not answer
/// until that finished looks like an outage. It therefore cannot report by
/// failing to start either, so every outcome is logged and a failure is logged
/// loudly.
pub async fn rewrite_record_metadata(state: AppState) {
    match already_done(&state, NAME).await {
        Ok(true) => return,
        Ok(false) => {}
        // Running the pass a second time is harmless — it would find nothing to
        // rewrite — but running it because PostgreSQL was unreachable would hide
        // that from an operator, so an unreadable marker stops the pass and says
        // so rather than being read as "not done yet".
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("读不到索引元数据改写的完成标记，本次启动不改写：{error}"),
            );
            return;
        }
    }

    report(
        &state,
        WatchLogLevel::Info,
        "开始改写索引中的对局元数据（规则与开局时间）".to_owned(),
    );
    let progress = match scan(&state).await {
        Ok(progress) => progress,
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("索引元数据改写失败，已改写的部分仍然有效，下次启动会重跑：{error}"),
            );
            return;
        }
    };
    // A pass that read nothing is not a pass that finished. `scan` deliberately
    // never fails on a row it could not read, so a boot with the object store
    // unreachable walks the whole index, rewrites nothing, and returns `Ok` —
    // and writing the marker there would end the rewrite for good on the one
    // boot where it did no work.
    if !progress.is_complete() {
        report(
            &state,
            WatchLogLevel::Error,
            format!(
                "索引元数据改写未完成：{} 条记录读不到字节（共扫描 {} 条，改写 {} 条，解析失败 {} 条）。\
                 下次启动会重跑；若确认这些 pack 已永久丢失，执行 \
                 INSERT INTO completed_backfills (name) VALUES ('{NAME}') 可以结束重跑",
                progress.unreadable, progress.scanned, progress.rewritten, progress.unparsable
            ),
        );
        return;
    }
    // The marker is written only here, so a pass that died partway runs again
    // from the beginning on the next boot. That is why the pass has to be
    // idempotent rather than resumable: a row it already rewrote derives the
    // same metadata a second time and is skipped, so a rerun costs the reads and
    // writes nothing.
    if let Err(error) = mark_done(&state, NAME).await {
        report(
            &state,
            WatchLogLevel::Error,
            format!("索引元数据已改写完成，但完成标记没有写入，下次启动会重跑：{error}"),
        );
        return;
    }
    report(
        &state,
        WatchLogLevel::Info,
        format!(
            "索引元数据改写完成：共扫描 {} 条，改写 {} 条，解析失败跳过 {} 条",
            progress.scanned, progress.rewritten, progress.unparsable
        ),
    );
}

async fn scan(state: &AppState) -> anyhow::Result<Progress> {
    let mut progress = Progress::default();
    let mut cursor = None;
    let mut reported = 0usize;
    loop {
        // The catalogue's export walk, which carries no time window — the whole
        // corpus is as much the point here as it is there. It pages
        // `(received_at DESC, record_id DESC)` from the newest row backwards,
        // and that is also what makes running this beside live ingest safe. A
        // row the pack/index worker writes while the scan is in flight either
        // sorts above the cursor, in which case the walk has already passed it
        // and never reads it, or sorts below it — a message that waited in the
        // topic keeps the produce timestamp it was given — in which case it is
        // read like any other row. Both are correct, because a row written now
        // came from the fixed parser and derives the metadata it already holds,
        // so it is skipped either way. Nothing here rewrites a live row from
        // stale bytes, because the bytes a row points at never change.
        let (page, next) = state
            .catalog
            .scan(&RecordFilter::default(), cursor, PAGE_SIZE)
            .await?;
        let mut batch = Vec::new();
        for row in &page {
            let raw = match state.packs.read(&row.storage).await {
                Ok(raw) => raw,
                Err(error) => {
                    // Logged and skipped, never removed from the index: the row
                    // that is there is still a true pointer at bytes that exist,
                    // and a pack the object store would not serve this minute is
                    // no reason to forget a record. These stay out of the
                    // console's buffer and only reach the container log — one
                    // unreachable pack is thousands of lines, and the buffer
                    // holds 500 of them.
                    tracing::warn!(record = %row.id, %error, "跳过一条读不到字节的记录");
                    progress.unreadable += 1;
                    continue;
                }
            };
            let metadata = match mjai::parse_metadata(&raw) {
                Ok(metadata) => metadata,
                Err(error) => {
                    tracing::warn!(record = %row.id, %error, "跳过一条解析不了的记录");
                    progress.unparsable += 1;
                    continue;
                }
            };
            if let Some(rewritten) = rewritten(row, &metadata) {
                batch.push(rewritten);
            }
        }
        progress.scanned += page.len();
        progress.rewritten += batch.len();
        // One insert per page rather than one per row, the same shape every
        // other writer of this table uses, so a rewritten page is one MergeTree
        // part instead of a thousand.
        state.catalog.insert_batch(&batch).await?;
        if progress.scanned - reported >= PROGRESS_EVERY {
            reported = progress.scanned;
            report(
                state,
                WatchLogLevel::Info,
                format!(
                    "索引元数据改写中：已扫描 {} 条，改写 {} 条，读不到 {} 条，解析失败 {} 条",
                    progress.scanned, progress.rewritten, progress.unreadable, progress.unparsable
                ),
            );
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(progress)
}

/// Names the pass that gives every record already in the index a claim under the
/// scope ingest computes today.
///
/// Deduplication is keyed on the game's own uuid, and the claim carrying that
/// key is the only place the uuid appears at all — the index has no column for
/// it. So a record indexed before that scoping existed has no such claim, and
/// nothing would recognise it if the same game arrived again. Worse, the claims
/// the live collector did leave under this key were written as expiring ones,
/// so without this pass the entire collected corpus loses its identity thirty
/// days after it was gathered and quietly starts accepting itself as new.
pub const GAME_CLAIMS_NAME: &str = "game_scoped_claims";

/// How many record reads are in flight at once.
///
/// The metadata pass reads a page's rows one at a time, which was fine for the
/// 61,934 rows it met. This one meets every row in the corpus, and once a pack
/// has been uploaded its local copy is gone, so a row is a Range GET: 810,004 of
/// them taken one at a time is over an hour of boot-time reads competing with
/// live ingest. Bounded rather than unbounded because the point is to finish,
/// not to saturate the object store the pack worker is also writing to.
const READ_CONCURRENCY: usize = 16;

/// Gives every already-indexed record a permanent claim under its game uuid.
///
/// Spawned behind the listener like the metadata rewrite, and for the same
/// reason: it reads the bytes of every record ever collected. It writes nothing
/// to the index and cannot damage a record — the worst a failure costs is that
/// the pass runs again next boot.
pub async fn write_game_scoped_claims(state: AppState) {
    match already_done(&state, GAME_CLAIMS_NAME).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("读不到对局幂等认领回填的完成标记，本次启动不回填：{error}"),
            );
            return;
        }
    }

    report(
        &state,
        WatchLogLevel::Info,
        "开始为索引中已有的记录补写按对局 uuid 的幂等认领".to_owned(),
    );
    let progress = match scan_game_claims(&state).await {
        Ok(progress) => progress,
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("对局幂等认领回填失败，已写入的部分仍然有效，下次启动会重跑：{error}"),
            );
            return;
        }
    };
    // The same rule the metadata pass follows, and for a sharper reason: a row
    // this pass could not read is a game whose identity is not recorded
    // anywhere, and marking the pass done would leave it that way permanently
    // while reporting success.
    if !progress.is_complete() {
        report(
            &state,
            WatchLogLevel::Error,
            format!(
                "对局幂等认领回填未完成：{} 条记录读不到字节（共扫描 {} 条，写入认领 {} 条，解析失败 {} 条）。\
                 下次启动会重跑；这些对局在补上之前不受全局去重保护",
                progress.unreadable, progress.scanned, progress.rewritten, progress.unparsable
            ),
        );
        return;
    }
    if let Err(error) = mark_done(&state, GAME_CLAIMS_NAME).await {
        report(
            &state,
            WatchLogLevel::Error,
            format!("对局幂等认领已补写完成，但完成标记没有写入，下次启动会重跑：{error}"),
        );
        return;
    }
    report(
        &state,
        WatchLogLevel::Info,
        format!(
            "对局幂等认领回填完成：共扫描 {} 条，写入认领 {} 条，无 majsoul 头跳过 {} 条",
            progress.scanned, progress.rewritten, progress.unparsable
        ),
    );
}

async fn scan_game_claims(state: &AppState) -> anyhow::Result<Progress> {
    use futures_util::StreamExt;
    use std::collections::HashSet;

    let mut progress = Progress::default();
    let mut cursor = None;
    let mut reported = 0usize;
    loop {
        // Paged newest-first like the metadata pass, and safe beside live
        // ingest for the same reason: a row written while the walk is in flight
        // either sorts above the cursor and is never read, or is read like any
        // other. Either way ingest has already written that row's claim itself,
        // so this pass has nothing to add for it.
        let (page, next) = state
            .catalog
            .scan(&RecordFilter::default(), cursor, PAGE_SIZE)
            .await?;
        // The location is cloned out of the row before the read is spawned
        // rather than borrowed from it: a closure returning a future that
        // borrows its own argument cannot be proven general enough over
        // lifetimes, and the resulting error surfaces at the `tokio::spawn` in
        // `main` rather than here.
        let locations: Vec<_> = page
            .iter()
            .enumerate()
            .map(|(index, row)| (index, row.storage.clone()))
            .collect();
        let read = futures_util::stream::iter(locations)
            .map(|(index, storage)| async move { (index, state.packs.read(&storage).await) })
            .buffer_unordered(READ_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut claims = Vec::with_capacity(read.len());
        let mut seen = HashSet::with_capacity(read.len());
        for (index, bytes) in read {
            let row = &page[index];
            let raw = match bytes {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(record = %row.id, %error, "跳过一条读不到字节的记录");
                    progress.unreadable += 1;
                    continue;
                }
            };
            // A record with no majsoul header has no game identity to record,
            // and its caller-scoped claim is all it can ever be deduplicated by.
            // Counted apart from an unreadable row because it is an outcome and
            // not a failure: it must not hold the completion marker hostage.
            let Some(uuid) = mjai::parse_metadata(&raw)
                .ok()
                .and_then(|metadata| metadata.majsoul_uuid)
            else {
                progress.unparsable += 1;
                continue;
            };
            let key = crate::indexer::game_scope_key(&uuid);
            // Two index rows carrying one game is exactly the duplication this
            // scoping exists to stop, and it is also the state PostgreSQL
            // refuses to let one statement update twice. The first row wins,
            // which is the same rule the ingest path follows.
            if seen.insert(key.clone()) {
                claims.push((key, row.id, row.sha256.clone()));
            }
        }
        progress.scanned += page.len();
        progress.rewritten += claims.len();
        state.catalog.adopt_game_claims(&claims).await?;
        if progress.scanned - reported >= PROGRESS_EVERY {
            reported = progress.scanned;
            report(
                state,
                WatchLogLevel::Info,
                format!(
                    "对局幂等认领回填中：已扫描 {} 条，写入认领 {} 条，读不到 {} 条",
                    progress.scanned, progress.rewritten, progress.unreadable
                ),
            );
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(progress)
}

/// Names the pass that scores every record already in the index.
///
/// `player_games` is written by the pack worker for everything it packs, which
/// covers records arriving from now on and nothing else. This is the other
/// 810,004.
pub const PLAYER_STATS_NAME: &str = "player_game_stats";

/// Scores every already-indexed record into `player_games`.
///
/// Spawned behind the listener like the other two passes and for the same
/// reason: it reads the bytes of every record ever collected. It writes only to
/// `player_games`, so the worst a failure costs is a statistics table that is
/// missing part of the corpus until the next boot re-runs the pass — no record
/// and no index row can be damaged by it.
///
/// Idempotent rather than resumable, the same as the others: a record scored
/// twice produces byte-identical rows under the same ReplacingMergeTree key,
/// so a rerun costs the reads and changes nothing.
pub async fn score_indexed_records(state: AppState) {
    match already_done(&state, PLAYER_STATS_NAME).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("读不到玩家战绩回填的完成标记，本次启动不回填：{error}"),
            );
            return;
        }
    }

    report(
        &state,
        WatchLogLevel::Info,
        "开始为索引中已有的记录统计玩家战绩".to_owned(),
    );
    let progress = match score(&state).await {
        Ok(progress) => progress,
        Err(error) => {
            report(
                &state,
                WatchLogLevel::Error,
                format!("玩家战绩回填失败，已写入的部分仍然有效，下次启动会重跑：{error}"),
            );
            return;
        }
    };
    if !progress.is_complete() {
        report(
            &state,
            WatchLogLevel::Error,
            format!(
                "玩家战绩回填未完成：{} 条记录读不到字节（共扫描 {} 条，写入 {} 条座位行，无法计分 {} 条）。\
                 下次启动会重跑；若确认这些 pack 已永久丢失，执行 \
                 INSERT INTO completed_backfills (name) VALUES (\'{PLAYER_STATS_NAME}\') 可以结束重跑",
                progress.unreadable, progress.scanned, progress.rewritten, progress.unparsable
            ),
        );
        return;
    }
    if let Err(error) = mark_done(&state, PLAYER_STATS_NAME).await {
        report(
            &state,
            WatchLogLevel::Error,
            format!("玩家战绩已回填完成，但完成标记没有写入，下次启动会重跑：{error}"),
        );
        return;
    }
    report(
        &state,
        WatchLogLevel::Info,
        format!(
            "玩家战绩回填完成：共扫描 {} 条，写入 {} 条座位行，无法计分跳过 {} 条",
            progress.scanned, progress.rewritten, progress.unparsable
        ),
    );
}

async fn score(state: &AppState) -> anyhow::Result<Progress> {
    use futures_util::StreamExt;

    let mut progress = Progress::default();
    let mut cursor = None;
    let mut reported = 0usize;
    loop {
        // Newest-first like the other two passes, and safe beside live ingest
        // for the same reason: a row written while the walk is in flight either
        // sorts above the cursor and is never read, or is read like any other —
        // and the worker has already scored it into rows this pass would
        // reproduce exactly.
        let (page, next) = state
            .catalog
            .scan(&RecordFilter::default(), cursor, PAGE_SIZE)
            .await?;
        let locations: Vec<_> = page
            .iter()
            .enumerate()
            .map(|(index, row)| (index, row.storage.clone()))
            .collect();
        let read = futures_util::stream::iter(locations)
            .map(|(index, storage)| async move { (index, state.packs.read(&storage).await) })
            .buffer_unordered(READ_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut games = Vec::with_capacity(read.len() * 4);
        for (index, bytes) in read {
            let row = &page[index];
            let raw = match bytes {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(record = %row.id, %error, "跳过一条读不到字节的记录");
                    progress.unreadable += 1;
                    continue;
                }
            };
            // A record that cannot be scored is an outcome, not a failure: an
            // mjai log with no hand ever dealt, or one whose seats have no
            // names, has nothing to attribute. Counted apart from an unreadable
            // row so that it cannot hold the completion marker hostage.
            let Some(scored) = mjai::events(&raw)
                .ok()
                .and_then(|events| replay::replay(&events))
            else {
                progress.unparsable += 1;
                continue;
            };
            games.extend(PlayerGame::of(row, scored));
        }
        progress.scanned += page.len();
        progress.rewritten += games.len();
        state.catalog.insert_player_games(&games).await?;
        if progress.scanned - reported >= PROGRESS_EVERY {
            reported = progress.scanned;
            report(
                state,
                WatchLogLevel::Info,
                format!(
                    "玩家战绩回填中：已扫描 {} 条，写入 {} 条座位行，读不到 {} 条，无法计分 {} 条",
                    progress.scanned, progress.rewritten, progress.unreadable, progress.unparsable
                ),
            );
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(progress)
}

async fn already_done(state: &AppState, name: &str) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query("SELECT 1 FROM completed_backfills WHERE name = $1")
            .bind(name)
            .fetch_optional(state.catalog.postgres())
            .await?
            .is_some(),
    )
}

/// `ON CONFLICT DO NOTHING` because two API replicas boot against one database
/// and both would run the pass; the second finishing is not an error, and the
/// first completion is the one worth keeping.
async fn mark_done(state: &AppState, name: &str) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO completed_backfills (name) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(name)
        .execute(state.catalog.postgres())
        .await?;
    Ok(())
}

/// Both logs, because they answer to different people: `tracing` is what an
/// operator reads out of the container, and the buffer behind
/// `/api/v1/watch/logs` is the only log the console shows.
fn report(state: &AppState, level: WatchLogLevel, message: String) {
    match level {
        WatchLogLevel::Error => tracing::error!("{message}"),
        _ => tracing::info!("{message}"),
    }
    state
        .watch_service
        .log_buffer()
        .append(level, LOG_SOURCE, message);
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::pack::PackLocation;

    /// A row as the live collector wrote it before the fix: no rule at all, and
    /// a `played_at` at the midnight of the day in the game's uuid.
    fn stale_row() -> Record {
        Record {
            id: Uuid::new_v4(),
            source: "majsoul-watch".into(),
            sha256: "0".repeat(64),
            received_at: "2026-07-16T09:41:07.123Z".parse().unwrap(),
            played_at: Some(at("2026-07-16T00:00:00Z")),
            players: vec!["p0".into(), "p1".into(), "p2".into()],
            rule: None,
            event_count: 300,
            majsoul_pb: None,
            storage: PackLocation {
                pack_key: "packs/2026/07/16/0-record.mjpack".into(),
                offset: 4096,
                compressed_size: 1024,
                raw_size: 4096,
                codec: "zstd",
            },
        }
    }

    fn parsed(rule: Option<&str>, played_at: Option<&str>) -> Metadata {
        Metadata {
            players: vec!["p0".into(), "p1".into(), "p2".into()],
            rule: rule.map(str::to_owned),
            event_count: 300,
            played_at: played_at.map(at),
            majsoul_uuid: None,
        }
    }

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    #[test]
    fn fills_in_the_rule_and_the_real_start_time() {
        let row = stale_row();
        let rewritten = rewritten(&row, &parsed(Some("3p-jade-south"), Some(START)))
            .expect("a row with no rule and a midnight played_at needed rewriting");
        assert_eq!(rewritten.rule.as_deref(), Some("3p-jade-south"));
        assert_eq!(rewritten.played_at, Some(at(START)));
    }

    #[test]
    fn leaves_a_row_the_parser_already_agrees_with() {
        let mut row = stale_row();
        row.rule = Some("3p-jade-south".into());
        row.played_at = Some(at(START));
        assert!(rewritten(&row, &parsed(Some("3p-jade-south"), Some(START))).is_none());
    }

    /// The one way this pass can be catastrophically wrong, and the one that
    /// would not be visible until someone counted. The sorting key is
    /// `(toDate(received_at), source, received_at, record_id)`, so a rewritten
    /// row that differs in any one of them is inserted beside the row it meant
    /// to replace rather than over it, and the index doubles. The pointer at the
    /// bytes is checked with them because a row that lost it is not a record.
    #[test]
    fn a_rewritten_row_keeps_every_column_of_the_sorting_key() {
        let row = stale_row();
        let rewritten = rewritten(&row, &parsed(Some("3p-jade-south"), Some(START)))
            .expect("the row needed rewriting");
        assert_eq!(rewritten.received_at, row.received_at);
        assert_eq!(rewritten.source, row.source);
        assert_eq!(rewritten.id, row.id);
        assert_eq!(rewritten.sha256, row.sha256);
        assert_eq!(rewritten.storage.pack_key, row.storage.pack_key);
        assert_eq!(rewritten.storage.offset, row.storage.offset);
        assert_eq!(
            rewritten.storage.compressed_size,
            row.storage.compressed_size
        );
        assert_eq!(rewritten.storage.raw_size, row.storage.raw_size);
        assert_eq!(rewritten.players, row.players);
        assert_eq!(rewritten.event_count, row.event_count);
    }

    /// An mjai log that never went through the converter carries no header, so
    /// the stored `played_at` is the only one there is. Nulling it would be this
    /// pass destroying data instead of restoring it.
    #[test]
    fn never_clears_what_the_record_cannot_re_derive() {
        let row = stale_row();
        assert!(rewritten(&row, &parsed(None, None)).is_none());

        let mut with_rule = stale_row();
        with_rule.rule = Some("tonpu".into());
        let rewritten = rewritten(&with_rule, &parsed(None, Some(START)))
            .expect("the real start time still had to be written");
        assert_eq!(rewritten.rule.as_deref(), Some("tonpu"));
        assert_eq!(rewritten.played_at, Some(at(START)));
    }

    /// The marker is one-shot, so the pass gets exactly one chance to spend it.
    /// A boot with the object store unreachable walks every row, fails every
    /// read, rewrites nothing and still returns `Ok` — spending the marker there
    /// would leave the corpus stale for good with an info line as the only
    /// trace. An unparsable record is the opposite: waiting for bytes that will
    /// never parse means never finishing at all.
    #[test]
    fn only_a_pass_that_could_read_its_records_may_be_marked_done() {
        assert!(Progress::default().is_complete(), "an empty index is done");
        assert!(
            Progress {
                scanned: 641_475,
                rewritten: 641_400,
                unparsable: 75,
                unreadable: 0,
            }
            .is_complete()
        );
        assert!(
            !Progress {
                scanned: 641_475,
                rewritten: 0,
                unparsable: 0,
                unreadable: 641_475,
            }
            .is_complete(),
            "an object store outage must not consume the one-shot marker"
        );
        assert!(
            !Progress {
                scanned: 641_475,
                rewritten: 641_474,
                unparsable: 0,
                unreadable: 1,
            }
            .is_complete(),
            "a single unreadable pack is an incident, not a rounding error"
        );
    }

    /// `majsoul.start_time` 1784211956 of the anonymised 3p fixture.
    const START: &str = "2026-07-16T14:25:56Z";
}
