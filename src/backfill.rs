use crate::{
    AppState,
    catalog::{Record, RecordFilter},
    mjai::{self, Metadata},
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
    match already_done(&state).await {
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
    if let Err(error) = mark_done(&state).await {
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

async fn already_done(state: &AppState) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query("SELECT 1 FROM completed_backfills WHERE name = $1")
            .bind(NAME)
            .fetch_optional(state.catalog.postgres())
            .await?
            .is_some(),
    )
}

/// `ON CONFLICT DO NOTHING` because two API replicas boot against one database
/// and both would run the pass; the second finishing is not an error, and the
/// first completion is the one worth keeping.
async fn mark_done(state: &AppState) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO completed_backfills (name) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(NAME)
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
