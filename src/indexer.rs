//! The record pipeline: what the ingest paths hand to the topic, and the worker
//! that drains it into packs and index rows.
//!
//! Both halves live here because the invariants they have to agree on are not
//! obvious from either side alone. The ingest half claims a record id in
//! PostgreSQL and produces the raw mjson under that id, and only then answers
//! `202`; the worker half appends what it consumes to a pack, seals it, uploads
//! it, inserts the rows, and only then commits the offset it read from. The
//! order of those last four steps is the one docs/architecture.md specifies: an
//! upload whose insert never lands is an orphan the GC collects, and rows
//! written before the offset commit are replayed and converge through
//! ReplacingMergeTree.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    AppState,
    catalog::{Catalog, CatalogError, IdempotencyClaim, Record},
    kafka::{IngestMessage, Kafka, KafkaError, PartitionConsumer, commit_offset, load_offset},
    mjai,
    objects::ObjectError,
    pack::{PackError, PackWriter},
};

/// How long a worker waits after a failed fetch, seal, upload or insert before
/// it tries again. Short enough that a ClickHouse restart costs seconds of
/// indexing latency, long enough that a broker that is down does not turn into
/// a spin.
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum IngestError {
    /// The caller's record is bad. Nothing was claimed and nothing was
    /// produced, and in a batch this is one member's problem, not the batch's.
    #[error("{0}")]
    Malformed(#[from] mjai::ParseError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    /// This server could not hand the record to the topic. Distinct from
    /// `Malformed` because it is a record the caller successfully gave us and
    /// we lost: it has to reach the caller as a `5xx`, never as a rejected
    /// member inside a `202`.
    #[error("could not hand the record to the record topic: {0}")]
    Produce(#[from] KafkaError),
    /// The topic's backlog is past `MJAI_KAFKA_MAX_LAG`. The durable
    /// replacement for the old in-memory pending-row cap.
    #[error("{0} records are still waiting to be packed and indexed; retry later")]
    Backlogged(i64),
}

/// A record that has been claimed and is waiting to be produced. The scoped
/// idempotency key travels with it so that no caller has to rebuild it to
/// release the claim, which is the one place the scoping rule could drift.
///
/// `message` is `None` when the key was already used for the same content,
/// which is the duplicate case: there is nothing left to produce, and the id is
/// the one the first request was given.
pub struct Claimed {
    pub id: Uuid,
    pub sha256: String,
    key: String,
    message: Option<IngestMessage>,
}

impl Claimed {
    pub fn is_duplicate(&self) -> bool {
        self.message.is_none()
    }

    /// What a batch import has to count against its produce bound.
    pub fn raw_len(&self) -> usize {
        self.message.as_ref().map_or(0, |message| message.raw.len())
    }
}

/// The outcome of a completed single-record ingest: the record is claimed and
/// the broker has acknowledged it.
pub struct Accepted {
    pub id: Uuid,
    pub sha256: String,
    pub duplicate: bool,
}

/// Validates, hashes and claims one record, returning the message to produce.
///
/// The idempotency key is scoped by source here rather than by the caller, and
/// the scoping is `{source}\0{key}`: the live collector's claims are stored as
/// `sha256("majsoul-watch\0{game uuid}")`, so any other separator or order
/// would miss every existing claim and re-ingest the whole corpus under new
/// record ids.
///
/// `played_at` is the caller's explicit override and nothing else. The record's
/// own `majsoul.start_time` is read by the worker out of the raw bytes, so it
/// does not have to travel.
pub async fn claim(
    catalog: &Catalog,
    kafka: &Kafka,
    source: &str,
    idempotency_key: &str,
    played_at: Option<DateTime<Utc>>,
    raw: &[u8],
) -> Result<Claimed, IngestError> {
    // Before the claim, so a refused record leaves nothing behind in
    // PostgreSQL. The reading is sampled rather than exact — see
    // `Kafka::sample_lag` — so this is a soft ceiling, and `>=` makes
    // `MJAI_KAFKA_MAX_LAG=0` an operator's off switch for ingest that does not
    // require stopping the process.
    let lag = kafka.lag();
    if lag >= kafka.max_lag() {
        return Err(IngestError::Backlogged(lag));
    }
    // Parsed for validation only: a record that is not mjai must be refused
    // while the caller is still holding it, not discovered by the worker after
    // the API has promised to keep it.
    mjai::parse_metadata(raw)?;
    let sha256 = hex::encode(Sha256::digest(raw));
    let id = Uuid::new_v4();
    let key = format!("{source}\0{idempotency_key}");
    match catalog.claim(&key, id, &sha256).await? {
        IdempotencyClaim::Existing(id) => Ok(Claimed {
            id,
            sha256,
            key,
            message: None,
        }),
        IdempotencyClaim::New => Ok(Claimed {
            id,
            sha256,
            key,
            message: Some(IngestMessage::new(id, source, played_at, raw.to_vec())),
        }),
    }
}

/// Produces claimed records and returns once the broker has acknowledged them,
/// which is what makes the `202` a durability claim.
///
/// A claim whose record never reached the topic is abandoned, because the
/// alternative is worse in a way that leaves no trace: the retry would find the
/// key claimed, be told it is a duplicate, and move on, and the record would be
/// gone with every response saying it was fine. Abandoning costs a known
/// duplicate instead — a produce the broker accepted but whose acknowledgement
/// was lost leaves the record in the topic *and* the claim released, so the
/// retry produces a second copy under a second record id and the index carries
/// two rows with one sha256. A visible duplicate beats an invisible loss. The
/// upgrade path is an idempotent producer, which rskafka has no support for, or
/// a periodic dedup pass over sha256.
///
/// The caller bounds the batch, and `produce_batch` splits whatever it is given
/// by its own wire bound, so the window of that ambiguity is one call rather
/// than a whole import.
pub async fn produce_claimed(
    catalog: &Catalog,
    kafka: &Kafka,
    claimed: Vec<Claimed>,
) -> Result<(), IngestError> {
    let mut keys = Vec::with_capacity(claimed.len());
    let mut messages = Vec::with_capacity(claimed.len());
    for record in claimed {
        // A duplicate carries no message: its record is already in the topic
        // from the request that claimed it first.
        if let Some(message) = record.message {
            keys.push((record.key, message.record_id));
            messages.push(message);
        }
    }
    if messages.is_empty() {
        return Ok(());
    }
    match kafka.produce_batch(messages).await {
        Ok(()) => Ok(()),
        Err(error) => {
            for (key, id) in &keys {
                if let Err(claim) = catalog.abandon_claim(key, *id).await {
                    // Reported rather than returned: the produce failure is
                    // what stopped the ingest, and a leaked claim only costs
                    // the collector a `409` on its next retry of that key.
                    tracing::warn!(%claim, "could not release the idempotency claim");
                }
            }
            Err(IngestError::Produce(error))
        }
    }
}

/// Releases claims for records that will never be produced.
///
/// Every way out of a batch import other than reaching the end has to run this,
/// because the claim outlives the request that made it. A member left claimed
/// but never produced is the invisible loss `produce_claimed` abandons a failed
/// produce to avoid, only worse: the operator retries the same archive, the
/// same scoped key hashes to the same row with the same content sha256, the
/// member is reported as a duplicate inside a `202`, and the record sits in
/// neither the topic nor the index until the claim is pruned thirty days later.
/// Nothing reconciles the idempotency table against the topic, so there is no
/// second chance to catch it.
///
/// A duplicate carries no message and its claim belongs to whichever request
/// produced the record first; releasing that one would let a later retry
/// re-produce a record that is already in the topic.
pub async fn abandon_claimed(catalog: &Catalog, claimed: Vec<Claimed>) {
    for record in claimed {
        if record.message.is_none() {
            continue;
        }
        if let Err(error) = catalog.abandon_claim(&record.key, record.id).await {
            tracing::warn!(%error, record = %record.id, "could not release the idempotency claim");
        }
    }
}

/// The whole ingest path for a single record, which is what both the API's
/// `POST /api/v1/records` and the collector use. The tar batch path claims and
/// produces in bulk instead, through the two halves above.
pub async fn ingest_one(
    catalog: &Catalog,
    kafka: &Kafka,
    source: &str,
    idempotency_key: &str,
    played_at: Option<DateTime<Utc>>,
    raw: &[u8],
) -> Result<Accepted, IngestError> {
    let claimed = claim(catalog, kafka, source, idempotency_key, played_at, raw).await?;
    let accepted = Accepted {
        id: claimed.id,
        sha256: claimed.sha256.clone(),
        duplicate: claimed.is_duplicate(),
    };
    produce_claimed(catalog, kafka, vec![claimed]).await?;
    Ok(accepted)
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error(transparent)]
    Pack(#[from] PackError),
    #[error(transparent)]
    Object(#[from] ObjectError),
    #[error(transparent)]
    Index(#[from] CatalogError),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error("could not read the sealed pack back: {0}")]
    Io(#[from] std::io::Error),
    #[error("uploaded {key} as {sent} bytes but the bucket reports {stored:?}")]
    Truncated {
        key: String,
        sent: u64,
        stored: Option<u64>,
    },
}

/// Appends a batch of consumed messages to a pack and builds the index rows
/// that point into it. No broker, no bucket and no index are involved, which is
/// what makes the part of the worker that can be got wrong testable on its own.
///
/// A message that cannot be parsed is dropped with an error log rather than
/// failing the batch. It cannot normally happen — ingest parses every record
/// before it claims one — so it means the bytes were corrupted in transit, and
/// failing here would stall the partition forever on one record and stop every
/// other record from ever being indexed.
pub fn append_batch(
    writer: &mut PackWriter,
    messages: Vec<IngestMessage>,
    rows: &mut Vec<Record>,
) -> Result<(), IndexError> {
    for message in messages {
        // The worker parses a second time because the message carries only the
        // raw bytes and the index row needs players, rule and event count. At
        // the live collection rate that is nothing; carrying the metadata in
        // the message headers is the upgrade path if this ever becomes the
        // worker's bottleneck.
        let metadata = match mjai::parse_metadata(&message.raw) {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::error!(
                    record = %message.record_id,
                    %error,
                    "记录 {} 在 Kafka 中已损坏，无法解析，本条不会进入索引",
                    message.record_id
                );
                continue;
            }
        };
        let location = writer.append(message.record_id, &message.raw)?;
        rows.push(Record {
            id: message.record_id,
            source: message.source.clone(),
            sha256: hex::encode(Sha256::digest(&message.raw)),
            // From the Kafka message, never from this process's clock.
            // `received_at` is the partition key and part of the sorting key,
            // so a replayed record stamped with the time of its replay writes a
            // *second* row instead of converging onto the first, and every
            // guarantee below this line about duplicates converging is gone.
            // `IngestMessage` offers no other way to reach it, which is
            // deliberate; do not add one.
            received_at: message.received_at(),
            // The caller's override wins over the record's own header, which is
            // the rule the single-record API documents.
            played_at: message.played_at.or(metadata.played_at),
            players: metadata.players,
            rule: metadata.rule,
            event_count: metadata.event_count,
            storage: location,
        });
    }
    Ok(())
}

/// One partition's pack/index worker. Written per partition so that raising
/// `MJAI_KAFKA_PARTITIONS` works, with the ceiling that nothing rebalances
/// partitions between processes: a second API replica would run a second copy
/// of every worker, and the two would fight over the same offsets. Splitting
/// this into its own binary — which is what the `state`-free helpers above are
/// shaped for — is the upgrade path.
pub struct PackWorker {
    state: AppState,
    partition: i32,
    consumer: PartitionConsumer,
    topic: String,
    postgres: PgPool,
    /// The offset to fetch from next.
    cursor: i64,
    /// The last offset committed, and therefore the point the worker falls back
    /// to when a seal fails. Everything between this and `cursor` is in hand
    /// but not yet durable anywhere.
    committed: i64,
    /// Whether the last fetch came back empty, which is this consumer's only
    /// signal that it has caught up with the partition. It decides whether an
    /// open pack is still waiting for records or has simply stopped growing.
    at_end_of_log: bool,
    open: Option<PackWriter>,
    rows: Vec<Record>,
}

impl PackWorker {
    pub async fn start(state: AppState, partition: i32) -> Result<Self, IndexError> {
        let consumer = state
            .kafka
            .consumer(partition)
            .ok_or_else(|| KafkaError::Malformed(format!("no such partition {partition}")))?;
        let topic = state.kafka.topic().to_owned();
        let postgres = state.catalog.postgres().clone();
        // Nothing committed is a first run: start at zero and let the fetch
        // jump forward if the head has already moved past it under retention.
        let committed = load_offset(&postgres, &topic, partition)
            .await?
            .unwrap_or(0);
        Ok(Self {
            state,
            partition,
            consumer,
            topic,
            postgres,
            cursor: committed,
            committed,
            at_end_of_log: false,
            open: None,
            rows: Vec::new(),
        })
    }

    /// The loop. Fetch from the committed offset, append what comes back, and
    /// seal when the pack reaches the size target or the age limit. Shutdown
    /// seals whatever is in hand: nothing would be lost by skipping it, since
    /// an uncommitted offset is simply replayed, but a redeploy would otherwise
    /// re-pack and re-upload up to a whole pack's worth of records.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if !*shutdown.borrow_and_update() {
                // The result is handled outside the `select!`, which is what
                // releases the borrow of the consumer that the fetch holds.
                // Shutdown arriving mid-fetch abandons it: the offset lives in
                // PostgreSQL, not in the request, so nothing is lost by that.
                let fetched = tokio::select! {
                    _ = shutdown.changed() => None,
                    result = self.consumer.fetch(self.cursor) => Some(result),
                };
                match fetched {
                    Some(Ok(fetched)) => {
                        let next = fetched.next_offset();
                        self.at_end_of_log = fetched.is_end_of_log();
                        let messages = self.decode(fetched.records);
                        if let Err(error) = self.append(messages) {
                            tracing::error!(
                                partition = self.partition,
                                %error,
                                "写入暂存 pack 失败，本批记录会从上次提交的位点重新消费"
                            );
                            self.discard();
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        }
                        self.cursor = next;
                    }
                    Some(Err(error)) => {
                        tracing::warn!(partition = self.partition, %error, "读取 Kafka 失败");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    None => {}
                }
            }
            let stopping = *shutdown.borrow_and_update();
            if (stopping || self.pack_is_ready())
                && self.open.is_some()
                && let Err(error) = self.seal().await
            {
                tracing::error!(
                    partition = self.partition,
                    %error,
                    "封包、上传或写索引失败，本批记录会从上次提交的位点重新消费"
                );
                self.discard();
                tokio::time::sleep(RETRY_DELAY).await;
            }
            if stopping {
                tracing::info!(partition = self.partition, "pack worker stopped");
                return;
            }
        }
    }

    /// A message that cannot be read at all is skipped rather than retried
    /// forever: it can only be a message this application did not write, and
    /// the partition has to keep moving. It is reported at error level because
    /// the record in it, if there was one, is gone.
    fn decode(&self, records: Vec<rskafka::record::RecordAndOffset>) -> Vec<IngestMessage> {
        records
            .into_iter()
            .filter_map(|record| {
                IngestMessage::from_record(record.record)
                    .inspect_err(|error| {
                        tracing::error!(
                            partition = self.partition,
                            offset = record.offset,
                            %error,
                            "Kafka 消息无法解析，已跳过"
                        );
                    })
                    .ok()
            })
            .collect()
    }

    /// Fetches to the end of the log, then seals and commits what it read. The
    /// deterministic form of `run`, and the only way an end-to-end test can
    /// observe a record becoming readable without polling for it.
    ///
    /// Unlike `run` it reports a failure instead of recovering from one, so a
    /// worker whose `drain` returned an error is not resumable: drop it, and
    /// the records it was holding are redelivered to the next one. The staging
    /// pack it leaves behind is discarded at the next boot.
    pub async fn drain(&mut self) -> Result<usize, IndexError> {
        loop {
            let fetched = self.consumer.fetch(self.cursor).await?;
            if fetched.is_end_of_log() {
                break;
            }
            let next = fetched.next_offset();
            let messages = self.decode(fetched.records);
            self.append(messages)?;
            self.cursor = next;
        }
        let indexed = self.rows.len();
        self.seal().await?;
        Ok(indexed)
    }

    fn append(&mut self, messages: Vec<IngestMessage>) -> Result<(), IndexError> {
        if messages.is_empty() {
            return Ok(());
        }
        if self.open.is_none() {
            self.open = Some(self.state.packs.staging_writer(self.partition)?);
        }
        let writer = self.open.as_mut().expect("a writer was just opened");
        append_batch(writer, messages, &mut self.rows)
    }

    /// Whether the open pack should be sealed now. Checked after a whole
    /// fetched batch rather than after each record, so a pack overshoots its
    /// target by at most one fetch — 8MiB against a 256MB target — and in
    /// exchange the offset to commit is always the end of a fetched batch
    /// rather than a point inside one.
    fn pack_is_ready(&self) -> bool {
        let Some(writer) = self.open.as_ref() else {
            return false;
        };
        let Some(age) = writer.age() else {
            return false;
        };
        should_seal(
            writer.size(),
            self.state.config.pack_target_bytes,
            age,
            Duration::from_secs(self.state.config.pack_max_age_secs),
            Duration::from_secs(self.state.config.pack_idle_secs),
            self.at_end_of_log,
        )
    }

    /// Seals whatever is open and, whatever comes of it, takes the local copy
    /// with it. On success the object is the record's home and every read
    /// resolves there; on failure the whole batch is replayed and packed again,
    /// so a file kept here would only be a staging pack stranded until the next
    /// boot discards it.
    async fn seal(&mut self) -> Result<(), IndexError> {
        let Some(writer) = self.open.take() else {
            return Ok(());
        };
        let path = writer.path().to_path_buf();
        let result = self.upload_and_index(writer).await;
        if let Err(error) = tokio::fs::remove_file(&path).await {
            tracing::warn!(?path, %error, "could not remove the staged pack copy");
        }
        result
    }

    /// Seal, upload, insert, commit — in that order, and the order is the whole
    /// design. Reversing any pair either points an index row at an object that
    /// is not there yet, which a reader cannot tell from data loss, or commits
    /// an offset for records that are not indexed, which loses them outright.
    async fn upload_and_index(&mut self, writer: PackWriter) -> Result<(), IndexError> {
        let (key, path) = writer.seal()?;
        // One PUT for the whole pack, 256MB at the target size, held in memory
        // once. Multipart upload is the upgrade path if that target rises past
        // what the container can hold.
        let bytes = tokio::fs::read(&path).await?;
        let size = bytes.len() as u64;
        self.state.objects.put(&key, bytes).await?;
        // A PUT answered 200 but stored short would leave every record in it
        // unreadable the moment the local copy goes, and the index rows about
        // to be written would say they are fine.
        match self.state.objects.head(&key).await? {
            Some(stored) if stored == size => {}
            stored => {
                return Err(IndexError::Truncated {
                    key,
                    sent: size,
                    stored,
                });
            }
        }
        let indexed = self.rows.len();
        self.state.catalog.insert_batch(&self.rows).await?;
        commit_offset(&self.postgres, &self.topic, self.partition, self.cursor).await?;
        self.committed = self.cursor;
        self.rows.clear();
        tracing::info!(
            partition = self.partition,
            pack = key,
            records = indexed,
            bytes = size,
            offset = self.committed,
            "sealed a pack, uploaded it and indexed its records"
        );
        Ok(())
    }

    /// Throws away everything not yet committed and goes back to the last
    /// committed offset. Resuming a half-written pack instead would cost a
    /// second entry for every record already in it, since the broker still owes
    /// all of them; what this costs is re-fetching and re-compressing up to one
    /// pack, and what it buys is that a pack is only ever built by one
    /// uninterrupted run of the worker.
    fn discard(&mut self) {
        if let Some(writer) = self.open.take()
            && let Err(error) = std::fs::remove_file(writer.path())
        {
            tracing::warn!(path = ?writer.path(), %error, "could not discard the staged pack");
        }
        self.rows.clear();
        self.cursor = self.committed;
    }
}

/// The seal decision, lifted out of the worker so it can be tested without a
/// broker, an object store and an index behind it.
///
/// The third clause is what keeps a quiet period from parking records in the
/// broker for the full age limit. Between two games arriving, the worker is at
/// the end of the log with a pack that has stopped growing: nothing is coming
/// to fill it, so waiting only delays every record in it becoming readable and
/// prolongs the window in which the broker's single volume holds the only copy
/// of bytes the API has already answered `202` for. It cannot inflate the
/// object count under load either — a worker with a backlog is never at the end
/// of the log, so the size target is what decides whenever it matters.
fn should_seal(
    size: u64,
    target: u64,
    age: Duration,
    max_age: Duration,
    idle_age: Duration,
    at_end_of_log: bool,
) -> bool {
    size >= target || age >= max_age || (at_end_of_log && age >= idle_age)
}

/// Starts one worker per partition. They own their offsets, so two processes
/// running these against one topic would double-index every record; this is
/// called once, from `main`.
pub fn spawn_workers(
    state: &AppState,
    shutdown: &watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..state.kafka.partition_count())
        .map(|partition| {
            let state = state.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                match PackWorker::start(state, partition).await {
                    // Never returns until shutdown; every failure inside is
                    // retried rather than propagated, because a worker that
                    // exits leaves ingest accepting records nothing consumes.
                    Ok(worker) => worker.run(shutdown).await,
                    Err(error) => tracing::error!(
                        partition,
                        %error,
                        "pack/index worker could not start; records will pile up in the topic"
                    ),
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

    use super::*;
    use crate::{objects::Objects, pack::PackStore};

    /// The size target and the age limit still decide on their own, the idle
    /// clause only fires once the worker has caught up, and it never overrides
    /// a pack that is too young to be worth an object.
    #[test]
    fn seals_early_only_when_nothing_is_left_to_consume() {
        const TARGET: u64 = 256 * 1024 * 1024;
        let max_age = Duration::from_secs(300);
        let idle = Duration::from_secs(30);
        let ready = |size, secs, at_end| {
            should_seal(
                size,
                TARGET,
                Duration::from_secs(secs),
                max_age,
                idle,
                at_end,
            )
        };

        assert!(ready(TARGET, 0, false), "a full pack seals whatever else");
        assert!(
            ready(1, 300, false),
            "the age limit does not need an idle log"
        );
        assert!(
            !ready(1, 29, true),
            "an idle log must not seal a pack younger than the idle age"
        );
        assert!(
            !ready(1, 299, false),
            "a worker with a backlog waits for the size target or the age limit"
        );
        assert!(
            ready(1, 30, true),
            "caught up with the log, a pack past the idle age has stopped growing"
        );
    }

    fn store(root: &Path) -> PackStore {
        // Nothing in these tests leaves the local pack directory, so the
        // endpoint is only a constructor argument.
        let objects =
            Objects::new("http://127.0.0.1:1", "mjai-raw", "us-east-1", "k", "s").unwrap();
        PackStore::new(root, Arc::new(objects)).unwrap()
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("mjai-indexer-test-{}", Uuid::new_v4()))
    }

    fn record(name: &str) -> Vec<u8> {
        format!(
            r#"{{"type":"start_game","names":["{name}","b","c","d"],"rule":"tonpu","majsoul":{{"start_time":1784207242}}}}
{{"type":"start_kyoku","bakaze":"E","kyoku":1}}"#
        )
        .into_bytes()
    }

    /// The batch that reaches the worker is a plain vector, so this is the whole
    /// packing and row-building step with no broker, no bucket and no index.
    #[tokio::test]
    async fn packs_a_batch_and_builds_a_row_for_every_record() {
        let root = temp_dir();
        let packs = store(&root);
        let mut writer = packs.staging_writer(0).unwrap();
        let messages: Vec<IngestMessage> = ["a", "e"]
            .iter()
            .map(|name| IngestMessage::new(Uuid::new_v4(), "watch-cn", None, record(name)))
            .collect();
        let mut rows = Vec::new();
        append_batch(&mut writer, messages.clone(), &mut rows).unwrap();

        assert_eq!(rows.len(), 2);
        for (row, message) in rows.iter().zip(&messages) {
            assert_eq!(row.id, message.record_id);
            assert_eq!(row.source, "watch-cn");
            assert_eq!(row.event_count, 2);
            assert_eq!(row.players.len(), 4);
            assert_eq!(row.sha256, hex::encode(Sha256::digest(&message.raw)));
            // Every record has its own frame in the one pack, and the location
            // in the row is what a read resolves.
            assert_eq!(row.storage.pack_key, rows[0].storage.pack_key);
            assert_eq!(packs.read(&row.storage).await.unwrap(), message.raw);
        }
        assert_ne!(rows[0].storage.offset, rows[1].storage.offset);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The one constraint the whole pipeline rests on: `received_at` is the
    /// message's own timestamp. A worker that stamped its own clock would write
    /// a second row for every replayed record instead of converging onto the
    /// first, because `received_at` is the partition key and part of the
    /// sorting key.
    #[test]
    fn received_at_comes_from_the_message_and_survives_a_replay() {
        let root = temp_dir();
        let packs = store(&root);
        let message = IngestMessage::new(Uuid::new_v4(), "watch-cn", None, record("a"));

        let mut rows = Vec::new();
        append_batch(
            &mut packs.staging_writer(0).unwrap(),
            vec![message.clone()],
            &mut rows,
        )
        .unwrap();
        // The same message consumed again, as a replay of an uncommitted offset
        // delivers it: a different pack, and every column of the sorting key
        // identical.
        let mut replayed = Vec::new();
        append_batch(
            &mut packs.staging_writer(0).unwrap(),
            vec![message.clone()],
            &mut replayed,
        )
        .unwrap();

        assert_eq!(rows[0].received_at, message.received_at());
        assert_eq!(replayed[0].received_at, rows[0].received_at);
        assert_eq!(replayed[0].id, rows[0].id);
        assert_ne!(
            replayed[0].storage.pack_key, rows[0].storage.pack_key,
            "a replay is supposed to land in a new pack; only the sorting key has to match"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A record corrupted in transit must not stall the partition: it is the
    /// only record in the batch that is lost, and it is reported.
    #[test]
    fn a_corrupt_record_is_dropped_without_failing_its_batch() {
        let root = temp_dir();
        let packs = store(&root);
        let mut rows = Vec::new();
        append_batch(
            &mut packs.staging_writer(0).unwrap(),
            vec![
                IngestMessage::new(Uuid::new_v4(), "watch-cn", None, b"not json".to_vec()),
                IngestMessage::new(Uuid::new_v4(), "watch-cn", None, record("a")),
            ],
            &mut rows,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].players[0], "a");
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The collector's override wins; a record with no override keeps the time
    /// out of its own majsoul header.
    #[test]
    fn played_at_prefers_the_override_over_the_record_header() {
        let root = temp_dir();
        let packs = store(&root);
        let override_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut rows = Vec::new();
        append_batch(
            &mut packs.staging_writer(0).unwrap(),
            vec![
                IngestMessage::new(Uuid::new_v4(), "s", Some(override_at), record("a")),
                IngestMessage::new(Uuid::new_v4(), "s", None, record("b")),
            ],
            &mut rows,
        )
        .unwrap();
        assert_eq!(rows[0].played_at, Some(override_at));
        assert_eq!(
            rows[1].played_at,
            Some("2026-07-16T13:07:22Z".parse::<DateTime<Utc>>().unwrap())
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
