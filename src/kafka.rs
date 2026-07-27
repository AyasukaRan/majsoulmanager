//! The transport half of the pack/index pipeline: raw mjson is produced to a
//! Redpanda topic at ingest time and consumed by the pack worker, which packs,
//! uploads and indexes it before committing the offset it read from.
//!
//! Two properties here are load-bearing for correctness rather than for
//! convenience. The first is that a message's Kafka timestamp is the ingest
//! time and is the only source of `received_at`: that column is the partition
//! key and part of the table's sorting key, so a consumer that re-derived it
//! from its own clock would write a *second* row for a replayed record instead
//! of converging onto the first through ReplacingMergeTree. `IngestMessage`
//! therefore stamps the timestamp in its constructor and offers no way to pass
//! one in, and the consumer's only route back to a message is `from_record`,
//! which reads the timestamp off the message. The second is that the record id
//! is assigned by the API at claim time and travels as the message key, so it
//! is stable across a replay for the same reason.
//!
//! rskafka has no consumer groups and no offset commit API, so offsets live in
//! PostgreSQL alongside the idempotency claims, which is where
//! docs/architecture.md already keeps transactional state.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, SubsecRound, Utc};
use rskafka::{
    BackoffConfig,
    client::{
        ClientBuilder,
        error::{Error as BrokerError, ProtocolError},
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    record::{Record, RecordAndOffset},
};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;

/// Redpanda accepts a produce request of at most `kafka_batch_max_bytes`, 1 MiB
/// by default, and rskafka packs the whole `Vec<Record>` it is handed into a
/// single RecordBatch with no way to split it — a produce is all-or-nothing at
/// the batch level. So this is a property of the broker, not a tuning knob.
/// Records run 11,352 to 106,157 bytes decompressed (measured over 300 real 4p
/// throne games) and the API caps a single one at `MJAI_MAX_RECORD_BYTES`,
/// 256 KiB, so 768 KiB leaves room for the batch framing, the headers, and the
/// one record that crosses the bound. Counting uncompressed bytes against a
/// limit the broker applies to the compressed batch errs the safe way.
///
/// Public because the batch import path stops accumulating at the same bound,
/// which is what makes one `produce_batch` call one produce request.
pub const MAX_PRODUCE_BATCH_BYTES: usize = 768 * 1024;

/// rskafka hardcodes `acks = -1` and a 30 second broker-side produce timeout,
/// and has no request timeout of its own: a half-open connection hangs the
/// future forever. This has to sit above the broker's own timeout, or a produce
/// the broker is still about to complete is abandoned and retried into a
/// duplicate.
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(45);

/// The long poll a caught-up worker parks in. The fetch already blocks for this
/// long waiting for a single byte, so an empty result after a full wait is the
/// correct idle path and needs no sleep on top of it.
const FETCH_MAX_WAIT_MS: i32 = 1_000;

/// `bytes.end - 1` is what actually goes on the wire as `max_bytes`, and it has
/// to stay under `ClientBuilder::max_message_size` (100 MiB by default).
const FETCH_MAX_BYTES: i32 = 8 * 1024 * 1024;

/// Above `FETCH_MAX_WAIT_MS` by enough to cover a reconnect inside the client's
/// own retry loop, which the long poll does not account for.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Metadata, topic creation and offset lookups: small requests that either
/// answer promptly or are talking to a broker that is not there.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);

/// rskafka's default backoff has no deadline at all and a 500 second maximum
/// sleep, so a broker that is down turns every call into a future that never
/// returns. Bounded here, and every call is additionally wrapped in a timeout
/// because the retry loop cannot regain control from a request that hangs.
fn backoff() -> BackoffConfig {
    BackoffConfig {
        max_backoff: Duration::from_secs(5),
        deadline: Some(Duration::from_secs(30)),
        ..BackoffConfig::default()
    }
}

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("broker call failed: {0}")]
    Broker(#[from] BrokerError),
    #[error("broker did not answer within {0:?}")]
    Timeout(Duration),
    #[error("kafka message is unusable: {0}")]
    Malformed(String),
    #[error("kafka offset store is unavailable: {0}")]
    Store(#[from] sqlx::Error),
}

/// One record on its way to the pack worker. Everything the worker needs to
/// build an index row travels with it except the pack location, which only
/// exists once the worker has written the record into a pack.
///
/// `received_at` is deliberately not a field the caller can set: see the module
/// documentation for what a consumer-side clock would cost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestMessage {
    pub record_id: Uuid,
    pub source: String,
    /// The collector's `X-Mjai-Played-At` override. The record's own
    /// `majsoul.start_time` is parsed by the consumer out of the raw bytes, so
    /// only the override has to be carried.
    pub played_at: Option<DateTime<Utc>>,
    pub raw: Vec<u8>,
    received_at: DateTime<Utc>,
}

impl IngestMessage {
    /// Stamps the ingest time. This is the only constructor, so there is no
    /// call site that can supply a different one and no way for the producer
    /// and the consumer to disagree about what `received_at` means.
    ///
    /// Truncated to the millisecond because that is the resolution a Kafka
    /// message timestamp has on the wire, so the value here is the value the
    /// consumer will read back rather than one that quietly loses digits in
    /// transit. The index columns are `DateTime64(3)` for the same reason.
    pub fn new(
        record_id: Uuid,
        source: &str,
        played_at: Option<DateTime<Utc>>,
        raw: Vec<u8>,
    ) -> Self {
        Self {
            record_id,
            source: source.to_owned(),
            played_at: played_at.map(|at| at.trunc_subsecs(3)),
            raw,
            received_at: Utc::now().trunc_subsecs(3),
        }
    }

    /// The ingest time, which is what the consumer writes as `received_at`.
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    fn into_record(self) -> Record {
        let mut headers = BTreeMap::from([("source".to_owned(), self.source.into_bytes())]);
        if let Some(played_at) = self.played_at {
            headers.insert("played_at".to_owned(), played_at.to_rfc3339().into_bytes());
        }
        Record {
            // The canonical hyphenated form rather than the sixteen raw bytes,
            // so that `rpk topic consume` on a stuck partition shows an id that
            // can be pasted straight into a query.
            key: Some(self.record_id.to_string().into_bytes()),
            value: Some(self.raw),
            headers,
            timestamp: self.received_at,
        }
    }

    /// The consumer's only way back to a message, so that `received_at` can
    /// only ever come from the message timestamp.
    pub fn from_record(record: Record) -> Result<Self, KafkaError> {
        let key = record
            .key
            .ok_or_else(|| KafkaError::Malformed("message has no record id key".into()))?;
        let record_id = std::str::from_utf8(&key)
            .ok()
            .and_then(|key| Uuid::parse_str(key).ok())
            .ok_or_else(|| KafkaError::Malformed("message key is not a record id".into()))?;
        let source = record
            .headers
            .get("source")
            .and_then(|source| std::str::from_utf8(source).ok())
            .ok_or_else(|| KafkaError::Malformed(format!("{record_id} carries no source")))?
            .to_owned();
        // A malformed override is dropped rather than failing the record: the
        // consumer falls back to the record's own `majsoul.start_time`, and a
        // header nobody can parse is not worth stalling a partition over.
        let played_at = record
            .headers
            .get("played_at")
            .and_then(|at| std::str::from_utf8(at).ok())
            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
            .map(|at| at.with_timezone(&Utc));
        Ok(Self {
            record_id,
            source,
            played_at,
            raw: record
                .value
                .ok_or_else(|| KafkaError::Malformed(format!("{record_id} has no body")))?,
            received_at: record.timestamp,
        })
    }
}

/// One `PartitionClient` per partition, each holding its own TCP connection to
/// the leader — rskafka pools nothing — so these are built once at boot and
/// shared, never per message.
pub struct Kafka {
    topic: String,
    partitions: Vec<Arc<PartitionClient>>,
    /// The backlog per partition as of the last sample. See `lag`.
    lag: Vec<AtomicI64>,
    max_lag: i64,
}

impl Kafka {
    pub async fn connect(config: &Config) -> Result<Self, KafkaError> {
        if config.kafka_partitions < 1 {
            return Err(KafkaError::Malformed(format!(
                "MJAI_KAFKA_PARTITIONS must be at least 1, got {}",
                config.kafka_partitions
            )));
        }
        let brokers: Vec<String> = config
            .kafka_bootstrap_servers
            .split(',')
            .map(|broker| broker.trim().to_owned())
            .filter(|broker| !broker.is_empty())
            .collect();
        if brokers.is_empty() {
            return Err(KafkaError::Malformed(
                "MJAI_KAFKA_BOOTSTRAP_SERVERS is empty".into(),
            ));
        }
        // `build` eagerly refreshes metadata, so an unreachable broker fails
        // here rather than on the first produce.
        let client = bounded(
            CONTROL_TIMEOUT,
            ClientBuilder::new(brokers)
                .client_id("mjai-management")
                .backoff_config(backoff())
                .build(),
        )
        .await?;

        // Creating a topic that exists is a hard error, not a no-op, so an
        // unhandled one would crash-loop the process on every boot after the
        // first.
        //
        // Retention, `retention.ms`, `retention.bytes` and `max.message.bytes`
        // cannot be set from here at all: rskafka hardcodes an empty config
        // list on CreateTopics and implements no AlterConfigs, so there is no
        // client-side knob to look for. They are cluster properties passed to
        // the broker on its command line in docker-compose.yml, under the
        // `redpanda` service's `command:` list as `--set` arguments — that is
        // where to change them, not here.
        let controller = client.controller_client()?;
        match bounded(
            CONTROL_TIMEOUT,
            controller.create_topic(
                config.kafka_topic.clone(),
                config.kafka_partitions,
                // One broker, so one replica is the only replication factor
                // that can be satisfied. A multi-broker cluster would want 3
                // and a corresponding `min.insync.replicas`, which is the same
                // cluster-property change as the retention settings above.
                1,
                CONTROL_TIMEOUT.as_millis() as i32,
            ),
        )
        .await
        {
            Ok(()) => tracing::info!(topic = config.kafka_topic, "created the ingest topic"),
            Err(KafkaError::Broker(BrokerError::ServerError {
                protocol_error: ProtocolError::TopicAlreadyExists,
                ..
            })) => {}
            Err(error) => return Err(error),
        }

        let mut partitions = Vec::with_capacity(config.kafka_partitions as usize);
        for partition in 0..config.kafka_partitions {
            // `Error` rather than `Retry`: the topic was just ensured, so a
            // partition that is still unknown means the configuration and the
            // cluster disagree, and retrying that only hides it.
            partitions.push(Arc::new(
                bounded(
                    CONTROL_TIMEOUT,
                    client.partition_client(
                        config.kafka_topic.clone(),
                        partition,
                        UnknownTopicHandling::Error,
                    ),
                )
                .await?,
            ));
        }
        Ok(Self {
            topic: config.kafka_topic.clone(),
            lag: (0..partitions.len()).map(|_| AtomicI64::new(0)).collect(),
            partitions,
            max_lag: config.kafka_max_lag,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The backlog at which ingest is refused. Carried here rather than read
    /// from the configuration at every call site so that the reading and the
    /// ceiling it is compared against cannot come from two different places.
    pub fn max_lag(&self) -> i64 {
        self.max_lag
    }

    pub fn partition_count(&self) -> i32 {
        self.partitions.len() as i32
    }

    /// Nothing else produces to this topic, so this is not Kafka's murmur2
    /// default partitioner and does not need to be. The only property required
    /// is that a given record id always lands in the same partition, so that a
    /// replay of the same record stays in the same offset stream.
    fn partition_for(&self, record_id: Uuid) -> usize {
        (record_id.as_u128() % self.partitions.len() as u128) as usize
    }

    /// Produces one record and returns once the broker has acknowledged it,
    /// which is what makes the API's `202` a durability claim.
    pub async fn produce(&self, message: IngestMessage) -> Result<(), KafkaError> {
        self.produce_batch(vec![message]).await
    }

    /// The tar import path, which hands over tens of thousands of records at
    /// once. Chunked by accumulated size rather than by count: at the measured
    /// maximum record size a fixed count of fifty would exceed the broker's
    /// maximum batch on a bad draw and fail the whole request.
    ///
    /// Not atomic across chunks. A failure part-way through leaves the earlier
    /// chunks durably produced, so the caller must treat the batch as failed
    /// and let the idempotency claims sort out the retry rather than assume
    /// nothing landed.
    pub async fn produce_batch(&self, messages: Vec<IngestMessage>) -> Result<(), KafkaError> {
        let mut routed: Vec<Vec<Record>> = vec![Vec::new(); self.partitions.len()];
        for message in messages {
            routed[self.partition_for(message.record_id)].push(message.into_record());
        }
        for (partition, records) in routed.into_iter().enumerate() {
            for chunk in produce_chunks(records) {
                bounded(
                    PRODUCE_TIMEOUT,
                    self.partitions[partition].produce(chunk, Compression::Zstd),
                )
                .await?;
            }
        }
        Ok(())
    }

    pub fn consumer(&self, partition: i32) -> Option<PartitionConsumer> {
        let index = usize::try_from(partition).ok()?;
        Some(PartitionConsumer {
            client: Arc::clone(self.partitions.get(index)?),
            partition,
        })
    }

    /// The backlog as of the last sample, summed over the partitions. The
    /// ingest path calls this per request to decide whether to refuse work, so
    /// it must not touch the network: it is a relaxed load per partition.
    pub fn lag(&self) -> i64 {
        self.lag.iter().map(|lag| lag.load(Ordering::Relaxed)).sum()
    }

    /// Refreshes the sample. Two broker round trips and one query per
    /// partition, which is why it is periodic rather than exact — doing it per
    /// ingest request would put a synchronous broker call in front of every
    /// record.
    ///
    /// The consequence of the staleness is bounded and one-directional: between
    /// samples the backlog can only be underestimated by however much was
    /// ingested in that interval, so `MJAI_KAFKA_MAX_LAG` is a soft ceiling
    /// that can be overshot by one interval's worth of records before ingest is
    /// refused. It is never overestimated into refusing work that a healthy
    /// worker is keeping up with, which is the direction that would matter.
    pub async fn sample_lag(&self, pool: &PgPool) -> Result<i64, KafkaError> {
        for (index, client) in self.partitions.iter().enumerate() {
            let partition = index as i32;
            let high_watermark =
                bounded(CONTROL_TIMEOUT, client.get_offset(OffsetAt::Latest)).await?;
            let earliest = bounded(CONTROL_TIMEOUT, client.get_offset(OffsetAt::Earliest)).await?;
            let committed = load_offset(pool, &self.topic, partition).await?;
            self.lag[index].store(
                partition_lag(high_watermark, earliest, committed),
                Ordering::Relaxed,
            );
        }
        Ok(self.lag())
    }

    /// The background sampler. A failed sample leaves the previous reading in
    /// place rather than zeroing it, because a broker that cannot be reached is
    /// not a broker with an empty backlog, and reporting zero would open the
    /// ingest gate exactly when the worker is least able to drain it.
    pub async fn sample_lag_forever(self: Arc<Self>, pool: PgPool, interval: Duration) {
        loop {
            if let Err(error) = self.sample_lag(&pool).await {
                tracing::warn!(%error, "could not sample the Kafka backlog");
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// The reading side of one partition. The worker drives `fetch` in a plain loop
/// and owns the offset itself; rskafka's `StreamConsumer` keeps the offset
/// inside the stream and terminates permanently on the first error it cannot
/// handle, neither of which suits a consumer whose commit points are decided by
/// a ClickHouse insert.
pub struct PartitionConsumer {
    client: Arc<PartitionClient>,
    partition: i32,
}

/// The result of one fetch. End of log is not an error and not an empty
/// `Option`: it is the ordinary state of a worker that has caught up, and the
/// fetch has already long-polled for it, so the worker idles by looping rather
/// than by sleeping.
pub struct Fetched {
    pub records: Vec<RecordAndOffset>,
    pub high_watermark: i64,
    /// The offset the fetch actually read from, which is the requested one
    /// unless it had fallen off the retention window.
    pub start_offset: i64,
}

impl Fetched {
    pub fn is_end_of_log(&self) -> bool {
        self.records.is_empty()
    }

    /// The offset to fetch from next, and the offset to commit once every
    /// record in this batch has been packed, uploaded and indexed. At the end
    /// of the log it is the offset that was read from, so a committed offset
    /// never moves backwards over an idle poll.
    pub fn next_offset(&self) -> i64 {
        self.records
            .last()
            .map_or(self.start_offset, |last| last.offset + 1)
    }
}

impl PartitionConsumer {
    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub async fn fetch(&self, offset: i64) -> Result<Fetched, KafkaError> {
        let attempt = self.fetch_at(offset).await;
        let (start_offset, result) = match attempt {
            Err(KafkaError::Broker(BrokerError::ServerError {
                protocol_error: ProtocolError::OffsetOutOfRange,
                ..
            })) => {
                let earliest =
                    bounded(CONTROL_TIMEOUT, self.client.get_offset(OffsetAt::Earliest)).await?;
                // Resuming from the earliest retained offset looks exactly like
                // recovery from the outside while actually being permanent data
                // loss, so it is reported at error level with the count, in the
                // language the console shows.
                if earliest > offset {
                    tracing::error!(
                        partition = self.partition,
                        committed = offset,
                        earliest,
                        skipped = earliest - offset,
                        "Kafka 分区 {} 的位点 {offset} 已被 retention 清理，直接跳到 {earliest}：\
                         中间 {} 条记录永远不会被打包或索引，已经永久丢失",
                        self.partition,
                        earliest - offset
                    );
                } else {
                    // The other side of the range: the stored offset is past
                    // the head, which is what a recreated or truncated topic
                    // looks like. Nothing is lost, but the offset was pointing
                    // at a log that no longer exists.
                    tracing::error!(
                        partition = self.partition,
                        committed = offset,
                        earliest,
                        "Kafka 分区 {} 的位点 {offset} 超出了分区末尾（topic 可能被重建过），\
                         回到 {earliest} 重新消费",
                        self.partition
                    );
                }
                (earliest, self.fetch_at(earliest).await)
            }
            other => (offset, other),
        };
        let (records, high_watermark) = result?;
        Ok(Fetched {
            records,
            high_watermark,
            start_offset,
        })
    }

    async fn fetch_at(&self, offset: i64) -> Result<(Vec<RecordAndOffset>, i64), KafkaError> {
        bounded(
            FETCH_TIMEOUT,
            self.client
                .fetch_records(offset, 1..FETCH_MAX_BYTES, FETCH_MAX_WAIT_MS),
        )
        .await
    }
}

/// The committed offset for a partition, or `None` when this deployment has
/// never consumed it — a fresh topic, or a worker starting for the first time.
pub async fn load_offset(
    pool: &PgPool,
    topic: &str,
    partition: i32,
) -> Result<Option<i64>, KafkaError> {
    let row =
        sqlx::query("SELECT next_offset FROM kafka_offsets WHERE topic = $1 AND partition_id = $2")
            .bind(topic)
            .bind(partition)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|row| row.try_get("next_offset")).transpose()?)
}

/// Advances the committed offset. One statement, so there is no window in which
/// a partition has half an offset: the upsert either replaces the row or does
/// not run, and a crash between a `DELETE` and an `INSERT`, or between two
/// updates, is not a state this can reach.
///
/// Called only after every record up to `next_offset` has been packed, uploaded
/// and indexed. That ordering is what makes the pipeline at-least-once: a crash
/// before this replays the batch, and the replay converges through
/// ReplacingMergeTree because `record_id` and `received_at` both travel with
/// the message.
pub async fn commit_offset(
    pool: &PgPool,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> Result<(), KafkaError> {
    sqlx::query(
        "INSERT INTO kafka_offsets (topic, partition_id, next_offset) VALUES ($1, $2, $3)
         ON CONFLICT (topic, partition_id)
         DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()",
    )
    .bind(topic)
    .bind(partition)
    .bind(next_offset)
    .execute(pool)
    .await?;
    Ok(())
}

/// Splits a partition's records into produce requests no larger than the
/// broker's maximum batch. A record that exceeds the bound on its own is left
/// in a chunk of one: the broker will reject it, which is the honest outcome,
/// and dropping it here would lose a record the API has already claimed.
fn produce_chunks(records: Vec<Record>) -> Vec<Vec<Record>> {
    let mut chunks: Vec<Vec<Record>> = Vec::new();
    let mut current: Vec<Record> = Vec::new();
    let mut current_bytes = 0usize;
    for record in records {
        let size = record.approximate_size();
        if !current.is_empty() && current_bytes + size > MAX_PRODUCE_BATCH_BYTES {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += size;
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// `OffsetAt::Latest` is the next offset to be written, not the last one
/// written, so the backlog is measured against the offset the worker will read
/// next and a caught-up partition reads zero rather than one. With nothing
/// committed the worker will start from the earliest retained offset, so that
/// is what the backlog is measured from — a topic whose head has moved on under
/// retention would otherwise report its whole history as backlog.
fn partition_lag(high_watermark: i64, earliest: i64, committed: Option<i64>) -> i64 {
    (high_watermark - committed.unwrap_or(earliest)).max(0)
}

/// rskafka has no request timeout anywhere: a half-open connection to a broker
/// leaves the request future hanging, and the backoff deadline cannot help
/// because the retry loop never regains control. Every call goes through here.
async fn bounded<T, F>(limit: Duration, call: F) -> Result<T, KafkaError>
where
    F: Future<Output = Result<T, BrokerError>>,
{
    match tokio::time::timeout(limit, call).await {
        Ok(result) => Ok(result?),
        Err(_) => Err(KafkaError::Timeout(limit)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The measured distribution of a real record, decompressed: the minimum,
    /// the median, the 95th percentile and the largest one ever seen.
    const MEASURED_SIZES: [usize; 4] = [11_352, 53_668, 80_374, 106_157];

    fn message(size: usize) -> IngestMessage {
        IngestMessage::new(Uuid::new_v4(), "test", None, vec![b'x'; size])
    }

    fn records(sizes: impl IntoIterator<Item = usize>) -> Vec<Record> {
        sizes
            .into_iter()
            .map(|size| message(size).into_record())
            .collect()
    }

    /// A count-based chunk size is what this replaces: fifty records drawn from
    /// the top of the measured distribution is 5.3MB, five times the broker's
    /// default maximum batch, and the produce fails for the whole batch.
    #[test]
    fn chunks_a_batch_by_size_not_by_count() {
        let sizes: Vec<usize> = MEASURED_SIZES.iter().copied().cycle().take(400).collect();
        let total: usize = sizes.iter().sum();
        let chunks = produce_chunks(records(sizes.clone()));

        assert!(chunks.len() > 1, "a 25MB batch was produced as one request");
        for chunk in &chunks {
            let bytes: usize = chunk.iter().map(Record::approximate_size).sum();
            assert!(
                bytes <= MAX_PRODUCE_BATCH_BYTES,
                "a chunk of {bytes} bytes exceeds the broker's maximum batch"
            );
        }
        // Nothing may be dropped or reordered on the way into the chunks: the
        // records were claimed in PostgreSQL before they got here.
        let chunked: Vec<usize> = chunks
            .iter()
            .flatten()
            .map(|record| record.value.as_ref().expect("a body").len())
            .collect();
        assert_eq!(chunked, sizes);
        assert!(total > MAX_PRODUCE_BATCH_BYTES);
    }

    /// Only records at the measured maximum, which is the draw that breaks a
    /// count-based chunker.
    #[test]
    fn chunks_a_batch_of_nothing_but_the_largest_records() {
        let chunks = produce_chunks(records([MEASURED_SIZES[3]; 50]));
        for chunk in &chunks {
            let bytes: usize = chunk.iter().map(Record::approximate_size).sum();
            assert!(
                bytes <= MAX_PRODUCE_BATCH_BYTES,
                "{bytes} bytes in one chunk"
            );
        }
        assert_eq!(chunks.iter().flatten().count(), 50);
    }

    /// A record too large for any batch keeps its own request rather than being
    /// silently dropped or wedged into one that is already full.
    #[test]
    fn keeps_an_oversized_record_in_a_chunk_of_its_own() {
        let chunks = produce_chunks(records([1_024, MAX_PRODUCE_BATCH_BYTES + 1, 1_024]));
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| chunk.len() == 1));
    }

    #[test]
    fn produces_no_request_for_an_empty_batch() {
        assert!(produce_chunks(Vec::new()).is_empty());
    }

    fn fetched(start_offset: i64, offsets: &[i64]) -> Fetched {
        Fetched {
            records: offsets
                .iter()
                .map(|offset| RecordAndOffset {
                    record: message(16).into_record(),
                    offset: *offset,
                })
                .collect(),
            high_watermark: offsets.last().map_or(start_offset, |last| last + 1),
            start_offset,
        }
    }

    #[test]
    fn the_next_offset_is_one_past_the_last_record() {
        let batch = fetched(41, &[41, 42, 43]);
        assert!(!batch.is_end_of_log());
        assert_eq!(batch.next_offset(), 44);
        assert_eq!(batch.next_offset(), batch.high_watermark);
    }

    /// An idle poll must not move the committed offset, in either direction.
    #[test]
    fn an_empty_fetch_is_end_of_log_and_holds_the_offset() {
        let batch = fetched(44, &[]);
        assert!(batch.is_end_of_log());
        assert_eq!(batch.next_offset(), 44);
    }

    /// After a jump over a retention gap the offset to commit follows the
    /// records actually read, not the offset that was asked for.
    #[test]
    fn the_next_offset_follows_a_jump_to_the_earliest_offset() {
        let batch = fetched(9_000, &[9_000, 9_001]);
        assert_eq!(batch.next_offset(), 9_002);
    }

    #[test]
    fn lag_is_measured_against_the_offset_the_worker_will_read_next() {
        assert_eq!(partition_lag(1_000, 0, Some(400)), 600);
        // Caught up: `Latest` is the next offset to be written, so the two are
        // equal and the backlog is zero rather than one.
        assert_eq!(partition_lag(1_000, 0, Some(1_000)), 0);
        // A committed offset past the head — a recreated topic — is a backlog
        // of nothing, not a negative one that would cancel another partition's.
        assert_eq!(partition_lag(1_000, 0, Some(1_200)), 0);
    }

    /// Nothing consumed yet. Measuring from zero would report the whole history
    /// of a topic whose head has moved under retention as backlog, and refuse
    /// ingest on a deployment that has simply never run the worker.
    #[test]
    fn lag_with_no_committed_offset_starts_from_the_earliest_retained_record() {
        assert_eq!(partition_lag(1_000, 900, None), 100);
        assert_eq!(partition_lag(1_000, 1_000, None), 0);
        assert_eq!(partition_lag(0, 0, None), 0);
    }

    /// The round trip the consumer depends on: everything the worker needs
    /// survives, and `received_at` comes back off the message timestamp rather
    /// than from any field a caller could have set.
    #[test]
    fn a_message_round_trips_through_a_kafka_record() {
        let played_at = Some(
            DateTime::parse_from_rfc3339("2026-07-16T13:07:22Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        let message = IngestMessage::new(Uuid::new_v4(), "watch-cn", played_at, b"{}".to_vec());
        let record = message.clone().into_record();
        assert_eq!(record.timestamp, message.received_at());
        assert_eq!(IngestMessage::from_record(record).unwrap(), message);
    }

    #[test]
    fn refuses_a_message_that_cannot_name_its_record() {
        let mut record = message(16).into_record();
        record.key = Some(b"not-a-uuid".to_vec());
        assert!(matches!(
            IngestMessage::from_record(record),
            Err(KafkaError::Malformed(_))
        ));

        let mut record = message(16).into_record();
        record.headers.remove("source");
        assert!(matches!(
            IngestMessage::from_record(record),
            Err(KafkaError::Malformed(_))
        ));
    }
}
