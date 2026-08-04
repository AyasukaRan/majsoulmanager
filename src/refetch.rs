//! Borrowing the live collector's Mahjong Soul session to fetch old games.
//!
//! Re-fetching a record needs an authenticated session, and the collector
//! already has one. Opening a second one with the same account is not an
//! option: Mahjong Soul allows one session per account, so a second login
//! disconnects the collector.
//!
//! So the two halves are split by what each already owns. The backfill owns the
//! walk over the corpus — it knows which records have no protobuf and can read
//! their uuids. A watch instance owns the session and the request pacing. This
//! is the counter between them: the backfill leaves a request, and an instance
//! picks it up during the part of its poll interval it would otherwise sleep
//! through.
//!
//! That shape is what makes "do not disturb live collection" true by
//! construction rather than by tuning. An instance only serves requests after
//! it has finished its own round, and only until the round is due again; a
//! collector with nothing spare serves nothing, and the backfill waits.

use std::{collections::VecDeque, sync::Mutex, time::Duration};

use tokio::sync::{Notify, oneshot};

/// How long a request waits for a collector to pick it up before the backfill
/// gives up on it and stops.
///
/// Long enough to cover a reconnect — a session that dropped is back inside a
/// minute — and short enough that a deployment with the watch turned off ends
/// the pass with a message instead of a task parked forever.
pub const CLAIM_TIMEOUT: Duration = Duration::from_secs(180);

/// The answer to one fetch: the protobuf, or why not.
pub type FetchResult = Result<Vec<u8>, RefetchError>;

/// Why a fetch produced nothing. The two arms mean opposite things to the
/// backfill, which is the whole reason this is not a string: a refusal is about
/// one record and the pass carries on, where nobody serving is about the
/// deployment and the pass has to stop rather than walk the entire corpus
/// timing out three minutes at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefetchError {
    /// No collector picked the request up. The watch is off, or every instance
    /// is between sessions.
    Unserved,
    /// A collector asked and Mahjong Soul would not give it: the record has
    /// aged out, or the game never had a paipu.
    Refused(String),
}

impl std::fmt::Display for RefetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unserved => write!(formatter, "没有采集器接手"),
            Self::Refused(why) => write!(formatter, "{why}"),
        }
    }
}

struct Request {
    uuid: String,
    answer: oneshot::Sender<FetchResult>,
}

/// The counter itself. Cloneable through an `Arc`; every instance and the
/// backfill share one.
#[derive(Default)]
pub struct RefetchBroker {
    queue: Mutex<VecDeque<Request>>,
    /// Woken when a request arrives, so a collector in its spare window learns
    /// about work without polling for it.
    arrived: Notify,
}

impl RefetchBroker {
    /// Leaves a request and waits for an answer.
    ///
    /// `Err` covers both "no collector took this within `CLAIM_TIMEOUT`" and
    /// "a collector tried and Mahjong Soul refused", because the caller does
    /// the same thing with either: it counts the record as not re-fetched and
    /// moves on, leaving the record exactly as it was.
    pub async fn fetch(&self, uuid: &str) -> FetchResult {
        let (answer, wait) = oneshot::channel();
        self.queue
            .lock()
            .expect("refetch queue")
            .push_back(Request {
                uuid: uuid.to_owned(),
                answer,
            });
        self.arrived.notify_one();
        match tokio::time::timeout(CLAIM_TIMEOUT, wait).await {
            Ok(Ok(result)) => result,
            // The collector took the request and then went away — a session
            // that dropped mid-fetch. Counted as unserved rather than refused:
            // Mahjong Soul never answered, so this says nothing about the
            // record.
            Ok(Err(_)) => Err(RefetchError::Unserved),
            Err(_) => Err(RefetchError::Unserved),
        }
    }

    /// Takes the next request, or `None` if there is none waiting. Never
    /// blocks: a collector calls this while it still owes its own round a
    /// return, and must not be parked here by an empty queue.
    pub fn claim(&self) -> Option<PendingFetch> {
        self.queue
            .lock()
            .expect("refetch queue")
            .pop_front()
            .map(|request| PendingFetch {
                uuid: request.uuid,
                answer: request.answer,
            })
    }

    /// Resolves once a request is waiting, or after `patience` — whichever
    /// comes first. A collector with spare time uses this instead of sleeping,
    /// so an idle deployment costs nothing and a busy one starts the moment
    /// work appears.
    pub async fn wait_for_work(&self, patience: Duration) {
        let _ = tokio::time::timeout(patience, self.arrived.notified()).await;
    }

    /// How many requests are waiting. Reported, not acted on.
    pub fn waiting(&self) -> usize {
        self.queue.lock().expect("refetch queue").len()
    }
}

/// A claimed request. Dropping it without answering releases the waiter with an
/// error rather than stranding it, which is what makes a collector that dies
/// mid-fetch cost one record instead of the whole pass.
pub struct PendingFetch {
    uuid: String,
    answer: oneshot::Sender<FetchResult>,
}

impl PendingFetch {
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    pub fn answer(self, result: FetchResult) {
        // The receiver is gone when the backfill timed out first. Its record is
        // simply left as it was, so there is nothing to report here.
        let _ = self.answer.send(result);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn a_request_reaches_a_collector_and_the_answer_comes_back() {
        let broker = Arc::new(RefetchBroker::default());
        let serving = Arc::clone(&broker);
        let collector = tokio::spawn(async move {
            serving.wait_for_work(Duration::from_secs(5)).await;
            let pending = serving.claim().expect("a request was waiting");
            assert_eq!(pending.uuid(), "260716-abc");
            pending.answer(Ok(b"protobuf".to_vec()));
        });

        assert_eq!(broker.fetch("260716-abc").await.unwrap(), b"protobuf");
        collector.await.unwrap();
        assert_eq!(broker.waiting(), 0);
    }

    /// A refusal is an answer. The backfill has to be able to tell "Mahjong
    /// Soul says this record is gone" from "nothing picked this up", because
    /// only the second one means the pass should stop.
    #[tokio::test]
    async fn a_refusal_travels_back_as_an_error() {
        let broker = Arc::new(RefetchBroker::default());
        let serving = Arc::clone(&broker);
        tokio::spawn(async move {
            serving.wait_for_work(Duration::from_secs(5)).await;
            serving
                .claim()
                .expect("a request was waiting")
                .answer(Err(RefetchError::Refused("服务端错误 1203".into())));
        });

        assert_eq!(
            broker.fetch("260716-abc").await.unwrap_err(),
            RefetchError::Refused("服务端错误 1203".into())
        );
    }

    /// A collector that takes a request and then dies must release the waiter.
    /// Parking the backfill forever on one dropped session would stop the pass
    /// with no message anywhere.
    #[tokio::test]
    async fn dropping_a_claim_releases_the_waiter() {
        let broker = Arc::new(RefetchBroker::default());
        let serving = Arc::clone(&broker);
        tokio::spawn(async move {
            serving.wait_for_work(Duration::from_secs(5)).await;
            drop(serving.claim().expect("a request was waiting"));
        });

        assert_eq!(
            broker.fetch("260716-abc").await.unwrap_err(),
            RefetchError::Unserved
        );
    }

    #[tokio::test]
    async fn claiming_an_empty_queue_returns_rather_than_waiting() {
        let broker = RefetchBroker::default();
        assert!(broker.claim().is_none());
        // And so does waiting for work that never comes.
        broker.wait_for_work(Duration::from_millis(10)).await;
        assert_eq!(broker.waiting(), 0);
    }
}
