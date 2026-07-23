use std::collections::HashMap;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::pack::PackLocation;

#[derive(Clone, Debug, Serialize)]
pub struct Record {
    pub id: Uuid,
    pub source: String,
    pub sha256: String,
    pub received_at: DateTime<Utc>,
    pub played_at: Option<DateTime<Utc>>,
    pub players: Vec<String>,
    pub rule: Option<String>,
    pub event_count: u32,
    pub storage: PackLocation,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RecordFilter {
    pub source: Option<String>,
    pub player: Option<String>,
    pub received_from: Option<DateTime<Utc>>,
    pub received_to: Option<DateTime<Utc>>,
    pub played_from: Option<DateTime<Utc>>,
    pub played_to: Option<DateTime<Utc>>,
}

impl RecordFilter {
    pub fn matches(&self, record: &Record) -> bool {
        self.source
            .as_ref()
            .is_none_or(|source| record.source == *source)
            && self
                .player
                .as_ref()
                .is_none_or(|player| record.players.iter().any(|name| name == player))
            && self
                .received_from
                .is_none_or(|from| record.received_at >= from)
            && self.received_to.is_none_or(|to| record.received_at < to)
            && self
                .played_from
                .is_none_or(|from| record.played_at.is_some_and(|played_at| played_at >= from))
            && self
                .played_to
                .is_none_or(|to| record.played_at.is_some_and(|played_at| played_at < to))
    }
}

#[derive(Default)]
pub struct Catalog {
    records: RwLock<HashMap<Uuid, Record>>,
    idempotency: RwLock<HashMap<String, (Uuid, String)>>,
    jobs: RwLock<HashMap<Uuid, DownloadJob>>,
}

#[derive(Debug, Error)]
pub enum IdempotencyError {
    #[error("idempotency key was already used with different content")]
    Conflict,
    #[error("the first request with this idempotency key is still being processed")]
    Pending,
}

pub enum IdempotencyClaim {
    New,
    Existing(Record),
}

impl Catalog {
    pub fn claim(
        &self,
        key: &str,
        id: Uuid,
        sha256: &str,
    ) -> Result<IdempotencyClaim, IdempotencyError> {
        let mut keys = self.idempotency.write();
        match keys.get(key) {
            Some((_existing_id, existing_hash)) if existing_hash != sha256 => {
                Err(IdempotencyError::Conflict)
            }
            Some((existing_id, _)) => self
                .records
                .read()
                .get(existing_id)
                .cloned()
                .map(IdempotencyClaim::Existing)
                .ok_or(IdempotencyError::Pending),
            None => {
                keys.insert(key.to_owned(), (id, sha256.to_owned()));
                Ok(IdempotencyClaim::New)
            }
        }
    }

    pub fn abandon_claim(&self, key: &str, id: Uuid) {
        let mut keys = self.idempotency.write();
        if keys.get(key).is_some_and(|(stored_id, _)| *stored_id == id) {
            keys.remove(key);
        }
    }

    pub fn insert(&self, record: Record) {
        self.records.write().insert(record.id, record);
    }

    pub fn get(&self, id: Uuid) -> Option<Record> {
        self.records.read().get(&id).cloned()
    }

    pub fn search(
        &self,
        filter: &RecordFilter,
        cursor: Option<Uuid>,
        limit: usize,
    ) -> (Vec<Record>, Option<Uuid>) {
        let mut matches: Vec<_> = self
            .records
            .read()
            .values()
            .filter(|record| filter.matches(record))
            .cloned()
            .collect();
        matches.sort_unstable_by(|left, right| {
            right
                .received_at
                .cmp(&left.received_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        let start = cursor
            .and_then(|id| matches.iter().position(|record| record.id == id))
            .map_or(0, |position| position + 1);
        let page: Vec<_> = matches.into_iter().skip(start).take(limit + 1).collect();
        let next_cursor = (page.len() > limit).then(|| page[limit - 1].id);
        (page.into_iter().take(limit).collect(), next_cursor)
    }

    pub fn all_matching(&self, filter: &RecordFilter) -> Vec<Record> {
        self.records
            .read()
            .values()
            .filter(|record| filter.matches(record))
            .cloned()
            .collect()
    }

    pub fn insert_job(&self, job: DownloadJob) {
        self.jobs.write().insert(job.id, job);
    }

    pub fn get_job(&self, id: Uuid) -> Option<DownloadJob> {
        self.jobs.read().get(&id).cloned()
    }

    pub fn update_job(&self, id: Uuid, update: impl FnOnce(&mut DownloadJob)) {
        if let Some(job) = self.jobs.write().get_mut(&id) {
            update(job);
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DownloadRequest {
    #[serde(default)]
    pub filter: RecordFilter,
    #[serde(default)]
    pub format: DownloadFormat,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub enum DownloadFormat {
    #[default]
    #[serde(rename = "tar.gz")]
    TarGz,
    #[serde(rename = "manifest.jsonl")]
    ManifestJsonl,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct DownloadJob {
    pub id: Uuid,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub record_count: usize,
    pub download_url: Option<String>,
    pub error: Option<String>,
}
