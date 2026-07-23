use std::collections::HashMap;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAX_RECENT_RECORDS: usize = 10_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchEventKind {
    Live,
    Pending,
    Fetching,
    Converting,
    Completed,
    FetchFailed,
    ConversionFailed,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WatchEvent {
    pub uuid: String,
    pub event: WatchEventKind,
    pub mode_id: Option<i32>,
    pub started_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
    pub record_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UuidState {
    Live,
    Pending,
    Fetching,
    Fetched,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversionState {
    Waiting,
    Converting,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct WatchRecord {
    pub uuid: String,
    pub uuid_state: UuidState,
    pub conversion_state: ConversionState,
    pub mode_id: Option<i32>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub attempts: u32,
    pub message: Option<String>,
    pub record_id: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("watch UUID must be 1-96 printable ASCII characters")]
    InvalidUuid,
}

#[derive(Default)]
pub struct WatchRegistry {
    records: RwLock<HashMap<String, WatchRecord>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WatchSummary {
    pub total: usize,
    pub live: usize,
    pub pending: usize,
    pub converting: usize,
    pub completed: usize,
    pub failed: usize,
    pub items: Vec<WatchRecord>,
}

impl WatchRegistry {
    pub fn apply(&self, event: WatchEvent) -> Result<WatchRecord, WatchError> {
        if event.uuid.is_empty()
            || event.uuid.len() > 96
            || !event
                .uuid
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b'/')
        {
            return Err(WatchError::InvalidUuid);
        }

        let now = Utc::now();
        let mut records = self.records.write();
        let record = records
            .entry(event.uuid.clone())
            .or_insert_with(|| WatchRecord {
                uuid: event.uuid.clone(),
                uuid_state: UuidState::Live,
                conversion_state: ConversionState::Waiting,
                mode_id: event.mode_id,
                started_at: event.started_at,
                updated_at: now,
                attempts: 0,
                message: None,
                record_id: None,
            });

        if event.mode_id.is_some() {
            record.mode_id = event.mode_id;
        }
        if event.started_at.is_some() {
            record.started_at = event.started_at;
        }
        record.updated_at = now;
        record.message = event.message;
        if event.record_id.is_some() {
            record.record_id = event.record_id;
        }

        match event.event {
            WatchEventKind::Live => {
                record.uuid_state = UuidState::Live;
                record.conversion_state = ConversionState::Waiting;
            }
            WatchEventKind::Pending => {
                record.uuid_state = UuidState::Pending;
                record.conversion_state = ConversionState::Waiting;
            }
            WatchEventKind::Fetching => {
                record.uuid_state = UuidState::Fetching;
                record.attempts = record.attempts.saturating_add(1);
            }
            WatchEventKind::Converting => {
                record.uuid_state = UuidState::Fetched;
                record.conversion_state = ConversionState::Converting;
            }
            WatchEventKind::Completed => {
                record.uuid_state = UuidState::Fetched;
                record.conversion_state = ConversionState::Completed;
            }
            WatchEventKind::FetchFailed => {
                record.uuid_state = UuidState::Failed;
                record.conversion_state = ConversionState::Waiting;
            }
            WatchEventKind::ConversionFailed => {
                record.uuid_state = UuidState::Fetched;
                record.conversion_state = ConversionState::Failed;
            }
        }

        let result = record.clone();
        if records.len() > MAX_RECENT_RECORDS {
            let oldest_terminal = records
                .values()
                .filter(|item| {
                    item.uuid_state == UuidState::Failed
                        || matches!(
                            item.conversion_state,
                            ConversionState::Completed | ConversionState::Failed
                        )
                })
                .min_by_key(|item| item.updated_at)
                .map(|item| item.uuid.clone());
            if let Some(uuid) = oldest_terminal {
                records.remove(&uuid);
            }
        }
        Ok(result)
    }

    pub fn summary(&self, state: Option<&str>, limit: usize) -> WatchSummary {
        let records = self.records.read();
        let mut all: Vec<_> = records.values().cloned().collect();
        all.sort_unstable_by_key(|item| std::cmp::Reverse(item.updated_at));

        let mut summary = WatchSummary {
            total: all.len(),
            ..WatchSummary::default()
        };
        for item in &all {
            match item.uuid_state {
                UuidState::Live => summary.live += 1,
                UuidState::Pending | UuidState::Fetching => summary.pending += 1,
                UuidState::Fetched | UuidState::Failed => {}
            }
            if item.conversion_state == ConversionState::Converting {
                summary.converting += 1;
            }
            if item.conversion_state == ConversionState::Completed {
                summary.completed += 1;
            }
            if item.uuid_state == UuidState::Failed
                || item.conversion_state == ConversionState::Failed
            {
                summary.failed += 1;
            }
        }
        summary.items = all
            .into_iter()
            .filter(|item| state.is_none_or(|value| state_matches(item, value)))
            .take(limit)
            .collect();
        summary
    }
}

fn state_matches(record: &WatchRecord, state: &str) -> bool {
    match state {
        "live" => record.uuid_state == UuidState::Live,
        "pending" => matches!(record.uuid_state, UuidState::Pending | UuidState::Fetching),
        "fetching" => record.uuid_state == UuidState::Fetching,
        "converting" => record.conversion_state == ConversionState::Converting,
        "completed" => record.conversion_state == ConversionState::Completed,
        "failed" => {
            record.uuid_state == UuidState::Failed
                || record.conversion_state == ConversionState::Failed
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(uuid: &str, kind: WatchEventKind) -> WatchEvent {
        WatchEvent {
            uuid: uuid.into(),
            event: kind,
            mode_id: Some(16),
            started_at: None,
            message: None,
            record_id: None,
        }
    }

    #[test]
    fn tracks_uuid_and_conversion_as_separate_states() {
        let registry = WatchRegistry::default();
        registry
            .apply(event("260723-a-b-c-d", WatchEventKind::Live))
            .unwrap();
        registry
            .apply(event("260723-a-b-c-d", WatchEventKind::Pending))
            .unwrap();
        let fetching = registry
            .apply(event("260723-a-b-c-d", WatchEventKind::Fetching))
            .unwrap();
        assert_eq!(fetching.uuid_state, UuidState::Fetching);
        assert_eq!(fetching.conversion_state, ConversionState::Waiting);
        assert_eq!(fetching.attempts, 1);

        registry
            .apply(event("260723-a-b-c-d", WatchEventKind::Converting))
            .unwrap();
        let completed = registry
            .apply(event("260723-a-b-c-d", WatchEventKind::Completed))
            .unwrap();
        assert_eq!(completed.uuid_state, UuidState::Fetched);
        assert_eq!(completed.conversion_state, ConversionState::Completed);
        assert_eq!(registry.summary(None, 10).completed, 1);
    }
}
