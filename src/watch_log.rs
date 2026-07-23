use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use uuid::Uuid;

pub const WATCH_LOG_CAPACITY: usize = 500;
const MAX_MESSAGE_CHARS: usize = 2048;
// 过短的机密(如空串、单字符)脱敏会把正常文本替换得面目全非。
const MIN_SECRET_CHARS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct WatchLogEntry {
    pub seq: u64,
    pub timestamp: DateTime<Utc>,
    pub level: WatchLogLevel,
    pub source: String,
    pub message: String,
}

pub struct WatchLogBuffer {
    entries: RwLock<VecDeque<WatchLogEntry>>,
    next_seq: AtomicU64,
    boot_id: Uuid,
    secrets: RwLock<Vec<String>>,
}

impl Default for WatchLogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchLogBuffer {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(WATCH_LOG_CAPACITY)),
            next_seq: AtomicU64::new(1),
            boot_id: Uuid::new_v4(),
            secrets: RwLock::new(Vec::new()),
        }
    }

    /// 缓冲随进程存活,boot_id 让客户端察觉后端重启并重置游标。
    pub fn boot_id(&self) -> Uuid {
        self.boot_id
    }

    /// 注册需要从日志里抹掉的机密(账号密码、代理凭据等)。模块 stderr
    /// 与错误链都可能回显它们,统一在 append 时替换成 ***。
    pub fn register_secret(&self, secret: impl Into<String>) {
        let secret = secret.into();
        if secret.chars().count() < MIN_SECRET_CHARS {
            return;
        }
        let mut secrets = self.secrets.write();
        if !secrets.iter().any(|known| known == &secret) {
            secrets.push(secret);
        }
    }

    pub fn append(&self, level: WatchLogLevel, source: &str, message: impl Into<String>) {
        let mut message = message.into();
        {
            let secrets = self.secrets.read();
            for secret in secrets.iter() {
                if message.contains(secret.as_str()) {
                    message = message.replace(secret.as_str(), "***");
                }
            }
        }
        if let Some((index, _)) = message.char_indices().nth(MAX_MESSAGE_CHARS) {
            message.truncate(index);
        }
        let mut entries = self.entries.write();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        if entries.len() == WATCH_LOG_CAPACITY {
            entries.pop_front();
        }
        entries.push_back(WatchLogEntry {
            seq,
            timestamp: Utc::now(),
            level,
            source: source.to_owned(),
            message,
        });
    }

    pub fn entries_after(&self, after: u64, limit: usize) -> Vec<WatchLogEntry> {
        let entries = self.entries.read();
        let start = entries.partition_point(|entry| entry.seq <= after);
        entries.iter().skip(start).take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_entries_with_ascending_sequences() {
        let buffer = WatchLogBuffer::new();
        buffer.append(WatchLogLevel::Info, "service", "第一条");
        buffer.append(WatchLogLevel::Warn, "collector", "第二条");
        let entries = buffer.entries_after(0, 10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[0].source, "service");
        assert_eq!(entries[0].message, "第一条");
        assert_eq!(entries[1].seq, 2);
        assert_eq!(entries[1].level, WatchLogLevel::Warn);
    }

    #[test]
    fn evicts_oldest_entries_beyond_capacity() {
        let buffer = WatchLogBuffer::new();
        for index in 0..WATCH_LOG_CAPACITY + 10 {
            buffer.append(WatchLogLevel::Debug, "service", format!("line {index}"));
        }
        let entries = buffer.entries_after(0, WATCH_LOG_CAPACITY + 10);
        assert_eq!(entries.len(), WATCH_LOG_CAPACITY);
        assert_eq!(entries.first().unwrap().seq, 11);
        assert_eq!(
            entries.last().unwrap().seq,
            (WATCH_LOG_CAPACITY + 10) as u64
        );
    }

    #[test]
    fn filters_entries_after_cursor_and_applies_limit() {
        let buffer = WatchLogBuffer::new();
        for index in 0..10 {
            buffer.append(WatchLogLevel::Info, "service", format!("line {index}"));
        }
        let entries = buffer.entries_after(4, 3);
        assert_eq!(
            entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
        assert!(buffer.entries_after(10, 3).is_empty());
    }

    #[test]
    fn redacts_registered_secrets_from_messages() {
        let buffer = WatchLogBuffer::new();
        buffer.register_secret("p@ssw0rd!");
        buffer.register_secret("abc"); // 短于阈值,忽略
        buffer.append(
            WatchLogLevel::Info,
            "module:login",
            r#"request {"password":"p@ssw0rd!","username":"tester-abc"}"#,
        );
        let entries = buffer.entries_after(0, 1);
        assert_eq!(
            entries[0].message,
            r#"request {"password":"***","username":"tester-abc"}"#
        );
    }

    #[test]
    fn truncates_long_messages_to_the_character_limit() {
        let buffer = WatchLogBuffer::new();
        buffer.append(WatchLogLevel::Error, "module:test", "牌".repeat(3000));
        let entries = buffer.entries_after(0, 1);
        assert_eq!(entries[0].message.chars().count(), MAX_MESSAGE_CHARS);
        assert!(
            entries[0]
                .message
                .chars()
                .all(|character| character == '牌')
        );
    }
}
