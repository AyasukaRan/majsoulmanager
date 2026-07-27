use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub players: Vec<String>,
    pub rule: Option<String>,
    pub event_count: u32,
    /// From `majsoul.start_time`; absent for mjai logs that carry no majsoul header.
    pub played_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("mjson must be UTF-8")]
    Utf8,
    #[error("mjson is empty")]
    Empty,
    #[error("invalid JSON: {0}")]
    Json(String),
    #[error("mjson must contain JSON object events")]
    NotEvents,
}

pub fn parse_metadata(payload: &[u8]) -> Result<Metadata, ParseError> {
    let text = std::str::from_utf8(payload).map_err(|_| ParseError::Utf8)?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }

    let events = if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<Value>>(trimmed)
            .map_err(|error| ParseError::Json(error.to_string()))?
    } else {
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .map_err(|error| ParseError::Json(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    if events.is_empty() || events.iter().any(|event| !event.is_object()) {
        return Err(ParseError::NotEvents);
    }

    let start = events
        .iter()
        .find(|event| event.get("type").and_then(Value::as_str) == Some("start_game"))
        .unwrap_or(&events[0]);
    let players = start
        .get("names")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(|name| name.chars().take(128).collect())
                .collect()
        })
        .unwrap_or_default();
    let rule = start.get("rule").map(|rule| match rule {
        Value::String(value) => value.chars().take(256).collect(),
        other => other.to_string(),
    });

    // majsoul2mjai merges the source game's unix start_time into the start_game event itself,
    // next to the names this function already reads; both fixtures under tests/fixtures have that
    // shape. Read off that event rather than scanned over all of them, so the cost does not grow
    // with the record and no other event can shadow the header.
    let played_at = start
        .get("majsoul")
        .and_then(|majsoul| majsoul.get("start_time"))
        .and_then(Value::as_i64)
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0));

    Ok(Metadata {
        players,
        rule,
        event_count: events.len().try_into().unwrap_or(u32::MAX),
        played_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ndjson_metadata() {
        let raw = br#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;
        let metadata = parse_metadata(raw).unwrap();
        assert_eq!(metadata.players, ["a", "b", "c", "d"]);
        assert_eq!(metadata.rule.as_deref(), Some("tonpu"));
        assert_eq!(metadata.event_count, 2);
        assert_eq!(metadata.played_at, None);
    }

    #[test]
    fn reads_played_at_from_the_majsoul_header() {
        let raw = br#"{"majsoul":{"room":"throne","start_time":1784207242,"uuid":"260716-00000000"},"names":["a","b","c","d"],"type":"start_game"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;
        let metadata = parse_metadata(raw).unwrap();
        assert_eq!(
            metadata.played_at,
            Some("2026-07-16T13:07:22Z".parse::<DateTime<Utc>>().unwrap())
        );
    }

    #[test]
    fn reads_played_at_off_the_start_game_event_wherever_it_sits() {
        let raw = br#"{"type":"none"}
{"majsoul":{"start_time":1784207242},"names":["a","b","c","d"],"type":"start_game"}"#;
        let metadata = parse_metadata(raw).unwrap();
        assert_eq!(
            metadata.played_at,
            Some("2026-07-16T13:07:22Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(metadata.players, ["a", "b", "c", "d"]);
    }

    #[test]
    fn rejects_non_event_json() {
        assert_eq!(parse_metadata(br#"{"a":1}"#).unwrap().event_count, 1);
        assert_eq!(parse_metadata(b"[]"), Err(ParseError::NotEvents));
    }
}
