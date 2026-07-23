use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub players: Vec<String>,
    pub rule: Option<String>,
    pub event_count: u32,
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

    Ok(Metadata {
        players,
        rule,
        event_count: events.len().try_into().unwrap_or(u32::MAX),
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
    }

    #[test]
    fn rejects_non_event_json() {
        assert_eq!(parse_metadata(br#"{"a":1}"#).unwrap().event_count, 1);
        assert_eq!(parse_metadata(b"[]"), Err(ParseError::NotEvents));
    }
}
