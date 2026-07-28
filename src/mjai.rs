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
    /// From `majsoul.uuid`: the game's own identity, which is what ingest
    /// deduplicates on whatever source presented the record. Absent for mjai
    /// logs that carry no majsoul header.
    pub majsoul_uuid: Option<String>,
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
    let majsoul = start.get("majsoul");

    // A converted Majsoul record never carries `start_game.rule`, so the rule has to come from the
    // header sitting next to the names instead, as `{players}p-{room}-{game_length}`. That keeps it
    // a filter token rather than a display string, and the twelve ranked modes are the only values
    // it can take. An explicit `rule` still wins: a non-Majsoul mjai log may carry one, and that is
    // what the field originally means. A record converted without the optional game metadata has a
    // header without those three keys, and there we would rather have no rule than a half-formed
    // token, which would be a thirteenth value nobody can filter for in a LowCardinality column.
    let rule = start
        .get("rule")
        .map(|rule| match rule {
            Value::String(value) => value.chars().take(256).collect(),
            other => other.to_string(),
        })
        .or_else(|| {
            let header = majsoul?;
            let players = header.get("players")?.as_u64()?;
            let room = header.get("room")?.as_str()?;
            let length = header.get("game_length")?.as_str()?;
            Some(format!("{players}p-{room}-{length}"))
        });

    // majsoul2mjai merges the source game's unix start_time into the start_game event itself,
    // next to the names this function already reads; both fixtures under tests/fixtures have that
    // shape. Read off that event rather than scanned over all of them, so the cost does not grow
    // with the record and no other event can shadow the header.
    let played_at = majsoul
        .and_then(|majsoul| majsoul.get("start_time"))
        .and_then(Value::as_i64)
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0));

    // Dropped rather than truncated when it is too long, which is the opposite
    // of what the names above do, because this one decides what counts as the
    // same game: two distinct uuids cut to a shared prefix would be one
    // idempotency key, and the second game to arrive would be answered as a
    // duplicate of the first and never stored. Falling back to the caller's own
    // key costs at worst a record that is not deduplicated globally. An empty
    // string is refused for the same reason — it would collapse every record
    // carrying one onto a single claim. A real uuid is 43 characters.
    let majsoul_uuid = majsoul
        .and_then(|majsoul| majsoul.get("uuid"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|uuid| !uuid.is_empty() && uuid.len() <= 128)
        .map(str::to_owned);

    Ok(Metadata {
        players,
        rule,
        event_count: events.len().try_into().unwrap_or(u32::MAX),
        played_at,
        majsoul_uuid,
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
    fn derives_the_rule_from_the_majsoul_header() {
        let raw = br#"{"majsoul":{"uuid":"260716-00000000","start_time":1784211956,"mode_id":24,"room":"jade","game_length":"south","players":3,"account_ids":[0,0,0],"year":2026},"names":["p0","p1","p2"],"type":"start_game"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;
        assert_eq!(
            parse_metadata(raw).unwrap().rule.as_deref(),
            Some("3p-jade-south")
        );

        let four_player = br#"{"majsoul":{"uuid":"260716-00000001","mode_id":15,"room":"throne","game_length":"east","players":4},"names":["p0","p1","p2","p3"],"type":"start_game"}"#;
        assert_eq!(
            parse_metadata(four_player).unwrap().rule.as_deref(),
            Some("4p-throne-east")
        );
    }

    #[test]
    fn leaves_the_rule_empty_when_the_header_lacks_the_mode() {
        // What a record converted without the optional GameMetadata looks like.
        let raw = br#"{"majsoul":{"uuid":"260716-00000000","start_time":1784211956,"account_ids":[0,0,0,0]},"names":["p0","p1","p2","p3"],"type":"start_game"}"#;
        assert_eq!(parse_metadata(raw).unwrap().rule, None);
    }

    #[test]
    fn an_explicit_rule_beats_the_majsoul_header() {
        let raw = br#"{"majsoul":{"room":"jade","game_length":"south","players":3},"names":["p0","p1","p2"],"rule":"tonpu","type":"start_game"}"#;
        assert_eq!(parse_metadata(raw).unwrap().rule.as_deref(), Some("tonpu"));
    }

    /// The uuid is what two ingests of one game are collapsed onto, so a value
    /// that could make two games share one — or every game share one — has to
    /// be refused outright rather than repaired into something plausible.
    #[test]
    fn reads_the_game_uuid_and_refuses_one_that_could_collide() {
        let with = br#"{"majsoul":{"uuid":"260716-00000000-0000-4000-8000-000000000004"},"names":["a","b","c","d"],"type":"start_game"}"#;
        assert_eq!(
            parse_metadata(with).unwrap().majsoul_uuid.as_deref(),
            Some("260716-00000000-0000-4000-8000-000000000004")
        );

        let without = br#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}"#;
        assert_eq!(parse_metadata(without).unwrap().majsoul_uuid, None);

        let empty = br#"{"majsoul":{"uuid":"   "},"names":["a"],"type":"start_game"}"#;
        assert_eq!(parse_metadata(empty).unwrap().majsoul_uuid, None);

        let overlong = format!(
            r#"{{"majsoul":{{"uuid":"{}"}},"names":["a"],"type":"start_game"}}"#,
            "u".repeat(129)
        );
        assert_eq!(
            parse_metadata(overlong.as_bytes()).unwrap().majsoul_uuid,
            None,
            "an oversized uuid must not be cut down to a prefix another game could share"
        );

        let wrong_type = br#"{"majsoul":{"uuid":12345},"names":["a"],"type":"start_game"}"#;
        assert_eq!(parse_metadata(wrong_type).unwrap().majsoul_uuid, None);
    }

    #[test]
    fn rejects_non_event_json() {
        assert_eq!(parse_metadata(br#"{"a":1}"#).unwrap().event_count, 1);
        assert_eq!(parse_metadata(b"[]"), Err(ParseError::NotEvents));
    }
}
