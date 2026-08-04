//! Manual protobuf decoder for Majsoul game records.

use anyhow::Result;

/// Decode a varint from buffer, return (value, bytes_consumed)
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut pos = 0;
    loop {
        if pos >= buf.len() {
            anyhow::bail!("Unexpected end in varint");
        }
        let byte = buf[pos];
        pos += 1;
        let part = (byte & 0x7f) as u64;
        // Reject non-canonical varints that overflow u64 instead of
        // panicking (debug) or silently wrapping the shift (release).
        if shift >= 64 || part > (u64::MAX >> shift) {
            anyhow::bail!("Varint too long");
        }
        value |= part << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok((value, pos))
}

/// Parse a protobuf message, extracting fields by number
pub struct FieldIterator<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> FieldIterator<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
}

#[derive(Debug)]
pub struct Field<'a> {
    pub number: u32,
    pub wire_type: u8,
    pub data: &'a [u8],
}

impl<'a> Iterator for FieldIterator<'a> {
    type Item = Result<Field<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }

        // Read tag as varint (not just single byte for larger field numbers)
        let (tag, tag_len) = match decode_varint(&self.buf[self.pos..]) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        self.pos += tag_len;

        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;

        let data_start = self.pos;
        let data = match wire_type {
            0 => {
                // Varint
                match decode_varint(&self.buf[self.pos..]) {
                    Ok((_, n)) => {
                        self.pos += n;
                        &self.buf[data_start..self.pos]
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
            1 => {
                // Fixed64
                if self.pos + 8 > self.buf.len() {
                    return Some(Err(anyhow::anyhow!("Buffer overflow in fixed64")));
                }
                self.pos += 8;
                &self.buf[data_start..self.pos]
            }
            2 => {
                // Length-delimited
                match decode_varint(&self.buf[self.pos..]) {
                    Ok((len, n)) => {
                        self.pos += n;
                        let end = self.pos + len as usize;
                        if end > self.buf.len() {
                            return Some(Err(anyhow::anyhow!(
                                "Buffer overflow in length-delimited"
                            )));
                        }
                        let data = &self.buf[self.pos..end];
                        self.pos = end;
                        data
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
            5 => {
                // Fixed32
                if self.pos + 4 > self.buf.len() {
                    return Some(Err(anyhow::anyhow!("Buffer overflow in fixed32")));
                }
                self.pos += 4;
                &self.buf[data_start..self.pos]
            }
            _ => {
                return Some(Err(anyhow::anyhow!("Unknown wire type: {}", wire_type)));
            }
        };

        Some(Ok(Field {
            number: field_number,
            wire_type,
            data,
        }))
    }
}

/// Extract a string field from length-delimited data
pub fn extract_string(data: &[u8]) -> String {
    String::from_utf8_lossy(data).to_string()
}

/// Extract a varint as u64 from varint-encoded data
pub fn extract_varint(data: &[u8]) -> Result<u64> {
    let (val, _) = decode_varint(data)?;
    Ok(val)
}

/// Decode packed repeated varints from length-delimited data
pub fn decode_packed_varints(data: &[u8]) -> Result<Vec<i32>> {
    let mut result = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (val, n) = decode_varint(&data[pos..])?;
        result.push(val as i32);
        pos += n;
    }
    Ok(result)
}

/// Decode Wrapper message: {name: string (field 1), data: bytes (field 2)}
pub fn decode_wrapper(buf: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut name = String::new();
    let mut data = Vec::new();

    for field in FieldIterator::new(buf) {
        let field = field?;
        match field.number {
            1 if field.wire_type == 2 => name = extract_string(field.data),
            2 if field.wire_type == 2 => data = field.data.to_vec(),
            _ => {}
        }
    }

    Ok((name, data))
}

/// A single game action (decoded from Wrapper)
#[derive(Debug, Clone)]
pub struct RecordAction {
    pub name: String,
    pub data: Vec<u8>,
}

/// Decoded ResGameRecord
#[derive(Debug)]
pub struct GameRecord {
    pub uuid: String,
    pub start_time: u32,
    pub player_names: Vec<String>,
    pub player_accounts: Vec<PlayerAccountMeta>,
    /// The settlement, seat-ordered. Empty for a record whose head carries no
    /// `GameEndResult` — an abandoned game, or a shape this decoder has not
    /// seen — which is why the conversion treats it as optional rather than
    /// assuming four entries.
    pub result: Vec<PlayerResult>,
    pub records: Vec<RecordAction>,
}

/// One seat's settlement: `lq.GameEndResult.PlayerItem`.
///
/// This is the only authoritative statement of how a game finished. The event
/// stream cannot substitute for it: a game that ends on an exhaustive draw has
/// no trailing event carrying the closing scores, so without this its final
/// standings are simply not recoverable.
#[derive(Debug, Clone, Default)]
pub struct PlayerResult {
    pub seat: u32,
    /// Settled points, uma and oka included, in Mahjong Soul's own scale.
    pub total_point: i32,
    /// Raw points at the end of the last hand.
    pub part_point_1: i32,
    pub part_point_2: i32,
    /// Rank points gained or lost, which is what moves the account's level.
    pub grading_score: i32,
}

fn decode_player_result(data: &[u8]) -> Result<PlayerResult> {
    let mut result = PlayerResult::default();
    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => result.seat = extract_varint(field.data)? as u32,
            (2, 0) => result.total_point = extract_varint(field.data)? as i32,
            (3, 0) => result.part_point_1 = extract_varint(field.data)? as i32,
            (4, 0) => result.part_point_2 = extract_varint(field.data)? as i32,
            (5, 0) => result.grading_score = extract_varint(field.data)? as i32,
            _ => {}
        }
    }
    Ok(result)
}

#[derive(Debug, Clone, Default)]
pub struct PlayerAccountMeta {
    pub account_id: u64,
    pub seat: u32,
    pub nickname: String,
    pub level_id: u32,
    pub level_score: i32,
}

fn decode_player_account(data: &[u8]) -> Result<PlayerAccountMeta> {
    let mut account = PlayerAccountMeta::default();
    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => account.account_id = extract_varint(field.data)?,
            (2, 0) => account.seat = extract_varint(field.data)? as u32,
            (3, 2) => account.nickname = extract_string(field.data),
            (7, 2) => {
                for level_field in FieldIterator::new(field.data) {
                    let level_field = level_field?;
                    match (level_field.number, level_field.wire_type) {
                        (1, 0) => account.level_id = extract_varint(level_field.data)? as u32,
                        (2, 0) => {
                            account.level_score = extract_varint(level_field.data)? as u32 as i32
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(account)
}

/// Decode ResGameRecord from raw protobuf bytes
pub fn decode_game_record(raw: &[u8]) -> Result<GameRecord> {
    let mut uuid = String::new();
    let mut start_time = 0u32;
    let mut player_accounts = Vec::new();
    let mut result = Vec::new();
    let mut compressed_data: Option<Vec<u8>> = None;

    for field in FieldIterator::new(raw) {
        let field = field?;
        match field.number {
            1 if field.wire_type == 2
                // Error message - check if non-empty
                && !field.data.is_empty() =>
            {
                for inner in FieldIterator::new(field.data) {
                    let inner = inner?;
                    if inner.number == 1 && inner.wire_type == 0 {
                        let code = extract_varint(inner.data)?;
                        if code != 0 {
                            anyhow::bail!("Game record error code: {}", code);
                        }
                    }
                }
            }
            3 if field.wire_type == 2 => {
                // head (GameRecordHeader)
                for inner in FieldIterator::new(field.data) {
                    let inner = inner?;
                    match inner.number {
                        1 if inner.wire_type == 2 => uuid = extract_string(inner.data),
                        2 if inner.wire_type == 0 => {
                            start_time = extract_varint(inner.data)? as u32
                        }
                        11 if inner.wire_type == 2 => {
                            player_accounts.push(decode_player_account(inner.data)?);
                        }
                        // result (GameEndResult), whose only field is a
                        // repeated PlayerItem.
                        12 if inner.wire_type == 2 => {
                            for item in FieldIterator::new(inner.data) {
                                let item = item?;
                                if (item.number, item.wire_type) == (1, 2) {
                                    result.push(decode_player_result(item.data)?);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            4 if field.wire_type == 2 => {
                // data (compressed GameDetailRecords)
                compressed_data = Some(field.data.to_vec());
            }
            5 if field.wire_type == 2 => {
                // data_url (string) - alternative way to get data
                // We skip this for now, as we expect inline data
            }
            _ => {}
        }
    }

    // Decompress and decode records
    let records = if let Some(data_bytes) = compressed_data {
        // Field 4 (data) is a Wrapper message: {name: string, data: bytes}
        // The wrapper's data contains GameDetailRecords
        let (_wrapper_name, wrapper_data) = decode_wrapper(&data_bytes)?;

        // Parse GameDetailRecords: {records: repeated bytes (field 1),
        //                           version: uint32 (field 2),
        //                           actions: repeated GameAction (field 3)}
        let mut version = 0u32;
        let mut raw_records: Vec<Vec<u8>> = Vec::new();
        let mut actions_data: Vec<Vec<u8>> = Vec::new();

        for field in FieldIterator::new(&wrapper_data) {
            let field = field?;
            match (field.number, field.wire_type) {
                (1, 2) => raw_records.push(field.data.to_vec()),
                (2, 0) => version = extract_varint(field.data)? as u32,
                (3, 2) => actions_data.push(field.data.to_vec()),
                _ => {}
            }
        }

        if version < 210715 && !raw_records.is_empty() {
            // Old format: each record is a Wrapper (or gzipped Wrapper list)
            decode_old_format_records(&raw_records)?
        } else if !actions_data.is_empty() {
            // New format: each action has a `result` field (field 3) which is a Wrapper
            decode_new_format_actions(&actions_data)?
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    player_accounts.sort_by_key(|account| account.seat);
    let player_names = player_accounts
        .iter()
        .map(|account| account.nickname.clone())
        .collect();

    result.sort_by_key(|player| player.seat);

    Ok(GameRecord {
        uuid,
        start_time,
        player_names,
        player_accounts,
        result,
        records,
    })
}

/// Decode old-format GameDetailRecords (version < 210715)
/// Each record in the list is a serialized Wrapper message
fn decode_old_format_records(raw_records: &[Vec<u8>]) -> Result<Vec<RecordAction>> {
    let mut records = Vec::new();
    for raw in raw_records {
        // Each record bytes is a Wrapper {name: string, data: bytes}
        let (name, data) = decode_wrapper(raw)?;
        if !name.is_empty() {
            records.push(RecordAction { name, data });
        }
    }
    Ok(records)
}

/// Decode new-format GameDetailRecords (version >= 210715)
/// Each action is a GameAction message with `result` field containing a Wrapper
fn decode_new_format_actions(actions_data: &[Vec<u8>]) -> Result<Vec<RecordAction>> {
    let mut records = Vec::new();
    for action_bytes in actions_data {
        // GameAction: {type: uint32 (field 1), result: bytes (field 3)}
        let mut result_data: Option<Vec<u8>> = None;
        for field in FieldIterator::new(action_bytes) {
            let field = field?;
            if field.number == 3 && field.wire_type == 2 {
                result_data = Some(field.data.to_vec());
            }
        }

        if let Some(result) = result_data {
            if !result.is_empty() {
                let (name, data) = decode_wrapper(&result)?;
                if !name.is_empty() {
                    records.push(RecordAction { name, data });
                }
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        assert_eq!(decode_varint(&[0x00]).unwrap(), (0, 1));
        assert_eq!(decode_varint(&[0x01]).unwrap(), (1, 1));
        assert_eq!(decode_varint(&[0x7f]).unwrap(), (127, 1));
        assert_eq!(decode_varint(&[0x80, 0x01]).unwrap(), (128, 2));
        assert_eq!(decode_varint(&[0xac, 0x02]).unwrap(), (300, 2));
    }

    #[test]
    fn test_decode_wrapper() {
        // Field 1 (string "test"): 0a 04 t e s t
        // Field 2 (bytes [1,2,3]): 12 03 01 02 03
        let buf = vec![
            0x0a, 0x04, b't', b'e', b's', b't', 0x12, 0x03, 0x01, 0x02, 0x03,
        ];
        let (name, data) = decode_wrapper(&buf).unwrap();
        assert_eq!(name, "test");
        assert_eq!(data, vec![1, 2, 3]);
    }
}
