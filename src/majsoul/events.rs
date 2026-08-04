//! Majsoul Record* event decoders.

use anyhow::Result;

use super::proto::{FieldIterator, decode_packed_varints, extract_string, extract_varint};
use super::tiles::tile_str_to_mjai;

/// Decoded RecordNewRound event
#[derive(Debug, Clone)]
pub struct NewRound {
    pub chang: u32,          // Round wind (0=E, 1=S, 2=W)
    pub ju: u32,             // Dealer position (0-3)
    pub ben: u32,            // Honba count
    pub liqibang: u32,       // Riichi sticks on table
    pub dora_marker: String, // First dora indicator, the one the round opens with
    /// Every indicator `doras` (16) carried. Normally just the one above, but
    /// this is the field the game actually sends and taking only its head is
    /// what used to lose every kan dora that a rewind replayed into it.
    pub doras: Vec<String>,
    pub scores: Vec<i32>,        // Starting scores
    pub tiles: Vec<Vec<String>>, // Starting hands (tiles0-tiles3)
}

/// Decoded RecordDealTile event
#[derive(Debug, Clone)]
pub struct DealTile {
    pub seat: u32,
    pub tile: String,
    /// Every dora indicator face up after this draw. A kan is the only way a
    /// new one appears mid-hand, and this is where the game reports it.
    pub doras: Vec<String>,
}

/// Decoded RecordDiscardTile event
#[derive(Debug, Clone)]
pub struct DiscardTile {
    pub seat: u32,
    pub tile: String,
    pub is_liqi: bool,  // Riichi declaration
    pub moqie: bool,    // Tsumogiri
    pub is_wliqi: bool, // Double riichi
}

/// Chi/Pon/Daiminkan type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChiPengGangType {
    Chi = 0,
    Pon = 1,
    Daiminkan = 2,
}

/// Decoded RecordChiPengGang event
#[derive(Debug, Clone)]
pub struct ChiPengGang {
    pub seat: u32,
    pub call_type: ChiPengGangType,
    pub tiles: Vec<String>,
    pub froms: Vec<u32>,
}

/// Ankan/Kakan type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnGangAddGangType {
    Ankan = 2,
    Kakan = 3,
}

/// Decoded RecordAnGangAddGang event
#[derive(Debug, Clone)]
pub struct AnGangAddGang {
    pub seat: u32,
    pub gang_type: AnGangAddGangType,
    pub tiles: String, // Single tile for kakan, representative for ankan
    /// Every dora indicator face up after the kan.
    pub doras: Vec<String>,
}

/// One scoring element of a winning hand: `lq.FanInfo`.
///
/// `id` is Mahjong Soul's own yaku number and is the only stable part — `name`
/// is a localisation key that changes with the client, and `val` is han for a
/// normal hand but a yakuman multiplier for a yakuman, which is why `HuleInfo`
/// carries `yiman` separately instead of leaving that to be inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanInfo {
    pub id: u32,
    pub name: String,
    pub val: u32,
}

/// A single winning hand in RecordHule
#[derive(Debug, Clone)]
pub struct HuleInfo {
    pub seat: u32,
    pub zimo: bool,
    pub hand: Vec<String>,
    pub hu_tile: String,
    pub fu: u32,
    pub point_rong: i32,      // Points from ron
    pub point_zimo_qin: i32,  // Points from dealer tsumo
    pub point_zimo_xian: i32, // Points from non-dealer tsumo
    /// Han, or the yakuman multiplier when `yiman` is set.
    pub han: u32,
    pub yiman: bool,
    pub riichi: bool,
    /// Dora indicators that counted for this hand, kan dora included.
    pub doras: Vec<String>,
    /// Ura dora indicators, which only exist for a riichi hand and are revealed
    /// nowhere else in the record.
    pub ura_doras: Vec<String>,
    pub yakus: Vec<FanInfo>,
}

/// Decoded RecordHule event
#[derive(Debug, Clone)]
pub struct Hule {
    pub hules: Vec<HuleInfo>,
    pub delta_scores: Vec<i32>,
    pub scores: Vec<i32>, // Final scores after this round
    /// Every dora indicator face up at the win, in reveal order.
    pub doras: Vec<String>,
}

/// Decoded RecordNoTile event (exhaustive draw)
#[derive(Debug, Clone)]
pub struct NoTile {
    pub scores: Vec<i32>,
    pub delta_scores: Vec<i32>,
    /// Whether each seat was tenpai, which is what decided the payments above.
    pub tenpai: Vec<bool>,
}

/// Decoded RecordLiuJu event (abortive draw)
#[derive(Debug, Clone)]
pub struct LiuJu {
    pub liuju_type: u32, // 1=9 terminals, 2=4 riichi, 3=4 kan, 4=4 wind, etc.
}

/// Decoded RecordBaBei (north tile declaration in 3-player)
#[derive(Debug, Clone)]
pub struct BaBei {
    pub seat: u32,
    pub moqie: bool,
    /// Every dora indicator face up after the declaration.
    pub doras: Vec<String>,
}

/// All possible game events
#[derive(Debug, Clone)]
pub enum GameEvent {
    NewRound(NewRound),
    DealTile(DealTile),
    DiscardTile(DiscardTile),
    ChiPengGang(ChiPengGang),
    AnGangAddGang(AnGangAddGang),
    Hule(Hule),
    NoTile(NoTile),
    LiuJu(LiuJu),
    BaBei(BaBei),
}

/// Parse RecordNewRound from protobuf data
pub fn parse_new_round(data: &[u8]) -> Result<NewRound> {
    let mut chang = 0u32;
    let mut ju = 0u32;
    let mut ben = 0u32;
    let mut liqibang = 0u32;
    let mut doras: Vec<String> = Vec::new();
    let mut scores = Vec::new();
    let mut tiles: Vec<Vec<String>> = vec![Vec::new(); 4];

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => chang = extract_varint(field.data)? as u32,
            (2, 0) => ju = extract_varint(field.data)? as u32,
            (3, 0) => ben = extract_varint(field.data)? as u32,
            // Field 5: scores (packed repeated int32)
            (5, 2) => scores.extend(decode_packed_varints(field.data)?),
            // Field 5: scores (unpacked individual varint)
            (5, 0) => scores.push(extract_varint(field.data)? as i32),
            // Field 6: liqibang
            (6, 0) => liqibang = extract_varint(field.data)? as u32,
            // Field 7: tiles0 (repeated string)
            (7, 2) => tiles[0].push(tile_str_to_mjai(&extract_string(field.data))?),
            // Field 8: tiles1
            (8, 2) => tiles[1].push(tile_str_to_mjai(&extract_string(field.data))?),
            // Field 9: tiles2
            (9, 2) => tiles[2].push(tile_str_to_mjai(&extract_string(field.data))?),
            // Field 10: tiles3
            (10, 2) => tiles[3].push(tile_str_to_mjai(&extract_string(field.data))?),
            // Field 16: doras (repeated string)
            (16, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            _ => {}
        }
    }

    Ok(NewRound {
        chang,
        ju,
        ben,
        liqibang,
        dora_marker: doras.first().cloned().unwrap_or_default(),
        doras,
        scores,
        tiles,
    })
}

/// Parse RecordDealTile from protobuf data
pub fn parse_deal_tile(data: &[u8]) -> Result<DealTile> {
    let mut seat = 0u32;
    let mut tile = String::new();
    let mut doras = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => seat = extract_varint(field.data)? as u32,
            (2, 2) => tile = tile_str_to_mjai(&extract_string(field.data))?,
            (6, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            _ => {}
        }
    }

    Ok(DealTile { seat, tile, doras })
}

/// Parse RecordDiscardTile from protobuf data
pub fn parse_discard_tile(data: &[u8]) -> Result<DiscardTile> {
    let mut seat = 0u32;
    let mut tile = String::new();
    let mut is_liqi = false;
    let mut moqie = false;
    let mut is_wliqi = false;

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => seat = extract_varint(field.data)? as u32,
            (2, 2) => tile = tile_str_to_mjai(&extract_string(field.data))?,
            (3, 0) => is_liqi = extract_varint(field.data)? != 0,
            (5, 0) => moqie = extract_varint(field.data)? != 0,
            (9, 0) => is_wliqi = extract_varint(field.data)? != 0,
            _ => {}
        }
    }

    Ok(DiscardTile {
        seat,
        tile,
        is_liqi,
        moqie,
        is_wliqi,
    })
}

/// Parse RecordChiPengGang from protobuf data
pub fn parse_chi_peng_gang(data: &[u8]) -> Result<ChiPengGang> {
    let mut seat = 0u32;
    let mut call_type = ChiPengGangType::Chi;
    let mut tiles = Vec::new();
    let mut froms = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => seat = extract_varint(field.data)? as u32,
            (2, 0) => {
                let t = extract_varint(field.data)? as u32;
                call_type = match t {
                    0 => ChiPengGangType::Chi,
                    1 => ChiPengGangType::Pon,
                    2 => ChiPengGangType::Daiminkan,
                    n => {
                        tracing::warn!("Unknown ChiPengGang type: {}, defaulting to Chi", n);
                        ChiPengGangType::Chi
                    }
                };
            }
            (3, 2) => tiles.push(tile_str_to_mjai(&extract_string(field.data))?),
            (4, 2) => {
                for v in decode_packed_varints(field.data)? {
                    froms.push(v as u32);
                }
            }
            (4, 0) => froms.push(extract_varint(field.data)? as u32),
            _ => {}
        }
    }

    Ok(ChiPengGang {
        seat,
        call_type,
        tiles,
        froms,
    })
}

/// Parse RecordAnGangAddGang from protobuf data
pub fn parse_an_gang_add_gang(data: &[u8]) -> Result<AnGangAddGang> {
    let mut seat = 0u32;
    let mut gang_type = AnGangAddGangType::Ankan;
    let mut tiles = String::new();
    let mut doras = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => seat = extract_varint(field.data)? as u32,
            (2, 0) => {
                let t = extract_varint(field.data)? as u32;
                gang_type = match t {
                    3 => AnGangAddGangType::Kakan,
                    2 => AnGangAddGangType::Ankan,
                    n => {
                        tracing::warn!("Unknown AnGangAddGang type: {}, defaulting to Ankan", n);
                        AnGangAddGangType::Ankan
                    }
                };
            }
            (3, 2) => tiles = tile_str_to_mjai(&extract_string(field.data))?,
            (6, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            _ => {}
        }
    }

    Ok(AnGangAddGang {
        seat,
        gang_type,
        tiles,
        doras,
    })
}

/// Parse a single HuleInfo from protobuf data
fn parse_hule_info(data: &[u8]) -> Result<HuleInfo> {
    let mut seat = 0u32;
    let mut zimo = false;
    let mut hand = Vec::new();
    let mut hu_tile = String::new();
    let mut fu = 0u32;
    let mut point_rong = 0i32;
    let mut point_zimo_qin = 0i32;
    let mut point_zimo_xian = 0i32;
    let mut han = 0u32;
    let mut yiman = false;
    let mut riichi = false;
    let mut doras = Vec::new();
    let mut ura_doras = Vec::new();
    let mut yakus = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 2) => hand.push(tile_str_to_mjai(&extract_string(field.data))?),
            (3, 2) => hu_tile = tile_str_to_mjai(&extract_string(field.data))?,
            (4, 0) => seat = extract_varint(field.data)? as u32,
            (5, 0) => zimo = extract_varint(field.data)? != 0,
            (7, 0) => riichi = extract_varint(field.data)? != 0,
            (8, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            (9, 2) => ura_doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            (10, 0) => yiman = extract_varint(field.data)? != 0,
            (11, 0) => han = extract_varint(field.data)? as u32,
            (12, 2) => yakus.push(parse_fan_info(field.data)?),
            (13, 0) => fu = extract_varint(field.data)? as u32,
            (15, 0) => point_rong = extract_varint(field.data)? as i32,
            (16, 0) => point_zimo_qin = extract_varint(field.data)? as i32,
            (17, 0) => point_zimo_xian = extract_varint(field.data)? as i32,
            _ => {}
        }
    }

    Ok(HuleInfo {
        seat,
        zimo,
        hand,
        hu_tile,
        fu,
        point_rong,
        point_zimo_qin,
        point_zimo_xian,
        han,
        yiman,
        riichi,
        doras,
        ura_doras,
        yakus,
    })
}

fn parse_fan_info(data: &[u8]) -> Result<FanInfo> {
    let mut fan = FanInfo {
        id: 0,
        name: String::new(),
        val: 0,
    };
    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 2) => fan.name = extract_string(field.data),
            (2, 0) => fan.val = extract_varint(field.data)? as u32,
            (3, 0) => fan.id = extract_varint(field.data)? as u32,
            _ => {}
        }
    }
    Ok(fan)
}

/// Parse RecordHule from protobuf data
pub fn parse_hule(data: &[u8]) -> Result<Hule> {
    let mut hules = Vec::new();
    let mut delta_scores = Vec::new();
    let mut scores = Vec::new();
    let mut doras = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 2) => hules.push(parse_hule_info(field.data)?),
            (3, 2) => delta_scores.extend(decode_packed_varints(field.data)?),
            (3, 0) => delta_scores.push(extract_varint(field.data)? as i32),
            (5, 2) => scores.extend(decode_packed_varints(field.data)?),
            (5, 0) => scores.push(extract_varint(field.data)? as i32),
            (7, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            _ => {}
        }
    }

    Ok(Hule {
        hules,
        delta_scores,
        scores,
        doras,
    })
}

/// Parse RecordNoTile from protobuf data
/// `RecordNoTile.players` (2) is one `NoTilePlayerInfo` per seat and
/// `RecordNoTile.scores` (3) is one `NoTileScoreInfo` per *payment*, whose
/// `old_scores` (2) and `delta_scores` (3) are each an array over seats.
///
/// This used to read those two arrays as if they were scalars — one value per
/// `NoTileScoreInfo` rather than one per seat — so every exhaustive draw came
/// out as `scores: [0], deltas: [0]` and the tenpai payments were never recorded
/// at all. A draw usually carries exactly one payment, but a 流局満貫 carries a
/// second, so the deltas are summed over all of them rather than taken from the
/// first.
pub fn parse_no_tile(data: &[u8]) -> Result<NoTile> {
    let mut old_scores: Vec<i32> = Vec::new();
    let mut delta_scores: Vec<i32> = Vec::new();
    let mut tenpai = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            // players: NoTilePlayerInfo { tingpai = 1, hand = 2, ... }
            (2, 2) => {
                let mut seat_tenpai = false;
                for inner in FieldIterator::new(field.data) {
                    let inner = inner?;
                    if (inner.number, inner.wire_type) == (1, 0) {
                        seat_tenpai = extract_varint(inner.data)? != 0;
                    }
                }
                tenpai.push(seat_tenpai);
            }
            // scores: NoTileScoreInfo { old_scores = 2, delta_scores = 3, ... }
            (3, 2) => {
                let mut old = Vec::new();
                let mut delta = Vec::new();
                for inner in FieldIterator::new(field.data) {
                    let inner = inner?;
                    match (inner.number, inner.wire_type) {
                        (2, 2) => old.extend(decode_packed_varints(inner.data)?),
                        (2, 0) => old.push(extract_varint(inner.data)? as i32),
                        (3, 2) => delta.extend(decode_packed_varints(inner.data)?),
                        (3, 0) => delta.push(extract_varint(inner.data)? as i32),
                        _ => {}
                    }
                }
                // The first payment's `old_scores` is what the seats held when
                // the hand ended; a later one starts from the previous result,
                // so only the earliest is the round's opening position.
                if old_scores.is_empty() {
                    old_scores = old;
                }
                if delta_scores.len() < delta.len() {
                    delta_scores.resize(delta.len(), 0);
                }
                for (total, one) in delta_scores.iter_mut().zip(delta) {
                    *total += one;
                }
            }
            _ => {}
        }
    }

    let scores = old_scores
        .iter()
        .zip(delta_scores.iter().chain(std::iter::repeat(&0)))
        .map(|(old, delta)| old + delta)
        .collect();

    Ok(NoTile {
        scores,
        delta_scores,
        tenpai,
    })
}

/// Parse RecordLiuJu from protobuf data
pub fn parse_liu_ju(data: &[u8]) -> Result<LiuJu> {
    let mut liuju_type = 0u32;

    for field in FieldIterator::new(data) {
        let field = field?;
        if field.number == 1 && field.wire_type == 0 {
            liuju_type = extract_varint(field.data)? as u32;
        }
    }

    Ok(LiuJu { liuju_type })
}

/// Parse RecordBaBei from protobuf data
pub fn parse_babei(data: &[u8]) -> Result<BaBei> {
    let mut seat = 0u32;
    let mut moqie = false;
    let mut doras = Vec::new();

    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 0) => seat = extract_varint(field.data)? as u32,
            (6, 2) => doras.push(tile_str_to_mjai(&extract_string(field.data))?),
            (8, 0) => moqie = extract_varint(field.data)? != 0,
            _ => {}
        }
    }

    Ok(BaBei { seat, moqie, doras })
}

/// Parse a RecordAction into a GameEvent
pub fn parse_record_action(name: &str, data: &[u8]) -> Result<Option<GameEvent>> {
    let event = match name {
        ".lq.RecordNewRound" => Some(GameEvent::NewRound(parse_new_round(data)?)),
        ".lq.RecordDealTile" => Some(GameEvent::DealTile(parse_deal_tile(data)?)),
        ".lq.RecordDiscardTile" => Some(GameEvent::DiscardTile(parse_discard_tile(data)?)),
        ".lq.RecordChiPengGang" => Some(GameEvent::ChiPengGang(parse_chi_peng_gang(data)?)),
        ".lq.RecordAnGangAddGang" => Some(GameEvent::AnGangAddGang(parse_an_gang_add_gang(data)?)),
        ".lq.RecordHule" => Some(GameEvent::Hule(parse_hule(data)?)),
        ".lq.RecordNoTile" => Some(GameEvent::NoTile(parse_no_tile(data)?)),
        ".lq.RecordLiuJu" => Some(GameEvent::LiuJu(parse_liu_ju(data)?)),
        ".lq.RecordBaBei" => Some(GameEvent::BaBei(parse_babei(data)?)),
        _ => None, // Skip unknown record types
    };
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chi_peng_gang_types() {
        assert_eq!(ChiPengGangType::Chi as u32, 0);
        assert_eq!(ChiPengGangType::Pon as u32, 1);
        assert_eq!(ChiPengGangType::Daiminkan as u32, 2);
    }

    /// Just enough protobuf to state what the game sends, so that these tests
    /// assert against the wire format rather than against whatever shape this
    /// decoder happens to want.
    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn scalar(field: u32, value: i64) -> Vec<u8> {
        let mut out = varint(u64::from(field << 3));
        out.extend(varint(value as u64));
        out
    }

    fn message(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = varint(u64::from((field << 3) | 2));
        out.extend(varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    fn text(field: u32, value: &str) -> Vec<u8> {
        message(field, value.as_bytes())
    }

    fn repeated(field: u32, values: &[i64]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| scalar(field, *value))
            .collect()
    }

    /// The payments at an exhaustive draw are one number per seat, and the
    /// decoder used to read them as one number per *payment* — taking the head
    /// of each array and throwing the rest away. Every draw in the corpus came
    /// out as `scores: [0], deltas: [0]`, so no tenpai payment was ever
    /// recorded, and a game that ended on a draw had no final standings at all.
    #[test]
    fn an_exhaustive_draw_pays_every_seat_and_says_who_was_tenpai() {
        let mut players = Vec::new();
        for tenpai in [true, false, true, false] {
            players.extend(message(2, &scalar(1, i64::from(tenpai))));
        }
        let mut payment = repeated(2, &[25_000, 25_000, 25_000, 25_000]);
        payment.extend(repeated(3, &[1_500, -1_500, 1_500, -1_500]));
        let mut record = players;
        record.extend(message(3, &payment));

        let draw = parse_no_tile(&record).unwrap();

        assert_eq!(draw.tenpai, vec![true, false, true, false]);
        assert_eq!(draw.delta_scores, vec![1_500, -1_500, 1_500, -1_500]);
        assert_eq!(draw.scores, vec![26_500, 23_500, 26_500, 23_500]);
    }

    /// 流局満貫 is a second payment on the same draw. Summing rather than
    /// keeping the first is what makes the deltas add up to what the seats
    /// actually gained.
    #[test]
    fn a_draw_with_two_payments_sums_them() {
        let mut record = message(3, &{
            let mut payment = repeated(2, &[25_000, 25_000]);
            payment.extend(repeated(3, &[1_000, -1_000]));
            payment
        });
        record.extend(message(3, &repeated(3, &[8_000, -8_000])));

        let draw = parse_no_tile(&record).unwrap();

        assert_eq!(draw.delta_scores, vec![9_000, -9_000]);
        assert_eq!(draw.scores, vec![34_000, 16_000]);
    }

    /// The yaku list, the han count and the ura dora indicators. The last of
    /// those is revealed nowhere else in a record: without it a riichi hand
    /// cannot be scored back to its own point total, and it was being dropped
    /// along with everything else this asserts.
    #[test]
    fn a_win_carries_its_yaku_its_han_and_its_ura_dora() {
        let mut hand = text(1, "1m");
        hand.extend(text(3, "2m")); // hu_tile
        hand.extend(scalar(4, 2)); // seat
        hand.extend(scalar(5, 1)); // zimo
        hand.extend(scalar(7, 1)); // liqi
        hand.extend(text(8, "3m")); // dora indicator
        hand.extend(text(9, "4m")); // ura dora indicator
        hand.extend(scalar(10, 0)); // not a yakuman
        hand.extend(scalar(11, 5)); // han
        hand.extend(message(12, &{
            let mut fan = text(1, "riichi");
            fan.extend(scalar(2, 1));
            fan.extend(scalar(3, 2));
            fan
        }));
        hand.extend(scalar(13, 40)); // fu

        let hule = parse_hule(&message(1, &hand)).unwrap();
        let win = &hule.hules[0];

        assert_eq!(win.seat, 2);
        assert!(win.zimo);
        assert!(win.riichi);
        assert!(!win.yiman);
        assert_eq!(win.han, 5);
        assert_eq!(win.fu, 40);
        assert_eq!(win.doras, vec!["3m"]);
        assert_eq!(win.ura_doras, vec!["4m"]);
        assert_eq!(
            win.yakus,
            vec![FanInfo {
                id: 2,
                name: "riichi".into(),
                val: 1,
            }]
        );
    }

    /// A kan turns a new indicator, and the game reports it on the replacement
    /// draw. Nothing read that field, so no record ever mentioned a kan dora.
    #[test]
    fn a_replacement_draw_carries_the_indicator_the_kan_turned() {
        let mut record = scalar(1, 3);
        record.extend(text(2, "5p"));
        record.extend(text(6, "1z"));
        record.extend(text(6, "2z"));

        let draw = parse_deal_tile(&record).unwrap();

        assert_eq!(draw.seat, 3);
        assert_eq!(draw.tile, "5p");
        assert_eq!(draw.doras, vec!["E", "S"]);
    }
}
