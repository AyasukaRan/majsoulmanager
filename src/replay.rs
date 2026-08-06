//! Per-seat statistics read off an mjai event stream.
//!
//! One game in, one row per seat out. Everything downstream — the index table,
//! the backfill, the API — treats this as the single definition of what a
//! statistic means, so that a number computed while indexing and the same
//! number computed by a backfill of the same record cannot disagree.
//!
//! Two kinds of record arrive here. Records converted before #72 carry only the
//! event skeleton: who drew, who discarded, who won and by how much. Records
//! converted after it also carry the scoring detail Mahjong Soul actually sends
//! — yaku, han, ura dora, who was tenpai at a draw, and the settlement. The
//! counters that need the second kind are gated behind [`GameStats::detailed`]
//! rather than silently reported as zero, because a ratio whose numerator is
//! only ever populated for a tenth of the corpus is worse than no ratio.

use serde_json::Value;

/// Mahjong Soul's own yaku id for 一発. Reading it off the win is exact where
/// reconstructing it from the event order is a rule the converter never stated;
/// the reconstruction is still there for records that predate the yaku list.
const YAKU_IPPATSU: u32 = 30;

/// What one seat did across one game.
///
/// Counters rather than ratios: a ratio cannot be summed across games, and
/// every question this feeds — by mode, by month, by room — is a sum over a
/// filtered set of games with the division done last.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeatStats {
    pub seat: u8,
    pub player: String,

    /// 1 through `seats`. Ties go to the seat closer to the starting dealer,
    /// which is the rule the game itself settles them by.
    pub placement: u8,
    pub final_score: i32,
    /// Mahjong Soul's settled total, uma and oka included. Zero when the record
    /// carries no settlement, which is every record converted before #72.
    pub settled_point: i32,
    /// Rank points gained or lost. Same caveat as `settled_point`.
    pub grading_score: i32,
    pub level_id: u32,
    /// Finished below zero. Counted from the final score rather than from a
    /// mid-game reading, because a seat can go negative and recover.
    pub busted: bool,

    pub hands: u16,
    pub hands_as_dealer: u16,
    /// Longest run of consecutive hands held as dealer, the honba streak.
    pub max_dealer_streak: u16,
    /// Net points across every hand, which is what the raw score moved by —
    /// the riichi sticks this seat put on the table included. See `Hand` for
    /// why that is not simply the sum of the deltas.
    pub net_points: i32,

    pub wins: u16,
    pub wins_tsumo: u16,
    /// What the winning hand was worth, honba included and riichi sticks
    /// excluded — the sticks are won, not scored.
    pub win_points: i32,
    /// Summed turn numbers at the win, so the mean is `win_turns / wins`.
    pub win_turns: u32,
    pub deal_ins: u16,
    /// Positive: what the seat paid. Summing a magnitude keeps the mean a mean.
    pub deal_in_points: i32,

    pub riichi: u16,
    pub riichi_wins: u16,
    pub riichi_deal_ins: u16,
    pub riichi_turns: u32,
    /// Declared before anyone else in the hand.
    pub riichi_first: u16,
    /// Declared after someone else already had.
    pub riichi_chasing: u16,
    /// Someone declared after this seat had.
    pub riichi_chased: u16,
    /// Net points across hands this seat declared riichi in, the 1000 point
    /// stick included because it is part of what riichi costs.
    pub riichi_net: i32,

    /// Hands in which the seat called at least once, and how many of those it
    /// went on to win.
    pub called: u16,
    pub called_wins: u16,

    pub draws: u16,

    // Everything below needs a record converted after #72.
    /// Draws the seat was tenpai at.
    pub draws_tenpai: u16,
    pub riichi_ippatsu: u16,
    /// Riichi wins with at least one ura dora indicator matching.
    pub riichi_ura_hits: u16,
    pub yakuman: u16,
    pub max_han: u16,
}

/// One game's worth of seats, plus what the record was able to say.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GameStats {
    pub seats: u8,
    /// Whether the record carried the scoring detail the last five counters of
    /// [`SeatStats`] need. A caller that mixes both kinds has to divide those
    /// by the number of detailed games, not by the number of games.
    pub detailed: bool,
    pub players: Vec<SeatStats>,
}

/// Reads one game. `None` when the stream is not a game this can score — no
/// `start_game`, no seats, or no hand ever started.
pub fn replay(events: &[Value]) -> Option<GameStats> {
    let start = events
        .iter()
        .find(|event| kind(event) == Some("start_game"))?;
    let names: Vec<String> = start
        .get("names")?
        .as_array()?
        .iter()
        .map(|name| name.as_str().unwrap_or_default().to_owned())
        .collect();
    let seats = names.len();
    if seats < 2 {
        return None;
    }

    let mut players: Vec<SeatStats> = names
        .into_iter()
        .enumerate()
        .map(|(seat, player)| SeatStats {
            seat: seat as u8,
            player,
            ..SeatStats::default()
        })
        .collect();
    for (seat, level) in levels(start).into_iter().enumerate() {
        if let Some(stats) = players.get_mut(seat) {
            stats.level_id = level;
        }
    }

    // A record converted before #72 says nothing about yaku, tenpai at a draw
    // or the settlement, and cannot be told from one where none of those
    // happened to apply — except by whether the fields exist at all.
    let detailed = events.iter().any(|event| match kind(event) {
        Some("hora") => event.get("yaku_ids").is_some(),
        Some("ryukyoku") => event.get("tenpais").is_some(),
        Some("end_game") => event.get("majsoul_result").is_some(),
        _ => false,
    });

    let mut hand = Hand::new(seats);
    let mut started = false;
    let mut streak: Vec<u16> = vec![0; seats];
    let mut last_scores: Option<Vec<i32>> = None;

    for event in events {
        let Some(name) = kind(event) else { continue };
        match name {
            "start_kyoku" => {
                started = true;
                hand = Hand::new(seats);
                hand.dealer = event.get("oya").and_then(Value::as_u64).unwrap_or(0) as usize;
                hand.kyotaku = event.get("kyotaku").and_then(Value::as_i64).unwrap_or(0) as i32;
                if let Some(scores) = int_array(event.get("scores")) {
                    last_scores = Some(scores);
                }
                for (seat, stats) in players.iter_mut().enumerate() {
                    stats.hands += 1;
                    if seat == hand.dealer {
                        stats.hands_as_dealer += 1;
                        streak[seat] += 1;
                        stats.max_dealer_streak = stats.max_dealer_streak.max(streak[seat]);
                    } else {
                        streak[seat] = 0;
                    }
                }
            }
            "tsumo" => {
                if let Some(seat) = seat_of(event, "actor", seats) {
                    hand.turns[seat] += 1;
                }
            }
            "dahai" => {
                if let Some(seat) = seat_of(event, "actor", seats) {
                    // One full go-around has passed for this seat, so whatever
                    // ippatsu it had is spent.
                    if hand.riichi_turn[seat].is_some() {
                        hand.ippatsu[seat] = false;
                    }
                }
            }
            "reach_accepted" => {
                let Some(seat) = seat_of(event, "actor", seats) else {
                    continue;
                };
                if hand.riichi_turn[seat].is_some() {
                    continue;
                }
                let earlier = hand.riichi_turn.iter().filter(|at| at.is_some()).count();
                hand.riichi_turn[seat] = Some(hand.turns[seat]);
                hand.ippatsu[seat] = true;
                players[seat].riichi += 1;
                players[seat].riichi_turns += u32::from(hand.turns[seat]);
                if earlier == 0 {
                    players[seat].riichi_first += 1;
                } else {
                    players[seat].riichi_chasing += 1;
                    for (other, at) in hand.riichi_turn.iter().enumerate() {
                        if other != seat && at.is_some() {
                            players[other].riichi_chased += 1;
                        }
                    }
                }
            }
            "pon" | "chi" | "daiminkan" | "kakan" | "ankan" => {
                if let Some(seat) = seat_of(event, "actor", seats) {
                    hand.called[seat] = true;
                }
                // A call kills every live ippatsu, including the caller's own.
                hand.ippatsu.iter_mut().for_each(|alive| *alive = false);
            }
            "hora" => {
                let Some(actor) = seat_of(event, "actor", seats) else {
                    continue;
                };
                let target = seat_of(event, "target", seats).unwrap_or(actor);
                hand.deltas = int_array(event.get("deltas")).or(hand.deltas.take());
                if let Some(scores) = int_array(event.get("scores")) {
                    last_scores = Some(scores);
                }

                players[actor].wins += 1;
                players[actor].win_turns += u32::from(hand.turns[actor]);
                if target == actor {
                    players[actor].wins_tsumo += 1;
                }
                if hand.called[actor] {
                    players[actor].called_wins += 1;
                }
                if hand.riichi_turn[actor].is_some() {
                    players[actor].riichi_wins += 1;
                    if ippatsu(event, hand.ippatsu[actor]) {
                        players[actor].riichi_ippatsu += 1;
                    }
                    if !array(event.get("uradora_markers")).is_empty() && ura_hit(event) {
                        players[actor].riichi_ura_hits += 1;
                    }
                }
                // The delta a winner is credited with includes every riichi
                // stick it swept off the table — the ones carried in and the
                // ones declared this hand. Those were won, not scored, so the
                // hand's own value is what is left after taking them back out.
                if let Some(gain) = hand.deltas.as_ref().and_then(|d| d.get(actor)) {
                    let sticks = hand.kyotaku
                        + hand.riichi_turn.iter().filter(|at| at.is_some()).count() as i32;
                    players[actor].win_points += *gain - 1_000 * sticks;
                }
                if target != actor {
                    players[target].deal_ins += 1;
                    if hand.riichi_turn[target].is_some() {
                        players[target].riichi_deal_ins += 1;
                    }
                    if let Some(loss) = hand.deltas.as_ref().and_then(|d| d.get(target)) {
                        // Stored as a magnitude so that a mean over deal-ins is
                        // a mean of what was paid, not of a negative number.
                        players[target].deal_in_points += -*loss;
                    }
                }
                if let Some(han) = event.get("fan").and_then(Value::as_u64) {
                    let han = han.min(u64::from(u16::MAX)) as u16;
                    if event.get("yakuman").and_then(Value::as_bool) == Some(true) {
                        players[actor].yakuman += 1;
                    } else {
                        players[actor].max_han = players[actor].max_han.max(han);
                    }
                }
            }
            "ryukyoku" => {
                hand.deltas = int_array(event.get("deltas")).or(hand.deltas.take());
                if let Some(scores) = int_array(event.get("scores")) {
                    last_scores = Some(scores);
                }
                let tenpais = event.get("tenpais").and_then(Value::as_array);
                for (seat, stats) in players.iter_mut().enumerate() {
                    stats.draws += 1;
                    if tenpais
                        .and_then(|list| list.get(seat))
                        .and_then(Value::as_bool)
                        == Some(true)
                    {
                        stats.draws_tenpai += 1;
                    }
                }
            }
            "end_kyoku" => {
                if !started {
                    continue;
                }
                // Once per hand, not once per `hora`: a double ron reports the
                // same delta array on both wins, and adding it twice would
                // invent points nobody gained.
                let deltas = hand.deltas.take().unwrap_or_default();
                for (seat, stats) in players.iter_mut().enumerate() {
                    let declared = hand.riichi_turn[seat].is_some();
                    let movement =
                        deltas.get(seat).copied().unwrap_or(0) - if declared { 1_000 } else { 0 };
                    stats.net_points += movement;
                    if declared {
                        stats.riichi_net += movement;
                    }
                    if hand.called[seat] {
                        stats.called += 1;
                    }
                }
            }
            "end_game" => {
                if let Some(scores) = int_array(event.get("scores")) {
                    if scores.len() == seats {
                        last_scores = Some(scores);
                    }
                }
                for (seat, result) in settlement(event).into_iter().enumerate() {
                    if let Some(stats) = players.get_mut(seat) {
                        stats.settled_point = result.0;
                        stats.grading_score = result.1;
                    }
                }
            }
            _ => {}
        }
    }

    if !started {
        return None;
    }

    let scores = last_scores.unwrap_or_else(|| vec![0; seats]);
    for (seat, stats) in players.iter_mut().enumerate() {
        stats.final_score = scores.get(seat).copied().unwrap_or(0);
        stats.busted = stats.final_score < 0;
    }
    // Higher score first; a tie goes to the seat nearer the starting dealer,
    // which is seat order because seat 0 deals the first hand.
    let mut order: Vec<usize> = (0..seats).collect();
    order.sort_by_key(|seat| (-players[*seat].final_score, *seat));
    for (rank, seat) in order.into_iter().enumerate() {
        players[seat].placement = rank as u8 + 1;
    }

    Some(GameStats {
        seats: seats as u8,
        detailed,
        players,
    })
}

/// Per-hand scratch state. Reset at every `start_kyoku`, which is also what
/// makes a truncated record score the hands it does have rather than nothing.
///
/// The thousand points a riichi costs is the reason this tracks more than the
/// deltas. Mahjong Soul does not put that payment in any `deltas` array: the
/// declaring seat's score drops by it, the winner's delta silently includes
/// every stick it sweeps up, and the two only balance once the declaration is
/// added back by hand. Summing the deltas straight through leaves a game's
/// points not adding up — measured on a real three-player fixture, six riichi
/// and six thousand points that came from nowhere.
struct Hand {
    dealer: usize,
    /// Sticks already on the table when the hand started.
    kyotaku: i32,
    /// Draws taken, which is the turn number this seat is on.
    turns: Vec<u16>,
    /// The turn each seat declared riichi on, if it did.
    riichi_turn: Vec<Option<u16>>,
    /// Whether a riichi's one-shot window is still open.
    ippatsu: Vec<bool>,
    called: Vec<bool>,
    deltas: Option<Vec<i32>>,
}

impl Hand {
    fn new(seats: usize) -> Self {
        Self {
            dealer: 0,
            kyotaku: 0,
            turns: vec![0; seats],
            riichi_turn: vec![None; seats],
            ippatsu: vec![false; seats],
            called: vec![false; seats],
            deltas: None,
        }
    }
}

fn kind(event: &Value) -> Option<&str> {
    event.get("type").and_then(Value::as_str)
}

fn seat_of(event: &Value, field: &str, seats: usize) -> Option<usize> {
    let seat = event.get(field).and_then(Value::as_u64)? as usize;
    (seat < seats).then_some(seat)
}

fn array(value: Option<&Value>) -> &[Value] {
    value.and_then(Value::as_array).map_or(&[], Vec::as_slice)
}

fn int_array(value: Option<&Value>) -> Option<Vec<i32>> {
    let list = value?.as_array()?;
    Some(
        list.iter()
            .map(|entry| entry.as_i64().unwrap_or(0) as i32)
            .collect(),
    )
}

fn levels(start: &Value) -> Vec<u32> {
    array(start.get("majsoul").and_then(|m| m.get("level_ids")))
        .iter()
        .map(|level| level.as_u64().unwrap_or(0) as u32)
        .collect()
}

/// `(settled_point, grading_score)` per seat, seat-ordered.
fn settlement(end_game: &Value) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    for player in array(end_game.get("majsoul_result")) {
        let seat = player.get("seat").and_then(Value::as_u64).unwrap_or(0) as usize;
        if out.len() <= seat {
            out.resize(seat + 1, (0, 0));
        }
        out[seat] = (
            player
                .get("total_point")
                .and_then(Value::as_i64)
                .unwrap_or(0) as i32,
            player
                .get("grading_score")
                .and_then(Value::as_i64)
                .unwrap_or(0) as i32,
        );
    }
    out
}

/// Whether a win was ippatsu, preferring what the game said over what the event
/// order implies. `reconstructed` is the fallback for records that predate the
/// yaku list, where the only evidence is that no call intervened and the seat
/// had not yet discarded again.
fn ippatsu(hora: &Value, reconstructed: bool) -> bool {
    match hora.get("yaku_ids").and_then(Value::as_array) {
        Some(ids) => ids
            .iter()
            .any(|id| id.as_u64() == Some(u64::from(YAKU_IPPATSU))),
        None => reconstructed,
    }
}

/// Whether any ura dora indicator actually matched a tile in the winning hand.
///
/// The indicator points at the tile *after* it in that suit's cycle, so this is
/// the same wrap-around the game applies: 9 wraps to 1, the four winds cycle
/// among themselves and the three dragons among themselves.
fn ura_hit(hora: &Value) -> bool {
    // The winning tile as well as the hand. `hora_tehais` is the concealed part
    // *before* the winning tile is added — measured across 309 real games, it
    // is 13 tiles minus three per meld, every time, and the winning tile is
    // never among them. Counting only those missed every ura dora that landed
    // on the tile the hand was won with: 337 hits over 987 riichi wins became
    // 357, a twentieth of them, and the shortfall was invisible because a
    // "miss" is an ordinary result.
    let hand: Vec<&str> = array(hora.get("hora_tehais"))
        .iter()
        .chain(hora.get("pai"))
        .filter_map(Value::as_str)
        .collect();
    array(hora.get("uradora_markers"))
        .iter()
        .filter_map(Value::as_str)
        .filter_map(dora_from)
        .any(|dora| hand.iter().any(|tile| normalise(tile) == dora))
}

/// The tile an indicator makes a dora, in mjai notation.
fn dora_from(marker: &str) -> Option<String> {
    const WINDS: [&str; 4] = ["E", "S", "W", "N"];
    const DRAGONS: [&str; 3] = ["P", "F", "C"];
    if let Some(index) = WINDS.iter().position(|wind| *wind == marker) {
        return Some(WINDS[(index + 1) % WINDS.len()].to_owned());
    }
    if let Some(index) = DRAGONS.iter().position(|dragon| *dragon == marker) {
        return Some(DRAGONS[(index + 1) % DRAGONS.len()].to_owned());
    }
    let normalised = normalise(marker);
    let (number, suit) = normalised.split_at(1);
    let number: u32 = number.parse().ok()?;
    Some(format!("{}{suit}", number % 9 + 1))
}

/// A red five counts as its own tile for dora purposes: `5mr` is a `5m`.
fn normalise(tile: &str) -> String {
    tile.trim_end_matches('r').to_owned()
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use serde_json::json;

    use super::*;

    fn events(values: Vec<Value>) -> Vec<Value> {
        values
    }

    fn four_player_start() -> Value {
        json!({"type": "start_game", "names": ["p0", "p1", "p2", "p3"]})
    }

    #[test]
    fn a_stream_that_is_not_a_game_scores_nothing() {
        assert!(replay(&[]).is_none());
        assert!(replay(&[json!({"type": "tsumo", "actor": 0})]).is_none());
        // A game that never dealt a hand is not a game either.
        assert!(replay(&[four_player_start(), json!({"type": "end_game"})]).is_none());
    }

    /// The turn a win landed on is the winner's own draw count, and a ron is
    /// attributed to the discarder rather than to the winner.
    #[test]
    fn counts_a_win_a_deal_in_and_the_turn_it_landed_on() {
        let stats = replay(&events(vec![
            four_player_start(),
            json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
            json!({"type": "tsumo", "actor": 1}),
            json!({"type": "dahai", "actor": 1, "pai": "1m"}),
            json!({"type": "tsumo", "actor": 1}),
            json!({"type": "dahai", "actor": 1, "pai": "2m"}),
            json!({"type": "tsumo", "actor": 3}),
            json!({"type": "dahai", "actor": 3, "pai": "3m"}),
            json!({"type": "hora", "actor": 1, "target": 3, "fan": 3,
                   "deltas": [0, 3900, 0, -3900], "scores": [25000, 28900, 25000, 21100]}),
            json!({"type": "end_kyoku"}),
            json!({"type": "end_game"}),
        ]))
        .unwrap();

        assert_eq!(stats.players[1].wins, 1);
        assert_eq!(stats.players[1].wins_tsumo, 0);
        assert_eq!(stats.players[1].win_points, 3_900);
        assert_eq!(stats.players[1].win_turns, 2, "won on its own second draw");
        assert_eq!(stats.players[3].deal_ins, 1);
        assert_eq!(stats.players[3].deal_in_points, 3_900);
        assert_eq!(stats.players[1].placement, 1);
        assert_eq!(stats.players[3].placement, 4);
        assert!(!stats.detailed, "no yaku list, no settlement");
    }

    /// A double ron reports the same delta array on both wins. Adding it once
    /// per `hora` would credit every seat twice and make the net points of a
    /// game disagree with its own final scores.
    #[test]
    fn a_double_ron_moves_the_points_once() {
        let deltas = json!([-8000, 5000, 3000, 0]);
        let stats = replay(&events(vec![
            four_player_start(),
            json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
            json!({"type": "tsumo", "actor": 0}),
            json!({"type": "dahai", "actor": 0, "pai": "1m"}),
            json!({"type": "hora", "actor": 1, "target": 0, "deltas": deltas}),
            json!({"type": "hora", "actor": 2, "target": 0, "deltas": deltas}),
            json!({"type": "end_kyoku"}),
            json!({"type": "end_game"}),
        ]))
        .unwrap();

        assert_eq!(stats.players[0].net_points, -8_000);
        assert_eq!(stats.players[0].deal_ins, 2, "one discard, two winners");
        assert_eq!(stats.players[1].wins, 1);
        assert_eq!(stats.players[2].wins, 1);
    }

    /// Who declared first, who chased, and who was chased. All three come from
    /// the order of `reach_accepted` within one hand.
    #[test]
    fn orders_the_riichi_declarations_within_a_hand() {
        let stats = replay(&events(vec![
            four_player_start(),
            json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
            json!({"type": "tsumo", "actor": 2}),
            json!({"type": "reach", "actor": 2}),
            json!({"type": "dahai", "actor": 2, "pai": "1m"}),
            json!({"type": "reach_accepted", "actor": 2}),
            json!({"type": "tsumo", "actor": 3}),
            json!({"type": "reach", "actor": 3}),
            json!({"type": "dahai", "actor": 3, "pai": "2m"}),
            json!({"type": "reach_accepted", "actor": 3}),
            json!({"type": "ryukyoku", "deltas": [0, 0, 1500, -1500]}),
            json!({"type": "end_kyoku"}),
            json!({"type": "end_game"}),
        ]))
        .unwrap();

        assert_eq!(stats.players[2].riichi_first, 1);
        assert_eq!(stats.players[2].riichi_chased, 1);
        assert_eq!(stats.players[2].riichi_chasing, 0);
        assert_eq!(stats.players[3].riichi_chasing, 1);
        assert_eq!(stats.players[3].riichi_first, 0);
        // The delta says -1500; the thousand the declaration cost is not in
        // it, and the seat is out both.
        assert_eq!(stats.players[3].riichi_net, -2_500);
        assert_eq!(stats.players[2].riichi_net, 500);
        assert_eq!(stats.players[2].draws, 1);
    }

    /// A call ends everyone's one-shot window, including the caller's own.
    #[test]
    fn a_call_kills_the_ippatsu_window() {
        let with_call = |call: bool| {
            let mut stream = vec![
                four_player_start(),
                json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
                json!({"type": "tsumo", "actor": 1}),
                json!({"type": "reach", "actor": 1}),
                json!({"type": "dahai", "actor": 1, "pai": "1m"}),
                json!({"type": "reach_accepted", "actor": 1}),
            ];
            if call {
                stream.push(json!({"type": "pon", "actor": 2, "target": 1, "pai": "1m"}));
                stream.push(json!({"type": "dahai", "actor": 2, "pai": "9p"}));
            }
            stream.push(
                json!({"type": "hora", "actor": 1, "target": 2, "deltas": [0, 5200, -5200, 0]}),
            );
            stream.push(json!({"type": "end_kyoku"}));
            stream.push(json!({"type": "end_game"}));
            replay(&stream).unwrap().players[1].riichi_ippatsu
        };

        assert_eq!(with_call(false), 1);
        assert_eq!(with_call(true), 0);
    }

    /// Where the record says so, the game's own yaku list decides rather than
    /// the reconstruction — the two disagree exactly where the reconstruction
    /// is wrong.
    #[test]
    fn the_yaku_list_overrides_the_reconstructed_ippatsu() {
        let stream = |ids: Value| {
            vec![
                four_player_start(),
                json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
                json!({"type": "tsumo", "actor": 1}),
                json!({"type": "reach", "actor": 1}),
                json!({"type": "dahai", "actor": 1, "pai": "1m"}),
                json!({"type": "reach_accepted", "actor": 1}),
                json!({"type": "hora", "actor": 1, "target": 2, "yaku_ids": ids,
                       "deltas": [0, 5200, -5200, 0]}),
                json!({"type": "end_kyoku"}),
                json!({"type": "end_game"}),
            ]
        };

        assert_eq!(
            replay(&stream(json!([1, 30]))).unwrap().players[1].riichi_ippatsu,
            1
        );
        // The event order says ippatsu; the game says it was not.
        assert_eq!(
            replay(&stream(json!([1]))).unwrap().players[1].riichi_ippatsu,
            0
        );
        assert!(replay(&stream(json!([1]))).unwrap().detailed);
    }

    /// The indicator points at the tile after it, wrapping 9 to 1 and cycling
    /// the honours among themselves.
    #[test]
    fn an_indicator_names_the_tile_after_it() {
        assert_eq!(dora_from("3m").as_deref(), Some("4m"));
        assert_eq!(dora_from("9s").as_deref(), Some("1s"));
        assert_eq!(dora_from("N").as_deref(), Some("E"));
        assert_eq!(dora_from("C").as_deref(), Some("P"));
        // A red five is a five.
        assert_eq!(dora_from("5pr").as_deref(), Some("6p"));
    }

    #[test]
    fn an_ura_indicator_counts_only_when_it_matches_the_hand() {
        let win = |markers: Value| {
            json!({"type": "hora", "actor": 0, "target": 0,
                   "hora_tehais": ["4m", "4m", "5pr", "6p", "7p"],
                   // The winning tile, which the game reports outside the hand.
                   "pai": "2m",
                   "uradora_markers": markers, "yaku_ids": [1],
                   "deltas": [3000, -1000, -1000, -1000]})
        };
        let stream = |markers: Value| {
            vec![
                four_player_start(),
                json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
                json!({"type": "tsumo", "actor": 0}),
                json!({"type": "reach", "actor": 0}),
                json!({"type": "dahai", "actor": 0, "pai": "1m"}),
                json!({"type": "reach_accepted", "actor": 0}),
                win(markers),
                json!({"type": "end_kyoku"}),
                json!({"type": "end_game"}),
            ]
        };

        // 3m makes 4m dora, and the hand holds two.
        assert_eq!(
            replay(&stream(json!(["3m"]))).unwrap().players[0].riichi_ura_hits,
            1
        );
        // 4p makes 5p dora, and `5pr` is a 5p.
        assert_eq!(
            replay(&stream(json!(["4p"]))).unwrap().players[0].riichi_ura_hits,
            1
        );
        assert_eq!(
            replay(&stream(json!(["1s"]))).unwrap().players[0].riichi_ura_hits,
            0
        );
        assert_eq!(
            replay(&stream(json!([]))).unwrap().players[0].riichi_ura_hits,
            0
        );
        // And the tile the hand was won with, which is not in `hora_tehais`:
        // that field is the concealed part before the winning tile is added.
        // Measured across 309 real games, the winning tile is never among them,
        // so counting only the hand missed one hit in twenty — 337 of 987
        // riichi wins scored where 357 should have.
        //
        // `1m` makes `2m` dora, which appears nowhere but the `pai`.
        assert_eq!(
            replay(&stream(json!(["1m"]))).unwrap().players[0].riichi_ura_hits,
            1
        );
    }

    /// The dealer streak is the run of consecutive hands, not the total.
    #[test]
    fn the_dealer_streak_is_the_longest_run() {
        let hand = |oya: u64| {
            vec![
                json!({"type": "start_kyoku", "oya": oya, "scores": [25000, 25000, 25000, 25000]}),
                json!({"type": "ryukyoku", "deltas": [0, 0, 0, 0]}),
                json!({"type": "end_kyoku"}),
            ]
        };
        let mut stream = vec![four_player_start()];
        for oya in [0, 0, 0, 1, 0, 0] {
            stream.extend(hand(oya));
        }
        stream.push(json!({"type": "end_game"}));

        let stats = replay(&stream).unwrap();
        assert_eq!(stats.players[0].hands_as_dealer, 5);
        assert_eq!(stats.players[0].max_dealer_streak, 3);
        assert_eq!(stats.players[0].hands, 6);
        assert_eq!(stats.players[0].draws, 6);
    }

    /// A game that ends on an exhaustive draw has no trailing event carrying
    /// the closing scores, which is why `end_game.scores` matters: without it
    /// the standings come from the draw itself, and before #72 that was `[0]`.
    #[test]
    fn the_settlement_decides_the_final_standings() {
        let stats = replay(&events(vec![
            json!({"type": "start_game", "names": ["p0", "p1", "p2"],
                   "majsoul": {"level_ids": [10501, 10301, 10701]}}),
            json!({"type": "start_kyoku", "oya": 0, "scores": [35000, 35000, 35000]}),
            json!({"type": "ryukyoku", "tenpais": [true, false, true],
                   "deltas": [1500, -3000, 1500], "scores": [36500, 32000, 36500]}),
            json!({"type": "end_kyoku"}),
            json!({"type": "end_game", "scores": [36500, 32000, 36500],
                   "majsoul_result": [
                       {"seat": 0, "total_point": 15000, "grading_score": 60},
                       {"seat": 1, "total_point": -30000, "grading_score": -75},
                       {"seat": 2, "total_point": 15000, "grading_score": 60}]}),
        ]))
        .unwrap();

        assert!(stats.detailed);
        assert_eq!(stats.seats, 3);
        assert_eq!(stats.players[0].final_score, 36_500);
        assert_eq!(stats.players[0].settled_point, 15_000);
        assert_eq!(stats.players[0].grading_score, 60);
        assert_eq!(stats.players[0].level_id, 10501);
        assert_eq!(stats.players[0].draws_tenpai, 1);
        assert_eq!(stats.players[1].draws_tenpai, 0);
        // A tie on score goes to the seat nearer the starting dealer.
        assert_eq!(stats.players[0].placement, 1);
        assert_eq!(stats.players[2].placement, 2);
        assert_eq!(stats.players[1].placement, 3);
    }

    /// The two fixtures are real converted games, one four-player and one
    /// three-player, so this is the whole thing against bytes nobody wrote for
    /// it. The assertions are the invariants a scored game has to satisfy
    /// whatever happened in it — which is what catches a counter that drifts
    /// out of step with the points it is supposed to describe.
    #[test]
    fn scores_the_real_fixtures_without_losing_a_point() {
        for (path, seats) in [
            (
                "tests/fixtures/260716-00000000-0000-4000-8000-000000000003.mjson",
                3,
            ),
            (
                "tests/fixtures/260716-00000000-0000-4000-8000-000000000004.mjson",
                4,
            ),
        ] {
            let compressed = std::fs::read(path).unwrap();
            let mut raw = String::new();
            flate2::read::GzDecoder::new(compressed.as_slice())
                .read_to_string(&mut raw)
                .unwrap();
            let events: Vec<Value> = raw
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();

            let stats = replay(&events).unwrap();
            assert_eq!(stats.seats, seats, "{path}");
            assert_eq!(stats.players.len(), usize::from(seats));

            let hands = events
                .iter()
                .filter(|event| kind(event) == Some("start_kyoku"))
                .count();
            let wins = events
                .iter()
                .filter(|event| kind(event) == Some("hora"))
                .count();

            let mut placements: Vec<u8> = stats.players.iter().map(|p| p.placement).collect();
            placements.sort_unstable();
            assert_eq!(
                placements,
                (1..=seats).collect::<Vec<_>>(),
                "{path}: every seat gets exactly one placement"
            );
            assert_eq!(
                stats
                    .players
                    .iter()
                    .map(|p| usize::from(p.hands))
                    .sum::<usize>(),
                hands * usize::from(seats),
                "{path}: every seat played every hand"
            );
            assert_eq!(
                stats
                    .players
                    .iter()
                    .map(|p| usize::from(p.wins))
                    .sum::<usize>(),
                wins,
                "{path}: every win belongs to exactly one seat"
            );
            // The table is closed: what one seat gained another lost, except
            // for riichi sticks still on the table at the end. This is the
            // assertion that caught the sticks being missing from `deltas`
            // altogether — before that it read +6000 on a game where six riichi
            // were declared.
            let net: i32 = stats.players.iter().map(|p| p.net_points).sum();
            assert!(
                net <= 0 && net % 1_000 == 0,
                "{path}: net points across seats is {net}, which is not a whole \
                 number of riichi sticks left behind"
            );
            let start: i32 = events
                .iter()
                .find(|event| kind(event) == Some("start_kyoku"))
                .and_then(|event| int_array(event.get("scores")))
                .map(|scores| scores.iter().sum())
                .unwrap();
            let finish: i32 = stats.players.iter().map(|p| p.final_score).sum();
            assert_eq!(
                finish,
                start + net,
                "{path}: the final scores do not follow from the hands"
            );
        }
    }

    #[test]
    fn a_seat_that_finished_below_zero_is_busted() {
        let stats = replay(&events(vec![
            four_player_start(),
            json!({"type": "start_kyoku", "oya": 0, "scores": [25000, 25000, 25000, 25000]}),
            json!({"type": "hora", "actor": 0, "target": 3, "deltas": [32000, 0, 0, -32000],
                   "scores": [57000, 25000, 25000, -7000]}),
            json!({"type": "end_kyoku"}),
            json!({"type": "end_game"}),
        ]))
        .unwrap();

        assert!(stats.players[3].busted);
        assert!(!stats.players[0].busted);
        assert_eq!(stats.players[3].placement, 4);
    }
}
