//! Majsoul to MJAI conversion.

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::warn;

use super::events::{AnGangAddGangType, ChiPengGangType, GameEvent, parse_record_action};
use super::modes::{mode_metadata, uuid_year};
use super::proto::{GameRecord, decode_game_record};

fn get_dan_name(level_id: u32) -> String {
    match level_id {
        10101 => "初心★1".to_string(),
        10102 => "初心★2".to_string(),
        10103 => "初心★3".to_string(),
        10201 => "雀士★1".to_string(),
        10202 => "雀士★2".to_string(),
        10203 => "雀士★3".to_string(),
        10301 => "雀傑★1".to_string(),
        10302 => "雀傑★2".to_string(),
        10303 => "雀傑★3".to_string(),
        10401 => "雀豪★1".to_string(),
        10402 => "雀豪★2".to_string(),
        10403 => "雀豪★3".to_string(),
        10501 => "雀聖★1".to_string(),
        10502 => "雀聖★2".to_string(),
        10503 => "雀聖★3".to_string(),
        10601 | 20601 => "魂天".to_string(),
        value if (10602..=10620).contains(&value) => format!("魂天Lv{}", value - 10600),
        value if (10701..=10720).contains(&value) => format!("魂天Lv{}", value - 10700),
        20101 => "初心★1".to_string(),
        20102 => "初心★2".to_string(),
        20103 => "初心★3".to_string(),
        20201 => "雀士★1".to_string(),
        20202 => "雀士★2".to_string(),
        20203 => "雀士★3".to_string(),
        20301 => "雀傑★1".to_string(),
        20302 => "雀傑★2".to_string(),
        20303 => "雀傑★3".to_string(),
        20401 => "雀豪★1".to_string(),
        20402 => "雀豪★2".to_string(),
        20403 => "雀豪★3".to_string(),
        20501 => "雀聖★1".to_string(),
        20502 => "雀聖★2".to_string(),
        20503 => "雀聖★3".to_string(),
        value if (20602..=20620).contains(&value) => format!("魂天Lv{}", value - 20600),
        value if (20701..=20720).contains(&value) => format!("魂天Lv{}", value - 20700),
        _ => format!("Rank {}", level_id),
    }
}

struct MajsoulConverter;

#[derive(Debug, Clone)]
pub struct GameMetadata {
    pub mode_id: i32,
    pub room: String,
    pub game_length: String,
    pub players: u8,
    pub year: i32,
}

impl GameMetadata {
    /// What the protobuf says about itself, or `None` for a game that is not a
    /// ranked one — a friend room carries no `mode_id`, and `mode_metadata` has
    /// no row for anything outside the twelve ranked modes.
    ///
    /// This is what lets a record be fetched by uuid alone. Every other caller
    /// has a header to pass, from the live list or from the row being replaced;
    /// a uuid that was never in this deployment's index has neither, and
    /// without these five fields the record lands with an empty `rule` and
    /// disappears from every query that filters on one.
    pub fn of(record: &GameRecord) -> Option<Self> {
        let (room, game_length, players) = mode_metadata(record.mode_id).ok()?;
        Some(Self {
            mode_id: record.mode_id,
            room: room.to_owned(),
            game_length: game_length.to_owned(),
            players,
            year: uuid_year(&record.uuid).ok()?,
        })
    }
}

impl MajsoulConverter {
    /// Convert parsed game events to MJAI JSON events
    fn events_to_mjai(
        &self,
        record: &GameRecord,
        events: &[GameEvent],
        metadata: Option<&GameMetadata>,
    ) -> Result<Vec<Value>> {
        let GameRecord {
            uuid,
            start_time,
            player_names,
            player_accounts,
            result,
            ..
        } = record;
        let (uuid, start_time) = (uuid.as_str(), *start_time);
        let mut mjai_events = Vec::new();
        let num_players = player_names.len();

        // Start game event
        let account_ids: Vec<u64> = player_accounts
            .iter()
            .map(|account| account.account_id)
            .collect();
        let level_ids: Vec<u32> = player_accounts
            .iter()
            .map(|account| account.level_id)
            .collect();
        let level_scores: Vec<i32> = player_accounts
            .iter()
            .map(|account| account.level_score)
            .collect();
        let ranks: Vec<String> = player_accounts
            .iter()
            .map(|account| get_dan_name(account.level_id))
            .collect();
        let mut majsoul = json!({
            "uuid": uuid,
            "start_time": start_time,
            "account_ids": account_ids,
            "level_ids": level_ids,
            "ranks": ranks,
            "level_scores": level_scores,
        });
        if let Some(metadata) = metadata {
            majsoul["mode_id"] = json!(metadata.mode_id);
            majsoul["room"] = json!(metadata.room);
            majsoul["game_length"] = json!(metadata.game_length);
            majsoul["players"] = json!(metadata.players);
            majsoul["year"] = json!(metadata.year);
        }
        mjai_events.push(json!({
            "type": "start_game",
            "names": player_names,
            "majsoul": majsoul,
        }));

        // Track state for reach_accepted
        let mut pending_reach: Option<u32> = None;
        // Track last discarder for ron target calculation
        let mut last_discarder: Option<u32> = None;
        // Indicators already announced this hand. Mahjong Soul reports the
        // whole face-up set on every event that can change it rather than the
        // one it just turned, so a `dora` event is emitted for the tail this
        // has not seen yet. Reset per hand, because the set starts over.
        let mut revealed_doras: Vec<String> = Vec::new();

        for event in events {
            match event {
                GameEvent::NewRound(nr) => {
                    // Emit reach_accepted if pending from previous round
                    pending_reach = None;

                    // Calculate bakaze (round wind)
                    let bakaze = match nr.chang {
                        0 => "E",
                        1 => "S",
                        2 => "W",
                        _ => "N",
                    };

                    // Collect tehais (starting hands)
                    let tehais: Vec<Vec<&str>> = nr
                        .tiles
                        .iter()
                        .take(num_players)
                        .map(|t| t.iter().map(|s| s.as_str()).collect())
                        .collect();

                    mjai_events.push(json!({
                        "type": "start_kyoku",
                        "bakaze": bakaze,
                        "dora_marker": nr.dora_marker,
                        "kyoku": nr.ju + 1,
                        "honba": nr.ben,
                        "kyotaku": nr.liqibang,
                        "oya": nr.ju,
                        "scores": nr.scores,
                        "tehais": tehais,
                    }));

                    // The opening indicator is part of `start_kyoku`, so only
                    // anything beyond it becomes a `dora` event.
                    revealed_doras = nr.doras.clone();
                    for marker in nr.doras.iter().skip(1) {
                        mjai_events.push(json!({
                            "type": "dora",
                            "dora_marker": marker,
                        }));
                    }
                }

                GameEvent::DealTile(dt) => {
                    // If there was a pending reach, emit reach_accepted
                    if let Some(actor) = pending_reach.take() {
                        mjai_events.push(json!({
                            "type": "reach_accepted",
                            "actor": actor,
                        }));
                    }

                    mjai_events.push(json!({
                        "type": "tsumo",
                        "actor": dt.seat,
                        "pai": dt.tile,
                    }));
                    push_new_doras(&mut mjai_events, &mut revealed_doras, &dt.doras);
                }

                GameEvent::DiscardTile(dt) => {
                    // Track last discarder for ron target calculation
                    last_discarder = Some(dt.seat);

                    // Check for riichi declaration
                    if dt.is_liqi || dt.is_wliqi {
                        mjai_events.push(json!({
                            "type": "reach",
                            "actor": dt.seat,
                        }));
                        pending_reach = Some(dt.seat);
                    }

                    mjai_events.push(json!({
                        "type": "dahai",
                        "actor": dt.seat,
                        "pai": dt.tile,
                        "tsumogiri": dt.moqie,
                    }));
                }

                GameEvent::ChiPengGang(cpg) => {
                    // If there was a pending reach, emit reach_accepted
                    if let Some(actor) = pending_reach.take() {
                        mjai_events.push(json!({
                            "type": "reach_accepted",
                            "actor": actor,
                        }));
                    }

                    let target = called_from(&cpg.froms, cpg.seat, last_discarder);

                    match cpg.call_type {
                        ChiPengGangType::Chi => {
                            mjai_events.push(json!({
                                "type": "chi",
                                "actor": cpg.seat,
                                "target": target,
                                "pai": cpg.tiles.last().unwrap_or(&String::new()),
                                "consumed": cpg.tiles.iter().take(cpg.tiles.len().saturating_sub(1)).collect::<Vec<_>>(),
                            }));
                        }
                        ChiPengGangType::Pon => {
                            mjai_events.push(json!({
                                "type": "pon",
                                "actor": cpg.seat,
                                "target": target,
                                "pai": cpg.tiles.last().unwrap_or(&String::new()),
                                "consumed": cpg.tiles.iter().take(cpg.tiles.len().saturating_sub(1)).collect::<Vec<_>>(),
                            }));
                        }
                        ChiPengGangType::Daiminkan => {
                            mjai_events.push(json!({
                                "type": "daiminkan",
                                "actor": cpg.seat,
                                "target": target,
                                "pai": cpg.tiles.last().unwrap_or(&String::new()),
                                "consumed": cpg.tiles.iter().take(cpg.tiles.len().saturating_sub(1)).collect::<Vec<_>>(),
                            }));
                        }
                    }
                }

                GameEvent::AnGangAddGang(ag) => {
                    match ag.gang_type {
                        AnGangAddGangType::Ankan => {
                            let consumed = generate_ankan_tiles(&ag.tiles);
                            mjai_events.push(json!({
                                "type": "ankan",
                                "actor": ag.seat,
                                "consumed": consumed,
                            }));
                        }
                        AnGangAddGangType::Kakan => {
                            // Kakan: only the added tile, the pon is already on the table
                            mjai_events.push(json!({
                                "type": "kakan",
                                "actor": ag.seat,
                                "pai": &ag.tiles,
                            }));
                        }
                    }
                    push_new_doras(&mut mjai_events, &mut revealed_doras, &ag.doras);
                }

                GameEvent::Hule(h) => {
                    // If there was a pending reach, emit reach_accepted
                    if let Some(actor) = pending_reach.take() {
                        mjai_events.push(json!({
                            "type": "reach_accepted",
                            "actor": actor,
                        }));
                    }

                    for hule in &h.hules {
                        // The only difference between a tsumo and a ron is who
                        // the target is; everything else was already identical
                        // in two copies that had to be kept in step by hand.
                        let target = if hule.zimo {
                            hule.seat
                        } else {
                            last_discarder.unwrap_or(0)
                        };
                        // Per-hand when the game says so, and the round's
                        // face-up set otherwise: a double ron reports the same
                        // indicators twice, and one of the two hands may count
                        // a kan dora the other cannot.
                        let dora_markers = if hule.doras.is_empty() {
                            &h.doras
                        } else {
                            &hule.doras
                        };
                        mjai_events.push(json!({
                            "type": "hora",
                            "actor": hule.seat,
                            "target": target,
                            "pai": hule.hu_tile,
                            "hora_tehais": hule.hand,
                            "fu": hule.fu,
                            // Han, or the yakuman multiplier when `yakuman` is
                            // set — Mahjong Soul overloads the one field, and
                            // splitting them here keeps `fan` comparable.
                            "fan": hule.han,
                            "yakuman": hule.yiman,
                            "riichi": hule.riichi,
                            "dora_markers": dora_markers,
                            // The only place these are ever revealed. A record
                            // without them cannot be scored back to its own
                            // point total.
                            "uradora_markers": hule.ura_doras,
                            // mjai's shape, `[name, value]`, with Mahjong Soul's
                            // own names — which are localisation keys and change
                            // with the client, so the stable ids travel beside
                            // them in the same order.
                            "yakus": hule
                                .yakus
                                .iter()
                                .map(|yaku| json!([yaku.name, yaku.val]))
                                .collect::<Vec<_>>(),
                            "yaku_ids": hule.yakus.iter().map(|yaku| yaku.id).collect::<Vec<_>>(),
                            "deltas": h.delta_scores,
                            "scores": h.scores,
                            "majsoul_points": {
                                "ron": hule.point_rong,
                                "tsumo_dealer": hule.point_zimo_qin,
                                "tsumo_non_dealer": hule.point_zimo_xian,
                            },
                        }));
                    }

                    mjai_events.push(json!({
                        "type": "end_kyoku",
                    }));
                }

                GameEvent::NoTile(nt) => {
                    // If there was a pending reach, emit reach_accepted
                    if let Some(actor) = pending_reach.take() {
                        mjai_events.push(json!({
                            "type": "reach_accepted",
                            "actor": actor,
                        }));
                    }

                    mjai_events.push(json!({
                        "type": "ryukyoku",
                        "reason": "fanpai",
                        // Who was tenpai is what decided the payments, and it
                        // is the only place a record says so.
                        "tenpais": nt.tenpai,
                        "scores": nt.scores,
                        "deltas": nt.delta_scores,
                    }));

                    mjai_events.push(json!({
                        "type": "end_kyoku",
                    }));
                }

                GameEvent::LiuJu(lj) => {
                    // If there was a pending reach, emit reach_accepted
                    if let Some(actor) = pending_reach.take() {
                        mjai_events.push(json!({
                            "type": "reach_accepted",
                            "actor": actor,
                        }));
                    }

                    // Abortive draw with reason
                    let reason = match lj.liuju_type {
                        1 => "yao9",   // 9 terminals
                        2 => "reach4", // 4 riichi
                        3 => "kan4",   // 4 kan
                        4 => "kaze4",  // 4 same wind
                        5 => "ron3",   // Triple ron
                        _ => "unknown",
                    };

                    mjai_events.push(json!({
                        "type": "ryukyoku",
                        "reason": reason,
                    }));

                    mjai_events.push(json!({
                        "type": "end_kyoku",
                    }));
                }

                GameEvent::BaBei(bb) => {
                    // North tile (kita) in 3-player mahjong
                    mjai_events.push(json!({
                        "type": "nukidora",
                        "actor": bb.seat,
                        "pai": "N",
                        "tsumogiri": bb.moqie,
                    }));
                    push_new_doras(&mut mjai_events, &mut revealed_doras, &bb.doras);
                }
            }
        }

        // The settlement, which is the only statement of how the game
        // finished: a game ending on an exhaustive draw has no trailing event
        // carrying the closing scores, so `end_game` used to be the empty
        // object and those standings were unrecoverable.
        //
        // `scores` is mjai's own field and holds the raw points; the settled
        // total and the rank points are Mahjong Soul's and sit under its own
        // key rather than pretending to be part of the format.
        mjai_events.push(json!({
            "type": "end_game",
            "scores": result.iter().map(|player| player.part_point_1).collect::<Vec<_>>(),
            "majsoul_result": result
                .iter()
                .map(|player| {
                    json!({
                        "seat": player.seat,
                        "total_point": player.total_point,
                        "part_point_1": player.part_point_1,
                        "part_point_2": player.part_point_2,
                        "grading_score": player.grading_score,
                    })
                })
                .collect::<Vec<_>>(),
        }));

        Ok(mjai_events)
    }
}

/// Generate 4 tiles for ankan, including one red 5 if applicable
fn generate_ankan_tiles(tile: &str) -> Vec<String> {
    // If it's a 5 of a numbered suit, include one red variant
    if tile.len() >= 2 {
        let chars: Vec<char> = tile.chars().collect();
        let num = chars[0];
        let suit = chars[1];

        if num == '5' && (suit == 'm' || suit == 'p' || suit == 's') {
            let regular = format!("5{}", suit);
            let red = format!("5{}r", suit);
            return vec![red, regular.clone(), regular.clone(), regular];
        }
    }
    vec![tile.to_string(); 4]
}

/// Convert raw .pb files from a directory to MJAI format (no database needed)
///
/// Reads .pb files from `input_dir`, converts to gzip MJAI JSONL in `output_dir`,
/// and optionally deletes the .pb files after successful conversion.
/// Which seat a chi, pon or daiminkan took its tile from.
///
/// `froms` says where each tile in `tiles` came from, and the called tile is the
/// last one — which is why `pai` is `tiles.last()`. This used to read the
/// *first* entry, which is one of the caller's own tiles, so every meld in the
/// corpus claimed to have been called off its own maker: 3,667 calls in a
/// 300-game sample, 3,667 of them with `target == actor`.
///
/// A record with no `froms` at all falls back to whoever discarded last, which
/// is who it must have been, and to the caller only if there is no such seat
/// either — a shape that cannot occur in a real record but should not panic.
fn called_from(froms: &[u32], seat: u32, last_discarder: Option<u32>) -> u32 {
    froms
        .last()
        .copied()
        .unwrap_or_else(|| last_discarder.unwrap_or(seat))
}

/// Announces the indicators in `face_up` that have not been announced yet.
///
/// Mahjong Soul reports the whole face-up set on every event that can change it
/// rather than the one tile it just turned, so the difference is what a reader
/// of the event stream needs; sending the set would make a hand with two kans
/// look like it revealed four indicators. Compared by position rather than by
/// value because the same tile can legitimately be turned twice.
fn push_new_doras(
    events: &mut Vec<serde_json::Value>,
    revealed: &mut Vec<String>,
    face_up: &[String],
) {
    for marker in face_up.iter().skip(revealed.len()) {
        events.push(json!({
            "type": "dora",
            "dora_marker": marker,
        }));
    }
    if face_up.len() > revealed.len() {
        *revealed = face_up.to_vec();
    }
}

pub fn convert_raw_files_with_metadata(
    input_dir: &Path,
    output_dir: &Path,
    delete_after: bool,
    metadata: Option<&Path>,
) -> Result<(usize, usize)> {
    fs::create_dir_all(output_dir)?;
    let metadata_map = metadata
        .map(read_metadata_manifest)
        .transpose()?
        .unwrap_or_default();

    // Collect all .pb files
    let pb_files: Vec<PathBuf> = fs::read_dir(input_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "pb"))
        .collect();

    if pb_files.is_empty() {
        tracing::info!("No .pb files found in {}", input_dir.display());
        return Ok((0, 0));
    }

    // Filter out already-converted files
    let pending: Vec<PathBuf> = pb_files
        .into_iter()
        .filter(|p| {
            let stem = p.file_stem().unwrap_or_default().to_string_lossy();
            let mjai_path = output_dir.join(format!("{}.mjson", stem));
            !mjai_path.exists()
        })
        .collect();

    if pending.is_empty() {
        tracing::info!("All files already converted");
        return Ok((0, 0));
    }

    tracing::info!("Converting {} .pb files to MJAI", pending.len());

    let pb = ProgressBar::new(pending.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) ({eta})")?
            .progress_chars("#>-"),
    );

    let success = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    let converter = MajsoulConverter;
    let metadata_map = std::sync::Arc::new(metadata_map);

    pending
        .par_iter()
        .progress_with(pb.clone())
        .for_each(|pb_path| {
            let stem = pb_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let metadata = metadata_map.get(&stem);
            match convert_single_file(&converter, pb_path, output_dir, metadata) {
                Ok(_) => {
                    success.fetch_add(1, Ordering::Relaxed);
                    if delete_after {
                        let _ = fs::remove_file(pb_path);
                    }
                }
                Err(e) => {
                    warn!("Failed to convert {}: {:#}", stem, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

    pb.finish_with_message("Done");

    Ok((
        success.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
    ))
}

/// Pipeline conversion path for very large datasets. Metadata lookups and
/// directory traversal are streamed in bounded batches instead of loading the
/// complete manifest and every filename into memory.
/// Same disk-bounded converter with an optional pipeline-wide progress bar.
/// The caller can count all year/room directories first and keep one visible
/// total instead of showing a new spinner for every directory.
pub fn convert_raw_files_with_database_progress(
    input_dir: &Path,
    output_dir: &Path,
    delete_after: bool,
    database_path: &Path,
    shared_progress: Option<&ProgressBar>,
    status: Option<&super::status::PipelineStatus>,
) -> Result<(usize, usize)> {
    const CONVERT_BATCH_SIZE: usize = 2_048;
    fs::create_dir_all(output_dir)?;
    if !input_dir.exists() {
        tracing::info!("No .pb files found in {}", input_dir.display());
        return Ok((0, 0));
    }
    let connection = Connection::open(database_path).with_context(|| {
        format!(
            "Failed to open discovery metadata database {}",
            database_path.display()
        )
    })?;
    let local_progress = ProgressBar::new_spinner();
    if shared_progress.is_none() {
        local_progress.set_style(ProgressStyle::default_spinner().template(
            "{spinner:.green} Convert [{elapsed_precise}] {pos} converted ({per_sec}) {msg}",
        )?);
    }
    let pb = shared_progress.unwrap_or(&local_progress);
    let converter = MajsoulConverter;
    let mut success = 0usize;
    let mut failed = 0usize;
    let mut batch = Vec::with_capacity(CONVERT_BATCH_SIZE);

    let process_batch = |batch: &mut Vec<(PathBuf, Option<GameMetadata>)>,
                         success: &mut usize,
                         failed: &mut usize| {
        let batch_success = AtomicUsize::new(0);
        let batch_failed = AtomicUsize::new(0);
        batch.par_iter().for_each(|(pb_path, metadata)| {
            match convert_single_file(&converter, pb_path, output_dir, metadata.as_ref()) {
                Ok(_) => {
                    batch_success.fetch_add(1, Ordering::Relaxed);
                    if delete_after {
                        let _ = fs::remove_file(pb_path);
                    }
                    if let Some(status) = status {
                        status.conversion_result(true);
                    }
                }
                Err(error) => {
                    warn!("Failed to convert {}: {:#}", pb_path.display(), error);
                    batch_failed.fetch_add(1, Ordering::Relaxed);
                    if let Some(status) = status {
                        status.conversion_result(false);
                    }
                }
            }
            pb.inc(1);
        });
        *success += batch_success.load(Ordering::Relaxed);
        *failed += batch_failed.load(Ordering::Relaxed);
        batch.clear();
    };

    let mut metadata_statement =
        connection.prepare("SELECT mode, full_uuid FROM games WHERE full_uuid=?1 LIMIT 1")?;
    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|extension| extension != "pb") {
            continue;
        }
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        if output_dir.join(format!("{}.mjson", stem)).exists() {
            continue;
        }
        let row = metadata_statement
            .query_row([stem.as_ref()], |row| {
                Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?;
        let metadata = row
            .map(|(mode, uuid)| -> Result<GameMetadata> {
                let (room, game_length, players) = super::modes::mode_metadata(mode)?;
                Ok(GameMetadata {
                    mode_id: mode,
                    room: room.to_string(),
                    game_length: game_length.to_string(),
                    players,
                    year: super::modes::uuid_year(&uuid)?,
                })
            })
            .transpose()?;
        batch.push((path, metadata));
        if batch.len() >= CONVERT_BATCH_SIZE {
            process_batch(&mut batch, &mut success, &mut failed);
        }
    }
    if !batch.is_empty() {
        process_batch(&mut batch, &mut success, &mut failed);
    }
    if shared_progress.is_none() {
        pb.finish_with_message("Done");
    }
    Ok((success, failed))
}

pub fn count_pending_raw_files(input_dir: &Path, output_dir: &Path) -> Result<usize> {
    if !input_dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|extension| extension != "pb") {
            continue;
        }
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        if !output_dir.join(format!("{}.mjson", stem)).exists() {
            count += 1;
        }
    }
    Ok(count)
}

fn read_metadata_manifest(path: &Path) -> Result<std::collections::HashMap<String, GameMetadata>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read game metadata manifest: {}", path.display()))?;
    let mut result = std::collections::HashMap::new();
    for (line_number, line) in content.lines().enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 6 {
            anyhow::bail!(
                "Invalid game metadata line {} in {}",
                line_number + 1,
                path.display()
            );
        }
        let metadata = GameMetadata {
            mode_id: fields[1]
                .parse()
                .with_context(|| format!("Invalid mode_id on metadata line {}", line_number + 1))?,
            room: fields[2].to_string(),
            game_length: fields[3].to_string(),
            players: fields[4].parse().with_context(|| {
                format!("Invalid player count on metadata line {}", line_number + 1)
            })?,
            year: fields[5]
                .parse()
                .with_context(|| format!("Invalid year on metadata line {}", line_number + 1))?,
        };
        result.insert(fields[0].to_string(), metadata);
    }
    Ok(result)
}

/// Convert a raw ResGameRecord protobuf into gzip-compressed MJAI JSONL bytes.
///
/// Returns the record's own UUID (empty if the record omitted it) alongside
/// the compressed payload, so callers can name the output file and write it
/// wherever they like. Used by both the file-based converter and the live
/// `watch` collector, which never touches disk for the raw bytes.
pub fn convert_record_bytes(
    raw_data: &[u8],
    metadata: Option<&GameMetadata>,
) -> Result<(String, Vec<u8>)> {
    if raw_data.len() < 20 {
        anyhow::bail!("record too small ({} bytes)", raw_data.len());
    }

    let record = decode_game_record(raw_data).context("Failed to decode game record")?;
    if record.records.is_empty() {
        anyhow::bail!("no game records in decoded record");
    }

    let mut events: Vec<GameEvent> = Vec::new();
    for action in &record.records {
        if let Some(event) = parse_record_action(&action.name, &action.data)? {
            events.push(event);
        }
    }

    // What the caller knows beats what the record says, because a caller that
    // passes a header took it from the live list or from the row being
    // replaced and those name the game this deployment already believes it
    // collected. Only where there is no header at all does the protobuf
    // describe itself — which is the case for a record fetched by uuid alone.
    let derived = metadata
        .is_none()
        .then(|| GameMetadata::of(&record))
        .flatten();
    let mjai_events =
        MajsoulConverter.events_to_mjai(&record, &events, metadata.or(derived.as_ref()))?;

    let mut compressed = Vec::new();
    {
        // Large historical archives are CPU-bound here. Fast gzip keeps the
        // same .mjson.gz format while trading a little disk space for much
        // higher conversion throughput.
        let mut encoder = GzEncoder::new(&mut compressed, Compression::fast());
        for event in mjai_events {
            let line = serde_json::to_string(&event)?;
            writeln!(encoder, "{}", line)?;
        }
        encoder.finish()?;
    }
    Ok((record.uuid.clone(), compressed))
}

/// Convert a single .pb file to gzip MJAI JSONL (.mjson)
fn convert_single_file(
    _converter: &MajsoulConverter,
    pb_path: &Path,
    output_dir: &Path,
    metadata: Option<&GameMetadata>,
) -> Result<()> {
    let raw_data = fs::read(pb_path).context("Failed to read .pb file")?;
    let stem = pb_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let (record_uuid, compressed) = convert_record_bytes(&raw_data, metadata)
        .with_context(|| format!("Failed to convert {}", stem))?;

    let output_uuid = if record_uuid.is_empty() {
        stem
    } else {
        record_uuid
    };
    let output_path = output_dir.join(format!("{}.mjson", output_uuid));
    if output_path.exists() {
        return Ok(());
    }
    let temporary_path = output_dir.join(format!(
        ".{}.{}.mjson.tmp",
        output_uuid,
        uuid::Uuid::new_v4()
    ));
    let mut file = File::create(&temporary_path)?;
    file.write_all(&compressed)?;
    file.sync_all()?;
    if let Err(error) = fs::rename(&temporary_path, &output_path) {
        if output_path.exists() {
            // Another parallel conversion of the same UUID won the race.
            // Its atomically published output is authoritative.
            let _ = fs::remove_file(&temporary_path);
            return Ok(());
        }
        let _ = fs::remove_file(&temporary_path);
        return Err(error.into());
    }

    Ok(())
}

/// Convert one downloaded raw file immediately. The output is written
/// atomically; callers may safely delete the .pb only after this returns.
pub fn convert_downloaded_file(
    pb_path: &Path,
    output_dir: &Path,
    metadata: Option<&GameMetadata>,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    convert_single_file(&MajsoulConverter, pb_path, output_dir, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_game_contains_discovery_metadata() {
        let metadata = GameMetadata {
            mode_id: 16,
            room: "throne".to_string(),
            game_length: "south".to_string(),
            players: 4,
            year: 2020,
        };
        let record = GameRecord {
            uuid: "uuid".to_string(),
            start_time: 123,
            mode_id: 0,
            player_names: Vec::new(),
            player_accounts: Vec::new(),
            result: Vec::new(),
            records: Vec::new(),
        };
        let events = MajsoulConverter
            .events_to_mjai(&record, &[], Some(&metadata))
            .unwrap();
        assert_eq!(events[0]["majsoul"]["mode_id"], 16);
        assert_eq!(events[0]["majsoul"]["room"], "throne");
        assert_eq!(events[0]["majsoul"]["game_length"], "south");
        assert_eq!(events[0]["majsoul"]["players"], 4);
        assert_eq!(events[0]["majsoul"]["year"], 2020);
    }

    /// A record fetched by uuid alone has no header to be handed, so the
    /// protobuf has to describe itself — otherwise it lands with an empty
    /// `rule` and vanishes from every query that filters on one.
    #[test]
    fn a_protobuf_describes_itself_when_no_header_is_passed() {
        let bare = |uuid: &str, mode_id: i32| GameRecord {
            uuid: uuid.to_string(),
            start_time: 123,
            mode_id,
            player_names: Vec::new(),
            player_accounts: Vec::new(),
            result: Vec::new(),
            records: Vec::new(),
        };
        // jade east, three players. Read off `head.config.meta.mode_id`.
        let derived =
            GameMetadata::of(&bare("260716-abcdef", 23)).expect("a ranked mode describes itself");
        assert_eq!(derived.room, "jade");
        assert_eq!(derived.game_length, "east");
        assert_eq!(derived.players, 3);
        assert_eq!(derived.year, 2026, "the year comes from the uuid prefix");

        // A friend room or a contest carries no ranked mode, and inventing one
        // would put a game the ladder never saw into `3p-jade-east`.
        assert!(GameMetadata::of(&bare("260716-abcdef", 0)).is_none());
        // So does a uuid with no date prefix to read a year out of.
        assert!(GameMetadata::of(&bare("abc", 23)).is_none());
    }

    /// A pon of `PPP` where the third tile came off seat 3: the caller's own
    /// two tiles are listed first, so the entry that names the discarder is the
    /// last one, the same position `pai` is taken from. Reading the first
    /// instead named the caller every time.
    #[test]
    fn a_call_is_attributed_to_the_seat_the_tile_came_from() {
        assert_eq!(called_from(&[1, 1, 3], 1, Some(3)), 3);
        // A chi can only come from the left, and that is still the last entry
        // rather than the first.
        assert_eq!(called_from(&[2, 2, 1], 2, Some(1)), 1);
        // Nothing in a real record, but it must not panic or invent a seat.
        assert_eq!(called_from(&[], 2, Some(0)), 0);
        assert_eq!(called_from(&[], 2, None), 2);
    }

    /// Mahjong Soul reports the whole face-up set every time it changes, so a
    /// hand with two kans would announce four indicators if the set were
    /// emitted rather than the difference.
    #[test]
    fn only_a_newly_turned_indicator_becomes_an_event() {
        let mut events = Vec::new();
        let mut revealed = vec!["1m".to_string()];

        push_new_doras(&mut events, &mut revealed, &["1m".into()]);
        assert!(events.is_empty(), "the opening indicator is start_kyoku's");

        push_new_doras(&mut events, &mut revealed, &["1m".into(), "2m".into()]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "dora");
        assert_eq!(events[0]["dora_marker"], "2m");

        // The same set again after some unrelated event announces nothing.
        push_new_doras(&mut events, &mut revealed, &["1m".into(), "2m".into()]);
        assert_eq!(events.len(), 1);

        // The same tile can legitimately be turned twice, and the second one is
        // a real reveal — which is why this compares by position, not by value.
        push_new_doras(
            &mut events,
            &mut revealed,
            &["1m".into(), "2m".into(), "2m".into()],
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[1]["dora_marker"], "2m");
    }
}
