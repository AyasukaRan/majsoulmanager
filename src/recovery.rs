use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::{
    catalog::{Catalog, Record},
    mjai,
    pack::{self, PackStore},
};

/// Ingest source stamped on rows the scan rebuilt. The original source lives
/// only in the lost index, and claiming it here would be a guess.
const RECOVERED_SOURCE: &str = "recovered";

/// Re-indexes every pack entry the index does not have. Safe on every boot:
/// entries already indexed are skipped, and a pack whose row count matches its
/// entry count is never read at all, so a start with nothing missing costs one
/// aggregate query plus a 24 byte read per record.
///
/// It walks `PackStore::packs`, which is the legacy corpus and only the legacy
/// corpus — the packs collected before object storage existed, whose local copy
/// is the deployment's only copy. The staging directory the pack worker fills
/// must never be scanned by this: those records are behind an uncommitted Kafka
/// offset, so the broker still owes every one of them, and indexing them here
/// as well would give each record two rows under two different pack keys. They
/// are discarded at boot instead, before this runs.
pub async fn recover(catalog: &Catalog, packs: &PackStore) -> anyhow::Result<usize> {
    // Keyed off the paths, before anything is opened: `pack_key` is derived from
    // the file name, so naming the packs costs a directory listing rather than a
    // scan of each one.
    //
    // Asking only about these is what keeps the query off the boot path's
    // critical list. Unrestricted it grouped every pack the index has ever
    // named — 44,697 of them against the 7 files this walks on the live
    // deployment — and held an exact hash set of every record id while it did.
    let paths = packs.packs()?;
    let keys: Vec<String> = paths
        .iter()
        .filter_map(|path| pack::pack_key(path))
        .collect();
    let indexed: HashMap<String, u64> = catalog.indexed_counts(&keys).await?.into_iter().collect();
    let mut recovered = 0usize;
    // One pack's entries are resident at a time; the whole corpus at once grows
    // with every record ever collected.
    for path in paths {
        let pack = pack::scan_pack(&path)?;
        let entries = pack.entries.len() as u64;
        if indexed.get(&pack.key).copied().unwrap_or(0) >= entries {
            continue;
        }
        let known: HashSet<_> = catalog.indexed_ids(&pack.key).await?.into_iter().collect();
        let received_at = DateTime::<Utc>::from(pack.modified);
        let mut rows = Vec::new();
        for (id, location) in pack.entries {
            if known.contains(&id) {
                continue;
            }
            let raw = match packs.read(&location).await {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(record = %id, pack = pack.key, %error, "skipped an unreadable pack entry");
                    continue;
                }
            };
            let metadata = match mjai::parse_metadata(&raw) {
                Ok(metadata) => metadata,
                Err(error) => {
                    tracing::warn!(record = %id, pack = pack.key, %error, "skipped an unparseable pack entry");
                    continue;
                }
            };
            rows.push(Record {
                id,
                source: RECOVERED_SOURCE.into(),
                sha256: hex::encode(Sha256::digest(&raw)),
                // The pack mtime is the only timestamp the bytes still carry,
                // and it is stable across boots, so a replay lands on the same
                // ReplacingMergeTree key. The cost is that a whole pack
                // collapses to one instant: received_at is also the partition
                // key, so a 256MB pack spanning weeks lands in the partition of
                // its last write and the ordering within it says nothing. Only
                // a per-record timestamp in the pack header would fix that, and
                // only for packs written after the format changed.
                received_at,
                played_at: None,
                players: metadata.players,
                rule: metadata.rule,
                event_count: metadata.event_count,
                // The legacy corpus this walks predates object storage, so it
                // predates keeping the protobuf by much further still. Nothing
                // in these packs has one.
                majsoul_pb: None,
                storage: location,
            });
        }
        // One insert per pack, the same shape the pack worker writes, so a
        // recovered pack is one MergeTree part rather than one per record.
        recovered += rows.len();
        catalog.insert_batch(&rows).await?;
        tracing::info!(pack = pack.key, entries, "re-indexed a pack");
    }
    Ok(recovered)
}
