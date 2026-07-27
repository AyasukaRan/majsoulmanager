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
pub async fn recover(catalog: &Catalog, packs: &PackStore) -> anyhow::Result<usize> {
    let indexed: HashMap<String, u64> = catalog.indexed_counts().await?.into_iter().collect();
    let mut recovered = 0usize;
    // One pack's entries are resident at a time; the whole corpus at once grows
    // with every record ever collected.
    for path in packs.packs()? {
        let pack = pack::scan_pack(&path)?;
        let entries = pack.entries.len() as u64;
        if indexed.get(&pack.key).copied().unwrap_or(0) >= entries {
            continue;
        }
        let known: HashSet<_> = catalog.indexed_ids(&pack.key).await?.into_iter().collect();
        let received_at = DateTime::<Utc>::from(pack.modified);
        for (id, location) in pack.entries {
            if known.contains(&id) {
                continue;
            }
            let raw = match packs.read(&location) {
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
            catalog
                .insert(Record {
                    id,
                    source: RECOVERED_SOURCE.into(),
                    sha256: hex::encode(Sha256::digest(&raw)),
                    // The pack mtime is the only timestamp the bytes still
                    // carry, and it is stable across boots, so a replay lands
                    // on the same ReplacingMergeTree key. The cost is that a
                    // whole pack collapses to one instant: received_at is also
                    // the partition key, so a 256MB pack spanning weeks lands
                    // in the partition of its last write and the ordering
                    // within it says nothing. Only a per-record timestamp in
                    // the pack header would fix that, and only for packs
                    // written after the format changed.
                    received_at,
                    played_at: None,
                    players: metadata.players,
                    rule: metadata.rule,
                    event_count: metadata.event_count,
                    storage: location,
                })
                .await?;
            recovered += 1;
        }
        tracing::info!(pack = pack.key, entries, "re-indexed a pack");
    }
    catalog.flush().await?;
    Ok(recovered)
}
