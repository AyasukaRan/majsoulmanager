use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};

use crate::{
    catalog::Catalog,
    objects::{ObjectEntry, Objects},
    pack::{PACK_PREFIX, PACK_SUFFIX},
};

/// Deletes the pack objects nothing points at. They exist because the upload of
/// a sealed pack lands before its index rows do, by design: reversing that order
/// would let a row name an object that is not there yet, and a reader cannot
/// tell that from data loss. An upload whose insert never followed is therefore
/// an orphan, and this is what collects it.
///
/// Returns how many objects it removed. Every failure short-circuits with
/// nothing deleted, which is the only safe direction: an object left behind
/// costs storage until the next run, an object deleted by mistake costs records
/// that only exist there.
pub async fn collect(
    catalog: &Catalog,
    objects: &Objects,
    grace: Duration,
) -> anyhow::Result<usize> {
    // The manifest is the per-pack row count the recovery scan already needs,
    // reused for its keys alone; a dedicated `SELECT DISTINCT pack_key` would
    // be the same full scan of the same column.
    let referenced: HashSet<String> = catalog
        .indexed_counts()
        .await?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    if referenced.is_empty() {
        tracing::warn!("对象回收已跳过：索引没有报告任何 pack，空索引和查询故障从这里看是一样的");
        return Ok(0);
    }
    let listed = objects.list(PACK_PREFIX).await?;
    let mut deleted = 0usize;
    for key in orphans(&listed, &referenced, Utc::now(), grace) {
        objects.delete(key).await?;
        tracing::info!(pack = key, "deleted an orphaned pack object");
        deleted += 1;
    }
    Ok(deleted)
}

/// The set difference the collector deletes, pulled out of the I/O so it can be
/// tested: an object under the pack prefix that no index row references and
/// that has been sitting there longer than the grace period.
pub fn orphans<'a>(
    listed: &'a [ObjectEntry],
    referenced: &HashSet<String>,
    now: DateTime<Utc>,
    grace: Duration,
) -> Vec<&'a str> {
    // Refused here as well as at the call site, because this is the function
    // that decides what dies and the caller that forgets the check is the one
    // that deletes the corpus.
    if referenced.is_empty() {
        return Vec::new();
    }
    listed
        .iter()
        .filter(|entry| entry.key.ends_with(PACK_SUFFIX))
        .filter(|entry| !referenced.contains(&entry.key))
        // A pack uploaded seconds ago whose index rows are still buffered looks
        // exactly like one whose writer died, so nothing young is touched. An
        // object stamped in the future — a clock skew between this process and
        // the bucket — yields a negative duration, which `to_std` refuses, and
        // that reads as "not old enough", which is the direction to fail in.
        .filter(|entry| {
            now.signed_duration_since(entry.last_modified)
                .to_std()
                .is_ok_and(|age| age >= grace)
        })
        .map(|entry| entry.key.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, age_hours: i64) -> ObjectEntry {
        ObjectEntry {
            key: key.to_owned(),
            size: 1024,
            last_modified: now() - chrono::Duration::hours(age_hours),
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-27T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn manifest(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|key| (*key).to_owned()).collect()
    }

    #[test]
    fn collects_only_old_unreferenced_packs() {
        let grace = Duration::from_secs(86_400);
        let listed = [
            entry("packs/2026/07/25/0-referenced.mjpack", 48),
            entry("packs/2026/07/25/0-orphan.mjpack", 48),
            entry("packs/2026/07/27/0-fresh-orphan.mjpack", 1),
            // Under the prefix but not a pack: an export, a marker, anything a
            // future stage puts there. Not ours to delete.
            entry("packs/2026/07/25/index.json", 48),
        ];
        let referenced = manifest(&["packs/2026/07/25/0-referenced.mjpack"]);
        assert_eq!(
            orphans(&listed, &referenced, now(), grace),
            vec!["packs/2026/07/25/0-orphan.mjpack"]
        );
    }

    /// The guard that matters: an index that answers with nothing is either
    /// healthy and idle or broken, and deleting on that answer deletes every
    /// pack in the bucket.
    #[test]
    fn deletes_nothing_when_the_manifest_is_empty() {
        let listed = [entry("packs/2026/07/25/0-orphan.mjpack", 720)];
        assert!(
            orphans(&listed, &HashSet::new(), now(), Duration::ZERO).is_empty(),
            "an empty manifest deleted an object"
        );
    }

    /// The flat keys the live corpus carries are as referenced as any other,
    /// and a listing that returns them must not read as a bucket full of
    /// orphans just because the key shape changed.
    #[test]
    fn keeps_referenced_legacy_flat_keys() {
        let listed = [entry("packs/9b1deb4d.mjpack", 720)];
        let referenced = manifest(&["packs/9b1deb4d.mjpack"]);
        assert!(orphans(&listed, &referenced, now(), Duration::ZERO).is_empty());
    }
}
