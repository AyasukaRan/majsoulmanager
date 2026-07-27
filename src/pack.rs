use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::objects::{ObjectError, Objects};

const MAGIC: &[u8; 8] = b"MJPACK01";
const ENTRY_HEADER_LEN: u64 = 24;

/// Every pack object lives under this prefix, which is what the GC lists and
/// what a key has to start with to be one of ours.
pub const PACK_PREFIX: &str = "packs/";

/// Objects under the prefix that do not end in this are not packs. The GC has
/// to tell them apart, because deleting one it merely failed to recognise is
/// the same accident as deleting a referenced pack.
pub const PACK_SUFFIX: &str = ".mjpack";

/// The corpus collected before object storage existed. On the live deployment
/// this is the only copy of those records, so nothing here is ever removed.
const LEGACY_DIR: &str = "packs";

/// Packs the pack/index worker is still filling. Distinct from the legacy
/// directory because everything here is discardable and nothing there is.
const STAGING_DIR: &str = "staging-packs";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackLocation {
    pub pack_key: String,
    pub offset: u64,
    pub compressed_size: u32,
    pub raw_size: u32,
    pub codec: &'static str,
}

#[derive(Debug, Error)]
pub enum PackError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("record is too large for the pack format")]
    TooLarge,
    #[error("pack entry is corrupt")]
    Corrupt,
    #[error(transparent)]
    Object(#[from] ObjectError),
}

/// The writing half of a pack. Exactly one owner drives it, which is what lets
/// the caller decide when a pack is sealed instead of discovering it was rolled
/// over underneath: the pack/index worker may only seal between records,
/// because a sealed pack's index rows are written before its Kafka offset is
/// committed and a pack sealed mid-batch would split that guarantee in two.
pub struct PackWriter {
    key: String,
    path: PathBuf,
    file: File,
    size: u64,
    first_append: Option<Instant>,
}

impl PackWriter {
    /// The local file is named after the last component of the object key, and
    /// that is the whole of the relationship between the two: `PackStore::read`
    /// resolves a local pack by taking `file_name` of the key it was given, so
    /// a key of any shape — the flat legacy one or the dated one — finds its
    /// file as long as it was created here.
    fn create(dir: &Path, key: &str) -> Result<Self, PackError> {
        let name = Path::new(key).file_name().ok_or(PackError::Corrupt)?;
        let path = dir.join(name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        file.write_all(MAGIC)?;
        Ok(Self {
            key: key.to_owned(),
            path,
            file,
            size: MAGIC.len() as u64,
            first_append: None,
        })
    }

    pub fn append(&mut self, record_id: Uuid, raw: &[u8]) -> Result<PackLocation, PackError> {
        let raw_size: u32 = raw.len().try_into().map_err(|_| PackError::TooLarge)?;
        let compressed = zstd::bulk::compress(raw, 3)?;
        let compressed_size: u32 = compressed
            .len()
            .try_into()
            .map_err(|_| PackError::TooLarge)?;
        self.file.write_all(record_id.as_bytes())?;
        self.file.write_all(&raw_size.to_be_bytes())?;
        self.file.write_all(&compressed_size.to_be_bytes())?;
        let offset = self.size + ENTRY_HEADER_LEN;
        self.file.write_all(&compressed)?;
        self.file.flush()?;
        self.size = offset + u64::from(compressed_size);
        self.first_append.get_or_insert_with(Instant::now);

        Ok(PackLocation {
            pack_key: self.key.clone(),
            offset,
            compressed_size,
            raw_size,
            codec: "zstd",
        })
    }

    /// Bytes on disk, the magic header included, which is what a size-based
    /// seal compares against its target.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// How long the oldest record in this pack has been waiting to be indexed,
    /// or `None` while the pack is still empty. An age-based seal reads this
    /// rather than the file's creation time so that it never produces an empty
    /// pack during a quiet period, which would be one object and one upload per
    /// age limit forever.
    pub fn age(&self) -> Option<Duration> {
        self.first_append.map(|first| first.elapsed())
    }

    /// Closes the pack and hands back its object key and the local file that
    /// holds it. `sync_all` because the next thing to happen to these bytes is
    /// an upload followed by index rows that point at them, and a crash in that
    /// window must not leave a pack whose tail never reached the platter.
    pub fn seal(self) -> Result<(String, PathBuf), PackError> {
        self.file.sync_all()?;
        Ok((self.key, self.path))
    }
}

/// The reading half, shared by every request. It resolves a pack locally first
/// — the staging pack a record was just written into has not been uploaded yet,
/// and the legacy corpus is still on disk — and falls back to a ranged read of
/// the object.
pub struct PackStore {
    legacy: PathBuf,
    staging: PathBuf,
    objects: Arc<Objects>,
    target_bytes: u64,
    /// The pre-Kafka ingest path still appends inline, from many request tasks
    /// at once, so its writer needs a lock the worker's will not. Both this
    /// field and `append` go away with that path.
    inline: Mutex<Option<PackWriter>>,
}

impl PackStore {
    /// Both directories are derived from `data_dir` here rather than passed in,
    /// which is what makes `discard_staging` safe by construction: no caller
    /// can hand it a path, so no caller can point it at the legacy corpus.
    pub fn new(
        data_dir: &Path,
        target_bytes: u64,
        objects: Arc<Objects>,
    ) -> Result<Self, PackError> {
        let legacy = data_dir.join(LEGACY_DIR);
        let staging = data_dir.join(STAGING_DIR);
        std::fs::create_dir_all(&legacy)?;
        std::fs::create_dir_all(&staging)?;
        Ok(Self {
            legacy,
            staging,
            objects,
            target_bytes,
            inline: Mutex::new(None),
        })
    }

    pub async fn read(&self, location: &PackLocation) -> Result<Vec<u8>, PackError> {
        let name = Path::new(&location.pack_key)
            .file_name()
            .ok_or(PackError::Corrupt)?;
        let length = u64::from(location.compressed_size);
        for root in [&self.legacy, &self.staging] {
            match File::open(root.join(name)) {
                Ok(mut file) => {
                    file.seek(SeekFrom::Start(location.offset))?;
                    let mut compressed = vec![0; location.compressed_size as usize];
                    file.read_exact(&mut compressed)?;
                    return decompress(&compressed, location);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let compressed = self
            .objects
            .get_range(&location.pack_key, location.offset, length)
            .await?;
        decompress(&compressed, location)
    }

    /// A fresh staging pack, keyed by date, Kafka partition and a new UUID as
    /// docs/architecture.md asks. The date is the sealing process's own, not
    /// any record's: it exists to keep a listing of the bucket navigable, and
    /// nothing reads meaning out of it.
    pub fn staging_writer(&self, partition: i32) -> Result<PackWriter, PackError> {
        let key = format!(
            "{PACK_PREFIX}{}/{partition}-{}{PACK_SUFFIX}",
            Utc::now().format("%Y/%m/%d"),
            Uuid::new_v4()
        );
        PackWriter::create(&self.staging, &key)
    }

    /// Drops every staging pack left behind by a crash, and reports how many.
    /// This is only correct because a staging pack holds nothing but records
    /// whose Kafka offset was never committed, so the broker still owes every
    /// one of them; keeping the file would land those records in a second pack
    /// as well. It takes no path, and `staging` cannot be the legacy directory,
    /// so there is no version of this call that deletes the corpus.
    pub fn discard_staging(&self) -> Result<usize, PackError> {
        let mut discarded = 0usize;
        for path in mjpacks(&self.staging)? {
            std::fs::remove_file(&path)?;
            discarded += 1;
        }
        Ok(discarded)
    }

    /// The legacy pack files on disk, by path only. Callers walk them one at a
    /// time through `scan_pack`: an entry list for the whole corpus would be
    /// one allocation that grows with every record ever collected, and nothing
    /// needs more than a single pack's entries at once.
    pub fn packs(&self) -> Result<Vec<PathBuf>, PackError> {
        mjpacks(&self.legacy)
    }

    /// Copies the legacy corpus into object storage, leaving every local file
    /// where it is: getting the only copy of the records off one disk is the
    /// point, and deleting that copy on the same boot that first uploads it is
    /// not worth the few hundred megabytes.
    ///
    /// Safe to repeat on every boot. A pack whose object already has the same
    /// size is skipped, so a second boot uploads nothing, and a pack that has
    /// grown since — the inline ingest path still appends to this directory —
    /// is uploaded again in full.
    pub async fn upload_legacy_packs(&self) -> Result<usize, PackError> {
        let mut uploaded = 0usize;
        for path in self.packs()? {
            let key = pack_key(&path).ok_or(PackError::Corrupt)?;
            let local = tokio::fs::metadata(&path).await?.len();
            if self.objects.head(&key).await? == Some(local) {
                continue;
            }
            // One PUT for the whole pack, which is 256MB at the target size.
            // Multipart upload is the upgrade path if that target ever rises
            // past what the container can hold at once.
            let bytes = tokio::fs::read(&path).await?;
            let size = bytes.len() as u64;
            self.objects.put(&key, bytes).await?;
            // A PUT that answered 200 but stored a truncated body would leave
            // an object that reads back as a corrupt pack for as long as the
            // local copy survives it, so confirm the size before believing it.
            match self.objects.head(&key).await? {
                Some(stored) if stored == size => uploaded += 1,
                stored => {
                    return Err(ObjectError::Malformed(format!(
                        "uploaded {key} as {size} bytes but the bucket reports {stored:?}"
                    ))
                    .into());
                }
            }
        }
        Ok(uploaded)
    }

    /// The pre-Kafka ingest path: append and index inline, under the flat
    /// `packs/{uuid}.mjpack` key. It deliberately does not use the dated key
    /// scheme, because `pack_key` derives a flat key from a file name in this
    /// directory and both the recovery scan and the boot-time upload depend on
    /// that derivation matching what the index rows say. Removed when the
    /// producer replaces this path.
    pub fn append(&self, record_id: Uuid, raw: &[u8]) -> Result<PackLocation, PackError> {
        let mut inline = self.inline.lock();
        let writer = match inline.as_mut() {
            Some(writer) => writer,
            None => {
                let key = format!("{PACK_PREFIX}{}{PACK_SUFFIX}", Uuid::new_v4());
                inline.insert(PackWriter::create(&self.legacy, &key)?)
            }
        };
        let location = writer.append(record_id, raw)?;
        // Sealed after the append that reached the target rather than before
        // the one that would overflow it, which is the rule the pack worker
        // applies too because it also has to check an age limit after each
        // record. A pack overshoots by at most one record, which against a
        // 256MB target is noise.
        if writer.size() >= self.target_bytes {
            *inline = None;
        }
        Ok(location)
    }
}

/// The frame is handed to the decoder only once it is exactly the length the
/// index recorded. `read_exact` guarantees that on the local path and
/// `Objects::get_range` refuses a response that is not the slice it asked for,
/// so this states the guarantee both paths owe rather than assuming it: a short
/// or over-long body decompressed as if it were whole is a record silently
/// replaced by something else.
fn decompress(compressed: &[u8], location: &PackLocation) -> Result<Vec<u8>, PackError> {
    if compressed.len() != location.compressed_size as usize {
        return Err(PackError::Corrupt);
    }
    let raw = zstd::bulk::decompress(compressed, location.raw_size as usize)?;
    if raw.len() != location.raw_size as usize {
        return Err(PackError::Corrupt);
    }
    Ok(raw)
}

fn mjpacks(dir: &Path) -> Result<Vec<PathBuf>, PackError> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "mjpack")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub struct PackFile {
    pub key: String,
    pub modified: std::time::SystemTime,
    pub entries: Vec<(Uuid, PackLocation)>,
}

/// The index key a legacy pack file is stored under, which is what `pack_key`
/// on a `PackLocation` holds for every row written before the pack worker
/// existed. It is flat by definition: a dated key cannot be recovered from a
/// file name, which is why the staging directory is scanned by nobody and the
/// inline path still writes flat keys.
pub fn pack_key(path: &Path) -> Option<String> {
    Some(format!(
        "{PACK_PREFIX}{}",
        path.file_name()?.to_string_lossy()
    ))
}

/// Rebuilds one pack's entry locations from its headers alone, which is what
/// docs/architecture.md means by "可以离线扫描 RustFS 重建 ClickHouse 索引".
/// Frames are skipped by seeking, so the cost is one 24 byte read per record
/// and no decompression.
pub fn scan_pack(path: &Path) -> Result<PackFile, PackError> {
    let key = pack_key(path).ok_or(PackError::Corrupt)?;
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    let mut magic = [0u8; MAGIC.len()];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(PackError::Corrupt);
    }
    let mut entries = Vec::new();
    let mut position = MAGIC.len() as u64;
    let mut header = [0u8; ENTRY_HEADER_LEN as usize];
    // A crash between the header write and the frame write leaves a trailing
    // entry whose frame is short; stopping there keeps the rest recoverable.
    while position + ENTRY_HEADER_LEN <= length {
        file.read_exact(&mut header)?;
        let record_id = Uuid::from_slice(&header[0..16]).map_err(|_| PackError::Corrupt)?;
        let raw_size = u32::from_be_bytes(header[16..20].try_into().expect("4 bytes"));
        let compressed_size = u32::from_be_bytes(header[20..24].try_into().expect("4 bytes"));
        // A tail of zeroes is what a truncated or sparsely extended file reads
        // as, and it parses as a perfectly well formed entry: the nil UUID with
        // an empty frame. Accepting those would invent one phantom entry per 24
        // bytes, none of which can ever match an indexed record, so the pack
        // would come up short of its entry count and be re-scanned on every
        // boot for as long as it exists. No real entry has either field zero:
        // ids come from `Uuid::new_v4`, and zstd emits a frame header even for
        // an empty input, so a written frame is never 0 bytes.
        if record_id.is_nil() || compressed_size == 0 {
            tracing::warn!(
                pack = key,
                offset = position,
                "pack ends in unwritten bytes; stopping the scan there"
            );
            break;
        }
        let offset = position + ENTRY_HEADER_LEN;
        if offset + u64::from(compressed_size) > length {
            break;
        }
        entries.push((
            record_id,
            PackLocation {
                pack_key: key.clone(),
                offset,
                compressed_size,
                raw_size,
                codec: "zstd",
            },
        ));
        position = offset + u64::from(compressed_size);
        file.seek(SeekFrom::Start(position))?;
    }
    Ok(PackFile {
        key,
        modified: file.metadata()?.modified()?,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every read in these tests resolves locally, so the endpoint is only ever
    /// contacted by the one test that means to prove the fallback happens.
    fn store(root: &Path, target_bytes: u64) -> PackStore {
        let objects =
            Objects::new("http://127.0.0.1:1", "mjai-raw", "us-east-1", "k", "s").unwrap();
        PackStore::new(root, target_bytes, Arc::new(objects)).unwrap()
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("mjai-pack-test-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn round_trips_one_record_by_offset() {
        let root = temp_dir();
        let store = store(&root, 1024);
        let raw = br#"{"type":"start_game","names":["a","b","c","d"]}"#;
        let location = store.append(Uuid::new_v4(), raw).unwrap();
        assert_eq!(store.read(&location).await.unwrap(), raw);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scan_rebuilds_every_location_from_the_headers() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        let written: Vec<_> = (0..3u8)
            .map(|index| {
                let id = Uuid::new_v4();
                let raw = vec![index; 64 + usize::from(index)];
                let location = store.append(id, &raw).unwrap();
                (id, location, raw)
            })
            .collect();

        let paths = store.packs().unwrap();
        assert_eq!(paths.len(), 1);
        let pack = scan_pack(&paths[0]).unwrap();
        assert_eq!(pack.entries.len(), 3);
        for ((id, location, raw), (scanned_id, scanned)) in written.iter().zip(&pack.entries) {
            assert_eq!(id, scanned_id);
            assert_eq!(location.pack_key, scanned.pack_key);
            assert_eq!(location.offset, scanned.offset);
            assert_eq!(&store.read(scanned).await.unwrap(), raw);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_stops_at_a_torn_trailing_entry() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        let location = store.append(Uuid::new_v4(), &[7; 64]).unwrap();
        store.append(Uuid::new_v4(), &[8; 64]).unwrap();
        let path = root
            .join(LEGACY_DIR)
            .join(Path::new(&location.pack_key).file_name().unwrap());
        let length = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 4)
            .unwrap();

        assert_eq!(scan_pack(&path).unwrap().entries.len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A pack extended but not written — a torn write, or a file grown by a
    /// sparse seek — reads back as zeroes, and 24 zero bytes parse as a valid
    /// entry header. Inventing those entries left the pack permanently short of
    /// its indexed count, so recovery re-scanned it on every single boot.
    #[test]
    fn scan_stops_at_a_tail_of_zeroes_instead_of_inventing_entries() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        let location = store.append(Uuid::new_v4(), &[7; 64]).unwrap();
        let path = root
            .join(LEGACY_DIR)
            .join(Path::new(&location.pack_key).file_name().unwrap());
        let written = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 240]).unwrap();
        file.flush().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), written + 240);

        let entries = scan_pack(&path).unwrap().entries;
        assert_eq!(entries.len(), 1, "zero bytes were scanned as entries");
        assert!(entries.iter().all(|(id, _)| !id.is_nil()));
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The seal now happens after the append that reaches the target, not
    /// before the one that would overflow it, so the target here is small
    /// enough that a single record trips it. What the test pins is unchanged:
    /// a location handed out before a rollover still reads afterwards.
    #[tokio::test]
    async fn rolls_over_without_breaking_old_locations() {
        let root = temp_dir();
        let store = store(&root, 8);
        let first = store.append(Uuid::new_v4(), &[1; 128]).unwrap();
        let second = store.append(Uuid::new_v4(), &[2; 128]).unwrap();
        assert_ne!(first.pack_key, second.pack_key);
        assert_eq!(store.read(&first).await.unwrap(), vec![1; 128]);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The 18,633 rows on the live deployment carry flat `packs/{uuid}.mjpack`
    /// keys written before the dated scheme existed, and a refactor that made
    /// local resolution depend on the key's shape would strand every one of
    /// them with no symptom until someone asked for a record.
    #[tokio::test]
    async fn resolves_flat_legacy_keys_and_dated_staging_keys_alike() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        let legacy = store.append(Uuid::new_v4(), b"legacy record").unwrap();
        assert!(
            legacy.pack_key.matches('/').count() == 1,
            "the inline path must keep writing flat keys, got {}",
            legacy.pack_key
        );
        assert_eq!(
            pack_key(Path::new(&legacy.pack_key)).as_ref(),
            Some(&legacy.pack_key)
        );

        let mut writer = store.staging_writer(0).unwrap();
        let staged = writer.append(Uuid::new_v4(), b"staged record").unwrap();
        let dated = format!("{PACK_PREFIX}{}/0-", Utc::now().format("%Y/%m/%d"));
        assert!(
            staged.pack_key.starts_with(&dated),
            "expected a dated key under {dated}, got {}",
            staged.pack_key
        );

        assert_eq!(store.read(&legacy).await.unwrap(), b"legacy record");
        assert_eq!(store.read(&staged).await.unwrap(), b"staged record");
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Both key shapes have to reach object storage when the local file is
    /// gone, which is the state every record ends up in once the local staging
    /// copy is cleaned up. There is no server here, so what this pins is that
    /// the fallback is attempted at all: without it a missing local file is a
    /// `NotFound` from `File::open` and the record reads as lost.
    #[tokio::test]
    async fn falls_back_to_object_storage_for_either_key_shape() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        for key in [
            "packs/00000000-0000-4000-8000-000000000000.mjpack",
            "packs/2026/07/27/0-00000000-0000-4000-8000-000000000000.mjpack",
        ] {
            let location = PackLocation {
                pack_key: key.to_owned(),
                offset: 8,
                compressed_size: 16,
                raw_size: 16,
                codec: "zstd",
            };
            assert!(
                matches!(store.read(&location).await, Err(PackError::Object(_))),
                "{key} did not reach object storage"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The discard must not be able to reach the legacy corpus, which is the
    /// live deployment's only copy of every record it holds.
    #[test]
    fn discarding_staging_leaves_the_legacy_corpus_alone() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        store.append(Uuid::new_v4(), b"legacy record").unwrap();
        store
            .staging_writer(0)
            .unwrap()
            .append(Uuid::new_v4(), b"staged record")
            .unwrap();

        assert_eq!(store.discard_staging().unwrap(), 1);
        assert_eq!(store.packs().unwrap().len(), 1);
        assert_eq!(mjpacks(&store.staging).unwrap().len(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_writer_has_no_age_until_it_holds_a_record() {
        let root = temp_dir();
        let store = store(&root, 1024 * 1024);
        let mut writer = store.staging_writer(0).unwrap();
        assert_eq!(writer.age(), None);
        assert_eq!(writer.size(), MAGIC.len() as u64);
        writer.append(Uuid::new_v4(), b"one record").unwrap();
        assert!(writer.age().is_some());
        let (key, path) = writer.seal().unwrap();
        assert_eq!(Path::new(&key).file_name(), path.file_name());
        std::fs::remove_dir_all(root).unwrap();
    }
}
