use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"MJPACK01";
const ENTRY_HEADER_LEN: u64 = 24;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackLocation {
    pub pack_key: String,
    pub offset: u64,
    pub compressed_size: u32,
    pub raw_size: u32,
    pub codec: &'static str,
}

struct Writer {
    id: Uuid,
    file: File,
    size: u64,
}

pub struct PackStore {
    root: PathBuf,
    target_bytes: u64,
    writer: Mutex<Option<Writer>>,
}

#[derive(Debug, Error)]
pub enum PackError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("record is too large for the pack format")]
    TooLarge,
    #[error("pack entry is corrupt")]
    Corrupt,
}

impl PackStore {
    pub fn new(root: PathBuf, target_bytes: u64) -> Result<Self, PackError> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            target_bytes,
            writer: Mutex::new(None),
        })
    }

    pub fn append(&self, record_id: Uuid, raw: &[u8]) -> Result<PackLocation, PackError> {
        let raw_size: u32 = raw.len().try_into().map_err(|_| PackError::TooLarge)?;
        let compressed = zstd::bulk::compress(raw, 3)?;
        let compressed_size: u32 = compressed
            .len()
            .try_into()
            .map_err(|_| PackError::TooLarge)?;
        let entry_size = ENTRY_HEADER_LEN + u64::from(compressed_size);
        let mut writer = self.writer.lock();
        if writer
            .as_ref()
            .is_none_or(|current| current.size + entry_size > self.target_bytes)
        {
            *writer = Some(self.create_writer()?);
        }
        let current = writer.as_mut().expect("writer was just created");
        current.file.write_all(record_id.as_bytes())?;
        current.file.write_all(&raw_size.to_be_bytes())?;
        current.file.write_all(&compressed_size.to_be_bytes())?;
        let offset = current.size + ENTRY_HEADER_LEN;
        current.file.write_all(&compressed)?;
        current.file.flush()?;
        current.size += entry_size;

        Ok(PackLocation {
            pack_key: format!("packs/{}.mjpack", current.id),
            offset,
            compressed_size,
            raw_size,
            codec: "zstd",
        })
    }

    pub fn read(&self, location: &PackLocation) -> Result<Vec<u8>, PackError> {
        let filename = Path::new(&location.pack_key)
            .file_name()
            .ok_or(PackError::Corrupt)?;
        let mut file = File::open(self.root.join(filename))?;
        file.seek(SeekFrom::Start(location.offset))?;
        let mut compressed = vec![0; location.compressed_size as usize];
        file.read_exact(&mut compressed)?;
        let raw = zstd::bulk::decompress(&compressed, location.raw_size as usize)?;
        if raw.len() != location.raw_size as usize {
            return Err(PackError::Corrupt);
        }
        Ok(raw)
    }

    fn create_writer(&self) -> Result<Writer, PackError> {
        let id = Uuid::new_v4();
        let path = self.root.join(format!("{id}.mjpack"));
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(MAGIC)?;
        Ok(Writer {
            id,
            file,
            size: MAGIC.len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_one_record_by_offset() {
        let root = std::env::temp_dir().join(format!("mjai-pack-test-{}", Uuid::new_v4()));
        let store = PackStore::new(root.clone(), 1024).unwrap();
        let raw = br#"{"type":"start_game","names":["a","b","c","d"]}"#;
        let location = store.append(Uuid::new_v4(), raw).unwrap();
        assert_eq!(store.read(&location).unwrap(), raw);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rolls_over_without_breaking_old_locations() {
        let root = std::env::temp_dir().join(format!("mjai-pack-test-{}", Uuid::new_v4()));
        let store = PackStore::new(root.clone(), 80).unwrap();
        let first = store.append(Uuid::new_v4(), &[1; 128]).unwrap();
        let second = store.append(Uuid::new_v4(), &[2; 128]).unwrap();
        assert_ne!(first.pack_key, second.pack_key);
        assert_eq!(store.read(&first).unwrap(), vec![1; 128]);
        std::fs::remove_dir_all(root).unwrap();
    }
}
