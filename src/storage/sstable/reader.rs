use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use super::block::{BlockDecodeError, DataBlock};
use super::bloom::{BloomDecodeError, BloomFilter};
use super::builder::{
    FILTER_META_KEY, FooterDecodeError, MetaIndex, MetaIndexError,
    SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES, SSTABLE_FOOTER_CHECKSUM_SIZE_BYTES,
    SSTABLE_FOOTER_SIZE_BYTES, decode_footer,
};
use super::index::{BlockHandle, IndexBlock, IndexDecodeError};

#[derive(Debug, Error)]
pub enum SSTableReadError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("file is too small to contain an SSTable footer: {0} bytes")]
    FileTooSmall(u64),
    #[error("invalid footer: {0}")]
    Footer(#[from] FooterDecodeError),
    #[error("invalid index block: {0}")]
    IndexDecode(#[from] IndexDecodeError),
    #[error("invalid metaindex block: {0}")]
    MetaIndex(#[from] MetaIndexError),
    #[error("invalid bloom filter block: {0}")]
    BloomDecode(#[from] BloomDecodeError),
    #[error("invalid data block: {0}")]
    DataBlock(#[from] BlockDecodeError),
    #[error("SSTable checksum mismatch: expected {expected:#x}, found {found:#x}")]
    ChecksumMismatch { expected: u64, found: u64 },
}

#[derive(Debug)]
pub struct SSTableReader {
    path: PathBuf,
    file: Mutex<File>,
    index: IndexBlock,
    bloom: Option<BloomFilter>,
    cache: RwLock<HashMap<u64, Arc<DataBlock>>>,
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SSTableReadError> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let file_size = file.metadata()?.len();
        if file_size < SSTABLE_FOOTER_SIZE_BYTES as u64 {
            return Err(SSTableReadError::FileTooSmall(file_size));
        }

        file.seek(SeekFrom::Start(file_size.saturating_sub(SSTABLE_FOOTER_SIZE_BYTES as u64)))?;

        let mut footer_bytes = [0_u8; SSTABLE_FOOTER_SIZE_BYTES];
        file.read_exact(&mut footer_bytes)?;
        let footer = decode_footer(&footer_bytes)?;
        verify_checksum(&mut file, file_size, footer.checksum)?;

        let index_bytes = read_handle_bytes(&mut file, footer.index_handle)?;
        let index = IndexBlock::decode(&index_bytes)?;

        let metaindex_bytes = read_handle_bytes(&mut file, footer.metaindex_handle)?;
        let metaindex = MetaIndex::decode(&metaindex_bytes)?;

        let bloom = if let Some(handle) = metaindex.get(FILTER_META_KEY) {
            let bytes = read_handle_bytes(&mut file, handle)?;
            Some(BloomFilter::decode(&bytes)?)
        } else {
            None
        };

        Ok(Self { path, file: Mutex::new(file), index, bloom, cache: RwLock::new(HashMap::new()) })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SSTableReadError> {
        if let Some(filter) = self.bloom.as_ref() {
            if !filter.may_contain(key) {
                return Ok(None);
            }
        }

        let Some(entry) = self.index.find_block_for_key(key) else {
            return Ok(None);
        };

        let block = self.load_block(entry.handle)?;
        Ok(block.get(key))
    }

    pub fn scan_range(
        &self,
        start_key_inclusive: Option<&[u8]>,
        end_key_exclusive: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SSTableReadError> {
        if let (Some(start), Some(end)) = (start_key_inclusive, end_key_exclusive) {
            if start >= end {
                return Ok(Vec::new());
            }
        }

        let start_index = start_key_inclusive
            .and_then(|key| self.index.find_block_index_for_key(key))
            .unwrap_or(0);

        if start_index >= self.index.len() {
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        let mut done = false;

        for entry in self.index.entries().iter().skip(start_index) {
            let block = self.load_block(entry.handle)?;

            for (key, value) in block.iter() {
                if let Some(start) = start_key_inclusive {
                    if key.as_slice() < start {
                        continue;
                    }
                }

                if let Some(end) = end_key_exclusive {
                    if key.as_slice() >= end {
                        done = true;
                        break;
                    }
                }

                rows.push((key, value));
            }

            if done {
                break;
            }
        }

        Ok(rows)
    }

    pub fn cached_block_count(&self) -> usize {
        self.cache.read().len()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn load_block(&self, handle: BlockHandle) -> Result<Arc<DataBlock>, SSTableReadError> {
        if let Some(cached) = self.cache.read().get(&handle.offset).cloned() {
            return Ok(cached);
        }

        let mut file = self.file.lock();
        let bytes = read_handle_bytes(&mut file, handle)?;
        let block = Arc::new(DataBlock::from_bytes(bytes)?);

        self.cache.write().insert(handle.offset, Arc::clone(&block));
        Ok(block)
    }
}

fn read_handle_bytes(file: &mut File, handle: BlockHandle) -> Result<Vec<u8>, io::Error> {
    file.seek(SeekFrom::Start(handle.offset))?;

    let mut bytes = vec![0_u8; handle.size as usize];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn verify_checksum(file: &mut File, file_size: u64, expected: u64) -> Result<(), SSTableReadError> {
    let found = compute_file_checksum(file, file_size)? as u64;
    if found != expected {
        return Err(SSTableReadError::ChecksumMismatch { expected, found });
    }

    Ok(())
}

fn compute_file_checksum(file: &mut File, file_size: u64) -> io::Result<u32> {
    let footer_start = file_size
        .checked_sub(SSTABLE_FOOTER_SIZE_BYTES as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid footer bounds"))?;
    let checksum_start = footer_start
        .checked_add(SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid checksum bounds"))?;
    let checksum_end = checksum_start
        .checked_add(SSTABLE_FOOTER_CHECKSUM_SIZE_BYTES as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid checksum bounds"))?;

    let mut hasher = Hasher::new();
    file.seek(SeekFrom::Start(0))?;

    let mut offset = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }

        let chunk_start = offset;
        let chunk_end = offset.saturating_add(read as u64);

        if chunk_start < checksum_end && chunk_end > checksum_start {
            let mut sanitized = buffer[..read].to_vec();
            let zero_start = checksum_start.saturating_sub(chunk_start) as usize;
            let zero_end = checksum_end.min(chunk_end).saturating_sub(chunk_start) as usize;
            sanitized[zero_start..zero_end].fill(0);
            hasher.update(&sanitized);
        } else {
            hasher.update(&buffer[..read]);
        }

        offset = chunk_end;
    }

    Ok(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::storage::sstable::builder::SSTableBuilder;

    fn temp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        dir.push(format!("lsmdb-sstable-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn builder_reader_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = dir.join("table.sst");

        let mut builder = SSTableBuilder::create(&path).expect("create builder");
        for i in 0..250_u32 {
            let key = format!("k{i:04}");
            let value = format!("value-{i:04}");
            builder.add(key.as_bytes(), value.as_bytes()).expect("add sorted row");
        }

        let summary = builder.finish().expect("finish SSTable");
        assert_eq!(summary.entry_count, 250);
        assert!(summary.data_block_count >= 1);

        let reader = SSTableReader::open(&path).expect("open reader");
        assert_eq!(reader.get(b"k0000").expect("lookup k0000"), Some(b"value-0000".to_vec()));
        assert_eq!(reader.get(b"k0123").expect("lookup k0123"), Some(b"value-0123".to_vec()));
        assert_eq!(reader.get(b"k9999").expect("lookup missing"), None);

        let range =
            reader.scan_range(Some(b"k0100"), Some(b"k0110")).expect("range scan should succeed");

        assert_eq!(range.len(), 10);
        assert_eq!(range.first().map(|(k, _)| k.clone()), Some(b"k0100".to_vec()));
        assert_eq!(range.last().map(|(k, _)| k.clone()), Some(b"k0109".to_vec()));

        let _ = reader.get(b"k0102").expect("cached lookup");
        let _ = reader.get(b"k0103").expect("cached lookup");
        assert!(reader.cached_block_count() >= 1);

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn rejects_tiny_file() {
        let dir = temp_dir("tiny");
        let path = dir.join("tiny.sst");
        fs::write(&path, [0_u8; 7]).expect("write tiny file");

        let err = SSTableReader::open(&path).expect_err("tiny file should fail");
        assert!(matches!(err, SSTableReadError::FileTooSmall(_)));

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn rejects_corrupted_file_with_checksum_mismatch() {
        let dir = temp_dir("checksum-mismatch");
        let path = dir.join("table.sst");

        let mut builder = SSTableBuilder::create(&path).expect("create builder");
        for i in 0..32_u32 {
            let key = format!("k{i:04}");
            let value = format!("value-{i:04}");
            builder.add(key.as_bytes(), value.as_bytes()).expect("add sorted row");
        }
        builder.finish().expect("finish table");

        let mut bytes = fs::read(&path).expect("read sstable bytes");
        bytes[0] ^= 0x5A;
        fs::write(&path, bytes).expect("write corrupted sstable");

        let err = SSTableReader::open(&path).expect_err("corrupted file should fail checksum");
        assert!(matches!(err, SSTableReadError::ChecksumMismatch { .. }));

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }
}
