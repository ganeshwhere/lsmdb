use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use thiserror::Error;

use super::block::{BlockBuildError, DEFAULT_RESTART_INTERVAL, DataBlockBuilder};
use super::bloom::{BloomFilterBuilder, DEFAULT_BITS_PER_KEY, DEFAULT_HASH_FUNCTIONS};
use super::index::{BlockHandle, IndexBlock, IndexBuildError, IndexEntry};

pub const DEFAULT_DATA_BLOCK_SIZE_BYTES: usize = 4 * 1024;
pub const SSTABLE_FOOTER_SIZE_BYTES: usize = 48;
pub const SSTABLE_MAGIC: u64 = 0xdb47_7524_8b80_fb57;
pub const SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES: usize = 32;
pub const SSTABLE_FOOTER_CHECKSUM_SIZE_BYTES: usize = 8;
pub(crate) const FILTER_META_KEY: &str = "filter.bloom";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub checksum: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FooterDecodeError {
    #[error("footer has invalid size: expected 48 bytes, found {0}")]
    InvalidSize(usize),
    #[error("invalid SSTable magic: expected {expected:#x}, found {found:#x}")]
    InvalidMagic { expected: u64, found: u64 },
}

pub fn encode_footer(
    metaindex_handle: BlockHandle,
    index_handle: BlockHandle,
    checksum: u64,
) -> [u8; 48] {
    let mut footer = [0_u8; SSTABLE_FOOTER_SIZE_BYTES];

    footer[0..8].copy_from_slice(&metaindex_handle.offset.to_le_bytes());
    footer[8..16].copy_from_slice(&metaindex_handle.size.to_le_bytes());
    footer[16..24].copy_from_slice(&index_handle.offset.to_le_bytes());
    footer[24..32].copy_from_slice(&index_handle.size.to_le_bytes());
    footer[SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES
        ..SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES + SSTABLE_FOOTER_CHECKSUM_SIZE_BYTES]
        .copy_from_slice(&checksum.to_le_bytes());
    footer[40..48].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());

    footer
}

pub fn decode_footer(bytes: &[u8]) -> Result<Footer, FooterDecodeError> {
    if bytes.len() != SSTABLE_FOOTER_SIZE_BYTES {
        return Err(FooterDecodeError::InvalidSize(bytes.len()));
    }

    let mut magic_raw = [0_u8; 8];
    magic_raw.copy_from_slice(&bytes[40..48]);
    let magic = u64::from_le_bytes(magic_raw);
    if magic != SSTABLE_MAGIC {
        return Err(FooterDecodeError::InvalidMagic { expected: SSTABLE_MAGIC, found: magic });
    }

    Ok(Footer {
        metaindex_handle: BlockHandle { offset: read_u64(bytes, 0), size: read_u64(bytes, 8) },
        index_handle: BlockHandle { offset: read_u64(bytes, 16), size: read_u64(bytes, 24) },
        checksum: read_u64(bytes, SSTABLE_FOOTER_CHECKSUM_OFFSET_BYTES),
    })
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MetaIndex {
    entries: Vec<(String, BlockHandle)>,
}

#[derive(Debug, Error)]
pub enum MetaIndexError {
    #[error("metaindex section is truncated")]
    Truncated,
    #[error("metaindex key is invalid UTF-8")]
    InvalidUtf8,
    #[error("metaindex contains duplicate key: {0}")]
    DuplicateKey(String),
}

impl MetaIndex {
    pub fn insert(&mut self, key: impl Into<String>, handle: BlockHandle) {
        self.entries.push((key.into(), handle));
    }

    pub fn get(&self, key: &str) -> Option<BlockHandle> {
        self.entries.iter().find(|(candidate, _)| candidate == key).map(|(_, handle)| *handle)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_u32(&mut out, self.entries.len() as u32);

        for (key, handle) in &self.entries {
            push_u32(&mut out, key.len() as u32);
            out.extend_from_slice(key.as_bytes());
            push_u64(&mut out, handle.offset);
            push_u64(&mut out, handle.size);
        }

        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaIndexError> {
        let mut cursor = 0_usize;
        let count = read_u32_checked(bytes, &mut cursor).map_err(|_| MetaIndexError::Truncated)?;

        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key_len = read_u32_checked(bytes, &mut cursor)
                .map_err(|_| MetaIndexError::Truncated)? as usize;
            let key_end = cursor.checked_add(key_len).ok_or(MetaIndexError::Truncated)?;
            if key_end > bytes.len() {
                return Err(MetaIndexError::Truncated);
            }

            let key_bytes = &bytes[cursor..key_end];
            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| MetaIndexError::InvalidUtf8)?
                .to_string();
            cursor = key_end;

            let offset =
                read_u64_checked(bytes, &mut cursor).map_err(|_| MetaIndexError::Truncated)?;
            let size =
                read_u64_checked(bytes, &mut cursor).map_err(|_| MetaIndexError::Truncated)?;

            if entries.iter().any(|(existing, _): &(String, BlockHandle)| existing == &key) {
                return Err(MetaIndexError::DuplicateKey(key));
            }

            entries.push((key, BlockHandle { offset, size }));
        }

        if cursor != bytes.len() {
            return Err(MetaIndexError::Truncated);
        }

        Ok(Self { entries })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SSTableBuilderOptions {
    pub data_block_size_bytes: usize,
    pub restart_interval: usize,
    pub bloom_bits_per_key: usize,
    pub bloom_hash_functions: u8,
}

impl Default for SSTableBuilderOptions {
    fn default() -> Self {
        Self {
            data_block_size_bytes: DEFAULT_DATA_BLOCK_SIZE_BYTES,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_bits_per_key: DEFAULT_BITS_PER_KEY,
            bloom_hash_functions: DEFAULT_HASH_FUNCTIONS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableBuildSummary {
    pub path: PathBuf,
    pub entry_count: usize,
    pub data_block_count: usize,
    pub file_size_bytes: u64,
}

#[derive(Debug, Error)]
pub enum SSTableBuildError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("data block build error: {0}")]
    BlockBuild(#[from] BlockBuildError),
    #[error("index build error: {0}")]
    IndexBuild(#[from] IndexBuildError),
    #[error("metaindex error: {0}")]
    MetaIndex(#[from] MetaIndexError),
    #[error("builder options are invalid")]
    InvalidOptions,
    #[error("keys must be globally sorted before writing to SSTable")]
    KeysNotSorted,
    #[error("finish() was already called")]
    AlreadyFinished,
}

#[derive(Debug)]
pub struct SSTableBuilder {
    path: PathBuf,
    writer: BufWriter<File>,
    options: SSTableBuilderOptions,
    current_block: DataBlockBuilder,
    index_entries: Vec<IndexEntry>,
    all_keys: Vec<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    current_offset: u64,
    data_block_count: usize,
    entry_count: usize,
    finished: bool,
    checksum_hasher: Hasher,
}

impl SSTableBuilder {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, SSTableBuildError> {
        Self::create_with_options(path, SSTableBuilderOptions::default())
    }

    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        options: SSTableBuilderOptions,
    ) -> Result<Self, SSTableBuildError> {
        if options.data_block_size_bytes == 0
            || options.restart_interval == 0
            || options.bloom_bits_per_key == 0
            || options.bloom_hash_functions == 0
        {
            return Err(SSTableBuildError::InvalidOptions);
        }

        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).truncate(true).write(true).open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            options,
            current_block: DataBlockBuilder::new(options.restart_interval)?,
            index_entries: Vec::new(),
            all_keys: Vec::new(),
            last_key: None,
            current_offset: 0,
            data_block_count: 0,
            entry_count: 0,
            finished: false,
            checksum_hasher: Hasher::new(),
        })
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), SSTableBuildError> {
        if self.finished {
            return Err(SSTableBuildError::AlreadyFinished);
        }

        if let Some(previous) = self.last_key.as_ref() {
            if key <= previous.as_slice() {
                return Err(SSTableBuildError::KeysNotSorted);
            }
        }

        self.current_block.add(key, value)?;
        self.last_key = Some(key.to_vec());
        self.all_keys.push(key.to_vec());
        self.entry_count += 1;

        if self.current_block.estimated_size_bytes() >= self.options.data_block_size_bytes {
            self.flush_data_block()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<SSTableBuildSummary, SSTableBuildError> {
        if self.finished {
            return Err(SSTableBuildError::AlreadyFinished);
        }

        self.flush_data_block()?;

        let mut bloom_builder = BloomFilterBuilder::new(
            self.options.bloom_bits_per_key,
            self.options.bloom_hash_functions,
        );
        for key in &self.all_keys {
            bloom_builder.add_key(key);
        }
        let bloom = bloom_builder.build();
        let filter_handle = self.write_block(&bloom.encode())?;

        let mut metaindex = MetaIndex::default();
        metaindex.insert(FILTER_META_KEY, filter_handle);
        let metaindex_handle = self.write_block(&metaindex.encode())?;

        let index_block = IndexBlock::from_entries(self.index_entries.clone())?;
        let index_handle = self.write_block(&index_block.encode())?;

        let checksum = self.compute_checksum(metaindex_handle, index_handle);
        let footer = encode_footer(metaindex_handle, index_handle, checksum);
        self.writer.write_all(&footer)?;
        self.current_offset = self
            .current_offset
            .checked_add(SSTABLE_FOOTER_SIZE_BYTES as u64)
            .ok_or(SSTableBuildError::InvalidOptions)?;

        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.finished = true;

        Ok(SSTableBuildSummary {
            path: self.path,
            entry_count: self.entry_count,
            data_block_count: self.data_block_count,
            file_size_bytes: self.current_offset,
        })
    }

    fn flush_data_block(&mut self) -> Result<(), SSTableBuildError> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let replacement = DataBlockBuilder::new(self.options.restart_interval)?;
        let block_builder = std::mem::replace(&mut self.current_block, replacement);
        let finished = block_builder.finish()?;

        let handle = self.write_block(&finished.bytes)?;
        self.index_entries.push(IndexEntry { last_key: finished.last_key, handle });
        self.data_block_count += 1;

        Ok(())
    }

    fn write_block(&mut self, bytes: &[u8]) -> Result<BlockHandle, io::Error> {
        let handle = BlockHandle { offset: self.current_offset, size: bytes.len() as u64 };
        self.writer.write_all(bytes)?;
        self.checksum_hasher.update(bytes);
        self.current_offset = self.current_offset.saturating_add(bytes.len() as u64);
        Ok(handle)
    }

    fn compute_checksum(&self, metaindex_handle: BlockHandle, index_handle: BlockHandle) -> u64 {
        let mut hasher = self.checksum_hasher.clone();
        let footer_without_checksum = encode_footer(metaindex_handle, index_handle, 0);
        hasher.update(&footer_without_checksum);
        hasher.finalize() as u64
    }
}

fn read_u64(raw: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&raw[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

fn read_u32_checked(raw: &[u8], cursor: &mut usize) -> Result<u32, ()> {
    let end = cursor.checked_add(4).ok_or(())?;
    if end > raw.len() {
        return Err(());
    }

    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&raw[*cursor..end]);
    *cursor = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_checked(raw: &[u8], cursor: &mut usize) -> Result<u64, ()> {
    let end = cursor.checked_add(8).ok_or(())?;
    if end > raw.len() {
        return Err(());
    }

    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&raw[*cursor..end]);
    *cursor = end;
    Ok(u64::from_le_bytes(bytes))
}

fn push_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let footer = encode_footer(
            BlockHandle { offset: 100, size: 20 },
            BlockHandle { offset: 120, size: 30 },
            0xA1_B2_C3_D4,
        );
        let decoded = decode_footer(&footer).expect("decode footer");

        assert_eq!(decoded.metaindex_handle.offset, 100);
        assert_eq!(decoded.index_handle.offset, 120);
        assert_eq!(decoded.checksum, 0xA1_B2_C3_D4);
    }

    #[test]
    fn metaindex_roundtrip() {
        let mut meta = MetaIndex::default();
        meta.insert("filter.bloom", BlockHandle { offset: 1, size: 2 });
        meta.insert("stats", BlockHandle { offset: 3, size: 4 });

        let encoded = meta.encode();
        let decoded = MetaIndex::decode(&encoded).expect("decode metaindex");

        assert_eq!(decoded.get("filter.bloom"), Some(BlockHandle { offset: 1, size: 2 }));
        assert_eq!(decoded.get("stats"), Some(BlockHandle { offset: 3, size: 4 }));
    }
}
