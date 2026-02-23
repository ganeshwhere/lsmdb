use std::cmp::Ordering;

use thiserror::Error;

pub const DEFAULT_RESTART_INTERVAL: usize = 16;
const ENTRY_HEADER_SIZE_BYTES: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishedBlock {
    pub bytes: Vec<u8>,
    pub last_key: Vec<u8>,
    pub entry_count: usize,
}

#[derive(Debug, Error)]
pub enum BlockBuildError {
    #[error("invalid restart interval: {0}")]
    InvalidRestartInterval(usize),
    #[error("keys must be strictly increasing inside a data block")]
    KeysNotSorted,
    #[error("cannot finish an empty data block")]
    EmptyBlock,
    #[error("block grew beyond u32 offset limits")]
    BlockTooLarge,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockDecodeError {
    #[error("block is too small to contain restart metadata")]
    BlockTooSmall,
    #[error("restart metadata is truncated")]
    TruncatedRestartSection,
    #[error("invalid restart count: {0}")]
    InvalidRestartCount(u32),
    #[error("restart offsets must start at 0")]
    MissingZeroRestart,
    #[error("restart offset {0} is out of bounds")]
    InvalidRestartOffset(usize),
    #[error("entry is truncated at offset {0}")]
    TruncatedEntry(usize),
    #[error("entry shared prefix is invalid at offset {0}")]
    InvalidSharedPrefix(usize),
    #[error("entry payload length is invalid at offset {0}")]
    InvalidEntryLength(usize),
}

#[derive(Debug, Clone)]
pub struct DataBlockBuilder {
    restart_interval: usize,
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    entries_since_restart: usize,
    entry_count: usize,
}

impl Default for DataBlockBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_RESTART_INTERVAL).expect("default restart interval is always valid")
    }
}

impl DataBlockBuilder {
    pub fn new(restart_interval: usize) -> Result<Self, BlockBuildError> {
        if restart_interval == 0 {
            return Err(BlockBuildError::InvalidRestartInterval(restart_interval));
        }

        Ok(Self {
            restart_interval,
            buffer: Vec::new(),
            restarts: vec![0],
            last_key: Vec::new(),
            entries_since_restart: 0,
            entry_count: 0,
        })
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), BlockBuildError> {
        if self.entry_count > 0 && key <= self.last_key.as_slice() {
            return Err(BlockBuildError::KeysNotSorted);
        }

        let mut shared_prefix = 0_usize;
        if self.entries_since_restart >= self.restart_interval {
            self.restarts.push(
                u32::try_from(self.buffer.len()).map_err(|_| BlockBuildError::BlockTooLarge)?,
            );
            self.entries_since_restart = 0;
        } else {
            shared_prefix = shared_prefix_len(&self.last_key, key);
        }

        let non_shared = key.len().saturating_sub(shared_prefix);
        push_u32(
            &mut self.buffer,
            u32::try_from(shared_prefix).map_err(|_| BlockBuildError::BlockTooLarge)?,
        );
        push_u32(
            &mut self.buffer,
            u32::try_from(non_shared).map_err(|_| BlockBuildError::BlockTooLarge)?,
        );
        push_u32(
            &mut self.buffer,
            u32::try_from(value.len()).map_err(|_| BlockBuildError::BlockTooLarge)?,
        );
        self.buffer.extend_from_slice(&key[shared_prefix..]);
        self.buffer.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        self.entry_count += 1;

        Ok(())
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.buffer.len()
            + (self.restarts.len() * std::mem::size_of::<u32>())
            + std::mem::size_of::<u32>()
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn finish(mut self) -> Result<FinishedBlock, BlockBuildError> {
        if self.entry_count == 0 {
            return Err(BlockBuildError::EmptyBlock);
        }

        for offset in &self.restarts {
            push_u32(&mut self.buffer, *offset);
        }
        push_u32(
            &mut self.buffer,
            u32::try_from(self.restarts.len()).map_err(|_| BlockBuildError::BlockTooLarge)?,
        );

        Ok(FinishedBlock {
            bytes: self.buffer,
            last_key: self.last_key,
            entry_count: self.entry_count,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DataBlock {
    raw: Vec<u8>,
    restart_offsets: Vec<usize>,
    restart_keys: Vec<Vec<u8>>,
    restart_section_offset: usize,
    entry_count: usize,
}

impl DataBlock {
    pub fn from_bytes(raw: Vec<u8>) -> Result<Self, BlockDecodeError> {
        if raw.len() < std::mem::size_of::<u32>() {
            return Err(BlockDecodeError::BlockTooSmall);
        }

        let restart_count = read_u32(&raw, raw.len() - 4)? as usize;
        if restart_count == 0 {
            return Err(BlockDecodeError::InvalidRestartCount(0));
        }

        let restart_bytes = restart_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(BlockDecodeError::TruncatedRestartSection)?;

        if raw.len() < 4 + restart_bytes {
            return Err(BlockDecodeError::TruncatedRestartSection);
        }

        let restart_section_offset = raw.len() - 4 - restart_bytes;

        let mut restart_offsets = Vec::with_capacity(restart_count);
        for i in 0..restart_count {
            let pos = restart_section_offset + (i * 4);
            let offset = read_u32(&raw, pos)? as usize;
            if offset >= restart_section_offset {
                return Err(BlockDecodeError::InvalidRestartOffset(offset));
            }
            restart_offsets.push(offset);
        }

        if restart_offsets.first().copied() != Some(0) {
            return Err(BlockDecodeError::MissingZeroRestart);
        }

        let mut restart_keys = Vec::with_capacity(restart_offsets.len());
        for &offset in &restart_offsets {
            let (_, key, _) = decode_entry(&raw, restart_section_offset, offset, &[])?;
            restart_keys.push(key);
        }

        let mut entry_count = 0_usize;
        let mut cursor = 0_usize;
        let mut previous_key = Vec::new();

        while cursor < restart_section_offset {
            let (next, key, _) = decode_entry(&raw, restart_section_offset, cursor, &previous_key)?;
            previous_key = key;
            cursor = next;
            entry_count += 1;
        }

        if cursor != restart_section_offset {
            return Err(BlockDecodeError::InvalidEntryLength(cursor));
        }

        Ok(Self { raw, restart_offsets, restart_keys, restart_section_offset, entry_count })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.entry_count == 0 {
            return None;
        }

        let restart_index =
            match self.restart_keys.binary_search_by(|candidate| candidate.as_slice().cmp(key)) {
                Ok(index) => index,
                Err(0) => 0,
                Err(index) => index.saturating_sub(1),
            };

        let mut cursor = self.restart_offsets[restart_index];
        let mut previous_key = Vec::new();

        while cursor < self.restart_section_offset {
            let (next, decoded_key, value) =
                decode_entry(&self.raw, self.restart_section_offset, cursor, &previous_key).ok()?;

            match decoded_key.as_slice().cmp(key) {
                Ordering::Less => {
                    previous_key = decoded_key;
                    cursor = next;
                }
                Ordering::Equal => return Some(value),
                Ordering::Greater => return None,
            }
        }

        None
    }

    pub fn iter(&self) -> DataBlockIterator<'_> {
        DataBlockIterator { block: self, cursor: 0, previous_key: Vec::new() }
    }

    pub fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.iter().collect()
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }
}

pub struct DataBlockIterator<'a> {
    block: &'a DataBlock,
    cursor: usize,
    previous_key: Vec<u8>,
}

impl Iterator for DataBlockIterator<'_> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.block.restart_section_offset {
            return None;
        }

        let (next, key, value) = decode_entry(
            &self.block.raw,
            self.block.restart_section_offset,
            self.cursor,
            &self.previous_key,
        )
        .ok()?;

        self.cursor = next;
        self.previous_key = key.clone();
        Some((key, value))
    }
}

fn decode_entry(
    raw: &[u8],
    data_section_end: usize,
    offset: usize,
    previous_key: &[u8],
) -> Result<(usize, Vec<u8>, Vec<u8>), BlockDecodeError> {
    if offset + ENTRY_HEADER_SIZE_BYTES > data_section_end {
        return Err(BlockDecodeError::TruncatedEntry(offset));
    }

    let shared = read_u32(raw, offset)? as usize;
    let non_shared = read_u32(raw, offset + 4)? as usize;
    let value_len = read_u32(raw, offset + 8)? as usize;

    if shared > previous_key.len() {
        return Err(BlockDecodeError::InvalidSharedPrefix(offset));
    }

    let payload_offset = offset + ENTRY_HEADER_SIZE_BYTES;
    let key_end = payload_offset
        .checked_add(non_shared)
        .ok_or(BlockDecodeError::InvalidEntryLength(offset))?;
    let value_end =
        key_end.checked_add(value_len).ok_or(BlockDecodeError::InvalidEntryLength(offset))?;

    if value_end > data_section_end {
        return Err(BlockDecodeError::InvalidEntryLength(offset));
    }

    let mut key = Vec::with_capacity(shared + non_shared);
    key.extend_from_slice(&previous_key[..shared]);
    key.extend_from_slice(&raw[payload_offset..key_end]);

    Ok((value_end, key, raw[key_end..value_end].to_vec()))
}

fn shared_prefix_len(previous: &[u8], current: &[u8]) -> usize {
    let upper = previous.len().min(current.len());
    let mut index = 0;
    while index < upper && previous[index] == current[index] {
        index += 1;
    }
    index
}

fn push_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(raw: &[u8], offset: usize) -> Result<u32, BlockDecodeError> {
    if offset + 4 > raw.len() {
        return Err(BlockDecodeError::TruncatedEntry(offset));
    }

    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&raw[offset..offset + 4]);
    Ok(u32::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_roundtrip_and_point_lookup() {
        let mut builder = DataBlockBuilder::new(2).expect("builder should initialize");
        builder.add(b"apple", b"1").expect("add apple");
        builder.add(b"apricot", b"2").expect("add apricot");
        builder.add(b"banana", b"3").expect("add banana");

        let finished = builder.finish().expect("finish block");
        let block = DataBlock::from_bytes(finished.bytes).expect("decode block");

        assert_eq!(block.get(b"apple"), Some(b"1".to_vec()));
        assert_eq!(block.get(b"banana"), Some(b"3".to_vec()));
        assert_eq!(block.get(b"blueberry"), None);
    }

    #[test]
    fn iterator_returns_sorted_entries() {
        let mut builder = DataBlockBuilder::new(DEFAULT_RESTART_INTERVAL).expect("builder init");
        for i in 0..32_u32 {
            let key = format!("k{i:04}");
            let value = format!("v{i:04}");
            builder.add(key.as_bytes(), value.as_bytes()).expect("insert sorted key");
        }

        let finished = builder.finish().expect("finish block");
        let block = DataBlock::from_bytes(finished.bytes).expect("decode block");

        let mut previous = None::<Vec<u8>>;
        for (key, _) in block.iter() {
            if let Some(prev) = previous.as_ref() {
                assert!(prev < &key);
            }
            previous = Some(key);
        }

        assert_eq!(block.entry_count(), 32);
    }

    #[test]
    fn rejects_unsorted_keys() {
        let mut builder = DataBlockBuilder::new(4).expect("builder init");
        builder.add(b"b", b"1").expect("first add");

        let err = builder.add(b"a", b"2").expect_err("out-of-order keys should fail");
        assert!(matches!(err, BlockBuildError::KeysNotSorted));
    }
}
