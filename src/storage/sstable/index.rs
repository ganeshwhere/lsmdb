use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn end_offset(self) -> u64 {
        self.offset.saturating_add(self.size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub last_key: Vec<u8>,
    pub handle: BlockHandle,
}

#[derive(Debug, Error)]
pub enum IndexBuildError {
    #[error("index keys must be sorted in ascending order")]
    UnsortedKeys,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IndexDecodeError {
    #[error("index block is truncated")]
    Truncated,
    #[error("index block key length is invalid")]
    InvalidKeyLength,
    #[error("index block entries are not sorted")]
    UnsortedEntries,
}

#[derive(Debug, Clone, Default)]
pub struct IndexBlock {
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    pub fn from_entries(entries: Vec<IndexEntry>) -> Result<Self, IndexBuildError> {
        for window in entries.windows(2) {
            if window[0].last_key >= window[1].last_key {
                return Err(IndexBuildError::UnsortedKeys);
            }
        }
        Ok(Self { entries })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_u32(&mut out, self.entries.len() as u32);

        for entry in &self.entries {
            push_u32(&mut out, entry.last_key.len() as u32);
            out.extend_from_slice(&entry.last_key);
            push_u64(&mut out, entry.handle.offset);
            push_u64(&mut out, entry.handle.size);
        }

        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IndexDecodeError> {
        let mut cursor = 0_usize;
        let entry_count = read_u32(bytes, &mut cursor)? as usize;

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let key_len = read_u32(bytes, &mut cursor)? as usize;
            let key_end = cursor.checked_add(key_len).ok_or(IndexDecodeError::InvalidKeyLength)?;

            if key_end > bytes.len() {
                return Err(IndexDecodeError::InvalidKeyLength);
            }

            let key = bytes[cursor..key_end].to_vec();
            cursor = key_end;

            let offset = read_u64(bytes, &mut cursor)?;
            let size = read_u64(bytes, &mut cursor)?;

            entries.push(IndexEntry { last_key: key, handle: BlockHandle { offset, size } });
        }

        if cursor != bytes.len() {
            return Err(IndexDecodeError::Truncated);
        }

        for window in entries.windows(2) {
            if window[0].last_key >= window[1].last_key {
                return Err(IndexDecodeError::UnsortedEntries);
            }
        }

        Ok(Self { entries })
    }

    pub fn find_block_for_key(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.find_block_index_for_key(key).and_then(|index| self.entries.get(index))
    }

    pub fn find_block_index_for_key(&self, key: &[u8]) -> Option<usize> {
        if self.entries.is_empty() {
            return None;
        }

        match self.entries.binary_search_by(|entry| entry.last_key.as_slice().cmp(key)) {
            Ok(index) => Some(index),
            Err(index) if index < self.entries.len() => Some(index),
            Err(_) => None,
        }
    }

    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn push_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, IndexDecodeError> {
    let end = cursor.checked_add(4).ok_or(IndexDecodeError::Truncated)?;
    if end > bytes.len() {
        return Err(IndexDecodeError::Truncated);
    }

    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, IndexDecodeError> {
    let end = cursor.checked_add(8).ok_or(IndexDecodeError::Truncated)?;
    if end > bytes.len() {
        return Err(IndexDecodeError::Truncated);
    }

    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(u64::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_roundtrip_and_binary_search() {
        let entries = vec![
            IndexEntry { last_key: b"k001".to_vec(), handle: BlockHandle { offset: 0, size: 10 } },
            IndexEntry { last_key: b"k010".to_vec(), handle: BlockHandle { offset: 10, size: 20 } },
            IndexEntry { last_key: b"k020".to_vec(), handle: BlockHandle { offset: 30, size: 40 } },
        ];

        let index = IndexBlock::from_entries(entries).expect("index should build");
        let encoded = index.encode();
        let decoded = IndexBlock::decode(&encoded).expect("index should decode");

        let first = decoded.find_block_for_key(b"k000").expect("first block must be selected");
        assert_eq!(first.handle.offset, 0);

        let middle = decoded.find_block_for_key(b"k011").expect("middle block must be selected");
        assert_eq!(middle.handle.offset, 30);

        assert!(decoded.find_block_for_key(b"k999").is_none());
    }

    #[test]
    fn rejects_unsorted_entries() {
        let entries = vec![
            IndexEntry { last_key: b"b".to_vec(), handle: BlockHandle { offset: 0, size: 1 } },
            IndexEntry { last_key: b"a".to_vec(), handle: BlockHandle { offset: 1, size: 1 } },
        ];

        let err = IndexBlock::from_entries(entries).expect_err("unsorted entries must fail");
        assert!(matches!(err, IndexBuildError::UnsortedKeys));
    }
}
