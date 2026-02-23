pub mod arena;
pub mod skiplist;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use self::skiplist::SkipList;

pub const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Delete = 0,
    Put = 1,
}

impl ValueType {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Delete),
            1 => Some(Self::Put),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub sequence: u64,
    pub value_type: ValueType,
}

pub fn encode_internal_key(user_key: &[u8], sequence: u64, value_type: ValueType) -> Vec<u8> {
    let mut key = Vec::with_capacity(user_key.len() + 9);
    key.extend_from_slice(user_key);
    key.extend_from_slice(&sequence.to_be_bytes());
    key.push(value_type as u8);
    key
}

pub fn decode_internal_key(key: &[u8]) -> Option<DecodedInternalKey<'_>> {
    if key.len() < 9 {
        return None;
    }

    let user_key_end = key.len() - 9;
    let mut sequence_bytes = [0_u8; 8];
    sequence_bytes.copy_from_slice(&key[user_key_end..user_key_end + 8]);

    let value_type = ValueType::from_byte(key[key.len() - 1])?;

    Some(DecodedInternalKey {
        user_key: &key[..user_key_end],
        sequence: u64::from_be_bytes(sequence_bytes),
        value_type,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    pub internal_key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct MemTable {
    data: SkipList,
    approximate_size_bytes: AtomicUsize,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new(arena::DEFAULT_ARENA_BLOCK_SIZE_BYTES)
    }
}

impl MemTable {
    pub fn new(arena_block_size_bytes: usize) -> Self {
        Self {
            data: SkipList::new(arena_block_size_bytes),
            approximate_size_bytes: AtomicUsize::new(0),
        }
    }

    pub fn put(&self, user_key: &[u8], sequence: u64, value: &[u8]) -> Option<Vec<u8>> {
        let internal_key = encode_internal_key(user_key, sequence, ValueType::Put);
        self.insert_internal(&internal_key, value)
    }

    pub fn delete(&self, user_key: &[u8], sequence: u64) -> Option<Vec<u8>> {
        let internal_key = encode_internal_key(user_key, sequence, ValueType::Delete);
        self.insert_internal(&internal_key, &[])
    }

    pub fn insert_internal(&self, internal_key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let previous = self.data.insert(internal_key, value);

        match previous.as_ref() {
            Some(old_value) => {
                if value.len() >= old_value.len() {
                    self.approximate_size_bytes
                        .fetch_add(value.len() - old_value.len(), Ordering::Relaxed);
                } else {
                    self.sub_size(old_value.len() - value.len());
                }
            }
            None => {
                self.approximate_size_bytes
                    .fetch_add(internal_key.len() + value.len(), Ordering::Relaxed);
            }
        }

        previous
    }

    pub fn get_internal(&self, internal_key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(internal_key)
    }

    pub fn ordered_entries(&self) -> Vec<MemTableEntry> {
        self.data
            .iter()
            .into_iter()
            .map(|(internal_key, value)| MemTableEntry { internal_key, value })
            .collect()
    }

    pub fn iter(&self) -> MemTableIterator {
        MemTableIterator { inner: self.ordered_entries().into_iter() }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn approximate_size_bytes(&self) -> usize {
        self.approximate_size_bytes.load(Ordering::Relaxed)
    }

    fn sub_size(&self, delta: usize) {
        let mut current = self.approximate_size_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(delta);
            match self.approximate_size_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

pub struct MemTableIterator {
    inner: std::vec::IntoIter<MemTableEntry>,
}

impl Iterator for MemTableIterator {
    type Item = MemTableEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug)]
pub struct MemTableManager {
    mutable: Arc<MemTable>,
    immutables: Vec<Arc<MemTable>>,
    max_memtable_size_bytes: usize,
    arena_block_size_bytes: usize,
}

impl Default for MemTableManager {
    fn default() -> Self {
        Self::new(DEFAULT_MEMTABLE_SIZE_BYTES, arena::DEFAULT_ARENA_BLOCK_SIZE_BYTES)
    }
}

impl MemTableManager {
    pub fn new(max_memtable_size_bytes: usize, arena_block_size_bytes: usize) -> Self {
        let max_memtable_size_bytes = max_memtable_size_bytes.max(1);

        Self {
            mutable: Arc::new(MemTable::new(arena_block_size_bytes)),
            immutables: Vec::new(),
            max_memtable_size_bytes,
            arena_block_size_bytes,
        }
    }

    pub fn mutable(&self) -> Arc<MemTable> {
        Arc::clone(&self.mutable)
    }

    pub fn immutables(&self) -> &[Arc<MemTable>] {
        &self.immutables
    }

    pub fn put(&mut self, user_key: &[u8], sequence: u64, value: &[u8]) -> bool {
        self.mutable.put(user_key, sequence, value);
        self.promote_if_needed()
    }

    pub fn delete(&mut self, user_key: &[u8], sequence: u64) -> bool {
        self.mutable.delete(user_key, sequence);
        self.promote_if_needed()
    }

    pub fn insert_internal(&mut self, internal_key: &[u8], value: &[u8]) -> bool {
        self.mutable.insert_internal(internal_key, value);
        self.promote_if_needed()
    }

    pub fn promote_if_needed(&mut self) -> bool {
        if self.mutable.approximate_size_bytes() < self.max_memtable_size_bytes {
            return false;
        }

        self.promote_mutable();
        true
    }

    pub fn promote_mutable(&mut self) {
        let next = Arc::new(MemTable::new(self.arena_block_size_bytes));
        let old = std::mem::replace(&mut self.mutable, next);
        self.immutables.push(old);
    }

    pub fn take_immutables(&mut self) -> Vec<Arc<MemTable>> {
        std::mem::take(&mut self.immutables)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn internal_key_roundtrip() {
        let encoded = encode_internal_key(b"users:42", 77, ValueType::Put);
        let decoded = decode_internal_key(&encoded).expect("decode should succeed");

        assert_eq!(decoded.user_key, b"users:42");
        assert_eq!(decoded.sequence, 77);
        assert_eq!(decoded.value_type, ValueType::Put);
    }

    #[test]
    fn memtable_orders_entries_by_internal_key() {
        let table = MemTable::default();

        table.insert_internal(b"k2", b"v2");
        table.insert_internal(b"k1", b"v1");
        table.insert_internal(b"k3", b"v3");

        let keys: Vec<Vec<u8>> = table.iter().map(|entry| entry.internal_key).collect();
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()]);
    }

    #[test]
    fn manager_promotes_mutable_when_size_limit_is_reached() {
        let mut manager = MemTableManager::new(20, 32);

        let promoted = manager.put(b"alpha", 1, b"1234567890");
        assert!(!promoted);

        let promoted = manager.put(b"beta", 2, b"1234567890");
        assert!(promoted);

        assert_eq!(manager.immutables().len(), 1);
        assert!(manager.mutable().is_empty());
    }

    #[test]
    fn memtable_accepts_concurrent_writes() {
        let table = Arc::new(MemTable::default());
        let mut handles = Vec::new();

        let workers = 4;
        let per_worker = 150;

        for worker in 0..workers {
            let table = Arc::clone(&table);
            handles.push(thread::spawn(move || {
                for i in 0..per_worker {
                    let key = format!("key-{worker}-{i}");
                    let value = format!("value-{worker}-{i}");
                    table.put(key.as_bytes(), i as u64, value.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().expect("writer thread should complete");
        }

        assert_eq!(table.len(), workers * per_worker);
    }
}
