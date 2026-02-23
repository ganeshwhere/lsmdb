use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use super::arena::{Arena, ArenaSlice, DEFAULT_ARENA_BLOCK_SIZE_BYTES};

pub const MAX_LEVEL: usize = 12;

#[derive(Debug, Clone)]
struct Node {
    key: ArenaSlice,
    value: ArenaSlice,
    next: Vec<Option<usize>>,
}

impl Node {
    fn new(level: usize, key: ArenaSlice, value: ArenaSlice) -> Self {
        Self { key, value, next: vec![None; level] }
    }

    fn next_at(&self, level: usize) -> Option<usize> {
        self.next.get(level).copied().flatten()
    }
}

#[derive(Debug)]
struct SkipListInner {
    arena: Arena,
    nodes: Vec<Node>,
    len: usize,
    level: usize,
}

#[derive(Debug)]
pub struct SkipList {
    inner: RwLock<SkipListInner>,
    seed: AtomicU64,
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new(DEFAULT_ARENA_BLOCK_SIZE_BYTES)
    }
}

impl SkipList {
    pub fn new(arena_block_size: usize) -> Self {
        let mut arena = Arena::new(arena_block_size);
        let empty = arena.allocate(&[]);

        let head = Node::new(MAX_LEVEL, empty, empty);
        let inner = SkipListInner { arena, nodes: vec![head], len: 0, level: 1 };

        Self { inner: RwLock::new(inner), seed: AtomicU64::new(initial_seed()) }
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let mut inner = self.inner.write();

        let mut updates = [0_usize; MAX_LEVEL];
        let candidate = find_greater_or_equal(&inner, key, Some(&mut updates));

        if let Some(existing) = candidate {
            if inner.arena.get(inner.nodes[existing].key) == key {
                let old_value = inner.arena.get(inner.nodes[existing].value).to_vec();
                let new_value = inner.arena.allocate(value);
                inner.nodes[existing].value = new_value;
                return Some(old_value);
            }
        }

        let node_level = self.random_level();
        if node_level > inner.level {
            for update in updates.iter_mut().take(node_level).skip(inner.level) {
                *update = 0;
            }
            inner.level = node_level;
        }

        let key_slice = inner.arena.allocate(key);
        let value_slice = inner.arena.allocate(value);

        let mut node = Node::new(node_level, key_slice, value_slice);
        let new_index = inner.nodes.len();

        for (level, update) in updates.iter().take(node_level).enumerate() {
            let prev = *update;
            node.next[level] = inner.nodes[prev].next_at(level);
        }

        inner.nodes.push(node);

        for (level, update) in updates.iter().take(node_level).enumerate() {
            let prev = *update;
            let next_ptr = inner
                .nodes
                .get_mut(prev)
                .and_then(|entry| entry.next.get_mut(level))
                .expect("skiplist predecessor must have pointer for current level");
            *next_ptr = Some(new_index);
        }

        inner.len += 1;
        None
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.read();
        let candidate = find_greater_or_equal(&inner, key, None)?;

        let node = &inner.nodes[candidate];
        if inner.arena.get(node.key) == key {
            return Some(inner.arena.get(node.value).to_vec());
        }

        None
    }

    pub fn iter(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let inner = self.inner.read();
        let mut entries = Vec::with_capacity(inner.len);

        let mut cursor = inner.nodes[0].next_at(0);
        while let Some(index) = cursor {
            let node = &inner.nodes[index];
            entries
                .push((inner.arena.get(node.key).to_vec(), inner.arena.get(node.value).to_vec()));
            cursor = node.next_at(0);
        }

        entries
    }

    pub fn len(&self) -> usize {
        self.inner.read().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn allocated_bytes(&self) -> usize {
        self.inner.read().arena.allocated_bytes()
    }

    fn random_level(&self) -> usize {
        let mut level = 1;
        while level < MAX_LEVEL {
            if (self.next_random() & 0b11) != 0 {
                break;
            }
            level += 1;
        }
        level
    }

    fn next_random(&self) -> u64 {
        let mut current = self.seed.load(AtomicOrdering::Relaxed);
        loop {
            let mut next = current;
            next ^= next << 13;
            next ^= next >> 7;
            next ^= next << 17;

            if next == 0 {
                next = 0x9E3779B97F4A7C15;
            }

            match self.seed.compare_exchange_weak(
                current,
                next,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }
}

fn find_greater_or_equal(
    inner: &SkipListInner,
    key: &[u8],
    mut updates: Option<&mut [usize; MAX_LEVEL]>,
) -> Option<usize> {
    let mut current = 0_usize;

    for level in (0..inner.level).rev() {
        loop {
            let next = inner.nodes[current].next_at(level);
            let Some(next_index) = next else {
                break;
            };

            match compare_node_key(inner, next_index, key) {
                Ordering::Less => current = next_index,
                Ordering::Equal | Ordering::Greater => break,
            }
        }

        if let Some(ref mut update_slots) = updates {
            update_slots[level] = current;
        }
    }

    inner.nodes[current].next_at(0)
}

fn compare_node_key(inner: &SkipListInner, node_index: usize, key: &[u8]) -> Ordering {
    inner.arena.get(inner.nodes[node_index].key).cmp(key)
}

fn initial_seed() -> u64 {
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos() as u64;
    epoch_nanos ^ ((std::process::id() as u64) << 16) | 1
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn insert_get_roundtrip() {
        let list = SkipList::default();
        list.insert(b"alpha", b"1");
        list.insert(b"beta", b"2");

        assert_eq!(list.get(b"alpha"), Some(b"1".to_vec()));
        assert_eq!(list.get(b"beta"), Some(b"2".to_vec()));
        assert_eq!(list.get(b"gamma"), None);
    }

    #[test]
    fn update_existing_key_returns_previous_value() {
        let list = SkipList::default();

        assert_eq!(list.insert(b"same", b"before"), None);
        assert_eq!(list.insert(b"same", b"after"), Some(b"before".to_vec()));
        assert_eq!(list.get(b"same"), Some(b"after".to_vec()));
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn iterator_returns_sorted_order() {
        let list = SkipList::default();
        list.insert(b"k3", b"v3");
        list.insert(b"k1", b"v1");
        list.insert(b"k2", b"v2");

        let entries = list.iter();
        let keys: Vec<Vec<u8>> = entries.into_iter().map(|(key, _)| key).collect();

        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()]);
    }

    #[test]
    fn supports_concurrent_inserts() {
        let list = Arc::new(SkipList::new(1024));
        let mut handles = Vec::new();

        let workers = 4;
        let per_worker = 250;

        for worker in 0..workers {
            let list = Arc::clone(&list);
            handles.push(thread::spawn(move || {
                for item in 0..per_worker {
                    let key = format!("{:04}", worker * per_worker + item);
                    let value = format!("value-{worker}-{item}");
                    list.insert(key.as_bytes(), value.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread should complete");
        }

        assert_eq!(list.len(), workers * per_worker);
        assert!(list.get(b"0000").is_some());
        assert!(list.get(b"0999").is_some());
    }
}
