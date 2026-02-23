use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Debug)]
struct SnapshotRegistryInner {
    active: Mutex<BTreeMap<u64, usize>>,
}

impl SnapshotRegistryInner {
    fn pin(&self, read_ts: u64) {
        let mut active = self.active.lock();
        let counter = active.entry(read_ts).or_insert(0);
        *counter += 1;
    }

    fn unpin(&self, read_ts: u64) {
        let mut active = self.active.lock();
        if let Some(counter) = active.get_mut(&read_ts) {
            *counter = counter.saturating_sub(1);
            if *counter == 0 {
                active.remove(&read_ts);
            }
        }
    }

    fn oldest_active(&self) -> Option<u64> {
        self.active.lock().keys().next().copied()
    }

    fn active_count(&self) -> usize {
        self.active.lock().values().sum()
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotRegistry {
    inner: Arc<SnapshotRegistryInner>,
}

impl Default for SnapshotRegistry {
    fn default() -> Self {
        Self { inner: Arc::new(SnapshotRegistryInner { active: Mutex::new(BTreeMap::new()) }) }
    }
}

impl SnapshotRegistry {
    pub fn pin(&self, read_ts: u64) -> Snapshot {
        self.inner.pin(read_ts);
        Snapshot { read_ts, inner: Arc::clone(&self.inner), released: false }
    }

    pub fn oldest_active_timestamp(&self) -> Option<u64> {
        self.inner.oldest_active()
    }

    pub fn active_snapshot_count(&self) -> usize {
        self.inner.active_count()
    }
}

#[derive(Debug)]
pub struct Snapshot {
    read_ts: u64,
    inner: Arc<SnapshotRegistryInner>,
    released: bool,
}

impl Snapshot {
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    pub fn release(&mut self) {
        if self.released {
            return;
        }

        self.inner.unpin(self.read_ts);
        self.released = true;
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_oldest_active_snapshot() {
        let registry = SnapshotRegistry::default();
        let first = registry.pin(10);
        let second = registry.pin(20);
        let third = registry.pin(5);

        assert_eq!(registry.active_snapshot_count(), 3);
        assert_eq!(registry.oldest_active_timestamp(), Some(5));

        drop(third);
        assert_eq!(registry.oldest_active_timestamp(), Some(10));

        drop(first);
        drop(second);
        assert_eq!(registry.oldest_active_timestamp(), None);
        assert_eq!(registry.active_snapshot_count(), 0);
    }

    #[test]
    fn release_is_idempotent() {
        let registry = SnapshotRegistry::default();
        let mut snapshot = registry.pin(7);

        snapshot.release();
        snapshot.release();

        assert_eq!(registry.active_snapshot_count(), 0);
        assert_eq!(registry.oldest_active_timestamp(), None);
    }
}
