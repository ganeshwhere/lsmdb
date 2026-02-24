use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;
use tracing::{debug, trace, warn};

use super::snapshot::{Snapshot, SnapshotRegistry};
use super::timestamp::TimestampOracle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedVersion {
    pub commit_ts: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransactionError {
    #[error(
        "write-write conflict on key '{key}': read_ts={read_ts}, conflicting_commit_ts={conflicting_commit_ts}"
    )]
    WriteWriteConflict { key: String, read_ts: u64, conflicting_commit_ts: u64 },
    #[error("transaction is no longer active")]
    Closed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneStats {
    pub scanned_keys: u64,
    pub removed_versions: u64,
}

#[derive(Debug, Default)]
struct MvccStoreData {
    versions: HashMap<Vec<u8>, Vec<CommittedVersion>>,
}

#[derive(Debug)]
struct MvccStoreInner {
    oracle: TimestampOracle,
    snapshots: SnapshotRegistry,
    data: RwLock<MvccStoreData>,
}

#[derive(Debug, Clone)]
pub struct MvccStore {
    inner: Arc<MvccStoreInner>,
}

impl Default for MvccStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MvccStoreInner {
                oracle: TimestampOracle::default(),
                snapshots: SnapshotRegistry::default(),
                data: RwLock::new(MvccStoreData::default()),
            }),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        let read_ts = self.inner.oracle.current();
        let snapshot = self.inner.snapshots.pin(read_ts);
        debug!(read_ts, "begin transaction");
        Transaction {
            store: self.clone(),
            snapshot: Some(snapshot),
            writes: BTreeMap::new(),
            closed: false,
        }
    }

    pub fn current_timestamp(&self) -> u64 {
        self.inner.oracle.current()
    }

    pub fn oldest_active_snapshot_timestamp(&self) -> Option<u64> {
        self.inner.snapshots.oldest_active_timestamp()
    }

    pub fn active_snapshot_count(&self) -> usize {
        self.inner.snapshots.active_snapshot_count()
    }

    pub fn gc_watermark_timestamp(&self) -> u64 {
        self.oldest_active_snapshot_timestamp().unwrap_or_else(|| self.current_timestamp())
    }

    pub fn read_at(&self, key: &[u8], read_ts: u64) -> Option<Vec<u8>> {
        let data = self.inner.data.read();
        let versions = data.versions.get(key)?;

        for version in versions.iter().rev() {
            if version.commit_ts <= read_ts {
                return version.value.clone();
            }
        }

        None
    }

    pub fn latest_commit_ts_for_key(&self, key: &[u8]) -> Option<u64> {
        self.inner
            .data
            .read()
            .versions
            .get(key)
            .and_then(|versions| versions.last())
            .map(|version| version.commit_ts)
    }

    pub fn version_count_for_key(&self, key: &[u8]) -> usize {
        self.inner.data.read().versions.get(key).map(Vec::len).unwrap_or(0)
    }

    pub fn versions_for_key(&self, key: &[u8]) -> Vec<CommittedVersion> {
        self.inner.data.read().versions.get(key).cloned().unwrap_or_default()
    }

    pub fn scan_prefix_at(&self, prefix: &[u8], read_ts: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.inner.data.read();
        let mut rows = Vec::new();

        for (key, versions) in &data.versions {
            if !key.starts_with(prefix) {
                continue;
            }

            for version in versions.iter().rev() {
                if version.commit_ts > read_ts {
                    continue;
                }

                if let Some(value) = &version.value {
                    rows.push((key.clone(), value.clone()));
                }
                break;
            }
        }

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    pub fn scan_prefix_latest(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_prefix_at(prefix, self.current_timestamp())
    }

    pub(crate) fn commit_writes(
        &self,
        read_ts: u64,
        writes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<u64, TransactionError> {
        if writes.is_empty() {
            trace!(read_ts, "commit with empty write set");
            return Ok(read_ts);
        }
        trace!(read_ts, write_count = writes.len(), "commit write set");

        let mut data = self.inner.data.write();

        for key in writes.keys() {
            if let Some(versions) = data.versions.get(key) {
                if let Some(latest) = versions.last() {
                    if latest.commit_ts > read_ts {
                        warn!(
                            key = %String::from_utf8_lossy(key),
                            read_ts,
                            conflicting_commit_ts = latest.commit_ts,
                            "write-write conflict detected"
                        );
                        return Err(TransactionError::WriteWriteConflict {
                            key: String::from_utf8_lossy(key).into_owned(),
                            read_ts,
                            conflicting_commit_ts: latest.commit_ts,
                        });
                    }
                }
            }
        }

        let commit_ts = self.inner.oracle.next_timestamp();

        for (key, value) in writes {
            let entry = data.versions.entry(key.clone()).or_default();
            entry.push(CommittedVersion { commit_ts, value: value.clone() });
        }

        trace!(commit_ts, write_count = writes.len(), "commit applied");
        Ok(commit_ts)
    }

    pub(crate) fn prune_versions_older_than(&self, watermark_ts: u64) -> PruneStats {
        let mut data = self.inner.data.write();
        let mut stats = PruneStats::default();

        for versions in data.versions.values_mut() {
            stats.scanned_keys = stats.scanned_keys.saturating_add(1);

            if versions.len() <= 1 {
                continue;
            }

            let split = versions
                .iter()
                .position(|version| version.commit_ts >= watermark_ts)
                .unwrap_or(versions.len());

            if split <= 1 {
                continue;
            }

            let remove_count = split - 1;
            versions.drain(0..remove_count);
            stats.removed_versions = stats.removed_versions.saturating_add(remove_count as u64);
        }

        stats
    }
}

#[derive(Debug)]
pub struct Transaction {
    store: MvccStore,
    snapshot: Option<Snapshot>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    closed: bool,
}

impl Transaction {
    pub fn read_ts(&self) -> Result<u64, TransactionError> {
        let snapshot = self.snapshot.as_ref().ok_or(TransactionError::Closed)?;
        Ok(snapshot.read_ts())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TransactionError> {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        if let Some(value) = self.writes.get(key) {
            return Ok(value.clone());
        }

        let read_ts = self.read_ts()?;
        Ok(self.store.read_at(key, read_ts))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TransactionError> {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        let read_ts = self.read_ts()?;
        let mut visible =
            self.store.scan_prefix_at(prefix, read_ts).into_iter().collect::<BTreeMap<_, _>>();

        for (key, value) in &self.writes {
            if !key.starts_with(prefix) {
                continue;
            }

            match value {
                Some(value) => {
                    visible.insert(key.clone(), value.clone());
                }
                None => {
                    visible.remove(key);
                }
            }
        }

        Ok(visible.into_iter().collect())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), TransactionError> {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        self.writes.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), TransactionError> {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<u64, TransactionError> {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        let read_ts = self.read_ts()?;
        debug!(read_ts, write_count = self.writes.len(), "commit transaction");
        let commit_ts = self.store.commit_writes(read_ts, &self.writes)?;

        self.writes.clear();
        self.closed = true;
        if let Some(mut snapshot) = self.snapshot.take() {
            snapshot.release();
        }

        debug!(commit_ts, "transaction committed");
        Ok(commit_ts)
    }

    pub fn rollback(&mut self) {
        debug!(write_count = self.writes.len(), "rollback transaction");
        self.writes.clear();
        self.closed = true;
        if let Some(mut snapshot) = self.snapshot.take() {
            snapshot.release();
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.closed {
            self.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn transaction_commit_and_visibility() {
        let store = MvccStore::new();

        let mut tx = store.begin_transaction();
        tx.put(b"alpha", b"1").expect("put alpha");
        let commit_ts = tx.commit().expect("commit alpha");
        assert_eq!(commit_ts, 1);

        let reader = store.begin_transaction();
        assert_eq!(reader.get(b"alpha").expect("read alpha"), Some(b"1".to_vec()));
    }

    #[test]
    fn detects_write_write_conflict() {
        let store = MvccStore::new();

        let mut tx_a = store.begin_transaction();
        let mut tx_b = store.begin_transaction();

        tx_a.put(b"same", b"A").expect("tx_a write");
        tx_b.put(b"same", b"B").expect("tx_b write");

        tx_a.commit().expect("first commit should succeed");
        let err = tx_b.commit().expect_err("second commit should conflict");

        assert!(matches!(err, TransactionError::WriteWriteConflict { .. }));
    }

    #[test]
    fn snapshot_reads_are_stable() {
        let store = MvccStore::new();

        let mut seed = store.begin_transaction();
        seed.put(b"k", b"v1").expect("seed write");
        seed.commit().expect("seed commit");

        let snapshot_reader = store.begin_transaction();

        let mut writer = store.begin_transaction();
        writer.put(b"k", b"v2").expect("writer put");
        writer.commit().expect("writer commit");

        assert_eq!(snapshot_reader.get(b"k").expect("snapshot read"), Some(b"v1".to_vec()));

        let latest = store.begin_transaction();
        assert_eq!(latest.get(b"k").expect("latest read"), Some(b"v2".to_vec()));
    }

    #[test]
    fn concurrent_commits_get_unique_timestamps() {
        let store = MvccStore::new();
        let mut handles = Vec::new();

        for worker in 0..4_u8 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                let mut tx = store.begin_transaction();
                let key = format!("k-{worker}");
                tx.put(key.as_bytes(), b"x").expect("write");
                tx.commit().expect("commit")
            }));
        }

        let mut commits = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread"))
            .collect::<Vec<_>>();
        commits.sort_unstable();
        commits.dedup();

        assert_eq!(commits.len(), 4);
    }

    #[test]
    fn scan_prefix_is_snapshot_aware() {
        let store = MvccStore::new();

        let mut seed = store.begin_transaction();
        seed.put(b"prefix/a", b"v1").expect("seed prefix/a");
        seed.put(b"prefix/b", b"v1").expect("seed prefix/b");
        seed.put(b"other/x", b"v1").expect("seed other/x");
        seed.commit().expect("seed commit");

        let snapshot = store.begin_transaction();
        let snapshot_ts = snapshot.read_ts().expect("snapshot ts");

        let mut writer = store.begin_transaction();
        writer.delete(b"prefix/a").expect("delete prefix/a");
        writer.put(b"prefix/b", b"v2").expect("update prefix/b");
        writer.commit().expect("writer commit");

        let historical = store.scan_prefix_at(b"prefix/", snapshot_ts);
        assert_eq!(
            historical,
            vec![(b"prefix/a".to_vec(), b"v1".to_vec()), (b"prefix/b".to_vec(), b"v1".to_vec())]
        );

        let latest = store.scan_prefix_latest(b"prefix/");
        assert_eq!(latest, vec![(b"prefix/b".to_vec(), b"v2".to_vec())]);
    }

    #[test]
    fn transaction_prefix_scan_includes_uncommitted_writes() {
        let store = MvccStore::new();

        let mut seed = store.begin_transaction();
        seed.put(b"k/a", b"v1").expect("seed a");
        seed.put(b"k/b", b"v1").expect("seed b");
        seed.commit().expect("seed commit");

        let mut tx = store.begin_transaction();
        tx.put(b"k/c", b"v2").expect("write c");
        tx.delete(b"k/a").expect("delete a");
        tx.put(b"other/x", b"ignore").expect("write other");

        let rows = tx.scan_prefix(b"k/").expect("prefix scan");
        assert_eq!(
            rows,
            vec![(b"k/b".to_vec(), b"v1".to_vec()), (b"k/c".to_vec(), b"v2".to_vec())]
        );
    }
}
