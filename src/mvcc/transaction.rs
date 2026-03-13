use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, trace, warn};

use crate::storage::engine::{StorageEngine, StorageEngineOptions};

use super::snapshot::{Snapshot, SnapshotRegistry};
use super::timestamp::TimestampOracle;

const MVCC_DURABLE_STATE_KEY: &[u8] = b"__mvcc__/state";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    #[error("durable MVCC persistence error: {0}")]
    Persistence(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionMetrics {
    pub started: u64,
    pub committed: u64,
    pub rolled_back: u64,
    pub write_conflicts: u64,
    pub active_transactions: usize,
    pub recovered_keys: u64,
    pub recovered_versions: u64,
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
    durable_engine: Option<Arc<StorageEngine>>,
    metrics: TransactionMetricsState,
}

#[derive(Debug, Default)]
struct TransactionMetricsState {
    started: AtomicU64,
    committed: AtomicU64,
    rolled_back: AtomicU64,
    write_conflicts: AtomicU64,
    recovered_keys: AtomicU64,
    recovered_versions: AtomicU64,
    #[cfg(test)]
    crash_after_durable_commit: AtomicBool,
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
        Self::from_parts(MvccStoreData::default(), None, 0)
    }

    pub fn with_storage_engine(engine: Arc<StorageEngine>) -> Result<Self, TransactionError> {
        let data = load_durable_state(engine.as_ref())?;
        let max_commit_ts = max_commit_ts_for_data(&data);
        Ok(Self::from_parts(data, Some(engine), max_commit_ts))
    }

    pub fn open_persistent<P: AsRef<Path>>(root_dir: P) -> Result<Self, TransactionError> {
        Self::open_persistent_with_options(root_dir, StorageEngineOptions::default())
    }

    pub fn open_persistent_with_options<P: AsRef<Path>>(
        root_dir: P,
        options: StorageEngineOptions,
    ) -> Result<Self, TransactionError> {
        let engine = StorageEngine::open_with_options(root_dir, options).map_err(|err| {
            TransactionError::Persistence(format!("open storage engine failed: {err}"))
        })?;
        Self::with_storage_engine(Arc::new(engine))
    }

    pub fn is_durable(&self) -> bool {
        self.inner.durable_engine.is_some()
    }

    #[cfg(test)]
    fn set_crash_after_durable_commit_for_test(&self, enabled: bool) {
        self.inner.metrics.crash_after_durable_commit.store(enabled, Ordering::Relaxed);
    }

    fn from_parts(
        data: MvccStoreData,
        durable_engine: Option<Arc<StorageEngine>>,
        initial_timestamp: u64,
    ) -> Self {
        let recovered_keys = u64::try_from(data.versions.len()).unwrap_or(u64::MAX);
        let recovered_versions = data
            .versions
            .values()
            .map(|versions| u64::try_from(versions.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add);
        let metrics = TransactionMetricsState::default();
        metrics.recovered_keys.store(recovered_keys, Ordering::Relaxed);
        metrics.recovered_versions.store(recovered_versions, Ordering::Relaxed);

        Self {
            inner: Arc::new(MvccStoreInner {
                oracle: TimestampOracle::new(initial_timestamp),
                snapshots: SnapshotRegistry::default(),
                data: RwLock::new(data),
                durable_engine,
                metrics,
            }),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        let read_ts = self.inner.oracle.current();
        let snapshot = self.inner.snapshots.pin(read_ts);
        self.inner.metrics.started.fetch_add(1, Ordering::Relaxed);
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

    pub fn metrics(&self) -> TransactionMetrics {
        TransactionMetrics {
            started: self.inner.metrics.started.load(Ordering::Relaxed),
            committed: self.inner.metrics.committed.load(Ordering::Relaxed),
            rolled_back: self.inner.metrics.rolled_back.load(Ordering::Relaxed),
            write_conflicts: self.inner.metrics.write_conflicts.load(Ordering::Relaxed),
            active_transactions: self.active_snapshot_count(),
            recovered_keys: self.inner.metrics.recovered_keys.load(Ordering::Relaxed),
            recovered_versions: self.inner.metrics.recovered_versions.load(Ordering::Relaxed),
        }
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
        self.scan_prefix_at_with_observer(prefix, read_ts, |_| true)
    }

    pub fn scan_prefix_at_limited(
        &self,
        prefix: &[u8],
        read_ts: u64,
        max_rows: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_prefix_at_limited_with_observer(prefix, read_ts, max_rows, |_| true)
    }

    pub fn scan_prefix_at_with_observer<F>(
        &self,
        prefix: &[u8],
        read_ts: u64,
        observer: F,
    ) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        F: FnMut(usize) -> bool,
    {
        self.scan_prefix_at_limited_with_observer(prefix, read_ts, usize::MAX, observer)
    }

    pub fn scan_prefix_at_limited_with_observer<F>(
        &self,
        prefix: &[u8],
        read_ts: u64,
        max_rows: usize,
        mut observer: F,
    ) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        F: FnMut(usize) -> bool,
    {
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
                    if !observer(rows.len()) {
                        rows.sort_by(|a, b| a.0.cmp(&b.0));
                        return rows;
                    }
                    if rows.len() >= max_rows {
                        rows.sort_by(|a, b| a.0.cmp(&b.0));
                        return rows;
                    }
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
                        self.inner.metrics.write_conflicts.fetch_add(1, Ordering::Relaxed);
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
        let mut previous_versions = Vec::with_capacity(writes.len());

        for (key, value) in writes {
            previous_versions.push((key.clone(), data.versions.get(key).cloned()));
            let entry = data.versions.entry(key.clone()).or_default();
            entry.push(CommittedVersion { commit_ts, value: value.clone() });
        }

        if let Some(engine) = self.inner.durable_engine.as_ref() {
            if let Err(err) = persist_durable_state(engine.as_ref(), &data.versions) {
                for (key, previous) in previous_versions {
                    match previous {
                        Some(versions) => {
                            data.versions.insert(key, versions);
                        }
                        None => {
                            data.versions.remove(&key);
                        }
                    }
                }
                return Err(err);
            }
        }

        #[cfg(test)]
        if self.inner.durable_engine.is_some()
            && self.inner.metrics.crash_after_durable_commit.load(Ordering::Relaxed)
        {
            panic!("simulated crash after durable commit");
        }

        trace!(commit_ts, write_count = writes.len(), "commit applied");
        Ok(commit_ts)
    }

    pub(crate) fn prune_versions_older_than(&self, watermark_ts: u64) -> PruneStats {
        let mut data = self.inner.data.write();
        let mut stats = PruneStats::default();
        let mut previous_versions = Vec::new();

        for (key, versions) in data.versions.iter_mut() {
            stats.scanned_keys = stats.scanned_keys.saturating_add(1);

            if versions.len() <= 1 {
                continue;
            }

            let split = versions
                .iter()
                .position(|version| version.commit_ts > watermark_ts)
                .unwrap_or(versions.len());

            if split <= 1 {
                continue;
            }

            previous_versions.push((key.clone(), versions.clone()));
            let remove_count = split - 1;
            versions.drain(0..remove_count);
            stats.removed_versions = stats.removed_versions.saturating_add(remove_count as u64);
        }

        if stats.removed_versions > 0 {
            if let Some(engine) = self.inner.durable_engine.as_ref() {
                if let Err(err) = persist_durable_state(engine.as_ref(), &data.versions) {
                    warn!(error = %err, "failed to persist GC-pruned MVCC state; reverting prune");
                    for (key, versions) in previous_versions {
                        data.versions.insert(key, versions);
                    }
                    stats.removed_versions = 0;
                }
            }
        }

        stats
    }
}

fn load_durable_state(engine: &StorageEngine) -> Result<MvccStoreData, TransactionError> {
    let Some(raw) = engine.get(MVCC_DURABLE_STATE_KEY).map_err(|err| {
        TransactionError::Persistence(format!("load durable MVCC state failed: {err}"))
    })?
    else {
        return Ok(MvccStoreData::default());
    };

    let versions =
        bincode::deserialize::<HashMap<Vec<u8>, Vec<CommittedVersion>>>(&raw).map_err(|err| {
            TransactionError::Persistence(format!("decode durable MVCC state failed: {err}"))
        })?;

    Ok(MvccStoreData { versions })
}

fn persist_durable_state(
    engine: &StorageEngine,
    versions: &HashMap<Vec<u8>, Vec<CommittedVersion>>,
) -> Result<(), TransactionError> {
    let payload = bincode::serialize(versions).map_err(|err| {
        TransactionError::Persistence(format!("encode durable MVCC state failed: {err}"))
    })?;

    engine.put(MVCC_DURABLE_STATE_KEY, &payload).map_err(|err| {
        TransactionError::Persistence(format!("persist durable MVCC state failed: {err}"))
    })?;

    Ok(())
}

fn max_commit_ts_for_data(data: &MvccStoreData) -> u64 {
    data.versions
        .values()
        .filter_map(|versions| versions.last().map(|version| version.commit_ts))
        .max()
        .unwrap_or(0)
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

    pub fn scan_prefix_with_observer<F>(
        &self,
        prefix: &[u8],
        observer: F,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TransactionError>
    where
        F: FnMut(usize) -> bool,
    {
        self.scan_prefix_limited_with_observer(prefix, usize::MAX, observer)
    }

    pub fn scan_prefix_limited(
        &self,
        prefix: &[u8],
        max_rows: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TransactionError> {
        self.scan_prefix_limited_with_observer(prefix, max_rows, |_| true)
    }

    pub fn scan_prefix_limited_with_observer<F>(
        &self,
        prefix: &[u8],
        max_rows: usize,
        mut observer: F,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TransactionError>
    where
        F: FnMut(usize) -> bool,
    {
        if self.closed {
            return Err(TransactionError::Closed);
        }

        let read_ts = self.read_ts()?;
        let write_overlap = self.writes.keys().filter(|key| key.starts_with(prefix)).count();
        let fetch_limit = max_rows.saturating_add(write_overlap).saturating_add(1);
        let mut visible = self
            .store
            .scan_prefix_at_limited_with_observer(prefix, read_ts, fetch_limit, |seen| {
                observer(seen)
            })
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        let mut observed = visible.len();
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
            observed = observed.saturating_add(1);
            if !observer(observed) {
                break;
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

        self.store.inner.metrics.committed.fetch_add(1, Ordering::Relaxed);
        debug!(commit_ts, "transaction committed");
        Ok(commit_ts)
    }

    pub fn rollback(&mut self) {
        if self.closed {
            return;
        }

        debug!(write_count = self.writes.len(), "rollback transaction");
        self.writes.clear();
        self.closed = true;
        if let Some(mut snapshot) = self.snapshot.take() {
            snapshot.release();
        }
        self.store.inner.metrics.rolled_back.fetch_add(1, Ordering::Relaxed);
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
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::PathBuf;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_dir(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        path.push(format!("lsmdb-mvcc-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

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

    #[test]
    fn durable_store_recovers_versions_and_advances_timestamp_after_restart() {
        let dir = test_dir("durable-recovery");

        {
            let store = MvccStore::open_persistent(&dir).expect("open durable store");
            assert!(store.is_durable());

            let mut tx = store.begin_transaction();
            tx.put(b"k", b"v1").expect("write v1");
            assert_eq!(tx.commit().expect("commit v1"), 1);

            let mut tx = store.begin_transaction();
            tx.put(b"k", b"v2").expect("write v2");
            assert_eq!(tx.commit().expect("commit v2"), 2);
            assert_eq!(store.version_count_for_key(b"k"), 2);
        }

        {
            let store = MvccStore::open_persistent(&dir).expect("reopen durable store");
            assert_eq!(store.version_count_for_key(b"k"), 2);
            let reader = store.begin_transaction();
            assert_eq!(reader.get(b"k").expect("read latest"), Some(b"v2".to_vec()));

            let mut tx = store.begin_transaction();
            tx.put(b"k", b"v3").expect("write v3");
            let commit_ts = tx.commit().expect("commit v3");
            assert!(commit_ts >= 3);
        }

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn durable_store_survives_crash_before_commit_acknowledgment() {
        let dir = test_dir("crash-before-ack");

        {
            let store = MvccStore::open_persistent(&dir).expect("open durable store");
            store.set_crash_after_durable_commit_for_test(true);

            let mut tx = store.begin_transaction();
            tx.put(b"crash-key", b"persisted").expect("write");
            let crashed = catch_unwind(AssertUnwindSafe(|| {
                let _ = tx.commit();
            }));
            assert!(crashed.is_err());
        }

        {
            let store = MvccStore::open_persistent(&dir).expect("reopen durable store");
            let reader = store.begin_transaction();
            assert_eq!(
                reader.get(b"crash-key").expect("read after simulated crash"),
                Some(b"persisted".to_vec())
            );
        }

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn durable_store_reports_recovered_metrics_after_restart() {
        let dir = test_dir("recovered-metrics");

        {
            let store = MvccStore::open_persistent(&dir).expect("open durable store");

            let mut tx = store.begin_transaction();
            tx.put(b"m/a", b"1").expect("put a");
            tx.commit().expect("commit a");

            let mut tx = store.begin_transaction();
            tx.put(b"m/a", b"2").expect("update a");
            tx.put(b"m/b", b"3").expect("put b");
            tx.commit().expect("commit second batch");
        }

        {
            let store = MvccStore::open_persistent(&dir).expect("reopen durable store");
            let metrics = store.metrics();
            assert_eq!(metrics.recovered_keys, 2);
            assert_eq!(metrics.recovered_versions, 3);
        }

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }
}
