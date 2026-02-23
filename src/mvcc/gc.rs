use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;

use super::transaction::{MvccStore, PruneStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    pub watermark_ts: u64,
    pub scanned_keys: u64,
    pub removed_versions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfig {
    pub interval: Duration,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self { interval: Duration::from_millis(100) }
    }
}

pub fn run_gc_once(store: &MvccStore) -> GcStats {
    let watermark_ts = store.gc_watermark_timestamp();
    let PruneStats { scanned_keys, removed_versions } =
        store.prune_versions_older_than(watermark_ts);

    GcStats { watermark_ts, scanned_keys, removed_versions }
}

#[derive(Debug)]
pub struct GcWorker {
    shutdown_tx: Sender<()>,
    handle: Option<JoinHandle<()>>,
    last_removed_versions: Arc<AtomicU64>,
}

#[derive(Debug, Error)]
pub enum GcWorkerError {
    #[error("failed to start MVCC GC worker thread: {0}")]
    Spawn(std::io::Error),
}

impl GcWorker {
    pub fn start(store: MvccStore, config: GcConfig) -> Result<Self, GcWorkerError> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
        let last_removed_versions = Arc::new(AtomicU64::new(0));
        let last_removed_versions_worker = Arc::clone(&last_removed_versions);

        let handle = thread::Builder::new()
            .name("lsmdb-mvcc-gc".to_string())
            .spawn(move || loop {
                match shutdown_rx.recv_timeout(config.interval) {
                    Ok(_) => break,
                    Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        let stats = run_gc_once(&store);
                        last_removed_versions_worker
                            .store(stats.removed_versions, Ordering::Release);
                    }
                }
            })
            .map_err(GcWorkerError::Spawn)?;

        Ok(Self { shutdown_tx, handle: Some(handle), last_removed_versions })
    }

    pub fn last_removed_versions(&self) -> u64 {
        self.last_removed_versions.load(Ordering::Acquire)
    }

    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        let _ = self.shutdown_tx.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for GcWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn gc_removes_obsolete_versions_without_active_snapshots() {
        let store = MvccStore::new();

        for value in [b"v1", b"v2", b"v3"] {
            let mut tx = store.begin_transaction();
            tx.put(b"k", value).expect("write version");
            tx.commit().expect("commit version");
        }

        assert_eq!(store.version_count_for_key(b"k"), 3);

        let stats = run_gc_once(&store);
        assert!(stats.removed_versions >= 2);
        assert_eq!(store.version_count_for_key(b"k"), 1);
    }

    #[test]
    fn gc_respects_active_snapshots() {
        let store = MvccStore::new();

        let mut tx = store.begin_transaction();
        tx.put(b"k", b"v1").expect("write v1");
        tx.commit().expect("commit v1");

        let snapshot = store.begin_transaction();

        for value in [b"v2", b"v3"] {
            let mut writer = store.begin_transaction();
            writer.put(b"k", value).expect("write new version");
            writer.commit().expect("commit new version");
        }

        let stats = run_gc_once(&store);
        assert_eq!(stats.removed_versions, 0);
        assert_eq!(snapshot.get(b"k").expect("snapshot read"), Some(b"v1".to_vec()));

        drop(snapshot);
        let stats = run_gc_once(&store);
        assert!(stats.removed_versions >= 1);
    }

    #[test]
    fn background_worker_runs_gc() {
        let store = MvccStore::new();

        for value in [b"a", b"b", b"c", b"d"] {
            let mut tx = store.begin_transaction();
            tx.put(b"x", value).expect("write");
            tx.commit().expect("commit");
        }

        let worker =
            GcWorker::start(store.clone(), GcConfig { interval: Duration::from_millis(20) })
                .expect("start worker");

        thread::sleep(Duration::from_millis(80));

        assert!(worker.last_removed_versions() >= 1 || store.version_count_for_key(b"x") <= 2);
        worker.stop();
    }
}
