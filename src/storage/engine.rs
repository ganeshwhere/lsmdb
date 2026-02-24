use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use super::compaction::CompactionMetrics;
use super::manifest::version::SSTableMetadata;
use super::manifest::{Manifest, ManifestError, VersionEdit};
use super::memtable::{MemTable, MemTableManager, ValueType, decode_internal_key};
use super::sstable::{
    SSTableBuildError, SSTableBuildSummary, SSTableBuilder, SSTableBuilderOptions,
    SSTableReadError, SSTableReader,
};
use super::wal::{WalReadError, WalReader, WalWriteError, WalWriter, WalWriterOptions};

const WAL_DIR_NAME: &str = "wal";
const SSTABLE_DIR_NAME: &str = "sst";
const MANIFEST_DIR_NAME: &str = "manifest";
const SSTABLE_FILE_PREFIX: &str = "sst-";
const SSTABLE_FILE_SUFFIX: &str = ".sst";

#[derive(Debug, Clone)]
pub struct StorageEngineOptions {
    pub memtable_size_bytes: usize,
    pub memtable_arena_block_size_bytes: usize,
    pub wal_options: WalWriterOptions,
    pub sstable_builder_options: SSTableBuilderOptions,
    pub flush_poll_interval: Duration,
    pub flush_timeout: Duration,
}

impl Default for StorageEngineOptions {
    fn default() -> Self {
        Self {
            memtable_size_bytes: super::memtable::DEFAULT_MEMTABLE_SIZE_BYTES,
            memtable_arena_block_size_bytes: super::memtable::arena::DEFAULT_ARENA_BLOCK_SIZE_BYTES,
            wal_options: WalWriterOptions::default(),
            sstable_builder_options: SSTableBuilderOptions::default(),
            flush_poll_interval: Duration::from_millis(10),
            flush_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL read error: {0}")]
    WalRead(#[from] WalReadError),
    #[error("WAL write error: {0}")]
    WalWrite(#[from] WalWriteError),
    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),
    #[error("SSTable build error: {0}")]
    SSTableBuild(#[from] SSTableBuildError),
    #[error("SSTable read error: {0}")]
    SSTableRead(#[from] SSTableReadError),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("background flush failed: {0}")]
    BackgroundFlush(String),
    #[error("background flush timed out after {0:?}")]
    FlushTimeout(Duration),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalMetricsSnapshot {
    pub appended_records: u64,
    pub appended_bytes: u64,
    pub replayed_records: u64,
    pub replayed_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemTableMetricsSnapshot {
    pub mutable_size_bytes: usize,
    pub immutable_tables: usize,
    pub pending_flush_tables: usize,
    pub flushes_completed: u64,
    pub flushes_empty: u64,
    pub flushes_failed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StorageEngineMetrics {
    pub puts: u64,
    pub deletes: u64,
    pub gets: u64,
    pub wal: WalMetricsSnapshot,
    pub memtable: MemTableMetricsSnapshot,
    pub sstable_count: usize,
    pub compaction: CompactionMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalOperation {
    Put { key: Vec<u8>, value: Vec<u8>, sequence: u64 },
    Delete { key: Vec<u8>, sequence: u64 },
}

#[derive(Debug, Clone)]
struct ResolvedVersion {
    sequence: u64,
    value_type: ValueType,
    value: Vec<u8>,
}

impl ResolvedVersion {
    fn as_user_value(self) -> Option<Vec<u8>> {
        match self.value_type {
            ValueType::Put => Some(self.value),
            ValueType::Delete => None,
        }
    }
}

#[derive(Debug)]
struct SSTableRuntime {
    metadata: SSTableMetadata,
    reader: Arc<SSTableReader>,
}

#[derive(Debug)]
struct EngineState {
    memtables: MemTableManager,
    pending_flush: HashSet<usize>,
    sstables: Vec<SSTableRuntime>,
    next_sequence: u64,
    next_table_id: u64,
}

#[derive(Debug)]
enum FlushRequest {
    Flush(FlushTask),
    Shutdown,
}

#[derive(Debug)]
struct FlushTask {
    table_id: u64,
    memtable: Arc<MemTable>,
    path: PathBuf,
    options: SSTableBuilderOptions,
}

#[derive(Debug)]
struct FlushCompleted {
    table_id: u64,
    memtable: Arc<MemTable>,
    summary: SSTableBuildSummary,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

#[derive(Debug)]
enum FlushResponse {
    Flushed(FlushCompleted),
    Empty { memtable: Arc<MemTable> },
    Failed { memtable: Arc<MemTable>, error: String },
}

#[derive(Debug, Clone, Copy, Default)]
struct WalReplayStats {
    max_sequence: u64,
    replayed_records: u64,
    replayed_bytes: u64,
}

#[derive(Debug, Default)]
struct EngineMetricsState {
    puts: AtomicU64,
    deletes: AtomicU64,
    gets: AtomicU64,
    wal_appended_records: AtomicU64,
    wal_appended_bytes: AtomicU64,
    wal_replayed_records: AtomicU64,
    wal_replayed_bytes: AtomicU64,
    flushes_completed: AtomicU64,
    flushes_empty: AtomicU64,
    flushes_failed: AtomicU64,
    compaction: Mutex<CompactionMetrics>,
}

impl EngineMetricsState {
    fn from_replay(stats: WalReplayStats) -> Self {
        let metrics = Self::default();
        metrics.wal_replayed_records.store(stats.replayed_records, Ordering::Relaxed);
        metrics.wal_replayed_bytes.store(stats.replayed_bytes, Ordering::Relaxed);
        metrics
    }
}

#[derive(Debug)]
pub struct StorageEngine {
    root_dir: PathBuf,
    wal_dir: PathBuf,
    sstable_dir: PathBuf,
    options: StorageEngineOptions,
    wal_writer: Mutex<WalWriter>,
    manifest: Mutex<Manifest>,
    state: Mutex<EngineState>,
    metrics: EngineMetricsState,
    flush_tx: Sender<FlushRequest>,
    flush_rx: Mutex<Receiver<FlushResponse>>,
    flush_thread: Mutex<Option<JoinHandle<()>>>,
}

impl StorageEngine {
    pub fn open<P: AsRef<Path>>(root_dir: P) -> Result<Self, EngineError> {
        Self::open_with_options(root_dir, StorageEngineOptions::default())
    }

    pub fn open_with_options<P: AsRef<Path>>(
        root_dir: P,
        options: StorageEngineOptions,
    ) -> Result<Self, EngineError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        info!(
            root_dir = %root_dir.display(),
            memtable_size_bytes = options.memtable_size_bytes,
            wal_segment_size_bytes = options.wal_options.segment_size_bytes,
            "opening storage engine"
        );
        let wal_dir = root_dir.join(WAL_DIR_NAME);
        let sstable_dir = root_dir.join(SSTABLE_DIR_NAME);
        let manifest_dir = root_dir.join(MANIFEST_DIR_NAME);

        std::fs::create_dir_all(&root_dir)?;
        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&sstable_dir)?;

        let manifest = Manifest::open(&manifest_dir)?;

        let mut sstable_runtimes = Vec::new();
        for table in manifest.version_set().all_tables_newest_first() {
            let path = sstable_dir.join(&table.file_name);
            let reader = Arc::new(SSTableReader::open(&path)?);
            sstable_runtimes.push(SSTableRuntime { metadata: table, reader });
        }

        let mut memtables = MemTableManager::new(
            options.memtable_size_bytes,
            options.memtable_arena_block_size_bytes,
        );
        let replay_stats = recover_from_wal(&wal_dir, &mut memtables)?;
        let recovered_max_sequence = replay_stats.max_sequence;
        debug!(
            recovered_max_sequence,
            wal_replayed_records = replay_stats.replayed_records,
            recovered_immutable_memtables = memtables.immutable_count(),
            "wal replay completed"
        );

        let next_table_id =
            manifest.version_set().max_table_id().map(|id| id.saturating_add(1)).unwrap_or(1);

        let wal_writer = WalWriter::open_with_options(&wal_dir, options.wal_options)?;

        let (flush_tx, flush_rx_task) = mpsc::channel::<FlushRequest>();
        let (result_tx, result_rx) = mpsc::channel::<FlushResponse>();

        let flush_thread = thread::Builder::new()
            .name("lsmdb-flush".to_string())
            .spawn(move || flush_worker_loop(flush_rx_task, result_tx))
            .map_err(|err| EngineError::BackgroundFlush(err.to_string()))?;

        let engine = Self {
            root_dir,
            wal_dir,
            sstable_dir,
            options,
            wal_writer: Mutex::new(wal_writer),
            manifest: Mutex::new(manifest),
            state: Mutex::new(EngineState {
                memtables,
                pending_flush: HashSet::new(),
                sstables: sstable_runtimes,
                next_sequence: recovered_max_sequence,
                next_table_id,
            }),
            metrics: EngineMetricsState::from_replay(replay_stats),
            flush_tx,
            flush_rx: Mutex::new(result_rx),
            flush_thread: Mutex::new(Some(flush_thread)),
        };

        {
            let mut state = engine.state.lock();
            engine.schedule_pending_flushes_locked(&mut state)?;
        }

        engine.drain_flush_results()?;
        info!(
            sstable_count = engine.sstable_count(),
            immutable_memtables = engine.immutable_memtable_count(),
            "storage engine opened"
        );

        Ok(engine)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, EngineError> {
        self.drain_flush_results()?;

        let sequence = {
            let mut state = self.state.lock();
            state.next_sequence = state.next_sequence.saturating_add(1);
            state.next_sequence
        };

        let operation = WalOperation::Put { key: key.to_vec(), value: value.to_vec(), sequence };
        self.append_wal(&operation)?;
        trace!(sequence, key_len = key.len(), value_len = value.len(), "put appended to wal");

        let mut state = self.state.lock();
        state.memtables.put(key, sequence, value);
        self.schedule_pending_flushes_locked(&mut state)?;
        self.metrics.puts.fetch_add(1, Ordering::Relaxed);

        Ok(sequence)
    }

    pub fn delete(&self, key: &[u8]) -> Result<u64, EngineError> {
        self.drain_flush_results()?;

        let sequence = {
            let mut state = self.state.lock();
            state.next_sequence = state.next_sequence.saturating_add(1);
            state.next_sequence
        };

        let operation = WalOperation::Delete { key: key.to_vec(), sequence };
        self.append_wal(&operation)?;
        trace!(sequence, key_len = key.len(), "delete appended to wal");

        let mut state = self.state.lock();
        state.memtables.delete(key, sequence);
        self.schedule_pending_flushes_locked(&mut state)?;
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);

        Ok(sequence)
    }

    pub fn get(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.drain_flush_results()?;
        self.metrics.gets.fetch_add(1, Ordering::Relaxed);
        trace!(key_len = user_key.len(), "read request");

        let (mutable, immutables, sstable_readers) = {
            let state = self.state.lock();
            let readers = state
                .sstables
                .iter()
                .map(|runtime| Arc::clone(&runtime.reader))
                .collect::<Vec<_>>();
            (state.memtables.mutable(), state.memtables.immutable_tables(), readers)
        };

        if let Some(version) = resolve_from_memtable(&mutable, user_key) {
            return Ok(version.as_user_value());
        }

        for table in immutables.iter().rev() {
            if let Some(version) = resolve_from_memtable(table, user_key) {
                return Ok(version.as_user_value());
            }
        }

        for reader in &sstable_readers {
            if let Some(version) = resolve_from_sstable(reader, user_key)? {
                return Ok(version.as_user_value());
            }
        }

        Ok(None)
    }

    pub fn force_flush(&self) -> Result<(), EngineError> {
        self.drain_flush_results()?;
        info!("force flush requested");

        {
            let mut state = self.state.lock();
            if !state.memtables.mutable().is_empty() {
                state.memtables.promote_mutable();
            }
            self.schedule_pending_flushes_locked(&mut state)?;
        }

        self.wait_for_background_flush(self.options.flush_timeout)
    }

    pub fn wait_for_background_flush(&self, timeout: Duration) -> Result<(), EngineError> {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain_flush_results()?;

            let pending = self.state.lock().pending_flush.len();
            if pending == 0 {
                debug!("background flush drained");
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(EngineError::FlushTimeout(timeout));
            }

            thread::sleep(self.options.flush_poll_interval);
        }
    }

    pub fn sstable_count(&self) -> usize {
        self.state.lock().sstables.len()
    }

    pub fn immutable_memtable_count(&self) -> usize {
        self.state.lock().memtables.immutable_count()
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn sstable_dir(&self) -> &Path {
        &self.sstable_dir
    }

    pub fn sstable_metadata(&self) -> Vec<SSTableMetadata> {
        self.state.lock().sstables.iter().map(|runtime| runtime.metadata.clone()).collect()
    }

    pub fn metrics(&self) -> StorageEngineMetrics {
        let (mutable_size_bytes, immutable_tables, pending_flush_tables, sstable_count) = {
            let state = self.state.lock();
            (
                state.memtables.mutable().approximate_size_bytes(),
                state.memtables.immutable_count(),
                state.pending_flush.len(),
                state.sstables.len(),
            )
        };

        StorageEngineMetrics {
            puts: self.metrics.puts.load(Ordering::Relaxed),
            deletes: self.metrics.deletes.load(Ordering::Relaxed),
            gets: self.metrics.gets.load(Ordering::Relaxed),
            wal: WalMetricsSnapshot {
                appended_records: self.metrics.wal_appended_records.load(Ordering::Relaxed),
                appended_bytes: self.metrics.wal_appended_bytes.load(Ordering::Relaxed),
                replayed_records: self.metrics.wal_replayed_records.load(Ordering::Relaxed),
                replayed_bytes: self.metrics.wal_replayed_bytes.load(Ordering::Relaxed),
            },
            memtable: MemTableMetricsSnapshot {
                mutable_size_bytes,
                immutable_tables,
                pending_flush_tables,
                flushes_completed: self.metrics.flushes_completed.load(Ordering::Relaxed),
                flushes_empty: self.metrics.flushes_empty.load(Ordering::Relaxed),
                flushes_failed: self.metrics.flushes_failed.load(Ordering::Relaxed),
            },
            sstable_count,
            compaction: self.metrics.compaction.lock().clone(),
        }
    }

    pub fn compaction_metrics(&self) -> CompactionMetrics {
        self.metrics.compaction.lock().clone()
    }

    pub fn set_compaction_metrics(&self, metrics: CompactionMetrics) {
        *self.metrics.compaction.lock() = metrics;
    }

    fn append_wal(&self, operation: &WalOperation) -> Result<(), EngineError> {
        let payload = bincode::serialize(operation)
            .map_err(|err| EngineError::Serialization(err.to_string()))?;
        let mut writer = self.wal_writer.lock();
        writer.append_and_commit(&payload)?;
        self.metrics.wal_appended_records.fetch_add(1, Ordering::Relaxed);
        self.metrics.wal_appended_bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
        trace!(payload_len = payload.len(), "wal append committed");
        Ok(())
    }

    fn schedule_pending_flushes_locked(&self, state: &mut EngineState) -> Result<(), EngineError> {
        for memtable in state.memtables.immutable_tables() {
            let marker = Arc::as_ptr(&memtable) as usize;
            if state.pending_flush.contains(&marker) {
                continue;
            }

            let table_id = state.next_table_id;
            state.next_table_id = state.next_table_id.saturating_add(1);

            let path = self.sstable_dir.join(sstable_file_name(table_id));
            let task = FlushTask {
                table_id,
                memtable: Arc::clone(&memtable),
                path,
                options: self.options.sstable_builder_options,
            };

            self.flush_tx.send(FlushRequest::Flush(task)).map_err(|_| {
                EngineError::BackgroundFlush("flush worker channel is closed".to_string())
            })?;
            debug!(table_id, "scheduled memtable flush");
            state.pending_flush.insert(marker);
        }

        Ok(())
    }

    fn drain_flush_results(&self) -> Result<(), EngineError> {
        let mut completed = Vec::new();

        {
            let rx = self.flush_rx.lock();
            loop {
                match rx.try_recv() {
                    Ok(message) => completed.push(message),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        return Err(EngineError::BackgroundFlush(
                            "flush worker disconnected unexpectedly".to_string(),
                        ));
                    }
                }
            }
        }

        for message in completed {
            self.apply_flush_result(message)?;
        }

        Ok(())
    }

    fn apply_flush_result(&self, message: FlushResponse) -> Result<(), EngineError> {
        match message {
            FlushResponse::Flushed(completed) => {
                let marker = Arc::as_ptr(&completed.memtable) as usize;
                let file_name = completed
                    .summary
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        EngineError::BackgroundFlush(
                            "flushed SSTable path has no file name".to_string(),
                        )
                    })?
                    .to_string();

                let metadata = SSTableMetadata {
                    table_id: completed.table_id,
                    level: 0,
                    file_name: file_name.clone(),
                    smallest_key: completed.smallest_key,
                    largest_key: completed.largest_key,
                    file_size_bytes: completed.summary.file_size_bytes,
                };
                info!(
                    table_id = metadata.table_id,
                    file_size_bytes = metadata.file_size_bytes,
                    "applied flushed sstable"
                );

                {
                    let mut manifest = self.manifest.lock();
                    manifest.apply_edit(VersionEdit::AddTable(metadata.clone()))?;
                }

                let reader = Arc::new(SSTableReader::open(&self.sstable_dir.join(&file_name))?);

                let mut state = self.state.lock();
                state.pending_flush.remove(&marker);
                state.memtables.remove_immutable(&completed.memtable);
                state.sstables.insert(0, SSTableRuntime { metadata, reader });
                self.metrics.flushes_completed.fetch_add(1, Ordering::Relaxed);
            }
            FlushResponse::Empty { memtable } => {
                let marker = Arc::as_ptr(&memtable) as usize;
                let mut state = self.state.lock();
                state.pending_flush.remove(&marker);
                state.memtables.remove_immutable(&memtable);
                debug!("dropped empty immutable memtable");
                self.metrics.flushes_empty.fetch_add(1, Ordering::Relaxed);
            }
            FlushResponse::Failed { memtable, error } => {
                let marker = Arc::as_ptr(&memtable) as usize;
                self.state.lock().pending_flush.remove(&marker);
                warn!(error = %error, "memtable flush failed");
                self.metrics.flushes_failed.fetch_add(1, Ordering::Relaxed);
                return Err(EngineError::BackgroundFlush(error));
            }
        }

        Ok(())
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        debug!("shutting down storage engine");
        let _ = self.flush_tx.send(FlushRequest::Shutdown);
        if let Some(handle) = self.flush_thread.lock().take() {
            let _ = handle.join();
        }
    }
}

fn flush_worker_loop(receiver: Receiver<FlushRequest>, result_tx: Sender<FlushResponse>) {
    while let Ok(message) = receiver.recv() {
        match message {
            FlushRequest::Flush(task) => {
                let response = flush_one_memtable(task);
                let _ = result_tx.send(response);
            }
            FlushRequest::Shutdown => break,
        }
    }
}

fn flush_one_memtable(task: FlushTask) -> FlushResponse {
    let entries = task.memtable.ordered_entries();
    if entries.is_empty() {
        return FlushResponse::Empty { memtable: task.memtable };
    }

    let smallest_key = entries.first().map(|entry| entry.internal_key.clone()).unwrap_or_default();
    let largest_key = entries.last().map(|entry| entry.internal_key.clone()).unwrap_or_default();

    let result = (|| -> Result<SSTableBuildSummary, SSTableBuildError> {
        let mut builder = SSTableBuilder::create_with_options(&task.path, task.options)?;
        for entry in &entries {
            builder.add(&entry.internal_key, &entry.value)?;
        }
        builder.finish()
    })();

    match result {
        Ok(summary) => FlushResponse::Flushed(FlushCompleted {
            table_id: task.table_id,
            memtable: task.memtable,
            summary,
            smallest_key,
            largest_key,
        }),
        Err(error) => FlushResponse::Failed { memtable: task.memtable, error: error.to_string() },
    }
}

fn recover_from_wal(
    wal_dir: &Path,
    memtables: &mut MemTableManager,
) -> Result<WalReplayStats, EngineError> {
    let reader = WalReader::open(wal_dir)?;
    let replay = reader.replay()?;

    let mut max_sequence = 0_u64;
    let replayed_records = replay.records.len() as u64;
    let mut replayed_bytes = 0_u64;

    for payload in replay.records {
        replayed_bytes = replayed_bytes.saturating_add(payload.len() as u64);
        let operation: WalOperation = bincode::deserialize(&payload)
            .map_err(|err| EngineError::Serialization(err.to_string()))?;

        match operation {
            WalOperation::Put { key, value, sequence } => {
                max_sequence = max_sequence.max(sequence);
                memtables.put(&key, sequence, &value);
            }
            WalOperation::Delete { key, sequence } => {
                max_sequence = max_sequence.max(sequence);
                memtables.delete(&key, sequence);
            }
        }
    }

    Ok(WalReplayStats { max_sequence, replayed_records, replayed_bytes })
}

fn resolve_from_memtable(table: &MemTable, user_key: &[u8]) -> Option<ResolvedVersion> {
    let mut latest: Option<ResolvedVersion> = None;

    for entry in table.iter() {
        let decoded = decode_internal_key(&entry.internal_key)?;
        if decoded.user_key != user_key {
            continue;
        }

        let candidate = ResolvedVersion {
            sequence: decoded.sequence,
            value_type: decoded.value_type,
            value: entry.value,
        };

        let should_replace =
            latest.as_ref().map(|current| candidate.sequence > current.sequence).unwrap_or(true);

        if should_replace {
            latest = Some(candidate);
        }
    }

    latest
}

fn resolve_from_sstable(
    reader: &SSTableReader,
    user_key: &[u8],
) -> Result<Option<ResolvedVersion>, SSTableReadError> {
    let range_end = prefix_end(user_key);
    let rows = reader.scan_range(Some(user_key), range_end.as_deref())?;

    let mut latest: Option<ResolvedVersion> = None;
    for (internal_key, value) in rows {
        let Some(decoded) = decode_internal_key(&internal_key) else {
            continue;
        };

        if decoded.user_key != user_key {
            continue;
        }

        let candidate =
            ResolvedVersion { sequence: decoded.sequence, value_type: decoded.value_type, value };

        let should_replace =
            latest.as_ref().map(|current| candidate.sequence > current.sequence).unwrap_or(true);

        if should_replace {
            latest = Some(candidate);
        }
    }

    Ok(latest)
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }

    let mut candidate = prefix.to_vec();
    for index in (0..candidate.len()).rev() {
        if candidate[index] != 0xFF {
            candidate[index] = candidate[index].saturating_add(1);
            candidate.truncate(index + 1);
            return Some(candidate);
        }
    }

    None
}

fn sstable_file_name(table_id: u64) -> String {
    format!("{SSTABLE_FILE_PREFIX}{table_id:020}{SSTABLE_FILE_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_end_computation() {
        assert_eq!(prefix_end(b"abc"), Some(vec![b'a', b'b', b'd']));
        assert_eq!(prefix_end(b"ab\xFF"), Some(vec![b'a', b'c']));
        assert_eq!(prefix_end(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn sstable_file_name_format() {
        assert_eq!(sstable_file_name(7), "sst-00000000000000000007.sst");
    }

    #[test]
    fn internal_key_sorting_for_user_key_versions() {
        let a = crate::storage::memtable::encode_internal_key(b"k", 1, ValueType::Put);
        let b = crate::storage::memtable::encode_internal_key(b"k", 2, ValueType::Put);
        assert!(a < b);
    }
}
