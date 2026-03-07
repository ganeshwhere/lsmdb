use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use super::compaction::{
    CompactionMetrics, CompactionPlan, CompactionScheduler, CompactionStrategy,
    LeveledCompactionConfig, ScheduledCompaction,
};
use super::manifest::version::{SSTableMetadata, VersionSet};
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
    pub compaction_strategy: CompactionStrategy,
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
            compaction_strategy: CompactionStrategy::Leveled(LeveledCompactionConfig::default()),
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
    #[error("background compaction failed: {0}")]
    BackgroundCompaction(String),
    #[error("background compaction timed out after {0:?}")]
    CompactionTimeout(Duration),
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
    compaction_scheduler: CompactionScheduler,
    in_flight_compaction_tables: HashSet<u64>,
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

#[derive(Debug)]
enum CompactionRequest {
    Compact(CompactionTask),
    Shutdown,
}

#[derive(Debug)]
struct CompactionTask {
    scheduled: ScheduledCompaction,
    input_tables: Vec<SSTableMetadata>,
    output_table_id: u64,
    output_level: u32,
    output_path: PathBuf,
    sstable_dir: PathBuf,
    options: SSTableBuilderOptions,
}

#[derive(Debug)]
struct CompactionCompleted {
    task_id: u64,
    input_tables: Vec<SSTableMetadata>,
    output_table_id: u64,
    output_level: u32,
    summary: SSTableBuildSummary,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

#[derive(Debug)]
enum CompactionResponse {
    Compacted(CompactionCompleted),
    Empty { task_id: u64, input_tables: Vec<SSTableMetadata> },
    Failed { task_id: u64, input_tables: Vec<SSTableMetadata>, error: String },
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
    compaction_tx: Sender<CompactionRequest>,
    compaction_rx: Mutex<Receiver<CompactionResponse>>,
    compaction_thread: Mutex<Option<JoinHandle<()>>>,
    shutdown_complete: AtomicBool,
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
        let (flush_result_tx, flush_result_rx) = mpsc::channel::<FlushResponse>();
        let (compaction_tx, compaction_rx_task) = mpsc::channel::<CompactionRequest>();
        let (compaction_result_tx, compaction_result_rx) = mpsc::channel::<CompactionResponse>();

        let flush_thread = thread::Builder::new()
            .name("lsmdb-flush".to_string())
            .spawn(move || flush_worker_loop(flush_rx_task, flush_result_tx))
            .map_err(|err| EngineError::BackgroundFlush(err.to_string()))?;
        let compaction_thread = thread::Builder::new()
            .name("lsmdb-compaction".to_string())
            .spawn(move || compaction_worker_loop(compaction_rx_task, compaction_result_tx))
            .map_err(|err| EngineError::BackgroundCompaction(err.to_string()))?;

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
                compaction_scheduler: CompactionScheduler::default(),
                in_flight_compaction_tables: HashSet::new(),
                next_sequence: recovered_max_sequence,
                next_table_id,
            }),
            metrics: EngineMetricsState::from_replay(replay_stats),
            flush_tx,
            flush_rx: Mutex::new(flush_result_rx),
            flush_thread: Mutex::new(Some(flush_thread)),
            compaction_tx,
            compaction_rx: Mutex::new(compaction_result_rx),
            compaction_thread: Mutex::new(Some(compaction_thread)),
            shutdown_complete: AtomicBool::new(false),
        };

        {
            let mut state = engine.state.lock();
            engine.refresh_compaction_space_metrics_locked(&state);
            engine.schedule_pending_flushes_locked(&mut state)?;
            engine.schedule_pending_compactions_locked(&mut state)?;
        }

        engine.drain_background_results()?;
        info!(
            sstable_count = engine.sstable_count(),
            immutable_memtables = engine.immutable_memtable_count(),
            "storage engine opened"
        );

        Ok(engine)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, EngineError> {
        self.drain_background_results()?;

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
        self.schedule_pending_compactions_locked(&mut state)?;
        self.refresh_compaction_space_metrics_locked(&state);
        self.metrics.puts.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .compaction
            .lock()
            .record_user_write((key.len().saturating_add(value.len())) as u64);

        Ok(sequence)
    }

    pub fn delete(&self, key: &[u8]) -> Result<u64, EngineError> {
        self.drain_background_results()?;

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
        self.schedule_pending_compactions_locked(&mut state)?;
        self.refresh_compaction_space_metrics_locked(&state);
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);
        self.metrics.compaction.lock().record_user_write(key.len() as u64);

        Ok(sequence)
    }

    pub fn get(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.drain_background_results()?;
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
            self.metrics.compaction.lock().record_point_lookup(0);
            return Ok(version.as_user_value());
        }

        for table in immutables.iter().rev() {
            if let Some(version) = resolve_from_memtable(table, user_key) {
                self.metrics.compaction.lock().record_point_lookup(0);
                return Ok(version.as_user_value());
            }
        }

        let mut files_checked = 0_u64;
        let mut latest: Option<ResolvedVersion> = None;
        for reader in &sstable_readers {
            files_checked = files_checked.saturating_add(1);
            if let Some(candidate) = resolve_from_sstable(reader, user_key)? {
                let should_replace = latest
                    .as_ref()
                    .map(|current| candidate.sequence > current.sequence)
                    .unwrap_or(true);
                if should_replace {
                    latest = Some(candidate);
                }
            }
        }
        self.metrics.compaction.lock().record_point_lookup(files_checked);

        Ok(latest.map(ResolvedVersion::as_user_value).unwrap_or(None))
    }

    pub fn force_flush(&self) -> Result<(), EngineError> {
        self.drain_background_results()?;
        info!("force flush requested");

        {
            let mut state = self.state.lock();
            if !state.memtables.mutable().is_empty() {
                state.memtables.promote_mutable();
            }
            self.schedule_pending_flushes_locked(&mut state)?;
            self.schedule_pending_compactions_locked(&mut state)?;
        }

        self.wait_for_background_flush(self.options.flush_timeout)
    }

    pub fn wait_for_background_flush(&self, timeout: Duration) -> Result<(), EngineError> {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain_background_results()?;

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

    pub fn wait_for_background_compaction(&self, timeout: Duration) -> Result<(), EngineError> {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain_background_results()?;

            let (pending, in_flight) = {
                let state = self.state.lock();
                (
                    state.compaction_scheduler.pending_count(),
                    state.compaction_scheduler.in_flight_count(),
                )
            };
            if pending == 0 && in_flight == 0 {
                debug!("background compaction drained");
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(EngineError::CompactionTimeout(timeout));
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

    pub fn shutdown(&self) -> Result<(), EngineError> {
        if self
            .shutdown_complete
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        info!("graceful storage engine shutdown requested");

        if let Err(err) = self.force_flush() {
            self.shutdown_complete.store(false, Ordering::Release);
            return Err(err);
        }
        if let Err(err) = self.wait_for_background_compaction(self.options.flush_timeout) {
            self.shutdown_complete.store(false, Ordering::Release);
            return Err(err);
        }

        {
            let mut writer = self.wal_writer.lock();
            if let Err(err) = writer.sync_data() {
                self.shutdown_complete.store(false, Ordering::Release);
                return Err(EngineError::WalWrite(err));
            }
        }

        {
            let mut manifest = self.manifest.lock();
            if let Err(err) = manifest.sync() {
                self.shutdown_complete.store(false, Ordering::Release);
                return Err(EngineError::Manifest(err));
            }
        }

        self.stop_compaction_worker()?;
        self.stop_flush_worker()?;
        info!("graceful storage engine shutdown complete");
        Ok(())
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

    fn schedule_pending_compactions_locked(
        &self,
        state: &mut EngineState,
    ) -> Result<(), EngineError> {
        loop {
            let versions = build_schedulable_version_set(state);
            let Some(_) = state
                .compaction_scheduler
                .schedule_from_versions(&versions, &self.options.compaction_strategy)
            else {
                break;
            };
            let Some(scheduled) = state.compaction_scheduler.pop_next() else {
                break;
            };

            let input_tables = compaction_plan_inputs(&scheduled.plan);
            if input_tables.is_empty() {
                state.compaction_scheduler.mark_completed(scheduled.task_id);
                continue;
            }
            if input_tables
                .iter()
                .any(|table| state.in_flight_compaction_tables.contains(&table.table_id))
            {
                state.compaction_scheduler.mark_completed(scheduled.task_id);
                continue;
            }
            let inputs_present = input_tables.iter().all(|table| {
                state.sstables.iter().any(|runtime| {
                    runtime.metadata.table_id == table.table_id
                        && runtime.metadata.level == table.level
                })
            });
            if !inputs_present {
                state.compaction_scheduler.mark_completed(scheduled.task_id);
                continue;
            }

            let output_table_id = state.next_table_id;
            state.next_table_id = state.next_table_id.saturating_add(1);
            let output_level = compaction_plan_output_level(&scheduled.plan);
            let task_id = scheduled.task_id;

            let task = CompactionTask {
                scheduled,
                input_tables: input_tables.clone(),
                output_table_id,
                output_level,
                output_path: self.sstable_dir.join(sstable_file_name(output_table_id)),
                sstable_dir: self.sstable_dir.clone(),
                options: self.options.sstable_builder_options,
            };

            self.compaction_tx.send(CompactionRequest::Compact(task)).map_err(|_| {
                EngineError::BackgroundCompaction("compaction worker channel is closed".to_string())
            })?;
            for table in &input_tables {
                state.in_flight_compaction_tables.insert(table.table_id);
            }
            debug!(
                task_id,
                output_level,
                input_count = input_tables.len(),
                "scheduled compaction task"
            );
        }

        Ok(())
    }

    fn drain_background_results(&self) -> Result<(), EngineError> {
        self.drain_flush_results()?;
        self.drain_compaction_results()?;
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

    fn drain_compaction_results(&self) -> Result<(), EngineError> {
        let mut completed = Vec::new();

        {
            let rx = self.compaction_rx.lock();
            loop {
                match rx.try_recv() {
                    Ok(message) => completed.push(message),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        return Err(EngineError::BackgroundCompaction(
                            "compaction worker disconnected unexpectedly".to_string(),
                        ));
                    }
                }
            }
        }

        for message in completed {
            self.apply_compaction_result(message)?;
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
                self.refresh_compaction_space_metrics_locked(&state);
                self.schedule_pending_compactions_locked(&mut state)?;
            }
            FlushResponse::Empty { memtable } => {
                let marker = Arc::as_ptr(&memtable) as usize;
                let mut state = self.state.lock();
                state.pending_flush.remove(&marker);
                state.memtables.remove_immutable(&memtable);
                debug!("dropped empty immutable memtable");
                self.metrics.flushes_empty.fetch_add(1, Ordering::Relaxed);
                self.schedule_pending_compactions_locked(&mut state)?;
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

    fn apply_compaction_result(&self, message: CompactionResponse) -> Result<(), EngineError> {
        match message {
            CompactionResponse::Compacted(completed) => {
                let file_name = completed
                    .summary
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        EngineError::BackgroundCompaction(
                            "compacted SSTable path has no file name".to_string(),
                        )
                    })?
                    .to_string();
                let metadata = SSTableMetadata {
                    table_id: completed.output_table_id,
                    level: completed.output_level,
                    file_name,
                    smallest_key: completed.smallest_key,
                    largest_key: completed.largest_key,
                    file_size_bytes: completed.summary.file_size_bytes,
                };
                let reader = Arc::new(SSTableReader::open(&completed.summary.path)?);

                let manifest_result = (|| -> Result<(), EngineError> {
                    let mut manifest = self.manifest.lock();
                    manifest.apply_edit(VersionEdit::AddTable(metadata.clone()))?;
                    for table in &completed.input_tables {
                        manifest.apply_edit(VersionEdit::RemoveTable {
                            level: table.level,
                            table_id: table.table_id,
                        })?;
                    }
                    Ok(())
                })();
                if let Err(err) = manifest_result {
                    self.release_compaction_task(completed.task_id, &completed.input_tables);
                    return Err(err);
                }

                let removed_file_names = {
                    let input_ids = completed
                        .input_tables
                        .iter()
                        .map(|table| table.table_id)
                        .collect::<HashSet<_>>();
                    let mut state = self.state.lock();
                    let removed_file_names = state
                        .sstables
                        .iter()
                        .filter(|runtime| input_ids.contains(&runtime.metadata.table_id))
                        .map(|runtime| runtime.metadata.file_name.clone())
                        .collect::<Vec<_>>();
                    state
                        .sstables
                        .retain(|runtime| !input_ids.contains(&runtime.metadata.table_id));
                    state.sstables.insert(0, SSTableRuntime { metadata: metadata.clone(), reader });
                    self.release_compaction_task_locked(
                        &mut state,
                        completed.task_id,
                        &completed.input_tables,
                    );
                    self.refresh_compaction_space_metrics_locked(&state);
                    self.schedule_pending_compactions_locked(&mut state)?;
                    removed_file_names
                };

                for file_name in removed_file_names {
                    let path = self.sstable_dir.join(file_name);
                    if let Err(err) = std::fs::remove_file(&path) {
                        if err.kind() != std::io::ErrorKind::NotFound {
                            warn!(error = %err, path = %path.display(), "failed to delete compacted input table");
                        }
                    }
                }

                let mut metrics = self.metrics.compaction.lock();
                metrics.record_compaction_write(completed.summary.file_size_bytes);
                metrics.mark_compaction_complete();
                info!(
                    task_id = completed.task_id,
                    output_table_id = completed.output_table_id,
                    output_level = completed.output_level,
                    output_file_size_bytes = completed.summary.file_size_bytes,
                    "applied compaction output"
                );
            }
            CompactionResponse::Empty { task_id, input_tables } => {
                let manifest_result = (|| -> Result<(), EngineError> {
                    let mut manifest = self.manifest.lock();
                    for table in &input_tables {
                        manifest.apply_edit(VersionEdit::RemoveTable {
                            level: table.level,
                            table_id: table.table_id,
                        })?;
                    }
                    Ok(())
                })();
                if let Err(err) = manifest_result {
                    self.release_compaction_task(task_id, &input_tables);
                    return Err(err);
                }

                {
                    let input_ids =
                        input_tables.iter().map(|table| table.table_id).collect::<HashSet<_>>();
                    let mut state = self.state.lock();
                    state
                        .sstables
                        .retain(|runtime| !input_ids.contains(&runtime.metadata.table_id));
                    self.release_compaction_task_locked(&mut state, task_id, &input_tables);
                    self.refresh_compaction_space_metrics_locked(&state);
                    self.schedule_pending_compactions_locked(&mut state)?;
                }

                for table in &input_tables {
                    let path = self.sstable_dir.join(&table.file_name);
                    if let Err(err) = std::fs::remove_file(&path) {
                        if err.kind() != std::io::ErrorKind::NotFound {
                            warn!(error = %err, path = %path.display(), "failed to delete compacted input table");
                        }
                    }
                }

                self.metrics.compaction.lock().mark_compaction_complete();
                info!(task_id, "applied empty compaction result");
            }
            CompactionResponse::Failed { task_id, input_tables, error } => {
                self.release_compaction_task(task_id, &input_tables);
                warn!(task_id, error = %error, "compaction task failed");
                return Err(EngineError::BackgroundCompaction(error));
            }
        }

        Ok(())
    }

    fn refresh_compaction_space_metrics_locked(&self, state: &EngineState) {
        let total_bytes =
            state.sstables.iter().map(|runtime| runtime.metadata.file_size_bytes).sum::<u64>();
        self.metrics.compaction.lock().set_space_bytes(total_bytes, total_bytes);
    }

    fn release_compaction_task(&self, task_id: u64, input_tables: &[SSTableMetadata]) {
        let mut state = self.state.lock();
        self.release_compaction_task_locked(&mut state, task_id, input_tables);
    }

    fn release_compaction_task_locked(
        &self,
        state: &mut EngineState,
        task_id: u64,
        input_tables: &[SSTableMetadata],
    ) {
        state.compaction_scheduler.mark_completed(task_id);
        for table in input_tables {
            state.in_flight_compaction_tables.remove(&table.table_id);
        }
    }

    fn stop_compaction_worker(&self) -> Result<(), EngineError> {
        let handle = {
            let mut guard = self.compaction_thread.lock();
            if guard.is_none() {
                return Ok(());
            }
            let _ = self.compaction_tx.send(CompactionRequest::Shutdown);
            guard.take()
        };

        if let Some(handle) = handle {
            handle.join().map_err(|_| {
                EngineError::BackgroundCompaction(
                    "compaction worker thread panicked during shutdown".to_string(),
                )
            })?;
        }

        Ok(())
    }

    fn stop_flush_worker(&self) -> Result<(), EngineError> {
        let handle = {
            let mut guard = self.flush_thread.lock();
            if guard.is_none() {
                return Ok(());
            }
            let _ = self.flush_tx.send(FlushRequest::Shutdown);
            guard.take()
        };

        if let Some(handle) = handle {
            handle.join().map_err(|_| {
                EngineError::BackgroundFlush(
                    "flush worker thread panicked during shutdown".to_string(),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        debug!("dropping storage engine");
        if let Err(err) = self.shutdown() {
            warn!(error = %err, "graceful shutdown failed during drop");
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

fn build_schedulable_version_set(state: &EngineState) -> VersionSet {
    let mut versions = VersionSet::default();
    for runtime in &state.sstables {
        if state.in_flight_compaction_tables.contains(&runtime.metadata.table_id) {
            continue;
        }
        versions.add_table(runtime.metadata.clone());
    }
    versions
}

fn compaction_plan_inputs(plan: &CompactionPlan) -> Vec<SSTableMetadata> {
    let mut inputs = match plan {
        CompactionPlan::Leveled(plan) => {
            let mut tables = plan.source_inputs.clone();
            tables.extend(plan.target_inputs.clone());
            tables
        }
        CompactionPlan::Tiered(plan) => plan.input_tables.clone(),
    };

    inputs.sort_by_key(|table| table.table_id);
    inputs.dedup_by_key(|table| table.table_id);
    inputs
}

fn compaction_plan_output_level(plan: &CompactionPlan) -> u32 {
    match plan {
        CompactionPlan::Leveled(plan) => plan.target_level,
        CompactionPlan::Tiered(plan) => plan.output_level,
    }
}

fn compaction_worker_loop(
    receiver: Receiver<CompactionRequest>,
    result_tx: Sender<CompactionResponse>,
) {
    while let Ok(message) = receiver.recv() {
        match message {
            CompactionRequest::Compact(task) => {
                let response = compact_one_task(task);
                let _ = result_tx.send(response);
            }
            CompactionRequest::Shutdown => break,
        }
    }
}

fn compact_one_task(task: CompactionTask) -> CompactionResponse {
    let task_id = task.scheduled.task_id;
    let input_tables = task.input_tables.clone();
    debug!(
        task_id,
        plan = ?task.scheduled.plan,
        input_count = input_tables.len(),
        "running compaction task"
    );

    let compacted = (|| -> Result<Option<CompactionCompleted>, String> {
        let mut rows = Vec::new();
        for table in &task.input_tables {
            let path = task.sstable_dir.join(&table.file_name);
            let reader = SSTableReader::open(&path).map_err(|err| err.to_string())?;
            let scanned = reader.scan_range(None, None).map_err(|err| err.to_string())?;
            for (key, value) in scanned {
                rows.push((key, value, table.table_id));
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        let merged = merge_compaction_rows(rows);

        if merged.is_empty() {
            return Ok(None);
        }

        let smallest_key = merged.first().map(|(key, _)| key.clone()).unwrap_or_default();
        let largest_key = merged.last().map(|(key, _)| key.clone()).unwrap_or_default();

        let summary = {
            let mut builder = SSTableBuilder::create_with_options(&task.output_path, task.options)
                .map_err(|err| err.to_string())?;
            for (key, value) in &merged {
                builder.add(key, value).map_err(|err| err.to_string())?;
            }
            builder.finish().map_err(|err| err.to_string())?
        };

        Ok(Some(CompactionCompleted {
            task_id,
            input_tables: input_tables.clone(),
            output_table_id: task.output_table_id,
            output_level: task.output_level,
            summary,
            smallest_key,
            largest_key,
        }))
    })();

    match compacted {
        Ok(Some(compacted)) => CompactionResponse::Compacted(compacted),
        Ok(None) => CompactionResponse::Empty { task_id, input_tables },
        Err(error) => CompactionResponse::Failed { task_id, input_tables, error },
    }
}

fn merge_compaction_rows(
    mut rows: Vec<(Vec<u8>, Vec<u8>, u64)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.2.cmp(&right.2)));

    // First collapse identical internal keys, preferring the newest table id.
    let mut deduped = Vec::with_capacity(rows.len());
    for (key, value, table_id) in rows {
        if let Some((last_key, last_value, last_table_id)) = deduped.last_mut() {
            if *last_key == key {
                if table_id >= *last_table_id {
                    *last_value = value;
                    *last_table_id = table_id;
                }
                continue;
            }
        }

        deduped.push((key, value, table_id));
    }

    // Then collapse MVCC history to only the latest sequence per user key.
    let mut collapsed = Vec::with_capacity(deduped.len());
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut current_latest: Option<(Vec<u8>, Vec<u8>)> = None;

    for (internal_key, value, _) in deduped {
        let Some(decoded) = decode_internal_key(&internal_key) else {
            if let Some(latest) = current_latest.take() {
                collapsed.push(latest);
            }
            current_user_key = None;
            collapsed.push((internal_key, value));
            continue;
        };

        match current_user_key.as_deref() {
            Some(user_key) if user_key == decoded.user_key => {
                current_latest = Some((internal_key, value));
            }
            _ => {
                if let Some(latest) = current_latest.take() {
                    collapsed.push(latest);
                }
                current_user_key = Some(decoded.user_key.to_vec());
                current_latest = Some((internal_key, value));
            }
        }
    }

    if let Some(latest) = current_latest {
        collapsed.push(latest);
    }

    collapsed
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
    use crate::storage::memtable::encode_internal_key;

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

    #[test]
    fn merge_compaction_rows_keeps_latest_version_per_user_key() {
        let rows = vec![
            (encode_internal_key(b"user:1", 1, ValueType::Put), b"a".to_vec(), 10),
            (encode_internal_key(b"user:1", 2, ValueType::Put), b"b".to_vec(), 11),
            (encode_internal_key(b"user:1", 3, ValueType::Delete), Vec::new(), 12),
            (encode_internal_key(b"user:2", 1, ValueType::Put), b"z".to_vec(), 20),
            (encode_internal_key(b"user:2", 4, ValueType::Put), b"zz".to_vec(), 21),
        ];

        let merged = merge_compaction_rows(rows);
        assert_eq!(merged.len(), 2);

        let decoded_0 = decode_internal_key(&merged[0].0).expect("decode first key");
        let decoded_1 = decode_internal_key(&merged[1].0).expect("decode second key");
        assert_eq!(decoded_0.user_key, b"user:1");
        assert_eq!(decoded_0.sequence, 3);
        assert_eq!(decoded_0.value_type, ValueType::Delete);
        assert_eq!(decoded_1.user_key, b"user:2");
        assert_eq!(decoded_1.sequence, 4);
        assert_eq!(merged[1].1, b"zz".to_vec());
    }

    #[test]
    fn merge_compaction_rows_prefers_newest_table_for_duplicate_internal_keys() {
        let duplicated = encode_internal_key(b"dup", 7, ValueType::Put);
        let rows = vec![
            (duplicated.clone(), b"old".to_vec(), 2),
            (duplicated.clone(), b"new".to_vec(), 8),
            (encode_internal_key(b"other", 1, ValueType::Put), b"x".to_vec(), 1),
        ];

        let merged = merge_compaction_rows(rows);
        let dup_row = merged
            .iter()
            .find(|(key, _)| decode_internal_key(key).map(|k| k.user_key == b"dup").unwrap_or(false))
            .expect("duplicate key row");
        assert_eq!(dup_row.1, b"new".to_vec());
    }
}
