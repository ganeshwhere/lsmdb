use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::wal::SyncMode;
use rusqlite::{Connection, TransactionBehavior, params};

const KEY_PREFIX: &[u8] = b"base:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_ROW_COUNT: usize = 100_000;
const DEFAULT_VALUE_SIZE_BYTES: usize = 256;
const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteSyncMode {
    Off,
    Normal,
    Full,
    Extra,
}

impl SqliteSyncMode {
    fn pragma_value(self) -> &'static str {
        match self {
            SqliteSyncMode::Off => "OFF",
            SqliteSyncMode::Normal => "NORMAL",
            SqliteSyncMode::Full => "FULL",
            SqliteSyncMode::Extra => "EXTRA",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BaselineConfig {
    row_count: usize,
    value_size_bytes: usize,
    memtable_size_bytes: usize,
    lsm_wal_sync_mode: SyncMode,
    sqlite_sync_mode: SqliteSyncMode,
}

impl BaselineConfig {
    fn from_env() -> Self {
        Self {
            row_count: parse_env_usize("LSMDB_BENCH_BASELINE_ROWS", DEFAULT_ROW_COUNT),
            value_size_bytes: parse_env_usize(
                "LSMDB_BENCH_BASELINE_VALUE_BYTES",
                DEFAULT_VALUE_SIZE_BYTES,
            ),
            memtable_size_bytes: parse_env_usize(
                "LSMDB_BENCH_MEMTABLE_BYTES",
                DEFAULT_MEMTABLE_SIZE_BYTES,
            ),
            lsm_wal_sync_mode: parse_env_sync_mode("LSMDB_BENCH_WAL_SYNC", SyncMode::OnCommit),
            sqlite_sync_mode: parse_sqlite_sync_mode(
                "LSMDB_BENCH_SQLITE_SYNC",
                SqliteSyncMode::Full,
            ),
        }
    }

    fn total_user_bytes(&self) -> u64 {
        let per_row = (KEY_LEN + self.value_size_bytes) as u64;
        self.row_count as u64 * per_row
    }
}

#[derive(Debug)]
struct TempBenchDir {
    path: PathBuf,
}

impl TempBenchDir {
    fn new(label: &str) -> Self {
        let mut dir = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("benchmark time should be after unix epoch")
            .as_nanos();
        dir.push(format!("lsmdb-bench-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("create benchmark temp directory");
        Self { path: dir }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBenchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn parse_env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_env_sync_mode(key: &str, default: SyncMode) -> SyncMode {
    let Ok(raw) = env::var(key) else {
        return default;
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "never" => SyncMode::Never,
        "on_commit" | "oncommit" => SyncMode::OnCommit,
        "always" => SyncMode::Always,
        _ => default,
    }
}

fn parse_sqlite_sync_mode(key: &str, default: SqliteSyncMode) -> SqliteSyncMode {
    let Ok(raw) = env::var(key) else {
        return default;
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => SqliteSyncMode::Off,
        "normal" => SqliteSyncMode::Normal,
        "full" => SqliteSyncMode::Full,
        "extra" => SqliteSyncMode::Extra,
        _ => default,
    }
}

fn encode_fixed_width_decimal(mut value: usize, out: &mut [u8]) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

fn run_lsmdb_write_workload(config: BaselineConfig, root_dir: &Path) {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(config.row_count < max_rows, "row_count exceeds key width");

    let mut options = StorageEngineOptions {
        memtable_size_bytes: config.memtable_size_bytes,
        ..Default::default()
    };
    options.wal_options.sync_mode = config.lsm_wal_sync_mode;
    let engine = StorageEngine::open_with_options(root_dir, options)
        .expect("open storage engine for baseline benchmark");

    let mut key = [0_u8; KEY_LEN];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);
    let value = vec![b'v'; config.value_size_bytes];

    for id in 0..config.row_count {
        encode_fixed_width_decimal(id, &mut key[KEY_PREFIX.len()..]);
        engine.put(&key, &value).expect("insert row into lsmdb");
    }

    engine.force_flush().expect("flush lsmdb writes");
}

fn run_sqlite_write_workload(config: BaselineConfig, root_dir: &Path) {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(config.row_count < max_rows, "row_count exceeds key width");

    let sqlite_path = root_dir.join("baseline.sqlite3");
    let mut conn = Connection::open(&sqlite_path).expect("open sqlite database");
    conn.pragma_update(None, "journal_mode", "WAL").expect("enable sqlite WAL mode");
    conn.pragma_update(None, "synchronous", config.sqlite_sync_mode.pragma_value())
        .expect("configure sqlite synchronous mode");
    conn.pragma_update(None, "temp_store", "MEMORY").expect("configure sqlite temp store");
    conn.execute_batch("CREATE TABLE kv (k TEXT PRIMARY KEY, v BLOB NOT NULL);")
        .expect("create sqlite kv table");

    let mut key = [0_u8; KEY_LEN];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);
    let value = vec![b'v'; config.value_size_bytes];

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("start sqlite write transaction");
    {
        let mut stmt =
            tx.prepare("INSERT INTO kv (k, v) VALUES (?1, ?2)").expect("prepare sqlite insert");
        for id in 0..config.row_count {
            encode_fixed_width_decimal(id, &mut key[KEY_PREFIX.len()..]);
            let key_text = std::str::from_utf8(&key).expect("key should be valid utf-8");
            stmt.execute(params![key_text, value.as_slice()]).expect("insert row into sqlite");
        }
    }
    tx.commit().expect("commit sqlite write transaction");
    conn.execute_batch("PRAGMA wal_checkpoint(FULL);").expect("checkpoint sqlite wal");
}

fn bench_sqlite_wal_baseline(c: &mut Criterion) {
    let config = BaselineConfig::from_env();
    eprintln!(
        "sqlite_wal_baseline_config rows={} value_bytes={} lsm_wal_sync={:?} sqlite_sync={}",
        config.row_count,
        config.value_size_bytes,
        config.lsm_wal_sync_mode,
        config.sqlite_sync_mode.pragma_value()
    );

    let mut group = c.benchmark_group("sqlite_wal_write_baseline");
    group.sample_size(10);

    group.throughput(Throughput::Elements(config.row_count as u64));
    group.bench_function(BenchmarkId::new("lsmdb_rows_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("baseline-lsmdb-rows"),
            |dir| run_lsmdb_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });
    group.bench_function(BenchmarkId::new("sqlite_rows_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("baseline-sqlite-rows"),
            |dir| run_sqlite_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(config.total_user_bytes()));
    group.bench_function(BenchmarkId::new("lsmdb_mb_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("baseline-lsmdb-mb"),
            |dir| run_lsmdb_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });
    group.bench_function(BenchmarkId::new("sqlite_mb_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("baseline-sqlite-mb"),
            |dir| run_sqlite_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(sqlite_baseline_benches, bench_sqlite_wal_baseline);
criterion_main!(sqlite_baseline_benches);
