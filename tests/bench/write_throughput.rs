use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::wal::SyncMode;

const KEY_PREFIX: &[u8] = b"bench:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_ROW_COUNT: usize = 100_000;
const DEFAULT_VALUE_SIZE_BYTES: usize = 256;
const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct BenchmarkConfig {
    row_count: usize,
    value_size_bytes: usize,
    memtable_size_bytes: usize,
    wal_sync_mode: SyncMode,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        Self {
            row_count: parse_env_usize("LSMDB_BENCH_WRITE_ROWS", DEFAULT_ROW_COUNT),
            value_size_bytes: parse_env_usize(
                "LSMDB_BENCH_WRITE_VALUE_BYTES",
                DEFAULT_VALUE_SIZE_BYTES,
            ),
            memtable_size_bytes: parse_env_usize(
                "LSMDB_BENCH_MEMTABLE_BYTES",
                DEFAULT_MEMTABLE_SIZE_BYTES,
            ),
            wal_sync_mode: parse_env_sync_mode("LSMDB_BENCH_WAL_SYNC", SyncMode::OnCommit),
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

fn encode_fixed_width_decimal(mut value: usize, out: &mut [u8]) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

fn run_write_workload(config: BenchmarkConfig, root_dir: &Path) {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(config.row_count < max_rows, "row_count exceeds key width");

    let mut options = StorageEngineOptions {
        memtable_size_bytes: config.memtable_size_bytes,
        ..Default::default()
    };
    options.wal_options.sync_mode = config.wal_sync_mode;

    let engine = StorageEngine::open_with_options(root_dir, options)
        .expect("open storage engine for write benchmark");

    let mut key = [0_u8; KEY_LEN];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);
    let value = vec![b'v'; config.value_size_bytes];

    for id in 0..config.row_count {
        encode_fixed_width_decimal(id, &mut key[KEY_PREFIX.len()..]);
        engine.put(&key, &value).expect("insert benchmark row");
    }

    engine.force_flush().expect("flush benchmark writes");
}

fn bench_sequential_write_throughput(c: &mut Criterion) {
    let config = BenchmarkConfig::from_env();
    let mut group = c.benchmark_group("sequential_write_throughput");
    group.sample_size(10);

    group.throughput(Throughput::Elements(config.row_count as u64));
    group.bench_function(BenchmarkId::new("rows_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("rows"),
            |dir| run_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(config.total_user_bytes()));
    group.bench_function(BenchmarkId::new("mb_per_sec", config.row_count), |b| {
        b.iter_batched(
            || TempBenchDir::new("mb"),
            |dir| run_write_workload(config, dir.path()),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(write_throughput_benches, bench_sequential_write_throughput);
criterion_main!(write_throughput_benches);
