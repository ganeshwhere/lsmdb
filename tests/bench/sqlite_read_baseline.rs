use std::cmp::Ordering;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::wal::SyncMode;
use rusqlite::{Connection, TransactionBehavior, params};

const KEY_PREFIX: &[u8] = b"read:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_KEY_SPACE: usize = 100_000;
const DEFAULT_READ_OPS: usize = 200_000;
const DEFAULT_VALUE_SIZE_BYTES: usize = 128;
const DEFAULT_ZIPF_EXPONENT: f64 = 1.2;
const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SEED: u64 = 0xE1A4_7B10_2026;

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
struct ReadBaselineConfig {
    key_space: usize,
    read_ops: usize,
    value_size_bytes: usize,
    zipf_exponent: f64,
    memtable_size_bytes: usize,
    lsmdb_preload_wal_sync_mode: SyncMode,
    sqlite_sync_mode: SqliteSyncMode,
    seed: u64,
}

impl ReadBaselineConfig {
    fn from_env() -> Self {
        Self {
            key_space: parse_env_usize("LSMDB_BENCH_READ_KEYSPACE", DEFAULT_KEY_SPACE),
            read_ops: parse_env_usize("LSMDB_BENCH_READ_OPS", DEFAULT_READ_OPS),
            value_size_bytes: parse_env_usize(
                "LSMDB_BENCH_READ_VALUE_BYTES",
                DEFAULT_VALUE_SIZE_BYTES,
            ),
            zipf_exponent: parse_env_f64("LSMDB_BENCH_READ_ZIPF_EXPONENT", DEFAULT_ZIPF_EXPONENT),
            memtable_size_bytes: parse_env_usize(
                "LSMDB_BENCH_MEMTABLE_BYTES",
                DEFAULT_MEMTABLE_SIZE_BYTES,
            ),
            lsmdb_preload_wal_sync_mode: parse_env_sync_mode(
                "LSMDB_BENCH_WAL_SYNC",
                SyncMode::OnCommit,
            ),
            sqlite_sync_mode: parse_sqlite_sync_mode(
                "LSMDB_BENCH_SQLITE_SYNC",
                SqliteSyncMode::Full,
            ),
            seed: parse_env_u64("LSMDB_BENCH_READ_SEED", DEFAULT_SEED),
        }
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

#[derive(Debug, Clone)]
struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(key_space: usize, exponent: f64) -> Self {
        assert!(key_space > 0, "key_space must be greater than zero");
        assert!(exponent > 0.0, "zipf exponent must be greater than zero");

        let mut cdf = Vec::with_capacity(key_space);
        let normalization =
            (1..=key_space).map(|rank| 1.0 / (rank as f64).powf(exponent)).sum::<f64>();

        let mut running = 0.0;
        for rank in 1..=key_space {
            running += (1.0 / (rank as f64).powf(exponent)) / normalization;
            cdf.push(running);
        }

        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }

        Self { cdf }
    }

    fn sample(&self, rng: &mut XorShift64) -> usize {
        let target = rng.next_f64();
        let position = self
            .cdf
            .binary_search_by(|value| value.partial_cmp(&target).unwrap_or(Ordering::Greater));

        match position {
            Ok(index) => index,
            Err(index) => index.min(self.cdf.len() - 1),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x9e37_79b9_7f4a_7c15 } else { seed };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / ((u64::MAX as f64) + 1.0)
    }
}

#[derive(Debug)]
struct LatencyProbe {
    elapsed: Duration,
    histogram: Histogram<u64>,
    operations: usize,
}

#[derive(Debug)]
struct LsmReadFixture {
    _dir: TempBenchDir,
    engine: StorageEngine,
    keys: Vec<Vec<u8>>,
    sampler: ZipfSampler,
}

impl LsmReadFixture {
    fn build(config: ReadBaselineConfig) -> Self {
        let keys = build_binary_keys(config.key_space);
        let value = vec![b'r'; config.value_size_bytes];
        let dir = TempBenchDir::new("sqlite-read-lsmdb");

        {
            let mut options = StorageEngineOptions {
                memtable_size_bytes: config.memtable_size_bytes,
                ..Default::default()
            };
            options.wal_options.sync_mode = config.lsmdb_preload_wal_sync_mode;
            let engine = StorageEngine::open_with_options(dir.path(), options)
                .expect("open lsmdb preload fixture");

            for key in &keys {
                engine.put(key, &value).expect("preload key/value for lsmdb fixture");
            }
            engine.force_flush().expect("flush lsmdb preload data");
        }

        let read_options = StorageEngineOptions {
            memtable_size_bytes: config.memtable_size_bytes,
            ..Default::default()
        };
        let engine = StorageEngine::open_with_options(dir.path(), read_options)
            .expect("open lsmdb read fixture");

        Self {
            _dir: dir,
            engine,
            keys,
            sampler: ZipfSampler::new(config.key_space, config.zipf_exponent),
        }
    }

    fn run_probe(&self, operations: usize, seed: u64) -> LatencyProbe {
        let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("create lsmdb histogram");
        let elapsed = self.execute_reads(operations, seed, Some(&mut histogram));
        LatencyProbe { elapsed, histogram, operations }
    }

    fn execute_reads(
        &self,
        operations: usize,
        seed: u64,
        mut histogram: Option<&mut Histogram<u64>>,
    ) -> Duration {
        let mut rng = XorShift64::new(seed);
        let started = Instant::now();

        for _ in 0..operations {
            let index = self.sampler.sample(&mut rng);
            let key = &self.keys[index];
            let read_started = Instant::now();
            let value = self.engine.get(key).expect("lsmdb point read");
            debug_assert!(value.is_some(), "preloaded lsmdb key should always be present");

            if let Some(hist) = histogram.as_mut() {
                let latency_ns = read_started.elapsed().as_nanos().max(1) as u64;
                let _ = (*hist).record(latency_ns);
            }
        }

        started.elapsed()
    }
}

#[derive(Debug)]
struct SqliteReadFixture {
    _dir: TempBenchDir,
    conn: Connection,
    keys: Vec<String>,
    sampler: ZipfSampler,
}

impl SqliteReadFixture {
    fn build(config: ReadBaselineConfig) -> Self {
        let keys = build_string_keys(config.key_space);
        let value = vec![b'r'; config.value_size_bytes];
        let dir = TempBenchDir::new("sqlite-read-sqlite");
        let sqlite_path = dir.path().join("read_baseline.sqlite3");

        let mut conn = Connection::open(&sqlite_path).expect("open sqlite fixture db");
        conn.pragma_update(None, "journal_mode", "WAL").expect("enable sqlite WAL mode");
        conn.pragma_update(None, "synchronous", config.sqlite_sync_mode.pragma_value())
            .expect("configure sqlite synchronous mode");
        conn.pragma_update(None, "temp_store", "MEMORY").expect("configure sqlite temp store");
        conn.execute_batch("CREATE TABLE kv (k TEXT PRIMARY KEY, v BLOB NOT NULL);")
            .expect("create sqlite fixture table");

        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("start sqlite preload transaction");
        {
            let mut stmt =
                tx.prepare("INSERT INTO kv (k, v) VALUES (?1, ?2)").expect("prepare sqlite insert");
            for key in &keys {
                stmt.execute(params![key, value.as_slice()]).expect("insert sqlite fixture row");
            }
        }
        tx.commit().expect("commit sqlite preload transaction");
        conn.execute_batch("PRAGMA wal_checkpoint(FULL);").expect("checkpoint sqlite preload wal");

        Self {
            _dir: dir,
            conn,
            keys,
            sampler: ZipfSampler::new(config.key_space, config.zipf_exponent),
        }
    }

    fn run_probe(&self, operations: usize, seed: u64) -> LatencyProbe {
        let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("create sqlite histogram");
        let elapsed = self.execute_reads(operations, seed, Some(&mut histogram));
        LatencyProbe { elapsed, histogram, operations }
    }

    fn execute_reads(
        &self,
        operations: usize,
        seed: u64,
        mut histogram: Option<&mut Histogram<u64>>,
    ) -> Duration {
        let mut rng = XorShift64::new(seed);
        let mut stmt =
            self.conn.prepare_cached("SELECT v FROM kv WHERE k = ?1").expect("prepare sqlite read");
        let started = Instant::now();

        for _ in 0..operations {
            let index = self.sampler.sample(&mut rng);
            let key = &self.keys[index];
            let read_started = Instant::now();

            let mut rows = stmt.query(params![key]).expect("sqlite query");
            let found = rows.next().expect("sqlite read next row").is_some();
            debug_assert!(found, "preloaded sqlite key should always be present");

            if let Some(hist) = histogram.as_mut() {
                let latency_ns = read_started.elapsed().as_nanos().max(1) as u64;
                let _ = (*hist).record(latency_ns);
            }
        }

        started.elapsed()
    }
}

fn build_binary_keys(key_space: usize) -> Vec<Vec<u8>> {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(key_space > 0, "key_space must be greater than zero");
    assert!(key_space < max_rows, "key_space exceeds key width");

    let mut key = [0_u8; KEY_LEN];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);
    let mut keys = Vec::with_capacity(key_space);

    for id in 0..key_space {
        encode_fixed_width_decimal(id, &mut key[KEY_PREFIX.len()..]);
        keys.push(key.to_vec());
    }

    keys
}

fn build_string_keys(key_space: usize) -> Vec<String> {
    build_binary_keys(key_space)
        .into_iter()
        .map(|key| String::from_utf8(key).expect("generated key must be valid utf-8"))
        .collect()
}

fn encode_fixed_width_decimal(mut value: usize, out: &mut [u8]) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

fn parse_env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|raw| raw.trim().parse::<u64>().ok()).unwrap_or(default)
}

fn parse_env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|value| *value > 0.0)
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

fn bench_sqlite_read_baseline(c: &mut Criterion) {
    let config = ReadBaselineConfig::from_env();
    let lsmdb_fixture = LsmReadFixture::build(config);
    let sqlite_fixture = SqliteReadFixture::build(config);

    let lsmdb_probe = lsmdb_fixture.run_probe(config.read_ops, config.seed);
    let sqlite_probe = sqlite_fixture.run_probe(config.read_ops, config.seed);

    let lsmdb_p50_us = lsmdb_probe.histogram.value_at_quantile(0.50) as f64 / 1_000.0;
    let lsmdb_p95_us = lsmdb_probe.histogram.value_at_quantile(0.95) as f64 / 1_000.0;
    let lsmdb_p99_us = lsmdb_probe.histogram.value_at_quantile(0.99) as f64 / 1_000.0;
    let lsmdb_throughput = lsmdb_probe.operations as f64 / lsmdb_probe.elapsed.as_secs_f64();

    let sqlite_p50_us = sqlite_probe.histogram.value_at_quantile(0.50) as f64 / 1_000.0;
    let sqlite_p95_us = sqlite_probe.histogram.value_at_quantile(0.95) as f64 / 1_000.0;
    let sqlite_p99_us = sqlite_probe.histogram.value_at_quantile(0.99) as f64 / 1_000.0;
    let sqlite_throughput = sqlite_probe.operations as f64 / sqlite_probe.elapsed.as_secs_f64();

    eprintln!(
        "sqlite_read_baseline_probe keyspace={} ops={} zipf={:.2} sqlite_sync={}",
        config.key_space,
        config.read_ops,
        config.zipf_exponent,
        config.sqlite_sync_mode.pragma_value(),
    );
    eprintln!(
        "lsmdb p50={:.2}us p95={:.2}us p99={:.2}us throughput={:.2} ops/s",
        lsmdb_p50_us, lsmdb_p95_us, lsmdb_p99_us, lsmdb_throughput
    );
    eprintln!(
        "sqlite p50={:.2}us p95={:.2}us p99={:.2}us throughput={:.2} ops/s",
        sqlite_p50_us, sqlite_p95_us, sqlite_p99_us, sqlite_throughput
    );

    let mut group = c.benchmark_group("sqlite_wal_point_read_baseline");
    group.sample_size(10);
    group.throughput(Throughput::Elements(config.read_ops as u64));
    group.bench_function(
        BenchmarkId::new(
            "lsmdb_zipf_ops_per_sec",
            format!("keys{}-a{:.2}", config.key_space, config.zipf_exponent),
        ),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for iteration in 0..iters {
                    let seed = config.seed.wrapping_add(iteration);
                    total += lsmdb_fixture.execute_reads(config.read_ops, seed, None);
                }
                total
            });
        },
    );
    group.bench_function(
        BenchmarkId::new(
            "sqlite_zipf_ops_per_sec",
            format!("keys{}-a{:.2}", config.key_space, config.zipf_exponent),
        ),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for iteration in 0..iters {
                    let seed = config.seed.wrapping_add(iteration);
                    total += sqlite_fixture.execute_reads(config.read_ops, seed, None);
                }
                total
            });
        },
    );
    group.finish();
}

criterion_group!(sqlite_read_baseline_benches, bench_sqlite_read_baseline);
criterion_main!(sqlite_read_baseline_benches);
