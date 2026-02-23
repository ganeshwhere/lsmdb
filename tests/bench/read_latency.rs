use std::cmp::Ordering;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hdrhistogram::Histogram;
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::wal::SyncMode;

const KEY_PREFIX: &[u8] = b"read:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_KEY_SPACE: usize = 100_000;
const DEFAULT_READ_OPS: usize = 200_000;
const DEFAULT_VALUE_SIZE_BYTES: usize = 128;
const DEFAULT_ZIPF_EXPONENT: f64 = 1.2;
const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SEED: u64 = 0x4d59_5df4_d0f3_3173;

#[derive(Debug, Clone, Copy)]
struct ReadLatencyConfig {
    key_space: usize,
    read_ops: usize,
    value_size_bytes: usize,
    zipf_exponent: f64,
    memtable_size_bytes: usize,
    preload_wal_sync_mode: SyncMode,
    seed: u64,
}

impl ReadLatencyConfig {
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
            preload_wal_sync_mode: parse_env_sync_mode("LSMDB_BENCH_WAL_SYNC", SyncMode::OnCommit),
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
        assert!(exponent > 0.0, "zipf exponent must be > 0");

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
struct ReadFixture {
    _dir: TempBenchDir,
    engine: StorageEngine,
    keys: Vec<Vec<u8>>,
    sampler: ZipfSampler,
}

#[derive(Debug)]
struct LatencyProbe {
    elapsed: Duration,
    histogram: Histogram<u64>,
    operations: usize,
}

impl ReadFixture {
    fn build(config: ReadLatencyConfig) -> Self {
        let keys = build_keys(config.key_space);
        let value = vec![b'r'; config.value_size_bytes];
        let dir = TempBenchDir::new("read-latency");

        {
            let mut options = StorageEngineOptions {
                memtable_size_bytes: config.memtable_size_bytes,
                ..Default::default()
            };
            options.wal_options.sync_mode = config.preload_wal_sync_mode;
            let engine =
                StorageEngine::open_with_options(dir.path(), options).expect("open preload engine");

            for key in &keys {
                engine.put(key, &value).expect("preload key/value");
            }
            engine.force_flush().expect("flush preloaded read fixture data");
        }

        let read_options = StorageEngineOptions {
            memtable_size_bytes: config.memtable_size_bytes,
            ..Default::default()
        };
        let engine = StorageEngine::open_with_options(dir.path(), read_options)
            .expect("open read fixture engine");

        Self {
            _dir: dir,
            engine,
            keys,
            sampler: ZipfSampler::new(config.key_space, config.zipf_exponent),
        }
    }

    fn run_probe(&self, operations: usize, seed: u64) -> LatencyProbe {
        let mut histogram =
            Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("create histogram");
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
            let value = self.engine.get(key).expect("point read");
            debug_assert!(value.is_some(), "fixture key should always be present");

            if let Some(hist) = histogram.as_mut() {
                let latency_ns = read_started.elapsed().as_nanos().max(1) as u64;
                let _ = (*hist).record(latency_ns);
            }
        }

        started.elapsed()
    }
}

fn build_keys(key_space: usize) -> Vec<Vec<u8>> {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(key_space < max_rows, "key_space exceeds key width");

    let mut keys = Vec::with_capacity(key_space);
    let mut key = [0_u8; KEY_LEN];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);

    for id in 0..key_space {
        encode_fixed_width_decimal(id, &mut key[KEY_PREFIX.len()..]);
        keys.push(key.to_vec());
    }

    keys
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

fn bench_point_read_latency(c: &mut Criterion) {
    let config = ReadLatencyConfig::from_env();
    let fixture = ReadFixture::build(config);

    let probe = fixture.run_probe(config.read_ops, config.seed);
    let p50_us = probe.histogram.value_at_quantile(0.50) as f64 / 1_000.0;
    let p95_us = probe.histogram.value_at_quantile(0.95) as f64 / 1_000.0;
    let p99_us = probe.histogram.value_at_quantile(0.99) as f64 / 1_000.0;
    let throughput = probe.operations as f64 / probe.elapsed.as_secs_f64();
    eprintln!(
        "read_latency_probe keyspace={} ops={} zipf={:.2} p50={:.2}us p95={:.2}us p99={:.2}us throughput={:.2} ops/s",
        config.key_space,
        probe.operations,
        config.zipf_exponent,
        p50_us,
        p95_us,
        p99_us,
        throughput,
    );

    let mut group = c.benchmark_group("point_read_latency_zipf");
    group.sample_size(10);
    group.throughput(Throughput::Elements(config.read_ops as u64));
    group.bench_function(
        BenchmarkId::new(
            "zipf_ops_per_sec",
            format!("keys{}-a{:.2}", config.key_space, config.zipf_exponent),
        ),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for iteration in 0..iters {
                    let seed = config.seed.wrapping_add(iteration);
                    total += fixture.execute_reads(config.read_ops, seed, None);
                }
                total
            });
        },
    );
    group.finish();
}

criterion_group!(read_latency_benches, bench_point_read_latency);
criterion_main!(read_latency_benches);
