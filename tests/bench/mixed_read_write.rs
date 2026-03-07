use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::wal::SyncMode;

const KEY_PREFIX: &[u8] = b"mix:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_KEY_SPACE: usize = 100_000;
const DEFAULT_DURATION_SECS: u64 = 60;
const DEFAULT_READ_RATIO_PERCENT: u8 = 80;
const DEFAULT_VALUE_SIZE_BYTES: usize = 128;
const DEFAULT_REPORT_INTERVAL_SECS: u64 = 5;
const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SEED: u64 = 0xC0FF_EE42_2026;

#[derive(Debug, Clone, Copy)]
struct WorkloadConfig {
    key_space: usize,
    duration: Duration,
    read_ratio_percent: u8,
    value_size_bytes: usize,
    report_interval: Duration,
    memtable_size_bytes: usize,
    wal_sync_mode: SyncMode,
    seed: u64,
}

impl WorkloadConfig {
    fn from_env() -> Self {
        let read_ratio_percent = parse_env_usize(
            "LSMDB_BENCH_MIXED_READ_RATIO_PERCENT",
            DEFAULT_READ_RATIO_PERCENT as usize,
        )
        .min(100) as u8;

        Self {
            key_space: parse_env_usize("LSMDB_BENCH_MIXED_KEYSPACE", DEFAULT_KEY_SPACE),
            duration: Duration::from_secs(parse_env_u64(
                "LSMDB_BENCH_MIXED_DURATION_SECS",
                DEFAULT_DURATION_SECS,
            )),
            read_ratio_percent,
            value_size_bytes: parse_env_usize(
                "LSMDB_BENCH_MIXED_VALUE_BYTES",
                DEFAULT_VALUE_SIZE_BYTES,
            ),
            report_interval: Duration::from_secs(parse_env_u64(
                "LSMDB_BENCH_MIXED_REPORT_INTERVAL_SECS",
                DEFAULT_REPORT_INTERVAL_SECS,
            )),
            memtable_size_bytes: parse_env_usize(
                "LSMDB_BENCH_MEMTABLE_BYTES",
                DEFAULT_MEMTABLE_SIZE_BYTES,
            ),
            wal_sync_mode: parse_env_sync_mode("LSMDB_BENCH_WAL_SYNC", SyncMode::OnCommit),
            seed: parse_env_u64("LSMDB_BENCH_MIXED_SEED", DEFAULT_SEED),
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
}

#[derive(Debug)]
struct WorkloadResult {
    elapsed: Duration,
    total_ops: u64,
    read_ops: u64,
    write_ops: u64,
    latency_all: Histogram<u64>,
    latency_read: Histogram<u64>,
    latency_write: Histogram<u64>,
    window_throughputs: Vec<f64>,
}

fn main() {
    let config = WorkloadConfig::from_env();
    eprintln!(
        "mixed_workload_config keyspace={} duration={}s read_ratio={} value_bytes={} report_interval={}s",
        config.key_space,
        config.duration.as_secs(),
        config.read_ratio_percent,
        config.value_size_bytes,
        config.report_interval.as_secs(),
    );

    let result = run_mixed_workload(config);
    print_summary(config, &result);
}

fn run_mixed_workload(config: WorkloadConfig) -> WorkloadResult {
    let keys = build_keys(config.key_space);
    let mut preload_value = vec![b'i'; config.value_size_bytes];
    encode_counter_bytes(0, &mut preload_value);

    let temp_dir = TempBenchDir::new("mixed-rw");
    let mut options = StorageEngineOptions {
        memtable_size_bytes: config.memtable_size_bytes,
        ..Default::default()
    };
    options.wal_options.sync_mode = config.wal_sync_mode;
    let engine = StorageEngine::open_with_options(temp_dir.path(), options)
        .expect("open storage engine for mixed workload");

    for key in &keys {
        engine.put(key, &preload_value).expect("preload key");
    }
    engine.force_flush().expect("flush preloaded keyspace");

    let mut latency_all =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("create all histogram");
    let mut latency_read =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("create read histogram");
    let mut latency_write =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("create write histogram");

    let mut rng = XorShift64::new(config.seed);
    let mut write_value = vec![b'w'; config.value_size_bytes];
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut next_report = start + config.report_interval;
    let mut last_report_time = start;
    let mut last_report_ops = 0_u64;
    let mut window_throughputs = Vec::new();

    let mut total_ops = 0_u64;
    let mut read_ops = 0_u64;
    let mut write_ops = 0_u64;

    while Instant::now() < deadline {
        let key_index = (rng.next_u64() as usize) % keys.len();
        let is_read = (rng.next_u64() % 100) < config.read_ratio_percent as u64;
        let op_started = Instant::now();

        if is_read {
            let value = engine.get(&keys[key_index]).expect("mixed read");
            debug_assert!(value.is_some(), "preloaded key should always be present");
            read_ops = read_ops.saturating_add(1);

            let latency_ns = op_started.elapsed().as_nanos().max(1) as u64;
            let _ = latency_read.record(latency_ns);
            let _ = latency_all.record(latency_ns);
        } else {
            encode_counter_bytes(total_ops, &mut write_value);
            engine.put(&keys[key_index], &write_value).expect("mixed write");
            write_ops = write_ops.saturating_add(1);

            let latency_ns = op_started.elapsed().as_nanos().max(1) as u64;
            let _ = latency_write.record(latency_ns);
            let _ = latency_all.record(latency_ns);
        }

        total_ops = total_ops.saturating_add(1);

        let now = Instant::now();
        if now >= next_report {
            let elapsed = now.duration_since(last_report_time).as_secs_f64();
            if elapsed > 0.0 {
                let interval_ops = total_ops.saturating_sub(last_report_ops);
                let throughput = interval_ops as f64 / elapsed;
                window_throughputs.push(throughput);
                eprintln!(
                    "mixed_workload_window t={:.1}s ops={} throughput={:.2} ops/s",
                    now.duration_since(start).as_secs_f64(),
                    interval_ops,
                    throughput,
                );
            }
            last_report_time = now;
            last_report_ops = total_ops;
            next_report = now + config.report_interval;
        }
    }

    let elapsed = start.elapsed();
    if elapsed.as_secs_f64() > 0.0 && total_ops > last_report_ops {
        let tail_elapsed =
            elapsed.saturating_sub(last_report_time.duration_since(start)).as_secs_f64();
        if tail_elapsed > 0.0 {
            let tail_ops = total_ops.saturating_sub(last_report_ops);
            window_throughputs.push(tail_ops as f64 / tail_elapsed);
        }
    }

    engine.force_flush().expect("flush workload writes");
    drop(engine);
    drop(temp_dir);

    WorkloadResult {
        elapsed,
        total_ops,
        read_ops,
        write_ops,
        latency_all,
        latency_read,
        latency_write,
        window_throughputs,
    }
}

fn print_summary(config: WorkloadConfig, result: &WorkloadResult) {
    let elapsed_secs = result.elapsed.as_secs_f64();
    let overall_ops_per_sec =
        if elapsed_secs > 0.0 { result.total_ops as f64 / elapsed_secs } else { 0.0 };
    let read_ratio = if result.total_ops > 0 {
        (result.read_ops as f64 / result.total_ops as f64) * 100.0
    } else {
        0.0
    };

    let (window_mean, window_min, window_max, window_stddev) =
        summarize_window_throughput(&result.window_throughputs);

    println!("mixed_read_write_summary");
    println!("duration_secs={:.2}", elapsed_secs);
    println!(
        "configured_mix={}read/{}write",
        config.read_ratio_percent,
        100 - config.read_ratio_percent
    );
    println!("actual_mix={:.2}read/{:.2}write", read_ratio, 100.0 - read_ratio);
    println!("total_ops={}", result.total_ops);
    println!("read_ops={}", result.read_ops);
    println!("write_ops={}", result.write_ops);
    println!("throughput_ops_per_sec={:.2}", overall_ops_per_sec);
    println!(
        "latency_all_us p50={:.2} p95={:.2} p99={:.2}",
        percentile_us(&result.latency_all, 0.50),
        percentile_us(&result.latency_all, 0.95),
        percentile_us(&result.latency_all, 0.99),
    );
    println!(
        "latency_read_us p50={:.2} p95={:.2} p99={:.2}",
        percentile_us(&result.latency_read, 0.50),
        percentile_us(&result.latency_read, 0.95),
        percentile_us(&result.latency_read, 0.99),
    );
    println!(
        "latency_write_us p50={:.2} p95={:.2} p99={:.2}",
        percentile_us(&result.latency_write, 0.50),
        percentile_us(&result.latency_write, 0.95),
        percentile_us(&result.latency_write, 0.99),
    );
    println!(
        "throughput_stability_ops_per_sec mean={:.2} min={:.2} max={:.2} stddev={:.2}",
        window_mean, window_min, window_max, window_stddev,
    );
}

fn summarize_window_throughput(values: &[f64]) -> (f64, f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }

    let mut min = values[0];
    let mut max = values[0];
    let mut sum = 0.0;
    for value in values {
        min = min.min(*value);
        max = max.max(*value);
        sum += *value;
    }
    let mean = sum / values.len() as f64;

    let mut variance_sum = 0.0;
    for value in values {
        let diff = *value - mean;
        variance_sum += diff * diff;
    }
    let stddev = (variance_sum / values.len() as f64).sqrt();

    (mean, min, max, stddev)
}

fn percentile_us(histogram: &Histogram<u64>, quantile: f64) -> f64 {
    if histogram.len() == 0 { 0.0 } else { histogram.value_at_quantile(quantile) as f64 / 1_000.0 }
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

fn encode_counter_bytes(counter: u64, out: &mut [u8]) {
    let bytes = counter.to_be_bytes();
    let take = out.len().min(bytes.len());
    out[..take].copy_from_slice(&bytes[..take]);
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
