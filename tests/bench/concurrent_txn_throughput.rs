use std::env;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use lsmdb::mvcc::{MvccStore, TransactionError};

const KEY_PREFIX: &[u8] = b"tx:";
const KEY_DIGITS: usize = 10;
const KEY_LEN: usize = KEY_PREFIX.len() + KEY_DIGITS;

const DEFAULT_CLIENTS: usize = 16;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_KEY_SPACE: usize = 20_000;
const DEFAULT_VALUE_SIZE_BYTES: usize = 64;
const DEFAULT_WRITE_RATIO_PERCENT: u8 = 60;
const DEFAULT_READS_PER_TX: usize = 2;
const DEFAULT_WRITES_PER_TX: usize = 1;
const DEFAULT_SEED: u64 = 0xB3A7_98F4_2026;

#[derive(Debug, Clone, Copy)]
struct BenchmarkConfig {
    clients: usize,
    duration: Duration,
    key_space: usize,
    value_size_bytes: usize,
    write_ratio_percent: u8,
    reads_per_tx: usize,
    writes_per_tx: usize,
    seed: u64,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        let write_ratio_percent = parse_env_usize(
            "LSMDB_BENCH_TXN_WRITE_RATIO_PERCENT",
            DEFAULT_WRITE_RATIO_PERCENT as usize,
        )
        .min(100) as u8;

        Self {
            clients: parse_env_usize("LSMDB_BENCH_TXN_CLIENTS", DEFAULT_CLIENTS),
            duration: Duration::from_secs(parse_env_u64(
                "LSMDB_BENCH_TXN_DURATION_SECS",
                DEFAULT_DURATION_SECS,
            )),
            key_space: parse_env_usize("LSMDB_BENCH_TXN_KEYSPACE", DEFAULT_KEY_SPACE),
            value_size_bytes: parse_env_usize(
                "LSMDB_BENCH_TXN_VALUE_BYTES",
                DEFAULT_VALUE_SIZE_BYTES,
            ),
            write_ratio_percent,
            reads_per_tx: parse_env_usize("LSMDB_BENCH_TXN_READS_PER_TX", DEFAULT_READS_PER_TX),
            writes_per_tx: parse_env_usize("LSMDB_BENCH_TXN_WRITES_PER_TX", DEFAULT_WRITES_PER_TX),
            seed: parse_env_u64("LSMDB_BENCH_TXN_SEED", DEFAULT_SEED),
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
}

#[derive(Debug)]
struct WorkerResult {
    attempted: u64,
    committed: u64,
    aborted_conflict: u64,
    aborted_other: u64,
    read_ops: u64,
    write_ops: u64,
    latency_histogram: Histogram<u64>,
}

impl WorkerResult {
    fn new() -> Self {
        Self {
            attempted: 0,
            committed: 0,
            aborted_conflict: 0,
            aborted_other: 0,
            read_ops: 0,
            write_ops: 0,
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                .expect("create worker histogram"),
        }
    }
}

#[derive(Debug)]
struct BenchmarkResult {
    elapsed: Duration,
    attempted: u64,
    committed: u64,
    aborted_conflict: u64,
    aborted_other: u64,
    read_ops: u64,
    write_ops: u64,
    latency_histogram: Histogram<u64>,
}

fn main() {
    let config = BenchmarkConfig::from_env();
    let result = run_benchmark(config);
    print_summary(config, &result);
}

fn run_benchmark(config: BenchmarkConfig) -> BenchmarkResult {
    let store = Arc::new(MvccStore::new());
    let keys = Arc::new(build_keys(config.key_space));
    seed_store(&store, &keys, config.value_size_bytes);

    let start_barrier = Arc::new(Barrier::new(config.clients.saturating_add(1)));
    let mut handles = Vec::with_capacity(config.clients);

    for worker_id in 0..config.clients {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        let barrier = Arc::clone(&start_barrier);
        let worker_seed = config.seed.wrapping_add(worker_id as u64).rotate_left(11);

        handles.push(thread::spawn(move || {
            run_worker(worker_id, store, keys, barrier, config, worker_seed)
        }));
    }

    start_barrier.wait();
    let started = Instant::now();

    let mut attempted = 0_u64;
    let mut committed = 0_u64;
    let mut aborted_conflict = 0_u64;
    let mut aborted_other = 0_u64;
    let mut read_ops = 0_u64;
    let mut write_ops = 0_u64;
    let mut latency_histogram =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("create latency histogram");

    for handle in handles {
        let worker = handle.join().expect("worker thread should complete");
        attempted = attempted.saturating_add(worker.attempted);
        committed = committed.saturating_add(worker.committed);
        aborted_conflict = aborted_conflict.saturating_add(worker.aborted_conflict);
        aborted_other = aborted_other.saturating_add(worker.aborted_other);
        read_ops = read_ops.saturating_add(worker.read_ops);
        write_ops = write_ops.saturating_add(worker.write_ops);

        let _ = latency_histogram.add(&worker.latency_histogram);
    }

    let elapsed = started.elapsed();
    BenchmarkResult {
        elapsed,
        attempted,
        committed,
        aborted_conflict,
        aborted_other,
        read_ops,
        write_ops,
        latency_histogram,
    }
}

fn run_worker(
    worker_id: usize,
    store: Arc<MvccStore>,
    keys: Arc<Vec<Vec<u8>>>,
    barrier: Arc<Barrier>,
    config: BenchmarkConfig,
    seed: u64,
) -> WorkerResult {
    let mut rng = XorShift64::new(seed);
    let mut result = WorkerResult::new();
    let mut value_buffer = vec![b'v'; config.value_size_bytes];

    barrier.wait();
    let deadline = Instant::now() + config.duration;

    while Instant::now() < deadline {
        let started = Instant::now();
        let mut tx = store.begin_transaction();
        let mut txn_failed = false;

        for _ in 0..config.reads_per_tx {
            let read_index = (rng.next_u64() as usize) % keys.len();
            let read_key = &keys[read_index];
            if tx.get(read_key).is_err() {
                result.aborted_other = result.aborted_other.saturating_add(1);
                tx.rollback();
                txn_failed = true;
                break;
            }
            result.read_ops = result.read_ops.saturating_add(1);
        }

        if txn_failed {
            result.attempted = result.attempted.saturating_add(1);
            let latency_ns = started.elapsed().as_nanos().max(1) as u64;
            let _ = result.latency_histogram.record(latency_ns);
            continue;
        }

        let is_write = (rng.next_u64() % 100) < config.write_ratio_percent as u64;
        if is_write {
            for _ in 0..config.writes_per_tx {
                let write_index = (rng.next_u64() as usize) % keys.len();
                let write_key = &keys[write_index];
                encode_worker_counter(worker_id as u32, result.attempted, &mut value_buffer);
                if tx.put(write_key, &value_buffer).is_err() {
                    result.aborted_other = result.aborted_other.saturating_add(1);
                    tx.rollback();
                    txn_failed = true;
                    break;
                }
                result.write_ops = result.write_ops.saturating_add(1);
            }
        }

        if txn_failed {
            result.attempted = result.attempted.saturating_add(1);
            let latency_ns = started.elapsed().as_nanos().max(1) as u64;
            let _ = result.latency_histogram.record(latency_ns);
            continue;
        }

        match tx.commit() {
            Ok(_) => {
                result.committed = result.committed.saturating_add(1);
            }
            Err(TransactionError::WriteWriteConflict { .. }) => {
                result.aborted_conflict = result.aborted_conflict.saturating_add(1);
                tx.rollback();
            }
            Err(TransactionError::Closed) => {
                result.aborted_other = result.aborted_other.saturating_add(1);
            }
        }

        result.attempted = result.attempted.saturating_add(1);
        let latency_ns = started.elapsed().as_nanos().max(1) as u64;
        let _ = result.latency_histogram.record(latency_ns);
    }

    result
}

fn seed_store(store: &MvccStore, keys: &[Vec<u8>], value_size_bytes: usize) {
    let mut tx = store.begin_transaction();
    let value = vec![b'i'; value_size_bytes];
    for key in keys {
        tx.put(key, &value).expect("seed key/value");
    }
    tx.commit().expect("commit seed data");
}

fn build_keys(key_space: usize) -> Vec<Vec<u8>> {
    let max_rows = 10_usize.pow(KEY_DIGITS as u32);
    assert!(key_space > 0, "key_space must be non-zero");
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

fn encode_fixed_width_decimal(mut value: usize, out: &mut [u8]) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

fn encode_worker_counter(worker_id: u32, counter: u64, out: &mut [u8]) {
    out.fill(b'v');
    let mut prefix = [0_u8; 16];
    let worker_bytes = worker_id.to_be_bytes();
    let counter_bytes = counter.to_be_bytes();
    prefix[0..4].copy_from_slice(&worker_bytes);
    prefix[4..12].copy_from_slice(&counter_bytes);
    let take = out.len().min(prefix.len());
    out[..take].copy_from_slice(&prefix[..take]);
}

fn print_summary(config: BenchmarkConfig, result: &BenchmarkResult) {
    let elapsed_secs = result.elapsed.as_secs_f64().max(f64::EPSILON);
    let attempted_tps = result.attempted as f64 / elapsed_secs;
    let committed_tps = result.committed as f64 / elapsed_secs;
    let abort_tps = (result.aborted_conflict + result.aborted_other) as f64 / elapsed_secs;
    let conflict_rate = if result.attempted > 0 {
        (result.aborted_conflict as f64 / result.attempted as f64) * 100.0
    } else {
        0.0
    };

    let p50 = if result.latency_histogram.len() > 0 {
        result.latency_histogram.value_at_quantile(0.50)
    } else {
        0
    };
    let p95 = if result.latency_histogram.len() > 0 {
        result.latency_histogram.value_at_quantile(0.95)
    } else {
        0
    };
    let p99 = if result.latency_histogram.len() > 0 {
        result.latency_histogram.value_at_quantile(0.99)
    } else {
        0
    };

    println!("concurrent_txn_throughput");
    println!(
        "clients={} duration_secs={} keyspace={} write_ratio_percent={} reads_per_tx={} writes_per_tx={}",
        config.clients,
        config.duration.as_secs(),
        config.key_space,
        config.write_ratio_percent,
        config.reads_per_tx,
        config.writes_per_tx
    );
    println!("attempted_transactions={}", result.attempted);
    println!("committed_transactions={}", result.committed);
    println!("aborted_conflict_transactions={}", result.aborted_conflict);
    println!("aborted_other_transactions={}", result.aborted_other);
    println!("read_operations={}", result.read_ops);
    println!("write_operations={}", result.write_ops);
    println!("attempted_txn_per_sec={:.2}", attempted_tps);
    println!("committed_txn_per_sec={:.2}", committed_tps);
    println!("aborted_txn_per_sec={:.2}", abort_tps);
    println!("conflict_rate_percent={:.3}", conflict_rate);
    println!("txn_latency_p50_us={:.3}", p50 as f64 / 1_000.0);
    println!("txn_latency_p95_us={:.3}", p95 as f64 / 1_000.0);
    println!("txn_latency_p99_us={:.3}", p99 as f64 / 1_000.0);
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
