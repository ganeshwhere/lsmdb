use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lsmdb::storage::wal::{
    SyncMode, WalReader, WalWriter, WalWriterOptions,
    DEFAULT_SEGMENT_SIZE_BYTES as WAL_DEFAULT_SEGMENT_SIZE_BYTES,
};

const DEFAULT_RECORDS: usize = 250_000;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_RUNS: usize = 5;
const DEFAULT_SEGMENT_SIZE_BYTES: u64 = WAL_DEFAULT_SEGMENT_SIZE_BYTES;
const DEFAULT_CORRUPTED_TAIL_BYTES: usize = 19;

#[derive(Debug, Clone, Copy)]
struct WalRecoveryConfig {
    records: usize,
    payload_bytes: usize,
    runs: usize,
    segment_size_bytes: u64,
    sync_mode: SyncMode,
    corrupted_tail_bytes: usize,
}

impl WalRecoveryConfig {
    fn from_env() -> Self {
        Self {
            records: parse_env_usize("LSMDB_BENCH_WAL_RECORDS", DEFAULT_RECORDS),
            payload_bytes: parse_env_usize("LSMDB_BENCH_WAL_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES),
            runs: parse_env_usize("LSMDB_BENCH_WAL_RUNS", DEFAULT_RUNS),
            segment_size_bytes: parse_env_u64(
                "LSMDB_BENCH_WAL_SEGMENT_BYTES",
                DEFAULT_SEGMENT_SIZE_BYTES,
            ),
            sync_mode: parse_env_sync_mode("LSMDB_BENCH_WAL_SYNC", SyncMode::Never),
            corrupted_tail_bytes: parse_env_usize(
                "LSMDB_BENCH_WAL_CORRUPTED_TAIL_BYTES",
                DEFAULT_CORRUPTED_TAIL_BYTES,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Clean,
    CrashTail,
}

impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Scenario::Clean => "clean_shutdown",
            Scenario::CrashTail => "crash_tail_corruption",
        }
    }
}

#[derive(Debug, Default)]
struct ScenarioSummary {
    samples: Vec<Duration>,
    total_dropped_corrupted_tails: usize,
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

fn main() {
    let config = WalRecoveryConfig::from_env();
    println!("wal_recovery_benchmark");
    println!(
        "records={} payload_bytes={} runs={} segment_bytes={} sync_mode={:?}",
        config.records,
        config.payload_bytes,
        config.runs,
        config.segment_size_bytes,
        config.sync_mode
    );

    let clean = run_scenario(Scenario::Clean, config);
    let crash = run_scenario(Scenario::CrashTail, config);

    print_summary(Scenario::Clean, config, &clean);
    print_summary(Scenario::CrashTail, config, &crash);
}

fn run_scenario(scenario: Scenario, config: WalRecoveryConfig) -> ScenarioSummary {
    let mut summary = ScenarioSummary::default();

    for run in 0..config.runs {
        let dir = TempBenchDir::new(scenario.label());
        let segment_path = seed_wal(dir.path(), config);

        if matches!(scenario, Scenario::CrashTail) {
            append_corrupted_tail(&segment_path, config.corrupted_tail_bytes);
        }

        let started = Instant::now();
        let reader = WalReader::open(dir.path()).expect("open wal reader");
        let replay = reader.replay().expect("replay wal");
        let elapsed = started.elapsed();

        assert_eq!(
            replay.records.len(),
            config.records,
            "replay record count mismatch for scenario={} run={}",
            scenario.label(),
            run
        );

        match scenario {
            Scenario::Clean => {
                assert_eq!(
                    replay.dropped_corrupted_tails, 0,
                    "clean replay should not drop tails (run={run})"
                );
            }
            Scenario::CrashTail => {
                assert!(
                    replay.dropped_corrupted_tails >= 1,
                    "crash-tail replay should drop at least one tail (run={run})"
                );
            }
        }

        summary.total_dropped_corrupted_tails =
            summary.total_dropped_corrupted_tails.saturating_add(replay.dropped_corrupted_tails);
        summary.samples.push(elapsed);
    }

    summary
}

fn seed_wal(dir: &Path, config: WalRecoveryConfig) -> PathBuf {
    let options = WalWriterOptions {
        segment_size_bytes: config.segment_size_bytes,
        sync_mode: config.sync_mode,
    };
    let mut writer = WalWriter::open_with_options(dir, options).expect("open wal writer");

    let mut payload = vec![b'r'; config.payload_bytes];
    for index in 0..config.records {
        encode_counter(index as u64, &mut payload);
        writer.append(&payload).expect("append wal record");
    }
    writer.commit().expect("commit wal data");

    let segment_path = writer.current_segment_path().to_path_buf();
    drop(writer);
    segment_path
}

fn append_corrupted_tail(path: &Path, bytes: usize) {
    let mut file = OpenOptions::new().append(true).open(path).expect("open wal segment");
    let garbage = vec![0xAB; bytes];
    file.write_all(&garbage).expect("append corrupted tail bytes");
    file.flush().expect("flush corrupted tail bytes");
}

fn print_summary(scenario: Scenario, config: WalRecoveryConfig, summary: &ScenarioSummary) {
    let p50 = percentile(&summary.samples, 0.50);
    let p95 = percentile(&summary.samples, 0.95);
    let max = percentile(&summary.samples, 1.00);
    let avg_secs = summary.samples.iter().map(Duration::as_secs_f64).sum::<f64>()
        / summary.samples.len() as f64;
    let avg = Duration::from_secs_f64(avg_secs);

    let replayed_bytes = config.records as u64 * config.payload_bytes as u64;
    let throughput_records_per_sec = config.records as f64 / p50.as_secs_f64().max(f64::EPSILON);
    let throughput_mb_per_sec =
        replayed_bytes as f64 / p50.as_secs_f64().max(f64::EPSILON) / (1024.0 * 1024.0);

    println!("scenario={}", scenario.label());
    println!("  avg_recovery_ms={:.3}", avg.as_secs_f64() * 1000.0);
    println!("  p50_recovery_ms={:.3}", p50.as_secs_f64() * 1000.0);
    println!("  p95_recovery_ms={:.3}", p95.as_secs_f64() * 1000.0);
    println!("  max_recovery_ms={:.3}", max.as_secs_f64() * 1000.0);
    println!("  replay_records_per_sec={:.2}", throughput_records_per_sec);
    println!("  replay_mb_per_sec={:.2}", throughput_mb_per_sec);
    println!("  total_dropped_corrupted_tails={}", summary.total_dropped_corrupted_tails);
}

fn percentile(samples: &[Duration], quantile: f64) -> Duration {
    assert!(!samples.is_empty(), "expected at least one sample");
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();

    if quantile <= 0.0 {
        return ordered[0];
    }
    if quantile >= 1.0 {
        return ordered[ordered.len() - 1];
    }

    let index = ((ordered.len() - 1) as f64 * quantile).round() as usize;
    ordered[index]
}

fn encode_counter(counter: u64, out: &mut [u8]) {
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
