use std::env;
use std::time::Instant;

use lsmdb::storage::compaction::{
    CompactionPlan, CompactionScheduler, CompactionStrategy, LeveledCompactionConfig,
    ScheduledCompaction, TieredCompactionConfig,
};
use lsmdb::storage::manifest::version::{SSTableMetadata, VersionSet};

const DEFAULT_USER_TABLES: usize = 5000;
const DEFAULT_USER_TABLE_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_KEY_SPACE: u32 = 1_000_000;
const DEFAULT_RANGE_SPAN: u32 = 20_000;
const DEFAULT_SEED: u64 = 0xA11C_EFA9_2026;

#[derive(Debug, Clone, Copy)]
struct AmplificationConfig {
    user_tables: usize,
    user_table_size_bytes: u64,
    key_space: u32,
    range_span: u32,
    seed: u64,
}

impl AmplificationConfig {
    fn from_env() -> Self {
        let key_space =
            parse_env_usize("LSMDB_BENCH_COMPACTION_KEY_SPACE", DEFAULT_KEY_SPACE as usize) as u32;
        let range_span =
            parse_env_usize("LSMDB_BENCH_COMPACTION_RANGE_SPAN", DEFAULT_RANGE_SPAN as usize)
                as u32;

        Self {
            user_tables: parse_env_usize("LSMDB_BENCH_COMPACTION_USER_TABLES", DEFAULT_USER_TABLES),
            user_table_size_bytes: parse_env_u64(
                "LSMDB_BENCH_COMPACTION_USER_TABLE_SIZE_BYTES",
                DEFAULT_USER_TABLE_SIZE_BYTES,
            ),
            key_space: key_space.max(2),
            range_span: range_span.max(1),
            seed: parse_env_u64("LSMDB_BENCH_COMPACTION_SEED", DEFAULT_SEED),
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
struct AmplificationResult {
    strategy: &'static str,
    elapsed_ms: u128,
    user_bytes: u64,
    compaction_bytes: u64,
    write_amplification: f64,
    completed_compactions: u64,
    final_table_count: usize,
    total_bytes_on_disk: u64,
}

fn main() {
    let config = AmplificationConfig::from_env();

    let leveled = run_strategy(
        "leveled",
        CompactionStrategy::Leveled(LeveledCompactionConfig::default()),
        config,
    );
    let tiered = run_strategy(
        "tiered",
        CompactionStrategy::Tiered(TieredCompactionConfig::default()),
        config,
    );

    println!("compaction_write_amplification");
    println!(
        "input_tables={} table_size_bytes={} key_space={} range_span={}",
        config.user_tables, config.user_table_size_bytes, config.key_space, config.range_span
    );
    print_result(&leveled);
    print_result(&tiered);

    if tiered.write_amplification > 0.0 {
        println!(
            "leveled_vs_tiered_waf_ratio={:.4}",
            leveled.write_amplification / tiered.write_amplification
        );
    }
}

fn run_strategy(
    strategy_name: &'static str,
    strategy: CompactionStrategy,
    config: AmplificationConfig,
) -> AmplificationResult {
    let started = Instant::now();
    let mut rng = XorShift64::new(config.seed);
    let mut versions = VersionSet::default();
    let mut scheduler = CompactionScheduler::default();
    let mut next_table_id = 1_u64;

    for _ in 0..config.user_tables {
        let (smallest, largest) = random_range(&mut rng, config.key_space, config.range_span);
        let table =
            make_table_metadata(next_table_id, 0, smallest, largest, config.user_table_size_bytes);
        next_table_id = next_table_id.saturating_add(1);
        versions.add_table(table);
        scheduler.metrics.record_user_write(config.user_table_size_bytes);

        run_pending_compactions(&mut versions, &mut scheduler, &strategy, &mut next_table_id);
    }

    run_pending_compactions(&mut versions, &mut scheduler, &strategy, &mut next_table_id);

    let total_bytes_on_disk =
        versions.all_tables_newest_first().iter().map(|table| table.file_size_bytes).sum::<u64>();
    scheduler.metrics.set_space_bytes(
        config.user_tables as u64 * config.user_table_size_bytes,
        total_bytes_on_disk,
    );

    let write_amplification = scheduler.metrics.write_amplification().unwrap_or(0.0);
    AmplificationResult {
        strategy: strategy_name,
        elapsed_ms: started.elapsed().as_millis(),
        user_bytes: scheduler.metrics.user_bytes_written,
        compaction_bytes: scheduler.metrics.compaction_bytes_written,
        write_amplification,
        completed_compactions: scheduler.metrics.completed_compactions,
        final_table_count: versions.total_table_count(),
        total_bytes_on_disk,
    }
}

fn run_pending_compactions(
    versions: &mut VersionSet,
    scheduler: &mut CompactionScheduler,
    strategy: &CompactionStrategy,
    next_table_id: &mut u64,
) {
    loop {
        if scheduler.schedule_from_versions(versions, strategy).is_none() {
            break;
        }

        let Some(task) = scheduler.pop_next() else {
            break;
        };
        apply_compaction(task, versions, scheduler, next_table_id);
    }
}

fn apply_compaction(
    task: ScheduledCompaction,
    versions: &mut VersionSet,
    scheduler: &mut CompactionScheduler,
    next_table_id: &mut u64,
) {
    let (inputs, output_level, output_size) = match &task.plan {
        CompactionPlan::Leveled(plan) => {
            let mut inputs =
                Vec::with_capacity(plan.source_inputs.len() + plan.target_inputs.len());
            inputs.extend(plan.source_inputs.iter().cloned());
            inputs.extend(plan.target_inputs.iter().cloned());
            (inputs, plan.target_level, plan.estimated_output_size_bytes())
        }
        CompactionPlan::Tiered(plan) => {
            (plan.input_tables.clone(), plan.output_level, plan.estimated_output_size_bytes())
        }
    };

    if inputs.is_empty() {
        scheduler.mark_completed(task.task_id);
        return;
    }

    for table in &inputs {
        let _ = versions.remove_table(table.level, table.table_id);
    }

    let (smallest_key, largest_key) = merged_key_bounds(&inputs);
    let output_table = SSTableMetadata {
        table_id: *next_table_id,
        level: output_level,
        file_name: format!("sst-{:020}.sst", *next_table_id),
        smallest_key,
        largest_key,
        file_size_bytes: output_size,
    };
    *next_table_id = next_table_id.saturating_add(1);

    versions.add_table(output_table);
    scheduler.metrics.record_compaction_write(output_size);
    scheduler.mark_completed(task.task_id);
}

fn merged_key_bounds(inputs: &[SSTableMetadata]) -> (Vec<u8>, Vec<u8>) {
    let mut smallest = inputs[0].smallest_key.clone();
    let mut largest = inputs[0].largest_key.clone();

    for table in &inputs[1..] {
        if table.smallest_key < smallest {
            smallest = table.smallest_key.clone();
        }
        if table.largest_key > largest {
            largest = table.largest_key.clone();
        }
    }

    (smallest, largest)
}

fn random_range(rng: &mut XorShift64, key_space: u32, span: u32) -> (Vec<u8>, Vec<u8>) {
    let bounded_space = key_space.max(2);
    let bounded_span = span.min(bounded_space - 1).max(1);
    let max_start = bounded_space.saturating_sub(bounded_span).max(1);
    let start = (rng.next_u64() % max_start as u64) as u32;
    let end = start.saturating_add(bounded_span).min(bounded_space - 1);
    (encode_key(start), encode_key(end))
}

fn encode_key(id: u32) -> Vec<u8> {
    format!("k{id:010}").into_bytes()
}

fn make_table_metadata(
    table_id: u64,
    level: u32,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    file_size_bytes: u64,
) -> SSTableMetadata {
    SSTableMetadata {
        table_id,
        level,
        file_name: format!("sst-{table_id:020}.sst"),
        smallest_key,
        largest_key,
        file_size_bytes,
    }
}

fn print_result(result: &AmplificationResult) {
    println!("strategy={}", result.strategy);
    println!("  elapsed_ms={}", result.elapsed_ms);
    println!("  user_bytes={}", result.user_bytes);
    println!("  compaction_bytes={}", result.compaction_bytes);
    println!("  write_amplification={:.4}", result.write_amplification);
    println!("  completed_compactions={}", result.completed_compactions);
    println!("  final_table_count={}", result.final_table_count);
    println!("  total_bytes_on_disk={}", result.total_bytes_on_disk);
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
