use lsmdb::storage::compaction::{
    CompactionScheduler, CompactionStrategy, LeveledCompactionConfig, LeveledTrigger,
    TieredCompactionConfig, pick_leveled_compaction, pick_tiered_compaction,
};
use lsmdb::storage::manifest::version::{SSTableMetadata, VersionSet};

fn table(level: u32, id: u64, start: &str, end: &str, size: u64) -> SSTableMetadata {
    SSTableMetadata {
        table_id: id,
        level,
        file_name: format!("sst-{id:020}.sst"),
        smallest_key: start.as_bytes().to_vec(),
        largest_key: end.as_bytes().to_vec(),
        file_size_bytes: size,
    }
}

#[test]
fn leveled_compaction_detects_l0_overflow() {
    let mut versions = VersionSet::default();
    versions.add_table(table(0, 1, "a", "f", 1024));
    versions.add_table(table(0, 2, "g", "l", 1024));
    versions.add_table(table(0, 3, "m", "r", 1024));
    versions.add_table(table(0, 4, "s", "z", 1024));

    let config = LeveledCompactionConfig::default();
    let plan = pick_leveled_compaction(&versions, &config).expect("expected leveled plan");

    assert_eq!(plan.trigger, LeveledTrigger::Level0Overflow);
    assert_eq!(plan.source_level, 0);
    assert_eq!(plan.target_level, 1);
    assert!(!plan.source_inputs.is_empty());
}

#[test]
fn tiered_compaction_triggers_on_component_threshold() {
    let mut versions = VersionSet::default();
    versions.add_table(table(0, 11, "a", "d", 10 * 1024));
    versions.add_table(table(0, 12, "e", "h", 11 * 1024));
    versions.add_table(table(0, 13, "i", "l", 12 * 1024));
    versions.add_table(table(0, 14, "m", "p", 13 * 1024));

    let config = TieredCompactionConfig::default();
    let plan = pick_tiered_compaction(&versions, &config).expect("expected tiered plan");

    assert_eq!(plan.input_tables.len(), 4);
    assert_eq!(plan.output_level, 0);
}

#[test]
fn scheduler_prioritizes_higher_scored_plan() {
    let mut versions = VersionSet::default();
    versions.add_table(table(0, 1, "a", "d", 1024));
    versions.add_table(table(0, 2, "e", "h", 1024));
    versions.add_table(table(0, 3, "i", "l", 1024));
    versions.add_table(table(0, 4, "m", "q", 1024));

    let mut scheduler = CompactionScheduler::default();

    let leveled_task = scheduler
        .schedule_from_versions(
            &versions,
            &CompactionStrategy::Leveled(LeveledCompactionConfig::default()),
        )
        .expect("leveled compaction should be scheduled");

    versions.add_table(table(0, 5, "r", "u", 1024));
    versions.add_table(table(0, 6, "v", "z", 1024));

    let tiered_task = scheduler
        .schedule_from_versions(
            &versions,
            &CompactionStrategy::Tiered(TieredCompactionConfig::default()),
        )
        .expect("tiered compaction should be scheduled");

    let first = scheduler.pop_next().expect("first task should exist");
    let second = scheduler.pop_next().expect("second task should exist");

    // Expect deterministic IDs and valid ordering by the computed priority score.
    assert!(first.priority >= second.priority);
    assert!(first.task_id == leveled_task || first.task_id == tiered_task);

    scheduler.mark_completed(first.task_id);
    scheduler.mark_completed(second.task_id);

    assert_eq!(scheduler.pending_count(), 0);
    assert_eq!(scheduler.in_flight_count(), 0);
}
