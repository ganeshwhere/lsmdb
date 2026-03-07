use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsmdb::storage::compaction::{CompactionStrategy, LeveledCompactionConfig};
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};

fn temp_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after epoch")
        .as_nanos();
    dir.push(format!("lsmdb-engine-{label}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).expect("create temp directory");
    dir
}

#[test]
fn engine_runs_background_compaction_and_preserves_reads() {
    let dir = temp_dir("background-compaction");
    let mut options = StorageEngineOptions::default();
    options.memtable_size_bytes = 256;
    options.compaction_strategy = CompactionStrategy::Leveled(LeveledCompactionConfig {
        level0_file_limit: 2,
        level_size_base_bytes: 1024,
        level_size_multiplier: 4,
        max_levels: 4,
    });

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("open storage engine");

        for batch in 0..6_u32 {
            for offset in 0..20_u32 {
                let id = batch * 20 + offset;
                let key = format!("k{id:04}");
                let value = format!("value-{id:04}");
                engine.put(key.as_bytes(), value.as_bytes()).expect("insert key/value pair");
            }
            engine.force_flush().expect("force flush batch");
        }

        engine
            .wait_for_background_compaction(Duration::from_secs(5))
            .expect("background compaction should complete");

        let metadata = engine.sstable_metadata();
        assert!(
            metadata.iter().any(|table| table.level > 0),
            "expected compaction to produce non-L0 tables: {metadata:?}"
        );

        let compaction_metrics = engine.compaction_metrics();
        assert!(
            compaction_metrics.completed_compactions >= 1,
            "expected at least one completed compaction, got {compaction_metrics:?}"
        );

        for sample in [0_u32, 37, 83, 119] {
            let key = format!("k{sample:04}");
            let expected = format!("value-{sample:04}").into_bytes();
            assert_eq!(engine.get(key.as_bytes()).expect("read key"), Some(expected));
        }
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}
