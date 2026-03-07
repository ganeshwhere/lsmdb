use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsmdb::storage::compaction::{CompactionStrategy, LeveledCompactionConfig};
use lsmdb::storage::engine::{StorageEngine, StorageEngineOptions};
use lsmdb::storage::memtable::decode_internal_key;
use lsmdb::storage::sstable::SSTableReader;

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
        assert!(compaction_metrics.total_bytes_on_disk > 0);
        assert!(
            compaction_metrics.live_bytes <= compaction_metrics.total_bytes_on_disk,
            "live bytes should not exceed total bytes: {compaction_metrics:?}"
        );

        for sample in [0_u32, 37, 83, 119] {
            let key = format!("k{sample:04}");
            let expected = format!("value-{sample:04}").into_bytes();
            assert_eq!(engine.get(key.as_bytes()).expect("read key"), Some(expected));
        }
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn compaction_collapses_old_versions_for_same_user_key() {
    let dir = temp_dir("collapse-versions");
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

        for version in 0..10_u32 {
            let hot_value = format!("hot-{version:02}");
            engine.put(b"hot-key", hot_value.as_bytes()).expect("write hot key");

            let cold_key = format!("cold-{version:02}");
            let cold_value = format!("value-{version:02}");
            engine.put(cold_key.as_bytes(), cold_value.as_bytes()).expect("write cold key");

            engine.force_flush().expect("force flush batch");
        }

        engine
            .wait_for_background_compaction(Duration::from_secs(5))
            .expect("background compaction should complete");

        assert_eq!(
            engine.get(b"hot-key").expect("read hot key"),
            Some(b"hot-09".to_vec())
        );

        let mut hot_versions = 0_usize;
        for table in engine.sstable_metadata() {
            let reader = SSTableReader::open(engine.sstable_dir().join(&table.file_name))
                .expect("open sstable reader");
            let rows = reader.scan_range(None, None).expect("scan table rows");

            for (internal_key, _value) in rows {
                if decode_internal_key(&internal_key)
                    .map(|decoded| decoded.user_key == b"hot-key")
                    .unwrap_or(false)
                {
                    hot_versions += 1;
                }
            }
        }

        assert_eq!(
            hot_versions, 1,
            "expected one compacted version for hot-key, found {hot_versions}"
        );

        let metrics = engine.compaction_metrics();
        assert!(metrics.total_bytes_on_disk > 0);
        assert!(
            metrics.live_bytes < metrics.total_bytes_on_disk,
            "expected compaction to reduce live/total ratio below 1.0, got {metrics:?}"
        );
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}
