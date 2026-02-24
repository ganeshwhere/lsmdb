use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsmdb::storage::compaction::CompactionMetrics;
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
fn engine_writes_reads_and_flushes_to_sstable() {
    let dir = temp_dir("flush");

    {
        let options =
            StorageEngineOptions { memtable_size_bytes: 256, ..StorageEngineOptions::default() };
        let engine = StorageEngine::open_with_options(&dir, options).expect("open storage engine");

        for i in 0..180_u32 {
            let key = format!("k{i:04}");
            let value = format!("v{i:04}");
            engine.put(key.as_bytes(), value.as_bytes()).expect("insert key/value pair");
        }

        engine.force_flush().expect("force-flush memtables");

        assert!(engine.sstable_count() >= 1);
        assert_eq!(engine.get(b"k0042").expect("get existing key"), Some(b"v0042".to_vec()));
        assert_eq!(engine.get(b"missing").expect("get missing key"), None);
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn engine_recovers_from_manifest_and_wal_after_restart() {
    let dir = temp_dir("restart");
    let options =
        StorageEngineOptions { memtable_size_bytes: 192, ..StorageEngineOptions::default() };

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("open storage engine");
        engine.put(b"alpha", b"1").expect("put alpha");
        engine.put(b"beta", b"2").expect("put beta");

        // Persist a snapshot to SSTable via background flush.
        engine.force_flush().expect("flush to sstable");

        // Keep one value only in WAL/mutable memtable before restart.
        engine.put(b"gamma", b"3").expect("put gamma");
    }

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("reopen storage engine");
        assert_eq!(engine.get(b"alpha").expect("read alpha"), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"beta").expect("read beta"), Some(b"2".to_vec()));
        assert_eq!(engine.get(b"gamma").expect("read gamma"), Some(b"3".to_vec()));

        engine.delete(b"beta").expect("delete beta");
        assert_eq!(engine.get(b"beta").expect("read deleted beta"), None);
    }

    {
        let engine = StorageEngine::open_with_options(&dir, options).expect("open after delete");
        assert_eq!(engine.get(b"alpha").expect("read alpha"), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"beta").expect("read beta"), None);
        assert_eq!(engine.get(b"gamma").expect("read gamma"), Some(b"3".to_vec()));
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn engine_exposes_metrics_for_wal_memtable_and_compaction() {
    let dir = temp_dir("metrics");
    let options = StorageEngineOptions {
        memtable_size_bytes: 1024 * 1024,
        ..StorageEngineOptions::default()
    };

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("open storage engine");

        engine.put(b"alpha", b"1").expect("put alpha");
        engine.put(b"beta", b"2").expect("put beta");
        assert_eq!(engine.get(b"alpha").expect("get alpha"), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"missing").expect("get missing"), None);
        engine.delete(b"beta").expect("delete beta");

        let mut compaction = CompactionMetrics::default();
        compaction.record_user_write(100);
        compaction.record_compaction_write(150);
        compaction.mark_compaction_complete();
        engine.set_compaction_metrics(compaction.clone());

        engine.force_flush().expect("flush metrics workload");

        let metrics = engine.metrics();
        assert_eq!(metrics.puts, 2);
        assert_eq!(metrics.deletes, 1);
        assert_eq!(metrics.gets, 2);
        assert_eq!(metrics.wal.appended_records, 3);
        assert!(metrics.wal.appended_bytes > 0);
        assert!(metrics.memtable.flushes_completed >= 1 || metrics.memtable.flushes_empty >= 1);
        assert_eq!(metrics.compaction.user_bytes_written, compaction.user_bytes_written);
        assert_eq!(
            metrics.compaction.compaction_bytes_written,
            compaction.compaction_bytes_written
        );
    }

    {
        let reopened = StorageEngine::open_with_options(&dir, options).expect("reopen engine");
        let metrics = reopened.metrics();
        assert!(metrics.wal.replayed_records >= 3);
        assert!(metrics.wal.replayed_bytes > 0);
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn engine_shutdown_is_idempotent_and_flushes_mutable_memtable() {
    let dir = temp_dir("shutdown-explicit");
    let options = StorageEngineOptions {
        memtable_size_bytes: 8 * 1024 * 1024,
        ..StorageEngineOptions::default()
    };

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("open storage engine");
        engine.put(b"alpha", b"1").expect("put alpha");
        engine.put(b"beta", b"2").expect("put beta");
        assert_eq!(engine.sstable_count(), 0);

        engine.shutdown().expect("explicit shutdown");
        engine.shutdown().expect("idempotent shutdown");

        assert!(engine.sstable_count() >= 1);
    }

    {
        let engine = StorageEngine::open_with_options(&dir, options).expect("reopen engine");
        assert_eq!(engine.get(b"alpha").expect("read alpha"), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"beta").expect("read beta"), Some(b"2".to_vec()));
        assert!(engine.sstable_count() >= 1);
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn engine_drop_runs_graceful_shutdown_and_flushes_memtable() {
    let dir = temp_dir("shutdown-drop");
    let options = StorageEngineOptions {
        memtable_size_bytes: 8 * 1024 * 1024,
        ..StorageEngineOptions::default()
    };

    {
        let engine =
            StorageEngine::open_with_options(&dir, options.clone()).expect("open storage engine");
        engine.put(b"gamma", b"3").expect("put gamma");
        engine.put(b"delta", b"4").expect("put delta");
        assert_eq!(engine.sstable_count(), 0);
    }

    {
        let engine = StorageEngine::open_with_options(&dir, options).expect("reopen engine");
        assert_eq!(engine.get(b"gamma").expect("read gamma"), Some(b"3".to_vec()));
        assert_eq!(engine.get(b"delta").expect("read delta"), Some(b"4".to_vec()));
        assert!(engine.sstable_count() >= 1);
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}
