use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsmdb::storage::wal::{
    BLOCK_SIZE_BYTES, DEFAULT_SEGMENT_SIZE_BYTES, SyncMode, WalReader, WalWriter, WalWriterOptions,
};

fn test_dir(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    path.push(format!("lsmdb-wal-integration-{label}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[test]
fn wal_recovery_replays_records_across_writer_restart() {
    let dir = test_dir("restart");

    {
        let options = WalWriterOptions {
            segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
            sync_mode: SyncMode::OnCommit,
        };
        let mut writer = WalWriter::open_with_options(&dir, options).expect("open writer");
        writer.append(b"record-a").expect("append record-a");
        writer.commit().expect("commit record-a");
    }

    {
        let options = WalWriterOptions {
            segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
            sync_mode: SyncMode::OnCommit,
        };
        let mut writer = WalWriter::open_with_options(&dir, options).expect("reopen writer");
        writer.append(b"record-b").expect("append record-b");
        writer.commit().expect("commit record-b");
    }

    let reader = WalReader::open(&dir).expect("open reader");
    let replay = reader.replay().expect("replay should succeed");

    assert_eq!(replay.records, vec![b"record-a".to_vec(), b"record-b".to_vec()]);
    assert_eq!(replay.dropped_corrupted_tails, 0);

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn wal_recovery_reassembles_fragmented_logical_record() {
    let dir = test_dir("fragmented");
    let options = WalWriterOptions {
        segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
        sync_mode: SyncMode::Never,
    };

    let mut writer = WalWriter::open_with_options(&dir, options).expect("open writer");
    let large_payload = vec![b'x'; (BLOCK_SIZE_BYTES * 2) + 777];

    writer.append(&large_payload).expect("append large payload");
    writer.commit().expect("commit large payload");

    let reader = WalReader::open(&dir).expect("open reader");
    let replay = reader.replay().expect("replay should succeed");

    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0], large_payload);
    assert_eq!(replay.dropped_corrupted_tails, 0);

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn wal_recovery_drops_corrupted_tail_and_preserves_valid_records() {
    let dir = test_dir("corrupted-tail");
    let options = WalWriterOptions {
        segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
        sync_mode: SyncMode::OnCommit,
    };

    let mut writer = WalWriter::open_with_options(&dir, options).expect("open writer");
    writer.append(b"stable-record").expect("append stable record");
    writer.commit().expect("commit stable record");

    let segment_path = writer.current_segment_path().to_path_buf();
    drop(writer);

    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .expect("open active segment for tail corruption");
    file.write_all(&[0xAB, 0xCD, 0xEF]).expect("append corrupt tail bytes");

    let reader = WalReader::open(&dir).expect("open reader");
    let replay = reader.replay().expect("replay should succeed");

    assert_eq!(replay.records, vec![b"stable-record".to_vec()]);
    assert_eq!(replay.dropped_corrupted_tails, 1);

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}
