use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::record::{checksum, parse_segment_id, RecordType, BLOCK_SIZE_BYTES, HEADER_LEN};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WalReplay {
    pub records: Vec<Vec<u8>>,
    pub dropped_corrupted_tails: usize,
}

#[derive(Debug, Error)]
pub enum WalReadError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone)]
pub struct WalReader {
    segments: Vec<PathBuf>,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, WalReadError> {
        let mut segments: Vec<(u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if let Some(id) = parse_segment_id(&path) {
                segments.push((id, path));
            }
        }

        segments.sort_by_key(|(id, _)| *id);

        Ok(Self { segments: segments.into_iter().map(|(_, path)| path).collect() })
    }

    pub fn segments(&self) -> &[PathBuf] {
        &self.segments
    }

    pub fn read_all(&self) -> Result<Vec<Vec<u8>>, WalReadError> {
        Ok(self.replay()?.records)
    }

    pub fn replay(&self) -> Result<WalReplay, WalReadError> {
        let mut replay = WalReplay::default();
        for segment_path in &self.segments {
            read_segment(segment_path, &mut replay)?;
        }
        Ok(replay)
    }
}

fn read_segment(path: &Path, replay: &mut WalReplay) -> Result<(), WalReadError> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(());
    }

    let mut cursor = 0;
    let mut block_offset = 0;
    let mut assembling: Option<Vec<u8>> = None;
    let mut corrupted_tail = false;

    while cursor < bytes.len() {
        let block_remaining = BLOCK_SIZE_BYTES - block_offset;

        if block_remaining < HEADER_LEN {
            let skip = block_remaining.min(bytes.len() - cursor);
            cursor += skip;
            block_offset = 0;
            continue;
        }

        if cursor + HEADER_LEN > bytes.len() {
            corrupted_tail = true;
            break;
        }

        let header = &bytes[cursor..cursor + HEADER_LEN];
        if header.iter().all(|byte| *byte == 0) {
            let skip = block_remaining.min(bytes.len() - cursor);
            cursor += skip;
            block_offset = 0;
            continue;
        }

        let expected_checksum = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let total_len = HEADER_LEN + payload_len;

        if total_len > block_remaining || cursor + total_len > bytes.len() {
            corrupted_tail = true;
            break;
        }

        let record_type = match RecordType::try_from(header[8]) {
            Ok(kind) => kind,
            Err(_) => {
                corrupted_tail = true;
                break;
            }
        };

        let payload = &bytes[cursor + HEADER_LEN..cursor + total_len];
        let found_checksum = checksum(record_type, payload);
        if found_checksum != expected_checksum {
            corrupted_tail = true;
            break;
        }

        match record_type {
            RecordType::Full => {
                if assembling.is_some() {
                    corrupted_tail = true;
                    break;
                }
                replay.records.push(payload.to_vec());
            }
            RecordType::First => {
                if assembling.is_some() {
                    corrupted_tail = true;
                    break;
                }
                assembling = Some(payload.to_vec());
            }
            RecordType::Middle => {
                let Some(acc) = assembling.as_mut() else {
                    corrupted_tail = true;
                    break;
                };
                acc.extend_from_slice(payload);
            }
            RecordType::Last => {
                let Some(mut acc) = assembling.take() else {
                    corrupted_tail = true;
                    break;
                };
                acc.extend_from_slice(payload);
                replay.records.push(acc);
            }
        }

        cursor += total_len;
        block_offset += total_len;
        if block_offset == BLOCK_SIZE_BYTES {
            block_offset = 0;
        }
    }

    if corrupted_tail || assembling.is_some() {
        replay.dropped_corrupted_tails += 1;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::wal::record::segment_file_name;

    use super::*;

    fn test_dir(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        path.push(format!("lsmdb-wal-reader-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).expect("create temp wal dir");
        path
    }

    #[test]
    fn lists_segments_in_order() {
        let dir = test_dir("ordered");
        fs::File::create(dir.join(segment_file_name(2))).expect("create segment 2");
        fs::File::create(dir.join(segment_file_name(0))).expect("create segment 0");
        fs::File::create(dir.join(segment_file_name(1))).expect("create segment 1");

        let reader = WalReader::open(&dir).expect("open reader");
        let ids: Vec<u64> =
            reader.segments().iter().filter_map(|path| parse_segment_id(path)).collect();

        assert_eq!(ids, vec![0, 1, 2]);

        fs::remove_dir_all(dir).expect("remove temp dir");
    }

    #[test]
    fn truncated_tail_is_detected() {
        let dir = test_dir("truncated");
        let mut file = fs::File::create(dir.join(segment_file_name(0))).expect("create segment");
        file.write_all(&[1, 2, 3]).expect("write truncated bytes");

        let reader = WalReader::open(&dir).expect("open reader");
        let replay = reader.replay().expect("replay should succeed");

        assert!(replay.records.is_empty());
        assert_eq!(replay.dropped_corrupted_tails, 1);

        fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
