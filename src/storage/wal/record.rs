use std::convert::TryFrom;
use std::path::Path;

use crc32fast::Hasher;
use thiserror::Error;

pub const HEADER_LEN: usize = 9;
pub const BLOCK_SIZE_BYTES: usize = 32 * 1024;
pub const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

const SEGMENT_FILE_PREFIX: &str = "wal-";
const SEGMENT_FILE_SUFFIX: &str = ".log";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl TryFrom<u8> for RecordType {
    type Error = RecordDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Full),
            2 => Ok(Self::First),
            3 => Ok(Self::Middle),
            4 => Ok(Self::Last),
            other => Err(RecordDecodeError::InvalidRecordType(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalRecord {
    pub record_type: RecordType,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum RecordEncodeError {
    #[error("record payload length {length} exceeds u32::MAX")]
    PayloadTooLarge { length: usize },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecordDecodeError {
    #[error("record buffer too small for header: {found} bytes")]
    TruncatedHeader { found: usize },
    #[error("record payload is truncated: expected {expected} bytes, found {found}")]
    TruncatedPayload { expected: usize, found: usize },
    #[error("invalid WAL record type byte: {0}")]
    InvalidRecordType(u8),
    #[error("WAL record checksum mismatch: expected {expected}, found {found}")]
    ChecksumMismatch { expected: u32, found: u32 },
}

pub fn checksum(record_type: RecordType, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&[record_type as u8]);
    hasher.update(payload);
    hasher.finalize()
}

pub fn encode_physical(
    record_type: RecordType,
    payload: &[u8],
) -> Result<Vec<u8>, RecordEncodeError> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| RecordEncodeError::PayloadTooLarge { length: payload.len() })?;

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&checksum(record_type, payload).to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.push(record_type as u8);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_physical(buf: &[u8]) -> Result<PhysicalRecord, RecordDecodeError> {
    if buf.len() < HEADER_LEN {
        return Err(RecordDecodeError::TruncatedHeader { found: buf.len() });
    }

    let expected_checksum = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let payload_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let record_type = RecordType::try_from(buf[8])?;

    let required = HEADER_LEN + payload_len;
    if buf.len() < required {
        return Err(RecordDecodeError::TruncatedPayload {
            expected: payload_len,
            found: buf.len().saturating_sub(HEADER_LEN),
        });
    }

    let payload = &buf[HEADER_LEN..required];
    let found_checksum = checksum(record_type, payload);
    if found_checksum != expected_checksum {
        return Err(RecordDecodeError::ChecksumMismatch {
            expected: expected_checksum,
            found: found_checksum,
        });
    }

    Ok(PhysicalRecord { record_type, payload: payload.to_vec() })
}

pub fn segment_file_name(segment_id: u64) -> String {
    format!("{SEGMENT_FILE_PREFIX}{segment_id:020}{SEGMENT_FILE_SUFFIX}")
}

pub fn parse_segment_id(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    if !file_name.starts_with(SEGMENT_FILE_PREFIX) || !file_name.ends_with(SEGMENT_FILE_SUFFIX) {
        return None;
    }

    let body = &file_name[SEGMENT_FILE_PREFIX.len()..file_name.len() - SEGMENT_FILE_SUFFIX.len()];
    body.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let payload = b"hello-wal";
        let encoded = encode_physical(RecordType::Full, payload).expect("encode should succeed");
        let decoded = decode_physical(&encoded).expect("decode should succeed");

        assert_eq!(decoded.record_type, RecordType::Full);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn decode_rejects_invalid_checksum() {
        let mut encoded =
            encode_physical(RecordType::First, b"payload").expect("encode should work");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        let err = decode_physical(&encoded).expect_err("decode must fail on checksum mismatch");
        assert!(matches!(err, RecordDecodeError::ChecksumMismatch { .. }));
    }

    #[test]
    fn segment_name_parse_roundtrip() {
        let file = segment_file_name(42);
        assert_eq!(file, "wal-00000000000000000042.log");
        assert_eq!(parse_segment_id(Path::new(&file)), Some(42));
    }
}
