use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::record::{
    BLOCK_SIZE_BYTES, DEFAULT_SEGMENT_SIZE_BYTES, HEADER_LEN, RecordEncodeError, RecordType,
    encode_physical, parse_segment_id, segment_file_name,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Never,
    OnCommit,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalWriterOptions {
    pub segment_size_bytes: u64,
    pub sync_mode: SyncMode,
}

impl Default for WalWriterOptions {
    fn default() -> Self {
        Self { segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES, sync_mode: SyncMode::OnCommit }
    }
}

#[derive(Debug, Error)]
pub enum WalWriteError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("record encoding error: {0}")]
    Encode(#[from] RecordEncodeError),
    #[error(
        "invalid segment size: configured={configured} but minimum supported value is {minimum}"
    )]
    InvalidSegmentSize { configured: u64, minimum: u64 },
}

#[derive(Debug)]
pub struct WalWriter {
    dir: PathBuf,
    options: WalWriterOptions,
    segment_id: u64,
    segment_path: PathBuf,
    file: BufWriter<File>,
    segment_len: u64,
    block_offset: usize,
}

impl WalWriter {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, WalWriteError> {
        Self::open_with_options(dir, WalWriterOptions::default())
    }

    pub fn open_with_options<P: AsRef<Path>>(
        dir: P,
        options: WalWriterOptions,
    ) -> Result<Self, WalWriteError> {
        let minimum_segment_size = (HEADER_LEN + 1) as u64;
        if options.segment_size_bytes < minimum_segment_size {
            return Err(WalWriteError::InvalidSegmentSize {
                configured: options.segment_size_bytes,
                minimum: minimum_segment_size,
            });
        }

        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let segment_id = next_segment_id(&dir)?;
        let (file, segment_path) = open_new_segment(&dir, segment_id)?;

        Ok(Self { dir, options, segment_id, segment_path, file, segment_len: 0, block_offset: 0 })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<(), WalWriteError> {
        let estimated_len = estimate_logical_record_len(payload.len(), self.block_offset) as u64;
        if self.segment_len > 0
            && self.segment_len.saturating_add(estimated_len) > self.options.segment_size_bytes
        {
            self.rotate_segment()?;
        }

        self.write_logical_record(payload)?;

        if matches!(self.options.sync_mode, SyncMode::Always) {
            self.sync()?;
        }

        Ok(())
    }

    pub fn append_and_commit(&mut self, payload: &[u8]) -> Result<(), WalWriteError> {
        self.append(payload)?;
        self.commit()
    }

    pub fn commit(&mut self) -> Result<(), WalWriteError> {
        match self.options.sync_mode {
            SyncMode::Never => self.file.flush()?,
            SyncMode::OnCommit | SyncMode::Always => self.sync()?,
        }
        Ok(())
    }

    pub fn current_segment_id(&self) -> u64 {
        self.segment_id
    }

    pub fn current_segment_path(&self) -> &Path {
        &self.segment_path
    }

    pub fn sync_data(&mut self) -> Result<(), WalWriteError> {
        self.sync()
    }

    fn write_logical_record(&mut self, payload: &[u8]) -> Result<(), WalWriteError> {
        if payload.is_empty() {
            self.prepare_for_empty_record()?;
            self.write_physical_record(RecordType::Full, &[])?;
            return Ok(());
        }

        let mut remaining = payload;
        let mut is_first = true;

        while !remaining.is_empty() {
            let available = self.ensure_space_for_fragment()?;
            let fragment_len = remaining.len().min(available);
            let is_last = fragment_len == remaining.len();

            let record_type = match (is_first, is_last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, false) => RecordType::Middle,
                (false, true) => RecordType::Last,
            };

            self.write_physical_record(record_type, &remaining[..fragment_len])?;
            remaining = &remaining[fragment_len..];
            is_first = false;
        }

        Ok(())
    }

    fn prepare_for_empty_record(&mut self) -> Result<(), WalWriteError> {
        let space_left = BLOCK_SIZE_BYTES - self.block_offset;
        if space_left < HEADER_LEN {
            self.write_padding(space_left)?;
        }
        Ok(())
    }

    fn ensure_space_for_fragment(&mut self) -> Result<usize, WalWriteError> {
        let mut space_left = BLOCK_SIZE_BYTES - self.block_offset;
        if space_left <= HEADER_LEN {
            self.write_padding(space_left)?;
            space_left = BLOCK_SIZE_BYTES;
        }

        Ok(space_left - HEADER_LEN)
    }

    fn write_physical_record(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> Result<(), WalWriteError> {
        let encoded = encode_physical(record_type, payload)?;
        self.file.write_all(&encoded)?;

        self.segment_len += encoded.len() as u64;
        self.block_offset += encoded.len();
        if self.block_offset == BLOCK_SIZE_BYTES {
            self.block_offset = 0;
        }

        Ok(())
    }

    fn write_padding(&mut self, count: usize) -> Result<(), WalWriteError> {
        if count == 0 {
            return Ok(());
        }

        self.file.write_all(&vec![0_u8; count])?;
        self.segment_len += count as u64;
        self.block_offset += count;
        if self.block_offset == BLOCK_SIZE_BYTES {
            self.block_offset = 0;
        }

        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), WalWriteError> {
        self.file.flush()?;

        self.segment_id += 1;
        let (file, segment_path) = open_new_segment(&self.dir, self.segment_id)?;

        self.file = file;
        self.segment_path = segment_path;
        self.segment_len = 0;
        self.block_offset = 0;

        Ok(())
    }

    fn sync(&mut self) -> Result<(), WalWriteError> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }
}

fn estimate_logical_record_len(payload_len: usize, mut block_offset: usize) -> usize {
    if payload_len == 0 {
        let space_left = BLOCK_SIZE_BYTES - block_offset;
        if space_left < HEADER_LEN {
            return space_left + HEADER_LEN;
        }
        return HEADER_LEN;
    }

    let mut remaining = payload_len;
    let mut written = 0usize;

    while remaining > 0 {
        let space_left = BLOCK_SIZE_BYTES - block_offset;
        if space_left <= HEADER_LEN {
            written += space_left;
            block_offset = 0;
            continue;
        }

        let fragment = remaining.min(space_left - HEADER_LEN);
        let encoded_len = HEADER_LEN + fragment;
        written += encoded_len;
        remaining -= fragment;

        block_offset += encoded_len;
        if block_offset == BLOCK_SIZE_BYTES {
            block_offset = 0;
        }
    }

    written
}

fn next_segment_id(dir: &Path) -> io::Result<u64> {
    let mut max_id = None;

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(id) = parse_segment_id(&path) {
            max_id = Some(max_id.map_or(id, |current: u64| current.max(id)));
        }
    }

    Ok(max_id.map_or(0, |id| id + 1))
}

fn open_new_segment(dir: &Path, segment_id: u64) -> io::Result<(BufWriter<File>, PathBuf)> {
    let segment_path = dir.join(segment_file_name(segment_id));
    let file = OpenOptions::new().create_new(true).append(true).open(&segment_path)?;
    Ok((BufWriter::new(file), segment_path))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_dir(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        path.push(format!("lsmdb-wal-writer-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).expect("create temp wal dir");
        path
    }

    #[test]
    fn rotates_segments_when_size_threshold_is_reached() {
        let dir = test_dir("rotate");
        let options = WalWriterOptions { segment_size_bytes: 80, sync_mode: SyncMode::Never };

        let mut writer = WalWriter::open_with_options(&dir, options).expect("open wal writer");
        let first_segment = writer.current_segment_id();

        writer
            .append(b"first-record-that-is-large-enough-to-trigger-rotation")
            .expect("append first");
        writer.commit().expect("commit first");
        writer.append(b"second-record").expect("append second should rotate first");

        assert!(writer.current_segment_id() > first_segment);
        drop(writer);

        fs::remove_dir_all(dir).expect("remove temp dir");
    }
}
