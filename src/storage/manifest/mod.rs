pub mod version;

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::version::{SSTableMetadata, VersionSet};

pub const DEFAULT_MANIFEST_FILE_NAME: &str = "MANIFEST.log";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersionEdit {
    AddTable(SSTableMetadata),
    RemoveTable { level: u32, table_id: u64 },
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("manifest encode error: {0}")]
    Encode(String),
    #[error("manifest decode error: {0}")]
    Decode(String),
}

#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
    version_set: VersionSet,
}

impl Manifest {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, ManifestError> {
        Self::open_with_file_name(dir, DEFAULT_MANIFEST_FILE_NAME)
    }

    pub fn open_with_file_name<P: AsRef<Path>>(
        dir: P,
        file_name: &str,
    ) -> Result<Self, ManifestError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let path = dir.join(file_name);
        let version_set = replay_manifest_file(&path)?;

        let file = OpenOptions::new().create(true).append(true).read(true).open(&path)?;

        Ok(Self { path, writer: BufWriter::new(file), version_set })
    }

    pub fn apply_edit(&mut self, edit: VersionEdit) -> Result<(), ManifestError> {
        let payload =
            bincode::serialize(&edit).map_err(|err| ManifestError::Encode(err.to_string()))?;
        let len = u32::try_from(payload.len())
            .map_err(|_| ManifestError::Encode("manifest record too large".to_string()))?;

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;

        apply_edit_to_version_set(&mut self.version_set, edit);
        Ok(())
    }

    pub fn version_set(&self) -> &VersionSet {
        &self.version_set
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sync(&mut self) -> Result<(), ManifestError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}

fn replay_manifest_file(path: &Path) -> Result<VersionSet, ManifestError> {
    let mut version_set = VersionSet::default();
    if !path.exists() {
        return Ok(version_set);
    }

    let mut bytes = Vec::new();
    let mut file = File::open(path)?;
    file.read_to_end(&mut bytes)?;

    let mut cursor = 0_usize;
    while cursor + 4 <= bytes.len() {
        let mut len_bytes = [0_u8; 4];
        len_bytes.copy_from_slice(&bytes[cursor..cursor + 4]);
        let payload_len = u32::from_le_bytes(len_bytes) as usize;
        cursor += 4;

        let end = match cursor.checked_add(payload_len) {
            Some(end) => end,
            None => break,
        };

        if end > bytes.len() {
            break;
        }

        let payload = &bytes[cursor..end];
        cursor = end;

        let edit: VersionEdit =
            bincode::deserialize(payload).map_err(|err| ManifestError::Decode(err.to_string()))?;
        apply_edit_to_version_set(&mut version_set, edit);
    }

    Ok(version_set)
}

fn apply_edit_to_version_set(version_set: &mut VersionSet, edit: VersionEdit) {
    match edit {
        VersionEdit::AddTable(table) => version_set.add_table(table),
        VersionEdit::RemoveTable { level, table_id } => {
            let _ = version_set.remove_table(level, table_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_manifest_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        dir.push(format!("lsmdb-manifest-{label}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp manifest dir");
        dir
    }

    fn table(level: u32, id: u64) -> SSTableMetadata {
        SSTableMetadata {
            table_id: id,
            level,
            file_name: format!("sst-{id:020}.sst"),
            smallest_key: b"a".to_vec(),
            largest_key: b"z".to_vec(),
            file_size_bytes: 1024,
        }
    }

    #[test]
    fn manifest_replays_version_edits_on_open() {
        let dir = temp_manifest_dir("replay");

        {
            let mut manifest = Manifest::open(&dir).expect("open manifest");
            manifest.apply_edit(VersionEdit::AddTable(table(0, 1))).expect("add table 1");
            manifest.apply_edit(VersionEdit::AddTable(table(0, 2))).expect("add table 2");
            manifest
                .apply_edit(VersionEdit::RemoveTable { level: 0, table_id: 1 })
                .expect("remove table 1");
        }

        let manifest = Manifest::open(&dir).expect("reopen manifest");
        let tables = manifest.version_set().all_tables_newest_first();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_id, 2);

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn manifest_ignores_truncated_tail_record() {
        let dir = temp_manifest_dir("truncated");
        let file_name = "custom-manifest.log";

        {
            let mut manifest =
                Manifest::open_with_file_name(&dir, file_name).expect("open manifest file");
            manifest.apply_edit(VersionEdit::AddTable(table(0, 7))).expect("apply add-table edit");
        }

        let path = dir.join(file_name);
        let mut file = OpenOptions::new().append(true).open(&path).expect("open manifest append");
        file.write_all(&[0xFF, 0xFF, 0x00]).expect("append truncated tail");

        let manifest = Manifest::open_with_file_name(&dir, file_name).expect("reopen manifest");
        let tables = manifest.version_set().all_tables_newest_first();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_id, 7);

        fs::remove_dir_all(dir).expect("cleanup temp dir");
    }
}
