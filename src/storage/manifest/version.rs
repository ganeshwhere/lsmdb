use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SSTableMetadata {
    pub table_id: u64,
    pub level: u32,
    pub file_name: String,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct VersionSet {
    levels: BTreeMap<u32, Vec<SSTableMetadata>>,
}

impl VersionSet {
    pub fn add_table(&mut self, table: SSTableMetadata) {
        let tables = self.levels.entry(table.level).or_default();
        tables.push(table);
        tables.sort_by_key(|entry| entry.table_id);
    }

    pub fn remove_table(&mut self, level: u32, table_id: u64) -> Option<SSTableMetadata> {
        let tables = self.levels.get_mut(&level)?;
        let index = tables.iter().position(|table| table.table_id == table_id)?;
        let removed = tables.remove(index);

        if tables.is_empty() {
            self.levels.remove(&level);
        }

        Some(removed)
    }

    pub fn level_tables(&self, level: u32) -> &[SSTableMetadata] {
        self.levels.get(&level).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn all_tables_newest_first(&self) -> Vec<SSTableMetadata> {
        let mut all =
            self.levels.values().flat_map(|tables| tables.iter().cloned()).collect::<Vec<_>>();
        all.sort_by(|left, right| right.table_id.cmp(&left.table_id));
        all
    }

    pub fn max_table_id(&self) -> Option<u64> {
        self.levels.values().flat_map(|tables| tables.iter()).map(|table| table.table_id).max()
    }

    pub fn total_table_count(&self) -> usize {
        self.levels.values().map(Vec::len).sum()
    }

    pub fn clear(&mut self) {
        self.levels.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(level: u32, id: u64) -> SSTableMetadata {
        SSTableMetadata {
            table_id: id,
            level,
            file_name: format!("sst-{id}.sst"),
            smallest_key: vec![b'a'],
            largest_key: vec![b'z'],
            file_size_bytes: 100,
        }
    }

    #[test]
    fn add_remove_tables() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 1));
        versions.add_table(table(0, 3));
        versions.add_table(table(0, 2));

        let level0 = versions.level_tables(0);
        assert_eq!(level0.len(), 3);
        assert_eq!(level0[0].table_id, 1);
        assert_eq!(level0[1].table_id, 2);
        assert_eq!(level0[2].table_id, 3);

        let removed = versions.remove_table(0, 2).expect("table should exist");
        assert_eq!(removed.table_id, 2);
        assert_eq!(versions.level_tables(0).len(), 2);
    }

    #[test]
    fn newest_first_ordering() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 11));
        versions.add_table(table(1, 7));
        versions.add_table(table(0, 15));

        let ids: Vec<u64> =
            versions.all_tables_newest_first().into_iter().map(|table| table.table_id).collect();
        assert_eq!(ids, vec![15, 11, 7]);
    }
}
