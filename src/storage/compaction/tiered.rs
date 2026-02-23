use std::collections::BTreeMap;

use crate::storage::manifest::version::{SSTableMetadata, VersionSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieredCompactionConfig {
    pub max_components_per_tier: usize,
    pub min_tier_size_bytes: u64,
    pub output_level: u32,
}

impl Default for TieredCompactionConfig {
    fn default() -> Self {
        Self { max_components_per_tier: 4, min_tier_size_bytes: 1, output_level: 0 }
    }
}

#[derive(Debug, Clone)]
pub struct TieredCompactionPlan {
    pub tier_id: u32,
    pub input_tables: Vec<SSTableMetadata>,
    pub output_level: u32,
    pub score: f64,
}

impl TieredCompactionPlan {
    pub fn input_table_ids(&self) -> Vec<u64> {
        let mut ids = self.input_tables.iter().map(|table| table.table_id).collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn estimated_output_size_bytes(&self) -> u64 {
        self.input_tables.iter().map(|table| table.file_size_bytes).sum()
    }
}

pub fn pick_compaction(
    version_set: &VersionSet,
    config: &TieredCompactionConfig,
) -> Option<TieredCompactionPlan> {
    let mut tiers = group_tables_into_tiers(version_set, config);
    if tiers.is_empty() {
        return None;
    }

    let mut best: Option<TieredCompactionPlan> = None;
    for (tier_id, mut tables) in tiers.drain() {
        if tables.len() < config.max_components_per_tier {
            continue;
        }

        tables.sort_by_key(|table| table.table_id);
        let score = tables.len() as f64
            + (tables.iter().map(|table| table.file_size_bytes).sum::<u64>() as f64
                / (64.0 * 1024.0 * 1024.0));

        let plan = TieredCompactionPlan {
            tier_id,
            input_tables: tables,
            output_level: config.output_level,
            score,
        };

        let replace = best.as_ref().map(|current| plan.score > current.score).unwrap_or(true);
        if replace {
            best = Some(plan);
        }
    }

    best
}

pub fn group_tables_into_tiers(
    version_set: &VersionSet,
    config: &TieredCompactionConfig,
) -> BTreeMap<u32, Vec<SSTableMetadata>> {
    let mut tiers: BTreeMap<u32, Vec<SSTableMetadata>> = BTreeMap::new();

    for table in version_set.all_tables_newest_first() {
        let tier_id = tier_id_for_size(table.file_size_bytes, config.min_tier_size_bytes);
        tiers.entry(tier_id).or_default().push(table);
    }

    tiers
}

pub fn tier_id_for_size(size_bytes: u64, min_tier_size_bytes: u64) -> u32 {
    let normalized = size_bytes.max(min_tier_size_bytes.max(1));
    63_u32.saturating_sub(normalized.leading_zeros())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(level: u32, id: u64, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            table_id: id,
            level,
            file_name: format!("sst-{id:020}.sst"),
            smallest_key: vec![b'a'],
            largest_key: vec![b'z'],
            file_size_bytes: size,
        }
    }

    #[test]
    fn groups_similar_sizes_into_same_tier() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 1, 1024));
        versions.add_table(table(1, 2, 1500));
        versions.add_table(table(2, 3, 2 * 1024 * 1024));

        let tiers = group_tables_into_tiers(&versions, &TieredCompactionConfig::default());
        let tier_sizes = tiers.values().map(Vec::len).collect::<Vec<_>>();

        assert!(tier_sizes.contains(&2));
        assert!(tier_sizes.contains(&1));
    }

    #[test]
    fn triggers_when_tier_has_enough_components() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 1, 1024));
        versions.add_table(table(0, 2, 1200));
        versions.add_table(table(0, 3, 1300));
        versions.add_table(table(0, 4, 1250));

        let config = TieredCompactionConfig::default();
        let plan = pick_compaction(&versions, &config).expect("expected tiered compaction plan");

        assert_eq!(plan.input_tables.len(), 4);
        assert_eq!(plan.output_level, 0);
    }
}
