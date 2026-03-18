use crate::storage::manifest::version::{SSTableMetadata, VersionSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeveledCompactionConfig {
    pub level0_file_limit: usize,
    pub level_size_base_bytes: u64,
    pub level_size_multiplier: u64,
    pub max_levels: u32,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            level0_file_limit: 4,
            level_size_base_bytes: 64 * 1024 * 1024,
            level_size_multiplier: 10,
            max_levels: 7,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeveledTrigger {
    Level0Overflow,
    LevelSizeExceeded,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionPlan {
    pub trigger: LeveledTrigger,
    pub source_level: u32,
    pub target_level: u32,
    pub source_inputs: Vec<SSTableMetadata>,
    pub target_inputs: Vec<SSTableMetadata>,
    pub score: f64,
}

impl LeveledCompactionPlan {
    pub fn input_table_ids(&self) -> Vec<u64> {
        let mut ids = self
            .source_inputs
            .iter()
            .chain(self.target_inputs.iter())
            .map(|table| table.table_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn estimated_output_size_bytes(&self) -> u64 {
        self.source_inputs
            .iter()
            .chain(self.target_inputs.iter())
            .map(|table| table.file_size_bytes)
            .sum()
    }
}

pub fn pick_compaction(
    version_set: &VersionSet,
    config: &LeveledCompactionConfig,
) -> Option<LeveledCompactionPlan> {
    if config.max_levels < 2 {
        return None;
    }

    let mut best: Option<LeveledCompactionPlan> = None;

    let level0 = version_set.level_tables(0);
    if !level0.is_empty() {
        let target_level = 1;
        let target_tables = version_set.level_tables(target_level);
        let source = pick_smallest_overlap_source(level0, target_tables)?;
        let overlaps = overlapping_tables(source, target_tables);

        let l0_pressure = level0.len() as f64 / config.level0_file_limit.max(1) as f64;
        let overflow_bonus = if level0.len() >= config.level0_file_limit { 10.0 } else { 1.0 };
        let score = l0_pressure + ((overlaps.len() as f64) * 0.01) + overflow_bonus;

        best = Some(LeveledCompactionPlan {
            trigger: LeveledTrigger::Level0Overflow,
            source_level: 0,
            target_level,
            source_inputs: vec![source.clone()],
            target_inputs: overlaps,
            score,
        });
    }

    for level in 1..config.max_levels.saturating_sub(1) {
        let source_tables = version_set.level_tables(level);
        if source_tables.is_empty() {
            continue;
        }

        let level_size = source_tables.iter().map(|table| table.file_size_bytes).sum::<u64>();
        let target = target_size_bytes(config, level);

        if level_size <= target {
            continue;
        }

        let target_level = level + 1;
        let target_tables = version_set.level_tables(target_level);
        let source = match pick_smallest_overlap_source(source_tables, target_tables) {
            Some(source) => source,
            None => continue,
        };
        let overlaps = overlapping_tables(source, target_tables);

        let score = (level_size as f64 / target.max(1) as f64) + ((overlaps.len() as f64) * 0.01);

        let plan = LeveledCompactionPlan {
            trigger: LeveledTrigger::LevelSizeExceeded,
            source_level: level,
            target_level,
            source_inputs: vec![source.clone()],
            target_inputs: overlaps,
            score,
        };

        let replace = best.as_ref().map(|current| plan.score > current.score).unwrap_or(true);
        if replace {
            best = Some(plan);
        }
    }

    best
}

pub fn target_size_bytes(config: &LeveledCompactionConfig, level: u32) -> u64 {
    if level <= 1 {
        return config.level_size_base_bytes;
    }

    let exponent = level.saturating_sub(1);
    let mut target = config.level_size_base_bytes;
    for _ in 0..exponent {
        target = target.saturating_mul(config.level_size_multiplier.max(1));
    }
    target
}

fn pick_smallest_overlap_source<'a>(
    source_tables: &'a [SSTableMetadata],
    target_tables: &'a [SSTableMetadata],
) -> Option<&'a SSTableMetadata> {
    source_tables.iter().min_by_key(|candidate| {
        let overlap_count =
            target_tables.iter().filter(|target| ranges_overlap(candidate, target)).count();
        (overlap_count, candidate.table_id)
    })
}

fn overlapping_tables(
    source: &SSTableMetadata,
    target_tables: &[SSTableMetadata],
) -> Vec<SSTableMetadata> {
    target_tables.iter().filter(|target| ranges_overlap(source, target)).cloned().collect()
}

pub fn ranges_overlap(left: &SSTableMetadata, right: &SSTableMetadata) -> bool {
    !(left.largest_key < right.smallest_key || right.largest_key < left.smallest_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(level: u32, id: u64, start: &str, end: &str, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            table_id: id,
            level,
            file_name: format!("sst-{id:020}.sst"),
            smallest_key: start.as_bytes().to_vec(),
            largest_key: end.as_bytes().to_vec(),
            file_size_bytes: size,
        }
    }

    #[test]
    fn triggers_on_l0_file_count() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 1, "a", "f", 10));
        versions.add_table(table(0, 2, "g", "h", 10));
        versions.add_table(table(0, 3, "i", "k", 10));
        versions.add_table(table(0, 4, "l", "z", 10));

        let config = LeveledCompactionConfig::default();
        let plan = pick_compaction(&versions, &config).expect("expected compaction plan");

        assert_eq!(plan.trigger, LeveledTrigger::Level0Overflow);
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.source_inputs.len(), 1);
    }

    #[test]
    fn picks_source_with_smallest_overlap() {
        let mut versions = VersionSet::default();
        versions.add_table(table(0, 10, "a", "d", 10));
        versions.add_table(table(0, 11, "x", "z", 10));

        versions.add_table(table(1, 20, "a", "m", 100));
        versions.add_table(table(1, 21, "n", "w", 100));

        let config =
            LeveledCompactionConfig { level0_file_limit: 2, ..LeveledCompactionConfig::default() };

        let plan = pick_compaction(&versions, &config).expect("expected plan");
        assert_eq!(plan.source_inputs[0].table_id, 11);
    }

    #[test]
    fn triggers_on_level_size() {
        let mut versions = VersionSet::default();
        versions.add_table(table(1, 1, "a", "h", 80));
        versions.add_table(table(1, 2, "i", "z", 80));

        let config = LeveledCompactionConfig {
            level0_file_limit: 99,
            level_size_base_bytes: 100,
            ..LeveledCompactionConfig::default()
        };

        let plan = pick_compaction(&versions, &config).expect("level-size compaction expected");
        assert_eq!(plan.trigger, LeveledTrigger::LevelSizeExceeded);
        assert_eq!(plan.source_level, 1);
        assert_eq!(plan.target_level, 2);
    }
}
