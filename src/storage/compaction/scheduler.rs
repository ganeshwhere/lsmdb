use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use crate::storage::manifest::version::VersionSet;

use super::leveled::{
    LeveledCompactionConfig, LeveledCompactionPlan, pick_compaction as pick_leveled,
};
use super::tiered::{TieredCompactionConfig, TieredCompactionPlan, pick_compaction as pick_tiered};

#[derive(Debug, Clone)]
pub enum CompactionStrategy {
    Leveled(LeveledCompactionConfig),
    Tiered(TieredCompactionConfig),
}

#[derive(Debug, Clone)]
pub enum CompactionPlan {
    Leveled(LeveledCompactionPlan),
    Tiered(TieredCompactionPlan),
}

impl CompactionPlan {
    pub fn score(&self) -> f64 {
        match self {
            CompactionPlan::Leveled(plan) => plan.score,
            CompactionPlan::Tiered(plan) => plan.score,
        }
    }

    pub fn signature(&self) -> String {
        let mut ids = match self {
            CompactionPlan::Leveled(plan) => plan.input_table_ids(),
            CompactionPlan::Tiered(plan) => plan.input_table_ids(),
        };
        ids.sort_unstable();

        let prefix = match self {
            CompactionPlan::Leveled(plan) => {
                format!("L{}->{}", plan.source_level, plan.target_level)
            }
            CompactionPlan::Tiered(plan) => format!("T{}->{}", plan.tier_id, plan.output_level),
        };

        format!("{prefix}:{ids:?}")
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactionMetrics {
    pub user_bytes_written: u64,
    pub compaction_bytes_written: u64,
    pub point_lookup_checks: u64,
    pub point_lookup_requests: u64,
    pub live_bytes: u64,
    pub total_bytes_on_disk: u64,
    pub completed_compactions: u64,
}

impl CompactionMetrics {
    pub fn record_user_write(&mut self, bytes: u64) {
        self.user_bytes_written = self.user_bytes_written.saturating_add(bytes);
    }

    pub fn record_compaction_write(&mut self, bytes: u64) {
        self.compaction_bytes_written = self.compaction_bytes_written.saturating_add(bytes);
    }

    pub fn record_point_lookup(&mut self, files_checked: u64) {
        self.point_lookup_requests = self.point_lookup_requests.saturating_add(1);
        self.point_lookup_checks = self.point_lookup_checks.saturating_add(files_checked);
    }

    pub fn set_space_bytes(&mut self, live_bytes: u64, total_bytes_on_disk: u64) {
        self.live_bytes = live_bytes;
        self.total_bytes_on_disk = total_bytes_on_disk;
    }

    pub fn mark_compaction_complete(&mut self) {
        self.completed_compactions = self.completed_compactions.saturating_add(1);
    }

    pub fn write_amplification(&self) -> Option<f64> {
        if self.user_bytes_written == 0 {
            return None;
        }
        Some(self.compaction_bytes_written as f64 / self.user_bytes_written as f64)
    }

    pub fn read_amplification(&self) -> Option<f64> {
        if self.point_lookup_requests == 0 {
            return None;
        }
        Some(self.point_lookup_checks as f64 / self.point_lookup_requests as f64)
    }

    pub fn space_amplification(&self) -> Option<f64> {
        if self.total_bytes_on_disk == 0 {
            return None;
        }
        Some(self.live_bytes as f64 / self.total_bytes_on_disk as f64)
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledCompaction {
    pub task_id: u64,
    pub priority: f64,
    pub plan: CompactionPlan,
}

#[derive(Debug)]
struct QueueItem {
    task_id: u64,
    insertion_order: u64,
    priority: f64,
    plan: CompactionPlan,
}

impl PartialEq for QueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
    }
}

impl Eq for QueueItem {}

impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .total_cmp(&other.priority)
            .then_with(|| other.insertion_order.cmp(&self.insertion_order))
    }
}

#[derive(Debug, Default)]
pub struct CompactionScheduler {
    next_task_id: u64,
    next_insertion_order: u64,
    pending: BinaryHeap<QueueItem>,
    in_flight: HashSet<u64>,
    signatures: HashSet<String>,
    pub metrics: CompactionMetrics,
}

impl CompactionScheduler {
    pub fn schedule_from_versions(
        &mut self,
        versions: &VersionSet,
        strategy: &CompactionStrategy,
    ) -> Option<u64> {
        let plan = match strategy {
            CompactionStrategy::Leveled(config) => {
                pick_leveled(versions, config).map(CompactionPlan::Leveled)
            }
            CompactionStrategy::Tiered(config) => {
                pick_tiered(versions, config).map(CompactionPlan::Tiered)
            }
        }?;

        self.enqueue(plan)
    }

    pub fn enqueue(&mut self, plan: CompactionPlan) -> Option<u64> {
        let signature = plan.signature();
        if self.signatures.contains(&signature) {
            return None;
        }

        self.next_task_id = self.next_task_id.saturating_add(1);
        self.next_insertion_order = self.next_insertion_order.saturating_add(1);

        let task_id = self.next_task_id;
        let item = QueueItem {
            task_id,
            insertion_order: self.next_insertion_order,
            priority: plan.score(),
            plan,
        };

        self.pending.push(item);
        self.signatures.insert(signature);
        Some(task_id)
    }

    pub fn pop_next(&mut self) -> Option<ScheduledCompaction> {
        let item = self.pending.pop()?;
        self.in_flight.insert(item.task_id);
        self.signatures.remove(&item.plan.signature());

        Some(ScheduledCompaction {
            task_id: item.task_id,
            priority: item.priority,
            plan: item.plan,
        })
    }

    pub fn mark_completed(&mut self, task_id: u64) {
        self.in_flight.remove(&task_id);
        self.metrics.mark_compaction_complete();
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    pub fn metrics_snapshot(&self) -> CompactionMetrics {
        self.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::compaction::leveled::{LeveledCompactionPlan, LeveledTrigger};
    use crate::storage::manifest::version::SSTableMetadata;

    use super::*;

    fn plan(score: f64, id: u64) -> CompactionPlan {
        CompactionPlan::Leveled(LeveledCompactionPlan {
            trigger: LeveledTrigger::Level0Overflow,
            source_level: 0,
            target_level: 1,
            source_inputs: vec![SSTableMetadata {
                table_id: id,
                level: 0,
                file_name: format!("sst-{id}.sst"),
                smallest_key: b"a".to_vec(),
                largest_key: b"z".to_vec(),
                file_size_bytes: 100,
            }],
            target_inputs: Vec::new(),
            score,
        })
    }

    #[test]
    fn pops_highest_priority_first() {
        let mut scheduler = CompactionScheduler::default();
        scheduler.enqueue(plan(1.0, 1));
        scheduler.enqueue(plan(5.0, 2));
        scheduler.enqueue(plan(3.0, 3));

        let first = scheduler.pop_next().expect("first task");
        let second = scheduler.pop_next().expect("second task");
        let third = scheduler.pop_next().expect("third task");

        assert!(first.priority > second.priority);
        assert!(second.priority > third.priority);
    }

    #[test]
    fn deduplicates_same_plan_signature() {
        let mut scheduler = CompactionScheduler::default();
        assert!(scheduler.enqueue(plan(1.0, 1)).is_some());
        assert!(scheduler.enqueue(plan(9.0, 1)).is_none());
        assert_eq!(scheduler.pending_count(), 1);
    }

    #[test]
    fn computes_amplification_metrics() {
        let mut metrics = CompactionMetrics::default();
        metrics.record_user_write(1000);
        metrics.record_compaction_write(2500);
        metrics.record_point_lookup(5);
        metrics.record_point_lookup(3);
        metrics.set_space_bytes(900, 1200);

        let waf = metrics.write_amplification().expect("write amplification");
        let raf = metrics.read_amplification().expect("read amplification");
        let saf = metrics.space_amplification().expect("space amplification");

        assert!((waf - 2.5).abs() < f64::EPSILON);
        assert!((raf - 4.0).abs() < f64::EPSILON);
        assert!((saf - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn exposes_metrics_snapshot() {
        let mut scheduler = CompactionScheduler::default();
        scheduler.metrics.record_user_write(42);
        scheduler.metrics.record_compaction_write(84);

        let snapshot = scheduler.metrics_snapshot();
        assert_eq!(snapshot.user_bytes_written, 42);
        assert_eq!(snapshot.compaction_bytes_written, 84);
    }
}
