use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct TimestampOracle {
    current: Arc<AtomicU64>,
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new(0)
    }
}

impl TimestampOracle {
    pub fn new(initial: u64) -> Self {
        Self { current: Arc::new(AtomicU64::new(initial)) }
    }

    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    pub fn next_timestamp(&self) -> u64 {
        self.current.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }

    pub fn advance_to_at_least(&self, timestamp: u64) -> u64 {
        let mut observed = self.current();
        while observed < timestamp {
            match self.current.compare_exchange_weak(
                observed,
                timestamp,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return timestamp,
                Err(found) => observed = found,
            }
        }

        observed
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn monotonic_sequence_generation() {
        let oracle = TimestampOracle::new(5);
        assert_eq!(oracle.current(), 5);
        assert_eq!(oracle.next_timestamp(), 6);
        assert_eq!(oracle.next_timestamp(), 7);
        assert_eq!(oracle.current(), 7);
    }

    #[test]
    fn supports_concurrent_timestamp_allocation() {
        let oracle = Arc::new(TimestampOracle::default());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let oracle = Arc::clone(&oracle);
            handles.push(thread::spawn(move || {
                let mut values = Vec::new();
                for _ in 0..250 {
                    values.push(oracle.next_timestamp());
                }
                values
            }));
        }

        let mut all = BTreeSet::new();
        for handle in handles {
            for ts in handle.join().expect("worker should finish") {
                all.insert(ts);
            }
        }

        assert_eq!(all.len(), 1000);
        assert_eq!(oracle.current(), 1000);
    }

    #[test]
    fn advance_to_target_timestamp() {
        let oracle = TimestampOracle::new(10);
        assert_eq!(oracle.advance_to_at_least(8), 10);
        assert_eq!(oracle.advance_to_at_least(42), 42);
        assert_eq!(oracle.current(), 42);
    }
}
