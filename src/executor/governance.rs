use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use super::ExecutionError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    UserRequested = 1,
}

impl CancellationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            CancellationReason::UserRequested => "user requested cancellation",
        }
    }
}

#[derive(Debug, Default)]
struct CancellationState {
    reason: AtomicU8,
}

#[derive(Debug, Clone)]
pub struct StatementCancellation {
    state: Arc<CancellationState>,
}

impl Default for StatementCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl StatementCancellation {
    pub fn new() -> Self {
        Self { state: Arc::new(CancellationState::default()) }
    }

    pub fn cancel(&self) -> bool {
        self.state
            .reason
            .compare_exchange(
                0,
                CancellationReason::UserRequested as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn reason(&self) -> Option<CancellationReason> {
        match self.state.reason.load(Ordering::SeqCst) {
            0 => None,
            1 => Some(CancellationReason::UserRequested),
            other => {
                debug_assert_eq!(other, CancellationReason::UserRequested as u8);
                Some(CancellationReason::UserRequested)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatementDeadline {
    deadline: Instant,
    timeout: Duration,
}

impl StatementDeadline {
    pub fn after(timeout: Duration) -> Self {
        Self { deadline: Instant::now() + timeout, timeout }
    }

    pub fn is_elapsed(self) -> bool {
        Instant::now() >= self.deadline
    }

    pub fn timeout_ms(self) -> u64 {
        let millis = self.timeout.as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionGovernance {
    deadline: Option<StatementDeadline>,
    cancellation: Option<StatementCancellation>,
}

impl ExecutionGovernance {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.deadline = Some(StatementDeadline::after(timeout));
        self
    }

    pub fn with_deadline(mut self, deadline: StatementDeadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_cancellation(mut self, cancellation: StatementCancellation) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    pub fn checkpoint(&self) -> Result<(), ExecutionError> {
        if let Some(cancellation) = &self.cancellation {
            if let Some(reason) = cancellation.reason() {
                return Err(ExecutionError::StatementCanceled { reason: reason.as_str() });
            }
        }

        if let Some(deadline) = self.deadline {
            if deadline.is_elapsed() {
                return Err(ExecutionError::StatementTimedOut {
                    timeout_ms: deadline.timeout_ms(),
                });
            }
        }

        Ok(())
    }
}
