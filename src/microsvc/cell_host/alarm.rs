//! The cell host's side of [`crate::schedule`]: due schedules are the
//! cell's alarms.
//!
//! On celld the Durable Object has a single alarm slot, owned jointly with the
//! outbox watchdog (`celld_outbox.rs`): every `setAlarm` targets the earliest
//! of the next due schedule and the watchdog's next drain. Neither replaces
//! the other. Each cell stores its schedules in its own `cell_alarms` table
//! (one cell is one stream, so no owner columns); the in-process reference
//! host keeps a [`MemorySchedules`](crate::schedule::MemorySchedules) table.

pub(crate) use crate::schedule::*;

use super::CellCommandIdentity;
use crate::microsvc::CausalDispatchError;

impl ArmedSchedule {
    /// Stable command-ledger identity of this arm in its cell's own ledger.
    pub fn command_identity(&self) -> Result<CellCommandIdentity, CausalDispatchError> {
        CellCommandIdentity::new(
            SCHEDULE_SERVICE_ID,
            SCHEDULE_SERVICE_ID,
            schedule_command_id(&self.name, self.generation),
        )
    }
}

/// Whether a dispatch failure should be retried rather than disarm the
/// schedule.
pub(crate) fn is_retryable(error: &CausalDispatchError) -> bool {
    matches!(
        error,
        CausalDispatchError::InProgress
            | CausalDispatchError::ConcurrentWrite
            | CausalDispatchError::Internal(_)
    )
}
