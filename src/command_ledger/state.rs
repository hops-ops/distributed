use super::CommandLedgerError;

/// Durable command lifecycle. `Unknown` is intentionally not stored: absence
/// is represented by [`CommandLookup::Unknown`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandLedgerState {
    InProgress,
    RetryableUnknown,
    Succeeded,
    SucceededPendingProjection,
    /// Terminal for an **atomic** command (same-tx read-model row sealed).
    Atomic,
    Rejected,
    ProjectionFailed,
    Expired,
}

impl CommandLedgerState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::RetryableUnknown => "retryable_unknown",
            Self::Succeeded => "succeeded",
            Self::SucceededPendingProjection => "succeeded_pending_projection",
            Self::Atomic => "atomic",
            Self::Rejected => "rejected",
            Self::ProjectionFailed => "projection_failed",
            Self::Expired => "expired",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, CommandLedgerError> {
        match value {
            "in_progress" => Ok(Self::InProgress),
            "retryable_unknown" => Ok(Self::RetryableUnknown),
            "succeeded" => Ok(Self::Succeeded),
            "succeeded_pending_projection" => Ok(Self::SucceededPendingProjection),
            "atomic" => Ok(Self::Atomic),
            "rejected" => Ok(Self::Rejected),
            "projection_failed" => Ok(Self::ProjectionFailed),
            "expired" => Ok(Self::Expired),
            other => Err(CommandLedgerError::Corrupt(format!(
                "stored command ledger state `{other}` is invalid"
            ))),
        }
    }

    pub(super) fn is_replayable(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::SucceededPendingProjection
                | Self::Atomic
                | Self::Rejected
                | Self::ProjectionFailed
        )
    }
}

/// States the command dispatcher may commit through an attempt fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalCommandState {
    Succeeded,
    SucceededPendingProjection,
    Atomic,
    Rejected,
}

impl From<TerminalCommandState> for CommandLedgerState {
    fn from(value: TerminalCommandState) -> Self {
        match value {
            TerminalCommandState::Succeeded => Self::Succeeded,
            TerminalCommandState::SucceededPendingProjection => Self::SucceededPendingProjection,
            TerminalCommandState::Atomic => Self::Atomic,
            TerminalCommandState::Rejected => Self::Rejected,
        }
    }
}

/// A replay without modeled projection metadata has no pending projection
/// work to describe, so only settled states may carry one.
pub(super) fn validate_replay_state_without_projection_metadata(
    state: CommandLedgerState,
) -> Result<(), String> {
    match state {
        CommandLedgerState::Succeeded
        | CommandLedgerState::Atomic
        | CommandLedgerState::Rejected => Ok(()),
        CommandLedgerState::SucceededPendingProjection | CommandLedgerState::ProjectionFailed => {
            Err(format!(
                "state `{}` requires modeled projection metadata",
                state.as_str()
            ))
        }
        _ => Err(format!(
            "state `{}` cannot contain a replay payload",
            state.as_str()
        )),
    }
}
