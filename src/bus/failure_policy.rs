//! What a consumer does with a message it failed to handle.

use super::TransportError;

/// The action taken for one failed delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FailureAction {
    /// Redeliver after the run's backoff.
    Nack,
    /// Store the message as a dead letter, then acknowledge it.
    DeadLetter,
    /// Log the error and acknowledge, dropping the message.
    LogAndAck,
    /// End the run with the error and leave the delivery unsettled.
    Stop,
}

/// What to do with a *permanent* failure. A retryable failure is always
/// redelivered, until [`max_attempts`](super::RunOptions::max_attempts)
/// dead-letters it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub enum FailurePolicy {
    /// Redeliver permanent failures too.
    Retry,
    /// Store the message as a dead letter, then acknowledge it.
    #[default]
    DeadLetter,
    /// Log the error and acknowledge, dropping the message. The only policy
    /// that discards a failed message.
    LogAndAck,
    /// End the run.
    Stop,
}

impl FailurePolicy {
    /// The action for `error` on its `attempt`th delivery (1-based).
    pub fn resolve(
        self,
        error: &TransportError,
        attempt: u32,
        max_attempts: Option<u32>,
    ) -> FailureAction {
        let retry = if max_attempts.is_some_and(|max| attempt >= max) {
            FailureAction::DeadLetter
        } else {
            FailureAction::Nack
        };
        if error.is_retryable() {
            return retry;
        }
        match self {
            FailurePolicy::Retry => retry,
            FailurePolicy::DeadLetter => FailureAction::DeadLetter,
            FailurePolicy::LogAndAck => FailureAction::LogAndAck,
            FailurePolicy::Stop => FailureAction::Stop,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [FailurePolicy; 4] = [
        FailurePolicy::Retry,
        FailurePolicy::DeadLetter,
        FailurePolicy::LogAndAck,
        FailurePolicy::Stop,
    ];

    #[test]
    fn a_retryable_failure_is_redelivered_under_every_policy() {
        let transient = TransportError::retryable("transient");
        for policy in ALL {
            assert_eq!(policy.resolve(&transient, 1, None), FailureAction::Nack);
            assert_eq!(policy.resolve(&transient, 900, None), FailureAction::Nack);
        }
    }

    #[test]
    fn a_retry_that_reaches_max_attempts_is_dead_lettered() {
        let transient = TransportError::retryable("transient");
        for policy in ALL {
            assert_eq!(policy.resolve(&transient, 2, Some(3)), FailureAction::Nack);
            assert_eq!(
                policy.resolve(&transient, 3, Some(3)),
                FailureAction::DeadLetter
            );
            assert_eq!(
                policy.resolve(&transient, 4, Some(3)),
                FailureAction::DeadLetter
            );
        }
        let permanent = TransportError::permanent("terminal");
        assert_eq!(
            FailurePolicy::Retry.resolve(&permanent, 3, Some(3)),
            FailureAction::DeadLetter
        );
    }

    #[test]
    fn a_permanent_failure_follows_the_policy() {
        let permanent = TransportError::permanent("terminal");
        let action = |policy: FailurePolicy| policy.resolve(&permanent, 1, None);
        assert_eq!(action(FailurePolicy::default()), FailureAction::DeadLetter);
        assert_eq!(action(FailurePolicy::Retry), FailureAction::Nack);
        assert_eq!(action(FailurePolicy::LogAndAck), FailureAction::LogAndAck);
        assert_eq!(action(FailurePolicy::Stop), FailureAction::Stop);
    }
}
