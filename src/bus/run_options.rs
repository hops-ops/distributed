//! Cross-cutting execution options for the async transport runner.
//!
//! [`RunOptions`] owns the policy that wraps `Service::dispatch_message`:
//! whether consumer execution is plain idempotent dispatch or wrapped by a
//! future consumer inbox, and what to do with permanent failures. Transports
//! stay free of this policy — they only obey the rule "acknowledge after the
//! runner reports success."

use super::stable_id::{validate_stable_message_id, StableMessageIdError};
use super::{FailurePolicy, Message};

/// Placeholder shape for the future consumer inbox hook.
///
/// A consumer inbox (see `specs/consumer-inbox-design`) wraps handler execution
/// so a message receipt commits atomically with the handler's side effects,
/// giving effectively-once local database effects on top of at-least-once
/// delivery. This slice only defines the *shape* of that hook so [`RunOptions`]
/// can carry one; the storage model and execution wrapper land with the inbox
/// subtask.
///
/// Every inbox scopes receipts by a consumer identity, which is the one piece
/// of contract that is already settled, so it is the only method defined here.
/// The trait is provisional: the inbox subtask is expected to add the
/// (asynchronous) execution-wrapping method, so external implementors should
/// expect it to grow rather than treat this shape as final.
pub trait InboxHook {
    /// The stable consumer name receipts are scoped to.
    fn consumer_name(&self) -> &str;
}

/// How the runner executes consumers for a run.
///
/// Defaults to [`ConsumerDeliveryMode::Idempotent`]: the convention is
/// idempotent handlers/projections, so a redelivered message is safe.
#[derive(Clone)]
pub enum ConsumerDeliveryMode<I> {
    /// Dispatch directly and acknowledge after handler success. Handlers are
    /// expected to be idempotent under redelivery.
    Idempotent,
    /// Wrap dispatch with a consumer inbox; acknowledge only after the inbox
    /// receipt and handler side effects commit together. Requires a stable
    /// message id (see [`RunOptions::validate_message_id`]).
    Inbox(I),
}

// A `#[derive(Default)]` would add an `I: Default` bound, which would forbid
// `ConsumerDeliveryMode<NoInbox>` (NoInbox is uninhabited). The default variant
// carries no `I`, so the bound is spurious — implement it by hand instead.
#[allow(clippy::derivable_impls)]
impl<I> Default for ConsumerDeliveryMode<I> {
    fn default() -> Self {
        ConsumerDeliveryMode::Idempotent
    }
}

/// Uninhabited marker for run options that carry no inbox hook.
///
/// Because it can never be constructed, a `RunOptions<NoInbox>` is statically
/// guaranteed to be in idempotent mode — which is exactly the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoInbox {}

/// What [`run_source`](super::run_source) does when `recv` returns `Ok(None)`.
///
/// Adapters use `None` both for "queue is idle" and for "source closed".
/// Drain (the default) keeps the historical test contract: process what is
/// there and return. Wait is the long-lived consumer: park on
/// [`MessageSource::wait`](super::MessageSource::wait) and recv again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdlePolicy {
    /// Stop the run when `recv` returns `Ok(None)`.
    #[default]
    Drain,
    /// Wait for work and recv again. Cancel by dropping the `listen` future.
    Wait,
}

/// Cross-cutting execution policy for a transport run.
///
/// Generic over the inbox hook type `I`, defaulting to [`NoInbox`] for the
/// common idempotent case.
#[derive(Clone)]
pub struct RunOptions<I = NoInbox> {
    /// Whether consumer execution is plain idempotent dispatch or inbox-wrapped.
    pub delivery_mode: ConsumerDeliveryMode<I>,
    /// What the runner does with a permanent handler/transport failure.
    pub failure_policy: FailurePolicy,
    /// Whether an empty `recv` ends the run or waits for more work.
    pub idle: IdlePolicy,
}

impl<I> Default for RunOptions<I> {
    /// Idempotent delivery with the default [`FailurePolicy`].
    fn default() -> Self {
        Self {
            delivery_mode: ConsumerDeliveryMode::default(),
            failure_policy: FailurePolicy::default(),
            idle: IdlePolicy::Drain,
        }
    }
}

impl RunOptions<NoInbox> {
    /// Idempotent run options: direct dispatch, default failure policy, no
    /// stable-id requirement.
    pub fn idempotent() -> Self {
        Self::default()
    }
}

impl<I> RunOptions<I> {
    /// Inbox-wrapped run options. Acknowledgement waits for the inbox receipt
    /// and handler side effects to commit, and a stable message id becomes
    /// required.
    pub fn inbox(hook: I) -> Self {
        Self {
            delivery_mode: ConsumerDeliveryMode::Inbox(hook),
            failure_policy: FailurePolicy::default(),
            idle: IdlePolicy::Drain,
        }
    }

    /// Override the failure policy.
    pub fn with_failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Keep listening when the source is idle instead of ending the run.
    pub fn wait_when_idle(mut self) -> Self {
        self.idle = IdlePolicy::Wait;
        self
    }

    /// Whether this run dispatches directly without an inbox.
    pub fn is_idempotent(&self) -> bool {
        matches!(self.delivery_mode, ConsumerDeliveryMode::Idempotent)
    }

    /// Whether this run requires a stable message id. True in inbox mode,
    /// because deduplication needs a durable key.
    pub fn requires_stable_id(&self) -> bool {
        matches!(self.delivery_mode, ConsumerDeliveryMode::Inbox(_))
    }

    /// Validate a message's id against this run's requirements, returning the
    /// dedup key when one is required.
    ///
    /// Inbox runs enforce the [stable id rules](validate_stable_message_id) and
    /// return `Ok(Some(id))` — the borrowed id the inbox should key its receipt
    /// on, exactly as the transport delivered it. A failure here is a permanent
    /// condition for the message: redelivery cannot supply a missing or
    /// malformed id. Idempotent runs need no dedup key and return `Ok(None)`,
    /// accepting any id including none.
    pub fn validate_message_id<'m>(
        &self,
        message: &'m Message,
    ) -> Result<Option<&'m str>, StableMessageIdError> {
        if self.requires_stable_id() {
            validate_stable_message_id(message.id()).map(Some)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MessageKind;

    struct FakeInbox {
        consumer: &'static str,
    }

    impl InboxHook for FakeInbox {
        fn consumer_name(&self) -> &str {
            self.consumer
        }
    }

    fn message_with_id(id: Option<&str>) -> Message {
        let mut message = Message::new("seat.reserved", MessageKind::Event, b"{}".to_vec());
        if let Some(id) = id {
            message = message.with_id(id);
        }
        message
    }

    #[test]
    fn default_run_options_are_idempotent_with_default_policy() {
        let options = RunOptions::<NoInbox>::default();
        assert!(options.is_idempotent());
        assert!(!options.requires_stable_id());
        assert_eq!(options.failure_policy, FailurePolicy::default());
    }

    #[test]
    fn idempotent_constructor_matches_default() {
        let options = RunOptions::idempotent();
        assert!(options.is_idempotent());
        assert!(!options.requires_stable_id());
    }

    #[test]
    fn inbox_mode_requires_a_stable_id_and_keeps_the_hook() {
        let options = RunOptions::inbox(FakeInbox {
            consumer: "seat-projection",
        });
        assert!(!options.is_idempotent());
        assert!(options.requires_stable_id());
        match &options.delivery_mode {
            ConsumerDeliveryMode::Inbox(hook) => {
                assert_eq!(hook.consumer_name(), "seat-projection")
            }
            ConsumerDeliveryMode::Idempotent => panic!("expected inbox mode"),
        }
    }

    #[test]
    fn with_failure_policy_overrides_the_default() {
        let options = RunOptions::idempotent().with_failure_policy(FailurePolicy::Stop);
        assert_eq!(options.failure_policy, FailurePolicy::Stop);
    }

    #[test]
    fn idempotent_mode_needs_no_dedup_key() {
        let options = RunOptions::idempotent();
        // No key is required, so even a present id yields `Ok(None)`.
        assert_eq!(
            options.validate_message_id(&message_with_id(None)),
            Ok(None)
        );
        assert_eq!(
            options.validate_message_id(&message_with_id(Some("evt-1"))),
            Ok(None)
        );
    }

    #[test]
    fn inbox_mode_returns_the_validated_dedup_key() {
        let options = RunOptions::inbox(FakeInbox { consumer: "c" });
        assert_eq!(
            options.validate_message_id(&message_with_id(None)),
            Err(StableMessageIdError::Missing)
        );
        assert_eq!(
            options.validate_message_id(&message_with_id(Some("  "))),
            Err(StableMessageIdError::Empty)
        );
        // A valid id is returned for the inbox to key its receipt on.
        assert_eq!(
            options.validate_message_id(&message_with_id(Some("evt-1"))),
            Ok(Some("evt-1"))
        );
    }
}
