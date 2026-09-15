//! High-level celld Queue drain lifecycle for aggregate-cell outboxes.

#[cfg(any(target_arch = "wasm32", test))]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use worker::Env;
#[cfg(target_arch = "wasm32")]
use worker::Storage;

#[cfg(any(target_arch = "wasm32", test))]
use super::cell::AggregateCell;
#[cfg(any(target_arch = "wasm32", test))]
use crate::bus::MessagePublisher;
use crate::bus::{CelldQueuePublisher, TransportError};
use crate::outbox_worker::OutboxDispatchOutcome;
#[cfg(any(target_arch = "wasm32", test))]
use crate::Aggregate;

/// Conventional celld Queue producer binding for aggregate outboxes.
pub const CELLD_OUTBOX_DEFAULT_BINDING: &str = "OUTBOX";
/// Optional Worker variable overriding the watchdog interval.
pub const CELLD_OUTBOX_DRAIN_INTERVAL_ENV: &str = "OUTBOX_DRAIN_INTERVAL_MS";
/// Default watchdog interval when the Worker variable is absent.
pub const CELLD_OUTBOX_DEFAULT_DRAIN_INTERVAL_MS: u64 = 5_000;
/// Default claim batch sent to celld Queue per drain pass.
pub const CELLD_OUTBOX_DEFAULT_BATCH_SIZE: usize = 100;
/// Default claim lease while a Queue send is in flight.
pub const CELLD_OUTBOX_DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// Default publish-failure ceiling before an outbox row is terminal.
pub const CELLD_OUTBOX_DEFAULT_MAX_ATTEMPTS: u32 = 10_000;

/// Result of attempting the Queue drain after an aggregate-cell commit.
///
/// Once the aggregate state, outbox rows, and watchdog alarm are durable, the
/// command is accepted. Failures after that boundary are reported in
/// [`Self::deferred`] while the alarm owns retry. They must not be turned back
/// into a command rejection: the mutation already happened, and a client retry
/// would only create an ambiguous duplicate command.
#[must_use = "inspect deferred drain diagnostics; the command may already be durably accepted"]
#[derive(Debug, Default)]
pub struct CelldOutboxDrainOutcome {
    /// Counts from the immediate drain pass, when one reached the dispatcher.
    pub dispatch: OutboxDispatchOutcome,
    /// Post-commit failures whose recovery is owned by the durable watchdog.
    pub deferred: Vec<TransportError>,
}

impl CelldOutboxDrainOutcome {
    /// Whether the durable watchdog still owns work from this drain attempt.
    pub fn is_deferred(&self) -> bool {
        !self.deferred.is_empty()
    }
}

/// celld Queue binding and drain policy attached to an [`AggregateCell`].
///
/// Domain aggregates remain transport-independent. The infrastructure-facing
/// cell host selects this binding once. Dispatch arms the watchdog before
/// invoking a command; draining publishes and deletes leased SQLite rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CelldOutbox {
    binding: String,
    drain_interval: Duration,
    lease: Duration,
    batch_size: usize,
    max_attempts: u32,
}

impl CelldOutbox {
    /// Configure an outbox binding with framework defaults.
    pub fn new(binding: impl Into<String>) -> Result<Self, TransportError> {
        let binding = binding.into();
        if binding.trim().is_empty() {
            return Err(TransportError::permanent(
                "celld outbox Queue binding must not be empty",
            ));
        }
        Ok(Self {
            binding,
            drain_interval: Duration::from_millis(CELLD_OUTBOX_DEFAULT_DRAIN_INTERVAL_MS),
            lease: CELLD_OUTBOX_DEFAULT_LEASE,
            batch_size: CELLD_OUTBOX_DEFAULT_BATCH_SIZE,
            max_attempts: CELLD_OUTBOX_DEFAULT_MAX_ATTEMPTS,
        })
    }

    /// Resolve and validate a Worker Queue binding, applying the optional
    /// `OUTBOX_DRAIN_INTERVAL_MS` watchdog override.
    pub fn from_env(env: &Env, binding: impl Into<String>) -> Result<Self, TransportError> {
        let mut outbox = Self::new(binding)?;
        // Fail during Durable Object assembly rather than on the first command.
        CelldQueuePublisher::from_env(env, &outbox.binding)?;

        if let Ok(value) = env.var(CELLD_OUTBOX_DRAIN_INTERVAL_ENV) {
            let raw = value.to_string();
            let milliseconds = raw.parse::<u64>().map_err(|_| {
                TransportError::permanent(format!(
                    "{CELLD_OUTBOX_DRAIN_INTERVAL_ENV} must be an integer number of milliseconds"
                ))
            })?;
            outbox = outbox.with_drain_interval(Duration::from_millis(milliseconds))?;
        }
        Ok(outbox)
    }

    pub fn with_drain_interval(mut self, interval: Duration) -> Result<Self, TransportError> {
        if interval < Duration::from_secs(1) {
            return Err(TransportError::permanent(
                "celld outbox drain interval must be at least 1000ms",
            ));
        }
        self.drain_interval = interval;
        Ok(self)
    }

    pub fn with_lease(mut self, lease: Duration) -> Result<Self, TransportError> {
        if lease.is_zero() {
            return Err(TransportError::permanent(
                "celld outbox claim lease must be greater than zero",
            ));
        }
        self.lease = lease;
        Ok(self)
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self, TransportError> {
        if batch_size == 0 {
            return Err(TransportError::permanent(
                "celld outbox batch size must be greater than zero",
            ));
        }
        self.batch_size = batch_size;
        Ok(self)
    }

    pub fn with_max_attempts(mut self, max_attempts: u32) -> Result<Self, TransportError> {
        if max_attempts == 0 {
            return Err(TransportError::permanent(
                "celld outbox max attempts must be greater than zero",
            ));
        }
        self.max_attempts = max_attempts;
        Ok(self)
    }

    pub fn binding(&self) -> &str {
        &self.binding
    }

    pub fn drain_interval(&self) -> Duration {
        self.drain_interval
    }

    pub fn lease(&self) -> Duration {
        self.lease
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn drain<A>(
        &self,
        cell: &AggregateCell<A>,
        env: &Env,
        storage: &Storage,
    ) -> Result<CelldOutboxDrainOutcome, TransportError>
    where
        A: Aggregate + Send + Sync + 'static,
    {
        self.drain_with(
            cell,
            || CelldQueuePublisher::from_env(env, &self.binding),
            &WorkerAlarm(storage),
        )
        .await
    }

    #[cfg(any(target_arch = "wasm32", test))]
    pub(super) async fn begin_command<W: CelldOutboxAlarm>(
        &self,
        activity: &CommandActivity,
        alarm: &W,
    ) -> Result<CommandGuard, TransportError> {
        // Count the command before yielding to setAlarm. An alarm firing while
        // this command is suspended must keep the next wake scheduled.
        let guard = activity.enter()?;
        alarm.arm(self.drain_interval).await?;
        Ok(guard)
    }

    #[cfg(any(target_arch = "wasm32", test))]
    async fn drain_with<A, P, PF, W>(
        &self,
        cell: &AggregateCell<A>,
        publisher: PF,
        alarm: &W,
    ) -> Result<CelldOutboxDrainOutcome, TransportError>
    where
        A: Aggregate + Send + Sync + 'static,
        P: MessagePublisher,
        PF: FnOnce() -> Result<P, TransportError>,
        W: CelldOutboxAlarm,
    {
        let pending = has_pending(cell).await?;
        if !pending && !cell.command_activity().is_active() {
            // Never delete an alarm: doing so can erase a concurrent command's
            // prearmed wake. The last scheduled alarm simply finds no work.
            return Ok(CelldOutboxDrainOutcome::default());
        }

        // Arm before Queue I/O, including during an alarm invocation. A crash
        // after Queue acceptance but before deletion retries the stable event ID.
        alarm.arm(self.drain_interval).await?;
        if !pending {
            return Ok(CelldOutboxDrainOutcome::default());
        }
        let publisher = match publisher() {
            Ok(publisher) => publisher,
            Err(error) => {
                return Ok(CelldOutboxDrainOutcome {
                    dispatch: OutboxDispatchOutcome::default(),
                    deferred: vec![error],
                })
            }
        };
        let dispatcher = cell.outbox_dispatcher(
            publisher,
            format!("celld-queue:{}", cell.instance_name()),
            self.lease,
            self.max_attempts,
        );
        match dispatcher.dispatch_batch(self.batch_size).await {
            Ok(dispatch) => Ok(CelldOutboxDrainOutcome {
                dispatch,
                deferred: Vec::new(),
            }),
            Err(error) => Ok(CelldOutboxDrainOutcome {
                dispatch: OutboxDispatchOutcome::default(),
                deferred: vec![error],
            }),
        }
    }
}

/// Tracks only currently executing commands, not durable data. The prearmed
/// alarm and pending SQL rows carry recovery across process loss.
#[cfg(any(target_arch = "wasm32", test))]
#[derive(Default)]
pub(super) struct CommandActivity(Arc<AtomicUsize>);

#[cfg(any(target_arch = "wasm32", test))]
impl CommandActivity {
    fn enter(&self) -> Result<CommandGuard, TransportError> {
        self.0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_add(1)
            })
            .map_err(|_| TransportError::retryable("cell command concurrency limit exceeded"))?;
        Ok(CommandGuard(Arc::clone(&self.0)))
    }
    fn is_active(&self) -> bool {
        self.0.load(Ordering::SeqCst) != 0
    }
}

#[cfg(any(target_arch = "wasm32", test))]
pub(super) struct CommandGuard(Arc<AtomicUsize>);
#[cfg(any(target_arch = "wasm32", test))]
impl Drop for CommandGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(any(target_arch = "wasm32", test))]
#[async_trait::async_trait(?Send)]
pub(super) trait CelldOutboxAlarm {
    async fn arm(&self, interval: Duration) -> Result<(), TransportError>;
}

#[cfg(target_arch = "wasm32")]
pub(super) struct WorkerAlarm<'a>(pub &'a Storage);
#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
impl CelldOutboxAlarm for WorkerAlarm<'_> {
    async fn arm(&self, interval: Duration) -> Result<(), TransportError> {
        self.0.set_alarm(interval).await.map_err(|error| {
            TransportError::retryable(format!("cannot arm celld outbox watchdog: {error}"))
        })
    }
}

#[cfg(any(target_arch = "wasm32", test))]
async fn has_pending<A>(cell: &AggregateCell<A>) -> Result<bool, TransportError>
where
    A: Aggregate + Send + Sync + 'static,
{
    use crate::outbox_worker::OutboxStore;
    use crate::OutboxMessageStatus;
    let store = cell.outbox_store();
    for status in [OutboxMessageStatus::Pending, OutboxMessageStatus::InFlight] {
        if !store
            .messages_by_status(status, 1)
            .await
            .map_err(|error| {
                TransportError::retryable(format!("cannot inspect aggregate cell outbox: {error}"))
            })?
            .is_empty()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{Entity, EventRecord};
    use crate::outbox_worker::{testing::block_on, OutboxStore};
    use crate::{OutboxMessage, OutboxMessageStatus};
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestAggregate {
        entity: Entity,
    }
    impl Aggregate for TestAggregate {
        type ReplayError = String;
        fn aggregate_type() -> &'static str {
            "celld_outbox_test"
        }
        fn entity(&self) -> &Entity {
            &self.entity
        }
        fn entity_mut(&mut self) -> &mut Entity {
            &mut self.entity
        }
        fn replay_event(&mut self, _: &EventRecord) -> Result<(), String> {
            Err("test aggregate has no replay events".into())
        }
    }
    #[derive(Clone)]
    struct RecordingPublisher {
        log: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }
    impl MessagePublisher for RecordingPublisher {
        async fn publish(&self, _: crate::bus::Message) -> Result<(), TransportError> {
            self.log.lock().unwrap().push("publish");
            if self.fail {
                Err(TransportError::retryable("Queue unavailable"))
            } else {
                Ok(())
            }
        }
    }
    struct RecordingAlarm {
        log: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }
    #[async_trait::async_trait(?Send)]
    impl CelldOutboxAlarm for RecordingAlarm {
        async fn arm(&self, _: Duration) -> Result<(), TransportError> {
            self.log.lock().unwrap().push("arm");
            if self.fail {
                Err(TransportError::retryable("alarm unavailable"))
            } else {
                Ok(())
            }
        }
    }
    fn cell(pending: bool) -> AggregateCell<TestAggregate> {
        let cell = AggregateCell::new("aggregate-1").unwrap();
        if pending {
            cell.restore_durable_outbox(vec![OutboxMessage::create(
                "evt-1",
                "test.created",
                vec![1],
            )
            .unwrap()])
                .unwrap();
        }
        cell
    }
    fn fixtures() -> (CelldOutbox, RecordingAlarm, RecordingPublisher) {
        let log = Arc::new(Mutex::new(Vec::new()));
        (
            CelldOutbox::new("OUTBOX").unwrap(),
            RecordingAlarm {
                log: log.clone(),
                fail: false,
            },
            RecordingPublisher { log, fail: false },
        )
    }

    #[test]
    fn defaults_are_safe_and_conventional() {
        let outbox = CelldOutbox::new(CELLD_OUTBOX_DEFAULT_BINDING).unwrap();
        assert_eq!(outbox.binding(), "OUTBOX");
        assert_eq!(
            outbox.drain_interval(),
            Duration::from_millis(CELLD_OUTBOX_DEFAULT_DRAIN_INTERVAL_MS)
        );
        assert_eq!(outbox.lease(), CELLD_OUTBOX_DEFAULT_LEASE);
        assert_eq!(outbox.batch_size(), CELLD_OUTBOX_DEFAULT_BATCH_SIZE);
        assert_eq!(outbox.max_attempts(), CELLD_OUTBOX_DEFAULT_MAX_ATTEMPTS);
    }
    #[test]
    fn invalid_policy_values_are_rejected() {
        assert!(CelldOutbox::new(" ").is_err());
        let base = CelldOutbox::new("OUTBOX").unwrap();
        assert!(base
            .clone()
            .with_drain_interval(Duration::from_millis(999))
            .is_err());
        assert!(base.clone().with_lease(Duration::ZERO).is_err());
        assert!(base.clone().with_batch_size(0).is_err());
        assert!(base.with_max_attempts(0).is_err());
    }
    #[test]
    fn arms_before_dispatch_and_publish_then_deletes_delivered_rows() {
        block_on(async {
            let cell = cell(true);
            let (outbox, alarm, publisher) = fixtures();
            let guard = outbox
                .begin_command(cell.command_activity(), &alarm)
                .await
                .unwrap();
            alarm.log.lock().unwrap().push("command");
            drop(guard);
            let result = outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .unwrap();
            assert_eq!(result.dispatch.published, 1);
            assert!(!result.is_deferred());
            assert!(cell.durable_outbox().unwrap().is_empty());
            assert_eq!(
                *alarm.log.lock().unwrap(),
                ["arm", "command", "arm", "publish"]
            );
            let (_, _, publisher) = fixtures();
            assert!(!outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .unwrap()
                .is_deferred());
            assert_eq!(
                alarm.log.lock().unwrap().len(),
                4,
                "idle wake neither rearms nor deletes another wake"
            );
        });
    }
    #[test]
    fn retryable_publish_keeps_pending_work_and_already_armed_wake() {
        block_on(async {
            let cell = cell(true);
            let (outbox, alarm, mut publisher) = fixtures();
            publisher.fail = true;
            let result = outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .unwrap();
            assert_eq!(result.dispatch.released, 1);
            let rows = cell.durable_outbox().unwrap();
            assert_eq!(rows[0].status, OutboxMessageStatus::Pending);
            assert_eq!(rows[0].attempts, 1);
            assert_eq!(*alarm.log.lock().unwrap(), ["arm", "publish"]);
        });
    }
    #[test]
    fn failed_prearm_does_not_admit_command_and_releases_activity_guard() {
        block_on(async {
            let cell = cell(false);
            let (outbox, mut alarm, _) = fixtures();
            alarm.fail = true;
            assert!(outbox
                .begin_command(cell.command_activity(), &alarm)
                .await
                .is_err());
            assert!(!cell.command_activity().is_active());
            assert_eq!(*alarm.log.lock().unwrap(), ["arm"]);
        });
    }
    #[test]
    fn alarm_keeps_waking_while_a_command_is_suspended_before_commit() {
        block_on(async {
            let cell = cell(false);
            let (outbox, alarm, publisher) = fixtures();
            let first = outbox
                .begin_command(cell.command_activity(), &alarm)
                .await
                .unwrap();
            assert!(!outbox
                .drain_with(&cell, || Ok(publisher.clone()), &alarm)
                .await
                .unwrap()
                .is_deferred());
            let second = outbox
                .begin_command(cell.command_activity(), &alarm)
                .await
                .unwrap();
            drop(first);
            assert!(!outbox
                .drain_with(&cell, || Ok(publisher.clone()), &alarm)
                .await
                .unwrap()
                .is_deferred());
            assert_eq!(*alarm.log.lock().unwrap(), ["arm", "arm", "arm", "arm"]);
            drop(second);
            assert!(!outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .unwrap()
                .is_deferred());
            assert_eq!(alarm.log.lock().unwrap().len(), 4);
        });
    }
    #[test]
    fn binding_failure_is_deferred_with_work_and_wake_retained() {
        block_on(async {
            let cell = cell(true);
            let (outbox, alarm, _) = fixtures();
            let result = outbox
                .drain_with(
                    &cell,
                    || Err::<RecordingPublisher, _>(TransportError::permanent("missing Queue")),
                    &alarm,
                )
                .await
                .unwrap();
            assert_eq!(result.deferred.len(), 1);
            assert_eq!(cell.durable_outbox().unwrap().len(), 1);
            assert_eq!(*alarm.log.lock().unwrap(), ["arm"]);
        });
    }
    #[test]
    fn alarm_failure_prevents_egress_without_losing_committed_rows() {
        block_on(async {
            let cell = cell(true);
            let (outbox, mut alarm, publisher) = fixtures();
            alarm.fail = true;
            assert!(outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .is_err());
            assert_eq!(cell.durable_outbox().unwrap().len(), 1);
            assert_eq!(*alarm.log.lock().unwrap(), ["arm"]);
        });
    }
    #[test]
    fn live_claim_rearms_without_publishing_until_lease_expires() {
        block_on(async {
            let cell = cell(true);
            let (outbox, alarm, publisher) = fixtures();
            cell.outbox_store()
                .claim(crate::outbox_worker::ClaimOutboxMessages::new(
                    "busy",
                    1,
                    Duration::from_secs(60),
                ))
                .await
                .unwrap();
            let result = outbox
                .drain_with(&cell, || Ok(publisher), &alarm)
                .await
                .unwrap();
            assert_eq!(result.dispatch.published, 0);
            assert_eq!(*alarm.log.lock().unwrap(), ["arm"]);
            assert_eq!(
                cell.durable_outbox().unwrap()[0].status,
                OutboxMessageStatus::InFlight
            );
        });
    }
}
