//! How a service runs [`crate::schedule`]s: the trusted session a fired
//! schedule dispatches with, the route guard that admits only it, the
//! host-clock claim, and the schedule worker for shared stores.
//!
//! # The schedule worker
//!
//! A host that owns its aggregates' tables fires its own schedules. A shared
//! repository (SQL, in-memory) only *stores* `ctx.schedule` rows in its
//! `aggregate_schedules` table (or the in-memory equivalent); a
//! [`ScheduleWorker`] fires them:
//!
//! 1. **Claim.** Every `poll_interval`, one statement claims up to
//!    `batch_size` armed rows due by the store clock (the database clock on
//!    SQL) that no live claim holds, until `now + claim_ttl`. Concurrent
//!    workers get disjoint rows: `FOR UPDATE SKIP LOCKED` on Postgres, the
//!    single writer on SQLite.
//! 2. **Dispatch** each through a [`CommandHost`](crate::CommandHost) (the
//!    [`Service`](crate::microsvc::Service) built over the repository) as the trusted schedule
//!    principal, with [`schedule_session`] and a command id derived from the
//!    owning stream,
//!    the schedule name and its generation
//!    ([`ScheduleWorker::command_id`]). The command ledger makes that id
//!    exactly-once: a re-dispatch replays the committed receipt.
//! 3. **Settle** only the generation that fired: done (disarm) on success or
//!    a terminal rejection, or retry with exponential backoff on a retryable
//!    failure. A handler that re-armed its own name produced a newer
//!    generation, which stays armed.
//!
//! A worker that crashes between dispatch and settle leaves its claim to
//! lapse after `claim_ttl`; the next worker re-dispatches the same command
//! id, the ledger replays it, and the arm settles. Effects therefore happen
//! once per (owner, name, generation) across crashes and restarts.
//!
//! **Latency.** A due schedule fires within one `poll_interval` (default
//! 250 ms) plus dispatch time, and only while a worker runs. A host that
//! fires its own schedules on a timer needs no worker.
//!
//! **Capability.** [`ScheduleWorker::attach`] to a
//! [`SqliteRepository`](crate::SqliteRepository),
//! [`PostgresRepository`](crate::PostgresRepository) or
//! [`InMemoryRepository`](crate::InMemoryRepository) is what makes the
//! repository declare [`Capability::Scheduling`](crate::schedule::Capability::Scheduling),
//! so routes that require it can be registered. Start the returned worker
//! with the service built over that repository.
//!
//! The scheduled command must address the owning aggregate through its
//! input (the handler that called `ctx.schedule` supplies that input).

use std::time::SystemTime;

use crate::schedule::{ms_to_system_time, system_time_to_ms, ArmedSchedule};
use crate::{ROLE_KEY, USER_ID_KEY};

use super::{CausalCommandContext, Session};

/// Role carried by the trusted session of a fired schedule. Commands that
/// only schedules may run are guarded with [`schedule_guard`].
pub const SCHEDULE_ROLE: &str = "schedule";
/// User id of the trusted session of a fired schedule.
pub const SCHEDULE_USER_ID: &str = "system:schedule";
/// Session claim naming the schedule that fired. Set only by the host.
pub const SCHEDULE_NAME_CLAIM: &str = "x-distributed-schedule";
/// Session claim with the fired schedule's due time (epoch ms). Periodic
/// handlers re-schedule relative to it so ticks do not drift.
pub const SCHEDULE_DUE_CLAIM: &str = "x-distributed-schedule-due-ms";
/// Session claim with the fired schedule's generation.
pub const SCHEDULE_GENERATION_CLAIM: &str = "x-distributed-schedule-generation";
/// Session claim with the host clock's time at dispatch (epoch ms). The
/// dispatcher removes any caller value and sets it for routes that declare
/// [`Capability::Clock`](crate::schedule::Capability::Clock); read it with
/// `CausalCommandContext::now`.
pub const HOST_NOW_CLAIM: &str = "x-distributed-now-ms";

/// The trusted system session a fired schedule dispatches with.
pub fn schedule_session(schedule: &ArmedSchedule) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, SCHEDULE_USER_ID);
    session.set(ROLE_KEY, SCHEDULE_ROLE);
    session.set(SCHEDULE_NAME_CLAIM, schedule.name.clone());
    session.set(SCHEDULE_DUE_CLAIM, schedule.due_at_ms.to_string());
    session.set(SCHEDULE_GENERATION_CLAIM, schedule.generation.to_string());
    session
}

/// The host clock's time at dispatch, when the dispatcher stamped it.
pub fn host_now(session: &Session) -> Option<SystemTime> {
    session
        .get(HOST_NOW_CLAIM)?
        .parse::<i64>()
        .ok()
        .filter(|ms| *ms >= 0)
        .map(ms_to_system_time)
}

/// Overwrite (or clear) the host-clock claim of a session about to run a
/// command: callers never choose it.
pub(crate) fn stamp_host_now(session: &mut Session, now: Option<SystemTime>) {
    session.remove(HOST_NOW_CLAIM);
    if let Some(now) = now {
        let ms = system_time_to_ms(now).unwrap_or(0);
        session.set(HOST_NOW_CLAIM, ms.to_string());
    }
}

/// Whether `session` is the trusted session of a fired schedule.
pub fn is_schedule_session(session: &Session) -> bool {
    session.user_id() == Some(SCHEDULE_USER_ID)
        && session.has_role(SCHEDULE_ROLE)
        && session.get(SCHEDULE_NAME_CLAIM).is_some()
}

/// Due time of the schedule that fired this command, when it was one.
pub fn scheduled_due(session: &Session) -> Option<SystemTime> {
    if !is_schedule_session(session) {
        return None;
    }
    session
        .get(SCHEDULE_DUE_CLAIM)?
        .parse::<i64>()
        .ok()
        .filter(|ms| *ms >= 0)
        .map(ms_to_system_time)
}

/// Route guard admitting only fired schedules.
pub fn schedule_guard<A>(ctx: &CausalCommandContext<'_, A>) -> bool
where
    A: crate::Aggregate + Send + Sync + 'static,
{
    is_schedule_session(ctx.session())
}

#[cfg(feature = "runtime-tokio")]
pub use worker::{
    ScheduleWorker, ScheduleWorkerConfig, ScheduleWorkerHandle, DEFAULT_SCHEDULE_CLAIM_TTL,
    DEFAULT_SCHEDULE_POLL_INTERVAL,
};

#[cfg(feature = "runtime-tokio")]
mod worker {
    use std::sync::Arc;
    use std::time::Duration;

    use sha2::{Digest, Sha256};

    use super::{schedule_session, SCHEDULE_USER_ID};
    use crate::command_dispatch::{CommandHost, SharedCommandHost, VerifiedPrincipal};
    use crate::microsvc::CausalDispatchError;
    use crate::repository::RepositoryError;
    use crate::schedule::{
        retry_delay, system_time_to_ms, FailedSchedule, FiredSchedule, ScheduleFireReport,
        ScheduleSettlement, ScheduleStore, SCHEDULE_FIRE_BATCH, SCHEDULE_SERVICE_ID,
    };
    use crate::StreamIdentity;

    /// Default time between claim passes.
    pub const DEFAULT_SCHEDULE_POLL_INTERVAL: Duration = Duration::from_millis(250);
    /// Default lifetime of a claim before another worker may take the row over.
    pub const DEFAULT_SCHEDULE_CLAIM_TTL: Duration = Duration::from_secs(30);

    /// How a [`ScheduleWorker`] polls.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ScheduleWorkerConfig {
        /// Time between claim passes when the last pass was not full.
        pub poll_interval: Duration,
        /// How long a claim keeps other workers off a row. Must exceed the
        /// longest expected dispatch; a lapsed claim is safe (the ledger
        /// replays) but dispatches the command a second time.
        pub claim_ttl: Duration,
        /// Most schedules claimed per pass.
        pub batch_size: usize,
    }

    impl Default for ScheduleWorkerConfig {
        fn default() -> Self {
            Self {
                poll_interval: DEFAULT_SCHEDULE_POLL_INTERVAL,
                claim_ttl: DEFAULT_SCHEDULE_CLAIM_TTL,
                batch_size: SCHEDULE_FIRE_BATCH,
            }
        }
    }

    impl ScheduleWorkerConfig {
        pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
            self.poll_interval = poll_interval;
            self
        }

        pub fn with_claim_ttl(mut self, claim_ttl: Duration) -> Self {
            self.claim_ttl = claim_ttl;
            self
        }

        pub fn with_batch_size(mut self, batch_size: usize) -> Self {
            self.batch_size = batch_size.max(1);
            self
        }
    }

    /// Fires the due schedules of a shared repository through a
    /// [`CommandHost`] (usually the [`Service`](crate::microsvc::Service)
    /// built over that repository).
    /// See the [module docs](super).
    #[must_use = "a schedule worker fires nothing until it runs: call `spawn`, `run` or `run_once`"]
    #[derive(Clone)]
    pub struct ScheduleWorker {
        store: Arc<dyn ScheduleStore>,
        config: ScheduleWorkerConfig,
        clock: Option<Arc<dyn crate::Clock>>,
    }

    impl std::fmt::Debug for ScheduleWorker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ScheduleWorker")
                .field("config", &self.config)
                .finish_non_exhaustive()
        }
    }

    impl ScheduleWorker {
        /// Attach a worker to `store` (a SQL or in-memory repository): the
        /// returned repository declares
        /// [`Capability::Scheduling`](crate::schedule::Capability::Scheduling),
        /// and the returned worker fires its due schedules by the store
        /// clock once started with the service built over that repository.
        /// Several processes may each run one over one database; claims keep
        /// them off each other's rows.
        #[allow(private_bounds)]
        pub fn attach<S>(store: S, config: ScheduleWorkerConfig) -> (S, Self)
        where
            S: ScheduleStore + Clone,
        {
            let store = store.with_schedule_dispatch();
            let worker = Self {
                store: Arc::new(store.clone()),
                config,
                clock: None,
            };
            (store, worker)
        }

        pub fn config(&self) -> ScheduleWorkerConfig {
            self.config
        }

        /// Test hook: measure due times and claims by `clock` instead of the
        /// store clock, so a test can cross `claim_ttl` without sleeping.
        #[doc(hidden)]
        pub fn with_clock(mut self, clock: Arc<dyn crate::Clock>) -> Self {
            self.clock = Some(clock);
            self
        }

        /// The command-ledger id a fired arm dispatches with: stable per
        /// (owner, name, generation). One command ledger serves every
        /// aggregate of a shared store, so the owning stream is part of it.
        pub fn command_id(owner: &StreamIdentity, name: &str, generation: u64) -> String {
            let mut hasher = Sha256::new();
            hasher.update(SCHEDULE_SERVICE_ID.as_bytes());
            hasher.update([1u8]);
            hasher.update(owner.aggregate_type().as_bytes());
            hasher.update([0u8]);
            hasher.update(owner.aggregate_id().as_bytes());
            hasher.update([0u8]);
            hasher.update(name.as_bytes());
            hasher.update([0u8]);
            hasher.update(generation.to_be_bytes());
            crate::command_ledger::deterministic_command_id(hasher)
        }

        /// One pass: claim the due schedules, dispatch each through
        /// `host` as the trusted schedule principal, and settle it. Store
        /// errors abort the pass (claims lapse and are retried).
        pub async fn run_once(
            &self,
            host: &dyn CommandHost,
        ) -> Result<ScheduleFireReport, RepositoryError> {
            let now = match &self.clock {
                Some(clock) => clock.now(),
                None => self.store.schedule_now().await?,
            };
            let now_ms = system_time_to_ms(now)?;
            let ttl_ms = i64::try_from(self.config.claim_ttl.as_millis()).unwrap_or(i64::MAX);
            let claimed = self
                .store
                .claim_due_schedules(
                    now_ms,
                    now_ms.saturating_add(ttl_ms),
                    self.config.batch_size.max(1),
                )
                .await?;
            let mut report = ScheduleFireReport::default();
            for (owner, schedule) in claimed {
                let command_id = Self::command_id(&owner, &schedule.name, schedule.generation);
                let result = host
                    .invoke(
                        &schedule.command,
                        &command_id,
                        schedule.input.clone(),
                        schedule_session(&schedule),
                        VerifiedPrincipal::trusted_transport(SCHEDULE_USER_ID),
                        None,
                    )
                    .await;
                let settlement = match result {
                    Ok(dispatch) => {
                        report.fired.push(FiredSchedule {
                            name: schedule.name.clone(),
                            generation: schedule.generation,
                            command: schedule.command.clone(),
                            replayed: dispatch.replayed(),
                            payload: dispatch.payload().clone(),
                        });
                        ScheduleSettlement::Done
                    }
                    Err(error) => {
                        let failed = FailedSchedule {
                            name: schedule.name.clone(),
                            generation: schedule.generation,
                            command: schedule.command.clone(),
                            code: error.code(),
                            message: match &error {
                                CausalDispatchError::Internal(message) => message.clone(),
                                other => other.client_message(),
                            },
                        };
                        if is_retryable(&error) {
                            report.retried.push(failed);
                            let delay = i64::try_from(retry_delay(schedule.attempts).as_millis())
                                .unwrap_or(i64::MAX);
                            ScheduleSettlement::Retry {
                                due_at_ms: now_ms.saturating_add(delay),
                            }
                        } else {
                            report.failed.push(failed);
                            ScheduleSettlement::Done
                        }
                    }
                };
                self.store
                    .settle_schedule(&owner, &schedule.name, schedule.generation, &settlement)
                    .await?;
            }
            Ok(report)
        }

        /// Poll until `shutdown` resolves. Store errors are logged and
        /// retried on the next pass; a full pass is followed immediately by
        /// another.
        pub async fn run(
            self,
            host: SharedCommandHost,
            shutdown: impl std::future::Future<Output = ()>,
        ) {
            tokio::pin!(shutdown);
            let mut sleep_for = Duration::ZERO;
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(sleep_for) => {}
                }
                sleep_for = match self.run_once(host.as_ref()).await {
                    Ok(report)
                        if report.fired.len() + report.retried.len() + report.failed.len()
                            >= self.config.batch_size =>
                    {
                        Duration::ZERO
                    }
                    Ok(_) => self.config.poll_interval,
                    Err(error) => {
                        tracing::warn!(%error, "schedule worker pass failed");
                        self.config.poll_interval
                    }
                };
            }
        }

        /// Spawn [`run`](Self::run) on the current tokio runtime.
        pub fn spawn(self, host: SharedCommandHost) -> ScheduleWorkerHandle {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::spawn(self.run(host, async move {
                let _ = stopped.await;
            }));
            ScheduleWorkerHandle {
                stop: Some(stop),
                join,
            }
        }
    }

    /// A spawned [`ScheduleWorker`]. Dropping it does not stop the task; call
    /// [`stop`](Self::stop).
    #[derive(Debug)]
    pub struct ScheduleWorkerHandle {
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        join: tokio::task::JoinHandle<()>,
    }

    impl ScheduleWorkerHandle {
        /// Stop after the current pass and wait for the task.
        pub async fn stop(mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            let _ = (&mut self.join).await;
        }

        /// Kill the task at its next await point (a crash, for tests): a pass
        /// in flight is abandoned without settling.
        pub fn abort(self) {
            self.join.abort();
        }
    }

    fn is_retryable(error: &CausalDispatchError) -> bool {
        match error {
            CausalDispatchError::InProgress
            | CausalDispatchError::ConcurrentWrite
            | CausalDispatchError::Internal(_) => true,
            CausalDispatchError::Handler(error) => error.status_code() >= 500,
            CausalDispatchError::Rejected { status, .. } => *status >= 500,
            _ => false,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::microsvc::HandlerError;

        #[test]
        fn storage_and_server_failures_retry_rejections_do_not() {
            assert!(is_retryable(&CausalDispatchError::InProgress));
            assert!(is_retryable(&CausalDispatchError::Internal("x".into())));
            assert!(is_retryable(&CausalDispatchError::Handler(
                HandlerError::Repository(RepositoryError::Model("conflict".into()))
            )));
            assert!(!is_retryable(&CausalDispatchError::Handler(
                HandlerError::Rejected("no".into())
            )));
            assert!(!is_retryable(&CausalDispatchError::Forbidden));
            assert!(!is_retryable(&CausalDispatchError::CommandIdReuse));
        }

        #[test]
        fn command_ids_are_scoped_to_the_owner() {
            let a = StreamIdentity::new("todo", "a").unwrap();
            let b = StreamIdentity::new("todo", "b").unwrap();
            assert_eq!(
                ScheduleWorker::command_id(&a, "due", 1),
                ScheduleWorker::command_id(&a, "due", 1)
            );
            assert_ne!(
                ScheduleWorker::command_id(&a, "due", 1),
                ScheduleWorker::command_id(&b, "due", 1)
            );
            assert_ne!(
                ScheduleWorker::command_id(&a, "due", 1),
                ScheduleWorker::command_id(&a, "due", 2)
            );
            assert_ne!(
                ScheduleWorker::command_id(&a, "due", 1),
                crate::schedule::schedule_command_id("due", 1)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn schedule_session_is_recognized_and_carries_due() {
        let armed = ArmedSchedule {
            name: "tick".into(),
            command: "c".into(),
            input: serde_json::json!({}),
            due_at_ms: 1_234,
            generation: 7,
            attempts: 0,
        };
        let session = schedule_session(&armed);
        assert!(is_schedule_session(&session));
        assert_eq!(
            scheduled_due(&session),
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_234))
        );
        let mut forged = Session::new();
        forged.set(USER_ID_KEY, "user-1");
        forged.set(ROLE_KEY, SCHEDULE_ROLE);
        forged.set(SCHEDULE_NAME_CLAIM, "tick");
        assert!(!is_schedule_session(&forged));
        assert_eq!(scheduled_due(&forged), None);
    }

    #[test]
    fn host_now_is_overwritten_never_taken_from_the_caller() {
        let mut session = Session::new();
        session.set(HOST_NOW_CLAIM, "1");
        stamp_host_now(&mut session, None);
        assert_eq!(host_now(&session), None, "an undeclared clock is cleared");
        stamp_host_now(
            &mut session,
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_234)),
        );
        assert_eq!(
            host_now(&session),
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_234))
        );
    }
}
