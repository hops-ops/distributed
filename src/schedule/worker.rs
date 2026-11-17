//! The schedule worker for shared stores (SQL and in-memory repositories).
//!
//! Aggregate cells dispatch their own schedules on their Durable Object
//! alarm. A shared repository only *stores* `ctx.schedule` rows in its
//! `aggregate_schedules` table (or the in-memory equivalent); this worker
//! fires them:
//!
//! 1. **Claim.** Every `poll_interval`, one statement claims up to
//!    `batch_size` armed rows due by the store clock (the database clock on
//!    SQL) that no live claim holds, until `now + claim_ttl`. Concurrent
//!    workers get disjoint rows: `FOR UPDATE SKIP LOCKED` on Postgres, the
//!    single writer on SQLite.
//! 2. **Dispatch** each through a [`CommandHost`] with the trusted
//!    [`schedule_session`] and a command id derived from the owning stream,
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
//! 250 ms) plus dispatch time, and only while a worker runs. On celld the
//! Durable Object alarm wakes the cell at the due time itself (typically a
//! few milliseconds late) and needs no worker process.
//!
//! **Capability.** Attaching a worker (`with_schedule_worker` on
//! [`SqliteRepository`](crate::SqliteRepository),
//! [`PostgresRepository`](crate::PostgresRepository) and
//! [`InMemoryRepository`](crate::InMemoryRepository)) is what makes the
//! repository declare [`Capability::Scheduling`](super::Capability::Scheduling),
//! so routes that require it can be registered. Start the returned worker
//! with the host of the service built over that repository.
//!
//! The scheduled command must address the owning aggregate through its
//! input (the handler that called `ctx.schedule` supplies that input).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::{
    owned_schedule_command_id, retry_delay, schedule_session, system_time_to_ms, ArmedSchedule,
    FailedSchedule, FiredSchedule, ScheduleFireReport, ScheduleSettlement, SCHEDULE_FIRE_BATCH,
    SCHEDULE_USER_ID,
};
use crate::command_dispatch::CommandHost;
use crate::graphql::identity::VerifiedPrincipal;
use crate::microsvc::CausalDispatchError;
use crate::repository::RepositoryError;
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

/// The store side of a schedule worker: its clock, the claim and the
/// settlement. Implemented by the framework's shared repositories.
#[async_trait]
pub(crate) trait ScheduleSource: Send + Sync + 'static {
    /// The store clock (the database clock on SQL).
    async fn schedule_now(&self) -> Result<SystemTime, RepositoryError>;

    /// Claim up to `limit` due, unclaimed armed schedules until
    /// `claim_until_ms`, earliest first.
    async fn claim_due_schedules(
        &self,
        now_ms: i64,
        claim_until_ms: i64,
        limit: usize,
    ) -> Result<Vec<(StreamIdentity, ArmedSchedule)>, RepositoryError>;

    /// Settle one fired arm (only that generation).
    async fn settle_schedule(
        &self,
        owner: &StreamIdentity,
        name: &str,
        generation: u64,
        settlement: &ScheduleSettlement,
    ) -> Result<(), RepositoryError>;
}

/// Fires the due schedules of a shared repository through a
/// [`CommandHost`]. See the [module docs](self).
#[must_use = "a schedule worker fires nothing until it runs: call `spawn`, `run` or `run_once`"]
#[derive(Clone)]
pub struct ScheduleWorker {
    source: Arc<dyn ScheduleSource>,
    config: ScheduleWorkerConfig,
}

impl std::fmt::Debug for ScheduleWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduleWorker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ScheduleWorker {
    pub(crate) fn new(source: impl ScheduleSource, config: ScheduleWorkerConfig) -> Self {
        Self {
            source: Arc::new(source),
            config,
        }
    }

    pub fn config(&self) -> ScheduleWorkerConfig {
        self.config
    }

    /// The command-ledger id a fired arm dispatches with: stable per
    /// (owner, name, generation).
    pub fn command_id(owner: &StreamIdentity, name: &str, generation: u64) -> String {
        owned_schedule_command_id(owner, name, generation)
    }

    /// One pass: claim the due schedules, dispatch each through `host`, and
    /// settle it. Store errors abort the pass (claims lapse and are retried).
    pub async fn run_once(
        &self,
        host: &dyn CommandHost,
    ) -> Result<ScheduleFireReport, RepositoryError> {
        let now = self.source.schedule_now().await?;
        let now_ms = system_time_to_ms(now)?;
        let ttl_ms = i64::try_from(self.config.claim_ttl.as_millis()).unwrap_or(i64::MAX);
        let claimed = self
            .source
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
                    VerifiedPrincipal::from_trusted_transport(SCHEDULE_USER_ID),
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
            self.source
                .settle_schedule(&owner, &schedule.name, schedule.generation, &settlement)
                .await?;
        }
        Ok(report)
    }

    /// Poll until `shutdown` resolves. Store errors are logged and retried on
    /// the next pass; a full pass is followed immediately by another.
    pub async fn run(
        self,
        host: Arc<dyn CommandHost>,
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
                    eprintln!("schedule worker: {error}");
                    self.config.poll_interval
                }
            };
        }
    }

    /// Spawn [`run`](Self::run) on the current tokio runtime.
    pub fn spawn(self, host: Arc<dyn CommandHost>) -> ScheduleWorkerHandle {
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

    /// Kill the task at its next await point (a crash, for tests): a pass in
    /// flight is abandoned without settling.
    pub fn abort(self) {
        self.join.abort();
    }
}

fn is_retryable(error: &CausalDispatchError) -> bool {
    match error {
        CausalDispatchError::InProgress | CausalDispatchError::Internal(_) => true,
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
