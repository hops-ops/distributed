//! Saga timeouts are [`Schedule`](crate::schedule::Schedule)s.
//!
//! Each saga stream owns one schedule, [`SAGA_TIMEOUT_SCHEDULE`]. Every
//! dispatch re-arms it at the awaited command's deadline (a new generation
//! replaces the previous one, so a superseded timer never fires), and a
//! terminal saga cancels it. When it fires, the host dispatches
//! [`SagaDefinition::TIMEOUT_COMMAND`] with a [`SagaTimeout`] into the saga
//! through [`SagaTimeouts`], which must be mounted on a service over the
//! saga repository that can fire schedules (a schedule worker attached, or
//! a host that fires its own).

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::{Saga, SagaDefinition, SagaError, SagaOutcome, SagaTransition};
use crate::command::{typed_command, PreparedCommand, Succeeded};
use crate::microsvc::schedule::schedule_guard;
use crate::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use crate::schedule::{Capability, ScheduleWrite};

/// Name of the one timeout schedule each saga stream owns.
pub const SAGA_TIMEOUT_SCHEDULE: &str = "saga.timeout";

/// Input of a fired saga timeout: the dispatch it times out. The saga
/// ignores it unless that dispatch is still the one it awaits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::command::CommandInput)]
pub struct SagaTimeout {
    pub saga_id: String,
    pub step: String,
    /// [`SagaPhase::as_str`](super::SagaPhase::as_str) of the dispatch.
    pub phase: String,
    pub attempt: u32,
}

#[derive(Debug, Serialize, crate::command::CommandOutput)]
struct SagaTimedOut {
    /// The timeout changed the saga (false: it had moved on).
    applied: bool,
}

/// The route a saga's timeout schedule fires: mount it on the service over
/// the repository that holds `Saga<D>` streams.
///
/// ```rust,ignore
/// let (repo, worker) = ScheduleWorker::attach(repo, ScheduleWorkerConfig::default());
/// let service = Arc::new(Service::new().routes(
///     Routes::new()
///         .with_repo(repo.clone().aggregate::<Saga<Transfer>>())
///         .mount(SagaTimeouts::<Transfer>::new()),
/// ));
/// worker.spawn(service);
/// ```
pub struct SagaTimeouts<D>(PhantomData<fn() -> D>);

impl<D> SagaTimeouts<D> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<D> Default for SagaTimeouts<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Deps, D> PortableCommand<Deps> for SagaTimeouts<D>
where
    D: SagaDefinition,
    Deps: CausalRouteDependencies<Aggregate = Saga<D>> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<Deps>) -> Routes<Deps> {
        routes
            .typed_command(typed_command::<SagaTimeout, Succeeded<SagaTimedOut>>(
                D::TIMEOUT_COMMAND,
            ))
            .requires(Capability::Scheduling)
            .requires(Capability::Clock)
            .shard_by(|timeout: &SagaTimeout| timeout.saga_id.clone())
            .guarded(schedule_guard::<Saga<D>>, time_out::<D>)
    }
}

async fn time_out<D: SagaDefinition>(
    ctx: &CausalCommandContext<'_, Saga<D>>,
    timeout: SagaTimeout,
) -> Result<PreparedCommand<Succeeded<SagaTimedOut>>, HandlerError> {
    let now = ctx.now()?;
    let mut saga = ctx
        .get(&timeout.saga_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(timeout.saga_id.clone()))?;
    let SagaTransition {
        outcome,
        messages,
        timer,
    } = saga.time_out(&timeout, now).map_err(handler_error)?;
    let applied = outcome == SagaOutcome::Applied;
    if !applied {
        return PreparedCommand::prepare(SagaTimedOut { applied })
            .map_err(|error| HandlerError::Other(Box::new(error)));
    }
    match timer {
        Some(ScheduleWrite::Arm(schedule)) => ctx.schedule(schedule)?,
        Some(ScheduleWrite::Cancel(name)) => ctx.cancel_schedule(name)?,
        None => {}
    }
    let mut messages = messages.into_iter();
    match messages.next() {
        None => ctx.commit(saga)?.succeeded(SagaTimedOut { applied }),
        Some(first) => messages
            .fold(ctx.outbox(first), |commit, message| commit.outbox(message))
            .commit(saga)?
            .succeeded(SagaTimedOut { applied }),
    }
}

fn handler_error(error: SagaError) -> HandlerError {
    match error {
        SagaError::Repository(error) => HandlerError::Repository(error),
        SagaError::NotFound { saga_id } => HandlerError::NotFound(saga_id),
        other => HandlerError::Rejected(other.to_string()),
    }
}
