use crate::aggregate::{AggregateBuilder, AggregateRepository};
use crate::clock::{Clock, SystemClock};
use crate::repository::{GetStream, RepositoryError, TransactionalCommit};

use super::{Saga, SagaDefinition, SagaError, SagaMessage, SagaOutcome, SagaTransition};

/// Attempts before a persistent `ConcurrentWrite` is surfaced.
const CONCURRENT_WRITE_RETRIES: usize = 3;

/// Load → transition → commit driver for a [`Saga`].
///
/// Each call loads the saga, applies one transition and commits the saga's
/// new events **together with** the commands and timers it emitted, in one
/// repository transaction. A concurrent handler of the same saga loses on the
/// stream version and is retried (the reload then sees the winner's state,
/// so a duplicate message becomes a no-op).
///
/// Use an *unqueued* repository: correctness comes from optimistic stream
/// versions, and the runner does not hold `QueuedRepository` locks across a
/// no-op. Inside a `microsvc` handler with a queued repo, call
/// [`Saga::start`]/[`Saga::handle`] directly and commit with
/// `ctx.repo().outbox(..).commit(&mut saga)`.
pub struct SagaRunner<R, D: SagaDefinition, C = SystemClock> {
    repo: AggregateRepository<R, Saga<D>>,
    clock: C,
}

impl<R, D: SagaDefinition> SagaRunner<R, D, SystemClock> {
    /// A runner on the system clock.
    pub fn new(repo: R) -> Self {
        Self::with_clock(repo, SystemClock)
    }
}

impl<R, D: SagaDefinition, C: Clock> SagaRunner<R, D, C> {
    /// A runner on an explicit clock (use a `ManualClock` in tests).
    pub fn with_clock(repo: R, clock: C) -> Self {
        Self {
            repo: repo.aggregate::<Saga<D>>(),
            clock,
        }
    }

    /// The typed saga repository.
    pub fn repository(&self) -> &AggregateRepository<R, Saga<D>> {
        &self.repo
    }
}

impl<R, D, C> SagaRunner<R, D, C>
where
    R: GetStream + TransactionalCommit,
    D: SagaDefinition,
    C: Clock,
{
    /// Load a saga.
    pub async fn get(&self, saga_id: &str) -> Result<Option<Saga<D>>, SagaError> {
        Ok(self.repo.get(saga_id).await?)
    }

    /// Start saga `saga_id` (idempotent: an existing saga yields
    /// [`SagaOutcome::Duplicate`]).
    pub async fn start(&self, saga_id: &str, data: D::Data) -> Result<SagaTransition, SagaError> {
        self.retrying(saga_id, true, |saga, now| {
            saga.start(saga_id, data.clone(), now)
        })
        .await
    }

    /// Apply an inbound reply/timeout.
    pub async fn handle(&self, message: &SagaMessage) -> Result<SagaTransition, SagaError> {
        if message.saga_type != D::SAGA_TYPE {
            return Err(SagaError::Malformed(format!(
                "message for saga type `{}` delivered to `{}`",
                message.saga_type,
                D::SAGA_TYPE
            )));
        }
        self.retrying(&message.saga_id, false, |saga, now| {
            saga.handle(message, now)
        })
        .await
    }

    async fn retrying(
        &self,
        saga_id: &str,
        create: bool,
        mut transition: impl FnMut(
            &mut Saga<D>,
            std::time::SystemTime,
        ) -> Result<SagaTransition, SagaError>,
    ) -> Result<SagaTransition, SagaError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut saga = match self.repo.get(saga_id).await? {
                Some(saga) => saga,
                None if create => Saga::<D>::new(),
                None => {
                    return Err(SagaError::NotFound {
                        saga_id: saga_id.to_string(),
                    })
                }
            };
            let result = transition(&mut saga, self.clock.now())?;
            if result.outcome != SagaOutcome::Applied {
                return Ok(result);
            }
            match self.commit(&mut saga, result.messages.clone()).await {
                Ok(()) => return Ok(result),
                Err(RepositoryError::ConcurrentWrite { .. })
                    if attempt < CONCURRENT_WRITE_RETRIES =>
                {
                    continue
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn commit(
        &self,
        saga: &mut Saga<D>,
        messages: Vec<crate::outbox::OutboxMessage>,
    ) -> Result<(), RepositoryError> {
        let mut messages = messages.into_iter();
        let Some(first) = messages.next() else {
            return self.repo.commit(saga).await;
        };
        let mut commit = self.repo.outbox(first);
        for message in messages {
            commit = commit.outbox(message);
        }
        commit.commit(saga).await.map(|_| ())
    }
}
