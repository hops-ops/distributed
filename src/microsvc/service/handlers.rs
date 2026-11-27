use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::aggregate::Aggregate;
use crate::bus::Message;
use crate::command::{
    Atomic, CommandOutcome, CommandOutputType, Eventual, PreparedCommand, Succeeded,
};
use crate::domain_event::DomainEvent;

use crate::microsvc::causal::{AggregatePublication, CausalWorkspace, CausalWorkspaceError};
use crate::microsvc::context::Context;
use crate::microsvc::error::HandlerError;
use crate::microsvc::session::Session;
use crate::outbox::{OutboxMessage, PreparedDomainEvent};
use crate::read_model::RelationalReadModel;

pub(super) type GuardFn<D> = dyn Fn(&Context<D>) -> bool + Send + Sync;
pub(super) type HandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send + 'a>>;
pub(super) type ProjectorBootstrapFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'a>>;
pub(super) type HandlerFn<D> =
    dyn for<'a> Fn(&'a Context<'a, D>) -> HandlerFuture<'a> + Send + Sync;

/// Lets an `async fn handle(ctx: &Context<D>) -> Result<Value, HandlerError>`
/// register directly as a handler. The higher-ranked bound ties the returned
/// future's lifetime to the borrowed [`Context`], which a plain generic future
/// parameter cannot express.
pub trait Handler<'a, D: 'a>: Send + Sync {
    /// The future returned by the handler for a context borrowed for `'a`.
    type Future: Future<Output = Result<Value, HandlerError>> + Send + 'a;
    fn call(&self, ctx: &'a Context<'a, D>) -> Self::Future;
}

/// Restricted metadata and staging context for one typed causal command.
///
/// The aggregate type is fixed by the route bundle; a bundle without an
/// aggregate ([`Routes::for_stores`](crate::microsvc::Routes::for_stores))
/// has `A = ()`, and its handlers stage outbox messages and read-model plans
/// only. The context exposes owned
/// checkouts and staging operations, but never the dependency value, backend,
/// raw repository, or a direct I/O commit method; the framework retains this
/// route's fenced durable commit capability and attaches the command-attempt
/// fence after the handler returns.
///
/// This is an API capability boundary, not a Rust sandbox. Application handler
/// code is trusted: a closure can still capture an external client/repository or
/// reach a global. Such out-of-band effects are outside the causal contract and
/// may repeat if an expired attempt is reclaimed. Only work staged through this
/// context receives the at-most-once committed-effects guarantee.
pub struct CausalCommandContext<'a, A> {
    message: &'a Message,
    session: &'a Session,
    workspace: &'a CausalWorkspace<'a, A>,
}

/// Capability-restricted aggregate repository for one causal command.
///
/// It preserves the ordinary repository authoring shape without exposing the
/// backend or an immediate I/O commit path. Every fluent commit is staged into
/// the command's fenced workspace for the dispatcher to validate and persist.
pub struct CausalRepository<'context, 'route, A> {
    context: &'context CausalCommandContext<'route, A>,
}

impl<'context, 'route, A> CausalRepository<'context, 'route, A>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Get one aggregate as an owned checkout.
    pub async fn get(
        &self,
        id: &str,
    ) -> Result<Option<crate::microsvc::AggregateCheckout<A>>, HandlerError> {
        self.context.get(id).await
    }

    /// Start a new aggregate checkout.
    pub fn create(&self) -> crate::microsvc::AggregateCheckout<A> {
        self.context.create()
    }

    /// Start an empty fluent unit of work and stage its final aggregate.
    pub fn commit(
        &self,
        checkout: crate::microsvc::AggregateCheckout<A>,
    ) -> Result<PreparedCausalCommit<'_, 'route, A, NoPublication, NoDirectProjection>, HandlerError>
    {
        self.context.commit(checkout)
    }

    /// Start a unit of work that publishes captured domain events.
    pub fn publish_events(
        &self,
    ) -> CausalCommitBuilder<'_, 'route, A, WithPublication, NoDirectProjection> {
        self.context.publish_events()
    }

    /// Start a unit of work with one explicit outward domain event.
    pub fn publish<E: DomainEvent>(
        &self,
        event: E,
    ) -> CausalCommitBuilder<'_, 'route, A, WithPublication, NoDirectProjection> {
        self.context.publish(event)
    }

    /// Stage one exact projected read-model row for same-transaction commit.
    ///
    /// Prefer materializing `row` with [`crate::mutation::Mutation::from_state`] so the
    /// handler write matches the portable mutation IR used for replay/async.
    pub fn readmodel<M>(
        &self,
        row: M,
    ) -> CausalCommitBuilder<'_, 'route, A, NoPublication, StagedProjectedRow<M>>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        self.context.readmodel(row)
    }

    /// Start a low-level integration-envelope publication unit of work.
    pub fn outbox(
        &self,
        message: OutboxMessage,
    ) -> CausalCommitBuilder<'_, 'route, A, WithPublication, NoDirectProjection> {
        self.context.outbox(message)
    }
}

/// Type-state marker for a unit of work with no publication leg.
#[doc(hidden)]
pub struct NoPublication;

/// Type-state marker proving that a unit of work declared durable publication.
#[doc(hidden)]
pub struct WithPublication;

/// Type-state marker for a unit of work with no direct projection intent.
#[doc(hidden)]
pub struct NoDirectProjection;

/// Handler-owned exact row staged for a same-transaction `Atomic<M>` result.
///
/// Built by [`CausalRepository::readmodel`] / [`CausalCommandContext::readmodel`].
/// The row should come from the same mutation program used for event→mutation
/// replay (typically [`crate::mutation::Mutation::from_state`]).
pub struct StagedProjectedRow<M>(M);

/// Fluent, handler-facing causal unit of work.
///
/// Its `commit` method performs no repository I/O. It only seals owned
/// aggregate checkouts and transaction participants into the framework-owned
/// workspace; the dispatcher later validates the ledger fence and performs the
/// sole durable commit.
pub struct CausalCommitBuilder<
    'context,
    'route,
    A,
    Publication = NoPublication,
    Projection = NoDirectProjection,
> {
    context: &'context CausalCommandContext<'route, A>,
    publish_captured_events: bool,
    explicit_events: Vec<PreparedDomainEvent>,
    external_events: Vec<crate::domain_event::DomainEventOccurrence>,
    outbox_messages: Vec<OutboxMessage>,
    error: Option<HandlerError>,
    projection: Projection,
    _publication: PhantomData<fn() -> Publication>,
}

impl<'context, 'route, A>
    CausalCommitBuilder<'context, 'route, A, NoPublication, NoDirectProjection>
{
    fn empty(context: &'context CausalCommandContext<'route, A>) -> Self {
        Self {
            context,
            publish_captured_events: false,
            explicit_events: Vec::new(),
            external_events: Vec::new(),
            outbox_messages: Vec::new(),
            error: None,
            projection: NoDirectProjection,
            _publication: PhantomData,
        }
    }
}

impl<'context, 'route, A, Publication, Projection>
    CausalCommitBuilder<'context, 'route, A, Publication, Projection>
{
    /// Stage a low-level integration envelope.
    ///
    /// Ordinary aggregate-derived publication should use
    /// [`publish_events`](Self::publish_events) or [`publish`](Self::publish).
    pub fn outbox(
        mut self,
        message: OutboxMessage,
    ) -> CausalCommitBuilder<'context, 'route, A, WithPublication, Projection> {
        self.outbox_messages.push(message);
        CausalCommitBuilder {
            context: self.context,
            publish_captured_events: self.publish_captured_events,
            explicit_events: self.explicit_events,
            external_events: self.external_events,
            outbox_messages: self.outbox_messages,
            error: self.error,
            projection: self.projection,
            _publication: PhantomData,
        }
    }

    /// Publish one typed fact from an external source: a provider webhook, a
    /// reconciliation, anything that is not an aggregate transition.
    ///
    /// The fact becomes a [`DomainEventOccurrence`](crate::domain_event::DomainEventOccurrence)
    /// whose identity is `source` alone, so the same source position published
    /// twice is one outbox message id (the second commit fails with
    /// [`RepositoryError::DuplicateOutboxMessage`](crate::RepositoryError::DuplicateOutboxMessage)).
    /// It commits with the command's ledger receipt and carries its causation,
    /// so a `projection!` over `E` projects it like an aggregate's event.
    pub fn publish_external<E: DomainEvent>(
        mut self,
        source: crate::domain_event::ExternalEventSource,
        event: E,
    ) -> CausalCommitBuilder<'context, 'route, A, WithPublication, Projection> {
        if self.error.is_none() {
            match self.context.capture_external(source, &event) {
                Ok(occurrence) => self.external_events.push(occurrence),
                Err(error) => self.error = Some(HandlerError::Other(Box::new(error))),
            }
        }
        CausalCommitBuilder {
            context: self.context,
            publish_captured_events: self.publish_captured_events,
            explicit_events: self.explicit_events,
            external_events: self.external_events,
            outbox_messages: self.outbox_messages,
            error: self.error,
            projection: self.projection,
            _publication: PhantomData,
        }
    }

    /// Move the staged legs into the workspace. Performs no repository I/O.
    fn seal(
        mut self,
    ) -> Result<PreparedCausalCommit<'context, 'route, A, Publication, Projection>, HandlerError>
    {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        for occurrence in self.external_events {
            self.context
                .workspace
                .stage_external_event(occurrence)
                .map_err(workspace_handler_error)?;
        }
        for message in self.outbox_messages {
            self.context
                .workspace
                .stage_outbox(message)
                .map_err(workspace_handler_error)?;
        }
        Ok(PreparedCausalCommit {
            context: self.context,
            projection: self.projection,
            _publication: PhantomData,
        })
    }
}

impl<'context, 'route, Publication>
    CausalCommitBuilder<'context, 'route, (), Publication, NoDirectProjection>
{
    /// Seal this unit of work of a route bundle without an aggregate: its
    /// outbox messages and external facts commit with the command's ledger
    /// receipt in one transaction, and no stream is written.
    ///
    /// This method is synchronous and performs no repository I/O.
    pub fn commit(
        self,
    ) -> Result<
        PreparedCausalCommit<'context, 'route, (), Publication, NoDirectProjection>,
        HandlerError,
    > {
        self.seal()
    }
}

impl<'context, 'route, A, Publication, Projection>
    CausalCommitBuilder<'context, 'route, A, Publication, Projection>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Publish captured domain-event occurrences for every aggregate staged by
    /// this builder.
    pub fn publish_events(
        self,
    ) -> CausalCommitBuilder<'context, 'route, A, WithPublication, Projection> {
        CausalCommitBuilder {
            context: self.context,
            publish_captured_events: true,
            explicit_events: self.explicit_events,
            external_events: self.external_events,
            outbox_messages: self.outbox_messages,
            error: self.error,
            projection: self.projection,
            _publication: PhantomData,
        }
    }

    /// Publish an explicit typed outward event from its own public DTO.
    ///
    /// The event is bound to the next aggregate passed to
    /// [`aggregate`](Self::aggregate) or [`commit`](Self::commit).
    pub fn publish<E: DomainEvent>(
        mut self,
        event: E,
    ) -> CausalCommitBuilder<'context, 'route, A, WithPublication, Projection> {
        if self.error.is_none() {
            match PreparedDomainEvent::new(event) {
                Ok(event) => self.explicit_events.push(event),
                Err(error) => {
                    self.error = Some(HandlerError::Other(Box::new(error)));
                }
            }
        }
        CausalCommitBuilder {
            context: self.context,
            publish_captured_events: self.publish_captured_events,
            explicit_events: self.explicit_events,
            external_events: self.external_events,
            outbox_messages: self.outbox_messages,
            error: self.error,
            projection: self.projection,
            _publication: PhantomData,
        }
    }

    /// Stage one exact projected read-model row for same-transaction commit.
    ///
    /// Unlocks `PreparedCausalCommit::projected` without a placement-selected
    /// service registry entry. Prefer rows from [`crate::mutation::Mutation::from_state`].
    pub fn readmodel<M>(
        self,
        row: M,
    ) -> CausalCommitBuilder<'context, 'route, A, Publication, StagedProjectedRow<M>>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        CausalCommitBuilder {
            context: self.context,
            publish_captured_events: self.publish_captured_events,
            explicit_events: self.explicit_events,
            external_events: self.external_events,
            outbox_messages: self.outbox_messages,
            error: self.error,
            projection: StagedProjectedRow(row),
            _publication: PhantomData,
        }
    }

    /// Stage another aggregate while keeping this builder open.
    ///
    /// Explicit events currently waiting on the builder bind to this aggregate;
    /// `publish_events` remains active for every later aggregate.
    pub fn aggregate(
        mut self,
        checkout: crate::microsvc::AggregateCheckout<A>,
    ) -> Result<Self, HandlerError> {
        self.stage_aggregate(checkout)?;
        Ok(self)
    }

    /// Seal this unit of work by staging its final aggregate.
    ///
    /// This method is synchronous and performs no repository I/O.
    pub fn commit(
        mut self,
        checkout: crate::microsvc::AggregateCheckout<A>,
    ) -> Result<PreparedCausalCommit<'context, 'route, A, Publication, Projection>, HandlerError>
    {
        self.stage_aggregate(checkout)?;
        self.seal()
    }

    fn stage_aggregate(
        &mut self,
        checkout: crate::microsvc::AggregateCheckout<A>,
    ) -> Result<(), HandlerError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.context
            .workspace
            .stage_with_publication(
                checkout,
                AggregatePublication {
                    publish_captured_events: self.publish_captured_events,
                    explicit_events: std::mem::take(&mut self.explicit_events),
                },
            )
            .map_err(workspace_handler_error)
    }
}

/// A causal unit of work sealed by a handler but not yet durably committed.
pub struct PreparedCausalCommit<'context, 'route, A, Publication, Projection> {
    context: &'context CausalCommandContext<'route, A>,
    projection: Projection,
    _publication: PhantomData<fn() -> Publication>,
}

impl<A, Publication, Projection> PreparedCausalCommit<'_, '_, A, Publication, Projection> {
    /// Prepare a successful command result with no causal visibility promise.
    pub fn succeeded<T>(self, payload: T) -> Result<PreparedCommand<Succeeded<T>>, HandlerError>
    where
        T: CommandOutputType + Serialize + Send + Sync + 'static,
    {
        let _ = self.projection;
        PreparedCommand::prepare(payload).map_err(|error| HandlerError::Other(Box::new(error)))
    }
}

impl<A, Projection> PreparedCausalCommit<'_, '_, A, WithPublication, Projection> {
    /// Prepare a causal result. This terminal exists only after a publication
    /// leg; the dispatcher additionally proves actual durable outbox coverage.
    pub fn eventual<T>(self, payload: T) -> Result<PreparedCommand<Eventual<T>>, HandlerError>
    where
        T: CommandOutputType + Serialize + Send + Sync + 'static,
    {
        let _ = self.projection;
        PreparedCommand::prepare(payload).map_err(|error| HandlerError::Other(Box::new(error)))
    }
}

impl<A, Publication, M> PreparedCausalCommit<'_, '_, A, Publication, StagedProjectedRow<M>>
where
    A: Aggregate + Send + Sync + 'static,
    M: RelationalReadModel + Serialize + Send + Sync + 'static,
{
    /// Return the handler-staged exact row as a same-transaction projected result.
    ///
    /// The dispatcher proves one complete-row upsert matching `M` and commits it
    /// with the aggregate. No service placement registry is consulted.
    pub fn atomic(self) -> Result<PreparedCommand<Atomic<M>>, HandlerError> {
        self.context
            .workspace
            .prepare_atomic(self.projection.0)
            .map_err(workspace_handler_error)
    }
}

impl<A, Publication> PreparedCausalCommit<'_, '_, A, Publication, NoDirectProjection>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Return the row a placement-selected direct projection writes: the
    /// service registration chose the executor for `M`, the handler stages
    /// nothing.
    pub fn atomic<M>(self) -> Result<PreparedCommand<Atomic<M>>, HandlerError>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        self.context
            .workspace
            .prepare_placement_selected_atomic()
            .map_err(workspace_handler_error)
    }
}

impl<'a, A> CausalCommandContext<'a, A> {
    pub(super) fn new(
        message: &'a Message,
        session: &'a Session,
        workspace: &'a CausalWorkspace<'a, A>,
    ) -> Self {
        Self {
            message,
            session,
            workspace,
        }
    }

    pub fn command_name(&self) -> &str {
        self.message.name()
    }

    pub fn message_id(&self) -> Option<&str> {
        self.message.id()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.message.correlation_id()
    }

    pub fn causation_id(&self) -> Option<&str> {
        self.message.causation_id()
    }

    pub fn trace_context(&self) -> crate::trace_context::TraceContext {
        self.message.trace_context()
    }

    /// Session for this command attempt (transport/gateway claims).
    pub fn session(&self) -> &Session {
        self.session
    }

    pub fn user_id(&self) -> Result<&str, HandlerError> {
        self.session
            .user_id()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| HandlerError::Unauthorized("missing user ID in session".into()))
    }

    pub fn role(&self) -> Option<&str> {
        self.session.role()
    }

    pub fn claim(&self, name: &str) -> Option<&str> {
        self.session.get(name)
    }

    /// Start a low-level integration-envelope publication unit of work.
    pub fn outbox(
        &self,
        message: OutboxMessage,
    ) -> CausalCommitBuilder<'_, 'a, A, WithPublication, NoDirectProjection> {
        CausalCommitBuilder::empty(self).outbox(message)
    }

    /// Start a unit of work that publishes one typed fact from an external
    /// source. See [`CausalCommitBuilder::publish_external`].
    pub fn publish_external<E: DomainEvent>(
        &self,
        source: crate::domain_event::ExternalEventSource,
        event: E,
    ) -> CausalCommitBuilder<'_, 'a, A, WithPublication, NoDirectProjection> {
        CausalCommitBuilder::empty(self).publish_external(source, event)
    }

    /// Capture `event` from `source` with this command's correlation and
    /// trace context. Causation is the ledger's, applied when it commits.
    fn capture_external<E: DomainEvent>(
        &self,
        source: crate::domain_event::ExternalEventSource,
        event: &E,
    ) -> Result<
        crate::domain_event::DomainEventOccurrence,
        crate::domain_event::DomainEventCaptureError,
    > {
        let mut metadata = std::collections::HashMap::new();
        self.trace_context().inject_map(&mut metadata);
        if let Some(correlation_id) = self.correlation_id() {
            metadata.insert(
                crate::trace_context::CORRELATION_ID.to_string(),
                correlation_id.to_string(),
            );
        }
        crate::domain_event::DomainEventOccurrence::capture_external(
            source,
            crate::time::now(),
            metadata.into_iter().collect(),
            event,
        )
    }

    /// The host clock when this command was dispatched: the Durable Object
    /// clock on celld, the store clock on the reference host and the
    /// in-memory repository (e.g. a `ManualClock`), the database clock on
    /// SQL. Never a caller's time. Requires the route to declare
    /// `.requires(Capability::Clock)`; otherwise an error says so.
    pub fn now(&self) -> Result<std::time::SystemTime, HandlerError> {
        crate::microsvc::schedule::host_now(self.session).ok_or_else(|| {
            HandlerError::Rejected(
                "ctx.now() needs a route declared with .requires(Capability::Clock)".into(),
            )
        })
    }

    /// Due time of the schedule that fired this command; `None` for any
    /// command not dispatched by a schedule.
    pub fn scheduled_due(&self) -> Option<std::time::SystemTime> {
        crate::microsvc::schedule::scheduled_due(self.session)
    }
}

impl<'a, A> CausalCommandContext<'a, A>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Return the capability-restricted repository facade for this causal
    /// command.
    ///
    /// This deliberately returns no backend or immediate commit capability.
    /// It exists so handlers retain the familiar repository → get/create →
    /// mutate → fluent commit shape while the dispatcher still owns I/O.
    pub fn repo(&self) -> CausalRepository<'_, 'a, A> {
        CausalRepository { context: self }
    }

    /// Get one aggregate as an owned checkout without retaining a queue lock.
    pub async fn get(
        &self,
        id: &str,
    ) -> Result<Option<crate::microsvc::AggregateCheckout<A>>, HandlerError> {
        self.load(id).await
    }

    /// Load one aggregate as an owned checkout without retaining a queue lock.
    pub async fn load(
        &self,
        id: &str,
    ) -> Result<Option<crate::microsvc::AggregateCheckout<A>>, HandlerError> {
        self.workspace
            .load(id)
            .await
            .map_err(workspace_handler_error)
    }

    /// Start a new aggregate checkout. The handler must assign a valid entity
    /// identity before staging it.
    pub fn create(&self) -> crate::microsvc::AggregateCheckout<A> {
        self.workspace.create()
    }

    /// Start an empty fluent unit of work and stage its final aggregate.
    pub fn commit(
        &self,
        checkout: crate::microsvc::AggregateCheckout<A>,
    ) -> Result<PreparedCausalCommit<'_, 'a, A, NoPublication, NoDirectProjection>, HandlerError>
    {
        CausalCommitBuilder::empty(self).commit(checkout)
    }

    /// Start a unit of work that publishes captured domain-event occurrences.
    pub fn publish_events(
        &self,
    ) -> CausalCommitBuilder<'_, 'a, A, WithPublication, NoDirectProjection> {
        CausalCommitBuilder::empty(self).publish_events()
    }

    /// Start a unit of work with one explicit typed outward event.
    pub fn publish<E: DomainEvent>(
        &self,
        event: E,
    ) -> CausalCommitBuilder<'_, 'a, A, WithPublication, NoDirectProjection> {
        CausalCommitBuilder::empty(self).publish(event)
    }

    /// Stage one exact projected read-model row for same-transaction commit.
    ///
    /// Prefer rows from [`crate::mutation::Mutation::from_state`] so the write matches the
    /// portable mutation used for event bindings and client cache application.
    pub fn readmodel<M>(
        &self,
        row: M,
    ) -> CausalCommitBuilder<'_, 'a, A, NoPublication, StagedProjectedRow<M>>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        CausalCommitBuilder::empty(self).readmodel(row)
    }

    /// Schedule (or re-schedule) work for the aggregate this command
    /// commits ([`crate::schedule`]).
    ///
    /// The schedule row commits in the same transaction as the command's
    /// events and receipt, on every backend; if the command fails, nothing is
    /// scheduled. The command must commit exactly one aggregate, which owns
    /// it. At the due time a host with [`Capability::Scheduling`] dispatches
    /// its command into that aggregate with the trusted
    /// [`schedule_session`](crate::microsvc::schedule::schedule_session); declare the
    /// route with `.requires(Capability::Scheduling)` so a host that cannot
    /// dispatch refuses it at service construction.
    ///
    /// [`Capability::Scheduling`]: crate::schedule::Capability::Scheduling
    pub fn schedule(&self, schedule: crate::schedule::Schedule) -> Result<(), HandlerError> {
        self.workspace
            .stage_schedule(crate::schedule::ScheduleWrite::Arm(schedule))
            .map_err(workspace_handler_error)
    }

    /// Cancel a schedule of this command's aggregate with its effects (no-op
    /// when it is not armed).
    pub fn cancel_schedule(&self, name: impl Into<String>) -> Result<(), HandlerError> {
        self.workspace
            .stage_schedule(crate::schedule::ScheduleWrite::Cancel(name.into()))
            .map_err(workspace_handler_error)
    }

    /// Low-level outbox staging retained for framework internals.
    #[cfg(all(test, feature = "graphql", feature = "sqlite"))]
    pub(crate) fn stage_outbox(&self, message: OutboxMessage) -> Result<(), HandlerError> {
        self.workspace
            .stage_outbox(message)
            .map_err(workspace_handler_error)
    }
}

pub(super) fn workspace_handler_error(error: CausalWorkspaceError) -> HandlerError {
    HandlerError::Other(Box::new(error))
}

/// A typed causal command handler. The framework binds the decoded input to
/// the same `I` used by the GraphQL declaration, and the handler may only
/// prepare a sealed consistency outcome for the durable committer.
/// Captured external side effects are unsupported because handler invocation
/// itself can repeat after lease expiry; see [`CausalCommandContext`].
pub trait PreparedCommandHandler<'a, A, I, K>: Send + Sync
where
    A: 'static,
    K: CommandOutcome,
{
    type Future: Future<Output = Result<PreparedCommand<K>, HandlerError>> + Send + 'a;
    fn call(&self, ctx: &'a CausalCommandContext<'a, A>, input: I) -> Self::Future;
}

impl<'a, D, F, Fut> Handler<'a, D> for F
where
    D: 'a,
    F: Fn(&'a Context<'a, D>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Value, HandlerError>> + Send + 'a,
{
    type Future = Fut;
    fn call(&self, ctx: &'a Context<'a, D>) -> Fut {
        self(ctx)
    }
}

impl<'a, A, I, K, F, Fut> PreparedCommandHandler<'a, A, I, K> for F
where
    A: 'static,
    I: 'a,
    K: CommandOutcome,
    F: Fn(&'a CausalCommandContext<'a, A>, I) -> Fut + Send + Sync,
    Fut: Future<Output = Result<PreparedCommand<K>, HandlerError>> + Send + 'a,
{
    type Future = Fut;

    fn call(&self, ctx: &'a CausalCommandContext<'a, A>, input: I) -> Self::Future {
        self(ctx, input)
    }
}

pub(super) fn boxed_handler<D, F>(handler: F) -> Arc<HandlerFn<D>>
where
    F: for<'a> Handler<'a, D> + 'static,
{
    Arc::new(move |ctx| Box::pin(handler.call(ctx)) as HandlerFuture<'_>)
}

pub(super) type PreparedHandlerFuture<'a, K> =
    Pin<Box<dyn Future<Output = Result<PreparedCommand<K>, HandlerError>> + Send + 'a>>;
pub(super) type PreparedHandlerFn<A, I, K> = dyn for<'a> Fn(&'a CausalCommandContext<'a, A>, I) -> PreparedHandlerFuture<'a, K>
    + Send
    + Sync;
pub(super) type CausalGuardFn<A> =
    dyn for<'a> Fn(&CausalCommandContext<'a, A>) -> bool + Send + Sync;

pub(super) fn boxed_prepared_handler<A, I, K, F>(handler: F) -> Arc<PreparedHandlerFn<A, I, K>>
where
    A: 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
    F: for<'a> PreparedCommandHandler<'a, A, I, K> + 'static,
{
    Arc::new(move |context, input| {
        Box::pin(handler.call(context, input)) as PreparedHandlerFuture<'_, K>
    })
}

pub(super) fn boxed_causal_guard<A, G>(guard: G) -> Arc<CausalGuardFn<A>>
where
    A: 'static,
    G: for<'a> Fn(&CausalCommandContext<'a, A>) -> bool + Send + Sync + 'static,
{
    Arc::new(guard)
}
