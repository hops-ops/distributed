use super::super::handlers::{
    boxed_causal_guard, boxed_handler, boxed_prepared_handler, CausalCommandContext, Handler,
    PreparedCommandHandler,
};
use super::registry::Routes;
use super::spec::HandlerSpec;
use crate::aggregate::Aggregate;
use crate::application::CommandMount;
use crate::command::{CommandOutcome, CompiledInputDefaults, TypedCommandContract};
use crate::microsvc::context::Context;
use crate::microsvc::dependencies::CausalRouteDependencies;
use crate::microsvc::error::HandlerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Domain-owned command that can install itself onto a host [`Routes`] bundle.
///
/// Command declarations live next to the aggregate. SOA and later cell hosts
/// call [`Routes::mount`] with the same value; the declaration must not name
/// sqlx, celld, or `QueuedRepository`.
///
/// Prefer [`crate::command!`] (`PCH-DEC-001`) for shard + invoke +
/// Eventual. The fluent builder on [`Routes`] remains the expansion target.
pub trait PortableCommand<D> {
    /// Register this command on `routes` and return the bundle.
    fn install(self, routes: Routes<D>) -> Routes<D>;
}

impl<D, F> PortableCommand<D> for F
where
    F: FnOnce(Routes<D>) -> Routes<D>,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        self(routes)
    }
}

/// Builder returned by [`Routes::event`] and [`Routes::events`].
pub struct EventRouteBuilder<D> {
    pub(super) routes: Routes<D>,
    pub(super) spec: HandlerSpec,
}

/// Builder returned by [`Routes::typed_command`].
///
/// The declaration and executable handler share one route object, the same
/// input and committed-outcome types, and the route bundle's single
/// aggregate repository.
pub struct TypedRouteBuilder<D, I, K: CommandOutcome> {
    pub(super) routes: Routes<D>,
    pub(super) route_name: &'static str,
    pub(super) contract: TypedCommandContract,
    pub(super) mount: Option<CommandMount>,
    pub(super) requires: Vec<crate::schedule::Capability>,
    pub(super) shard: Option<CommandShard<I>>,
    pub(super) _types: std::marker::PhantomData<fn(I) -> K>,
}

/// The aggregate id (shard) a command input addresses.
pub(super) type CommandShard<I> = Arc<dyn Fn(&I) -> String + Send + Sync>;

impl<D: Send + Sync + 'static> EventRouteBuilder<D> {
    /// Register an async handler without a guard.
    pub fn handle<F>(self, handler: F) -> Routes<D>
    where
        F: for<'a> Handler<'a, D> + 'static,
    {
        self.routes
            .register_event_handler(self.spec, None, boxed_handler(handler))
    }

    /// Register an async handler with a (synchronous) guard.
    pub fn guarded<G, F>(self, guard: G, handler: F) -> Routes<D>
    where
        G: Fn(&Context<D>) -> bool + Send + Sync + 'static,
        F: for<'a> Handler<'a, D> + 'static,
    {
        self.routes
            .register_event_handler(self.spec, Some(Arc::new(guard)), boxed_handler(handler))
    }
}

impl<D, I, K> TypedRouteBuilder<D, I, K>
where
    K: CommandOutcome,
{
    /// Override the GraphQL mutation field name (defaults from the command id).
    #[must_use]
    pub fn field_name(mut self, field_name: impl Into<String>) -> Self {
        self.contract.field_name = field_name.into();
        self
    }

    /// Restrict the command to these surface roles.
    #[must_use]
    pub fn roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.contract.roles = roles.into_iter().map(Into::into).collect();
        self.contract.roles.sort();
        self.contract.roles.dedup();
        self
    }

    /// Declare values generated once into the canonical command input before
    /// dispatch.
    #[must_use]
    pub fn input_defaults(mut self, defaults: CompiledInputDefaults<I>) -> Self {
        self.contract.input_defaults = defaults.0;
        self.contract
            .input_defaults
            .sort_by(|left, right| left.path.cmp(&right.path));
        self
    }

    /// Declare that one emitted state or flat event-body field is the
    /// authenticated `x-user-id`. `B` is the exact serialized body type.
    ///
    /// This is preview provenance only; command admission and handler-side
    /// principal binding remain mandatory.
    #[must_use]
    pub fn authenticated_user_field<E, B>(mut self, rust_field: &'static str) -> Self
    where
        E: crate::domain_event::DomainEventBodyContract<B>,
        B: crate::command::CommandProjectionBody,
    {
        let values = crate::command::authenticated_user_field_preview::<E, B>(rust_field);
        self.contract
            .projections
            .add_authenticated_user_field(rust_field, values);
        self
    }

    /// Pure reducer over a known cache row for client auto-optimism.
    #[must_use]
    pub fn preview_reduce_known_record(
        mut self,
        reduce: crate::command::CommandProjectionPureReduce,
    ) -> Self {
        self.contract.projections.add_pure_reduce(reduce);
        self
    }

    /// Attach the mount produced by the same command declaration. The route
    /// adapter checks its identity before registration.
    pub fn mount(mut self, mount: CommandMount) -> Self {
        self.mount = Some(mount);
        self
    }

    /// Declare the aggregate id (shard) each input addresses: the stream it
    /// loads or creates. A command host that places aggregates elsewhere (a
    /// cell per shard) routes by it. `load_by`, `load_or_create_by` and
    /// `create_by` declare their id this way.
    #[must_use]
    pub fn shard_by<F>(mut self, shard: F) -> Self
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        self.shard = Some(Arc::new(shard));
        self
    }

    /// Declare a host capability this command needs
    /// ([`Capability::Scheduling`](crate::schedule::Capability::Scheduling)
    /// for `ctx.schedule`, [`Capability::Clock`](crate::schedule::Capability::Clock)
    /// for `ctx.now()`). Registering the route on a backend without it panics
    /// with the missing capabilities, at service construction rather than at
    /// the first commit.
    #[must_use]
    pub fn requires(mut self, capability: crate::schedule::Capability) -> Self {
        if !self.requires.contains(&capability) {
            self.requires.push(capability);
        }
        self
    }
}

impl<D, I, K> TypedRouteBuilder<D, I, K>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    /// Register a typed causal command handler without a guard.
    pub fn handle<F>(self, handler: F) -> Routes<D>
    where
        F: for<'a> PreparedCommandHandler<'a, D::Aggregate, I, K> + 'static,
    {
        self.routes.register_typed_handler(
            self.route_name,
            self.contract,
            self.mount,
            None,
            boxed_prepared_handler(handler),
            self.requires,
            self.shard,
        )
    }

    /// Register a typed causal command handler with a synchronous guard.
    pub fn guarded<G, F>(self, guard: G, handler: F) -> Routes<D>
    where
        G: for<'a> Fn(&CausalCommandContext<'a, D::Aggregate>) -> bool + Send + Sync + 'static,
        F: for<'a> PreparedCommandHandler<'a, D::Aggregate, I, K> + 'static,
    {
        let guard = boxed_causal_guard(guard);
        self.routes.register_typed_handler(
            self.route_name,
            self.contract,
            self.mount,
            Some(guard),
            boxed_prepared_handler(handler),
            self.requires,
            self.shard,
        )
    }
}

impl<D, I, K> TypedRouteBuilder<D, I, K>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    /// Load the aggregate by an id taken from the command input; `NotFound`
    /// when it does not exist.
    ///
    /// Chain [`.invoke`](ThinCommandBuilder::invoke) then
    /// [`.eventual`](ThinCommandBuilder::eventual). Application code does not
    /// write a [`CausalCommandContext`] body.
    pub fn load_by<F>(self, id: F) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        let (builder, id) = self.sharded_by(id);
        builder.thin(move |input| super::super::CommandLoad::Required(id(input)))
    }

    /// Load the aggregate by id, or start a new one when it does not exist.
    pub fn load_or_create_by<F>(self, id: F) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        let (builder, id) = self.sharded_by(id);
        builder.thin(move |input| super::super::CommandLoad::OrCreate(id(input)))
    }

    /// Start a new aggregate; the command is rejected when `id` already has one.
    pub fn create_by<F>(self, id: F) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        let (builder, id) = self.sharded_by(id);
        builder.thin(move |input| super::super::CommandLoad::Create(Some(id(input))))
    }

    /// The id the command loads is its shard.
    fn sharded_by<F>(mut self, id: F) -> (Self, CommandShard<I>)
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        let id: CommandShard<I> = Arc::new(id);
        self.shard = Some(Arc::clone(&id));
        (self, id)
    }

    /// Start a new aggregate that names itself in its transition.
    pub fn create(self) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded> {
        self.thin(|_| super::super::CommandLoad::Create(None))
    }

    fn thin(
        self,
        load: impl Fn(&I) -> super::super::CommandLoad + Send + Sync + 'static,
    ) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded> {
        ThinCommandBuilder {
            inner: self,
            load: Arc::new(load),
            guard: None,
            transition: None,
            _phase: std::marker::PhantomData,
        }
    }
}

/// Marker: id source chosen (`load_by`, `load_or_create_by`, `create_by`, `create`).
pub struct ThinCommandLoaded;

/// Marker: domain transition registered.
pub struct ThinCommandInvoked;

type ThinTransition<A, I> = dyn Fn(&mut crate::microsvc::AggregateCheckout<A>, &I, &str) -> Result<(), String>
    + Send
    + Sync;

/// Commit a checkout and build the command's outcome from its payload.
type ThinFinish<A, K> = dyn for<'a> Fn(
        &CausalCommandContext<'a, A>,
        crate::microsvc::AggregateCheckout<A>,
        &str,
    ) -> Result<crate::command::PreparedCommand<K>, HandlerError>
    + Send
    + Sync;

/// Load/create + domain invoke + commit. The framework owns the handler body.
pub struct ThinCommandBuilder<D, I, K: CommandOutcome, Phase>
where
    D: CausalRouteDependencies,
    D::Aggregate: Aggregate + Send + Sync + 'static,
{
    inner: TypedRouteBuilder<D, I, K>,
    load: Arc<dyn Fn(&I) -> super::super::CommandLoad + Send + Sync>,
    guard: Option<Arc<super::super::handlers::CausalGuardFn<D::Aggregate>>>,
    transition: Option<Arc<ThinTransition<D::Aggregate, I>>>,
    _phase: std::marker::PhantomData<Phase>,
}

impl<D, I, K, Phase> ThinCommandBuilder<D, I, K, Phase>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    /// Admit the command only when `guard` holds, on top of its roles.
    #[must_use]
    pub fn guard<G>(mut self, guard: G) -> Self
    where
        G: for<'a> Fn(&CausalCommandContext<'a, D::Aggregate>) -> bool + Send + Sync + 'static,
    {
        self.guard = Some(boxed_causal_guard(guard));
        self
    }
}

impl<D, I, K> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    K: CommandOutcome,
{
    /// Apply a domain method. The third argument is the session principal
    /// when `.roles` require one, and empty otherwise.
    pub fn invoke<T, E>(self, transition: T) -> ThinCommandBuilder<D, I, K, ThinCommandInvoked>
    where
        T: Fn(&mut crate::microsvc::AggregateCheckout<D::Aggregate>, &I, &str) -> Result<(), E>
            + Send
            + Sync
            + 'static,
        E: std::fmt::Display,
    {
        ThinCommandBuilder {
            inner: self.inner,
            load: self.load,
            guard: self.guard,
            transition: Some(Arc::new(move |aggregate, input, principal| {
                transition(aggregate, input, principal).map_err(|error| error.to_string())
            })),
            _phase: std::marker::PhantomData,
        }
    }
}

impl<D, I, K> ThinCommandBuilder<D, I, K, ThinCommandInvoked>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    K: CommandOutcome,
{
    fn finish(self, finish: Arc<ThinFinish<D::Aggregate, K>>) -> Routes<D> {
        let handler = ThinHandler {
            load: self.load,
            transition: self
                .transition
                .expect("the Invoked phase always holds a transition"),
            finish,
            needs_principal: crate::application::command_roles_require_principal(
                &self.inner.contract.roles,
            ),
        };
        let inner = self.inner;
        inner.routes.register_typed_handler(
            inner.route_name,
            inner.contract,
            inner.mount,
            self.guard,
            boxed_prepared_handler(handler),
            inner.requires,
            inner.shard,
        )
    }
}

impl<D, I, T> ThinCommandBuilder<D, I, crate::command::Eventual<T>, ThinCommandInvoked>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    /// Publish captured events, commit, and return an Eventual payload built
    /// from the aggregate and the session principal.
    ///
    /// This registers the command. Application code supplies only the payload
    /// mapping, not a handler-context body.
    pub fn eventual<F>(self, payload: F) -> Routes<D>
    where
        F: Fn(&crate::microsvc::AggregateCheckout<D::Aggregate>, &str) -> T + Send + Sync + 'static,
    {
        self.finish(Arc::new(move |ctx, checkout, principal| {
            let body = payload(&checkout, principal);
            ctx.publish_events().commit(checkout)?.eventual(body)
        }))
    }
}

impl<D, I, T> ThinCommandBuilder<D, I, crate::command::Succeeded<T>, ThinCommandInvoked>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    /// Publish captured events, commit, and return a Succeeded payload built
    /// from the aggregate and the session principal. Used when the command is
    /// not Eventual: publication is the same, only the visibility promise
    /// differs.
    pub fn succeeded<F>(self, payload: F) -> Routes<D>
    where
        F: Fn(&crate::microsvc::AggregateCheckout<D::Aggregate>, &str) -> T + Send + Sync + 'static,
    {
        self.finish(Arc::new(move |ctx, checkout, principal| {
            let body = payload(&checkout, principal);
            ctx.publish_events().commit(checkout)?.succeeded(body)
        }))
    }
}

struct ThinHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    load: Arc<dyn Fn(&I) -> super::super::CommandLoad + Send + Sync>,
    transition: Arc<ThinTransition<A, I>>,
    finish: Arc<ThinFinish<A, K>>,
    needs_principal: bool,
}

impl<'a, A, I, K> PreparedCommandHandler<'a, A, I, K> for ThinHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    K: CommandOutcome + 'static,
{
    type Future = Pin<
        Box<
            dyn Future<Output = Result<crate::command::PreparedCommand<K>, HandlerError>>
                + Send
                + 'a,
        >,
    >;

    fn call(&self, ctx: &'a CausalCommandContext<'a, A>, input: I) -> Self::Future {
        let load = self.load.clone();
        let transition = self.transition.clone();
        let finish = self.finish.clone();
        let needs_principal = self.needs_principal;
        Box::pin(async move {
            let principal = if needs_principal {
                ctx.user_id()?.to_string()
            } else {
                String::new()
            };
            let mut checkout = super::super::load_for_command(ctx, load(&input)).await?;
            super::super::invoke_transition(&mut checkout, |checkout| {
                transition(checkout, &input, &principal)
            })?;
            finish(ctx, checkout, &principal)
        })
    }
}
