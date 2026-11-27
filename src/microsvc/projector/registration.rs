use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::graphql::SurfaceProjector;
#[cfg(feature = "graphql")]
use crate::projection::lower::ProjectionDescriptor;
#[cfg(feature = "graphql")]
use crate::projection::ProjectionProgramId;
use crate::projection_protocol::CompiledProjectionTopology;
use crate::read_model::RelationalReadModel;
use crate::table::TableSchema;

use super::super::dependencies::CausalProjectionRouteDependencies;
use super::super::service::{HandlerSpec, Routes};
use super::super::HandlerError;
use super::context::CausalProjectorContext;
use super::runtime::RegisteredProjector;

pub(in crate::microsvc) type ProjectorHandlerFuture =
    Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'static>>;
pub(super) type ProjectorHandlerFn<I> =
    dyn Fn(CausalProjectorContext, I) -> ProjectorHandlerFuture + Send + Sync;
fn boxed_projector_handler<I, F, Fut>(handler: F) -> Arc<ProjectorHandlerFn<I>>
where
    I: Send + 'static,
    F: Fn(CausalProjectorContext, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    Arc::new(move |context, input| Box::pin(handler(context, input)))
}

#[cfg(feature = "graphql")]
pub(in crate::microsvc) type ModeledProjectorHandlerFn =
    dyn Fn(CausalProjectorContext, ModeledProjection) -> ProjectorHandlerFuture + Send + Sync;

#[cfg(feature = "graphql")]
fn boxed_modeled_projector_handler<F, Fut>(handler: F) -> Arc<ModeledProjectorHandlerFn>
where
    F: Fn(CausalProjectorContext, ModeledProjection) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    Arc::new(move |context, projection| Box::pin(handler(context, projection)))
}

/// One resolved invocation of a compiler-modeled projection.
///
/// Explicit projector handlers receive this token and apply it through the
/// capability-restricted [`CausalProjectorContext`]. The runtime rejects a
/// handler that returns success without applying the token.
#[must_use = "a modeled projection handler must apply this token"]
#[cfg(feature = "graphql")]
pub struct ModeledProjection {
    program_id: ProjectionProgramId,
    plan: Option<crate::projection::lower::LoweredProjectionPlan>,
    applied: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "graphql")]
impl ModeledProjection {
    pub(in crate::microsvc) fn new(
        program_id: ProjectionProgramId,
        plan: Option<crate::projection::lower::LoweredProjectionPlan>,
    ) -> (Self, Arc<std::sync::atomic::AtomicBool>) {
        let applied = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            Self {
                program_id,
                plan,
                applied: Arc::clone(&applied),
            },
            applied,
        )
    }

    /// Stage the modeled read-model operations in the projector's causal
    /// workspace. The runtime performs the atomic protocol commit after the
    /// handler returns.
    pub async fn apply<D>(
        self,
        descriptor: ProjectionDescriptor<D>,
        context: &CausalProjectorContext,
    ) -> Result<(), HandlerError> {
        let declared_program_id = descriptor
            .program_id()
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        if declared_program_id != self.program_id {
            return Err(HandlerError::Rejected(format!(
                "modeled projector handler applied projection `{}` but route resolved `{}`",
                declared_program_id, self.program_id
            )));
        }
        if let Some(plan) = self.plan {
            context.apply_portable(plan).await?;
        }
        self.applied
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Builder for an explicit handler that applies one modeled projection.
#[cfg(feature = "graphql")]
pub struct ModeledProjectorRouteBuilder<D> {
    routes: Routes<D>,
    declaration: SurfaceProjector,
}

#[cfg(feature = "graphql")]
impl<D> ModeledProjectorRouteBuilder<D>
where
    D: CausalProjectionRouteDependencies + Send + Sync + 'static,
{
    pub(in crate::microsvc) fn new(routes: Routes<D>, declaration: SurfaceProjector) -> Self {
        Self {
            routes,
            declaration,
        }
    }

    /// Register the event handler that explicitly applies the compiler-modeled
    /// projection.
    pub fn handle<F, Fut>(self, handler: F) -> Routes<D>
    where
        F: Fn(CausalProjectorContext, ModeledProjection) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        self.routes.register_modeled_projection(
            self.declaration,
            Some(boxed_modeled_projector_handler(handler)),
        )
    }
}

/// Builder for one typed causal projector route.
pub struct CausalProjectorRouteBuilder<D, I> {
    routes: Routes<D>,
    declaration: SurfaceProjector,
    schemas: Vec<&'static TableSchema>,
    _input: PhantomData<fn(I)>,
}

impl<D, I> CausalProjectorRouteBuilder<D, I>
where
    D: CausalProjectionRouteDependencies + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
{
    pub(in crate::microsvc) fn new(routes: Routes<D>, declaration: SurfaceProjector) -> Self {
        Self {
            routes,
            declaration,
            schemas: Vec::new(),
            _input: PhantomData,
        }
    }

    /// Register one complete typed output model. Call once for every model in
    /// the reused [`SurfaceProjector`] declaration.
    pub fn model<M>(mut self) -> Self
    where
        M: RelationalReadModel + 'static,
    {
        self.schemas.push(M::schema());
        self
    }

    /// Register the capability-restricted handler.
    ///
    /// Invalid/incomplete topology declarations panic at service construction,
    /// before transport traffic can start, matching ordinary duplicate-route
    /// registration behavior.
    pub fn handle<F, Fut>(self, handler: F) -> Routes<D>
    where
        F: Fn(CausalProjectorContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        if !self.declaration.modeled.is_empty() {
            panic!(
                "causal projector `{}` carries modeled projection registrations; mount it with `Routes::modeled_projector(...).handle(...)` or `Routes::consume_projection(...)` instead of the legacy `causal_projector(...).model(...).handle(...)` builder",
                self.declaration.name
            );
        }
        if self.declaration.facts.is_empty() {
            panic!(
                "causal projector `{}` requires at least one accepted fact",
                self.declaration.name
            );
        }
        let compiled = CompiledProjectionTopology::compile(
            &self.declaration.name,
            &self.declaration.facts,
            &self.declaration.models,
            &self.declaration.partition,
            self.schemas,
        )
        .unwrap_or_else(|error| {
            panic!(
                "causal projector `{}` has an invalid compiled topology: {error}",
                self.declaration.name
            )
        });
        let spec = HandlerSpec::projector(self.declaration.facts.clone());
        self.routes.register_projector(
            spec,
            Box::new(RegisteredProjector::<I> {
                compiled,
                handle: boxed_projector_handler(handler),
            }),
        )
    }
}
