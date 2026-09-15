use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use crate::bus::{Message, MessageKind, OrderedDelivery};
use crate::projection_protocol::{
    CompiledProjectionTopology, ProjectionEpoch, ProjectionGeneration, ProjectionInputCursor,
    ProjectionInputDisposition, ProjectionModelOwnership, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionWorkspace, ProjectorTopologyId, TrustedProjectionInput,
};

use super::super::dependencies::CausalProjectionRouteDependencies;
use super::super::HandlerError;
use super::context::{unavailable_workspace, CausalProjectorContext};
use super::errors::{
    canonical_message_fingerprint, handle_projector_error, preflight_runtime_state,
    projection_error_is_retryable, record_ingress_failure, record_terminal_protocol_failure,
    terminal_recorded,
};
use super::handle::ProjectionRepairHandle;
use super::registration::ProjectorHandlerFn;
#[cfg(feature = "graphql")]
use super::registration::{ModeledProjection, ModeledProjectorHandlerFn};

pub(in crate::microsvc) type ProjectorDispatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'a>>;
pub(in crate::microsvc) type ProjectorRepairFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ProjectionGeneration>, HandlerError>> + Send + 'a>>;
pub(in crate::microsvc) type ProjectorRepairLookupFuture<'a> =
    Pin<Box<dyn Future<Output = Result<bool, HandlerError>> + Send + 'a>>;

pub(in crate::microsvc) trait ErasedProjectorHandler<D>: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        message: &'a Message,
        ordered: Option<&'a OrderedDelivery>,
    ) -> ProjectorDispatchFuture<'a>;

    fn registration(&self) -> ProjectorRegistration;

    fn bootstrap<'a>(&'a self, dependencies: &'a D) -> ProjectorDispatchFuture<'a>;

    fn repair<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a>;

    fn locates_failure<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a>;
}

#[derive(Clone)]
pub(in crate::microsvc) struct ProjectorRegistration {
    pub(in crate::microsvc) topology: ProjectorTopologyId,
    pub(in crate::microsvc) ownership: Vec<ProjectionModelOwnership>,
}

pub(super) struct RegisteredProjector<I> {
    pub(super) compiled: CompiledProjectionTopology,
    pub(super) change_epoch: ProjectionEpoch,
    pub(super) handle: Arc<ProjectorHandlerFn<I>>,
}

#[cfg(feature = "graphql")]
pub(in crate::microsvc) struct RegisteredModeledProjector {
    pub(in crate::microsvc) compiled: CompiledProjectionTopology,
    pub(in crate::microsvc) change_epoch: ProjectionEpoch,
    pub(in crate::microsvc) executor: crate::projection::lower::ProjectionServerExecutorDescriptor,
    pub(in crate::microsvc) handle: Option<Arc<ModeledProjectorHandlerFn>>,
}

#[cfg(feature = "graphql")]
impl<D> ErasedProjectorHandler<D> for RegisteredModeledProjector
where
    D: CausalProjectionRouteDependencies + Send + Sync + 'static,
{
    fn registration(&self) -> ProjectorRegistration {
        ProjectorRegistration {
            topology: self.compiled.topology().clone(),
            ownership: self.compiled.ownership().to_vec(),
        }
    }

    fn bootstrap<'a>(&'a self, dependencies: &'a D) -> ProjectorDispatchFuture<'a> {
        Box::pin(async move {
            dependencies
                .__causal_projection_store()
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            Ok(())
        })
    }

    fn repair<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a> {
        Box::pin(async move {
            let store = dependencies.__causal_projection_store();
            let Some(location) = store
                .projection_failure_location(handle.failure_id())
                .await?
            else {
                return Ok(None);
            };
            if &location.topology != self.compiled.topology() {
                return Ok(None);
            }
            store
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            let generation = store
                .repair_projection(&location.topology, &location.partition, handle.failure_id())
                .await?;
            Ok(Some(generation))
        })
    }

    fn locates_failure<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a> {
        Box::pin(async move {
            let location = dependencies
                .__causal_projection_store()
                .projection_failure_location(handle.failure_id())
                .await?;
            Ok(location
                .as_ref()
                .is_some_and(|location| &location.topology == self.compiled.topology()))
        })
    }

    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        message: &'a Message,
        ordered: Option<&'a OrderedDelivery>,
    ) -> ProjectorDispatchFuture<'a> {
        Box::pin(async move {
            let ordered = ordered.ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(format!(
                    "modeled causal projector `{}` requires adapter-authenticated ordered delivery",
                    self.compiled.topology().name()
                ))
            })?;
            if message.kind != MessageKind::Event {
                return Err(HandlerError::UnqualifiedProjectionDelivery(
                    "modeled causal projector routes accept only event deliveries".into(),
                ));
            }
            let message_id = message.id().ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(
                    "modeled causal projector delivery is missing a stable message ID".into(),
                )
            })?;
            let causation_id = message.causation_id().ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(
                    "modeled causal projector delivery is missing a causation ID".into(),
                )
            })?;
            let store = dependencies.__causal_projection_store();
            let occurrence =
                match crate::DomainEventOccurrence::from_canonical_bytes(&message.payload) {
                    Ok(occurrence)
                        if occurrence.id() == message_id
                            && occurrence.descriptor().name == message.name
                            && occurrence.causation_id() == Some(causation_id) =>
                    {
                        occurrence
                    }
                    Ok(_) => {
                        return record_ingress_failure(
                            store,
                            &self.compiled,
                            self.change_epoch.clone(),
                            message,
                            ordered,
                            message_id,
                            causation_id,
                            HandlerError::DecodeFailed(
                                "domain occurrence differs from its transport envelope".into(),
                            ),
                        )
                        .await
                    }
                    Err(error) => {
                        return record_ingress_failure(
                            store,
                            &self.compiled,
                            self.change_epoch.clone(),
                            message,
                            ordered,
                            message_id,
                            causation_id,
                            HandlerError::Other(Box::new(error)),
                        )
                        .await
                    }
                };
            let lowered = if self.executor.matches(&occurrence) {
                match self.executor.plan(&occurrence) {
                    Ok(lowered) => Some(lowered),
                    Err(error) => {
                        return record_ingress_failure(
                            store,
                            &self.compiled,
                            self.change_epoch.clone(),
                            message,
                            ordered,
                            message_id,
                            causation_id,
                            HandlerError::Other(Box::new(error)),
                        )
                        .await
                    }
                }
            } else if self.executor.has_unit_partition() {
                // Name-based transports may fan one event name to descriptors
                // with distinct versions/schemas. A well-formed occurrence
                // outside this exact selector set is not a projector failure.
                // Unit partitions still seal an empty checkpoint so gap-free
                // source ordering remains contiguous for the next match.
                None
            } else {
                // A dynamic partition cannot be derived from an occurrence
                // outside the program's body contract. Dynamic modeled
                // projectors never claim source-wide gap-free ordering, so the
                // broker ACK is sufficient and no false partition cursor is
                // invented.
                return Ok(());
            };
            let partition_value = lowered
                .as_ref()
                .map(|lowered| {
                    crate::projection::executor::resolved_partition_json(
                        lowered.resolved.partition(),
                    )
                })
                .transpose()?
                .flatten();
            let projection_partition = self
                .compiled
                .codec()
                .encode_partition(partition_value.as_ref())
                .map_err(|error| {
                    ProjectionProtocolError::InvalidBatch(format!(
                        "modeled causal projector partition could not be encoded: {error}"
                    ))
                })?;
            let cursor = ProjectionInputCursor::new(
                self.compiled.topology().clone(),
                projection_partition.clone(),
                ordered.source().clone(),
                ordered.epoch().clone(),
                ordered.position(),
            )
            .map_err(ProjectionProtocolError::from)?;
            let fingerprint = canonical_message_fingerprint(message);
            let gap_free = ordered.is_gap_free() && partition_value.is_none();

            store
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            let runtime_state = store
                .projection_partition_runtime_state(self.compiled.topology(), &projection_partition)
                .await?;
            let generation = preflight_runtime_state(
                runtime_state.as_ref(),
                &cursor,
                fingerprint,
                message_id,
                causation_id,
                gap_free,
            )?;
            let trusted = TrustedProjectionInput::mint(
                cursor,
                fingerprint,
                message_id,
                causation_id,
                generation,
                gap_free,
            )?;
            match store.projection_input_disposition(&trusted).await? {
                ProjectionInputDisposition::Pending => {}
                ProjectionInputDisposition::Duplicate(_) | ProjectionInputDisposition::Stale(_) => {
                    return Ok(())
                }
            }
            let failure_input = trusted.clone();
            let workspace = ProjectionWorkspace::new_with_program_id(
                self.compiled.codec(),
                partition_value,
                trusted,
                self.change_epoch.clone(),
                Some(self.executor.program_id),
            )?;
            let (context, workspace) =
                CausalProjectorContext::new(message, D::Store::clone(store), workspace);
            if let Some(handle) = &self.handle {
                let (projection, applied) =
                    ModeledProjection::new(self.executor.program_id, lowered);
                if let Err(error) = handle(context, projection).await {
                    return handle_projector_error(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "modeled_handler",
                        error,
                    )
                    .await;
                }
                if !applied.load(std::sync::atomic::Ordering::Acquire) {
                    return handle_projector_error(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "modeled_handler",
                        HandlerError::Rejected(
                            "modeled projector handler returned without applying its projection"
                                .into(),
                        ),
                    )
                    .await;
                }
            } else if let Some(lowered) = lowered {
                if let Err(error) = context.apply_portable(lowered).await {
                    return handle_projector_error(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "modeled_apply",
                        error,
                    )
                    .await;
                }
            }
            let workspace = workspace
                .lock()
                .map_err(|_| unavailable_workspace("seal modeled projection batch"))?
                .take()
                .ok_or_else(|| unavailable_workspace("seal modeled projection batch"))?;
            let batch = match workspace.into_batch() {
                Ok(batch) => batch,
                Err(error) => {
                    return record_terminal_protocol_failure(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "modeled_workspace_seal",
                        error,
                    )
                    .await
                }
            };
            match store.commit_projection(batch).await {
                Ok(_) => Ok(()),
                Err(ProjectionProtocolError::PartitionStopped { failure_id }) => {
                    Err(terminal_recorded(failure_id))
                }
                Err(error) if projection_error_is_retryable(&error) => Err(error.into()),
                Err(error) => {
                    record_terminal_protocol_failure(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "modeled_commit_projection",
                        error,
                    )
                    .await
                }
            }
        })
    }
}

impl<D, I> ErasedProjectorHandler<D> for RegisteredProjector<I>
where
    D: CausalProjectionRouteDependencies + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
{
    fn registration(&self) -> ProjectorRegistration {
        ProjectorRegistration {
            topology: self.compiled.topology().clone(),
            ownership: self.compiled.ownership().to_vec(),
        }
    }

    fn bootstrap<'a>(&'a self, dependencies: &'a D) -> ProjectorDispatchFuture<'a> {
        Box::pin(async move {
            dependencies
                .__causal_projection_store()
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            Ok(())
        })
    }

    fn repair<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a> {
        Box::pin(async move {
            let store = dependencies.__causal_projection_store();
            let Some(location) = store
                .projection_failure_location(handle.failure_id())
                .await?
            else {
                return Ok(None);
            };
            if &location.topology != self.compiled.topology() {
                return Ok(None);
            }
            store
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            let generation = store
                .repair_projection(&location.topology, &location.partition, handle.failure_id())
                .await?;
            Ok(Some(generation))
        })
    }

    fn locates_failure<'a>(
        &'a self,
        dependencies: &'a D,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a> {
        Box::pin(async move {
            let location = dependencies
                .__causal_projection_store()
                .projection_failure_location(handle.failure_id())
                .await?;
            Ok(location
                .as_ref()
                .is_some_and(|location| &location.topology == self.compiled.topology()))
        })
    }

    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        message: &'a Message,
        ordered: Option<&'a OrderedDelivery>,
    ) -> ProjectorDispatchFuture<'a> {
        Box::pin(async move {
            let ordered = ordered.ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(format!(
                    "causal projector `{}` requires adapter-authenticated ordered delivery",
                    self.compiled.topology().name()
                ))
            })?;
            if message.kind != MessageKind::Event {
                return Err(HandlerError::UnqualifiedProjectionDelivery(
                    "causal projector routes accept only event deliveries".into(),
                ));
            }
            let message_id = message.id().ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(
                    "causal projector delivery is missing a stable message ID".into(),
                )
            })?;
            let causation_id = message.causation_id().ok_or_else(|| {
                HandlerError::UnqualifiedProjectionDelivery(
                    "causal projector delivery is missing a causation ID".into(),
                )
            })?;

            let mut canonical_input = None;
            let partition_value = if self.compiled.partition().requires_input() {
                match message.payload_json::<Value>() {
                    Ok(input) => match self.compiled.partition().resolve(&input) {
                        Ok(partition) => {
                            canonical_input = Some(input);
                            partition
                        }
                        Err(error) => {
                            return record_ingress_failure(
                                dependencies.__causal_projection_store(),
                                &self.compiled,
                                self.change_epoch.clone(),
                                message,
                                ordered,
                                message_id,
                                causation_id,
                                error.into(),
                            )
                            .await
                        }
                    },
                    Err(error) => {
                        return record_ingress_failure(
                            dependencies.__causal_projection_store(),
                            &self.compiled,
                            self.change_epoch.clone(),
                            message,
                            ordered,
                            message_id,
                            causation_id,
                            HandlerError::from(error),
                        )
                        .await
                    }
                }
            } else {
                self.compiled.partition().resolve(&Value::Null)?
            };
            let projection_partition = self
                .compiled
                .codec()
                .encode_partition(partition_value.as_ref())
                .map_err(|error| {
                    ProjectionProtocolError::InvalidBatch(format!(
                        "causal projector partition could not be encoded: {error}"
                    ))
                })?;
            let cursor = ProjectionInputCursor::new(
                self.compiled.topology().clone(),
                projection_partition.clone(),
                ordered.source().clone(),
                ordered.epoch().clone(),
                ordered.position(),
            )
            .map_err(ProjectionProtocolError::from)?;
            let fingerprint = canonical_message_fingerprint(message);
            // Adapter gap-free evidence is scoped to its source partition.
            // A dynamic input-path partition splits that sequence, so it can
            // preserve ordering but cannot prove gap-free progress for any one
            // derived projection partition.
            let gap_free =
                ordered.is_gap_free() && self.compiled.partition().preserves_source_sequence();
            let store = dependencies.__causal_projection_store();

            // Idempotent global ownership bootstrap deliberately precedes every
            // partition read/handler invocation. Adapters make repeated exact
            // registration cheap and reject any conflicting owner.
            store
                .register_projection_models(self.compiled.topology(), self.compiled.ownership())
                .await?;
            let runtime_state = store
                .projection_partition_runtime_state(self.compiled.topology(), &projection_partition)
                .await?;
            let generation = preflight_runtime_state(
                runtime_state.as_ref(),
                &cursor,
                fingerprint,
                message_id,
                causation_id,
                gap_free,
            )?;
            let trusted = TrustedProjectionInput::mint(
                cursor,
                fingerprint,
                message_id,
                causation_id,
                generation,
                gap_free,
            )?;
            match store.projection_input_disposition(&trusted).await? {
                ProjectionInputDisposition::Pending => {}
                ProjectionInputDisposition::Duplicate(_) | ProjectionInputDisposition::Stale(_) => {
                    return Ok(())
                }
            }
            let failure_input = trusted.clone();
            let canonical_input = match canonical_input {
                Some(input) => input,
                None => match message.payload_json::<Value>() {
                    Ok(input) => input,
                    Err(error) => {
                        return handle_projector_error(
                            store,
                            failure_input,
                            self.change_epoch.clone(),
                            "json_decode",
                            HandlerError::from(error),
                        )
                        .await
                    }
                },
            };
            let input = match serde_json::from_value::<I>(canonical_input) {
                Ok(input) => input,
                Err(error) => {
                    return handle_projector_error(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "typed_decode",
                        HandlerError::from(error),
                    )
                    .await
                }
            };
            let workspace = ProjectionWorkspace::new(
                self.compiled.codec(),
                partition_value,
                trusted,
                self.change_epoch.clone(),
            )?;
            let (context, workspace) =
                CausalProjectorContext::new(message, D::Store::clone(store), workspace);
            if let Err(error) = (self.handle)(context, input).await {
                return handle_projector_error(
                    store,
                    failure_input,
                    self.change_epoch.clone(),
                    "handler_error",
                    error,
                )
                .await;
            }
            let workspace = workspace
                .lock()
                .map_err(|_| unavailable_workspace("seal projection batch"))?
                .take()
                .ok_or_else(|| unavailable_workspace("seal projection batch"))?;
            let batch = match workspace.into_batch() {
                Ok(batch) => batch,
                Err(error) => {
                    return record_terminal_protocol_failure(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "workspace_seal",
                        error,
                    )
                    .await
                }
            };
            match store.commit_projection(batch).await {
                Ok(_) => Ok(()),
                Err(ProjectionProtocolError::PartitionStopped { failure_id }) => {
                    Err(terminal_recorded(failure_id))
                }
                Err(error) if projection_error_is_retryable(&error) => Err(error.into()),
                Err(error) => {
                    record_terminal_protocol_failure(
                        store,
                        failure_input,
                        self.change_epoch.clone(),
                        "commit_projection",
                        error,
                    )
                    .await
                }
            }
        })
    }
}
