#![allow(clippy::module_inception)]
#![doc = include_str!("../README.md")]

// Allow proc-macros to reference this crate by name even when used internally
extern crate self as distributed;

/// Macro implementation support. External packages may rename the
/// distributed dependency and re-export it under the generated path; the
/// generated code must not require a direct serde dependency of its own.
#[doc(hidden)]
pub mod __private {
    pub use serde;
}

/// Service guard shared with the integration tests.
#[cfg(test)]
pub(crate) use common::env as test_env;

mod time;

pub mod aggregate;
pub mod application;
pub mod bus;
pub mod clock;
pub mod command;
pub mod command_dispatch;
pub mod domain_event;
pub mod entity;
pub mod repository;

pub(crate) mod command_ledger;
mod commit_builder;
#[cfg(feature = "gateway")]
pub mod gateway;
pub mod graphql;
mod in_memory_repo;
pub mod lease;
pub mod lock;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod microsvc;
/// Celld Durable Object host adapter (not a sqlx dialect; no `celld` feature).
pub use microsvc::cell_host;
pub mod mutation;
pub mod outbox;
pub mod outbox_worker;
#[cfg(feature = "postgres")]
pub mod postgres_repo;
pub mod projection;
pub mod projection_protocol;
pub mod queued_repo;
pub mod read_model;
pub mod saga;
pub mod schedule;
pub mod snapshot;
#[cfg(feature = "sqlite")]
pub mod sqlite_repo;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sqlx_repo;
pub mod table;
mod telemetry;
pub mod trace_context;

// Re-export entity types at crate root for convenience
pub use entity::{
    upcast_events, upcast_events_for_replay, upcast_payload, BitcodePayloadCodec, Entity,
    EventRecord, EventRecordError, EventUpcaster, PayloadCodec, Recorded, UpcastError,
    BITCODE_PAYLOAD_CODEC, BITCODE_PAYLOAD_CODEC_VERSION,
};

// Placement-independent application composition. The module path remains the
// canonical namespace; these common contract types are also convenient at the
// crate root for contract-only packages.
pub use application::{
    admit_command_session, command_roles_require_principal, Application, ApplicationError,
    ApplicationManifest, CommandMount, CommandMountRegistrar, CommandSpec, ContractCompiler,
    FrameworkCompatibility, LogicalId, Module, ModuleManifest, ProjectionSpec, SurfaceSpec,
    APPLICATION_MANIFEST_SCHEMA_VERSION,
};
#[cfg(feature = "graphql")]
pub use command_dispatch::{
    CellRequestContext, CommandHost, HttpCommandHost, LocalCommandHost, SharedCommandHost,
    TrustedRequestMetadata,
};

// Domain events: typed outward contracts distinct from replay events/snapshots.
pub use domain_event::{
    DomainDeletion, DomainDeletionError, DomainEvent, DomainEventBodyDescriptor,
    DomainEventBodyKind, DomainEventCaptureError, DomainEventCaptureOutcome,
    DomainEventCapturePoison, DomainEventCommitGuardError, DomainEventDescriptor,
    DomainEventEnvelope, DomainEventOccurrence, DomainState, DomainStateDescriptor,
    ExternalEventSource, DOMAIN_EVENT_BODY_CODEC, DOMAIN_EVENT_BODY_CODEC_VERSION,
    DOMAIN_EVENT_OCCURRENCE_VERSION, MAX_DOMAIN_EVENT_BODY_BYTES,
    MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES,
};

// Logical projection contracts. Physical read-model lowering deliberately lives
// behind adapters and is not part of this semantic surface.
pub use projection::{
    LocalProjectionMounts, LocalProjectionMountsBuilder, ProjectionArm, ProjectionAssignment,
    ProjectionEnvelopeField, ProjectionEventSelector, ProjectionEventSet, ProjectionExpression,
    ProjectionField, ProjectionInvalidation, ProjectionKeyField, ProjectionMutationProvenance,
    ProjectionObjectValueField, ProjectionOccurrenceProvenance, ProjectionOperation,
    ProjectionPartition, ProjectionPlanTemplate, ProjectionProgram, ProjectionProgramError,
    ProjectionProgramId, ProjectionProgramLimits, ProjectionRelationship,
    ProjectionRelationshipEffect, ProjectionRelationshipEffectKind, ProjectionScalarTransform,
    ProjectionTarget, ProjectionValue, ProjectionValueRef, ProjectionValueType,
    ResolvedProjectionKey, ResolvedProjectionMutation, ResolvedProjectionMutationScope,
    ResolvedProjectionPartition, ResolvedProjectionPartitionRef, ResolvedProjectionPlan,
    ResolvedProjectionRelationshipEffect, ResolvedProjectionValue, SourceOrdering,
    MAX_PROJECTION_EXPRESSION_DEPTH, MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
    MAX_PROJECTION_PATH_SEGMENTS,
};

// Event-independent mutations and their binding to events.
pub use mutation::{
    bind_event_apply_mutation, bind_event_to_mutation, body_field_binding, compile_projection,
    delete_by_pk_program_for_model, envelope_binding, inventory_single_model, lower_single_model,
    state_upsert_program_for_model, Mutation, MutationColumn, MutationInput, MutationInputBinding,
    MutationKind, MutationOperation, MutationProgram, MutationStep, ProjectionHandler,
    ProjectionInputSource,
};

/// Spec-shaped authoring: **mutations** + **event-first projections**.
///
/// One or more `on { events, mutation, input }` arms. `input` declares how the
/// occurrence fills mutation inputs (`body` = event body object root;
/// `aggregate_id` = envelope id for delete-by-pk).
///
/// ```ignore
/// projection! {
///     pub const TODOS: ProjectionDescriptor<EventualOnly> = {
///         name: "project_todos",
///         version: 1,
///         epoch: "e2e-ui-todos-v2",
///         model: Todos,
///         on {
///             events: [
///                 TodoCreatedDomainEvent,
///                 TodoCompletedDomainEvent,
///             ],
///             mutation: SaveTodo,
///             input: { todo: body },
///         },
///         on {
///             events: [TodoPurgedDomainEvent],
///             mutation: DeleteTodo,
///             input: { todo_id: aggregate_id },
///         },
///     };
/// }
/// ```
///
/// A `program:` arm remains as an escape hatch for custom partition / multi-binding
/// factories. Prefer the event-first form when partition is unit.
#[macro_export]
macro_rules! projection {
    // Event-first: one or more `on { events, mutation, input }` arms.
    (
        $vis:vis const $id:ident : $desc_ty:ty = {
            name: $name:literal,
            version: $version:expr,
            epoch: $epoch:literal,
            model: $model:ty,
            $(source: $source:ident,)?
            $(
                on {
                    events: [ $($event_ty:ty),+ $(,)? ],
                    mutation: $mutation:path,
                    input: { $input_key:ident : $input_src:ident },
                    $(,)?
                }
            ),+ $(,)?
        } $(;)?
    ) => {
        $vis const $id: $desc_ty = {
            fn __program() -> ::core::result::Result<
                &'static $crate::ProjectionProgram,
                $crate::ProjectionProgramError,
            > {
                static PROGRAM: $crate::projection::lower::ProjectionProgramCell =
                    $crate::projection::lower::ProjectionProgramCell::new();
                PROGRAM.get_or_build(|| {
                    use $crate::domain_event::DomainEventContract;
                    let mut handlers = ::std::vec::Vec::new();
                    $(
                        {
                            let mutation = $mutation().program()?;
                            let input_source = $crate::__projection_input_source!($input_src);
                            $(
                                handlers.push($crate::bind_event_apply_mutation(
                                    &<$event_ty>::descriptor(),
                                    mutation.clone(),
                                    ::core::stringify!($input_key),
                                    input_source,
                                )?);
                            )+
                        }
                    )+
                    let program = $crate::compile_projection(
                        $name,
                        $version,
                        $crate::ProjectionPartition::Unit,
                        handlers,
                    )?;
                    $(let program = program.with_source_ordering(
                        $crate::__projection_source_ordering!($source),
                    )?;)?
                    Ok(program)
                })
            }
            $crate::__projection_descriptor!($name, $version, $epoch, $model, __program)
        };
    };

    // Escape hatch: custom program factory (partition / multi-binding / etc.).
    // Prefer the event-first arms above when partition is unit.
    (
        $vis:vis const $id:ident : $desc_ty:ty = {
            name: $name:literal,
            version: $version:expr,
            epoch: $epoch:literal,
            model: $model:ty,
            program: $program:path $(,)?
        } $(;)?
    ) => {
        $vis const $id: $desc_ty = {
            fn __program() -> ::core::result::Result<
                &'static $crate::ProjectionProgram,
                $crate::ProjectionProgramError,
            > {
                static PROGRAM: $crate::projection::lower::ProjectionProgramCell =
                    $crate::projection::lower::ProjectionProgramCell::new();
                PROGRAM.get_or_build($program)
            }
            $crate::__projection_descriptor!($name, $version, $epoch, $model, __program)
        };
    };
}

/// The descriptor of a single-model projection whose program `$program` returns.
#[doc(hidden)]
#[macro_export]
macro_rules! __projection_descriptor {
    ($name:expr, $version:expr, $epoch:expr, $model:ty, $program:ident) => {{
        fn __lower(
            plan: &$crate::ResolvedProjectionPlan,
        ) -> ::core::result::Result<
            $crate::projection::lower::LoweredProjectionPlan,
            $crate::projection::lower::ProjectionLoweringError,
        > {
            $crate::lower_single_model::<$model>(plan)
        }
        fn __inventory() -> ::core::result::Result<
            $crate::projection::lower::ProjectionOutputInventory,
            $crate::projection::lower::ProjectionLoweringError,
        > {
            $crate::inventory_single_model::<$model>()
        }
        $crate::projection::lower::ProjectionDescriptor::new(
            $name,
            $version,
            $epoch,
            $program,
            __lower,
            __inventory,
        )
    }};
}

/// Map `source: aggregate_snapshot | external_snapshot` for [`projection!`].
#[doc(hidden)]
#[macro_export]
macro_rules! __projection_source_ordering {
    (aggregate_snapshot) => {
        $crate::SourceOrdering::AggregateSnapshot
    };
    (external_snapshot) => {
        $crate::SourceOrdering::ExternalSnapshot
    };
}

/// Map `input: { key: body | aggregate_id }` keywords for [`projection!`].
#[macro_export]
#[doc(hidden)]
macro_rules! __projection_input_source {
    (body) => {
        $crate::ProjectionInputSource::Body
    };
    (aggregate_id) => {
        $crate::ProjectionInputSource::AggregateId
    };
}

pub type SourcedResult<T = ()> = std::result::Result<T, EventRecordError>;

// Re-export repository traits at crate root for convenience
pub use repository::{
    CommitBatch, GetStream, InboxOutcome, InboxReceipt, InboxStore, PreparedEventAppend,
    ReadModelWritePlanStore, RelationalReadModelQueryStore, Repository, RepositoryError,
    SnapshotStore, SnapshotWrite, StreamIdentity, StreamWrite, TransactionalCommit,
};

// Re-export aggregate types at crate root for convenience
pub use aggregate::{hydrate, Aggregate, AggregateBuilder, AggregateRepository};

pub use in_memory_repo::{InMemoryOutboxStore, InMemoryRepository};
#[cfg(feature = "postgres")]
pub use postgres_repo::{PostgresOutboxStore, PostgresRepository};
#[cfg(feature = "sqlite")]
pub use sqlite_repo::{SqliteOutboxStore, SqliteRepository};

// Re-export lock traits and types at crate root for convenience
pub use lock::{
    InMemoryLock, InMemoryLockFuture, InMemoryLockManager, Lock, LockError, LockManager,
};
// Durable SQLx lease-table lock managers (feature-gated like the SQLx repos).
#[cfg(feature = "postgres")]
pub use lock::{PostgresLock, PostgresLockManager};
#[cfg(feature = "sqlite")]
pub use lock::{SqliteLock, SqliteLockManager};

// Coordination primitives: injectable clock, fenced leases, sagas.
pub use clock::{Clock, ManualClock, SystemClock};
#[cfg(feature = "postgres")]
pub use lease::PostgresLeaseStore;
#[cfg(feature = "sqlite")]
pub use lease::SqliteLeaseStore;
pub use lease::{Fence, FencingToken, InMemoryLeaseStore, Lease, LeaseError, LeaseStore};
pub use saga::{
    Saga, SagaAction, SagaCommand, SagaDefinition, SagaError, SagaMessage, SagaOutcome, SagaReply,
    SagaRunner, SagaStatus, SagaStep,
};
pub use schedule::{Capabilities, Capability, Schedule, ScheduleWrite};

// Outbox: commit concerns (aggregate + outbox in one commit).
//
// Low-level row plumbing (`outbox_message_insert_plan`, `outbox_message_row_values`)
// stays reachable under `distributed::outbox::*` and is intentionally NOT re-exported
// at the crate root — it is an adapter-authoring detail, not part of the quick-start API.
pub use outbox::{
    outbox_message_key, outbox_message_schema, AggregateCommit, CommitReceipt, OutboxMessage,
    OutboxMessageStatus, OutboxPublisherConfig, OUTBOX_MESSAGES_TABLE,
};

// Outbox Worker: drain and publish concerns.
//
// Adapter-authoring constants (`SOURCED_METADATA_PREFIX`,
// `DEFAULT_OUTBOX_SOURCE_BATCH`, `DEFAULT_OUTBOX_SOURCE_LEASE`) stay reachable
// under `distributed::outbox_worker::*` and are intentionally NOT re-exported
// at the crate root.
#[cfg(any(feature = "runtime-tokio", test))]
pub use outbox_worker::{
    drain_worker_id, OutboxDrainHandle, OutboxDrainRunner, OutboxPublishMailbox,
    DEFAULT_OUTBOX_HINT_CAPACITY,
};
pub use outbox_worker::{
    BusPublisher, ClaimOutboxMessages, OutboxClaimRef, OutboxDispatchOutcome, OutboxDispatcher,
    OutboxPublishFailureAction, OutboxSource, OutboxStore, ReceivedOutboxMessage, OUTBOX_RETRY,
};

pub use queued_repo::{
    // WithOpts + unlock traits for the queued repository variant.
    GetAllWithOpts,
    GetWithOpts,
    // Queued repository
    Queueable,
    QueuedRepository,
    ReadOpts,
};

// Read models: projections and read-optimized views.
//
// Only the quick-start surface is re-exported at the crate root: the traits
// user models implement (incl. `RelationalReadModelIncludes`, which the
// `ReadModel` derive expands to), the in-memory default store, workspace
// entry points, and the version marker. Physical write-plan builders are
// low-level adapter detail under `distributed::read_model::*` (not projector
// authoring). Load-graph, query, and row-include plumbing stays there too.
pub use read_model::{
    InMemoryReadModelStore, ReadModel, ReadModelChange, ReadModelWorkspaceExt, RelationalReadModel,
    RelationalReadModelIncludes, Versioned,
};

// Neutral table/row primitives: the canonical schema, row, mutation, write-plan,
// and error vocabulary shared by read models and operational tables (outbox,
// inbox/checkpoint, and future operational tables). Read models build on these,
// so they are part of the crate-root surface; SQL rendering helpers stay under
// `distributed::table::*`.
pub use table::{
    ColumnType, DeleteTableRowMutation, ExpectedVersion, ForeignKey, JsonField, JsonShape,
    JsonType, PatchMode, PatchTableRowMutation, PrimaryKey, ReadModelCatalog, RelationshipDef,
    RelationshipKind, RowKey, RowPatch, RowValue, RowValues, RowWriteMode, TableColumn, TableIndex,
    TableKind, TableMigrationArtifact, TableMutation, TableRowMutation, TableSchema,
    TableSchemaBootstrap, TableSchemaRegistry, TableStoreError, TableWritePlan,
    DEFAULT_TABLE_VERSION_COLUMN,
};
pub use trace_context::{
    is_valid_traceparent, TraceContext, CAUSATION_ID, CORRELATION_ID, TRACEPARENT, TRACESTATE,
};

// CommitBuilder: transactional batches of read models, outbox, and aggregates
pub use commit_builder::{
    CommitBuilder, CommitBuilderExt, ReadModelWritePlanCommitExt, StagedCommitBuilder,
};

// Snapshot: state snapshot payloads and rebuildable cache records for hydration
pub use snapshot::{hydrate_from_snapshot, InMemorySnapshotStore, SnapshotRecord, Snapshottable};

// Session convenience re-exports used by GraphQL permission filters.
pub use microsvc::{
    MessageEndpointDescriptor, MetricsEndpointDescriptor, ServiceDescriptor,
    ServiceObservabilityDescriptor, TraceExportMode, TracePropagationMode, TracingDescriptor,
    TransportDescriptor, ROLE_KEY, USER_ID_KEY,
};

// Re-export proc macros. The old event-owning projection proc-macro and
// separately authored `command_effects!` / `command_confirmations!` are gone.
// Use `mutation!` / `mutation_file!` + declarative `projection!` (event→mutation
// mount); commands predict events via `.emits`/`.preview`.
pub use distributed_macros::{
    command, command_input_defaults, mutation, mutation_file, sourced, CommandInput, CommandOutput,
    DomainEvent, DomainState, JsonType, ReadModel, Snapshot,
};
