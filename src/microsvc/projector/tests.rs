use std::error::Error as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::bus::{Bus, FailurePolicy, InMemoryBus, RunOptions};
use crate::bus::{Message, MessageKind};
use crate::graphql::SurfaceProjector;
use crate::microsvc::{CausalProjectorContext, HandlerError, ProjectionRepairHandle, Routes};
use crate::projection_protocol::{
    CompiledProjectionTopology, ProjectionCheckpointProbe, ProjectionGeneration,
    ProjectionInputCursor, ProjectionProtocolStore, ProjectionQuerySnapshotRequest,
};
#[cfg(feature = "graphql")]
use crate::projection_protocol::{ProjectionScopeCodec, ProjectorTopologyId};
use crate::read_model::RelationalReadModel;
use crate::table::{RowKey, RowValue};
use crate::InMemoryRepository;

use crate::microsvc::Service;

const FACT_NAME: &str = "task15.todo_changed";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, crate::domain_event::DomainState)]
#[domain_state(version = 1)]
struct TodoChanged {
    id: String,
    title: String,
}

#[derive(
    Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::read_model::ReadModel,
)]
#[readmodel(table = "task15_primary_views", primary_key = ["id"])]
struct PrimaryView {
    id: String,
    title: String,
}

#[derive(
    Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::read_model::ReadModel,
)]
#[readmodel(table = "task15_secondary_views", primary_key = ["id"])]
struct SecondaryView {
    id: String,
    title: String,
}

#[derive(
    Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::read_model::ReadModel,
)]
#[readmodel(table = "task15_malformed_views", primary_key = ["id"])]
struct MalformedView {
    id: String,
}

#[cfg(feature = "graphql")]
fn generated_modeled_projector_program(
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    modeled_projector_program("title")
}

/// The projector's program, with the row's title taken from the event's
/// `title_from` field.
#[cfg(feature = "graphql")]
fn modeled_projector_program(
    title_from: &'static str,
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    use crate::mutation::{
        body_field_binding, compile_projection, MutationColumn, MutationOperation, MutationProgram,
        ProjectionHandler,
    };
    use crate::projection::MutationKind;
    use crate::projection::{
        ProjectionEventSelector, ProjectionPartition, ProjectionTarget, ProjectionValueType,
    };

    let column = |name: &str| MutationColumn::try_new(name, ProjectionValueType::String, [name]);
    let mutation = MutationProgram::try_new(
        "task11_multi_mutation",
        1,
        vec![
            MutationOperation::try_new(
                "upsert-primary",
                MutationKind::Upsert,
                ProjectionTarget::try_new("PrimaryView", "task15_primary_views")?,
                vec![column("id")?],
                vec![column("id")?, column("title")?],
            )?,
            MutationOperation::try_new(
                "upsert-secondary",
                MutationKind::Upsert,
                ProjectionTarget::try_new("SecondaryView", "task15_secondary_views")?,
                vec![column("id")?],
                vec![column("id")?, column("title")?],
            )?,
        ],
    )?;
    let selector = ProjectionEventSelector::try_from_descriptor(
        &crate::domain_event::DomainEventDescriptor::state::<TodoChanged>("task15.todo_changed", 1),
    )?;
    compile_projection(
        "task11-generated-multi-table",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::try_new(
            "changed",
            selector,
            vec![
                body_field_binding(["id"], ["id"], ProjectionValueType::String)?,
                body_field_binding(["title"], [title_from], ProjectionValueType::String)?,
            ],
            mutation,
        )?],
    )
}

#[cfg(feature = "graphql")]
fn generated_modeled_projector_lower(
    plan: &crate::projection::ResolvedProjectionPlan,
) -> Result<
    crate::projection::lower::LoweredProjectionPlan,
    crate::projection::lower::ProjectionLoweringError,
> {
    use crate::projection::lower::{finish_lowering, lower_model_mutation};
    let mut records = Vec::new();
    for mutation in plan.mutations() {
        match mutation.target().model() {
            "PrimaryView" => records.push(lower_model_mutation::<PrimaryView>(mutation)?),
            "SecondaryView" => records.push(lower_model_mutation::<SecondaryView>(mutation)?),
            other => {
                return Err(crate::projection::lower::ProjectionLoweringError::Table(
                    crate::table::TableStoreError::Metadata(format!("unknown model `{other}`")),
                ));
            }
        }
    }
    finish_lowering(plan, records)
}

#[cfg(feature = "graphql")]
fn generated_modeled_projector_inventory() -> Result<
    crate::projection::lower::ProjectionOutputInventory,
    crate::projection::lower::ProjectionLoweringError,
> {
    use crate::projection::lower::{ProjectionOutputInventory, ProjectionOutputModel};
    Ok(ProjectionOutputInventory::new(
        vec![
            ProjectionOutputModel::of::<PrimaryView>()?,
            ProjectionOutputModel::of::<SecondaryView>()?,
        ],
        Vec::new(),
    ))
}

#[cfg(feature = "graphql")]
const GENERATED_MODELED_PROJECTOR: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::EventualOnly,
> = crate::projection::lower::ProjectionDescriptor::new(
    "task11-generated-multi-table",
    1,
    || {
        static PROGRAM: crate::projection::lower::ProjectionProgramCell =
            crate::projection::lower::ProjectionProgramCell::new();
        PROGRAM.get_or_build(generated_modeled_projector_program)
    },
    generated_modeled_projector_lower,
    generated_modeled_projector_inventory,
);

/// The same projection after a release changed its program: the title is
/// now the event's id.
#[cfg(feature = "graphql")]
const CHANGED_MODELED_PROJECTOR: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::EventualOnly,
> = crate::projection::lower::ProjectionDescriptor::new(
    "task11-generated-multi-table",
    1,
    || {
        static PROGRAM: crate::projection::lower::ProjectionProgramCell =
            crate::projection::lower::ProjectionProgramCell::new();
        PROGRAM.get_or_build(|| modeled_projector_program("id"))
    },
    generated_modeled_projector_lower,
    generated_modeled_projector_inventory,
);

fn primary_projector() -> SurfaceProjector {
    SurfaceProjector::new("task15_a_primary")
        .facts([FACT_NAME])
        .models(["PrimaryView"])
}

fn secondary_projector() -> SurfaceProjector {
    SurfaceProjector::new("task15_b_secondary")
        .facts([FACT_NAME])
        .models(["SecondaryView"])
}

fn fact_message(id: &str, title: &str) -> Message {
    Message::new(
        FACT_NAME,
        MessageKind::Event,
        serde_json::to_vec(&serde_json::json!({ "id": id, "title": title })).unwrap(),
    )
    .with_id(format!("fact-{id}"))
    .with_metadata(crate::trace_context::CAUSATION_ID, format!("command-{id}"))
}

fn row_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.to_string()))])
}

#[cfg(feature = "graphql")]
fn generated_modeled_projector(
    route: crate::projection::placement::ProjectionExecutorRoute,
) -> (SurfaceProjector, ProjectorTopologyId) {
    modeled_projector(GENERATED_MODELED_PROJECTOR, route)
}

#[cfg(feature = "graphql")]
fn modeled_projector(
    descriptor: crate::projection::lower::ProjectionDescriptor<
        crate::projection::lower::EventualOnly,
    >,
    route: crate::projection::placement::ProjectionExecutorRoute,
) -> (SurfaceProjector, ProjectorTopologyId) {
    use crate::graphql::SurfaceModeledProjection;
    use crate::projection::catalog::{ProjectionBindingActivation, ProjectionCatalog};
    use crate::projection::placement::{
        ProjectionBinding, ProjectionBindingState, ProjectionOutput, ProjectionOwner,
        ProjectionPhysicalTopology, ProjectionSourceBinding, PROJECTION_PARTITION_CODEC_VERSION,
    };

    let topology = ProjectorTopologyId::new(1, "task11-generated-runtime", [0x6b; 32]).unwrap();
    let binding = ProjectionBinding::materialize_eventual(
        descriptor.eventual(),
        ProjectionSourceBinding::try_new("task11-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("task11-generated-owner").unwrap(),
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![
            ProjectionOutput::try_new(
                "PrimaryView",
                "task15_primary_views",
                PrimaryView::schema().clone(),
            )
            .unwrap(),
            ProjectionOutput::try_new(
                "SecondaryView",
                "task15_secondary_views",
                SecondaryView::schema().clone(),
            )
            .unwrap(),
        ],
        Vec::new(),
        Some(ProjectionPhysicalTopology::from_protocol(&topology)),
    )
    .unwrap();
    let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(vec![ProjectionBindingActivation::new(
            binding.id(),
            binding.program_id(),
            ProjectionBindingState::Active,
            Some(route),
        )])
        .unwrap();
    let modeled =
        SurfaceModeledProjection::try_from_descriptor(descriptor, &catalog, &active, binding.id())
            .unwrap();
    (
        SurfaceProjector::new("task11-generated-runtime").modeled(modeled),
        topology,
    )
}

#[cfg(feature = "graphql")]
#[test]
#[should_panic(expected = "mount it with `Routes::modeled_projector(...).handle(...)`")]
fn legacy_route_builder_rejects_unit_partition_modeled_projection() {
    let (projector, _) = generated_modeled_projector(
        crate::projection::placement::ProjectionExecutorRoute::local("task11-service").unwrap(),
    );

    let _ = Routes::new()
        .with_read_model_store(InMemoryRepository::new())
        .causal_projector::<TodoChanged>(projector)
        .model::<PrimaryView>()
        .handle(|_context: CausalProjectorContext, _fact: TodoChanged| async move { Ok(()) });
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn explicit_modeled_projector_handler_applies_the_compiled_projection() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let invoked = Arc::new(AtomicBool::new(false));
    let handler_invoked = Arc::clone(&invoked);
    let (projector, topology) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-explicit").unwrap());
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .modeled_projector(projector)
        .handle(move |context, projection| {
            let invoked = Arc::clone(&handler_invoked);
            async move {
                invoked.store(true, Ordering::Release);
                projection
                    .apply(GENERATED_MODELED_PROJECTOR, &context)
                    .await
            }
        });
    let service = Service::new()
        .named("task11-explicit")
        .routes(routes)
        .with_bus(bus.clone());
    bus.publish_message(modeled_fact_message("todo-explicit", "visible handler", 1))
        .await
        .unwrap();

    service.run(RunOptions::until_idle()).await.unwrap();

    assert!(invoked.load(Ordering::Acquire));
    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    let snapshot = repository
        .projection_query_snapshot(&modeled_snapshot_request(
            &topology,
            &ordered,
            "PrimaryView",
            PrimaryView::schema(),
            "todo-explicit",
        ))
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .row
            .as_ref()
            .and_then(|row| row.get_serde::<String>("title").ok())
            .as_deref(),
        Some("visible handler")
    );
}

#[cfg(feature = "graphql")]
fn modeled_fact_message(id: &str, title: &str, event_version: u64) -> Message {
    let mut occurrence = crate::domain_event::DomainEventOccurrence::capture(
        crate::domain_event::DomainEventDescriptor::state::<TodoChanged>(FACT_NAME, event_version),
        crate::domain_event::DomainEventEnvelope {
            aggregate_type: "todo".into(),
            aggregate_id: id.into(),
            aggregate_sequence: 1,
            publication_ordinal: 0,
            occurred_at: std::time::UNIX_EPOCH + std::time::Duration::from_secs(1),
            metadata: std::collections::BTreeMap::new(),
        },
        &TodoChanged {
            id: id.into(),
            title: title.into(),
        },
    )
    .unwrap();
    let causation_id = format!("command-{id}");
    occurrence.overwrite_causation_id(&causation_id);
    let occurrence_id = occurrence.id().to_owned();
    Message::new(
        FACT_NAME,
        MessageKind::Event,
        occurrence.canonical_bytes().unwrap(),
    )
    .with_id(occurrence_id)
    .with_metadata(crate::trace_context::CAUSATION_ID, format!("command-{id}"))
}

#[cfg(feature = "graphql")]
fn modeled_snapshot_request(
    topology: &ProjectorTopologyId,
    ordered: &crate::bus::OrderedDelivery,
    model: &str,
    schema: &'static crate::table::TableSchema,
    id: &str,
) -> ProjectionQuerySnapshotRequest {
    let codec = ProjectionScopeCodec::with_models(
        topology.clone(),
        [
            ("PrimaryView", PrimaryView::schema()),
            ("SecondaryView", SecondaryView::schema()),
        ],
    )
    .unwrap();
    let partition = codec.encode_partition(None).unwrap();
    ProjectionQuerySnapshotRequest::new(
        &codec,
        None,
        model,
        RowKey::new([("id", RowValue::String(id.into()))]),
        vec![ProjectionCheckpointProbe::new(
            topology.clone(),
            partition,
            ordered.source().clone(),
            ordered.epoch().clone(),
            ProjectionGeneration::initial(),
        )],
    )
    .unwrap_or_else(|error| {
        panic!(
            "modeled snapshot request for {} / {} failed: {error}",
            schema.model_name, model
        )
    })
}

#[cfg(feature = "graphql")]
#[test]
fn generated_catalog_mount_is_fluent_and_remote_routes_do_not_subscribe() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let (local, _) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-service").unwrap());
    let local_routes = Routes::new()
        .with_read_model_store(repository.clone())
        .consume_projection(local);
    assert_eq!(
        Service::new()
            .routes(local_routes)
            .subscription_plan()
            .events,
        vec![FACT_NAME]
    );

    let (remote, _) =
        generated_modeled_projector(ProjectionExecutorRoute::remote("task11-remote").unwrap());
    let remote_routes = Routes::new()
        .with_read_model_store(repository)
        .consume_projection(remote);
    assert!(Service::new()
        .routes(remote_routes)
        .subscription_plan()
        .events
        .is_empty());
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn the_same_catalog_program_executes_in_a_separate_projector_service() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let (application_route, _) =
        generated_modeled_projector(ProjectionExecutorRoute::remote("task11-remote").unwrap());
    let application = Service::new().routes(
        Routes::new()
            .with_read_model_store(repository.clone())
            .consume_projection(application_route),
    );
    assert!(
        application.subscription_plan().events.is_empty(),
        "the application records the remote binding without consuming its event"
    );

    let (projector_route, topology) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-remote").unwrap());
    let projector_service = Service::new()
        .named("task11-remote")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .consume_projection(projector_route),
        )
        .with_bus(bus.clone());
    assert_eq!(
        projector_service.subscription_plan().events,
        vec![FACT_NAME]
    );

    bus.publish_message(modeled_fact_message("todo-remote", "remote catalog", 1))
        .await
        .unwrap();
    projector_service
        .run(RunOptions::until_idle())
        .await
        .unwrap();

    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    for (model, schema) in [
        ("PrimaryView", PrimaryView::schema()),
        ("SecondaryView", SecondaryView::schema()),
    ] {
        let snapshot = repository
            .projection_query_snapshot(&modeled_snapshot_request(
                &topology,
                &ordered,
                model,
                schema,
                "todo-remote",
            ))
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .row
                .as_ref()
                .and_then(|row| row.get_serde::<String>("title").ok())
                .as_deref(),
            Some("remote catalog")
        );
        assert_eq!(
            snapshot
                .record
                .as_ref()
                .map(|record| record.revision.revision()),
            Some(1)
        );
        assert_eq!(
            snapshot.checkpoints[0]
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.input().position()),
            Some(ordered.position())
        );
    }
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn generated_local_mount_rejects_missing_or_mismatched_service_identity_before_bootstrap() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let (unnamed, _) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-service").unwrap());
    let unnamed = Service::new().routes(
        Routes::new()
            .with_read_model_store(InMemoryRepository::new())
            .consume_projection(unnamed),
    );
    assert!(matches!(
        unnamed.bootstrap_projectors().await,
        Err(HandlerError::Projection(
            crate::projection_protocol::ProjectionProtocolError::InvalidBatch(_)
        ))
    ));

    let (mismatched, _) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-service").unwrap());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Service::new()
            .routes(
                Routes::new()
                    .with_read_model_store(InMemoryRepository::new())
                    .consume_projection(mismatched),
            )
            .named("different-service")
    }));
    assert!(result.is_err());
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn generated_mount_applies_both_modeled_rows_through_one_causal_commit() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let (projector, topology) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-service").unwrap());
    let service = Service::new()
        .named("task11-service")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .consume_projection(projector),
        )
        .with_bus(bus.clone());
    bus.publish_message(modeled_fact_message("todo-modeled", "catalog runtime", 1))
        .await
        .unwrap();

    service.run(RunOptions::until_idle()).await.unwrap();

    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    let primary = repository
        .projection_query_snapshot(&modeled_snapshot_request(
            &topology,
            &ordered,
            "PrimaryView",
            PrimaryView::schema(),
            "todo-modeled",
        ))
        .await
        .unwrap();
    let secondary = repository
        .projection_query_snapshot(&modeled_snapshot_request(
            &topology,
            &ordered,
            "SecondaryView",
            SecondaryView::schema(),
            "todo-modeled",
        ))
        .await
        .unwrap();
    for snapshot in [primary, secondary] {
        assert_eq!(
            snapshot
                .row
                .as_ref()
                .and_then(|row| row.get_serde::<String>("title").ok())
                .as_deref(),
            Some("catalog runtime")
        );
        assert_eq!(
            snapshot
                .record
                .as_ref()
                .map(|record| record.revision.revision()),
            Some(1)
        );
        assert_eq!(
            snapshot.checkpoints[0]
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.input().position()),
            Some(ordered.position())
        );
    }
}

/// The stored change epoch of the modeled projector's one partition, read
/// from the head of its change log.
#[cfg(feature = "graphql")]
async fn stored_change_epoch(
    repository: &InMemoryRepository,
    topology: &ProjectorTopologyId,
) -> crate::projection_protocol::ProjectionEpoch {
    use crate::projection_protocol::ProjectionChangeRead;

    let partition = ProjectionScopeCodec::new(topology.clone())
        .encode_partition(None)
        .unwrap();
    match repository
        .projection_changes(topology, &partition, None, 100)
        .await
        .unwrap()
    {
        ProjectionChangeRead::Changes {
            head: Some(head), ..
        } => head.epoch().clone(),
        other => panic!("the partition has a readable change log: {other:?}"),
    }
}

#[cfg(feature = "graphql")]
async fn modeled_title(
    repository: &InMemoryRepository,
    topology: &ProjectorTopologyId,
    bus: &InMemoryBus,
    id: &str,
) -> Option<String> {
    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    repository
        .projection_query_snapshot(&modeled_snapshot_request(
            topology,
            &ordered,
            "PrimaryView",
            PrimaryView::schema(),
            id,
        ))
        .await
        .unwrap()
        .row
        .and_then(|row| row.get_serde::<String>("title").ok())
}

/// A store an earlier release wrote: its partition was created with the
/// epoch the application declared then. The service starts on it and
/// delivers into the same change log, with nothing reset or refused.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn a_partition_created_with_a_declared_epoch_keeps_it_when_the_service_starts() {
    use crate::projection::placement::ProjectionExecutorRoute;
    use crate::projection_protocol::ProjectionEpoch;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let (projector, topology) = modeled_projector(
        GENERATED_MODELED_PROJECTOR,
        ProjectionExecutorRoute::local("task11-service").unwrap(),
    );
    let declared = ProjectionEpoch::new("todos-v1").unwrap();
    repository.insert_projection_partition(
        &topology,
        &ProjectionScopeCodec::new(topology.clone())
            .encode_partition(None)
            .unwrap(),
        &declared,
    );
    let service = Service::new()
        .named("task11-service")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .consume_projection(projector),
        )
        .with_bus(bus.clone());
    bus.publish_message(modeled_fact_message("todo-kept", "delivered", 1))
        .await
        .unwrap();

    service.run(RunOptions::until_idle()).await.unwrap();

    assert_eq!(
        modeled_title(&repository, &topology, &bus, "todo-kept")
            .await
            .as_deref(),
        Some("delivered")
    );
    assert_eq!(stored_change_epoch(&repository, &topology).await, declared);
}

/// During a rollout two nodes of one service run different programs of the
/// same projection. Both write the one partition: each event is projected by
/// the program of the node that received it, into one change log.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn nodes_on_different_projection_programs_write_the_same_partition() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let node = |descriptor| {
        let (projector, topology) = modeled_projector(
            descriptor,
            ProjectionExecutorRoute::local("task11-service").unwrap(),
        );
        let service = Service::new()
            .named("task11-service")
            .routes(
                Routes::new()
                    .with_read_model_store(repository.clone())
                    .consume_projection(projector),
            )
            .with_bus(bus.clone());
        (service, topology)
    };
    let (previous, topology) = node(GENERATED_MODELED_PROJECTOR);
    let (next, next_topology) = node(CHANGED_MODELED_PROJECTOR);
    assert_eq!(topology, next_topology);
    assert_ne!(
        GENERATED_MODELED_PROJECTOR.program_id().unwrap(),
        CHANGED_MODELED_PROJECTOR.program_id().unwrap()
    );

    bus.publish_message(modeled_fact_message("todo-1", "first", 1))
        .await
        .unwrap();
    previous.run(RunOptions::until_idle()).await.unwrap();
    let epoch = stored_change_epoch(&repository, &topology).await;

    bus.publish_message(modeled_fact_message("todo-2", "second", 1))
        .await
        .unwrap();
    next.run(RunOptions::until_idle()).await.unwrap();
    bus.publish_message(modeled_fact_message("todo-3", "third", 1))
        .await
        .unwrap();
    let (previous, _) = node(GENERATED_MODELED_PROJECTOR);
    previous.run(RunOptions::until_idle()).await.unwrap();

    let title = |id| modeled_title(&repository, &topology, &bus, id);
    assert_eq!(title("todo-1").await.as_deref(), Some("first"));
    assert_eq!(title("todo-2").await.as_deref(), Some("todo-2"));
    assert_eq!(title("todo-3").await.as_deref(), Some("third"));
    assert_eq!(stored_change_epoch(&repository, &topology).await, epoch);
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn same_name_different_version_is_an_empty_unit_checkpoint_not_a_failure() {
    use crate::projection::placement::ProjectionExecutorRoute;

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let (projector, topology) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-service").unwrap());
    let service = Service::new()
        .named("task11-service")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .consume_projection(projector),
        )
        .with_bus(bus.clone());
    bus.publish_message(modeled_fact_message("todo-v2", "not selected", 2))
        .await
        .unwrap();

    service.run(RunOptions::until_idle()).await.unwrap();

    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    let snapshot = repository
        .projection_query_snapshot(&modeled_snapshot_request(
            &topology,
            &ordered,
            "PrimaryView",
            PrimaryView::schema(),
            "todo-v2",
        ))
        .await
        .unwrap();
    assert!(snapshot.row.is_none());
    let checkpoint = snapshot.checkpoints[0]
        .checkpoint
        .as_ref()
        .expect("unit projection records an empty checkpoint for a skipped selector");
    assert!(checkpoint.is_gap_free());
    assert_eq!(checkpoint.input().position(), ordered.position());
}

#[tokio::test]
async fn public_builder_fans_one_fact_out_and_replays_partial_success_exactly() {
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let fail_secondary_once = Arc::new(AtomicBool::new(true));

    let primary = primary_projector();
    let secondary = secondary_projector();
    let primary_calls = Arc::clone(&calls);
    let secondary_calls = Arc::clone(&calls);
    let secondary_failure = Arc::clone(&fail_secondary_once);
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<TodoChanged>(primary.clone())
        .model::<PrimaryView>()
        .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
            let calls = Arc::clone(&primary_calls);
            async move {
                {
                    let mut calls = calls.lock().unwrap();
                    if calls.contains(&"primary") {
                        return Err(HandlerError::Rejected(
                            "an applied projector must not be reinvoked on sibling retry".into(),
                        ));
                    }
                    calls.push("primary");
                }
                context
                    .project(&PrimaryView {
                        id: fact.id,
                        title: fact.title,
                    })
                    .await?;
                Ok(())
            }
        })
        .causal_projector::<TodoChanged>(secondary)
        .model::<SecondaryView>()
        .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
            let calls = Arc::clone(&secondary_calls);
            let fail = Arc::clone(&secondary_failure);
            async move {
                calls.lock().unwrap().push("secondary");
                if fail.swap(false, Ordering::SeqCst) {
                    return Err(HandlerError::Other(
                        "injected transient projector failure".into(),
                    ));
                }
                context
                    .project(&SecondaryView {
                        id: fact.id,
                        title: fact.title,
                    })
                    .await?;
                Ok(())
            }
        });
    let service = Service::new().routes(routes).with_bus(bus.clone());
    bus.publish_message(fact_message("todo-1", "causal cache"))
        .await
        .unwrap();

    service.run(RunOptions::until_idle()).await.unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        vec!["primary", "secondary", "secondary"],
        "an applied sibling is skipped while only the failed projector retries"
    );

    let compiled = CompiledProjectionTopology::compile(
        &primary.name,
        &primary.facts,
        &primary.models,
        &primary.partition,
        [PrimaryView::schema()],
    )
    .unwrap();
    let codec = compiled.codec();
    let partition = codec.encode_partition(None).unwrap();
    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    let cursor = ProjectionInputCursor::new(
        compiled.topology().clone(),
        partition.clone(),
        ordered.source().clone(),
        ordered.epoch().clone(),
        ordered.position(),
    )
    .unwrap();
    let snapshot = repository
        .projection_query_snapshot(
            &ProjectionQuerySnapshotRequest::new(
                &codec,
                None,
                "PrimaryView",
                row_key("todo-1"),
                vec![ProjectionCheckpointProbe::new(
                    compiled.topology().clone(),
                    partition,
                    ordered.source().clone(),
                    ordered.epoch().clone(),
                    ProjectionGeneration::initial(),
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let row = snapshot.row.expect("projected physical row");
    assert_eq!(row.get_serde::<String>("title").unwrap(), "causal cache");
    let record = snapshot.record.expect("projection record revision");
    assert_eq!(record.revision.incarnation(), 1);
    assert_eq!(record.revision.revision(), 1);
    let checkpoint = snapshot.checkpoints[0]
        .checkpoint
        .as_ref()
        .expect("source checkpoint");
    assert_eq!(checkpoint.input(), &cursor);
    assert!(checkpoint.is_gap_free());
}

#[tokio::test]
async fn service_fans_same_fact_out_across_projector_only_route_bundles() {
    static INFERRED_PRIMARY_CALLS: AtomicUsize = AtomicUsize::new(0);

    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let secondary_calls = Arc::new(AtomicUsize::new(0));
    INFERRED_PRIMARY_CALLS.store(0, Ordering::SeqCst);

    let primary_routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<TodoChanged>(primary_projector())
        .model::<PrimaryView>()
        .handle(|context, fact: TodoChanged| async move {
            INFERRED_PRIMARY_CALLS.fetch_add(1, Ordering::SeqCst);
            context
                .project(&PrimaryView {
                    id: fact.id,
                    title: fact.title,
                })
                .await?;
            Ok(())
        });
    let secondary_seen = Arc::clone(&secondary_calls);
    let secondary_routes = Routes::new()
        .with_read_model_store(repository)
        .causal_projector::<TodoChanged>(secondary_projector())
        .model::<SecondaryView>()
        .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
            let seen = Arc::clone(&secondary_seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                context
                    .project(&SecondaryView {
                        id: fact.id,
                        title: fact.title,
                    })
                    .await?;
                Ok(())
            }
        });
    let service = Service::new()
        .routes(primary_routes)
        .routes(secondary_routes)
        .with_bus(bus.clone());
    assert_eq!(
        service.subscription_plan().events,
        vec![FACT_NAME.to_string()]
    );
    bus.publish_message(fact_message("todo-2", "cross bundle"))
        .await
        .unwrap();
    service.run(RunOptions::until_idle()).await.unwrap();

    assert_eq!(INFERRED_PRIMARY_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(secondary_calls.load(Ordering::SeqCst), 1);
}

/// Two projectors of one fact in a named service. With `primary_rejects` the
/// primary projector's handler refuses every input it is given, the way a
/// release with a deterministic projection bug does.
fn two_projector_service(
    repository: &InMemoryRepository,
    primary_rejects: bool,
    applied: &Arc<Mutex<Vec<(&'static str, String)>>>,
) -> Service {
    let primary_applied = Arc::clone(applied);
    let secondary_applied = Arc::clone(applied);
    Service::new().named("task15-isolation").routes(
        Routes::new()
            .with_read_model_store(repository.clone())
            .causal_projector::<TodoChanged>(primary_projector())
            .model::<PrimaryView>()
            .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
                let applied = Arc::clone(&primary_applied);
                async move {
                    applied.lock().unwrap().push(("primary", fact.id.clone()));
                    if primary_rejects {
                        return Err(HandlerError::rejected("the primary view cannot hold this"));
                    }
                    context
                        .project(&PrimaryView {
                            id: fact.id,
                            title: fact.title,
                        })
                        .await?;
                    Ok(())
                }
            })
            .causal_projector::<TodoChanged>(secondary_projector())
            .model::<SecondaryView>()
            .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
                let applied = Arc::clone(&secondary_applied);
                async move {
                    applied.lock().unwrap().push(("secondary", fact.id.clone()));
                    context
                        .project(&SecondaryView {
                            id: fact.id,
                            title: fact.title,
                        })
                        .await?;
                    Ok(())
                }
            }),
    )
}

/// The 2026-11-25 halt: a rollout leaves one projection unable to accept its
/// inputs. That projector must stop alone with a recorded failure, and resume
/// from its failed input once the release is corrected and the failure
/// repaired.
#[tokio::test]
async fn a_projector_that_rejects_its_input_stops_alone_and_resumes_after_repair() {
    rejecting_projector_stops_alone_and_resumes_after_repair(InMemoryBus::new()).await;
}

/// The same on a durable bus whose group is set explicitly, as an
/// application does: the projectors still consume as separate groups.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_rejecting_projector_stops_alone_on_a_sql_bus_with_an_explicit_group() {
    use sqlx::Row as _;

    let directory =
        std::env::temp_dir().join(format!("projector-isolation-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&directory).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(directory.join("bus.sqlite"))
                .create_if_missing(true),
        )
        .await
        .unwrap();
    let bus = crate::bus::SqliteBus::new(pool.clone()).group("isolation");
    bus.ensure_tables().await.unwrap();

    rejecting_projector_stops_alone_and_resumes_after_repair(bus).await;

    let mut consumers: Vec<String> = sqlx::query("SELECT consumer FROM bus_offset")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get("consumer"))
        .collect();
    consumers.sort();
    assert_eq!(
        consumers,
        ["isolation-task15_a_primary", "isolation-task15_b_secondary"],
        "one cursor per projector; the service has no event handler of its own"
    );
}

async fn rejecting_projector_stops_alone_and_resumes_after_repair<B>(bus: B)
where
    B: Bus + crate::bus::BusConsumer + Clone + 'static,
{
    let repository = InMemoryRepository::new();
    let applied = Arc::new(Mutex::new(Vec::new()));
    let take = |applied: &Arc<Mutex<Vec<(&'static str, String)>>>| {
        std::mem::take(&mut *applied.lock().unwrap())
    };
    let ids = |projector: &str, applied: &[(&'static str, String)]| -> Vec<String> {
        applied
            .iter()
            .filter(|(name, _)| *name == projector)
            .map(|(_, id)| id.clone())
            .collect()
    };

    bus.publish_message(fact_message("a", "before the rollout"))
        .await
        .unwrap();
    two_projector_service(&repository, false, &applied)
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .unwrap();
    let before = take(&applied);
    assert_eq!(ids("primary", &before), ["a"]);
    assert_eq!(ids("secondary", &before), ["a"]);

    // The rollout: the primary projector now rejects its inputs.
    for id in ["b", "c"] {
        bus.publish_message(fact_message(id, "after the rollout"))
            .await
            .unwrap();
    }
    let stopped = two_projector_service(&repository, true, &applied)
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .expect_err("the rejecting projector reports its terminal stop");
    assert!(stopped.is_permanent() && stopped.should_retain_and_stop());
    let handle = repair_handle(&stopped);
    assert!(
        matches!(
            stopped
                .source()
                .and_then(|source| source.downcast_ref::<HandlerError>()),
            Some(HandlerError::ProjectionTerminalRecorded { projection, .. })
                if projection == "task15_a_primary"
        ),
        "the failure is recorded and names its projection: {stopped}"
    );
    assert!(
        stopped.to_string().contains("task15_a_primary"),
        "{stopped}"
    );
    let during = take(&applied);
    assert_eq!(
        ids("secondary", &during),
        ["b", "c"],
        "the other projector keeps delivering, in order"
    );
    assert_eq!(
        ids("primary", &during),
        ["b"],
        "no later input passes the failed one"
    );

    // A restart changes nothing: the stop is durable and still local.
    bus.publish_message(fact_message("d", "while stopped"))
        .await
        .unwrap();
    let still_stopped = two_projector_service(&repository, true, &applied)
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .expect_err("the stop survives a restart");
    assert_eq!(repair_handle(&still_stopped), handle);
    // A host learns of the stop from the store, without the run's error.
    let stopped_service = two_projector_service(&repository, true, &applied);
    assert_eq!(
        stopped_service.stopped_projections().await.unwrap(),
        [crate::microsvc::StoppedProjection {
            projection: "task15_a_primary".into(),
            repair: handle.clone(),
        }]
    );
    let while_stopped = take(&applied);
    assert_eq!(ids("secondary", &while_stopped), ["d"]);
    assert!(ids("primary", &while_stopped).is_empty());

    // The release is corrected and the operator repairs the failure.
    let repaired = two_projector_service(&repository, false, &applied);
    assert_eq!(repaired.repair_projection(&handle).await.unwrap().get(), 2);
    assert!(repaired.stopped_projections().await.unwrap().is_empty());
    repaired
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .unwrap();
    let after = take(&applied);
    assert_eq!(
        ids("primary", &after),
        ["b", "c", "d"],
        "the repaired projector resumes from its failed input"
    );
    assert!(ids("secondary", &after).is_empty());
}

/// A long-running service does not return when a projector stops. A host
/// still learns which projector stopped, and gets its repair handle, from
/// the store, while the other projector keeps running.
#[tokio::test]
async fn a_long_running_host_can_ask_which_projectors_are_stopped() {
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let applied = Arc::new(Mutex::new(Vec::new()));
    let secondary = |applied: &Arc<Mutex<Vec<(&'static str, String)>>>| -> Vec<String> {
        applied
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| *name == "secondary")
            .map(|(_, id)| id.clone())
            .collect()
    };

    bus.publish_message(fact_message("a", "before the rollout"))
        .await
        .unwrap();
    two_projector_service(&repository, false, &applied)
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .unwrap();

    let shutdown = crate::bus::Shutdown::new();
    let running = tokio::spawn(
        two_projector_service(&repository, true, &applied)
            .with_bus(bus.clone())
            .run(RunOptions::until_shutdown(shutdown.clone())),
    );
    // The host's own service over the same stores, as its API process has.
    let host = two_projector_service(&repository, true, &applied);
    assert!(host.stopped_projections().await.unwrap().is_empty());

    bus.publish_message(fact_message("b", "after the rollout"))
        .await
        .unwrap();
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let stopped = host.stopped_projections().await.unwrap();
            if !stopped.is_empty() {
                break stopped;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the stop is visible while the service runs");
    assert_eq!(stopped.len(), 1);
    assert_eq!(stopped[0].projection, "task15_a_primary");
    assert!(!running.is_finished(), "the service keeps running");

    bus.publish_message(fact_message("c", "while stopped"))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while secondary(&applied) != ["a", "b", "c"] {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the other projector keeps delivering");

    shutdown.trigger();
    let error = running
        .await
        .unwrap()
        .expect_err("the run reports the stop when it ends");
    assert_eq!(repair_handle(&error), stopped[0].repair);
}

/// An event handler that retries until a projector has projected the same
/// event. The in-memory bus hands a failed delivery straight back, so the
/// handler's consumer must still let the projector's consumer run.
#[tokio::test]
async fn a_retrying_handler_does_not_starve_the_projector_it_waits_for() {
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let projected = Arc::new(AtomicBool::new(false));
    let retries = Arc::new(AtomicUsize::new(0));
    let (mark_projected, wait_for_projection, count_retries) = (
        Arc::clone(&projected),
        Arc::clone(&projected),
        Arc::clone(&retries),
    );
    let service = Service::new()
        .named("task15-starvation")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .causal_projector::<TodoChanged>(primary_projector())
                .model::<PrimaryView>()
                .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
                    let projected = Arc::clone(&mark_projected);
                    async move {
                        context
                            .project(&PrimaryView {
                                id: fact.id,
                                title: fact.title,
                            })
                            .await?;
                        projected.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                }),
        )
        .routes(Routes::new().with_dependencies(()).event(FACT_NAME).handle(
            move |_: &crate::microsvc::Context<()>| {
                let projected = Arc::clone(&wait_for_projection);
                let retries = Arc::clone(&count_retries);
                async move {
                    if projected.load(Ordering::SeqCst) {
                        return Ok(serde_json::json!({}));
                    }
                    retries.fetch_add(1, Ordering::SeqCst);
                    Err(HandlerError::Other("the row is not projected yet".into()))
                }
            },
        ))
        .with_bus(bus.clone());

    bus.publish_message(fact_message("a", "awaited"))
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        service.run(RunOptions::until_idle()),
    )
    .await
    .expect("the retrying handler let the projector run")
    .unwrap();

    assert!(projected.load(Ordering::SeqCst));
    assert!(
        retries.load(Ordering::SeqCst) > 0,
        "the handler ran before the projection and retried"
    );
}

/// A handler publishes the fact a projector consumes, after that projector's
/// consumer has already found nothing to do. One drain still projects it.
#[tokio::test]
async fn one_drain_projects_what_a_handler_published_during_it() {
    const TRIGGER: &str = "task15.trigger";
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let projected = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&projected);
    let publisher = bus.clone();
    let service = Service::new()
        .named("task15-drain")
        .routes(
            Routes::new()
                .with_read_model_store(repository.clone())
                .causal_projector::<TodoChanged>(primary_projector())
                .model::<PrimaryView>()
                .handle(move |context: CausalProjectorContext, fact: TodoChanged| {
                    let record = Arc::clone(&record);
                    async move {
                        context
                            .project(&PrimaryView {
                                id: fact.id.clone(),
                                title: fact.title,
                            })
                            .await?;
                        record.lock().unwrap().push(fact.id);
                        Ok(())
                    }
                }),
        )
        .routes(Routes::new().with_dependencies(()).event(TRIGGER).handle(
            move |_: &crate::microsvc::Context<()>| {
                let publisher = publisher.clone();
                async move {
                    // Long enough for the projector's consumer to go idle.
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                    publisher
                        .publish_message(fact_message("late", "published by a handler"))
                        .await
                        .map_err(|error| HandlerError::Other(Box::new(error)))?;
                    Ok(serde_json::json!({}))
                }
            },
        ))
        .with_bus(bus.clone());

    bus.publish_message(Message::new(TRIGGER, MessageKind::Event, b"{}".to_vec()).with_id("t-1"))
        .await
        .unwrap();
    service.run(RunOptions::until_idle()).await.unwrap();

    assert_eq!(*projected.lock().unwrap(), ["late"]);
}

fn malformed_service(repository: InMemoryRepository, invoked: Arc<AtomicUsize>) -> Service {
    let projector = SurfaceProjector::new("task15_malformed")
        .facts(["task15.malformed"])
        .models(["MalformedView"])
        .partition_by(["tenant"]);
    Service::new().routes(
        Routes::new()
            .with_read_model_store(repository)
            .causal_projector::<TodoChanged>(projector)
            .model::<MalformedView>()
            .handle(
                move |_context: CausalProjectorContext, _fact: TodoChanged| {
                    let invoked = Arc::clone(&invoked);
                    async move {
                        invoked.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            ),
    )
}

fn repair_handle(error: &crate::bus::TransportError) -> ProjectionRepairHandle {
    error
        .source()
        .and_then(|source| source.downcast_ref::<HandlerError>())
        .and_then(HandlerError::projection_repair_handle)
        .cloned()
        .expect("terminal transport error carries an operator repair handle")
}

#[tokio::test]
async fn malformed_ingress_emits_safe_handle_and_repair_restart_stays_terminal() {
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let invoked = Arc::new(AtomicUsize::new(0));
    let malformed = Message::new(
        "task15.malformed",
        MessageKind::Event,
        b"{tenant-secret".to_vec(),
    )
    .with_id("malformed-1")
    .with_metadata(crate::trace_context::CAUSATION_ID, "command-malformed");
    bus.publish_message(malformed).await.unwrap();

    let first = malformed_service(repository.clone(), Arc::clone(&invoked))
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .expect_err("malformed ingress must retain its exact position and stop");
    let first_handle = repair_handle(&first);
    let token = first_handle.to_string();
    assert!(!token.contains("tenant-secret"));
    assert_eq!(
        token.parse::<ProjectionRepairHandle>().unwrap(),
        first_handle
    );
    assert_eq!(
        serde_json::from_str::<ProjectionRepairHandle>(
            &serde_json::to_string(&first_handle).unwrap()
        )
        .unwrap(),
        first_handle
    );

    let repaired = malformed_service(repository, Arc::clone(&invoked));
    assert_eq!(
        repaired
            .repair_projection(&first_handle)
            .await
            .unwrap()
            .get(),
        2
    );
    let second = repaired
        .with_bus(bus)
        .run(RunOptions::until_idle())
        .await
        .expect_err("unchanged malformed bytes must stop the repaired generation again");
    let second_handle = repair_handle(&second);
    assert_ne!(second_handle, first_handle);
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
}

#[test]
fn repair_handle_parser_rejects_noncanonical_or_non_hash_tokens() {
    for token in [
        "",
        "distributed-repair-v2:abcd",
        "distributed-repair-v1:abcd",
        "distributed-repair-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        assert!(token.parse::<ProjectionRepairHandle>().is_err(), "{token}");
    }
}

#[tokio::test]
async fn unrecorded_permanent_projector_failure_retains_and_stops_under_drop_policies() {
    let unqualified = || {
        let message = Message::new(
            FACT_NAME,
            MessageKind::Event,
            br#"{"id":"secret-tenant","title":"must not cross"}"#.to_vec(),
        );
        [
            // No stable message ID.
            message.clone(),
            // A message ID but no causation ID.
            message.with_id("fact-without-causation"),
        ]
    };
    for (policy, message) in [FailurePolicy::DeadLetter, FailurePolicy::LogAndAck]
        .into_iter()
        .flat_map(|policy| unqualified().map(|message| (policy, message)))
    {
        let repository = InMemoryRepository::new();
        let bus = InMemoryBus::new();
        let invoked = Arc::new(AtomicUsize::new(0));
        let handler_invoked = Arc::clone(&invoked);
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_read_model_store(repository)
                    .causal_projector::<TodoChanged>(primary_projector())
                    .model::<PrimaryView>()
                    .handle(move |_context, _fact| {
                        let invoked = Arc::clone(&handler_invoked);
                        async move {
                            invoked.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                    }),
            )
            .with_bus(bus.clone());
        bus.publish_message(message).await.unwrap();

        let error = service
            .run(RunOptions::until_idle().with_failure_policy(policy))
            .await
            .expect_err("unrecorded permanent projector failure must stop the runner");
        assert!(error.is_permanent());
        assert!(error.should_retain_and_stop());
        let halted = error
            .source()
            .and_then(|source| source.downcast_ref::<HandlerError>());
        assert!(
            matches!(halted, Some(HandlerError::ProjectionDeliveryHalted { .. })),
            "projector-only dispatch must erase the unrecorded internal failure"
        );
        assert!(
            matches!(
                halted
                    .and_then(|halted| halted.source())
                    .and_then(|source| source.downcast_ref::<HandlerError>()),
                Some(HandlerError::UnqualifiedProjectionDelivery(_))
            ),
            "operator diagnostics retain the original failure only as an error source"
        );
        assert!(!error.to_string().contains("secret-tenant"));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
    }
}

/// Input the projector cannot read is a deterministic failure: it is
/// recorded at its exact position and stops the projector before its
/// handler runs, without echoing the payload.
#[tokio::test]
async fn an_unreadable_input_stops_its_projector_with_a_recorded_failure_before_the_handler() {
    struct Case {
        name: &'static str,
        partition_by_tenant: bool,
        payload: &'static [u8],
    }
    let cases = [
        Case {
            name: "bytes that are not JSON",
            partition_by_tenant: false,
            payload: b"{secret-tenant",
        },
        Case {
            name: "JSON that is not the projector's input type",
            partition_by_tenant: false,
            payload: br#"{"id":7,"title":"secret-tenant"}"#,
        },
        Case {
            name: "JSON without its partition key",
            partition_by_tenant: true,
            payload: br#"{"id":"todo-1","title":"secret-tenant"}"#,
        },
    ];
    for case in cases {
        let invoked = Arc::new(AtomicUsize::new(0));
        let handler_invoked = Arc::clone(&invoked);
        let projector = if case.partition_by_tenant {
            primary_projector().partition_by(["tenant"])
        } else {
            primary_projector()
        };
        let bus = InMemoryBus::new();
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_read_model_store(InMemoryRepository::new())
                    .causal_projector::<TodoChanged>(projector)
                    .model::<PrimaryView>()
                    .handle(move |_context, _fact| {
                        let invoked = Arc::clone(&handler_invoked);
                        async move {
                            invoked.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                    }),
            )
            .with_bus(bus.clone());
        bus.publish_message(
            Message::new(FACT_NAME, MessageKind::Event, case.payload.to_vec())
                .with_id("unreadable-1")
                .with_metadata(crate::trace_context::CAUSATION_ID, "command-unreadable"),
        )
        .await
        .unwrap();

        let stopped = service
            .run(RunOptions::until_idle())
            .await
            .expect_err(case.name);
        assert!(
            stopped.is_permanent() && stopped.should_retain_and_stop(),
            "{}",
            case.name
        );
        assert!(
            matches!(
                stopped
                    .source()
                    .and_then(|source| source.downcast_ref::<HandlerError>()),
                Some(HandlerError::ProjectionTerminalRecorded { projection, .. })
                    if projection == "task15_a_primary"
            ),
            "{}: {stopped}",
            case.name
        );
        let _ = repair_handle(&stopped);
        assert!(
            !stopped.to_string().contains("secret-tenant"),
            "{}",
            case.name
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{}", case.name);
    }
}

#[cfg(feature = "graphql")]
fn without_causation(mut message: Message) -> Message {
    message
        .metadata
        .retain(|(key, _)| key != crate::trace_context::CAUSATION_ID);
    message
}

/// A modeled projector service named `task11-explicit` whose handler applies
/// its projection only when `applies`, counting its invocations.
#[cfg(feature = "graphql")]
fn explicit_modeled_service(
    repository: &InMemoryRepository,
    applies: bool,
    invoked: &Arc<AtomicUsize>,
) -> (Service, ProjectorTopologyId) {
    use crate::projection::placement::ProjectionExecutorRoute;

    let invoked = Arc::clone(invoked);
    let (projector, topology) =
        generated_modeled_projector(ProjectionExecutorRoute::local("task11-explicit").unwrap());
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .modeled_projector(projector)
        .handle(move |context, projection| {
            let invoked = Arc::clone(&invoked);
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                if applies {
                    projection
                        .apply(GENERATED_MODELED_PROJECTOR, &context)
                        .await
                } else {
                    Ok(())
                }
            }
        });
    (
        Service::new().named("task11-explicit").routes(routes),
        topology,
    )
}

/// A modeled handler must apply the projection it is handed. One that
/// returns without doing so is a deterministic bug: the projector stops with
/// a recorded failure, and resumes from that input once a corrected release
/// repairs it.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn a_modeled_handler_that_skips_its_projection_stops_and_resumes_after_repair() {
    let repository = InMemoryRepository::new();
    let bus = InMemoryBus::new();
    let invoked = Arc::new(AtomicUsize::new(0));
    bus.publish_message(modeled_fact_message(
        "todo-skipped",
        "applied after repair",
        1,
    ))
    .await
    .unwrap();

    let (broken, topology) = explicit_modeled_service(&repository, false, &invoked);
    let stopped = broken
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .expect_err("a handler that skips its projection stops the projector");
    assert!(stopped.is_permanent() && stopped.should_retain_and_stop());
    assert!(matches!(
        stopped
            .source()
            .and_then(|source| source.downcast_ref::<HandlerError>()),
        Some(HandlerError::ProjectionTerminalRecorded { .. })
    ));
    let handle = repair_handle(&stopped);
    let ordered = bus.ordered_topic_evidence(FACT_NAME, 0);
    let request = modeled_snapshot_request(
        &topology,
        &ordered,
        "PrimaryView",
        PrimaryView::schema(),
        "todo-skipped",
    );
    assert!(repository
        .projection_query_snapshot(&request)
        .await
        .unwrap()
        .row
        .is_none());

    let (fixed, _) = explicit_modeled_service(&repository, true, &invoked);
    assert_eq!(fixed.repair_projection(&handle).await.unwrap().get(), 2);
    fixed
        .with_bus(bus.clone())
        .run(RunOptions::until_idle())
        .await
        .unwrap();
    assert_eq!(invoked.load(Ordering::SeqCst), 2, "once broken, once fixed");
    assert_eq!(
        repository
            .projection_query_snapshot(&request)
            .await
            .unwrap()
            .row
            .as_ref()
            .and_then(|row| row.get_serde::<String>("title").ok())
            .as_deref(),
        Some("applied after repair")
    );
}

/// The modeled projector reads its event from the domain occurrence in the
/// payload. An occurrence that is not one, or that disagrees with its
/// transport envelope, is recorded and stops the projector before its
/// handler runs.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn a_modeled_projector_records_an_event_whose_occurrence_it_cannot_trust() {
    let not_an_occurrence = Message::new(
        FACT_NAME,
        MessageKind::Event,
        br#"{"id":"todo-1","title":"secret-tenant"}"#.to_vec(),
    )
    .with_id("modeled-garbage")
    .with_metadata(crate::trace_context::CAUSATION_ID, "command-garbage");
    let cases = [
        (
            "a payload that is not a domain occurrence",
            not_an_occurrence,
        ),
        (
            "an occurrence whose ID differs from the message ID",
            modeled_fact_message("todo-1", "secret-tenant", 1).with_id("another-message"),
        ),
        (
            "an occurrence whose causation differs from the message's",
            without_causation(modeled_fact_message("todo-1", "secret-tenant", 1))
                .with_metadata(crate::trace_context::CAUSATION_ID, "command-other"),
        ),
    ];
    for (name, message) in cases {
        let invoked = Arc::new(AtomicUsize::new(0));
        let bus = InMemoryBus::new();
        bus.publish_message(message).await.unwrap();
        let (service, _) = explicit_modeled_service(&InMemoryRepository::new(), true, &invoked);
        let stopped = service
            .with_bus(bus)
            .run(RunOptions::until_idle())
            .await
            .expect_err(name);
        assert!(
            matches!(
                stopped
                    .source()
                    .and_then(|source| source.downcast_ref::<HandlerError>()),
                Some(HandlerError::ProjectionTerminalRecorded { .. })
            ),
            "{name}: {stopped}"
        );
        let _ = repair_handle(&stopped);
        assert!(!stopped.to_string().contains("secret-tenant"), "{name}");
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{name}");
    }
}

/// Without a message ID or causation ID a delivery cannot be recorded at
/// all: the modeled projector halts the runner instead of acknowledging it.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn a_modeled_projector_halts_on_a_delivery_it_cannot_identify() {
    let message = modeled_fact_message("todo-1", "unidentified", 1);
    let mut without_id = message.clone();
    without_id.id = None;
    for (name, message) in [
        ("no message ID", without_id),
        ("no causation ID", without_causation(message)),
    ] {
        let invoked = Arc::new(AtomicUsize::new(0));
        let bus = InMemoryBus::new();
        bus.publish_message(message).await.unwrap();
        let (service, _) = explicit_modeled_service(&InMemoryRepository::new(), true, &invoked);
        let halted = service
            .with_bus(bus)
            .run(RunOptions::until_idle())
            .await
            .expect_err(name);
        assert!(halted.should_retain_and_stop(), "{name}");
        let halted = halted
            .source()
            .and_then(|source| source.downcast_ref::<HandlerError>());
        assert!(
            matches!(
                halted
                    .and_then(|halted| halted.source())
                    .and_then(|source| source.downcast_ref::<HandlerError>()),
                Some(HandlerError::UnqualifiedProjectionDelivery(_))
            ),
            "{name}: {halted:?}"
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{name}");
    }
}
