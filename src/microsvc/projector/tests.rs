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
use crate::projection_protocol::{ProjectionEpoch, ProjectionScopeCodec, ProjectorTopologyId};
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
                body_field_binding(["title"], ["title"], ProjectionValueType::String)?,
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
    "task11-generated-v1",
    || {
        static PROGRAM: crate::projection::lower::ProjectionProgramCell =
            crate::projection::lower::ProjectionProgramCell::new();
        PROGRAM.get_or_build(generated_modeled_projector_program)
    },
    generated_modeled_projector_lower,
    generated_modeled_projector_inventory,
);

fn primary_projector() -> SurfaceProjector {
    SurfaceProjector::new("task15_a_primary")
        .facts([FACT_NAME])
        .models(["PrimaryView"])
        .change_epoch("task15-primary-v1")
}

fn secondary_projector() -> SurfaceProjector {
    SurfaceProjector::new("task15_b_secondary")
        .facts([FACT_NAME])
        .models(["SecondaryView"])
        .change_epoch("task15-secondary-v1")
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
    use crate::graphql::SurfaceModeledProjection;
    use crate::projection::catalog::{ProjectionBindingActivation, ProjectionCatalog};
    use crate::projection::placement::{
        ProjectionBinding, ProjectionBindingState, ProjectionOutput, ProjectionOwner,
        ProjectionPhysicalTopology, ProjectionSourceBinding, PROJECTION_PARTITION_CODEC_VERSION,
    };

    let topology = ProjectorTopologyId::new(1, "task11-generated-runtime", [0x6b; 32]).unwrap();
    let binding = ProjectionBinding::materialize_eventual(
        GENERATED_MODELED_PROJECTOR.eventual(),
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
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new("task11-generated-v1").unwrap(),
                ProjectionBindingState::Active,
                Some(route),
            )],
            None,
        )
        .unwrap();
    let modeled = SurfaceModeledProjection::try_from_descriptor(
        GENERATED_MODELED_PROJECTOR,
        &catalog,
        &active,
        binding.id(),
    )
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

fn malformed_service(repository: InMemoryRepository, invoked: Arc<AtomicUsize>) -> Service {
    let projector = SurfaceProjector::new("task15_malformed")
        .facts(["task15.malformed"])
        .models(["MalformedView"])
        .change_epoch("task15-malformed-v1")
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
    for policy in [FailurePolicy::DeadLetter, FailurePolicy::LogAndAck] {
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
        bus.publish_message(Message::new(
            FACT_NAME,
            MessageKind::Event,
            br#"{"id":"secret-tenant","title":"must not cross"}"#.to_vec(),
        ))
        .await
        .unwrap();

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
