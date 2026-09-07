//! Minimal Distributed service fixture for `distributed` manifest-harness integration
//! tests: one read model (→ an `orders` table) registered in the read-model catalog.

use std::any::TypeId;

use distributed::command::{
    typed_command, Atomic, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField,
    PreparedCommand,
};
use distributed::graphql::{
    build_surface, surface_for_role, DistributedClientSurfaceExport, RoleGrant, SurfaceOptions,
    SurfaceProjector,
};
use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
use distributed::{
    Aggregate, AggregateRepository, Application, ApplicationManifest, Entity, EventRecord,
    InMemoryRepository, ReadModel, ReadModelCatalog, SurfaceSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("orders")]
pub struct OrderView {
    #[id("order_id")]
    pub order_id: String,
    pub status: String,
}

impl CommandOutputType for OrderView {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "OrderView",
            vec![
                CommandTypeField {
                    name: "order_id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    name: "status".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
            ],
        )
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Deserialize)]
struct ProjectOrderInput {
    order_id: String,
}

impl CommandInputType for ProjectOrderInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "ProjectOrderInput",
            vec![CommandTypeField {
                name: "order_id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Default)]
struct FixtureAggregate {
    entity: Entity,
}

impl Aggregate for FixtureAggregate {
    type ReplayError = std::convert::Infallible;

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

async fn project_order(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    _input: ProjectOrderInput,
) -> Result<PreparedCommand<Atomic<OrderView>>, HandlerError> {
    Err(HandlerError::Rejected(
        "manifest-only fixture does not execute commands".into(),
    ))
}

/// The entrypoint `distributed schema` calls by default
/// (`<crate>::read_model_catalog`). This is the physical read-model catalog,
/// not the logical application manifest.
pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new("orders").read_model::<OrderView>()
}

/// The logical application artifact is built from the same non-executable
/// Surface contract as the client export; physical catalog SQL remains a
/// separate read-model utility.
pub fn application_manifest() -> ApplicationManifest {
    let catalog = read_model_catalog();
    let surface = build_surface(&catalog.tables, &SurfaceOptions::sqlite())
        .expect("fixture Surface should build");
    let surface = SurfaceSpec::from_surface("orders", &surface)
        .expect("fixture Surface contract should compile");
    Application::new("orders")
        .surface(surface)
        .build()
        .expect("fixture application manifest should compile")
        .manifest()
        .clone()
}

/// Pool-free client export used by `distributed client-manifest`. Both the CLI harness
/// and a runtime engine finish through `DistributedClientSurfaceExport::manifest`.
pub fn distributed_client_surface() -> DistributedClientSurfaceExport {
    let catalog = read_model_catalog();
    let service = Service::new().named("orders").routes(
        Routes::new()
            .with_repo(AggregateRepository::<_, FixtureAggregate>::new(
                InMemoryRepository::new(),
            ))
            .typed_command(
                typed_command::<ProjectOrderInput, Atomic<OrderView>>("order.project")
                    .field_name("orders_project")
                    .roles(["user"]),
            )
            .handle(project_order),
    );
    let full = build_surface(&catalog.tables, &SurfaceOptions::sqlite())
        .expect("fixture Surface should build")
        .with_service(&service)
        .expect("fixture typed service should bind")
        .with_projectors([SurfaceProjector::new("project_orders")
            .facts(["order.projected"])
            .models(["OrderView"])
            .change_epoch("orders-v1")])
        .expect("fixture projector should bind");
    let grants = std::collections::BTreeMap::from([(
        "OrderView".to_string(),
        RoleGrant::all_columns().with_aggregations(),
    )]);
    let user =
        surface_for_role(&full, "user", &grants).expect("fixture role policy should be valid");
    DistributedClientSurfaceExport::from_selected("orders", user)
        .expect("fixture Surface should be role-selected")
}
