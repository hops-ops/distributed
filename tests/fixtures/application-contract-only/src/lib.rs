use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use distributed::application::{
    ApplicationExtension, ApplicationManifest, ContractCompiler, Module, SurfaceSpec,
};
use distributed::graphql::{
    build_surface, col, surface_for_application_contract, RoleGrant, Surface, SurfaceOptions,
};
#[cfg(feature = "application-runtime")]
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::table::{
    ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema, DEFAULT_TABLE_VERSION_COLUMN,
};
#[cfg(feature = "application-runtime")]
use distributed::AggregateBuilder;
use distributed::{Aggregate, CommandInput, CommandOutput, Entity, EventRecord};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct ContractAggregate {
    entity: Entity,
}

impl Aggregate for ContractAggregate {
    type ReplayError = std::convert::Infallible;

    fn aggregate_type() -> &'static str {
        "contract-only.aggregate"
    }

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

#[derive(Clone, Deserialize, CommandInput)]
pub struct CreateTodoInput {
    pub id: String,
    pub title: String,
}

#[derive(Clone, Serialize, CommandOutput)]
pub struct CreateTodoOutput {
    pub id: String,
}

/// This handler body is intentionally compiled only in the runtime-enabled
/// sibling. Contract-only artifacts retain its typed declaration but no
/// handler symbol or executable function body.
#[distributed::command(
    id = "todo.create",
    roles(user, admin),
    default(title = uuid_v7),
    input = CreateTodoInput,
    outcome = distributed::command::Succeeded<CreateTodoOutput>
)]
pub async fn create_todo(
    _context: &CausalCommandContext<'_, ContractAggregate>,
    input: CreateTodoInput,
) -> Result<
    distributed::command::PreparedCommand<distributed::command::Succeeded<CreateTodoOutput>>,
    HandlerError,
> {
    Ok(
        distributed::command::PreparedCommand::prepare(CreateTodoOutput { id: input.id })
            .expect("generated output is valid"),
    )
}

distributed::module! {
    pub TODO_MODULE {
        id: "todo-contract",
        commands: [CREATE_TODO_DEFINITION],
        capabilities: ["events", "identity"],
    }
}

fn todo_surface() -> Surface {
    let mut id = TableColumn::new("todo_id", "todo_id", ColumnType::Text);
    id.primary_key = true;
    let schema = TableSchema {
        model_name: "TodoView".into(),
        table_name: "todos".into(),
        columns: vec![
            id,
            TableColumn::new("title", "title", ColumnType::Text),
            TableColumn::new("status", "status", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["todo_id"]),
        version_column: Some(DEFAULT_TABLE_VERSION_COLUMN.into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let catalog = build_surface(&[schema], &SurfaceOptions::sqlite())
        .expect("non-empty contract fixture surface should compile")
        .with_module(&TODO_MODULE)
        .expect("module command contracts should bind before authorization");
    let grants = BTreeMap::from([(
        "user".to_string(),
        BTreeMap::from([(
            "TodoView".to_string(),
            RoleGrant::all_columns()
                .with_aggregations()
                .rows(col("status").eq("open")),
        )]),
    )]);
    surface_for_application_contract(&catalog, "web", &["user".into()], &["user".into()], &grants)
        .expect("contract fixture role surface should compile")
}

static WEB_SURFACE: LazyLock<SurfaceSpec> = LazyLock::new(|| {
    SurfaceSpec::from_surface("web", &todo_surface())
        .expect("contract fixture SurfaceSpec should compile")
});

static UI_EXTENSION: LazyLock<ApplicationExtension> = LazyLock::new(|| {
    ApplicationExtension::try_new(
        "todo.ui",
        1,
        serde_json::json!({"default_view": "board", "literal": "https://domain.example/view"}),
    )
    .expect("data-only extension should be portable")
});

distributed::application! {
    pub TODO_APPLICATION {
        id: "todo-contract-only",
        modules: [TODO_MODULE],
        surfaces: [WEB_SURFACE],
        capabilities: ["identity"],
        extensions: [UI_EXTENSION],
    }
}

pub fn application() -> &'static distributed::application::Application {
    &TODO_APPLICATION
}

pub fn manifest() -> ApplicationManifest {
    TODO_APPLICATION.manifest().clone()
}

pub fn compiler() -> ContractCompiler {
    ContractCompiler::from_surface("todo-contract-only", "web", Arc::new(todo_surface()))
        .expect("authoritative contract Surface should bind")
        .modules([(&*TODO_MODULE).clone()])
}

pub fn manifest_bytes() -> Vec<u8> {
    compiler()
        .manifest()
        .expect("contract manifest should be serializable")
        .canonical_bytes()
        .expect("contract manifest bytes should be canonical")
}

pub fn surface_sdl() -> String {
    compiler()
        .graphql_sdl()
        .expect("contract Surface SDL should compile")
}

pub fn client_manifest_bytes() -> Vec<u8> {
    serde_json::to_vec(
        &compiler()
            .client_manifest()
            .expect("client artifact should compile"),
    )
    .expect("client artifact should serialize")
}

pub fn selected_surface_fingerprint() -> String {
    WEB_SURFACE.fingerprint.clone()
}

#[cfg(feature = "linked-unlisted")]
pub fn linked_unlisted_marker() -> &'static str {
    application_unlisted::marker()
}

#[cfg(feature = "application-runtime")]
pub fn runtime_service() -> distributed::microsvc::Service {
    use distributed::{InMemoryRepository, ReadModelCatalog};

    let _catalog = ReadModelCatalog::new("todo-contract-only");
    let repository = InMemoryRepository::new();
    let routes = create_todo_register(
        distributed::microsvc::Routes::new().with_repo(repository.aggregate::<ContractAggregate>()),
    );
    distributed::microsvc::Service::new()
        .named("todo-contract-only")
        .routes(routes)
}

pub fn module() -> Module {
    (&*TODO_MODULE).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_default_contract_artifacts_retain_the_declared_command() {
        let surface = todo_surface();
        assert_eq!(
            surface
                .commands()
                .iter()
                .filter(|command| command.command_name == "todo.create")
                .count(),
            1
        );
        assert!(WEB_SURFACE.commands_attached);
        assert_eq!(
            WEB_SURFACE
                .commands
                .iter()
                .filter(|command| command.id == "todo.create")
                .count(),
            1
        );

        let sdl = surface_sdl();
        assert!(sdl.contains("type Mutation"));
        assert_eq!(sdl.matches("todo_create").count(), 1, "{sdl}");

        let manifest: serde_json::Value =
            serde_json::from_slice(&client_manifest_bytes()).expect("client manifest JSON");
        let commands = manifest["commands"]
            .as_array()
            .expect("client manifest command inventory");
        assert_eq!(
            commands
                .iter()
                .filter(|command| command["name"] == "todo.create")
                .count(),
            1
        );
        assert_eq!(
            commands
                .iter()
                .find(|command| command["name"] == "todo.create")
                .and_then(|command| command["mutation_field"].as_str()),
            Some("todo_create")
        );
    }

    #[test]
    fn no_default_contract_definition_has_no_executable_mount() {
        assert_eq!(TODO_MODULE.mounts().len(), 0);
        assert!(TODO_MODULE
            .definitions()
            .iter()
            .all(|definition| definition.mount().is_none()));
    }
}
