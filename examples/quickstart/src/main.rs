//! The no-Docker quickstart: one aggregate, one command, one projection and
//! one read model in one process over a SQLite file.
//!
//! ```bash
//! cargo run -p distributed-quickstart
//! ```
//!
//! Then, from another terminal, run the command and read the projected row:
//!
//! ```bash
//! curl -s http://127.0.0.1:4000/todo.create \
//!   -H 'content-type: application/json' -H 'x-user-id: alice' -H 'x-roles: user' \
//!   -d '{"commandId":"0190a000-0000-7000-8000-000000000001","input":{"todo_id":"t1","title":"Hello"}}'
//!
//! curl -s http://127.0.0.1:4000/graphql \
//!   -H 'content-type: application/json' -H 'x-user-id: alice' -H 'x-roles: user' \
//!   -d '{"query":"{ todos { todo_id owner_id title } }"}'
//! ```
//!
//! `todo.create` records `todo.created` on the aggregate and commits it with
//! its ledger receipt; sending the same `commandId` again replays the receipt.
//! The projector applies `SaveTodo` to the `todos` read model, and the query
//! reads the row back, filtered to the caller by the model's row policy (as
//! `bob`, the list is empty). GraphiQL is at `http://127.0.0.1:4000/graphql`
//! in a browser.
//!
//! Two local-only opt-ins stand in for a real identity provider, and both
//! trust `x-user-id` / `x-roles` exactly as the client sends them:
//!
//! - `IdentityConfig::dev_headers()` on the GraphQL engine, for queries.
//! - `Service::with_http_command_routes()`, so `POST /{command}` takes the
//!   principal from `x-user-id`, as it would behind a trusted proxy.
//!
//! GraphQL command mutations (`todos_create`) need a verified OIDC bearer, so
//! under DevHeaders they answer `401`. A real application puts
//! `IdentityConfig::OidcBearer` on its public edge and leaves the HTTP command
//! routes off; `tests/e2e-ui` is that application.
//!
//! Environment: `DATABASE_URL` (default `sqlite:quickstart.db?mode=rwc`) and
//! `BIND` (default `127.0.0.1:4000`).

use std::sync::Arc;

use distributed::domain_event::DomainState;
use distributed::graphql::{
    claim, col, read, AuthorizedReadModel, GraphqlEngine, IdentityConfig, ModelPermissions,
    ReadModels,
};
use distributed::microsvc::{
    spawn_service_consumer_loop, AggregateDependencies, Routes, Service, ServiceStores, SqlStores,
    SqliteStores,
};
use distributed::mutation::Mutation;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::projection::LocalProjectionMounts;
use distributed::read_model::ReadModel;
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

const APPLICATION: &str = "quickstart";

// --- Write side: the aggregate and its command -----------------------------

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Todo {
    entity: Entity,
    owner_id: String,
    title: String,
}

/// The public state a `todo.*` domain event carries, and the command's payload.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    DomainState,
    distributed::command::CommandOutput,
)]
#[domain_state(version = 1)]
pub struct TodoState {
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
}

impl From<&Todo> for TodoState {
    fn from(todo: &Todo) -> Self {
        Self {
            todo_id: todo.entity.id().to_string(),
            owner_id: todo.owner_id.clone(),
            title: todo.title.clone(),
        }
    }
}

impl TodoState {
    pub fn from_todo(todo: &Todo) -> Self {
        Self::from(todo)
    }
}

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct CreateTodo {
    pub todo_id: String,
    pub title: String,
}

#[sourced(entity, aggregate_type = "todo", domain_state = TodoState)]
impl Todo {
    /// `todo.create` (GraphQL field `todos_create`). The owner is the
    /// authenticated principal, never a field of the input. `Succeeded`: the
    /// receipt answers when the command commits; the projector fills the read
    /// model a moment later.
    #[command(
        "todo.create",
        shard = todo_id,
        load = create,
        input = CreateTodo,
        outcome = Succeeded<TodoState>,
        payload = TodoState::from_todo,
        roles = ["user"],
        field = "todos_create",
    )]
    pub fn create(
        &mut self,
        #[principal] owner_id: &str,
        todo_id: &str,
        title: &str,
    ) -> Result<(), String> {
        if title.trim().is_empty() {
            return Err("a todo needs a title".into());
        }
        self.created(todo_id.into(), owner_id.into(), title.into())
            .map_err(|error| error.to_string())
    }

    #[event("todo.created", version = 1, domain)]
    fn created(&mut self, todo_id: String, owner_id: String, title: String) {
        self.entity.set_id(&todo_id);
        self.owner_id = owner_id;
        self.title = title;
    }
}

// --- Read side: the read model, its permissions and its projection ---------

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
    #[readmodel(id)]
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
}

impl AuthorizedReadModel for Todos {
    /// A user sees only the todos they own.
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new().grant(
            "user",
            read()
                .all_columns()
                .rows(col("owner_id").eq(claim("x-user-id"))),
        )
    }
}

/// Upsert the whole row from the event body. Internal IR, not a GraphQL field.
#[allow(non_snake_case)]
pub fn SaveTodo() -> Mutation<()> {
    distributed::mutation! {
        mutation SaveTodo {
            upsert_todos(object: $input.todo)
        }
    }
}

distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        epoch: "quickstart-todos-v1",
        model: Todos,
        on {
            events: [TodoCreatedDomainEvent],
            mutation: SaveTodo,
            input: { todo: body },
        },
    };
}

// --- The process: one Service, one database --------------------------------

fn read_models() -> ReadModels {
    ReadModels::new(APPLICATION).model::<Todos>()
}

fn projections() -> LocalProjectionMounts {
    LocalProjectionMounts::builder(APPLICATION, "domain-events")
        .eventual(TODOS)
        .build()
        .expect("quickstart projection catalog")
}

fn todo_routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, Todo>> {
    Routes::for_aggregate(stores)
        .mount(Create)
        .consume_projection(projections.projector(TODOS))
}

fn build_service<St: ServiceStores>(stores: &St) -> Service {
    Service::new()
        .named(APPLICATION)
        .routes(todo_routes(stores, &projections()))
        // Local only: `POST /{command}` trusts `x-user-id` as the principal,
        // which is what a trusted proxy would inject.
        .with_http_command_routes()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Internal errors reach the client masked; the log has the detail.
    tracing_subscriber::fmt().init();
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:quickstart.db?mode=rwc".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:4000".into());

    // Event store, read models, outbox and bus all live in the one SQLite file.
    let stores = SqliteStores::connect(&database_url).await?;
    stores
        .bootstrap_read_models(read_models().catalog())
        .await?;

    let service = build_service(&stores).with_bus(stores.bus(APPLICATION));
    let engine = GraphqlEngine::builder(stores.graphql_pool())
        // A fixed key is fine on a laptop; a deployment injects its own secret.
        .protocol_token_key([0x51; 32])
        .roles(&["user"])
        .read_models(&read_models())
        .service(&service)
        .client_projection_owners(projections().all_owners())
        .change_stream(stores.read_model_changes())
        // Local only: trust `x-user-id` / `x-roles` as sent.
        .identity(IdentityConfig::dev_headers())
        .graphiql(true)
        .build()?;
    let service = Arc::new(service.try_with_graphql(engine)?);

    // The projector: consume the domain events the commands publish.
    spawn_service_consumer_loop(move || build_service(&stores).with_bus(stores.bus(APPLICATION)));

    eprintln!("quickstart: GraphQL on http://{bind}/graphql ({database_url})");
    distributed::microsvc::serve(service, &bind).await?;
    Ok(())
}
