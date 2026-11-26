# Application composition

How an application is put together from domain crates, modules, a `Service`
and a host. `tests/e2e-ui` is the reference; every snippet below is taken from
it (paths are relative to `tests/e2e-ui/crates/`).

There are two planes, and they change for different reasons:

| Plane | Contents | Changes when |
|---|---|---|
| **Logical** | Aggregates, commands, projections, read models, client surfaces | The product changes |
| **Runtime** | Event store, read models, locks, bus, outbox, consumer, GraphQL, identity, HTTP | The deployment or the database changes |

Switching SQLite for Postgres touches only the runtime plane. Adding a
command touches only the logical plane.

## Logical plane

### Crates: write each fact once

```text
todo-domain/  chat-domain/  blob-domain/   aggregates, events, their commands
readmodels/                                read-model rows and their permissions
projections/                               projection! descriptors and mutation programs
service/                                   modules, the Service, GraphQL, the host
```

Each fact has one home:

- A command is declared next to its aggregate, with `#[command]` inside
  `#[sourced]` or with `command!` (`todo-domain/src/commands/`).
- A read model's permissions are on the model (`impl AuthorizedReadModel`,
  `readmodels/src/models/`).
- A projection's name, version, epoch, model and event-to-mutation mapping
  are in its `projection!` descriptor (`projections/src/`).

### Projection mounts

`service/src/modules/projections.rs` mounts each projection for this process
and says how it is applied:

```rust,ignore
use distributed::projection::LocalProjectionMounts;
use e2e_projections::{AUTH_USERS, BLOB_GAMES, CHAT_MESSAGES, TODOS};

pub fn projections() -> LocalProjectionMounts {
    LocalProjectionMounts::builder("e2e-ui", "ordered-domain-events")
        .eventual(TODOS)          // applied by the consumer after the commit
        .eventual(CHAT_MESSAGES)
        .direct(BLOB_GAMES)       // sealed by the blob commands in their own transaction
        .eventual(AUTH_USERS)
        .build()
        .expect("e2e-ui projection catalog")
}
```

Catalog activation, topology digests and surface packaging come from
`LocalProjectionMounts`; the application never writes them.

### Modules

A module is one bounded context: its command mounts and the projections it
consumes, generic over the stores (`service/src/modules/todo.rs`):

```rust,ignore
pub fn routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, Todo>> {
    Routes::for_aggregate(stores)
        .mount(todo_domain::commands::create())
        .mount(todo_domain::commands::rename())
        // …
        .consume_projection(projections.projector(TODOS))
}
```

### The Service

`service/src/modules/compose.rs` lists the modules. That list is the
application:

```rust,ignore
pub fn build_service<St: ServiceStores>(stores: &St) -> Service {
    let projections = projections();
    Service::new()
        .named(E2E_UI_APPLICATION)
        .routes(todo::routes(stores, &projections))
        .routes(chat::routes(stores, &projections))
        .routes(blob::routes(stores))
        .routes(identity::routes(stores, &projections))
        .with_http_command(zitadel::COMMAND, zitadel::http_edge)
        .with_http_command(zitadel_scrape::COMMAND, zitadel_scrape::http_edge)
}
```

`build_service` takes any `ServiceStores`. The host passes SQL stores, and a
test can pass in-memory ones.

### Read models and client surfaces

One `ReadModels` list (`readmodels/src/lib.rs`) feeds schema bootstrap, the
GraphQL engine and the client export. The client surfaces are constants in
`service/src/application.rs`: who may open each one, and whose grants shape
its schema.

```rust,ignore
pub const CLIENT_SURFACE: ClientSurface = ClientSurface {
    name: "e2e-ui",
    eligible_roles: &["admin", "user"],
    schema_roles: &["user"],
};
```

The application manifest is derived from the composed `Service` and is never
listed by hand. `Service::application(name, SurfaceSpec::from_surface(..))`
turns each command namespace (`todo.*`, `chat.*`, …) into a module, and it
refuses a command that the surface does not authorize
(`service/src/modules/graphql.rs`).

### Eventual and Atomic

| Outcome | Where the read model is written |
|---|---|
| `Eventual<P>` / `Succeeded<P>` | The consumer applies the projection after the commit. The projector may run in another process, on the same crates. |
| `Atomic<Model>` | The command's own transaction writes the row (`.direct(..)` mount), and the response carries it. The row can only be sealed where the command runs. |

Client optimism takes the same path either way: transition event, then
role-visible mutation, then the optimistic layer, then the seal. The outcome
only changes where the server applies the mutation and how the client proves
the seal.

## Runtime plane

### One dialect choice pairs store, locks, bus and GraphQL pool

`service/src/host.rs` picks the backend from `DATABASE_URL` and writes the
host body once, against `SqlStores`:

```rust,ignore
pub async fn run(database_url: &str, options: HostOptions) -> Result<(), BoxError> {
    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        host("postgres", PostgresStores::connect(database_url).await?, options).await
    } else {
        host("sqlite", SqliteStores::connect(database_url).await?, options).await
    }
}

async fn host<St: SqlStores>(backend: &str, stores: St, options: HostOptions) -> Result<(), BoxError> {
    stores.bootstrap_read_models(read_models().catalog()).await?;
    let lifecycle = LifecycleGate::from_env();   // under `distributed dev`, only the active generation writes
    let service = build_service(&stores)
        .with_lifecycle(lifecycle.clone())
        .with_bus(stores.bus("e2e-ui"));         // also drains the outbox
    let gql = build_graphql_engine_with_delivery(stores.graphql_pool(), &service, options.identity.clone(), /* … */)?;
    let service = Arc::new(service.try_with_graphql(gql)?);
    spawn_service_consumer_loop(move || {
        build_service(&stores).with_lifecycle(lifecycle.clone()).with_bus(stores.bus("e2e-ui"))
    });
    serve(service, &options).await?;
    Ok(())
}
```

| Concern | Comes from |
|---|---|
| Event store, read models, outbox | The one database (`SqliteStores` / `PostgresStores`, migrated on connect) |
| Locks | The matching lock manager in the same stores |
| Bus | `stores.bus(group)`, the SQL bus on the same database |
| GraphQL | `stores.graphql_pool()` and `stores.read_model_changes()` for live queries |

[`examples/quickstart`](../examples/quickstart/src/main.rs) is the same host
reduced to one aggregate.

### Transports are edges

| Edge | Use |
|---|---|
| GraphQL (queries, commands, live) | The application's public read and write surface, behind OIDC |
| Bus (outbox to consumer) | Eventual projections and cross-service commands |
| HTTP command routes | Off by default. `with_http_command(command, edge)` exposes one command behind an edge that authenticates the request (e2e-ui does this for the Zitadel Action ingress). `with_http_command_routes()` exposes every command, trusting `x-user-id` from a proxy. |
| Gateway | `service.command_gateway_routes(binding)` routes every command path to the service binding, so a command URL never falls through to the UI (`service/src/http.rs`) |

## Not built

A process today runs everything its `Service` mounts: commands, projectors,
outbox drain and GraphQL. Splitting the same crates into a writer process and
a projector process is a matter of composing two `Service`s by hand. There is
no framework-level process-role selector, and no check that refuses an Atomic
command in a process that cannot write. The design question is recorded in
[design/process-roles.md](design/process-roles.md).
