# Distributed

**Distributed** is an end-to-end fullstack framework for building distributed systems and realtime applications with CQRS and Event Sourcing, with generated GraphQL query APIs over rust defined Read Models with RBAC, and automatic client side optimism via generated command clients and client side replica cache.

## Principles

### 01 · Unidirectional

Changes go one way. There is order.

Front-end developers know this from Redux: dispatch in, state updates on a
defined path, UI reads the result. Distributed is that idea for the **whole
system**.

Client → **command** → **aggregate** state change → **domain event** →
**projection** → **read model** → client. No dual-write from the UI. CAP
and eventual consistency sit on the read side; optimistic UI is how the
front end meets that honestly.

### 02 · CQRS

Decisions and views are different models.

In the business, “complete this todo” is a decision with rules. “Show my
open todos” is a question about a list. Commands load aggregates; queries
hit a SQL-shaped read model. You avoid forcing both into “update a row,”
so domain code stays about rules and screens stay about presentation.

```ts
// Commands → aggregates (accept / reject business rules)
commands.todo.create({ title })
commands.todo.archive({ todo_id })
```

```graphql
# Queries → SQL-shaped read models (never write tables)
query Todos @load {
  todos {
    todo_id
    title
    status
  }
}
```

### 03 · Event-sourced aggregates

Business rules as plain types — with history.

Express the business as ordinary Rust structs and methods: who may do what,
what state is allowed next. Under the hood that’s event sourcing —
repository, append-only events, optional upcasters — so you get a timeline
and easy unit tests without putting rules in SQL or HTTP.

[`tests/e2e-ui/crates/todo-domain/src/models/todo.rs`](tests/e2e-ui/crates/todo-domain/src/models/todo.rs)

```rust,ignore
#[sourced(
    entity,
    events = "TodoEvent",
    aggregate_type = "todo",
    domain_state = TodoState,
)]
impl Todo {
    pub fn create(
        &mut self,
        todo_id: impl Into<String>,
        owner_id: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<(), TodoError> {
        // …validate…
        self.record_created(todo_id, owner_id, title)?;
        Ok(())
    }

    #[event("todo.created", version = 1, domain)]
    fn record_created(&mut self, todo_id: String, owner_id: String, title: String) {
        self.entity.set_id(&todo_id);
        self.todo_id = todo_id;
        self.owner_id = owner_id;
        self.title = title;
        self.status = TodoStatus::Open;
    }
}
```

### 04 · SQL read models + RBAC

What the user is allowed to see.

Screens need tables: lists, filters, joins. Read models are that query
shape, with row/column permissions next to the model — “owner sees only
their todos,” “admin sees all.” Queries and commands share the same idea
of who the actor is.

[`tests/e2e-ui/crates/readmodels/src/models/todos.rs`](tests/e2e-ui/crates/readmodels/src/models/todos.rs)

```rust,ignore
#[derive(Clone, Debug, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
    #[readmodel(id)]
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    pub status: String,
}

impl AuthorizedReadModel for Todos {
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            )
            .grant("admin", read().all_columns())
    }
}

// One list feeds schema bootstrap, the GraphQL engine and client export.
pub fn read_models() -> ReadModels {
    ReadModels::new("e2e-ui")
        .model::<Todos>()
        .model::<ChatMessages>()
        .model::<BlobGames>()
        .model::<AuthUsers>()
}
```

### 05 · Inferred query API

Rust models generate GraphQL.

The read model, permissions, and command contracts in Rust are the source.
Distributed **generates** the GraphQL schema — filters, order, pagination,
joins, RBAC, and command mutations. You do not write resolvers or a REST
endpoint per screen.

The page file only selects fields against that generated schema. Commands
stay domain verbs on the write side. The typed TypeScript client is
generated from the same inventory.

[`tests/e2e-ui/ui/src/routes/todos/+page.graphql`](tests/e2e-ui/ui/src/routes/todos/+page.graphql)

```graphql
# Page declares the shape it needs — no hand-written query API
query Todos @load {
  todos(order_by: [{ status: asc }, { todo_id: asc }]) {
    todo_id
    owner_id
    title
    status
  }
}
```

### 06 · Projections

One mutation. Two runtimes.

After a command succeeds, events describe what happened. A projection
names the **effect**: on these events, run this mutation program
(`upsert_todos`, `delete_todos_by_pk`). That program is the update — not
a second cache language on the page.

The same mutation runs in two places: the **server projector** writes the
SQL read model; the **client replica** applies it to the cache for
auto-optimism. The mutation file looks like GraphQL but is **internal IR**,
not a public client field. Field names are snake_case table names
(`upsert_todos`). Pages still send domain commands.

The read model is an ordinary SQL database. A changed projection applies to
the events that come after it; rows already written stay as they are, and
nothing is reset or refused when a new program starts.

[`tests/e2e-ui/crates/projections/src/todos.rs`](tests/e2e-ui/crates/projections/src/todos.rs)

```rust,ignore
// Abbreviated from todos.rs — event → mutation (server projector + client optimism)
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        model: Todos,
        on {
            events: [
                TodoCreatedDomainEvent,
                TodoCompletedDomainEvent,
                TodoArchivedDomainEvent,
                // … rename, reopen, reassign, force-archive
            ],
            mutation: SaveTodo,
            input: { todo: body },
        },
        on {
            events: [TodoPurgedDomainEvent],
            mutation: DeleteTodo,
            input: { todo_id: aggregate_id },
        },
    };
}
```

```graphql
# Syntax-only IR → MutationProgram (not a public GraphQL field).
# Same program applies to the SQL read model and the browser replica.
mutation SaveTodo {
  upsert_todos(object: $input.todo)
}
```

Handlers stay thin. The shortest form is the command on the aggregate
method itself: `#[command]` inside the `#[sourced]` impl generates the
handler and the portable mount (see [Command contracts](docs/guides/commands.md#command-contracts)):

```rust,ignore
#[command(
    "todo.complete",
    shard = todo_id,
    load = required,
    input = TodoCompleteInput,
    outcome = Eventual<TodoStatusPayload>,
    payload = TodoStatusPayload::from_todo,
    roles = ["user", "admin"],
    field = "todos_complete",
)]
pub fn complete(&mut self, #[principal] owner_id: &str) -> Result<(), TodoError> { .. }
```

The Todo playground spells the same registration out with
`command!` — shard, invoke one domain method, commit Eventual. `load:`
is `required`, `or_create` or `create` (an id that already has a stream
is rejected); `guard:` adds a check on top of the roles; the payload
closure also receives the session principal. `handle:` remains the
escape hatch for a body that does more than one domain call.

```rust,ignore
command! {
    name: "todo.complete",
    transition: domain_commands::Complete,
    aggregate: Todo,
    input: TodoCompleteInput,
    outcome: Eventual<TodoStatusPayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_complete",
    invoke: |todo, _input, principal| todo.complete(principal),
    payload: |todo, _principal| TodoStatusPayload::from_todo(&**todo),
}
```

### 07 · Service crates

Compose the process. Keep the domain still.

A module mounts one bounded context — commands, guards, projectors. A
**service crate** lists those modules. That list *is* the process: the
playground is one `Service`, one host, one runner that only reads env and
calls `run`. You do not set a runtime role flag.

Todo commands are `command!` declarations in `todo-domain`. This
playground mounts them on a local Service. The sibling example
[`tests/e2e-celld`](tests/e2e-celld) is one runner crate over the same
service crate: the same `build_service`, another command host. Every Todo
command and `chat.post` wait-dispatches to a **cell** (one private SQLite per
todo or message), routed by `CelldCommandHost::route_aggregate::<Todo>()` from
the mounted declarations: the cell kind is the aggregate type, the shard is
each declaration's `shard:`. GraphQL `@live` and Eventual projectors stay off
the cell.

The same packages can back a different `Service` later: all modules in one
binary, or commands here and Eventual projectors there. **Atomic** work
(blob’s board seal) stays with the command process. **Eventual** work can
split. Topology is explicit composition — not a hidden matrix.

[`tests/e2e-ui/crates/service/src/modules/compose.rs`](tests/e2e-ui/crates/service/src/modules/compose.rs)

```rust,ignore
// todo.rs: one bound for every store; the projection comes from its descriptor.
pub fn routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, Todo>> {
    Routes::for_aggregate(stores)
        .mount(todo_domain::commands::create())
        // …
        .consume_projection(projections.projector(TODOS))
}

// compose.rs (trimmed). The modules are the command namespaces; the
// application manifest is derived from this Service.
pub fn build_service<St: ServiceStores>(stores: &St) -> Service {
    let projections = LocalProjectionMounts::builder("e2e-ui", "ordered-domain-events")
        .eventual(TODOS)
        .eventual(CHAT_MESSAGES)
        .direct(BLOB_GAMES)
        .eventual(AUTH_USERS)
        .build()
        .expect("e2e-ui projection catalog");
    Service::new()
        .named("e2e-ui")
        .routes(todo::routes(stores, &projections))
        .routes(chat::routes(stores, &projections))
        .routes(blob::routes(stores))
        .routes(identity::routes(stores, &projections))
}

// host.rs: one host for SQLite and Postgres.
async fn host<St: SqlStores>(stores: St, options: HostOptions) -> Result<(), BoxError> {
    stores.bootstrap_read_models(read_models().catalog()).await?;
    let service = build_service(&stores).with_bus(stores.bus("e2e-ui"));
    // … GraphQL engine over stores.graphql_pool(), serve
}

// Another crate can list the same modules, or only Eventual projectors.
// You write that Service. You do not flip a Runtime::role flag.
```

HTTP `POST /{command}` is **off by default**. Browser writes use the
GraphQL command proxy. For an intentional non-GraphQL ingress (a provider
webhook), expose that one command with `.with_http_command(command, edge)`:
its edge authenticates the request and returns what to dispatch. Call
`.with_http_command_routes()` only behind a trusted proxy.

### 08 · Browser replica

Auto-optimism is a cache update.

The generated client is a **replica cache** of the authorized read-model
slice, plus typed commands. The page reads `query.use()` (or a `@window`
operation's `useWindow()`) and calls `commands.todo…`. It does not patch
arrays, merge pages or write `setState` recipes.

When a command fires, the replica applies the **same projection mutation**
to the cache immediately. The server later writes SQL with that program;
live/causal confirmation reconciles. Most rows are input + defaults +
claims. When the next row needs the known record (blob’s next board),
ship the domain **pure function as WASM**. Gen-client hosts it. Do not
write a TypeScript twin.

[`tests/e2e-ui/ui/src/routes/todos/+page.svelte`](tests/e2e-ui/ui/src/routes/todos/+page.svelte)
· [`tests/e2e-ui/crates/service/src/modules/blob.rs`](tests/e2e-ui/crates/service/src/modules/blob.rs)

```ts
// Generated operation + typed commands — no cache recipes in the page
import { Todos, useCommands } from '$distributed';

const query = Todos.use();
const commands = useCommands();
const todos = $derived($query.complete ? $query.data.todos : []);

await commands.todo.create({ title: text });
await commands.todo.complete({ todo_id });
// Replica applies SaveTodo (upsert_todos) to the cache. Page does not.

// A created row the server has not returned yet is pending: no hand-kept
// id sets. Edits of a row the server returned never make it pending.
{@const saving = $query.pending.has(todo)}
```

Ids and timestamps are command input defaults, declared next to the
command in Rust:

```rust,ignore
defaults: command_input_defaults! {
    input: ChatPostInput;
    default input.message_id = uuid_v7(); // client-generated, kept
    default input.created_at = now();     // client previews, server stamps
},
```

The generated call fills them, and returns them synchronously
(`commands.chat.post(input).input.message_id`) before the server answers.
A `now()` value only previews the row: the server overwrites it with its
host clock before the handler runs, so the client's clock is never the
stored ordering key. The row leaves `pending` as soon as the server's row
reaches the replica. A failed command rolls its preview back, so its row
leaves `pending` too.

```rust,ignore
// Advanced optimism: the same domain pure, shipped as WASM.
// blob-domain: typed row, arguments and output.
pub fn preview_move(row: MoveRow, args: MoveArgs) -> Option<MovePreview> {
    simulate_move(&row.map, row.score, args.direction).ok()
}
distributed::wasm_pure! {
    pub const PREVIEW_MOVE = preview_move(MoveRow, MoveArgs) -> MovePreview;
}

// The command: key, argument and patched columns come from the types.
preview_reduce_known_record:
    CommandProjectionPureReduce::known_record::<BlobGames>(PREVIEW_MOVE),

// distributed build compiles it; the generated client hosts it.
// No TypeScript board rules.
```

JS package deep-dive: [`js/README.md`](js/README.md).

### 09 · SvelteKit GraphQL islands

Put each query where its UI ownership lives.

`@load` and `@live` use the same GraphQL operation for server render,
rehydration, navigation, prefetch, and a push change feed. A page document is
replaced with the page. A layout document remains retained across its child
pages. A reusable component document is discovered from its Svelte import
graph and promoted to the nearest static page or layout that can supply its
variables. Generated bindings resolve route params, search params, trusted
session values, constants, and forwarded props once; UI code receives a typed
operation and does not duplicate that wiring.

[`tests/e2e-ui/ui/src/routes/chat/+layout.graphql`](tests/e2e-ui/ui/src/routes/chat/+layout.graphql)

```graphql
# This nested layout owns the lobby across /chat child pages: the newest page
# is live and `@window` pages older messages by keyset cursor.
query ChatMessages($limit: Int! = 25) @load @live @window {
  chat_messages(
    where: { room_id: { _eq: "lobby" } }
    limit: $limit
    order_by: [{ created_at: desc }, { message_id: desc }]
  ) {
    message_id
    body
    author { display_name }
  }
}
```

GraphQL defaults are the preferred home for stable island inputs. They become
optional in the generated TypeScript API and are canonicalized before cache
identity or transport, so SSR can load this query and the component can simply
call `ChatMessages.use()`, or `ChatMessages.useWindow()` for a `@window`
operation.

`@window` turns `order_by` into a keyset: the compiler adds a nullable cursor
variable per order column (and per primary-key column, which breaks ties) and
ANDs "after the cursor" into `where`, so authorization row filters still apply
to every page. With the cursor omitted the document is the head page that SSR
loads and `@live` keeps current. `useWindow()` returns one ordered,
de-duplicated `rows` list: the live head plus every older page `fetchMore()`
loaded, with `hasMore`, `fetchingMore` and `fetchMoreError`. Arrivals at the
head never shift a loaded page, because pages are keyed by row values, not
offsets. Route parameters with matching variable names are
inferred. Exceptional values such as search parameters, trusted session claims,
or forwarded props live beside the document in `<document>.bindings.js`, not in
the application-wide build configuration.

Reusable component island:
[`SelectedBlobGame.graphql`](tests/e2e-ui/ui/src/lib/components/blob/SelectedBlobGame.graphql)
is consumed by
[`SelectedBlobGame.svelte`](tests/e2e-ui/ui/src/lib/components/blob/SelectedBlobGame.svelte).
Its optional `gameId` comes from `[[gameId]]`; the generated boundary uses that
same binding for SSR, hydration, navigation, prefetch, and `SelectedBlobGame.use()`.

### 10 · OIDC

Who the user is — in the model and the UI.

Real products need real identity. OIDC is first-class (Zitadel in the
playground; Keycloak and Authentik in tests). Sessions and JWTs become
claims the domain already uses for ownership and roles — the same claims
that scope the client replica.

- **Claims → RBAC.** Row filters and command handlers share claims like
  `x-user-id` and roles.
- **Surfaces.** User, admin, and public clients stay separate so elevated
  power does not leak.

---

## Multiplayer games

`distributed_game` adds a server-authoritative realtime engine on top of
this framework: zones that tick at a fixed rate, binary delta replication
with interest management, client prediction (`@hops-ops/distributed/game`),
outcomes saved as idempotent commands on cells without ever pausing the
game, and zones spread across servers with handoff, hibernation and live
content updates. Start with **[`distributed_game/README.md`](distributed_game/README.md)**;
eight runnable demos are in [`examples/game`](examples/game/README.md).

---

## Quick start

Two ways in: a single-file service on SQLite that needs only Rust, and the
full e2e-ui playground with login, SSR and live updates.

### Without Docker: SQLite in one process

[`examples/quickstart`](examples/quickstart/src/main.rs) is one aggregate,
one command, one projection and one read model, served from one process over
a SQLite file. From a checkout:

```bash
cargo run -p distributed-quickstart
# quickstart: GraphQL on http://127.0.0.1:4000/graphql (sqlite:quickstart.db?mode=rwc)
```

In another terminal, run the command, then read the row its projection wrote:

```bash
curl -s http://127.0.0.1:4000/todo.create \
  -H 'content-type: application/json' -H 'x-user-id: alice' -H 'x-roles: user' \
  -d '{"commandId":"0190a000-0000-7000-8000-000000000001","input":{"todo_id":"t1","title":"Hello"}}'
# {"payload":{"owner_id":"alice","title":"Hello","todo_id":"t1"},"receipt":{…,"state":"succeeded"}}

curl -s http://127.0.0.1:4000/graphql \
  -H 'content-type: application/json' -H 'x-user-id: alice' -H 'x-roles: user' \
  -d '{"query":"{ todos { todo_id owner_id title } }"}'
# {"data":{"todos":[{"todo_id":"t1","owner_id":"alice","title":"Hello"}]},…}
```

Send the same `commandId` again and you get the same receipt; the command
ran once. Ask as `bob` and the list is empty: the row policy on the read
model scopes it to its owner. GraphiQL is at `http://127.0.0.1:4000/graphql`
in a browser.

There is no identity provider here, so the example opts in to two
**local-only** shortcuts that trust the `x-user-id` and `x-roles` headers as
sent: `IdentityConfig::dev_headers()` for GraphQL queries, and
`Service::with_http_command_routes()` for `POST /{command}`. GraphQL command
mutations need a verified OIDC bearer, which is why the write above uses the
HTTP route. A real application keeps both shortcuts off and authenticates with
OIDC, as the playground below does.

### The e2e-ui playground (`tests/e2e-ui`)

A copyable multi-crate product: pure domains, GraphQL-only edge, Zitadel
OIDC, SvelteKit SSR, generated clients, live WS. Full runbook:
**[`tests/e2e-ui/README.md`](tests/e2e-ui/README.md)**.

It needs Docker plus `jq`, `curl` and `openssl` for the identity provider,
and Node for the UI:

```bash
cd tests/e2e-ui
make up      # Docker: Postgres + Zitadel + OIDC bootstrap → e2e-ui.env
make run     # `distributed dev .` from this checkout: build, then supervise API + UI
# UI and GraphQL: http://localhost:8791   (graphql at /graphql)
```

Open `http://localhost:8791`, not `127.0.0.1`: the login cookie is set for
`localhost`. The backend serves GraphQL and proxies the SvelteKit app, which
listens internally on port 5180. `make run` runs the checkout's CLI through
Cargo (`cargo run -p distributed_cli --bin distributed -- dev .`), so the
CLI, the crate and the JS package all come from this checkout; the
`distributed` binary does not need to be on your `PATH`. On a cold checkout,
`make application-build` first compiles everything once, so `make run` starts
inside its readiness window. `make test` runs the offline suites;
`make test-browser` runs Playwright against a running `make run`.

```bash
# Optional: same application, Todo and chat.post on celld cells
cd tests/e2e-ui && make up && make up-celld-nats
cd ../e2e-celld && make run
```

Demo logins after `make up`: `alice` / `bob` / `admin` · `Password1!`.
Full celld runbook: **[`tests/e2e-celld/README.md`](tests/e2e-celld/README.md)**.

Small apps, full patterns. Each screen has **How it is built**: query,
then command, then handler, then domain, then events, then service and
host. Todos also run against celld from [`tests/e2e-celld`](tests/e2e-celld)
with the same service crate.

| Demo | Tag | What it shows |
|---|---|---|
| [`/chat`](tests/e2e-ui/ui/src/routes/chat) | Live + anonymous | Shared room with SSR, live updates, guest reads |
| [`/todos`](tests/e2e-ui/ui/src/routes/todos) | Eventual · celld | Ownership rules, optimistic commands, projector fill. Same declarations on a Service or a cell |
| [`/blob`](tests/e2e-ui/ui/src/routes/blob) | Atomic + WASM | Atomic board in the response. Same domain pure runs as WASM in the replica |
| [`/admin`](tests/e2e-ui/ui/src/routes/admin) | Surface | Elevated surface — separate client, more power |
| [`/session`](tests/e2e-ui/ui/src/routes/session) | OIDC | Who you are: tokens, groups, roles |

Start here in the code:

| File | Why it is nice |
|---|---|
| [`ui/src/routes/todos/+page.graphql`](tests/e2e-ui/ui/src/routes/todos/+page.graphql) | Co-located read. `@load` → SSR seed; no hand-written load function for the list. |
| [`ui/src/routes/todos/+page.svelte`](tests/e2e-ui/ui/src/routes/todos/+page.svelte) | `Todos.use()` + `useCommands()` — page never invents a cache or optimistic recipe. |
| [`ui/src/routes/chat/+layout.graphql`](tests/e2e-ui/ui/src/routes/chat/+layout.graphql) | Same document does layout-retained SSR, live **and** history: `@load @live @window`. |
| [`ui/src/lib/components/blob/SelectedBlobGame.graphql`](tests/e2e-ui/ui/src/lib/components/blob/SelectedBlobGame.graphql) | Reusable component island promoted to the nearest static route boundary; one route-param binding drives SSR and component reads. |
| [`ui/src/routes/blob/[[gameId]]/+page.svelte`](tests/e2e-ui/ui/src/routes/blob/[[gameId]]/+page.svelte) | Arrow keys → `commands.blob.move`; board from `BlobGames.use()`. |
| [`crates/service/src/modules/compose.rs`](tests/e2e-ui/crates/service/src/modules/compose.rs) | One `Service` lists modules. No `Runtime::role`. |
| [`crates/service/src/modules/blob.rs`](tests/e2e-ui/crates/service/src/modules/blob.rs) | `command!` returning `Atomic<BlobGames>`: the map/score row commits with the event, and a WASM pure previews the move. |
| [`crates/todo-domain/src/models/todo.rs`](tests/e2e-ui/crates/todo-domain/src/models/todo.rs) | Plain aggregate — no GraphQL in the domain. |
| [`ui/src/auth.ts`](tests/e2e-ui/ui/src/auth.ts) | Auth.js + Zitadel scopes/groups → engine roles. |

### GraphiQL playground (engine only)

```bash
cargo run --example graphiql --features "graphql,sqlite"
# → http://127.0.0.1:4000/graphql
```

---

## Use as a dependency

Adopt the whole path, or one crate feature. `#[sourced]` aggregates, the
bus, GraphQL, and the replica are independent. The playground uses all of
them. Your crate does not have to.

The Rust crate, the `distributed` CLI and the `@hops-ops/distributed` npm
package ship as one release, tagged `vX.Y.Z`, and an application must use the
same version of all three; `distributed build` refuses a mismatch and names
the three versions. Until a release matches this README, depend on a checkout
or the repository:

```toml
[dependencies]
distributed = { path = "../distributed" }
# or: distributed = { git = "https://github.com/hops-ops/distributed" }
```

A domain crate needs the macros and traits only, so it takes the empty
default feature set. The smallest aggregate, committed and loaded back:

```rust
# // Inside this package's own doctests the macros name `crate::` paths.
# use distributed::*;
use distributed::prelude::*;

#[derive(Default)]
pub struct Todo {
    entity: Entity,
    title: String,
    done: bool,
}

#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    #[event("todo.created")]
    fn create(&mut self, id: String, title: String) {
        self.entity.set_id(&id);
        self.title = title;
    }

    #[event("todo.completed", when = !self.done)]
    fn complete(&mut self) {
        self.done = true;
    }
}

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let todos = InMemoryRepository::new().aggregate::<Todo>();

let mut todo = Todo::default();
todo.create("t1".into(), "Ship it".into())?;
todo.complete()?;
todos.commit(&mut todo).await?;

let loaded = todos.get("t1").await?.expect("committed");
assert!(loaded.done);
# Ok(())
# }
```

The service crate turns on what the process runs, for example
`features = ["postgres", "graphql"]`. Copy the **e2e-ui** layout when you want
the full product; each crate has one job:

```text
tests/e2e-ui/crates/
  todo-domain/   chat-domain/   blob-domain/   # aggregates, events and their commands
  readmodels/    # read-model rows and their permissions
  projections/   # projection! descriptors and mutation programs
  service/       # modules (one per bounded context), the Service, GraphQL, the host
  runner/        # main: read env, call run
  suite/         # in-process behavioural tests (SQLite, no Docker)
```

Depend on `distributed` only. The proc macros (`#[sourced]`, `command!`,
`#[derive(ReadModel)]`, `#[derive(Snapshot)]`, …) are re-exported from it;
`distributed_macros` is an implementation detail. `distributed_cli` is the
tooling, not a runtime dependency.

Every public item has one path. The crate root holds the event-sourcing
core (`Entity`, `Aggregate`, `AggregateRepository`, the `Repository`
traits and backends, snapshots) and the attribute and function-like macros
(`#[sourced]`, `command!`, `mutation!`, `projection!`). Everything else
lives in its module, and a derive lives with the trait it implements:
`distributed::read_model::ReadModel`, `distributed::domain_event::DomainEvent`,
`distributed::command::CommandInput`. `use distributed::prelude::*;` brings
in what a typical aggregate, command or read-model file needs.

## Feature Flags

The in-memory repository and the service bus facade are part of the core crate and
always available. Optional features pull in transports, persistence adapters, and
network servers.

| Feature | Default | Adds |
|---|---:|---|
| `http` | No | Axum HTTP transport for `microsvc`. |
| `knative` | No | Knative: `bus::KnativeBus`, `bus::knative_triggers` and the CloudEvents ingress `microsvc::cloud_events_router` (implies `http`). |
| `grpc` | No | Tonic gRPC transport for `microsvc`. |
| `graphql` | No | GraphQL query service over read models (pulls in `http` + WebSocket). Pair with `sqlite` and/or `postgres` for a dialect. |
| `postgres` | No | `PostgresRepository` and the Postgres outbox/transport (`PostgresBus`). |
| `sqlite` | No | `SqliteRepository` async SQL adapter and local durable transport (`SqliteBus`). |
| `nats` | No | `NatsBus` (NATS JetStream source/publisher). |
| `rabbitmq` | No | `RabbitBus` (RabbitMQ source/publisher). |
| `kafka` | No | `KafkaBus` (Kafka source/publisher). |
| `redis-streams` | No | `bus::RedisStreamBus` and `bus::RedisStreamPublisher` (Redis Streams `XADD` with id dedupe, consumer groups). Sorted-set leaderboards live in the `distributed_leaderboard` crate (its own `redis` feature). |
| `metrics` | No | `distributed::metrics`: framework counters and gauges with Prometheus text exposition (no SDK dependency); with `http`, a scrape handler. Bus receive loops record into it. |
| `otel` | No | OpenTelemetry tracing spans for bus receive loops and `microsvc` dispatch/handlers (`opentelemetry`, `tracing-opentelemetry`). |
| `cloudflare-workers` | No | Code that runs inside a celld Worker (`wasm32` Durable Objects, Cloudflare `worker` crate): the cell host's Durable Object storage and outbox, and the celld Queue publisher/relay (`bus::CelldQueue*`). |
| `gateway` | No | `distributed::gateway`: portable gateway declarations (bindings, routes, dispatch), whole-operation GraphQL selection (parser only) and the delivery identity/freshness contracts (snapshots, coalescing, live sharing). Starts no listeners. |
| `gateway-native` | No | Native HTTP gateway adapter (hyper/tower), independent of GraphQL and SQL. |
| `gateway-graphql-native` | No | Native GraphQL execution behind the gateway, including WebSocket `@live` and native delivery (snapshots, coalescing, live sharing; each opt-in at runtime). Implies `gateway-native`, `graphql`. |
| `gateway-worker` | No | workers-rs gateway ingress and delivery contracts, without native executors. |

`runtime-tokio` is internal: every Tokio-using feature above (`http`, `grpc`,
`postgres`, `sqlite`, `nats`, `rabbitmq`, `kafka`, `redis-streams`) implies it, and the crate gates its Tokio-only code
(the outbox drain and consumer workers) on it. Enable the transport or store you
need rather than `runtime-tokio` itself.

> The `InMemoryBus`, `PostgresBus`, and `SqliteBus` need no separate broker
> feature. SQL-backed bus support comes from the matching `postgres` or `sqlite`
> feature; the in-memory bus is always available for dev and tests.

## Guides

The principles above are the short version. The guides are the reference.

| Guide | Covers |
|---|---|
| [Event sourcing](docs/guides/event-sourcing.md) | `#[sourced]`, events and versions, metadata, repositories, queued locking, snapshots, upcasters |
| [Commands](docs/guides/commands.md) | Command contracts, `#[command]` and `command!`, the `microsvc` service, roles and guards, HTTP/gRPC/bus ingress, errors, the trust boundary |
| [Projections and read models](docs/guides/projections-and-read-models.md) | Read-model rows, typed JSON columns, Atomic and Eventual projections, source-snapshot fencing, rebuilds |
| [GraphQL](docs/guides/graphql.md) | The generated query API, permissions, identity (OIDC), live queries, client surfaces |
| [Outbox, bus and transports](docs/guides/bus-and-outbox.md) | The outbox, schedules and sagas, the bus facade and its adapters, retries and dead letters |
| [The `distributed` CLI](docs/guides/cli.md) | `build` / `dev`, manifests, schema output, metrics |
| [Testing](docs/guides/testing.md) | Test suites, live identity-provider stacks, broker integration tests, coverage |
| [JavaScript client](js/README.md) | `@hops-ops/distributed`: transport, replica, commands, SvelteKit adapter |
| [Multiplayer games](distributed_game/README.md) | `distributed_game`: zones, replication, prediction, cells |

Contracts and limits, one topic each:
[bus contract](docs/bus-contract.md) ·
[consumer delivery lanes](docs/consumer-delivery-lanes.md) ·
[coordination primitives](docs/coordination-primitives.md) ·
[application composition](docs/application-composition.md) ·
[external facts](docs/external-facts.md) ·
[unsigned command inputs](docs/unsigned-command-inputs.md) ·
[bounded read-model scans](docs/bounded-read-model-scans.md) ·
[compact projection contracts](docs/compact-projection-contracts.md) ·
[GraphQL query cost](docs/graphql-query-cost.md) ·
[live query delivery](docs/live-query-delivery.md) ·
[application gateway](docs/gateway/application.md).

Design notes record why a narrow behaviour is the way it is; they live in
[`docs/design/`](docs/design/README.md).

Upgrading from v4: [migrating to v5](docs/migrating-to-v5.md).

## Repository layout

```text
src/                      # the `distributed` crate
distributed_macros/       # its proc macros (re-exported by `distributed`)
distributed_cli/          # the `distributed` CLI; also a library
distributed_game/         # multiplayer zone runtime (+ distributed_game_macros/)
distributed_leaderboard/  # sorted-set leaderboards as a projection target
js/                       # @hops-ops/distributed: JS/TS client and SvelteKit adapter
examples/quickstart/      # the no-Docker quickstart
examples/graphiql.rs      # GraphQL engine over a seeded table, no commands
examples/game/            # game demos and the Mob Pit SvelteKit app (own workspace)
tests/e2e-ui/             # the full-stack playground and template (own workspace)
tests/e2e-celld/          # the same application on celld cells and NATS
migrations/               # SQLite and Postgres migrations
docs/                     # guides, contracts, design notes
```

Building, testing and the CI jobs are described in [`AGENTS.md`](AGENTS.md).

## License

The Rust workspace metadata declares its crates as MIT licensed, but this
repository does not currently contain a top-level license file. The npm package
therefore remains `UNLICENSED` until maintainers explicitly choose and add its
license.
