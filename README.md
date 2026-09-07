# Distributed

**Distributed** is an end-to-end fullstack framework for building distributed systems and realtime applications with CQRS and Event Sourcing, with generated GraphQL query APIs over rust defined Read Models with RBAC, and automatic client side optimism via generated command clients and client side replica cache.

## How it works

Write the domain once, compose it into one `Service` or several, then
generate the client. Each stage below uses real code from
[`tests/e2e-ui`](tests/e2e-ui).

```mermaid
sequenceDiagram
    actor Author
    participant Build as distributed build + Vite
    participant Kit as SvelteKit
    participant API as GraphQL gateway
    participant Domain as Command handler + aggregate
    participant Projector as Projection worker
    participant DB as Read-model database
    participant Replica as Browser replica

    rect rgb(245, 247, 255)
        Note over Author,Replica: Generate one application from authored Rust and page GraphQL
        Author->>Build: Domain crates: commands, events, command RBAC
        Author->>Build: Read-model crates: query shapes, relationships, read RBAC
        Author->>Build: Projections: event → internal read-model mutation
        Author->>Build: Page, layout, or component .graphql: @load and optional @live
        Build-->>API: Command + query/subscription surfaces with RBAC
        Build-->>Projector: Server projection programs
        Build-->>Kit: Island boundaries, live operations, typed commands, replica plans
        Build-->>Replica: Optimistic projection programs + declared Rust/WASM pures
    end

    rect rgb(245, 255, 247)
        Note over Kit,Replica: Initial page render — @load
        Kit->>API: Authorized @load query (SSR or navigation)
        API->>DB: Read the authorized model slice
        DB-->>API: Rows + identities + revisions
        API-->>Kit: Query result
        Kit->>Replica: Normalize, dehydrate, hydrate + server authority
        Note right of Replica: Browser does not repeat the first query
        Replica-->>Kit: Reactive confirmed snapshot
    end

    rect rgb(255, 251, 240)
        Note over Kit,Replica: Ongoing page updates — @live
        Kit->>Replica: Generated operation attaches live automatically
        Replica->>API: Subscribe with the same query + variables
    end

    rect rgb(255, 245, 250)
        Note over Kit,Replica: Write path — public domain command, never public model mutation
        Kit->>Replica: Generated typed command + input
        Replica-->>Kit: Apply predicted projection immediately
        Replica->>API: Authorized domain command
        API->>Domain: Execute command
        Domain-->>Projector: Commit domain event
        Projector->>DB: Apply internal projection mutation
        DB-->>API: Publish committed read-model change
        API-->>Replica: @live records + causal clocks
        Replica-->>Kit: Confirm or reconcile the optimistic snapshot
        Note over Projector,Replica: The generated projection protocol drives both server updates and browser optimism
    end
```

Co-located GraphQL documents are part of the application contract. An `@load`
operation may belong to a page, a layout, or a reusable component. The compiler
promotes it to the nearest static SvelteKit boundary, emits one variable-binding
plan, and uses that plan for SSR, hydration, navigation, prefetch, and component
reads. `@live` retains the same operation as a subscription for the lifetime of
that boundary. Public writes still use domain commands. Projection mutation
programs stay internal and provide the shared server-update and
optimistic-replica protocol.

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

impl Todos {
    pub fn permissions() -> ModelPermissions<Self> {
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

[`tests/e2e-ui/crates/projections/src/todos.rs`](tests/e2e-ui/crates/projections/src/todos.rs)

```rust,ignore
// Abbreviated from todos.rs — event → mutation (server projector + client optimism)
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        epoch: "e2e-ui-todos-v2",
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

#### Full-state projections and out-of-order events

Broker delivery order is not necessarily aggregate commit order. For a read
model whose rows are complete snapshots owned by one aggregate stream, opt in
to source-version fencing:

```rust,ignore
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 2,
        epoch: "todos-source-snapshots-v1",
        model: Todos,
        source: aggregate_snapshot,
        on {
            events: [TodoCreatedDomainEvent, TodoCompletedDomainEvent],
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

The framework compares canonical `(aggregate_sequence, publication_ordinal)`
within the owning `(aggregate_type, aggregate_id)` stream. A late snapshot
cannot overwrite a newer row or resurrect a deleted one. Even deletion before
creation leaves a durable tombstone. A newer snapshot can recreate the row.
Source fences, physical rows, row revisions, broker checkpoints and causal
observations commit atomically in the memory, SQLite and PostgreSQL adapters.
A stale input confirms the current row revision without generating a fake row
update; a concurrent change makes that confirmation retry through the normal
projection conflict path.

Use this only for complete replacement snapshots with stable row keys. Every
affected key must be owned by one aggregate stream; joins, counters, partial
patches and relationship side effects need their own ordering/fold semantics.
This mode rejects delta operations, expression partitions and direct placement.
Different aggregate streams cannot take over an existing fenced key. Matching
source versions with conflicting occurrence content fail closed.

Migration `0005_projection_source_snapshots` persists the fence alongside each
record, including tombstones; compaction does not remove it. Enabling this on
an existing unversioned projection requires an explicit read-model rebuild
from retained canonical events. Merely changing the projection version or epoch
does not infer a source version for existing rows. The normal ordered-delivery
contract and transport identity checks still apply; this is not a replacement
for reliable broker delivery or an incremental-event reorder buffer.

Custom program factories can use `ProjectionProgram::with_source_snapshots()`;
the policy is part of the canonical program identity. Browser optimism continues
to use the same mutation program, and authoritative confirmation uses committed
row revisions rather than comparing browser timestamps.

#### Rebuilding existing snapshot projections

Use explicit maintenance, not a startup fallback or republishing loop:

```rust,ignore
use distributed::projection::rebuild::SnapshotProjectionRebuild;

// Stop producers, drain outboxes, stop consumers, and back up the read model.
// Reuse the same generated SurfaceProjector mounted by the application.
let rebuild = SnapshotProjectionRebuild::begin(&repository, &projector).await?;
// Read history AFTER capturing the target inventory. Another authoritative
// archive can supply these canonical occurrences; rebuilding is bus-neutral.
let events = nats_bus.retained_domain_events().await?;
let plan = rebuild.from_complete_history(&events)?;
println!("{} records to rebuild", plan.record_count());
plan.apply(&repository).await?;
```

Memory, SQLite and PostgreSQL commit complete rows/tombstones, source fences,
new record revisions and live-query changes atomically. A concurrent update,
insertion or deletion invalidates the captured inventory. Failure rolls back the
whole projection. Broker checkpoints, inbox receipts, command ledgers and prior
causal observations are preserved; maintenance does not claim a domain command
occurred. No domain handlers, external effects or queue sends run.

The archive must cover every existing record and retain its stored source
occurrence, where present. Conflicting duplicate occurrences, missing aggregate
sequence prefixes and stale source heads fail closed. The caller must establish
history completeness: even a broker stream starting at sequence one may have
been created after its aggregates. The NATS reader verifies a stable, gap-free
retained stream without creating consumers or acknowledging messages, but cannot
prove that unpublished events or earlier deleted streams never existed.

This first API is bounded offline maintenance for one active local,
unit-partition snapshot binding: at most 10,000 records and 100,000 occurrences /
64 MiB of canonical history. It intentionally rejects delta folds and histories
whose publication sequences have gaps. It does not migrate schemas/topologies,
perform online shadow swaps, or make several independent projections one
transaction. Keep services stopped if a multi-projection maintenance run fails;
capture fresh inventories and rerun before resuming them.

Handlers stay thin: most Todo commands are `portable_command!` — shard,
invoke one domain method, commit Eventual. `todo.create` keeps a `handle:`
escape hatch when the body needs extra checks.

```rust,ignore
portable_command! {
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
    payload: |todo| TodoStatusPayload::from_todo(&**todo),
}
```

### 07 · Service crates

Compose the process. Keep the domain still.

A module mounts one bounded context — commands, guards, projectors. A
**service crate** lists those modules. That list *is* the process: the
playground is one `Service`, one host, one runner that only reads env and
calls `run`. You do not set a runtime role flag.

Todo commands are `portable_command!` declarations in `todo-domain`. This
playground mounts them on a local Service. The sibling example
[`tests/e2e-celld`](tests/e2e-celld) mounts the same declarations and
wait-dispatches create, complete, and `chat.post` to a **cell** (one private
SQLite per todo or message). GraphQL `@live` and Eventual projectors stay
off the cell.

The same packages can back a different `Service` later: all modules in one
binary, or commands here and Eventual projectors there. **Atomic** work
(blob’s board seal) stays with the command process. **Eventual** work can
split. Topology is explicit composition — not a hidden matrix.

[`tests/e2e-ui/crates/service/src/modules/compose.rs`](tests/e2e-ui/crates/service/src/modules/compose.rs)

```rust,ignore
// compose.rs (trimmed). Each routes(...) takes repo, locks, read models,
// and the projection owner for that module.
pub const MODULE_IDS: &[&str] = &[
    todo::MODULE_ID, chat::MODULE_ID, blob::MODULE_ID, "identity",
];

Service::new()
    .named("e2e-ui")
    .routes(todo::routes(repo.clone(), locks.clone(), read_models.clone(), projections.todo))
    .routes(chat::routes(repo.clone(), locks.clone(), read_models.clone(), projections.chat))
    .routes(blob::routes(repo, locks, read_models, projections.blob))

// Another crate can list the same modules, or only Eventual projectors.
// You write that Service. You do not flip a Runtime::role flag.
```

HTTP `POST /{command}` is **off by default**. Browser writes use the
GraphQL command proxy. Call `.with_http_command_routes()` only for an
intentional non-GraphQL ingress.

### 08 · Browser replica

Auto-optimism is a cache update.

The generated client is a **replica cache** of the authorized read-model
slice, plus typed commands. The page reads `query.use()` and calls
`commands.todo…`. It does not patch arrays or write `setState` recipes.

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
```

```rust,ignore
// Advanced optimism: same domain pure, shipped as WASM
.preview_reduce_known_record(CommandProjectionPureReduce::wasm(
    "blob.simulate_move",
    "blob/pkg/blob_wasm",   // wasm-pack under $lib
    "blobSimulateMove",     // (recordJson, argsJson) → assignJson
    "BlobGames",
))

// Generated client hosts the module. No TypeScript board rules.
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
# This nested layout owns one finite live window across /chat child pages.
query ChatMessages($limit: Int! = 25, $offset: Int! = 0) @load @live {
  chat_messages(
    where: { room_id: { _eq: "lobby" } }
    limit: $limit
    offset: $offset
    order_by: [{ created_at: desc }]
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
call `ChatMessages.use()`. Route parameters with matching variable names are
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

## See it run

### e2e-ui playground (`tests/e2e-ui`)

A copyable multi-crate product: pure domains, GraphQL-only edge, Zitadel
OIDC, SvelteKit SSR, generated clients, live WS. Full runbook:
**[`tests/e2e-ui/README.md`](tests/e2e-ui/README.md)**.

```bash
# Default: one process (SQLite or Postgres + bus)
cd tests/e2e-ui
make up                    # Postgres + Zitadel → e2e-ui.env
distributed dev            # prepares JS/WASM + clients; starts Rust + SvelteKit
# UI  http://localhost:5180
# API http://127.0.0.1:8791

distributed build          # typed manifest + Rust build + Vite build

# Optional: same UI, Todo create/complete on celld
cd tests/e2e-ui && make up && make up-celld-nats
cd ../e2e-celld && make run
```

Demo logins after `make up`: `alice` / `bob` / `admin` · `Password1!`.
Full celld runbook: **[`tests/e2e-celld/README.md`](tests/e2e-celld/README.md)**.

Small apps, full patterns. Each screen has **How it is built**: query,
then command, then handler, then domain, then events, then service and
host. Todos also run against celld from [`tests/e2e-celld`](tests/e2e-celld)
with the same domain crate.

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
| [`ui/src/routes/chat/+layout.graphql`](tests/e2e-ui/ui/src/routes/chat/+layout.graphql) | Same document does layout-retained SSR **and** live: `@load @live`. |
| [`ui/src/lib/components/blob/SelectedBlobGame.graphql`](tests/e2e-ui/ui/src/lib/components/blob/SelectedBlobGame.graphql) | Reusable component island promoted to the nearest static route boundary; one route-param binding drives SSR and component reads. |
| [`ui/src/routes/blob/[[gameId]]/+page.svelte`](tests/e2e-ui/ui/src/routes/blob/[[gameId]]/+page.svelte) | Arrow keys → `commands.blob.move`; board from `BlobGames.use()`. |
| [`crates/service/src/modules/compose.rs`](tests/e2e-ui/crates/service/src/modules/compose.rs) | One `Service` lists modules. No `Runtime::role`. |
| [`crates/service/src/handlers/commands/blob_move.rs`](tests/e2e-ui/crates/service/src/handlers/commands/blob_move.rs) | `PreparedCommand<Atomic<BlobGameView>>` — map/score written with the event. |
| [`crates/todo-domain/src/models/todo.rs`](tests/e2e-ui/crates/todo-domain/src/models/todo.rs) | Plain aggregate — no GraphQL in the domain. |
| [`ui/src/auth.ts`](tests/e2e-ui/ui/src/auth.ts) | Auth.js + Zitadel scopes/groups → engine roles. |

### GraphiQL playground (engine only)

```bash
cargo run --example graphiql --features "graphql,sqlite"
# → http://127.0.0.1:4000/graphql
```

### First-class OIDC (Zitadel, Keycloak, Authentik)

GraphQL identity is built into the engine (`OidcBearer`: JWKS, iss/aud/exp,
claim → role/session). Live against three local IdPs — not mocks only:

| Provider | Compose + bootstrap | Live test | Gate |
|---|---|---|---|
| **[Zitadel](tests/graphql_oidc_zitadel/)** (reference) | `./scripts/oidc-zitadel-up.sh` | `cargo test --test graphql_oidc_zitadel --features graphql,sqlite` | `ZITADEL_E2E=1` |
| **[Keycloak](tests/graphql_oidc_keycloak/)** | `./scripts/oidc-keycloak-up.sh` | `cargo test --test graphql_oidc_keycloak --features graphql,sqlite` | `KEYCLOAK_E2E=1` |
| **[Authentik](tests/graphql_oidc_authentik/)** | `./scripts/oidc-authentik-up.sh` | `cargo test --test graphql_oidc_authentik --features graphql,sqlite` | `AUTHENTIK_E2E=1` |

Shared **E1–E8** in [`tests/graphql_oidc_common/`](tests/graphql_oidc_common/).
Gated binaries skip cleanly when unset. Offline:
`cargo test --test graphql_identity --features graphql,sqlite`.

e2e-ui boots **Zitadel** for the browser path; the three stacks prove the
same `OidcBearer` edge is not vendor-locked.

---

## Use as a dependency

Adopt the whole path, or one crate feature. `#[sourced]` aggregates, the
bus, GraphQL, and the replica are independent. This playground uses all of
them. Your crate does not have to.

Copy the **e2e-ui** layout when you want the full product: domain crates
stay feature-light; a **service crate** lists which modules this process
runs.

```text
crates/
  todo-domain/     # personal todos (owner-scoped)
  chat-domain/     # lobby chat (shared room)
  readmodels/      # projections + read_model_catalog
  service/         # thin command handlers + event projectors + GraphQL
  runner/          # store + bus + bind
```

The shared bounded-context crate depends on `distributed` with the empty
default feature set. It needs macros and traits, not HTTP servers, SQL
adapters, or broker clients:

```toml
# crates/todo-domain/Cargo.toml
[dependencies]
distributed = "0.1"
serde = { version = "1", features = ["derive"] }
```

Executable service crates depend on the domain crates and enable the
runtime features they need:

```toml
# crates/service/Cargo.toml
[dependencies]
todo-domain = { path = "../todo-domain" }
distributed = { version = "0.1", features = ["postgres", "graphql", "sqlite"] }
```

For local development against a checkout of this repository, use a path
dependency instead:

```toml
[dependencies]
distributed = { path = "../distributed" }
```

In a multi-crate workspace, put the dependency in the workspace root and
inherit it from member crates. Keep the root dependency feature-light, then
enable service-specific features only in the service crates.

Most application crates should depend on `distributed` only. The proc
macros (`#[sourced]`, `#[digest]`, `#[derive(ReadModel)]`,
`#[derive(Snapshot)]`) are re-exported from `distributed`; do not add
`distributed_macros` directly unless you are working on the macro crate
itself. The `distributed_cli` crate installs the `distributed` tooling
and is not needed as a runtime dependency unless you are embedding the
CLI in another command such as `hops service`.

The rest of this README is the API reference for each piece.

---
## Feature Flags

The in-memory repository and the service bus facade are part of the core crate and
always available. Optional features pull in transports, persistence adapters, and
network servers.

| Feature | Default | Adds |
|---|---:|---|
| `emitter` | No | In-process event emission and `#[enqueue]`. |
| `http` | No | Axum HTTP transport for `microsvc` + the Knative/CloudEvents ingress router. |
| `grpc` | No | Tonic gRPC transport for `microsvc`. |
| `graphql` | No | GraphQL query service over read models (pulls in `http` + WebSocket). Pair with `sqlite` and/or `postgres` for a dialect. |
| `postgres` | No | `PostgresRepository` and the Postgres outbox/transport (`PostgresBus`). |
| `sqlite` | No | `SqliteRepository` async SQL adapter and local durable transport (`SqliteBus`). |
| `nats` | No | `NatsBus` (NATS JetStream source/publisher). |
| `rabbitmq` | No | `RabbitBus` (RabbitMQ source/publisher). |
| `kafka` | No | `KafkaBus` (Kafka source/publisher). |

> The `InMemoryBus`, `PostgresBus`, and `SqliteBus` need no separate broker
> feature. SQL-backed bus support comes from the matching `postgres` or `sqlite`
> feature; the in-memory bus is always available for dev and tests.

## Core Concepts

- **Entity**: Holds the event history. You embed it in your domain structs.
- **EventRecord**: An immutable aggregate event record with name, payload, sequence, timestamp, and optional metadata. It is replayable model history, not automatically a published domain event.
- **Aggregate**: A struct that embeds an `Entity` and replays `EventRecord`s. `aggregate_type()` provides the durable stream-identity component for persistence.
- **Repository / AggregateRepository**: Persists and loads aggregates by event history. The event store is optimized for append and replay; `get`/`commit` are async.
- **InMemoryRepository**: In-memory repository for tests and examples. Implements every async trait (repository, read-model, snapshot, outbox).
- **SqliteRepository / PostgresRepository**: Durable async SQL adapters (optional features).
- **QueuedRepository**: Wraps any repository and adds async per-entity queue locking.
- **EventUpcaster**: A pure, stateless transformation that converts event payloads from one version to another at read time.
- **Snapshottable**: Opt-in trait for aggregates that produce state snapshot payload DTOs. Use `#[derive(Snapshot)]` to auto-generate the payload struct and trait impl.
- **OutboxMessage**: A durable publication work item for a domain event, integration event, command, or generic transport message. Supports optional `destination` for point-to-point routing and metadata propagation.
- **OutboxDispatcher**: Drains durable outbox rows and publishes them to a transport, sharing one claim → publish → complete path.
- **ReadModel**: Query-optimized relational projection state for UI/API reads. Read models may be updated atomically with a command or eventually from published messages.
- **GraphqlEngine**: Deny-by-default GraphQL surface over registered read-model tables: role-scoped columns/row filters, optional command mutations, live subscriptions via `ChangeHub`, and identity modes (`OidcBearer`, `TrustedProxy`, `Hybrid`, `DevHeaders`).
- **Bus / BusConsumer**: The service bus facade — `send`/`publish` (produce) and `listen`/`subscribe` (consume), implemented by a per-transport `*Bus` type.
- **microsvc::Service**: Convention-based async command/event handler framework with pluggable transports (HTTP, gRPC, bus, GraphQL mutations, direct dispatch).

## Terminology And CQRS Boundaries

Event sourcing is the model-level persistence strategy: aggregates record replayable `EventRecord`s when command methods such as `#[event]` (within `#[sourced]`) or `#[digest]` methods succeed. Those records are the write-side history used to hydrate the aggregate.

CQRS is the architectural split between write-side aggregates and query-side read models. Repositories load aggregate event streams by ID for command handling; production business queries should read from `ReadModel` projections shaped for that query.

Published messages are a separate boundary. An aggregate event record is not automatically a domain event. When other services, projections, or transports need a fact or command, create an `OutboxMessage` and commit it with the aggregate. The outbox payload can represent a domain event, integration event, command, or any other transport message.

The existing names and serialized fields such as `EventRecord::event_name` remain part of the compatibility contract. Terminology cleanup should clarify usage without renaming stored event records unless a migration path is explicitly designed.

## Pluggable by Default

Every infrastructure concern in `distributed` follows the same pattern: a **trait** defines the contract, an **in-memory implementation** ships out of the box for testing and development, and you swap in your own for production.

| Concern | Trait(s) | In-memory default | Swap in for production |
|---|---|---|---|
| Storage | `GetStream` + `TransactionalCommit` | `InMemoryRepository` | `PostgresRepository`, `SqliteRepository`, … |
| Messaging | `Bus` + `BusConsumer` | `InMemoryBus` | `NatsBus`, `PostgresBus`, `SqliteBus`, `RabbitBus`, `KafkaBus`, `KnativeBus` |
| Read model rows | `ReadModelWritePlanStore` + `RelationalReadModelQueryStore` | `InMemoryReadModelStore` | Postgres, SQLite |
| Snapshot store | `SnapshotStore` | `InMemorySnapshotStore` | Postgres, SQLite, … |
| Outbox publishing | `OutboxStore` + async `MessagePublisher` | `InMemoryRepository` outbox store (dev/test) | Any `MessagePublisher` (e.g. `BusPublisher` over a real `Bus`) |
| Locking | `Lock` + `LockManager` | `InMemoryLockManager` | `PostgresLockManager`, `SqliteLockManager` (durable leases), Redis, … |

All in-memory defaults are `Clone` and `Send + Sync`, so they work in single-task tests and multi-task servers alike. When you're ready for production, implement the trait for your infrastructure and plug it in — handler code does not change.

## The `#[sourced]` Macro

The `#[sourced]` attribute macro is the recommended way to define event-sourced aggregates. Place it on an impl block and annotate command methods with lowercase, past-tense aggregate event names such as `#[event("initialized")]`. It replaces both `#[digest]` and `aggregate!()`, and auto-generates a typed event enum plus the `Aggregate` impl.

Event methods are rewritten to return `SourcedResult`, even when the source method omits an explicit return type. Call them with `?` in application code so serialization and event-recording failures are propagated.

### Basic Usage

```rust,ignore
use distributed::{sourced, Entity};

#[derive(Default)]
struct Todo {
    entity: Entity,
    user_id: String,
    task: String,
    completed: bool,
}

#[sourced(entity)]
impl Todo {
    #[event("initialized")]
    fn initialize(&mut self, id: String, user_id: String, task: String) {
        self.entity.set_id(&id);
        self.user_id = user_id;
        self.task = task;
    }

    #[event("completed", when = !self.completed)]
    fn complete(&mut self) {
        self.completed = true;
    }
}
```

This generates:

```rust,ignore
// Typed event enum with named fields from method parameters
#[derive(Debug, Clone, PartialEq)]
pub enum TodoEvent {
    Initialized { id: String, user_id: String, task: String },
    Completed,
}

impl TodoEvent {
    pub fn event_name(&self) -> &'static str { /* ... */ }
}

// Convert stored events to typed enum
impl TryFrom<&EventRecord> for TodoEvent { /* ... */ }

// Full Aggregate trait impl (entity accessors + replay logic)
impl Aggregate for Todo { /* ... */ }
```

### Durable Stream Identity

`Aggregate::aggregate_type()` provides the type component of a persistence stream's identity (the pair `(aggregate_type, aggregate_id)`). The default uses Rust's type name for development convenience, but **production persistence should set an explicit, stable durable name**:

```rust,ignore
#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    // events are stored under the durable stream type "todo"
}
```

### Using the Typed Event Enum

The generated enum enables exhaustive matching — if you add or remove an event, the compiler tells you everywhere that needs updating:

```rust,ignore
use distributed::EventRecord;

fn print_todo_event(record: &EventRecord) -> Result<(), String> {
    let event = TodoEvent::try_from(record)?;
    match event {
        TodoEvent::Initialized { id, user_id, task } => {
            println!("Todo {} created by {}: {}", id, user_id, task);
        }
        TodoEvent::Completed => println!("Todo completed"),
    }
    Ok(())
}
```

### Custom Enum Name

```rust,ignore
#[sourced(entity, events = "TodoCommand")]
impl Todo {
    // generates TodoCommand enum instead of TodoEvent
}
```

### Versioned Events

Create events at a specific version for [upcasting](#event-upcasting--versioning):

```rust,ignore
type InitV1 = (String, String);
type InitV2 = (String, String, u8);

fn upcast_init_v1_v2((id, task): InitV1) -> InitV2 {
    (id, task, 0)
}

#[sourced(entity, upcasters(
    ("initialized", 1 => 2, InitV1 => InitV2, upcast_init_v1_v2),
))]
impl TodoV2 {
    #[event("initialized", version = 2)]
    fn initialize(&mut self, id: String, task: String, priority: u8) {
        // creates events at version 2
    }

    #[event("completed", when = !self.completed)]
    fn complete(&mut self) {
        self.completed = true;
    }
}
```

### Custom Entity Field

```rust,ignore
#[sourced(my_entity)]
impl MyAggregate {
    #[event("initialized")]
    fn create(&mut self, name: String) {
        // uses self.my_entity
    }
}
```

### With `enqueue` for Choreography

Add `enqueue` to `#[sourced]` to automatically queue events for in-process emission alongside digest. Every `#[event]` method both records to the entity stream and enqueues for emission:

```rust,ignore
use distributed::{sourced, Entity};
use distributed::emitter::EntityEmitter;

#[derive(Default)]
struct Order {
    entity: Entity,
    emitter: EntityEmitter,
    status: String,
}

#[sourced(entity, enqueue)]
impl Order {
    #[event("initialized")]
    fn create(&mut self, order_id: String, customer: String) {
        self.entity.set_id(&order_id);
        self.status = "created".into();
    }

    #[event("shipped", when = self.status == "created")]
    fn ship(&mut self) {
        self.status = "shipped".into();
    }
}
```

**Custom emitter field** — when your emitter field isn't named `emitter`:

```rust,ignore
#[sourced(entity, enqueue(my_emitter))]
impl Notifier {
    #[event("sent")]
    fn send(&mut self, id: String, message: String) {
        self.entity.set_id(&id);
        self.message = message;
    }
}
```

## The `#[digest]` Macro and `aggregate!()` Macro

The `#[digest]` and `aggregate!()` macros are the lower-level building blocks that `#[sourced]` replaces. They're still fully supported and useful when you want more granular control. Like `#[event]` methods, `#[digest]` methods become fallible and should be called with `?`.

### The `#[digest]` Macro

```rust,ignore
// Basic — captures function parameters
#[digest("initialized")]
fn initialize(&mut self, id: String, user_id: String, task: String) {
    self.entity.set_id(&id);
    self.user_id = user_id;
    self.task = task;
}

// Guard conditions — only emit when the condition is true
#[digest("completed", when = !self.completed)]
fn complete(&mut self) {
    self.completed = true;
}

// Versioned events
#[digest("initialized", version = 2)]
fn initialize(&mut self, id: String, task: String, priority: u8) { /* ... */ }

// Custom entity field
#[digest(my_entity, "initialized")]
fn create(&mut self, name: String) { /* uses self.my_entity */ }
```

### The `aggregate!` Macro

Generates the `Aggregate` trait implementation with replay logic:

```rust,ignore
aggregate!(Todo, entity, aggregate_type = "todo" {
    "initialized"(id, user_id, task) => initialize,
    "completed"() => complete(),
});
```

With [upcasters](#event-upcasting--versioning) for event schema evolution:

```rust,ignore
type InitV1 = (String, String);
type InitV2 = (String, String, u8);

fn upcast_initialized_v1_v2((id, task): InitV1) -> InitV2 {
    (id, task, 0)
}

aggregate!(Todo, entity {
    "initialized"(id, task, priority) => initialize,
    "completed"() => complete(),
} upcasters [
    ("initialized", 1 => 2, InitV1 => InitV2, upcast_initialized_v1_v2),
]);
```

## Event Metadata

Metadata lets you attach cross-cutting context — correlation IDs, causation IDs, user context, trace spans — to events without changing your domain model.

### Setting Metadata on an Entity

Set metadata on the entity before calling command methods. Every event produced by `#[event]` or `#[digest]` automatically inherits it:

```rust,ignore
let mut todo = Todo::default();

todo.entity.set_correlation_id("req-abc-123");
todo.entity.set_causation_id("cmd-create-todo");
todo.entity.set_meta("user_id", "u-42");

todo.initialize("todo-1".into(), "user-1".into(), "Ship it".into())?;

assert_eq!(todo.entity.events()[0].correlation_id(), Some("req-abc-123"));
```

Entity metadata is **transient** — it is not serialized with the entity. It is a request-scoped context you set before each command invocation.

### Propagating Metadata to Outbox Messages

Use `encode_for_entity` to create outbox messages that automatically inherit the entity's metadata context:

```rust,ignore
let outbox = OutboxMessage::encode_for_entity(
    format!("{}:created", order.entity.id()),
    "order.initialized",
    &payload,
    &order.entity,  // metadata propagates automatically
)?;

repo.outbox(outbox).commit(&mut order).await?;
```

The metadata flows through the full chain:

```text
Entity.set_correlation_id("req-123")
  → #[event] / #[digest] → EventRecord.metadata
  → encode_for_entity → OutboxMessage.metadata
  → OutboxDispatcher → transport Message.metadata
  → subscriber receives the message with correlation_id() == "req-123"
```

Framework-derived metadata (codec, destination, source aggregate) is namespaced under the reserved `x-sourced-` prefix so it cannot be shadowed by user metadata.

### Reading Metadata

```rust,ignore
// On EventRecord (event store)
event_record.correlation_id()  // Option<&str>
event_record.causation_id()
event_record.meta("user_id")

// On OutboxMessage
message.correlation_id()
message.meta("trace_id")
```

## In-Process Event Choreography (requires `emitter` feature)

The `emitter` feature adds in-process event-driven choreography — queue local events during commands and emit them after commit for reactive workflows within a single process.

### With `#[sourced(entity, enqueue)]`

Every `#[event]` method automatically records to the entity stream (for replay) and enqueues for in-process emission:

```rust,ignore
use serde::{Deserialize, Serialize};
use distributed::{sourced, Entity};
use distributed::emitter::EntityEmitter;

#[derive(Default, Serialize, Deserialize)]
struct OrderSaga {
    entity: Entity,
    #[serde(skip, default)]
    emitter: EntityEmitter,
    order_id: String,
    status: String,
}

#[sourced(entity, enqueue)]
impl OrderSaga {
    #[event("started")]
    fn start(&mut self, order_id: String) {
        self.entity.set_id(&order_id);
        self.order_id = order_id;
        self.status = "started".into();
    }

    #[event("completed", when = self.status == "started")]
    fn complete_step(&mut self) {
        self.status = "completed".into();
    }
}
```

### Emitting After Commit

Queued events are held until you explicitly emit them after a successful commit:

```rust,ignore
let mut saga = OrderSaga::default();
saga.start("order-1".into())?;

// Commit the aggregate...
repo.commit(&mut saga).await?;

// Then emit queued events to registered listeners
saga.emitter.emit_queued();
```

### Registering Listeners

```rust,ignore
let shared_state = Arc::new(Mutex::new(Vec::new()));
let state = Arc::clone(&shared_state);

saga.emitter.on("started", move |payload: String| {
    if let Ok(mut events) = state.lock() {
        events.push(payload);
    }
});
```

This pattern is useful for reactive workflows within the same process. For cross-service messaging, use the [Outbox Pattern](#outbox-pattern) and [Service Bus](#service-bus).

## Queued Repository

Per-entity async locking for serialized workflows. `get` acquires the lock, `commit` releases it:

```rust,ignore
use distributed::{AggregateBuilder, InMemoryRepository, Queueable, RepositoryError};

let repo = InMemoryRepository::new().queued().aggregate::<Todo>();

let Some(mut todo) = repo.get("todo-1").await? else {
    return Err(RepositoryError::NotFound { id: "todo-1".into() });
}; // locks this ID
// ... mutate ...
repo.commit(&mut todo).await?; // unlocks

// Or release without changes:
repo.abort(&todo).await?;

// Read without locking:
let _ = repo.peek("todo-1").await?;
```

By default, locking is in-memory (`InMemoryLockManager`) — process-local, lost
on restart. For **cross-process** serialization, back the queue with a durable
SQLx lease lock (feature `postgres` or `sqlite`). It implements the same
`LockManager` trait, so it's a drop-in via `queued_with`:

```rust,ignore
use distributed::{PostgresLockManager, PostgresRepository};

let repo = PostgresRepository::connect_and_migrate(&database_url).await?;
// The `aggregate_locks` lease table is created by the repository's migrations.
let locks = PostgresLockManager::new(repo.pool().clone());
let todos = repo.queued_with(locks).aggregate::<Todo>();
```

The lease records each held key in the `aggregate_locks` table (`SqliteLockManager`
is the SQLite equivalent). It is a **mutual-exclusion optimization, not a fencing
guarantee** — the event store's `(aggregate_type, aggregate_id, sequence)` primary
key remains the authoritative concurrency boundary. v1 has **no lease renewal**, so
set the lease TTL above your longest critical section. Tune with `with_lease_ttl`,
`with_retry_interval`, and `with_max_wait`; reclaim rows from crashed holders with
`sweep_expired`. Any custom `LockManager` (e.g. Redis) plugs in the same way.

## Persistent Repositories

The optional `sqlite` and `postgres` features add async, SQL-backed repositories
that implement the same async traits as `InMemoryRepository`. They persist aggregate
event streams, relational read-model write plans, processed-message marks,
snapshots, and outbox rows — staging everything through one SQL transaction when
committed via `CommitBatch`. They also enable SQL-backed bus transports over the
same database connection (`SqliteBus` / `PostgresBus`).

```rust,ignore
// SQLite — local persistence, conformance, and bus tables (requires `sqlite`)
let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:").await?;

// Postgres — the production SQL event-store path (requires `postgres`)
let repo = distributed::PostgresRepository::connect_and_migrate(database_url).await?;
```

`connect_and_migrate` applies the explicit migrations under `migrations/`. Plain
`connect` from an existing pool does **not** create tables implicitly, so
applications can control bootstrap order.

SQLite is the no-extra-process local durable path: one SQLite database can back
repositories, read models, the outbox, locks, and `SqliteBus` for tests, demos,
and small single-node deployments. Postgres is the low-ops starter for production:
a single Postgres cluster can back repositories, read models, the outbox, **and**
the durable transport (`PostgresBus`).

### Repository traits (async-only)

Streams are keyed by full **stream identity** `(aggregate_type, aggregate_id)`,
not bare IDs. Prefer an explicit durable `aggregate_type` in production
(`impl_aggregate!(..., aggregate_type = "...")` or the sourced/aggregate macros).

| Trait | Role |
| --- | --- |
| `GetStream` | Load one or more event streams by identity |
| `TransactionalCommit` | Commit `CommitBatch` (streams, read-model write plans, snapshots) in one backend transaction |
| `ReadModelWritePlanStore` / `RelationalReadModelQueryStore` | Relational projection write + PK load surfaces for adapters |
| `SnapshotStore` | Rebuildable snapshot cache by stream identity |
| `OutboxStore` | Claim/update durable outbox rows (workers; not aggregate rehydration) |

`InMemoryRepository` (plus in-memory read-model/snapshot stores) is the behavioral
reference for conformance tests — not a production I/O adapter. SQL adapters
implement the same traits with `sqlx`.

## Outbox Pattern

Each outbox message is a durable delivery row committed alongside your domain
entity. Aggregate event records are write-side replay history. A
domain-marked transition captures a separate canonical outward occurrence,
which is published only when the unit of work selects `publish_events()`.

```rust,ignore
let mut todo = Todo::default();
todo.entity.set_correlation_id("req-abc");
todo.initialize("todo-1".into(), "user-1".into(), "Buy milk".into())?;

// Commit replay history + the typed TodoState occurrence + outbox atomically.
// Snapshots remain a private hydration cache and are never published implicitly.
repo.publish_events().commit(&mut todo).await?;
```

For an explicitly authored outward DTO, use `publish(event)`. For low-level
integration envelopes or custom IDs, use `encode_for_entity`:

```rust,ignore
use distributed::OutboxMessage;

let message = OutboxMessage::encode_for_entity(
    format!("{}:init", todo.entity.id()),
    "todo.initialized",
    &custom_payload,
    &todo.entity,
)?;
```

### Publishing the Outbox

How a committed row reaches the bus depends on whether a bus is attached to the
service:

- **Bus attached (`service.with_bus(bus)`)** — `repo.outbox(msg).commit(agg)`
  claims the row in the commit transaction (born `InFlight` under a short lease)
  and publishes it **immediately** after commit. A crash before the publish, or a
  publish failure, leaves the row claimed under that lease; when the lease expires
  the polling worker takes it.
- **No bus** — the row is committed `pending` and a worker publishes it.

The polling worker is the durable backstop in both cases. It is the same
`OutboxDispatcher` primitive composed with your runtime's timer — run it in the
service process or as a separate worker, against the same outbox store:

```rust,ignore
use distributed::{BusPublisher, OutboxDispatcher};
use std::{sync::Arc, time::Duration};

let dispatcher = OutboxDispatcher::new(
    repo.outbox_store(),
    BusPublisher::new(Arc::new(bus)),   // routes commands/events by kind
    "outbox-worker-1",
    Duration::from_secs(30),            // claim lease
    5,                                  // max publish attempts
);

loop {
    dispatcher.dispatch_batch(100).await?;          // claim → publish → complete
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

A row completes only after `publish()` resolves `Ok`; an unknown or failed publish
leaves it retryable (released until the attempt ceiling, then moved to `Failed`).
Claims use leases, so the immediate path and competing workers never publish the
same row concurrently.

## Service Bus

The service bus is a thin, ergonomic facade over the transport adapters. It exposes
two messaging patterns through two traits:

- **`Bus` (produce)** — `send` a point-to-point command (1:1, competing consumers) or `publish` a fan-out event (1:N).
- **`BusConsumer` (consume)** — `listen` for commands (competing) or `subscribe` to events (fan-out). `listen`/`subscribe` derive the message names from the service's registered handlers, build the transport's source with the right topology, and run it through the shared runner — handler code never changes.

A concrete `*Bus` implements both, so the **application surface is identical across
transports; only the constructor line changes.**

```rust,ignore
use std::sync::Arc;
use distributed::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};

// Built once — handlers are transport-agnostic. The service name becomes the
// default durable consumer group for broker-backed buses.
let service = Arc::new(build_service().named("order-api"));

// Dev/test: in-memory.
let bus = InMemoryBus::new();
bus.send("place.bet", payload).await?;          // point-to-point command (1:1)
bus.publish("seat.reserved", payload).await?;   // fan-out event (1:N)
bus.listen(service.clone(), RunOptions::idempotent()).await?;     // competing
bus.subscribe(service.clone(), RunOptions::idempotent()).await?;  // fan-out

// Production: swap the one constructor line — send/listen/publish/subscribe
// and the handlers are unchanged. A named Service supplies the consumer group.
let namespace = "orders-prod";
//   let bus = NatsBus::connect("nats://localhost:4222").namespace(namespace).await?;
//   let bus = PostgresBus::new(pool);
//   let bus = SqliteBus::new(pool);
//   let bus = RabbitBus::connect("amqp://localhost:5672/%2f").namespace(namespace).await?;
//   let bus = KafkaBus::connect("localhost:9092").namespace(namespace).await?;
```

This is the low-level facade. For a `microsvc::Service`, the one-call convenience
is `service.with_bus(bus).run(opts)`: it derives the command names to `listen`
and the event names to `subscribe` from the registered handlers, and makes
`repo.outbox(msg).commit(agg)` publish on commit. Drop to `listen` / `subscribe`
/ `send` / `publish` directly when you need finer control.

Consumer identity controls the durable broker state in each transport. Command
handlers should normally be owned by one service deployment, with every replica
using the same `group` so the deployment competes as one logical consumer. Event
handlers use distinct `group`s when each service needs its own copy.

The `group` is not a list of handler names. Handler names come from
`subscription_plan()`; `group` tells the broker which durable consumer, offset, or
queue belongs to this running service. `Service::named(..)` supplies that group
for `service.with_bus(bus).run(..)`; direct `Handlers` or manual
`listen`/`subscribe` calls can set it with `bus.group(..)` or `Handlers::named(..)`.
Groups/service names should use portable deployment IDs (`A-Z`, `a-z`, `0-9`,
`_`, `-`); namespaces may also include `.`. Blank names, whitespace, control
characters, path separators, broker wildcards, and names longer than 128 bytes
are rejected before broker topology is created.

| `*Bus` | Feature | `send` / `listen` (competing) | `publish` / `subscribe` (fan-out) |
|---|---|---|---|
| `InMemoryBus` | (always) | named queue, popped once | retained log + per-subscriber cursor |
| `PostgresBus` | `postgres` | `bus_queue`, `FOR UPDATE SKIP LOCKED` | `bus_log` + `bus_offset` per group (Kafka-style) |
| `SqliteBus` | `sqlite` | `bus_queue`, atomic `UPDATE ... RETURNING` lease claim | `bus_log` + `bus_offset` per group |
| `NatsBus` | `nats` | shared durable `{group}_cmd` on the stream | durable `{group}_evt` per group |
| `RabbitBus` | `rabbitmq` | default exchange → durable queue `{ns}.cmd.{name}` | topic exchange → queue `{ns}.evt.{group}` per group |
| `KafkaBus` | `kafka` | shared consumer group `{ns}.{group}.cmd` | consumer group per service `{ns}.{group}.evt` |
| `KnativeBus` | `http` | POST CloudEvent → `{target}-commands` broker ingress | POST → `{source}-events` broker; consume via generated Triggers |

`SqliteBus` uses the same single-database pattern scaled down to SQLite:
`bus_queue` is claimed with a conditional `UPDATE ... RETURNING` lease because
SQLite has no `FOR UPDATE SKIP LOCKED`, and `bus_log` / `bus_offset` provide
fan-out. It is intended for local durable transport, tests, demos, and small
single-node deployments, not as a high-throughput broker replacement.

`KnativeBus` implements only `Bus` (produce → broker-ingress POST). It has no
in-process consume loop: `KnativeBus::manifests(&plan, &subscriptions)` renders the
role-based `Broker` + per-name `Trigger` YAML, and the service mounts
`cloud_events_router` so those Triggers reach `dispatch_message`.

### Idempotency and Failure Policy

`RunOptions::idempotent()` enables idempotent dispatch by default. `RunOptions` also
carries a `FailurePolicy` controlling what happens to a **permanent** handler
failure — `Retry`, `DeadLetter`, `Park`, `LogAndAck`, or `Stop`:

```rust,ignore
use distributed::bus::{FailurePolicy, RunOptions};

bus.listen(
    service.clone(),
    RunOptions::idempotent().with_failure_policy(FailurePolicy::Stop),
).await?;
```

Retryable failures (e.g. transient `NotFound`) are nacked for redelivery; the runner
never silently acks a handler error.

### Transport boundaries (producer vs consumer)

`microsvc` owns registration, guards, typed decoding, and dispatch. **Transport
adapters** own receive/ack/retry/publish and topic mapping. Shared vocabulary lives
in `bus` (no concrete broker dependency).

| Type | Purpose |
| --- | --- |
| `TransportError` / `TransportErrorKind` | Retryable vs permanent — drives redelivery vs failure policy |
| `FailurePolicy` / `FailureAction` | Permanent failure: `Retry`, `DeadLetter`, `Park`, `LogAndAck`, `Stop` |
| `RunOptions` / `ConsumerDeliveryMode` | Idempotent dispatch by default; optional inbox hook |
| `TransportCapabilities` | Per-transport durability, confirms, retry ownership, ack kind |
| `MessageSource` + `run_source` | Pull loop: dispatch then settle only after the handler finishes |
| `MessagePublisher` + `OutboxDispatcher` | Publish threshold for outbox completion; claim → publish → complete |

**Two confirmation thresholds** (do not collapse them):

1. **Producer publish** — when an outbox row may be marked published (SQL commit,
   broker confirm/ack, Knative 2xx, in-memory accept). Unknown outcomes stay retryable.
2. **Consumer ack** — only after the handler (and optional inbox receipt) committed.
   Never silently ack a handler error.

```rust,ignore
use distributed::bus::{run_source, RunOptions};

// Low-level receive loop (facade buses wrap this)
run_source(service, source, RunOptions::idempotent()).await?;
```

## Command contracts

Command semantics live in `distributed::command`, independently of the query
transport. Use `CommandInput` and `CommandOutput` to describe Serde input/output
shapes, and `command::{typed_command, Succeeded, Eventual, Atomic, PreparedCommand}`
to declare and prepare commands. The GraphQL adapter derives its surface from
those contracts; GraphQL naming restrictions are checked when exposing them.

```rust
# use distributed::command;
use distributed::{CommandInput, CommandOutput};
use distributed::command::{typed_command, Succeeded};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, CommandInput)]
struct RenameInput { id: String, title: String }

#[derive(Serialize, CommandOutput)]
struct RenameOutput { id: String }

# fn main() {
let declaration = typed_command::<RenameInput, Succeeded<RenameOutput>>("todo.rename");
# }
```

This is a breaking Rust API change. Replace `GraphqlInput`/`GraphqlOutput`
with `CommandInput`/`CommandOutput` throughout the DTO's nested types and
move command imports from `distributed::graphql` to `distributed::command`.
Manual metadata implementations now use `CommandInputType`/`CommandOutputType`,
`command_type()`, and `CommandTypeDef`/`CommandTypeField`. The old GraphQL
command re-exports, metadata types, and derives have been removed.

GraphQL remains the application's UI gateway: it exposes the command contract
alongside queries and subscriptions. Command handlers describe their data and
consistency through the core API, independently of where that gateway runs.

`Atomic<M>` continues to obtain its response shape from the relational read
model and requires the existing transaction proof. Moving command ownership
does not change Eventual projection confirmation or Atomic response sealing.

## Microservice Framework (`microsvc`)

The `microsvc` module provides a convention-based async command/event handler framework. Register handlers on typed `Routes<D>` bundles, collect them into a non-generic `Service`, then expose that service over HTTP, gRPC, the bus, or direct dispatch.

### Defining a Service

A `Routes<D>` bundle is generic over a dependency type `D` that handlers read via `ctx`. Build one fluently from `Routes::new()`: add `.with_repo(repo)` for aggregate command handlers, `.with_read_model_store(store)` for projection handlers (chain both when a handler needs both), or `.with_dependencies(deps)` for custom dependencies. Add one or more route bundles to `Service::new()` with `.routes(routes)`, then use `.with_bus(bus)` to consume from / publish to a transport.

Handlers are registered with a fluent builder. `.command(name)` / `.event(name)` start a registration; `.handle(closure)` adds an unguarded handler and `.guarded(guard, closure)` adds a guarded one. The handler closure receives `&Context<D>` and returns a future:

```rust,ignore
use std::sync::Arc;
use distributed::microsvc::{Context, HandlerError, Routes, Service, Session};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;

let routes = Routes::new()
    .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
    .command("counter.initialize")
    .handle(|ctx: &Context<Repo>| {
        let input = ctx.input::<CreateCounter>();
        async move {
            let input = input?;
            let mut counter = Counter::default();
            counter.create(input.id.clone())?;
            ctx.repo().commit(&mut counter).await?;
            Ok(json!({ "id": input.id }))
        }
    })
    .command("counter.increment")
    .handle(|ctx: &Context<Repo>| {
        let input = ctx.input::<IncrementCounter>();
        async move {
            let input = input?;
            let mut counter = ctx.repo().get(&input.id).await?
                .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
            counter.increment(input.amount)?;
            ctx.repo().commit(&mut counter).await?;
            Ok(json!({ "value": counter.value }))
        }
    });
let service = Arc::new(Service::new().routes(routes));

// Direct dispatch
let _result = service
    .dispatch("counter.initialize", json!({ "id": "c1" }), Session::new())
    .await?;
```

### Guards

`.guarded(guard, handler)` runs the guard before the handler — if it returns `false`, the command is rejected:

```rust,ignore
let routes = routes
    .command("admin.reset")
    .guarded(
        |ctx: &Context<Repo>| ctx.role() == Some("admin"),
        |_ctx: &Context<Repo>| async { Ok(json!({ "reset": true })) },
    );
```

### Handler File Convention

For larger services, organize handlers into separate files. Each handler module exports a `COMMAND` (or `EVENT` / `EVENTS`) name, a `guard`, and an async `handle`:

```rust,ignore
// src/handlers/counter_create.rs
use serde::Deserialize;
use serde_json::{json, Value};
use distributed::microsvc::{Context, HandlerError};
use super::Repo;
use crate::models::counter::Counter;

pub const COMMAND: &str = "counter.initialize";

#[derive(Deserialize)]
struct Input { id: String }

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["id"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<Input>()?;

    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!("counter {} already exists", input.id)));
    }

    let mut counter = Counter::default();
    counter.create(input.id.clone())?;

    // `counter.initialized` is domain-marked on the aggregate.
    ctx.repo().publish_events().commit(&mut counter).await?;

    Ok(json!({ "id": input.id }))
}
```

Register them with the `routes!` macro:

```rust,ignore
let routes = distributed::routes!(
    Routes::new().with_repo(InMemoryRepository::new().queued().aggregate::<Counter>()),
    command handlers::counter_create,
    command handlers::counter_increment,
);
let service = Service::new().routes(routes);
```

Event projection handlers use `EVENT` / `EVENTS` and `event handlers::...` in the same way; inside the handler, `ctx.message()` gives the raw transport `Message` and `ctx.input::<T>()` decodes its payload.

### HTTP Transport (requires `http` feature)

The `http` feature adds an axum-based HTTP transport. Every registered command becomes a `POST /:command` endpoint. Request headers flow into the `Session` verbatim — including identity claims, which the framework does **not** authenticate. Deploy behind a trusted proxy that strips client-supplied identity headers and injects authenticated ones (see [Security / Trust Boundary](#security--trust-boundary)).

```rust,ignore
use std::sync::Arc;
use distributed::microsvc;

// Get an axum Router to compose with other routes
let app = microsvc::router(service.clone());

// Or serve directly
microsvc::serve(service, "0.0.0.0:3000").await?;
```

Routes:

| Method | Path | Description |
|---|---|---|
| `POST` | `/:command` | Dispatch a command. Body = JSON input, headers = session variables. |
| `GET` | `/health` | Health check: `{ "ok": true, "commands": ["counter.initialize", ...] }` |

```bash
curl -X POST http://localhost:3000/counter.initialize \
  -H 'Content-Type: application/json' \
  -H 'x-user-id: user-42' \
  -d '{"id": "c1"}'

curl http://localhost:3000/health
```

`x-user-id` / `x-roles` are convenience keys for `Session::user_id()` /
`Session::roles()` only — not a required protocol. Your gateway can inject any
claim names; handlers read them with `session.get("…")` or map claims to the
convenience keys at the edge.

### gRPC Transport (requires `grpc` feature)

The `grpc` feature adds a tonic-based gRPC transport using standard protobuf wire format (no `.proto` file needed):

```rust,ignore
// Get a CommandServiceServer to compose with other tonic routes
let grpc_svc = microsvc::grpc_server(service.clone());

// Or serve directly
microsvc::serve_grpc(service, "[::1]:50051").await?;
```

| RPC | Input | Output | Description |
|---|---|---|---|
| `Dispatch` | `GrpcRequest` | `GrpcResponse` | Dispatch a command. `input` = JSON string, `session_variables` = metadata map. |
| `Health` | `HealthRequest` | `HealthResponse` | Health check. |

Session handling mirrors HTTP — gRPC metadata headers are merged with payload `session_variables`. **Transport metadata (trusted, proxy-injected) takes precedence over the client-controlled payload**, so a client cannot spoof identity via the request body. See [Security / Trust Boundary](#security--trust-boundary) below. Errors are returned inside `GrpcResponse.status` (HTTP-style status codes) with internal (5xx) error detail masked to a generic message, keeping client behavior identical across transports.

### Bus Transport

Attach a bus with `service.with_bus(bus)` and drive it with `run(opts)`: it
derives `listen` (point-to-point commands) and `subscribe` (fan-out events) from
the registered handlers, and makes `repo.outbox(msg).commit(agg)` publish on
commit. The same `Service` can handle commands from multiple transports
simultaneously — HTTP, gRPC, bus, and direct dispatch all share the same handlers
and repository. For finer-grained control, call the `listen` / `subscribe` facade
methods directly. See [Service Bus](#service-bus) above.

### Error Handling

`HandlerError` maps to HTTP-style status codes:

| Variant | Status Code |
|---|---|
| `UnknownCommand` | 404 |
| `DecodeFailed` | 400 |
| `GuardRejected` | 400 |
| `Rejected` | 422 |
| `NotFound` | 404 |
| `Unauthorized` | 401 |
| `Repository` | 500 |
| `Other` | 500 |

Internal (5xx) errors are **masked** before being returned to clients — the
response body carries a generic `"Internal server error"` so SQL text, driver
detail, or internal paths never leak. The original error is logged
server-side. Client-fault (4xx) errors keep their descriptive message. This
applies identically to the HTTP and gRPC transports.

### Security / Trust Boundary

**This framework does NOT authenticate requests.** The `Session` is an opaque
string map built from whatever the transport provides — HTTP request headers,
gRPC metadata, and (for gRPC) the request payload's `session_variables`.
Identity claims are trusted at face value by handlers. Claim **names** are
deployment convention, not a fixed protocol (`Session::user_id` /
`Session::roles` only look up the convenience keys `x-user-id` / `x-roles`).

You **must** deploy `microsvc` behind a **trusted proxy / API gateway**
(JWT middleware, authenticating ingress, a query-layer action such as Hasura,
a custom BFF, …) that:

- **Strips** any client-supplied identity headers/metadata on the way in, and
- **Injects** only identity claims it has authenticated.

Without that proxy, any caller can set identity keys and assume any identity
or role.

**Source precedence:** when identity arrives in more than one place, the trusted
transport channel wins over the client-controlled payload. For gRPC, transport
**metadata overrides** payload `session_variables` — a client cannot override a
proxy-injected subject claim via the request body. For HTTP, request headers
populate the session and the proxy is responsible for ensuring they are
authenticated. Never trust the request body for identity.

## Read Models

Read models are query-optimized relational projections derived from aggregates, event records, or published messages. They are written as declared relational rows using table metadata from `#[derive(ReadModel)]`. Use JSON/JSONB columns for whole-view or semistructured fields.

### Defining a Read Model

```rust,ignore
use serde::{Deserialize, Serialize};
use distributed::ReadModel;

#[derive(Clone, Debug, Serialize, Deserialize, ReadModel)]
#[table("game_views")]
pub struct GameView {
    #[id]
    pub id: String,
    pub player_name: String,
    pub score: i32,
    #[jsonb]
    pub metadata: serde_json::Value,
}
```

### Atomic Commits (Read Model + Aggregate)

When the response to a command must include the fully consistent, updated view, commit the aggregate and read model together in one transaction:

```rust,ignore
use distributed::{ReadModelWritePlanCommitExt, ReadModelWritePlanBuilder};

// Player submits a move
game.make_move(player_move)?;

// Build the view from the updated aggregate
let view = GameView::from(&game);

// Commit aggregate + view in one transactional batch
let mut read_models = ReadModelWritePlanBuilder::new();
read_models.upsert(&view)?;
repo.read_models(read_models).commit(&mut game).await?;

// Return `view` to the client — it reflects the committed state
```

For related rows, build the same structured write plan:

```rust,ignore
let mut read_models = ReadModelWritePlanBuilder::new();
read_models.upsert(&player_view)?;
read_models.upsert_related(&player_view, "weapons", &weapon_view)?;
repo.read_models(read_models).commit(&mut game).await?;
```

This is a deliberate consistency tradeoff: the read model is in sync with the aggregate only when the repository can write both in the same transaction boundary (`TransactionalCommit`). For cross-service or cross-database views, use the eventually consistent outbox/projector pattern instead.

### Eventual Projection

Distributed projectors subscribe to published messages and commit read-model rows through a workspace, marking the message processed in the same adapter transaction for SQL idempotency:

```rust,ignore
use distributed::ReadModelWorkspaceExt;

let mut workspace = ctx.read_model_store().workspace();
workspace.upsert(&row)?;
workspace.commit().await?;
```

### Loading

```rust,ignore
use distributed::{ReadModelWorkspaceExt, RowKey, RowValue};

let loaded = repo
    .workspace()
    .load::<GameView>(RowKey::new([("id", RowValue::String("view-1".into()))]))
    .one()
    .await?;
```

### Relational metadata, includes, and schema

- **Derive:** `#[derive(ReadModel)]` + `#[table("...")]` (or `#[readmodel(table = "...")]`)
  emit `RelationalReadModel` metadata, row conversion, PKs, indexes, FKs, and an
  adapter-owned version column. Use `#[id]`, `#[index]` / `#[unique]`,
  `#[readmodel(jsonb)]`, and relationship attributes (`has_many` / `belongs_to` /
  `many_to_many` + `foreign_key` / `through` / `target_foreign_key`).
- **Relationship keys:** `foreign_key` lists columns on the FK-holding table in
  the other end's PK order (comma-separated, same arity as that PK). GraphQL
  ANDs those equalities for `has_many` and `belongs_to`, including composite
  identities. `many_to_many` uses a `through` table that holds each end's full
  PK (same-named columns, or `foreign_key` / `target_foreign_key` in PK order).
  A one-column `foreign_key` on a composite PK is an error, not a silent
  `.first()`.
- **Writes:** `ReadModelWritePlan` / workspace `upsert` + `commit` (same transaction
  as events when staged on `CommitBatch`).
- **Internal loads:** PK-anchored includes —
  `store.workspace().load(...).include(...).one()` (one-level, opt-in;
  `has_many` / `belongs_to` only, single-column join). Nested queries go
  through GraphQL.
- **Schema lifecycle:** `ReadModelSchemaRegistry` + adapter for migration artifacts
  and startup verification; `distributed schema` / `read_model_catalog()` for SQL.
- **Non-goals:** public query APIs belong on the GraphQL layer below (not the ORM
  include loader); do not write projections outside the projection path.

```rust,ignore
#[derive(Clone, Debug, ReadModel)]
#[readmodel(table = "projects", primary_key = ["workspace_id", "path"])]
pub struct ProjectView {
    pub workspace_id: String,
    pub path: String,
    #[readmodel(belongs_to = "WorkspaceView", foreign_key = "workspace_id")]
    pub workspace: Option<WorkspaceView>,
    #[readmodel(has_many = "ProjectFileView", foreign_key = "workspace_id,path")]
    pub files: Vec<ProjectFileView>,
    #[readmodel(
        many_to_many = "LabelView",
        through = "project_labels",
        foreign_key = "workspace_id,path",
        target_foreign_key = "label_id"
    )]
    pub labels: Vec<LabelView>,
}
```

## GraphQL query service

Auto-generated GraphQL over relational read models — Hasura-style filtering,
ordering, pagination, relationships, role-based column allowlists and row
filters, live subscriptions after write-plan commits, and typed command
mutations derived from the executable `Service` (including `Atomic<T>` and
`Eventual<T>` + projector paths).

This is the public query/command edge for full-stack apps. The companion
TypeScript package [`@hops-ops/distributed`](js/) (see
[`js/README.md`](js/README.md)) supplies transport, a normalized causal replica,
command runtime, diagnostics, and SvelteKit/React adapters. End-to-end template:
[`tests/e2e-ui/`](tests/e2e-ui/). Scaffold with `distributed scaffold … --query-api`.
Example playground: `cargo run --example graphiql --features "graphql,sqlite"`.

### Enable

```toml
# Query engine + SQLite dialect (local / tests)
distributed = { version = "0.1", features = ["graphql", "sqlite"] }

# Production-shaped: GraphQL + Postgres repository/bus
distributed = { version = "0.1", features = ["graphql", "postgres"] }
```

`graphql` implies `http` (Axum router, including `/graphql/ws`). SDL helpers under
`distributed::graphql::{naming,sdl}` compile without the feature so
`distributed schema --format graphql` works in tooling crates.

### Scope

| In | Out |
|---|---|
| `SELECT`-only query surface from `TableSchema` / read models | Table mutations / write-to-projection via GraphQL |
| Nested `has_many` / `belongs_to` / `many_to_many`, including composite keys | ORM `include()` of m2m or composite direct joins |
| Role column allowlists + row filters (`claim(...)`) | Full IdP product UI (login pages live in your app / Auth.js) |
| **First-class OIDC Bearer validation** (JWKS, iss/aud/exp, claim → session) | Assuming raw HTTP microsvc routes authenticate without a proxy or GraphQL edge |
| SQLite + Postgres dialects | Cross-service federation / remote schemas |
| Typed causal command mutations (`Service` → GraphQL) | Raw JSON GraphQL command registries |
| Live list subscriptions via commit-path invalidation | Querying outbox / event-store operational tables |

### Mount on a service

```rust,ignore
use distributed::command::{typed_command, Eventual};
use distributed::graphql::{claim, col, read, GraphqlEngine};
use distributed::microsvc::{Routes, Service};

let routes = Routes::new()
    .with_repo(repository.clone().aggregate::<Todo>())
    .typed_command(
        typed_command::<CreateTodoInput, Eventual<TodoStatusPayload>>("todo.create")
            .field_name("todos_create")
            .roles(["user", "admin"])
            .emits(distributed::events![TodoCreatedDomainEvent])
            .applies(/* state_preview! binding for optimism */),
    )
    .handle(create_todo)
    .typed_command(
        typed_command::<ForceArchiveInput, Eventual<TodoStatusPayload>>("todo.force_archive")
            .field_name("todos_force_archive")
            .roles(["admin"])
            .emits(distributed::events![TodoArchivedDomainEvent]),
    )
    .handle(force_archive);

let service = Service::new()
    .named("todos")
    .routes(routes);

let engine = GraphqlEngine::from_schema_catalog(&manifest, &repository)?
    // This exact executable inventory is the only mutation source.
    .service(&service)
    // Stable nonzero deployment secret shared by replicas of this endpoint.
    .protocol_token_key(protocol_token_key)
    .roles(&["user", "admin", "anonymous"])
    .permission::<TodoView>(
        "user",
        read()
            .all_columns()
            .rows(col("owner_id").eq(claim("x-user-id"))),
    )
    .permission::<TodoView>("admin", read().all_columns())
    .graphiql(true) // local only — see GraphiQL section
    .build()?;

let service = service.try_with_graphql(engine)?;

// POST /graphql           — queries + command mutations
// GET  /graphql           — GraphiQL when enabled
// GET  /graphql/ws        — subscriptions (graphql-transport-ws / graphql-ws)
```

### Permissions (deny by default)

Three axes — **grant** a role, **columns** they may see, **rows** they may access.
Unmentioned models/roles fail closed (that is the deny). There is no separate
`.deny()` list: omit the role, narrow columns, or tighten `.rows(...)`.

```rust,ignore
use distributed::graphql::{read, col, claim, ModelPermissions};

ModelPermissions::new()
    .grant(
        "user",
        read()
            .all_columns()
            .rows(col("owner_id").eq(claim("x-user-id"))),
    )
    .grant("admin", read().all_columns()) // all rows
    .grant("anonymous", read().columns(["id", "status"]));
```

Row predicates can bind session claims (`claim("x-user-id")`, …) so multi-tenant
RLS lives in the engine, not ad-hoc handler SQL.

### Identity (first-class OIDC)

Auth is a **built-in GraphQL concern**, not a separate product you wire after the
fact. The engine validates tokens, maps claims into a `microsvc::Session`, and
feeds the same claim map into RLS (`claim("x-user-id")`, roles, …). Modes live
under [`src/graphql/identity/`](src/graphql/identity/):

| Mode | When to use |
|---|---|
| **`OidcBearer`** | **Default for public edges:** JWT access tokens (`Authorization: Bearer …`), JWKS (incl. discovery), iss/aud/exp/nbf, alg allowlist (no `alg=none`), claim → engine roles. Configure with `OIDC_ISSUER` / `OIDC_AUDIENCE` (and related). |
| **`TrustedProxy`** | Mesh/gateway already authenticated; inject trusted headers, strip client spoofing. |
| **`Hybrid`** | Bearer when present, else trusted proxy headers. |
| **`DevHeaders`** | Local only: ambient `x-user-id` / `x-roles`. **Never** on a public edge. |

Scaffolds prefer **`OidcBearer`** whenever OIDC env is set — not DevHeaders.

**Provider-portable by design.** Live compose + bootstrap + e2e binaries ship for:

- **Zitadel** — `tests/graphql_oidc_zitadel` + `scripts/oidc-zitadel-up.sh` (JWT-bearer mint; also powers e2e-ui login)
- **Keycloak** — `tests/graphql_oidc_keycloak` + `scripts/oidc-keycloak-up.sh` (client_credentials + realm roles)
- **Authentik** — `tests/graphql_oidc_authentik` + `scripts/oidc-authentik-up.sh` (client_credentials + groups)

Shared assertions (E1–E8): discovery/JWKS, happy path, role isolation, multi-audience / `azp`, expired and forged tokens, etc. Generic OIDC also works for SaaS IdPs (e.g. Okta) without a dedicated compose stack.

**WebSocket subscriptions:** browsers cannot set `Authorization` on the upgrade.
Clients send the access token in `connection_init` (`authorization` /
`accessToken` / nested headers). Do not put long-lived tokens in query strings
for production. e2e-ui chat demonstrates the OIDC path.

> **Note:** Raw microsvc HTTP/gRPC routes still treat `Session` as opaque unless
> you terminate auth at a proxy **or** put the public API on GraphQL
> (`OidcBearer`). The GraphQL edge is where first-class token validation lives.

### Command mutations vs HTTP commands

Command fields on the GraphQL schema are an RPC facade: same guards, same
handlers, same outbox/projector path as other transports. Prefer a
**GraphQL-only public API** for browser apps (HTTP command routes stay off unless you call `.with_http_command_routes()`)
so the edge is one protocol. Handler guards should require a session user
(and role where needed); never trust client-supplied owner fields over the
session principal.

### Live subscriptions

After projectors commit read-model rows, a `ChangeHub` invalidates matching
subscriptions so clients receive updated lists without polling. Wire projectors
to the same pool the engine uses; the e2e-ui chat subscription is the reference.

### GraphiQL

```bash
cargo run --example graphiql --features "graphql,sqlite"
# open http://127.0.0.1:4000/graphql  (override with GRAPHIQL_ADDR)
```

GraphiQL is a **developer** tool. Default headers in the playground trust
`x-roles` / `x-user-id` (DevHeaders-style). For real services:

- Prefer **`graphiql(false)`** or env policy (`GRAPHIQL=0`, production
  `RUST_ENV` / `graphiql_enabled_from_env`) so production never ships the IDE.
- Treat GraphiQL + DevHeaders as local-only; pair public scaffolds with
  `OidcBearer`.

### Client surfaces, generated artifacts, and CI

```bash
# Optional human-readable GraphQL SDL artifact
distributed schema --format graphql --out schema.graphql
git diff --exit-code schema.graphql   # drift gate
```

The Rust `Service` inventory and GraphQL `Surface` IR are the source of truth
for schema, authorization, commands, optimistic effects, and client artifacts.
`distributed client-manifest` exports one role or named application surface, and
`distributed client` compiles that manifest with co-located `.graphql` operations into
typed query/live/command modules. Common and elevated applications use separate
manifest entrypoints, document sets, generated directories, virtual modules,
and request-local replicas; an admin superset is never bundled into the common
client.

For an application such as `tests/e2e-ui`:

```bash
distributed build  # manifest + clients + Svelte check/build + atomic activation
distributed dev    # same generation lifecycle with supervised API/UI processes
```

Application code does not invoke the client compiler directly or commit its
outputs. `distributed client-manifest` and `distributed client` are internal
compiler primitives used by the lifecycle, not parallel application workflows.

See [`js/README.md`](js/README.md) for the package API and
[`tests/e2e-ui/README.md`](tests/e2e-ui/README.md) for the complete integration
flow.

### Full-stack template (`tests/e2e-ui`)

Copyable product shape (not a toy workshop): multi-crate domains, GraphQL-only
edge, real OIDC, SSR, live subscriptions, and a teaching **Blob** aggregate that
uses `Atomic<BlobGames>` (direct placement: same mutation IR as
eventual, applied in the command handler so the response can carry the row —
no async blob event handler).

| Piece | Role |
|---|---|
| Domain crates | Pure aggregates: todos, chat, blob |
| Read models | Eventual projector rows (todos/chat) *and* handler-owned `Atomic` rows (blob) — one mutation IR, different apply site |
| GraphQL edge | Owner RLS, admin surfaces, joins to `auth_users`, chat live sub, blob commands |
| Identity | Zitadel + Auth.js (PKCE), optional Zitadel user-scrape → `auth_users` |
| SvelteKit | `$distributed` / `$distributed/admin`, SSR from co-located `+page.graphql`, hydration, generated live ops + optimistic commands |
| Suite | GraphQL-only edge, IDOR, OIDC isolation, Playwright (incl. projected-move races) |

```bash
cd tests/e2e-ui
make up
distributed dev
# UI http://127.0.0.1:5180  ·  API GraphQL http://127.0.0.1:8791/graphql
# /todos  /chat  /blob  /admin  /login
make test         # domain + behavioral + JS-backed UI build/typecheck/tests

# Same UI against celld (Todo + chat.post wait-dispatch; @live stays on GraphQL)
make up-celld-nats && cd ../e2e-celld && make run
```

### TypeScript client (`js/` → `@hops-ops/distributed`)

| Export | Purpose |
|---|---|
| `@hops-ops/distributed` | Typed documents, HTTP GraphQL client, identity helpers |
| `…/replica` | Normalized causal replica, command runtime, projected fences |
| `…/sveltekit` | Vite virtual modules, SSR load/hydrate, app shells |
| `…/react` | Optional React hooks adapter |
| `…/diagnostics` | Client diagnostics helpers |

The lifecycle internally lowers the Rust surface through:

```bash
distributed client-manifest …
distributed client …
```

Application authors use `distributed build` and `distributed dev`; these
low-level commands are documented for compiler integration and diagnostics.

See [`js/README.md`](js/README.md) for package API and packaging.

### Tests in this repo

| Suite | Focus |
|---|---|
| `tests/graphql_*` | Engine, HTTP, SDL, dialects, harden (authz/DoS/inject), causal transport |
| `tests/graphql_identity` | Always-on OIDC/JWT matrix (mock JWKS; no Docker) |
| `tests/graphql_oidc_{zitadel,keycloak,authentik}` | **Live** multi-IdP e2e (compose + real JWKS; gated) |
| `tests/typed_commands` | Eventual / Atomic / Succeeded command registration |
| `tests/e2e-ui` | Multi-crate product template + SvelteKit + Zitadel UI login + Playwright |
| `js/tests` | Replica, command runtime, adapters |
| `examples/graphiql.rs` | Seeded local playground |

```bash
cargo test --test graphql_engine --features "graphql,sqlite"
cargo test --test graphql_identity --features "graphql,sqlite"
cargo test --test graphql_harden --features "graphql,sqlite"
cd js && npm run quality
# Live IdPs (optional):
#   ./scripts/oidc-zitadel-up.sh && set -a && source graphql-oidc.env && set +a
#   cargo test --test graphql_oidc_zitadel --features graphql,sqlite
# Full UI matrix: cd tests/e2e-ui && make test
```

## Snapshots

As aggregates accumulate events, replaying from scratch gets expensive. The framework keeps aggregate events as the durable source of truth and stores repository snapshots as a rebuildable hydration cache. A snapshot cache record can be deleted and rebuilt from events without changing aggregate correctness.

### Making an Aggregate Snapshottable

Add `#[derive(Snapshot)]` to your aggregate struct. This generates a state snapshot payload DTO (e.g. `TodoSnapshot`), a `fn snapshot()` method, and the full `impl Snapshottable`:

```rust,ignore
use distributed::{Entity, Snapshot};

#[derive(Default, Snapshot)]
struct Todo {
    entity: Entity,
    user_id: String,
    task: String,
    completed: bool,
}
```

Fields with `#[serde(skip)]` (like `emitter: EntityEmitter`) are automatically excluded.

**Custom ID key** — when the entity ID maps to a domain field like `sku`:

```rust,ignore
#[derive(Default, Snapshot)]
#[snapshot(id = "sku")]
struct Inventory {
    entity: Entity,
    sku: String,
    available: u32,
}
```

**Custom entity field name**:

```rust,ignore
#[derive(Default, Snapshot)]
#[snapshot(entity = "my_entity")]
struct Widget {
    my_entity: Entity,
    name: String,
}
```

### Using Snapshots

Chain `.with_snapshots(frequency)` onto any aggregate repository. The frequency is how many events between automatic snapshots:

```rust,ignore
use distributed::{AggregateBuilder, InMemoryRepository, Queueable, RepositoryError};

let repo = InMemoryRepository::new()
    .queued()
    .aggregate::<Todo>()
    .with_snapshots(10); // snapshot every 10 events

// Commit works normally — snapshots are created automatically at the threshold
let mut todo = Todo::default();
todo.initialize("todo-1".into(), "user-1".into(), "Ship it".into())?;
repo.commit(&mut todo).await?;

// Load transparently restores from the latest snapshot + replays newer events
let Some(todo) = repo.get("todo-1").await? else {
    return Err(RepositoryError::NotFound { id: "todo-1".into() });
};
```

### How It Works

- **On commit**: If `entity.version().saturating_sub(snapshot_version) >= frequency`, the aggregate's state is serialized via `create_snapshot()` and staged into the same commit transaction as the event append.
- **On load**: If a usable snapshot cache record exists, the aggregate is restored from its payload and only events with `sequence > snapshot.version` are replayed. Invalid, incompatible, or ahead-of-stream cache records fall back to full replay.
- **Storage**: Snapshot cache records are stored separately from the event stream, keyed by full stream identity. They carry aggregate type, aggregate ID, covered event version, snapshot payload type/version, codec metadata, cache metadata, and timestamp.

## Event Upcasting / Versioning

Event schemas evolve over time. When you add a field to an event (e.g., `priority` to `Initialized`), old serialized events in storage can't deserialize into the new type. **Upcasters** solve this: typed functions that transform old event payload shapes into the current format at read time, without modifying stored data.

### Defining an Upcaster

An upcaster is a plain function that converts a typed payload from one version to the next. The crate handles payload decoding and encoding:

```rust,ignore
type InitV1 = (String, String);
type InitV2 = (String, String, u8);

/// Upcasts Initialized v1 (id, task) → v2 (id, task, priority)
fn upcast_init_v1_v2((id, task): InitV1) -> InitV2 {
    (id, task, 0)
}
```

### Registering Upcasters

With `#[sourced]`, add upcasters directly in the attribute:

```rust,ignore
#[sourced(entity, upcasters(
    ("initialized", 1 => 2, InitV1 => InitV2, upcast_init_v1_v2),
))]
impl Todo {
    #[event("initialized", version = 2)]
    fn initialize(&mut self, id: String, task: String, priority: u8) {
        self.entity.set_id(&id);
        self.task = task;
        self.priority = priority;
    }

    #[event("completed", when = !self.completed)]
    fn complete(&mut self) {
        self.completed = true;
    }
}
```

Old events stored as `(id, task)` at v1 are transparently upcast to `(id, task, 0u8)` at v2 during hydration. New events are created at v2 via the `version = 2` parameter on `#[event]`.

### Chaining Upcasters

Upcasters chain automatically. Each transforms one version to the next (v1→v2→v3):

```rust,ignore
#[sourced(entity, upcasters(
    ("initialized", 1 => 2, InitV1 => InitV2, upcast_init_v1_v2),
    ("initialized", 2 => 3, InitV2 => InitV3, upcast_init_v2_v3),
))]
impl Todo { /* ... */ }
```

A v1 event automatically chains through v1→v2→v3; a v2 event only goes through v2→v3; a v3 event passes through unchanged.

### How It Works

- **On hydrate**: Before replaying events, the aggregate's registered upcasters are applied by event name and version.
- **On snapshot hydrate**: Only post-snapshot events are upcast — the snapshot already contains the current state.
- **No stored data modified**: Upcasters are read-time transformations.
- **Zero overhead when unused**: Aggregates with no upcasters take the fast hydration path.

## Service CLI (`distributed`)

The [`distributed_cli`](distributed_cli/) crate ships `distributed` — tooling to scaffold
services, inspect a service's logical application artifact, and render physical
read-model schema artifacts. It is
also a library, so `hops` mounts the same commands under `hops service` (anything
below as `distributed <cmd>` works as `hops service <cmd>`).

The CLI exists to keep the generated and handwritten parts of a back-end service
separate. A Distributed service should usually reduce to a small custom surface:
aggregate models, command/event handlers, read models, and the occasional
handwritten integration. The framework, macros, application artifacts, and CLI generate the
repeatable wiring around that surface.

That boundary matters for AI-assisted development. AI generation is
probabilistic, so Distributed tries to make the AI-authored surface small and
make the surrounding structure deterministic. Event storming produces commands,
past-tense events, aggregates, policies, and read models. Those names map
directly onto Distributed conventions, so an AI assistant can generate or revise
a smaller target: model fields, event methods, handler bodies, and projection
shapes. Boilerplate service setup, manifest discovery, schema output, and GitOps
artifacts stay deterministic.

For an existing typed `Service`, derive the logical application from its command
inventory and the same full GraphQL `Surface` used by the runtime:

```rust,ignore
let surface = distributed::SurfaceSpec::from_surface("catalog", &full_surface)?;
let application = service.application("catalog", surface)?;
let manifest = application.manifest();
```

Command namespaces such as `orders.submit` supply module names; there is no
second module list to maintain. Assembly rejects commands missing from the
Surface or exposed by the Surface without a Service owner. Role-selected client
exports remain authorization views of that full contract. Complete application
manifests are bounded at 4 MiB; each opaque JSON contract remains bounded at
1 MiB, with the existing collection, string and nesting limits still enforced.

An event-driven policy that emits through another aggregate can explicitly carry
the incoming command's causal identity before recording its events:

```rust,ignore
ctx.inherit_causation(&mut downstream)?;
downstream.record(observation)?;
```

This requires an incoming causation ID and does not manufacture one for external
events. NATS preserves the message's payload content type and keeps reserved
transport headers out of user metadata across delivery.

```bash
cargo install distributed_cli            # installs `distributed`

distributed scaffold orders \
  --model order \
  --read-models \
  --command order.submit \
  --event order.submitted \
  --store postgres \
  --transport http \
  --bus nats \
  --gitops \
  --metrics prometheus

cd orders
cargo test
distributed describe                  # print the ApplicationManifest as JSON
distributed schema --dialect postgres # render migration SQL from read models
```

For a full application, run `distributed build` or `distributed dev` from its
Cargo workspace root. The CLI discovers the typed application, runtime binary,
conventional `ui/` SvelteKit app, and `@hops-ops/distributed` dependency. A
published app must use the same exact Distributed version in Cargo, the CLI,
and npm because those artifacts ship as one release; a mismatch fails before
the build and names all three resolved versions.

A UI may instead use a local dependency such as
`"@hops-ops/distributed": "file:../../../js"`. When Rust, CLI, and JS resolve
from the same checkout, the lifecycle installs that package's build
dependencies, rebuilds stale source, verifies its exported files, and records a
receipt under the application `.distributed/` directory. `distributed dev`
keeps the linked package compiled while Vite handles application HMR. Changed
framework outputs are built in isolation and published content-by-content, so
unchanged files do not create a Vite reload storm. The framework receipt is a
declared application input, so a real framework change still activates a new
coherent generation. There is no separate JS preparation command for
application authors.

When `ui/distributed.clients.json` is present, `distributed build` and
`distributed dev` also own client generation. The inventory and
`ui/distributed.config.js` become declared lifecycle inputs; generated client
trees are stored beside the typed application manifest in the same immutable
generation. A clean checkout contains no generated client tree. A
GraphQL document or colocated binding edit invalidates only the client node,
while a Rust surface edit invalidates the application node and its client
successor. Failed client compilation never advances the active pointer.

### Coherent development reloads

CSS and HMR-safe Svelte modules stay on Vite's native fast path. A Rust,
generated-client, worker, required WASM, or linked-framework change that needs
the whole application follows one bounded transaction:

```mermaid
sequenceDiagram
    participant Edit as Source edit
    participant Dev as distributed dev
    participant Browser as SvelteKit browser
    participant API as API / workers

    Edit->>Dev: declared input changes
    Dev->>Dev: stage manifest + generated clients; validate pending generation
    Dev-->>Browser: preparing(from, to, compatibility)
    Browser->>Browser: close command gate; capture declared state + confirmed replica
    Browser-->>Dev: bounded prepare acknowledgement
    Dev->>API: restart affected processes with pending IDs
    Dev->>Dev: readiness probes; atomically activate
    Dev-->>Browser: active generation
    Browser->>Browser: one controlled document reload
    Browser->>Browser: restore compatible partitions; revalidate the rest
```

The browser capsule is size/depth/deadline bounded. It contains only explicitly
declared JSON state, confirmed replica data, and pending command IDs—never auth
credentials or command inputs. Browser commands close before optimism and
transport, while API mutations and worker/service message dispatch fail closed
until the process generation is active. A failed prepare, process readiness
probe, or activation leaves the prior generation active and continues watching
for the next edit.

Use the event-storming board as the input:

- Aggregates become `--model <name>`.
- Commands become `--command <aggregate.action>`.
- Events and policy/projection subscriptions become `--event <fact.happened>`.
- Query views become `--read-models`, then concrete `#[derive(ReadModel)]`
  structs in the generated service.

The scaffold is intentionally a starting point. Replace placeholder aggregate
fields, event methods, guards, handler bodies, and read model columns with the
domain behavior discovered in the session. If a service needs custom code outside
those conventions, write normal Rust and keep the generated manifest updated.

The `--metrics prometheus` scaffold option enables Distributed's `/metrics`
endpoint and, when paired with `--gitops`, emits Prometheus Operator
`ServiceMonitor` and `PrometheusRule` templates for HTTP services. The generated
values keep those CRDs disabled until an environment explicitly enables them.
Bus-only and worker services can expose the same registry on a side port with
`distributed::metrics::serve_http("0.0.0.0:9100", Some("orders-worker")).await?`,
or compose `distributed::metrics::http_router_for_service("orders-worker")` into
an existing Axum app. Scrape `GET /metrics` (Prometheus text). Keep `/metrics` on
a **private** listener — unauthenticated by design.

**Label policy (closed set):** `service`, `message_kind`, `message`, `status`,
`transport`, `outcome`, `failure_class`, `action`, plus GraphQL `root_field` when
applicable. Do **not** label metrics with `user_id`, `tenant_id`, free-form paths,
or raw command input (unknown commands bucket as `message=unknown`).

`describe`/`schema` compile your crate and call explicit artifact entrypoints
(override with `--entrypoint`). `describe` reads the logical
`application_manifest()` owner; `schema` reads the separate
`read_model_catalog()` owner that registers the [read models](#read-models) and
tables defining physical schema:

```rust,ignore
pub fn read_model_catalog() -> distributed::ReadModelCatalog {
    distributed::ReadModelCatalog::new("orders").read_model::<OrderView>()
}
```

### Apply schema in-cluster with Atlas

`distributed schema --format atlas` wraps the desired-state SQL into an `AtlasSchema`
(`db.atlasgo.io/v1alpha1`) for the [ariga atlas-operator](https://github.com/ariga/atlas-operator),
so migrations apply declaratively in-cluster. The resource is written to
**stdout** — redirect it wherever you keep schema manifests (a file, or a separate
GitOps repo); `distributed` does not choose a location for it.

```bash
distributed schema --format atlas --name orders --db-secret orders-db > orders.schema.yaml
```

Use `--db-secret`/`--db-secret-key` for a Secret reference (GitOps-friendly) or
`--db-url` for an inline dev URL; `--namespace` and `--dev-url` are optional. Full
reference: [`distributed_cli/README.md`](distributed_cli/README.md).

## Project Structure

```text
src/
  aggregate/      # Aggregate trait, hydration, async aggregate repository helpers
  bus/            # Bus facade + adapters (in-memory, sqlite, postgres, nats, rabbitmq, kafka, knative)
  commit_builder/ # Transactional batches for aggregates, outbox, and read models
  emitter/        # In-process event emitter helpers (feature = "emitter")
  entity/         # Entity, event records, metadata, upcasting codecs
  graphql/        # Query service: engine, permissions, identity, SDL, HTTP/WS (feature = "graphql")
  in_memory_repo/   # In-memory repository (implements every async trait)
  lock/           # Lock + lock manager traits, in-memory locks
  microsvc/       # Command/event handler framework: service, context, session
  outbox/         # Durable outbox message + commit extension
  outbox_worker/  # Outbox claiming, publishing, workers
  postgres_repo/  # Postgres async SQL repository (feature = "postgres")
  queued_repo/    # Queue-based locking repository wrapper
  read_model/     # Read model store traits, in-memory store, schema metadata
  snapshot/       # Snapshot store traits, in-memory store, snapshot repository
  sqlite_repo/    # SQLite async SQL repository (feature = "sqlite")
  table/          # Neutral table/row primitives shared by read models and ops tables
  lib.rs          # Public exports
distributed_macros/
  src/            # Proc macros: sourced, digest, aggregate, enqueue, ReadModel, Snapshot
js/               # @hops-ops/distributed JS/TS client, command runtime, and SvelteKit adapter
tests/e2e-ui/     # Full-stack CQRS + GraphQL + SvelteKit template (nested workspace)
migrations/       # Explicit SQLite and Postgres migrations
compose.yaml      # Local postgres / rabbitmq / kafka / nats for integration tests
```

## Running Tests

```bash
cargo test                  # default feature set
cargo test --features emitter
cargo test --features http
cargo test --features grpc
make test                 # starts compose and runs full local coverage
cargo test --all-features   # all features; broker tests skip without env vars
```

### Transport Integration Tests

The transport adapters have integration tests against real brokers or a local
SQLite database. Broker tests are feature-gated and **skip when their env var is
unset**; SQLite uses a temporary database file and needs no Docker service.

```bash
docker compose up -d   # postgres, rabbitmq, kafka, nats (see compose.yaml)

DATABASE_URL=postgres://sourced:sourced@localhost:5432/distributed \
  cargo test --test postgres_transport --features postgres
cargo test --test sqlite_transport --features sqlite
NATS_URL=nats://localhost:4222 \
  cargo test --test nats_transport --features nats
AMQP_URL=amqp://guest:guest@localhost:5672/%2f \
  cargo test --test rabbitmq_transport --features rabbitmq
KAFKA_BROKERS=127.0.0.1:9092 \
  cargo test --test kafka_transport --features kafka
```

Each external broker has a matching reusable GitHub Actions job
(`.github/workflows/integration-*.yaml`) that runs on PRs and on push to `main`.

## Coverage Reporting

This project uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov):

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov

cargo llvm-cov --all-features --summary-only
cargo llvm-cov --all-features --lcov --output-path lcov.info
```

CI also publishes `lcov.info` as a workflow artifact and attempts an optional Codecov upload.

## Examples

**Start here (product demos):** [See it run](#see-it-run) — e2e-ui (`tests/e2e-ui`),
e2e-celld (`tests/e2e-celld`), Blob game, live chat, GraphiQL.

| Path | What it showcases |
|---|---|
| [`tests/e2e-ui/`](tests/e2e-ui/) | Full-stack CQRS + GraphQL + OIDC + SvelteKit (todos, chat, blob) |
| [`tests/e2e-celld/`](tests/e2e-celld/) | Same UI; Todo + `chat.post` wait-dispatch to celld; `@live` stays on GraphQL |
| [`js/`](js/) | `@hops-ops/distributed` — transport, causal replica, SvelteKit/React |
| [`examples/graphiql.rs`](examples/graphiql.rs) | Seeded GraphQL playground (`--features "graphql,sqlite"`) |
| `tests/graphql_*` | Engine, HTTP/WS, harden, identity, multi-IdP OIDC |
| `tests/typed_commands/` | Eventual / `Atomic` / `Succeeded` command registration |
| `tests/microsvc/` | Handlers on HTTP, gRPC, bus, session |
| `tests/read_models/`, `tests/distributed_read_model/` | Atomic vs eventual projections |
| `tests/sourced*` / `tests/snapshots/` / `tests/upcasting/` | Macros, snapshots, event versioning |
| `tests/sagas/` | Orchestration + choreography with the outbox |
| `tests/*_transport/`, `tests/knative_cloudevents/` | Broker adapters + conformance |

## License

The Rust workspace metadata declares its crates as MIT licensed, but this
repository does not currently contain a top-level license file. The npm package
therefore remains `UNLICENSED` until maintainers explicitly choose and add its
license.
