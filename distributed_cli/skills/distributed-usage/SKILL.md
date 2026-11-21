---
name: distributed-usage
description: Build Distributed CQRS/event-sourced Rust services where you mostly write models and handlers while the framework and distributed generate persistence, transports, manifests, and deploy wiring. Use model-first TDD to specify plain aggregate behavior with fast unit tests before implementing models and thin handlers. Use when designing, testing, writing, or modifying a Distributed service or domain model.
---

# Using the Distributed framework

**The point of Distributed: you mostly just write models and handlers.**

**The model-first advantage: specify and exhaustively test plain domain models
first, then write thin handlers around proven behavior.**

Everything else — service wiring, transports, persistence, manifests, schema,
CI/GitOps — is deterministic structure the framework, macros, and `distributed`
generate. Your authored surface is deliberately small: aggregate models
(`#[sourced]` event methods), command/event handler bodies, and read-model
shapes. If you find yourself hand-writing service plumbing, routing, broker
topology, or deploy YAML, stop — the framework or CLI almost certainly
generates it, and hand-rolled copies drift.

**Composition direction (logical app vs runtime vs process role):** see
[docs/application-composition.md](../../../docs/application-composition.md).
Same packages re-cut as monolith or microservices; Eventual projectors may
live in another process; Atomic seals stay collocated with the command
handler (CAP). Persistence, locks, bus, and transports pair as one runtime
plane — not open-coded per dialect in `main`.

**Always reach for the highest-level API first.** The macros and one-call
conveniences are the recommended surface, not sugar:

- `#[sourced]` — not a hand-written `Aggregate` impl and replay match
- `#[derive(Snapshot)]` / `#[derive(ReadModel)]` — not hand-written snapshot
  payloads or table plumbing
- `routes!` + `Service::routes(..)` — not manual handler registration
- `service.with_bus(bus).run(opts)` — not direct `listen`/`subscribe`/
  `send`/`publish` wiring (drop to the facade only for finer control)

Dropping a level means re-implementing conventions the macro layer keeps
correct for you (event enums, replay, subscription plans, outbox publication
on commit). Do it deliberately, not by default.

Distributed is a CQRS + event-sourcing framework for Rust. Domain state lives in
plain structs; `#[sourced]` command methods record replayable `EventRecord`s;
read models serve queries; published messages are created deliberately through
the outbox. Infrastructure (storage, bus, locks) is pluggable behind async
traits — production swaps are one constructor line, never a handler change.

## Workflow

1. Scaffold a service instead of hand-rolling layout:
   `distributed scaffold <name> --model <agg> --command <agg.action> --event <fact.happened> --store postgres --transport http --bus nats --gitops`
   (from an event-storming board: aggregates → `--model`, commands →
   `--command`, events/policies → `--event`, query views → `--read-models`).
2. Write failing, colocated unit tests against the aggregate command API you
   want. Exercise the plain model directly, without a handler or infrastructure.
3. Implement the model fields, event methods, validation, and guards until the
   contract tests pass. Refactor while they stay green.
4. Implement thin handlers around the proven model behavior. Keep the generated
   structure around them.
5. Start in-memory (`InMemoryRepository`, `InMemoryBus`); swap constructors for
   Postgres/a broker when deploying. Handlers do not change.

## Model-first TDD

Treat an aggregate's public command methods as the domain API. Define and prove
that API before implementing handlers or services:

1. Write the behavior you want as a failing test.
2. Instantiate the plain model and call its command methods directly. Do not use
   a repository, bus, handler `Context`, async runtime, database, or mocks.
3. Assert the complete observable contract: returned result and resulting
   state/snapshot; decode the generated event enum for its name and payload, and
   assert the record version, sequence, and event count separately.
4. Cover every valid transition, validation failure/domain rejection, guard or
   no-op, repeated call, invariant, and boundary case. An explicit rejection or
   intentional guard/no-op must not mutate state or add a pending event.
5. Implement the smallest model behavior that makes the test pass, then
   refactor. Repeat until the model modules have 100% coverage.
6. Only then add handlers. A handler should decode input, load or create the
   aggregate, invoke an already-proven command method, then commit. Add an
   outbox message only when a fact must be published outside the aggregate.

Use the generated event enum and `TryFrom<&EventRecord>` to assert business
facts; do not couple domain tests to encoded payload bytes. `Entity::events()`
is the aggregate's complete in-memory history, while `Entity::new_events()` is
the set added since it was loaded or committed.

A `#[event(..., when = condition)]` command returns `Ok(())` when its condition
is false: this is a successful no-op, not a domain error. If the API should
reject instead, test for `Err`, unchanged state, and no new event, then validate
in a public command method before calling a private recorded event applier. An
error returned from a fallible recorded event body does not roll back the event
that was already recorded, so do not use that as the rejection boundary.

Run `cargo llvm-cov --lib --summary-only` in the bounded-context crate to check
model coverage. Coverage confirms execution; the result, state, invariant, and
event assertions above define the actual contract.

## Aggregates

An aggregate is a plain struct embedding `distributed::Entity`. `#[sourced]` on
its impl block turns `#[event("...")]` methods into recorded events and
generates the typed event enum + `Aggregate` impl.

```rust
#[derive(Clone, Serialize, DomainState)]
#[domain_state(version = 1)]
struct TodoState {
    id: String,
    task: String,
    completed: bool,
}

#[derive(Default, Snapshot)]
struct Todo {
    entity: Entity,
    task: String,
    completed: bool,
}

impl From<&Todo> for TodoState {
    fn from(todo: &Todo) -> Self {
        Self {
            id: todo.entity.id().to_string(),
            task: todo.task.clone(),
            completed: todo.completed,
        }
    }
}

#[sourced(entity, aggregate_type = "todo", domain_state = TodoState)]
impl Todo {
    #[event("todo.initialized", version = 1, domain)]
    fn initialize(&mut self, id: String, task: String) {
        self.entity.set_id(&id);
        self.task = task;
    }

    #[event("todo.completed", version = 1, when = !self.completed, domain)]
    fn complete(&mut self) {
        self.completed = true;
    }
}
```

Rules that prevent real bugs:

- **Always set an explicit `aggregate_type`** — the default is the Rust type
  name, which silently changes the durable stream identity if the type is
  renamed.
- **Event methods become fallible.** An `#[event]` method declares no return
  type and may not use `?` or `return`; the macro makes it return
  `SourcedResult`, so call it with `?`. Validate in the public method that
  calls it.
- Event names are lowercase past-tense facts (`"initialized"`, `"completed"`).
  Use `when = <expr>` for intentional idempotent/no-op transitions so they
  record nothing; use explicit validation when the API promises a rejection.
- Evolving an event's payload? Do not edit stored data — add an upcaster:
  `#[sourced(entity, upcasters(("initialized", 1 => 2, V1 => V2, upcast_fn)))]`
  and mark new-format methods `#[event("initialized", version = 2)]`.

## Command handlers

For a browser-facing GraphQL service, use the typed causal command path instead
of exposing a raw `Context`/`serde_json::Value` handler as the mutation contract:

- declare `.typed_command(typed_command::<Input, Succeeded<Payload> | Eventual<Payload> | Atomic<Model>>(...))`
  on the executable route;
- implement the handler with `CausalCommandContext` and return a
  `PreparedCommand<_>` so the framework owns commit, ledger, outbox, and
  projection atomicity;
- bind that exact `Service` through `GraphqlEngineBuilder::service`, configure
  public OIDC. HTTP command routes stay off; browser writes use
  only the GraphQL command proxy. Call `.with_http_command_routes()` only for
  an intentional non-GraphQL ingress;
- generate the strictly typed client with `distributed client`.

Use the `distributed-graphql` skill for the complete route, consistency,
authorization, and client-generation contract. The raw handler form below
remains valid for intentional non-GraphQL transports.

One module per handler, exporting `COMMAND` (or `EVENT`/`EVENTS`), a `guard`,
and an async `handle`:

```rust
pub const COMMAND: &str = "todo.initialize";

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["id", "task"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<CreateTodo>()?;
    let mut todo = Todo::default();
    todo.initialize(input.id.clone(), input.task)?;
    ctx.repo().publish_events().commit(&mut todo).await?;
    Ok(json!({ "id": input.id }))
}
```

Register with `routes!` and serve — the same handlers work over direct
dispatch, HTTP (`microsvc::serve`, feature `http`), gRPC (feature `grpc`), and
the bus:

```rust
let routes = distributed::routes!(
    Routes::new().with_repo(repo.queued().aggregate::<Todo>()),
    command handlers::todo_create,
    command handlers::todo_complete,
);
let service = Service::new()
    .named("todo-api")
    .with_http_command_routes() // required for POST /{command}; GraphQL stays off by default
    .routes(routes);
service.with_bus(bus).run(RunOptions::default()).await?; // until the process ends
```

## Publication is explicit (outbox)

An `EventRecord` is write-side replay history, **not** automatically a domain
event other services see. A domain-marked transition captures its separate
canonical outward occurrence. Publish that occurrence and the aggregate in one
transaction:

```rust
repo.publish_events().commit(&mut todo).await?;
```

With a bus attached (`service.with_bus(bus)`), commit publishes immediately;
without one, rows stay pending for an `OutboxDispatcher` worker. Snapshots are
private hydration caches and are never published implicitly. Use
`OutboxMessage::encode_for_entity(id, name, &payload, &entity)` for custom
payloads and automatic correlation/causation metadata propagation.

## Read models

`#[derive(ReadModel)]` declares a query-optimized relational projection:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, ReadModel)]
#[table("todo_views")]
pub struct TodoView {
    #[id]
    pub id: String,
    pub task: String,
    #[jsonb]
    pub metadata: serde_json::Value,
}
```

Two update modes — pick deliberately:

- **Atomic** (same service, response must reflect the write):
  `repo.read_models(plan).commit(&mut agg).await?` commits rows and events in
  one transaction.
- **Eventual** (cross-service): a projector subscribes to published messages
  and commits through `ctx.read_model_store().workspace()`, which marks the
  message processed in the same transaction for idempotency.

## Choosing infrastructure

| Concern | Dev/test | Production |
|---|---|---|
| Storage | `InMemoryRepository` | `PostgresRepository::connect_and_migrate(url)` (feature `postgres`); `SqliteRepository` for single-node |
| Bus | `InMemoryBus` | `NatsBus`, `RabbitBus`, `KafkaBus`, `PostgresBus`/`SqliteBus` (same DB, no broker), `KnativeBus` |
| Locking | `InMemoryLockManager` (via `.queued()`) | `PostgresLockManager` via `.queued_with(locks)` for cross-process serialization |

Gotchas:

- Application crates depend on `distributed` only — the macros are re-exported;
  never add `distributed_macros` directly.
- Keep the shared bounded-context crate feature-light; enable `postgres`/
  `http`/`nats`/... only in executable service crates.
- `Service::named(..)` is the durable consumer group — same name for every
  replica of one deployment. `namespace(..)` scopes broker topology per
  app/environment. Names are validated: portable IDs only (`A-Za-z0-9_-`,
  `.` also allowed in namespaces, max 128 bytes).
- Generic `microsvc` command routes do **not** authenticate themselves. Protect
  intentional non-GraphQL routes at a trusted edge. Public GraphQL scaffolds
  instead wire `OidcBearer` validation; generic command POST routes stay off;
  never trust client-supplied identity headers.
- `connect_and_migrate` applies migrations; plain `connect` does not create
  tables.

## Multi-crate single-service layout (and later microservices)

Copy the **e2e-ui** fixture under `tests/e2e-ui/` (README; see `tests/e2e-ui/README.md`):

```text
crates/
  todo-domain/     # personal todos (owner-scoped)
  chat-domain/     # lobby chat (shared room)
  readmodels/      # projections + read_model_catalog
  service/         # thin command handlers + event projectors + GraphQL
  runner/          # store + bus + bind
  suite/           # HTTP/GraphQL behavioral cases
ui/                # SvelteKit: todos + chat with GraphQL subscriptions
```

Rules the fixture demonstrates:

- **Domain unit tests first** (`cargo test -p todo-domain`) — no repository
- **Owner from session**, never from untrusted create body
- **One mutation IR, two apply sites:** Eventual projectors (event handlers)
  write todos/chat; Blob stages the same IR in the command handler and seals
  `Atomic` so the response can carry the row (impossible on an event handler)
- GraphQL row filter: `owner_id = claim(x-user-id)` for role `user`
- **Typed GraphQL commands** (`Eventual` / `Atomic`) via the OIDC command
  proxy; generic direct command POST routes are disabled
- **Generated client**: `distributed client` produces the typed replica/query/command
  artifacts consumed by the SvelteKit app
- **Subscriptions**: wire `SqliteRepository::read_model_changes()` into
  `GraphqlEngineBuilder::change_stream`; clients use WebSocket `/graphql/ws`

Run the full app: `cd tests/e2e-ui && make`. Suite: `make test`.

### e2e-ui cluster-dev dogfood (agents)

Namespace = hops `--name`. Fix the live pods, not a host `make run`. After
source changes: rebuild through `distributed build`, restart, **wait until API
logs `listening` and Vite is ready**, then curl the FQDNs. Schema/pure errors usually mean
stale build, not a roles redesign. Full checklist: hops skill
`references/local-workbench.md` § Dogfood.

## Manifest entrypoint

Every service should export `read_model_catalog()` registering its read
models — `distributed describe` and `distributed schema` compile the crate and call it:

```rust
pub fn read_model_catalog() -> distributed::ReadModelCatalog {
    distributed::ReadModelCatalog::new("todos").read_model::<TodoView>()
}
```

Keep it updated when adding read models or tables; see the
`distributed-schema` skill for rendering schema artifacts from it.

## References

- Framework guide: the `distributed` crate README (https://crates.io/crates/distributed)
- Read models: `README § Read Models`; transports: `README § Event Bus / transports`;
  repositories: `README § Repositories` in the Distributed repo
- CI/GitOps scaffolding: the `distributed-ci` skill
- Schema and manifest tooling: the `distributed-schema` skill
