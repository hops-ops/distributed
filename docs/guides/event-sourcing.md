# Event sourcing: aggregates, repositories, snapshots

How aggregates record and replay events, how repositories persist them, and
how snapshots and upcasters keep replay cheap and compatible.

## Core Concepts

- **Entity**: Holds the event history. You embed it in your domain structs.
- **EventRecord**: An immutable aggregate event record with name, payload, event version, sequence, timestamp, and optional metadata. It is replayable model history, not automatically a published domain event.
- **Aggregate**: A struct that embeds an `Entity` and replays `EventRecord`s. `aggregate_type()` provides the durable stream-identity component for persistence.
- **Repository / AggregateRepository**: Persists and loads aggregates by event history. The event store is optimized for append and replay; `get`/`commit` are async.
- **InMemoryRepository**: In-memory repository for tests and examples. Implements the async repository, read-model, snapshot and inbox traits; `outbox_store()` returns its `InMemoryOutboxStore`.
- **SqliteRepository / PostgresRepository**: Durable async SQL adapters (optional features).
- **QueuedRepository**: Wraps any repository and adds async per-entity queue locking.
- **EventUpcaster**: A pure, stateless transformation that converts event payloads from one version to another at read time.
- **Snapshottable**: Opt-in trait for aggregates that produce state snapshot payload DTOs. Use `#[derive(Snapshot)]` to auto-generate the payload struct and trait impl.
- **`outbox::OutboxMessage`**: A durable publication work item for a domain event, integration event, command, or generic transport message. Supports optional `destination` for point-to-point routing and metadata propagation.
- **`outbox_worker::OutboxDispatcher`**: Drains durable outbox rows and publishes them to a transport. Background polling and after-commit dispatch share one claim → publish → settle path.
- **`read_model::ReadModel`**: Query-optimized relational projection state for UI/API reads. Read models may be updated atomically with a command or eventually from published messages.
- **`graphql::GraphqlEngine`** (feature `graphql`): Deny-by-default GraphQL surface over registered read-model tables: role-scoped columns/row filters, optional command mutations, live subscriptions via `ChangeHub`, and the `IdentityConfig` modes `OidcBearer`, `Gateway` (shared-secret trusted gateway) and `DevHeaders`.
- **`bus::Bus` / `bus::BusConsumer`**: The service bus facade — `send`/`publish` (produce) and `listen`/`subscribe` (consume), implemented by a per-transport `*Bus` type.
- **`microsvc::Service`**: Convention-based async command/event handler framework with pluggable transports (HTTP, gRPC, bus, GraphQL mutations, in-process dispatch).

## Terminology And CQRS Boundaries

Event sourcing is the model-level persistence strategy: aggregates record replayable `EventRecord`s when command methods such as `#[event]` (within `#[sourced]`) succeed. Those records are the write-side history used to hydrate the aggregate.

CQRS is the architectural split between write-side aggregates and query-side read models. Repositories load aggregate event streams by ID for command handling; production business queries should read from `ReadModel` projections shaped for that query.

Published messages are a separate boundary. An aggregate event record is not automatically a domain event. When other services, projections, or transports need a fact, publish it in the same commit as the aggregate: `repo.publish_events()` publishes the domain events the aggregate captured, `repo.publish(event)` one explicit event, and `repo.outbox(message)` stages a raw `OutboxMessage` (the low-level escape hatch for integration events, commands, or any other transport message). Each starts an `AggregateCommit`; finish it with `.commit(&mut aggregate)`.

The outbox stores delivery work, not publication history. Successful delivery
deletes the row under its active lease. Failed or ambiguous sends retain the row
for retry or inspection. A crash after transport acceptance but before deletion
can deliver the same event ID again; consumers must remain idempotent. Event
history belongs in the event store, and command retry results belong in command
receipts. Neither depends on retaining delivered outbox rows.

Commands dispatched to another aggregate cell use the same command ledger, but
with an explicit external dispatch binding. The binding records the logical
route kind and shard alongside the command identity; it is immutable for that
reservation and survives lease reclaim and terminal replay. External completion
updates only the ledger after the returned attempt fence and binding match. It
does not append local events, write the outbox, or treat the cell's commit as
proof that an asynchronous read-model projection has completed. A local causal
commit cannot complete an externally bound reservation, and an external
completion cannot complete a local reservation. Retained cell-only receipts
without a gateway binding remain explicit unknowns until a matching gateway
reservation and trusted completion protocol can be established; they are never
retrofitted into a different route or silently replayed.

The celld command host reserves that gateway intent before making the cell
request. The reservation's causation ID and logical route binding must be
echoed by the trusted cell receipt; a changed command ID, input, causation, or
route is rejected before it can complete the gateway row. A terminal cell
receipt is durably recorded in the gateway ledger before the response is
returned. Status checks current authorization and re-evaluates the durable
receipt's projection evidence, so an in-memory completed-status cache is never
the source of truth. An ambiguous transport result remains retryable against
the same reservation and cell; it does not invoke the local domain handler or
re-publish the cell's events and outbox.

`OutboxStore::complete` and `complete_many` delete the delivered rows. Settling
a row that is already gone returns `NotFound`; a stale claim on an existing row
returns `InvalidState`.

## Pluggable by Default

Every infrastructure concern in `distributed` follows the same pattern: a **trait** defines the contract, an **in-memory implementation** ships out of the box for testing and development, and you swap in your own for production.

| Concern | Trait(s) | In-memory default | Swap in for production |
|---|---|---|---|
| Storage | `GetStream` + `TransactionalCommit` | `InMemoryRepository` | `PostgresRepository`, `SqliteRepository`, … |
| Messaging | `bus::Bus` + `bus::BusConsumer` | `InMemoryBus` | `NatsBus`, `PostgresBus`, `SqliteBus`, `RabbitBus`, `KafkaBus`, `KnativeBus` |
| Read model rows | `ReadModelWritePlanStore` + `RelationalReadModelQueryStore` | `InMemoryReadModelStore` | Postgres, SQLite |
| Snapshot store | `SnapshotStore` | `InMemorySnapshotStore` | Postgres, SQLite, … |
| Outbox publishing | `outbox_worker::OutboxStore` + async `bus::MessagePublisher` | `InMemoryOutboxStore` (from `InMemoryRepository::outbox_store()`) | Any `MessagePublisher` (e.g. `outbox_worker::BusPublisher` over a real `Bus`) |
| Locking | `lock::Lock` + `lock::LockManager` | `InMemoryLockManager` | `PostgresLockManager`, `SqliteLockManager` (durable leases); implement the traits for anything else |

All in-memory defaults are `Clone` and `Send + Sync`, so they work in single-task tests and multi-task servers alike. When you're ready for production, implement the trait for your infrastructure and plug it in — handler code does not change.

## The `#[sourced]` Macro

The `#[sourced]` attribute macro is the recommended way to define event-sourced aggregates. Place it on an impl block and annotate command methods with lowercase, past-tense aggregate event names such as `#[event("initialized")]`. It generates a typed event enum plus the `Aggregate` impl.

An `#[event]` method must not declare a return type: the macro records the event before the body runs and rewrites the method to return `SourcedResult`. The body therefore cannot fail; validate in a plain method that then calls the recorder. Call recorders with `?` so serialization and event-recording failures propagate. A `when = <expr>` guard that is false skips both the event and the body and returns `Ok(())`.

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

#[sourced(entity, aggregate_type = "todo")]
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

### Flat events and optimistic updates

With `domain = event`, recorder parameters are the flat outward event body:

```rust,ignore
#[sourced(entity, aggregate_type = "review")]
impl Review {
    pub fn approve(&mut self, id: String) -> distributed::SourcedResult {
        // Validate the decision here, before recording it.
        self.record_status(id, "approved".into())?;
        Ok(())
    }

    #[event("review.status_recorded", version = 1, domain = event)]
    fn record_status(&mut self, id: String, status: String) {
        self.entity.set_id(id);
        self.status = status;
    }
}
```

Optimistic projections need to know what a command will leave behind before
the event exists. That is declared, never read out of a method body. A state
recorder (`domain`) lists the values it always sets, in the field grammar of
`state_preview!`:

```rust,ignore
#[event("todo.completed", version = 1, domain, known(status: "completed"))]
fn completed(&mut self) { self.status = TodoStatus::Completed; }
```

A flat event (`domain = event`) declares the body fields it always carries
the same way. When each outcome is its own event, the constant belongs to the
event, and every command that records it gets the preview:

```rust,ignore
#[event("review.archived", version = 1, domain = event, known(status: "archived"))]
fn record_archived(&mut self, id: String, status: String) { /* .. */ }
```

A flat event shared by several outcomes carries no single constant; its
commands declare their preview on the registration instead, with
`.applies(event_preview! { ReviewStatusRecorded => ReviewStatusRecorded {
id: input.id, status: "approved", ..unknown } })`. All of these are checked
against the body's fields when the command is registered. Neither predicts authorization
or guarantees that the event will occur.

Public transitions can delegate to shared decision helpers in the same
`#[sourced]` impl. For example, `leave` and an owner-authorized `remove_member`
can both call a private `self.remove_member_decision(...)`. Each gets its own
`domain_commands::Leave` / `domain_commands::RemoveMember` witness containing
the outward events reachable through those helpers. Discovery follows static
`self.method(...)` calls, deduplicates events, and terminates on recursive helper
graphs. It does not follow foreign receivers or helpers outside the impl, and
does not propagate argument values through helpers. Custom
`domain = with(Body, adapter)` events use the declared body contract, including
qualified or relative type paths.

For an event whose body contains the authenticated principal, bind that one
field using the existing command API:

```rust,ignore
command_transition::<domain_commands::Leave, LeaveInput, Eventual<LeaveOutput>>("group.leave")
    .authenticated_user_field::<GroupMemberRemovedDomainEvent, GroupMemberRemovedDomainEvent>(
        "user_id",
    )
```

The second type is the exact serialized body. Use the flat event itself for
`domain = event` or `#[derive(DomainEvent)]`; state-capture events continue to
use their `DomainState` type. The equivalent `command!` entry is:

```rust,ignore
authenticated_user_field: (
    GroupMemberRemovedDomainEvent,
    GroupMemberRemovedDomainEvent,
    "user_id"
),
```

This gives a membership-delete projection the trusted `x-user-id` key without
adding a caller-controlled user ID to the command input. The framework checks
the exact event/body descriptor and serialized string field, including Serde
renames. The handler must still obtain the principal from its authenticated
session and enforce membership rules; this declaration only supplies the
generated preview's provenance.

See [the executable contract tests](../../tests/core/sourced/flat_preview.rs), including
the generated delete-projection client, helper replay and conflicting/dynamic-value cases.

### Durable Stream Identity

`Aggregate::aggregate_type()` provides the type component of a persistence stream's identity (the pair `(aggregate_type, aggregate_id)`). It is required, and must be a stable durable name: renaming or moving the Rust type must not change it.

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
#[sourced(entity, events = "TodoCommand", aggregate_type = "todo")]
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

#[sourced(entity, aggregate_type = "todo_v2", upcasters(
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
#[sourced(my_entity, aggregate_type = "my_aggregate")]
impl MyAggregate {
    #[event("initialized")]
    fn create(&mut self, name: String) {
        // uses self.my_entity
    }
}
```

### Replay and event versions

Replay requires each event's schema version to match its registered handler
after upcasting. `#[sourced]` reads this version from
`#[event(..., version = N)]`. Omitted versions default to 1.
Older events need an explicit upcaster chain to the registered version; future
versions are rejected. Matching payload layouts do not bypass this check.
The check runs before payload decoding or handler invocation, including when
loading only the event tail after a snapshot in a native repository or cell.

## Event Metadata

Metadata lets you attach cross-cutting context — correlation IDs, causation IDs, user context, trace spans — to events without changing your domain model.

### Setting Metadata on an Entity

Set metadata on the entity before calling command methods. Every event produced by `#[event]` automatically inherits it:

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
use distributed::outbox::OutboxMessage;

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
  → #[event] → EventRecord.metadata
  → encode_for_entity → OutboxMessage.metadata
  → OutboxDispatcher → transport Message.metadata
  → subscriber receives the message with correlation_id() == "req-123"
```

Framework-derived metadata (codec, destination, source aggregate) is namespaced under the reserved `x-distributed-` prefix so it cannot be shadowed by user metadata.

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

## Queued Repository

Per-entity async locking for serialized workflows. `get` waits for the lock and returns an aggregate that owns it. `commit` releases it, and so does dropping the aggregate, so an early return cannot leak a lock:

```rust,ignore
use distributed::{AggregateBuilder, InMemoryRepository, Queueable, RepositoryError};

let repo = InMemoryRepository::new().queued().aggregate::<Todo>();

let Some(mut todo) = repo.get("todo-1").await? else {
    return Err(RepositoryError::NotFound { id: "todo-1".into() });
}; // locks this ID
// ... mutate ...
repo.commit(&mut todo).await?; // unlocks

// Or release without changes:
drop(todo);

// Read without locking:
let _ = repo.peek("todo-1").await?;
```

By default, locking is in-memory (`InMemoryLockManager`) — process-local, lost
on restart. For **cross-process** serialization, back the queue with a durable
SQLx lease lock (feature `postgres` or `sqlite`). It implements the same
`LockManager` trait, so it's a drop-in via `queued_with`:

```rust,ignore
use distributed::lock::PostgresLockManager;
use distributed::PostgresRepository;

let repo = PostgresRepository::connect_and_migrate(&database_url).await?;
// The `aggregate_locks` lease table is created by the repository's migrations.
let locks = PostgresLockManager::new(repo.pool().clone());
let todos = repo.queued_with(locks).aggregate::<Todo>();
```

The lease records each held key in the `aggregate_locks` table (`SqliteLockManager`
is the SQLite equivalent). It is a **mutual-exclusion optimization, not a fencing
guarantee** — the event store's `(aggregate_type, aggregate_id, sequence)` primary
key remains the authoritative concurrency boundary. There is **no lease renewal**, so
set the lease TTL above your longest critical section. Tune with `with_lease_ttl`,
`with_retry_interval`, and `with_max_wait`; reclaim rows from crashed holders with
`sweep_expired`. Any custom `LockManager` (e.g. Redis) plugs in the same way.

## Persistent Repositories

The optional `sqlite` and `postgres` features add async, SQL-backed repositories
that implement the same async traits as `InMemoryRepository`. They persist aggregate
event streams, relational read-model write plans, consumer inbox receipts,
snapshots, and outbox rows. A `CommitBatch` stages event streams, outbox rows,
snapshots, inbox receipts and schedules through one SQL transaction. They also enable SQL-backed bus transports over the
same database connection (`SqliteBus` / `PostgresBus`).

```rust,ignore
// SQLite — local persistence, conformance, and bus tables (requires `sqlite`)
let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:").await?;

// Postgres — the production SQL event-store path (requires `postgres`)
let repo = distributed::PostgresRepository::connect_and_migrate(&database_url).await?;
```

`connect_and_migrate` applies the explicit migrations under `migrations/`.
`connect(url)` and `new(pool)` (wrap an existing pool) do **not** create tables,
so applications can control bootstrap order; call `migrate()` when ready.

SQLite is the no-extra-process local durable path: one SQLite database can back
repositories, read models, the outbox, locks, and `SqliteBus` for tests, demos,
and small single-node deployments. Postgres is the low-ops starter for production:
a single Postgres cluster can back repositories, read models, the outbox, **and**
the durable transport (`PostgresBus`).

### Repository traits (async-only)

Streams are keyed by full **stream identity** `(aggregate_type, aggregate_id)`,
not bare IDs; `aggregate_type` comes from
`#[sourced(entity, aggregate_type = "...")]`.

| Trait | Role |
| --- | --- |
| `GetStream` | Load one or more event streams by identity |
| `TransactionalCommit` | Commit a `CommitBatch` (event streams, outbox rows, snapshots, inbox receipts, schedules) in one backend transaction |
| `ReadModelWritePlanStore` / `RelationalReadModelQueryStore` | Relational projection write + PK load surfaces for adapters |
| `SnapshotStore` | Rebuildable snapshot cache by stream identity |
| `outbox_worker::OutboxStore` | Claim/update durable outbox rows (workers; not aggregate rehydration) |

`InMemoryRepository` (plus in-memory read-model/snapshot stores) is the behavioral
reference for conformance tests — not a production I/O adapter. SQL adapters
implement the same traits with `sqlx`.

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

Fields with `#[serde(skip)]` are automatically excluded.

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

### How Snapshots Work

- **On commit**: If `entity.version().saturating_sub(entity.snapshot_version()) >= frequency`, the aggregate's state is serialized via `Snapshottable::create_snapshot()` and staged into the same commit transaction as the event append.
- **On load**: If a usable snapshot cache record exists, the aggregate is restored from its payload and only events with `sequence > snapshot.version` are replayed. Invalid, incompatible, or ahead-of-stream cache records fall back to full replay.
- **Storage**: Snapshot cache records are stored separately from the event stream, keyed by full stream identity. They carry aggregate type, aggregate ID, covered event version, snapshot schema version (`Snapshottable::SNAPSHOT_VERSION`, set with `#[snapshot(version = N)]`), payload codec and codec version, cache metadata, and timestamp. A record whose schema version differs from the aggregate's is ignored and the aggregate is replayed in full.

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
#[sourced(entity, aggregate_type = "todo", upcasters(
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
#[sourced(entity, aggregate_type = "todo", upcasters(
    ("initialized", 1 => 2, InitV1 => InitV2, upcast_init_v1_v2),
    ("initialized", 2 => 3, InitV2 => InitV3, upcast_init_v2_v3),
))]
impl Todo { /* ... */ }
```

A v1 event automatically chains through v1→v2→v3; a v2 event only goes through v2→v3; a v3 event passes through unchanged.

### How Upcasting Works

- **On hydrate**: Before replaying events, the aggregate's registered upcasters are applied by event name and version.
- **On snapshot hydrate**: Only post-snapshot events are upcast — the snapshot already contains the current state.
- **No stored data modified**: Upcasters are read-time transformations.
- **Zero overhead when unused**: Aggregates with no upcasters take the fast hydration path.
