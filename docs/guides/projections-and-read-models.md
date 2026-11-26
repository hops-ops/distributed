# Projections and read models

Read models are the query side; projections are the only thing that writes
them. For the short version see principles 04 and 06 in the
[README](../../README.md).

## Full-state projections and out-of-order events

Broker delivery order is not necessarily aggregate commit order. For a read
model whose rows are complete snapshots owned by one aggregate stream, opt in
to source-version fencing with `source: aggregate_snapshot`:

```rust,ignore
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 2,
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
`source: external_snapshot` is the same fence keyed by an external source's
own version (see [external facts](../external-facts.md)).

Migration `0005_projection_source_snapshots` persists the fence alongside each
record, including tombstones; compaction does not remove it. Enabling this on
an existing unversioned projection requires an explicit read-model rebuild
from retained canonical events. Merely changing the projection version does
not infer a source version for existing rows. The normal ordered-delivery
contract and transport identity checks still apply; this is not a replacement
for reliable broker delivery or an incremental-event reorder buffer.

A hand-built `ProjectionProgram` opts in with
`.with_source_ordering(SourceOrdering::AggregateSnapshot)`; the ordering is
part of the canonical program identity. Browser optimism uses the same
mutation program, and authoritative confirmation uses committed row revisions
rather than comparing browser timestamps.

## Rebuilding existing snapshot projections

Use explicit maintenance, not a startup fallback or republishing loop:

```rust,ignore
// Stop producers, drain outboxes, stop consumers, and back up the read model.
// Reuse the projector the application mounts.
let projector = projections().projector(TODOS);
let rebuild = projector.begin_snapshot_rebuild(&repository).await?;
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
`from_complete_history_with_coverage` additionally checks the history against
`AggregateRebuildCoverage` captured from the original event store.

Rebuild is bounded offline maintenance for one active local, unit-partition
snapshot binding: at most 10,000 records and 100,000 occurrences / 64 MiB of
canonical history. It rejects delta folds and aggregate histories with sequence
gaps. It does not migrate schemas/topologies, perform online shadow swaps, or
make several independent projections one transaction. Keep services stopped if
a multi-projection maintenance run fails; capture fresh inventories and rerun
before resuming them.

## Read Models

Read models are query-optimized relational tables derived from aggregates,
event records, or published messages. They are written as declared relational
rows using table metadata from `#[derive(ReadModel)]`. Use JSON/JSONB columns
for whole-view or semistructured fields.

### Defining a Read Model

```rust,ignore
use serde::{Deserialize, Serialize};
use distributed::read_model::ReadModel;

#[derive(Clone, Debug, Serialize, Deserialize, ReadModel)]
#[table("game_views")]
pub struct GameView {
    #[id]
    pub id: String,
    pub player_name: String,
    pub score: i32,
    #[readmodel(jsonb)]
    pub metadata: serde_json::Value,
}
```

Read permissions live next to the model in `impl AuthorizedReadModel`, and one
`ReadModels` list feeds schema bootstrap, the GraphQL engine and client export
(see [GraphQL permissions](graphql.md#permissions-deny-by-default)).

### Typed JSON columns

A `Vec`, map or `#[readmodel(jsonb)]` field is a JSON column. Its Rust type
must implement `JsonType`, the serialized shape of the value. Generated
clients type the column from it instead of `unknown`: a
`Vec<Vec<Tile>>` with a unit-only `Tile` enum becomes
`readonly (readonly ("hole" | "player" | …)[])[]`. Derive it next to Serde:

```rust,ignore
#[derive(Clone, Copy, Serialize, Deserialize, distributed::table::JsonType)]
#[serde(rename_all = "snake_case")]
pub enum Tile { Hole, Unvisited, Visited, DeadBySuicide, DeadByHole, Player }
```

The derive covers named-field structs and unit-only enums, and follows
`rename` / `rename_all`. A Serde attribute that changes the shape
(`flatten`, `skip*`, `with`, `tag`, `untagged`, …) is a compile error. A
`serde_json::Value` column states that its shape is unknown.

A generated client's contract (and its schema hash) does not depend on the
database. Its JSON-column filters are the dialect-neutral operators (`_eq`,
`_in`, `_is_null`, …); Postgres's `jsonb` operators (`_contains`,
`_contained_in`, `_has_key`) are on its GraphQL schema for raw GraphQL only.

### Eventual Projection

A projection maps domain events to a mutation program: on these events, run
this mutation with this input. The mutation is authored as a syntax-only
GraphQL-shaped document (`mutation_file!`), not a public GraphQL field, and
the same program runs in the server projector and in the browser replica for
optimism
([`todos.rs`](../../tests/e2e-ui/crates/projections/src/todos.rs)):

```rust,ignore
#[allow(non_snake_case)]
pub fn SaveTodo() -> Mutation<()> {
    mutation_file!("src/mutations/save_todo.mutation.graphql")
}

distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        model: Todos,
        on {
            events: [TodoCreatedDomainEvent, TodoCompletedDomainEvent /* … */],
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
mutation SaveTodo {
  upsert_todos(object: $input.todo)
}
```

Mount it once per process with `LocalProjectionMounts` and consume it from
the module's routes
([`projections.rs`](../../tests/e2e-ui/crates/service/src/modules/projections.rs),
[`todo.rs`](../../tests/e2e-ui/crates/service/src/modules/todo.rs)):

```rust,ignore
let projections = LocalProjectionMounts::builder("e2e-ui", "ordered-domain-events")
    .eventual(TODOS)
    .direct(BLOB_GAMES)
    .build()?;

Routes::for_aggregate(stores)
    .mount(todo_domain::commands::create())
    .consume_projection(projections.projector(TODOS))
```

The projector consumes published domain events and commits the row changes,
row revisions, broker checkpoint and causal observations in one adapter
transaction, so bus redelivery is idempotent.

Each projector consumes through a durable consumer of its own. `Service::run`
subscribes one per projector, in the consumer group `<group>-<projector>`, so
each keeps its own cursor, retry and dead-letter path and sees its events in
broker order. Event handlers registered with `.event(..)` share the service's
own consumer and never wait behind a projector.

A projector is itself a handler of the event, and the framework sets no order
between it and any other handler. A handler must not depend on a read-model
row written by the same event: take what it needs from the event or the
aggregate, or react to a later domain event.

`RunOptions::until_idle()` drains all of these consumers together. The run
ends when every consumer is idle at the same time, so an event a handler
publishes during the run is projected in that run.

A projector that cannot accept an input records a durable failure for that
partition and its consumer stops, retaining the failed delivery. The other
projectors keep consuming, and `run` returns the stopped projector's error
when they finish. The error names the projection and carries a
`ProjectionRepairHandle`.

A long-running service does not finish, so do not wait for that error. Ask
instead: `Service::stopped_projections()` reads the stops from the projection
store and returns each stopped projection's name and repair handle. Any
service built over the same stores answers, in any process, before and after
a restart; an empty list means none is stopped. Fix the cause, call
`Service::repair_projection(&handle)`, and restart consumption: the projector
resumes from its failed input, so no later event passes it.

A projector needs ordered delivery evidence from the bus. The in-memory, SQL
and NATS JetStream buses provide it. On Kafka, RabbitMQ, Redis Streams and
the Knative ingress a projector's consumer stops at its first event.

### Changing a projection

A changed projection applies to the events delivered after it is deployed.
Rows already written stay as they are: starting a service with a new program
resets nothing and refuses nothing, and two nodes running different programs
can write the same partition during a rollout. To recompute existing rows,
rebuild them explicitly (see above).

A projection declares no epoch. The store gives each partition's change log
a random identity when the first write creates the partition, and every
later reader and writer uses the stored one. A partition created by an
earlier release keeps the epoch it was created with. A live query opened
before that first write is positioned before it: it receives the first write
as an ordinary update, with no reset.

### Atomic Commits (Read Model + Aggregate)

When the response to a command must include the fully consistent, updated
view, declare the command `Atomic<Model>` and mount its projection with
`.direct(..)` (a `ProjectionDescriptor<DirectCandidate>`). The handler builds
the row with the projection's own mutation (`Mutation::from_state`) and stages
it with the aggregate; the row commits in the same transaction as the
aggregate's events and the ledger receipt, and the response is that committed
row ([`blob.rs`](../../tests/e2e-ui/crates/service/src/modules/blob.rs)):

```rust,ignore
pub async fn handle_move(
    ctx: &CausalCommandContext<'_, BlobGame>,
    input: BlobMoveInput,
) -> Result<PreparedCommand<Atomic<BlobGames>>, HandlerError> {
    let owner = ctx.user_id()?.to_string();
    // … parse input.direction into `direction` …
    let repo = ctx.repo();
    let mut game = repo.get(&input.game_id).await?
        .ok_or_else(|| HandlerError::NotFound(input.game_id.clone()))?;
    game.move_dir(&owner, direction).map_err(HandlerError::rejected)?;
    let row = SaveBlobGame()
        .from_state(&BlobGameState::from(&*game))
        .map_err(|error| HandlerError::Other(Box::new(error)))?;
    repo.readmodel(row).publish_events().commit(game)?.atomic()
}
```

The commit is checked against the command's declared direct projection
target: it must stage exactly one upsert of the returned model, and that row
must equal the returned row. `CommitBatch` carries streams,
outbox messages, snapshots, schedules and inbox receipts only, never raw
read-model rows.

This is a deliberate consistency tradeoff: the row is in sync with the
aggregate only when the repository can write both in the same transaction
boundary. For cross-service or cross-database views, use an eventual
projection instead.

### Loading

```rust,ignore
use distributed::read_model::ReadModelWorkspaceExt;
use distributed::table::{RowKey, RowValue};

let loaded = repo
    .workspace()
    .load::<GameView>(RowKey::new([("id", RowValue::String("view-1".into()))]))
    .one()
    .await?;
```

`one()` returns `Option<Versioned<GameView>>`.

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
- **Unique-key joins:** direct relationships can set
  `references = "namespace,revision"` alongside
  `foreign_key = "object_namespace,object_revision"`. The referenced columns
  must be a complete declared unique key, such as
  `#[unique(columns = ["namespace", "revision"])]`, on the target of
  `belongs_to` or the source of `has_many`. Foreign-key columns pair in the
  declared reference order. This lets a model retain a surrogate primary key
  for row identity while relating by a domain key. Missing/null foreign keys
  produce no match; nested target permissions still apply. Omitting `references`
  selects the primary key. `references` is not supported on `many_to_many`.
- **Cyclic singular relationships:** use `Option<Box<T>>` for a `belongs_to`
  field when two models refer to each other. For example,
  `#[readmodel(belongs_to = "Profile", foreign_key = "id")]`
  `profile: Option<Box<Profile>>`. The box only gives Rust a finite-sized type;
  GraphQL still exposes one nullable `Profile`, not a list or embedded JSON
  column. Ordinary `Option<T>` remains the same singular relationship.
- **Writes:** only projections write read models: eventual projectors, or the
  `Atomic` path above for a direct projection.
- **Internal loads:** PK-anchored includes —
  `store.workspace().load(...).include(...).one()` (one-level, opt-in;
  `has_many` / `belongs_to` only, single-column join). Nested queries go
  through GraphQL.
- **Schema lifecycle:** `ReadModels::catalog()` returns the `ReadModelCatalog`;
  `SqlStores::bootstrap_read_models(catalog)` creates its tables for
  development and tests, and `distributed schema` renders migration SQL (or an
  Atlas resource) from the service's `read_model_catalog()` entrypoint (see the
  [CLI guide](cli.md)).
- **Non-goals:** public query APIs belong on the [GraphQL layer](graphql.md)
  (not the include loader).

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
