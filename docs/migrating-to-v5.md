# Migrating to v5

v5 is a breaking release. This page lists the public types, APIs, features
and wire formats that were removed or renamed, and what to use instead. It
does not cover internal changes.

## Imports

- **One public path per item.** Most crate-root re-exports are gone. The
  root keeps the event-sourcing core (`Entity`, `EventRecord`, `Aggregate`,
  `AggregateRepository`, the `Repository` traits and backends, snapshots,
  `QueuedRepository`) and the macros. Import everything else from its module
  (`distributed::command::…`, `distributed::microsvc::…`,
  `distributed::outbox::…`), or use `distributed::prelude::*`.
- **Derives live with their traits:** `distributed::read_model::ReadModel`,
  `distributed::domain_event::{DomainEvent, DomainState}`,
  `distributed::command::{CommandInput, CommandOutput}`,
  `distributed::table::JsonType`.
- **`GraphqlInput` / `GraphqlOutput`** are `CommandInput` / `CommandOutput`
  in `distributed::command`. Hand-written metadata uses
  `CommandInputType` / `CommandOutputType`.
- **`distributed::leaderboard`** moved to the `distributed_leaderboard` crate.
  `GraphqlEngineBuilder::leaderboard` is `external_read_source(LeaderboardRoots)`.

## Aggregates and macros

- `#[sourced]` requires `aggregate_type = "…"`; there is no default.
- An `#[event]` method may not declare a return type, use `?` or `return`.
  Validate in the public method, then call the recorder.
- Removed macros: `aggregate!`, `impl_aggregate!`, `#[digest]`, `#[enqueue]`,
  `#[sourced(enqueue)]`, `graphql_models!`, `module!`, `application!`, the
  classic `mutation!` grammar and the fn-form `#[command(id = …)]` attribute.
  `portable_command!` is `command!`. The method form
  `#[command("agg.verb", …)]` inside `#[sourced]` is unchanged.
- `#[readmodel(skip)]` / `#[readmodel(private)]` are spelled
  `#[readmodel(skip_query)]`.

## Repositories and commits

- One commit builder, `AggregateCommit`: `repo.commit(&mut agg)` is its
  zero-option form; `repo.outbox(..)`, `repo.publish_events()` and friends
  return it. The `commit_builder` module, `CommitBuilderExt`,
  `commit_entities`, `repo_mut`, `Entity::rehydrate`, `ReplayGuard`,
  `PayloadCodec`, `InboxOutcome` and `GetWithOpts` / `GetAllWithOpts` are
  removed.
- Read models are written only by projections. `read_models(..)` on a
  commit, `ReadModelWritePlanCommitExt` and read-model plans on `CommitBatch`
  are removed; declare the command `Atomic<Model>` when its response must
  carry the committed row.
- A queued repository's lock is owned by the loaded aggregate: committing or
  dropping it releases the lock.

## Commands

- Typed commands are the only command path, and every command is recorded
  in the command ledger. Removed: `Service::dispatch`, `Routes::command`,
  `Routes::handler`, the `command` arm of `routes!` (event arms remain),
  `Context::command_name`, `LocalCommandHost`, `AggregateCell::dispatch`,
  `Routes::invoke_cell`, `CommandMount::from_handler` /
  `from_request_handler` / `invoke`, `CommandRequest`, `CommandResponse`,
  `Service::dispatch_mount` and `dispatch_request`. Register commands with
  `#[command]`, `command!` or `typed_command(..)`; `Service` implements
  `CommandHost` (`invoke`).
- HTTP and gRPC accept only `{ "commandId", "input" }`; any other body is a
  400. An unknown command is a 400, not a 404. HTTP command routes are off
  unless you call `with_http_command(command, edge)` or
  `with_http_command_routes()`.
- Commands delivered by a bus, Knative or the schedule worker run as the
  transport principal: their declaration must list
  `microsvc::TRANSPORT_ROLE`, and the message must carry an id, which becomes
  the ledger command id. `Bus::send(name, payload)` no longer mints one.

## Outbox, sagas and schedules

- The outbox has no delayed delivery: `deliver_at`, `deliver_after` and
  `deliver_now` are removed, and delivered rows are deleted rather than kept
  as `Published`. Use `ctx.schedule(Schedule)` for future work.
- `SagaSignal::TimedOut` is removed; saga step timeouts are schedules and a
  saga needs its `TIMEOUT_COMMAND` route.
- `outbox_message_schema` and `OUTBOX_MESSAGES_TABLE` are removed.

## Bus and wire format

- Reserved headers are `x-distributed-*` (were `x-sourced-*`);
  `X-Distributed-Occurrence-Id` is gone. Old and new versions cannot share a
  broker, and metadata that names a reserved header is refused.
- Kafka topics are keyed by stream id and the topic layout changed;
  `with_strip_prefix` is removed. `MessageSource` has a `finish()` method.
- The `redis` feature is `redis-streams` (`bus::RedisStreamBus`).

## Storage

- Framework timestamp columns are integer microseconds. Migrations `0016`
  and `0017` convert existing SQLite and Postgres databases in place.
- PostgreSQL text range comparisons and `order_by` use `COLLATE "C"`; an
  index meant to serve them must use that collation.

## Features

- Removed or renamed: `workers-rs`, `gateway-graphql`, `gateway-delivery`,
  `emitter`, `application-runtime`, `runtime`, `leaderboard-background`,
  `redis` (now `redis-streams`). Knative needs the `knative` feature.
- The application `Runtime`, `ProcessPreset`, `MountSelector` and
  `required_capabilities` are removed: a process is the `Service` you compose.

## GraphQL

- A comparison with a `null` operand in a client `where` (for example
  `_eq: null`) now matches every row instead of none.
- `GraphqlEngine::build()` requires an explicit `.identity(..)`.

## JavaScript client

- `@hops-ops/distributed/react`, replica persistence and the offline command
  outbox are removed, with `documentToString` and `GqlDocument`. The client
  core is framework-neutral; SvelteKit is the shipped adapter.

## CLI

- `distributed contracts check` and `distributed contracts accept` are
  removed.
