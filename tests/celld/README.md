# celld aggregate cells, Queue, and bus relay

This fixture exercises the write side of the Distributed flow:

```text
command -> AggregateCell -> domain event -> same-cell outbox
        -> celld OUTBOX Queue binding -> Worker relay -> Distributed Bus -> NATS
```

The aggregate Worker owns one SQLite Durable Object per Todo or Chat shard.
The same SQL operations used by native repositories append event rows, maintain
snapshots, fence command receipts and enqueue pending delivery rows. Each
command commits those participants inside one `storage.transactionSync`.
There is no whole-cell JSON value, authoritative in-memory shadow, or host
persistence callback.

```rust,ignore
let cell = AggregateCell::<Todo>::from_state_with_snapshots(state, 100)?
    .mount(create())
    .mount(complete())
    .with_celld_outbox(CelldOutbox::from_env(&env, "OUTBOX")?);

// Dispatch arms the durable watchdog before invoking the command.
let result = cell.dispatch_idempotent(command, &identity, input, session).await?;

// Optional immediate delivery after commit. A drain error is diagnostic here:
// it cannot turn an already committed command into a rejection.
match cell.drain_outbox(&env).await {
    Ok(drain) => {
        for error in drain.deferred {
            worker::console_error!("outbox drain deferred: {error}");
        }
    }
    Err(error) => worker::console_error!("outbox drain deferred: {error}"),
}

// The Durable Object alarm handler calls cell.alarm(&env): it fires due cell
// alarms (see docs/coordination-primitives.md) and then drains the outbox.
```

Domain cell alarms share the one Durable Object alarm with the outbox
watchdog; every wake is set to the earliest of the two. `alarm-worker/` is a
Ticker cell that arms, re-arms and cancels alarms, and `alarms.mjs` proves it
on an isolated `celld dev` (including a crash and restart):

```sh
(cd tests/celld/alarm-worker && worker-build --release)
CELLD_BIN=celld node tests/celld/alarms.mjs
```

The Queue lives in another cell: its acceptance is **not** part of the
aggregate's SQLite transaction. celld's output gate orders Queue egress after
the aggregate write becomes durable. The prearmed alarm covers the gap between
that commit and Queue acceptance, including process loss without another request.

A drain claims rows, sends them, and deletes each matching lease-fenced row
after acceptance. A crash after acceptance but before deletion can deliver the
same stable event ID again; consumers must deduplicate. Queue publication or
settlement failures retain pending/in-flight work for the watchdog. An alarm
keeps a wake scheduled while commands are running, including suspended commands.
An empty drain never deletes another command's alarm; the last scheduled wake
simply finds no work and stops.

Snapshots use the ordinary repository cache validation, upcasting and tail
loader on both native and cell command paths. A valid snapshot avoids loading
old event payloads; an unusable cache rebuilds from the authoritative events.

### Breaking storage and host API change

Workers now open cells with `AggregateCell::from_state` or
`from_state_with_snapshots`, and use `drain_outbox` from their alarm handler.
The whole-state export/restore and `persist_and_drain_outbox` APIs are not
available on the Worker host. Native in-process conformance fixtures are not a
production persistence adapter.

### No native durable cells

Durable cells run on celld only, for local development (`celld dev`) and
deployment alike. The native `cell-sqlite` backend (one rusqlite file per
cell) was removed in v5: a native build has only the in-process reference
host, for unit tests (`CellStreamStore::for_identity`, whole-state
export/restore, `ManualClock` alarms). A native process that needs durable
aggregates talks to celld over the internal path (`CelldCommandHost`,
`distributed_game::celld::CelldFactSink`); see `examples/game/cells` for a
zone server's cells.

Existing `cell_state` databases are rejected with an explicit migration-required
error. They are not silently reset or opened through a legacy adapter. Back up
retained development data and migrate it explicitly before changing an existing
fleet; creating a fresh test namespace is appropriate only for disposable data.
The SQL migration inventory and checksums also reject incompatible or newer
schemas.

Queue is intentionally not modeled as a full Distributed `Bus`: it has one
consumer and no fanout. `CelldQueueRelay` is generic over `MessagePublisher`,
and `BusPublisher<B>` can route to any Distributed `Bus`. The included Worker
consumer POSTs the canonical envelope to the authenticated native relay route.
The e2e-celld host wires that one route to `BusPublisher<NatsBus>`. Kafka,
RabbitMQ, and Knative use the same Queue consumer and relay contract; only the
native `BusPublisher<B>` changes.

## Command retry receipts

The wait-path response gets its projection inputs from
`CellDispatchResult::projection_events()`, stored atomically with the command's
terminal retry receipt. It does not scan the outbox. A retry therefore returns
the same result and projection inputs even when delivery has removed the outbox
rows. Receipt evidence follows the existing command replay retention; event
history belongs in the event store, not in delivery records.

This changes the internal successful cell replay payload. Previously persisted
successful receipts are not silently accepted as the new format. The public
HTTP response shape is unchanged.

## Queue naming and sharding

The producer binding name is local to the Worker and may be any valid binding
name. This fixture calls it `OUTBOX` because it is the aggregate's durable
egress port. The fleet-wide Queue resource is `distributed-outbox` because its
role is the durable handoff from those aggregate outboxes to the bus:

```json
{
  "queues": {
    "producers": [
      { "binding": "OUTBOX", "queue": "distributed-outbox" }
    ]
  }
}
```

Use one Queue per bounded context or independently deployed write service by
default. That keeps configuration, relay deployments, DLQs, redrive, and
monitoring proportional to operational boundaries rather than to the number of
domain types. A temporary downstream-bus outage backs up every Queue targeting
that bus, so additional Queue shards do not by themselves improve that failure
mode. Start with consumer concurrency, batching, retry/DLQ policy, queue-depth
monitoring, and idempotent downstream handlers.

Split a bounded context into one Queue per aggregate type only when the types
need independent throughput, availability, pause/purge/redrive controls,
security policy, or downstream transports. Each binding then names its own
Queue, for example `TODO_OUTBOX` -> `todo-outbox` and `CHAT_OUTBOX` ->
`chat-outbox`. This prevents one aggregate type's backlog from consuming
another type's relay capacity, at the cost of another Queue, consumer
registration, DLQ, and set of operational signals for every shard.

Do not create a Queue per aggregate instance. That makes Queue and consumer
configuration grow with domain cardinality, and it does not guarantee
end-to-end ordering once concurrent relay delivery reaches the downstream bus.
Preserve stable message ids and aggregate versions so projectors can deduplicate
and reject stale transitions instead of relying on Queue topology for
correctness.

## Local celld

Prerequisites: Docker (for NATS), celld (the version in `/.celld-version`,
checked by `scripts/celld.sh`; `CELLD_BIN` selects the binary), `worker-build`, and the
`wasm32-unknown-unknown` Rust target.

```sh
make -C tests/celld up
make -C tests/celld test
make -C tests/celld down
```

celld development mode supplies its own local object store; Azurite, MinIO,
and an external S3 endpoint are not used. The canonical profile stores cell and
Queue data in `tests/celld/worker/.celld/dev`. `make down` and `make reload`
preserve that directory.

celld permits only one consumer script per Queue, and that script cannot also
export `fetch`, so it is deliberately a separate project. `celld dev` serves
one project at a time. `make up` delegates to the e2e-ui profile, which registers
`worker/relay.wrangler.jsonc`, stops that process, and then starts the aggregate
Worker from the same persistent `.celld/dev` store. The retained Queue row is
then delivered to the e2e-celld native host, accepted by NATS JetStream, and
applied by the normal Todo projection. A deployed fleet can keep the producer
and consumer deployments as separate scripts without changing the relay code.

Without `CELLD_URL`, `cargo test --test celld` checks the fixtures and skips the
live HTTP round trips.

The independent storage proof owns a temporary celld fleet and deliberately
kills only the processes it starts:

```sh
(cd tests/celld/worker && worker-build --release --features storage-conformance)
node tests/celld/storage-conformance.mjs
```

It requires Node.js, sqlite3, esbuild and celld in addition to the Worker
toolchain. It tests final-write rollback across multi-chunk event/outbox inserts
(the Worker uses at most 100 bound parameters per SQL statement), lease fencing during commit,
Queue-acceptance/delete crash redelivery with stable IDs, prearmed-alarm
recovery without another cell request, and receipt replay after delivery. It
also retains 1,101 unsent messages and more than 8 MiB of event payload in one
snapshot-backed cell, restarts it, and verifies that alarms drain the backlog
without another aggregate request. Events and receipts remain; delivered
outbox rows do not.
Queue row visibility is not a local settlement barrier: publication and deletion
are separate awaited operations, and a competing replay drain may find every row
already claimed by an alarm. The crash proof uses bounded read-only probes to
observe zero local outbox rows before replay or another command. Exact zero-row,
stable-envelope and event-count assertions remain; no sleep substitutes for
observing settlement.
It prints and retains its temporary artifact directory. Fault probes are
feature-gated out of ordinary Worker builds. CI runs this proof separately
from the full Queue/NATS/browser profile.

Every non-health aggregate route requires `DISTRIBUTED_INTERNAL_SECRET`. The
checked-in value is loopback test data only; production requires a separately
provisioned secret plus the normal network policy.

## Zones as celld objects

`zone-worker/` hosts a small tally zone with `distributed_game`'s
`celld-zone` feature (one Durable Object per zone, `celld_zone_object!`),
its `Tally` cells and the `ZoneActivity` cell. `distributed_game`'s
`celld_zone` test drives it over WebSockets: on `celld dev` (join, input,
facts, dormant → hibernated, wake with state, single-use tickets, a
doorway handoff) and on a three-node production-mode fleet on one RustFS
bucket started with docker compose (a forwarded socket closes with 1012
when the owner node is killed, the zone recovers on another node, and 200
in-flight facts reach the cell exactly once):

```sh
(cd tests/celld/zone-worker && worker-build --release)
CELLD_BIN=… CELLD_ESBUILD=… cargo test -p distributed_game --features celld,net --test celld_zone
```
