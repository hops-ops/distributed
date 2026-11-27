# Outbox, bus and transports

How committed messages leave the process (the outbox), how they travel (the
service bus and its adapters), and how consumers settle them.

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
use distributed::outbox::OutboxMessage;

let message = OutboxMessage::encode_for_entity(
    format!("{}:init", todo.entity.id()),
    "todo.initialized",
    &custom_payload,
    &todo.entity,
)?;
```

### Facts derived by event handlers

An event handler may verify an external result without making another aggregate
decision—for example, extracting document metadata after an upload. Derive a
typed fact from the incoming canonical occurrence:

```rust,ignore
#[derive(serde::Serialize, distributed::domain_event::DomainEvent)]
#[domain_event(name = "document.indexed", version = 1)]
struct DocumentIndexed {
    document_id: String,
    word_count: u64,
}

let indexed = source.derive(
    "document-indexer",
    "metadata-v1",
    &DocumentIndexed { document_id, word_count },
)?;
```

The same source, producer, output key and body produce identical canonical bytes
and an identical event ID. Different outputs or changed bodies have different
IDs. `indexed.derivation()` identifies the immediate source and producer; the
aggregate fields retain the original transition's provenance, not an invented
commit. Timestamp, causation, correlation and trace metadata come from the source.
Ordinary typed projections can consume the derived fact. Aggregate-snapshot
projections reject it because it does not establish a new aggregate version.

Publish the canonical bytes using the existing bus API and `indexed.id()` as
the message ID. Propagate its causal metadata. Acknowledge the incoming delivery
only after every output publish succeeds; a failure retries the source and may
republish an accepted prefix with the same IDs. This is at-least-once delivery,
not an atomic external effect plus publish. Aggregate decisions still use a
transactional outbox. Bound external reads and make effects idempotent before
using this pattern.

### Schedules, sagas and fenced leases

A command schedules future work for its aggregate with `ctx.schedule(Schedule)`
(`distributed::schedule`): one durable row per (aggregate, name), committed
with the aggregate's events, that a host fires back into the service as a
command, once per generation. On a shared SQL or in-memory repository that
host is `microsvc::schedule::ScheduleWorker`. The route declares
`.requires(Capability::Scheduling)`; the fired command runs with the trusted
`schedule` role (`microsvc::schedule::SCHEDULE_ROLE`, admitted by
`schedule_guard`). It is the one timer mechanism: cross-cell workflows use the
event-sourced `Saga<D>` (`distributed::saga`), an ordered sequence of steps
with compensation, whose step
timeouts are a schedule that fires the definition's `TIMEOUT_COMMAND` through
the `SagaTimeouts` route, mounted on the service over the saga repository.
Single-owner work uses TTL leases with monotonic fencing tokens
(`distributed::lease`). See
[coordination primitives](../coordination-primitives.md).

### Publishing the Outbox

How a committed row reaches the bus depends on whether a bus is attached to the
service:

- **Bus attached (`service.with_bus(bus)`)** — the row is committed `pending`
  and its id is handed to a bounded background worker, which claims it,
  publishes it and deletes it. The commit does not wait for the broker. The
  same worker also polls pending rows, so a crash or a full mailbox is
  recovered in process. Without the `runtime-tokio` feature there is no
  worker: the commit path publishes its new rows before returning, and a
  failed publish leaves them pending for a dispatcher you run.
- **No bus** — the row is committed `pending` and stays there until an
  `OutboxDispatcher` you run publishes it.

A publish that fails is retried with a growing delay (250 ms doubling to one
minute). A retryable failure, such as the broker being down, never marks a row
`failed`; only a permanent failure does, once the row has been attempted
`max_attempts` times. `OutboxStore::requeue_failed(id)` puts a failed row back
in the queue.

The polling worker is the `OutboxDispatcher` primitive composed with your
runtime's timer. Run it when no bus is attached, or as an extra competing
drainer beside one, in the service process or as a separate worker against
the same outbox store:

```rust,ignore
use distributed::microsvc::HasOutboxStore;
use distributed::outbox_worker::{BusPublisher, OutboxDispatcher};
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

A row completes (is deleted) only after `publish()` resolves `Ok`; an unknown or
failed publish releases it for a later attempt, and only a permanent failure at
the attempt ceiling moves it to `Failed`.
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
bus.send("place.bet", payload.clone()).await?;  // point-to-point command (1:1)
bus.publish("seat.reserved", payload).await?;   // fan-out event (1:N)
bus.listen(service.clone(), RunOptions::default()).await?;     // competing
bus.subscribe(service.clone(), RunOptions::default()).await?;  // fan-out

// Production: swap the one constructor line — send/listen/publish/subscribe
// and the handlers are unchanged. A named Service supplies the consumer group.
let namespace = "orders-prod";
//   let bus = NatsBus::connect("nats://localhost:4222").namespace(namespace).await?;
//   let bus = PostgresBus::new(pool);
//   let bus = SqliteBus::new(pool);
//   let bus = RabbitBus::connect("amqp://localhost:5672/%2f").namespace(namespace).await?;
//   let bus = KafkaBus::connect("localhost:9092").namespace(namespace).await?;
//   let bus = RedisStreamBus::connect("redis://localhost:6379")?.namespace(namespace);
```

This is the low-level facade. For a `microsvc::Service`, the one-call convenience
is `service.with_bus(bus).run(opts)`: it derives the command names to `listen`
and the event names to `subscribe` from the registered handlers, and makes
`repo.outbox(msg).commit(&mut agg)` publish on commit. Drop to `listen` / `subscribe`
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
| `InMemoryBus` | (always) | one queue, each command taken once | one retained log + a position per group |
| `PostgresBus` | `postgres` | `bus_queue`, `FOR UPDATE SKIP LOCKED` | `bus_log` + `bus_offset` per group, one reader per group (Kafka-style) |
| `SqliteBus` | `sqlite` | `bus_queue`, atomic `UPDATE ... RETURNING` lease claim | `bus_log` + `bus_offset` per group, one reader per group |
| `NatsBus` | `nats` | shared durable `{group}_cmd` on the stream | durable `{group}_evt` per group |
| `RabbitBus` | `rabbitmq` | default exchange → durable queue `{ns}.cmd.{name}` | topic exchange → queue `{ns}.evt.{group}` per group |
| `KafkaBus` | `kafka` | topic `{ns}.cmd` keyed by stream, shared consumer group `{ns}.{group}.cmd` | topic `{ns}.evt` keyed by stream, consumer group per service `{ns}.{group}.evt` |
| `RedisStreamBus` | `redis-streams` | stream `<prefix>:{ns}:cmd`, Redis consumer group per service | stream `<prefix>:{ns}:evt`, Redis consumer group per service |
| `KnativeBus` | `knative` | POST CloudEvent → `{target}-commands` broker ingress | POST → `{source}-events` broker; consume via generated Triggers |

`SqliteBus` uses the same single-database pattern scaled down to SQLite:
`bus_queue` is claimed with a conditional `UPDATE ... RETURNING` lease because
SQLite has no `FOR UPDATE SKIP LOCKED`, and `bus_log` / `bus_offset` provide
fan-out. It is intended for local durable transport, tests, demos, and small
single-node deployments, not as a high-throughput broker replacement.

A SQL `listen` claims one `bus_queue` row per receive and renews its lease
every third of the lease while the handler runs, so a handler may outlive the
lease without a second worker running the same row. The lease (`with_lease`,
default 30 s) is how long a crashed worker's command waits for redelivery.

### One wire envelope

Every adapter writes and reads a `Message` through `WireEnvelope`. The id,
kind and content type travel as the reserved headers `x-distributed-id`,
`x-distributed-kind` and `x-distributed-content-type`, ahead of the metadata
(Kafka record headers, NATS headers, the AMQP header table, Redis
`header:<key>` fields). Kafka, which has one topic per kind, also carries the
message name in `x-distributed-name`. Knative maps them onto CloudEvents attributes: `ce-id`,
`content-type` and the `distributedkind` extension. Metadata that names a
reserved header in any letter case is refused at publish with a permanent
error, and a delivery that repeats one is refused on receive. The same
validation (message name, stable id, content type, metadata keys) runs at the
publish boundary of the SQL, in-memory and celld Queue transports. NATS also
writes `Nats-Msg-Id` for JetStream deduplication and refuses `Nats-*`
metadata. A delivery without `x-distributed-kind` is an event.

`KnativeBus` implements only `Bus` (produce → broker-ingress POST). It has no
in-process consume loop: `KnativeBus::manifests(&plan, &subscriptions)` renders the
role-based `Broker` + per-name `Trigger` YAML, and the service mounts
`cloud_events_router` so those Triggers reach `dispatch_message`. A command a
Trigger delivers runs as the transport identity, so its declaration lists
`TRANSPORT_ROLE`, and the CloudEvent id is the id its ledger deduplicates on.

### Running, retries and dead letters

`RunOptions` decides how a consumer run behaves. The same options mean the
same thing on every transport, because the runner applies them above the
adapter.

```rust,ignore
use distributed::bus::{FailurePolicy, RunOptions, Shutdown};

let shutdown = Shutdown::new();
bus.listen(
    service.clone(),
    RunOptions::until_shutdown(shutdown.clone())
        .with_max_attempts(8)
        .with_failure_policy(FailurePolicy::DeadLetter),
).await?;
// elsewhere: shutdown.trigger();
```

- **When a run ends.** `RunOptions::default()` and `until_shutdown(signal)`
  keep consuming while the source is quiet. The run returns `Ok(())` only when
  the signal is triggered, after settling the delivery in hand.
  `RunOptions::until_idle()` returns as soon as nothing is available; it is for
  tests and one-shot drains. On Kafka "nothing is available" means the
  consumer has joined its group and read every assigned partition to its
  end (checked against the broker's high watermarks each time it is asked),
  not that the topic was quiet for a while; `with_fetch_timeout` (8 s by
  default) only bounds one wait on the broker, for the group to be joined or
  for the next record.
- **Retries.** A retryable failure (for example a transient `NotFound`) is
  redelivered after a backoff that doubles with each delivery (50 ms up to 5 s
  by default; `with_backoff`). `with_max_attempts(n)` dead-letters a message
  that still fails on its `n`th delivery. Without it retries are unlimited.
- **Permanent failures** follow the `FailurePolicy`: `DeadLetter` (default),
  `Retry`, `LogAndAck`, or `Stop`. A deterministic, durably recorded rejection
  is a permanent error, not a retryable one.
- **Dead letters.** The runner stores `{message, reason, consumer, attempts}`
  and only then acknowledges the delivery. A bus stores dead letters by
  publishing a `distributed.dead_letter` event to itself; `with_dead_letters`
  names another `DeadLetterSink`. If the dead letter cannot be stored the
  delivery is handed back and the run ends with an error.

RabbitMQ reports only whether a delivery is a redelivery, so `max_attempts`
above 2 never triggers there. Positional sources (Kafka, the SQL log) cannot
deliver a later message first: they hold their position for the backoff.

The [bus contract](../bus-contract.md) lists what every transport promises
and where they differ. One conformance suite runs it against every bus.

A service can place independent route bundles in named delivery lanes
(`Service::lane(name, routes)`). Each lane keeps broker order and runs
concurrently with the others, and a delivery is settled only after every lane
finished it. See [consumer delivery lanes](../consumer-delivery-lanes.md) for
the contract and when a route may leave the default lane.

### Transport boundaries (producer vs consumer)

`microsvc` owns registration, guards, typed decoding, and dispatch. **Transport
adapters** own receive/ack/retry/publish and topic mapping. Shared vocabulary lives
in `bus` (no concrete broker dependency).

| Type | Purpose |
| --- | --- |
| `TransportError` / `TransportErrorKind` | Retryable vs permanent — drives redelivery vs failure policy |
| `FailurePolicy` / `FailureAction` | Permanent failure: `Retry`, `DeadLetter`, `LogAndAck`, `Stop` |
| `RunOptions` | When a run ends, retry backoff, max attempts, dead-letter sink |
| `DeadLetterSink` | Where a given-up message is stored before it is acknowledged |
| `MessageSource` + `run_source` | Pull loop: `recv`, dispatch, settle only after the handler finishes; `finish` once when the run ends (writes a batched offset, gives up claims) |
| `MessagePublisher` + `outbox_worker::OutboxDispatcher` | Publish threshold for outbox completion; claim → publish → complete |

**Two confirmation thresholds** (do not collapse them):

1. **Producer publish** — when an outbox row may be deleted (SQL commit,
   broker confirm/ack, Knative 2xx, in-memory accept). Unknown outcomes stay retryable.
2. **Consumer ack** — only after the handler (and optional inbox receipt) committed.
   Never silently ack a handler error.

```rust,ignore
use distributed::bus::{run_source, RunOptions};

// Low-level receive loop (facade buses wrap this). A bare source has no
// dead-letter sink; name one or choose another failure policy.
run_source(service, source, RunOptions::default().with_dead_letters(sink)).await?;
```
