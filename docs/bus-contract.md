# Bus contract

What every `Bus + BusConsumer` promises, and where a transport differs. The
promises are tested by one suite, `tests/common/src/transport/bus.rs`, which
the in-memory, SQLite, Postgres, NATS, Kafka, RabbitMQ, Redis Streams and
Knative buses all run. Where a transport differs, its harness declares the
difference (`BusCapabilities`) and the suite checks the declared behaviour.

## What every bus does

| Promise | Meaning |
| --- | --- |
| **At least once** | A message is delivered until a handler finishes it. A handler may see the same message more than once, so handlers must be idempotent. |
| **Commands are point-to-point** | `send` + `listen`: within one consumer group, one consumer handles each command. |
| **Events fan out** | `publish` + `subscribe`: every consumer group gets every event published after the group first subscribed. A group keeps its position between runs. |
| **A group shares its work** | Consumers of one group running side by side handle each message once between them (Kafka: see below). |
| **The message arrives unchanged** | Name, kind, id, payload, content type and user metadata are what was sent. |
| **One stream keeps its order** | Messages of one aggregate (the `x-distributed-source-aggregate-id` metadata the outbox sets) of one name arrive in the order they were produced. |
| **Retryable failures are redelivered** | The message is handed back and delivered again after a backoff that grows with each delivery (`RunOptions::with_backoff`, `with_max_attempts`). |
| **Permanent failures are dead-lettered** | The runner stores `{message, reason, consumer, attempts}` as a `distributed.dead_letter` event on the same bus, then acknowledges the delivery. Later messages are not held up. A payload the handler cannot decode is a permanent failure like any other. |
| **A consumer runs until shutdown** | A quiet transport does not end a run. `Shutdown::trigger` ends it with `Ok(())` after the delivery in hand is settled. `RunOptions::until_idle()` is the drain for tests: it ends when the transport has nothing to deliver (a Kafka consumer: its group joined and every assigned partition read to its end). |

Retry, the attempt limit, dead-lettering and shutdown are done by the runner
above the transport, which is why they are the same everywhere. A transport
only receives, acknowledges, and hands a delivery back with a delay.

## Where transports differ

Declared in each harness and checked by the suite:

| Capability | In-memory | SQLite / Postgres | NATS | Kafka | RabbitMQ | Redis Streams | Knative (emulated) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Same id produced twice is stored once | events | events | yes, within the dedupe window | no | no | yes, within the dedupe window | events |
| A message retried at once keeps its place | yes | yes | no | yes | no | yes | yes |
| Order across names (one stream) | yes | yes | yes | yes, keyed by stream | events only | yes | yes |
| A group member joining or leaving may redeliver | no | no | no | yes (partition rebalance) | no | no | no |
| Metadata added on delivery | none | none | none | none | none | none | `source` |

Not covered by the suite; do not rely on them across transports:

| Behaviour | In-memory | SQLite / Postgres | NATS | Kafka | RabbitMQ | Redis Streams |
| --- | --- | --- | --- | --- | --- | --- |
| Backoff delay honoured | no, redelivered at once | yes | yes | yes, by holding the consumer | yes, by holding the consumer | yes, capped at the lease |
| Delivery count seen by `max_attempts` | exact | exact (queue); not counted (log) | exact | counted per consumer process | 1 or 2 only | exact |
| Ordered-delivery evidence for projections | yes | yes | yes | no | no | no |
| Events published before a group first subscribes | delivered | delivered | delivered | delivered | dropped | delivered |

The shapes behind the table:

- **In-memory** keeps one command queue and one event log, both in publish
  order. A nacked command goes back to the head of the queue. One subscriber
  of a group reads at a time; the others wait until it stops.
- **SQLite / Postgres** keep commands in `bus_queue` (claimed under a lease,
  `SKIP LOCKED` on Postgres) and events in `bus_log`. One subscriber of a
  group reads the log at a time, under the group's lease in
  `bus_consumer_lease`; the others receive nothing until it stops or its
  lease expires. A subscriber reads 16 events per transaction and writes its
  offset once per batch and when it stops. A held claim or group lease is
  renewed every third of the lease, counted from when each renewal started,
  so it lapses only if the holder stalls, or one renewal takes, longer than
  two thirds of the lease. Starting a consumer reads the catalog and runs
  the schema DDL only when a bus table or index is missing: `CREATE INDEX IF
  NOT EXISTS` locks the table, which would queue every claim, renewal and
  ack behind the longest write in flight.
- **Kafka** sends every message of a kind to one topic (`{ns}.cmd`,
  `{ns}.evt`), keyed by the stream id (else the message id), and carries the
  name in the `x-distributed-name` header. A consumer acknowledges names it
  does not handle. A consumer is idle once it has joined its group and read
  every partition assigned to it to its end (a topic that does not exist yet
  assigns none), and the broker's high watermarks show nothing past what it
  has read, so a drain that looks again after another consumer's dispatch
  sees what that dispatch produced; the fetch timeout (8 s) only bounds one wait on the broker,
  for the group to be joined or for the next record, so a broker that stops
  answering cannot hang a drain.
- **RabbitMQ** declares one queue per command name on send, so a command
  sent before any listener is kept; a listener of two names reads two
  queues. A group's events share one queue.
- **Redis Streams** sends every message of a kind to one stream
  (`<prefix>:{<ns>}:cmd`, `…:evt`) read with a consumer group. A nacked or
  abandoned entry is claimed (`XAUTOCLAIM`) before new ones are read.
- **Knative** delivers by HTTP push through Triggers and has no in-process
  consumer. Its suite runs `KnativeBus` against a broker emulator that
  decodes each CloudEvent with the ingress decoder (`decode_cloud_event`)
  and keeps it in an in-memory bus: it proves the produce path and the
  CloudEvent round trip, and its delivery semantics are the in-memory bus's.

## Adding a transport

Implement `BusHarness` in the transport's test target and call
`bus_conformance_tests!(TheHarness);`. `start` sets up an isolated namespace
and returns `None` when the service is not configured; `bus()` returns a
connected bus with no group of its own (every suite consumer names its group
through its router). Declare `CAPABILITIES` honestly: the suite fails a
transport that promises more than it does, and a deduping transport that
says it does not.
