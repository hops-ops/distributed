# Bus contract

What every `Bus + BusConsumer` promises, and where a transport differs. The
promises are tested by one suite, `tests/transport_conformance/bus.rs`, which
the in-memory, SQLite, Postgres, NATS, Kafka and RabbitMQ buses all run.

## What every bus does

| Promise | Meaning |
| --- | --- |
| **At least once** | A message is delivered until a handler finishes it. A handler may see the same message more than once, so handlers must be idempotent. |
| **Commands are point-to-point** | `send` + `listen`: within one consumer group, one consumer handles each command. |
| **Events fan out** | `publish` + `subscribe`: every consumer group gets every event published after the group first subscribed. A group keeps its position between runs. |
| **The message arrives unchanged** | Name, kind, id, payload, content type and user metadata are what was sent. |
| **Retryable failures are redelivered** | The message is handed back and delivered again after a backoff that grows with each delivery (`RunOptions::with_backoff`, `with_max_attempts`). |
| **Permanent failures are dead-lettered** | The runner stores `{message, reason, consumer, attempts}` as a `distributed.dead_letter` event on the same bus, then acknowledges the delivery. Later messages are not held up. |
| **A consumer runs until shutdown** | A quiet transport does not end a run. `Shutdown::trigger` ends it with `Ok(())` after the delivery in hand is settled. `RunOptions::until_idle()` is the drain for tests. |

Retry, the attempt limit, dead-lettering and shutdown are done by the runner
above the transport, which is why they are the same everywhere. A transport
only receives, acknowledges, and hands a delivery back with a delay.

## Where transports differ

These are not covered by the suite. Do not rely on them across transports.

| Behaviour | In-memory | SQLite / Postgres | NATS | Kafka | RabbitMQ |
| --- | --- | --- | --- | --- | --- |
| Event order across names | per name only | publish order | publish order | per topic (one topic per name) | per group queue |
| A redelivered message blocks later ones | yes | yes (log); no (queue) | no | yes | no |
| Backoff delay honoured | no, redelivered at once | yes | yes | yes, by holding the consumer | yes, by holding the consumer |
| Delivery count seen by `max_attempts` | not counted | exact (queue); not counted (log) | exact | counted per consumer process | 1 or 2 only |
| Two consumers of one group, events | may both handle an event | may both handle an event | compete | compete | compete |
| Publishing the same id twice | stored once | stored once | stored once within the dedup window | stored twice | stored twice |
| Ordered-delivery evidence for projections | yes | yes | yes | no | no |
| Events published before a group first subscribes | delivered | delivered | delivered | delivered | dropped |

An application that needs `created` before `shipped` for the same aggregate
must not depend on the bus for it on Kafka or the in-memory bus today. The
projection engine orders by its own cursor where the transport gives
ordered-delivery evidence.

## Adding a transport

Implement `BusHarness` in the transport's test target and call
`bus_conformance_tests!(TheHarness);`. `start` sets up an isolated namespace
and returns `None` when the service is not configured; `bus(group)` returns a
connected bus for that consumer group.
