# Consumer delivery lanes and retry backoff

One durable consumer (`listen`/`subscribe` group) delivers messages in broker
order and, by default, runs every registered route for a message before it
receives the next one. That is the simplest ordering contract, but it couples
latency across unrelated work: a process-manager policy that only turns a fact
into the next idempotent command waits behind every projection and slow
external effect registered for earlier messages. It also lets one message
that keeps failing monopolize the consumer.

## Retry backoff for retryable failures

A retryable failure NAKs the delivery so the broker redelivers it later. On
NATS JetStream the NAK carries a delay derived from the delivery count:
`base * 2^(delivered - 1)`, capped (defaults 50 ms and 5 s;
`NatsBus::with_nack_backoff` / `NatsJetStreamSource::with_nack_backoff`).
The first retry is still prompt. A message that fails every time stops
being redelivered ahead of newer messages in a hot loop. Retries remain
unlimited and the message is still retained; only the redelivery time moves.
Setting the base to zero restores an immediate NAK.

This is not a substitute for classifying failures correctly. A command
rejection that is durably recorded under a deterministic command identity
replays the same rejection on every retry. The handler must return a
permanent error, which the configured failure policy (dead-letter by default)
settles and records in transport metrics. Transient infrastructure failures
stay retryable.

## Lanes

A router may partition its routes into named **lanes**. `Service::lane(name,
routes)` registers a route bundle in a lane. Plain `Service::routes` uses the
default lane. Lanes are opt-in. A service that registers no named lane runs
exactly as before.

When a router has more than one lane and the source settles each delivery
independently (`MessageSource::settles_independently`, true for NATS
JetStream), the runner:

1. receives messages in broker order and hands each one to every lane that
   has a route for it;
2. runs each lane's messages strictly in receive order, one at a time, with
   the lane's routes in registration order. This is the same order a
   single-lane consumer gives those routes;
3. runs different lanes concurrently, so a slow route in one lane cannot delay
   another lane;
4. settles a delivery only after every lane that received it has finished it.
   Any retryable lane failure NAKs the delivery, and redelivery runs all of its
   lanes again. A permanent failure applies the failure policy once;
5. bounds the number of received but unsettled deliveries
   (`RunOptions::with_lane_window`, default 16). When the window is full the
   runner stops receiving until a delivery settles;
6. stops a lane at its first stop-class failure (`FailurePolicy::Stop` or a
   retain-and-stop error such as `ApplicationReloading`). That delivery is
   settled as in sequential mode. Later deliveries queued for the halted lane
   are NAKed, not skipped. Other in-flight deliveries finish and settle, then
   the run returns the error.

The default lane keeps its existing behavior, including route-order
dependencies between its routes within one delivery.

Sources whose acknowledgement is positional (Kafka offset commits) or
lease-based (SQL table rows) report `settles_independently() == false`. For
them the runner keeps the sequential loop and dispatches every lane's routes
in order for each message. Lanes then change nothing about delivery.

### What a lane must satisfy

Put a route bundle in its own lane only when **no route in another lane
depends on its effects within the same delivery, and it depends on none of
theirs**. Typical examples are process-manager policies that read only the
delivered fact and send idempotent commands. A route that reads a projection
written by an earlier route for the same message must stay in that route's
lane.

Lanes also reorder effects **across deliveries**: one lane may finish later
messages while another lane is still working on earlier ones. A route whose
correctness (or whose downstream readers' correctness) needs another lane's
effects from an *earlier* message — for example, a process policy that
completes a workflow the UI then reads through a projection maintained in
another lane — must share that lane, or its readers must tolerate the
projection arriving later. Forge hit this: provisioning completed before the
read-access projection existed, producing transient 404s, until the
derivation moved into the process lane.

Lanes do not weaken existing guarantees:

- **Ordering:** each lane observes deliveries in broker order. A redelivery
  (after NAK or ack-wait expiry) can arrive after later messages. That is
  already true for a single-lane consumer.
- **At-least-once:** a delivery is acknowledged only after all of its lanes
  succeeded. A crash or failure before that redelivers it to every lane, so
  every route must stay idempotent, as today.
- **Failure policy and DLQ:** unchanged, applied once per delivery after all
  lanes finish.
- **Ack deadline:** a delivery waiting in a busy lane still counts against the
  broker's ack wait. Keep the window small enough that a lane's backlog clears
  well within it. An expired delivery is redelivered, which is safe under the
  idempotency requirement.
