# Coordination primitives: sagas, durable timers and fenced leases

A cell has one atomic boundary. Work that spans cells, or must happen later,
or must be owned by exactly one process at a time needs explicit
coordination. Three primitives cover the common shapes: a zone transfer, an
encounter settlement with a deadline, a boss controller that must never run
twice.

| Need | Primitive | Module |
|------|-----------|--------|
| Change state in two cells with rollback | Saga / process manager | `distributed::saga` |
| Run something at a time in the future, across restarts | `OutboxMessage::deliver_at` | `distributed::outbox` |
| One owner per key, with stale writers rejected | Lease with fencing token | `distributed::lease` |
| Deterministic time in tests | `Clock`, `ManualClock` | `distributed::clock` |

All three are dependency-free in core. The SQL lease stores follow the
existing `sqlite` / `postgres` features. No new cargo feature was added and
default features are unchanged.

## Durable timers: `deliver_at`

```rust
let timer = OutboxMessage::create_to(id, "settle-encounter", "encounters", payload)?
    .deliver_after(now, Duration::from_secs(10));
repo.outbox(timer).commit(&mut encounter).await?;
```

A scheduled outbox row is committed with the change that arms it, so it is
exactly as durable as that change. Every claim path skips it until it is due:

- the in-memory store (`OutboxMessage::is_claimable_at`);
- the SQLite and Postgres claim queries, which already filtered on the
  `next_available_at` column. That column now stores `deliver_at` and falls
  back to `created_at`, so **no schema change was needed** and existing rows
  (and `None`) stay immediately deliverable;
- explicit-id claims (`ClaimOutboxMessages::for_ids`), so the after-commit
  immediate publisher cannot bypass the schedule;
- the in-transaction pre-claim used when a repository has a publish hook but
  no worker mailbox (aggregate commits and causal routes). Scheduled rows are
  left pending.

The drain worker (`OutboxDrainRunner`) picks a due row up on its next poll,
so a timer fires within one poll interval of its deadline. `release` (a
retry) resets availability to now, so a timer that failed to publish is
retried promptly instead of being re-delayed. `OutboxMessage.deliver_at` is
`#[serde(default)]`.

## Leases with fencing tokens

```rust
let lease = leases.acquire("boss:hydra", "zone-7", Duration::from_secs(15)).await?;
// every 5s
let lease = leases.renew(&lease, Duration::from_secs(15)).await?; // Err(Lost) => stop acting
// in the protected aggregate (same cell, same transaction as the write):
self.fence.admit("boss:hydra", cmd.fencing_token)?;               // Err(StaleFence) => reject
```

- `acquire` gives a free, expired or released key to the caller with a token
  strictly greater than any earlier token for that key. Re-acquiring a live
  lease you already hold returns it unchanged (an idempotent retry). A key
  held by someone else returns `LeaseError::Held`.
- `renew` extends a live lease and keeps its token. `release` ends it early.
  The row is **kept**, with `expires_at = 0`, so the counter never resets.
- `Fence` is a serializable high-water mark to embed in the protected state.
  It admits tokens greater than or equal to the newest one seen. This is the
  authoritative check. `LeaseStore::check_fence` is only a point-in-time
  advisory check.

Backends: `InMemoryLeaseStore<C: Clock>` (use a `ManualClock` in tests),
`SqliteLeaseStore` and `PostgresLeaseStore`. Every SQL operation is one
atomic conditional statement evaluated against the **database clock**. The
`coordination_leases` table comes from repository migration
`0010_coordination_leases` (inventory version 10), or from
`SqlxLeaseStore::migrate` for standalone pools.

This is a sibling of `distributed::lock`, not an extension of it. The lock
module is `QueuedRepository`'s internal mutual-exclusion optimization. Its
`Lock` trait has no holder identity, renewal or token, and it deletes
`aggregate_locks` rows on release, which would reset a token.

## Sagas

A saga is an event-sourced aggregate, `Saga<D: SagaDefinition>`, with one
stream per instance. The definition is pure. It lists the steps, says which
command performs each step and which command undoes it, and folds reply
payloads into the saga data. The framework owns the state machine.

```text
start ─► dispatch(step 0) ─► reply ok ─► dispatch(step 1) ─► reply ok ─► Completed
                 │                              │
                 │ reply failed                 │ timeout (outcome unknown)
                 ▼                              ▼
       compensate applied steps        compensate applied steps + step 1
       in reverse ─► Compensated       in reverse ─► Compensated
                     (or Stuck after MAX_COMPENSATION_ATTEMPTS)
```

Each transition returns outbox messages that are committed **with** the
saga's events: the participant command, plus a `saga-timeout` timer to the
saga's own `REPLY_TO` queue, scheduled with `deliver_at = now + step.timeout`.
A crash between deciding and sending is impossible. A timer for a step that
already finished arrives later and is `Ignored`.

Participants answer with `SagaReply::from_outbox(&cmd)?.succeeded(..)` or
`.failed(reason)` and commit the reply with their own state change. Inbound
replies and timers become `SagaMessage::from_outbox` / `from_bus` and go to
`SagaRunner::handle`. Inside a `microsvc` handler you can also call
`Saga::handle` and commit with `ctx.repo().outbox(..)`.

Guarantees and obligations:

- **Idempotent.** The saga records every message id that changed its state,
  so a redelivery is `Duplicate`. Late replies and superseded timers are
  `Ignored`. Concurrent handlers are fenced by the saga stream's optimistic
  version, and `SagaRunner` reloads and retries on `ConcurrentWrite`.
- **Deterministic ids.** Command ids are `type:saga:step:phase:attempt` and a
  reply id is `<command id>:reply`. Participants dedupe on the command id
  (an inbox receipt or aggregate state) and do not re-emit a reply on
  redelivery.
- **A timeout means the outcome is unknown,** so the timed-out step is
  compensated as well. Compensations must be idempotent and must tolerate
  arriving *before* their action. The participant records a tombstone for
  the saga id and rejects a late action.
- **`on_step_succeeded` is replayed,** so keep it deterministic.

### Worked example

`tests/saga_transfer/main.rs` moves value between wallets in two separate
cells. The `debit` step goes to the source cell (compensated by `refund`) and
the `credit` step goes to the target cell (compensated by `reverse_credit`).
A test network delivers outbox rows whose `deliver_at` is due on a
`ManualClock`, so timeouts fire without sleeping. The scenarios are:

- the happy path, including an idempotent restart and ignored leftover timers;
- target rejection, where the debit is refunded;
- source rejection, where nothing needs compensating;
- target timeout, where both steps are compensated and the late credit is
  neutralized;
- the compensation arriving before the late action, where the tombstone
  rejects the credit;
- compensation timeouts that are retried and then marked `Stuck`;
- duplicate command delivery and duplicate replies;
- saga state and its timer surviving a restart on a SQLite file database.

## Tests

```sh
cargo test --features sqlite,postgres --test saga_transfer --test coordination_lease --test scheduled_outbox
cargo test --features sqlite --lib -- outbox lease clock
```

The Postgres cases skip when `DATABASE_URL` is unset (see `compose.yaml`).
