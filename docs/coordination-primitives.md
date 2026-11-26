# Coordination primitives: sagas, schedules and fenced leases

A cell has one atomic boundary. Work that spans cells, or must happen later,
or must be owned by exactly one process at a time needs explicit
coordination. Three primitives cover the common shapes: a zone transfer, an
encounter settlement with a deadline, a boss controller that must never run
twice.

| Need | Primitive | Module |
|------|-----------|--------|
| Change state in two cells with rollback | Saga / process manager | `distributed::saga` |
| Run something at a time in the future, across restarts | `Schedule` (`ctx.schedule`) | `distributed::schedule` |
| A cell schedules its own future or recurring work | Re-armable cell alarm | `distributed::cell_host` |
| One owner per key, with stale writers rejected | Lease with fencing token | `distributed::lease` |
| Deterministic time in tests | `Clock`, `ManualClock` | `distributed::clock` |

All three are dependency-free in core. The SQL lease stores follow the
existing `sqlite` / `postgres` features. No new cargo feature was added and
default features are unchanged.

## Schedules: an aggregate schedules its own work

`Schedule` is the one durable timer. When an aggregate must act on itself
later (a matchmaking tick, an invite expiry, a daily reset, a lease expiry, a
saga step's timeout), a causal command handler **schedules** it with its
events (`distributed::schedule`). Outbox rows are never delayed except by a
publish retry's backoff; work that must happen later is a schedule whose
handler emits the message then.

```rust
// on the route: what the command needs from its host
routes.typed_command(start).requires(Capability::Scheduling).requires(Capability::Clock)

// in the handler
let now = ctx.now()?;                                   // the host clock
ctx.schedule(Schedule::after("tick", Duration::from_secs(5), "pool.tick", &TickInput { pool })?)?;
ctx.cancel_schedule("invite-expiry")?;

// in the schedule's own handler: re-schedule from the fired due time (no drift)
let due = ctx.scheduled_due().expect("fired by a schedule") + period;
ctx.schedule(Schedule::at("tick", due, "pool.tick", &input)?)?;
```

- Schedule rows are part of `CommitBatch` (`batch.schedules`) and commit in
  the **same transaction** as the command's events, ledger receipt and
  outbox rows, on every backend: a cell's own `cell_alarms` table (migration
  11), the in-memory repository's per-stream table, SQL's
  `aggregate_schedules` (migration 12, keyed by the owning stream). A failed
  command schedules nothing. The batch must commit exactly one aggregate,
  which owns the schedule. `Schedule::after` resolves against the backend's
  clock at commit (cell clock, injected `Clock`, database clock).
- One row per (owner, name). Scheduling a name again replaces it and bumps
  its `generation`; `cancel_schedule` disarms it.
- When due, the host dispatches the command **into the owner** through its
  command ledger, with a command id derived from `(name, generation)` and
  the trusted session from `microsvc::schedule::schedule_session` (role
  `schedule`, plus claims only the host sets). Guard schedule-only commands
  with `microsvc::schedule::schedule_guard`
  so callers cannot invoke them. A crash after the handler committed but
  before the schedule settled replays the receipt; the handler never runs
  twice for one arm.
- Rejections disarm the schedule (reported as `failed`). Retryable failures
  back off from 1 s, doubling, up to 5 min (`retried`). At most 32 schedules
  fire per wake, 16 schedule writes per command and 1,024 armed schedules
  per aggregate.

**Capabilities.** Backends declare what they provide (`Capabilities`):

| Backend | Stores schedules | Dispatches them (`Scheduling`) | Clock (`Clock`) |
|---|---|---|---|
| Aggregate cell (celld; reference host) | `cell_alarms` | yes: the Durable Object alarm; `fire_due_schedules` on the reference host | DO clock / store clock |
| In-memory repository | per-stream table | with a schedule worker attached (`ScheduleWorker::attach`) | injected `Clock` (`with_clock`) |
| SQL (Postgres, SQLite) | `aggregate_schedules` | with a schedule worker attached (`ScheduleWorker::attach`) | database clock |

A route declares what it needs with `.requires(..)`; registering it on a
backend without it panics with the missing capabilities while the service is
built, never at the first commit. `ctx.now()` answers only on routes that
declared `Clock` (the dispatcher stamps the host clock into the session and
removes any caller value); `ctx.schedule` refuses unless the route declared
`Scheduling`.

**Shared stores: the schedule worker** (`distributed::microsvc::schedule`,
any Tokio runtime feature). SQL and in-memory repositories store schedules
for every aggregate in one table, so a worker process fires them:

```rust
let (repo, schedules) = ScheduleWorker::attach(
    PostgresRepository::connect_and_migrate(url).await?,
    ScheduleWorkerConfig::default(),
); // repo now declares Scheduling
let service = Arc::new(build_service(repo.clone()));
let worker = schedules.spawn(service); // worker.stop().await on shutdown
```

- Each pass claims up to 32 due, unclaimed rows in one statement
  (`UPDATE … WHERE … IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`;
  SQLite serializes writers instead), marking them `claimed_until_ms`
  (default 30 s ahead). Concurrent workers in several processes get
  disjoint rows.
- Each claimed arm is dispatched through the `Service` with
  `schedule_session` and a command id derived from the owning stream, the
  name and the generation (one ledger serves every aggregate here, so the
  owner is part of the id). A worker that crashes after the command
  committed leaves the claim to lapse; the next worker re-dispatches the
  same id and the ledger replays it, so the arm fires once.
- Settlement touches only the generation that fired, like on celld.
- The scheduled command finds its aggregate through its input; the handler
  that called `ctx.schedule` supplies that input.

**Latency.** On celld the Durable Object alarm wakes the cell at the due
time (a few milliseconds late) without any extra process. The schedule
worker fires a due row within one poll interval (`poll_interval`, default
250 ms) plus the dispatch, and only while a worker runs; in the test suite
with a 20 ms poll the observed delay past due was about 20–45 ms on SQLite.
Lower the interval for tighter timing at the cost of one claim query per
interval per worker.

**celld.** A Durable Object has one alarm. The outbox watchdog
(`CelldOutbox`) and due schedules share it: every `setAlarm` targets the
earliest of the next schedule and the next drain, so a watchdog rearm never
postpones a schedule and a far schedule never postpones a drain. The
Worker's `alarm()` handler calls `AggregateCell::alarm(&env)`, which fires
due schedules, then drains the outbox and re-plans the wake. Commands that
schedule move the wake earlier right after they commit.

**Reference host.** The in-process host (unit tests; durable cells run on
celld) has no timer. Give the store a `ManualClock`
(`CellStreamStore::with_clock`), advance it and call
`AggregateCell::fire_due_schedules()`: the same ledger and settlement path
runs deterministically.

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

Each transition returns what is committed **with** the saga's events: the
participant commands (outbox rows) and the saga's timeout schedule
(`SagaTransition::timer`). A crash between deciding and sending is
impossible.

**Timeouts are a `Schedule`.** Each saga stream owns one schedule,
`saga.timeout` (`SAGA_TIMEOUT_SCHEDULE`). Every dispatch re-arms it at
`now + step.timeout`, which bumps its generation, so the previous step's
timeout never fires; a terminal saga cancels it. When due, the host dispatches
`SagaDefinition::TIMEOUT_COMMAND` with a `SagaTimeout { saga_id, step, phase,
attempt }` into the saga through the `SagaTimeouts<D>` route, which calls
`Saga::time_out`. Mount that route on a service over the saga repository and
attach a schedule worker (or run on a host that fires its own schedules):

```rust
let (repo, worker) = ScheduleWorker::attach(repo, ScheduleWorkerConfig::default());
let service = Arc::new(Service::new().named("transfer-saga").routes(
    Routes::new()
        .with_repo(repo.clone().aggregate::<Saga<Transfer>>())
        .mount(SagaTimeouts::<Transfer>::new()),
));
let _timeouts = worker.spawn(service);
let runner = SagaRunner::<_, Transfer>::new(repo);
```

The timeout runs through the command ledger once per generation; a timeout
whose `(step, phase, attempt)` the saga no longer awaits is `Ignored`.

A participant is a typed command of a `Service`, delivered by the bus. It
declares the transport role (`.roles([TRANSPORT_ROLE])`); a typed command
that does not is refused when a bus delivers it. The handler builds the
reply from its session and stages it in the same commit as its state change:

```rust
let reply = SagaReply::from_session(ctx.session()).map_err(rejected)?;
// ... load the aggregate, apply or refuse the step ...
let reply = reply.succeeded(output).map_err(rejected)?;
ctx.outbox(reply).commit(aggregate)?.succeeded(payload)
```

(`rejected` maps a `SagaError` to `HandlerError::Rejected`;
`tests/services/saga_transfer/mod.rs` is the complete participant.)

`from_session` reads the saga headers, which arrive as session claims, and
the saga command id, which the transport puts in `TRANSPORT_MESSAGE_ID_CLAIM`.
A step the participant refuses is still a committed command: stage
`reply.failed(reason)` and return `Ok`. Returning `Err` records the rejection
in the ledger but stages no reply, so the saga would wait for its timeout.

Inbound replies become `SagaMessage::from_outbox` / `from_bus` and go to
`SagaRunner::handle`. Inside a `microsvc` handler you can also call
`Saga::handle` and commit the transition's messages and timer through `ctx`
(`ctx.outbox(..)`, `ctx.schedule(..)` / `ctx.cancel_schedule(..)`).

Guarantees and obligations:

- **Idempotent.** The saga records every message id that changed its state,
  so a redelivery is `Duplicate`. Late replies and timeouts of a dispatch
  the saga no longer awaits are `Ignored`. Concurrent handlers are fenced by the saga stream's optimistic
  version, and `SagaRunner` reloads and retries on `ConcurrentWrite`.
- **Deterministic ids.** Command ids are `type:saga:step:phase:attempt` and a
  reply id is `<command id>:reply`.
- **Redelivery is deduplicated by the command ledger** for a typed
  participant. The ledger command id is derived from the service name, the
  command name and the saga command id, so the same saga command delivered
  again replays the stored receipt: the handler does not run, no state
  changes and no second reply is staged. The first reply cannot be missing,
  because it was committed in the transaction that wrote the receipt. This
  covers one command id only. A retry of a compensation is a new attempt
  with a new command id, and the handler runs again.
- **Write every participant as a typed command.** The bus, Knative and
  schedule ingresses all dispatch through `CommandHost::invoke` with a
  trusted-transport principal and a ledger command id derived from the
  message id, so the deduplication above is the framework's, not the
  participant's. The command declares the `transport` role to be reachable
  over the bus.
- **A timeout means the outcome is unknown,** so the timed-out step is
  compensated as well. Compensations must be idempotent and must tolerate
  arriving *before* their action. The participant records a tombstone for
  the saga id and rejects a late action. This is the participant's own
  state on every path: the ledger knows command ids, not which transfer was
  cancelled.
- **`on_step_succeeded` is replayed,** so keep it deterministic.

### Fan-out steps

A step can send commands to N cells chosen at run time, for example one
ticket reservation per party when a match starts. `action` returns
`SagaAction::FanOut(commands)` instead of a single command
(`SagaCommand` converts with `.into()`); `compensation` and
`on_step_succeeded` receive the member index (`0` for single steps).

```rust
fn action(step: &str, saga_id: &str, d: &MatchStart) -> Result<SagaAction, SagaError> {
    match step {
        "reserve" => Ok(SagaAction::fan_out(
            d.parties.iter().map(|p| SagaCommand::json(format!("party:{p}"), "ticket.reserve", &saga_id))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Ok(SagaCommand::json("match", "match.confirm", &saga_id)?.into()),
    }
}
fn compensation(step: &str, member: usize, saga_id: &str, d: &MatchStart)
    -> Result<Option<SagaCommand>, SagaError> {
    match step {
        "reserve" => {
            SagaCommand::json(format!("party:{}", d.parties[member]), "ticket.release", &saga_id).map(Some)
        }
        _ => Ok(None),
    }
}
```

- 1 to `MAX_SAGA_FAN_OUT` (64) members; an empty or larger fan-out is a
  `Definition` error. Membership must be a deterministic function of the data.
- Every member gets its own command, carrying `saga-member` metadata. Command
  ids are `type:saga:step:phase:attempt:member`, so retries dedupe per
  member. The members share the step's deadline, so one timeout schedule
  covers them: when it fires, every member that has not replied times out.
- The step **succeeds** when every member replies `succeeded`. It
  **settles** only after every member has replied or timed out, so the saga
  knows which members applied. On any rejection or timeout the step fails and
  only the members that applied, or timed out (outcome unknown), are
  compensated, one at a time, highest index first. Members that rejected are
  not compensated.
- A reply arriving after the deadline is `Ignored`; the timeout already
  settled that member and it will be compensated.
- Events: `saga.member_succeeded`, `saga.member_failed`,
  `saga.fan_out_settled`. Replaying the stream reproduces the per-member
  state (`Saga::fan_out`, `Saga::applied_members`).

The tests are in `src/saga/fan_out_tests.rs` (N = 1..16, partial failure,
the shared deadline and late replies, per-member compensation retries,
bounds, and replay from a repository).

### Worked example

`tests/services/saga_transfer/mod.rs` moves value between wallets in two separate
cells. The `debit` step goes to the source cell (compensated by `refund`) and
the `credit` step goes to the target cell (compensated by `reverse_credit`).
A test network delivers outbox rows, and a `ScheduleWorker` on the saga
store's `ManualClock` fires the timeout schedules into the saga service, so
timeouts fire without sleeping. The scenarios are:

- the happy path, including an idempotent restart and a cancelled timeout;
- target rejection, where the debit is refunded;
- source rejection, where nothing needs compensating;
- target timeout, where both steps are compensated and the late credit is
  neutralized;
- the compensation arriving before the late action, where the tombstone
  rejects the credit;
- compensation timeouts that are retried and then marked `Stuck`;
- duplicate command delivery and duplicate replies;
- a settled timeout generation that never fires again and a stale timeout
  that is ignored;
- the saga and its timeout schedule surviving a restart on a SQLite file
  database, then firing through a new worker at the deadline.

## Tests

```sh
cargo test --features sqlite,postgres --test services --test repositories -- saga_transfer:: coordination_lease:: schedule_worker::
cargo test --features sqlite --lib -- outbox lease clock
cargo test --features cloudflare-workers --lib cell_host
(cd tests/celld/alarm-worker && worker-build --release) && node tests/celld/alarms.mjs
```

`tests/celld/alarms.mjs` owns its own `celld dev` process (no Docker; set
`CELLD_BIN` to choose the binary) and proves the alarm row commits atomically,
periodic re-arm at the alarm cadence, that the watchdog and domain alarms do
not starve each other, and exactly-once firing across a celld restart.

The Postgres cases skip when `DATABASE_URL` is unset (see `compose.yaml`).
