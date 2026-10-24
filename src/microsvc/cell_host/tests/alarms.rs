//! Schedules on the in-process reference cell host, driven by a
//! `ManualClock`. The same command/ledger/settlement path runs on celld; only
//! the wake source differs (Durable Object alarm vs. the test calling
//! `fire_due_schedules`). Also: schedule rows on the in-memory repository,
//! `ctx.now()`, and the capability check at registration.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::super::{AggregateCell, CellCommandIdentity, CellStreamStore};
use super::owner_session;
use crate::command::{typed_command, PreparedCommand, Succeeded};
use crate::entity::Entity;
use crate::microsvc::service::{CausalCommandContext, PortableCommand, Routes};
use crate::microsvc::HandlerError;
use crate::repository::{CommitBatch, StreamIdentity, TransactionalCommit};
use crate::schedule::{
    schedule_guard, schedule_session, Capability, Schedule, ScheduleWrite, HOST_NOW_CLAIM,
    SCHEDULE_RETRY_BASE,
};
use crate::{sourced, Clock, InMemoryRepository, ManualClock};

#[derive(Clone, Default, Serialize, Deserialize, crate::Snapshot)]
struct Ticker {
    entity: Entity,
    ticks: u32,
    limit: u32,
    period_ms: u64,
}

#[sourced(entity, aggregate_type = "Ticker", events = "TickerEvent")]
impl Ticker {
    #[event("ticker.started", version = 1)]
    fn start(&mut self, id: String, limit: u32, period_ms: u64) {
        self.entity.set_id(id);
        self.limit = limit;
        self.period_ms = period_ms;
    }

    #[event("ticker.ticked", version = 1)]
    fn tick(&mut self) {
        self.ticks += 1;
    }
}

#[derive(Debug, Deserialize, crate::CommandInput)]
struct StartInput {
    id: String,
    limit: u32,
    period_ms: u64,
    fail_after_arming: bool,
    tick_command: String,
}

#[derive(Debug, Serialize, Deserialize, crate::CommandInput)]
struct TickInput {
    id: String,
}

#[derive(Debug, Serialize, crate::CommandOutput)]
struct TickerPayload {
    id: String,
    ticks: u32,
    /// `ctx.now()` of the start command (epoch ms), 0 otherwise.
    started_at_ms: u64,
}

const TICK: &str = "ticker.tick";

struct Start;
impl<D> PortableCommand<D> for Start
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<StartInput, Succeeded<TickerPayload>>(
                "ticker.start",
            ))
            .requires(Capability::Scheduling)
            .requires(Capability::Clock)
            .guarded(
                |ctx: &CausalCommandContext<'_, Ticker>| ctx.session().user_id().is_some(),
                handle_start,
            )
    }
}

async fn handle_start(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: StartInput,
) -> Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    let mut ticker = repo.create();
    ticker
        .start(input.id.clone(), input.limit, input.period_ms)
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    ctx.schedule(
        Schedule::after(
            "tick",
            Duration::from_millis(input.period_ms),
            &input.tick_command,
            &TickInput {
                id: input.id.clone(),
            },
        )
        .map_err(|error| HandlerError::Rejected(error.to_string()))?,
    )?;
    if input.fail_after_arming {
        return Err(HandlerError::Rejected("start refused after arming".into()));
    }
    let started_at_ms = ctx
        .now()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64);
    repo.commit(ticker)?.succeeded(TickerPayload {
        id: input.id,
        ticks: 0,
        started_at_ms,
    })
}

struct Tick;
impl<D> PortableCommand<D> for Tick
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickInput, Succeeded<TickerPayload>>(TICK))
            .requires(Capability::Scheduling)
            .guarded(schedule_guard::<Ticker>, handle_tick)
    }
}

async fn handle_tick(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickInput,
) -> Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    let mut ticker = repo
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::Rejected("no ticker".into()))?;
    ticker
        .tick()
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    if ticker.ticks < ticker.limit {
        // Re-arm from the fired due time, not wall time: no drift.
        let due = ctx.scheduled_due().expect("schedule session")
            + Duration::from_millis(ticker.period_ms);
        ctx.schedule(
            Schedule::at("tick", due, TICK, &input)
                .map_err(|error| HandlerError::Rejected(error.to_string()))?,
        )?;
    }
    let ticks = ticker.ticks;
    repo.commit(ticker)?.succeeded(TickerPayload {
        id: input.id,
        ticks,
        started_at_ms: 0,
    })
}

struct Stop;
impl<D> PortableCommand<D> for Stop
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickInput, Succeeded<TickerPayload>>(
                "ticker.stop",
            ))
            .requires(Capability::Scheduling)
            .guarded(
                |ctx: &CausalCommandContext<'_, Ticker>| ctx.session().user_id().is_some(),
                handle_stop,
            )
    }
}

async fn handle_stop(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickInput,
) -> Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    let ticker = repo
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::Rejected("no ticker".into()))?;
    ctx.cancel_schedule("tick")?;
    let ticks = ticker.ticks;
    repo.commit(ticker)?.succeeded(TickerPayload {
        id: input.id,
        ticks,
        started_at_ms: 0,
    })
}

static FLAKY_FAILURES: AtomicUsize = AtomicUsize::new(0);

fn take_flaky_failure() -> bool {
    let mut current = FLAKY_FAILURES.load(Ordering::SeqCst);
    while current > 0 {
        match FLAKY_FAILURES.compare_exchange(
            current,
            current - 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
    false
}

/// Fails with an internal (retryable) error while `FLAKY_FAILURES` > 0.
struct Flaky;
impl<D> PortableCommand<D> for Flaky
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickInput, Succeeded<TickerPayload>>(
                "ticker.flaky",
            ))
            .requires(Capability::Scheduling)
            .guarded(schedule_guard::<Ticker>, handle_flaky)
    }
}

async fn handle_flaky(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickInput,
) -> Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    if take_flaky_failure() {
        return Err(HandlerError::Other("transient dependency outage".into()));
    }
    handle_tick(ctx, input).await
}

const START_MS: u64 = 1_700_000_000_000;

fn clock() -> ManualClock {
    ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(START_MS))
}

fn ticker_cell(clock: &ManualClock) -> AggregateCell<Ticker> {
    let store = CellStreamStore::for_identity(StreamIdentity::new("Ticker", "t1").unwrap())
        .with_clock(clock.clone());
    mount(AggregateCell::from_store(store).unwrap())
}

fn mount(cell: AggregateCell<Ticker>) -> AggregateCell<Ticker> {
    cell.mount(Start).mount(Tick).mount(Stop).mount(Flaky)
}

fn identity(command_id: &str) -> CellCommandIdentity {
    CellCommandIdentity::new("ticker-service", "user-1", command_id).unwrap()
}

async fn start(cell: &AggregateCell<Ticker>, input: serde_json::Value) {
    cell.dispatch_idempotent(
        "ticker.start",
        &identity("0190a000-0000-7000-8000-000000000001"),
        input,
        owner_session(),
    )
    .await
    .unwrap();
}

async fn ticks(cell: &AggregateCell<Ticker>) -> u32 {
    cell.load().await.unwrap().map_or(0, |ticker| ticker.ticks)
}

#[tokio::test]
async fn alarm_commits_with_the_events_and_is_absent_when_the_command_fails() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    let refused = cell
        .dispatch_idempotent(
            "ticker.start",
            &identity("0190a000-0000-7000-8000-000000000002"),
            json!({ "id": "t1", "limit": 1, "period_ms": 1000, "fail_after_arming": true, "tick_command": "ticker.tick" }),
            owner_session(),
        )
        .await;
    assert!(refused.is_err());
    assert!(cell.armed_schedules().unwrap().is_empty());
    assert!(cell.load().await.unwrap().is_none());

    start(&cell, json!({ "id": "t1", "limit": 1, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;
    let armed = cell.armed_schedules().unwrap();
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].name, "tick");
    assert_eq!(armed[0].command, TICK);
    assert_eq!(armed[0].generation, 1);
    assert_eq!(
        cell.next_schedule_due().unwrap(),
        Some(clock.now() + Duration::from_millis(1000)),
        "a relative alarm resolves against the cell clock at commit"
    );
}

#[tokio::test]
async fn alarm_fires_once_per_arm_with_the_trusted_session() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(&cell, json!({ "id": "t1", "limit": 1, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;

    clock.advance(Duration::from_millis(999));
    assert!(cell.fire_due_schedules().await.unwrap().is_empty());
    clock.advance(Duration::from_millis(1));
    let report = cell.fire_due_schedules().await.unwrap();
    assert_eq!(report.fired.len(), 1);
    assert!(!report.fired[0].replayed);
    assert_eq!(report.fired[0].payload["ticks"], 1);
    clock.advance(Duration::from_secs(60));
    assert!(cell.fire_due_schedules().await.unwrap().is_empty());
    assert_eq!(ticks(&cell).await, 1);
    assert!(cell.armed_schedules().unwrap().is_empty());

    // The alarm-only command refuses ordinary callers.
    let forged = cell
        .dispatch_idempotent(
            TICK,
            &identity("0190a000-0000-7000-8000-000000000003"),
            json!({ "id": "t1" }),
            owner_session(),
        )
        .await;
    assert!(forged.is_err());
    assert_eq!(ticks(&cell).await, 1);
}

#[tokio::test]
async fn rearming_in_the_handler_ticks_one_hundred_times_without_drift() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(&cell, json!({ "id": "t1", "limit": 100, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;
    let first_due = cell.next_schedule_due().unwrap().unwrap();
    for tick in 1..=100u64 {
        let expected_due = first_due + Duration::from_millis((tick - 1) * 1000);
        assert_eq!(cell.next_schedule_due().unwrap(), Some(expected_due));
        // Wake late; the next tick still re-arms from the due time.
        clock.set(expected_due + Duration::from_millis(250));
        let report = cell.fire_due_schedules().await.unwrap();
        assert_eq!(report.fired.len(), 1, "tick {tick}");
        assert_eq!(report.fired[0].generation, tick);
        assert!(report.failed.is_empty() && report.retried.is_empty());
    }
    assert_eq!(ticks(&cell).await, 100);
    assert!(cell.armed_schedules().unwrap().is_empty());
    clock.advance(Duration::from_secs(3600));
    assert!(cell.fire_due_schedules().await.unwrap().is_empty());
    assert_eq!(ticks(&cell).await, 100);
}

#[tokio::test]
async fn a_fired_command_that_committed_before_settlement_replays_instead_of_rerunning() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(&cell, json!({ "id": "t1", "limit": 1, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;
    clock.advance(Duration::from_secs(1));

    // Crash window: the alarm command committed but its row was not settled.
    let alarm = cell.armed_schedules().unwrap().remove(0);
    cell.dispatch_idempotent(
        &alarm.command,
        &alarm.command_identity().unwrap(),
        alarm.input.clone(),
        schedule_session(&alarm),
    )
    .await
    .unwrap();
    assert_eq!(ticks(&cell).await, 1);

    let report = cell.fire_due_schedules().await.unwrap();
    assert_eq!(report.fired.len(), 1);
    assert!(report.fired[0].replayed, "the ledger replays the receipt");
    assert_eq!(ticks(&cell).await, 1, "the handler did not run twice");
    assert!(cell.armed_schedules().unwrap().is_empty());
}

#[tokio::test]
async fn cancel_disarms_and_rejections_are_terminal() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(&cell, json!({ "id": "t1", "limit": 5, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;
    cell.dispatch_idempotent(
        "ticker.stop",
        &identity("0190a000-0000-7000-8000-000000000004"),
        json!({ "id": "t1" }),
        owner_session(),
    )
    .await
    .unwrap();
    assert!(cell.armed_schedules().unwrap().is_empty());
    clock.advance(Duration::from_secs(10));
    assert!(cell.fire_due_schedules().await.unwrap().is_empty());

    let other = ticker_cell(&clock);
    start(
        &other,
        json!({ "id": "t1", "limit": 5, "period_ms": 10, "tick_command": "ticker.missing", "fail_after_arming": false }),
    )
    .await;
    clock.advance(Duration::from_secs(1));
    let report = other.fire_due_schedules().await.unwrap();
    assert!(report.fired.is_empty());
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].code, "BAD_REQUEST");
    assert!(other.armed_schedules().unwrap().is_empty());
}

#[tokio::test]
async fn retryable_failures_back_off_and_then_fire() {
    FLAKY_FAILURES.store(2, Ordering::SeqCst);
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(
        &cell,
        json!({ "id": "t1", "limit": 1, "period_ms": 1000, "tick_command": "ticker.flaky", "fail_after_arming": false }),
    )
    .await;
    clock.advance(Duration::from_secs(1));
    let first = cell.fire_due_schedules().await.unwrap();
    assert_eq!(first.retried.len(), 1);
    assert_eq!(first.retried[0].code, "INTERNAL");
    let armed = cell.armed_schedules().unwrap();
    assert_eq!(armed[0].attempts, 1);
    assert_eq!(armed[0].due(), clock.now() + SCHEDULE_RETRY_BASE);

    clock.advance(SCHEDULE_RETRY_BASE);
    let second = cell.fire_due_schedules().await.unwrap();
    assert_eq!(second.retried.len(), 1);
    assert_eq!(
        cell.armed_schedules().unwrap()[0].due(),
        clock.now() + SCHEDULE_RETRY_BASE * 2,
        "backoff doubles"
    );
    clock.advance(SCHEDULE_RETRY_BASE * 2);
    let third = cell.fire_due_schedules().await.unwrap();
    assert_eq!(third.fired.len(), 1);
    assert_eq!(ticks(&cell).await, 1);
}

#[tokio::test]
async fn armed_alarms_survive_a_reference_host_restart() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    start(&cell, json!({ "id": "t1", "limit": 2, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" })).await;
    let exported = cell.durable_state().unwrap();
    drop(cell);

    let restarted = ticker_cell(&clock);
    restarted.restore_durable_state(exported).unwrap();
    clock.advance(Duration::from_secs(1));
    assert_eq!(restarted.fire_due_schedules().await.unwrap().fired.len(), 1);
    clock.advance(Duration::from_secs(1));
    assert_eq!(restarted.fire_due_schedules().await.unwrap().fired.len(), 1);
    assert_eq!(ticks(&restarted).await, 2);
}

#[tokio::test]
async fn the_in_memory_repository_stores_schedules_with_their_one_owner() {
    let clock = clock();
    let repo = InMemoryRepository::new().with_clock(clock.clone());
    let owner = StreamIdentity::new("Ticker", "t9").unwrap();
    let mut ticker = Ticker::default();
    ticker.start("t9".into(), 1, 1000).unwrap();
    let mut batch = CommitBatch::new(vec![crate::repository::StreamWrite::new(
        owner.clone(),
        &mut ticker.entity,
    )]);
    batch.schedules.push(ScheduleWrite::Arm(
        Schedule::after(
            "tick",
            Duration::from_secs(5),
            TICK,
            &TickInput { id: "t9".into() },
        )
        .unwrap(),
    ));
    repo.commit_batch(batch).await.unwrap();
    let armed = repo.armed_schedules(&owner).unwrap();
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].generation, 1);
    assert_eq!(
        armed[0].due(),
        clock.now() + Duration::from_secs(5),
        "a relative schedule resolves against the repository clock"
    );

    // A schedule needs exactly one owning stream.
    let mut orphan = CommitBatch::empty();
    orphan.schedules.push(ScheduleWrite::Cancel("tick".into()));
    let error = repo.commit_batch(orphan).await.unwrap_err();
    assert!(
        error.to_string().contains("exactly one aggregate"),
        "{error}"
    );
}

#[test]
#[should_panic(
    expected = "command `ticker.start` requires scheduling that its backend does not provide"
)]
fn a_scheduling_route_on_a_backend_that_cannot_dispatch_fails_at_registration() {
    let routes = Routes::new().with_repo(crate::aggregate::AggregateRepository::new(
        InMemoryRepository::new(),
    ));
    let _ = Start.install(routes);
}

#[tokio::test]
async fn handlers_see_the_cell_clock_and_callers_cannot_forge_it() {
    let clock = clock();
    let cell = ticker_cell(&clock);
    let mut forged = owner_session();
    forged.set(HOST_NOW_CLAIM, "1");
    let started = cell.dispatch_idempotent(
        "ticker.start",
        &identity("0190a000-0000-7000-8000-000000000009"),
        json!({ "id": "t1", "limit": 1, "period_ms": 1000, "fail_after_arming": false, "tick_command": "ticker.tick" }),
        forged,
    )
    .await
    .unwrap();
    assert_eq!(
        started.payload()["started_at_ms"],
        START_MS,
        "ctx.now() is the cell clock, not the forged claim"
    );
    // `after(period)` resolved by the cell clock, not the forged claim.
    assert_eq!(
        cell.next_schedule_due().unwrap(),
        Some(clock.now() + Duration::from_millis(1000))
    );
    let mut session = crate::microsvc::Session::new();
    assert_eq!(crate::schedule::host_now(&session), None);
    session.set(HOST_NOW_CLAIM, "1234");
    assert_eq!(
        crate::schedule::host_now(&session),
        Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1234))
    );
}
