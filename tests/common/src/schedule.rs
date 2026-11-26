//! The schedule-worker contract: `ctx.schedule` rows stored by a repository
//! fire through the `Service` exactly once per (owner, name, generation),
//! across worker crashes, restarts and concurrent workers. One case list, run
//! on every repository by
//! [`schedule_conformance_tests!`](crate::schedule_conformance_tests).
//!
//! The repository's schedule store is crate-private, so the cases are
//! expanded over the backend's concrete repository type rather than written
//! as generic functions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::schedule::{schedule_guard, ScheduleWorkerConfig};
use distributed::microsvc::{CausalCommandContext, HandlerError, PortableCommand, Routes, Service};
use distributed::schedule::{ArmedSchedule, Capability, Schedule};
use distributed::{sourced, AggregateRepository, Entity, GetStream, ManualClock, StreamIdentity};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub use distributed::microsvc::schedule::ScheduleWorker;

/// A repository under test. Each [`ScheduleBackend::repository`] call is a
/// new process over the backend's storage.
pub trait ScheduleBackend: Sized {
    type Repository: GetStream + Clone + Send + Sync + 'static;

    async fn open(case: &str) -> Option<Self>;
    async fn repository(&self) -> Self::Repository;
    async fn armed(repository: &Self::Repository, owner: &StreamIdentity) -> Vec<ArmedSchedule>;
}

/// Capability `settle_faults`: the store can be made to refuse settling
/// (disarming) a schedule, so a worker dies between its committed dispatch
/// and the settlement.
pub trait SettleFaults: ScheduleBackend {
    async fn refuse_settlement(repository: &Self::Repository);
    async fn allow_settlement(repository: &Self::Repository);
}

/// Capability `store_clock`: due times follow a clock the test drives.
pub trait StoreClock: ScheduleBackend {
    fn repository_on(&self, clock: ManualClock) -> Self::Repository;
}

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Ticker {
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

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct StartInput {
    id: String,
    limit: u32,
    period_ms: u64,
    tick_command: String,
}

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
struct TickInput {
    id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct TickerPayload {
    id: String,
    ticks: u32,
}

pub const TICK: &str = "ticker.tick";
pub const FLAKY: &str = "ticker.flaky";

struct Start;
impl<D> PortableCommand<D> for Start
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<StartInput, Succeeded<TickerPayload>>(
                "ticker.start",
            ))
            .requires(Capability::Scheduling)
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
    repo.commit(ticker)?.succeeded(TickerPayload {
        id: input.id,
        ticks: 0,
    })
}

struct Tick;
impl<D> PortableCommand<D> for Tick
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
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
    })
}

/// Ticker ids whose next `ticker.flaky` fails once (tests run in parallel,
/// so each uses its own id).
static FLAKY_ONCE: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// Make the next `ticker.flaky` for `id` fail with a retryable error.
pub fn fail_once(id: &str) {
    FLAKY_ONCE.lock().unwrap().insert(id.to_string());
}

/// Fails with a retryable (500) error once per id in `FLAKY_ONCE`.
struct Flaky;
impl<D> PortableCommand<D> for Flaky
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickInput, Succeeded<TickerPayload>>(FLAKY))
            .requires(Capability::Scheduling)
            .guarded(schedule_guard::<Ticker>, handle_flaky)
    }
}

async fn handle_flaky(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickInput,
) -> Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    if FLAKY_ONCE.lock().unwrap().remove(&input.id) {
        return Err(HandlerError::Other("transient dependency outage".into()));
    }
    handle_tick(ctx, input).await
}

/// The ticker service over `repo`. It panics when `repo` has no schedule
/// worker attached: its routes need the Scheduling capability.
pub fn service<R>(repo: R) -> Arc<Service>
where
    AggregateRepository<R, Ticker>: distributed::microsvc::HasRepo
        + distributed::microsvc::HasOutboxStore
        + distributed::microsvc::ConfigurableOutboxPublisher
        + distributed::microsvc::CausalRouteDependencies<Aggregate = Ticker>
        + Send
        + Sync
        + 'static,
{
    Arc::new(
        Service::new().named("tickers").routes(
            Routes::new()
                .with_repo(AggregateRepository::<R, Ticker>::new(repo))
                .mount(Start)
                .mount(Tick)
                .mount(Flaky),
        ),
    )
}

static NEXT_COMMAND: AtomicU64 = AtomicU64::new(1);

pub fn command_id() -> String {
    let n = NEXT_COMMAND.fetch_add(1, Ordering::SeqCst);
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    format!(
        "0190a000-{:04x}-7000-8000-{:012x}",
        n & 0xffff,
        nanos & 0xffff_ffff_ffff
    )
}

/// Start ticker `id`: `limit` ticks of `command`, `period_ms` apart.
pub async fn start(service: &Service, id: &str, limit: u32, period_ms: u64, command: &str) {
    service
        .dispatch_causal(
            "ticker.start",
            &command_id(),
            json!({ "id": id, "limit": limit, "period_ms": period_ms, "tick_command": command }),
            crate::session::session("user", "user-1"),
            VerifiedPrincipal::internal("user-1").unwrap(),
        )
        .await
        .expect("start");
}

pub fn owner(id: &str) -> StreamIdentity {
    StreamIdentity::new("Ticker", id).unwrap()
}

/// How many times ticker `id` ticked.
pub async fn ticks<R: GetStream>(repo: &R, id: &str) -> u32 {
    let Some(entity) = repo.get_stream(&owner(id)).await.unwrap() else {
        return 0;
    };
    entity
        .events()
        .iter()
        .filter(|event| event.event_name == "ticker.ticked")
        .count() as u32
}

pub fn fast() -> ScheduleWorkerConfig {
    ScheduleWorkerConfig::default()
        .with_poll_interval(Duration::from_millis(20))
        .with_claim_ttl(Duration::from_millis(400))
}

/// Poll `probe` until it holds, failing after `timeout`.
pub async fn until<F, Fut>(what: &str, timeout: Duration, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// The panic message of `f`, which must panic.
pub fn panic_message(f: impl FnOnce()) -> String {
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).expect_err("expected a panic");
    match panic.downcast::<String>() {
        Ok(message) => *message,
        Err(panic) => panic
            .downcast::<&str>()
            .map(|message| message.to_string())
            .unwrap_or_default(),
    }
}

/// The in-memory repository, on the system clock unless a case drives it.
pub struct InMemory(distributed::InMemoryRepository);

impl ScheduleBackend for InMemory {
    type Repository = distributed::InMemoryRepository;

    async fn open(_case: &str) -> Option<Self> {
        Some(Self(distributed::InMemoryRepository::new()))
    }

    /// Clones share the store: the in-memory "processes" see one storage.
    async fn repository(&self) -> Self::Repository {
        self.0.clone()
    }

    async fn armed(repository: &Self::Repository, owner: &StreamIdentity) -> Vec<ArmedSchedule> {
        repository.armed_schedules(owner).unwrap()
    }
}

impl StoreClock for InMemory {
    fn repository_on(&self, clock: ManualClock) -> Self::Repository {
        distributed::InMemoryRepository::new().with_clock(clock)
    }
}

#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;

#[cfg(feature = "sqlite")]
mod sqlite {
    use distributed::schedule::ArmedSchedule;
    use distributed::{SqliteRepository, StreamIdentity};

    use super::{ScheduleBackend, SettleFaults};
    use crate::sqlite::TempDb;

    /// SQLite: a temp-file database, so a second repository can play the
    /// restarted (or concurrent) process.
    pub struct Sqlite(TempDb);

    impl ScheduleBackend for Sqlite {
        type Repository = SqliteRepository;

        async fn open(case: &str) -> Option<Self> {
            Some(Self(TempDb::new(&format!("schedule_{case}"))))
        }

        async fn repository(&self) -> SqliteRepository {
            let repository = SqliteRepository::new(self.0.pool().await);
            repository.migrate().await.unwrap();
            repository
        }

        async fn armed(
            repository: &SqliteRepository,
            owner: &StreamIdentity,
        ) -> Vec<ArmedSchedule> {
            repository.armed_schedules(owner).await.unwrap()
        }
    }

    impl SettleFaults for Sqlite {
        async fn refuse_settlement(repository: &SqliteRepository) {
            sqlx::query(
                "CREATE TRIGGER crash_before_settle BEFORE UPDATE OF armed ON aggregate_schedules \
                 WHEN NEW.armed = 0 BEGIN SELECT RAISE(ABORT, 'worker crashed before settling'); END",
            )
            .execute(repository.pool())
            .await
            .unwrap();
        }

        async fn allow_settlement(repository: &SqliteRepository) {
            sqlx::query("DROP TRIGGER crash_before_settle")
                .execute(repository.pool())
                .await
                .unwrap();
        }
    }
}

#[cfg(feature = "postgres")]
pub use self::postgres::Postgres;

#[cfg(feature = "postgres")]
mod postgres {
    use distributed::schedule::ArmedSchedule;
    use distributed::{PostgresRepository, StreamIdentity};

    use super::{ScheduleBackend, SettleFaults};
    use crate::postgres::PostgresTestSchema;

    /// Postgres: a fresh schema of `DATABASE_URL` per test.
    pub struct Postgres(PostgresTestSchema);

    impl ScheduleBackend for Postgres {
        type Repository = PostgresRepository;

        async fn open(case: &str) -> Option<Self> {
            PostgresTestSchema::create_from_env("schedule_worker", case)
                .await
                .map(Self)
        }

        async fn repository(&self) -> PostgresRepository {
            self.0.repository().await
        }

        async fn armed(
            repository: &PostgresRepository,
            owner: &StreamIdentity,
        ) -> Vec<ArmedSchedule> {
            repository.armed_schedules(owner).await.unwrap()
        }
    }

    impl SettleFaults for Postgres {
        async fn refuse_settlement(repository: &PostgresRepository) {
            for statement in [
                "CREATE FUNCTION crash_before_settle() RETURNS trigger LANGUAGE plpgsql AS \
                 $$ BEGIN RAISE EXCEPTION 'worker crashed before settling'; END $$",
                "CREATE TRIGGER crash_before_settle BEFORE UPDATE OF armed ON aggregate_schedules \
                 FOR EACH ROW WHEN (NEW.armed = 0) EXECUTE FUNCTION crash_before_settle()",
            ] {
                sqlx::query(statement)
                    .execute(repository.pool())
                    .await
                    .unwrap();
            }
        }

        async fn allow_settlement(repository: &PostgresRepository) {
            for statement in [
                "DROP TRIGGER crash_before_settle ON aggregate_schedules",
                "DROP FUNCTION crash_before_settle()",
            ] {
                sqlx::query(statement)
                    .execute(repository.pool())
                    .await
                    .unwrap();
            }
        }
    }
}

/// Expands every schedule-worker conformance case as a `#[tokio::test]` for
/// `$backend`, a [`ScheduleBackend`]. Each capability must be stated.
///
/// ```ignore
/// common::schedule_conformance_tests!(common::schedule::Sqlite, settle_faults: true, store_clock: false);
/// ```
#[macro_export]
macro_rules! schedule_conformance_tests {
    ($backend:ty, settle_faults: $settle:tt, store_clock: $clock:tt $(,)?) => {
        $crate::conformance_cases! { $crate::schedule::ScheduleBackend, $backend, open;
            #[tokio::test]
            scheduling_routes_need_an_attached_worker(b) => async move {
                use $crate::schedule::*;
                let repo = b.repository().await;
                let unattached = repo.clone();
                let message = panic_message(|| {
                    service(unattached);
                });
                assert!(message.contains("scheduling"), "{message}");
                let (repo, _worker) = ScheduleWorker::attach(repo, fast());
                let _ = service(repo);
            };
            #[tokio::test]
            fires_each_arm_once_and_reports_latency(b) => async move {
                use $crate::schedule::*;
                use std::sync::Arc;
                use std::time::{Duration, Instant};
                let (repo, worker) = ScheduleWorker::attach(b.repository().await, fast());
                let service = service(repo.clone());
                let started = Instant::now();
                start(&service, "once", 1, 200, TICK).await;
                let running = worker.spawn(service.clone());
                until("the first tick", Duration::from_secs(10), || async {
                    ticks(&repo, "once").await == 1
                })
                .await;
                let latency = started.elapsed().saturating_sub(Duration::from_millis(200));
                eprintln!("schedule latency past due (poll 20 ms): {latency:?}");
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(ticks(&repo, "once").await, 1, "an arm fires once");
                assert!(
                    <$backend>::armed(&repo, &owner("once")).await.is_empty(),
                    "a fired arm is disarmed"
                );

                // Periodic: each tick re-arms the next generation until the limit.
                start(&service, "periodic", 3, 50, TICK).await;
                until("three ticks", Duration::from_secs(10), || async {
                    ticks(&repo, "periodic").await == 3
                })
                .await;
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(ticks(&repo, "periodic").await, 3);
                assert!(<$backend>::armed(&repo, &owner("periodic")).await.is_empty());
                running.stop().await;
            };
            #[tokio::test]
            concurrent_workers_fire_each_schedule_once(b) => async move {
                use $crate::schedule::*;
                use std::time::Duration;
                let (repo, first) = ScheduleWorker::attach(b.repository().await, fast());
                let service_a = service(repo.clone());
                for index in 0..12 {
                    start(&service_a, &format!("many-{index}"), 1, 30, TICK).await;
                }
                let (other_repo, second) = ScheduleWorker::attach(b.repository().await, fast());
                let a = first.spawn(service_a);
                let b = second.spawn(service(other_repo));
                until("every ticker to tick", Duration::from_secs(20), || async {
                    let mut all = true;
                    for index in 0..12 {
                        all &= ticks(&repo, &format!("many-{index}")).await == 1;
                    }
                    all
                })
                .await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                for index in 0..12 {
                    let id = format!("many-{index}");
                    assert_eq!(ticks(&repo, &id).await, 1);
                    assert!(<$backend>::armed(&repo, &owner(&id)).await.is_empty());
                }
                a.stop().await;
                b.stop().await;
            };
            #[tokio::test]
            retryable_failures_back_off_then_fire_once(b) => async move {
                use $crate::schedule::*;
                use std::time::Duration;
                let (repo, worker) = ScheduleWorker::attach(b.repository().await, fast());
                let host = service(repo.clone());
                let flaky = format!("flaky-{}", command_id());
                fail_once(&flaky);
                start(&host, &flaky, 1, 0, FLAKY).await;
                let report = worker.run_once(host.as_ref()).await.unwrap();
                assert_eq!(report.retried.len(), 1, "{report:?}");
                let retry = <$backend>::armed(&repo, &owner(&flaky)).await;
                assert_eq!(retry[0].attempts, 1);
                assert!(
                    worker.run_once(host.as_ref()).await.unwrap().is_empty(),
                    "backing off"
                );
                tokio::time::sleep(Duration::from_millis(1_050)).await;
                let report = worker.run_once(host.as_ref()).await.unwrap();
                assert_eq!(report.fired.len(), 1, "{report:?}");
                assert_eq!(ticks(&repo, &flaky).await, 1);
                assert!(<$backend>::armed(&repo, &owner(&flaky)).await.is_empty());
            };
        }
        $crate::conformance_cases! { @if $settle $crate::schedule::ScheduleBackend, $backend, open;
            #[tokio::test]
            crash_between_commit_and_settle_does_not_fire_twice(b) => async move {
                use $crate::schedule::*;
                use std::sync::Arc;
                use std::time::{Duration, SystemTime};
                let claim_ttl = Duration::from_secs(30);
                let config = fast().with_claim_ttl(claim_ttl);
                let (repo, crashing) = ScheduleWorker::attach(b.repository().await, config);
                let crashed = service(repo.clone());
                start(&crashed, "crash", 1, 0, TICK).await;
                // Both workers measure claims by one manual clock, a second
                // ahead of the database clock that stamped the due time.
                let clock = distributed::ManualClock::new(SystemTime::now() + Duration::from_secs(1));
                let crashing = crashing.with_clock(Arc::new(clock.clone()));

                // The worker claims the arm and dispatches it (the tick
                // commits), then dies before settling.
                <$backend as SettleFaults>::refuse_settlement(&repo).await;
                crashing
                    .run_once(crashed.as_ref())
                    .await
                    .expect_err("the settlement fails: the worker crashed");
                <$backend as SettleFaults>::allow_settlement(&repo).await;
                assert_eq!(ticks(&repo, "crash").await, 1, "the tick committed");
                assert_eq!(<$backend>::armed(&repo, &owner("crash")).await.len(), 1, "the arm is unsettled");

                // A new process, before the claim lapses: the arm is neither
                // dispatched nor settled.
                let (repo, worker) = ScheduleWorker::attach(b.repository().await, config);
                let worker = worker.with_clock(Arc::new(clock.clone()));
                let service = service(repo.clone());
                clock.advance(claim_ttl - Duration::from_millis(1));
                let held = worker.run_once(service.as_ref()).await.unwrap();
                assert!(held.is_empty(), "the crashed worker's claim holds: {held:?}");
                assert_eq!(ticks(&repo, "crash").await, 1);
                assert_eq!(<$backend>::armed(&repo, &owner("crash")).await.len(), 1);

                // The claim lapses: the same command id replays and settles once.
                clock.advance(Duration::from_millis(1));
                let replay = worker.run_once(service.as_ref()).await.unwrap();
                assert_eq!(replay.fired.len(), 1, "{replay:?}");
                assert!(replay.fired[0].replayed, "the ledger replays the committed tick");
                assert_eq!(ticks(&repo, "crash").await, 1, "exactly once across the crash");
                assert!(<$backend>::armed(&repo, &owner("crash")).await.is_empty());
                assert!(worker.run_once(service.as_ref()).await.unwrap().is_empty());
            };
        }
        $crate::conformance_cases! { @if $clock $crate::schedule::ScheduleBackend, $backend, open;
            #[tokio::test]
            fires_by_the_store_clock_once_per_arm(b) => async move {
                use $crate::schedule::*;
                use std::time::{Duration, SystemTime};
                let clock = distributed::ManualClock::new(
                    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
                );
                let (repo, worker) = ScheduleWorker::attach(b.repository_on(clock.clone()), fast());
                let host = service(repo.clone());
                start(&host, "clocked", 2, 1_000, TICK).await;
                assert!(worker.run_once(host.as_ref()).await.unwrap().is_empty(), "not due yet");
                clock.advance(Duration::from_millis(1_000));
                let first = worker.run_once(host.as_ref()).await.unwrap();
                assert_eq!(first.fired.len(), 1);
                assert!(!first.fired[0].replayed);
                assert!(worker.run_once(host.as_ref()).await.unwrap().is_empty(), "re-armed for later");
                clock.advance(Duration::from_millis(1_000));
                assert_eq!(worker.run_once(host.as_ref()).await.unwrap().fired.len(), 1);
                clock.advance(Duration::from_secs(60));
                assert!(worker.run_once(host.as_ref()).await.unwrap().is_empty());
                assert_eq!(ticks(&repo, "clocked").await, 2);
                assert!(<$backend>::armed(&repo, &owner("clocked")).await.is_empty());
            };
        }
    };
}
