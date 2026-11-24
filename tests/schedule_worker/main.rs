//! The schedule worker on shared repositories: `ctx.schedule` rows stored by
//! SQLite / Postgres / the in-memory repository are fired through the
//! `Service`, exactly once per (owner, name, generation), across worker
//! crashes, restarts and concurrent workers.
//!
//! Postgres runs when `DATABASE_URL` is set (each test in its own schema).

#![cfg(any(feature = "sqlite", feature = "postgres"))]

#[cfg(feature = "postgres")]
#[path = "../support/postgres.rs"]
mod postgres;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::json;

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::schedule::{schedule_guard, ScheduleWorker, ScheduleWorkerConfig};
use distributed::microsvc::{
    CausalCommandContext, HandlerError, PortableCommand, Routes, Service, Session,
};
use distributed::schedule::{Capability, Schedule};
use distributed::{
    sourced, AggregateRepository, Entity, ManualClock, StreamIdentity, ROLE_KEY, USER_ID_KEY,
};

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
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

const TICK: &str = "ticker.tick";
const FLAKY: &str = "ticker.flaky";

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

/// Ticker ids whose next `ticker.flaky` fails once (tests on different
/// backends run in parallel, so each uses its own id).
static FLAKY_ONCE: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

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

fn service<R>(repo: R) -> Arc<Service>
where
    AggregateRepository<R, Ticker>: distributed::microsvc::HasRepo
        + distributed::microsvc::HasOutboxStore
        + distributed::microsvc::ConfigurableOutboxPublisher
        + Send
        + Sync
        + 'static,
    Routes<AggregateRepository<R, Ticker>>: Sized,
    AggregateRepository<R, Ticker>:
        distributed::microsvc::CausalRouteDependencies<Aggregate = Ticker>,
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

fn command_id() -> String {
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

async fn start(service: &Service, id: &str, limit: u32, period_ms: u64, command: &str) {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "user-1");
    session.set(ROLE_KEY, "user");
    service
        .dispatch_causal(
            "ticker.start",
            &command_id(),
            json!({ "id": id, "limit": limit, "period_ms": period_ms, "tick_command": command }),
            session,
            VerifiedPrincipal::internal("user-1").unwrap(),
        )
        .await
        .expect("start");
}

fn owner(id: &str) -> StreamIdentity {
    StreamIdentity::new("Ticker", id).unwrap()
}

fn fast() -> ScheduleWorkerConfig {
    ScheduleWorkerConfig::default()
        .with_poll_interval(Duration::from_millis(20))
        .with_claim_ttl(Duration::from_millis(400))
}

async fn until<F, Fut>(what: &str, timeout: Duration, mut probe: F)
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

/// The scenarios, written once over any repository with a schedule worker.
///
/// `$crash` makes the store refuse to settle (disarm) a schedule, so a
/// worker pass dies between its committed dispatch and the settlement;
/// `$heal` lifts that again.
macro_rules! scenarios {
    ($repo_ty:ty, $fresh:expr, $reopen:expr, $crash:expr, $heal:expr) => {
        async fn ticks(repo: &$repo_ty, id: &str) -> u32 {
            use distributed::GetStream;
            let Some(entity) = repo.get_stream(&owner(id)).await.unwrap() else {
                return 0;
            };
            entity
                .events()
                .iter()
                .filter(|event| event.event_name == "ticker.ticked")
                .count() as u32
        }

        async fn armed(repo: &$repo_ty, id: &str) -> usize {
            repo.armed_schedules(&owner(id)).await.unwrap().len()
        }

        #[tokio::test]
        async fn scheduling_routes_need_an_attached_worker() {
            let Some(ctx) = $fresh.await else { return };
            let repo: $repo_ty = ctx.repo().await;
            let unattached = repo.clone();
            let refused =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service(unattached)));
            let message = refused
                .err()
                .and_then(|panic| panic.downcast::<String>().ok())
                .expect("a scheduling route on a repository without a worker must not register");
            assert!(message.contains("scheduling"), "{message}");
            let (repo, _worker) = ScheduleWorker::attach(repo, fast());
            let _ = service(repo);
        }

        #[tokio::test]
        async fn fires_each_arm_once_and_reports_latency() {
            let Some(ctx) = $fresh.await else { return };
            let (repo, worker) = ScheduleWorker::attach(ctx.repo().await, fast());
            let service = service(repo.clone());
            let started = Instant::now();
            start(&service, "once", 1, 200, TICK).await;
            let running = worker.spawn(Arc::clone(&service));
            until("the first tick", Duration::from_secs(10), || async {
                ticks(&repo, "once").await == 1
            })
            .await;
            let latency = started.elapsed().saturating_sub(Duration::from_millis(200));
            eprintln!("schedule latency past due (poll 20 ms): {latency:?}");
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(ticks(&repo, "once").await, 1, "an arm fires once");
            assert_eq!(armed(&repo, "once").await, 0, "a fired arm is disarmed");

            // Periodic: each tick re-arms the next generation until the limit.
            start(&service, "periodic", 3, 50, TICK).await;
            until("three ticks", Duration::from_secs(10), || async {
                ticks(&repo, "periodic").await == 3
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(ticks(&repo, "periodic").await, 3);
            assert_eq!(armed(&repo, "periodic").await, 0);
            running.stop().await;
        }

        #[tokio::test]
        async fn crash_between_commit_and_settle_does_not_fire_twice() {
            let Some(ctx) = $fresh.await else { return };
            let claim_ttl = Duration::from_secs(30);
            let config = fast().with_claim_ttl(claim_ttl);
            let (repo, crashing) = ScheduleWorker::attach(ctx.repo().await, config);
            let crashed = service(repo.clone());
            start(&crashed, "crash", 1, 0, TICK).await;
            // Both workers measure claims by one manual clock, a second ahead
            // of the database clock that stamped the due time.
            let clock = ManualClock::new(SystemTime::now() + Duration::from_secs(1));
            let crashing = crashing.with_clock(Arc::new(clock.clone()));

            // The worker claims the arm and dispatches it (the tick commits),
            // then dies before settling: the store refuses the settlement.
            $crash(&repo).await;
            crashing
                .run_once(&crashed)
                .await
                .expect_err("the settlement fails: the worker crashed");
            $heal(&repo).await;
            assert_eq!(ticks(&repo, "crash").await, 1, "the tick committed");
            assert_eq!(armed(&repo, "crash").await, 1, "the arm is unsettled");

            // A new process on the same database, before the claim lapses:
            // the arm is neither dispatched nor settled.
            let (repo, worker) = ScheduleWorker::attach($reopen(&ctx).await, config);
            let worker = worker.with_clock(Arc::new(clock.clone()));
            let service = service(repo.clone());
            clock.advance(claim_ttl - Duration::from_millis(1));
            let held = worker.run_once(&service).await.unwrap();
            assert!(
                held.is_empty(),
                "the crashed worker's claim holds: {held:?}"
            );
            assert_eq!(ticks(&repo, "crash").await, 1);
            assert_eq!(armed(&repo, "crash").await, 1);

            // The claim lapses: the same command id replays and settles once.
            clock.advance(Duration::from_millis(1));
            let replay = worker.run_once(&service).await.unwrap();
            assert_eq!(replay.fired.len(), 1, "{replay:?}");
            assert!(
                replay.fired[0].replayed,
                "the ledger replays the committed tick"
            );
            assert_eq!(
                ticks(&repo, "crash").await,
                1,
                "exactly once across the crash"
            );
            assert_eq!(armed(&repo, "crash").await, 0);
            assert!(worker.run_once(&service).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn concurrent_workers_fire_each_schedule_once() {
            let Some(ctx) = $fresh.await else { return };
            let (repo, first) = ScheduleWorker::attach(ctx.repo().await, fast());
            let service_a = service(repo.clone());
            for index in 0..12 {
                start(&service_a, &format!("many-{index}"), 1, 30, TICK).await;
            }
            let (other_repo, second) = ScheduleWorker::attach($reopen(&ctx).await, fast());
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
                assert_eq!(ticks(&repo, &format!("many-{index}")).await, 1);
                assert_eq!(armed(&repo, &format!("many-{index}")).await, 0);
            }
            a.stop().await;
            b.stop().await;
        }

        #[tokio::test]
        async fn retryable_failures_back_off_then_fire_once() {
            let Some(ctx) = $fresh.await else { return };
            let (repo, worker) = ScheduleWorker::attach(ctx.repo().await, fast());
            let host = service(repo.clone());
            let flaky = format!("flaky-{}", command_id());
            FLAKY_ONCE.lock().unwrap().insert(flaky.clone());
            start(&host, &flaky, 1, 0, FLAKY).await;
            let report = worker.run_once(&host).await.unwrap();
            assert_eq!(report.retried.len(), 1, "{report:?}");
            let retry = repo.armed_schedules(&owner(&flaky)).await.unwrap();
            assert_eq!(retry[0].attempts, 1);
            assert!(
                worker.run_once(&host).await.unwrap().is_empty(),
                "backing off"
            );
            tokio::time::sleep(Duration::from_millis(1_050)).await;
            let report = worker.run_once(&host).await.unwrap();
            assert_eq!(report.fired.len(), 1, "{report:?}");
            assert_eq!(ticks(&repo, &flaky).await, 1);
            assert_eq!(armed(&repo, &flaky).await, 0);
        }
    };
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use distributed::SqliteRepository;

    /// A temporary database file, so a second repository can play the
    /// restarted (or concurrent) process.
    pub struct Db(std::path::PathBuf);

    impl Db {
        pub async fn repo(&self) -> SqliteRepository {
            SqliteRepository::connect_and_migrate(&format!(
                "sqlite://{}?mode=rwc",
                self.0.display()
            ))
            .await
            .unwrap()
        }
    }

    impl Drop for Db {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    async fn fresh() -> Option<Db> {
        Some(Db(std::env::temp_dir().join(format!(
            "distributed-schedule-worker-{}-{}.db",
            std::process::id(),
            command_id()
        ))))
    }

    async fn reopen(db: &Db) -> SqliteRepository {
        db.repo().await
    }

    async fn crash_before_settle(repo: &SqliteRepository) {
        sqlx::query(
            "CREATE TRIGGER crash_before_settle BEFORE UPDATE OF armed ON aggregate_schedules \
             WHEN NEW.armed = 0 BEGIN SELECT RAISE(ABORT, 'worker crashed before settling'); END",
        )
        .execute(repo.pool())
        .await
        .unwrap();
    }

    async fn heal(repo: &SqliteRepository) {
        sqlx::query("DROP TRIGGER crash_before_settle")
            .execute(repo.pool())
            .await
            .unwrap();
    }

    scenarios!(SqliteRepository, fresh(), reopen, crash_before_settle, heal);
}

#[cfg(feature = "postgres")]
mod postgres_backend {
    use super::*;
    use crate::postgres::PostgresTestSchema;
    use distributed::PostgresRepository;

    pub struct Db(PostgresTestSchema);

    impl Db {
        pub async fn repo(&self) -> PostgresRepository {
            self.0.repository().await
        }
    }

    async fn fresh() -> Option<Db> {
        PostgresTestSchema::create_from_env("schedule_worker", "Postgres schedule worker")
            .await
            .map(Db)
    }

    async fn reopen(db: &Db) -> PostgresRepository {
        db.repo().await
    }

    async fn crash_before_settle(repo: &PostgresRepository) {
        for statement in [
            "CREATE FUNCTION crash_before_settle() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RAISE EXCEPTION 'worker crashed before settling'; END $$",
            "CREATE TRIGGER crash_before_settle BEFORE UPDATE OF armed ON aggregate_schedules \
             FOR EACH ROW WHEN (NEW.armed = 0) EXECUTE FUNCTION crash_before_settle()",
        ] {
            sqlx::query(statement).execute(repo.pool()).await.unwrap();
        }
    }

    async fn heal(repo: &PostgresRepository) {
        for statement in [
            "DROP TRIGGER crash_before_settle ON aggregate_schedules",
            "DROP FUNCTION crash_before_settle()",
        ] {
            sqlx::query(statement).execute(repo.pool()).await.unwrap();
        }
    }

    scenarios!(
        PostgresRepository,
        fresh(),
        reopen,
        crash_before_settle,
        heal
    );
}

/// The in-memory repository fires its schedules by its injected clock.
mod memory {
    use super::*;
    use distributed::{GetStream, InMemoryRepository};

    #[tokio::test]
    async fn in_memory_fires_by_the_store_clock_once_per_arm() {
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let unattached = InMemoryRepository::new().with_clock(clock.clone());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service(unattached.clone())))
                .is_err(),
            "no worker, no Scheduling capability"
        );
        let (repo, worker) = ScheduleWorker::attach(unattached, fast());
        let host = service(repo.clone());
        start(&host, "mem", 2, 1_000, TICK).await;
        assert!(
            worker.run_once(&host).await.unwrap().is_empty(),
            "not due yet"
        );
        clock.advance(Duration::from_millis(1_000));
        let first = worker.run_once(&host).await.unwrap();
        assert_eq!(first.fired.len(), 1);
        assert!(!first.fired[0].replayed);
        assert!(
            worker.run_once(&host).await.unwrap().is_empty(),
            "re-armed for later"
        );
        clock.advance(Duration::from_millis(1_000));
        assert_eq!(worker.run_once(&host).await.unwrap().fired.len(), 1);
        clock.advance(Duration::from_secs(60));
        assert!(worker.run_once(&host).await.unwrap().is_empty());
        let events = repo.get_stream(&owner("mem")).await.unwrap().unwrap();
        assert_eq!(
            events
                .events()
                .iter()
                .filter(|event| event.event_name == "ticker.ticked")
                .count(),
            2
        );
        assert!(repo.armed_schedules(&owner("mem")).unwrap().is_empty());
    }
}
