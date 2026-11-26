//! Worked example: a two-cell transfer saga.
//!
//! Two wallets live in two *different* cells (separate repositories — no
//! shared transaction). Moving value between them is a saga:
//!
//! 1. `debit`  → source cell (compensation: `refund`)
//! 2. `credit` → target cell (compensation: `reverse_credit`)
//!
//! A target rejection compensates the debit; a target timeout compensates
//! both (the credit may have applied), and participants tombstone a
//! compensated transfer so a late action is rejected. Time is a
//! `ManualClock` on the saga store; a timeout is the saga's `Schedule`, fired
//! by a `ScheduleWorker` into the saga service's `SagaTimeouts` route when
//! that clock reaches its deadline, so timeouts fire without sleeping.
//!
//! Each participant is a `Service` of typed commands that declare the
//! transport role. The network hands a saga command to it as a bus message,
//! so the command ledger deduplicates a redelivery (its command id derives
//! from the saga command id) and the wallet keeps no dedupe record of its
//! own. The wallet still keeps its tombstones: those are domain state.

#[cfg(feature = "sqlite")]
use common::sqlite as sqlite_support;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use distributed::bus::Message;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::schedule::{ScheduleWorker, ScheduleWorkerConfig};
use distributed::microsvc::{
    CausalCommandContext, HandlerError, PortableCommand, Routes, Service, TRANSPORT_ROLE,
};
use distributed::saga::{SagaCursor, SagaTimeout, SagaTimeouts, SAGA_TIMEOUT_SCHEDULE};
use distributed::{
    sourced, Aggregate, AggregateBuilder, AggregateRepository, ClaimOutboxMessages, Entity,
    InMemoryOutboxStore, InMemoryRepository, ManualClock, OutboxClaimRef, OutboxMessage,
    OutboxStore, Saga, SagaAction, SagaCommand, SagaDefinition, SagaError, SagaMessage,
    SagaOutcome, SagaReply, SagaRunner, SagaStatus, SagaStep, StreamIdentity,
};

// ===========================================================================
// Participant domain: a wallet (one per cell)
// ===========================================================================

#[derive(Default)]
pub struct Wallet {
    pub entity: Entity,
    balance: i64,
    accepts_credit: bool,
    /// Transfers whose action applied here (what a compensation undoes).
    applied: BTreeSet<String>,
    /// Compensated transfers (tombstones: a late action is rejected).
    cancelled: BTreeSet<String>,
}

#[allow(dead_code)]
#[sourced(entity, aggregate_type = "example.wallet")]
impl Wallet {
    #[event("opened")]
    pub fn open(&mut self, id: String, balance: i64, accepts_credit: bool) {
        self.entity.set_id(&id);
        self.balance = balance;
        self.accepts_credit = accepts_credit;
    }

    #[event("moved")]
    pub fn moved(&mut self, transfer: String, delta: i64) {
        self.balance += delta;
        self.applied.insert(transfer);
    }

    #[event("cancelled")]
    pub fn cancelled(&mut self, transfer: String, delta: i64) {
        self.balance += delta;
        self.applied.remove(&transfer);
        self.cancelled.insert(transfer);
    }

    pub fn balance(&self) -> i64 {
        self.balance
    }
}

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
struct MoveCmd {
    transfer: String,
    wallet: String,
    amount: i64,
}

// ===========================================================================
// The saga definition
// ===========================================================================

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransferData {
    from: String,
    to: String,
    amount: i64,
    /// Folded in from the credit reply (shows `on_step_succeeded`).
    credit_receipt: Option<String>,
}

pub struct Transfer;

const SOURCE_CELL: &str = "source-cell";
const TARGET_CELL: &str = "target-cell";

impl SagaDefinition for Transfer {
    const SAGA_TYPE: &'static str = "example.transfer";
    const REPLY_TO: &'static str = "transfer-saga";
    const TIMEOUT_COMMAND: &'static str = "example.transfer.timeout";
    const MAX_COMPENSATION_ATTEMPTS: u32 = 3;
    type Data = TransferData;

    fn steps() -> &'static [SagaStep] {
        const STEPS: &[SagaStep] = &[
            SagaStep::new("debit", Duration::from_secs(10)),
            SagaStep::new("credit", Duration::from_secs(10)),
        ];
        STEPS
    }

    fn action(step: &str, saga_id: &str, data: &TransferData) -> Result<SagaAction, SagaError> {
        let (cell, kind, wallet) = match step {
            "debit" => (SOURCE_CELL, "wallet.debit", &data.from),
            "credit" => (TARGET_CELL, "wallet.credit", &data.to),
            other => return Err(SagaError::Definition(format!("unknown step {other}"))),
        };
        SagaCommand::json(
            cell,
            kind,
            &MoveCmd {
                transfer: saga_id.into(),
                wallet: wallet.clone(),
                amount: data.amount,
            },
        )
        .map(Into::into)
    }

    fn compensation(
        step: &str,
        _member: usize,
        saga_id: &str,
        data: &TransferData,
    ) -> Result<Option<SagaCommand>, SagaError> {
        let (cell, kind, wallet) = match step {
            "debit" => (SOURCE_CELL, "wallet.refund", &data.from),
            "credit" => (TARGET_CELL, "wallet.reverse_credit", &data.to),
            other => return Err(SagaError::Definition(format!("unknown step {other}"))),
        };
        SagaCommand::json(
            cell,
            kind,
            &MoveCmd {
                transfer: saga_id.into(),
                wallet: wallet.clone(),
                amount: data.amount,
            },
        )
        .map(Some)
    }

    fn on_step_succeeded(step: &str, _member: usize, data: &mut TransferData, output: &[u8]) {
        if step == "credit" {
            data.credit_receipt = Some(String::from_utf8_lossy(output).into_owned());
        }
    }
}

// ===========================================================================
// Participant: one cell, a service of typed commands. One commit carries the
// wallet's events, the saga reply and the ledger receipt.
// ===========================================================================

const WALLET_COMMANDS: [&str; 4] = [
    "wallet.debit",
    "wallet.credit",
    "wallet.refund",
    "wallet.reverse_credit",
];

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct MoveOutcome {
    applied: bool,
}

/// How many times the handler ran for each saga command id, so a test can
/// show a redelivery never reaches it. Keyed by saga command id, which is
/// unique per test (each test starts its own saga ids).
static HANDLER_RUNS: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

fn handler_runs(saga_command_id: &str) -> usize {
    HANDLER_RUNS
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|runs| runs.get(saga_command_id).copied())
        .unwrap_or(0)
}

/// One saga command. The ledger has already settled whether this command id
/// ran before, so the handler holds no dedupe of its own; what it decides
/// from wallet state (funds, tombstones) is the domain's.
async fn handle_move(
    ctx: &CausalCommandContext<'_, Wallet>,
    cmd: MoveCmd,
) -> Result<PreparedCommand<Succeeded<MoveOutcome>>, HandlerError> {
    let reply = SagaReply::from_session(ctx.session())
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    *HANDLER_RUNS
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(reply.command_id().to_string())
        .or_default() += 1;
    let mut wallet = ctx
        .get(&cmd.wallet)
        .await?
        .ok_or_else(|| HandlerError::NotFound(cmd.wallet.clone()))?;
    let transfer = cmd.transfer;
    let command = ctx.command_name();
    let outcome = match command {
        "wallet.debit" | "wallet.credit" => {
            let debit = command == "wallet.debit";
            if wallet.cancelled.contains(&transfer) {
                Err("transfer was cancelled")
            } else if debit && wallet.balance < cmd.amount {
                Err("insufficient funds")
            } else if !debit && !wallet.accepts_credit {
                Err("wallet does not accept credits")
            } else {
                let delta = if debit { -cmd.amount } else { cmd.amount };
                wallet
                    .moved(transfer, delta)
                    .map_err(|error| HandlerError::Rejected(error.to_string()))?;
                Ok(())
            }
        }
        // A compensation: idempotent across saga attempts (each attempt is a
        // new command id, so the ledger does not stop it), and a tombstone
        // when its action never applied here.
        _ => {
            if !wallet.cancelled.contains(&transfer) {
                let delta = match (wallet.applied.contains(&transfer), command) {
                    (false, _) => 0,
                    (true, "wallet.refund") => cmd.amount,
                    (true, _) => -cmd.amount,
                };
                wallet
                    .cancelled(transfer, delta)
                    .map_err(|error| HandlerError::Rejected(error.to_string()))?;
            }
            Ok(())
        }
    };
    // A refusal is still a committed command: it carries the `failed` reply.
    // Returning `Err` would record the rejection but stage no reply.
    let reply = match outcome {
        Ok(()) => reply.succeeded(format!("receipt:{}", reply.command_id()).into_bytes()),
        Err(reason) => reply.failed(reason),
    }
    .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    ctx.outbox(reply).commit(wallet)?.succeeded(MoveOutcome {
        applied: outcome.is_ok(),
    })
}

struct WalletCommand(&'static str);

impl<D> PortableCommand<D> for WalletCommand
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = Wallet> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(
                typed_command::<MoveCmd, Succeeded<MoveOutcome>>(self.0).roles([TRANSPORT_ROLE]),
            )
            .handle(handle_move)
    }
}

struct Cell {
    service: Service,
    wallets: AggregateRepository<InMemoryRepository, Wallet>,
    outbox: InMemoryOutboxStore,
}

impl Cell {
    fn new(name: &str) -> Self {
        let repo = InMemoryRepository::new();
        let mut routes = Routes::new().with_repo(repo.clone().aggregate::<Wallet>());
        for command in WALLET_COMMANDS {
            routes = routes.mount(WalletCommand(command));
        }
        Self {
            service: Service::new().named(name).routes(routes),
            outbox: repo.outbox_store(),
            wallets: repo.aggregate::<Wallet>(),
        }
    }

    async fn open(&self, id: &str, balance: i64, accepts_credit: bool) {
        let mut wallet = Wallet::default();
        wallet.open(id.into(), balance, accepts_credit).unwrap();
        self.wallets.commit(&mut wallet).await.unwrap();
    }

    async fn balance(&self, id: &str) -> i64 {
        self.wallets.get(id).await.unwrap().unwrap().balance()
    }

    /// Deliver a saga command as a bus consumer would: the outbox row
    /// becomes a transport message and the service dispatches it.
    async fn handle(&self, command: &OutboxMessage) {
        self.service
            .dispatch_message(&Message::from(command.clone()))
            .await
            .unwrap();
    }
}

// ===========================================================================
// The test "network"
// ===========================================================================

struct World {
    clock: ManualClock,
    saga: SagaRunner<InMemoryRepository, Transfer, ManualClock>,
    saga_repo: InMemoryRepository,
    saga_outbox: InMemoryOutboxStore,
    /// Fires the saga's timeout schedules into `saga_service`.
    timeouts: ScheduleWorker,
    saga_service: Service,
    source: Cell,
    target: Cell,
    /// Destinations that are currently unreachable (messages stay queued).
    down: Mutex<HashSet<&'static str>>,
}

impl World {
    async fn new(target_accepts_credit: bool) -> Self {
        // Start the manual clock in the past so the in-memory store's
        // wall-clock claim never hides a row the manual clock says is due.
        let clock = ManualClock::new(SystemTime::now() - Duration::from_secs(3600));
        // The store clock is what the worker compares deadlines with and
        // what the timeout route's `ctx.now()` reads.
        let (saga_repo, timeouts) = ScheduleWorker::attach(
            InMemoryRepository::new().with_clock(clock.clone()),
            ScheduleWorkerConfig::default(),
        );
        let saga_service = Service::new().named("transfer-saga").routes(
            Routes::new()
                .with_repo(saga_repo.clone().aggregate::<Saga<Transfer>>())
                .mount(SagaTimeouts::<Transfer>::new()),
        );
        let world = Self {
            saga_outbox: saga_repo.outbox_store(),
            saga: SagaRunner::with_clock(saga_repo.clone(), clock.clone()),
            saga_repo,
            timeouts,
            saga_service,
            clock,
            source: Cell::new(SOURCE_CELL),
            target: Cell::new(TARGET_CELL),
            down: Mutex::new(HashSet::new()),
        };
        world.source.open("alice", 100, true).await;
        world.target.open("bob", 0, target_accepts_credit).await;
        world
    }

    fn data(amount: i64) -> TransferData {
        TransferData {
            from: "alice".into(),
            to: "bob".into(),
            amount,
            credit_receipt: None,
        }
    }

    fn set_down(&self, destination: &'static str, down: bool) {
        let mut set = self.down.lock().unwrap();
        if down {
            set.insert(destination);
        } else {
            set.remove(destination);
        }
    }

    /// Claim every due, reachable row from one outbox.
    async fn take_due(&self, store: &InMemoryOutboxStore) -> Vec<OutboxMessage> {
        let down = self.down.lock().unwrap().clone();
        let ids: Vec<String> = store
            .pending(1000)
            .await
            .unwrap()
            .into_iter()
            .filter(|m| !down.contains(m.destination.as_deref().unwrap_or_default()))
            .map(|m| m.id().to_string())
            .collect();
        if ids.is_empty() {
            return Vec::new();
        }
        let claimed = store
            .claim(ClaimOutboxMessages::for_ids(
                "net",
                ids,
                Duration::from_secs(60),
            ))
            .await
            .unwrap();
        for message in &claimed {
            store
                .complete(&OutboxClaimRef::from_message(message).unwrap())
                .await
                .unwrap();
        }
        claimed
    }

    /// Fire the saga timeouts due on the manual clock; returns how many fired.
    async fn fire_timeouts(&self) -> usize {
        let report = self.timeouts.run_once(&self.saga_service).await.unwrap();
        assert!(
            report.retried.is_empty() && report.failed.is_empty(),
            "a saga timeout failed: {report:?}"
        );
        report.fired.len()
    }

    /// Deliver and fire until quiescent; returns the reply outcomes observed.
    async fn pump(&self) -> Vec<SagaOutcome> {
        let mut outcomes = Vec::new();
        loop {
            let mut batch = Vec::new();
            for store in [&self.saga_outbox, &self.source.outbox, &self.target.outbox] {
                batch.extend(self.take_due(store).await);
            }
            let fired = self.fire_timeouts().await;
            if batch.is_empty() && fired == 0 {
                return outcomes;
            }
            for message in batch {
                match message.destination.as_deref() {
                    Some(SOURCE_CELL) => self.source.handle(&message).await,
                    Some(TARGET_CELL) => self.target.handle(&message).await,
                    Some(Transfer::REPLY_TO) => {
                        let saga_message = SagaMessage::from_outbox(&message).unwrap();
                        outcomes.push(self.saga.handle(&saga_message).await.unwrap().outcome);
                    }
                    other => panic!("no route for {other:?}"),
                }
            }
        }
    }

    async fn saga_state(&self, id: &str) -> Saga<Transfer> {
        self.saga.get(id).await.unwrap().unwrap()
    }

    fn armed_timeouts(&self, id: &str) -> Vec<distributed::schedule::ArmedSchedule> {
        self.saga_repo
            .armed_schedules(&StreamIdentity::new(Transfer::SAGA_TYPE, id).unwrap())
            .unwrap()
    }

    async fn balances(&self) -> (i64, i64) {
        (
            self.source.balance("alice").await,
            self.target.balance("bob").await,
        )
    }
}

// ===========================================================================
// Scenarios
// ===========================================================================

#[tokio::test]
async fn happy_path_moves_value_between_cells() {
    let world = World::new(true).await;
    let started = world.saga.start("t-1", World::data(30)).await.unwrap();
    assert_eq!(started.outcome, SagaOutcome::Applied);
    // The debit command, committed with the saga's armed timeout.
    assert_eq!(started.messages.len(), 1);
    let armed = world.armed_timeouts("t-1");
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].name, SAGA_TIMEOUT_SCHEDULE);
    assert_eq!(armed[0].command, Transfer::TIMEOUT_COMMAND);

    world.pump().await;
    let saga = world.saga_state("t-1").await;
    assert_eq!(saga.status(), SagaStatus::Completed);
    assert_eq!(world.balances().await, (70, 30));
    assert!(saga
        .data()
        .credit_receipt
        .as_deref()
        .unwrap()
        .contains("credit"));

    // Starting again is idempotent.
    let again = world.saga.start("t-1", World::data(30)).await.unwrap();
    assert_eq!(again.outcome, SagaOutcome::Duplicate);

    // The finished saga cancelled its timeout: nothing fires later.
    assert!(world.armed_timeouts("t-1").is_empty());
    world.clock.advance(Duration::from_secs(60));
    assert_eq!(world.fire_timeouts().await, 0);
    assert_eq!(world.balances().await, (70, 30));
}

#[tokio::test]
async fn target_rejection_compensates_the_debit() {
    let world = World::new(false).await;
    world.saga.start("t-2", World::data(40)).await.unwrap();
    world.pump().await;

    let saga = world.saga_state("t-2").await;
    assert_eq!(saga.status(), SagaStatus::Compensated);
    let failure = saga.failure().unwrap();
    assert_eq!(failure.step, "credit");
    assert!(!failure.timed_out);
    assert!(failure.reason.contains("does not accept"));
    assert_eq!(world.balances().await, (100, 0), "debit was refunded");
}

#[tokio::test]
async fn source_rejection_needs_no_compensation() {
    let world = World::new(true).await;
    world.saga.start("t-3", World::data(500)).await.unwrap();
    world.pump().await;
    let saga = world.saga_state("t-3").await;
    assert_eq!(saga.status(), SagaStatus::Compensated);
    assert_eq!(saga.failure().unwrap().step, "debit");
    assert_eq!(world.balances().await, (100, 0));
}

#[tokio::test]
async fn target_timeout_compensates_and_tombstones_late_credit() {
    let world = World::new(true).await;
    world.set_down(TARGET_CELL, true);
    world.saga.start("t-4", World::data(25)).await.unwrap();
    world.pump().await;

    // Debit applied, credit stuck in the target's queue.
    let saga = world.saga_state("t-4").await;
    assert_eq!(saga.status(), SagaStatus::Running);
    let SagaCursor { step, attempt, .. } = saga.cursor().unwrap().clone();
    assert_eq!((step, attempt), (1, 1));
    assert_eq!(world.balances().await.0, 75);

    // No reply within 10s: the timeout schedule fires (manual clock, no
    // sleep). Each dispatch re-armed it, so only the credit's deadline is left.
    let armed = world.armed_timeouts("t-4");
    assert_eq!(armed.len(), 1);
    let timeout: SagaTimeout = serde_json::from_value(armed[0].input.clone()).unwrap();
    assert_eq!((timeout.step.as_str(), timeout.attempt), ("credit", 1));
    world.clock.advance(Duration::from_secs(9));
    world.pump().await;
    assert_eq!(world.saga_state("t-4").await.status(), SagaStatus::Running);
    world.clock.advance(Duration::from_secs(2));
    world.pump().await;

    let saga = world.saga_state("t-4").await;
    assert_eq!(saga.status(), SagaStatus::Compensating);
    assert!(saga.failure().unwrap().timed_out);
    // Outcome unknown: both steps are on the compensation stack.
    assert_eq!(saga.applied_steps(), vec!["debit", "credit"]);

    // The target comes back. Its queue holds the late credit *and* the
    // reverse_credit compensation; whichever order they run in, the
    // tombstone keeps the books balanced.
    world.set_down(TARGET_CELL, false);
    world.pump().await;
    let saga = world.saga_state("t-4").await;
    assert_eq!(saga.status(), SagaStatus::Compensated);
    assert_eq!(world.balances().await, (100, 0), "value is conserved");
}

#[tokio::test]
async fn compensation_arriving_before_late_action_tombstones_it() {
    let world = World::new(true).await;
    world.set_down(TARGET_CELL, true);
    world.saga.start("t-4b", World::data(25)).await.unwrap();
    world.pump().await;
    world.clock.advance(Duration::from_secs(11));
    world.pump().await;

    // Deliver the target's queue in the worst order: compensation first.
    world.set_down(TARGET_CELL, false);
    let mut queued: Vec<OutboxMessage> = world
        .take_due(&world.saga_outbox)
        .await
        .into_iter()
        .filter(|m| m.destination.as_deref() == Some(TARGET_CELL))
        .collect();
    queued.sort_by_key(|m| m.event_type != "wallet.reverse_credit");
    assert_eq!(queued.len(), 2);
    for message in &queued {
        world.target.handle(message).await;
    }
    assert_eq!(world.balances().await.1, 0, "late credit was rejected");

    world.pump().await;
    assert_eq!(
        world.saga_state("t-4b").await.status(),
        SagaStatus::Compensated
    );
    assert_eq!(world.balances().await, (100, 0));
}

#[tokio::test]
async fn compensation_timeouts_retry_then_mark_stuck() {
    let world = World::new(false).await;
    world.saga.start("t-5", World::data(10)).await.unwrap();
    // Let the debit apply, then take the source down before the refund.
    world.set_down(TARGET_CELL, true);
    world.pump().await;
    world.set_down(SOURCE_CELL, true);
    world.set_down(TARGET_CELL, false);
    world.pump().await;
    let saga = world.saga_state("t-5").await;
    assert_eq!(saga.status(), SagaStatus::Compensating);
    assert_eq!(saga.cursor().unwrap().attempt, 1);

    for attempt in 2..=Transfer::MAX_COMPENSATION_ATTEMPTS {
        world.clock.advance(Duration::from_secs(11));
        world.pump().await;
        assert_eq!(
            world.saga_state("t-5").await.cursor().unwrap().attempt,
            attempt
        );
    }
    world.clock.advance(Duration::from_secs(11));
    world.pump().await;
    assert_eq!(world.saga_state("t-5").await.status(), SagaStatus::Stuck);
    assert!(world.armed_timeouts("t-5").is_empty());
}

/// A fired timeout is settled: its generation never fires again, and a stale
/// timeout input delivered anyway is ignored by the saga.
#[tokio::test]
async fn a_settled_or_stale_timeout_changes_nothing() {
    let world = World::new(true).await;
    world.set_down(TARGET_CELL, true);
    world.saga.start("t-11", World::data(5)).await.unwrap();
    world.pump().await;
    let armed = world.armed_timeouts("t-11").remove(0);
    let stale: SagaTimeout = serde_json::from_value(armed.input.clone()).unwrap();
    world.clock.advance(Duration::from_secs(11));
    assert_eq!(world.fire_timeouts().await, 1);
    let compensating = world.saga_state("t-11").await;
    assert_eq!(compensating.status(), SagaStatus::Compensating);
    let version = compensating.entity().version();

    // The superseded generation does not fire again.
    assert_eq!(world.fire_timeouts().await, 0);
    // Delivered anyway (a stale schedule input), the saga ignores it.
    let mut saga = world.saga_state("t-11").await;
    let outcome = saga.time_out(&stale, SystemTime::now()).unwrap();
    assert_eq!(outcome.outcome, SagaOutcome::Ignored);
    assert_eq!(world.saga_state("t-11").await.entity().version(), version);
    assert_eq!(
        world.armed_timeouts("t-11")[0].generation,
        armed.generation + 1
    );
}

/// The ledger makes a redelivered saga command a replay: the wallet changes
/// once and one reply exists, committed with the first run.
#[tokio::test]
async fn redelivered_saga_command_applies_once_and_replies_once() {
    let world = World::new(true).await;
    world.set_down(TARGET_CELL, true);
    world.saga.start("t-9", World::data(5)).await.unwrap();
    let commands = world.take_due(&world.saga_outbox).await;
    let debit = commands
        .iter()
        .find(|m| m.destination.as_deref() == Some(SOURCE_CELL))
        .unwrap();

    world.source.handle(debit).await;
    // The reply is durable as soon as the first delivery returns.
    let replies = world.source.outbox.pending(10).await.unwrap();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].id(), format!("{}:reply", debit.id()));

    for _redelivery in 0..3 {
        world.source.handle(debit).await;
    }
    assert_eq!(
        handler_runs(debit.id()),
        1,
        "the ledger replayed every redelivery"
    );
    assert_eq!(world.balances().await.0, 95, "debit applied once");
    let wallet = world.source.wallets.get("alice").await.unwrap().unwrap();
    assert_eq!(wallet.entity.version(), 2, "opened, then moved once");
    let replies = world.take_due(&world.source.outbox).await;
    assert_eq!(replies.len(), 1, "exactly one reply");

    // A rejection is deduplicated the same way: one `failed` reply.
    let world = World::new(true).await;
    world.saga.start("t-10", World::data(500)).await.unwrap();
    let commands = world.take_due(&world.saga_outbox).await;
    let debit = commands
        .iter()
        .find(|m| m.destination.as_deref() == Some(SOURCE_CELL))
        .unwrap();
    world.source.handle(debit).await;
    world.source.handle(debit).await;
    assert_eq!(handler_runs(debit.id()), 1, "a refusal is replayed too");
    let replies = world.take_due(&world.source.outbox).await;
    assert_eq!(replies.len(), 1);
    assert_eq!(world.balances().await.0, 100);
    let wallet = world.source.wallets.get("alice").await.unwrap().unwrap();
    assert_eq!(wallet.entity.version(), 1, "a refusal changes nothing");
}

#[tokio::test]
async fn duplicate_replies_are_deduplicated() {
    let world = World::new(true).await;
    world.set_down(TARGET_CELL, true);
    world.saga.start("t-6", World::data(5)).await.unwrap();

    // Deliver the debit command twice to the source cell (at-least-once).
    let commands = world.take_due(&world.saga_outbox).await;
    let debit = commands
        .iter()
        .find(|m| m.destination.as_deref() == Some(SOURCE_CELL))
        .unwrap();
    world.source.handle(debit).await;
    world.source.handle(debit).await;
    assert_eq!(world.balances().await.0, 95, "debit applied once");

    // The redelivery emitted nothing; deliver the one reply twice and the
    // saga applies it once.
    let replies = world.take_due(&world.source.outbox).await;
    assert_eq!(replies.len(), 1);
    let reply = SagaMessage::from_outbox(&replies[0]).unwrap();
    assert_eq!(
        world.saga.handle(&reply).await.unwrap().outcome,
        SagaOutcome::Applied
    );
    assert_eq!(
        world.saga.handle(&reply).await.unwrap().outcome,
        SagaOutcome::Duplicate
    );
}

#[tokio::test]
async fn saga_aggregate_replays_to_the_same_state() {
    let world = World::new(false).await;
    world.saga.start("t-7", World::data(40)).await.unwrap();
    world.pump().await;
    let loaded = world.saga_state("t-7").await;
    assert_eq!(loaded.status(), SagaStatus::Compensated);
    assert_eq!(loaded.applied_steps(), Vec::<&str>::new());
    assert!(loaded.cursor().is_none());
}

// ===========================================================================
// Restart survival on a SQLite file database
// ===========================================================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn saga_timeout_fires_through_its_schedule_after_restart_on_sqlite() {
    use crate::saga_transfer::sqlite_support::TempDb;
    use distributed::SqliteRepository;

    let db = TempDb::new("saga_restart");
    let started_at = SystemTime::now();
    let owner = StreamIdentity::new(Transfer::SAGA_TYPE, "t-8").unwrap();
    {
        let repo = SqliteRepository::new(db.pool().await);
        repo.migrate().await.unwrap();
        let runner = SagaRunner::<_, Transfer, _>::with_clock(repo, ManualClock::new(started_at));
        runner.start("t-8", World::data(10)).await.unwrap();
    } // "crash": every handle dropped

    let (repo, worker) = ScheduleWorker::attach(
        SqliteRepository::new(db.pool().await),
        ScheduleWorkerConfig::default(),
    );
    let armed = repo.armed_schedules(&owner).await.unwrap();
    assert_eq!(armed.len(), 1, "the timeout survived the restart");
    assert_eq!(armed[0].name, SAGA_TIMEOUT_SCHEDULE);
    let deadline = armed[0].due();
    let service = Arc::new(
        Service::new().named("transfer-saga").routes(
            Routes::new()
                .with_repo(repo.clone().aggregate::<Saga<Transfer>>())
                .mount(SagaTimeouts::<Transfer>::new()),
        ),
    );

    // Not due yet: nothing fires.
    let clock = ManualClock::new(deadline - Duration::from_millis(1));
    let worker = worker.with_clock(Arc::new(clock.clone()));
    assert!(worker.run_once(&*service).await.unwrap().fired.is_empty());

    // At the deadline the restarted worker fires it and the step compensates.
    clock.advance(Duration::from_millis(1));
    let report = worker.run_once(&*service).await.unwrap();
    assert_eq!(report.fired.len(), 1, "{report:?}");
    let runner = SagaRunner::<_, Transfer>::new(repo.clone());
    let saga = runner.get("t-8").await.unwrap().unwrap();
    assert_eq!(saga.status(), SagaStatus::Compensating);
    assert!(saga.failure().unwrap().timed_out);
    assert_eq!(saga.applied_steps(), vec!["debit"]);
    // The refund's deadline re-armed the same schedule, one generation on.
    let rearmed = repo.armed_schedules(&owner).await.unwrap();
    assert_eq!(rearmed.len(), 1);
    assert_eq!(rearmed[0].generation, armed[0].generation + 1);
    let timeout: SagaTimeout = serde_json::from_value(rearmed[0].input.clone()).unwrap();
    assert_eq!(
        (timeout.step.as_str(), timeout.phase.as_str()),
        ("debit", "compensation")
    );
    // The settled generation never fires again.
    assert!(worker.run_once(&*service).await.unwrap().fired.is_empty());
}
