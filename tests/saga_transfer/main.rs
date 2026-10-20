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
//! `ManualClock`; the test "network" delivers outbox rows whose `deliver_at`
//! is due on that clock, so timeouts fire without sleeping.

#[cfg(feature = "sqlite")]
#[path = "../support/sqlite.rs"]
mod sqlite_support;

use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use distributed::saga::SagaCursor;
use distributed::{
    sourced, AggregateBuilder, AggregateRepository, ClaimOutboxMessages, Entity,
    InMemoryOutboxStore, InMemoryRepository, ManualClock, OutboxClaimRef, OutboxMessage,
    OutboxStore, Saga, SagaCommand, SagaDefinition, SagaError, SagaMessage, SagaOutcome, SagaReply,
    SagaRunner, SagaStatus, SagaStep,
};

// ===========================================================================
// Participant domain: a wallet (one per cell)
// ===========================================================================

#[derive(Default)]
pub struct Wallet {
    pub entity: Entity,
    balance: i64,
    accepts_credit: bool,
    /// Transfers whose action applied here (dedupe).
    applied: BTreeSet<String>,
    /// Compensated transfers (tombstones: a late action is rejected).
    cancelled: BTreeSet<String>,
    /// Saga command ids already handled (the participant's dedupe record;
    /// a consumer inbox receipt keyed by the command id works the same way).
    handled: BTreeSet<String>,
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
    pub fn moved(&mut self, command: String, transfer: String, delta: i64) {
        self.balance += delta;
        self.applied.insert(transfer);
        self.handled.insert(command);
    }

    #[event("cancelled")]
    pub fn cancelled(&mut self, command: String, transfer: String, delta: i64) {
        self.balance += delta;
        self.applied.remove(&transfer);
        self.cancelled.insert(transfer);
        self.handled.insert(command);
    }

    #[event("rejected")]
    pub fn rejected(&mut self, command: String) {
        self.handled.insert(command);
    }

    #[event("acknowledged")]
    pub fn acknowledged(&mut self, command: String) {
        self.handled.insert(command);
    }

    pub fn balance(&self) -> i64 {
        self.balance
    }
}

#[derive(Serialize, Deserialize)]
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
    const MAX_COMPENSATION_ATTEMPTS: u32 = 3;
    type Data = TransferData;

    fn steps() -> &'static [SagaStep] {
        const STEPS: &[SagaStep] = &[
            SagaStep::new("debit", Duration::from_secs(10)),
            SagaStep::new("credit", Duration::from_secs(10)),
        ];
        STEPS
    }

    fn action(step: &str, saga_id: &str, data: &TransferData) -> Result<SagaCommand, SagaError> {
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
    }

    fn compensation(
        step: &str,
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

    fn on_step_succeeded(step: &str, data: &mut TransferData, output: &[u8]) {
        if step == "credit" {
            data.credit_receipt = Some(String::from_utf8_lossy(output).into_owned());
        }
    }
}

// ===========================================================================
// Participant handler: one cell, one atomic commit (wallet + reply)
// ===========================================================================

struct Cell {
    wallets: AggregateRepository<InMemoryRepository, Wallet>,
    outbox: InMemoryOutboxStore,
}

impl Cell {
    fn new() -> Self {
        let repo = InMemoryRepository::new();
        Self {
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

    async fn handle(&self, command: &OutboxMessage) {
        let reply = SagaReply::from_outbox(command).unwrap();
        let cmd: MoveCmd = serde_json::from_slice(&command.payload).unwrap();
        let mut wallet = self.wallets.get(&cmd.wallet).await.unwrap().unwrap();
        if wallet.handled.contains(command.id()) {
            return; // redelivery: effects and reply were committed the first time
        }
        let t = cmd.transfer.clone();
        let c = command.id().to_string();
        let outcome = match command.event_type.as_str() {
            "wallet.debit" | "wallet.credit" => {
                let debit = command.event_type == "wallet.debit";
                if wallet.cancelled.contains(&t) {
                    Err("transfer was cancelled")
                } else if debit && wallet.balance < cmd.amount {
                    Err("insufficient funds")
                } else if !debit && !wallet.accepts_credit {
                    Err("wallet does not accept credits")
                } else {
                    let delta = if debit { -cmd.amount } else { cmd.amount };
                    wallet.moved(c.clone(), t, delta).unwrap();
                    Ok(())
                }
            }
            "wallet.refund" | "wallet.reverse_credit" => {
                if !wallet.cancelled.contains(&t) {
                    let delta = match (wallet.applied.contains(&t), command.event_type.as_str()) {
                        (false, _) => 0, // tombstone only: action never applied
                        (true, "wallet.refund") => cmd.amount,
                        (true, _) => -cmd.amount,
                    };
                    wallet.cancelled(c.clone(), t, delta).unwrap();
                } else {
                    wallet.acknowledged(c.clone()).unwrap();
                }
                Ok(())
            }
            other => panic!("unexpected command {other}"),
        };
        let reply = match outcome {
            Ok(()) => reply.succeeded(format!("receipt:{c}").into_bytes()),
            Err(reason) => {
                wallet.rejected(c).unwrap();
                reply.failed(reason)
            }
        }
        .unwrap();
        self.wallets
            .outbox(reply)
            .commit(&mut wallet)
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
    saga_outbox: InMemoryOutboxStore,
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
        let saga_repo = InMemoryRepository::new();
        let world = Self {
            saga_outbox: saga_repo.outbox_store(),
            saga: SagaRunner::with_clock(saga_repo, clock.clone()),
            clock,
            source: Cell::new(),
            target: Cell::new(),
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
        let now = distributed::Clock::now(&self.clock);
        let down = self.down.lock().unwrap().clone();
        let ids: Vec<String> = store
            .pending(1000)
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.is_due_at(now))
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

    /// Deliver until quiescent; returns saga outcomes observed.
    async fn pump(&self) -> Vec<SagaOutcome> {
        let mut outcomes = Vec::new();
        loop {
            let mut batch = Vec::new();
            for store in [&self.saga_outbox, &self.source.outbox, &self.target.outbox] {
                batch.extend(self.take_due(store).await);
            }
            if batch.is_empty() {
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
    // Debit command + its durable timeout timer.
    assert_eq!(started.messages.len(), 2);
    assert!(started.messages[1].deliver_at.is_some());

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

    // The leftover timers fire later and are ignored.
    world.clock.advance(Duration::from_secs(60));
    let outcomes = world.pump().await;
    assert_eq!(outcomes, vec![SagaOutcome::Ignored, SagaOutcome::Ignored]);
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

    // No reply within 10s: the durable timer fires (manual clock, no sleep).
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
async fn saga_and_its_timer_survive_restart_on_sqlite() {
    use crate::sqlite_support::TempDb;
    use distributed::{SqliteRepository, SystemClock};

    let db = TempDb::new("saga_restart");
    let clock = ManualClock::new(SystemTime::now());
    {
        let repo = SqliteRepository::new(db.pool().await);
        repo.migrate().await.unwrap();
        let runner = SagaRunner::<_, Transfer, _>::with_clock(repo, clock.clone());
        runner.start("t-8", World::data(10)).await.unwrap();
    } // "crash": every handle dropped

    let repo = SqliteRepository::new(db.pool().await);
    let store = repo.outbox_store();
    let pending = store.pending(10).await.unwrap();
    assert_eq!(pending.len(), 2);
    let timer = pending
        .iter()
        .find(|m| m.event_type == distributed::saga::SAGA_TIMEOUT)
        .unwrap();
    let deadline = timer.deliver_at.expect("timer keeps its schedule");

    // Only the debit command is claimable now; the timer is not yet due.
    let claimed = store
        .claim(ClaimOutboxMessages::new("w", 10, Duration::from_secs(60)))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].event_type, "wallet.debit");

    // Once due (the timer was armed 10s after the manual start time, which
    // is ~now), the restarted runner times the step out and compensates.
    let saga_message = SagaMessage::from_outbox(timer).unwrap();
    let runner = SagaRunner::<_, Transfer, _>::with_clock(repo, SystemClock);
    let saga = runner.get("t-8").await.unwrap().unwrap();
    assert_eq!(saga.cursor().unwrap().deadline, deadline);
    let outcome = runner.handle(&saga_message).await.unwrap();
    assert_eq!(outcome.outcome, SagaOutcome::Applied);
    let saga = runner.get("t-8").await.unwrap().unwrap();
    assert_eq!(saga.status(), SagaStatus::Compensating);
    assert_eq!(saga.applied_steps(), vec!["debit"]);
}
