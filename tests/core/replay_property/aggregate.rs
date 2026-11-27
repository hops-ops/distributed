//! A small but non-trivial sample aggregate for replay-determinism property
//! tests. It has multiple event types, a guarded command (so not every command
//! produces an event), and accumulates derived state — enough that a replay bug
//! (wrong order, dropped event, snapshot drift) would change the final state.

use distributed::{sourced, Entity, Snapshottable};
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone)]
pub struct Ledger {
    pub entity: Entity,
    /// Running balance; deposits add, withdrawals subtract.
    pub balance: i64,
    /// Number of events that actually applied (guarded ones may not).
    pub applied: u64,
    /// Last note set, to exercise a String field in snapshots.
    pub last_note: String,
}

#[sourced(entity, aggregate_type = "replay_property.ledger")]
impl Ledger {
    #[event("opened", when = self.entity.id().is_empty())]
    pub fn open(&mut self, id: String) {
        self.entity.set_id(&id);
    }

    #[event("deposited", when = amount > 0)]
    pub fn deposit(&mut self, amount: i64) {
        self.balance += amount;
        self.applied += 1;
    }

    // Guarded: only applies when the balance can cover it. This means an
    // identical command stream can yield different events depending on prior
    // state — a good stress for replay determinism.
    #[event("withdrawn", when = amount > 0 && self.balance >= amount)]
    pub fn withdraw(&mut self, amount: i64) {
        self.balance -= amount;
        self.applied += 1;
    }

    #[event("annotated", when = !note.is_empty())]
    pub fn annotate(&mut self, note: String) {
        self.last_note = note;
        self.applied += 1;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LedgerSnapshot {
    pub id: String,
    pub balance: i64,
    pub applied: u64,
    pub last_note: String,
}

impl Snapshottable for Ledger {
    type Snapshot = LedgerSnapshot;

    fn create_snapshot(&self) -> LedgerSnapshot {
        LedgerSnapshot {
            id: self.entity.id().to_string(),
            balance: self.balance,
            applied: self.applied,
            last_note: self.last_note.clone(),
        }
    }

    fn restore_from_snapshot(&mut self, snapshot: LedgerSnapshot) {
        self.entity.set_id(&snapshot.id);
        self.balance = snapshot.balance;
        self.applied = snapshot.applied;
        self.last_note = snapshot.last_note;
    }
}

/// The closed set of commands a property test can generate.
#[derive(Clone, Debug)]
pub enum Command {
    Deposit(i64),
    Withdraw(i64),
    Annotate(String),
}

impl Command {
    /// Apply a command to an in-memory aggregate, mirroring exactly what the
    /// generated command methods do (a domain-invalid command is simply a
    /// no-op that records no event — same as the guarded `#[event]` behaviour).
    pub fn apply(&self, ledger: &mut Ledger) {
        match self {
            // Amounts are generated positive, so these never error; a guard that
            // does not hold (e.g. overdraw) simply records no event.
            Command::Deposit(amount) => {
                let _ = ledger.deposit(*amount);
            }
            Command::Withdraw(amount) => {
                let _ = ledger.withdraw(*amount);
            }
            Command::Annotate(note) => {
                let _ = ledger.annotate(note.clone());
            }
        }
    }
}
