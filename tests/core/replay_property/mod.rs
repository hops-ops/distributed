//! Replay-determinism property tests.
//!
//! Event sourcing's core promise is that an aggregate's state is a pure
//! function of its event stream. These proptests generate arbitrary command
//! sequences for a sample aggregate and assert two invariants that the recent
//! publish-on-commit + snapshot work depends on:
//!
//! 1. **Reload determinism** — applying commands in memory and reloading from
//!    storage yields identical state. (round-trip is lossless)
//! 2. **Snapshot transparency** — a snapshot-hydrated reload equals a
//!    full-replay reload, for *any* snapshot frequency. Snapshots are an
//!    optimization that must never change observable state.

mod aggregate;

use aggregate::{Command, Ledger};
use distributed::{AggregateBuilder, InMemoryRepository};
use proptest::prelude::*;
use tokio::runtime::Runtime;

/// Generate a single command. Amounts are kept positive (the aggregate's guards
/// already cover the "no-op" cases like overdraw) and bounded to keep balances
/// in a sane range.
fn command_strategy() -> impl Strategy<Value = Command> {
    prop_oneof![
        (1i64..1_000).prop_map(Command::Deposit),
        (1i64..1_000).prop_map(Command::Withdraw),
        "[a-z]{1,8}".prop_map(Command::Annotate),
    ]
}

fn commands_strategy() -> impl Strategy<Value = Vec<Command>> {
    prop::collection::vec(command_strategy(), 0..40)
}

/// Build the canonical in-memory aggregate by applying `commands` to a freshly
/// opened ledger. This is the source of truth the storage round-trips must match.
fn in_memory_ledger(id: &str, commands: &[Command]) -> Ledger {
    let mut ledger = Ledger::default();
    ledger.open(id.to_string()).expect("open should succeed");
    for command in commands {
        command.apply(&mut ledger);
    }
    ledger
}

/// Compare the *domain* projection (the part that is a pure fold of events).
/// Does not compare persistence bookkeeping like `committed_version`, so it is
/// valid to compare a reloaded aggregate against a never-persisted in-memory one.
fn assert_same_domain_state(actual: &Ledger, expected: &Ledger, context: &str) {
    assert_eq!(
        actual.balance, expected.balance,
        "balance mismatch ({context})"
    );
    assert_eq!(
        actual.applied, expected.applied,
        "applied mismatch ({context})"
    );
    assert_eq!(
        actual.last_note, expected.last_note,
        "last_note mismatch ({context})"
    );
    assert_eq!(
        actual.entity.id(),
        expected.entity.id(),
        "id mismatch ({context})"
    );
}

/// Compare two *reloaded* aggregates fully, including the committed version —
/// both came from storage, so the version is meaningful and must agree.
fn assert_same_persisted_state(actual: &Ledger, expected: &Ledger, context: &str) {
    assert_same_domain_state(actual, expected, context);
    assert_eq!(
        actual.entity.committed_version(),
        expected.entity.committed_version(),
        "version mismatch ({context})"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    /// Applying commands in memory then reloading from storage reproduces the
    /// exact same state — for any command sequence.
    #[test]
    fn reload_reproduces_in_memory_state(commands in commands_strategy()) {
        let runtime = Runtime::new().expect("runtime");
        runtime.block_on(async move {
            let id = "ledger-reload";
            let repo = InMemoryRepository::new();
            let ledger_repo = repo.aggregate::<Ledger>();

            // Build and commit the whole sequence at once.
            let mut expected = in_memory_ledger(id, &commands);
            ledger_repo.commit(&mut expected).await.expect("commit");

            let reloaded = ledger_repo
                .get(id)
                .await
                .expect("reload")
                .expect("ledger should exist");

            // `expected` was committed through the same repo, so its version is
            // set — compare the full persisted state.
            assert_same_persisted_state(&reloaded, &expected, "reload round-trip");
        });
    }

    /// Snapshots are a transparent optimization: hydrating from a snapshot at any
    /// frequency yields the same state as a full replay of the raw stream.
    #[test]
    fn snapshot_hydration_matches_full_replay(
        commands in commands_strategy(),
        frequency in 1u64..7,
    ) {
        let runtime = Runtime::new().expect("runtime");
        runtime.block_on(async move {
            let id = "ledger-snapshot";
            let base = InMemoryRepository::new();

            // Commit through a snapshot-enabled repo, one command per cycle so
            // snapshots actually trigger at the chosen frequency. Each cycle does
            // a real load-modify-commit, exercising the partial-replay-from-
            // snapshot path on every load after the first snapshot.
            let snap_repo = base.clone().aggregate::<Ledger>().with_snapshots(frequency);
            {
                let mut ledger = Ledger::default();
                ledger.open(id.to_string()).expect("open");
                snap_repo.commit(&mut ledger).await.expect("open commit");
            }
            for command in &commands {
                let mut ledger = snap_repo
                    .get(id)
                    .await
                    .expect("load")
                    .expect("ledger exists");
                command.apply(&mut ledger);
                // Commit even when no event was recorded — an empty commit is a
                // no-op and must not corrupt state.
                snap_repo.commit(&mut ledger).await.expect("cycle commit");
            }

            // Path A: hydrate via the snapshot-aware repo (uses the snapshot +
            // post-snapshot replay).
            let via_snapshot = snap_repo
                .get(id)
                .await
                .expect("snapshot reload")
                .expect("ledger exists");

            // Path B: full replay of the raw stream from the same storage, with
            // no snapshot policy at all.
            let via_full_replay = base
                .aggregate::<Ledger>()
                .get(id)
                .await
                .expect("full replay reload")
                .expect("ledger exists");

            // The expected state is the in-memory fold of the same commands.
            let expected = in_memory_ledger(id, &commands);

            // `expected` is an in-memory fold (never persisted) — compare domain
            // state only. The two storage reloads are compared in full.
            assert_same_domain_state(&via_full_replay, &expected, "full replay vs in-memory");
            assert_same_persisted_state(&via_snapshot, &via_full_replay, "snapshot vs full replay");
        });
    }
}
