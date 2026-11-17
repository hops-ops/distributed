//! The `ContentRollout` cell on the in-process reference host, driven by a
//! `ManualClock`: only a content admin starts a rollout, the cell freezes →
//! applies → settles on its own schedule, one rollout at a time, and a zone
//! following it (`zone_step`) freezes, then switches and resumes.

use std::time::{Duration, SystemTime};

use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellStreamStore,
};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::{ManualClock, StreamIdentity};
use distributed_game::cell::command_id_for;
use distributed_game::world::content_rollout::{
    mount_content_rollout, zone_step, ContentRollout, ZoneStep, CONTENT_ADMIN_ROLE, PHASE_APPLIED,
    PHASE_FREEZING, PHASE_RUNNING, START,
};
use serde_json::{json, Value};

const T0: u64 = 1_700_000_000_000;

fn cell(clock: &ManualClock) -> AggregateCell<ContentRollout> {
    let store =
        CellStreamStore::for_identity(StreamIdentity::new("ContentRollout", "world").unwrap())
            .with_clock(clock.clone());
    mount_content_rollout(AggregateCell::from_store(store).unwrap())
}

fn session(admin: bool) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "admin:dev");
    if admin {
        session.set(ROLE_KEY, CONTENT_ADMIN_ROLE);
    }
    session
}

async fn start(
    cell: &AggregateCell<ContentRollout>,
    id: &str,
    version: &str,
    admin: bool,
) -> Result<Value, CellDispatchError> {
    let identity = CellCommandIdentity::new("test", "admin:dev", command_id_for("t", id)).unwrap();
    let input = json!({
        "world": "world", "version": version, "notes": format!("{version} notes"),
        "freeze_ms": 2_000, "hard": false,
    });
    cell.dispatch_idempotent(START, &identity, input, session(admin))
        .await
        .map(|r| r.payload().clone())
}

async fn state(
    cell: &AggregateCell<ContentRollout>,
) -> distributed_game::world::content_rollout::ContentState {
    cell.load().await.unwrap().expect("rollout").state()
}

#[tokio::test]
async fn an_admin_rolls_content_out_on_the_cells_own_schedule() {
    let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(T0));
    let cell = cell(&clock);

    assert!(
        start(&cell, "a", "frostfall", false).await.is_err(),
        "not a content admin"
    );
    let receipt = start(&cell, "b", "frostfall", true).await.unwrap();
    assert_eq!(receipt["phase"], PHASE_FREEZING);
    let s = state(&cell).await;
    assert_eq!((s.version.as_str(), s.target.as_str()), ("", "frostfall"));
    assert_eq!(s.notes, "frostfall notes");
    assert!(
        start(&cell, "c", "emberfall", true).await.is_err(),
        "one rollout at a time"
    );
    // A zone following it freezes, once.
    assert_eq!(zone_step(&s, "", false), ZoneStep::Freeze);
    assert_eq!(zone_step(&s, "", true), ZoneStep::Stay);

    // The freeze holds for `freeze_ms`, then the version applies.
    clock.advance(Duration::from_millis(1_999));
    assert!(cell.fire_due_schedules().await.unwrap().fired.is_empty());
    clock.advance(Duration::from_millis(1));
    assert_eq!(cell.fire_due_schedules().await.unwrap().fired.len(), 1);
    let s = state(&cell).await;
    assert_eq!(s.phase, PHASE_APPLIED);
    assert_eq!(s.version, "frostfall");
    assert_eq!(zone_step(&s, "", true), ZoneStep::Apply("frostfall".into()));

    // …and the rollout settles; a zone that slept through it catches up.
    clock.advance(Duration::from_millis(3_000));
    assert_eq!(cell.fire_due_schedules().await.unwrap().fired.len(), 1);
    let s = state(&cell).await;
    assert_eq!(s.phase, PHASE_RUNNING);
    assert_eq!(
        zone_step(&s, "", false),
        ZoneStep::Apply("frostfall".into())
    );
    assert_eq!(zone_step(&s, "frostfall", false), ZoneStep::Stay);
    assert!(
        start(&cell, "d", "frostfall", true).await.is_err(),
        "that version is live"
    );
    start(&cell, "e", "emberfall", true).await.unwrap();

    // Every phase change left as an outward event.
    let events: Vec<_> = cell
        .durable_outbox()
        .unwrap()
        .into_iter()
        .map(|m| m.event_type)
        .collect();
    for name in ["content.freezing", "content.applied", "content.settled"] {
        assert!(
            events
                .iter()
                .any(|e| e.contains(name.split('.').nth(1).unwrap())),
            "{name} in {events:?}"
        );
    }
}
