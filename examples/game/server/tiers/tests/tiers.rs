//! The tier cells on the in-process reference host (a `ManualClock`): a
//! boss room's fact raises its hood, a late or foreign report changes
//! nothing, the respawn schedule brings the boss back, and the saga steps
//! decoded from the outbox ripple the change into the borough and the city
//! (a stale step is ignored).

use std::time::{Duration, SystemTime};

use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellStreamStore,
};
use distributed::microsvc::{Session, USER_ID_KEY};
use distributed::{ManualClock, StreamIdentity};
use mobpit_tiers::*;
use serde_json::{json, Value};

const T0: u64 = 1_700_000_000_000;

fn cell<A: distributed::Aggregate + Send + Sync + 'static>(
    clock: &ManualClock,
    aggregate: &str,
    shard: &str,
) -> AggregateCell<A> {
    let store = CellStreamStore::for_identity(StreamIdentity::new(aggregate, shard).unwrap())
        .with_clock(clock.clone());
    AggregateCell::from_store(store).unwrap()
}

fn session(principal: &str) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, principal);
    session
}

async fn run<A: distributed::Aggregate + Send + Sync + 'static>(
    cell: &AggregateCell<A>,
    command: &str,
    id: &str,
    input: Value,
    principal: &str,
) -> Result<Value, CellDispatchError> {
    let identity = CellCommandIdentity::new(
        "test",
        principal,
        distributed_game::cell::command_id_for("t", id),
    )
    .unwrap();
    cell.dispatch_idempotent(command, &identity, input, session(principal))
        .await
        .map(|r| r.payload().clone())
}

/// Saga steps of every outbox row of `cell`, in stream order (the outbox
/// itself is not ordered).
fn steps<A: distributed::Aggregate + Send + Sync + 'static>(
    cell: &AggregateCell<A>,
) -> Vec<SagaStep> {
    cell.durable_outbox()
        .unwrap()
        .into_iter()
        .filter_map(|m| saga_step(&m.event_type, &m.payload))
        .collect::<Vec<_>>()
        .tap_sort()
}

trait TapSort {
    fn tap_sort(self) -> Self;
}

impl TapSort for Vec<SagaStep> {
    fn tap_sort(mut self) -> Self {
        self.sort_by_key(|s| s.input["sequence"].as_u64());
        self
    }
}

#[tokio::test]
async fn a_boss_kill_ripples_up_and_the_boss_returns() {
    let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(T0));
    let seed = game_sim::mobpit::DEFAULT_SEED;
    let hood_at = (1, 0);
    let boss = game_sim::mobpit::boss_room(seed, hood_at);
    let room = game_sim::mobpit::room_zone(boss);
    let hood_shard = game_sim::mobpit::pair_shard(hood_at);
    let hood = mount_hood(cell::<Hood>(&clock, "Hood", &hood_shard));
    let fact = |by: &str| json!({ "hood": hood_shard, "room": room, "generation": 0, "by": by });
    let zone = format!("zone:{room}");

    // Only the room itself, only into its own hood.
    let foreign = run(&hood, BOSS_DEFEATED, "f1", fact("x"), "zone:pit.9.9").await;
    assert!(
        matches!(foreign, Err(CellDispatchError::Rejected { .. })),
        "{foreign:?}"
    );
    let other_room = json!({ "hood": hood_shard, "room": "pit.9.9", "generation": 0, "by": "x" });
    assert!(run(&hood, BOSS_DEFEATED, "f2", other_room, "zone:pit.9.9")
        .await
        .is_err());

    let first = run(&hood, BOSS_DEFEATED, "k1", fact("Ana"), &zone)
        .await
        .unwrap();
    assert_eq!(
        (
            first["boss_down"].clone(),
            first["tier"].clone(),
            first["changed"].clone()
        ),
        (json!(true), json!(1), json!(true))
    );
    // A late report of the same fall changes nothing.
    let again = run(&hood, BOSS_DEFEATED, "k2", fact("Bob"), &zone)
        .await
        .unwrap();
    assert_eq!(again["changed"], false);
    let state = hood.load().await.unwrap().unwrap();
    assert_eq!(
        (state.boss_down, state.tier, state.last_by.as_str()),
        (true, 1, "Ana")
    );

    // The saga: hood → borough → city.
    let up = steps(&hood);
    assert_eq!(up.len(), 1, "{up:?}");
    assert_eq!((up[0].kind, up[0].command), (BOROUGH_KIND, HOOD_CHANGED));
    let borough = mount_borough(cell::<Borough>(&clock, "Borough", &up[0].shard));
    run(
        &borough,
        HOOD_CHANGED,
        "s1",
        up[0].input.clone(),
        SAGA_PRINCIPAL,
    )
    .await
    .unwrap();
    // Only the saga may run it; a repeated step changes nothing.
    assert!(run(
        &borough,
        HOOD_CHANGED,
        "s2",
        up[0].input.clone(),
        "zone:pit.0.0"
    )
    .await
    .is_err());
    let repeat = run(
        &borough,
        HOOD_CHANGED,
        "s3",
        up[0].input.clone(),
        SAGA_PRINCIPAL,
    )
    .await
    .unwrap();
    assert_eq!(repeat["changed"], false);
    let b = borough.load().await.unwrap().unwrap().state();
    assert_eq!((b.bosses_down, b.weather, b.tier), (1, 0, 1));
    let up = steps(&borough);
    assert_eq!(up.len(), 1);
    assert_eq!((up[0].kind, up[0].shard.as_str()), (CITY_KIND, CITY_SHARD));
    let city = mount_city(cell::<City>(&clock, "City", CITY_SHARD));
    run(
        &city,
        BOROUGH_CHANGED,
        "c1",
        up[0].input.clone(),
        SAGA_PRINCIPAL,
    )
    .await
    .unwrap();
    let c = city.load().await.unwrap().unwrap().state();
    assert_eq!(c.bosses_down, 1);
    assert!(c.banner.contains("1 boss down"), "{}", c.banner);

    // The boss returns on the hood's own schedule; the borough hears of it.
    clock.advance(Duration::from_millis(BOSS_RESPAWN_MS - 1));
    assert!(hood.fire_due_schedules().await.unwrap().fired.is_empty());
    clock.advance(Duration::from_millis(1));
    assert_eq!(hood.fire_due_schedules().await.unwrap().fired.len(), 1);
    let state = hood.load().await.unwrap().unwrap();
    assert_eq!((state.boss_down, state.tier), (false, 1), "the tier stays");
    let up = steps(&hood);
    assert_eq!(up.len(), 2);
    run(
        &borough,
        HOOD_CHANGED,
        "s4",
        up[1].input.clone(),
        SAGA_PRINCIPAL,
    )
    .await
    .unwrap();
    // The first step delivered late is stale.
    let late = run(
        &borough,
        HOOD_CHANGED,
        "s5",
        up[0].input.clone(),
        SAGA_PRINCIPAL,
    )
    .await
    .unwrap();
    assert_eq!(late["changed"], false);
    assert_eq!(
        borough.load().await.unwrap().unwrap().state().bosses_down,
        0
    );
}
