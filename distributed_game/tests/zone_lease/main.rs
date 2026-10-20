//! `ZoneLease` cells on the in-process reference host, driven by a
//! `ManualClock`: ownership conformance, no outbox rows for renewals,
//! exactly one `zone.lease_expired`, and renewals racing the alarm.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use distributed::cell_host::{
    cell_alarm_session, AggregateCell, CellCommandIdentity, CellDispatchError, CellStreamStore,
};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::{Clock, ManualClock, StreamIdentity};
use distributed_game::cell::command_id_for;
use distributed_game::world::zone_lease::{
    mount_zone_lease, ownership_error, ClaimPayload, ZoneLease, ACQUIRE, EXPIRE, RELEASE, RENEW,
    ZONE_HOST_ROLE,
};
use distributed_game::world::{ownership_conformance, OwnershipError, ZoneClaim, ZoneOwnership};
use distributed_game::BoxFuture;
use serde_json::{json, Value};

const T0: u64 = 1_700_000_000_000;

fn clock() -> ManualClock {
    ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(T0))
}

fn lease_cell(clock: &ManualClock, zone: &str) -> AggregateCell<ZoneLease> {
    let store = CellStreamStore::for_identity(StreamIdentity::new("ZoneLease", zone).unwrap())
        .with_clock(clock.clone());
    mount_zone_lease(AggregateCell::from_store(store).unwrap())
}

fn host_session() -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "zone-server-a");
    session.set(ROLE_KEY, ZONE_HOST_ROLE);
    session
}

/// `ZoneOwnership` over one in-process cell, as `CellZoneOwnership` is over
/// celld (same commands, same error mapping).
struct InProcess {
    cell: AggregateCell<ZoneLease>,
    calls: AtomicU64,
}

impl InProcess {
    async fn call(&self, command: &str, input: Value) -> Result<ClaimPayload, OwnershipError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let identity = CellCommandIdentity::new(
            "zone-host",
            "zone-server-a",
            command_id_for("t", &n.to_string()),
        )
        .unwrap();
        match self
            .cell
            .dispatch_idempotent(command, &identity, input, host_session())
            .await
        {
            Ok(result) => Ok(serde_json::from_value(result.payload().clone()).unwrap()),
            Err(CellDispatchError::Rejected { message, .. }) => Err(ownership_error(&message)),
            Err(error) => Err(OwnershipError::Backend(format!("{error:?}"))),
        }
    }
}

fn claim(payload: ClaimPayload) -> ZoneClaim {
    ZoneClaim {
        zone_id: payload.zone_id,
        holder: payload.holder,
        token: payload.token,
        expires_at: SystemTime::UNIX_EPOCH + Duration::from_millis(payload.expires_at_ms),
    }
}

impl ZoneOwnership for InProcess {
    fn acquire<'a>(
        &'a self,
        zone_id: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            self.call(
                ACQUIRE,
                json!({ "zone_id": zone_id, "holder": holder, "ttl_ms": ttl.as_millis() as u64 }),
            )
            .await
            .map(claim)
        })
    }

    fn renew<'a>(
        &'a self,
        current: &'a ZoneClaim,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            self.call(
                RENEW,
                json!({ "zone_id": current.zone_id, "holder": current.holder,
                        "token": current.token, "ttl_ms": ttl.as_millis() as u64 }),
            )
            .await
            .map(claim)
        })
    }

    fn release<'a>(&'a self, current: &'a ZoneClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = self
                .call(
                    RELEASE,
                    json!({ "zone_id": current.zone_id, "holder": current.holder,
                            "token": current.token }),
                )
                .await;
        })
    }
}

fn in_process(clock: &ManualClock, zone: &str) -> Arc<InProcess> {
    Arc::new(InProcess {
        cell: lease_cell(clock, zone),
        calls: AtomicU64::new(0),
    })
}

/// Outward event types in the cell's outbox, sorted (the reference host's
/// export is unordered; delivery order comes from `created_at`).
fn outward(cell: &AggregateCell<ZoneLease>) -> Vec<String> {
    let mut types = cell
        .durable_outbox()
        .unwrap()
        .into_iter()
        .map(|message| message.event_type)
        .collect::<Vec<_>>();
    types.sort();
    types
}

#[tokio::test]
async fn the_lease_cell_passes_the_ownership_conformance_suite() {
    let clock = clock();
    let own = in_process(&clock, "conformance");
    ownership_conformance(own.as_ref(), "conformance").await;
    assert_eq!(
        outward(&own.cell),
        [
            "zone.activated",
            "zone.activated",
            "zone.activated",
            "zone.released",
            "zone.released",
            "zone.released"
        ]
    );
}

#[tokio::test]
async fn renewals_produce_no_outbox_rows_and_move_the_expiry_alarm() {
    let clock = clock();
    let own = in_process(&clock, "z1");
    let ttl = Duration::from_secs(10);
    let mut current = own.acquire("z1", "a#1", ttl).await.unwrap();
    assert_eq!(outward(&own.cell), ["zone.activated"]);
    assert_eq!(
        own.cell.next_alarm_due().unwrap(),
        Some(SystemTime::UNIX_EPOCH + Duration::from_millis(T0 + 10_000))
    );
    for step in 1..=20u64 {
        clock.advance(Duration::from_secs(3));
        current = own.renew(&current, ttl).await.unwrap();
        assert_eq!(
            own.cell.next_alarm_due().unwrap(),
            Some(clock.now() + ttl),
            "step {step}"
        );
        assert!(own.cell.fire_due_alarms().await.unwrap().is_empty());
    }
    assert_eq!(
        outward(&own.cell),
        ["zone.activated"],
        "no heartbeat events"
    );
    assert_eq!(
        own.cell.load().await.unwrap().unwrap().holder(),
        Some("a#1")
    );
}

#[tokio::test]
async fn a_holder_that_stops_renewing_expires_exactly_once() {
    let clock = clock();
    let own = in_process(&clock, "z2");
    let ttl = Duration::from_secs(10);
    let held = own.acquire("z2", "a#1", ttl).await.unwrap();
    clock.advance(Duration::from_millis(9_999));
    assert!(own.cell.fire_due_alarms().await.unwrap().is_empty());
    clock.advance(Duration::from_millis(1));
    let report = own.cell.fire_due_alarms().await.unwrap();
    assert_eq!(report.fired.len(), 1);
    assert_eq!(report.fired[0].payload["expired"], true);
    assert_eq!(outward(&own.cell), ["zone.activated", "zone.lease_expired"]);
    clock.advance(Duration::from_secs(60));
    assert!(own.cell.fire_due_alarms().await.unwrap().is_empty());
    assert_eq!(outward(&own.cell).len(), 2, "exactly one expiry");
    assert_eq!(own.renew(&held, ttl).await, Err(OwnershipError::Lost));
    let next = own.acquire("z2", "b#1", ttl).await.unwrap();
    assert!(next.token > held.token);
}

#[tokio::test]
async fn a_renewal_racing_the_alarm_never_produces_a_false_expiry() {
    let clock = clock();
    let own = in_process(&clock, "z3");
    let ttl = Duration::from_secs(10);
    let held = own.acquire("z3", "a#1", ttl).await.unwrap();

    // The alarm comes due, but the renewal lands before the wake.
    clock.advance(ttl);
    let held = own.renew(&held, ttl).await.unwrap();
    assert!(own.cell.fire_due_alarms().await.unwrap().is_empty());

    // The wake read the due alarm, then the renewal interleaved, then the
    // stale expiry command runs: the handler re-reads the lease and keeps it.
    clock.advance(ttl);
    let stale = own.cell.armed_alarms().unwrap().remove(0);
    let _held = own.renew(&held, ttl).await.unwrap();
    let result = own
        .cell
        .dispatch_idempotent(
            EXPIRE,
            &stale.command_identity().unwrap(),
            stale.input.clone(),
            cell_alarm_session(&stale),
        )
        .await
        .unwrap();
    assert_eq!(result.payload()["expired"], false);
    assert_eq!(outward(&own.cell), ["zone.activated"]);
    assert_eq!(
        own.cell.load().await.unwrap().unwrap().holder(),
        Some("a#1")
    );

    // Callers cannot run the expiry command themselves.
    let forged = own
        .cell
        .dispatch_idempotent(
            EXPIRE,
            &CellCommandIdentity::new("zone-host", "zone-server-a", command_id_for("t", "forged"))
                .unwrap(),
            json!({ "zone_id": "z3" }),
            host_session(),
        )
        .await;
    assert!(forged.is_err());
}

#[tokio::test]
async fn only_zone_hosts_may_acquire() {
    let clock = clock();
    let cell = lease_cell(&clock, "z4");
    let mut player = Session::new();
    player.set(USER_ID_KEY, "player-1");
    let refused = cell
        .dispatch_idempotent(
            ACQUIRE,
            &CellCommandIdentity::new("zone-host", "player-1", command_id_for("t", "p")).unwrap(),
            json!({ "zone_id": "z4", "holder": "p#1", "ttl_ms": 1000 }),
            player,
        )
        .await;
    assert!(refused.is_err());
    assert!(cell.load().await.unwrap().is_none());
}
