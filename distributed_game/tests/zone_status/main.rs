//! The zone status read model end to end on the reference host: lease and
//! zone-state cells commit their outward events into their outboxes; the
//! messages go onto a SQLite bus; the `zone_status` projectors (a consumer
//! service like any other) upsert `zone_leases` / `zone_snapshots`; and
//! `ZoneStatus::of` tells active, hibernated and cold zones apart without
//! opening a cell.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use distributed::bus::{Message, MessagePublisher, SqliteBus};
use distributed::cell_host::{AggregateCell, CellCommandIdentity, CellStreamStore};
use distributed::microsvc::{
    spawn_service_consumer_loop, CausalProjectorContext, HandlerError, ModeledProjection, Routes,
    Service, Session, CONSUMER_IDLE_POLL, ROLE_KEY, USER_ID_KEY,
};
use distributed::{Aggregate, BusPublisher, ManualClock, SqliteRepository, StreamIdentity};
use distributed_game::cell::command_id_for;
use distributed_game::world::zone_lease::{mount_zone_lease, ZoneLease, ACQUIRE, RELEASE};
use distributed_game::world::zone_state::{mount_zone_state, ZoneState, SAVE, ZONE_HOST_ROLE};
use distributed_game::world::zone_status::{
    projectors, read_model_catalog, ZoneLeases, ZoneSnapshots, ZoneStatus, APPLICATION,
    ZONE_LEASES, ZONE_SNAPSHOTS,
};
use serde_json::{json, Value};

const T0: u64 = 1_700_000_000_000;
static CALLS: AtomicU64 = AtomicU64::new(0);

fn host_session() -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "zone-server-a");
    session.set(ROLE_KEY, ZONE_HOST_ROLE);
    session
}

fn cell<A: Aggregate + Send + Sync + 'static>(
    kind: &str,
    zone: &str,
    clock: &ManualClock,
) -> AggregateCell<A> {
    let store = CellStreamStore::for_identity(StreamIdentity::new(kind, zone).unwrap())
        .with_clock(clock.clone());
    AggregateCell::from_store(store).unwrap()
}

async fn dispatch<A: Aggregate + Send + Sync + 'static>(
    cell: &AggregateCell<A>,
    command: &str,
    input: Value,
) -> Value {
    let n = CALLS.fetch_add(1, Ordering::SeqCst);
    let identity = CellCommandIdentity::new(
        "zone-host",
        "zone-server-a",
        command_id_for("t", &n.to_string()),
    )
    .unwrap();
    cell.dispatch_idempotent(command, &identity, input, host_session())
        .await
        .unwrap_or_else(|error| panic!("{command}: {error:?}"))
        .payload()
        .clone()
}

async fn project_leases(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(ZONE_LEASES, &context).await
}

async fn project_snapshots(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(ZONE_SNAPSHOTS, &context).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cell_events_project_into_zone_status_rows() {
    let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(T0));
    // z-active: held. z-asleep: held, snapshot saved, released.
    // z-cold: held and released without a snapshot.
    let active = mount_zone_lease(cell::<ZoneLease>("ZoneLease", "z-active", &clock));
    let asleep = mount_zone_lease(cell::<ZoneLease>("ZoneLease", "z-asleep", &clock));
    let asleep_state = mount_zone_state(cell::<ZoneState>("ZoneState", "z-asleep", &clock));
    let cold = mount_zone_lease(cell::<ZoneLease>("ZoneLease", "z-cold", &clock));
    let acquire = |zone: &str| json!({ "zone_id": zone, "holder": "A#1", "ttl_ms": 60_000 });
    dispatch(&active, ACQUIRE, acquire("z-active")).await;
    let held = dispatch(&asleep, ACQUIRE, acquire("z-asleep")).await;
    use base64::Engine as _;
    dispatch(
        &asleep_state,
        SAVE,
        json!({ "zone_id": "z-asleep", "token": held["token"], "content_version": "v1",
                "inline_b64": base64::engine::general_purpose::STANDARD.encode([7u8; 42]) }),
    )
    .await;
    dispatch(
        &asleep,
        RELEASE,
        json!({ "zone_id": "z-asleep", "holder": "A#1", "token": held["token"] }),
    )
    .await;
    let held = dispatch(&cold, ACQUIRE, acquire("z-cold")).await;
    dispatch(
        &cold,
        RELEASE,
        json!({ "zone_id": "z-cold", "holder": "A#1", "token": held["token"] }),
    )
    .await;

    // Outbox → bus, in commit order per cell.
    let dir = std::env::temp_dir().join(format!(
        "zone-status-{}-{}",
        std::process::id(),
        CALLS.load(Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let bus_url = format!("sqlite:{}?mode=rwc", dir.join("bus.db").display());
    let pool = sqlx::SqlitePool::connect(&bus_url).await.unwrap();
    let bus = SqliteBus::new(pool.clone());
    bus.ensure_tables().await.unwrap();
    let publisher = BusPublisher::new(Arc::new(bus));
    let mut messages = Vec::new();
    messages.extend(active.durable_outbox().unwrap());
    messages.extend(asleep.durable_outbox().unwrap());
    messages.extend(asleep_state.durable_outbox().unwrap());
    messages.extend(cold.durable_outbox().unwrap());
    messages.sort_by_key(|m| m.created_at);
    assert_eq!(messages.len(), 6, "activated ×3, released ×2, saved");
    for message in messages {
        publisher.publish(Message::from(message)).await.unwrap();
    }

    // The projectors, as a consumer service.
    let read_url = format!("sqlite:{}?mode=rwc", dir.join("read.db").display());
    let repo = SqliteRepository::connect_and_migrate(&read_url)
        .await
        .unwrap();
    repo.bootstrap_table_schema_for_dev(&read_model_catalog().table_registry().unwrap())
        .await
        .unwrap();
    let read_models = repo.clone();
    spawn_service_consumer_loop(move || {
        let (leases, snapshots) = projectors();
        Service::new()
            .named(APPLICATION)
            .routes(
                Routes::new()
                    .with_read_model_store(read_models.clone())
                    .modeled_projector(leases)
                    .handle(project_leases)
                    .modeled_projector(snapshots)
                    .handle(project_snapshots),
            )
            .with_bus(SqliteBus::new(pool.clone()).with_idle_poll(CONSUMER_IDLE_POLL))
    });

    let status = |zone: &'static str| {
        let repo = repo.clone();
        async move {
            let lease: Option<ZoneLeases> = sqlx::query_as::<_, (String, String, i64, String)>(
                "SELECT zone_id, holder, token, status FROM zone_leases WHERE zone_id = ?",
            )
            .bind(zone)
            .fetch_optional(repo.pool())
            .await
            .unwrap()
            .map(|(zone_id, holder, token, status)| ZoneLeases {
                zone_id,
                holder,
                token,
                status,
            });
            let snapshot: Option<ZoneSnapshots> =
                sqlx::query_as::<_, (String, i64, bool, String, i64)>(
                    "SELECT zone_id, token, stored, content_version, bytes FROM zone_snapshots WHERE zone_id = ?",
                )
                .bind(zone)
                .fetch_optional(repo.pool())
                .await
                .unwrap()
                .map(|(zone_id, token, stored, content_version, bytes)| ZoneSnapshots {
                    zone_id,
                    token,
                    stored,
                    content_version,
                    bytes,
                });
            (lease, snapshot)
        }
    };
    let start = Instant::now();
    loop {
        let (lease, snapshot) = status("z-asleep").await;
        let done = lease.as_ref().is_some_and(|l| l.status == "released") && snapshot.is_some();
        if done
            && status("z-cold")
                .await
                .0
                .is_some_and(|l| l.status == "released")
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "projection timed out"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let of = |(lease, snapshot): (Option<ZoneLeases>, Option<ZoneSnapshots>)| {
        ZoneStatus::of(lease.as_ref(), snapshot.as_ref())
    };
    assert_eq!(
        of(status("z-active").await),
        ZoneStatus::Active {
            holder: "A#1".into()
        }
    );
    assert_eq!(
        of(status("z-asleep").await),
        ZoneStatus::Hibernated { bytes: 42 }
    );
    let (_, snapshot) = status("z-asleep").await;
    assert_eq!(snapshot.unwrap().content_version, "v1");
    assert_eq!(of(status("z-cold").await), ZoneStatus::Cold);
    assert_eq!(of(status("z-never").await), ZoneStatus::Cold);
    let _ = std::fs::remove_dir_all(dir);
}

/// A celld-hosted zone's lifecycle reports (`zone_activity.record` facts)
/// project into `zone_activities`; a report emitted earlier but settled
/// later never overwrites a newer one; only the zone itself may report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zone_activity_reports_project_into_zone_status() {
    use distributed_game::cell::trusted_session;
    use distributed_game::world::zone_activity::{
        mount_zone_activity, zone_principal, RecordActivity, ZoneActivity, RECORD,
    };
    use distributed_game::world::zone_status::{
        activity_projector_for, ZoneActivities, ZONE_ACTIVITIES,
    };
    use distributed_game::{Fact, FactStamp};

    let clock = ManualClock::new(SystemTime::UNIX_EPOCH + Duration::from_millis(T0));
    let cell = mount_zone_activity(cell::<ZoneActivity>("ZoneActivity", "pit.0.1", &clock));
    let report = |status: &str, seq: u64, principal: &str| {
        let fact = Fact::trusted(
            format!("zone_activity.record:pit.0.1:9:{seq}"),
            RECORD,
            serde_json::to_value(RecordActivity {
                status: status.into(),
                activation: "act-1".into(),
                restored: status == "active" && seq > 1,
                players: u32::from(status == "active"),
                node: "node-b".into(),
                isolate: "iso".into(),
            })
            .unwrap(),
            Some(principal.to_string()),
            FactStamp {
                zone_id: "pit.0.1".into(),
                activation: 9,
                seq,
                emitted_at_ms: T0 + seq,
                ..FactStamp::default()
            },
        );
        (fact.clone(), trusted_session(principal, &fact))
    };
    let send = |status: &'static str, seq: u64, principal: &'static str| {
        let cell = &cell;
        async move {
            let (fact, session) = report(status, seq, principal);
            let identity = CellCommandIdentity::new(
                "celld-zone",
                principal,
                command_id_for("celld-zone", fact.key()),
            )
            .unwrap();
            cell.dispatch_idempotent(RECORD, &identity, fact.input().clone(), session)
                .await
        }
    };
    let me = Box::leak(zone_principal("pit.0.1").into_boxed_str());
    assert_eq!(
        send("active", 1, me).await.unwrap().payload()["applied"],
        true
    );
    assert_eq!(
        send("hibernated", 3, me).await.unwrap().payload()["applied"],
        true
    );
    // Emitted before `hibernated`, settled after it: ignored.
    let late = send("dormant", 2, me).await.unwrap();
    assert_eq!(late.payload()["applied"], false);
    assert_eq!(late.payload()["status"], "hibernated");
    assert!(
        send("active", 4, "guest-x").await.is_err(),
        "only the zone reports"
    );
    assert_eq!(cell.load().await.unwrap().unwrap().status(), "hibernated");

    let dir = std::env::temp_dir().join(format!(
        "zone-activity-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let bus_url = format!("sqlite:{}?mode=rwc", dir.join("bus.db").display());
    let pool = sqlx::SqlitePool::connect(&bus_url).await.unwrap();
    let bus = SqliteBus::new(pool.clone());
    bus.ensure_tables().await.unwrap();
    let publisher = BusPublisher::new(Arc::new(bus));
    let mut messages = cell.durable_outbox().unwrap();
    messages.sort_by_key(|m| m.created_at);
    assert_eq!(messages.len(), 2, "one event per applied report");
    for message in messages {
        publisher.publish(Message::from(message)).await.unwrap();
    }
    let read_url = format!("sqlite:{}?mode=rwc", dir.join("read.db").display());
    let repo = SqliteRepository::connect_and_migrate(&read_url)
        .await
        .unwrap();
    repo.bootstrap_table_schema_for_dev(&read_model_catalog().table_registry().unwrap())
        .await
        .unwrap();
    let read_models = repo.clone();
    async fn project(
        context: CausalProjectorContext,
        projection: ModeledProjection,
    ) -> Result<(), HandlerError> {
        projection.apply(ZONE_ACTIVITIES, &context).await
    }
    // A consumer of its own (a minimap service would be one too).
    spawn_service_consumer_loop(move || {
        Service::new()
            .named("zone-minimap")
            .routes(
                Routes::new()
                    .with_read_model_store(read_models.clone())
                    .modeled_projector(activity_projector_for("zone-minimap"))
                    .handle(project),
            )
            .with_bus(SqliteBus::new(pool.clone()).with_idle_poll(CONSUMER_IDLE_POLL))
    });
    let start = Instant::now();
    let row = loop {
        let row = sqlx::query_as::<_, (String, String, String, i64)>(
            "SELECT zone_id, status, node, activations FROM zone_activities WHERE zone_id = ?",
        )
        .bind("pit.0.1")
        .fetch_optional(repo.pool())
        .await
        .unwrap();
        if let Some(row) = row.filter(|row| row.1 == "hibernated") {
            break row;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "projection timed out"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(row.2, "node-b");
    assert_eq!(row.3, 1);
    let activity = ZoneActivities {
        zone_id: row.0,
        status: row.1,
        node: row.2,
        ..ZoneActivities::default()
    };
    assert_eq!(
        ZoneStatus::of_activity(Some(&activity)),
        ZoneStatus::Hibernated { bytes: 0 }
    );
    let _ = std::fs::remove_dir_all(dir);
}
