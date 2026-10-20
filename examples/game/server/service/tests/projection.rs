//! Kill settlements → celld Crawler cell → outbox → celld Queue → the zone
//! server's relay route → SQLite bus → `player_stats` → GraphQL `@live`,
//! with a real `celld dev` serving `examples/game/cells`, the real
//! zone-server world (fact sink + relay route) and the service's projector
//! and engine:
//!
//! 1. settled kills project into one row per player with the cell's totals
//!    (XP decided by the cell; a replayed fact changes nothing);
//! 2. a live leaderboard subscription pushes the next settled kill;
//! 3. the rows equal the cells after a restart of the projector (cursor
//!    resumes, redelivery is deduplicated);
//! 4. the surface has no mutations and the HTTP endpoint refuses a request
//!    without a verified Bearer token.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use async_graphql::Request;
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::SqliteRepository;
use distributed_game::{Fact, FactSink, FactStamp};
use futures_util::StreamExt;
use game_demos::common::random_hex;
use game_demos::devcelld::DevCelld;
use game_demos::mobpit::crawler::RECORD_KILL;
use game_demos::mobpit::{MobPitWorld, WorldOptions};
use mobpit_service::{
    build_engine, open_shared_sqlite, read_model_catalog, spawn_projector, BearerIdentity,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("mobpit-service-{}", random_hex(10)))
}

fn kill(key: &str, principal: &str, tier: u8, name: &str) -> Fact {
    Fact::trusted(
        key,
        RECORD_KILL,
        json!({ "mob": format!("pit-1/{key}"), "tier": tier, "name": name }),
        Some(principal.into()),
        FactStamp::default(),
    )
}

fn identity() -> BearerIdentity {
    // Never used to verify here (sessions are passed directly); HTTP refuses
    // a request without a Bearer before any key is needed.
    BearerIdentity {
        issuer: "https://mobpit.test".into(),
        audience: "mobpit-service".into(),
        jwks: Some(r#"{"keys":[]}"#.into()),
    }
}

fn user(id: &str) -> Session {
    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    session.set(USER_ID_KEY, id);
    session
}

async fn rows(repo: &SqliteRepository) -> Vec<(String, String, i64, i64, i64)> {
    sqlx::query_as(
        "SELECT player_id, name, xp, kills, level FROM player_stats ORDER BY xp DESC, player_id",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap()
}

async fn eventually<F: std::future::Future<Output = bool>>(
    what: &str,
    mut check: impl FnMut() -> F,
) {
    let start = Instant::now();
    while !check().await {
        assert!(
            start.elapsed() < Duration::from_secs(45),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_kills_project_into_live_player_stats() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let bus_url = format!("sqlite:{}", dir.join("bus.db").display());
    let db_url = format!("sqlite:{}?mode=rwc", dir.join("service.db").display());

    // Zone-server side: the relay route on a known port, celld relaying its
    // outbox Queue there, the world's fact sink into the cells.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = format!(
        "http://{}{}",
        listener.local_addr().unwrap(),
        distributed::bus::CELLD_QUEUE_RELAY_PATH
    );
    let celld = DevCelld::start(Some(relay)).unwrap_or_else(|error| panic!("celld dev: {error}"));
    let world = MobPitWorld::open(WorldOptions::new(dir.join("local"), celld.options())).unwrap();
    let routes = game_demos::mobpit::routes::internal_routes(world.clone());
    tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
    let sink = world.fact_sink();
    // A kill settled before the bus is attached waits in the Queue (the
    // relay answers 503 and the consumer retries).
    sink.commit(&kill("k0", "dev-ana", 2, "ana")).await.unwrap();
    world.attach_projection_bus(&bus_url).await.unwrap();

    // Service side.
    let repo = SqliteRepository::connect_and_migrate(&db_url)
        .await
        .unwrap();
    repo.bootstrap_table_schema_for_dev(&read_model_catalog().table_registry().unwrap())
        .await
        .unwrap();
    let bus = open_shared_sqlite(&bus_url).await.unwrap();
    spawn_projector(repo.clone(), bus.clone());
    let engine = build_engine(&repo, &identity(), [7; 32]).unwrap();

    sink.commit(&kill("k1", "dev-ana", 3, "ana")).await.unwrap();
    sink.commit(&kill("k2", "dev-bo", 1, "bo")).await.unwrap();
    // A redelivered fact replays the receipt: no new event, no double XP.
    assert!(
        sink.commit(&kill("k1", "dev-ana", 3, "ana"))
            .await
            .unwrap()
            .replayed
    );
    eventually("projection", || async {
        rows(&repo).await
            == vec![
                ("dev-ana".into(), "ana".into(), 85, 2, 2),
                ("dev-bo".into(), "bo".into(), 10, 1, 1),
            ]
    })
    .await;

    // Live leaderboard: the initial result, then a push for the next kill.
    let board = Request::new(
        "subscription { player_stats(order_by: [{ xp: desc }, { player_id: asc }], limit: 5) { player_id xp kills level } }",
    );
    let mut live = Box::pin(engine.execute_stream(&user("dev-bo"), board));
    let first = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let data: Value = serde_json::to_value(&first.data).unwrap();
    assert_eq!(data["player_stats"][0]["player_id"], "dev-ana");
    sink.commit(&kill("k3", "dev-bo", 3, "bo")).await.unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(10), live.next())
        .await
        .expect("live push after the settled kill")
        .unwrap();
    let data: Value = serde_json::to_value(&pushed.data).unwrap();
    assert_eq!(
        data["player_stats"][0],
        json!({ "player_id": "dev-ana", "xp": 85, "kills": 2, "level": 2 })
    );
    assert_eq!(
        data["player_stats"][1],
        json!({ "player_id": "dev-bo", "xp": 70, "kills": 2, "level": 2 })
    );

    // "My stats" is the same table filtered by the session's id.
    let mine = engine
        .execute(
            &user("dev-bo"),
            Request::new(
                r#"query { player_stats(where: { player_id: { _eq: "dev-bo" } }) { xp } }"#,
            ),
        )
        .await;
    assert_eq!(
        serde_json::to_value(&mine.data).unwrap(),
        json!({ "player_stats": [{ "xp": 70 }] })
    );

    // Read-only surface: there is no mutation to credit XP with.
    let forged = engine
        .execute(
            &user("dev-bo"),
            Request::new(r#"mutation { upsert_player_stats(object: { player_id: "dev-bo", xp: 9999 }) { xp } }"#),
        )
        .await;
    assert!(!forged.errors.is_empty(), "mutations must not exist");

    // A second projector over the same cursor (a restart) re-reads nothing
    // twice: the rows still equal the cells.
    spawn_projector(repo.clone(), bus.clone());
    sink.commit(&kill("k4", "dev-cy", 2, "cy")).await.unwrap();
    eventually("third player", || async { rows(&repo).await.len() == 3 }).await;
    for (player, _, xp, kills, level) in rows(&repo).await {
        let cell = world.cells().crawler(&player).await.unwrap().state.unwrap();
        assert_eq!(
            (xp, kills, level),
            (cell.xp as i64, i64::from(cell.kills), i64::from(cell.level))
        );
    }

    // HTTP: no Bearer → refused (no header-trust identity mode).
    let app = mobpit_service::http::router(std::sync::Arc::new(engine), &["http://ui.test".into()]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let body = r#"{"query":"{ player_stats { player_id xp } }"}"#;
    let raw = |extra: &str| {
        format!(
            "POST /graphql HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    for extra in ["", "x-user-id: dev-ana\r\nx-roles: user\r\n"] {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(raw(extra).as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(
            !response.contains("dev-ana"),
            "unauthenticated read leaked rows: {response}"
        );
        assert!(response.starts_with("HTTP/1.1 401"), "{response}");
    }

    let _ = std::fs::remove_dir_all(dir);
}
