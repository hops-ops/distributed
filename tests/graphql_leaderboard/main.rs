//! Leaderboards through the generated GraphQL surface: role-structured roots,
//! viewer-bound `myRank`, `@live` top-N driven by the projector's change
//! listener, the client manifest section and the static SDL.

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_graphql::Request;
use distributed::graphql::client_manifest::{
    DistributedClientManifest, DistributedClientSurfaceExport,
};
use distributed::graphql::sdl::graphql_sdl_from_surface;
use distributed::graphql::surface::{
    build_surface, surface_for_role, LeaderboardGrant, LeaderboardSurface, SurfaceDialect,
    SurfaceOptions,
};
use distributed::graphql::GraphqlEngine;
use distributed::leaderboard::{
    Board, InMemoryLeaderboard, LeaderboardChange, LeaderboardError, LeaderboardProjector,
    LeaderboardStore, ScorePolicy, ScoreSubmission,
};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::{DomainEventOccurrence, ExternalEventSource};
use futures_util::StreamExt;

fn xp_board() -> Board {
    Board::high_first("mobpit.xp").unwrap()
}

fn xp_surface() -> LeaderboardSurface {
    LeaderboardSurface::single("xp", &xp_board())
        .limits(3, 5, 2)
        .grant("user", LeaderboardGrant::read())
        .grant("moderator", LeaderboardGrant::read().rank_any_member())
}

fn season_surface() -> LeaderboardSurface {
    LeaderboardSurface::family(
        "season",
        "season.xp",
        distributed::leaderboard::ScoreOrder::HighFirst,
    )
    .grant("user", LeaderboardGrant::top_only())
}

async fn engine(store: Arc<InMemoryLeaderboard>) -> GraphqlEngine {
    let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let catalog = distributed::ReadModelCatalog::new("leaderboards");
    GraphqlEngine::from_schema_catalog(&catalog, repo.pool().clone())
        .unwrap()
        .roles(&["user", "moderator"])
        .leaderboard(xp_surface(), store.clone())
        .leaderboard(season_surface(), store)
        .build()
        .expect("build engine")
}

fn session(role: &str, user: Option<&str>) -> Session {
    let mut session = Session::new();
    session.set(ROLE_KEY, role);
    if let Some(user) = user {
        session.set(USER_ID_KEY, user);
    }
    session
}

async fn seed(store: &InMemoryLeaderboard) {
    for (member, score) in [("ann", 50.0), ("bob", 40.0), ("cat", 30.0), ("dan", 20.0)] {
        store
            .submit(
                &xp_board(),
                &ScoreSubmission::new(member, score, ScorePolicy::Replace),
            )
            .await
            .unwrap();
    }
}

async fn run(engine: &GraphqlEngine, session: &Session, query: &str) -> serde_json::Value {
    let response = engine.execute(session, Request::new(query)).await;
    serde_json::to_value(&response).unwrap()
}

fn error_code(response: &serde_json::Value) -> Option<&str> {
    response["errors"][0]["extensions"]["code"].as_str()
}

#[tokio::test]
async fn top_around_and_my_rank_read_the_board_for_the_viewer() {
    let store = Arc::new(InMemoryLeaderboard::new());
    seed(&store).await;
    let engine = engine(store).await;
    let cat = session("user", Some("cat"));

    let top = run(&engine, &cat, "{ xpLeaderboard { rank member score } }").await;
    assert_eq!(
        top["data"]["xpLeaderboard"],
        serde_json::json!([
            { "rank": 0, "member": "ann", "score": 50.0 },
            { "rank": 1, "member": "bob", "score": 40.0 },
            { "rank": 2, "member": "cat", "score": 30.0 },
        ]),
        "default first = 3"
    );
    let around = run(
        &engine,
        &cat,
        "{ xpLeaderboardAround(radius: 1) { member } }",
    )
    .await;
    assert_eq!(
        around["data"]["xpLeaderboardAround"],
        serde_json::json!([{ "member": "bob" }, { "member": "cat" }, { "member": "dan" }])
    );
    let mine = run(&engine, &cat, "{ xpMyRank { rank member score } }").await;
    assert_eq!(
        mine["data"]["xpMyRank"],
        serde_json::json!({ "rank": 2, "member": "cat", "score": 30.0 })
    );
    let unranked = run(
        &engine,
        &session("user", Some("zed")),
        "{ xpMyRank { rank } }",
    )
    .await;
    assert_eq!(unranked["data"]["xpMyRank"], serde_json::Value::Null);

    let season = run(
        &engine,
        &cat,
        r#"{ seasonLeaderboard(board: "s1") { member } }"#,
    )
    .await;
    assert_eq!(season["data"]["seasonLeaderboard"], serde_json::json!([]));
}

#[tokio::test]
async fn my_rank_is_bound_to_the_session_and_ranking_others_needs_the_grant() {
    let store = Arc::new(InMemoryLeaderboard::new());
    seed(&store).await;
    let engine = engine(store).await;
    let cat = session("user", Some("cat"));

    // A user cannot name another member: no argument, no `xpRank` root.
    let spoofed = run(&engine, &cat, r#"{ xpMyRank(member: "ann") { rank } }"#).await;
    assert!(spoofed["errors"].is_array(), "{spoofed}");
    let other = run(&engine, &cat, r#"{ xpRank(member: "ann") { rank } }"#).await;
    assert!(other["errors"].is_array(), "{other}");
    // Around-me is the viewer's neighbourhood only.
    let around = run(
        &engine,
        &cat,
        r#"{ xpLeaderboardAround(member: "ann") { rank } }"#,
    )
    .await;
    assert!(around["errors"].is_array(), "{around}");

    // A moderator granted `rank_any_member` may rank anyone.
    let moderator = session("moderator", Some("mod-1"));
    let ranked = run(
        &engine,
        &moderator,
        r#"{ xpRank(member: "ann") { rank score } }"#,
    )
    .await;
    assert_eq!(
        ranked["data"]["xpRank"],
        serde_json::json!({ "rank": 0, "score": 50.0 })
    );

    // Without the member claim the viewer-relative roots fail closed.
    let anonymous = run(&engine, &session("user", None), "{ xpMyRank { rank } }").await;
    assert_eq!(error_code(&anonymous), Some("FORBIDDEN"), "{anonymous}");
}

#[tokio::test]
async fn arguments_are_validated() {
    let store = Arc::new(InMemoryLeaderboard::new());
    let engine = engine(store).await;
    let cat = session("user", Some("cat"));
    for query in [
        "{ xpLeaderboard(first: 6) { rank } }",
        "{ xpLeaderboard(first: 0) { rank } }",
        "{ xpLeaderboardAround(radius: 3) { rank } }",
        r#"{ seasonLeaderboard(board: "not a key") { rank } }"#,
    ] {
        let response = run(&engine, &cat, query).await;
        assert_eq!(
            error_code(&response),
            Some("BAD_REQUEST"),
            "{query}: {response}"
        );
    }
    let missing = run(&engine, &cat, "{ seasonLeaderboard { rank } }").await;
    assert!(
        missing["errors"].is_array(),
        "family boards require `board`"
    );
}

#[derive(serde::Serialize, serde::Deserialize, distributed::DomainEvent)]
#[domain_event(name = "test.xp_gained", version = 1)]
struct XpGained {
    player: String,
    xp: u32,
}

fn gained(position: u64, player: &str, xp: u32) -> DomainEventOccurrence {
    DomainEventOccurrence::capture_external(
        ExternalEventSource {
            producer: "zone.sim".into(),
            stream: "zone-1".into(),
            position,
            key: "xp".into(),
        },
        UNIX_EPOCH + Duration::from_millis(position),
        Default::default(),
        &XpGained {
            player: player.into(),
            xp,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn live_top_n_updates_after_an_applied_score_change() {
    let store = Arc::new(InMemoryLeaderboard::new());
    seed(&store).await;
    let engine = engine(store.clone()).await;
    let projection =
        |event: &DomainEventOccurrence| -> Result<Vec<LeaderboardChange>, LeaderboardError> {
            let body: XpGained = event
                .decode_body()
                .map_err(|error| LeaderboardError::invalid(error.to_string()))?;
            Ok(vec![LeaderboardChange::new(
                xp_board(),
                ScoreSubmission::new(body.player, f64::from(body.xp), ScorePolicy::Increment),
            )])
        };
    let projector = LeaderboardProjector::new(store.clone(), Arc::new(projection))
        .with_change_listener(engine.leaderboard_change_listener());

    let mut stream = engine.execute_stream(
        &session("user", Some("dan")),
        Request::new("subscription { xpLeaderboard(first: 2) { member score } }"),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first.data).unwrap()["xpLeaderboard"],
        serde_json::json!([{ "member": "ann", "score": 50.0 }, { "member": "bob", "score": 40.0 }])
    );

    // A change below the top two re-executes but does not push (hash gate).
    projector.apply_event(&gained(1, "dan", 5)).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), stream.next())
            .await
            .is_err(),
        "no push when the top-N is unchanged"
    );

    projector.apply_event(&gained(2, "dan", 100)).await.unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&pushed.data).unwrap()["xpLeaderboard"],
        serde_json::json!([{ "member": "dan", "score": 125.0 }, { "member": "ann", "score": 50.0 }])
    );

    // A redelivered (duplicate) event changes nothing and pushes nothing.
    projector.apply_event(&gained(2, "dan", 100)).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), stream.next())
            .await
            .is_err()
    );
}

fn surfaces() -> distributed::graphql::surface::Surface {
    let options = SurfaceOptions {
        dialect: SurfaceDialect::Sqlite,
        aggregates: false,
        subscriptions: true,
        default_limit: 100,
        max_limit: 1000,
    };
    build_surface(&[], &options)
        .unwrap()
        .with_leaderboards([xp_surface(), season_surface()])
        .unwrap()
}

fn role_manifest(role: &str) -> DistributedClientManifest {
    let selected = surface_for_role(&surfaces(), role, &Default::default()).unwrap();
    DistributedClientSurfaceExport::from_selected("leaderboards", selected)
        .unwrap()
        .manifest()
        .unwrap()
        .clone()
}

#[test]
fn board_queries_are_in_the_client_manifest_per_role() {
    let user = role_manifest("user");
    assert!(user.capabilities.live_queries);
    let names = user
        .leaderboards
        .iter()
        .flat_map(|leaderboard| leaderboard.roots.iter().map(|root| root.name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "seasonLeaderboard",
            "xpLeaderboard",
            "xpLeaderboardAround",
            "xpMyRank"
        ]
    );
    let xp = user.leaderboards.iter().find(|l| l.name == "xp").unwrap();
    assert_eq!(xp.entry_typename, "LeaderboardEntry");
    assert_eq!(xp.order, "high_first");
    assert_eq!((xp.default_first, xp.max_first, xp.max_radius), (3, 5, 2));
    let my_rank = xp.roots.iter().find(|r| r.name == "xpMyRank").unwrap();
    assert!(
        my_rank.arguments.is_empty(),
        "the member is never an argument"
    );
    assert!(my_rank.live && !my_rank.list);

    let moderator = role_manifest("moderator");
    let rank = moderator.leaderboards[0]
        .roots
        .iter()
        .find(|root| root.name == "xpRank")
        .unwrap();
    assert!(!rank.live);
    assert_eq!(rank.arguments[0].name, "member");

    let guest = role_manifest("guest");
    assert!(guest.leaderboards.is_empty());
    let json = serde_json::to_value(&guest).unwrap();
    assert!(json.get("leaderboards").is_none(), "absent when empty");

    // Round-trips through the strict manifest schema.
    let encoded = serde_json::to_string(&user).unwrap();
    let decoded: DistributedClientManifest = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, user);
    assert_ne!(user.schema_fingerprint, guest.schema_fingerprint);
}

#[test]
fn sdl_declares_the_entry_type_and_role_roots() {
    let user = surface_for_role(&surfaces(), "user", &Default::default()).unwrap();
    let sdl = graphql_sdl_from_surface(&user).unwrap();
    assert!(sdl
        .contains("type LeaderboardEntry {\n  rank: Int!\n  member: String!\n  score: Float!\n}"));
    assert!(sdl.contains("  xpLeaderboard(first: Int): [LeaderboardEntry!]!\n"));
    assert!(sdl.contains("  xpMyRank: LeaderboardEntry\n"));
    assert!(sdl.contains("  seasonLeaderboard(board: String!, first: Int): [LeaderboardEntry!]!\n"));
    assert!(!sdl.contains("xpRank"));
    assert!(sdl.contains("type Subscription {"));
}

/// With the causal client protocol on (as Mob Pit runs it), leaderboard
/// results carry an incomplete snapshot and, live, `snapshot` mode with reset.
#[tokio::test]
async fn protocol_envelopes_mark_leaderboards_as_snapshot_only() {
    let store = Arc::new(InMemoryLeaderboard::new());
    seed(&store).await;
    let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let catalog = distributed::ReadModelCatalog::new("leaderboards");
    let engine = GraphqlEngine::from_schema_catalog(&catalog, repo.pool().clone())
        .unwrap()
        .roles(&["user", "moderator"])
        .protocol_token_key([7; 32])
        .leaderboard(xp_surface(), store.clone())
        .build()
        .expect("protocol engine");
    let cat = session("user", Some("cat"));

    let query = engine
        .execute(&cat, Request::new("{ xpMyRank { rank } }"))
        .await;
    assert!(query.errors.is_empty(), "{:?}", query.errors);
    let query = serde_json::to_value(&query).unwrap();
    let snapshot = &query["extensions"]["distributed"]["snapshot"];
    assert_eq!(snapshot["recordsComplete"], false, "{query}");
    assert_eq!(snapshot["indexesComparable"], false, "{query}");

    let mut stream = engine.execute_stream(
        &cat,
        Request::new("subscription { xpLeaderboard(first: 1) { member } }"),
    );
    let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(frame.errors.is_empty(), "{:?}", frame.errors);
    let frame = serde_json::to_value(&frame).unwrap();
    let live = &frame["extensions"]["distributed"]["live"];
    assert_eq!(live["mode"], "snapshot", "{frame}");
    assert_eq!(live["reset"], true, "{frame}");
}

/// The framework's manifest compiles with the CLI client compiler: the CLI
/// recomputes the same schema fingerprint and generates `leaderboards.ts`.
#[test]
fn the_cli_compiles_a_framework_leaderboard_manifest_into_the_ts_client() {
    use distributed::graphql::surface::RoleGrant;
    use distributed_cli::{
        compile_client, ClientCompileInput, ClientDocument, ClientSurfaceSelector,
    };

    let items = distributed::TableSchema {
        model_name: "ItemView".into(),
        table_name: "items".into(),
        columns: vec![
            distributed::TableColumn {
                primary_key: true,
                ..distributed::TableColumn::new("id", "id", distributed::ColumnType::Text)
            },
            distributed::TableColumn::new("name", "name", distributed::ColumnType::Text),
        ],
        primary_key: distributed::PrimaryKey::new(["id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: distributed::TableKind::ReadModel,
    };
    let options = SurfaceOptions {
        dialect: SurfaceDialect::Sqlite,
        aggregates: false,
        subscriptions: true,
        default_limit: 100,
        max_limit: 1000,
    };
    let surface = build_surface(&[items], &options)
        .unwrap()
        .with_leaderboards([xp_surface()])
        .unwrap();
    let grants =
        std::collections::BTreeMap::from([("ItemView".to_string(), RoleGrant::all_columns())]);
    let selected = surface_for_role(&surface, "user", &grants).unwrap();
    let manifest = DistributedClientSurfaceExport::from_selected("leaderboards", selected)
        .unwrap()
        .manifest_json_pretty()
        .unwrap();
    let project = compile_client(ClientCompileInput::new(
        serde_json::from_str(&manifest).unwrap(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/items.graphql",
            "query Items { items { name } }",
        )],
    ))
    .expect("the CLI accepts the framework manifest");
    let leaderboards = project
        .files
        .iter()
        .find(|file| file.path == "leaderboards.ts")
        .expect("leaderboards.ts");
    assert!(leaderboards
        .contents
        .contains("export const XpMyRankLeaderboard = {"));
    assert!(leaderboards
        .contents
        .contains(r#"document: "query XpLeaderboardAround($radius: Int) { xpLeaderboardAround(radius: $radius) { rank member score } }","#));
}

/// The projector runs in another process (here: another bus connection and
/// no shared engine): it publishes `distributed.leaderboard.changed` on the
/// bus, and the GraphQL process's bus handlers drive `@live` from it.
#[cfg(feature = "leaderboard-background")]
#[tokio::test]
async fn live_boards_follow_a_projector_in_another_process_over_the_bus() {
    use distributed::bus::SqliteBus;
    use distributed::bus::{BusConsumer, RunOptions};
    use distributed::leaderboard::LeaderboardChangePublisher;
    use distributed::outbox_worker::BusPublisher;

    let path = std::env::temp_dir().join(format!(
        "distributed-board-notices-{}-{}.db",
        std::process::id(),
        UNIX_EPOCH.elapsed().unwrap().as_nanos()
    ));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let open = || async {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(
                url.parse::<sqlx::sqlite::SqliteConnectOptions>()
                    .unwrap()
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let bus = SqliteBus::new(pool).with_idle_poll(Duration::from_millis(20));
        bus.ensure_tables().await.unwrap();
        bus
    };

    // Shared board storage (Redis in production).
    let store = Arc::new(InMemoryLeaderboard::new());
    seed(&store).await;

    // GraphQL process: engine + bus handlers under its own consumer group.
    let engine = engine(store.clone()).await;
    let handlers = engine.leaderboard_change_handlers("graphql-1-boards");
    let graphql_bus = open().await;
    let consumer = tokio::spawn(async move {
        graphql_bus
            .subscribe(Arc::new(handlers), RunOptions::default())
            .await
    });

    // Projector process: no engine, only the bus.
    let projector_bus = open().await;
    let notices = LeaderboardChangePublisher::spawn(BusPublisher::new(Arc::new(projector_bus)));
    let projection =
        |event: &DomainEventOccurrence| -> Result<Vec<LeaderboardChange>, LeaderboardError> {
            let body: XpGained = event
                .decode_body()
                .map_err(|error| LeaderboardError::invalid(error.to_string()))?;
            Ok(vec![LeaderboardChange::new(
                xp_board(),
                ScoreSubmission::new(body.player, f64::from(body.xp), ScorePolicy::Increment),
            )])
        };
    let projector = LeaderboardProjector::new(
        store.clone() as Arc<dyn LeaderboardStore>,
        Arc::new(projection),
    )
    .with_change_listener(notices.listener());

    let mut stream = engine.execute_stream(
        &session("user", Some("dan")),
        Request::new("subscription { xpLeaderboard(first: 2) { member score } }"),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first.data).unwrap()["xpLeaderboard"][0]["member"],
        "ann"
    );

    projector.apply_event(&gained(1, "dan", 100)).await.unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("a projector in another process drives @live over the bus")
        .unwrap();
    assert_eq!(
        serde_json::to_value(&pushed.data).unwrap()["xpLeaderboard"],
        serde_json::json!([{ "member": "dan", "score": 120.0 }, { "member": "ann", "score": 50.0 }])
    );

    consumer.abort();
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
