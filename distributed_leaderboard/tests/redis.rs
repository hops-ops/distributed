//! Redis leaderboard store integration tests.
//!
//! Tests that need a server skip (with a logged reason) when `REDIS_URL` is
//! unset; the unavailable-server test always runs.
//!
//!   REDIS_URL=redis://127.0.0.1:6379 cargo test -p distributed_leaderboard --features redis,graphql --test redis
#![cfg(feature = "redis")]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use distributed::bus::{Message, MessageKind};
use distributed::DomainEventOccurrence;
use distributed_leaderboard::{
    conformance, Board, LeaderboardChange, LeaderboardError, LeaderboardErrorKind,
    LeaderboardProjector, LeaderboardStore, RedisLeaderboard, ScorePolicy, ScoreSubmission,
};

#[path = "../../tests/support/env.rs"]
mod env_support;

fn redis_url() -> Option<String> {
    env_support::require_service("REDIS_URL", "redis leaderboard test")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
}

#[derive(serde::Serialize, serde::Deserialize, distributed::DomainEvent)]
#[domain_event(name = "dungeon.gold_looted", version = 1)]
struct GoldLooted {
    player: String,
    gold: u32,
}

fn looted(position: u64, player: &str, gold: u32) -> DomainEventOccurrence {
    DomainEventOccurrence::capture_external(
        distributed::ExternalEventSource {
            producer: "zone.sim".into(),
            stream: "zone-7".into(),
            position,
            key: "loot".into(),
        },
        UNIX_EPOCH + Duration::from_secs(position),
        Default::default(),
        &GoldLooted {
            player: player.into(),
            gold,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn redis_store_satisfies_the_leaderboard_contract() {
    let Some(url) = redis_url() else { return };
    let store = RedisLeaderboard::connect(&url)
        .unwrap()
        .with_prefix(unique("test:lb"));
    conformance::run_store_contract(&store, "contract").await;
}

#[tokio::test]
async fn redis_projector_dedupes_replays_and_rebuilds() {
    let Some(url) = redis_url() else { return };
    let store = Arc::new(
        RedisLeaderboard::connect(&url)
            .unwrap()
            .with_prefix(unique("test:lb")),
    );
    let board = Board::high_first("gold").unwrap();
    let projection_board = board.clone();
    let projection = move |event: &DomainEventOccurrence| {
        let body: GoldLooted = event
            .decode_body()
            .map_err(|err| LeaderboardError::invalid(err.to_string()))?;
        Ok(vec![LeaderboardChange::new(
            projection_board.clone(),
            ScoreSubmission::new(body.player, f64::from(body.gold), ScorePolicy::Increment),
        )])
    };
    let projector = LeaderboardProjector::new(Arc::clone(&store), Arc::new(projection));
    let history = vec![
        looted(1, "ann", 10),
        looted(2, "bob", 7),
        looted(3, "ann", 5),
    ];

    for event in &history {
        let message = Message::new(
            event.descriptor().name.to_string(),
            MessageKind::Event,
            event.canonical_bytes().unwrap(),
        )
        .with_id(event.id());
        assert_eq!(projector.apply_message(&message).await.unwrap().applied, 1);
        // Redelivery is a no-op.
        assert_eq!(
            projector.apply_message(&message).await.unwrap().duplicates,
            1
        );
    }
    assert_eq!(store.score(&board, "ann").await.unwrap(), Some(15.0));

    store
        .submit(
            &board,
            &ScoreSubmission::new("drift", 99.0, ScorePolicy::Replace),
        )
        .await
        .unwrap();
    let report = projector
        .rebuild(std::slice::from_ref(&board), &history)
        .await
        .unwrap();
    assert_eq!(report.applied, 3);
    let top = store.top(&board, 10).await.unwrap();
    let rows: Vec<_> = top.iter().map(|e| (e.member.as_str(), e.score)).collect();
    assert_eq!(rows, vec![("ann", 15.0), ("bob", 7.0)]);
    assert_eq!(
        projector.apply_event(&history[2]).await.unwrap().duplicates,
        1
    );
    store.reset(&board).await.unwrap();
}

#[tokio::test]
async fn unreachable_redis_fails_fast_as_retryable() {
    // Port 1 is never a Redis server; construction must not block.
    let store = RedisLeaderboard::connect("redis://127.0.0.1:1").unwrap();
    let board = Board::high_first("gold").unwrap();
    let started = std::time::Instant::now();
    let err = store
        .submit(
            &board,
            &ScoreSubmission::new("p", 1.0, ScorePolicy::Increment),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), LeaderboardErrorKind::Unavailable);
    assert!(err.into_transport_error().is_retryable());
    assert!(started.elapsed() < Duration::from_secs(10));
}

/// The same Redis boards served through GraphQL: roots read the sorted sets
/// and an applied projector change refreshes a `@live` top-N.
#[cfg(feature = "graphql")]
#[tokio::test]
async fn redis_boards_serve_graphql_queries_and_live_updates() {
    use distributed::graphql::{GraphqlEngine, IdentityConfig};
    use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
    use distributed_leaderboard::{LeaderboardGrant, LeaderboardRoots, LeaderboardSurface};
    use futures_util::StreamExt;

    let Some(url) = redis_url() else { return };
    let store = Arc::new(
        RedisLeaderboard::connect(&url)
            .unwrap()
            .with_prefix(unique("gql")),
    );
    let board = Board::high_first("gold").unwrap();
    for (member, score) in [("ann", 30.0), ("bob", 20.0)] {
        store
            .submit(
                &board,
                &ScoreSubmission::new(member, score, ScorePolicy::Replace),
            )
            .await
            .unwrap();
    }
    let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let engine = GraphqlEngine::from_schema_catalog(
        &distributed::ReadModelCatalog::new("gold-service"),
        repo.pool().clone(),
    )
    .unwrap()
    .identity(IdentityConfig::dev_headers())
    .roles(&["user"])
    .external_read_source(Arc::new(LeaderboardRoots::new(
        LeaderboardSurface::single("gold", &board).grant("user", LeaderboardGrant::read()),
        store.clone(),
    )))
    .build()
    .unwrap();
    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    session.set(USER_ID_KEY, "bob");

    let mine = engine
        .execute(
            &session,
            async_graphql::Request::new("{ goldMyRank { rank score } }"),
        )
        .await;
    assert!(mine.errors.is_empty(), "{:?}", mine.errors);
    assert_eq!(
        serde_json::to_value(&mine.data).unwrap()["goldMyRank"],
        serde_json::json!({ "rank": 1, "score": 20.0 })
    );

    let projector = LeaderboardProjector::new(
        store.clone(),
        Arc::new(
            |event: &DomainEventOccurrence| -> Result<Vec<LeaderboardChange>, LeaderboardError> {
                let body: GoldLooted = event
                    .decode_body()
                    .map_err(|error| LeaderboardError::invalid(error.to_string()))?;
                Ok(vec![LeaderboardChange::new(
                    Board::high_first("gold").unwrap(),
                    ScoreSubmission::new(body.player, f64::from(body.gold), ScorePolicy::Increment),
                )])
            },
        ),
    )
    .with_change_listener(distributed_leaderboard::change_listener(&engine));
    let mut live = engine.execute_stream(
        &session,
        async_graphql::Request::new("subscription { goldLeaderboard(first: 1) { member } }"),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first.data).unwrap()["goldLeaderboard"],
        serde_json::json!([{ "member": "ann" }])
    );
    projector.apply_event(&looted(1, "bob", 50)).await.unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&pushed.data).unwrap()["goldLeaderboard"],
        serde_json::json!([{ "member": "bob" }])
    );
}
