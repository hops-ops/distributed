//! Redis leaderboard store and Redis Streams publisher integration tests.
//!
//! Tests that need a server skip (with a logged reason) when `REDIS_URL` is
//! unset; the unavailable-server test always runs.
//!
//!   REDIS_URL=redis://127.0.0.1:6379 cargo test --test redis_leaderboard --features redis
#![cfg(feature = "redis")]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use distributed::bus::{Message, MessageKind, MessagePublisher, RedisStreamPublisher};
use distributed::leaderboard::{
    conformance, Board, LeaderboardChange, LeaderboardError, LeaderboardErrorKind,
    LeaderboardProjector, LeaderboardStore, RedisLeaderboard, ScorePolicy, ScoreSubmission,
};
use distributed::DomainEventOccurrence;

#[path = "../support/env.rs"]
mod env_support;

/// `XRANGE` reply row: entry id and field/value pairs.
type StreamEntry = (String, Vec<(String, Vec<u8>)>);

fn redis_url() -> Option<String> {
    env_support::broker_env("REDIS_URL", "redis leaderboard test")
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
async fn redis_stream_publisher_appends_and_dedupes_by_message_id() {
    let Some(url) = redis_url() else { return };
    let stream = unique("test:stream");
    let publisher = RedisStreamPublisher::connect(&url)
        .unwrap()
        .with_stream(stream.clone());
    let message = Message::new(
        "dungeon.gold_looted",
        MessageKind::Event,
        b"\x00bin".to_vec(),
    )
    .with_id("msg-1")
    .with_metadata("correlation_id", "corr-1");
    assert!(publisher.publish_entry(&message).await.unwrap());
    assert!(!publisher.publish_entry(&message).await.unwrap());
    publisher.publish(message.clone()).await.unwrap();
    publisher
        .publish(Message::new(
            "dungeon.gold_looted",
            MessageKind::Event,
            vec![],
        ))
        .await
        .unwrap();

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let len: u64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 2, "the duplicate id must not append a second entry");
    let entries: Vec<StreamEntry> = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let fields: std::collections::HashMap<_, _> = entries[0].1.iter().cloned().collect();
    assert_eq!(fields["id"], b"msg-1");
    assert_eq!(fields["kind"], b"event");
    assert_eq!(fields["payload"], b"\x00bin");
    assert_eq!(fields["meta:correlation_id"], b"corr-1");
    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .arg(RedisStreamPublisher::dedupe_key(&stream, "msg-1"))
        .query_async(&mut conn)
        .await
        .unwrap();
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

    let publisher = RedisStreamPublisher::connect("redis://127.0.0.1:1").unwrap();
    let err = publisher
        .publish(Message::new("dungeon.gold_looted", MessageKind::Event, vec![]).with_id("x"))
        .await
        .unwrap_err();
    assert!(err.is_retryable());
}
