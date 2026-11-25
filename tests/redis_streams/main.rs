//! Redis Streams publisher integration tests.
//!
//! Tests that need a server skip (with a logged reason) when `REDIS_URL` is
//! unset; the unavailable-server test always runs.
//!
//!   REDIS_URL=redis://127.0.0.1:6379 cargo test --test redis_streams --features redis-streams
#![cfg(feature = "redis-streams")]

use distributed::bus::{Message, MessageKind, MessagePublisher, RedisStreamPublisher};

use common::env as env_support;

/// `XRANGE` reply row: entry id and field/value pairs.
type StreamEntry = (String, Vec<(String, Vec<u8>)>);

fn redis_url() -> Option<String> {
    env_support::require_service("REDIS_URL", "redis streams test")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
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
    let publisher = RedisStreamPublisher::connect("redis://127.0.0.1:1").unwrap();
    let err = publisher
        .publish(Message::new("dungeon.gold_looted", MessageKind::Event, vec![]).with_id("x"))
        .await
        .unwrap_err();
    assert!(err.is_retryable());
}
