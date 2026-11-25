//! Durable timers: `OutboxMessage::deliver_at`. Every outbox store honouring
//! the schedule, and a schedule surviving a restart, are repository
//! conformance cases (`outbox_honours_deliver_at`,
//! `scheduled_message_survives_restart_and_fires_when_due`).

use std::time::{Duration, SystemTime};

use distributed::OutboxMessage;

#[test]
fn deliver_at_round_trips_through_serde_and_defaults_to_none() {
    let message = OutboxMessage::create("m", "T", vec![])
        .unwrap()
        .deliver_after(SystemTime::now(), Duration::from_secs(1));
    let json = serde_json::to_value(&message).unwrap();
    let back: OutboxMessage = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back.deliver_at, message.deliver_at);
    let mut legacy = json;
    legacy.as_object_mut().unwrap().remove("deliver_at");
    let old: OutboxMessage = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        old.deliver_at, None,
        "rows without the field stay immediate"
    );
}
