//! Host-level world features without a transport: safe-room version
//! policy, handoff issuance, freeze readiness, tier-up forwarding,
//! hibernate/restore hooks and version stamping.

#[path = "../world/realm.rs"]
mod realm;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::protocol::{error, kick, Notice};
use distributed_game::*;
use realm::*;

fn claims(principal: &str, zone: &str) -> TicketClaims {
    TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: u64::MAX,
        nonce: format!("n-{principal}"),
        role: Role::Player,
        name: None,
        extensions: Default::default(),
        wake: false,
    }
}

fn host(zone: &str, config: HostConfig, clock: &ManualClock, cfg: RealmConfig) -> ZoneHost<Realm> {
    ZoneHost::new(zone, config, Arc::new(clock.clone()), cfg)
}

fn messages(out: &[Outbound], client: ClientId) -> Vec<ServerMessage> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Frame { client: c, frame } if *c == client => {
                ServerMessage::decode(frame, &()).ok()
            }
            _ => None,
        })
        .collect()
}

fn closed(out: &[Outbound], client: ClientId) -> bool {
    out.iter()
        .any(|o| matches!(o, Outbound::Close { client: c } if *c == client))
}

fn safe_room(version: &str) -> HostConfig {
    HostConfig {
        content_version: version.into(),
        stale_policy: StalePolicy::SafeRoom { roam: false },
        ..HostConfig::default()
    }
}

fn transfer(principal: &str, from: &str, to: &str) -> Transfer {
    Transfer {
        claims: claims(principal, to),
        source_zone: from.into(),
        issuer: "h1".into(),
        generation: 1,
        carried: b"gold=7".to_vec(),
        departed_at_ms: 0,
    }
}

#[test]
fn safe_room_policy_admits_stale_clients_only_where_allowed() {
    let clock = ManualClock::new();
    let mut field = host("realm.field", safe_room("v2"), &clock, RealmConfig::new());
    let mut gate = host(
        "realm.inn",
        safe_room("v2"),
        &clock,
        RealmConfig {
            gate: true,
            ..RealmConfig::new()
        },
    );

    // Fresh joins with stale content are told to reload.
    let err = field
        .join(&claims("ana", "realm.field"), 0, "v1")
        .unwrap_err();
    assert_eq!(err.code, error::VERSION_MISMATCH);
    assert!(err.message.contains("reload"));
    // Stale players may not roam into non-gate zones...
    let err = field
        .join_transfer(&transfer("ana", "realm.inn", "realm.field"), 0, "v1")
        .unwrap_err();
    assert_eq!(err.code, error::VERSION_MISMATCH);
    // ...but up-to-date ones may.
    field
        .join_transfer(&transfer("bo", "realm.inn", "realm.field"), 0, "v2")
        .unwrap();

    // Entering a gate: admitted, then Kick(RELOAD), kept for reconnect.
    let joined = gate
        .join_transfer(&transfer("ana", "realm.field", "realm.inn"), 0, "v1")
        .unwrap();
    let out = gate.drain_outbound();
    let msgs = messages(&out, joined.client);
    let ServerMessage::Welcome(welcome) = &msgs[0] else {
        panic!("welcome first: {msgs:?}")
    };
    assert!(matches!(
        msgs.last(),
        Some(ServerMessage::Kick { code, .. }) if *code == kick::RELOAD
    ));
    assert!(closed(&out, joined.client));
    assert_eq!(gate.player_count(), 1, "kept in the reconnect grace window");
    assert_eq!(gate.zone().arrivals.len(), 1);

    // Reconnecting still stale at a gate -> reload again; reloaded -> in.
    let again = gate
        .reconnect_with_version(&welcome.reconnect_token, 0, "v1")
        .unwrap();
    let out = gate.drain_outbound();
    let msgs = messages(&out, again.client);
    let ServerMessage::Welcome(welcome) = &msgs[0] else {
        panic!()
    };
    assert!(closed(&out, again.client));
    let ok = gate
        .reconnect_with_version(&welcome.reconnect_token, 0, "v2")
        .unwrap();
    let out = gate.drain_outbound();
    assert!(!closed(&out, ok.client));
    let Viewer::Player(player) = ok.viewer else {
        panic!()
    };
    assert_eq!(gate.player_content_version(player), Some("v2"));
}

#[test]
fn strict_policy_rejects_stale_reconnects_and_hard_mode_reloads() {
    let clock = ManualClock::new();
    let config = HostConfig {
        content_version: "v1".into(),
        ..HostConfig::default()
    };
    let mut h = host("z", config, &clock, RealmConfig::new());
    let joined = h.join(&claims("ana", "z"), 0, "v1").unwrap();
    let out = h.drain_outbound();
    let ServerMessage::Welcome(welcome) = &messages(&out, joined.client)[0] else {
        panic!()
    };
    h.disconnect(joined.client, false);
    let err = h
        .reconnect_with_version(&welcome.reconnect_token, 0, "v0")
        .unwrap_err();
    assert_eq!(err.code, error::VERSION_MISMATCH);
    let joined = h
        .reconnect_with_version(&welcome.reconnect_token, 0, "v1")
        .unwrap();
    h.drain_outbound();

    // Barrier commit to v2 in hard mode: the v1 client is reloaded.
    h.set_content_version("v2");
    assert_eq!(h.zone().versions, vec![("v1".into(), "v2".into())]);
    assert_eq!(h.reload_stale_players(), 1);
    let out = h.drain_outbound();
    assert!(messages(&out, joined.client)
        .iter()
        .any(|m| matches!(m, ServerMessage::Kick { code, .. } if *code == kick::RELOAD)));
    assert_eq!(h.player_count(), 1);
}

#[test]
fn handoff_issues_ticket_and_removes_player_first() {
    let clock = ManualClock::new();
    let requests: Arc<Mutex<Vec<HandoffRequest>>> = Arc::default();
    let seen = requests.clone();
    let mut h = host(
        "realm.east",
        HostConfig::default(),
        &clock,
        RealmConfig {
            neighbour: Some("realm.west".into()),
            ..RealmConfig::new()
        },
    )
    .with_transfer_issuer(move |r: &HandoffRequest| {
        seen.lock().unwrap().push(r.clone());
        Some("signed-transfer".into())
    });
    let joined = h.join(&claims("ana", "realm.east"), 0, "").unwrap();
    h.drain_outbound();
    h.push_input(joined.client, 1, 0, &[CMD_HANDOFF]);
    clock.advance(Duration::from_millis(40));
    h.advance();
    let out = h.drain_outbound();
    let msgs = messages(&out, joined.client);
    assert!(msgs.contains(&ServerMessage::Handoff {
        target_zone: "realm.west".into(),
        transfer_ticket: "signed-transfer".into(),
    }));
    assert!(closed(&out, joined.client));
    assert_eq!(h.player_count(), 0);
    assert_eq!(h.zone().left[0].1, LeaveReason::Handoff);
    let r = &requests.lock().unwrap()[0];
    assert_eq!(
        (
            r.principal.as_str(),
            r.source_zone.as_str(),
            r.target_zone.as_str()
        ),
        ("ana", "realm.east", "realm.west")
    );
    assert_eq!(r.carried, b"ana:gold=0");

    // Without an issuer the zone gets an error and the player stays.
    let mut lone = host(
        "solo",
        HostConfig::default(),
        &clock,
        RealmConfig {
            neighbour: Some("x".into()),
            ..RealmConfig::new()
        },
    );
    let joined = lone.join(&claims("bo", "solo"), 0, "").unwrap();
    lone.push_input(joined.client, 1, 0, &[CMD_HANDOFF]);
    clock.advance(Duration::from_millis(40));
    lone.advance();
    assert_eq!(lone.zone().handoff_errors, vec![HandoffError::NoIssuer]);
    assert_eq!(lone.player_count(), 1);
}

#[test]
fn freeze_waits_for_readiness_and_counts_dropped_inputs() {
    let clock = ManualClock::new();
    let mut h = host(
        "z",
        HostConfig::default(),
        &clock,
        RealmConfig {
            freeze_ready: false,
            ..RealmConfig::new()
        },
    );
    let joined = h.join(&claims("ana", "z"), 0, "").unwrap();
    let notice = Notice {
        reason: "content_update".into(),
        version: "v2".into(),
        notes: String::new(),
    };
    assert_eq!(h.request_freeze(notice.clone()), FreezeState::Freezing);
    clock.advance(Duration::from_millis(100));
    assert!(h.advance().ticks > 0, "still ticking while not ready");
    h.zone_mut().cfg.freeze_ready = true;
    clock.advance(Duration::from_millis(100));
    h.advance();
    assert_eq!(h.freeze_state(), FreezeState::Frozen);
    let out = h.drain_outbound();
    assert!(messages(&out, joined.client).contains(&ServerMessage::Freeze(notice)));
    let before = h.tick();
    h.push_input(joined.client, 1, 0, &[CMD_COUNT]);
    h.push_input(joined.client, 2, 0, &[CMD_COUNT]);
    clock.advance(Duration::from_millis(100));
    assert_eq!(h.advance().ticks, 0);
    assert_eq!(h.tick(), before);
    assert_eq!(h.inputs_dropped_frozen(), 2);

    let resume = Notice {
        reason: "content_update_aborted".into(),
        version: String::new(),
        notes: String::new(),
    };
    h.cancel_freeze(resume.clone());
    assert_eq!(h.freeze_state(), FreezeState::Running);
    assert!(messages(&h.drain_outbound(), joined.client).contains(&ServerMessage::Resume(resume)));
}

#[test]
fn child_events_are_delivered_later_and_forwarded_up() {
    let clock = ManualClock::new();
    let mut h = host(
        "realm.east",
        HostConfig::default(),
        &clock,
        RealmConfig::new(),
    );
    let event = ChildEvent {
        origin_zone: "realm.east.keep".into(),
        from_zone: "realm.east.keep".into(),
        kind: "boss_defeated".into(),
        payload: b"hydra".to_vec(),
        origin_tick: 9,
        content_version: String::new(),
        levels: 3,
    };
    h.deliver_child_event(event.clone());
    assert!(h.zone().child_events.is_empty(), "not synchronous");
    clock.advance(Duration::from_millis(40));
    h.advance();
    assert_eq!(h.zone().child_events, vec![event.clone()]);
    let up = h.drain_upward();
    assert_eq!(up.len(), 1);
    assert_eq!(up[0].from_zone, "realm.east");
    assert_eq!(up[0].origin_zone, "realm.east.keep");
    assert_eq!(up[0].levels, 2);

    // A zone's own upward event.
    let joined = h.join(&claims("ana", "realm.east"), 0, "").unwrap();
    h.push_input(joined.client, 1, 0, &[CMD_BOSS]);
    clock.advance(Duration::from_millis(40));
    h.advance();
    let up = h.drain_upward();
    assert_eq!(up[0].origin_zone, "realm.east");
    assert_eq!(up[0].levels, ChildEvent::TO_ROOT);
}

#[test]
fn hibernate_restore_and_version_stamps() {
    let clock = ManualClock::new();
    let (queue, _feed) = fact_channel(16, RetryPolicy::default());
    let mut h = host(
        "z",
        HostConfig {
            content_version: "v1".into(),
            ..HostConfig::default()
        },
        &clock,
        RealmConfig::new(),
    )
    .with_facts(queue);
    let joined = h.join(&claims("ana", "z"), 0, "v1").unwrap();
    h.push_input(joined.client, 1, 0, &[CMD_COUNT]);
    clock.advance(Duration::from_millis(40));
    h.advance();
    let saved = h.hibernate_state().unwrap();

    let mut restored = host("z", HostConfig::default(), &clock, RealmConfig::new());
    restored.restore(&saved, "v1");
    assert_eq!(restored.zone().counter, 1);
    assert_eq!(restored.zone().restored_from.as_deref(), Some("v1"));

    // Facts emitted after a version switch carry the new version.
    let (queue, feed) = fact_channel(16, RetryPolicy::default());
    let sink = MemoryFactSink::new();
    let mut h = host(
        "z",
        HostConfig {
            content_version: "v1".into(),
            ..HostConfig::default()
        },
        &clock,
        RealmConfig::new(),
    )
    .with_facts(queue);
    let joined = h.join(&claims("ana", "z"), 0, "v1").unwrap();
    h.set_content_version("v2");
    h.push_input(joined.client, 1, 0, &[CMD_COUNT]);
    clock.advance(Duration::from_millis(40));
    h.advance();
    drop(h);
    futures_executor(feed.run(sink.clone(), |_| async {}));
    let committed = sink.committed();
    assert_eq!(committed[0].stamp().content_version, "v2");
}

/// Minimal executor for the fact feed (no runtime needed: the memory sink
/// never pends).
fn futures_executor(f: impl std::future::Future<Output = ()>) {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut f = std::pin::pin!(f);
    while f.as_mut().poll(&mut cx).is_pending() {}
}

#[test]
fn world_entry_points_record_and_replay_deterministically() {
    use distributed_game::replay::{replay, Recorder, Recording};
    let clock = ManualClock::new();
    let cfg = RealmConfig {
        neighbour: Some("realm.west".into()),
        ..RealmConfig::new()
    };
    let mut rec = Recorder::<Realm>::new(
        "realm.east",
        safe_room("v1"),
        Arc::new(clock.clone()),
        cfg.clone(),
        Vec::new(),
    )
    .with_transfer_issuer(|_| Some("live-ticket".into()));
    rec.restore(&5u32.to_le_bytes(), "v0");
    let ana = rec
        .join_transfer(&transfer("ana", "realm.inn", "realm.east"), 0, "v1")
        .unwrap();
    let bo = rec.join(&claims("bo", "realm.east"), 0, "v1").unwrap();
    rec.push_input(bo.client, 1, 0, &[CMD_COUNT]);
    clock.advance(Duration::from_millis(40));
    rec.advance();
    rec.deliver_child_event(ChildEvent {
        origin_zone: "realm.east.keep".into(),
        from_zone: "realm.east.keep".into(),
        kind: "boss_defeated".into(),
        payload: vec![1],
        origin_tick: 3,
        content_version: "v1".into(),
        levels: 1,
    });
    let notice = Notice {
        reason: "content_update".into(),
        version: "v2".into(),
        notes: String::new(),
    };
    rec.request_freeze(notice.clone());
    rec.push_input(ana.client, 1, 0, &[CMD_COUNT]);
    clock.advance(Duration::from_millis(40));
    rec.advance();
    rec.set_content_version("v2");
    rec.set_config_version("combat@2");
    rec.resume(notice);
    rec.reload_stale_players();
    let out = rec.drain_outbound();
    let ServerMessage::Welcome(welcome) = &messages(&out, bo.client)[0] else {
        panic!()
    };
    let token = welcome.reconnect_token.clone();
    let bo = rec.reconnect_with_version(&token, 0, "v2").unwrap();
    rec.push_input(bo.client, 2, 0, &[CMD_HANDOFF]);
    clock.advance(Duration::from_millis(40));
    rec.advance();
    assert_eq!(rec.host().zone().counter, 6);
    assert_eq!(rec.host().config().config_version, "combat@2");
    assert_eq!(rec.host().zone().child_events.len(), 1);
    assert_eq!(rec.host().player_count(), 1, "bo handed off");

    let recording = Recording::decode(&rec.finish().encode()).unwrap();
    let summary = replay::<Realm>(&recording, cfg).unwrap();
    assert!(summary.ticks >= 2, "{summary:?}");
}

#[test]
fn handoff_target_stamps_after_source_departure_despite_clock_skew() {
    // One virtual time, two hosts whose wall clocks disagree: the target's
    // reads 9 000 s *behind* the source's.
    let time = ManualClock::new();
    let source_clock = time.clone().with_unix_origin_ms(10_000_000);
    let target_clock = time.clone().with_unix_origin_ms(1_000_000);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let (source_queue, source_feed) = fact_channel(16, RetryPolicy::default());
    let mut source = ZoneHost::<Realm>::new(
        "realm.east",
        HostConfig {
            boot_epoch: 3,
            config_version: "combat@12".into(),
            ..HostConfig::default()
        },
        Arc::new(source_clock),
        RealmConfig {
            neighbour: Some("realm.west".into()),
            move_facts: true,
            ..RealmConfig::new()
        },
    )
    .with_facts(source_queue)
    .with_transfer_issuer(move |r: &HandoffRequest| {
        seen.lock().unwrap().push(r.clone());
        Some("signed".into())
    });
    let (target_queue, target_feed) = fact_channel(16, RetryPolicy::default());
    let mut target = ZoneHost::<Realm>::new(
        "realm.west",
        HostConfig {
            boot_epoch: 8,
            ..HostConfig::default()
        },
        Arc::new(target_clock),
        RealmConfig {
            move_facts: true,
            ..RealmConfig::new()
        },
    )
    .with_facts(target_queue);

    let ana = source.join(&claims("ana", "realm.east"), 0, "").unwrap();
    source.push_input(ana.client, 1, 0, &[CMD_HANDOFF]);
    time.advance(Duration::from_millis(40));
    source.advance();
    let request = requests.lock().unwrap().pop().expect("handoff issued");
    // The ManualClock decides the stamp time: origin + virtual time.
    assert_eq!(request.departed_at_ms, 10_000_040);

    // The source's leave runs later on its clock, still stamped with the
    // departure time the ticket carries.
    time.advance(Duration::from_millis(5));
    let transfer = Transfer {
        claims: claims("ana", "realm.west"),
        source_zone: request.source_zone.clone(),
        issuer: "h1".into(),
        generation: 3,
        carried: request.carried.clone(),
        departed_at_ms: request.departed_at_ms,
    };
    target.join_transfer(&transfer, 0, "").unwrap();
    drop(source);
    drop(target);

    // The target commits first.
    let sink = MemoryFactSink::new();
    futures_executor(target_feed.run(sink.clone(), |_| async {}));
    futures_executor(source_feed.run(sink.clone(), |_| async {}));
    let committed = sink.committed();
    let commands: Vec<_> = committed.iter().map(Fact::command).collect();
    assert_eq!(commands, ["realm.arrived", "realm.left"]);
    let (arrived, left) = (committed[0].stamp(), committed[1].stamp());

    assert_eq!(left.emitted_at_ms, 10_000_040);
    assert_eq!(
        (left.activation, left.config_version.as_str()),
        (3, "combat@12")
    );
    // The target's own clock says 1 000 045; the stamp starts after the
    // departure instead.
    assert_eq!(arrived.emitted_at_ms, 10_000_041);
    assert_eq!(arrived.activation, 8);
    assert!(arrived.emitted_after(left));
    let mut ordered = committed.clone();
    ordered.sort_by(|a, b| a.stamp().emission_order(b.stamp()));
    let ordered: Vec<_> = ordered.iter().map(Fact::command).collect();
    assert_eq!(ordered, ["realm.left", "realm.arrived"]);
}

#[test]
fn stamps_follow_the_manual_clock_and_config_version() {
    let clock = ManualClock::new().with_unix_origin_ms(500);
    let h = host(
        "z",
        HostConfig {
            boot_epoch: 42,
            ..HostConfig::default()
        },
        &clock,
        RealmConfig::new(),
    );
    clock.advance(Duration::from_millis(250));
    let (queue, feed) = fact_channel(16, RetryPolicy::default());
    let mut h = h.with_facts(queue);
    let joined = h.join(&claims("bo", "z"), 0, "").unwrap();
    h.push_input(joined.client, 1, 0, &[CMD_COUNT]);
    h.set_config_version("combat@13");
    clock.advance(Duration::from_millis(40));
    h.advance();
    drop(h);
    let sink = MemoryFactSink::new();
    futures_executor(feed.run(sink.clone(), |_| async {}));
    let stamp = sink.committed()[0].stamp().clone();
    assert_eq!(stamp.emitted_at_ms, 790);
    assert_eq!(stamp.activation, 42);
    assert_eq!(stamp.config_version, "combat@13");
    assert_eq!(stamp.zone_id, "z");
}
