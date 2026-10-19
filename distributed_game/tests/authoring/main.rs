//! Authoring derives: `Replicated`, `GameInput`, `GameEvent`, `GameRecord`,
//! `FactCommand`, the `GameSurface` they describe, and the typed `Ctx`
//! helpers.
//!
//! `fixtures/authoring.json` (sample surface + encoded samples) is the Rust
//! side of the generated-client round trip: `distributed_cli` generates
//! TypeScript from its surface (golden `js/tests/fixtures/game-generated/`)
//! and `js/tests/game-generated.test.mjs` encodes/decodes the samples with
//! it. Regenerate with `UPDATE_GOLDEN=1 cargo test -p distributed_game --test authoring`.

use std::sync::Arc;
use std::time::Duration;

use distributed_game::protocol::{Reader, Writer};
use distributed_game::surface::{EventPayload, GameSurface};
use distributed_game::{
    fact_channel, schema, Correction, Ctx, EntitySchema, FactCommand, FactOutcome, FactSettlement,
    FieldType, FieldValue, GameEvent, GameInput, GameRecord, HostConfig, ManualClock,
    MemoryFactSink, PlayerId, Replicated, RetryPolicy, Role, TicketClaims, Wire, WireType,
    WorldView, Zone, ZoneHost,
};
use serde::Serialize;
use serde_json::{json, Value};

// ---- sample surface -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0)]
pub struct PlayerView {
    #[quant(centered = 20.0, bits = 18)]
    #[correct(ease = 150)]
    pub x: f32,
    #[quant(centered = 20.0, bits = 18)]
    #[correct(ease = 150)]
    pub y: f32,
    #[quant(min = -16.0, max = 16.0, bits = 12)]
    #[correct(snap)]
    pub vx: f32,
    #[angle(bits = 8)]
    #[correct(replace)]
    pub face: f32,
    #[priority(0.2)]
    #[correct(replace)]
    pub name: String,
    pub hp: u8,
    pub level: u16,
    pub dead: bool,
    pub score: i32,
    pub raw: f32,
    pub blob: Vec<u8>,
    pub kills: u32,
}

const POS: FieldType = FieldType::quant_centered(64.0, 16);

#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "marker", id = 2, x = px, y = py)]
pub struct MarkerView {
    #[field_type(POS)]
    pub px: f32,
    #[field_type(POS)]
    pub py: f32,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "stats", id = 4, global)]
pub struct StatsView {
    pub slow_ms: u16,
    pub tick_ms: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, GameInput)]
pub struct SampleInput {
    #[range(min = -127, max = 127)]
    pub mx: i8,
    #[range(min = -127, max = 127)]
    pub my: i8,
    #[buttons(dash, attack)]
    pub buttons: u8,
    #[angle(bits = 10)]
    pub aim: f32,
    #[range(min = 0.0, max = 1.0)]
    pub power: f32,
    pub view_tick: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
pub struct Row {
    pub rank: u16,
    pub name: String,
    pub score: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
pub struct Board {
    pub version: u32,
    pub kills: Vec<Row>,
    pub note: Option<String>,
    #[long]
    pub motd: String,
}

#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum SampleEvent {
    #[game_event(id = 1)]
    Hello {
        entity: u32,
        view_radius: f32,
        #[quant(min = 0.0, max = 64.0, bits = 10)]
        half: f32,
    },
    #[game_event(id = 2)]
    Board(Board),
    #[game_event(id = 3, name = "respawned")]
    Respawn,
    #[game_event(id = 4)]
    Shot {
        hit: bool,
        target: Option<u32>,
        offset: i16,
        small: i8,
        at_ms: f64,
        tags: Vec<String>,
        bytes: Vec<u8>,
        total: u64,
    },
}

#[derive(Debug, Clone, PartialEq, GameEvent)]
#[game_event(id = 9)]
pub struct Ping {
    pub seq: u32,
}

/// The cell command name, as the cell would mount it.
pub const RECORD_KILL: &str = "sample.record_kill";

#[derive(Debug, Clone, PartialEq, Serialize, FactCommand)]
#[fact(command = RECORD_KILL, key(mob, generation))]
pub struct RecordKill {
    pub mob: u32,
    pub generation: u32,
    pub tier: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, FactCommand)]
#[fact(command = "sample.tick_note")]
pub struct TickNote {
    pub note: String,
}

pub fn sample_surface() -> GameSurface {
    GameSurface::for_zone::<Sample>("sample")
        .events::<SampleEvent>()
        .events::<Ping>()
}

// ---- a zone using all of it ----------------------------------------------

#[derive(Default)]
pub struct Sample {
    players: Vec<PlayerId>,
    pub settled: Vec<FactSettlement>,
    pub keys: Vec<String>,
    pub last_input: Option<SampleInput>,
    kills: u32,
}

impl Zone for Sample {
    type Config = ();
    type Input = SampleInput;
    type Timer = ();

    fn schema() -> EntitySchema {
        schema![PlayerView, MarkerView, StatsView]
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
        Self::default()
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        self.players.push(player);
        ctx.send(
            player,
            &SampleEvent::Hello {
                entity: 1000 + player.0,
                view_radius: 14.0,
                half: 20.0,
            },
        );
        ctx.broadcast(&Ping { seq: player.0 });
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<SampleInput> {
        SampleInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        for player in self.players.clone() {
            for input in ctx.take_inputs(player, distributed_game::ConsumePolicy::Drain) {
                self.last_input = Some(input.input);
                if input.input.buttons & SampleInput::ATTACK != 0 {
                    self.kills += 1;
                    let key = ctx
                        .emit(
                            player,
                            RecordKill {
                                mob: 7,
                                generation: self.kills,
                                tier: 2,
                            },
                        )
                        .unwrap();
                    self.keys.push(key);
                    // Two keyless facts in one tick get distinct keys.
                    self.keys
                        .push(ctx.emit_zone(TickNote { note: "a".into() }).unwrap());
                    self.keys
                        .push(ctx.emit_zone(TickNote { note: "b".into() }).unwrap());
                }
            }
        }
    }

    fn on_fact_settled(&mut self, _ctx: &mut Ctx<'_, Self>, settlement: FactSettlement) {
        self.settled.push(settlement);
    }

    fn replicate(&self, out: &mut WorldView) {
        for (i, player) in self.players.iter().enumerate() {
            out.add(1000 + player.0, &sample_player(i as f32));
        }
        out.add(
            50,
            &MarkerView {
                px: 3.0,
                py: -2.0,
                label: "spawn".into(),
            },
        )
        .priority(0.5);
        out.add(
            900,
            &StatsView {
                slow_ms: 12,
                tick_ms: 33.3,
            },
        );
    }
}

fn sample_player(offset: f32) -> PlayerView {
    PlayerView {
        x: 1.25 + offset,
        y: -3.5,
        vx: 0.5,
        face: 1.0,
        name: "Ada".into(),
        hp: 90,
        level: 3,
        dead: false,
        score: -12,
        raw: 0.1,
        blob: vec![1, 2, 3],
        kills: 70_000,
    }
}

// ---- derive behaviour -----------------------------------------------------

#[test]
fn replicated_kind_def_and_values_share_one_field_order() {
    let kind = PlayerView::kind_def();
    assert_eq!(
        (kind.id, kind.name.as_str(), kind.priority),
        (1, "player", 2.0)
    );
    let names: Vec<_> = kind.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        ["x", "y", "vx", "face", "name", "hp", "level", "dead", "score", "raw", "blob", "kills"]
    );
    assert_eq!(kind.fields[0].ty, FieldType::quant_centered(20.0, 18));
    assert_eq!(
        kind.fields[0].correction,
        Some(Correction::Ease { duration_ms: 150 })
    );
    assert_eq!(kind.fields[2].correction, Some(Correction::Snap));
    assert_eq!(kind.fields[3].ty, FieldType::Angle { bits: 8 });
    assert_eq!(kind.fields[4].priority, 0.2);
    assert_eq!(kind.fields[5].ty, FieldType::U8);
    assert_eq!(kind.fields[6].ty, FieldType::U16);
    assert_eq!(kind.fields[10].ty, FieldType::Bytes);
    let view = sample_player(0.0);
    let values = view.field_values();
    assert_eq!(values.len(), kind.fields.len());
    assert_eq!(values[4], FieldValue::Str("Ada".into()));
    assert_eq!(values[8], FieldValue::I(-12));
    assert_eq!(view.position(), (1.25, -3.5));
    const { assert!(!PlayerView::GLOBAL) };
    const { assert!(StatsView::GLOBAL) };
    assert_eq!(
        MarkerView {
            px: 3.0,
            py: 4.0,
            label: String::new()
        }
        .position(),
        (3.0, 4.0)
    );
    assert_eq!(MarkerView::kind_def().fields[0].ty, POS);

    let schema = Sample::schema();
    assert_eq!(schema.kinds().len(), 3);
    let mut view = WorldView::default();
    view.add(
        5,
        &StatsView {
            slow_ms: 1,
            tick_ms: 2.0,
        },
    )
    .local()
    .priority(3.0);
    view.add(6, &sample_player(0.0)).global();
    assert!(!view.entities()[0].global);
    assert_eq!(view.entities()[0].priority, 3.0);
    assert!(view.entities()[1].global);
    assert_eq!(view.entities()[1].kind, PlayerView::KIND_ID);
}

#[test]
fn game_input_codec_and_sanitize() {
    let input = SampleInput {
        mx: -128,
        my: 64,
        buttons: 0b1111,
        aim: 1.0,
        power: 2.0,
        view_tick: 300,
    };
    let bytes = input.to_payload();
    // i8, i8, u8, angle varuint (10 bits), f32, varuint.
    assert_eq!(bytes[..3], [0x80, 0x40, 0x0f]);
    let decoded = SampleInput::from_payload(&bytes).expect("valid");
    assert_eq!(decoded.mx, -127, "range clamps");
    assert_eq!(
        decoded.buttons,
        SampleInput::DASH | SampleInput::ATTACK,
        "unknown bits cleared"
    );
    assert_eq!(decoded.power, 1.0);
    assert_eq!((SampleInput::DASH, SampleInput::ATTACK), (1, 2));
    let mut long = bytes.clone();
    long.push(0);
    assert_eq!(
        SampleInput::from_payload(&long),
        None,
        "trailing bytes reject"
    );
    assert_eq!(
        SampleInput::from_payload(&bytes[..4]),
        None,
        "short frames reject"
    );
    let nan = SampleInput {
        power: f32::NAN,
        ..input
    };
    assert_eq!(
        SampleInput::from_payload(&nan.to_payload()),
        None,
        "non-finite floats reject"
    );
}

#[test]
fn events_have_stable_ids_and_round_trip() {
    let events = sample_events();
    for event in &events {
        let payload = event.to_payload();
        let back = SampleEvent::decode_event(event.event_id(), &payload).unwrap();
        assert_eq!(back.as_ref(), Some(event));
        assert_eq!(
            Ping::decode_event(event.event_id(), &payload).unwrap(),
            None
        );
    }
    assert_eq!(SampleEvent::HELLO, 1);
    assert_eq!(SampleEvent::RESPAWN, 3);
    assert_eq!(Ping::EVENT_ID, 9);
    assert_eq!(SampleEvent::decode_event(77, &[]).unwrap(), None);
    assert!(SampleEvent::decode_event(1, &[1]).is_err());
    let ping = Ping { seq: 5 };
    assert_eq!(
        Ping::decode_event(9, &ping.to_payload()).unwrap(),
        Some(ping)
    );
}

#[test]
fn records_round_trip_through_the_value_codec() {
    let board = sample_board();
    let mut w = Writer::new();
    board.encode(&mut w);
    let mut r = Reader::new(w.as_bytes());
    assert_eq!(Board::decode(&mut r).unwrap(), board);
    r.finish().unwrap();
    assert_eq!(
        Board::wire_type(),
        WireType::Record {
            name: "Board".into()
        }
    );
}

#[test]
fn surface_describes_everything_once() {
    let surface = sample_surface();
    assert_eq!(surface.schema_version, 1);
    assert_eq!(surface.kinds.len(), 3);
    assert_eq!(
        surface.kinds[0].fields[0].correction,
        Some(Correction::Ease { duration_ms: 150 })
    );
    let input = surface.input.as_ref().expect("typed input");
    assert_eq!(input.name, "SampleInput");
    assert_eq!(input.fields[0].range, Some([-127.0, 127.0]));
    assert_eq!(input.fields[2].buttons, ["dash", "attack"]);
    let ids: Vec<_> = surface
        .events
        .iter()
        .map(|e| (e.id, e.name.as_str()))
        .collect();
    assert_eq!(
        ids,
        [
            (1, "hello"),
            (2, "board"),
            (3, "respawned"),
            (4, "shot"),
            (9, "ping")
        ]
    );
    assert_eq!(
        surface.events[1].payload,
        EventPayload::Value {
            ty: WireType::Record {
                name: "Board".into()
            }
        }
    );
    let records: Vec<_> = surface.records.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(records, ["Board", "Row"]);
    // JSON round trip (the CLI reads this shape).
    let json = surface.to_json();
    let back: GameSurface = serde_json::from_str(&json).unwrap();
    assert_eq!(back, surface);
}

#[test]
#[should_panic(expected = "duplicate event id")]
fn surface_rejects_duplicate_event_ids_across_types() {
    #[derive(GameEvent)]
    #[game_event(id = 1)]
    struct Clash {}
    let _ = sample_surface().events::<Clash>();
}

// ---- typed facts through a real host -------------------------------------

fn claims(principal: &str) -> TicketClaims {
    TicketClaims::new(
        principal,
        "z1",
        u64::MAX,
        format!("n-{principal}"),
        Role::Player,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_facts_bind_the_principal_and_derive_restart_safe_keys() {
    let sink = MemoryFactSink::new();
    let (queue, feed) = fact_channel(64, RetryPolicy::default());
    tokio::spawn(feed.run(sink.clone(), tokio::time::sleep));
    let clock = ManualClock::new();
    let config = HostConfig {
        boot_epoch: 42,
        ..HostConfig::default()
    };
    let mut host =
        ZoneHost::<Sample>::new("z1", config, Arc::new(clock.clone()), ()).with_facts(queue);
    let joined = host.join(&claims("alice"), 0, "").unwrap();
    let attack = SampleInput {
        mx: 0,
        my: 0,
        buttons: SampleInput::ATTACK,
        aim: 0.0,
        power: 0.5,
        view_tick: 0,
    };
    host.push_input(joined.client, 1, 0, &attack.to_payload());
    clock.advance(Duration::from_millis(40));
    host.advance();
    let keys = host.zone().keys.clone();
    assert_eq!(keys[0], "sample.record_kill:z1:42:7:1");
    assert!(
        keys[1].starts_with("sample.tick_note:z1:42:t"),
        "{}",
        keys[1]
    );
    assert_ne!(keys[1], keys[2], "keyless facts are numbered within a tick");

    for _ in 0..200 {
        if sink.committed().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let committed = sink.committed();
    let kill = committed
        .iter()
        .find(|f| f.is::<RecordKill>())
        .expect("kill committed");
    assert_eq!(
        kill.principal(),
        Some("alice"),
        "principal from the verified ticket"
    );
    assert_eq!(
        kill.input(),
        &json!({ "mob": 7, "generation": 1, "tier": 2 })
    );
    let note = committed
        .iter()
        .find(|f| f.is::<TickNote>())
        .expect("zone fact");
    assert_eq!(note.principal(), None);

    for _ in 0..50 {
        clock.advance(Duration::from_millis(40));
        host.advance();
        if host.zone().settled.len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let settled = &host.zone().settled;
    assert!(settled.iter().any(|s| s.is::<RecordKill>()));
    assert!(settled
        .iter()
        .all(|s| matches!(s.outcome, FactOutcome::Committed(_))));
    assert_eq!(RecordKill::COMMAND, RECORD_KILL);
}

// ---- golden fixture for the generated TypeScript --------------------------

fn sample_board() -> Board {
    Board {
        version: 7,
        kills: vec![
            Row {
                rank: 1,
                name: "Ada".into(),
                score: 9_007_199_254_740_991,
            },
            Row {
                rank: 2,
                name: "Bob ✓".into(),
                score: 3,
            },
        ],
        note: None,
        motd: "x".repeat(300),
    }
}

fn sample_events() -> Vec<SampleEvent> {
    vec![
        SampleEvent::Hello {
            entity: 1001,
            view_radius: 14.0,
            half: 64.0,
        },
        SampleEvent::Board(sample_board()),
        SampleEvent::Respawn,
        SampleEvent::Shot {
            hit: true,
            target: Some(42),
            offset: -300,
            small: -5,
            at_ms: 1234.5,
            tags: vec!["a".into(), "".into()],
            bytes: vec![0, 255],
            total: 1 << 40,
        },
        SampleEvent::Shot {
            hit: false,
            target: None,
            offset: 0,
            small: 0,
            at_ms: 0.0,
            tags: vec![],
            bytes: vec![],
            total: 0,
        },
    ]
}

/// The TypeScript-side value of each sample (camelCase keys).
fn sample_event_values() -> Vec<Value> {
    let board = json!({
        "version": 7,
        "kills": [
            {"rank": 1, "name": "Ada", "score": 9_007_199_254_740_991u64},
            {"rank": 2, "name": "Bob ✓", "score": 3}
        ],
        "note": null,
        "motd": "x".repeat(300),
    });
    vec![
        json!({"entity": 1001, "viewRadius": 14.0, "half": 64.0}),
        board,
        json!({}),
        json!({"hit": true, "target": 42, "offset": -300, "small": -5, "atMs": 1234.5, "tags": ["a", ""], "bytes": [0, 255], "total": 1u64 << 40}),
        json!({"hit": false, "target": null, "offset": 0, "small": 0, "atMs": 0.0, "tags": [], "bytes": [], "total": 0}),
    ]
}

fn f32_of(ty: FieldType, v: f32) -> f64 {
    match ty.canonicalize(&FieldValue::F(v)) {
        FieldValue::F(v) => f64::from(v),
        _ => unreachable!(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn golden_fixture_is_current() {
    let input = SampleInput {
        mx: -128,
        my: 100,
        buttons: 0b111,
        aim: -1.0,
        power: 0.25,
        view_tick: 123_456,
    };
    let sanitized = SampleInput::from_payload(&input.to_payload()).unwrap();
    let aim_wire = f32_of(FieldType::Angle { bits: 10 }, -1.0);
    let events: Vec<Value> = sample_events()
        .iter()
        .zip(sample_event_values())
        .map(|(event, value)| {
            let name = sample_surface()
                .events
                .into_iter()
                .find(|e| e.id == event.event_id())
                .unwrap()
                .name;
            json!({ "id": event.event_id(), "name": name, "value": value, "hex": hex(&event.to_payload()) })
        })
        .collect();
    let fixture = json!({
        // Through `to_json` so floats keep their shortest binary32 form.
        "surface": serde_json::from_str::<Value>(&sample_surface().to_json()).unwrap(),
        "input": {
            "value": {"mx": -128, "my": 100, "buttons": 7, "aim": -1.0, "power": 0.25, "viewTick": 123_456},
            "hex": hex(&input.to_payload()),
            "decoded": {"mx": -128, "my": 100, "buttons": 7, "aim": aim_wire, "power": 0.25, "viewTick": 123_456},
            "sanitized": {"mx": sanitized.mx, "my": sanitized.my, "buttons": sanitized.buttons, "aim": aim_wire, "power": 0.25, "viewTick": 123_456},
        },
        "events": events,
    });
    let text = serde_json::to_string_pretty(&fixture).unwrap() + "\n";
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/authoring.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(path, &text).unwrap();
    }
    let current = std::fs::read_to_string(path).unwrap_or_default();
    assert_eq!(
        current, text,
        "run with UPDATE_GOLDEN=1 to regenerate {path}"
    );
}
