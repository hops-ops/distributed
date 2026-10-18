//! Golden byte fixtures for independent (e.g. JavaScript) implementations.
//!
//! Writes `tests/fixtures/*.json` when missing or when `UPDATE_GOLDEN=1`;
//! otherwise asserts the encoder still produces exactly the committed bytes
//! and that every fixture decodes back to its structure. JSON conventions are
//! documented in `PROTOCOL.md` §9.

use std::collections::BTreeMap;
use std::path::PathBuf;

use distributed_game::protocol::{
    error, kick, ClientMessage, Delta, EntityCreate, EntityUpdate, Hello, Notice, Reader,
    ServerMessage, Welcome, Writer,
};
use distributed_game::{EntitySchema, FieldType, FieldValue, KindDef, Role, PROTOCOL_VERSION};
use serde_json::{json, Value};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn f32j(v: f32) -> Value {
    json!({ "f32": f64::from(v), "bits": format!("{:08x}", v.to_bits()) })
}

fn field_type_json(ty: &FieldType) -> Value {
    match ty {
        FieldType::Bool => json!({ "type": "bool" }),
        FieldType::U8 => json!({ "type": "u8" }),
        FieldType::U16 => json!({ "type": "u16" }),
        FieldType::U32 => json!({ "type": "u32" }),
        FieldType::I32 => json!({ "type": "i32" }),
        FieldType::F32 => json!({ "type": "f32" }),
        FieldType::QuantF32 { min, max, bits } => {
            json!({ "type": "quant_f32", "min": f32j(*min), "max": f32j(*max), "bits": bits })
        }
        FieldType::Angle { bits } => json!({ "type": "angle", "bits": bits }),
        FieldType::ShortStr => json!({ "type": "short_str" }),
        FieldType::Bytes => json!({ "type": "bytes" }),
    }
}

fn schema_json(schema: &EntitySchema) -> Value {
    Value::Array(
        schema
            .kinds()
            .iter()
            .map(|k| {
                json!({
                    "id": k.id,
                    "name": k.name,
                    "fields": k.fields.iter().map(|f| {
                        let mut v = field_type_json(&f.ty);
                        v["name"] = json!(f.name);
                        v
                    }).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn value_json(v: &FieldValue) -> Value {
    match v {
        FieldValue::Bool(b) => json!({ "bool": b }),
        FieldValue::U(u) => json!({ "u": u }),
        FieldValue::I(i) => json!({ "i": i }),
        FieldValue::F(f) => f32j(*f),
        FieldValue::Str(s) => json!({ "str": s }),
        FieldValue::Bytes(b) => json!({ "bytes": hex(b) }),
    }
}

fn client_json(m: &ClientMessage) -> Value {
    match m {
        ClientMessage::Hello(h) => json!({
            "type": "Hello",
            "protocol_version": h.protocol_version,
            "content_version": h.content_version,
            "capabilities": h.capabilities,
            "ticket": h.ticket,
            "reconnect_token": h.reconnect_token,
        }),
        ClientMessage::Input {
            seq,
            client_tick,
            payload,
        } => {
            json!({ "type": "Input", "seq": seq, "client_tick": client_tick, "payload": hex(payload) })
        }
        ClientMessage::Ping { client_time } => {
            json!({ "type": "Ping", "client_time": client_time })
        }
        ClientMessage::Leave => json!({ "type": "Leave" }),
    }
}

fn server_json(m: &ServerMessage) -> Value {
    match m {
        ServerMessage::Welcome(w) => json!({
            "type": "Welcome",
            "session_id": w.session_id,
            "zone_id": w.zone_id,
            "role": match w.role { Role::Player => "player", Role::Observer => "observer" },
            "tick_rate": w.tick_rate,
            "patch_rate": w.patch_rate,
            "server_tick": w.server_tick,
            "server_time": w.server_time,
            "reconnect_token": w.reconnect_token,
            "content_version": w.content_version,
            "schema": schema_json(&w.schema),
        }),
        ServerMessage::Delta(d) => json!({
            "type": "Delta",
            "server_tick": d.server_tick,
            "ack_seq": d.ack_seq,
            "removes": d.removes,
            "creates": d.creates.iter().map(|c| json!({
                "id": c.id, "kind": c.kind,
                "fields": c.fields.iter().map(value_json).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "updates": d.updates.iter().map(|u| json!({
                "id": u.id, "mask": u.mask,
                "fields": u.fields.iter().map(value_json).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }),
        ServerMessage::Event { type_id, payload } => {
            json!({ "type": "Event", "type_id": type_id, "payload": hex(payload) })
        }
        ServerMessage::Pong {
            client_time,
            server_time,
        } => json!({ "type": "Pong", "client_time": client_time, "server_time": server_time }),
        ServerMessage::Freeze(n) | ServerMessage::Resume(n) => json!({
            "type": if matches!(m, ServerMessage::Freeze(_)) { "Freeze" } else { "Resume" },
            "reason": n.reason, "version": n.version, "notes": n.notes,
        }),
        ServerMessage::Handoff {
            target_zone,
            transfer_ticket,
        } => {
            json!({ "type": "Handoff", "target_zone": target_zone, "transfer_ticket": transfer_ticket })
        }
        ServerMessage::Kick { code, message } => {
            json!({ "type": "Kick", "code": code, "message": message })
        }
        ServerMessage::Error { code, message } => {
            json!({ "type": "Error", "code": code, "message": message })
        }
    }
}

fn fixture_schema() -> EntitySchema {
    EntitySchema::new()
        .kind(
            KindDef::new(1, "hero")
                .field("alive", FieldType::Bool)
                .field("level", FieldType::U8)
                .field("hp", FieldType::U16)
                .field("gold", FieldType::U32)
                .field("dz", FieldType::I32)
                .field("speed", FieldType::F32)
                .field(
                    "x",
                    FieldType::QuantF32 {
                        min: -100.0,
                        max: 100.0,
                        bits: 16,
                    },
                )
                .field("yaw", FieldType::Angle { bits: 10 })
                .field("name", FieldType::ShortStr)
                .field("tag", FieldType::Bytes),
        )
        .kind(KindDef::new(2, "orb").field(
            "x",
            FieldType::QuantF32 {
                min: 0.0,
                max: 64.0,
                bits: 12,
            },
        ))
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn check_or_write(name: &str, value: Value) {
    let path = fixtures_dir().join(name);
    let text = serde_json::to_string_pretty(&value).unwrap() + "\n";
    if std::env::var("UPDATE_GOLDEN").is_ok() || !path.exists() {
        std::fs::create_dir_all(fixtures_dir()).unwrap();
        std::fs::write(&path, &text).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        committed, text,
        "{name} drifted; the wire format changed (UPDATE_GOLDEN=1 to accept)"
    );
}

#[test]
fn primitives() {
    let mut cases = Vec::new();
    for v in [0u32, 1, 127, 128, 300, 16_384, u32::MAX] {
        let mut w = Writer::new();
        w.varuint(v);
        cases.push(json!({ "kind": "varuint", "value": v, "hex": hex(w.as_bytes()) }));
    }
    for v in [0i32, -1, 1, -64, 64, i32::MIN, i32::MAX] {
        let mut w = Writer::new();
        w.varint(v);
        assert_eq!(Reader::new(w.as_bytes()).varint().unwrap(), v);
        cases.push(json!({ "kind": "varint", "value": v, "hex": hex(w.as_bytes()) }));
    }
    for v in [1.5f32, -0.0, std::f32::consts::PI] {
        let mut w = Writer::new();
        w.f32(v);
        cases.push(json!({ "kind": "f32", "value": f32j(v), "hex": hex(w.as_bytes()) }));
    }
    for v in [1234.5f64, 0.1] {
        let mut w = Writer::new();
        w.f64(v);
        cases.push(json!({ "kind": "f64", "value": v, "hex": hex(w.as_bytes()) }));
    }
    for s in ["", "hi", "héros"] {
        let mut w = Writer::new();
        w.string(s);
        cases.push(json!({ "kind": "string", "value": s, "hex": hex(w.as_bytes()) }));
        let mut w = Writer::new();
        w.short_string(s);
        cases.push(json!({ "kind": "short_string", "value": s, "hex": hex(w.as_bytes()) }));
    }
    check_or_write("primitives.json", json!({ "cases": cases }));
}

#[test]
fn quantization() {
    let mut cases = Vec::new();
    let linear = [
        (
            -100.0f32,
            100.0f32,
            16u8,
            vec![-100.0f32, 0.0, 33.3, 99.999, 250.0, -1e9],
        ),
        (0.0, 64.0, 12, vec![0.0, 1.0 / 3.0, 31.99, 64.0]),
        (-1.0, 1.0, 8, vec![-1.0, 0.0, 0.5, f32::NAN]),
        (0.0, 1.0, 32, vec![0.123_456_79, 1.0]),
    ];
    for (min, max, bits, values) in linear {
        let ty = FieldType::QuantF32 { min, max, bits };
        for v in values {
            let mut w = Writer::new();
            ty.encode(&mut w, &FieldValue::F(v));
            let q = Reader::new(w.as_bytes()).varuint().unwrap();
            let FieldValue::F(back) = ty.canonicalize(&FieldValue::F(v)) else {
                panic!()
            };
            cases.push(json!({
                "field": field_type_json(&ty),
                "input_bits": format!("{:08x}", v.to_bits()),
                "q": q,
                "hex": hex(w.as_bytes()),
                "decoded": f32j(back),
            }));
        }
    }
    for bits in [8u8, 10, 16] {
        let ty = FieldType::Angle { bits };
        for v in [
            0.0f32,
            1.0,
            std::f32::consts::PI,
            -std::f32::consts::FRAC_PI_2,
            7.0,
            -0.000_001,
        ] {
            let mut w = Writer::new();
            ty.encode(&mut w, &FieldValue::F(v));
            let q = Reader::new(w.as_bytes()).varuint().unwrap();
            let FieldValue::F(back) = ty.canonicalize(&FieldValue::F(v)) else {
                panic!()
            };
            cases.push(json!({
                "field": field_type_json(&ty),
                "input_bits": format!("{:08x}", v.to_bits()),
                "q": q,
                "hex": hex(w.as_bytes()),
                "decoded": f32j(back),
            }));
        }
    }
    check_or_write("quantization.json", json!({ "cases": cases }));
}

#[test]
fn messages() {
    let schema = fixture_schema();
    let mut out = Vec::new();

    let client = vec![
        (
            "hello",
            ClientMessage::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                content_version: "c1".into(),
                capabilities: 5,
                ticket: "t.k.n".into(),
                reconnect_token: String::new(),
            }),
        ),
        (
            "hello_reconnect",
            ClientMessage::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                content_version: "c1".into(),
                capabilities: 0,
                ticket: String::new(),
                reconnect_token: "abcd".into(),
            }),
        ),
        (
            "input",
            ClientMessage::Input {
                seq: 300,
                client_tick: 1_000,
                payload: vec![0x01, 0xff],
            },
        ),
        (
            "ping",
            ClientMessage::Ping {
                client_time: 1234.5,
            },
        ),
        ("leave", ClientMessage::Leave),
    ];
    for (name, msg) in client {
        let bytes = msg.encode();
        assert_eq!(ClientMessage::decode(&bytes).unwrap(), msg);
        out.push(json!({
            "name": name, "direction": "client_to_server",
            "hex": hex(&bytes), "decoded": client_json(&msg),
        }));
    }

    let known: BTreeMap<u32, u16> = [(5u32, 1u16), (6, 2)].into_iter().collect();
    let ctx = (schema.clone(), known.clone());
    let hero_fields = vec![
        FieldValue::Bool(true),
        FieldValue::U(7),
        FieldValue::U(300),
        FieldValue::U(70_000),
        FieldValue::I(-2),
        FieldValue::F(1.5),
        FieldValue::F(-100.0),
        FieldValue::F(0.0),
        FieldValue::Str("Ana".into()),
        FieldValue::Bytes(vec![0xde, 0xad]),
    ];
    let server = vec![
        (
            "welcome",
            ServerMessage::Welcome(Welcome {
                session_id: "s1".into(),
                zone_id: "z1".into(),
                role: Role::Player,
                tick_rate: 30,
                patch_rate: 20,
                server_tick: 42,
                server_time: 1000.25,
                reconnect_token: "rt".into(),
                content_version: "c1".into(),
                schema: schema.clone(),
            }),
        ),
        (
            "delta_empty",
            ServerMessage::Delta(Delta {
                server_tick: 43,
                ack_seq: 0,
                ..Delta::default()
            }),
        ),
        (
            "delta",
            ServerMessage::Delta(Delta {
                server_tick: 200,
                ack_seq: 17,
                removes: vec![9, 130],
                creates: vec![
                    EntityCreate {
                        id: 300,
                        kind: 1,
                        fields: hero_fields,
                    },
                    EntityCreate {
                        id: 301,
                        kind: 2,
                        fields: vec![FieldValue::F(32.0)],
                    },
                ],
                updates: vec![
                    EntityUpdate {
                        id: 5,
                        mask: 0b0100_0100,
                        fields: vec![FieldValue::U(299), FieldValue::F(0.0)],
                    },
                    EntityUpdate {
                        id: 6,
                        mask: 0b1,
                        fields: vec![FieldValue::F(64.0)],
                    },
                    EntityUpdate {
                        id: 301,
                        mask: 0b1,
                        fields: vec![FieldValue::F(0.0)],
                    },
                ],
            }),
        ),
        (
            "event",
            ServerMessage::Event {
                type_id: 3,
                payload: b"xp".to_vec(),
            },
        ),
        (
            "pong",
            ServerMessage::Pong {
                client_time: 1234.5,
                server_time: 99.0,
            },
        ),
        (
            "freeze",
            ServerMessage::Freeze(Notice {
                reason: "content_update".into(),
                version: "c2".into(),
                notes: "brb".into(),
            }),
        ),
        (
            "resume",
            ServerMessage::Resume(Notice {
                reason: "content_update".into(),
                version: "c2".into(),
                notes: String::new(),
            }),
        ),
        (
            "handoff",
            ServerMessage::Handoff {
                target_zone: "z2".into(),
                transfer_ticket: "tt".into(),
            },
        ),
        (
            "kick",
            ServerMessage::Kick {
                code: kick::SHUTDOWN,
                message: "bye".into(),
            },
        ),
        (
            "error",
            ServerMessage::Error {
                code: error::UNAUTHORIZED,
                message: "Expired".into(),
            },
        ),
    ];
    for (name, msg) in server {
        let bytes = msg.encode(&ctx).unwrap();
        let decoded = ServerMessage::decode(&bytes, &ctx).unwrap();
        // Quantized values decode to their canonical form; compare JSON of
        // the decoded message (what a client will see).
        let mut entry = json!({
            "name": name, "direction": "server_to_client",
            "hex": hex(&bytes), "decoded": server_json(&decoded),
        });
        if matches!(msg, ServerMessage::Delta(_)) {
            entry["known_entities"] = json!(known
                .iter()
                .map(|(id, kind)| json!({ "id": id, "kind": kind }))
                .collect::<Vec<_>>());
            entry["schema"] = schema_json(&schema);
        }
        // Re-encoding the decoded form is byte-identical.
        assert_eq!(decoded.encode(&ctx).unwrap(), bytes, "{name}");
        out.push(entry);
    }
    check_or_write("messages.json", json!({ "messages": out }));

    // Fixture hex decodes back (guards hand edits).
    let text = std::fs::read_to_string(fixtures_dir().join("messages.json")).unwrap();
    let parsed: Value = serde_json::from_str(&text).unwrap();
    for m in parsed["messages"].as_array().unwrap() {
        let bytes = unhex(m["hex"].as_str().unwrap());
        if m["direction"] == "client_to_server" {
            assert_eq!(
                client_json(&ClientMessage::decode(&bytes).unwrap()),
                m["decoded"]
            );
        } else {
            let msg = ServerMessage::decode(&bytes, &ctx).unwrap();
            assert_eq!(server_json(&msg), m["decoded"]);
        }
    }
}
