//! Golden fixture for the shared Arena movement step.
//!
//! Writes/compares `examples/game/fixtures/arena-movement.json`, which the
//! TypeScript client replays in `client/tests/movement-parity.test.mjs`.
//! Regenerate after an intended change to BOTH implementations:
//! `UPDATE_GOLDEN=1 cargo test --test movement_fixture`.

use distributed_game::GameInput;
use std::path::PathBuf;

use game_demos::arena::movement::{self, MoveInput, MoveState, BUTTON_DASH};
use serde_json::{json, Value};

/// A scripted input sequence covering acceleration, diagonals, partial
/// stick, dash (stick and velocity direction), held dash, pillar and wall
/// contact.
fn script() -> Vec<MoveInput> {
    let i = |mx: i8, my: i8, buttons: u8| MoveInput { mx, my, buttons };
    let mut out = Vec::new();
    let mut push = |input: MoveInput, n: usize| out.extend(std::iter::repeat_n(input, n));
    push(i(127, 0, 0), 20); // accelerate right
    push(i(90, 90, 0), 15); // diagonal (clamped length)
    push(i(30, -64, 0), 10); // partial stick
    push(i(0, 0, BUTTON_DASH), 1); // dash along velocity
    push(i(0, 0, 0), 12); // decelerate
    push(i(-127, 0, BUTTON_DASH), 8); // dash along stick, held
    push(i(-128, 5, 0), 40); // into the left pillars / wall (-128 sanitizes)
    push(i(127, 127, 0), 60); // towards the centre pillar
    push(i(0, 127, BUTTON_DASH), 70); // repeated dashes after cooldown
    push(i(0, -127, 0), 120); // across the floor into the far wall
    out
}

fn state_json(s: &MoveState) -> Value {
    json!({ "x": s.x, "y": s.y, "vx": s.vx, "vy": s.vy, "dash": s.dash, "cd": s.cooldown })
}

fn field_json(name: &str, ty: distributed_game::FieldType) -> Value {
    match ty {
        distributed_game::FieldType::QuantF32 { min, max, bits } => {
            json!({ "name": name, "type": "quant_f32", "min": min, "max": max, "bits": bits })
        }
        _ => unreachable!(),
    }
}

fn build() -> Value {
    let start = movement::canonicalize(MoveState {
        x: -5.0,
        y: -5.0,
        ..MoveState::default()
    });
    let mut s = start;
    let mut steps = Vec::new();
    for input in script() {
        // The server decodes the wire payload, so the fixture does too.
        let decoded = MoveInput::from_payload(&input.to_payload()).unwrap();
        s = movement::step(s, decoded);
        steps.push(json!({
            "input": [input.mx, input.my, input.buttons],
            "state": state_json(&s),
        }));
    }
    json!({
        "description": "Arena movement parity: Rust movement::step vs client/src/arena/movement.ts",
        "constants": {
            "dt": movement::DT, "half": movement::HALF, "playerRadius": movement::PLAYER_RADIUS,
            "maxSpeed": movement::MAX_SPEED, "accel": movement::ACCEL, "dashSpeed": movement::DASH_SPEED,
            "dashFrames": movement::DASH_FRAMES, "dashCooldown": movement::DASH_COOLDOWN,
            "pillars": movement::PILLARS.iter().map(|(x, y, r)| json!([x, y, r])).collect::<Vec<_>>(),
        },
        "fields": [
            field_json("x", movement::POS), field_json("y", movement::POS),
            field_json("vx", movement::VEL), field_json("vy", movement::VEL),
        ],
        "start": state_json(&start),
        "steps": steps,
    })
}

/// Stable, diff-friendly text: one step per line. Compared as text because
/// floats are written as the exact f64 widening of each f32.
fn render(v: &Value) -> String {
    let mut out = String::from("{\n");
    for key in ["description", "constants", "fields", "start"] {
        out += &format!("  \"{key}\": {},\n", v[key]);
    }
    out += "  \"steps\": [\n";
    let steps = v["steps"].as_array().unwrap();
    for (i, step) in steps.iter().enumerate() {
        let sep = if i + 1 == steps.len() { "" } else { "," };
        out += &format!("    {step}{sep}\n");
    }
    out + "  ]\n}\n"
}

#[test]
fn movement_fixture_is_current() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/arena-movement.json");
    let fresh = render(&build());
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, fresh).unwrap();
        return;
    }
    let stored =
        std::fs::read_to_string(&path).expect("fixture exists (UPDATE_GOLDEN=1 to create)");
    assert!(
        stored == fresh,
        "movement changed: update client/src/arena/movement.ts too, then UPDATE_GOLDEN=1"
    );
}
