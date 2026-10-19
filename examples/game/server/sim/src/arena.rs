//! Shared deterministic movement step for the Arena demo.
//!
//! **This file has a twin:** `examples/game/client/src/arena/movement.ts`.
//! The client predicts its own capsule with the same step, so both must
//! produce the same numbers for the same inputs. Rules that keep them equal:
//!
//! - all state and arithmetic is binary32 (`f32` here, `Math.fround` after
//!   every operation in TypeScript); only `+ - * /` and `sqrt` are used
//!   (correctly rounded in both languages; no trig, no `powf`);
//! - one input = one command frame of exactly [`DT`] seconds, independent
//!   of the server's measured tick length;
//! - after every step the replicated fields are snapped to their wire
//!   quantization ([`canonicalize`]), so the server's state is exactly what
//!   clients decode and a rebase never introduces error.
//!
//! `tests/movement_fixture.rs` writes `examples/game/fixtures/arena-movement.json`
//! (inputs → states) and the client's `tests/movement-parity.test.mjs` replays
//! it through the TypeScript step. Change both files together, then
//! regenerate with `UPDATE_GOLDEN=1 cargo test --test movement_fixture`.

use distributed_game::{FieldType, GameInput};

/// Command frame length (s): the 30 Hz tick.
pub const DT: f32 = 1.0 / 30.0;
/// Arena half extent (the floor spans -20..20 m on both axes).
pub const HALF: f32 = 20.0;
/// Capsule radius.
pub const PLAYER_RADIUS: f32 = 0.5;
/// Walking speed (m/s).
pub const MAX_SPEED: f32 = 5.0;
/// Acceleration toward the stick velocity (m/s²).
pub const ACCEL: f32 = 40.0;
/// Dash velocity (m/s).
pub const DASH_SPEED: f32 = 14.0;
/// Dash duration in command frames.
pub const DASH_FRAMES: u8 = 6;
/// Dash cooldown in command frames (1.5 s).
pub const DASH_COOLDOWN: u8 = 45;
/// Input bit: dash.
pub const BUTTON_DASH: u8 = MoveInput::DASH;

/// Static circular pillars `(x, y, radius)`.
pub const PILLARS: [(f32, f32, f32); 9] = [
    (0.0, 0.0, 2.5),
    (-9.0, -9.0, 1.5),
    (9.0, -9.0, 1.5),
    (-9.0, 9.0, 1.5),
    (9.0, 9.0, 1.5),
    (0.0, -14.0, 1.0),
    (0.0, 14.0, 1.0),
    (-14.0, 0.0, 1.0),
    (14.0, 0.0, 1.0),
];

/// Wire type of `x`/`y`: 20/2^17 m ≈ 0.15 mm steps, 0 exact.
pub const POS: FieldType = FieldType::quant_centered(HALF, 18);
/// Wire type of `vx`/`vy`: 1/128 m/s steps, 0 exact (a resting capsule
/// must have velocity exactly 0 or it creeps).
pub const VEL: FieldType = FieldType::quant_centered(16.0, 12);

/// One command frame from the client: stick in `[-127, 127]` per axis
/// (+x right, +y forward) and button bits. Wire payload `[mx, my, buttons]`
/// (3 bytes); [`GameInput::from_payload`] clamps `-128` to `-127` and clears
/// unknown buttons. The TypeScript encoder/sanitizer is generated from this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, GameInput)]
pub struct MoveInput {
    /// Stick x.
    #[range(min = -127, max = 127)]
    pub mx: i8,
    /// Stick y.
    #[range(min = -127, max = 127)]
    pub my: i8,
    /// [`MoveInput::DASH`].
    #[buttons(dash)]
    pub buttons: u8,
}

/// Everything the step reads. Every field is replicated, so a client can
/// rebase its prediction on the authoritative copy.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct MoveState {
    /// Position x.
    pub x: f32,
    /// Position y.
    pub y: f32,
    /// Velocity x.
    pub vx: f32,
    /// Velocity y.
    pub vy: f32,
    /// Remaining dash frames.
    pub dash: u8,
    /// Remaining cooldown frames.
    pub cooldown: u8,
}

/// Advance one command frame.
pub fn step(s: MoveState, input: MoveInput) -> MoveState {
    let mut s = s;
    // Stick direction, length clamped to 1.
    let mut dx = f32::from(input.mx) / 127.0;
    let mut dy = f32::from(input.my) / 127.0;
    let l2 = dx * dx + dy * dy;
    if l2 > 1.0 {
        let inv = 1.0 / l2.sqrt();
        dx *= inv;
        dy *= inv;
    }

    s.cooldown = s.cooldown.saturating_sub(1);
    if input.buttons & BUTTON_DASH != 0 && s.cooldown == 0 && s.dash == 0 {
        // Dash along the stick, else along the current velocity.
        let v2 = s.vx * s.vx + s.vy * s.vy;
        let dir = if l2 > 0.01 {
            let l = (dx * dx + dy * dy).sqrt();
            Some((dx / l, dy / l))
        } else if v2 > 0.01 {
            let l = v2.sqrt();
            Some((s.vx / l, s.vy / l))
        } else {
            None
        };
        if let Some((nx, ny)) = dir {
            s.vx = nx * DASH_SPEED;
            s.vy = ny * DASH_SPEED;
            s.dash = DASH_FRAMES;
            s.cooldown = DASH_COOLDOWN;
        }
    }

    if s.dash > 0 {
        s.dash -= 1;
    } else {
        let ex = dx * MAX_SPEED - s.vx;
        let ey = dy * MAX_SPEED - s.vy;
        let e2 = ex * ex + ey * ey;
        let max_dv = ACCEL * DT;
        if e2 > max_dv * max_dv {
            let k = max_dv / e2.sqrt();
            s.vx += ex * k;
            s.vy += ey * k;
        } else {
            s.vx += ex;
            s.vy += ey;
        }
    }

    s.x += s.vx * DT;
    s.y += s.vy * DT;

    for (px, py, pr) in PILLARS {
        let ox = s.x - px;
        let oy = s.y - py;
        let d2 = ox * ox + oy * oy;
        let min = PLAYER_RADIUS + pr;
        if d2 < min * min {
            let d = d2.sqrt();
            let (nx, ny) = if d > 1e-6 {
                (ox / d, oy / d)
            } else {
                (1.0, 0.0)
            };
            s.x = px + nx * min;
            s.y = py + ny * min;
            let vn = s.vx * nx + s.vy * ny;
            if vn < 0.0 {
                s.vx -= vn * nx;
                s.vy -= vn * ny;
            }
        }
    }

    let lim = HALF - PLAYER_RADIUS;
    if s.x < -lim {
        s.x = -lim;
        s.vx = s.vx.max(0.0);
    } else if s.x > lim {
        s.x = lim;
        s.vx = s.vx.min(0.0);
    }
    if s.y < -lim {
        s.y = -lim;
        s.vy = s.vy.max(0.0);
    } else if s.y > lim {
        s.y = lim;
        s.vy = s.vy.min(0.0);
    }
    canonicalize(s)
}

fn snap(ty: FieldType, v: f32) -> f32 {
    match ty {
        FieldType::QuantF32 { min, max, bits } => FieldType::dequantize_linear(
            FieldType::quantize_linear(v, min, max, bits),
            min,
            max,
            bits,
        ),
        _ => v,
    }
}

/// Snap to the replicated precision (see the module docs).
pub fn canonicalize(s: MoveState) -> MoveState {
    MoveState {
        x: snap(POS, s.x),
        y: snap(POS, s.y),
        vx: snap(VEL, s.vx),
        vy: snap(VEL, s.vy),
        ..s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(mut s: MoveState, input: MoveInput, frames: usize) -> MoveState {
        for _ in 0..frames {
            s = step(s, input);
        }
        s
    }

    const START: MoveState = MoveState {
        x: -5.0,
        y: -5.0,
        vx: 0.0,
        vy: 0.0,
        dash: 0,
        cooldown: 0,
    };

    #[test]
    fn walks_at_max_speed_after_accelerating() {
        let s = run(
            START,
            MoveInput {
                mx: 127,
                my: 0,
                buttons: 0,
            },
            30,
        );
        assert!((s.vx - MAX_SPEED).abs() < 0.01, "{s:?}");
        // 1 s of walking minus the acceleration phase (~0.06 m lost).
        assert!(s.x > -5.0 + 4.6 && s.x < 0.0, "{s:?}");
    }

    #[test]
    fn diagonal_stick_is_not_faster() {
        let open = MoveState {
            x: -15.0,
            y: 5.0,
            ..START
        };
        let s = run(
            open,
            MoveInput {
                mx: 127,
                my: 127,
                buttons: 0,
            },
            20,
        );
        let speed = (s.vx * s.vx + s.vy * s.vy).sqrt();
        assert!((speed - MAX_SPEED).abs() < 0.02, "{speed}");
    }

    #[test]
    fn pillars_and_walls_block() {
        // Walk right into the central pillar from the left.
        let at = MoveState {
            x: -6.0,
            y: 0.0,
            ..START
        };
        let s = run(
            at,
            MoveInput {
                mx: 127,
                my: 0,
                buttons: 0,
            },
            90,
        );
        assert!(s.x <= -(2.5 + PLAYER_RADIUS) + 1e-3, "{s:?}");
        let s = run(
            START,
            MoveInput {
                mx: -127,
                my: 0,
                buttons: 0,
            },
            200,
        );
        assert!((s.x + (HALF - PLAYER_RADIUS)).abs() < 1e-3, "{s:?}");
        assert_eq!(s.vx, 0.0);
    }

    #[test]
    fn dash_bursts_then_cools_down() {
        let dash = MoveInput {
            mx: 0,
            my: 127,
            buttons: BUTTON_DASH,
        };
        let s = step(START, dash);
        assert_eq!((s.dash, s.cooldown), (DASH_FRAMES - 1, DASH_COOLDOWN));
        assert!(s.vy > 13.9);
        let s = run(s, dash, 14);
        assert!(
            s.vy <= MAX_SPEED + 0.01,
            "dash over, back to walking: {s:?}"
        );
        assert!(
            s.cooldown > 0 && s.dash == 0,
            "held button does not re-dash"
        );
        let s = run(s, MoveInput::default(), 31);
        assert_eq!(s.cooldown, 0);
    }

    #[test]
    fn input_round_trips_and_sanitizes() {
        let i = MoveInput {
            mx: -127,
            my: 5,
            buttons: BUTTON_DASH,
        };
        assert_eq!(MoveInput::from_payload(&i.to_payload()), Some(i));
        assert_eq!(
            MoveInput::from_payload(&[0x80, 0, 0xff]),
            Some(MoveInput {
                mx: -127,
                my: 0,
                buttons: BUTTON_DASH
            })
        );
        assert_eq!(MoveInput::from_payload(&[1, 2]), None);
    }
}
