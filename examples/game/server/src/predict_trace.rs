//! Native traces of every demo's client-predicted step over recorded input
//! streams, and of every `#[game_pure]` function over recorded arguments:
//! the reference side of the native ↔ WebAssembly exact-match proof.
//! `game-demos predict-trace` prints them as JSON; the client's
//! `tests/prediction-wasm.test.mjs` replays the same streams and arguments
//! through each surface's compiled module and requires every state and
//! result to be byte-identical.
//!
//! Every step runs through [`step_bytes`] and every function through
//! [`call_bytes`], the same decode → run → encode paths the WASM exports
//! take, so the comparison covers the ABI as well as the arithmetic. Inputs
//! are raw payload bytes, deliberately including values the decoder
//! sanitizes (`-128` sticks, unknown button bits).

use distributed_game::prediction::{step_bytes, PredictContext, PredictStep};
use distributed_game::pure::{call_bytes, GamePure};
use distributed_game::Wire;
use game_sim::arena::{self, ArenaStep, MoveState};
use game_sim::grid::{
    self, GridCellFn, GridContext, GridPoint, GridSampleFn, GridStep, WalkerState, BUTTON_SPRINT,
    CELL, DEFAULT_SEED,
};
use game_sim::mobpit::PitStep;
use game_sim::ripple::HoodStep;
use serde_json::{json, Value};

/// Deterministic xorshift32 (streams must be identical on every run).
struct Rng(u32);

impl Rng {
    fn next(&mut self) -> u32 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.0 = x;
        x
    }

    fn stick(&mut self) -> u8 {
        match self.next() % 8 {
            0 => 0,
            1 => 0x80, // -128: sanitized to -127
            2 => 127,
            3 => 0x81, // -127
            _ => self.next() as u8,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A recorded stream: the start state, the context and one payload per frame.
struct Stream {
    name: String,
    start: Vec<u8>,
    context: Vec<u8>,
    inputs: Vec<Vec<u8>>,
}

fn trace<P: PredictStep>(streams: Vec<Stream>) -> Value {
    let streams: Vec<Value> = streams
        .into_iter()
        .map(|stream| {
            let mut state = stream.start.clone();
            let mut states = Vec::with_capacity(stream.inputs.len());
            for input in &stream.inputs {
                state = step_bytes::<P>(&state, input, &stream.context, arena::DT)
                    .unwrap_or_else(|e| panic!("{} {}: {e:?}", P::EXPORT, stream.name));
                states.push(hex(&state));
            }
            json!({
                "name": stream.name,
                "start": hex(&stream.start),
                "context": hex(&stream.context),
                "inputs": stream.inputs.iter().map(|i| hex(i)).collect::<Vec<_>>(),
                "states": states,
            })
        })
        .collect();
    json!({ "export": P::EXPORT, "kind": P::KIND, "streams": streams })
}

fn state_bytes<T: Wire>(state: &T) -> Vec<u8> {
    distributed_game::codec::to_bytes(state)
}

/// The Arena movement script (acceleration, diagonals, partial stick,
/// dashes along stick and velocity, held dash, pillar and wall contact),
/// as `[mx, my, buttons]`; `extra` appends bytes (Mob Pit / Ripple bits).
fn scripted(extra: &dyn Fn(usize) -> Vec<u8>) -> Vec<Vec<u8>> {
    let dash = arena::BUTTON_DASH;
    let script: [([i8; 2], u8, usize); 10] = [
        ([127, 0], 0, 20),
        ([90, 90], 0, 15),
        ([30, -64], 0, 10),
        ([0, 0], dash, 1),
        ([0, 0], 0, 12),
        ([-127, 0], dash, 8),
        ([-128, 5], 0, 40),
        ([127, 127], 0, 60),
        ([0, 127], dash, 70),
        ([0, -127], 0, 120),
    ];
    let mut out = Vec::new();
    for ([mx, my], buttons, n) in script {
        for _ in 0..n {
            let mut frame = vec![mx as u8, my as u8, buttons];
            frame.extend(extra(out.len()));
            out.push(frame);
        }
    }
    out
}

/// Random held inputs (1–20 frames each) over the whole byte range.
fn random(seed: u32, frames: usize, extra_bytes: usize) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let mut out = Vec::with_capacity(frames);
    while out.len() < frames {
        let mut frame = vec![rng.stick(), rng.stick(), rng.next() as u8];
        for _ in 0..extra_bytes {
            frame.push(rng.next() as u8);
        }
        let hold = 1 + rng.next() as usize % 20;
        for _ in 0..hold.min(frames - out.len()) {
            out.push(frame.clone());
        }
    }
    out
}

fn capsule_streams(extra_bytes: usize) -> Vec<Stream> {
    let start = |x: f32, y: f32| {
        state_bytes(&arena::canonicalize(MoveState {
            x,
            y,
            ..MoveState::default()
        }))
    };
    let extra = move |i: usize| vec![(i as u8).wrapping_mul(37); extra_bytes];
    vec![
        Stream {
            name: "scripted".into(),
            start: start(-5.0, -5.0),
            context: Vec::new(),
            inputs: scripted(&extra),
        },
        Stream {
            name: "random".into(),
            start: start(8.0, 3.0),
            context: Vec::new(),
            inputs: random(0x1234_5678, 3000, extra_bytes),
        },
        Stream {
            name: "corner".into(),
            start: start(18.5, -18.5),
            context: Vec::new(),
            inputs: random(0x0bad_cafe, 600, extra_bytes),
        },
    ]
}

/// The densest of a few cells, so a walk bumps into props.
fn busy_cell(seed: u32) -> (i32, i32) {
    (0..12)
        .map(|i| (i, 2))
        .max_by_key(|c| {
            grid::cell_props(seed, *c)
                .iter()
                .filter(|p| p.solid)
                .count()
        })
        .expect("cells")
}

fn grid_context(cell: (i32, i32)) -> GridContext {
    GridContext {
        seed: DEFAULT_SEED,
        cx: cell.0,
        cy: cell.1,
    }
}

/// Head for each solid prop of the busiest cell in turn (pushing into it),
/// then walk off two borders.
fn prop_walk() -> Stream {
    let cell = busy_cell(DEFAULT_SEED);
    let context = grid_context(cell);
    let props = grid::cell_props(DEFAULT_SEED, cell);
    let mut state = WalkerState { x: 16.0, y: 16.0 };
    let targets: Vec<(f32, f32)> = props
        .iter()
        .filter(|p| p.solid)
        .map(|p| (p.x, p.y))
        .chain([(40.0, 16.0), (16.0, -5.0)])
        .collect();
    let mut inputs = Vec::new();
    for (k, (tx, ty)) in targets.iter().enumerate() {
        for _ in 0..60 {
            let (dx, dy) = (tx - state.x, ty - state.y);
            let l = (dx * dx + dy * dy).sqrt().max(1e-3);
            let input = [
                (dx / l * 127.0) as i8 as u8,
                if k % 3 == 0 {
                    0x80
                } else {
                    (dy / l * 127.0) as i8 as u8
                },
                if k % 2 == 0 { BUTTON_SPRINT } else { 0 },
            ];
            state = grid::grid_step(
                &state,
                &<grid::MoveInput as distributed_game::GameInput>::from_payload(&input)
                    .expect("valid"),
                &context,
                arena::DT,
            );
            inputs.push(input.to_vec());
        }
    }
    Stream {
        name: format!("props {},{}", cell.0, cell.1),
        start: state_bytes(&WalkerState { x: 16.0, y: 16.0 }),
        context: context.encode_context(),
        inputs,
    }
}

fn grid_streams() -> Vec<Stream> {
    let mut streams = vec![prop_walk()];
    for (i, cell) in [(0, 0), (3, -7), (999_999, -1_000_000)]
        .into_iter()
        .enumerate()
    {
        streams.push(Stream {
            name: format!("random {},{}", cell.0, cell.1),
            start: state_bytes(&WalkerState { x: 16.0, y: 16.0 }),
            context: grid_context(cell).encode_context(),
            inputs: random(0x9e37_79b9 ^ i as u32, 1500, 0),
        });
    }
    streams
}

fn pure_trace<F: GamePure>(args: Vec<Vec<u8>>) -> Value {
    let calls: Vec<Value> = args
        .iter()
        .map(|arg| {
            let output = call_bytes::<F>(arg).unwrap_or_else(|e| panic!("{}: {e:?}", F::EXPORT));
            json!({ "arg": hex(arg), "output": hex(&output) })
        })
        .collect();
    json!({ "export": F::EXPORT, "calls": calls })
}

/// Cells for the content traces: around the origin, far away, the
/// coordinate limits (lattice wrap-around) and the random walk's cells.
fn content_cells() -> Vec<(i32, i32)> {
    let mut cells = vec![
        (0, 0),
        (-1, 0),
        (1, -1),
        (3, -7),
        (-12, 5),
        (999_999, -1_000_000),
        (-1_000_000, 1_000_000),
        busy_cell(DEFAULT_SEED),
    ];
    let mut rng = Rng(0x5eed_cafe);
    for _ in 0..24 {
        let c = |r: &mut Rng| (r.next() % 2001) as i32 - 1000;
        cells.push((c(&mut rng), c(&mut rng)));
    }
    cells
}

/// `grid_cell` for every content cell, under two seeds.
fn grid_cell_args() -> Vec<Vec<u8>> {
    let mut args = Vec::new();
    for seed in [DEFAULT_SEED, 0xdead_beef] {
        for (cx, cy) in content_cells() {
            args.push(GridContext { seed, cx, cy }.encode_context());
        }
    }
    args
}

/// `grid_sample` on lattice points, borders, arbitrary floats and points
/// outside the cell (clamped).
fn grid_sample_args() -> Vec<Vec<u8>> {
    let mut rng = Rng(0x0dd_ba11);
    let mut args = Vec::new();
    for (cx, cy) in content_cells() {
        let mut points: Vec<(f32, f32)> = vec![
            (0.0, 0.0),
            (CELL, CELL),
            (CELL, 7.25),
            (16.0, 0.0),
            (-3.0, 40.0),
        ];
        for _ in 0..40 {
            let f = |r: &mut Rng| (r.next() >> 8) as f32 / 16_777_216.0 * (CELL + 4.0) - 2.0;
            points.push((f(&mut rng), f(&mut rng)));
        }
        for (x, y) in points {
            args.push(
                GridPoint {
                    seed: DEFAULT_SEED,
                    cx,
                    cy,
                    x,
                    y,
                }
                .encode_context(),
            );
        }
    }
    args
}

/// Every demo step's and pure function's native trace (see the module docs).
pub fn traces() -> Value {
    json!({
        "abi": distributed_game::prediction::ABI_VERSION,
        "pures": [
            pure_trace::<GridCellFn>(grid_cell_args()),
            pure_trace::<GridSampleFn>(grid_sample_args()),
        ],
        "steps": [
            trace::<ArenaStep>(capsule_streams(0)),
            trace::<PitStep>(capsule_streams(0)),
            trace::<HoodStep>(capsule_streams(1)),
            trace::<GridStep>(grid_streams()),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_step_is_the_zone_step_against_the_cell_props() {
        let cell = busy_cell(DEFAULT_SEED);
        let props = grid::cell_props(DEFAULT_SEED, cell);
        let mut zone = (16.0f32, 16.0f32);
        let mut predicted = WalkerState { x: 16.0, y: 16.0 };
        for input in random(7, 400, 0) {
            let input = <grid::MoveInput as distributed_game::GameInput>::from_payload(&input)
                .expect("valid");
            zone = grid::step(zone.0, zone.1, input, &props);
            predicted = grid::grid_step(&predicted, &input, &grid_context(cell), arena::DT);
            assert_eq!((predicted.x, predicted.y), zone);
        }
    }

    #[test]
    fn grid_cell_is_the_world_generation() {
        let cell = busy_cell(DEFAULT_SEED);
        let content = grid::grid_cell(&grid_context(cell));
        assert_eq!(content.props, grid::cell_props(DEFAULT_SEED, cell));
        assert_eq!(content.heights.len(), grid::SAMPLES * grid::SAMPLES);
        let (i, j) = (5, 11);
        let l = (
            i as f32 * grid::SAMPLE_SPACING,
            j as f32 * grid::SAMPLE_SPACING,
        );
        let k = j * grid::SAMPLES + i;
        assert_eq!(content.heights[k], grid::height(DEFAULT_SEED, cell, l));
        assert_eq!(content.biomes[k], grid::biome(DEFAULT_SEED, cell, l));
        let sample = grid::grid_sample(&GridPoint {
            seed: DEFAULT_SEED,
            cx: cell.0,
            cy: cell.1,
            x: l.0,
            y: l.1,
        });
        assert_eq!(
            (sample.height, sample.moisture, sample.biome),
            (content.heights[k], content.moisture[k], content.biomes[k])
        );
    }

    #[test]
    fn traces_cover_every_step_and_collide() {
        let traces = traces();
        let steps = traces["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 4);
        for step in steps {
            for stream in step["streams"].as_array().unwrap() {
                assert_eq!(
                    stream["inputs"].as_array().unwrap().len(),
                    stream["states"].as_array().unwrap().len()
                );
            }
        }
    }
}
