//! Golden fixture for the Endless Grid's procedural world and movement.
//!
//! Writes/compares `examples/game/fixtures/grid-world.json`, which the
//! TypeScript client replays in `client/tests/grid-world-parity.test.mjs`
//! (bit-exact). Regenerate after an intended change to BOTH
//! `server/src/grid/world.rs` and `client/src/grid/world.ts`:
//! `UPDATE_GOLDEN=1 cargo test --test grid_fixture`.

use std::path::PathBuf;

use game_demos::grid::world::{self, MoveInput, BUTTON_SPRINT, DEFAULT_SEED};
use serde_json::{json, Value};

const CELLS: [(i32, i32); 6] = [
    (0, 0),
    (-1, 0),
    (1, -1),
    (3, -7),
    (-12, 5),
    (999_999, -1_000_000),
];

fn cell_json(seed: u32, cell: (i32, i32)) -> Value {
    let props: Vec<Value> = world::cell_props(seed, cell)
        .iter()
        .map(|p| json!([p.kind as u8, p.x, p.y, p.r, p.solid, p.variant]))
        .collect();
    let mut heights = Vec::new();
    let mut moisture = Vec::new();
    let mut biomes = Vec::new();
    for j in 0..=8 {
        for i in 0..=8 {
            let l = (i as f32 * 4.0, j as f32 * 4.0 + 0.25);
            heights.push(json!(world::height(seed, cell, l)));
            moisture.push(json!(world::moisture(seed, cell, l)));
            biomes.push(json!(world::biome(seed, cell, l)));
        }
    }
    json!({ "cell": [cell.0, cell.1], "props": props, "heights": heights, "moisture": moisture, "biomes": biomes })
}

/// The densest of a few cells, so the walk bumps into props.
fn busy_cell(seed: u32) -> (i32, i32) {
    (0..12)
        .map(|i| (i, 2))
        .max_by_key(|c| {
            world::cell_props(seed, *c)
                .iter()
                .filter(|p| p.solid)
                .count()
        })
        .unwrap()
}

fn walk(seed: u32) -> Value {
    let cell = busy_cell(seed);
    let props = world::cell_props(seed, cell);
    // Head for each solid prop in turn (pushing into it), then wander off.
    let (mut x, mut y) = (16.0f32, 16.0f32);
    let mut steps = Vec::new();
    let targets: Vec<(f32, f32)> = props
        .iter()
        .filter(|p| p.solid)
        .map(|p| (p.x, p.y))
        .chain([(40.0, 16.0), (16.0, -5.0)])
        .collect();
    for (k, (tx, ty)) in targets.iter().enumerate() {
        for _ in 0..60 {
            let (dx, dy) = (tx - x, ty - y);
            let l = (dx * dx + dy * dy).sqrt().max(1e-3);
            let input = MoveInput {
                mx: (dx / l * 127.0) as i8,
                my: if k % 3 == 0 {
                    -128
                } else {
                    (dy / l * 127.0) as i8
                },
                buttons: if k % 2 == 0 { BUTTON_SPRINT } else { 0 },
            };
            let decoded = MoveInput::decode(&input.encode()).unwrap();
            (x, y) = world::step(x, y, decoded, &props);
            steps.push(json!({ "input": [input.mx, input.my, input.buttons], "x": x, "y": y }));
        }
    }
    json!({ "cell": [cell.0, cell.1], "start": [16.0, 16.0], "steps": steps })
}

fn build() -> Value {
    let seed = DEFAULT_SEED;
    let hashes = [0u32, 1, 0xdead_beef, u32::MAX].map(world::hash32);
    let mixes = [
        world::mix(seed, 3, -5, 7),
        world::mix(0, 0, i32::MIN, i32::MAX),
    ];
    json!({
        "description": "Endless Grid parity: Rust grid::world vs client/src/grid/world.ts",
        "constants": {
            "seed": seed, "contentVersion": world::CONTENT_VERSION, "cell": world::CELL,
            "propMargin": world::PROP_MARGIN, "hysteresis": world::HYSTERESIS,
            "playerRadius": world::PLAYER_RADIUS, "walk": world::WALK_SPEED, "sprint": world::SPRINT_SPEED,
            "hashes": hashes,
            "mix": mixes,
        },
        "cells": CELLS.iter().map(|c| cell_json(seed, *c)).collect::<Vec<_>>(),
        "walk": walk(seed),
    })
}

fn render(v: &Value) -> String {
    let mut out = String::from("{\n");
    for key in ["description", "constants"] {
        out += &format!("  \"{key}\": {},\n", v[key]);
    }
    out += "  \"cells\": [\n";
    let cells = v["cells"].as_array().unwrap();
    for (i, cell) in cells.iter().enumerate() {
        let sep = if i + 1 == cells.len() { "" } else { "," };
        out += &format!("    {cell}{sep}\n");
    }
    out += &format!("  ],\n  \"walk\": {}\n}}\n", v["walk"]);
    out
}

#[test]
fn grid_world_fixture_is_current() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/grid-world.json");
    let fresh = render(&build());
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, fresh).unwrap();
        return;
    }
    let stored =
        std::fs::read_to_string(&path).expect("fixture exists (UPDATE_GOLDEN=1 to create)");
    assert!(
        stored == fresh,
        "grid world changed: update client/src/grid/world.ts too, then UPDATE_GOLDEN=1"
    );
}

#[test]
fn biomes_and_props_vary() {
    let mut biomes = [0usize; 4];
    let mut props = 0;
    for cx in -10..10 {
        for cy in -10..10 {
            biomes[world::biome(DEFAULT_SEED, (cx, cy), (16.0, 16.0)) as usize] += 1;
            props += world::cell_props(DEFAULT_SEED, (cx, cy)).len();
        }
    }
    eprintln!("biomes {biomes:?}, props/cell {}", props as f32 / 400.0);
    assert!(
        biomes.iter().all(|n| *n > 10),
        "every biome appears: {biomes:?}"
    );
    assert!((3..12).contains(&(props / 400)), "props per cell");
}
