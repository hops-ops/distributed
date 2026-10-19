//! Golden fixture for the Endless Grid's procedural world *content*
//! (terrain, biomes, props), which the client still renders from a
//! TypeScript twin.
//!
//! Writes/compares `examples/game/fixtures/grid-world.json`, which the
//! TypeScript client replays in `client/tests/grid-world-parity.test.mjs`
//! (bit-exact). Regenerate after an intended change to BOTH
//! `server/sim/src/grid.rs` and `client/src/grid/world.ts`:
//! `UPDATE_GOLDEN=1 cargo test --test grid_fixture`. Movement is not part
//! of it: the client predicts with the Rust step compiled to WebAssembly
//! (proved by `client/tests/prediction-wasm.test.mjs`).

use std::path::PathBuf;

use game_demos::grid::world::{self, DEFAULT_SEED};
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

fn build() -> Value {
    let seed = DEFAULT_SEED;
    let hashes = [0u32, 1, 0xdead_beef, u32::MAX].map(world::hash32);
    let mixes = [
        world::mix(seed, 3, -5, 7),
        world::mix(0, 0, i32::MIN, i32::MAX),
    ];
    json!({
        "description": "Endless Grid world-content parity: Rust game_sim::grid vs client/src/grid/world.ts",
        "constants": {
            "seed": seed, "contentVersion": world::CONTENT_VERSION, "cell": world::CELL,
            "propMargin": world::PROP_MARGIN,
            "hashes": hashes,
            "mix": mixes,
        },
        "cells": CELLS.iter().map(|c| cell_json(seed, *c)).collect::<Vec<_>>(),
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
    out + "  ]\n}\n"
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
