/**
 * Rust ↔ TypeScript parity of the Endless Grid's procedural world content
 * (rendering only): replays `examples/game/fixtures/grid-world.json`
 * (written by `server/tests/grid_fixture.rs`) through `src/grid/world.ts`.
 * Every value must match bit for bit — the client draws the props the
 * server collides against. Movement prediction runs the Rust step as WASM
 * (`prediction-wasm.test.mjs`), so it has no TypeScript twin to check.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import * as world from '../src/grid/world.ts';

const fixture = JSON.parse(readFileSync(new URL('../../fixtures/grid-world.json', import.meta.url), 'utf8'));
const f = Math.fround;

test('constants and hashes match', () => {
	const c = fixture.constants;
	assert.equal(c.seed, world.DEFAULT_SEED);
	assert.equal(c.contentVersion, world.CONTENT_VERSION);
	assert.equal(c.cell, world.CELL);
	assert.equal(c.propMargin, world.PROP_MARGIN);
	assert.deepEqual([0, 1, 0xdeadbeef, 0xffffffff].map(world.hash32), c.hashes);
	assert.deepEqual([world.mix(c.seed, 3, -5, 7), world.mix(0, 0, -2147483648, 2147483647)], c.mix);
});

test('terrain, moisture, biomes and props are bit-identical', () => {
	const seed = fixture.constants.seed;
	let props = 0;
	for (const expected of fixture.cells) {
		const cell = expected.cell;
		let k = 0;
		for (let j = 0; j <= 8; j += 1) {
			for (let i = 0; i <= 8; i += 1, k += 1) {
				const lx = i * 4;
				const ly = j * 4 + 0.25;
				assert.equal(world.height(seed, cell, lx, ly), f(expected.heights[k]), `height ${cell} ${i},${j}`);
				assert.equal(world.moisture(seed, cell, lx, ly), f(expected.moisture[k]), `moisture ${cell} ${i},${j}`);
				assert.equal(world.biome(seed, cell, lx, ly), expected.biomes[k], `biome ${cell} ${i},${j}`);
			}
		}
		const actual = world.cellProps(seed, cell).map((p) => [p.kind, p.x, p.y, p.r, p.solid, p.variant]);
		const want = expected.props.map(([kind, x, y, r, solid, variant]) => [kind, f(x), f(y), f(r), solid, variant]);
		assert.deepEqual(actual, want, `props of ${cell}`);
		props += actual.length;
	}
	assert.ok(props > 10, 'fixture cells have props');
});

test('zone ids round-trip', () => {
	assert.deepEqual(world.parseZone('grid.-3.12'), [-3, 12]);
	assert.equal(world.zoneId([4, -2]), 'grid.4.-2');
	for (const bad of ['grid.1', 'grid.01.2', 'grid.1.2.3', 'grid.2000000.0', 'arena-1']) {
		assert.equal(world.parseZone(bad), undefined, bad);
	}
});
