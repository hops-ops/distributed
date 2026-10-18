/**
 * Rust ↔ TypeScript parity of the Arena movement step: replays the fixture
 * written by `server/tests/movement_fixture.rs` through `src/arena/movement.ts`
 * (Node strips the types). Prediction is only exact if this passes.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import * as movement from '../src/arena/movement.ts';

const fixture = JSON.parse(
	readFileSync(new URL('../../fixtures/arena-movement.json', import.meta.url), 'utf8')
);
const EPSILON = 1e-4;

test('constants and wire quantization match the Rust side', () => {
	const c = fixture.constants;
	assert.equal(Math.fround(c.dt), movement.DT);
	assert.equal(c.half, movement.HALF);
	assert.equal(c.playerRadius, movement.PLAYER_RADIUS);
	assert.equal(c.maxSpeed, movement.MAX_SPEED);
	assert.equal(c.accel, movement.ACCEL);
	assert.equal(c.dashSpeed, movement.DASH_SPEED);
	assert.equal(c.dashFrames, movement.DASH_FRAMES);
	assert.equal(c.dashCooldown, movement.DASH_COOLDOWN);
	assert.deepEqual(c.pillars, movement.PILLARS.map((p) => [...p]));
	const byName = Object.fromEntries(fixture.fields.map((field) => [field.name, field]));
	for (const [name, quant] of [['x', movement.POS], ['y', movement.POS], ['vx', movement.VEL], ['vy', movement.VEL]]) {
		assert.deepEqual(
			{ min: byName[name].min, max: byName[name].max, bits: byName[name].bits },
			{ ...quant },
			name
		);
	}
});

test('the TypeScript step reproduces every Rust state', () => {
	const toState = (s) => ({ x: s.x, y: s.y, vx: s.vx, vy: s.vy, dash: s.dash, cooldown: s.cd });
	let state = toState(fixture.start);
	let exact = 0;
	let maxError = 0;
	fixture.steps.forEach(({ input, state: expected }, i) => {
		state = movement.step(state, { mx: input[0], my: input[1], buttons: input[2] });
		const want = toState(expected);
		for (const key of ['x', 'y', 'vx', 'vy']) {
			const error = Math.abs(state[key] - Math.fround(want[key]));
			maxError = Math.max(maxError, error);
			assert.ok(error <= EPSILON, `step ${i} ${key}: ts ${state[key]} rust ${want[key]}`);
		}
		assert.equal(state.dash, want.dash, `step ${i} dash`);
		assert.equal(state.cooldown, want.cooldown, `step ${i} cooldown`);
		if (['x', 'y', 'vx', 'vy'].every((k) => state[k] === Math.fround(want[k]))) exact += 1;
	});
	assert.ok(fixture.steps.length > 300);
	// Binary32 discipline makes the twins bit-identical, not just close.
	assert.equal(exact, fixture.steps.length, `bit-exact ${exact}/${fixture.steps.length}, max error ${maxError}`);
});
