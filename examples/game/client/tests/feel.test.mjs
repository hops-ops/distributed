// Presentation feel helpers (shared/feel.ts): easing, trauma shake, pools.
import assert from 'node:assert/strict';
import test from 'node:test';
import { easeOutBack, easeOutCubic, perlin, Pool, Trauma, tween } from '../src/shared/feel.ts';

test('easing starts at 0 and ends at 1; easeOutBack overshoots on the way', () => {
	for (const ease of [easeOutCubic, easeOutBack]) {
		assert.ok(Math.abs(ease(0)) < 1e-12);
		assert.ok(Math.abs(ease(1) - 1) < 1e-12);
	}
	assert.ok(Math.max(...Array.from({ length: 99 }, (_, i) => easeOutBack((i + 1) / 100))) > 1.05);
	assert.equal(tween(2, 4, -5, 300), 2);
	assert.equal(tween(2, 4, 900, 300), 4);
	assert.ok(tween(0, 1, 150, 300) > 0.5, 'fast start');
});

test('Perlin noise is smooth, bounded and differs per channel', () => {
	let biggestStep = 0;
	for (let x = 0; x < 50; x += 0.01) {
		const v = perlin(1, x);
		assert.ok(Math.abs(v) <= 1.0001, `${v} at ${x}`);
		biggestStep = Math.max(biggestStep, Math.abs(perlin(1, x + 0.01) - v));
	}
	assert.ok(biggestStep < 0.05, `noise jumps ${biggestStep} in 0.01`);
	assert.notEqual(perlin(1, 3.5), perlin(2, 3.5));
});

test('trauma caps at 1, decays 1 per second, and shakes by its square', () => {
	const t = new Trauma({ x: 1, y: 1, roll: 1 });
	t.add(0.6);
	t.add(0.6);
	assert.equal(t.value, 1);
	t.update(0.5);
	assert.ok(Math.abs(t.value - 0.5) < 1e-9);
	let peak = 0;
	for (let i = 0; i < 200; i += 1) {
		const s = new Trauma({ x: 1, y: 1, roll: 1 });
		s.add(0.5);
		s.update(i * 0.0037);
		peak = Math.max(peak, Math.abs(s.update(0).x));
	}
	assert.ok(peak <= 0.25 + 1e-9, `0.5 trauma shakes at most 0.5² = 0.25, saw ${peak}`);
	t.update(1);
	const still = t.update(0.016);
	assert.ok(still.x === 0 && still.y === 0 && still.roll === 0, 'no trauma, no shake');
});

test('a pool reuses released objects and warns when it has to grow', () => {
	let made = 0;
	const asleep = new Set();
	const pool = new Pool('things', 2, () => ({ n: made++ }), (o) => asleep.add(o));
	assert.equal(made, 2);
	const a = pool.take();
	const b = pool.take();
	const warn = console.warn;
	const warnings = [];
	console.warn = (m) => warnings.push(m);
	const c = pool.take();
	console.warn = warn;
	assert.equal(made, 3);
	assert.equal(warnings.length, 1);
	assert.equal(pool.active, 3);
	pool.release(a);
	pool.release(a);
	assert.equal(pool.active, 2);
	assert.equal(pool.take(), a, 'the released one comes back');
	assert.equal(made, 3);
	assert.ok(asleep.has(a) && b !== c);
});
