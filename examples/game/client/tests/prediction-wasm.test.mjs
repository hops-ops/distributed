/**
 * Native ↔ WebAssembly exact match of every demo's client-predicted step,
 * and the cost of prediction with it.
 *
 * `game-demos predict-trace` (the native build) prints, per step function,
 * recorded input streams with the state after every frame. Each surface's
 * compiled module (`src/generated/<surface>/wasm/<surface>.wasm`, built by
 * `distributed game-wasm`) replays the same streams and must produce the
 * same bytes — every float bit included. The generated `predict` (state ↔
 * entity fields, input encoder) is checked over the same streams, and a
 * `Predictor` reconciling with 10 pending inputs is timed.
 *
 * The steps only use operations that are correctly rounded on every target
 * (`+ - * /`, `sqrt`, comparisons); see `distributed_game::prediction` for
 * the float rules (no platform transcendentals).
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
	PredictModule,
	Predictor,
	decodePayload,
	decodePredictState,
	encodePayload,
	encodePredictState,
	fromHex,
	toHex
} from '@hops-ops/distributed/game';

import * as floor from '../src/arena/floor.ts';

const GENERATED = new URL('../src/generated/', import.meta.url);
const SERVER_DIR = fileURLToPath(new URL('../../server/', import.meta.url));

function nativeTraces() {
	const bin = process.env.GAME_DEMOS_BIN;
	const out = bin
		? execFileSync(bin, ['predict-trace'], { maxBuffer: 64 << 20 })
		: execFileSync('cargo', ['run', '-q', '--', 'predict-trace'], { cwd: SERVER_DIR, maxBuffer: 64 << 20 });
	return JSON.parse(String(out));
}

const traces = nativeTraces();
const DT = Math.fround(1 / 30);

/** Every generated surface with a predicted step. */
const surfaces = readdirSync(GENERATED)
	.filter((name) => existsSync(new URL(`${name}/predict.ts`, GENERATED)))
	.map((name) => {
		const surface = JSON.parse(readFileSync(new URL(`${name}/surface.json`, GENERATED), 'utf8'));
		return { name, predict: surface.predict };
	});

test('every predicting demo has a compiled step and a native trace', () => {
	assert.deepEqual(surfaces.map((s) => s.name).sort(), ['arena', 'freeze', 'grid', 'mobpit', 'ripple']);
	assert.equal(traces.abi, 1);
	for (const { name, predict } of surfaces) {
		assert.ok(existsSync(new URL(`${name}/wasm/${name}.wasm`, GENERATED)), `${name}: run \`run.sh gen\``);
		assert.ok(traces.steps.some((s) => s.export === predict.export), `${name}: trace for ${predict.export}`);
	}
	assert.deepEqual(
		traces.steps.map((s) => s.export).sort(),
		[...new Set(surfaces.map((s) => s.predict.export))].sort(),
		'every traced step is some surface\'s step'
	);
});

for (const { name, predict } of surfaces) {
	test(`${name}: WASM ${predict.export} matches the native step bit for bit`, async () => {
		const module = await PredictModule.load(new URL(`${name}/wasm/${name}.wasm`, GENERATED));
		assert.ok(module.steps.includes(predict.export));
		const trace = traces.steps.find((s) => s.export === predict.export);
		assert.equal(trace.kind, predict.kind);
		let frames = 0;
		for (const stream of trace.streams) {
			let state = fromHex(stream.start);
			const context = fromHex(stream.context);
			stream.inputs.forEach((input, i) => {
				state = module.step(predict.export, state, fromHex(input), context, DT);
				assert.equal(toHex(state), stream.states[i], `${stream.name} frame ${i}`);
				frames += 1;
			});
		}
		assert.ok(frames > 3000, `${frames} frames compared`);
	});

	test(`${name}: the generated prediction maps entity fields through the step`, async () => {
		const generated = await import(`../src/generated/${name}/index.ts`);
		const prediction = generated[`create${name[0].toUpperCase()}${name.slice(1)}Prediction`]();
		await prediction.ready;
		const trace = traces.steps.find((s) => s.export === predict.export);
		for (const stream of trace.streams.slice(0, 2)) {
			if (generated.PREDICT_CONTEXT !== undefined) {
				// The recorded context, decoded with the generated layout.
				prediction.setContext(decodePayload(generated.PREDICT_CONTEXT, fromHex(stream.context)));
			}
			let fields = decodePredictState(generated.PREDICT_STATE, fromHex(stream.start));
			stream.inputs.forEach((input, i) => {
				const next = prediction.predict(fields, generated.decodeInput(fromHex(input)), 1000 / 30);
				fields = { ...fields, ...next };
				assert.equal(toHex(encodePredictState(generated.PREDICT_STATE, fields)), stream.states[i], `${stream.name} frame ${i}`);
			});
		}
	});
}

test('drawing constants of the Arena floor match the simulation', () => {
	assert.equal(floor.HALF, traces.arena.half);
	assert.equal(floor.PLAYER_RADIUS, traces.arena.playerRadius);
	assert.equal(floor.DASH_COOLDOWN, traces.arena.dashCooldown);
	assert.deepEqual(floor.PILLARS.map((p) => [...p]), traces.arena.pillars);
});

/** Median of `runs` timings of `fn` (each `iterations` calls), µs per call. */
function measure(fn, iterations, runs = 7) {
	const samples = [];
	for (let r = 0; r < runs; r += 1) {
		const start = process.hrtime.bigint();
		for (let i = 0; i < iterations; i += 1) fn(i);
		samples.push(Number(process.hrtime.bigint() - start) / 1000 / iterations);
	}
	samples.sort((a, b) => a - b);
	return samples[Math.floor(runs / 2)];
}

test('cost: one predicted step and a reconciliation replaying 10 inputs at 30 Hz', async () => {
	const report = {};
	for (const name of ['arena', 'grid']) {
		const generated = await import(`../src/generated/${name}/index.ts`);
		const prediction = generated[`create${name[0].toUpperCase()}${name.slice(1)}Prediction`]();
		await prediction.ready;
		if (name === 'grid') prediction.setContext({ seed: 0x00c0ffee, cx: 3, cy: 2 });
		const kind = { id: 1, name: generated.PREDICT.kind, priority: 1, fields: generated.PREDICT_STATE.map(([field]) => ({ name: field, type: 'f32', priority: 1 })) };
		const start = name === 'grid' ? { x: 16, y: 16 } : { x: -5, y: -5, vx: 0, vy: 0, dash: 0, cd: 0 };
		const input = { mx: 90, my: -40, buttons: 0 };

		// Raw ABI call (encoded bytes in, bytes out).
		const module = prediction.module();
		const stateBytes = encodePredictState(generated.PREDICT_STATE, start);
		const inputBytes = generated.encodeInput(input);
		const contextBytes =
			name === 'grid' ? encodePayload(generated.PREDICT_CONTEXT, prediction.context()) : new Uint8Array(0);
		const abiUs = measure(() => module.step(generated.PREDICT.export, stateBytes, inputBytes, contextBytes, DT), 20_000);
		// `predict` as GameClient calls it (fields → bytes → step → fields).
		const predictUs = measure(() => prediction.predict(start, input, 1000 / 30), 20_000);

		// Reconcile: authoritative Delta + replay of 10 pending inputs.
		const predictor = new Predictor({ predict: prediction.predict });
		predictor.bind(7, kind, start);
		for (let seq = 1; seq <= 10; seq += 1) predictor.record(seq, input, 1000 / 30);
		const reconcileUs = measure((i) => predictor.reconcile(0, start, i), 5_000);
		report[name] = { abiUs, predictUs, reconcile10Us: reconcileUs };
	}
	// 30 Hz leaves 33 ms per frame; a reconciliation per Delta (20 Hz) plus
	// one step per input (30 Hz) must be a rounding error of it.
	console.log(`prediction cost (Node ${process.version}, µs, median): ${JSON.stringify(report)}`);
	for (const [name, r] of Object.entries(report)) {
		assert.ok(r.reconcile10Us < 1000, `${name}: reconcile with 10 pending took ${r.reconcile10Us} µs`);
		const perSecondMs = (r.reconcile10Us * 20 + r.predictUs * 30) / 1000;
		assert.ok(perSecondMs < 33, `${name}: ${perSecondMs} ms of prediction per second`);
	}
});
