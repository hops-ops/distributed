/**
 * Native ↔ WebAssembly exact match of every demo's client-predicted step
 * and `#[game_pure]` function, and the cost of using them.
 *
 * `game-demos predict-trace` (the native build) prints, per step function,
 * recorded input streams with the state after every frame, and per pure
 * function recorded arguments with their results. Each surface's compiled
 * module (`src/generated/<surface>/wasm/<surface>.wasm`, built by
 * `distributed game-wasm`) replays the same streams and calls and must
 * produce the same bytes — every float bit included. The generated
 * `predict` (state ↔ entity fields, input encoder) and pure wrappers
 * (`createGridPures`) are checked over the same data; a `Predictor`
 * reconciling with 10 pending inputs and generating a room are timed.
 *
 * The code only uses operations that are correctly rounded on every target
 * (`+ - * /`, `sqrt`, `floor`, comparisons); see
 * `distributed_game::prediction` for the float rules (no platform
 * transcendentals).
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
	decodeValue,
	encodePayload,
	encodePredictState,
	encodeValue,
	fromHex,
	toHex
} from '@hops-ops/distributed/game';

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
	assert.deepEqual(surfaces.map((s) => s.name).sort(), ['freeze', 'mobpit']);
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

/** Every generated surface with pure functions. */
const pureSurfaces = readdirSync(GENERATED)
	.filter((name) => existsSync(new URL(`${name}/pures.ts`, GENERATED)))
	.map((name) => ({ name, pures: JSON.parse(readFileSync(new URL(`${name}/surface.json`, GENERATED), 'utf8')).pures }));

test('every pure function has a native trace and every trace a surface', () => {
	assert.deepEqual(pureSurfaces.map((s) => s.name), ['mobpit']);
	const declared = pureSurfaces.flatMap((s) => s.pures.map((p) => p.export)).sort();
	assert.deepEqual(traces.pures.map((p) => p.export).sort(), declared);
});

for (const { name, pures } of pureSurfaces) {
	for (const pure of pures) {
		test(`${name}: WASM ${pure.export} matches the native function bit for bit`, async () => {
			const module = await PredictModule.load(new URL(`${name}/wasm/${name}.wasm`, GENERATED));
			assert.ok(module.pures.includes(pure.export));
			const trace = traces.pures.find((p) => p.export === pure.export);
			for (const [i, call] of trace.calls.entries()) {
				assert.equal(toHex(module.call(pure.export, fromHex(call.arg))), call.output, `call ${i}`);
			}
			assert.ok(trace.calls.length >= 60, `${trace.calls.length} calls compared`);
		});
	}
}

test('mobpit: the generated pitRoom wrapper decodes what the native generation encodes', async () => {
	const generated = await import('../src/generated/mobpit/index.ts');
	const pures = generated.createMobpitPures();
	await pures.ready;
	const { arg, output } = pureType(generated, 'mobpit', 'pit_room');
	const trace = traces.pures.find((p) => p.export === 'pit_room');
	for (const call of trace.calls.slice(0, 12)) {
		const value = pures.pitRoom(decodeValue(arg, fromHex(call.arg)));
		assert.equal(toHex(encodeValue(output, value)), call.output);
	}
	const room = pures.pitRoom({ seed: generated.PIT_WORLD.defaultSeed, x: 0, y: 0 });
	assert.ok(room.biome < generated.PIT_WORLD.biomes.length);
	assert.ok(room.mobs.length >= 6);
});

/** Argument / output wire types of a pure function, from its surface (records resolved, camelCase fields). */
function pureType(generated, surfaceName, exportName) {
	const surface = JSON.parse(readFileSync(new URL(`${surfaceName}/surface.json`, GENERATED), 'utf8'));
	const camel = (n) => n.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
	const resolve = (type) => {
		if (type.type === 'record') {
			const record = surface.records.find((r) => r.name === type.name);
			return { type: 'record', fields: record.fields.map((f) => [camel(f.name), resolve(f.type)]) };
		}
		return type.item === undefined ? type : { ...type, item: resolve(type.item) };
	};
	const pure = surface.pures.find((p) => p.export === exportName);
	return { arg: resolve(pure.arg), output: resolve(pure.output) };
}

/** Median of `runs` timings of `fn` (each `iterations` calls), µs per call. */
function measure(fn, iterations, runs = 7) {
	for (let i = 0; i < iterations; i += 1) fn(i); // warm up (JIT tiers, wasm tier-up)
	const samples = [];
	for (let r = 0; r < runs; r += 1) {
		const start = process.hrtime.bigint();
		for (let i = 0; i < iterations; i += 1) fn(i);
		samples.push(Number(process.hrtime.bigint() - start) / 1000 / iterations);
	}
	samples.sort((a, b) => a - b);
	return samples[Math.floor(runs / 2)];
}

test('cost: generating a room in WASM', async () => {
	const generated = await import('../src/generated/mobpit/index.ts');
	const pures = generated.createMobpitPures();
	await pures.ready;
	const seed = generated.PIT_WORLD.defaultSeed;
	// A different room every call (no cache inside or outside the module).
	const roomUs = measure((i) => pures.pitRoom({ seed, x: (i % 97) - 48, y: Math.floor(i / 97) - 20 }), 2000);
	console.log(`room generation cost (Node ${process.version}, µs, median): ${JSON.stringify({ roomUs })}`);
	// The page builds up to 5 rooms per doorway; it must fit in a frame easily.
	assert.ok(roomUs < 500, `pit_room took ${roomUs} µs`);
});

test('cost: one predicted step and a reconciliation replaying 10 inputs at 30 Hz', async () => {
	const report = {};
	for (const name of ['mobpit', 'freeze']) {
		const generated = await import(`../src/generated/${name}/index.ts`);
		const prediction = generated[`create${name[0].toUpperCase()}${name.slice(1)}Prediction`]();
		await prediction.ready;
		const kind = { id: 1, name: generated.PREDICT.kind, priority: 1, fields: generated.PREDICT_STATE.map(([field]) => ({ name: field, type: 'f32', priority: 1 })) };
		const start = { x: -5, y: -5, vx: 0, vy: 0, dash: 0, cd: 0 };
		const input = { mx: 90, my: -40, buttons: 0 };

		// Raw ABI call (encoded bytes in, bytes out).
		const module = prediction.module();
		const stateBytes = encodePredictState(generated.PREDICT_STATE, start);
		const inputBytes = generated.encodeInput(input);
		const contextBytes = new Uint8Array(0);
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
