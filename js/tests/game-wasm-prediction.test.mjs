// WebAssembly prediction host (`createWasmPrediction`, `PredictModule`):
// loading, the binary ABI round trip, errors, and the context gate. The
// demos' real Rust steps are checked bit for bit against native in
// `examples/game/client/tests/prediction-wasm.test.mjs`.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import test from 'node:test';

import {
	PredictAbiError,
	PredictModule,
	Predictor,
	createWasmPrediction,
	decodePredictState,
	encodePredictState,
	loadPredictModule
} from '@hops-ops/distributed/game';

import { predictModuleBytes } from './fixtures/predict-module.mjs';

const STATE = [
	['x', { type: 'f32' }],
	['hp', { type: 'u8' }],
	['dead', { type: 'bool' }]
];

test('state layouts encode entity fields losslessly as binary32', () => {
	const fields = { x: Math.fround(0.1), hp: 7, dead: true, name: 'ignored' };
	const bytes = encodePredictState(STATE, fields);
	assert.equal(bytes.length, 4 + 1 + 1);
	assert.deepEqual(decodePredictState(STATE, bytes), { x: Math.fround(0.1), hp: 7, dead: true });
});

test('a module runs steps over the scratch buffer and reports ABI errors', async () => {
	const module = await PredictModule.load(predictModuleBytes({ echo: 'echo' }));
	assert.deepEqual([...module.steps].sort(), ['echo', 'fail']);
	const state = new Uint8Array([1, 2, 3, 4]);
	const next = module.step('echo', state, new Uint8Array([9]), new Uint8Array([8, 8]), 1 / 30);
	assert.deepEqual([...next], [1, 2, 3, 4]);
	assert.notEqual(next.buffer, state.buffer, 'a copy, not a view of wasm memory');
	assert.throws(() => module.step('fail', state, state, state, 0), (e) => e instanceof PredictAbiError && e.code === -3);
	assert.throws(() => module.step('missing', state, state, state, 0), /no step dg_predict_missing/);
	assert.throws(() => module.step('echo', new Uint8Array(70_000), state, state, 0), (e) => e.code === -1);
});

test('modules with another ABI version or without the exports are refused', async () => {
	await assert.rejects(loadPredictModule(predictModuleBytes({ abi: 2 })), /ABI v2/);
	await assert.rejects(
		loadPredictModule(new Uint8Array([0x00, 0x61, 0x73, 0x6d, 1, 0, 0, 0])),
		/not a distributed_game prediction module/
	);
});

test('file: URLs load in Node and are instantiated once', async () => {
	const dir = mkdtempSync(join(tmpdir(), 'predict-'));
	try {
		const path = join(dir, 'step.wasm');
		writeFileSync(path, predictModuleBytes());
		const url = pathToFileURL(path);
		const [a, b] = await Promise.all([loadPredictModule(url), loadPredictModule(url.href)]);
		assert.equal(a, b, 'cached by URL');
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test('createWasmPrediction is a no-op until loaded and (if needed) given a context', async () => {
	const prediction = createWasmPrediction({
		source: predictModuleBytes(),
		exportName: 'sample_step',
		state: STATE,
		context: [['speed', { type: 'f32' }]],
		encodeInput: (input) => new Uint8Array([input.dx & 0xff])
	});
	const fields = { x: 1.5, hp: 3, dead: false };
	assert.deepEqual(prediction.predict(fields, { dx: 1 }, 33), {}, 'not loaded yet');
	await prediction.ready;
	assert.ok(prediction.loaded());
	assert.deepEqual(prediction.predict(fields, { dx: 1 }, 33), {}, 'no context yet');
	prediction.setContext({ speed: 2 });
	assert.deepEqual(prediction.context(), { speed: 2 });
	assert.deepEqual(prediction.predict(fields, { dx: 1 }, 33), { x: 1.5, hp: 3, dead: false });
	assert.equal(prediction.steps(), 1);

	// Drives a Predictor like GameClient does.
	const kind = { id: 1, name: 'player', priority: 1, fields: STATE.map(([name]) => ({ name, type: 'f32', priority: 1 })) };
	const predictor = new Predictor({ predict: prediction.predict });
	predictor.bind(5, kind, fields);
	for (let seq = 1; seq <= 10; seq += 1) predictor.record(seq, { dx: 1 }, 33);
	predictor.reconcile(4, { x: 2, hp: 3, dead: false }, 0);
	assert.equal(predictor.pending.length, 6);
	assert.equal(prediction.steps(), 1 + 10 + 6);
	assert.equal(predictor.predicted().x, 2);
});

test('a module that does not load is reported and prediction stays off', async () => {
	const errors = [];
	const prediction = createWasmPrediction({
		source: new Uint8Array([1, 2, 3]),
		exportName: 'sample_step',
		state: STATE,
		encodeInput: () => new Uint8Array(0),
		onError: (e) => errors.push(e)
	});
	await assert.rejects(prediction.ready);
	assert.equal(errors.length, 1);
	assert.deepEqual(prediction.predict({ x: 1 }, {}, 33), {});
	const missing = createWasmPrediction({
		source: predictModuleBytes({ echo: 'other' }),
		exportName: 'sample_step',
		state: STATE,
		encodeInput: () => new Uint8Array(0),
		onError: () => undefined
	});
	await assert.rejects(missing.ready, /no step dg_predict_sample_step/);
});
