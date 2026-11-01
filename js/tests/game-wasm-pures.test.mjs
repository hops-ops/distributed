// Pure functions compiled to WebAssembly (`createWasmPures`,
// `PredictModule.call`): the `dg_pure_<fn>` ABI, typed wrappers, errors and
// the load gate. The demos' real Rust functions are checked bit for bit
// against native in `examples/game/client/tests/prediction-wasm.test.mjs`.
import assert from 'node:assert/strict';
import test from 'node:test';

import { PredictAbiError, PredictModule, createWasmPures, decodeValue, encodeValue } from '@hops-ops/distributed/game';

import { predictModuleBytes } from './fixtures/predict-module.mjs';

const POINT = { type: 'record', fields: [['x', { type: 'f32' }], ['label', { type: 'short_str' }]] };

test('a module calls pure functions over the scratch buffer', async () => {
	const module = await PredictModule.load(predictModuleBytes({ echo: 'step', pures: ['echo', 'same'] }));
	assert.deepEqual([...module.pures].sort(), ['echo', 'same']);
	assert.deepEqual([...module.steps].sort(), ['fail', 'step'], 'pure functions are not steps');
	const arg = encodeValue(POINT, { x: 0.1, label: 'a' });
	const out = module.call('echo', arg);
	assert.deepEqual([...out], [...arg]);
	assert.notEqual(out.buffer, arg.buffer, 'a copy, not a view of wasm memory');
	assert.deepEqual(decodeValue(POINT, out), { x: Math.fround(0.1), label: 'a' });
	assert.equal(module.call('echo', new Uint8Array(0)).length, 0);
	assert.throws(() => module.call('missing', arg), /no pure function dg_pure_missing/);
	assert.throws(
		() => module.call('echo', new Uint8Array(70_000)),
		(e) => e instanceof PredictAbiError && e.code === -1 && /dg_pure_echo/.test(e.message)
	);
});

test('createWasmPures wraps each function with its types and gates on loading', async () => {
	const pures = createWasmPures(predictModuleBytes({ pures: ['mirror', 'list'] }), {
		mirror: { exportName: 'mirror', arg: POINT, output: POINT },
		list: { exportName: 'list', arg: { type: 'list', item: { type: 'u16' } }, output: { type: 'list', item: { type: 'u16' } } }
	});
	assert.equal(pures.loaded(), false);
	assert.throws(() => pures.mirror({ x: 1, label: '' }), /before its module loaded/);
	await pures.ready;
	assert.equal(pures.loaded(), true);
	assert.ok(pures.module() instanceof PredictModule);
	assert.deepEqual(pures.mirror({ x: 1.5, label: 'z' }), { x: 1.5, label: 'z' });
	assert.deepEqual(pures.list([1, 300, 65535]), [1, 300, 65535]);
});

test('a module without a wrapped function rejects ready; reserved names are refused', async () => {
	const missing = createWasmPures(predictModuleBytes({ pures: ['a'] }), {
		b: { exportName: 'b', output: { type: 'u8' } }
	});
	await assert.rejects(missing.ready, /no pure function dg_pure_b/);
	assert.equal(missing.loaded(), false);
	assert.throws(
		() => createWasmPures(predictModuleBytes(), { ready: { exportName: 'ready', output: { type: 'u8' } } }),
		/reserved/
	);
});
