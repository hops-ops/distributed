// Generated game clients (`distributed game-client`) against Rust.
//
// `distributed_game/tests/fixtures/authoring.json` holds a sample surface and
// payloads encoded by the Rust derives; the TypeScript generated from that
// surface is pinned in `distributed_cli/tests/fixtures/game-client/sample/`.
// This test loads the generated files (Node strips their types) and checks
// they encode and decode byte-for-byte like Rust.
import assert from 'node:assert/strict';
import { cpSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { after, before, test } from 'node:test';

import { fromHex, toHex } from '@hops-ops/distributed/game';

import { predictModuleBytes } from './fixtures/predict-module.mjs';

const fixture = JSON.parse(
	readFileSync(new URL('../../distributed_game/tests/fixtures/authoring.json', import.meta.url), 'utf8')
);
const GOLDEN = fileURLToPath(new URL('../../distributed_cli/tests/fixtures/game-client/sample/', import.meta.url));

let dir;
let gen;
before(async () => {
	// Inside the package so `@hops-ops/distributed/game` resolves to this build.
	dir = mkdtempSync(fileURLToPath(new URL('./.generated-', import.meta.url)));
	cpSync(GOLDEN, dir, { recursive: true });
	gen = await import(pathToFileURL(`${dir}/index.ts`).href);
});
after(() => rmSync(dir, { recursive: true, force: true }));

const camel = (name) => name.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());

/** JSON fixture values → what the decoder returns (null → undefined, byte arrays). */
function normalize(value) {
	if (value instanceof Uint8Array) return [...value];
	if (value === null) return undefined;
	if (Array.isArray(value)) return value.map(normalize);
	if (typeof value === 'object') {
		const out = {};
		for (const [k, v] of Object.entries(value)) out[k] = normalize(v);
		return out;
	}
	return value;
}

test('input encoder, decoder and sanitize match the Rust derive', () => {
	const { value, hex, decoded, sanitized } = fixture.input;
	assert.equal(toHex(gen.encodeInput(value)), hex);
	assert.deepEqual(gen.decodeInput(fromHex(hex)), decoded);
	assert.deepEqual(gen.sanitizeInput(value), sanitized);
	assert.equal(gen.sanitizeInput({ ...value, power: Number.NaN }), undefined, 'non-finite floats reject');
	assert.deepEqual(gen.BUTTON, { dash: 1, attack: 2 });
});

test('every event encodes and decodes like Rust', () => {
	for (const sample of fixture.events) {
		const codec = gen.EVENTS[camel(sample.name)];
		assert.equal(codec.id, sample.id);
		const value = normalize(sample.value);
		const bytes = codec.encode(sample.value);
		assert.equal(toHex(bytes), sample.hex, `${sample.name} encodes`);
		const decoded = codec.decode(fromHex(sample.hex));
		assert.deepEqual(normalize(decoded), value, `${sample.name} decodes`);
		const any = gen.decodeEvent({ typeId: sample.id, payload: fromHex(sample.hex) });
		assert.equal(any.name, camel(sample.name));
	}
	assert.equal(gen.decodeEvent({ typeId: 999, payload: new Uint8Array() }), undefined);
	assert.throws(() => gen.EVENTS.hello.decode(fromHex('01')));
});

test('kinds, typed field accessors and correction defaults follow the surface', () => {
	const surface = fixture.surface;
	for (const kind of surface.kinds) assert.equal(gen.KIND[camel(kind.name)], kind.id);
	const fields = { x: 1.5, y: 2, vx: 0, face: 3, name: 'Ada', hp: 9, level: 2, dead: true, score: -1, raw: 0.5, blob: new Uint8Array([1]), kills: 4 };
	const player = gen.playerFields({ kind: 1, fields });
	assert.equal(player.name, 'Ada');
	assert.equal(player.dead, true);
	assert.deepEqual([...player.blob], [1]);
	assert.equal(gen.isPlayer({ kind: 1 }), true);
	assert.equal(gen.isStats({ kind: 1 }), false);
	assert.deepEqual(gen.statsFields({ fields: { slow_ms: 12, tick_ms: 33 } }), { slowMs: 12, tickMs: 33 });
	assert.deepEqual(gen.CORRECTIONS.player.x, { mode: 'ease', durationMs: 150 });
	assert.deepEqual(gen.CORRECTIONS.player.name, { mode: 'replace' });
	// Generated bounds are the exact binary32 values (the surface JSON spells them shortest).
	const x = surface.kinds[0].fields[0].type;
	assert.deepEqual(gen.FIELD_TYPES.player.x, { ...x, min: Math.fround(x.min), max: Math.fround(x.max) });
});

test('typed subscriptions decode only their own event', () => {
	const listeners = [];
	const source = { on: (type, listener) => (listeners.push([type, listener]), () => undefined) };
	const seen = [];
	gen.onEvent(source, 'ping', (value, zone) => seen.push([value, zone]));
	const ping = fixture.events.find((e) => e.name === 'hello');
	for (const [type, listener] of listeners) {
		assert.equal(type, 'event');
		listener('z1', { typeId: 9, payload: gen.EVENTS.ping.encode({ seq: 5 }) });
		listener('z1', { typeId: ping.id, payload: fromHex(ping.hex) });
	}
	assert.deepEqual(seen, [[{ seq: 5 }, 'z1']]);
});

test('the client factory wires the encoder and merged corrections', () => {
	const typed = gen.createSampleClient({
		url: () => 'ws://unused',
		ticket: async () => 'unused',
		prediction: { predict: (state) => state, corrections: { player: { y: { mode: 'snap' } } } }
	});
	assert.equal(typeof typed.client.sendInput, 'function');
	assert.equal(typed.send({ mx: 0, my: 0, buttons: 0, aim: 0, power: 0, viewTick: 0 }), undefined, 'not connected');
	typed.client.close();
});

test('the predicted step is described and wired in by default', async () => {
	const surface = fixture.surface;
	assert.equal(gen.PREDICT.export, surface.predict.export);
	assert.equal(gen.PREDICT.kind, 'player');
	// Wire field names of the kind, with the state record's plain types.
	assert.deepEqual(gen.PREDICT_STATE.map(([name]) => name), ['x', 'vx', 'hp', 'dead']);
	assert.deepEqual(gen.PREDICT_STATE.map(([, type]) => type.type), ['f32', 'f32', 'u8', 'bool']);
	assert.deepEqual(gen.PREDICT_CONTEXT.map(([name]) => name), ['speed', 'gusts']);
	assert.ok(gen.PREDICT_WASM_URL.href.endsWith('/wasm/sample.wasm'));

	// Default: the compiled step (here a stand-in module that echoes the state).
	const typed = gen.createSampleClient({ url: () => 'ws://unused', ticket: async () => 'unused', wasm: predictModuleBytes() });
	assert.ok(typed.prediction !== undefined);
	await typed.prediction.ready;
	typed.prediction.setContext({ speed: 2, gusts: 1 });
	const fields = { x: 1.25, vx: 0, hp: 5, dead: false, name: 'n' };
	assert.deepEqual(typed.prediction.predict(fields, { mx: 1, my: 0, buttons: 0, aim: 0, power: 0, viewTick: 0 }, 33), {
		x: 1.25,
		vx: 0,
		hp: 5,
		dead: false
	});
	typed.client.close();

	// `false` turns prediction off; a custom `predict` replaces the step.
	const off = gen.createSampleClient({ url: () => 'ws://unused', ticket: async () => 'unused', prediction: false });
	assert.equal(off.prediction, undefined);
	assert.equal(off.client.predictor, undefined);
	off.client.close();
});
