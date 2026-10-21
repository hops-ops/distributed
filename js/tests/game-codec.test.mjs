import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
	GameClientMirror,
	GameDecodeError,
	WireReader,
	WireWriter,
	decodeClientMessage,
	decodeServerMessage,
	defineGameSchema,
	dequantizeAngle,
	dequantizeLinear,
	encodeClientMessage,
	encodeServerMessage,
	frameStream,
	fromHex,
	quantizeAngle,
	quantizeLinear,
	toHex,
	unframeStream
} from '@hops-ops/distributed/game';

// Golden fixtures produced by the Rust crate (`cargo test --test golden`).
const FIXTURES = new URL('../../distributed_game/tests/fixtures/', import.meta.url);
const fixture = (name) => JSON.parse(readFileSync(new URL(name, FIXTURES), 'utf8'));

function f32FromBits(bits) {
	const view = new DataView(new ArrayBuffer(4));
	view.setUint32(0, Number.parseInt(bits, 16));
	return view.getFloat32(0);
}

function bitsOfF32(value) {
	const view = new DataView(new ArrayBuffer(4));
	view.setFloat32(0, value);
	return view.getUint32(0).toString(16).padStart(8, '0');
}

function schemaFromJson(kinds) {
	return defineGameSchema(
		kinds.map((kind) => ({
			id: kind.id,
			name: kind.name,
			fields: kind.fields.map((field) =>
				field.type === 'quant_f32'
					? {
							name: field.name,
							type: field.type,
							min: f32FromBits(field.min.bits),
							max: f32FromBits(field.max.bits),
							bits: field.bits
						}
					: field.type === 'angle'
						? { name: field.name, type: field.type, bits: field.bits }
						: { name: field.name, type: field.type }
			)
		}))
	);
}

function schemaToJson(schema) {
	return schema.kinds.map((kind) => ({
		fields: kind.fields.map((field) => {
			if (field.type === 'quant_f32') {
				return {
					bits: field.bits,
					max: { bits: bitsOfF32(field.max), f32: field.max },
					min: { bits: bitsOfF32(field.min), f32: field.min },
					name: field.name,
					type: field.type
				};
			}
			if (field.type === 'angle') return { bits: field.bits, name: field.name, type: field.type };
			return { name: field.name, type: field.type };
		}),
		id: kind.id,
		name: kind.name
	}));
}

function valueFromJson(tagged) {
	if ('bool' in tagged) return tagged.bool;
	if ('u' in tagged) return tagged.u;
	if ('i' in tagged) return tagged.i;
	if ('bits' in tagged) return f32FromBits(tagged.bits);
	if ('str' in tagged) return tagged.str;
	if ('bytes' in tagged) return fromHex(tagged.bytes);
	throw new Error(`unknown tagged value ${JSON.stringify(tagged)}`);
}

function valueToJson(field, value) {
	switch (field.type) {
		case 'bool':
			return { bool: value };
		case 'u8':
		case 'u16':
		case 'u32':
			return { u: value };
		case 'i32':
			return { i: value };
		case 'f32':
		case 'quant_f32':
		case 'angle':
			return { bits: bitsOfF32(value) };
		case 'short_str':
			return { str: value };
		case 'bytes':
			return { bytes: toHex(value) };
	}
}

/** Compare tagged values, floats by exact bit pattern only. */
function normalizeTagged(tagged) {
	return 'bits' in tagged ? { bits: tagged.bits } : tagged;
}

function kindsFor(entry) {
	const schema = schemaFromJson(entry.schema);
	const known = new Map((entry.known_entities ?? []).map((e) => [e.id, e.kind]));
	return { schema, kindOf: (id) => known.get(id) };
}

function messageFromJson(decoded, schema) {
	switch (decoded.type) {
		case 'Hello':
			return {
				type: 'Hello',
				protocolVersion: decoded.protocol_version,
				contentVersion: decoded.content_version,
				capabilities: decoded.capabilities,
				ticket: decoded.ticket,
				reconnectToken: decoded.reconnect_token,
				near: decoded.near
			};
		case 'Input':
			return {
				type: 'Input',
				seq: decoded.seq,
				clientTick: decoded.client_tick,
				payload: fromHex(decoded.payload)
			};
		case 'Ping':
			return { type: 'Ping', clientTime: decoded.client_time };
		case 'Leave':
			return { type: 'Leave' };
		case 'Content':
			return { type: 'Content', contentVersion: decoded.content_version };
		case 'Near':
			return { type: 'Near', near: decoded.near };
		case 'Welcome':
			return {
				type: 'Welcome',
				sessionId: decoded.session_id,
				zoneId: decoded.zone_id,
				role: decoded.role,
				tickRate: decoded.tick_rate,
				patchRate: decoded.patch_rate,
				serverTick: decoded.server_tick,
				serverTime: decoded.server_time,
				reconnectToken: decoded.reconnect_token,
				contentVersion: decoded.content_version,
				schema: schemaFromJson(decoded.schema)
			};
		case 'Delta':
			return {
				type: 'Delta',
				serverTick: decoded.server_tick,
				ackSeq: decoded.ack_seq,
				removes: decoded.removes,
				creates: decoded.creates.map((c) => ({
					id: c.id,
					kind: c.kind,
					fields: c.fields.map(valueFromJson)
				})),
				updates: decoded.updates.map((u) => ({
					id: u.id,
					mask: u.mask,
					fields: u.fields.map(valueFromJson)
				}))
			};
		case 'Event':
			return { type: 'Event', typeId: decoded.type_id, payload: fromHex(decoded.payload) };
		case 'Pong':
			return { type: 'Pong', clientTime: decoded.client_time, serverTime: decoded.server_time };
		case 'Freeze':
		case 'Resume':
			return {
				type: decoded.type,
				reason: decoded.reason,
				version: decoded.version,
				notes: decoded.notes
			};
		case 'Handoff':
			return {
				type: 'Handoff',
				targetZone: decoded.target_zone,
				transferTicket: decoded.transfer_ticket
			};
		case 'Kick':
		case 'Error':
			return { type: decoded.type, code: decoded.code, message: decoded.message };
		case 'Redirect':
			return { type: 'Redirect', zoneId: decoded.zone_id, url: decoded.url };
	}
	throw new Error(`unhandled ${decoded.type} (${schema})`);
}

function messageToJson(message, kinds) {
	switch (message.type) {
		case 'Hello':
			return {
				capabilities: message.capabilities,
				content_version: message.contentVersion,
				near: message.near,
				protocol_version: message.protocolVersion,
				reconnect_token: message.reconnectToken,
				ticket: message.ticket,
				type: 'Hello'
			};
		case 'Input':
			return {
				client_tick: message.clientTick,
				payload: toHex(message.payload),
				seq: message.seq,
				type: 'Input'
			};
		case 'Ping':
			return { client_time: message.clientTime, type: 'Ping' };
		case 'Leave':
			return { type: 'Leave' };
		case 'Content':
			return { content_version: message.contentVersion, type: 'Content' };
		case 'Near':
			return { near: message.near, type: 'Near' };
		case 'Welcome':
			return {
				content_version: message.contentVersion,
				patch_rate: message.patchRate,
				reconnect_token: message.reconnectToken,
				role: message.role,
				schema: schemaToJson(message.schema),
				server_tick: message.serverTick,
				server_time: message.serverTime,
				session_id: message.sessionId,
				tick_rate: message.tickRate,
				type: 'Welcome',
				zone_id: message.zoneId
			};
		case 'Delta': {
			const created = new Map(message.creates.map((c) => [c.id, c.kind]));
			const kindDef = (id) =>
				kinds.schema.byId.get(created.get(id) ?? kinds.kindOf(id));
			return {
				ack_seq: message.ackSeq,
				creates: message.creates.map((c) => ({
					fields: c.fields.map((v, i) => valueToJson(kinds.schema.byId.get(c.kind).fields[i], v)),
					id: c.id,
					kind: c.kind
				})),
				removes: message.removes,
				server_tick: message.serverTick,
				type: 'Delta',
				updates: message.updates.map((u) => {
					const fields = kindDef(u.id).fields.filter((_, i) => (u.mask >>> i) & 1);
					return {
						fields: u.fields.map((v, i) => valueToJson(fields[i], v)),
						id: u.id,
						mask: u.mask
					};
				})
			};
		}
		case 'Event':
			return { payload: toHex(message.payload), type: 'Event', type_id: message.typeId };
		case 'Pong':
			return { client_time: message.clientTime, server_time: message.serverTime, type: 'Pong' };
		case 'Freeze':
		case 'Resume':
			return { notes: message.notes, reason: message.reason, type: message.type, version: message.version };
		case 'Handoff':
			return {
				target_zone: message.targetZone,
				transfer_ticket: message.transferTicket,
				type: 'Handoff'
			};
		case 'Kick':
		case 'Error':
			return { code: message.code, message: message.message, type: message.type };
		case 'Redirect':
			return { type: 'Redirect', url: message.url, zone_id: message.zoneId };
	}
}

/** Deep-normalize a fixture `decoded` value: floats compare by bits only. */
function normalizeDecoded(value) {
	if (Array.isArray(value)) return value.map(normalizeDecoded);
	if (value !== null && typeof value === 'object') {
		if ('bits' in value && 'f32' in value) return { bits: value.bits };
		const out = {};
		for (const key of Object.keys(value).sort()) out[key] = normalizeDecoded(value[key]);
		return out;
	}
	return value;
}

test('primitives match the Rust golden bytes in both directions', () => {
	const { cases } = fixture('primitives.json');
	assert.ok(cases.length >= 20);
	for (const c of cases) {
		const w = new WireWriter();
		const value = typeof c.value === 'object' && c.value !== null ? f32FromBits(c.value.bits) : c.value;
		switch (c.kind) {
			case 'varuint':
				w.varuint(value);
				break;
			case 'varint':
				w.varint(value);
				break;
			case 'f32':
				w.f32(value);
				break;
			case 'f64':
				w.f64(value);
				break;
			case 'string':
				w.string(value);
				break;
			case 'short_string':
				w.shortString(value);
				break;
			default:
				assert.fail(`unknown primitive kind ${c.kind}`);
		}
		assert.equal(toHex(w.finish()), c.hex, `${c.kind} ${JSON.stringify(c.value)}`);

		const r = new WireReader(fromHex(c.hex));
		const decoded = {
			varuint: () => r.varuint(),
			varint: () => r.varint(),
			f32: () => r.f32(),
			f64: () => r.f64(),
			string: () => r.string(),
			short_string: () => r.shortString()
		}[c.kind]();
		r.finish();
		if (c.kind === 'f32') {
			assert.equal(bitsOfF32(decoded), c.value.bits);
		} else {
			assert.ok(Object.is(decoded, value), `${c.kind} decode ${c.hex}`);
		}
	}
});

test('quantization reproduces the Rust integers and binary32 results exactly', () => {
	const { cases } = fixture('quantization.json');
	assert.ok(cases.length > 30);
	for (const c of cases) {
		const input = f32FromBits(c.input_bits);
		let q;
		let back;
		if (c.field.type === 'quant_f32') {
			const min = f32FromBits(c.field.min.bits);
			const max = f32FromBits(c.field.max.bits);
			q = quantizeLinear(input, min, max, c.field.bits);
			back = dequantizeLinear(q, min, max, c.field.bits);
		} else {
			q = quantizeAngle(input, c.field.bits);
			back = dequantizeAngle(q, c.field.bits);
		}
		assert.equal(q, c.q, `${c.field.type} input ${c.input_bits}`);
		assert.equal(toHex(new WireWriter().varuint(q).finish()), c.hex);
		assert.equal(bitsOfF32(back), c.decoded.bits, `${c.field.type} decode q=${c.q}`);
	}
});

/** Rust message fixtures, including the additive v1 Redirect and Kick RELOAD. */
function allMessages() {
	return fixture('messages.json').messages;
}

test('every message fixture encodes and decodes byte-exactly', () => {
	const messages = allMessages();
	const names = messages.map((m) => m.name);
	for (const required of ['hello', 'hello_reconnect', 'input', 'ping', 'leave', 'welcome', 'delta', 'delta_empty', 'event', 'pong', 'freeze', 'resume', 'handoff', 'kick', 'error', 'kick_reload', 'redirect', 'hello_near', 'near', 'passive']) {
		assert.ok(names.includes(required), `fixture ${required}`);
	}
	for (const entry of messages) {
		const kinds = entry.schema === undefined ? undefined : kindsFor(entry);
		const message = messageFromJson(entry.decoded);
		const bytes =
			entry.direction === 'client_to_server'
				? encodeClientMessage(message)
				: encodeServerMessage(message, kinds);
		assert.equal(toHex(bytes), entry.hex, `encode ${entry.name}`);

		const decoded =
			entry.direction === 'client_to_server'
				? decodeClientMessage(fromHex(entry.hex))
				: decodeServerMessage(fromHex(entry.hex), kinds);
		assert.deepEqual(
			normalizeDecoded(messageToJson(decoded, kinds)),
			normalizeDecoded(entry.decoded),
			`decode ${entry.name}`
		);
	}
});

test('client mirror applies the golden delta like Rust ClientMirror', () => {
	const { messages } = fixture('messages.json');
	const welcome = messages.find((m) => m.name === 'welcome');
	const delta = messages.find((m) => m.name === 'delta');
	const mirror = new GameClientMirror();
	const hello = mirror.apply(fromHex(welcome.hex));
	assert.equal(hello.type, 'Welcome');
	// Seed the entities the delta fixture assumes are known.
	mirror.entities.set(5, { kind: 1, fields: [true, 1, 10, 0, 0, 0, 0, 0, 'x', new Uint8Array()] });
	mirror.entities.set(6, { kind: 2, fields: [1] });
	mirror.entities.set(9, { kind: 2, fields: [2] });
	mirror.apply(fromHex(delta.hex));
	assert.equal(mirror.serverTick, 200);
	assert.equal(mirror.ackSeq, 17);
	assert.equal(mirror.entities.has(9), false);
	assert.equal(mirror.entities.get(5).fields[2], 299);
	assert.equal(bitsOfF32(mirror.entities.get(5).fields[6]), '3ac800c8');
	assert.equal(mirror.entities.get(6).fields[0], 64);
	assert.equal(mirror.entities.get(300).fields[8], 'Ana');
	assert.equal(mirror.entities.get(301).fields[0], 0);
});

test('decoders reject malformed input like the Rust reference', () => {
	const reject = (hex, code, kinds) =>
		assert.throws(
			() => decodeServerMessage(fromHex(hex), kinds),
			(error) => error instanceof GameDecodeError && error.code === code,
			hex
		);
	reject('ffffffff10', 'UNKNOWN_MESSAGE');
	reject('8203027870ff', 'TRAILING_BYTES');
	reject('8203057870', 'UNEXPECTED_EOF');
	assert.throws(() => new WireReader(fromHex('ffffffff10')).varuint(), /VARINT_OVERFLOW/);
	assert.throws(() => new WireReader(fromHex('02')).bool(), /INVALID/);
	assert.throws(() => new WireReader(fromHex('02c328')).string(), /INVALID_UTF8/);
	assert.throws(() => decodeClientMessage(fromHex('07')), /UNKNOWN_MESSAGE/);
	// A delta update for an unknown entity id.
	const { messages } = fixture('messages.json');
	const delta = messages.find((m) => m.name === 'delta');
	const schema = kindsFor(delta).schema;
	reject(delta.hex, 'UNKNOWN_ENTITY', { schema, kindOf: () => undefined });
	// A mask bit beyond the kind's fields (orb has 1 field): id 6, mask 0b10.
	reject('812b0000000106020000', 'INVALID', { schema, kindOf: () => 2 });
	// Short strings truncate on a UTF-8 boundary at 255 bytes.
	const long = 'é'.repeat(200);
	const bytes = new WireWriter().shortString(long).finish();
	assert.equal(bytes[0], 254);
	assert.equal(new WireReader(bytes).shortString(), 'é'.repeat(127));
	// Stream framing round-trips.
	const framed = frameStream(fromHex('04'));
	assert.deepEqual([...framed], [1, 4]);
	assert.deepEqual(unframeStream(framed), { frame: fromHex('04'), consumed: 2 });
	assert.equal(unframeStream(fromHex('05')), undefined);
});
