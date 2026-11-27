/**
 * Entity schema, field types and quantization (`PROTOCOL.md` §4–§5).
 *
 * Quantization math runs in binary64 in exactly the order the spec writes it
 * so this client reproduces the Rust server's integers and decoded binary32
 * values bit-for-bit (verified against the golden fixtures).
 */
import { GameDecodeError, type WireReader, type WireWriter } from './wire.js';

/** Maximum fields per kind (update masks are 32-bit). */
export const MAX_FIELDS = 32;

/** 2π as used by angle quantization. */
export const TAU = 6.283185307179586;

export type FieldTypeName =
	| 'bool'
	| 'u8'
	| 'u16'
	| 'u32'
	| 'i32'
	| 'f32'
	| 'quant_f32'
	| 'angle'
	| 'short_str'
	| 'bytes';

const TYPE_TAGS: readonly FieldTypeName[] = [
	'bool',
	'u8',
	'u16',
	'u32',
	'i32',
	'f32',
	'quant_f32',
	'angle',
	'short_str',
	'bytes'
];

/** One component field. `min`/`max`/`bits` are present for quantized types. */
export type GameFieldDef = Readonly<{
	name: string;
	type: FieldTypeName;
	/** `quant_f32` lower bound (a binary32 value). */
	min?: number;
	/** `quant_f32` upper bound (a binary32 value). */
	max?: number;
	/** `quant_f32` / `angle` precision, 1..=32. */
	bits?: number;
}>;

export type GameKindDef = Readonly<{
	id: number;
	name: string;
	fields: readonly GameFieldDef[];
}>;

/** Decoded schema from a Welcome. */
export type GameSchema = Readonly<{
	kinds: readonly GameKindDef[];
	/** Kind lookup by id. */
	byId: ReadonlyMap<number, GameKindDef>;
}>;

/**
 * A decoded field value: `boolean` (bool), `number` (integers and floats),
 * `string` (short_str) or `Uint8Array` (bytes).
 */
export type FieldValue = boolean | number | string | Uint8Array;

/** Build a schema (validates ids, field counts and quantization params). */
export function defineGameSchema(kinds: readonly GameKindDef[]): GameSchema {
	const byId = new Map<number, GameKindDef>();
	const frozen: GameKindDef[] = [];
	for (const kind of kinds) {
		if (!Number.isInteger(kind.id) || kind.id < 0 || kind.id > 0xffff) {
			throw new GameDecodeError('INVALID', 'kind id > u16');
		}
		if (byId.has(kind.id)) throw new GameDecodeError('INVALID', 'duplicate kind id');
		if (kind.fields.length > MAX_FIELDS) {
			throw new GameDecodeError('INVALID', 'more than 32 fields');
		}
		const fields = kind.fields.map((field) => {
			if (!TYPE_TAGS.includes(field.type)) {
				throw new GameDecodeError('UNKNOWN_FIELD_TYPE', field.type);
			}
			if (field.type === 'quant_f32' || field.type === 'angle') checkBits(field.bits);
			if (field.type === 'quant_f32') {
				return Object.freeze({
					name: field.name,
					type: field.type,
					min: Math.fround(field.min ?? 0),
					max: Math.fround(field.max ?? 0),
					bits: field.bits
				});
			}
			if (field.type === 'angle') {
				return Object.freeze({ name: field.name, type: field.type, bits: field.bits });
			}
			return Object.freeze({ name: field.name, type: field.type });
		});
		const def = Object.freeze({ id: kind.id, name: kind.name, fields: Object.freeze(fields) });
		byId.set(kind.id, def);
		frozen.push(def);
	}
	return Object.freeze({ kinds: Object.freeze(frozen), byId });
}

function checkBits(bits: number | undefined): asserts bits is number {
	if (bits === undefined || !Number.isInteger(bits) || bits < 1 || bits > 32) {
		throw new GameDecodeError('INVALID', 'quantization bits must be 1..=32');
	}
}

const maxQ = (bits: number): number => 2 ** bits - 1;
const steps = (bits: number): number => 2 ** bits;

/** Clamped linear quantization (§4.1). `value` is widened from binary32. */
export function quantizeLinear(value: number, min: number, max: number, bits: number): number {
	const v = Math.fround(value);
	const lo = Math.fround(min);
	const hi = Math.fround(max);
	const mq = maxQ(bits);
	if (hi <= lo) return 0;
	let x = Number.isNaN(v) ? lo : v;
	x = Math.min(Math.max(x, lo), hi);
	const t = (x - lo) / (hi - lo);
	return Math.min(Math.floor(t * mq + 0.5), mq);
}

/** Inverse of {@link quantizeLinear}. */
export function dequantizeLinear(q: number, min: number, max: number, bits: number): number {
	const lo = Math.fround(min);
	const hi = Math.fround(max);
	return Math.fround(lo + (q / maxQ(bits)) * (hi - lo));
}

/** Angle quantization (§4.2): radians wrapped to [0, 2π). */
export function quantizeAngle(value: number, bits: number): number {
	let a = Math.fround(value);
	if (!Number.isFinite(a)) return 0;
	const s = steps(bits);
	a %= TAU;
	if (a < 0) a += TAU;
	return Math.floor((a / TAU) * s + 0.5) % s;
}

/** Inverse of {@link quantizeAngle}. */
export function dequantizeAngle(q: number, bits: number): number {
	return Math.fround((q * TAU) / steps(bits));
}

function numeric(value: FieldValue): number {
	if (typeof value === 'boolean') return value ? 1 : 0;
	if (typeof value === 'number') return value;
	return 0;
}

function saturate(n: number, lo: number, hi: number): number {
	// Rust `f64 as uN` semantics: NaN → 0, saturating, truncating.
	if (Number.isNaN(n)) return 0;
	return Math.trunc(Math.min(Math.max(n, lo), hi));
}

/** Encode one field value, coercing like the server (§4.3). */
export function encodeField(w: WireWriter, field: GameFieldDef, value: FieldValue): void {
	const n = numeric(value);
	switch (field.type) {
		case 'bool':
			w.bool(n !== 0); // Rust `n != 0.0`: NaN is true
			return;
		case 'u8':
			w.u8(saturate(n, 0, 255));
			return;
		case 'u16':
			w.varuint(saturate(n, 0, 65535));
			return;
		case 'u32':
			w.varuint(saturate(n, 0, 0xffff_ffff));
			return;
		case 'i32':
			w.varint(saturate(n, -0x8000_0000, 0x7fff_ffff));
			return;
		case 'f32':
			w.f32(n);
			return;
		case 'quant_f32':
			w.varuint(quantizeLinear(n, field.min!, field.max!, field.bits!));
			return;
		case 'angle':
			w.varuint(quantizeAngle(n, field.bits!));
			return;
		case 'short_str':
			w.shortString(typeof value === 'string' ? value : '');
			return;
		case 'bytes':
			w.bytes(
				value instanceof Uint8Array
					? value
					: typeof value === 'string'
						? new TextEncoder().encode(value)
						: new Uint8Array(0)
			);
			return;
	}
}

/** Decode one field value. */
export function decodeField(r: WireReader, field: GameFieldDef): FieldValue {
	switch (field.type) {
		case 'bool':
			return r.bool();
		case 'u8':
			return r.u8();
		case 'u16': {
			const v = r.varuint();
			if (v > 65535) throw new GameDecodeError('INVALID', 'u16 out of range');
			return v;
		}
		case 'u32':
			return r.varuint();
		case 'i32':
			return r.varint();
		case 'f32':
			return r.f32();
		case 'quant_f32': {
			const q = r.varuint();
			if (q > maxQ(field.bits!)) {
				throw new GameDecodeError('INVALID', 'quantized value out of range');
			}
			return dequantizeLinear(q, field.min!, field.max!, field.bits!);
		}
		case 'angle': {
			const q = r.varuint();
			if (q >= steps(field.bits!)) throw new GameDecodeError('INVALID', 'angle out of range');
			return dequantizeAngle(q, field.bits!);
		}
		case 'short_str':
			return r.shortString();
		case 'bytes':
			return r.bytes();
	}
}

/**
 * The value a client will decode for `value` — what the server will send
 * after quantization. Use it in `predict` so predicted state matches the
 * authoritative wire precision.
 */
export function canonicalizeField(field: GameFieldDef, value: FieldValue): FieldValue {
	switch (field.type) {
		case 'quant_f32':
			return dequantizeLinear(
				quantizeLinear(numeric(value), field.min!, field.max!, field.bits!),
				field.min!,
				field.max!,
				field.bits!
			);
		case 'angle':
			return dequantizeAngle(quantizeAngle(numeric(value), field.bits!), field.bits!);
		case 'f32':
			return Math.fround(numeric(value));
		case 'bool':
			return numeric(value) !== 0;
		case 'u8':
			return saturate(numeric(value), 0, 255);
		case 'u16':
			return saturate(numeric(value), 0, 65535);
		case 'u32':
			return saturate(numeric(value), 0, 0xffff_ffff);
		case 'i32':
			return saturate(numeric(value), -0x8000_0000, 0x7fff_ffff);
		default:
			return value;
	}
}

/** Serialize a schema (§5). */
export function encodeSchema(w: WireWriter, schema: GameSchema): void {
	w.varuint(schema.kinds.length);
	for (const kind of schema.kinds) {
		w.varuint(kind.id).string(kind.name).varuint(kind.fields.length);
		for (const field of kind.fields) {
			w.string(field.name).u8(TYPE_TAGS.indexOf(field.type));
			if (field.type === 'quant_f32') {
				w.f32(field.min!).f32(field.max!).u8(field.bits!);
			} else if (field.type === 'angle') {
				w.u8(field.bits!);
			}
		}
	}
}

/** Deserialize a schema (§5). */
export function decodeSchema(r: WireReader): GameSchema {
	const count = r.varuint();
	const kinds: GameKindDef[] = [];
	const seen = new Set<number>();
	for (let k = 0; k < count; k += 1) {
		const id = r.varuint();
		if (id > 0xffff) throw new GameDecodeError('INVALID', 'kind id > u16');
		const name = r.string();
		const fieldCount = r.varuint();
		if (fieldCount > MAX_FIELDS) throw new GameDecodeError('INVALID', 'more than 32 fields');
		const fields: GameFieldDef[] = [];
		for (let f = 0; f < fieldCount; f += 1) {
			const fieldName = r.string();
			const tag = r.u8();
			const type = TYPE_TAGS[tag];
			if (type === undefined) {
				throw new GameDecodeError('UNKNOWN_FIELD_TYPE', `field type tag ${tag}`);
			}
			if (type === 'quant_f32') {
				const min = r.f32();
				const max = r.f32();
				const bits = r.u8();
				checkBits(bits);
				fields.push({ name: fieldName, type, min, max, bits });
			} else if (type === 'angle') {
				const bits = r.u8();
				checkBits(bits);
				fields.push({ name: fieldName, type, bits });
			} else {
				fields.push({ name: fieldName, type });
			}
		}
		if (seen.has(id)) throw new GameDecodeError('INVALID', 'duplicate kind id');
		seen.add(id);
		kinds.push({ id, name, fields });
	}
	return defineGameSchema(kinds);
}
