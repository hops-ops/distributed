/**
 * Primitive wire encodings of the `distributed_game` protocol
 * (`distributed_game/PROTOCOL.md` §2). Little-endian fixed-width numbers,
 * minimal unsigned LEB128 `varuint`, zigzag `varint`, length-prefixed UTF-8.
 */

/** Largest `string`/`bytes` length accepted by decoders (1 MiB). */
export const MAX_BLOB_LEN = 1 << 20;

export type GameDecodeErrorCode =
	| 'UNEXPECTED_EOF'
	| 'VARINT_OVERFLOW'
	| 'INVALID_UTF8'
	| 'UNKNOWN_MESSAGE'
	| 'UNKNOWN_FIELD_TYPE'
	| 'UNKNOWN_ENTITY'
	| 'INVALID'
	| 'TRAILING_BYTES';

/** Any malformed or unexpected byte sequence. */
export class GameDecodeError extends Error {
	readonly code: GameDecodeErrorCode;
	constructor(code: GameDecodeErrorCode, message: string) {
		super(`${code}: ${message}`);
		this.name = 'GameDecodeError';
		this.code = code;
	}
}

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder('utf-8', { fatal: true });

function assertU32(value: number, what: string): number {
	if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
		throw new RangeError(`${what} must be an unsigned 32-bit integer, got ${value}`);
	}
	return value;
}

function assertI32(value: number, what: string): number {
	if (!Number.isInteger(value) || value < -0x8000_0000 || value > 0x7fff_ffff) {
		throw new RangeError(`${what} must be a signed 32-bit integer, got ${value}`);
	}
	return value;
}

/** Growable append-only encoder. */
export class WireWriter {
	#buf: Uint8Array;
	#view: DataView;
	#len = 0;

	constructor(capacity = 64) {
		this.#buf = new Uint8Array(capacity);
		this.#view = new DataView(this.#buf.buffer);
	}

	get length(): number {
		return this.#len;
	}

	/** Copy of the bytes written so far. */
	finish(): Uint8Array {
		return this.#buf.slice(0, this.#len);
	}

	#reserve(extra: number): void {
		const need = this.#len + extra;
		if (need <= this.#buf.length) return;
		let size = this.#buf.length * 2;
		while (size < need) size *= 2;
		const next = new Uint8Array(size);
		next.set(this.#buf.subarray(0, this.#len));
		this.#buf = next;
		this.#view = new DataView(next.buffer);
	}

	raw(bytes: Uint8Array): this {
		this.#reserve(bytes.length);
		this.#buf.set(bytes, this.#len);
		this.#len += bytes.length;
		return this;
	}

	u8(value: number): this {
		if (!Number.isInteger(value) || value < 0 || value > 255) {
			throw new RangeError(`u8 out of range: ${value}`);
		}
		this.#reserve(1);
		this.#buf[this.#len++] = value;
		return this;
	}

	bool(value: boolean): this {
		return this.u8(value ? 1 : 0);
	}

	/** Minimal unsigned LEB128 of a u32. */
	varuint(value: number): this {
		let v = assertU32(value, 'varuint');
		this.#reserve(5);
		for (;;) {
			const byte = v & 0x7f;
			v = Math.floor(v / 128);
			if (v === 0) {
				this.#buf[this.#len++] = byte;
				return this;
			}
			this.#buf[this.#len++] = byte | 0x80;
		}
	}

	/** Zigzag-mapped signed varint. */
	varint(value: number): this {
		const v = assertI32(value, 'varint');
		return this.varuint(((v << 1) ^ (v >> 31)) >>> 0);
	}

	f32(value: number): this {
		this.#reserve(4);
		this.#view.setFloat32(this.#len, value, true);
		this.#len += 4;
		return this;
	}

	f64(value: number): this {
		this.#reserve(8);
		this.#view.setFloat64(this.#len, value, true);
		this.#len += 8;
		return this;
	}

	bytes(value: Uint8Array): this {
		return this.varuint(value.length).raw(value);
	}

	string(value: string): this {
		return this.bytes(utf8Encoder.encode(value));
	}

	/** `u8` length + UTF-8, truncated at a char boundary to fit 255 bytes. */
	shortString(value: string): this {
		const bytes = utf8Encoder.encode(value);
		let end = Math.min(bytes.length, 255);
		// Back off continuation bytes (0b10xx_xxxx) so we cut on a boundary.
		while (end < bytes.length && end > 0 && (bytes[end] & 0xc0) === 0x80) end -= 1;
		return this.u8(end).raw(bytes.subarray(0, end));
	}
}

/** Cursor over one message. */
export class WireReader {
	readonly #buf: Uint8Array;
	readonly #view: DataView;
	#pos = 0;

	constructor(buf: Uint8Array) {
		this.#buf = buf;
		this.#view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
	}

	get remaining(): number {
		return this.#buf.length - this.#pos;
	}

	/** Error unless every byte was consumed. */
	finish(): void {
		if (this.remaining !== 0) {
			throw new GameDecodeError('TRAILING_BYTES', `${this.remaining} trailing bytes`);
		}
	}

	raw(n: number): Uint8Array {
		if (this.remaining < n) throw new GameDecodeError('UNEXPECTED_EOF', 'input ended mid-value');
		const out = this.#buf.slice(this.#pos, this.#pos + n);
		this.#pos += n;
		return out;
	}

	u8(): number {
		if (this.remaining < 1) throw new GameDecodeError('UNEXPECTED_EOF', 'input ended mid-value');
		return this.#buf[this.#pos++];
	}

	bool(): boolean {
		const byte = this.u8();
		if (byte === 0) return false;
		if (byte === 1) return true;
		throw new GameDecodeError('INVALID', 'bool must be 0 or 1');
	}

	varuint(): number {
		let value = 0;
		for (let index = 0; index < 5; index += 1) {
			const byte = this.u8();
			if (index === 4 && byte > 0x0f) {
				throw new GameDecodeError('VARINT_OVERFLOW', 'varuint exceeds 32 bits');
			}
			// Multiplication, not `<<`: the 5th group would overflow int32.
			value += (byte & 0x7f) * 2 ** (7 * index);
			if ((byte & 0x80) === 0) return value;
		}
		throw new GameDecodeError('VARINT_OVERFLOW', 'varuint exceeds 32 bits');
	}

	varint(): number {
		const z = this.varuint();
		return (z >>> 1) ^ -(z & 1);
	}

	f32(): number {
		if (this.remaining < 4) throw new GameDecodeError('UNEXPECTED_EOF', 'input ended mid-value');
		const value = this.#view.getFloat32(this.#pos, true);
		this.#pos += 4;
		return value;
	}

	f64(): number {
		if (this.remaining < 8) throw new GameDecodeError('UNEXPECTED_EOF', 'input ended mid-value');
		const value = this.#view.getFloat64(this.#pos, true);
		this.#pos += 8;
		return value;
	}

	bytes(): Uint8Array {
		const len = this.varuint();
		if (len > MAX_BLOB_LEN) throw new GameDecodeError('INVALID', 'length exceeds 1 MiB');
		return this.raw(len);
	}

	string(): string {
		return decodeUtf8(this.bytes());
	}

	shortString(): string {
		return decodeUtf8(this.raw(this.u8()));
	}
}

function decodeUtf8(bytes: Uint8Array): string {
	try {
		return utf8Decoder.decode(bytes);
	} catch {
		throw new GameDecodeError('INVALID_UTF8', 'string is not valid UTF-8');
	}
}

/** Length-prefix a message for byte-stream transports (§1.2). */
export function frameStream(message: Uint8Array): Uint8Array {
	return new WireWriter(message.length + 5).bytes(message).finish();
}

/**
 * Split one length-prefixed frame off the front of `buf`. Returns
 * `undefined` when more bytes are needed.
 */
export function unframeStream(
	buf: Uint8Array
): { frame: Uint8Array; consumed: number } | undefined {
	const reader = new WireReader(buf);
	try {
		const frame = reader.bytes();
		return { frame, consumed: buf.length - reader.remaining };
	} catch (error) {
		if (error instanceof GameDecodeError && error.code === 'UNEXPECTED_EOF') return undefined;
		throw error;
	}
}

/** Lowercase hex of bytes. */
export function toHex(bytes: Uint8Array): string {
	let out = '';
	for (const byte of bytes) out += byte.toString(16).padStart(2, '0');
	return out;
}

/** Bytes of a hex string. */
export function fromHex(hex: string): Uint8Array {
	if (hex.length % 2 !== 0 || /[^0-9a-f]/i.test(hex)) throw new TypeError('invalid hex');
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i += 1) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	return out;
}
