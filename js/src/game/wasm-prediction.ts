/**
 * Client prediction backed by the zone's Rust step compiled to WebAssembly
 * (`#[predict]` in `distributed_game`, built by `distributed game-wasm`).
 *
 * The module speaks a small binary ABI (`distributed_game::prediction`, v1):
 * it imports nothing and exports `memory`, `dg_predict_abi() -> u32`,
 * `dg_predict_buffer() -> ptr` (a 64 KiB scratch area) and one
 * `dg_predict_<step>(state_len, input_len, context_len, dt) -> i32` per
 * step. A call writes `state ‖ input ‖ context` into the scratch area —
 * the state and context with the payload codec (`f32` stays binary32), the
 * input as its wire payload — and reads the next state back from the start
 * of it. No JSON, no allocation across the boundary.
 *
 * Generated clients (`distributed game-client`) wire this in for you:
 * `create<Surface>Client` predicts with the compiled step by default.
 *
 * The same module may also export pure functions (`#[game_pure]`,
 * `dg_pure_<fn>(arg_len) -> i32`, see `wasm-pures.ts`): `PredictModule.call`
 * runs one over the same scratch area.
 */
import { readWire, writeWire, encodePayload, type PayloadLayout, type WireType } from './codec.js';
import type { PredictFn } from './prediction.js';
import type { EntityFields } from './replica.js';
import type { FieldValue } from './schema.js';
import { WireReader, WireWriter } from './wire.js';

/** The prediction ABI version this SDK speaks. */
export const PREDICT_ABI_VERSION = 1;

/** Size of the module's scratch area (ABI v1). */
export const PREDICT_SCRATCH_BYTES = 64 * 1024;

/**
 * Where a compiled step comes from: a URL (fetched in browsers, read from
 * disk for `file:` URLs in Node), its bytes, a compiled module or a
 * response.
 */
export type WasmSource =
	| URL
	| string
	| ArrayBuffer
	| ArrayBufferView
	| WebAssembly.Module
	| Response
	| PromiseLike<Response>;

/** Predicted fields: `[wire field name, codec type]` in state-record order. */
export type PredictStateLayout = readonly (readonly [string, WireType])[];

/** A step export returned a negative ABI code. */
export class PredictAbiError extends Error {
	readonly code: number;
	constructor(exportName: string, code: number) {
		const reasons: Record<number, string> = {
			[-1]: 'input larger than the scratch buffer',
			[-2]: 'state does not decode',
			[-3]: 'input payload rejected',
			[-4]: 'context does not decode',
			[-5]: 'output does not fit',
			[-6]: 'argument does not decode'
		};
		super(`${exportName.startsWith('dg_') ? exportName : `dg_predict_${exportName}`} failed (${code}): ${reasons[code] ?? 'unknown error'}`);
		this.name = 'PredictAbiError';
		this.code = code;
	}
}

type StepExport = (stateLen: number, inputLen: number, contextLen: number, dt: number) => number;
type PureExport = (argLen: number) => number;

/** An instantiated prediction module. One instance can serve every client. */
export class PredictModule {
	readonly #exports: WebAssembly.Exports;
	readonly #memory: WebAssembly.Memory;
	readonly #pointer: number;
	#view: Uint8Array;
	readonly #steps = new Map<string, StepExport>();
	readonly #pures = new Map<string, PureExport>();

	constructor(instance: WebAssembly.Instance) {
		const exports = instance.exports;
		const memory = exports.memory;
		const buffer = exports.dg_predict_buffer;
		const abi = exports.dg_predict_abi;
		if (!(memory instanceof WebAssembly.Memory) || typeof buffer !== 'function' || typeof abi !== 'function') {
			throw new TypeError('not a distributed_game prediction module (missing memory / dg_predict_buffer / dg_predict_abi)');
		}
		const version = (abi as () => number)();
		if (version !== PREDICT_ABI_VERSION) {
			throw new TypeError(`prediction module speaks ABI v${version}; this SDK speaks v${PREDICT_ABI_VERSION}`);
		}
		this.#exports = exports;
		this.#memory = memory;
		this.#pointer = (buffer as () => number)() >>> 0;
		this.#view = new Uint8Array(memory.buffer, this.#pointer, PREDICT_SCRATCH_BYTES);
	}

	/** Instantiate a compiled step (cached per URL / module, see `WasmSource`). */
	static load(source: WasmSource): Promise<PredictModule> {
		return loadPredictModule(source);
	}

	/** Step names (`dg_predict_<name>`) this module exports. */
	get steps(): readonly string[] {
		return Object.keys(this.#exports)
			.filter((name) => name.startsWith('dg_predict_') && name !== 'dg_predict_abi' && name !== 'dg_predict_buffer')
			.map((name) => name.slice('dg_predict_'.length));
	}

	/** Pure function names (`dg_pure_<name>`) this module exports. */
	get pures(): readonly string[] {
		return Object.keys(this.#exports)
			.filter((name) => name.startsWith('dg_pure_'))
			.map((name) => name.slice('dg_pure_'.length));
	}

	/**
	 * Call the pure function `dg_pure_<exportName>` with an encoded
	 * argument (empty for none). Returns a copy of the encoded result.
	 */
	call(exportName: string, arg: Uint8Array): Uint8Array {
		let fn = this.#pures.get(exportName);
		if (fn === undefined) {
			const found = this.#exports[`dg_pure_${exportName}`];
			if (typeof found !== 'function') throw new TypeError(`module has no pure function dg_pure_${exportName}`);
			fn = found as PureExport;
			this.#pures.set(exportName, fn);
		}
		const name = `dg_pure_${exportName}`;
		if (arg.length > PREDICT_SCRATCH_BYTES) throw new PredictAbiError(name, -1);
		this.#scratch().set(arg, 0);
		const length = fn(arg.length);
		if (length < 0) throw new PredictAbiError(name, length);
		return this.#scratch().slice(0, length);
	}

	/**
	 * Run `dg_predict_<exportName>` once. Returns a copy of the encoded next
	 * state. `dt` is the frame length in seconds (rounded to binary32).
	 */
	step(exportName: string, state: Uint8Array, input: Uint8Array, context: Uint8Array, dt: number): Uint8Array {
		const fn = this.#step(exportName);
		const total = state.length + input.length + context.length;
		if (total > PREDICT_SCRATCH_BYTES) throw new PredictAbiError(exportName, -1);
		let view = this.#scratch();
		view.set(state, 0);
		view.set(input, state.length);
		view.set(context, state.length + input.length);
		const length = fn(state.length, input.length, context.length, Math.fround(dt));
		if (length < 0) throw new PredictAbiError(exportName, length);
		view = this.#scratch(); // the call may have grown memory
		return view.slice(0, length);
	}

	#step(exportName: string): StepExport {
		let fn = this.#steps.get(exportName);
		if (fn === undefined) {
			const found = this.#exports[`dg_predict_${exportName}`];
			if (typeof found !== 'function') {
				throw new TypeError(`prediction module has no step dg_predict_${exportName}`);
			}
			fn = found as StepExport;
			this.#steps.set(exportName, fn);
		}
		return fn;
	}

	#scratch(): Uint8Array {
		if (this.#view.buffer !== this.#memory.buffer || this.#view.byteLength === 0) {
			this.#view = new Uint8Array(this.#memory.buffer, this.#pointer, PREDICT_SCRATCH_BYTES);
		}
		return this.#view;
	}
}

const cache = new Map<string, Promise<PredictModule>>();
const moduleCache = new WeakMap<WebAssembly.Module, Promise<PredictModule>>();

/**
 * Instantiate a prediction module. URL sources (and compiled modules) are
 * cached, so every client of a page shares one instance.
 */
export function loadPredictModule(source: WasmSource): Promise<PredictModule> {
	if (source instanceof WebAssembly.Module) {
		let loaded = moduleCache.get(source);
		if (loaded === undefined) {
			loaded = WebAssembly.instantiate(source, {}).then((instance) => new PredictModule(instance));
			moduleCache.set(source, loaded);
		}
		return loaded;
	}
	if (typeof source === 'string' || source instanceof URL) {
		const href = String(source);
		let loaded = cache.get(href);
		if (loaded === undefined) {
			loaded = instantiateUrl(href);
			cache.set(href, loaded);
			loaded.catch(() => cache.delete(href));
		}
		return loaded;
	}
	return instantiateBytesOrResponse(source);
}

async function instantiateUrl(href: string): Promise<PredictModule> {
	const url = new URL(href, typeof location === 'undefined' ? undefined : location.href);
	if (url.protocol === 'file:') {
		// Node (tests, tools): fetch() has no file: support. The specifier is
		// computed so bundlers do not try to include `node:fs` in browsers.
		const fsModule = 'node:fs/promises';
		const fs = (await import(/* @vite-ignore */ fsModule)) as {
			readFile(path: URL): Promise<Uint8Array>;
		};
		return instantiateBytesOrResponse(await fs.readFile(url));
	}
	return instantiateBytesOrResponse(fetch(url));
}

async function instantiateBytesOrResponse(
	source: ArrayBuffer | ArrayBufferView | Response | PromiseLike<Response>
): Promise<PredictModule> {
	if (source instanceof ArrayBuffer || ArrayBuffer.isView(source)) {
		const { instance } = await WebAssembly.instantiate(source as BufferSource, {});
		return new PredictModule(instance);
	}
	const response = await source;
	if (!response.ok) throw new Error(`prediction module: HTTP ${response.status} for ${response.url}`);
	if (typeof WebAssembly.instantiateStreaming === 'function' && response.headers.get('content-type')?.startsWith('application/wasm')) {
		const { instance } = await WebAssembly.instantiateStreaming(response, {});
		return new PredictModule(instance);
	}
	const { instance } = await WebAssembly.instantiate(await response.arrayBuffer(), {});
	return new PredictModule(instance);
}

/** Encode the predicted fields of an entity (`[wire name, type]` layout). */
export function encodePredictState(layout: PredictStateLayout, fields: EntityFields): Uint8Array {
	const w = new WireWriter(32);
	for (const [name, type] of layout) writeWire(w, type, fields[name]);
	return w.finish();
}

/** Decode a state encoded by the step back into entity fields. */
export function decodePredictState(layout: PredictStateLayout, bytes: Uint8Array): Record<string, FieldValue> {
	const r = new WireReader(bytes);
	const out: Record<string, FieldValue> = {};
	for (const [name, type] of layout) out[name] = readWire(r, type) as FieldValue;
	r.finish();
	return out;
}

export type WasmPredictionOptions<TInput> = Readonly<{
	/** The compiled step (generated: `PREDICT_WASM_URL`). */
	source: WasmSource;
	/** Step name: the module export is `dg_predict_<exportName>`. */
	exportName: string;
	/** Predicted fields and their codec types (generated `PREDICT_STATE`). */
	state: PredictStateLayout;
	/** Context record layout, if the step takes one (generated `PREDICT_CONTEXT`). */
	context?: PayloadLayout;
	/** Input payload encoder (generated `encodeInput`). */
	encodeInput(input: TInput): Uint8Array;
	/** Loading failed (default: `console.error`); prediction then stays a no-op. */
	onError?(error: unknown): void;
}>;

/** A compiled step wrapped as `GameClient` prediction. */
export type WasmPrediction<TInput, TContext> = Readonly<{
	/** `PredictFn` for `GameClient` / `Predictor`: the state unchanged until ready. */
	predict: PredictFn<TInput>;
	/** Resolves when the module is instantiated; rejects if it cannot load. */
	ready: Promise<void>;
	/** True once the module is instantiated. */
	loaded(): boolean;
	/** Set what the step reads besides state and input (steps with a context). */
	setContext(context: TContext): void;
	/** The current context. */
	context(): TContext | undefined;
	/** The instantiated module, once loaded. */
	module(): PredictModule | undefined;
	/** Steps run so far (diagnostics). */
	steps(): number;
}>;

const NONE = new Uint8Array(0);

/**
 * Wrap a compiled step as prediction. `predict` encodes the controlled
 * entity's predicted fields, the input and the context, runs the step and
 * returns the next fields. Until the module is loaded (or, for steps with a
 * context, until `setContext`), it returns no change, so the entity simply
 * follows the server until prediction can start; the next reconciliation
 * then replays every pending input through the step.
 */
export function createWasmPrediction<TInput, TContext = undefined>(
	options: WasmPredictionOptions<TInput>
): WasmPrediction<TInput, TContext> {
	let module: PredictModule | undefined;
	let context: TContext | undefined;
	let contextBytes: Uint8Array | undefined = options.context === undefined ? NONE : undefined;
	let steps = 0;
	const ready = loadPredictModule(options.source).then((loaded) => {
		if (!loaded.steps.includes(options.exportName)) {
			throw new TypeError(`prediction module has no step dg_predict_${options.exportName}`);
		}
		module = loaded;
	});
	ready.catch((error: unknown) => {
		if (options.onError !== undefined) options.onError(error);
		else console.error('distributed game prediction: the compiled step did not load', error);
	});
	const predict: PredictFn<TInput> = (fields, input, dtMs) => {
		if (module === undefined || contextBytes === undefined) return {};
		const next = module.step(
			options.exportName,
			encodePredictState(options.state, fields),
			options.encodeInput(input),
			contextBytes,
			dtMs / 1000
		);
		steps += 1;
		return decodePredictState(options.state, next);
	};
	return {
		predict,
		ready,
		loaded: () => module !== undefined,
		setContext(value) {
			if (options.context === undefined) return;
			context = value;
			contextBytes = encodePayload(options.context, value as object);
		},
		context: () => context,
		module: () => module,
		steps: () => steps
	};
}
