/**
 * Pure game functions compiled to WebAssembly (`#[game_pure]` in
 * `distributed_game::pure`, built by `distributed game-wasm` into the same
 * module as the zone's predicted step).
 *
 * Each function is an export `dg_pure_<fn>(arg_len) -> i32` over the module's
 * scratch area (ABI v1): the argument's payload-codec encoding goes in, the
 * result's encoding comes back at the start of the area. Generated clients
 * (`pures.ts`) wrap every function with its types:
 * `createGridPures().gridCell({ seed, cx, cy })`.
 */
import { decodeValue, encodeValue, type WireType } from './codec.js';
import { loadPredictModule, type PredictModule, type WasmSource } from './wasm-prediction.js';

/** How to call one pure function: its export and argument / result types. */
export type WasmPureSpec = Readonly<{
	/** The export is `dg_pure_<exportName>`. */
	exportName: string;
	/** Argument type (omitted: the function takes none). */
	arg?: WireType;
	/** Result type. */
	output: WireType;
}>;

/** Loading state of a set of pure functions. */
export type WasmPuresHandle = Readonly<{
	/** Resolves when the module is instantiated and exports every function. */
	ready: Promise<void>;
	/** True once `ready` resolved. */
	loaded(): boolean;
	/** The instantiated module, once loaded. */
	module(): PredictModule | undefined;
}>;

/** Typed functions `F` (synchronous; they throw until loaded) plus the handle. */
export type WasmPures<F> = F & WasmPuresHandle;

const RESERVED = new Set(['ready', 'loaded', 'module']);

/**
 * Wrap the pure functions of a compiled module. Each wrapper encodes its
 * argument, calls the export and decodes the result; calls before `ready`
 * resolves throw (await `ready`, or check `loaded()`). URL sources share
 * one instance with the predicted step (`loadPredictModule` caches them).
 */
export function createWasmPures<F>(
	source: WasmSource,
	specs: Readonly<Record<string, WasmPureSpec>>
): WasmPures<F> {
	let module: PredictModule | undefined;
	const ready = loadPredictModule(source).then((loaded) => {
		for (const spec of Object.values(specs)) {
			if (!loaded.pures.includes(spec.exportName)) {
				throw new TypeError(`module has no pure function dg_pure_${spec.exportName}`);
			}
		}
		module = loaded;
	});
	ready.catch(() => undefined); // surfaced through `ready` and the calls
	const fns: Record<string, (arg?: unknown) => unknown> = {};
	for (const [name, spec] of Object.entries(specs)) {
		if (RESERVED.has(name)) throw new TypeError(`pure function name ${name} is reserved`);
		const empty = new Uint8Array(0);
		fns[name] = (arg?: unknown) => {
			if (module === undefined) {
				throw new Error(`pure function ${spec.exportName} called before its module loaded (await ready)`);
			}
			const bytes = spec.arg === undefined ? empty : encodeValue(spec.arg, arg);
			return decodeValue(spec.output, module.call(spec.exportName, bytes));
		};
	}
	return {
		...fns,
		ready,
		loaded: () => module !== undefined,
		module: () => module
	} as WasmPures<F>;
}
