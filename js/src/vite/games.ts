/**
 * `distributed_game` surfaces: GameSurface → typed client (+ WebAssembly).
 * The public options and one-shot generation; `game-generator.ts` holds the
 * Vite-process generation. Needs no Svelte, and its declarations no Node types.
 */
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

import { isWithin } from '../lib/node-fs.js';
import {
	containedPath,
	manifestWatchRoots,
	nonempty,
	resolveCompilerCommand,
	runCancellable
} from './compiler-process.js';
import { generateGames } from './game-generator.js';
import type { CompilerCommand } from './types.js';

export type DistributedGameManifestSource =
	| string
	| Readonly<{
			/**
			 * Arguments passed to the configured distributed command. The first value
			 * must be `game-manifest`; stdout (the GameSurface JSON) becomes
			 * ephemeral compiler input.
			 */
			args: readonly string[];
	  }>;

/** One `distributed_game` surface: GameSurface → typed client (+ WebAssembly). */
export type DistributedGameSurfaceCompiler = Readonly<{
	/** Existing GameSurface JSON path, or canonical `distributed game-manifest` argv. */
	manifest: DistributedGameManifestSource;
	/** Directory owned by `distributed game-client` (and `game-wasm`'s `wasm/`), relative to `cwd`. */
	out: string;
	/** `game-client --import-extension`. Default `ts` (Vite resolves it). */
	importExtension?: 'ts' | 'js' | 'none';
	/**
	 * `distributed game-wasm` for the zone's `#[predict]` step and
	 * `#[game_pure]` functions: `'auto'` (default) when the surface declares
	 * any, using the manifest's `--manifest-path`; `{ manifestPath }` to name
	 * the Cargo manifest; `false` to skip.
	 */
	wasm?: 'auto' | false | Readonly<{ manifestPath: string }>;
	/**
	 * Extra Rust roots whose changes regenerate this surface (e.g. a local
	 * crate holding the predicted step). The manifest crate's `Cargo.toml`,
	 * `src/` and `crates/` are always watched.
	 */
	watch?: readonly string[];
}>;

export type ResolvedGame = Readonly<{
	manifest: DistributedGameManifestSource;
	out: string;
	importExtension: 'ts' | 'js' | 'none';
	wasm: false | 'auto' | Readonly<{ manifestPath: string }>;
	watchRoots: readonly string[];
}>;

/** The compiler and the surfaces it generates. */
export type GameTarget = CompilerCommand & Readonly<{ games: readonly ResolvedGame[] }>;

/** What the game entry points accept: surfaces plus the compiler to run. */
export type DistributedGamesOptions = Readonly<{
	/** Project root for the compiler cwd and output containment. */
	cwd?: string;
	/** Executable invoked without a shell. Defaults to `distributed`. */
	command?: string;
	/** Prefix argv, e.g. `cargo run ... --`; never interpreted by a shell. */
	commandArgs?: readonly string[];
	/** Required; the SvelteKit options type, where it is optional, is accepted too. */
	games?: readonly DistributedGameSurfaceCompiler[];
}>;

const GAME_EXTENSIONS = new Set(['ts', 'js', 'none']);

export function resolveGames(
	cwd: string,
	games: readonly DistributedGameSurfaceCompiler[],
	clientOutputs: readonly string[]
): ResolvedGame[] {
	const outputs = [...clientOutputs];
	return games.map((game, index): ResolvedGame => {
		const label = `Distributed game[${index}]`;
		if (game === null || typeof game !== 'object') {
			throw new TypeError(`${label} must be an object`);
		}
		const manifest = game.manifest;
		if (typeof manifest === 'string') {
			nonempty(manifest, `${label} manifest path`);
		} else if (
			manifest === null ||
			typeof manifest !== 'object' ||
			!Array.isArray(manifest.args) ||
			manifest.args[0] !== 'game-manifest' ||
			manifest.args.some((argument) => typeof argument !== 'string')
		) {
			throw new TypeError(`${label} manifest args must begin with game-manifest`);
		}
		const out = containedPath(cwd, game.out, `${label} out`);
		if (out === cwd) throw new TypeError(`${label} cannot use the project root as generated output`);
		for (const existing of outputs) {
			if (isWithin(existing, out) || isWithin(out, existing)) {
				throw new TypeError(`Distributed generated outputs must not overlap: \`${existing}\` and \`${out}\``);
			}
		}
		outputs.push(out);
		const importExtension = game.importExtension ?? 'ts';
		if (!GAME_EXTENSIONS.has(importExtension)) {
			throw new TypeError(`${label} importExtension must be ts, js or none`);
		}
		const wasm = game.wasm ?? 'auto';
		let resolvedWasm: ResolvedGame['wasm'];
		if (wasm === false || wasm === 'auto') {
			resolvedWasm = wasm;
		} else if (wasm !== null && typeof wasm === 'object') {
			resolvedWasm = Object.freeze({
				manifestPath: resolve(cwd, nonempty(wasm.manifestPath, `${label} wasm.manifestPath`))
			});
		} else {
			throw new TypeError(`${label} wasm must be 'auto', false or { manifestPath }`);
		}
		const extra = (game.watch ?? []).map((root, rootIndex) =>
			resolve(cwd, nonempty(root, `${label} watch[${rootIndex}]`))
		);
		const wasmRoots =
			typeof resolvedWasm === 'object' ? manifestWatchRoots(cwd, resolvedWasm.manifestPath) : [];
		const watchRoots = [
			...new Set([
				...manifestWatchRoots(cwd, manifest),
				...wasmRoots,
				...extra.filter((root) => existsSync(root))
			])
		].sort();
		return Object.freeze({
			manifest,
			out,
			importExtension,
			wasm: resolvedWasm,
			watchRoots: Object.freeze(watchRoots)
		});
	});
}

/** A Rust source of a game surface (or its Cargo manifest / lockfile). */
/** Throw unless `options` names at least one game surface. */
export function assertGamesOptions(
	options: DistributedGamesOptions
): asserts options is DistributedGamesOptions & Readonly<{ games: readonly DistributedGameSurfaceCompiler[] }> {
	if (options === null || typeof options !== 'object' || !Array.isArray(options.games)) {
		throw new TypeError('distributedGames requires a `games` array');
	}
	if (options.games.length === 0) {
		throw new TypeError('distributedGames requires at least one game surface');
	}
}

/** Resolve `options` into a target with at least one surface. */
export function resolveGameTarget(options: DistributedGamesOptions, fallbackCwd: string): GameTarget {
	assertGamesOptions(options);
	const cwd = resolve(options.cwd ?? fallbackCwd);
	return Object.freeze({
		...resolveCompilerCommand(cwd, options),
		games: Object.freeze(resolveGames(cwd, options.games, []))
	});
}

/** Generate every configured game surface once (what the plugin does on start). */
export async function generateDistributedGames(options: DistributedGamesOptions): Promise<boolean> {
	const target = resolveGameTarget(options, process.cwd());
	return runCancellable((children, signal) => generateGames(target, 'generate', children, signal));
}

/**
 * CI: `game-client --check` for every game surface; never writes. Run it
 * after `generateDistributedGames`.
 */
export async function checkDistributedGames(options: DistributedGamesOptions): Promise<void> {
	const target = resolveGameTarget(options, process.cwd());
	await runCancellable((children, signal) => generateGames(target, 'check', children, signal));
}

