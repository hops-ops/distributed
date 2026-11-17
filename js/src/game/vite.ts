/**
 * Vite integration for `distributed_game` surfaces in any Vite app (no
 * SvelteKit required). The plugin generates each surface's typed client and
 * WebAssembly from the Rust definition on dev start, on Rust changes and on
 * build. Generated clients are build outputs: keep their directories out of
 * git so they can never drift from the Rust surface.
 */
import {
	checkDistributedGames as checkGames,
	generateDistributedGames as generateGames,
	type DistributedGameManifestSource,
	type DistributedGameSurfaceCompiler,
	type DistributedSvelteKitViteOptions,
	type DistributedSvelteKitVitePlugin
} from '../sveltekit/vite.js';
import { distributedSvelteKit } from '../sveltekit/public-vite.js';

export type { DistributedGameManifestSource, DistributedGameSurfaceCompiler };

export type DistributedGamesViteOptions = Readonly<
	Pick<DistributedSvelteKitViteOptions, 'cwd' | 'command' | 'commandArgs'> & {
		games: readonly DistributedGameSurfaceCompiler[];
	}
>;

export type DistributedGamesVitePlugin = DistributedSvelteKitVitePlugin;

const gamesOnly = (options: DistributedGamesViteOptions): DistributedSvelteKitViteOptions => {
	if (options === null || typeof options !== 'object' || !Array.isArray(options.games)) {
		throw new TypeError('distributedGames requires a `games` array');
	}
	if (options.games.length === 0) {
		throw new TypeError('distributedGames requires at least one game surface');
	}
	return {
		cwd: options.cwd,
		command: options.command,
		commandArgs: options.commandArgs,
		games: options.games
	};
};

/** Vite plugin: generate game clients (+ WASM) on dev start, Rust changes and build. */
export function distributedGames(options: DistributedGamesViteOptions): DistributedGamesVitePlugin {
	return distributedSvelteKit(gamesOnly(options));
}

/** Generate every game client once (e.g. before `tsc`); resolves true when output changed. */
export function generateDistributedGames(options: DistributedGamesViteOptions): Promise<boolean> {
	return generateGames(gamesOnly(options));
}

/**
 * Verify generation is deterministic: a fresh generation must match the
 * output directory. Run it after `generateDistributedGames`; never writes.
 */
export function checkDistributedGames(options: DistributedGamesViteOptions): Promise<void> {
	return checkGames(gamesOnly(options));
}
