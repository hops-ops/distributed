/**
 * Vite integration for `distributed_game` surfaces in any Vite app (no
 * SvelteKit or Svelte required). The plugin generates each surface's typed
 * client and WebAssembly from the Rust definition on dev start, on Rust
 * changes and on build. Generated clients are build outputs: keep their
 * directories out of git so they can never drift from the Rust surface.
 */
import type { ChildProcess } from 'node:child_process';
import { isMainThread } from 'node:worker_threads';

import { stopCompiler } from '../vite/compiler-process.js';
import { GameGenerator } from '../vite/game-generator.js';
import {
	assertGamesOptions,
	resolveGameTarget,
	type DistributedGamesOptions,
	type DistributedGameSurfaceCompiler
} from '../vite/games.js';
import type { RollupWatchContextLike, ViteHotContextLike, ViteServerLike } from '../vite/types.js';

export {
	checkDistributedGames,
	generateDistributedGames,
	type DistributedGameManifestSource,
	type DistributedGameSurfaceCompiler
} from '../vite/games.js';

export type DistributedGamesViteOptions = DistributedGamesOptions &
	Readonly<{ games: readonly DistributedGameSurfaceCompiler[] }>;

/** Minimal structural Vite plugin type; avoids making Vite a runtime dependency. */
export type DistributedGamesVitePlugin = Readonly<{
	name: string;
	enforce: 'pre';
	configResolved(
		config: Readonly<{
			root: string;
			command?: 'serve' | 'build';
			logger?: Readonly<{ error(message: string): void }>;
		}>
	): Promise<void>;
	configureServer(server: ViteServerLike): void;
	buildStart(this: RollupWatchContextLike): void;
	handleHotUpdate(context: ViteHotContextLike): Promise<never[] | undefined>;
	watchChange(id: string): Promise<void>;
	closeBundle(): Promise<void>;
}>;

/** Vite plugin: generate game clients (+ WASM) on dev start, Rust changes and build. */
export function distributedGames(options: DistributedGamesViteOptions): DistributedGamesVitePlugin {
	assertGamesOptions(options);
	const children = new Set<ChildProcess>();
	const cancellation = new AbortController();
	const games = new GameGenerator(children, cancellation.signal);
	let configured = false;
	const stop = async (): Promise<void> => {
		if (cancellation.signal.aborted) return;
		await stopCompiler(cancellation, children, [games.running]);
	};
	return {
		name: '@hops-ops/distributed:games',
		enforce: 'pre',
		async configResolved(config): Promise<void> {
			if (configured) {
				throw new Error('Distributed games Vite plugin was configured more than once');
			}
			configured = true;
			games.configure(resolveGameTarget(options, config.root), config);
			// SvelteKit's post-build analysis worker reads configuration only.
			if (!isMainThread && process.env.SVELTEKIT_FORK === 'true') return;
			await games.start(config.command === 'build');
		},
		configureServer(server): void {
			games.configureServer(server);
			server.httpServer?.once('close', () => {
				void stop();
			});
		},
		buildStart(): void {
			for (const root of games.watchRoots()) this.addWatchFile(root);
		},
		async handleHotUpdate(context): Promise<never[] | undefined> {
			if (!games.isInput(context.file)) return undefined;
			await games.hotUpdate(context.server);
			return [];
		},
		async watchChange(id): Promise<void> {
			if (games.isInput(id)) await games.watchChange();
		},
		closeBundle: stop
	};
}
