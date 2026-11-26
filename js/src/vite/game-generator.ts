/**
 * Game generation in a Vite process (and for the one-shot entry points):
 * `game-manifest` → `game-client` (→ `game-wasm`) per surface.
 */
import type { ChildProcess } from 'node:child_process';
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, relative, resolve, sep } from 'node:path';

import { isMissing, isWithin } from '../lib/node-fs.js';
import {
	runCommand,
	throwIfAborted,
	validateNearestExistingParent,
	viteError
} from './compiler-process.js';
import type { GameTarget, ResolvedGame } from './games.js';
import type { ViteServerLike } from './types.js';

const MAX_GAME_TREE_FILES = 2_000;
const MAX_GAME_TREE_BYTES = 64 * 1024 * 1024;

export function isGameInput(file: string, integration: GameTarget): boolean {
	if (integration.games.length === 0) return false;
	const absolute = resolve(integration.cwd, file);
	const name = basename(absolute);
	if (!absolute.endsWith('.rs') && name !== 'Cargo.toml' && name !== 'Cargo.lock') return false;
	if (absolute.split(sep).includes('target')) return false;
	return integration.games.some((game) =>
		game.watchRoots.some(
			(root) => isWithin(root, absolute) || (name === 'Cargo.lock' && isWithin(dirname(root), absolute))
		)
	);
}

function gameManifestPath(game: ResolvedGame, cwd: string): string | undefined {
	if (typeof game.wasm === 'object') return game.wasm.manifestPath;
	if (typeof game.manifest === 'string') return undefined;
	const index = game.manifest.args.indexOf('--manifest-path');
	const value = index === -1 ? undefined : game.manifest.args[index + 1];
	return value === undefined ? undefined : resolve(cwd, value);
}

async function gameTreeDigest(root: string): Promise<string> {
	const { createHash } = await import('node:crypto');
	const hash = createHash('sha256');
	const budget = { files: 0, bytes: 0 };
	const walk = async (directory: string): Promise<void> => {
		let entries;
		try {
			entries = await readdir(directory, { withFileTypes: true });
		} catch (error) {
			if (isMissing(error)) return;
			throw error;
		}
		entries.sort((a, b) => a.name.localeCompare(b.name));
		for (const entry of entries) {
			const path = join(directory, entry.name);
			if (entry.isDirectory()) {
				await walk(path);
			} else if (entry.isFile()) {
				budget.files += 1;
				const bytes = await readFile(path);
				budget.bytes += bytes.byteLength;
				if (budget.files > MAX_GAME_TREE_FILES || budget.bytes > MAX_GAME_TREE_BYTES) {
					throw new Error(`generated game output ${root} exceeds its size budget`);
				}
				hash.update(relative(root, path)).update('\0').update(bytes).update('\0');
			}
		}
	};
	await walk(root);
	return hash.digest('hex');
}

/**
 * Run `game-manifest` → `game-client` (→ `game-wasm`) for every game surface.
 * `check` runs `game-client --check` only (the WebAssembly is a build output,
 * never committed). Resolves true when any generated output changed.
 */
export async function generateGames(
	integration: GameTarget,
	mode: 'generate' | 'check',
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<boolean> {
	let changed = false;
	for (const [index, game] of integration.games.entries()) {
		throwIfAborted(signal);
		const scratch = await mkdtemp(join(tmpdir(), 'distributed-game-'));
		try {
			let manifest: string;
			if (typeof game.manifest === 'string') {
				manifest = resolve(integration.cwd, game.manifest);
			} else {
				const result = await runCommand(integration, [...game.manifest.args], children, signal);
				manifest = join(scratch, `surface-${index}.json`);
				await writeFile(manifest, result.stdout, { encoding: 'utf8', flag: 'wx' });
			}
			let surface: { predict?: unknown; pures?: unknown };
			try {
				surface = JSON.parse(await readFile(manifest, 'utf8')) as typeof surface;
			} catch (error) {
				throw new Error(`distributed game-manifest for ${game.out} did not emit valid JSON`, {
					cause: error
				});
			}
			const before = mode === 'generate' ? await gameTreeDigest(game.out) : '';
			if (mode === 'generate') await mkdir(game.out, { recursive: true });
			await validateNearestExistingParent(integration.cwd, game.out);
			await runCommand(
				integration,
				[
					'game-client',
					'--manifest',
					manifest,
					'--out',
					game.out,
					'--import-extension',
					game.importExtension,
					...(mode === 'check' ? ['--check'] : [])
				],
				children,
				signal
			);
			const declaresWasm =
				(surface.predict !== undefined && surface.predict !== null) ||
				(Array.isArray(surface.pures) && surface.pures.length > 0);
			if (mode === 'generate' && game.wasm !== false && (game.wasm !== 'auto' || declaresWasm)) {
				const manifestPath = gameManifestPath(game, integration.cwd);
				if (manifestPath === undefined) {
					throw new Error(
						`Distributed game ${game.out}: game-wasm needs the Cargo manifest; pass wasm: { manifestPath }`
					);
				}
				await runCommand(
					integration,
					['game-wasm', '--manifest', manifest, '--manifest-path', manifestPath, '--out', game.out],
					children,
					signal
				);
			}
			if (mode === 'generate' && (await gameTreeDigest(game.out)) !== before) changed = true;
		} finally {
			await rm(scratch, { recursive: true, force: true });
		}
	}
	return changed;
}

const BUILD_GAME_RUNS = Symbol.for('@hops-ops/distributed/sveltekit/game-build-runs/v1');

export function onceForBuild(integration: GameTarget, run: () => Promise<unknown>): Promise<unknown> {
	const scope = globalThis as unknown as Record<symbol, Map<string, Promise<unknown>> | undefined>;
	const runs = (scope[BUILD_GAME_RUNS] ??= new Map());
	const key = JSON.stringify([integration.cwd, integration.games]);
	let pending = runs.get(key);
	if (pending === undefined) {
		pending = run();
		runs.set(key, pending);
		pending.catch(() => runs.delete(key));
	}
	return pending;
}


/**
 * The game half of a Vite plugin: serialized, coalescing generation that a
 * dev server survives. A failed dev generation goes to the console and the
 * error overlay (replayed to overlays that connect later) and the server
 * keeps the last generated client; a build fails.
 */
export class GameGenerator {
	readonly #children: Set<ChildProcess>;
	readonly #signal: AbortSignal;
	#target: GameTarget | undefined;
	#serving = false;
	#logger: Readonly<{ error(message: string): void }> = {
		error: (message) => console.error(message)
	};
	#dirty = false;
	#running: Promise<boolean> | undefined;
	#failure: unknown;
	#server: Pick<ViteServerLike, 'ws'> | undefined;

	/** Children join the plugin's set; `signal` (the plugin stopping) closes it. */
	constructor(children: Set<ChildProcess>, signal: AbortSignal) {
		this.#children = children;
		this.#signal = signal;
	}

	/** In-flight generation, for a plugin waiting to stop. */
	get running(): Promise<boolean> | undefined {
		return this.#running;
	}

	/** Vite `configResolved`: the target, and whether this is `vite dev`. */
	configure(
		target: GameTarget,
		config: Readonly<{
			command?: 'serve' | 'build';
			logger?: Readonly<{ error(message: string): void }>;
		}>
	): void {
		this.#target = target;
		this.#serving = config.command === 'serve';
		if (config.logger !== undefined) this.#logger = config.logger;
	}

	/**
	 * Generate at startup: once per build process (SvelteKit's client pass
	 * reloads the config); in dev, a Rust error must not keep the server down.
	 */
	async start(build: boolean): Promise<void> {
		const target = this.#require();
		if (target.games.length === 0) return;
		if (build) await onceForBuild(target, () => this.generate());
		else await this.#generateInDev();
	}

	watchRoots(): readonly string[] {
		return this.#target?.games.flatMap((game) => game.watchRoots) ?? [];
	}

	/** A Rust source of a game surface (or its Cargo manifest / lockfile). */
	isInput(file: string): boolean {
		return this.#target !== undefined && isGameInput(file, this.#target);
	}

	configureServer(server: ViteServerLike): void {
		const roots = this.watchRoots();
		if (roots.length > 0) server.watcher.add(roots);
		this.#server = server;
		// An overlay opened after a failed startup generation still shows it.
		server.ws.on?.('connection', () => {
			if (this.#failure !== undefined) {
				server.ws.send({ type: 'error', err: viteError(this.#failure) });
			}
		});
	}

	/** Vite `handleHotUpdate` of a game input: regenerate, reload on change. */
	async hotUpdate(server: Pick<ViteServerLike, 'ws'>): Promise<void> {
		this.#server ??= server;
		if (await this.#generateInDev()) server.ws.send({ type: 'full-reload', path: '*' });
	}

	/**
	 * Rollup `watchChange` of a game input. `vite dev` calls it from its file
	 * watcher without handling a rejection (the process would exit): there,
	 * report and keep serving. `vite build --watch` still fails the rebuild.
	 */
	async watchChange(): Promise<void> {
		if (this.#serving) await this.#generateInDev();
		else await this.generate();
	}

	/** Serialized, coalescing generation; resolves true when any output changed. */
	generate(): Promise<boolean> {
		if (this.#signal.aborted) {
			return Promise.reject(new Error('Distributed game compiler is closed'));
		}
		this.#dirty = true;
		if (this.#running !== undefined) return this.#running;
		this.#running = (async () => {
			let changed = false;
			while (this.#dirty && !this.#signal.aborted) {
				this.#dirty = false;
				if (await generateGames(this.#require(), 'generate', this.#children, this.#signal)) {
					changed = true;
				}
			}
			return changed;
		})().finally(() => {
			this.#running = undefined;
		});
		return this.#running;
	}

	async #generateInDev(): Promise<boolean> {
		try {
			const changed = await this.generate();
			this.#failure = undefined;
			return changed;
		} catch (error) {
			if (this.#signal.aborted) return false;
			this.#report(error);
			return false;
		}
	}

	#report(error: unknown): void {
		// The watcher and HMR hooks share one run: report its failure once.
		if (error === this.#failure) return;
		this.#failure = error;
		this.#logger.error(
			`[distributed] game client generation failed; the dev server keeps the last generated client:\n${
				error instanceof Error ? error.message : String(error)
			}`
		);
		this.#server?.ws.send({ type: 'error', err: viteError(error) });
	}

	#require(): GameTarget {
		if (this.#target === undefined) {
			throw new Error('Distributed game generation has not received resolved config');
		}
		return this.#target;
	}
}
