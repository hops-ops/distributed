import type { ChildProcess } from 'node:child_process';
import { existsSync, realpathSync } from 'node:fs';
import {
	lstat,
	mkdir,
	mkdtemp,
	readFile,
	readdir,
	realpath,
	rename,
	rm,
	writeFile
} from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { isMainThread } from 'node:worker_threads';

import { isMissing, isWithin } from '../lib/node-fs.js';
import {
	acquireCompilerLock,
	releaseCompilerLock,
	requireCompilerLock,
	withCompilerLock,
	withCompilerStartup,
	type CompilerLockLease
} from '../vite/compiler-lock.js';
import {
	containedPath,
	manifestWatchRoots,
	nonempty,
	physicalPathKey,
	plannedPhysicalPath,
	portablePath,
	resolveCompilerCommand,
	runCancellable,
	runCommand,
	stopCompiler,
	throwIfAborted,
	validateNearestExistingParent,
	viteError
} from '../vite/compiler-process.js';
import { GameGenerator } from '../vite/game-generator.js';
import {
	resolveGames,
	type DistributedGameSurfaceCompiler,
	type ResolvedGame
} from '../vite/games.js';
import {
	activeLifecycleGenerationRoot,
	canonicalExistingPath,
	configureLifecycleServer,
	lifecycleActivePointer,
	lifecycleGenerationMeta,
	localFrameworkDist,
	suppressFrameworkHotUpdate
} from '../vite/lifecycle-middleware.js';
import type {
	CompilerManifestSource,
	LifecycleHtmlTag,
	RollupWatchContextLike,
	ViteHotContextLike,
	ViteServerLike
} from '../vite/types.js';
import {
	analyzeDistributedSvelteKitBoundaries,
	validateDistributedSvelteKitBoundaryPlan,
	type DistributedIslandInventory,
	type DistributedSvelteKitBoundaryPlan,
	type DistributedSvelteKitBoundaryRegistration
} from './islands/boundaries.js';

export {
	analyzeDistributedSvelteKitBoundaries,
	validateDistributedSvelteKitBoundaryPlan,
	type DistributedIslandInventory,
	type DistributedIslandPlanInput,
	type DistributedSvelteKitBoundary,
	type DistributedSvelteKitBoundaryAnalysisClient,
	type DistributedSvelteKitBoundaryAnalysisOptions,
	type DistributedSvelteKitBoundaryOccurrence,
	type DistributedSvelteKitBoundaryPlan,
	type DistributedSvelteKitBoundaryRegistration
} from './islands/boundaries.js';

export {
	checkDistributedGames,
	generateDistributedGames,
	type DistributedGameManifestSource,
	type DistributedGameSurfaceCompiler
} from '../vite/games.js';

const GENERATED_SVELTEKIT_MODULE = 'sveltekit.ts';
const GENERATED_BOUNDARIES_MODULE = 'boundaries.ts';
const MAX_GENERATED_COMPARE_FILES = 20_000;
const MAX_GENERATED_COMPARE_BYTES = 128 * 1024 * 1024;
const MODULE_NAME = /^\$distributed(?:\/[A-Za-z0-9][A-Za-z0-9._-]*)*$/;

export type DistributedGraphqlProxyOptions = {
	/** Absolute Distributed API origin, for example `http://127.0.0.1:8791`. */
	target: string;
	/** Proxy key. Defaults to `/graphql` and includes its `/ws` child. */
	path?: string;
};

/** Vite proxy config for GraphQL HTTP and WebSocket traffic. */
export function distributedGraphqlProxy(
	options: DistributedGraphqlProxyOptions | string
): Record<string, { target: string; changeOrigin: true; ws: true }> {
	const target = (
		typeof options === 'string' ? options : options.target
	)
		.trim()
		.replace(/\/$/, '');
	const path =
		typeof options === 'string' ? '/graphql' : (options.path ?? '/graphql');
	if (!/^https?:\/\//.test(target)) {
		throw new Error(
			'distributedGraphqlProxy target must be an absolute http(s) URL'
		);
	}
	if (!path.startsWith('/')) {
		throw new Error('distributedGraphqlProxy path must start with /');
	}
	return { [path]: { target, changeOrigin: true, ws: true } };
}

export type DistributedSvelteKitManifestSource =
	| string
	| Readonly<{
			/**
			 * Arguments passed to the configured distributed command. The first value
			 * must be `client-manifest`; stdout becomes ephemeral compiler input.
			 */
			args: readonly string[];
	  }>;

export type DistributedSvelteKitClientCompiler = Readonly<{
	/** `$distributed` or an explicit elevated entrypoint such as `$distributed/admin`. */
	module: string;
	/** Existing manifest path, or canonical `distributed client-manifest` argv. */
	manifest: DistributedSvelteKitManifestSource;
	/** Verify exactly one concrete role. Mutually exclusive with `surface`. */
	role?: string;
	/** Verify exactly one Rust-declared application surface. */
	surface?: string;
	/** GraphQL globs passed verbatim as repeated `distributed client --documents`. */
	documents: readonly string[];
	/** Typed fallback when static component ownership cannot be proven. */
	boundaries?: readonly DistributedSvelteKitBoundaryRegistration[];
	/** Compiler-owned artifact directory, relative to `cwd` by default. */
	out: string;
}>;


export type DistributedSvelteKitViteOptions = Readonly<{
	/** Project root used for distributed cwd, document globs, and output containment. */
	cwd?: string;
	/** Executable invoked without a shell. Defaults to `distributed`. */
	command?: string;
	/**
	 * Prefix argv for standalone command execution, e.g. `cargo run ... --`;
	 * never interpreted by a shell. Lifecycle-owned compilation invokes the
	 * trusted initiating executable directly and ignores this launcher prefix.
	 */
	commandArgs?: readonly string[];
	/** SvelteKit route source root. Defaults to `src/routes`. */
	routesDir?: string;
	/** SvelteKit library source root. Defaults to `src/lib`. */
	libDir?: string;
	/** Additional project-local `$name` aliases used by static component imports. */
	aliases?: Readonly<Record<string, string>>;
	/** GraphQL client surfaces (lifecycle-owned). Optional when `games` is set. */
	clients?: readonly DistributedSvelteKitClientCompiler[];
	/**
	 * `distributed_game` surfaces generated by this plugin on dev start, on
	 * Rust changes and on build (`game-manifest` → `game-client` →
	 * `game-wasm`). Not lifecycle-owned: they run in the Vite process.
	 */
	games?: readonly DistributedGameSurfaceCompiler[];
}>;


type ResolvedClient = Readonly<{
	module: string;
	sourceRoot: string;
	manifest: DistributedSvelteKitManifestSource;
	selector: readonly ['--role' | '--surface', string];
	documents: readonly string[];
	boundaries: readonly DistributedSvelteKitBoundaryRegistration[];
	out: string;
	entry: string;
	adapterOut: string;
	watchRoots: readonly string[];
	manifestWatchRoots: readonly string[];
}>;

type ResolvedIntegration = Readonly<{
	cwd: string;
	/** Root that contains compiler-owned outputs; differs from cwd for lifecycle stages. */
	outputRoot: string;
	command: string;
	commandArgs: readonly string[];
	routesDir: string;
	libDir: string;
	aliases: Readonly<Record<string, string>>;
	clients: readonly ResolvedClient[];
	games: readonly ResolvedGame[];
}>;


/** Minimal structural Vite plugin type; avoids making Vite a runtime dependency. */
export type DistributedSvelteKitVitePlugin = Readonly<{
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
	resolveId(source: string, importer?: string): string | undefined;
	load(id: string): string | undefined;
	transform(code: string, id: string, options?: Readonly<{ ssr?: boolean }>): string | undefined;
	transformIndexHtml(): LifecycleHtmlTag[];
	handleHotUpdate(context: ViteHotContextLike): Promise<never[] | undefined>;
	watchChange(id: string): Promise<void>;
	closeBundle(): Promise<void>;
}>;

export type DistributedLifecycleVitePlugin = Readonly<{
	name: string;
	enforce: 'pre';
	configResolved(config: Readonly<{ root: string }>): void;
	configureServer(server: ViteServerLike): void;
	transformIndexHtml(): LifecycleHtmlTag[];
	handleHotUpdate(context: ViteHotContextLike): never[] | undefined;
}>;

/** Internal browser lifecycle side channel without generated-client resolution. */
export function distributedLifecycle(): DistributedLifecycleVitePlugin {
	let frameworkDist: string | undefined;
	return {
		name: '@hops-ops/distributed:lifecycle',
		enforce: 'pre',
		configResolved(config): void {
			frameworkDist = localFrameworkDist(config.root);
		},
		configureServer: configureLifecycleServer,
		transformIndexHtml: lifecycleGenerationMeta,
		handleHotUpdate(context): never[] | undefined {
			return suppressFrameworkHotUpdate(context, frameworkDist);
		}
	};
}

/** Generate every configured surface through the same transaction used by Vite. */
export async function generateDistributedSvelteKit(
	options: DistributedSvelteKitViteOptions
): Promise<void> {
	await runCompilerOnce(options, 'generate');
}

/**
 * Generate a client tree into an immutable lifecycle stage.
 *
 * This is the compiler boundary used by `distributed build` and `distributed
 * dev`; application sources are still read from `options.cwd`, while every
 * compiler-owned output is redirected beneath the candidate generation.
 */
export async function generateDistributedSvelteKitLifecycle(
	options: DistributedSvelteKitViteOptions,
	lifecycle: Readonly<{ projectRoot: string; stage: string }>
): Promise<void> {
	const resolved = resolveIntegration(options, options.cwd ?? process.cwd());
	await runResolvedCompilerOnce(
		relocateLifecycleOutputs(resolved, lifecycle),
		'generate'
	);
}

/** Check every configured surface through canonical `distributed client --check`; never write. */
export async function checkDistributedSvelteKit(
	options: DistributedSvelteKitViteOptions
): Promise<void> {
	await runCompilerOnce(options, 'check');
}

/**
 * Compiler-watching Vite integration for one or more authorization surfaces.
 *
 * Every run stages every surface first, then swaps the complete generated
 * trees as one rollback-capable transaction. Child processes are serialized,
 * coalesced, abortable, and always invoked without a shell.
 */
export function distributedSvelteKit(
	options: DistributedSvelteKitViteOptions
): DistributedSvelteKitVitePlugin {
	const lifecycleOwnsCompile =
		process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE === '1';
	let resolved: ResolvedIntegration | undefined;
	let dirty = false;
	let running: Promise<void> | undefined;
	let completedGeneration = 0;
	let reloadedGeneration = 0;
	let stopped = false;
	let frameworkDist: string | undefined;
	const children = new Set<ChildProcess>();
	const cancellation = new AbortController();
	let lock: CompilerLockLease | undefined;
	const games = new GameGenerator(children, cancellation.signal);

	const stop = async (): Promise<void> => {
		if (stopped) return;
		stopped = true;
		dirty = false;
		await stopCompiler(cancellation, children, [running, games.running]);
		if (lock !== undefined) {
			await releaseCompilerLock(lock);
			lock = undefined;
		}
	};

	const compile = (reason: string, startup = false): Promise<void> => {
		if (stopped) {
			return Promise.reject(
				new Error(`Distributed SvelteKit compiler is closed (${reason})`)
			);
		}
		dirty = true;
		if (running !== undefined) return running;
		running = (async () => {
			let startupPending = startup;
			while (dirty && !stopped) {
				dirty = false;
				const lease = requireCompilerLock(lock);
				const integration = requireResolved(resolved);
				const operation = () =>
					compileTransaction(integration, children, cancellation.signal);
				await (startupPending
					? withCompilerStartup(lease, JSON.stringify(integration), operation)
					: withCompilerLock(lease, operation));
				startupPending = false;
			}
			completedGeneration += 1;
		})().finally(() => {
			running = undefined;
		});
		return running;
	};

	return {
		name: '@hops-ops/distributed:sveltekit',
		enforce: 'pre',
		async configResolved(config): Promise<void> {
			if (resolved !== undefined) {
				throw new Error(
					'Distributed SvelteKit Vite plugin was configured more than once'
				);
			}
			resolved = resolveIntegration(options, options.cwd ?? config.root);
			games.configure(resolved, config);
			frameworkDist = localFrameworkDist(config.root);
			await validateResolvedPaths(resolved);
			/*
			 * The lifecycle has already built the active generation before it starts
			 * this UI process. The supervisor activates later generations atomically
			 * while this server stays available; compiling again here would race that
			 * owner and can prevent the UI from reaching its readiness probe.
			 */
			/*
			 * SvelteKit post-build analysis loads Vite config in an isolated
			 * worker marked with SVELTEKIT_FORK. That pass reads framework
			 * configuration only; running the compiler there would contend with
			 * the owning build's physical project lock and duplicate generation.
			 */
			if (!isMainThread && process.env.SVELTEKIT_FORK === 'true') return;
			// Game surfaces are not lifecycle outputs: they are generated here.
			await games.start(config.command === 'build');
			if (lifecycleOwnsCompile || resolved.clients.length === 0) return;
			lock = await acquireCompilerLock(resolved.cwd);
			try {
				await compile('Vite startup', true);
			} catch (error) {
				await releaseCompilerLock(lock);
				lock = undefined;
				throw error;
			}
		},
		configureServer(server): void {
			configureLifecycleServer(server);
			const integration = requireResolved(resolved);
			const activePointer = lifecycleActivePointer();
			if (lifecycleOwnsCompile && activePointer !== undefined) {
				server.watcher.add(activePointer);
			}
			if (!lifecycleOwnsCompile && integration.clients.length > 0) {
				const roots = [
					integration.routesDir,
					integration.libDir,
					...integration.clients.flatMap((client) => [
						...client.watchRoots,
						...client.manifestWatchRoots
					])
				];
				if (roots.length > 0) server.watcher.add(roots);
			}
			games.configureServer(server);
			server.httpServer?.once('close', () => {
				void stop();
			});
		},
		buildStart(): void {
			const integration = requireResolved(resolved);
			for (const root of games.watchRoots()) this.addWatchFile(root);
			if (lifecycleOwnsCompile || integration.clients.length === 0) return;
			this.addWatchFile(integration.routesDir);
			this.addWatchFile(integration.libDir);
			for (const client of integration.clients) {
				for (const root of [...client.watchRoots, ...client.manifestWatchRoots]) this.addWatchFile(root);
			}
		},
		resolveId(source, importer): string | undefined {
			if (lifecycleOwnsCompile && frameworkDist !== undefined) {
				if (source === '@hops-ops/distributed/replica/lazy') {
					return join(frameworkDist, 'replica', 'lazy.js');
				}
				if (source === '@hops-ops/distributed/replica') {
					return join(frameworkDist, 'replica', 'index.js');
				}
				if (source === '@hops-ops/distributed/sveltekit') {
					return join(frameworkDist, 'sveltekit', 'index.js');
				}
			}
			if (
				lifecycleOwnsCompile &&
				importer !== undefined &&
				(source.startsWith('./') || source.startsWith('../'))
			) {
				const lifecycleRoot = process.env.DISTRIBUTED_LIFECYCLE_DIR;
				if (
					lifecycleRoot !== undefined &&
					isAbsolute(lifecycleRoot) &&
					isWithin(join(resolve(lifecycleRoot), 'generations'), importer)
				) {
					const generationRoot = activeLifecycleGenerationRoot();
					const candidate = resolve(dirname(importer), source);
					if (!isWithin(generationRoot, importer) || existsSync(candidate)) {
						return undefined;
					}
					const projectRoot = process.env.DISTRIBUTED_LIFECYCLE_PROJECT_ROOT;
					if (projectRoot === undefined || !isAbsolute(projectRoot)) {
						throw new Error('Distributed lifecycle project root is unavailable');
					}
					const workspaceCandidate = join(
						resolve(projectRoot),
						relative(generationRoot, candidate)
					);
					if (existsSync(workspaceCandidate)) return workspaceCandidate;
				}
			}
			const client = resolved?.clients.find(
				(candidate) => candidate.module === source
			);
			return client === undefined ? undefined : virtualId(client.module);
		},
		load(id): string | undefined {
			const client = resolved?.clients.find(
				(candidate) => virtualId(candidate.module) === id
			);
			if (client === undefined) return undefined;
			const entry = lifecycleOwnsCompile
				? activeLifecycleClientEntry(client)
				: client.entry;
			return `export * from ${JSON.stringify(portablePath(entry))};\n`;
		},
		transform(code, id, options): string | undefined {
			if (!lifecycleOwnsCompile || options?.ssr || frameworkDist === undefined) return;
			if (id.split('?', 1)[0] !== join(frameworkDist, 'sveltekit', 'lifecycle.js')) return;
			// SvelteKit aliases can resolve generated clients before the virtual
			// module hook. Attach once to the actual shared browser lifecycle.
			return code + `\nif (import.meta.hot) import.meta.hot.on('vite:ws:disconnect', () => distributedReloadLifecycle().deferDevTransportReload());\n`;
		},
		transformIndexHtml: lifecycleGenerationMeta,
		async handleHotUpdate(context): Promise<never[] | undefined> {
			const suppressed = suppressFrameworkHotUpdate(context, frameworkDist);
			if (suppressed !== undefined) return suppressed;
			const integration = requireResolved(resolved);
			if (
				lifecycleOwnsCompile &&
				lifecycleActivePointer() === canonicalExistingPath(context.file)
			) {
				for (const client of integration.clients) {
					const module = context.server.moduleGraph.getModuleById(
						virtualId(client.module)
					);
					if (module !== undefined) context.server.moduleGraph.invalidateModule(module);
				}
				// The browser lifecycle owns the one coordinated document reload after
				// every participant has persisted its capsule. Vite only needs to drop
				// the old virtual modules so that reload resolves the active generation.
				return [];
			}
			if (games.isInput(context.file)) {
				await games.hotUpdate(context.server);
				if (!isCompilerInput(context.file, integration)) return [];
			}
			if (!isCompilerInput(context.file, integration)) return undefined;
			if (lifecycleOwnsCompile) return [];
			try {
				await compile(`GraphQL/Svelte change ${context.file}`);
			} catch (error) {
				context.server.ws.send({
					type: 'error',
					err: viteError(error)
				});
				throw error;
			}
			if (completedGeneration <= reloadedGeneration) return [];
			for (const client of integration.clients) {
				const module = context.server.moduleGraph.getModuleById(
					virtualId(client.module)
				);
				if (module !== undefined) {
					context.server.moduleGraph.invalidateModule(module);
				}
			}
			if (!lifecycleOwnsCompile) {
				context.server.ws.send({ type: 'full-reload', path: '*' });
			}
			reloadedGeneration = completedGeneration;
			return [];
		},
		async watchChange(id): Promise<void> {
			const integration = requireResolved(resolved);
			if (games.isInput(id)) await games.watchChange();
			if (lifecycleOwnsCompile) return;
			if (isCompilerInput(id, integration)) {
				await compile(`GraphQL/Svelte watch change ${id}`);
			}
		},
		closeBundle: stop
	};
}



async function runCompilerOnce(
	options: DistributedSvelteKitViteOptions,
	mode: 'generate' | 'check'
): Promise<void> {
	const integration = resolveIntegration(
		options,
		options.cwd ?? process.cwd()
	);
	await runResolvedCompilerOnce(integration, mode);
}

async function runResolvedCompilerOnce(
	integration: ResolvedIntegration,
	mode: 'generate' | 'check'
): Promise<void> {
	await validateResolvedPaths(integration);
	const lock = await acquireCompilerLock(integration.cwd);
	try {
		await runCancellable((children, signal) =>
			withCompilerLock(lock, () =>
				mode === 'generate'
					? compileTransaction(integration, children, signal)
					: checkTransaction(integration, children, signal)
			)
		);
	} finally {
		await releaseCompilerLock(lock);
	}
}

/**
 * SvelteKit language-tool aliases for the exact files used by Vite resolution.
 *
 * Keep this alongside the Vite plugin in the Node-only export. It performs no
 * generation and is safe to call from `svelte.config.js`.
 */
export function distributedSvelteKitAliases(
	options: Pick<DistributedSvelteKitViteOptions, 'cwd' | 'clients'>
): Readonly<Record<string, string>> {
	const integration = resolveIntegration(
		{ ...options, command: 'distributed', commandArgs: [] },
		options.cwd ?? process.cwd()
	);
	validateResolvedPathsSync(integration);
	const lifecycleOwnsCompile =
		process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE === '1';
	const frameworkDist = lifecycleOwnsCompile
		? localFrameworkDist(integration.cwd)
		: undefined;
	if (lifecycleOwnsCompile && frameworkDist === undefined) {
		throw new Error('Installed @hops-ops/distributed package is unavailable');
	}
	const aliases = [...integration.clients]
		.sort(
			(left, right) =>
				right.module.length - left.module.length ||
				left.module.localeCompare(right.module)
		)
		.map((client) => [
			client.module,
			lifecycleOwnsCompile ? activeLifecycleClientEntry(client) : client.entry
		]);
	if (frameworkDist !== undefined) {
		aliases.push(
			['@hops-ops/distributed/replica/lazy', join(frameworkDist, 'replica', 'lazy.js')],
			['@hops-ops/distributed/replica', join(frameworkDist, 'replica')],
			['@hops-ops/distributed/sveltekit', join(frameworkDist, 'sveltekit')]
		);
	}
	return Object.freeze(
		Object.fromEntries(aliases)
	);
}


function activeLifecycleClientEntry(client: ResolvedClient): string {
	const generationRoot = activeLifecycleGenerationRoot();
	const projectRoot = process.env.DISTRIBUTED_LIFECYCLE_PROJECT_ROOT!;
	const project = realpathSync(resolve(projectRoot));
	const sourceRoot = realpathSync(client.sourceRoot);
	if (!isWithin(project, sourceRoot) || !isWithin(client.sourceRoot, client.entry)) {
		throw new Error('Distributed client entry escapes the lifecycle project root');
	}
	return join(
		generationRoot,
		relative(project, sourceRoot),
		relative(client.sourceRoot, client.entry)
	);
}


function resolveIntegration(
	options: DistributedSvelteKitViteOptions,
	fallbackCwd: string
): ResolvedIntegration {
	if (options === null || typeof options !== 'object') {
		throw new TypeError('distributedSvelteKit requires configuration');
	}
	const cwd = resolve(options.cwd ?? fallbackCwd);
	const compiler = resolveCompilerCommand(cwd, options);
	const configuredClients = options.clients ?? [];
	const configuredGames = options.games ?? [];
	if (!Array.isArray(configuredClients) || !Array.isArray(configuredGames)) {
		throw new TypeError('Distributed SvelteKit clients and games must be arrays');
	}
	if (configuredClients.length === 0 && configuredGames.length === 0) {
		throw new TypeError(
			'Distributed SvelteKit requires at least one client or game surface'
		);
	}
	const modules = new Set<string>();
	const outputs: string[] = [];
	const routesDir = containedPath(cwd, options.routesDir ?? 'src/routes', 'routesDir');
	const libDir = containedPath(cwd, options.libDir ?? 'src/lib', 'libDir');
	const aliases = Object.freeze(
		Object.fromEntries(
			Object.entries(options.aliases ?? {}).map(([key, value]) => {
				if (!/^\$[A-Za-z0-9_-]+$/.test(key)) {
					throw new TypeError(`Distributed SvelteKit alias \`${key}\` must be a single $name segment`);
				}
				return [key, portablePath(relative(cwd, containedPath(cwd, value, `alias ${key}`)))];
			})
		)
	);
	const clients = configuredClients.map((client, index): ResolvedClient => {
		if (client === null || typeof client !== 'object') {
			throw new TypeError(`Distributed client[${index}] must be an object`);
		}
		if (!MODULE_NAME.test(client.module)) {
			throw new TypeError(
				`Distributed client module \`${client.module}\` must be $distributed or a safe $distributed/<entrypoint>`
			);
		}
		if (modules.has(client.module)) {
			throw new TypeError(
				`duplicate Distributed client module \`${client.module}\``
			);
		}
		modules.add(client.module);
		const selector =
			client.role !== undefined && client.surface === undefined
				? (['--role', nonempty(client.role, `${client.module} role`)] as const)
				: client.surface !== undefined && client.role === undefined
					? ([
							'--surface',
							nonempty(client.surface, `${client.module} surface`)
						] as const)
					: undefined;
		if (selector === undefined) {
			throw new TypeError(
				`Distributed client \`${client.module}\` requires exactly one of role or surface`
			);
		}
		if (
			!Array.isArray(client.documents) ||
			client.documents.length === 0
		) {
			throw new TypeError(
				`Distributed client \`${client.module}\` requires at least one GraphQL document glob`
			);
		}
		const documents = client.documents.map((document: string, documentIndex: number) =>
			nonempty(
				document,
				`${client.module} documents[${documentIndex}]`
			)
		);
		const boundaries = (client.boundaries ?? []).map((
			boundary: DistributedSvelteKitBoundaryRegistration,
			boundaryIndex: number
		) => {
			if (
				boundary === null ||
				typeof boundary !== 'object' ||
				(boundary.kind !== 'page' && boundary.kind !== 'layout')
			) {
				throw new TypeError(`${client.module} boundaries[${boundaryIndex}] is invalid`);
			}
			return Object.freeze({
				operation: nonempty(boundary.operation, `${client.module} boundaries[${boundaryIndex}].operation`),
				route: nonempty(boundary.route, `${client.module} boundaries[${boundaryIndex}].route`),
				kind: boundary.kind,
				...(boundary.variables === undefined
					? {}
					: { variables: boundary.variables })
			});
		});
		validateManifestSource(client.module, client.manifest);
		const out = containedPath(
			cwd,
			client.out,
			`${client.module} generated output`
		);
		if (out === cwd) {
			throw new TypeError(
				`Distributed client \`${client.module}\` cannot use the project root as generated output`
			);
		}
		for (const existing of outputs) {
			if (isWithin(existing, out) || isWithin(out, existing)) {
				throw new TypeError(
					`Distributed client outputs must not overlap: \`${existing}\` and \`${out}\``
				);
			}
		}
		outputs.push(out);
		const adapterOut = join(
			cwd,
			'.svelte-kit',
			'distributed',
			'clients',
			Buffer.from(client.module).toString('base64url')
		);
		return Object.freeze({
			module: client.module,
			sourceRoot: cwd,
			manifest: client.manifest,
			selector,
			documents: Object.freeze(documents),
			boundaries: Object.freeze(boundaries),
			out,
			entry: join(out, GENERATED_SVELTEKIT_MODULE),
			adapterOut,
			watchRoots: Object.freeze(documentWatchRoots(cwd, documents, out)),
			manifestWatchRoots: Object.freeze(manifestWatchRoots(cwd, client.manifest))
		});
	});
	return Object.freeze({
		...compiler,
		outputRoot: cwd,
		routesDir,
		libDir,
		aliases,
		clients: Object.freeze(clients),
		games: Object.freeze(resolveGames(cwd, configuredGames, outputs))
	});
}

function relocateLifecycleOutputs(
	integration: ResolvedIntegration,
	lifecycle: Readonly<{ projectRoot: string; stage: string }>
): ResolvedIntegration {
	const projectRoot = realpathSync(resolve(lifecycle.projectRoot));
	const sourceRoot = realpathSync(integration.cwd);
	const stage = realpathSync(resolve(lifecycle.stage));
	if (!isWithin(projectRoot, sourceRoot)) {
		throw new TypeError('Distributed lifecycle UI must stay within the project root');
	}
	if (stage === projectRoot || isWithin(stage, projectRoot)) {
		throw new TypeError('Distributed lifecycle stage must not contain the project source tree');
	}
	const sourcePrefix = relative(projectRoot, sourceRoot);
	const relocate = (path: string): string =>
		join(stage, sourcePrefix, relative(integration.cwd, path));
	return Object.freeze({
		...integration,
		outputRoot: stage,
		clients: Object.freeze(
			integration.clients.map((client) => {
				const out = relocate(client.out);
				return Object.freeze({
					...client,
					out,
					entry: join(out, GENERATED_SVELTEKIT_MODULE),
					adapterOut: relocate(client.adapterOut)
				});
			})
		)
	});
}

function validateManifestSource(
	module: string,
	source: CompilerManifestSource
): void {
	if (typeof source === 'string') {
		nonempty(source, `${module} manifest path`);
		return;
	}
	if (
		source === null ||
		typeof source !== 'object' ||
		!Array.isArray(source.args) ||
		source.args.length === 0 ||
		source.args[0] !== 'client-manifest' ||
		source.args.some((argument) => typeof argument !== 'string')
	) {
		throw new TypeError(
			`Distributed client \`${module}\` manifest args must begin with client-manifest`
		);
	}
}


function documentWatchRoots(
	cwd: string,
	documents: readonly string[],
	out: string
): string[] {
	const roots = new Set<string>();
	for (const pattern of documents) {
		const normalized = pattern.replaceAll('\\', '/');
		const wildcard = normalized.search(/[*?[\]{}]/);
		const prefix = wildcard === -1 ? normalized : normalized.slice(0, wildcard);
		const base = prefix.endsWith('/')
			? prefix.slice(0, -1)
			: dirname(prefix);
		const watch = resolve(cwd, base === '.' || base.length === 0 ? '.' : base);
		if (!isWithin(cwd, watch)) {
			throw new TypeError(
				`Distributed GraphQL document glob \`${pattern}\` escapes project root ${cwd}`
			);
		}
		if (!isWithin(out, watch)) roots.add(watch);
	}
	return [...roots].sort();
}


function isCompilerInput(file: string, integration: ResolvedIntegration): boolean {
	if (integration.clients.length === 0) return false;
	if (isGraphqlInput(file, integration)) return true;
	const absolute = resolve(integration.cwd, file);
	const basenameValue = basename(absolute);
	if (!absolute.endsWith('.rs') && basenameValue !== 'Cargo.toml' && basenameValue !== 'Cargo.lock') {
		return false;
	}
	return integration.clients.some((client) =>
		client.manifestWatchRoots.some((root) => isWithin(root, absolute))
	);
}

function isGraphqlInput(
	file: string,
	integration: ResolvedIntegration
): boolean {
	const absolute = resolve(integration.cwd, file);
	const isDocument = absolute.endsWith('.graphql') || absolute.endsWith('.gql');
	const isBindings =
		absolute.endsWith('.graphql.bindings.js') ||
		absolute.endsWith('.gql.bindings.js');
	if (
		absolute.endsWith('.svelte') &&
		(isWithin(integration.routesDir, absolute) ||
			isWithin(integration.libDir, absolute))
	) {
		return true;
	}
	if (
		(!isDocument && !isBindings) ||
		!isWithin(integration.cwd, absolute)
	) {
		return false;
	}
	if (
		integration.clients.some((client) => isWithin(client.out, absolute))
	) {
		return false;
	}
	return integration.clients.some((client) =>
		client.watchRoots.some((root) => isWithin(root, absolute))
	);
}

async function compileTransaction(
	integration: ResolvedIntegration,
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<void> {
	throwIfAborted(signal);
	await validateResolvedPaths(integration);
	const transactionRoot = await compilerTransactionRoot(integration);
	const transaction = await mkdtemp(
		join(transactionRoot, '.distributed-sveltekit-')
	);
	const staged: Array<{
		client: ResolvedClient;
		output: string;
		backup: string;
		hadOutput: boolean;
		adapterOutput: string;
		adapterBackup: string;
		hadAdapterOutput: boolean;
	}> = [];
	try {
		for (const [index, client] of integration.clients.entries()) {
			throwIfAborted(signal);
			const manifest = await materializeManifest(
				integration,
				client,
				transaction,
				index,
				children,
				signal
			);
			const output = join(transaction, `output-${index}`);
			let hadOutput = false;
			try {
				const metadata = await lstat(client.out);
				if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
					throw new Error(
						`generated output ${client.out} must be a real directory`
					);
				}
				hadOutput = true;
			} catch (error) {
				if (!isMissing(error)) throw error;
			}
			await mkdir(output, { recursive: true });
			const args = [
				'client',
				'--manifest',
				manifest,
				client.selector[0],
				client.selector[1],
				...client.documents.flatMap((document) => [
					'--documents',
					document
				]),
				'--out',
				output
			];
			await runCommand(integration, args, children, signal);
			await validateGeneratedEntrypoint(transaction, output, client.module);
			staged.push({
				client,
				output,
				backup: join(transaction, `backup-${index}`),
				hadOutput,
				adapterOutput: join(transaction, `adapter-${index}`),
				adapterBackup: join(transaction, `adapter-backup-${index}`),
				hadAdapterOutput: await realDirectoryExists(client.adapterOut)
			});
		}
		const plans = await analyzeStagedBoundaries(integration, transaction, staged);
		const plansByModule = new Map(plans.map((plan) => [plan.module, plan]));
		for (const item of staged) {
			const plan = plansByModule.get(item.client.module);
			if (plan === undefined) {
				throw new Error(
					`Distributed SvelteKit boundary analysis returned no plan for ${item.client.module}`
				);
			}
			await writeFile(
				join(item.output, GENERATED_BOUNDARIES_MODULE),
				boundaryModuleSource(plan),
				{ encoding: 'utf8', flag: 'wx' }
			);
			await exposeBoundaryModule(item.output);
			await mkdir(item.adapterOutput, { recursive: true });
			await writeFile(
				join(item.adapterOutput, 'boundaries.json'),
				boundaryPlanSource(plan),
				{ encoding: 'utf8', flag: 'wx' }
			);
		}
		throwIfAborted(signal);
		await commitOutputs(integration, staged, signal);
	} finally {
		await rm(transaction, { recursive: true, force: true });
	}
}

async function checkTransaction(
	integration: ResolvedIntegration,
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<void> {
	throwIfAborted(signal);
	await validateResolvedPaths(integration);
	const transactionRoot = await compilerTransactionRoot(integration);
	const transaction = await mkdtemp(
		join(transactionRoot, '.distributed-sveltekit-check-')
	);
	try {
		const staged: Array<Readonly<{ client: ResolvedClient; output: string }>> = [];
		for (const [index, client] of integration.clients.entries()) {
			throwIfAborted(signal);
			const manifest = await materializeManifest(
				integration,
				client,
				transaction,
				index,
				children,
				signal
			);
			const output = join(transaction, `output-${index}`);
			await mkdir(output, { recursive: true });
			await runCommand(
				integration,
				[
					'client',
					'--manifest',
					manifest,
					client.selector[0],
					client.selector[1],
					...client.documents.flatMap((document) => [
						'--documents',
						document
					]),
					'--out',
					output
				],
				children,
				signal
			);
			await validateGeneratedEntrypoint(integration.cwd, output, client.module);
			staged.push(Object.freeze({ client, output }));
		}
		const plans = await analyzeStagedBoundaries(integration, transaction, staged);
		const plansByModule = new Map(plans.map((plan) => [plan.module, plan]));
		for (const [index, client] of integration.clients.entries()) {
			const output = staged[index]!.output;
			const plan = plansByModule.get(client.module);
			if (plan === undefined) {
				throw new Error(
					`Distributed SvelteKit boundary analysis returned no plan for ${client.module}`
				);
			}
			await writeFile(
				join(output, GENERATED_BOUNDARIES_MODULE),
				boundaryModuleSource(plan),
				{ encoding: 'utf8', flag: 'wx' }
			);
			await exposeBoundaryModule(output);
			await compareGeneratedTrees(client.out, output, client.module);
			await validateAdapterBoundaryPlan(client, plan);
		}
	} finally {
		await rm(transaction, { recursive: true, force: true });
	}
}

async function compilerTransactionRoot(
	integration: ResolvedIntegration
): Promise<string> {
	const lifecycle = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	const root = lifecycle !== undefined && isAbsolute(lifecycle)
		? resolve(lifecycle)
		: integration.cwd;
	await mkdir(root, { recursive: true });
	return root;
}

async function validateAdapterBoundaryPlan(
	client: ResolvedClient,
	plan: DistributedSvelteKitBoundaryPlan
): Promise<void> {
	let actual: string;
	try {
		actual = await readFile(join(client.adapterOut, 'boundaries.json'), 'utf8');
	} catch (error) {
		/*
		 * `.svelte-kit` is SvelteKit-owned build state. A production build may
		 * replace it after our Vite startup generation, while the durable client
		 * tree (including boundaries.ts) remains current and was checked above.
		 */
		if (isMissing(error)) return;
		throw error;
	}
	let persisted: unknown;
	try {
		persisted = JSON.parse(actual);
	} catch {
		throw new Error(
			`[distributed.island.boundary_plan_invalid] ${client.module} boundaries.json is not valid JSON`
		);
	}
	validateDistributedSvelteKitBoundaryPlan(persisted, client.module);
	const expected = boundaryPlanSource(plan);
	if (actual !== expected) {
		throw new Error(
			`Distributed SvelteKit boundary plan for ${client.module} is stale; run generation without check`
		);
	}
}

async function analyzeStagedBoundaries(
	integration: ResolvedIntegration,
	transactionRoot: string,
	staged: readonly Readonly<{ client: ResolvedClient; output: string }>[]
): Promise<readonly DistributedSvelteKitBoundaryPlan[]> {
	return await analyzeDistributedSvelteKitBoundaries({
		cwd: integration.cwd,
		routesDir: portablePath(relative(integration.cwd, integration.routesDir)),
		libDir: portablePath(relative(integration.cwd, integration.libDir)),
		aliases: integration.aliases,
		clients: await Promise.all(
			staged.map(async ({ client, output }) => ({
				module: client.module,
				inventory: await readIslandInventory(transactionRoot, output),
				explicitBoundaries: client.boundaries
			}))
		)
	});
}

async function readIslandInventory(
	transactionRoot: string,
	output: string
): Promise<DistributedIslandInventory> {
	const path = join(output, 'islands.json');
	const metadata = await lstat(path);
	if (metadata.isSymbolicLink() || !metadata.isFile()) {
		throw new Error(`Distributed island inventory ${portablePath(relative(transactionRoot, path))} must be a regular file`);
	}
	const canonicalRoot = await realpath(transactionRoot);
	const canonical = await realpath(path);
	if (!isWithin(canonicalRoot, canonical)) {
		throw new Error('Distributed island inventory escaped the project root');
	}
	return JSON.parse(await readFile(canonical, 'utf8')) as DistributedIslandInventory;
}

function boundaryPlanSource(plan: DistributedSvelteKitBoundaryPlan): string {
	return `${JSON.stringify(plan, null, 2)}\n`;
}

function boundaryModuleSource(plan: DistributedSvelteKitBoundaryPlan): string {
	validateDistributedSvelteKitBoundaryPlan(plan, plan.module);
	const occurrences = plan.boundaries.flatMap((boundary) =>
		boundary.islands.map((island) => ({ boundary, island }))
	);
	const artifacts = new Map<string, Readonly<{ alias: string; module: string; exportName: string }>>();
	for (const { island } of occurrences) {
		if (
			!/^operations\/[A-Za-z0-9._-]+\.ts$/.test(island.modulePath) ||
			!/^[_A-Za-z][_0-9A-Za-z]*$/.test(island.exportName)
		) {
			throw new Error(
				`[distributed.island.boundary_plan_invalid] ${island.graphqlSource} has an unsafe generated artifact reference`
			);
		}
		const key = `${island.modulePath}\u0000${island.exportName}`;
		if (!artifacts.has(key)) {
			artifacts.set(key, Object.freeze({
				alias: `DistributedBoundaryArtifact_${artifacts.size}`,
				module: island.modulePath.slice(0, -3),
				exportName: island.exportName
			}));
		}
	}
	const imports = [...artifacts.values()].map(
		({ alias, module, exportName }) =>
			`import { ${exportName} as ${alias} } from './${module}.js';`
	);
	const definitions = occurrences.map(({ boundary, island }, index) => {
		const artifact = artifacts.get(`${island.modulePath}\u0000${island.exportName}`)!;
		const discovery =
			island.reason === 'static_component_import' ? 'component' : island.reason;
		return [
			`const DistributedBoundaryBinding_${index} = defineDistributedBoundaryBinding(`,
			`  ${artifact.alias},`,
			`  ${JSON.stringify(island.binding.sources, null, 2)} as const`,
			`);`,
			`const DistributedBoundaryOperation_${index} = defineDistributedBoundaryOperation(`,
			`  ${JSON.stringify({
				operation: island.operation,
				route: boundary.route,
				kind: boundary.kind,
				sourcePath: island.graphqlSource,
				discovery
			}, null, 2)} as const,`,
			`  ${artifact.alias},`,
			`  DistributedBoundaryBinding_${index}`,
			`);`
		].join('\n');
	});
	const operations = occurrences.map((_, index) => `  DistributedBoundaryOperation_${index}`);
	return [
		'/** GENERATED by the Distributed SvelteKit boundary planner. Do not edit. */',
		"import { defineDistributedBoundaryBinding, defineDistributedBoundaryOperation } from '@hops-ops/distributed/sveltekit';",
		...imports,
		'',
		`export const DISTRIBUTED_BOUNDARY_PLAN = ${JSON.stringify(plan, null, 2)} as const;`,
		'',
		...definitions,
		'',
		'/** Executable SSR/browser ownership assembled from the same boundary plan. */',
		`export const DISTRIBUTED_BOUNDARY_OPERATIONS = ${operations.length === 0 ? '[]' : `[\n${operations.join(',\n')}\n]`} as const;`,
		''
	].join('\n');
}

async function exposeBoundaryModule(output: string): Promise<void> {
	const path = join(output, GENERATED_SVELTEKIT_MODULE);
	const source = await readFile(path, 'utf8');
	if (!source.startsWith('/** GENERATED by distributed client. Do not edit. */')) {
		throw new Error('generated SvelteKit entrypoint is missing its ownership marker');
	}
	await writeFile(
		path,
		`${source.trimEnd()}\n\nexport { DISTRIBUTED_BOUNDARY_OPERATIONS, DISTRIBUTED_BOUNDARY_PLAN } from './boundaries.js';\n`,
		'utf8'
	);
}

async function compareGeneratedTrees(
	actualRoot: string,
	expectedRoot: string,
	module: string
): Promise<void> {
	const [actual, expected] = await Promise.all([
		readGeneratedTree(actualRoot),
		readGeneratedTree(expectedRoot)
	]);
	const drift: string[] = [];
	for (const [path, contents] of expected) {
		const current = actual.get(path);
		if (current === undefined) drift.push(`missing ${path}`);
		else if (current !== contents) drift.push(`changed ${path}`);
	}
	for (const path of actual.keys()) {
		if (!expected.has(path)) drift.push(`unexpected ${path}`);
	}
	if (drift.length > 0) {
		throw new Error(
			`Distributed SvelteKit client ${module} is stale:\n  ${drift.sort().join('\n  ')}\nrun generation without check`
		);
	}
}

async function readGeneratedTree(root: string): Promise<ReadonlyMap<string, string>> {
	const rootMetadata = await lstat(root);
	if (rootMetadata.isSymbolicLink() || !rootMetadata.isDirectory()) {
		throw new Error(`generated output ${root} must be a real directory`);
	}
	const files = new Map<string, string>();
	const pending: Array<Readonly<{ absolute: string; relative: string }>> = [
		{ absolute: root, relative: '' }
	];
	while (pending.length > 0) {
		const directory = pending.pop()!;
		for (const entry of await readdir(directory.absolute, { withFileTypes: true })) {
			const relativePath = directory.relative.length === 0
				? entry.name
				: `${directory.relative}/${entry.name}`;
			const absolutePath = join(directory.absolute, entry.name);
			if (entry.isSymbolicLink()) {
				throw new Error(`generated output contains unsupported symlink ${relativePath}`);
			}
			if (entry.isDirectory()) {
				pending.push({ absolute: absolutePath, relative: relativePath });
				continue;
			}
			if (!entry.isFile()) {
				throw new Error(`generated output contains unsupported entry ${relativePath}`);
			}
			files.set(relativePath, await readFile(absolutePath, 'utf8'));
			if (files.size > 8_192) {
				throw new Error('generated output exceeds 8192 files');
			}
		}
	}
	return files;
}

async function realDirectoryExists(path: string): Promise<boolean> {
	try {
		const metadata = await lstat(path);
		if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
			throw new Error(`Distributed SvelteKit adapter output ${path} must be a real directory`);
		}
		return true;
	} catch (error) {
		if (isMissing(error)) return false;
		throw error;
	}
}

async function materializeManifest(
	integration: ResolvedIntegration,
	client: ResolvedClient,
	transaction: string,
	index: number,
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<string> {
	throwIfAborted(signal);
	if (typeof client.manifest === 'string') {
		const path = containedPath(
			integration.cwd,
			client.manifest,
			`${client.module} manifest`
		);
		const canonicalRoot = await realpath(integration.cwd);
		const canonical = await realpath(path);
		if (!isWithin(canonicalRoot, canonical)) {
			throw new Error(
				`Distributed manifest ${path} resolves outside project root ${integration.cwd}`
			);
		}
		JSON.parse(await readFile(canonical, 'utf8')) as unknown;
		return canonical;
	}
	const result = await runCommand(
		integration,
		[...client.manifest.args],
		children,
		signal
	);
	try {
		JSON.parse(result.stdout) as unknown;
	} catch (error) {
		throw new Error(
			`distributed client-manifest for ${client.module} did not emit valid JSON`,
			{ cause: error }
		);
	}
	const temporary = join(transaction, `manifest-${index}.json.tmp`);
	const manifest = join(transaction, `manifest-${index}.json`);
	await writeFile(temporary, result.stdout, {
		encoding: 'utf8',
		flag: 'wx'
	});
	await rename(temporary, manifest);
	return manifest;
}

async function validateGeneratedEntrypoint(
	cwd: string,
	output: string,
	module: string
): Promise<void> {
	const root = await realpath(cwd);
	const entry = join(output, GENERATED_SVELTEKIT_MODULE);
	const metadata = await lstat(entry);
	if (metadata.isSymbolicLink() || !metadata.isFile()) {
		throw new Error(
			`distributed client for ${module} did not emit a regular ${GENERATED_SVELTEKIT_MODULE}`
		);
	}
	const canonical = await realpath(entry);
	if (!isWithin(root, canonical)) {
		throw new Error(
			`distributed client entrypoint ${canonical} escaped project root ${root}`
		);
	}
}

async function commitOutputs(
	integration: ResolvedIntegration,
	staged: readonly {
		client: ResolvedClient;
		output: string;
		backup: string;
		hadOutput: boolean;
		adapterOutput: string;
		adapterBackup: string;
		hadAdapterOutput: boolean;
	}[],
	signal: AbortSignal
): Promise<void> {
	throwIfAborted(signal);
	await validateResolvedPaths(integration);
	type Output = Readonly<{
		target: string;
		output: string;
		backup: string;
		hadOutput: boolean;
	}>;
	const outputs: Output[] = staged.flatMap((item) => [
		{
			target: item.client.out,
			output: item.output,
			backup: item.backup,
			hadOutput: item.hadOutput
		},
		{
			target: item.client.adapterOut,
			output: item.adapterOutput,
			backup: item.adapterBackup,
			hadOutput: item.hadAdapterOutput
		}
	]);
	const applied: Output[] = [];
	try {
		for (const item of outputs) {
			throwIfAborted(signal);
			await mkdir(dirname(item.target), { recursive: true });
			await validateNearestExistingParent(integration.outputRoot, item.target);
			if (item.hadOutput && await generatedTreesEqual(item.target, item.output)) {
				continue;
			}
			if (item.hadOutput) await rename(item.target, item.backup);
			try {
				await rename(item.output, item.target);
			} catch (error) {
				if (item.hadOutput) await rename(item.backup, item.target);
				throw error;
			}
			applied.push(item);
		}
		throwIfAborted(signal);
	} catch (error) {
		for (const item of [...applied].reverse()) {
			await rm(item.target, { recursive: true, force: true });
			if (item.hadOutput) await rename(item.backup, item.target);
		}
		throw error;
	}
}

async function generatedTreesEqual(left: string, right: string): Promise<boolean> {
	const budget = { files: 0, bytes: 0 };
	const compare = async (leftDirectory: string, rightDirectory: string): Promise<boolean> => {
		const [leftEntries, rightEntries] = await Promise.all([
			readdir(leftDirectory, { withFileTypes: true }),
			readdir(rightDirectory, { withFileTypes: true })
		]);
		leftEntries.sort((a, b) => a.name.localeCompare(b.name));
		rightEntries.sort((a, b) => a.name.localeCompare(b.name));
		if (leftEntries.length !== rightEntries.length) return false;
		for (let index = 0; index < leftEntries.length; index += 1) {
			const leftEntry = leftEntries[index];
			const rightEntry = rightEntries[index];
			if (
				leftEntry.name !== rightEntry.name ||
				leftEntry.isDirectory() !== rightEntry.isDirectory() ||
				leftEntry.isFile() !== rightEntry.isFile()
			) return false;
			if (!leftEntry.isDirectory() && !leftEntry.isFile()) return false;
			const leftPath = join(leftDirectory, leftEntry.name);
			const rightPath = join(rightDirectory, rightEntry.name);
			if (leftEntry.isDirectory()) {
				if (!await compare(leftPath, rightPath)) return false;
				continue;
			}
			budget.files += 1;
			if (budget.files > MAX_GENERATED_COMPARE_FILES) return false;
			const [leftMetadata, rightMetadata] = await Promise.all([
				lstat(leftPath),
				lstat(rightPath)
			]);
			if (leftMetadata.size !== rightMetadata.size) return false;
			budget.bytes += leftMetadata.size;
			if (budget.bytes > MAX_GENERATED_COMPARE_BYTES) return false;
			const [leftBytes, rightBytes] = await Promise.all([
				readFile(leftPath),
				readFile(rightPath)
			]);
			if (!leftBytes.equals(rightBytes)) return false;
		}
		return true;
	};
	return compare(left, right);
}


async function validateResolvedPaths(
	integration: ResolvedIntegration
): Promise<void> {
	validateResolvedPathsSync(integration);
}

function validateResolvedPathsSync(integration: ResolvedIntegration): void {
	const canonicalRoot = realpathSync(integration.outputRoot);
	const physicalOutputs: Array<{ module: string; path: string }> = [];
	for (const client of integration.clients) {
		const output = plannedPhysicalPath(canonicalRoot, integration.outputRoot, client.out);
		for (const existing of physicalOutputs) {
			if (
				physicalPathKey(output).startsWith(
					`${physicalPathKey(existing.path)}${sep}`
				) ||
				physicalPathKey(existing.path).startsWith(
					`${physicalPathKey(output)}${sep}`
				) ||
				physicalPathKey(existing.path) === physicalPathKey(output)
			) {
				throw new Error(
					`Distributed client outputs physically overlap: ${existing.module} (${existing.path}) and ${client.module} (${output})`
				);
			}
		}
		physicalOutputs.push({ module: client.module, path: output });
		const canonicalSourceRoot = realpathSync(integration.cwd);
		for (const watchRoot of client.watchRoots) {
			plannedPhysicalPath(canonicalSourceRoot, integration.cwd, watchRoot);
		}
	}
}



function requireResolved(
	value: ResolvedIntegration | undefined
): ResolvedIntegration {
	if (value === undefined) {
		throw new Error(
			'Distributed SvelteKit Vite plugin has not received resolved config'
		);
	}
	return value;
}

function virtualId(module: string): string {
	return `\0@hops-ops/distributed:sveltekit:${module}`;
}


