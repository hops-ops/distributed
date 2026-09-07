import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
	existsSync,
	lstatSync,
	readFileSync,
	realpathSync
} from 'node:fs';
import {
	lstat,
	mkdir,
	mkdtemp,
	open,
	readFile,
	readdir,
	realpath,
	rename,
	rm,
	unlink,
	writeFile
} from 'node:fs/promises';
import {
	basename,
	dirname,
	isAbsolute,
	join,
	relative,
	resolve,
	sep
} from 'node:path';
import { isMainThread } from 'node:worker_threads';

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

const GENERATED_SVELTEKIT_MODULE = 'sveltekit.ts';
const SHA256_ID = /^sha256:[0-9a-f]{64}$/;
const GENERATED_BOUNDARIES_MODULE = 'boundaries.ts';
const MAX_COMMAND_OUTPUT_BYTES = 16 * 1024 * 1024;
const MAX_GENERATED_COMPARE_FILES = 20_000;
const MAX_GENERATED_COMPARE_BYTES = 128 * 1024 * 1024;
const MODULE_NAME = /^\$distributed(?:\/[A-Za-z0-9][A-Za-z0-9._-]*)*$/;
const COMPILER_LOCK = join('.svelte-kit', 'distributed', 'compiler.lock');
const GENERATION_META = 'distributed-generation';
const MAX_LIFECYCLE_STATE_BYTES = 1024 * 1024;
const COMPILER_COORDINATORS = Symbol.for(
	'@hops-ops/distributed/sveltekit/compiler-coordinators/v1'
);

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
	/** Prefix argv, e.g. `cargo run ... --`; never interpreted by a shell. */
	commandArgs?: readonly string[];
	/** SvelteKit route source root. Defaults to `src/routes`. */
	routesDir?: string;
	/** SvelteKit library source root. Defaults to `src/lib`. */
	libDir?: string;
	/** Additional project-local `$name` aliases used by static component imports. */
	aliases?: Readonly<Record<string, string>>;
	clients: readonly DistributedSvelteKitClientCompiler[];
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
}>;

type CompilerCoordinator = {
	path: string;
	references: number;
	ready: Promise<void>;
	tail: Promise<void>;
	startupRuns: Map<string, Promise<void>>;
	closing?: Promise<void>;
};

type CompilerLockLease = {
	coordinator: CompilerCoordinator;
	released: boolean;
};

type ViteWebSocketLike = Readonly<{
	send(payload: unknown): void;
}>;

type ViteModuleGraphLike = Readonly<{
	getModuleById(id: string): unknown;
	invalidateModule(module: unknown): void;
}>;

type ViteMiddlewareRequest = Readonly<{
	method?: string;
	headers: Readonly<Record<string, string | readonly string[] | undefined>>;
	on(event: 'data', listener: (chunk: Uint8Array) => void): void;
	on(event: 'end' | 'error', listener: (value?: unknown) => void): void;
}>;

type ViteMiddlewareResponse = {
	statusCode: number;
	setHeader(name: string, value: string): void;
	end(body?: string | Uint8Array): void;
};

type ViteServerLike = Readonly<{
	watcher: Readonly<{ add(paths: string | readonly string[]): void }>;
	ws: ViteWebSocketLike;
	moduleGraph: ViteModuleGraphLike;
	middlewares?: Readonly<{
		use(
			path: string,
			handler: (request: ViteMiddlewareRequest, response: ViteMiddlewareResponse) => void
		): void;
	}>;
	httpServer?:
		| Readonly<{
				once(event: 'close', listener: () => void): void;
		  }>
		| null;
}>;

type ViteHotContextLike = Readonly<{
	file: string;
	server: ViteServerLike;
	modules?: readonly unknown[];
}>;

type RollupWatchContextLike = Readonly<{
	addWatchFile(path: string): void;
}>;

/** Minimal structural Vite plugin type; avoids making Vite a runtime dependency. */
export type DistributedSvelteKitVitePlugin = Readonly<{
	name: string;
	enforce: 'pre';
	configResolved(config: Readonly<{ root: string }>): Promise<void>;
	configureServer(server: ViteServerLike): void;
	buildStart(this: RollupWatchContextLike): void;
	resolveId(source: string, importer?: string): string | undefined;
	load(id: string): string | undefined;
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

type LifecycleHtmlTag = Readonly<{
	tag: 'meta';
	attrs: Readonly<{ name: string; content: string }>;
	injectTo: 'head-prepend';
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

	const stop = async (): Promise<void> => {
		if (stopped) return;
		stopped = true;
		dirty = false;
		cancellation.abort();
		for (const child of children) killChild(child, 'SIGTERM');
		const force = setTimeout(() => {
			for (const child of children) killChild(child, 'SIGKILL');
		}, 1_500);
		force.unref();
		await Promise.race([
			running?.catch(() => undefined) ?? Promise.resolve(),
			new Promise<void>((resolvePromise) => {
				const bounded = setTimeout(resolvePromise, 3_000);
				bounded.unref();
			})
		]);
		clearTimeout(force);
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
					? withCompilerStartup(lease, integration, operation)
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
			if (lifecycleOwnsCompile) return;
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
			if (!lifecycleOwnsCompile) {
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
			server.httpServer?.once('close', () => {
				void stop();
			});
		},
		buildStart(): void {
			const integration = requireResolved(resolved);
			if (lifecycleOwnsCompile) return;
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
			if (lifecycleOwnsCompile) return;
			if (isCompilerInput(id, integration)) {
				await compile(`GraphQL/Svelte watch change ${id}`);
			}
		},
		closeBundle: stop
	};
}

function localFrameworkDist(root: string): string | undefined {
	try {
		const candidate = join(
			root,
			'node_modules',
			'@hops-ops',
			'distributed',
			'dist'
		);
		return existsSync(candidate) ? realpathSync(candidate) : undefined;
	} catch {
		return undefined;
	}
}

function suppressFrameworkHotUpdate(
	context: ViteHotContextLike,
	frameworkDist: string | undefined
): never[] | undefined {
	if (
		process.env.DISTRIBUTED_LIFECYCLE_DIR === undefined ||
		frameworkDist === undefined ||
		!isWithin(frameworkDist, canonicalExistingPath(context.file))
	) return undefined;
	for (const module of context.modules ?? []) {
		context.server.moduleGraph.invalidateModule(module);
	}
	return [];
}

function canonicalExistingPath(value: string): string {
	try {
		return realpathSync(value);
	} catch {
		return resolve(value);
	}
}

const CONTROL_ID = /^[A-Za-z0-9_:-]{16,128}$/;
const MAX_ACK_BYTES = 4096;

function lifecycleGenerationMeta(): LifecycleHtmlTag[] {
	const configured = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	if (configured === undefined || !isAbsolute(configured)) return [];
	try {
		const encoded = readFileSync(join(resolve(configured), 'dev.json'));
		if (encoded.byteLength > MAX_LIFECYCLE_STATE_BYTES) return [];
		const state = JSON.parse(encoded.toString('utf8')) as {
			active?: { generationId?: unknown };
		};
		const generationId = state.active?.generationId;
		if (
			typeof generationId !== 'string' ||
			generationId.length === 0 ||
			generationId.length > 512 ||
			generationId !== generationId.trim() ||
			/[\u0000-\u001f\u007f]/.test(generationId)
		) return [];
		return [Object.freeze({
			tag: 'meta',
			attrs: Object.freeze({ name: GENERATION_META, content: generationId }),
			injectTo: 'head-prepend'
		})];
	} catch {
		// A lifecycle file is atomically replaced. Missing or malformed state
		// simply omits the hint; the browser falls back to its first poll.
		return [];
	}
}

function configureLifecycleServer(server: ViteServerLike): void {
	const configured = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	if (configured === undefined || !isAbsolute(configured)) return;
	const lifecycleRoot = resolve(configured);
	server.middlewares?.use('/__distributed/lifecycle', (request, response) => {
		void handleLifecycleRequest(lifecycleRoot, request, response).catch(() => {
			if (response.statusCode < 400) response.statusCode = 500;
			response.end();
		});
	});
}

async function handleLifecycleRequest(
	lifecycleRoot: string,
	request: ViteMiddlewareRequest,
	response: ViteMiddlewareResponse
): Promise<void> {
	response.setHeader('cache-control', 'no-store');
	if (request.method === 'GET') {
		const participant = singleHeader(request.headers['x-distributed-participant']);
		if (participant !== undefined && CONTROL_ID.test(participant)) {
			await writeParticipantHeartbeat(lifecycleRoot, participant).catch(
				() => undefined
			);
		}
		const state = await readLifecycleState(lifecycleRoot);
		if (state === undefined) {
			response.statusCode = 404;
			response.end();
			return;
		}
		response.statusCode = 200;
		response.setHeader('content-type', 'application/json');
		response.end(state);
		return;
	}
	if (request.method === 'POST') {
		const contentType = singleHeader(request.headers['content-type']);
		if (contentType?.split(';', 1)[0]?.trim().toLowerCase() !== 'application/json') {
			response.statusCode = 415;
			response.end();
			return;
		}
		if (!sameOriginLifecycleRequest(request)) {
			response.statusCode = 403;
			response.end();
			return;
		}
		const body = JSON.parse(await readRequestBody(request, MAX_ACK_BYTES)) as Record<string, unknown>;
		if (
			!CONTROL_ID.test(String(body.transitionId ?? '')) ||
			!CONTROL_ID.test(String(body.participantId ?? '')) ||
			typeof body.ok !== 'boolean'
		) {
			response.statusCode = 400;
			response.end();
			return;
		}
		const stateSource = await readLifecycleState(lifecycleRoot);
		const state = stateSource === undefined ? undefined : JSON.parse(stateSource) as Record<string, unknown>;
		if (state?.phase !== 'preparing' || state.transitionId !== body.transitionId) {
			response.statusCode = 409;
			response.end();
			return;
		}
		await writeControlJson(
			join(lifecycleRoot, 'dev-control', 'acks', String(body.transitionId)),
			`${String(body.participantId)}.json`,
			{ ok: body.ok }
		);
		response.statusCode = 204;
		response.end();
		return;
	}
	response.statusCode = 405;
	response.end();
}

function writeParticipantHeartbeat(root: string, participant: string): Promise<void> {
	return writeControlJson(
		join(root, 'dev-control', 'participants'),
		`${participant}.json`,
		{ seenAtUnixMs: Date.now() }
	);
}

async function readLifecycleState(root: string): Promise<string | undefined> {
	const path = join(root, 'dev.json');
	let metadata;
	try {
		metadata = await lstat(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
		throw error;
	}
	if (metadata.isSymbolicLink() || !metadata.isFile() || metadata.size > MAX_LIFECYCLE_STATE_BYTES) {
		throw new Error('invalid Distributed lifecycle state file');
	}
	return readFile(path, 'utf8');
}

async function writeControlJson(
	directory: string,
	name: string,
	value: Readonly<Record<string, unknown>>
): Promise<void> {
	await mkdir(directory, { recursive: true });
	const temporary = join(
		directory,
		`.${name}.${process.pid}.${Date.now()}.${randomUUID()}`
	);
	await writeFile(temporary, `${JSON.stringify(value)}\n`, { flag: 'wx', mode: 0o600 });
	await rename(temporary, join(directory, name));
}

function sameOriginLifecycleRequest(request: ViteMiddlewareRequest): boolean {
	const origin = singleHeader(request.headers.origin);
	const host = singleHeader(request.headers.host);
	if (origin === undefined || host === undefined) return false;
	try {
		const parsed = new URL(origin);
		const forwarded = singleHeader(request.headers['x-forwarded-proto']);
		const socket = (request as Readonly<{
			socket?: Readonly<{ encrypted?: boolean }>;
		}>).socket;
		const protocol =
			forwarded === 'http' || forwarded === 'https'
				? `${forwarded}:`
				: socket?.encrypted === true
					? 'https:'
					: 'http:';
		return (
			parsed.protocol === protocol &&
			parsed.host === host &&
			parsed.origin === origin
		);
	} catch {
		return false;
	}
}

function singleHeader(value: string | readonly string[] | undefined): string | undefined {
	return typeof value === 'string' ? value : value?.[0];
}

function readRequestBody(
	request: ViteMiddlewareRequest,
	maximum: number
): Promise<string> {
	return new Promise((resolvePromise, reject) => {
		const chunks: Uint8Array[] = [];
		let bytes = 0;
		let exceeded = false;
		request.on('data', (chunk) => {
			if (exceeded) return;
			bytes += chunk.byteLength;
			if (bytes > maximum) {
				exceeded = true;
				chunks.length = 0;
				reject(new Error('lifecycle acknowledgement exceeds bound'));
				return;
			}
			chunks.push(chunk);
		});
		request.on('error', reject);
		request.on('end', () => {
			if (!exceeded) resolvePromise(Buffer.concat(chunks).toString('utf8'));
		});
	});
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
	const children = new Set<ChildProcess>();
	const cancellation = new AbortController();
	const cancel = (): void => {
		cancellation.abort();
		for (const child of children) killChild(child, 'SIGTERM');
	};
	process.once('SIGINT', cancel);
	process.once('SIGTERM', cancel);
	try {
		await withCompilerLock(lock, () =>
			mode === 'generate'
				? compileTransaction(integration, children, cancellation.signal)
				: checkTransaction(integration, children, cancellation.signal)
		);
	} finally {
		process.removeListener('SIGINT', cancel);
		process.removeListener('SIGTERM', cancel);
		for (const child of children) killChild(child, 'SIGKILL');
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

function lifecycleActivePointer(): string | undefined {
	const root = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	return root !== undefined && isAbsolute(root)
		? canonicalExistingPath(join(resolve(root), 'active.json'))
		: undefined;
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

function activeLifecycleGenerationRoot(): string {
	const lifecycleRoot = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	const projectRoot = process.env.DISTRIBUTED_LIFECYCLE_PROJECT_ROOT;
	if (
		lifecycleRoot === undefined ||
		projectRoot === undefined ||
		!isAbsolute(lifecycleRoot) ||
		!isAbsolute(projectRoot)
	) {
		throw new Error(
			'Distributed lifecycle-owned client resolution requires absolute lifecycle and project roots'
		);
	}
	const requestedGeneration = process.env.DISTRIBUTED_LIFECYCLE_GENERATION_ID;
	let generation: unknown = requestedGeneration;
	if (generation === undefined) {
		const pointer = readFileSync(join(resolve(lifecycleRoot), 'active.json'));
		if (pointer.byteLength > MAX_LIFECYCLE_STATE_BYTES) {
			throw new Error('Distributed active lifecycle pointer exceeds 1 MiB');
		}
		const parsed = JSON.parse(pointer.toString('utf8')) as { generation_id?: unknown };
		generation = parsed.generation_id;
	}
	if (
		typeof generation !== 'string' ||
		!SHA256_ID.test(generation)
	) {
		throw new Error('Distributed active lifecycle pointer has an invalid generation ID');
	}
	return join(resolve(lifecycleRoot), 'generations', generation);
}

function resolveIntegration(
	options: DistributedSvelteKitViteOptions,
	fallbackCwd: string
): ResolvedIntegration {
	if (options === null || typeof options !== 'object') {
		throw new TypeError('distributedSvelteKit requires configuration');
	}
	const cwd = resolve(options.cwd ?? fallbackCwd);
	const command = (options.command ?? 'distributed').trim();
	if (command.length === 0) {
		throw new TypeError('Distributed SvelteKit command must not be empty');
	}
	if (!Array.isArray(options.clients) || options.clients.length === 0) {
		throw new TypeError(
			'Distributed SvelteKit requires at least one client surface'
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
	const clients = options.clients.map((client, index): ResolvedClient => {
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
		cwd,
		outputRoot: cwd,
		command,
		commandArgs: Object.freeze(
			(options.commandArgs ?? []).map((argument, index) => {
				if (typeof argument !== 'string') {
					throw new TypeError(
						`Distributed commandArgs[${index}] must be a string`
					);
				}
				return argument;
			})
		),
		routesDir,
		libDir,
		aliases,
		clients: Object.freeze(clients)
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
	source: DistributedSvelteKitManifestSource
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

function nonempty(value: unknown, label: string): string {
	if (typeof value !== 'string' || value.trim().length === 0) {
		throw new TypeError(`${label} must be a non-empty string`);
	}
	return value;
}

function containedPath(root: string, value: string, label: string): string {
	const target = resolve(root, nonempty(value, label));
	if (!isWithin(root, target)) {
		throw new TypeError(`${label} must stay within project root ${root}`);
	}
	return target;
}

function isWithin(root: string, target: string): boolean {
	const rel = relative(root, target);
	return (
		rel === '' ||
		(!rel.startsWith(`..${sep}`) && rel !== '..' && !isAbsolute(rel))
	);
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

function manifestWatchRoots(
	cwd: string,
	manifest: DistributedSvelteKitManifestSource
): string[] {
	const manifestPath =
		typeof manifest === 'string'
			? resolve(cwd, manifest)
			: (() => {
					const index = manifest.args.indexOf('--manifest-path');
					return index === -1 || manifest.args[index + 1] === undefined
						? undefined
						: resolve(cwd, manifest.args[index + 1]);
				})();
	if (manifestPath === undefined) return [];
	const root = dirname(manifestPath);
	return [manifestPath, join(root, 'src'), join(root, 'crates')]
		.filter((candidate) => existsSync(candidate))
		.sort();
}

function isCompilerInput(file: string, integration: ResolvedIntegration): boolean {
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

async function runCommand(
	integration: ResolvedIntegration,
	args: readonly string[],
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<{ stdout: string; stderr: string }> {
	throwIfAborted(signal);
	const argv = [...integration.commandArgs, ...args];
	return await new Promise((resolvePromise, rejectPromise) => {
		const child = spawn(integration.command, argv, {
			cwd: integration.cwd,
			env: process.env,
			shell: false,
			detached: process.platform !== 'win32',
			stdio: ['ignore', 'pipe', 'pipe']
		});
		children.add(child);
		const stdout: Buffer[] = [];
		const stderr: Buffer[] = [];
		let bytes = 0;
		let overflow = false;
		let forceKill: ReturnType<typeof setTimeout> | undefined;
		const cancel = (): void => {
			killChild(child, 'SIGTERM');
			forceKill ??= setTimeout(() => killChild(child, 'SIGKILL'), 1_500);
			forceKill.unref();
		};
		signal.addEventListener('abort', cancel, { once: true });
		const collect = (target: Buffer[]) => (chunk: Buffer | string): void => {
			if (overflow) return;
			const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
			bytes += value.length;
			if (bytes > MAX_COMMAND_OUTPUT_BYTES) {
				overflow = true;
				cancel();
				return;
			}
			target.push(value);
		};
		child.stdout?.on('data', collect(stdout));
		child.stderr?.on('data', collect(stderr));
		child.once('error', (error) => {
			children.delete(child);
			signal.removeEventListener('abort', cancel);
			if (forceKill !== undefined) clearTimeout(forceKill);
			rejectPromise(
				commandFailure(integration, argv, '', error.message)
			);
		});
		child.once('close', (code, signal) => {
			children.delete(child);
			cancellationCleanup();
			const out = Buffer.concat(stdout).toString('utf8');
			const err = Buffer.concat(stderr).toString('utf8');
			if (overflow) {
				rejectPromise(
					commandFailure(
						integration,
						argv,
						err,
						`output exceeded ${MAX_COMMAND_OUTPUT_BYTES} bytes`
					)
				);
			} else if (code !== 0) {
				rejectPromise(
					commandFailure(
						integration,
						argv,
						err,
						`exit ${code ?? 'unknown'}${signal === null ? '' : ` (${signal})`}`
					)
				);
			} else {
				resolvePromise({ stdout: out, stderr: err });
			}
		});

		function cancellationCleanup(): void {
			signal.removeEventListener('abort', cancel);
			if (forceKill !== undefined) clearTimeout(forceKill);
		}
	});
}

function commandFailure(
	integration: ResolvedIntegration,
	argv: readonly string[],
	stderr: string,
	summary: string
): Error {
	const renderedArgv = JSON.stringify([integration.command, ...argv]);
	const details = [
		`Distributed compiler command failed: ${summary}`,
		`cwd: ${integration.cwd}`,
		`argv: ${renderedArgv}`,
		stderr.trim().length === 0 ? undefined : `stderr:\n${stderr.trim()}`
	].filter((value): value is string => value !== undefined);
	return new Error(details.join('\n'));
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

function plannedPhysicalPath(
	canonicalRoot: string,
	lexicalRoot: string,
	target: string
): string {
	const rel = relative(lexicalRoot, target);
	let lexical = lexicalRoot;
	if (rel !== '') {
		for (const component of rel.split(sep)) {
			lexical = join(lexical, component);
			if (!existsSync(lexical)) break;
			const metadata = lstatSync(lexical);
			if (metadata.isSymbolicLink()) {
				throw new Error(
					`Distributed path ${target} contains symlink component ${lexical}`
				);
			}
		}
	}

	let existing = target;
	const suffix: string[] = [];
	while (!existsSync(existing)) {
		const parent = dirname(existing);
		if (parent === existing) {
			throw new Error(
				`Distributed path ${target} has no existing parent within ${canonicalRoot}`
			);
		}
		suffix.unshift(basename(existing));
		existing = parent;
	}
	const canonicalExisting = realpathSync(existing);
	if (!isWithin(canonicalRoot, canonicalExisting)) {
		throw new Error(
			`Distributed path ${target} resolves outside project root ${canonicalRoot}`
		);
	}
	const planned = resolve(canonicalExisting, ...suffix);
	if (!isWithin(canonicalRoot, planned)) {
		throw new Error(
			`Distributed path ${target} resolves outside project root ${canonicalRoot}`
		);
	}
	return planned;
}

function physicalPathKey(path: string): string {
	return process.platform === 'win32' || process.platform === 'darwin'
		? path.toLocaleLowerCase('en-US')
		: path;
}

async function validateNearestExistingParent(
	root: string,
	target: string
): Promise<void> {
	const canonicalRoot = realpathSync(root);
	plannedPhysicalPath(canonicalRoot, root, target);
}

function compilerCoordinatorRegistry(): Map<string, CompilerCoordinator> {
	const globalScope = globalThis as unknown as Record<symbol, unknown>;
	const existing = globalScope[COMPILER_COORDINATORS];
	if (existing instanceof Map) {
		return existing as Map<string, CompilerCoordinator>;
	}
	const registry = new Map<string, CompilerCoordinator>();
	globalScope[COMPILER_COORDINATORS] = registry;
	return registry;
}

async function acquireCompilerLock(cwd: string): Promise<CompilerLockLease> {
	const path = join(cwd, COMPILER_LOCK);
	const registry = compilerCoordinatorRegistry();
	for (;;) {
		const existing = registry.get(path);
		if (existing?.closing !== undefined) {
			await existing.closing;
			continue;
		}
		const coordinator =
			existing ??
			({
				path,
				references: 0,
				ready: acquirePhysicalCompilerLock(cwd, path),
				tail: Promise.resolve(),
				startupRuns: new Map()
			} satisfies CompilerCoordinator);
		if (existing === undefined) registry.set(path, coordinator);
		coordinator.references += 1;
		try {
			await coordinator.ready;
			return { coordinator, released: false };
		} catch (error) {
			coordinator.references -= 1;
			if (
				coordinator.references === 0 &&
				registry.get(path) === coordinator
			) {
				registry.delete(path);
			}
			throw error;
		}
	}
}

async function acquirePhysicalCompilerLock(
	cwd: string,
	path: string
): Promise<void> {
	await validateNearestExistingParent(cwd, dirname(path));
	await mkdir(dirname(path), { recursive: true });
	await validateNearestExistingParent(cwd, path);
	for (let attempt = 0; attempt < 2; attempt += 1) {
		try {
			const handle = await open(path, 'wx', 0o600);
			try {
				await handle.writeFile(
					`${JSON.stringify({ pid: process.pid, version: 1 })}\n`,
					'utf8'
				);
			} finally {
				await handle.close();
			}
			return;
		} catch (error) {
			if (!isAlreadyExists(error)) throw error;
			let owner: unknown;
			try {
				owner = JSON.parse(await readFile(path, 'utf8')) as unknown;
			} catch {
				owner = undefined;
			}
			const pid =
				owner !== null &&
				typeof owner === 'object' &&
				'pid' in owner &&
				typeof owner.pid === 'number'
					? owner.pid
					: undefined;
			if (pid !== undefined && processExists(pid)) {
				throw new Error(
					`Distributed SvelteKit compiler already owns ${cwd} (pid ${pid})`
				);
			}
			await unlink(path).catch((unlinkError: unknown) => {
				if (!isMissing(unlinkError)) throw unlinkError;
			});
		}
	}
	throw new Error(`could not acquire Distributed SvelteKit compiler lock ${path}`);
}

function withCompilerLock<T>(
	lease: CompilerLockLease,
	operation: () => Promise<T>
): Promise<T> {
	if (lease.released) {
		return Promise.reject(
			new Error('Distributed SvelteKit compiler lock lease is already released')
		);
	}
	const coordinator = lease.coordinator;
	const run = coordinator.tail.catch(() => undefined).then(operation);
	coordinator.tail = run.then(
		() => undefined,
		() => undefined
	);
	return run;
}

function withCompilerStartup(
	lease: CompilerLockLease,
	integration: ResolvedIntegration,
	operation: () => Promise<void>
): Promise<void> {
	if (lease.released) {
		return Promise.reject(
			new Error('Distributed SvelteKit compiler lock lease is already released')
		);
	}
	const key = JSON.stringify(integration);
	const coordinator = lease.coordinator;
	const existing = coordinator.startupRuns.get(key);
	if (existing !== undefined) return existing;
	const run = withCompilerLock(lease, operation);
	const cached = run.catch((error: unknown) => {
		if (coordinator.startupRuns.get(key) === cached) {
			coordinator.startupRuns.delete(key);
		}
		throw error;
	});
	coordinator.startupRuns.set(key, cached);
	return cached;
}

function requireCompilerLock(
	lease: CompilerLockLease | undefined
): CompilerLockLease {
	if (lease === undefined) {
		throw new Error('Distributed SvelteKit compiler lock is not initialized');
	}
	return lease;
}

async function releaseCompilerLock(lease: CompilerLockLease): Promise<void> {
	if (lease.released) return;
	lease.released = true;
	const coordinator = lease.coordinator;
	await coordinator.tail.catch(() => undefined);
	coordinator.references -= 1;
	if (coordinator.references > 0) return;
	if (coordinator.references < 0) {
		throw new Error('Distributed SvelteKit compiler lock reference underflow');
	}
	const registry = compilerCoordinatorRegistry();
	if (registry.get(coordinator.path) !== coordinator) return;
	const closing = unlink(coordinator.path)
		.catch((error: unknown) => {
			if (!isMissing(error)) throw error;
		})
		.finally(() => {
			if (registry.get(coordinator.path) === coordinator) {
				registry.delete(coordinator.path);
			}
		});
	coordinator.closing = closing;
	await closing;
}

function processExists(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (
			error !== null &&
			typeof error === 'object' &&
			'code' in error &&
			error.code === 'EPERM'
		);
	}
}

function killChild(
	child: ChildProcess,
	signal: NodeJS.Signals
): void {
	if (child.pid === undefined) return;
	if (process.platform !== 'win32') {
		try {
			process.kill(-child.pid, signal);
			return;
		} catch {
			// The child may have exited or failed to become its own process group.
		}
	}
	child.kill(signal);
}

function throwIfAborted(signal: AbortSignal): void {
	if (!signal.aborted) return;
	const error = new Error('Distributed SvelteKit compiler was cancelled');
	error.name = 'AbortError';
	throw error;
}

function isMissing(error: unknown): boolean {
	return (
		error !== null &&
		typeof error === 'object' &&
		'code' in error &&
		error.code === 'ENOENT'
	);
}

function isAlreadyExists(error: unknown): boolean {
	return (
		error !== null &&
		typeof error === 'object' &&
		'code' in error &&
		error.code === 'EEXIST'
	);
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

function portablePath(path: string): string {
	return path.replaceAll('\\', '/');
}

function viteError(error: unknown): Readonly<{
	message: string;
	stack?: string;
}> {
	if (error instanceof Error) {
		return {
			message: error.message,
			...(error.stack === undefined ? {} : { stack: error.stack })
		};
	}
	return { message: String(error) };
}
