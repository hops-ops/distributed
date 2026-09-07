import {
	sameAuthCredential,
	snapshotAuthCredential
} from '../identity.js';
import {
	createDistributedReplica,
	createReplicaGraphqlTransport,
	type DistributedReplica,
	type DistributedReplicaOptions,
	type ReplicaAuthoritativeScope,
	type ReplicaCommandReceipt,
	type ReplicaCommandRuntime,
	type ReplicaCommandRuntimeOptions,
	type ReplicaGraphqlTransport,
	type ReplicaOperationArtifact,
	type ReplicaSnapshot,
	type ReplicaWatch,
	type WatchReplicaOptions
} from '../replica/index.js';
import type { GqlAuth, GqlError, GraphqlVariables } from '../types.js';
import type { WebSocketConstructor } from '../websocket.js';
import type { FetchLike } from '../request.js';
import { replicaCommandProjectedLifecycleOf } from '../replica/command-runtime.js';
import { authFromPageData, type PageGraphqlData } from './auth.js';
import {
	defineDistributedBoundaryBinding,
	defineDistributedBoundaryOperation,
	type DistributedBoundaryPlan,
	type DistributedBoundaryOperation,
	type DistributedBoundaryVariableSources,
	type DistributedBoundaryVariableContext
} from './boundary-variables.js';
import {
	DistributedSvelteKitBoundaryController,
	type DistributedSvelteKitBoundaryInstance,
	type DistributedSvelteKitBoundaryLocation,
	type DistributedSvelteKitLocationContext,
	type SveltekitBoundaryLifecycleDiagnostic,
	type SveltekitBoundaryRetention
} from './boundary-lifecycle.js';
import {
	distributedReloadLifecycle,
	registerDistributedReloadClient,
	type DistributedReloadOptions
} from './lifecycle.js';

type UnknownCommandEntries = Readonly<Record<string, never>>;

export type SveltekitSessionSource = Readonly<{
	/** Current credential. HTTP, WS, and commands all call this exact source. */
	getAuth(): GqlAuth | Promise<GqlAuth>;
	/**
	 * Notify on token, logout, role, tenant, or session changes.
	 *
	 * The adapter re-reads `getAuth`; callers never label a cache scope.
	 */
	subscribe?(listener: () => void): () => void;
}>;

export type SveltekitPageDataSource<TData extends PageGraphqlData> = Readonly<{
	get(): TData;
	subscribe?(listener: () => void): () => void;
}>;

export type SveltekitPageDataSessionSource<TData extends PageGraphqlData> =
	Readonly<{
		session: SveltekitSessionSource;
		get(): TData;
		set(next: TData): void;
	}>;

export type SveltekitReplicaHydration = Readonly<{
	version: 1;
	state: import('../replica/index.js').ReplicaDehydratedState;
	readonly operations?: readonly string[];
	/** Exact variable-binding fingerprints used to create this SSR seed. */
	readonly bindings?: readonly string[];
}>;

/**
 * Independently trusted SSR/session authority for one hydration transfer.
 *
 * Do not persist this beside replica state. State never authorizes its own
 * scope; the adapter compares its embedded scope to this server-owned value.
 */
export type SveltekitReplicaAuthority = Readonly<{
	version: 1;
	scope: ReplicaAuthoritativeScope;
}>;

export type SveltekitDistributedPageData = PageGraphqlData &
	Readonly<{
		distributed?: SveltekitReplicaHydration;
		distributedAuthority?: SveltekitReplicaAuthority;
	}>;

export type SveltekitCommandRuntimeLike<TCommands> = Pick<
	ReplicaCommandRuntime<UnknownCommandEntries>,
	'dispose' | 'pendingCommandIds'
> &
	Readonly<{
		commands: TCommands;
		preload?(): Promise<void>;
	}>;

export type SveltekitCommandRuntimeFactory<TCommands> = (
	replica: DistributedReplica,
	transport: ReplicaGraphqlTransport,
	options: SveltekitCommandRuntimeFactoryOptions
) => SveltekitCommandRuntimeLike<TCommands>;

export type SveltekitCommandRuntimeFactoryOptions = Pick<
	ReplicaCommandRuntimeOptions,
	'diagnostics' | 'lifecycle'
>;

export type CreateDistributedSvelteKitOptions<TCommands = Readonly<Record<never, never>>> =
	Readonly<{
		session: SveltekitSessionSource;
		/** Same-origin `/graphql` by default. */
		url?: string | (() => string);
		fetch?: FetchLike;
		webSocket?: WebSocketConstructor;
		/**
		 * Set to SvelteKit's `$app/environment` `browser` flag. When false,
		 * store subscriptions remain side-effect-free during component SSR.
		 */
		browser?: boolean;
		hydration?: SveltekitReplicaHydration;
		/** Required with `hydration`; must come from current trusted SSR page data. */
		authority?: SveltekitReplicaAuthority;
		createCommands?: SveltekitCommandRuntimeFactory<TCommands>;
		replica?: Omit<DistributedReplicaOptions, 'transport'>;
		/** Generated boundary operations accepted by this component-tree client. */
		boundaries: readonly DistributedBoundaryOperation[];
		/** Redacted structural lifecycle events for boundary ownership diagnostics. */
		onBoundaryDiagnostic?: (event: SveltekitBoundaryLifecycleDiagnostic) => void;
		onAuthError?: (error: unknown) => void;
		/** Generated clients supply the surface key; apps may declare safe state partitions. */
		reload?: DistributedReloadOptions;
	}>;

export type SveltekitQuerySnapshot<TData> = ReplicaSnapshot<TData> &
	Readonly<{
	loading: boolean;
	error?: GqlError;
	/** Successful causal commands still waiting for projection visibility. */
	pending: readonly ReplicaCommandReceipt<unknown>[];
	refetch(): Promise<void>;
}>;

export interface SveltekitQueryStore<TData> {
	/** Current value for imperative code and adapter conformance. */
	get(): SveltekitQuerySnapshot<TData>;
	/** Svelte-readable store contract; use `$query` in a component. */
	subscribe(listener: (snapshot: SveltekitQuerySnapshot<TData>) => void): () => void;
	refetch(): Promise<void>;
	destroy(): void;
	readonly data: ReplicaSnapshot<TData>['data'];
	readonly status: ReplicaSnapshot<TData>['status'];
	readonly complete: boolean;
	readonly loading: boolean;
	readonly fetching: boolean;
	readonly stale: boolean;
	readonly live: ReplicaSnapshot<TData>['live'];
	readonly errors: readonly GqlError[];
	readonly error: GqlError | undefined;
	readonly pending: readonly ReplicaCommandReceipt<unknown>[];
}

export type UseSveltekitOperationOptions = Readonly<{
	/** Defaults to true when the compiler emitted a live companion. */
	live?: boolean;
}>;

type UseOperationArguments<TVariables extends GraphqlVariables> =
	Record<string, never> extends TVariables
		?
				| []
				| [variables: TVariables]
				| [
						variables: TVariables,
						options: UseSveltekitOperationOptions
				  ]
		: [
				variables: TVariables,
				options?: UseSveltekitOperationOptions
		  ];

export type SveltekitBoundOperation<
	TData,
	TVariables extends GraphqlVariables
> = Readonly<{
	artifact: ReplicaOperationArtifact<TData, TVariables>;
	use(
		...args: UseOperationArguments<TVariables>
	): SveltekitQueryStore<TData>;
	read(variables: TVariables): ReplicaSnapshot<TData>;
	/** Client-side hover/nav warmup; no-ops when the replica already has a complete snapshot. */
	prefetch(variables: TVariables): Promise<void>;
	/** One typed binding shared by SSR, navigation, prefetch, hydration, and live use. */
	boundary<
		TSession = unknown,
		TProps = Readonly<Record<string, unknown>>
	>(
		plan: DistributedBoundaryPlan,
		sources: DistributedBoundaryVariableSources<TVariables>
	): DistributedBoundaryOperation<TData, TVariables, TSession, TProps>;
}>;

export type DistributedSvelteKitClient<TCommands> = Readonly<{
	replica: DistributedReplica;
	transport: ReplicaGraphqlTransport;
	commands: TCommands;
	operation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): SveltekitBoundOperation<TData, TVariables>;
	boundary<
		TData,
		TVariables extends GraphqlVariables,
		TSession,
		TProps
	>(
		operation: DistributedBoundaryOperation<TData, TVariables, TSession, TProps>
	): SveltekitBoundBoundaryOperation<TData, TVariables, TSession, TProps>;
	/** Retain every generated selection owned by one mounted page/layout instance. */
	retainBoundary<TSession, TProps>(
		instance: DistributedSvelteKitBoundaryInstance,
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): SveltekitBoundaryRetention;
	/** Retain the nearest generated page/layout boundary at this location. */
	retainLocation<TSession, TProps>(
		location: DistributedSvelteKitBoundaryLocation,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): SveltekitBoundaryRetention;
	/** Prefetch the generated page and owning layout selections for a target URL. */
	prefetchLocation<TSession, TProps>(
		pathname: string,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): Promise<void>;
	/**
	 * Apply a server seed. A malformed or mismatched seed closes the old
	 * generation and returns false so the bound operation refetches.
	 */
	hydrate(
		hydration: SveltekitReplicaHydration,
		authority: SveltekitReplicaAuthority
	): boolean;
	prefetch(
		artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
		variables: GraphqlVariables
	): Promise<void>;
	/** Load deferred command code without dispatching a command. */
	preloadCommands(): Promise<void>;
	invalidateAuthorization(): void;
	destroy(): void;
}>;

export type SveltekitBoundBoundaryOperation<
	TData,
	TVariables extends GraphqlVariables,
	TSession,
	TProps
> = Readonly<{
	operation: DistributedBoundaryOperation<TData, TVariables, TSession, TProps>;
	variables(
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): TVariables;
	use(
		context: DistributedBoundaryVariableContext<TSession, TProps>,
		options?: UseSveltekitOperationOptions
	): SveltekitQueryStore<TData>;
	read(
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): ReplicaSnapshot<TData>;
	prefetch(
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): Promise<void>;
}>;

/**
 * Bind the framework-neutral replica to Svelte's readable-store lifecycle.
 *
 * Generated `$distributed` modules call this once, then export
 * `client.operation(Operation_Todos)` and `client.commands`.
 */
export function createDistributedSvelteKit<TCommands = Readonly<Record<never, never>>>(
	options: CreateDistributedSvelteKitOptions<TCommands>
): DistributedSvelteKitClient<TCommands> {
	if (
		options.session === null ||
		typeof options.session !== 'object' ||
		typeof options.session.getAuth !== 'function'
	) {
		throw new TypeError('Distributed SvelteKit requires one session source');
	}
	if (!Array.isArray(options.boundaries)) {
		throw new TypeError('Distributed SvelteKit requires generated boundary operations');
	}
	if (
		(options.hydration === undefined) !==
		(options.authority === undefined)
	) {
		throw new TypeError(
			'Distributed SvelteKit hydration requires separate trusted SSR authority'
		);
	}

	let replica: DistributedReplica | undefined;
	let boundaryController: DistributedSvelteKitBoundaryController | undefined;
	const boundaryIds = Object.freeze(
		[...new Set(options.boundaries.map(({ binding }) => binding.id))].sort()
	);
	const auth = createAuthorizationFence(
		options.session,
		() => {
			boundaryController?.disposeScope();
			replica?.invalidateAuthorization();
		},
		options.onAuthError
	);
	const configuredUrl = options.url;
	const transport = createReplicaGraphqlTransport({
		getUrl:
			typeof configuredUrl === 'function'
				? configuredUrl
				: () => configuredUrl ?? '/graphql',
		getAuth: auth.read,
		...(options.fetch === undefined ? {} : { fetch: options.fetch }),
		...(options.webSocket === undefined
			? {}
			: { webSocket: options.webSocket })
	});
	replica = createDistributedReplica({
		transport,
		...(options.replica ?? {}),
		onAuthorizationGenerationDispose: () => {
			boundaryController?.disposeScope();
			options.replica?.onAuthorizationGenerationDispose?.();
		}
	});
	boundaryController = new DistributedSvelteKitBoundaryController(
		replica,
		options.boundaries,
		options.onBoundaryDiagnostic
	);

	const stores = new Set<SveltekitQueryStoreImpl<unknown>>();
	const pending = new PendingReceiptStore();
	let destroyed = false;
	const hydrate = (
		hydration: SveltekitReplicaHydration,
		authority: SveltekitReplicaAuthority
	): boolean => {
		let accepted = false;
		try {
			const expected = validatedHydrationAuthority(authority);
			const hydrationBindings = [...(hydration.bindings ?? [])].sort();
			if (hydrationBindings.some((value) => !boundaryIds.includes(value))) {
				throw new TypeError(
					'Distributed SvelteKit hydration boundary binding fingerprint changed'
				);
			}
			const active = replica!.scope;
			if (active !== undefined && !sameReplicaScope(active, expected)) {
				throw new TypeError(
					'Distributed SvelteKit hydration authority changed before session invalidation'
				);
			}
			accepted =
				hydration?.version === 1 &&
				replica!.hydrate(hydration.state, expected);
		} catch {
			accepted = false;
		}
		if (!accepted) {
			boundaryController!.disposeScope();
			replica!.invalidateAuthorization();
		}
		return accepted;
	};
	if (options.hydration !== undefined && options.authority !== undefined) {
		hydrate(options.hydration, options.authority);
	}

	// Hydration must precede command authority registration: trusted presets are
	// server-derived and restored only as part of an exact authoritative seed.
	const commandRuntime = options.createCommands?.(
		replica,
		transport,
		Object.freeze({
			...(options.replica?.diagnostics === undefined
				? {}
				: { diagnostics: options.replica.diagnostics }),
			...(options.browser === true && options.reload !== undefined
				? { lifecycle: distributedReloadLifecycle() }
				: {})
		})
	);
	let unregisterReload: (() => void) | undefined;
	try {
		unregisterReload =
			options.browser === true && options.reload !== undefined
				? registerDistributedReloadClient(replica, commandRuntime, options.reload)
				: undefined;
	} catch (error) {
		// Client construction is transactional: a malformed reload declaration
		// must not retain its session subscription or command authority.
		try {
			commandRuntime?.dispose();
		} catch {
			// Preserve the construction failure; disposal is best effort here.
		}
		try {
			auth.dispose();
		} catch {
			// Preserve the construction failure; disposal is best effort here.
		}
		throw error;
	}
	const commands =
		commandRuntime === undefined
			? (Object.freeze({}) as TCommands)
			: wrapCommandTree(commandRuntime.commands, pending);

	const operation = <TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): SveltekitBoundOperation<TData, TVariables> =>
		bindOperation(replica!, artifact, pending, {
			isAlive: () => !destroyed,
			activateWatches: () => options.browser ?? true,
			active: (store) => {
				stores.add(store as SveltekitQueryStoreImpl<unknown>);
			},
			idle: (store) => {
				stores.delete(store as SveltekitQueryStoreImpl<unknown>);
			}
		});
	const boundary = <
		TData,
		TVariables extends GraphqlVariables,
		TSession,
		TProps
	>(
		binding: DistributedBoundaryOperation<TData, TVariables, TSession, TProps>
	): SveltekitBoundBoundaryOperation<TData, TVariables, TSession, TProps> => {
		if (!boundaryIds.includes(binding.binding.id)) {
			throw new TypeError(
				'Distributed boundary operation was not registered with this SvelteKit client'
			);
		}
		const bound = operation(binding.artifact);
		const variables = (
			context: DistributedBoundaryVariableContext<TSession, TProps>
		): TVariables => binding.binding.resolve(context);
		return Object.freeze({
			operation: binding,
			variables,
			use(context, useOptions) {
				const invoke = bound.use as (
					variables: TVariables,
					options?: UseSveltekitOperationOptions
				) => SveltekitQueryStore<TData>;
				return invoke(variables(context), useOptions);
			},
			read(context) {
				return bound.read(variables(context));
			},
			prefetch(context) {
				return bound.prefetch(variables(context));
			}
		});
	};

	return Object.freeze({
		replica,
		transport,
		commands,
		operation,
		boundary,
		retainBoundary(instance, context) {
			if (destroyed) {
				throw new Error('Distributed SvelteKit client is destroyed');
			}
			return boundaryController!.retain(instance, context);
		},
		retainLocation(location, context) {
			if (destroyed) {
				throw new Error('Distributed SvelteKit client is destroyed');
			}
			return boundaryController!.retainLocation(location, context);
		},
		prefetchLocation(pathname, context) {
			if (destroyed) {
				return Promise.reject(
					new Error('Distributed SvelteKit client is destroyed')
				);
			}
			return boundaryController!.prefetchLocation(pathname, context);
		},
		hydrate,
		prefetch(artifact, variables) {
			return prefetchReplicaOperation(replica!, artifact, variables);
		},
		preloadCommands(): Promise<void> {
			if (destroyed) return Promise.reject(new Error('Distributed SvelteKit client is destroyed'));
			return commandRuntime?.preload?.() ?? Promise.resolve();
		},
		invalidateAuthorization(): void {
			if (!destroyed) {
				boundaryController!.disposeScope();
				replica!.invalidateAuthorization();
			}
		},
		destroy(): void {
			if (destroyed) return;
			destroyed = true;
			auth.dispose();
			boundaryController!.destroy();
			for (const store of [...stores]) store.destroy();
			stores.clear();
			pending.clear();
			unregisterReload?.();
			commandRuntime?.dispose();
		}
	});
}

/**
 * Low-level composition seam for applications that already own a replica.
 *
 * Most SvelteKit applications use `createDistributedSvelteKit().operation()`;
 * this form exists for custom composition and framework adapter conformance.
 */
export function bindSveltekitOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	replica: DistributedReplica,
	artifact: ReplicaOperationArtifact<TData, TVariables>
): SveltekitBoundOperation<TData, TVariables> {
	return bindOperation(
		replica,
		artifact,
		new PendingReceiptStore(),
		{
			isAlive: () => true,
			activateWatches: () => true,
			active: () => undefined,
			idle: () => undefined
		}
	);
}

/** Map SvelteKit page data into the one shared transport/session source. */
export function sessionSourceFromPageData<TData extends PageGraphqlData>(
	source: SveltekitPageDataSource<TData>
): SveltekitSessionSource {
	if (
		source === null ||
		typeof source !== 'object' ||
		typeof source.get !== 'function'
	) {
		throw new TypeError('page-data session source requires get');
	}
	return Object.freeze({
		getAuth: () => authFromPageData(source.get()),
		...(source.subscribe === undefined
			? {}
			: { subscribe: source.subscribe.bind(source) })
	});
}

/**
 * Create the single mutable page-data/session source owned by one layout.
 *
 * HTTP, WebSocket reconnects, commands, and authorization invalidation all
 * observe `session`; navigation updates the same source through `set`.
 */
export function createPageDataSessionSource<TData extends PageGraphqlData>(
	initial: TData
): SveltekitPageDataSessionSource<TData> {
	let current = initial;
	const listeners = new Set<() => void>();
	const source = Object.freeze({
		get(): TData {
			return current;
		},
		subscribe(listener: () => void): () => void {
			listeners.add(listener);
			return () => listeners.delete(listener);
		}
	});
	return Object.freeze({
		session: sessionSourceFromPageData(source),
		get: source.get,
		set(next: TData): void {
			current = next;
			for (const listener of [...listeners]) listener();
		}
	});
}

function bindOperation<TData, TVariables extends GraphqlVariables>(
	replica: DistributedReplica,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	pending: PendingReceiptStore,
	lifecycle: SveltekitStoreLifecycle<TData>
): SveltekitBoundOperation<TData, TVariables> {
	const use = (
		...args: UseOperationArguments<TVariables>
	): SveltekitQueryStore<TData> => {
		const variables = (args[0] ?? {}) as TVariables;
		const useOptions = args[1] as UseSveltekitOperationOptions | undefined;
		const store = new SveltekitQueryStoreImpl(
			replica,
			artifact,
			variables,
			Object.freeze({
				live: useOptions?.live ?? artifact.live !== undefined
			}),
			pending,
			lifecycle
		);
		return store;
	};
	return Object.freeze({
		artifact,
		use,
		read: (variables: TVariables) => replica.read(artifact, variables),
		prefetch: (variables: TVariables) =>
			prefetchReplicaOperation(replica, artifact, variables),
		boundary<TSession, TProps>(
			plan: DistributedBoundaryPlan,
			sources: DistributedBoundaryVariableSources<TVariables>
		): DistributedBoundaryOperation<TData, TVariables, TSession, TProps> {
			return defineDistributedBoundaryOperation(
				plan,
				artifact,
				defineDistributedBoundaryBinding<
					TData,
					TVariables,
					TSession,
					TProps
				>(artifact, sources)
			);
		}
	});
}

function prefetchReplicaOperation<TData, TVariables extends GraphqlVariables>(
	replica: DistributedReplica,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables
): Promise<void> {
	const snapshot = replica.read(artifact, variables);
	if (snapshot.complete && !snapshot.stale) return Promise.resolve();
	const watch = replica.watch(artifact, variables, { live: false });
	return watch.refresh().finally(() => watch.destroy());
}

type SveltekitStoreLifecycle<TData> = Readonly<{
	isAlive(): boolean;
	activateWatches(): boolean;
	active(store: SveltekitQueryStoreImpl<TData>): void;
	idle(store: SveltekitQueryStoreImpl<TData>): void;
}>;

class SveltekitQueryStoreImpl<TData> implements SveltekitQueryStore<TData> {
	readonly #replica: DistributedReplica;
	readonly #artifact: ReplicaOperationArtifact<TData, GraphqlVariables>;
	readonly #variables: GraphqlVariables;
	readonly #options: WatchReplicaOptions;
	readonly #pending: PendingReceiptStore;
	readonly #lifecycle: SveltekitStoreLifecycle<TData>;
	readonly #listeners = new Set<
		(snapshot: SveltekitQuerySnapshot<TData>) => void
	>();
	#watch: ReplicaWatch<TData> | undefined;
	#unsubscribeWatch: (() => void) | undefined;
	#unsubscribePending: (() => void) | undefined;
	#snapshot: SveltekitQuerySnapshot<TData>;
	#destroyed = false;

	constructor(
		replica: DistributedReplica,
		artifact: ReplicaOperationArtifact<TData, GraphqlVariables>,
		variables: GraphqlVariables,
		options: WatchReplicaOptions,
		pending: PendingReceiptStore,
		lifecycle: SveltekitStoreLifecycle<TData>
	) {
		this.#replica = replica;
		this.#artifact = artifact;
		this.#variables = variables;
		this.#options = options;
		this.#pending = pending;
		this.#lifecycle = lifecycle;
		this.#snapshot = querySnapshot(
			replica.read(artifact, variables),
			pending.get(),
			() => this.refetch()
		);
	}

	get(): SveltekitQuerySnapshot<TData> {
		if (!this.#destroyed && this.#watch === undefined) {
			this.#snapshot = querySnapshot(
				this.#replica.read(this.#artifact, this.#variables),
				this.#pending.get(),
				() => this.refetch()
			);
		}
		return this.#snapshot;
	}

	subscribe(
		listener: (snapshot: SveltekitQuerySnapshot<TData>) => void
	): () => void {
		if (this.#destroyed) {
			throw new Error('Distributed SvelteKit query is destroyed');
		}
		if (typeof listener !== 'function') {
			throw new TypeError('Distributed SvelteKit query listener must be a function');
		}
		if (!this.#lifecycle.isAlive()) {
			throw new Error('Distributed SvelteKit client is destroyed');
		}
		if (
			this.#listeners.size === 0 &&
			this.#lifecycle.activateWatches()
		) {
			this.#activate();
		}
		this.#listeners.add(listener);
		listener(this.#snapshot);
		let active = true;
		return () => {
			if (!active) return;
			active = false;
			this.#listeners.delete(listener);
			if (this.#listeners.size === 0) this.#deactivate();
		};
	}

	async refetch(): Promise<void> {
		if (this.#destroyed) {
			throw new Error('Distributed SvelteKit query is destroyed');
		}
		if (!this.#lifecycle.isAlive()) {
			throw new Error('Distributed SvelteKit client is destroyed');
		}
		if (this.#watch !== undefined) {
			await this.#watch.refresh();
			return;
		}
		const temporary = this.#replica.watch(
			this.#artifact,
			this.#variables,
			{ ...this.#options, live: false }
		);
		try {
			await temporary.refresh();
			this.#snapshot = querySnapshot(
				temporary.get(),
				this.#pending.get(),
				() => this.refetch()
			);
		} finally {
			temporary.destroy();
		}
	}

	destroy(): void {
		if (this.#destroyed) return;
		this.#destroyed = true;
		this.#deactivate();
		this.#listeners.clear();
	}

	get data(): ReplicaSnapshot<TData>['data'] {
		return this.#snapshot.data;
	}
	get status(): ReplicaSnapshot<TData>['status'] {
		return this.#snapshot.status;
	}
	get complete(): boolean {
		return this.#snapshot.complete;
	}
	get loading(): boolean {
		return this.#snapshot.loading;
	}
	get fetching(): boolean {
		return this.#snapshot.fetching;
	}
	get stale(): boolean {
		return this.#snapshot.stale;
	}
	get live(): ReplicaSnapshot<TData>['live'] {
		return this.#snapshot.live;
	}
	get errors(): readonly GqlError[] {
		return this.#snapshot.errors;
	}
	get error(): GqlError | undefined {
		return this.#snapshot.error;
	}
	get pending(): readonly ReplicaCommandReceipt<unknown>[] {
		return this.#snapshot.pending;
	}

	#activate(): void {
		let watch: ReplicaWatch<TData> | undefined;
		try {
			watch = this.#replica.watch(
				this.#artifact,
				this.#variables,
				this.#options
			);
			this.#watch = watch;
			this.#snapshot = querySnapshot(
				watch.get(),
				this.#pending.get(),
				() => this.refetch()
			);
			this.#unsubscribeWatch = watch.subscribe((snapshot) => {
				this.#publish(snapshot, this.#pending.get());
			});
			this.#unsubscribePending = this.#pending.subscribe((receipts) => {
				const current = this.#watch;
				if (current !== undefined) this.#publish(current.get(), receipts);
			});
			this.#lifecycle.active(this);
		} catch (error) {
			this.#unsubscribeWatch?.();
			this.#unsubscribePending?.();
			watch?.destroy();
			this.#watch = undefined;
			this.#unsubscribeWatch = undefined;
			this.#unsubscribePending = undefined;
			throw error;
		}
	}

	#deactivate(): void {
		if (this.#watch === undefined) return;
		this.#unsubscribeWatch?.();
		this.#unsubscribePending?.();
		this.#watch.destroy();
		this.#watch = undefined;
		this.#unsubscribeWatch = undefined;
		this.#unsubscribePending = undefined;
		this.#lifecycle.idle(this);
	}

	#publish(
		replicaSnapshot: ReplicaSnapshot<TData>,
		receipts: readonly ReplicaCommandReceipt<unknown>[]
	): void {
		if (this.#destroyed) return;
		const next = querySnapshot(replicaSnapshot, receipts, () => this.refetch());
		if (sameQuerySnapshot(this.#snapshot, next)) return;
		this.#snapshot = next;
		for (const listener of [...this.#listeners]) listener(next);
	}
}

class PendingReceiptStore {
	readonly #receipts = new Map<string, ReplicaCommandReceipt<unknown>>();
	readonly #listeners = new Set<
		(receipts: readonly ReplicaCommandReceipt<unknown>[]) => void
	>();
	#snapshot: readonly ReplicaCommandReceipt<unknown>[] = Object.freeze([]);

	get(): readonly ReplicaCommandReceipt<unknown>[] {
		return this.#snapshot;
	}

	subscribe(
		listener: (receipts: readonly ReplicaCommandReceipt<unknown>[]) => void
	): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	track(receipt: ReplicaCommandReceipt<unknown>): void {
		const lifecycle =
			replicaCommandProjectedLifecycleOf(receipt) ?? receipt.projected;
		if (lifecycle === undefined || this.#receipts.has(receipt.commandId)) {
			return;
		}
		this.#receipts.set(receipt.commandId, receipt);
		this.#publish();
		void lifecycle.then(
			() => this.#remove(receipt.commandId),
			() => this.#remove(receipt.commandId)
		);
	}

	clear(): void {
		if (this.#receipts.size === 0) return;
		this.#receipts.clear();
		this.#publish();
		this.#listeners.clear();
	}

	#remove(commandId: string): void {
		if (!this.#receipts.delete(commandId)) return;
		this.#publish();
	}

	#publish(): void {
		this.#snapshot = Object.freeze([...this.#receipts.values()]);
		for (const listener of [...this.#listeners]) listener(this.#snapshot);
	}
}

function wrapCommandTree<TCommands>(
	value: TCommands,
	pending: PendingReceiptStore
): TCommands {
	if (typeof value === 'function') {
		const command = value as (...args: unknown[]) => unknown;
		return ((...args: unknown[]) => {
			const result = command(...args);
			if (isPromiseLike(result)) {
				return result.then((receipt: unknown) => {
					if (isCommandReceipt(receipt)) pending.track(receipt);
					return receipt;
				});
			}
			return result;
		}) as TCommands;
	}
	if (value === null || typeof value !== 'object') return value;
	const output = Object.create(null) as Record<string, unknown>;
	for (const key of Object.keys(value)) {
		output[key] = wrapCommandTree(
			(value as Record<string, unknown>)[key],
			pending
		);
	}
	return Object.freeze(output) as TCommands;
}

function isCommandReceipt(
	value: unknown
): value is ReplicaCommandReceipt<unknown> {
	return (
		value !== null &&
		typeof value === 'object' &&
		typeof (value as { commandId?: unknown }).commandId === 'string' &&
		typeof (value as { status?: unknown }).status === 'function'
	);
}

function isPromiseLike(value: unknown): value is Promise<unknown> {
	return (
		value !== null &&
		(typeof value === 'object' || typeof value === 'function') &&
		typeof (value as { then?: unknown }).then === 'function'
	);
}

function querySnapshot<TData>(
	snapshot: ReplicaSnapshot<TData>,
	pending: readonly ReplicaCommandReceipt<unknown>[],
	refetch: () => Promise<void>
): SveltekitQuerySnapshot<TData> {
	return Object.freeze({
		...snapshot,
		loading: snapshot.fetching || snapshot.status === 'loading',
		...(snapshot.errors[0] === undefined
			? {}
			: { error: snapshot.errors[0] }),
		pending,
		refetch
	}) as SveltekitQuerySnapshot<TData>;
}

function sameQuerySnapshot<TData>(
	left: SveltekitQuerySnapshot<TData>,
	right: SveltekitQuerySnapshot<TData>
): boolean {
	return (
		left.data === right.data &&
		left.status === right.status &&
		left.complete === right.complete &&
		left.loading === right.loading &&
		left.fetching === right.fetching &&
		left.stale === right.stale &&
		left.live === right.live &&
		left.errors === right.errors &&
		left.pending === right.pending
	);
}

function createAuthorizationFence(
	source: SveltekitSessionSource,
	invalidate: () => void,
	onError: ((error: unknown) => void) | undefined
): Readonly<{ read(): Promise<GqlAuth>; dispose(): void }> {
	let current: Readonly<GqlAuth> | undefined;
	let queue = Promise.resolve();
	let disposed = false;
	const read = (): Promise<GqlAuth> => {
		const candidate = Promise.resolve().then(() => source.getAuth());
		const transition = queue.then(async () => {
			try {
				const next = snapshotAuthCredential(await candidate);
				if (
					current !== undefined &&
					!sameAuthCredential(current, next)
				) {
					invalidate();
				}
				current = next;
				return next;
			} catch (error) {
				current = undefined;
				invalidate();
				throw error;
			}
		});
		queue = transition.then(
			() => undefined,
			() => undefined
		);
		return transition;
	};
	const unsubscribe = source.subscribe?.(() => {
		if (disposed) return;
		void read().catch((error: unknown) => onError?.(error));
	});
	// Snapshot the bind-time credential so a later first request can fence a
	// token refresh that retained the same unverified JWT subject.
	void read().catch((error: unknown) => onError?.(error));
	return Object.freeze({
		read,
		dispose(): void {
			if (disposed) return;
			disposed = true;
			unsubscribe?.();
		}
	});
}

function validatedHydrationAuthority(
	value: SveltekitReplicaAuthority
): ReplicaAuthoritativeScope {
	const scope = value?.scope;
	if (
		value?.version !== 1 ||
		scope === null ||
		typeof scope !== 'object' ||
		scope.protocolVersion !== 1 ||
		typeof scope.schemaHash !== 'string' ||
		scope.schemaHash.length === 0 ||
		typeof scope.authorizationGeneration !== 'string' ||
		scope.authorizationGeneration.length === 0 ||
		typeof scope.cacheScope !== 'string' ||
		scope.cacheScope.length === 0
	) {
		throw new TypeError('Distributed SvelteKit hydration authority is invalid');
	}
	return scope;
}

function sameReplicaScope(
	left: ReplicaAuthoritativeScope,
	right: ReplicaAuthoritativeScope
): boolean {
	return (
		left.protocolVersion === right.protocolVersion &&
		left.schemaHash === right.schemaHash &&
		left.authorizationGeneration === right.authorizationGeneration &&
		left.cacheScope === right.cacheScope
	);
}
