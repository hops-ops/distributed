import {
	createDistributedClient,
	prefetchReplicaOperation,
	type CreateDistributedClientOptions,
	type DistributedHydration,
	type DistributedHydrationAuthority,
	type DistributedSessionSource
} from '../client.js';
import type {
	DistributedReplica,
	ReplicaGraphqlTransport,
	ReplicaPendingRows,
	ReplicaOperationArtifact,
	ReplicaSnapshot,
	ReplicaWatch,
	ReplicaWindowOperationArtifact,
	ReplicaWindowRow,
	WatchReplicaOptions
} from '../replica/index.js';
import { replicaWindowPageSize } from '../replica/window.js';
import type { GqlError, GraphqlVariables } from '../types.js';
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
	SveltekitWindowStoreImpl,
	type SveltekitWindowStore,
	type UseSveltekitWindowOptions
} from './window.js';
import {
	distributedReloadLifecycle,
	registerDistributedReloadClient,
	type DistributedReloadOptions
} from './lifecycle.js';

export type SveltekitPageDataSource<TData extends PageGraphqlData> = Readonly<{
	get(): TData;
	subscribe?(listener: () => void): () => void;
}>;

export type SveltekitPageDataSessionSource<TData extends PageGraphqlData> =
	Readonly<{
		session: DistributedSessionSource;
		get(): TData;
		set(next: TData): void;
	}>;

export type SveltekitDistributedPageData = PageGraphqlData &
	Readonly<{
		distributed?: DistributedHydration;
		distributedAuthority?: DistributedHydrationAuthority;
	}>;

export type CreateDistributedSvelteKitOptions<TCommands = Readonly<Record<never, never>>> =
	Omit<CreateDistributedClientOptions<TCommands>, 'lifecycle' | 'acceptsHydration'> &
		Readonly<{
			/** Generated boundary operations accepted by this component-tree client. */
			boundaries: readonly DistributedBoundaryOperation[];
			/** Redacted structural lifecycle events for boundary ownership diagnostics. */
			onBoundaryDiagnostic?: (event: SveltekitBoundaryLifecycleDiagnostic) => void;
			/**
			 * Set to SvelteKit's `$app/environment` `browser` flag. When false,
			 * store subscriptions remain side-effect-free during component SSR.
			 */
			browser?: boolean;
			/** Generated clients supply the surface key; apps may declare safe state partitions. */
			reload?: DistributedReloadOptions;
		}>;

export type SveltekitQuerySnapshot<TData> = ReplicaSnapshot<TData> &
	Readonly<{
	loading: boolean;
	error?: GqlError;
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
	/** `$query.pending.has(row)`: the row is a preview the server has not returned yet. */
	readonly pending: ReplicaPendingRows;
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

type UseWindowArguments<TVariables extends GraphqlVariables> =
	Record<string, never> extends TVariables
		?
				| []
				| [variables: TVariables]
				| [variables: TVariables, options: UseSveltekitWindowOptions]
		: [variables: TVariables, options?: UseSveltekitWindowOptions];

/** A generated `@window` operation: everything a query has, plus `useWindow`. */
export type SveltekitWindowOperation<
	TData,
	TVariables extends GraphqlVariables
> = SveltekitBoundOperation<TData, TVariables> &
	Readonly<{
		/**
		 * The live head page plus every page `fetchMore()` loaded, as one
		 * ordered, de-duplicated `rows` list. Cursor and `limit` variables
		 * are the window's; pass the operation's other variables.
		 */
		useWindow(
			...args: UseWindowArguments<TVariables>
		): SveltekitWindowStore<ReplicaWindowRow<TData>>;
	}>;

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
		artifact: ReplicaWindowOperationArtifact<TData, TVariables>
	): SveltekitWindowOperation<TData, TVariables>;
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
		hydration: DistributedHydration,
		authority: DistributedHydrationAuthority
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
 * Bind the framework-neutral client to Svelte's readable-store lifecycle and
 * to the generated page/layout boundaries this component tree owns.
 *
 * Generated `$distributed` modules call this once, then export
 * `client.operation(Operation_Todos)` and `client.commands`.
 */
export function createDistributedSvelteKit<TCommands = Readonly<Record<never, never>>>(
	options: CreateDistributedSvelteKitOptions<TCommands>
): DistributedSvelteKitClient<TCommands> {
	if (!Array.isArray(options?.boundaries)) {
		throw new TypeError('Distributed SvelteKit requires generated boundary operations');
	}
	const boundaryIds = Object.freeze(
		[...new Set(options.boundaries.map(({ binding }) => binding.id))].sort()
	);
	// Boundary retention ends with every authorization generation; the replica
	// announces each one, including those the client itself closes.
	let boundaries: DistributedSvelteKitBoundaryController | undefined;
	const reload = options.browser === true ? options.reload : undefined;
	const client = createDistributedClient<TCommands>({
		...options,
		acceptsHydration: (hydration) =>
			(hydration.bindings ?? []).every((id) => boundaryIds.includes(id)),
		replica: {
			...options.replica,
			onAuthorizationGenerationDispose: () => {
				boundaries?.disposeScope();
				options.replica?.onAuthorizationGenerationDispose?.();
			}
		},
		lifecycle:
			reload === undefined
				? undefined
				: {
						commands: distributedReloadLifecycle,
						register: (replica, runtime) =>
							registerDistributedReloadClient(replica, runtime, reload)
					}
	});
	boundaries = new DistributedSvelteKitBoundaryController(
		client.replica,
		options.boundaries,
		options.onBoundaryDiagnostic
	);
	const controller = boundaries;
	const assertAlive = (): void => {
		if (!client.alive()) throw new Error('Distributed SvelteKit client is destroyed');
	};
	const lifecycle: SveltekitStoreLifecycle = Object.freeze({
		isAlive: client.alive,
		activateWatches: () => options.browser ?? true,
		active: client.track,
		idle: client.release
	});
	const operation = (<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): SveltekitBoundOperation<TData, TVariables> =>
		bindOperation(client.replica, artifact, lifecycle)) as DistributedSvelteKitClient<TCommands>['operation'];
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
		replica: client.replica,
		transport: client.transport,
		commands: client.commands,
		operation,
		boundary,
		retainBoundary(instance, context) {
			assertAlive();
			return controller.retain(instance, context);
		},
		retainLocation(location, context) {
			assertAlive();
			return controller.retainLocation(location, context);
		},
		prefetchLocation(pathname, context) {
			if (!client.alive()) {
				return Promise.reject(new Error('Distributed SvelteKit client is destroyed'));
			}
			return controller.prefetchLocation(pathname, context);
		},
		hydrate: client.hydrate,
		prefetch: client.prefetch,
		preloadCommands: client.preloadCommands,
		invalidateAuthorization: client.invalidateAuthorization,
		destroy(): void {
			if (!client.alive()) return;
			controller.destroy();
			client.destroy();
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
	artifact: ReplicaWindowOperationArtifact<TData, TVariables>
): SveltekitWindowOperation<TData, TVariables>;
export function bindSveltekitOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	replica: DistributedReplica,
	artifact: ReplicaOperationArtifact<TData, TVariables>
): SveltekitBoundOperation<TData, TVariables>;
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
): DistributedSessionSource {
	if (
		source === null ||
		typeof source !== 'object' ||
		typeof source.get !== 'function'
	) {
		throw new TypeError('page-data session source requires get');
	}
	return Object.freeze({
		getAuth: () => authFromPageData(source.get()),
		getHydration: () => {
			const data: SveltekitDistributedPageData = source.get();
			return data.distributed === undefined || data.distributedAuthority === undefined
				? undefined
				: { hydration: data.distributed, authority: data.distributedAuthority };
		},
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
	lifecycle: SveltekitStoreLifecycle
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
			lifecycle
		);
		return store;
	};
	const window = artifact.window;
	const useWindow =
		window === undefined
			? undefined
			: (
					variables: TVariables = {} as TVariables,
					options: UseSveltekitWindowOptions = {}
				): SveltekitWindowStore<unknown> => {
					const pageSize = replicaWindowPageSize(window, options.pageSize);
					const live = options.live ?? artifact.live !== undefined;
					const open = (values: GraphqlVariables, pageLive: boolean) =>
						use(...([values, { live: pageLive }] as UseOperationArguments<TVariables>));
					// The head keeps the caller's variables (and so the cache key
					// SSR seeded) unless a page size overrides the default limit.
					const head =
						options.pageSize === undefined
							? variables
							: { ...variables, [window.limitVariable]: pageSize };
					return new SveltekitWindowStoreImpl(
						window,
						artifact.roots[0]!.responseKey,
						open,
						variables,
						pageSize,
						head,
						live
					);
				};
	return Object.freeze({
		artifact,
		use,
		...(useWindow === undefined ? {} : { useWindow }),
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

type SveltekitStoreLifecycle = Readonly<{
	isAlive(): boolean;
	activateWatches(): boolean;
	active(store: Readonly<{ destroy(): void }>): void;
	idle(store: Readonly<{ destroy(): void }>): void;
}>;

class SveltekitQueryStoreImpl<TData> implements SveltekitQueryStore<TData> {
	readonly #replica: DistributedReplica;
	readonly #artifact: ReplicaOperationArtifact<TData, GraphqlVariables>;
	readonly #variables: GraphqlVariables;
	readonly #options: WatchReplicaOptions;
	readonly #lifecycle: SveltekitStoreLifecycle;
	readonly #listeners = new Set<
		(snapshot: SveltekitQuerySnapshot<TData>) => void
	>();
	#watch: ReplicaWatch<TData> | undefined;
	#unsubscribeWatch: (() => void) | undefined;
	#snapshot: SveltekitQuerySnapshot<TData>;
	#destroyed = false;

	constructor(
		replica: DistributedReplica,
		artifact: ReplicaOperationArtifact<TData, GraphqlVariables>,
		variables: GraphqlVariables,
		options: WatchReplicaOptions,
		lifecycle: SveltekitStoreLifecycle
	) {
		this.#replica = replica;
		this.#artifact = artifact;
		this.#variables = variables;
		this.#options = options;
		this.#lifecycle = lifecycle;
		this.#snapshot = querySnapshot(replica.read(artifact, variables), () =>
			this.refetch()
		);
	}

	get(): SveltekitQuerySnapshot<TData> {
		if (!this.#destroyed && this.#watch === undefined) {
			this.#snapshot = querySnapshot(
				this.#replica.read(this.#artifact, this.#variables),
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
			this.#snapshot = querySnapshot(temporary.get(), () => this.refetch());
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
	get pending(): ReplicaPendingRows {
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
			this.#snapshot = querySnapshot(watch.get(), () => this.refetch());
			this.#unsubscribeWatch = watch.subscribe((snapshot) => {
				this.#publish(snapshot);
			});
			this.#lifecycle.active(this);
		} catch (error) {
			this.#unsubscribeWatch?.();
			watch?.destroy();
			this.#watch = undefined;
			this.#unsubscribeWatch = undefined;
			throw error;
		}
	}

	#deactivate(): void {
		if (this.#watch === undefined) return;
		this.#unsubscribeWatch?.();
		this.#watch.destroy();
		this.#watch = undefined;
		this.#unsubscribeWatch = undefined;
		this.#lifecycle.idle(this);
	}

	#publish(replicaSnapshot: ReplicaSnapshot<TData>): void {
		if (this.#destroyed) return;
		const next = querySnapshot(replicaSnapshot, () => this.refetch());
		if (sameQuerySnapshot(this.#snapshot, next)) return;
		this.#snapshot = next;
		for (const listener of [...this.#listeners]) listener(next);
	}
}

function querySnapshot<TData>(
	snapshot: ReplicaSnapshot<TData>,
	refetch: () => Promise<void>
): SveltekitQuerySnapshot<TData> {
	return Object.freeze({
		...snapshot,
		loading: snapshot.fetching || snapshot.status === 'loading',
		...(snapshot.errors[0] === undefined
			? {}
			: { error: snapshot.errors[0] }),
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
