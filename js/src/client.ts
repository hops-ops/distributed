import { sameAuthCredential, snapshotAuthCredential } from './identity.js';
import { createDistributedReplica } from './replica/distributed-replica/index.js';
import { createReplicaGraphqlTransport, type ReplicaGraphqlTransport } from './replica/graphql-transport.js';
import type {
	ReplicaCommandRuntime,
	ReplicaCommandRuntimeOptions
} from './replica/command-runtime/types.js';
import type {
	DistributedReplica,
	DistributedReplicaOptions,
	ReplicaAuthoritativeScope,
	ReplicaDehydratedState,
	ReplicaOperationArtifact
} from './replica/types.js';
import type { GqlAuth, GraphqlVariables } from './types.js';
import type { WebSocketConstructor } from './websocket.js';
import type { FetchLike } from './request.js';
import {
	DistributedSvelteKitBoundaryController,
	type DistributedSvelteKitBoundaryInstance,
	type DistributedSvelteKitBoundaryLocation,
	type DistributedSvelteKitLocationContext,
	type SveltekitBoundaryLifecycleDiagnostic,
	type SveltekitBoundaryRetention
} from './boundary-lifecycle.js';
import type {
	DistributedBoundaryOperation,
	DistributedBoundaryVariableContext
} from './sveltekit/boundary-variables.js';

// The `Sveltekit*` names below are the published types of the SvelteKit entry;
// they describe framework-neutral contracts and are defined here so every
// adapter composes the same client.

type UnknownCommandEntries = Readonly<Record<string, never>>;

type SessionHydration = Readonly<{
	hydration: SveltekitReplicaHydration;
	authority: SveltekitReplicaAuthority;
}>;

export type SveltekitSessionSource = Readonly<{
	/** Current credential. HTTP, WS, and commands all call this exact source. */
	getAuth(): GqlAuth | Promise<GqlAuth>;
	/**
	 * Fresh server page data received together with the current credential.
	 * Only independently authorized, same-scope hydration permits reuse during
	 * credential rotation. Never synthesize this from a token or cached state.
	 */
	getHydration?(): SessionHydration | undefined;
	/**
	 * Notify on token, logout, role, tenant, or session changes.
	 *
	 * The adapter re-reads `getAuth`; callers never label a cache scope.
	 */
	subscribe?(listener: () => void): () => void;
}>;

export type SveltekitReplicaHydration = Readonly<{
	version: 1;
	state: ReplicaDehydratedState;
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

export type CreateDistributedClientOptions<TCommands> = Readonly<{
	session: SveltekitSessionSource;
	/** Same-origin `/graphql` by default. */
	url?: string | (() => string);
	fetch?: FetchLike;
	webSocket?: WebSocketConstructor;
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
	/**
	 * Page lifecycle participation (the browser reload transaction). Read after
	 * hydration; `register` runs transactionally with construction and its
	 * disposer runs on `destroy`.
	 */
	lifecycle?: Readonly<{
		commands(): NonNullable<SveltekitCommandRuntimeFactoryOptions['lifecycle']>;
		register(
			replica: DistributedReplica,
			runtime: SveltekitCommandRuntimeLike<TCommands> | undefined
		): () => void;
	}>;
}>;

/**
 * The framework-neutral client one adapter wraps: transport, replica, command
 * runtime, authorization fence, hydration, generated boundary retention, and
 * the observers destroyed with it. Adapters add their reactive bindings.
 */
export type DistributedClient<TCommands> = Readonly<{
	replica: DistributedReplica;
	transport: ReplicaGraphqlTransport;
	commands: TCommands;
	alive(): boolean;
	/** Ensure a generated boundary operation was registered with this client. */
	accepts(operation: Readonly<{ binding: Readonly<{ id: string }> }>): boolean;
	/** Destroy `observer` with the client unless it is released first. */
	track(observer: Readonly<{ destroy(): void }>): void;
	release(observer: Readonly<{ destroy(): void }>): void;
	retainBoundary<TSession, TProps>(
		instance: DistributedSvelteKitBoundaryInstance,
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): SveltekitBoundaryRetention;
	retainLocation<TSession, TProps>(
		location: DistributedSvelteKitBoundaryLocation,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): SveltekitBoundaryRetention;
	prefetchLocation<TSession, TProps>(
		pathname: string,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): Promise<void>;
	hydrate(
		hydration: SveltekitReplicaHydration,
		authority: SveltekitReplicaAuthority
	): boolean;
	prefetch(
		artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
		variables: GraphqlVariables
	): Promise<void>;
	preloadCommands(): Promise<void>;
	invalidateAuthorization(): void;
	destroy(): void;
}>;

export function createDistributedClient<TCommands>(
	options: CreateDistributedClientOptions<TCommands>
): DistributedClient<TCommands> {
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
		options.onAuthError,
		(transfer) => {
			const active = replica?.scope;
			if (
				active === undefined ||
				!sameReplicaScope(active, validatedHydrationAuthority(transfer.authority))
			) return false;
			if ((transfer.hydration.bindings ?? []).some(value => !boundaryIds.includes(value))) {
				return false;
			}
			return transfer.hydration.version === 1 && replica!.reauthorize(
				transfer.hydration.state,
				validatedHydrationAuthority(transfer.authority)
			);
		}
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

	const observers = new Set<Readonly<{ destroy(): void }>>();
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
			...(options.lifecycle === undefined
				? {}
				: { lifecycle: options.lifecycle.commands() })
		})
	);
	let unregister: (() => void) | undefined;
	try {
		unregister = options.lifecycle?.register(replica, commandRuntime);
	} catch (error) {
		// Client construction is transactional: a malformed lifecycle declaration
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
			: commandRuntime.commands;
	const assertAlive = (): void => {
		if (destroyed) throw new Error('Distributed SvelteKit client is destroyed');
	};

	return Object.freeze({
		replica,
		transport,
		commands,
		alive: () => !destroyed,
		accepts: (operation) => boundaryIds.includes(operation.binding.id),
		track(observer) {
			observers.add(observer);
		},
		release(observer) {
			observers.delete(observer);
		},
		retainBoundary(instance, context) {
			assertAlive();
			return boundaryController!.retain(instance, context);
		},
		retainLocation(location, context) {
			assertAlive();
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
			for (const observer of [...observers]) observer.destroy();
			observers.clear();
			unregister?.();
			commandRuntime?.dispose();
		}
	});
}

/** Warm one operation unless the replica already holds a complete, fresh snapshot. */
export function prefetchReplicaOperation<TData, TVariables extends GraphqlVariables>(
	replica: DistributedReplica,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables
): Promise<void> {
	const snapshot = replica.read(artifact, variables);
	if (snapshot.complete && !snapshot.stale) return Promise.resolve();
	const watch = replica.watch(artifact, variables, { live: false });
	return watch.refresh().finally(() => watch.destroy());
}

function createAuthorizationFence(
	source: SveltekitSessionSource,
	invalidate: () => void,
	onError: ((error: unknown) => void) | undefined,
	refresh: (transfer: SessionHydration) => boolean
): Readonly<{ read(): Promise<GqlAuth>; dispose(): void }> {
	let current: Readonly<GqlAuth> | undefined;
	const seenHydrations = new WeakSet<SveltekitReplicaHydration>();
	const seenAuthorities = new WeakSet<SveltekitReplicaAuthority>();
	let queue = Promise.resolve();
	let disposed = false;
	const read = (): Promise<GqlAuth> => {
		const candidate = (async () => {
			// Capture the credential and its independent server transfer together,
			// synchronously at notification time. A queued read would observe a
			// later seedless page update and lose the refresh's scope evidence.
			const credential = source.getAuth();
			const transfer = source.getHydration?.();
			return { auth: await credential, transfer };
		})();
		const transition = queue.then(async () => {
			try {
				const { auth, transfer } = await candidate;
				const next = snapshotAuthCredential(auth);
				if (
					current !== undefined &&
					!sameAuthCredential(current, next)
				) {
					const freshTransfer = transfer !== undefined &&
						!seenHydrations.has(transfer.hydration) &&
						!seenAuthorities.has(transfer.authority);
					if (!freshTransfer || !refresh(transfer)) invalidate();
				}
				current = next;
				if (transfer !== undefined) {
					seenHydrations.add(transfer.hydration);
					seenAuthorities.add(transfer.authority);
				}
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
