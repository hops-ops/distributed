'use client';

import { useDebugValue, useMemo, useSyncExternalStore } from 'react';

import type { GraphqlVariables } from '../types.js';
import { canonicalizeOperationVariables } from '../replica/identity/codec.js';
import type {
	DistributedReplica,
	ReplicaOperationArtifact,
	ReplicaSnapshot,
	ReplicaWatch,
	WatchReplicaOptions
} from '../replica/types.js';
import { useDistributedReplica } from './context.js';

const EMPTY_VARIABLES = Object.freeze({}) as Readonly<Record<never, never>>;

type UseDistributedQueryArguments<TVariables extends GraphqlVariables> =
	keyof TVariables extends never
		? readonly [
				variables?: TVariables,
				options?: WatchReplicaOptions
			]
		: readonly [
				variables: TVariables,
				options?: WatchReplicaOptions
			];

export type DistributedQueryResult<TData> = ReplicaSnapshot<TData> & {
	/** Force the shared cache-and-live coordinator to revalidate this operation. */
	readonly refresh: () => Promise<void>;
};

/**
 * React's commit-safe bridge around one framework-neutral ReplicaWatch.
 *
 * Constructing a core watch starts transport work, so this wrapper creates it
 * only from React's subscribe phase. Server renders use the side-effect-free
 * replica read path and therefore leave no abandoned watch behind.
 */
class ReactReplicaExternalStore<
	TData,
	TVariables extends GraphqlVariables
> {
	readonly #replica: DistributedReplica;
	readonly #artifact: ReplicaOperationArtifact<TData, TVariables>;
	readonly #variables: TVariables;
	readonly #options: WatchReplicaOptions;
	#watch: ReplicaWatch<TData> | undefined;
	#snapshot: ReplicaSnapshot<TData> | undefined;
	#subscriberCount = 0;
	#serverHold: (() => void) | undefined;

	constructor(
		replica: DistributedReplica,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		options: WatchReplicaOptions
	) {
		this.#replica = replica;
		this.#artifact = artifact;
		this.#variables = variables;
		this.#options = options;
	}

	readonly getSnapshot = (): ReplicaSnapshot<TData> => {
		if (this.#watch !== undefined) {
			this.#snapshot = this.#watch.get();
		}
		this.#snapshot ??= this.#replica.read(this.#artifact, this.#variables);
		return this.#snapshot;
	};

	/**
	 * A server render never subscribes, so it holds what it rendered for
	 * `dehydrate`; the request's replica is discarded with the hold. During
	 * client hydration the subscription that follows takes the hold over.
	 */
	readonly getServerSnapshot = (): ReplicaSnapshot<TData> => {
		this.#serverHold ??= this.#replica.retainOperation(
			this.#artifact,
			this.#variables
		);
		return this.getSnapshot();
	};

	readonly subscribe = (notify: () => void): (() => void) => {
		const watch = this.#watch ?? this.#createWatch();
		this.#subscriberCount += 1;
		this.#serverHold?.();
		this.#serverHold = undefined;
		let active = true;
		const unsubscribe = watch.subscribe((snapshot) => {
			this.#snapshot = snapshot;
			notify();
		});

		return () => {
			if (!active) return;
			active = false;
			unsubscribe();
			this.#subscriberCount -= 1;
			if (this.#subscriberCount === 0 && this.#watch === watch) {
				watch.destroy();
				this.#watch = undefined;
			}
		};
	};

	readonly refresh = async (): Promise<void> => {
		const activeWatch = this.#watch;
		if (activeWatch !== undefined) {
			await activeWatch.refresh();
			return;
		}

		// This path is primarily defensive for an event fired before subscription
		// commit. The temporary watch is always retired.
		const temporaryWatch = this.#replica.watch(
			this.#artifact,
			this.#variables,
			this.#options
		);
		try {
			await temporaryWatch.refresh();
			this.#snapshot = temporaryWatch.get();
		} finally {
			temporaryWatch.destroy();
		}
	};

	#createWatch(): ReplicaWatch<TData> {
		const watch = this.#replica.watch(
			this.#artifact,
			this.#variables,
			this.#options
		);
		this.#watch = watch;
		this.#snapshot = watch.get();
		return watch;
	}
}

/**
 * Bind a generated operation artifact to React without introducing a React
 * cache. All reads, fetches, live updates, optimism, and authorization fences
 * continue through the supplied DistributedReplica.
 */
export function useDistributedQuery<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	...args: UseDistributedQueryArguments<TVariables>
): DistributedQueryResult<TData> {
	const replica = useDistributedReplica();
	const suppliedVariables = (args[0] ?? EMPTY_VARIABLES) as TVariables;
	const live = args[1]?.live === true;

	// Core canonicalization is the only operation-input authority. The JSON text
	// is merely a React memo identity for that already-canonical frozen value.
	const canonicalVariables = canonicalizeOperationVariables(
		artifact,
		suppliedVariables
	);
	const variableIdentity = JSON.stringify(canonicalVariables);
	const store = useMemo(
		() =>
			new ReactReplicaExternalStore(
				replica,
				artifact,
				canonicalVariables,
				Object.freeze({ live })
			),
		[replica, artifact, variableIdentity, live]
	);
	const snapshot = useSyncExternalStore(
		store.subscribe,
		store.getSnapshot,
		store.getServerSnapshot
	);
	useDebugValue(`${artifact.id}: ${snapshot.status}`);

	return useMemo(
		() =>
			Object.freeze({
				...snapshot,
				refresh: store.refresh
			}) as DistributedQueryResult<TData>,
		[snapshot, store]
	);
}
