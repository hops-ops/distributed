import type { GraphqlVariables } from '../../types.js';
import { canonicalizeOperationVariables } from '../identity/codec.js';
import type { MaterializedReplicaResult } from '../materialize.js';
import type {
	ReplicaOperationArtifact,
	ReplicaSnapshot,
	ReplicaWatch,
	WatchReplicaOptions
} from '../types.js';
import type { DistributedReplicaImpl } from './impl.js';
import { operationKey, snapshotEqual, snapshotFrom, deepEqual } from './helpers.js';

export class ReplicaWatchState<TData, TVariables extends GraphqlVariables>
	implements ReplicaWatch<TData>
{
	readonly key: string;
	readonly artifact: ReplicaOperationArtifact<TData, TVariables>;
	readonly variables: TVariables;
	readonly liveRequested: boolean;
	materialized: MaterializedReplicaResult<TData>;
	readonly #owner: DistributedReplicaImpl;
	readonly #listeners = new Set<(snapshot: ReplicaSnapshot<TData>) => void>();
	#snapshot: ReplicaSnapshot<TData>;
	#identitySignature: string;
	#destroyed = false;
	#autoFetchScheduled = false;
	readonly #unregister: () => void;

	constructor(
		owner: DistributedReplicaImpl,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		options: WatchReplicaOptions
	) {
		this.#owner = owner;
		this.artifact = artifact;
		this.variables = canonicalizeOperationVariables(artifact, variables);
		this.key = operationKey(artifact, this.variables);
		this.liveRequested = options.live === true;
		this.materialized = owner._materialize(artifact, this.variables);
		this.#identitySignature = this.materialized.identitySignature;
		this.#snapshot = snapshotFrom(this.materialized, owner._state(this.key));
		this.#unregister = owner._register(this);
	}

	get(): ReplicaSnapshot<TData> {
		return this.#snapshot;
	}

	subscribe(listener: (snapshot: ReplicaSnapshot<TData>) => void): () => void {
		if (this.#destroyed) throw new Error('replica watch is destroyed');
		if (typeof listener !== 'function') throw new TypeError('replica listener must be a function');
		this.#listeners.add(listener);
		try {
			listener(this.#snapshot);
		} catch (error) {
			this.#listeners.delete(listener);
			this.#owner._reportObserverErrors([error]);
		}
		return () => this.#listeners.delete(listener);
	}

	refresh(): Promise<void> {
		if (this.#destroyed) return Promise.reject(new Error('replica watch is destroyed'));
		return this.#owner._fetch(this, true);
	}

	destroy(): void {
		if (this.#destroyed) return;
		this.#destroyed = true;
		this.#unregister();
		this.#listeners.clear();
	}

	_cacheChanged(materialized: MaterializedReplicaResult<TData>): void {
		if (this.#destroyed) return;
		this.materialized = materialized;
		this.#sync(false);
		this.#scheduleAutoFetch();
	}

	_stateChanged(allowFetch: boolean): void {
		if (this.#destroyed) return;
		this.#sync(allowFetch);
	}

	#sync(allowFetch: boolean): void {
		const state = this.#owner._state(this.key);
		const nextRaw = snapshotFrom(this.materialized, state);
		const keepData =
			this.#identitySignature === this.materialized.identitySignature &&
			deepEqual(this.#snapshot.data, nextRaw.data);
		this.#identitySignature = this.materialized.identitySignature;
		const next: ReplicaSnapshot<TData> = keepData
			? (Object.freeze({
					...nextRaw,
					data: this.#snapshot.data
				}) as ReplicaSnapshot<TData>)
			: nextRaw;
		if (!snapshotEqual(this.#snapshot, next)) {
			this.#snapshot = next;
			const errors: unknown[] = [];
			for (const listener of this.#listeners) {
				try {
					listener(next);
				} catch (error) {
					errors.push(error);
				}
			}
			this.#owner._reportObserverErrors(errors);
		}
		if (allowFetch) void this.#owner._fetch(this, false);
	}

	#scheduleAutoFetch(): void {
		if (this.#autoFetchScheduled || this.#destroyed) return;
		this.#autoFetchScheduled = true;
		queueMicrotask(() => {
			this.#autoFetchScheduled = false;
			if (!this.#destroyed) void this.#owner._fetch(this, false);
		});
	}
}
