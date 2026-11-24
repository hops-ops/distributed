import type { CacheEngine } from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import {
	compareDistributedDecimal,
	type DistributedDecimalString,
	type DistributedLiveCursor,
	type DistributedProtocolEnvelope,
	type DistributedQuerySnapshot
} from '../../protocol.js';
import { replicaIndexKey, resolveArguments } from '../identity/keys.js';
import type { ReplicaOperationArtifact, ReplicaWriteSource } from '../types.js';
import {
	compareIndexVector,
	incrementCanonicalDecimal,
	indexClockMap,
	latestCursors,
	protocolInvalid
} from './clocks.js';
import { serializeOperationProtocolGroup } from './hydration.js';
import type {
	OperationProtocolGroup,
	OperationProtocolSource,
	OperationProtocolState,
	ParsedReplicaHydration,
	RetiredOperation,
	SerializedOperationProtocolGroup,
	SharedIndexDisposition
} from './types.js';

/**
 * The protocol state of every operation: for its query and its live stream,
 * the indexes each wrote and at which local revision, their causal clocks and
 * resume cursors, and which of the two currently owns the result. Also the
 * local revision counter that orders index writes, and the per-operation
 * generation that fences callbacks of a superseded source.
 */
export class OperationProtocols {
	readonly #engine: CacheEngine;
	readonly #groups = new Map<string, OperationProtocolGroup>();
	readonly #generations = new Map<string, number>();
	#nextIndexRevision = '0';

	constructor(engine: CacheEngine) {
		this.#engine = engine;
	}

	get nextIndexRevision(): string {
		return this.#nextIndexRevision;
	}

	allocateIndexRevision(): string {
		this.#nextIndexRevision = incrementCanonicalDecimal(
			this.#nextIndexRevision
		);
		return this.#nextIndexRevision;
	}

	/** An operation's state, to read. Change it through this class. */
	group(key: string): Readonly<OperationProtocolGroup> | undefined {
		return this.#groups.get(key);
	}

	/** The state of one source of an operation, created on first use. */
	state(
		key: string,
		operation: string,
		source: OperationProtocolSource
	): OperationProtocolState {
		let group = this.#groups.get(key);
		if (group === undefined) {
			group = {};
			this.#groups.set(key, group);
		}
		const current = group[source];
		if (current !== undefined) {
			if (current.operation !== operation) {
				protocolInvalid('extensions.distributed.operation');
			}
			return current;
		}
		const created: OperationProtocolState = {
			operation,
			indexClocks: new Map(),
			indexKeys: new Set(),
			pathRecords: new Map(),
			cursors: Object.freeze([])
		};
		group[source] = created;
		return created;
	}

	generation(key: string): number {
		return this.#generations.get(key) ?? 0;
	}

	advance(key: string): void {
		this.#generations.set(key, this.generation(key) + 1);
	}

	/**
	 * Make `source` the owner of an operation's result, discarding the indexes
	 * the previous owner wrote. Returns whether ownership changed hands.
	 */
	activate<TData, TVariables extends GraphqlVariables>(
		key: string,
		source: OperationProtocolSource,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): boolean {
		const group = this.#groups.get(key);
		if (group === undefined) return false;
		const previous = group.active;
		if (previous === source) return false;
		if (previous !== undefined) {
			const discardedStates = new Set(
				[group.query, group.live].filter(
					(state): state is OperationProtocolState =>
						state !== undefined
				)
			);
			const revisionsByKey = new Map<string, Set<string>>();
			for (const state of [group.query, group.live]) {
				if (state?.indexRevision === undefined) continue;
				for (const indexKey of state.indexKeys) {
					let revisions = revisionsByKey.get(indexKey);
					if (revisions === undefined) {
						revisions = new Set();
						revisionsByKey.set(indexKey, revisions);
					}
					revisions.add(state.indexRevision);
				}
			}
			for (const root of artifact.roots) {
				const indexKey = replicaIndexKey({
					field: root.field,
					arguments: resolveArguments(
						root.arguments,
						variables,
						root.coverage
					)
				});
				let revisions = revisionsByKey.get(indexKey);
				if (revisions === undefined) {
					revisions = new Set();
					revisionsByKey.set(indexKey, revisions);
				}
				for (const state of [group.query, group.live]) {
					if (state?.indexRevision !== undefined) {
						revisions.add(state.indexRevision);
					}
				}
			}
			const confirmedFences = this.#engine.confirmedIndexFences([
				...revisionsByKey.keys()
			]);
			const ownedKeys = [...revisionsByKey].flatMap(
				([indexKey, revisions]) => {
					const revision = confirmedFences.get(indexKey);
					return revision !== undefined &&
						revisions.has(revision) &&
						!this.#claimedByAnother(
							indexKey,
							revision,
							discardedStates
						)
						? [indexKey]
						: [];
				}
			);
			this.#engine.discardIndexes(ownedKeys);
		}
		group.active = source;
		this.advance(key);
		return previous !== undefined;
	}

	/**
	 * How an incoming snapshot compares with the other operations that last
	 * wrote the indexes it shares with them. `liveStart` is the start revision
	 * of the receiving stream when its frame carries no comparable vector.
	 */
	sharedIndexDisposition(
		currentKey: string,
		incomingIndexKeys: ReadonlySet<string>,
		snapshot: DistributedQuerySnapshot,
		requestRevision: string | undefined,
		source: ReplicaWriteSource,
		liveStart: string | undefined
	): SharedIndexDisposition {
		if (incomingIndexKeys.size === 0) return { compared: false };
		const confirmedRevisions =
			this.#engine.confirmedIndexFences([...incomingIndexKeys]);
		let compared = false;
		let lower = false;
		let higher = false;
		let incomparable = false;
		let restartAfterRetirement = false;
		const fencedIndexKeys = new Set<string>();
		let equalRevision: string | undefined;
		let latestOwnerRevision: string | undefined;
		// An owner wrote one of the incoming indexes last, at its own revision.
		const ownerRevisions = new Set(confirmedRevisions.values());
		for (const [key, group] of this.#groups) {
			if (key === currentKey) continue;
			for (const state of [group.query, group.live]) {
				if (state?.indexRevision === undefined) continue;
				if (!ownerRevisions.has(state.indexRevision)) continue;
				const ownedIncomingKeys: string[] = [];
				for (const indexKey of state.indexKeys) {
					if (
						incomingIndexKeys.has(indexKey) &&
						confirmedRevisions.get(indexKey) === state.indexRevision
					) {
						ownedIncomingKeys.push(indexKey);
					}
				}
				if (ownedIncomingKeys.length === 0) continue;
				if (
					!snapshot.indexesComparable &&
					state === group.live &&
					state.retiredAtRevision !== undefined &&
					liveStart !== undefined
				) {
					// A disposed stream is no longer an owner. Its boundary is
					// still retained so a stream that started before disposal
					// cannot win merely because the old transport later closed.
					if (compareDistributedDecimal(liveStart, state.retiredAtRevision) > 0) {
						continue;
					}
					// Reopen at most once per observed retirement boundary: the
					// replacement's allocated start is strictly newer. Active
					// siblings never trigger this recovery and remain fenced.
					restartAfterRetirement = true;
				}
				latestOwnerRevision =
					latestOwnerRevision === undefined ||
					compareDistributedDecimal(
						state.indexRevision,
						latestOwnerRevision
					) > 0
						? state.indexRevision
						: latestOwnerRevision;
				/*
				 * Snapshot scope is bound to one operation plan instance and is
				 * not a cross-artifact identity. Shared semantic indexes are
				 * comparable through their projection/scope/position vector.
				 */
				const disposition =
					!snapshot.indexesComparable
						? 'incomparable'
						: state.indexClocks.size === 0 ||
							  snapshot.indexes.length === 0
							? state.indexClocks.size === snapshot.indexes.length
								? 'equal'
								: 'incomparable'
							: compareIndexVector(
									state.indexClocks,
									snapshot.indexes
								);
				if (
					disposition === 'fresh' ||
					disposition === 'incomparable'
				) {
					// A registered stream starts after the query seeds already in
					// this replica. It can take over those seeds, but not another
					// live stream or query ownership acquired after it started.
					if (
						liveStart !== undefined && state === group.query &&
						compareDistributedDecimal(liveStart, state.indexRevision) > 0
					) continue;
					incomparable = true;
					for (const indexKey of ownedIncomingKeys) fencedIndexKeys.add(indexKey);
					continue;
				}
				compared = true;
				if (disposition === 'lower') lower = true;
				else if (disposition === 'higher') higher = true;
				else {
					equalRevision =
						equalRevision === undefined ||
						compareDistributedDecimal(
							state.indexRevision,
							equalRevision
						) > 0
							? state.indexRevision
							: equalRevision;
				}
			}
		}
		/*
		 * One response is a coherent replacement graph. If its vector straddles
		 * the owners of two shared indexes, accepting only part would fabricate
		 * a snapshot the server never produced, so preserve the current graph.
		 */
		if (lower) return { compared: true, disposition: 'lower' };
		if (incomparable) {
			/*
			 * A comparable sibling must not promote an incomparable membership
			 * to response-arrival order. Fall back to request-start order for
			 * the whole graph, and reject it atomically when that request began
			 * before any owner it would replace. Independent live streams have
			 * no shared request-start fence, so they cannot safely replace it;
			 * explicit synchronous ingress retains its caller-defined order.
			 */
			if (requestRevision === undefined && source === 'live') {
				return {
					compared: true,
					disposition: 'lower',
					...(restartAfterRetirement ? { restartAfterRetirement: true } : {}),
					...(!snapshot.indexesComparable ? { fencedIndexKeys } : {})
				};
			}
			if (requestRevision === undefined) return { compared: false };
			return latestOwnerRevision !== undefined &&
				compareDistributedDecimal(requestRevision, latestOwnerRevision) <
					0
				? { compared: true, disposition: 'lower' }
				: { compared: false };
		}
		if (!compared) return { compared: false };
		if (higher) return { compared: true, disposition: 'higher' };
		return {
			compared: true,
			disposition: 'equal',
			...(equalRevision === undefined
				? {}
				: { indexRevision: equalRevision })
		};
	}

	resumeCursors(key: string): readonly DistributedLiveCursor[] {
		const group = this.#groups.get(key);
		if (group?.active === 'query' && group.query?.cursors.length) {
			return group.query.cursors;
		}
		if (group?.active === 'live' && group.live?.cursors.length) {
			return group.live.cursors;
		}
		return group?.live?.cursors.length
			? group.live.cursors
			: (group?.query?.cursors ?? Object.freeze([]));
	}

	/** Drop a source's state and the indexes only it owns. */
	discardSnapshot<TData, TVariables extends GraphqlVariables>(
		state: OperationProtocolState,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): void {
		const keys = new Set(state.indexKeys);
		for (const root of artifact.roots) {
			keys.add(
				replicaIndexKey({
					field: root.field,
					arguments: resolveArguments(
						root.arguments,
						variables,
						root.coverage
					)
				})
			);
		}
		const indexRevision = state.indexRevision;
		const discardedStates = new Set([state]);
		const confirmedFence = this.#engine.confirmedIndexFences([...keys]);
		const ownedKeys =
			indexRevision === undefined
				? []
				: [...keys].filter(
						(key) =>
							confirmedFence.get(key) === indexRevision &&
							!this.#claimedByAnother(
								key,
								indexRevision,
								discardedStates
							)
					);
		this.#engine.discardIndexes(ownedKeys);
		this.reset(state);
	}

	reset(state: OperationProtocolState): void {
		this.resetCausal(state);
		state.indexRevision = undefined;
		state.indexKeys.clear();
		state.pathRecords.clear();
	}

	/** Drop what requires a comparable causal vector; keep membership. */
	resetCausal(state: OperationProtocolState): void {
		state.snapshotScope = undefined;
		state.indexClocks = new Map();
		state.cursors = Object.freeze([]);
	}

	/** Remember which record each response path of an admitted result named. */
	rememberPaths(
		state: OperationProtocolState,
		pathRecords: ReadonlyMap<string, string>
	): void {
		for (const [path, recordKey] of pathRecords) {
			state.pathRecords.set(path, recordKey);
		}
	}

	/** Record which indexes an admitted snapshot wrote for one source, and how. */
	commitSnapshot(
		state: OperationProtocolState,
		written: {
			readonly writeIndexes: boolean;
			readonly indexRevision: string;
			readonly indexKeys: readonly string[];
			readonly snapshot: DistributedQuerySnapshot;
			readonly live: DistributedProtocolEnvelope['live'];
		}
	): void {
		const { snapshot, live } = written;
		if (written.writeIndexes) {
			if (!snapshot.indexesComparable) this.resetCausal(state);
			state.indexRevision = written.indexRevision;
			for (const indexKey of written.indexKeys) state.indexKeys.add(indexKey);
			if (snapshot.indexesComparable) {
				state.snapshotScope = snapshot.scopeToken;
				state.indexClocks = indexClockMap(snapshot.indexes);
				state.cursors = latestCursors(snapshot, live);
			}
		} else if (live?.reset === true || !snapshot.indexesComparable) {
			state.cursors = Object.freeze([]);
		}
	}

	/**
	 * Mark live protocol state as no longer backed by a transport. The state
	 * is retained for cache and hydration bookkeeping, but a later live stream
	 * may replace its incomparable membership only when that stream started
	 * after this boundary.
	 */
	retireLive(key: string): void {
		const state = this.#groups.get(key)?.live;
		if (state === undefined) return;
		state.retiredAtRevision = this.allocateIndexRevision();
	}

	reviveLive(key: string): void {
		const state = this.#groups.get(key)?.live;
		if (state !== undefined) state.retiredAtRevision = undefined;
	}

	/** A completed stream no longer owns the result; a query may take it. */
	releaseLiveOwnership(key: string): void {
		const group = this.#groups.get(key);
		if (group?.active === 'live') group.active = undefined;
	}

	/** Identifies the last result written for an operation, however it arrived. */
	written(key: string): RetiredOperation['written'] {
		const group = this.#groups.get(key);
		return {
			group,
			query: group?.query?.indexRevision,
			live: group?.live?.indexRevision
		};
	}

	delete(key: string): void {
		this.#groups.delete(key);
		this.#generations.delete(key);
	}

	clear(): void {
		this.#groups.clear();
		this.#generations.clear();
		this.#nextIndexRevision = '0';
	}

	/** Replace the state with a dehydrated one's (cold hydration). */
	restore(parsed: ParsedReplicaHydration): void {
		this.#groups.clear();
		for (const [key, group] of parsed.operationProtocols) {
			this.#groups.set(key, group);
		}
		this.#generations.clear();
		for (const [key, generation] of parsed.operationGenerations) {
			this.#generations.set(key, generation);
		}
		this.#nextIndexRevision = parsed.nextIndexRevision;
	}

	/**
	 * Merge a same-scope dehydrated state (soft navigation, second SSR seed).
	 * A route seed is not a transport handoff: its query-only protocol must
	 * not replace a retained layout's live cursor or generation. Returns the
	 * indexes the current state keeps, which the seed must not overwrite.
	 */
	merge(parsed: ParsedReplicaHydration): ReadonlySet<string> {
		const retainedIndexes = new Set<string>();
		for (const group of this.#groups.values()) {
			if (group.live !== undefined && group.live.retiredAtRevision === undefined) {
				for (const key of group.live.indexKeys) retainedIndexes.add(key);
			}
		}
		for (const [key, group] of parsed.operationProtocols) {
			const previous = this.#groups.get(key);
			if (previous?.live !== undefined && previous.live.retiredAtRevision === undefined) continue;
			const before = previous?.query;
			const incoming = group.query;
			if (
				incoming !== undefined &&
				[...incoming.indexKeys].some((index) => retainedIndexes.has(index))
			) continue;
			if (before !== undefined && incoming !== undefined) {
				const disposition = before.snapshotScope === incoming.snapshotScope
					? compareIndexVector(
						before.indexClocks,
						[...incoming.indexClocks].map(
							([projection, clock]) => ({ projection, ...clock })
						)
					)
					: 'incomparable';
				if (disposition === 'lower' || disposition === 'incomparable') {
					for (const index of incoming.indexKeys) retainedIndexes.add(index);
					continue;
				}
			}
			this.#groups.set(key, {
				...group,
				...(previous?.live === undefined ? {} : { live: previous.live })
			});
		}
		for (const [key, generation] of parsed.operationGenerations) {
			// These are replica-local callback fences, not comparable server clocks.
			if (!this.#generations.has(key)) {
				this.#generations.set(key, generation);
			}
		}
		return retainedIndexes;
	}

	/** Never let a merged state move the local revision counter back. */
	advanceIndexRevisionTo(revision: string): void {
		if (
			compareDistributedDecimal(
				revision as DistributedDecimalString,
				this.#nextIndexRevision as DistributedDecimalString
			) > 0
		) {
			this.#nextIndexRevision = revision;
		}
	}

	/**
	 * Serialize the rendered operations and every other operation that
	 * governs a reachable index or record clock.
	 */
	dehydrate(
		rendered: Iterable<string>,
		reachableIndexKeys: ReadonlySet<string>,
		clockRecordKeys: ReadonlySet<string>
	): readonly SerializedOperationProtocolGroup[] {
		const operationKeys = new Set(rendered);
		for (const [key, group] of this.#groups) {
			const governsReachableState = [group.query, group.live].some(
				(state) =>
					state !== undefined &&
					(
						[...state.indexKeys].some((indexKey) =>
							reachableIndexKeys.has(indexKey)
						) ||
						[...state.pathRecords.values()].some((recordKey) =>
							clockRecordKeys.has(recordKey)
						)
					)
			);
			if (governsReachableState) operationKeys.add(key);
		}
		return Object.freeze(
			[...operationKeys].sort().flatMap((key) => {
				const group = this.#groups.get(key);
				if (group === undefined) return [];
				return [
					serializeOperationProtocolGroup(key, group, this.generation(key))
				];
			})
		);
	}

	sizes(): Record<string, number> {
		return {
			operationProtocols: this.#groups.size,
			operationGenerations: this.#generations.size
		};
	}

	#claimedByAnother(
		indexKey: string,
		revision: string,
		excluded: ReadonlySet<OperationProtocolState>
	): boolean {
		for (const group of this.#groups.values()) {
			for (const state of [group.query, group.live]) {
				if (
					state !== undefined &&
					!excluded.has(state) &&
					state.indexRevision === revision &&
					state.indexKeys.has(indexKey)
				) {
					return true;
				}
			}
		}
		return false;
	}
}
