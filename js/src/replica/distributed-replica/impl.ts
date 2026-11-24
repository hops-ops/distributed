import {
	CacheRevisionConflictError,
	createCacheEngine,
	type BaseCacheWriter,
	type CacheEngine,
	type CacheReader,
	type DerivedIndexMutation,
	type DerivedIndexReconciler,
	type OptimisticLayerView
} from '../../internal/cache-engine.js';
import {
	defaultScheduler,
	unrefTimer,
	type Scheduler
} from '../../lib/backoff.js';
import type { GraphqlVariables } from '../../types.js';
import {
	compareDistributedDecimal,
	DistributedProtocolError,
	parseGraphqlResponseExtensions,
	type DistributedCommandMetadata,
	type DistributedProtocolEnvelope,
	type DistributedQuerySnapshot
} from '../../protocol.js';
import {
	replicaCommandAuthority,
	replicaCommandDirectProjection,
	replicaCommandFreshness,
	replicaCommandProjectionDelta,
	replicaCommandReadRecord,
	replicaResultObservation
} from '../command-runtime/symbols.js';
import type {
	ReplicaCommandAuthorityRegistration,
	ReplicaCommandAuthoritySnapshot,
	ReplicaCommandDirectProjection,
	ReplicaCommandSurfaceContract,
	ReplicaResultObservationRegistration
} from '../command-runtime/types.js';
import { canonicalizeOperationVariables } from '../identity/codec.js';
import {
	replicaIndexKey,
	replicaRecordKey,
	resolveArguments
} from '../identity/keys.js';
import {
	materializeReplicaOperation,
	type MaterializedReplicaResult
} from '../materialize.js';
import {
	normalizeReplicaResult,
	type ReplicaNormalizationProtocol,
	type ReplicaProtocolRecordResolution
} from '../normalize.js';
import { createReplicaRevalidationMatcher } from '../revalidation.js';
import { formatReplicaIndexStaleReason } from '../index-maintenance/engine.js';
import { createReplicaIndexMaintenanceRegistry } from '../index-maintenance/registry.js';
import type {
	ReplicaIndexPlanRegistration,
	ReplicaIndexSemanticChange
} from '../index-maintenance/types.js';
import type {
	DistributedReplicaOptions,
	DistributedReplica as DistributedReplicaApi,
	ReplicaAuthoritativeScope,
	ReplicaBaseWriter,
	ReplicaDehydratedState,
	ReplicaIdentity,
	ReplicaIndexInspection,
	ReplicaIndexTarget,
	ReplicaModelArtifact,
	ReplicaOperationArtifact,
	ReplicaOptimisticWriter,
	ReplicaRecordInspection,
	ReplicaRevalidationPlan,
	ReplicaRevision,
	ReplicaResultEnvelope,
	ReplicaSnapshot,
	ReplicaValue,
	ReplicaWatch,
	ReplicaWriteSource,
	WatchReplicaOptions
} from '../types.js';
import {
	EMPTY_CACHE_SNAPSHOT,
	IDLE_QUERY_STATE,
	OPERATION_RETENTION_GRACE_MS
} from './constants.js';
import type {
	IndexDisposition,
	ParsedReplicaHydration,
	ProtocolGeneration,
	QueryState,
	RenderedOperation,
	RetiredOperation,
	ReplicaDehydratedPayloadV1,
	SharedIndexDisposition
} from './types.js';
import {
	compareSnapshotToOperationState,
	isComparableHandoffDisposition,
	protocolInvalid,
	modelFromRecordKey,
	responsePathKey
} from './clocks.js';
import {
	assertWriteSource,
	indexKeyFromTarget,
	indexMaintenanceSnapshot,
	indexSemanticLayer,
	isGraphqlFailurePayload,
	operationKey,
	prepareRecordEvidence,
	protocolOperationSource,
	replicaResultIndexKeys,
	type ReplicaIndexMemberships,
	reportSafely,
	reportUnhandledObserverError,
	snapshotFrom,
	stableErrors
} from './helpers.js';
import {
	hydrationTables,
	parseAuthoritativeScope,
	parseDehydratedState,
	reachableConfirmedState
} from './hydration.js';
import { ReplicaWatchState } from './watch.js';
import { ReplicaAuthority } from './authority.js';
import { DiagnosticsReporter } from './diagnostics.js';
import { LiveCoordinator } from './live-coordinator.js';
import { OperationProtocols } from './operation-protocols.js';
import { OptimisticLayers } from './optimistic-layers.js';
import { RecordClocks } from './record-clocks.js';

/**
 * The replica: one cache engine and the modules that own its protocol state.
 *
 * - `ReplicaAuthority`: scope, trusted presets, bound contracts, generation.
 * - `RecordClocks`: per-record causal clocks and projected-record fences.
 * - `OperationProtocols`: per-operation query/live protocol state and the
 *   local index revision counter.
 * - `OptimisticLayers`: per-command receipts, membership fences, freshness.
 * - `LiveCoordinator`: watches, query state, HTTP requests, live streams.
 * - `DiagnosticsReporter`: the optional sink and its layer mirror.
 *
 * This class composes them: it admits results (one cache transaction that
 * spans clocks, protocols and receipts), hydrates, and owns the rendered
 * operations, their index plans and their retention.
 */
export class DistributedReplicaImpl implements DistributedReplicaApi {
	readonly #engine: CacheEngine;
	readonly #scheduler: Scheduler;
	readonly #retentionGraceMs: number;
	readonly #reportObserverError: (error: AggregateError) => void;
	readonly #authority: ReplicaAuthority;
	readonly #diagnostics: DiagnosticsReporter;
	readonly #clocks = new RecordClocks();
	readonly #operations: OperationProtocols;
	readonly #optimistic: OptimisticLayers;
	readonly #live: LiveCoordinator;
	readonly #renderedOperations = new Map<string, RenderedOperation>();
	/** Watches and retained operations holding each rendered operation. */
	readonly #renderCounts = new Map<string, number>();
	/** Operations whose last holder left, awaiting collection. */
	readonly #retired = new Map<string, RetiredOperation>();
	#retentionTimer: unknown;
	readonly #resultObservers = new Set<
		(envelope: ReplicaResultEnvelope<unknown>) => void
	>();
	readonly #indexMaintenance = createReplicaIndexMaintenanceRegistry();
	readonly #indexPlanRegistrations = new Map<
		string,
		ReplicaIndexPlanRegistration
	>();
	readonly #derivedIndexReconciler: DerivedIndexReconciler = (
		confirmed,
		layers
	) => this.#deriveMaintainedIndexes(confirmed, layers);

	constructor(options: DistributedReplicaOptions = {}) {
		this.#scheduler = options.scheduler ?? defaultScheduler;
		this.#retentionGraceMs =
			options.retentionGraceMs ?? OPERATION_RETENTION_GRACE_MS;
		this.#reportObserverError = options.onObserverError ?? reportUnhandledObserverError;
		this.#engine = createCacheEngine({ onWatcherError: this.#reportObserverError });
		this.#authority = new ReplicaAuthority({
			onDispose: options.onAuthorizationGenerationDispose,
			reportObserverErrors: (errors) => this._reportObserverErrors(errors),
			purge: () => this.#purge()
		});
		this.#diagnostics = new DiagnosticsReporter({
			sink: options.diagnostics,
			engine: this.#engine,
			reportObserverError: this.#reportObserverError,
			scope: () => this.#authority.scope,
			generation: () => this.#authority.sequence,
			receipt: (id) => this.#optimistic.receipt(id)
		});
		this.#optimistic = new OptimisticLayers(this.#engine, this.#diagnostics);
		this.#operations = new OperationProtocols(this.#engine);
		this.#live = new LiveCoordinator({
			transport: options.transport,
			scheduler: this.#scheduler,
			random: options.random,
			authority: this.#authority,
			operations: this.#operations,
			clocks: this.#clocks,
			diagnostics: this.#diagnostics,
			write: (
				artifact,
				stableVariables,
				envelope,
				source,
				requestRevision,
				responseProjectionGeneration
			) =>
				this.#writeCanonicalResult(
					artifact,
					stableVariables,
					envelope,
					source,
					requestRevision,
					responseProjectionGeneration
				),
			freshness: (artifact, key) => this.#freshness(artifact, key)
		});
		this.#engine.setDerivedIndexReconciler(this.#derivedIndexReconciler);
		this.#diagnostics.sync();
	}

	get scope(): ReplicaAuthoritativeScope | undefined {
		return this.#authority.publicScope();
	}

	get authorizationGeneration(): number {
		return this.#authority.sequence;
	}

	[replicaCommandAuthority](
		contract: ReplicaCommandSurfaceContract
	): ReplicaCommandAuthorityRegistration {
		this.#authority.registerCommands(contract);
		let active = true;
		const read = (): ReplicaCommandAuthoritySnapshot => {
			if (!active) {
				throw new TypeError('replica command authority registration is disposed');
			}
			return this.#authority.commandSnapshot();
		};
		return Object.freeze({
			read,
			dispose(): void {
				active = false;
			}
		});
	}

	[replicaCommandFreshness](commandId: string, plan: ReplicaRevalidationPlan): void {
		// Validate before dispatch, using the existing generated dependency matcher.
		createReplicaRevalidationMatcher(plan);
		this.#optimistic.registerFreshness(commandId, plan, this.#authority.sequence);
	}

	#freshness(artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>, key: string): Readonly<Record<string, unknown>> | undefined {
		const scope = this.#authority.scope;
		const protocolHash = artifact.protocol.protocolHash ?? this.#authority.protocolHash;
		if (scope === undefined || protocolHash === undefined) return undefined;
		const pending: unknown[] = [];
		for (const plan of this.#optimistic.pendingFreshness(this.#authority.sequence)) {
			if (!createReplicaRevalidationMatcher(plan)(artifact)) continue;
			// Overlap already uses full generated list/filter/count/relationship
			// dependencies. An overlapping request must use primary even when an
			// incomplete producer only supplied an opaque dependency name.
			pending.push({ complete: false, models: [...plan.models], relationships: [] });
		}
		const minimum: unknown[] = [];
		// Retained clocks live independently of pending layer state. Index scopes
		// belong to this query/live plan; never compare a different query's clock.
		const group = this.#operations.group(key);
		for (const state of [group?.query, group?.live]) {
			for (const [projection, clock] of state?.indexClocks ?? []) {
				minimum.push({ kind: 'index', projection, scopeToken: clock.scopeToken, position: clock.position });
			}
		}
		// Direct Atomic effects have not yet acquired a query index checkpoint.
		// Once a proving query retires this fence, its retained index clock covers
		// membership (including later deletion) without requiring an absent row.
		for (const [recordKey, { clock }] of this.#clocks.projectedFences()) {
			const model = modelFromRecordKey(recordKey);
			if (model === undefined || !createReplicaRevalidationMatcher({ dependencies: [], models: [model], relationships: [] })(artifact)) continue;
			minimum.push({ kind: 'record', model, scopeToken: clock.scopeToken, incarnation: clock.incarnation, revision: clock.revision });
		}
		if (pending.length + minimum.length > 256) throw new Error('causal delivery context exceeds bounded evidence budget');
		return Object.freeze({ version: 1, schemaHash: scope.schemaHash, protocolHash, authorizationGeneration: scope.authorizationGeneration, cacheScope: scope.cacheScope, pending, minimum });
	}

	[replicaResultObservation](
		observer: (envelope: ReplicaResultEnvelope<unknown>) => void
	): ReplicaResultObservationRegistration {
		if (typeof observer !== 'function') {
			throw new TypeError('replica result observer must be a function');
		}
		this.#resultObservers.add(observer);
		let active = true;
		return Object.freeze({
			dispose: (): void => {
				if (!active) return;
				active = false;
				this.#resultObservers.delete(observer);
			}
		});
	}

	[replicaCommandDirectProjection](
		commandId: string,
		projection: ReplicaCommandDirectProjection
	): void {
		const { model, identity, evidence, fields } = projection;
		if (evidence.model !== model.id || evidence.tombstone) {
			protocolInvalid('extensions.distributed.command.records');
		}
		const recordKey = replicaRecordKey(model, identity);
		const admitted = this.#clocks.evidence();
		const apply = admitted.resolve(recordKey, evidence);
		/*
		 * The Atomic output is a complete authoritative row. Seal every active
		 * collection membership that the compiler plan can prove from that row in
		 * the same base transaction; otherwise the record exists by key while a
		 * warm @load index still omits it until refresh.
		 */
		const indexMutations = apply
			? this.#directProjectionIndexMutations(
					commandId,
					model,
					recordKey,
					fields
				)
			: Object.freeze([]);
		const indexRevision =
			indexMutations.length === 0
				? undefined
				: this.#operations.allocateIndexRevision();

		this.#optimistic.confirmWithCacheWriter(commandId, (writer) => {
			if (!apply) return false;
			const wrote = writer.writeRecord({
				key: recordKey,
				revision: evidence.revision,
				incarnation: evidence.incarnation,
				fields
			});
			if (indexRevision !== undefined) {
				for (const mutation of indexMutations) {
					switch (mutation.kind) {
						case 'write':
							writer.writeIndex({
								...mutation.write,
								revision: indexRevision
							});
							break;
						case 'stale':
							writer.markIndexStale(
								mutation.key,
								mutation.reason,
								indexRevision
							);
							break;
						case 'delete':
							writer.deleteIndex(mutation.key, indexRevision);
							break;
					}
				}
			}
			return wrote;
		});

		admitted.commit();
		/*
		 * Do not also take a membership fence: Atomic lists are @load, not
		 * @live. A membership fence would reject later complete snapshots
		 * until a live frame that never comes, stalling blob/new games and
		 * client-side navigations.
		 */
		if (apply) this.#clocks.project(recordKey, fields, evidence);
	}

	[replicaCommandProjectionDelta](
		commandId: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[]
	): boolean {
		return this.#optimistic.replace(commandId, update, semanticChanges);
	}

	read<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): ReplicaSnapshot<TData> {
		this.#authority.bindArtifact(artifact);
		const stableVariables = canonicalizeOperationVariables(artifact, variables);
		const key = operationKey(artifact, stableVariables);
		const materialized = this.#engine.read((reader) =>
			materializeReplicaOperation(reader, artifact, stableVariables)
		);
		// A read registers nothing, so it must not create query state either.
		return snapshotFrom(
			materialized,
			this.#live.peekState(key) ?? IDLE_QUERY_STATE
		);
	}

	retainOperation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): () => void {
		this.#authority.bindArtifact(artifact);
		const stableVariables = canonicalizeOperationVariables(artifact, variables);
		const key = operationKey(artifact, stableVariables);
		this.#rememberRenderedOperation(key, artifact, stableVariables);
		let retained = true;
		return () => {
			if (!retained) return;
			retained = false;
			this.#forgetRenderedOperation(key);
		};
	}

	watch<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		options: WatchReplicaOptions = {}
	): ReplicaWatch<TData> {
		this.#authority.bindArtifact(artifact);
		return new ReplicaWatchState(this, artifact, variables, options);
	}

	writeResult<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource
	): void {
		assertWriteSource(source);
		this.#authority.bindArtifact(artifact);
		const stableVariables = canonicalizeOperationVariables(artifact, variables);
		this.#writeCanonicalResult(artifact, stableVariables, envelope, source);
	}

	revalidate(plan: ReplicaRevalidationPlan): Promise<void> {
		const matches = createReplicaRevalidationMatcher(plan);
		const generation = this.#authority.sequence;
		const active = new Map<
			string,
			ReplicaWatchState<unknown, GraphqlVariables>
		>();
		for (const watch of this.#live.watchedOperations()) {
			if (matches(watch.artifact)) active.set(watch.key, watch);
		}
		/*
		 * A command-status observation may arrive while an older query is
		 * already in flight. Drain that request first; the authoritative
		 * revalidation below must start after the command fence.
		 */
		const prior = [...active.keys()].flatMap((key) => {
			const request = this.#live.request(key);
			return request === undefined ? [] : [request];
		});
		return Promise.all(prior)
			.then(() => {
				if (this.#authority.sequence !== generation) return [];

				/*
				 * Stale every rendered matching root before fetching. This both
				 * starts active watches through their normal coordinator and
				 * leaves inactive SSR/read consumers fail-closed until a future
				 * watch can refresh them.
				 */
				const rootKeys = new Set<string>();
				for (const rendered of this.#renderedOperations.values()) {
					if (!matches(rendered.artifact)) continue;
					for (const root of rendered.artifact.roots) {
						rootKeys.add(
							replicaIndexKey({
								field: root.field,
								arguments: resolveArguments(
									root.arguments,
									rendered.variables,
									root.coverage
								)
							})
						);
					}
				}
				for (const key of rootKeys) {
					this.#engine.batch((writer) =>
						writer.markIndexStale(
							key,
							'command-authoritative-revalidation'
						)
					);
				}

				// One operation key may have several component subscribers. The
				// existing fetch coordinator owns deduplication and response fences.
				return [...active.values()].map((watch) =>
					this._fetch(watch, true).then(() => watch)
				);
			})
			.then((requests) => Promise.all(requests))
			.then((watches) => {
				if (this.#authority.sequence !== generation) return;
				const failed = watches.some((watch) => {
					const state = this.#live.state(watch.key);
					/*
					 * Revalidation proves that the authoritative HTTP response
					 * refreshed the confirmed graph. A surviving accepted layer
					 * may deliberately keep the visible index stale when its row
					 * policy cannot be evaluated locally; the command runtime
					 * retires that layer only after this proof succeeds.
					 */
					const confirmed = this.#engine.readConfirmed((reader) =>
						materializeReplicaOperation(
							reader,
							watch.artifact,
							watch.variables
						)
					);
					return (
						state.errors.length > 0 ||
						!confirmed.complete ||
						confirmed.stale
					);
				});
				if (failed) {
					throw new Error(
						'authoritative command revalidation did not produce a complete result'
					);
				}
			});
	}

	invalidateAuthorization(): void {
		this.#purge();
	}

	dehydrate(): ReplicaDehydratedState {
		const scope = this.#authority.scope;
		if (scope === undefined) {
			throw new Error(
				'cannot dehydrate replica before the server establishes an authoritative scope'
			);
		}
		const reachable = reachableConfirmedState(
			this.#engine.extract(),
			this.#renderedOperations,
			(key) => this.#operations.group(key)
		);
		const payload: ReplicaDehydratedPayloadV1 = Object.freeze({
			cache: reachable.cache,
			operations: this.#operations.dehydrate(
				this.#renderedOperations.keys(),
				new Set(reachable.cache.indexes.map((index) => index.key)),
				reachable.clockRecordKeys
			),
			...this.#clocks.dehydrate(reachable.clockRecordKeys, reachable.models),
			trustedPresets: this.#authority.trustedPresets,
			nextIndexRevision: this.#operations.nextIndexRevision
		});
		return Object.freeze({
			version: 1 as const,
			scope: Object.freeze({
				protocolVersion: 1 as const,
				schemaHash: scope.schemaHash,
				authorizationGeneration: scope.authorizationGeneration,
				cacheScope: scope.cacheScope
			}),
			payload
		});
	}

	hydrate(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope
	): boolean {
		return this.#hydrate(state, authoritativeScope, 'merge');
	}

	reauthorize(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope
	): boolean {
		return this.#hydrate(state, authoritativeScope, 'reauthorize');
	}

	#hydrate(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope,
		mode: 'merge' | 'reauthorize'
	): boolean {
		const rejected = (
			reason:
				| 'invalid'
				| 'scope-mismatch'
				| 'artifact-mismatch'
				| 'active-scope-mismatch'
		): false => {
			if (this.#diagnostics.enabled) {
				this.#diagnostics.event(
					Object.freeze({ kind: 'hydration', action: 'rejected', reason })
				);
			}
			return false;
		};
		let parsed: ParsedReplicaHydration;
		let expectedScope: ProtocolGeneration;
		try {
			// Parsed whole, cache included, before any transport closes or any live
			// state changes.
			parsed = hydrationTables(parseDehydratedState(state));
			expectedScope = parseAuthoritativeScope(authoritativeScope);
		} catch {
			return rejected('invalid');
		}
		if (
			parsed.scope.protocolVersion !== expectedScope.protocolVersion ||
			parsed.scope.schemaHash !== expectedScope.schemaHash ||
			parsed.scope.authorizationGeneration !==
				expectedScope.authorizationGeneration ||
			parsed.scope.cacheScope !== expectedScope.cacheScope
		) {
			return rejected('scope-mismatch');
		}
		const rejection = this.#authority.hydrationRejection(
			parsed.scope,
			parsed.trustedPresets,
			mode
		);
		if (rejection !== undefined) return rejected(rejection);
		const preserveLocalCommandState = this.#authority.scope !== undefined;

		if (mode === 'reauthorize') {
			// The independent scope authorizes continued use of the current replica.
			// The accompanying snapshot may predate an intervening command: do not
			// merge its records, clocks, indexes, or freshness evidence into that state.
			this.#live.closeTransports();
			this.#live.clearStates();
			this.#live.resumeLive();
			this.#live.refreshWatches();
			this.#diagnostics.sync();
			return true;
		}

		if (preserveLocalCommandState) {
			// Warm same-scope re-hydrate (soft nav / second SSR seed): keep confirmed
			// records and indexes the route seed omitted. Seed keys upsert; purge only
			// happens on auth/scope change (closeAuthorizationGeneration path).
			const retainedIndexes = this.#operations.merge(parsed);
			const retainedRecords = this.#clocks.merge(parsed);
			this.#authority.hydrate(parsed.scope, parsed.trustedPresets);
			this.#operations.advanceIndexRevisionTo(parsed.nextIndexRevision);
			this.#engine.mergeConfirmed({
				...parsed.cache,
				records: parsed.cache.records.filter(
					(record) => !retainedRecords.has(record.key)
				),
				indexes: parsed.cache.indexes.filter(
					(index) => !retainedIndexes.has(index.key)
				)
			});
		} else {
			this.#closeAuthorizationGeneration();
			this.#live.clearStates();
			this.#operations.restore(parsed);
			this.#clocks.restore(parsed);
			this.#optimistic.clearReceipts();
			this.#diagnostics.clearLayers();
			this.#authority.hydrate(parsed.scope, parsed.trustedPresets);
			this.#engine.restore(parsed.cache);
		}
		this.#live.resumeLive();
		this.#diagnostics.sync();
		if (this.#diagnostics.enabled) {
			this.#diagnostics.event(
				Object.freeze({
					kind: 'hydration',
					action: 'accepted',
					reason: preserveLocalCommandState ? 'same-scope-merge' : 'accepted'
				})
			);
			this.#diagnostics.event(
				Object.freeze({
					kind: 'scope',
					action: 'established',
					generation: this.#authority.sequence,
					schemaHash: parsed.scope.schemaHash
				})
			);
		}
		return true;
	}

	/**
	 * Empty the replica: end the authorization generation and drop every
	 * protocol, clock, receipt and cached row it established. Watches and
	 * retained operations outlive it; their index plans are registered again.
	 */
	#purge(): void {
		this.#closeAuthorizationGeneration();
		this.#live.clearStates();
		this.#operations.clear();
		this.#clocks.clear();
		this.#optimistic.clear();
		this.#diagnostics.clearLayers();
		this.#indexMaintenance.clear();
		this.#indexPlanRegistrations.clear();
		this.#authority.reset();
		this.#engine.restore(EMPTY_CACHE_SNAPSHOT);
		this.#diagnostics.sync();
		this.#restoreIndexPlans();
		this.#live.refreshWatches();
		if (this.#diagnostics.enabled) {
			this.#diagnostics.event(
				Object.freeze({
					kind: 'scope',
					action: 'invalidated',
					generation: this.#authority.sequence
				})
			);
		}
	}

	#closeAuthorizationGeneration(): void {
		this.#authority.close();
		this.#live.closeTransports();
	}

	#writeCanonicalResult<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		stableVariables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource,
		requestRevision?: string,
		responseProjectionGeneration?: number
	): DistributedProtocolEnvelope {
		const extensions = parseGraphqlResponseExtensions(envelope.extensions);
		const parsedEnvelope: ReplicaResultEnvelope<TData> = Object.freeze({
			...envelope,
			...(extensions === undefined ? {} : { extensions })
		});
		const key = operationKey(artifact, stableVariables);
		const distributed = extensions?.distributed;
		if (distributed === undefined) {
			protocolInvalid('extensions.distributed');
		}
		const accepted = this.#writeProtocolResult(
			key,
			artifact,
			stableVariables,
			parsedEnvelope,
			source,
			distributed,
			requestRevision,
			responseProjectionGeneration
		);
		if (accepted) this.#notifyResultObservers(parsedEnvelope);
		return distributed;
	}

	#writeProtocolResult<TData, TVariables extends GraphqlVariables>(
		key: string,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		stableVariables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource,
		distributed: DistributedProtocolEnvelope,
		requestRevision: string | undefined,
		responseProjectionGeneration: number | undefined
	): boolean {
		const previousScope = this.#authority.scope;
		const staged = this.#authority.stage(artifact, distributed, source);

		const snapshot = distributed.snapshot;
		const live = distributed.live;
		if (live !== undefined && snapshot === undefined) {
			protocolInvalid('extensions.distributed.snapshot');
		}
		// A failed live execution has no result: an error-only frame with only
		// the base envelope. It is receipt-only, like an HTTP error response,
		// and must not carry a command receipt or create stream state.
		const liveFailure =
			source === 'live' &&
			snapshot === undefined &&
			live === undefined &&
			isGraphqlFailurePayload(envelope);
		if (liveFailure && distributed.command !== undefined) {
			protocolInvalid('extensions.distributed.command');
		}
		if (
			source === 'live' &&
			!liveFailure &&
			(envelope.data !== undefined || envelope.errors !== undefined) &&
			(snapshot === undefined || live === undefined)
		) {
			protocolInvalid('extensions.distributed.live');
		}

		const operation = distributed.operation;
		if ((snapshot !== undefined || live !== undefined) && operation === undefined) {
			protocolInvalid('extensions.distributed.operation');
		}
		const operationSource = protocolOperationSource(source);
		const operationState =
			operation === undefined || liveFailure
				? undefined
				: this.#operations.state(key, operation, operationSource);
		if (snapshot === undefined || operationState === undefined) {
			this.#optimistic.applyReceiptOnly(distributed.command);
			this.#authority.commit(staged);
			this.#live.resumeLive();
			this.#diagnostics.scopeTransition(previousScope, staged.scope);
			this.#diagnostics.sync();
			return true;
		}

		this.#validateLiveSnapshot(snapshot, live);
		const snapshotLive = source === 'live' && live?.mode === 'snapshot';
		const reset = live?.reset === true;
		const group = this.#operations.group(key)!;
		const previousActiveSource = group.active;
		const handoff =
			previousActiveSource !== undefined &&
			previousActiveSource !== operationSource;
		const activeState =
			previousActiveSource === undefined
				? undefined
				: group[previousActiveSource];
		const ownDisposition = reset
			? 'fresh'
			: compareSnapshotToOperationState(operationState, snapshot);
		const activeDisposition =
			handoff && activeState !== undefined
				? compareSnapshotToOperationState(activeState, snapshot)
				: 'fresh';
		const incomingMemberships: ReplicaIndexMemberships = new Map();
		const fencedSharedDisposition = this.#operations.sharedIndexDisposition(
			key,
			replicaResultIndexKeys(
				artifact,
				stableVariables,
				envelope,
				snapshot,
				incomingMemberships
			),
			snapshot,
			requestRevision,
			source,
			source === 'live' && !snapshot.indexesComparable
				? this.#live.liveStartRevision(key)
				: undefined
		);
		/*
		 * A snapshot frame that agrees with an independent owner on every
		 * index they share is exactly its server result once admitted: the
		 * owner keeps those indexes and this frame writes only the rest
		 * (docs/live-query-delivery.md). Disagreement keeps the atomic fence.
		 */
		const fencedIndexKeys =
			snapshotLive ? fencedSharedDisposition.fencedIndexKeys : undefined;
		const sharedMembershipAdmitted =
			fencedIndexKeys !== undefined &&
			(envelope.errors ?? []).length === 0 &&
			this.#fencedMembershipsMatch(fencedIndexKeys, incomingMemberships);
		const sharedDisposition: SharedIndexDisposition = sharedMembershipAdmitted
			? { compared: false }
			: fencedSharedDisposition;
		// A non-resumable stream has no causal vector to compare. An HTTP
		// refresh may replace it only if the request began after its last
		// accepted membership. fetchWatch also fences intervening live frames.
		// Taking query ownership restarts the stream, fencing queued callbacks.
		const snapshotRefresh =
			source === 'network' &&
			previousActiveSource === 'live' &&
			!snapshot.indexesComparable &&
			activeState?.snapshotScope === undefined &&
			activeState?.indexRevision !== undefined &&
			requestRevision !== undefined &&
			compareDistributedDecimal(requestRevision, activeState.indexRevision) > 0;
		const handoffBlocked =
			handoff &&
			!snapshotLive &&
			!snapshotRefresh &&
			(
				!snapshot.indexesComparable ||
				!isComparableHandoffDisposition(ownDisposition) ||
				!isComparableHandoffDisposition(activeDisposition)
			);
		let disposition: IndexDisposition = handoffBlocked
			? !isComparableHandoffDisposition(activeDisposition)
				? activeDisposition
				: !isComparableHandoffDisposition(ownDisposition)
					? ownDisposition
					: 'incomparable'
			: ownDisposition;
		if (
			!handoffBlocked &&
			isComparableHandoffDisposition(disposition) &&
			sharedDisposition.compared
		) {
			disposition =
				sharedDisposition.disposition === 'lower'
					? 'lower'
					: sharedDisposition.disposition ?? disposition;
		}
		const sourceSwitched =
			!handoffBlocked &&
			isComparableHandoffDisposition(disposition) &&
			this.#operations.activate(
				key,
				operationSource,
				artifact,
				stableVariables
			);
		const rejectedHandoff = handoff && !sourceSwitched;
		if (
			snapshot.indexesComparable &&
			(reset || ownDisposition === 'incomparable')
		) {
			if (rejectedHandoff) {
				this.#operations.reset(operationState);
			} else {
				this.#operations.discardSnapshot(
					operationState,
					artifact,
					stableVariables
				);
			}
		}
		if (sourceSwitched && disposition !== 'incomparable') {
			// Query and live operation hashes describe independent protocol
			// streams. A handoff is authoritative, but must receive a fresh local
			// index revision rather than reusing the inactive stream's revision.
			disposition = 'fresh';
		}

		if (!snapshot.indexesComparable) {
			if (rejectedHandoff) {
				this.#operations.reset(operationState);
			} else {
				/*
				 * The server-authorized GraphQL payload is still an exact
				 * replacement result. Only its partition-wide causal vector is
				 * unavailable (most commonly because exposing that position
				 * would leak denied-row activity). Preserve local membership,
				 * while dropping every capability that requires comparison.
				 */
				this.#operations.resetCausal(operationState);
			}
			disposition = rejectedHandoff
				? 'incomparable'
				: sharedDisposition.disposition === 'lower'
					? 'lower'
					: 'fresh';
		} else if (disposition === 'incomparable') {
			if (rejectedHandoff) {
				this.#operations.reset(operationState);
			} else {
				this.#operations.discardSnapshot(
					operationState,
					artifact,
					stableVariables
				);
			}
		}

		const writeIndexes =
			!rejectedHandoff &&
			disposition !== 'lower' &&
			disposition !== 'incomparable';
		// Snapshot frames receive local membership revisions, not fabricated
		// projection clocks. Live ownership fences older overlapping HTTP work.
		const indexRevision =
			writeIndexes &&
			(sourceSwitched || sharedDisposition.disposition === 'higher')
				? this.#operations.allocateIndexRevision()
				: writeIndexes &&
					  sharedDisposition.disposition === 'equal' &&
					  sharedDisposition.indexRevision !== undefined
					? sharedDisposition.indexRevision
				: snapshot.indexesComparable &&
					  disposition === 'equal' &&
					  operationState.indexRevision !== undefined
					? operationState.indexRevision
					: (requestRevision ?? this.#operations.allocateIndexRevision());
		const recordEvidence = prepareRecordEvidence(
			snapshot,
			distributed.command?.records ?? Object.freeze([])
		);
		const admitted = this.#clocks.evidence();
		const pendingPathRecords = new Map<string, string>();
		const consumedRecordPaths = new Set<string>();
		const observationsAdmissible =
			snapshot.recordsComplete &&
				snapshot.indexesComparable &&
				disposition !== 'incomparable' &&
				(disposition !== 'lower' ||
					this.#operations.group(key)?.active === operationSource);
		const receiptPlan = this.#optimistic.planReceipts(
			distributed.command,
			[
				...(distributed.command?.observations ?? []),
				...(observationsAdmissible ? snapshot.observations : [])
			],
			observationsAdmissible
		);
		const normalizationProtocol: ReplicaNormalizationProtocol = {
			indexRevision,
			writeIndexes,
			/*
			 * This is cache-membership completeness, not causal-vector
			 * comparability. normalizeReplicaResult independently downgrades
			 * GraphQL path errors and missing selected fields.
			 */
			indexesComplete: true,
			allowSnapshotOnlyRecords: !snapshot.recordsComplete,
			record: (
				path,
				model,
				recordKey,
				fields
			): ReplicaProtocolRecordResolution | undefined => {
				const encodedPath = responsePathKey(path);
				const evidence = recordEvidence.byPath.get(encodedPath);
				if (evidence === undefined) {
					if (snapshot.recordsComplete) {
						protocolInvalid(
							'extensions.distributed.snapshot.records'
						);
					}
					return undefined;
				}
				if (evidence.model !== model) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.model'
					);
				}
				if (evidence.tombstone) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.tombstone'
					);
				}
				consumedRecordPaths.add(encodedPath);
				const apply = admitted.row(
					recordKey,
					evidence,
					fields,
					responseProjectionGeneration
				);
				pendingPathRecords.set(encodedPath, recordKey);
				return Object.freeze({ evidence, apply });
			}
		};

		const state = this.#live.state(key);
		const previousErrors = state.errors;
		state.errors = stableErrors(previousErrors, envelope.errors ?? []);
		let summary: ReturnType<typeof normalizeReplicaResult>;
		try {
			const update = (writer: BaseCacheWriter) => {
				const guarded = this.#optimistic.guardIndexWriter(
					writer,
					sharedMembershipAdmitted ? fencedIndexKeys : undefined
				);
				admitted.tombstones(
					guarded,
					recordEvidence.tombstones,
					operationState.pathRecords,
					consumedRecordPaths
				);
				const normalized = normalizeReplicaResult(
					guarded,
					artifact,
					stableVariables,
					envelope,
					normalizationProtocol
				);
				for (const path of recordEvidence.livePaths) {
					if (!consumedRecordPaths.has(path)) {
						protocolInvalid(
							'extensions.distributed.snapshot.records.path'
						);
					}
				}
				admitted.pathless(
					guarded,
					recordEvidence.pathless,
					recordEvidence.byPath,
					consumedRecordPaths
				);
				return normalized;
			};
			summary =
				receiptPlan.satisfied.length === 0
					? this.#engine.batch(update)
					: this.#engine.confirmOptimisticLayers(
							receiptPlan.satisfied,
							update
						);
		} catch (error) {
			state.errors = previousErrors;
			if (
				error instanceof CacheRevisionConflictError ||
				error instanceof DistributedProtocolError
			) {
				this.#operations.discardSnapshot(
					operationState,
					artifact,
					stableVariables
				);
			}
			this.#live.emit(key, false);
			throw error;
		}

		admitted.commit();
		this.#operations.rememberPaths(operationState, pendingPathRecords);
		this.#optimistic.commitReceipts(receiptPlan);
		this.#operations.commitSnapshot(operationState, {
			writeIndexes,
			indexRevision,
			indexKeys: summary.indexKeys,
			snapshot,
			live
		});
		if (source === 'live') {
			this.#operations.advance(key);
		}
		if (source !== 'live' && sourceSwitched) {
			this.#live.restartLive(key);
		}
		if (source === 'live') {
			this.#live.fenceLive(
				key,
				fencedIndexKeys !== undefined && !sharedMembershipAdmitted
					? fencedIndexKeys
					: undefined
			);
		}
		if (writeIndexes) {
			this.#live.reopenFencedBy(
				key,
				summary.indexKeys.filter(
					(indexKey) =>
						!(sharedMembershipAdmitted && fencedIndexKeys!.has(indexKey))
				)
			);
		}
		if (source === 'live' && sharedDisposition.restartAfterRetirement) {
			// This receiver began before a shared owner retired. Its buffered
			// frame stays fenced, but a fresh receiver starts after that boundary
			// and can obtain an authoritative replacement without polling.
			this.#live.restartLive(key);
		}
		this.#authority.commit(staged);
		this.#live.resumeLive();
		this.#live.emit(key, false);
		this.#optimistic.flushDeferredConfirms();
		if (this.#diagnostics.enabled) {
			this.#diagnostics.event(
				Object.freeze({
					kind: 'normalization',
					operation: artifact.id,
					source,
					records: this.#engine.recordCount,
					indexes: summary.indexKeys.length,
					partial:
						summary.partial ||
						disposition === 'lower' ||
						disposition === 'incomparable' ||
						rejectedHandoff
				})
			);
			for (const indexKey of summary.indexKeys) {
				const staleReason = this.#engine.readConfirmed(
					(confirmed) => confirmed.index(indexKey)?.metadata?.staleReason
				);
				this.#diagnostics.event(
					Object.freeze({
						kind: 'index-decision',
						index: indexKey,
						decision:
							!writeIndexes ||
							!snapshot.indexesComparable ||
							disposition === 'incomparable'
								? ('revalidate' as const)
								: staleReason === undefined
									? ('maintained' as const)
									: ('stale' as const),
						...(staleReason === undefined
							? {}
							: { reason: staleReason })
					})
				);
			}
			this.#diagnostics.scopeTransition(previousScope, staged.scope);
			this.#diagnostics.sync();
		}
		if (disposition === 'incomparable' || rejectedHandoff) return false;
		if (disposition !== 'lower') return true;
		// A lower index snapshot from the already-active source cannot replace
		// membership, but it can still carry independently fenced record clocks
		// and exact causal observations. Notify only when that transaction
		// actually committed data or receipt progress.
		return summary.wrote || receiptPlan.satisfied.length > 0;
	}

	#notifyResultObservers(envelope: ReplicaResultEnvelope<unknown>): void {
		if (this.#resultObservers.size === 0) return;
		const errors: unknown[] = [];
		for (const observer of [...this.#resultObservers]) {
			try {
				observer(envelope);
			} catch (error) {
				errors.push(error);
			}
		}
		this._reportObserverErrors(errors);
	}

	createOptimisticLayer(
		id: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[] = Object.freeze([])
	): void {
		this.#optimistic.create(id, update, semanticChanges);
	}

	markOptimisticLayerAccepted(
		id: string,
		receipt?: DistributedCommandMetadata
	): boolean {
		return this.#optimistic.markAccepted(id, receipt);
	}

	confirmOptimisticLayer<T>(
		id: string,
		update: (writer: ReplicaBaseWriter) => T
	): T {
		return this.#optimistic.confirm(id, update);
	}

	rejectOptimisticLayer(id: string): boolean {
		return this.#optimistic.reject(id);
	}

	#fencedMembershipsMatch(
		fencedIndexKeys: ReadonlySet<string>,
		incoming: ReplicaIndexMemberships
	): boolean {
		return this.#engine.readConfirmed((reader) => {
			for (const indexKey of fencedIndexKeys) {
				const membership = incoming.get(indexKey);
				const current = reader.index(indexKey);
				if (
					membership === undefined ||
					membership === null ||
					current === undefined ||
					!current.complete ||
					(current.staleRevision !== undefined &&
						compareDistributedDecimal(current.staleRevision, current.revision) > 0) ||
					(current.metadata?.nullValue === true) !== membership.nullValue ||
					current.records.length !== membership.records.length ||
					current.records.some(
						(record, ordinal) => record !== membership.records[ordinal]
					)
				) {
					return false;
				}
			}
			return true;
		});
	}

	tombstoneRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		revision: ReplicaRevision
	): boolean {
		const wrote = this.#engine.batch((writer) =>
			writer.tombstoneRecord(replicaRecordKey(model, identity), revision)
		);
		if (wrote) this.#diagnostics.sync();
		return wrote;
	}

	markIndexStale(target: ReplicaIndexTarget, reason: string): boolean {
		const key = indexKeyFromTarget(target);
		const marked = this.#engine.batch((writer) =>
			writer.markIndexStale(key, reason)
		);
		if (marked) {
			if (this.#diagnostics.enabled) {
				this.#diagnostics.event(
					Object.freeze({
						kind: 'index-decision',
						index: key,
						decision: 'stale',
						reason
					})
				);
			}
			this.#diagnostics.sync();
		}
		return marked;
	}

	inspectRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity
	): ReplicaRecordInspection | undefined {
		const key = replicaRecordKey(model, identity);
		return this.#engine.read((reader) => {
			const record = reader.record(key);
			if (!record) return undefined;
			return Object.freeze({
				key,
				revision: record.revision,
				incarnation: record.incarnation,
				presentFields: Object.freeze(Object.keys(record.fields).sort())
			});
		});
	}

	/**
	 * Package-private live fields for pure-reduce optimism.
	 * @internal
	 */
	[replicaCommandReadRecord](
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity
	): Readonly<Record<string, ReplicaValue>> | undefined {
		const key = replicaRecordKey(model, identity);
		return this.#engine.read((reader) => {
			const record = reader.record(key);
			if (!record) return undefined;
			return Object.freeze({ ...record.fields });
		});
	}

	inspectIndex(target: ReplicaIndexTarget): ReplicaIndexInspection | undefined {
		const key = indexKeyFromTarget(target);
		return this.#engine.read((reader) => {
			const index = reader.index(key);
			if (!index?.metadata) return undefined;
			return Object.freeze({
				key,
				revision: index.revision,
				...(index.staleRevision === undefined
					? {}
					: { staleRevision: index.staleRevision }),
				records: Object.freeze([...index.records]),
				complete: index.complete,
				field: index.metadata.field,
				...(index.metadata.parent === undefined
					? {}
					: { parent: index.metadata.parent }),
				arguments: index.metadata.arguments,
				coverage: index.metadata.coverage,
				dependencies: index.metadata.dependencies,
				...(index.metadata.staleReason === undefined
					? {}
					: { staleReason: index.metadata.staleReason }),
				nullValue: index.metadata.nullValue === true
			});
		});
	}

	#rememberRenderedOperation<TData, TVariables extends GraphqlVariables>(
		key: string,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): void {
		this.#registerIndexPlan(key, artifact, variables);
		this.#renderedOperations.set(
			key,
			Object.freeze({
				artifact: artifact as ReplicaOperationArtifact<unknown, GraphqlVariables>,
				variables
			})
		);
		this.#renderCounts.set(key, (this.#renderCounts.get(key) ?? 0) + 1);
		this.#retired.delete(key);
	}

	#registerIndexPlan<TData, TVariables extends GraphqlVariables>(
		key: string,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): void {
		this.#diagnostics.operation(artifact);
		if (this.#indexPlanRegistrations.has(key)) return;
		const registration = this.#indexMaintenance.registerOperation(
			artifact,
			variables
		);
		this.#indexPlanRegistrations.set(key, registration);
		try {
			this.#refreshIndexMaintenance();
		} catch (error) {
			this.#indexPlanRegistrations.delete(key);
			registration.dispose();
			throw error;
		}
	}

	#restoreIndexPlans(): void {
		for (const [key, rendered] of this.#renderedOperations) {
			this.#registerIndexPlan(key, rendered.artifact, rendered.variables);
		}
	}

	#forgetRenderedOperation(key: string): void {
		const count = this.#renderCounts.get(key);
		if (count === undefined) return;
		if (count > 1) {
			this.#renderCounts.set(key, count - 1);
			return;
		}
		this.#renderCounts.delete(key);
		const rendered = this.#renderedOperations.get(key);
		this.#renderedOperations.delete(key);
		this.#disposeIndexPlan(key);
		if (rendered !== undefined) this.#retireOperation(key, rendered);
	}

	/**
	 * The last watch or retained handle of an operation left. Its protocol
	 * state and cached result stay for a grace period, so a remount finds
	 * them; then they are collected.
	 */
	#retireOperation(key: string, rendered: RenderedOperation): void {
		if (!Number.isFinite(this.#retentionGraceMs)) return;
		this.#retired.set(key, {
			...rendered,
			due: this.#scheduler.now() + this.#retentionGraceMs,
			written: this.#operations.written(key)
		});
		this.#scheduleCollection();
	}

	#scheduleCollection(): void {
		// Every due time is "now + grace", so a pending timer is the earliest.
		if (this.#retentionTimer !== undefined || this.#retired.size === 0) return;
		let due = Infinity;
		for (const retired of this.#retired.values()) due = Math.min(due, retired.due);
		this.#retentionTimer = this.#scheduler.setTimeout(
			() => {
				this.#retentionTimer = undefined;
				this.#collectRetiredOperations();
			},
			Math.max(0, due - this.#scheduler.now())
		);
		unrefTimer(this.#retentionTimer);
	}

	/**
	 * Drop every retired operation whose grace period ended: its protocol and
	 * query state, its in-flight request, and the indexes only it owned. Then
	 * collect the records no remaining index, optimistic layer or retained
	 * record reaches, with their clocks, scopes and projected fences.
	 *
	 * A result written for the operation during the grace period (a route
	 * seed for a page about to mount, a late response) restarts the period
	 * instead: something still produces this operation.
	 */
	#collectRetiredOperations(): void {
		const now = this.#scheduler.now();
		let dropped = false;
		for (const [key, retired] of this.#retired) {
			if (retired.due > now) continue;
			const written = this.#operations.written(key);
			if (
				written.group !== retired.written.group ||
				written.query !== retired.written.query ||
				written.live !== retired.written.live
			) {
				retired.written = written;
				retired.due = now + this.#retentionGraceMs;
				continue;
			}
			this.#retired.delete(key);
			this.#live.forget(key);
			for (const state of [written.group?.query, written.group?.live]) {
				if (state !== undefined) {
					this.#operations.discardSnapshot(
						state,
						retired.artifact,
						retired.variables
					);
				}
			}
			this.#operations.delete(key);
			dropped = true;
		}
		if (dropped) {
			const collected = this.#engine.gc();
			if (collected.length > 0) this.#clocks.forget(collected);
			if (this.#diagnostics.enabled) {
				this.#diagnostics.event(
					Object.freeze({ kind: 'gc', records: collected.length })
				);
			}
			this.#diagnostics.sync();
		}
		this.#scheduleCollection();
	}

	#disposeIndexPlan(key: string): void {
		const registration = this.#indexPlanRegistrations.get(key);
		if (registration === undefined) return;
		this.#indexPlanRegistrations.delete(key);
		registration.dispose();
		this.#refreshIndexMaintenance();
	}

	#refreshIndexMaintenance(): void {
		this.#engine.setDerivedIndexReconciler(this.#derivedIndexReconciler);
	}

	#deriveMaintainedIndexes(
		confirmed: CacheReader,
		layers: readonly OptimisticLayerView[]
	): readonly DerivedIndexMutation[] {
		const snapshot = indexMaintenanceSnapshot(confirmed);
		const semanticLayers = layers.map(indexSemanticLayer);
		const mutations: DerivedIndexMutation[] = [];
		for (const decision of this.#indexMaintenance.evaluate(
			snapshot,
			semanticLayers,
			this.#authority.trustedPresets
		)) {
			if (decision.kind === 'unchanged') continue;
			if (decision.kind === 'stale') {
				mutations.push({
					kind: 'stale',
					key: decision.indexKey,
					reason: formatReplicaIndexStaleReason(decision.reason)
				});
				continue;
			}
			const index = confirmed.index(decision.indexKey);
			if (index === undefined) {
				throw new Error(
					`index maintenance returned an unknown index: ${decision.indexKey}`
				);
			}
			mutations.push({
				kind: 'write',
				write: {
					key: decision.indexKey,
					records: decision.records,
					complete: true,
					metadata: index.metadata
				}
			});
		}
		return Object.freeze(mutations);
	}

	#directProjectionIndexMutations(
		commandId: string,
		model: ReplicaModelArtifact,
		recordKey: string,
		fields: Readonly<Record<string, ReplicaValue>>
	): readonly DerivedIndexMutation[] {
		const change: ReplicaIndexSemanticChange = Object.freeze({
			kind: 'upsert',
			model: model.id,
			key: recordKey,
			fields
		});
		const layer: OptimisticLayerView = Object.freeze({
			id: commandId,
			sequence: Number.MAX_SAFE_INTEGER,
			state: 'accepted',
			context: Object.freeze({
				id: commandId,
				changes: Object.freeze([change])
			})
		});
		return this.#engine.readConfirmed((confirmed) =>
			this.#deriveMaintainedIndexes(confirmed, [layer])
		);
	}

	#validateLiveSnapshot(
		snapshot: DistributedQuerySnapshot,
		live: DistributedProtocolEnvelope['live']
	): void {
		if (live === undefined || live.mode === 'snapshot') return;
		if (!snapshot.indexesComparable) {
			protocolInvalid(
				'extensions.distributed.snapshot.indexesComparable'
			);
		}
		const indexes = new Map(
			snapshot.indexes.map((index) => [index.projection, index])
		);
		if (indexes.size !== live.cursors.length) {
			protocolInvalid('extensions.distributed.live.cursors');
		}
		for (const cursor of live.cursors) {
			const index = indexes.get(cursor.projection);
			if (
				index === undefined ||
				index.position !== cursor.position ||
				(index.resume !== undefined &&
					index.resume.token !== cursor.token)
			) {
				protocolInvalid('extensions.distributed.live.cursors');
			}
		}
	}

	/** Package-internal hook used by one watched operation. */
	_register<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>
	): () => void {
		const watched = watch as ReplicaWatchState<unknown, GraphqlVariables>;
		this.#rememberRenderedOperation(watch.key, watch.artifact, watch.variables);
		this.#live.addWatch(watched);
		const unwatch = this.#engine.watch(
			(reader) => materializeReplicaOperation(reader, watch.artifact, watch.variables),
			(materialized) => watch._cacheChanged(materialized),
			{ immediate: true }
		);
		if (watch.liveRequested) this.#live.retainLive(watch);
		void this._fetch(watch, false);
		return () => {
			unwatch();
			this.#live.removeWatch(watched);
			this.#forgetRenderedOperation(watch.key);
			if (watch.liveRequested) this.#live.releaseLive(watch.key);
		};
	}

	/**
	 * Package-internal: how many entries each per-operation and per-record
	 * structure holds. The retention test compares it with a baseline.
	 */
	_sizes(): Readonly<Record<string, number>> {
		const cache = this.#engine.extract();
		return Object.freeze({
			records: cache.records.length,
			indexes: cache.indexes.length,
			...this.#live.sizes(),
			...this.#operations.sizes(),
			...this.#clocks.sizes(),
			...this.#optimistic.sizes(),
			renderedOperations: this.#renderedOperations.size,
			renderCounts: this.#renderCounts.size,
			retired: this.#retired.size,
			indexPlans: this.#indexPlanRegistrations.size
		});
	}

	/** Package-internal query-state lookup. */
	_state(key: string): QueryState {
		return this.#live.state(key);
	}

	/** Package-internal materialization for watch construction. */
	_materialize<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): MaterializedReplicaResult<TData> {
		return this.#engine.read((reader) =>
			materializeReplicaOperation(reader, artifact, variables)
		);
	}

	/** Package-internal cache-and-live coordinator. */
	_fetch<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>,
		force: boolean
	): Promise<void> {
		return this.#live.fetch(watch, force);
	}

	_reportObserverErrors(errors: unknown[]): void {
		if (errors.length === 0) return;
		reportSafely(
			this.#reportObserverError,
			new AggregateError(errors, 'replica observer delivery failed')
		);
	}
}
