import { replicaCommandFreshness } from '../command-runtime/symbols.js';
import {
	CacheRevisionConflictError,
	createCacheEngine,
	type BaseCacheWriter,
	type CacheEngine,
	type CacheEngineSnapshot,
	type DerivedIndexMutation,
	type DerivedIndexReconciler,
	type OptimisticLayerView
} from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import {
	compareDistributedDecimal,
	DistributedProtocolError,
	parseGraphqlResponseExtensions,
	type DistributedCommandMetadata,
	type DistributedLiveCursor,
	type DistributedOpaqueString,
	type DistributedProjectionObservation,
	type DistributedProtocolEnvelope,
	type DistributedQuerySnapshot,
	type DistributedRecordRevision,
	type DistributedTrustedPreset
} from '../../protocol.js';
import {
	matchReplicaTrustedPresetInventory
} from '../commands.js';
import type {
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticLayerInput,
	ReplicaDiagnosticsSink
} from '../diagnostics.js';
import {
	replicaCommandAuthority,
	replicaCommandDirectProjection,
	replicaCommandProjectionDelta,
	replicaCommandReadRecord,
	replicaResultObservation,
	type ReplicaCommandAuthorityRegistration,
	type ReplicaCommandAuthoritySnapshot,
	type ReplicaCommandDirectProjection,
	type ReplicaCommandSurfaceContract,
	type ReplicaResultObservationRegistration
} from '../command-runtime.js';
import {
	canonicalizeOperationVariables,
	replicaIndexKey,
	replicaRecordKey,
	resolveArguments
} from '../identity.js';
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
import {
	validateReplicaOperationBinding as validatedArtifactBinding
} from '../operation-binding.js';
import {
	createReplicaIndexMaintenanceRegistry,
	formatReplicaIndexStaleReason,
	type ReplicaIndexPlanRegistration,
	type ReplicaIndexSemanticChange
} from '../index-maintenance.js';
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
	ReplicaRecordPatch,
	ReplicaRevalidationPlan,
	ReplicaRevision,
	ReplicaResultEnvelope,
	ReplicaSnapshot,
	ReplicaTransport,
	ReplicaValue,
	ReplicaWatch,
	ReplicaWriteSource,
	WatchReplicaOptions
} from '../types.js';
import {
	EMPTY_ERRORS,
	EMPTY_TRUSTED_PRESETS,
	MAX_ANONYMOUS_RECORD_CLOCKS
} from './constants.js';
import type {
	AnonymousRecordProtocolClock,
	IndexDisposition,
	LiveEntry,
	OperationProtocolGroup,
	OperationProtocolSource,
	OperationProtocolState,
	OptimisticReceiptState,
	ProjectedRecordFence,
	ProtocolGeneration,
	QueryState,
	RecordProtocolClock,
	RegisteredCommandAuthorityContract,
	RenderedOperation,
	ReplicaArtifactBinding,
	SharedIndexDisposition,
	ValidatedArtifactBinding
} from './types.js';
import {
	compareCanonicalDecimalStrings,
	compareEvidenceToProjectedFence,
	compareIndexVector,
	compareProjectedRecordFields,
	compareRecordClock,
	compareSnapshotToOperationState,
	incrementCanonicalDecimal,
	indexClockMap,
	isComparableHandoffDisposition,
	latestCursors,
	protocolInvalid,
	recordKeyMatchesModel,
	modelFromRecordKey,
	responsePathKey,
	sameRecordClock,
	sameRecordRevision
} from './clocks.js';
import { diagnosticReceiptCounts } from './optimistic.js';
import {
	assertWriteSource,
	baseWriter,
	indexKeyFromTarget,
	indexMaintenanceSnapshot,
	indexSemanticLayer,
	operationKey,
	prepareRecordEvidence,
	protocolOperationSource,
	replicaResultIndexKeys,
	reportSafely,
	reportUnhandledObserverError,
	snapshotFrom,
	stableErrors,
	trustedPresetDescriptorFingerprint,
	validatedCommandAuthorityContract
} from './helpers.js';
import { ReplicaWatchState } from './watch.js';
import {
	closeActiveTransports as closeActiveTransportsOn,
	emitWatchState,
	fetchWatch,
	releaseLive as releaseLiveOn,
	restartLive as restartLiveOn,
	retainLive as retainLiveOn,
	resumeLiveWatches as resumeLiveWatchesOn,
	type FetchLiveHost
} from './impl-fetch-live.js';
import {
	closeAuthorizationGeneration as closeAuthorizationGenerationOn,
	purgeProtocolGeneration as purgeProtocolGenerationOn,
	stageProtocolGeneration as stageProtocolGenerationOn,
	stageTrustedPresets as stageTrustedPresetsOn,
	validateProtocolBinding as validateProtocolBindingOn,
	type ProtocolHost
} from './impl-protocol.js';
import {
	applyReceiptOnly as applyReceiptOnlyOn,
	confirmOptimisticLayerOn,
	confirmOptimisticLayerWithCacheWriterOn,
	createOptimisticLayerOn,
	markOptimisticLayerAcceptedOn,
	planOptimisticReceipts as planOptimisticReceiptsOn,
	rejectOptimisticLayerOn,
	replaceReplicaOptimisticLayerOn,
	type OptimisticHost
} from './impl-optimistic.js';
import {
	dehydrateReplica,
	finishDehydration as finishDehydrationOn,
	hydrateReplica,
	type HydrationHost
} from './impl-hydration-orchestrate.js';
import {
	diagnosticEvent as diagnosticEventOn,
	diagnosticOperation as diagnosticOperationOn,
	diagnosticScopeTransition as diagnosticScopeTransitionOn,
	retireDiagnosticLayer as retireDiagnosticLayerOn,
	syncDiagnostics as syncDiagnosticsOn,
	type DiagnosticsHost
} from './impl-diagnostics.js';

export class DistributedReplicaImpl implements DistributedReplicaApi {
	readonly #engine: CacheEngine;
	readonly #transport: ReplicaTransport | undefined;
	readonly #reportObserverError: (error: AggregateError) => void;
	readonly #onAuthorizationGenerationDispose: (() => void) | undefined;
	readonly #diagnostics: ReplicaDiagnosticsSink | undefined;
	readonly #diagnosticLayers:
		| Map<string, ReplicaDiagnosticLayerInput>
		| undefined;
	readonly #inFlight = new Map<string, Promise<void>>();
	readonly #inFlightAborts = new Map<string, AbortController>();
	readonly #queryStates = new Map<string, QueryState>();
	readonly #watches = new Map<string, Set<ReplicaWatchState<unknown, GraphqlVariables>>>();
	readonly #lives = new Map<string, LiveEntry>();
	readonly #operationProtocols = new Map<string, OperationProtocolGroup>();
	readonly #operationGenerations = new Map<string, number>();
	readonly #recordClocks = new Map<string, RecordProtocolClock>();
	readonly #recordKeysByScope = new Map<DistributedOpaqueString, string>();
	readonly #projectedRecordFences = new Map<string, ProjectedRecordFence>();
	/**
	 * Record keys inserted by Eventual projection-delta. A later complete
	 * query/live index that omits them is behind command confirmation and
	 * must not shrink the visible list. Atomic rows use projected-record
	 * fences only — they have no @live that would otherwise clear this map.
	 */
	readonly #membershipFences = new Map<
		string,
		Map<string, Set<string>>
	>();
	readonly #deferredMembershipConfirms = new Set<string>();
	readonly #anonymousRecordClocks = new Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>();
	readonly #freshnessPlans = new Map<string, { generation: number; plan: ReplicaRevalidationPlan }>();
	readonly #optimisticReceipts = new Map<string, OptimisticReceiptState>();
	readonly #renderedOperations = new Map<string, RenderedOperation>();
	readonly #readOperationKeys = new Set<string>();
	readonly #watchRenderCounts = new Map<string, number>();
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
	#commandAuthorityContract: RegisteredCommandAuthorityContract | undefined;
	#trustedPresets: readonly DistributedTrustedPreset[] = EMPTY_TRUSTED_PRESETS;
	#authorizationAbort = new AbortController();
	#artifactBinding: ReplicaArtifactBinding | undefined;
	#protocolGeneration: ProtocolGeneration | undefined;
	#protocolGenerationSequence = 0;
	#projectionGeneration = 0;
	#nextIndexRevision = '0';
	#diagnosticLayerSequence = 0;

	#fetchLiveHostCache: FetchLiveHost | undefined;
	#protocolHostCache: ProtocolHost | undefined;
	#optimisticHostCache: OptimisticHost | undefined;
	#hydrationHostCache: HydrationHost | undefined;
	#diagnosticsHostCache: DiagnosticsHost | undefined;

	constructor(options: DistributedReplicaOptions = {}) {
		this.#transport = options.transport;
		this.#reportObserverError = options.onObserverError ?? reportUnhandledObserverError;
		this.#onAuthorizationGenerationDispose =
			options.onAuthorizationGenerationDispose;
		this.#diagnostics = options.diagnostics;
		this.#diagnosticLayers =
			options.diagnostics === undefined ? undefined : new Map();
		this.#engine = createCacheEngine({ onWatcherError: this.#reportObserverError });
		this.#engine.setDerivedIndexReconciler(this.#derivedIndexReconciler);
		this.#syncDiagnostics();
	}

	#diagnosticsEnabled(): { readonly enabled: boolean } {
		return Object.freeze({ enabled: this.#diagnostics !== undefined });
	}

	#diagnosticsHost(): DiagnosticsHost {
		if (this.#diagnosticsHostCache !== undefined) return this.#diagnosticsHostCache;
		const self = this;
		this.#diagnosticsHostCache = {
			get engine() {
				return self.#engine;
			},
			get diagnostics() {
				return self.#diagnostics;
			},
			get diagnosticLayers() {
				return self.#diagnosticLayers;
			},
			get optimisticReceipts() {
				return self.#optimisticReceipts;
			},
			get reportObserverError() {
				return self.#reportObserverError;
			},
			getProtocolGeneration: () => self.#protocolGeneration,
			getProtocolGenerationSequence: () => self.#protocolGenerationSequence
		};
		return this.#diagnosticsHostCache;
	}

	#fetchLiveHost(): FetchLiveHost {
		if (this.#fetchLiveHostCache !== undefined) return this.#fetchLiveHostCache;
		const self = this;
		this.#fetchLiveHostCache = {
			get transport() {
				return self.#transport;
			},
			get inFlight() {
				return self.#inFlight;
			},
			get inFlightAborts() {
				return self.#inFlightAborts;
			},
			get lives() {
				return self.#lives;
			},
			get watches() {
				return self.#watches;
			},
			get operationProtocols() {
				return self.#operationProtocols;
			},
			get diagnostics() {
				return self.#diagnosticsEnabled();
			},
			protocolGenerationSequence: () => self.#protocolGenerationSequence,
			projectionGeneration: () => self.#projectionGeneration,
			protocolGeneration: () => self.#protocolGeneration,
			queryState: (key) => self.#queryState(key),
			emitState: (key, allowFetch) => self.#emitState(key, allowFetch),
			operationGeneration: (key) => self.#operationGeneration(key),
			allocateIndexRevision: () => self.#allocateIndexRevision(),
			writeCanonicalResult: (artifact, stableVariables, envelope, source, requestRevision, responseProjectionGeneration) =>
				self.#writeCanonicalResult(
					artifact,
					stableVariables,
					envelope,
					source,
					requestRevision,
					responseProjectionGeneration
				),
			diagnosticEvent: (event) => self.#diagnosticEvent(event),
			resumeCursors: (key) => self.#resumeCursors(key),
			freshness: (artifact, key) => self.#freshness(artifact, key)
		};
		return this.#fetchLiveHostCache;
	}

	#protocolHost(): ProtocolHost {
		if (this.#protocolHostCache !== undefined) return this.#protocolHostCache;
		const self = this;
		this.#protocolHostCache = {
			get engine() {
				return self.#engine;
			},
			get queryStates() {
				return self.#queryStates;
			},
			get operationProtocols() {
				return self.#operationProtocols;
			},
			get operationGenerations() {
				return self.#operationGenerations;
			},
			get recordClocks() {
				return self.#recordClocks;
			},
			get recordKeysByScope() {
				return self.#recordKeysByScope;
			},
			get projectedRecordFences() {
				return self.#projectedRecordFences;
			},
			get membershipFences() {
				return self.#membershipFences;
			},
			get deferredMembershipConfirms() {
				return self.#deferredMembershipConfirms;
			},
			get anonymousRecordClocks() {
				return self.#anonymousRecordClocks;
			},
			get optimisticReceipts() {
				return self.#optimisticReceipts;
			},
			get diagnosticLayers() {
				return self.#diagnosticLayers;
			},
			get indexPlanRegistrations() {
				return self.#indexPlanRegistrations;
			},
			get renderedOperations() {
				return self.#renderedOperations;
			},
			get readOperationKeys() {
				return self.#readOperationKeys;
			},
			get watchRenderCounts() {
				return self.#watchRenderCounts;
			},
			get watches() {
				return self.#watches;
			},
			get diagnostics() {
				return self.#diagnosticsEnabled();
			},
			getProtocolGeneration: () => self.#protocolGeneration,
			setProtocolGeneration: (value) => {
				self.#protocolGeneration = value;
			},
			getProtocolGenerationSequence: () => self.#protocolGenerationSequence,
			bumpProtocolGenerationSequence: () => {
				self.#protocolGenerationSequence += 1;
			},
			disposeAuthorizationGeneration: () => {
				try {
					self.#onAuthorizationGenerationDispose?.();
				} catch (error) {
					self._reportObserverErrors([error]);
				}
			},
			getTrustedPresets: () => self.#trustedPresets,
			setTrustedPresets: (value) => {
				self.#trustedPresets = value;
			},
			getCommandAuthorityContract: () => self.#commandAuthorityContract,
			setProjectionGeneration: (value) => {
				self.#projectionGeneration = value;
			},
			setNextIndexRevision: (value) => {
				self.#nextIndexRevision = value;
			},
			setDiagnosticLayerSequence: (value) => {
				self.#diagnosticLayerSequence = value;
			},
			clearIndexMaintenance: () => {
				self.#indexMaintenance.clear();
			},
			abortAuthorization: () => {
				self.#authorizationAbort.abort();
				self.#authorizationAbort = new AbortController();
			},
			closeActiveTransports: () => self.#closeActiveTransports(),
			syncDiagnostics: () => self.#syncDiagnostics(),
			emitState: (key, allowFetch) => self.#emitState(key, allowFetch),
			diagnosticEvent: (event) => self.#diagnosticEvent(event),
			rememberRenderedOperation: (key, artifact, variables, source) =>
				self.#rememberRenderedOperation(key, artifact, variables, source)
		};
		return this.#protocolHostCache;
	}

	#optimisticHost(): OptimisticHost {
		if (this.#optimisticHostCache !== undefined) return this.#optimisticHostCache;
		const self = this;
		this.#optimisticHostCache = {
			get engine() {
				return self.#engine;
			},
			get optimisticReceipts() {
				return self.#optimisticReceipts;
			},
			get diagnosticLayers() {
				return self.#diagnosticLayers;
			},
			get diagnostics() {
				return self.#diagnosticsEnabled();
			},
			getDiagnosticLayerSequence: () => self.#diagnosticLayerSequence,
			setDiagnosticLayerSequence: (value) => {
				self.#diagnosticLayerSequence = value;
			},
			diagnosticEvent: (event) => self.#diagnosticEvent(event),
			syncDiagnostics: () => self.#syncDiagnostics(),
			retireDiagnosticLayer: (id, action, receiptState, receipt) =>
				self.#retireDiagnosticLayer(id, action, receiptState, receipt)
		};
		return this.#optimisticHostCache;
	}

	#hydrationHost(): HydrationHost {
		if (this.#hydrationHostCache !== undefined) return this.#hydrationHostCache;
		const self = this;
		this.#hydrationHostCache = {
			get engine() {
				return self.#engine;
			},
			get operationProtocols() {
				return self.#operationProtocols;
			},
			get operationGenerations() {
				return self.#operationGenerations;
			},
			get recordClocks() {
				return self.#recordClocks;
			},
			get recordKeysByScope() {
				return self.#recordKeysByScope;
			},
			get anonymousRecordClocks() {
				return self.#anonymousRecordClocks;
			},
			get optimisticReceipts() {
				return self.#optimisticReceipts;
			},
			get diagnosticLayers() {
				return self.#diagnosticLayers;
			},
			get renderedOperations() {
				return self.#renderedOperations;
			},
			get readOperationKeys() {
				return self.#readOperationKeys;
			},
			get watchRenderCounts() {
				return self.#watchRenderCounts;
			},
			get indexPlanRegistrations() {
				return self.#indexPlanRegistrations;
			},
			get queryStates() {
				return self.#queryStates;
			},
			get diagnostics() {
				return self.#diagnosticsEnabled();
			},
			getProtocolGeneration: () => self.#protocolGeneration,
			setProtocolGeneration: (value) => {
				self.#protocolGeneration = value;
			},
			getProtocolGenerationSequence: () => self.#protocolGenerationSequence,
			getTrustedPresets: () => self.#trustedPresets,
			setTrustedPresets: (value) => {
				self.#trustedPresets = value;
			},
			getNextIndexRevision: () => self.#nextIndexRevision,
			setNextIndexRevision: (value) => {
				self.#nextIndexRevision = value;
			},
			getArtifactBinding: () => self.#artifactBinding,
			setArtifactBinding: (value) => {
				self.#artifactBinding = value;
			},
			getCommandAuthorityContract: () => self.#commandAuthorityContract,
			setDiagnosticLayerSequence: (value) => {
				self.#diagnosticLayerSequence = value;
			},
			operationGeneration: (key) => self.#operationGeneration(key),
			closeActiveTransports: () => self.#closeActiveTransports(),
			closeAuthorizationGeneration: () => self.#closeAuthorizationGeneration(),
			resumeLiveWatches: () => self.#resumeLiveWatches(),
			syncDiagnostics: () => self.#syncDiagnostics(),
			diagnosticEvent: (event) => self.#diagnosticEvent(event),
			refreshIndexMaintenance: () => self.#refreshIndexMaintenance(),
			finishDehydration: () => self.#finishDehydration()
		};
		return this.#hydrationHostCache;
	}

	get scope(): ReplicaAuthoritativeScope | undefined {
		const scope = this.#protocolGeneration;
		return scope === undefined
			? undefined
			: Object.freeze({
					protocolVersion: scope.protocolVersion,
					schemaHash: scope.schemaHash,
					authorizationGeneration: scope.authorizationGeneration,
					cacheScope: scope.cacheScope
				});
	}

	get authorizationGeneration(): number {
		return this.#protocolGenerationSequence;
	}

	[replicaCommandAuthority](
		contract: ReplicaCommandSurfaceContract
	): ReplicaCommandAuthorityRegistration {
		const next = validatedCommandAuthorityContract(contract);
		const current = this.#commandAuthorityContract;
		if (current !== undefined && current.fingerprint !== next.fingerprint) {
			throw new TypeError(
				'command inventory does not match the active replica client surface'
			);
		}

		const binding = this.#artifactBinding;
		if (binding === undefined) {
			this.#artifactBinding = Object.freeze({
				version: 1,
				schemaHash: next.schemaHash,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
		} else if (
			binding.version !== 1 ||
			binding.schemaHash !== next.schemaHash ||
			(
				binding.surfaceIdentity !== undefined &&
				binding.surfaceIdentity !== next.surfaceIdentity
			) ||
			(
				binding.trustedPresets !== undefined &&
				trustedPresetDescriptorFingerprint(binding.trustedPresets) !==
					trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			throw new TypeError(
				'command inventory does not match the active replica client surface'
			);
		} else if (
			binding.surfaceIdentity === undefined ||
			binding.trustedPresets === undefined
		) {
			this.#artifactBinding = Object.freeze({
				...binding,
				...(binding.surfaceIdentity === undefined
					? { surfaceIdentity: next.surfaceIdentity }
					: {}),
				...(binding.trustedPresets === undefined
					? { trustedPresets: next.trustedPresets }
					: {})
			});
		}

		if (this.#protocolGeneration !== undefined) {
			try {
				matchReplicaTrustedPresetInventory(
					next.trustedPresets,
					this.#trustedPresets
				);
			} catch (error) {
				this.#purgeProtocolGeneration();
				throw error;
			}
		}
		this.#commandAuthorityContract = next;

		let active = true;
		const read = (): ReplicaCommandAuthoritySnapshot => {
			if (!active) {
				throw new TypeError('replica command authority registration is disposed');
			}
			return Object.freeze({
				generation: this.#protocolGenerationSequence,
				scope: this.scope,
				trustedPresets: this.#trustedPresets,
				signal: this.#authorizationAbort.signal
			});
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
		for (const [id, entry] of this.#freshnessPlans) {
			if (entry.generation !== this.#protocolGenerationSequence || this.#engine.optimisticLayerState(id) === undefined) this.#freshnessPlans.delete(id);
		}
		if (!this.#freshnessPlans.has(commandId) && this.#freshnessPlans.size >= 256) throw new Error('too many pending causal commands');
		this.#freshnessPlans.set(commandId, { generation: this.#protocolGenerationSequence, plan });
	}

	#freshness(artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>, key: string): Readonly<Record<string, unknown>> | undefined {
		const scope = this.#protocolGeneration;
		const protocolHash = artifact.protocol.protocolHash ?? this.#commandAuthorityContract?.protocolHash;
		if (scope === undefined || protocolHash === undefined) return undefined;
		const pending: unknown[] = [];
		for (const [id, entry] of this.#freshnessPlans) {
			if (entry.generation !== this.#protocolGenerationSequence || this.#engine.optimisticLayerState(id) === undefined) {
				this.#freshnessPlans.delete(id);
				continue;
			}
			if (!createReplicaRevalidationMatcher(entry.plan)(artifact)) continue;
			// Overlap already uses full generated list/filter/count/relationship
			// dependencies. An overlapping request must use primary even when an
			// incomplete producer only supplied an opaque dependency name.
			pending.push({ complete: false, models: [...entry.plan.models], relationships: [] });
		}
		const minimum: unknown[] = [];
		// Retained clocks live independently of pending layer state. Index scopes
		// belong to this query/live plan; never compare a different query's clock.
		const group = this.#operationProtocols.get(key);
		for (const state of [group?.query, group?.live]) {
			for (const [projection, clock] of state?.indexClocks ?? []) {
				minimum.push({ kind: 'index', projection, scopeToken: clock.scopeToken, position: clock.position });
			}
		}
		// Direct Atomic effects have not yet acquired a query index checkpoint.
		// Once a proving query retires this fence, its retained index clock covers
		// membership (including later deletion) without requiring an absent row.
		for (const [recordKey, { clock }] of this.#projectedRecordFences) {
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
		const pendingRecordClocks = new Map<string, RecordProtocolClock>();
		const pendingRecordScopes = new Map<DistributedOpaqueString, string>();
		const pendingAnonymousRecordClocks = new Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>();
		const consumedAnonymousRecordClocks = new Set<DistributedOpaqueString>();
		const apply = this.#resolveRecordEvidence(
			recordKey,
			evidence,
			pendingRecordClocks,
			pendingRecordScopes,
			pendingAnonymousRecordClocks,
			consumedAnonymousRecordClocks
		);
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
				: this.#allocateIndexRevision();

		confirmOptimisticLayerWithCacheWriterOn(
			this.#optimisticHost(),
			commandId,
			(writer) => {
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
			}
		);

		for (const [key, clock] of pendingRecordClocks) {
			this.#recordClocks.set(key, clock);
			this.#recordKeysByScope.set(clock.scopeToken, key);
		}
		for (const [scopeToken, key] of pendingRecordScopes) {
			this.#recordKeysByScope.set(scopeToken, key);
		}
		for (const [scopeToken, clock] of pendingAnonymousRecordClocks) {
			if (!consumedAnonymousRecordClocks.has(scopeToken)) {
				this.#anonymousRecordClocks.set(scopeToken, clock);
			}
		}
		for (const scopeToken of consumedAnonymousRecordClocks) {
			this.#anonymousRecordClocks.delete(scopeToken);
		}
		if (apply) {
			this.#projectionGeneration += 1;
			/*
			 * The command result is authoritative before an asynchronous read
			 * model necessarily catches up. Retain its complete row and causal
			 * clock as a write fence until a query acknowledges it or a newer
			 * record/tombstone supersedes it.
			 *
			 * Do not also take a membership fence: Atomic lists are @load, not
			 * @live. A membership fence would reject later complete snapshots
			 * until a live frame that never comes, stalling blob/new games and
			 * client-side navigations.
			 */
			this.#projectedRecordFences.set(
				recordKey,
				Object.freeze({
					fields: Object.freeze({ ...fields }),
					clock: Object.freeze({
						scopeToken: evidence.scopeToken,
						incarnation: evidence.incarnation,
						revision: evidence.revision,
						tombstone: false
					}),
					projectionGeneration: this.#projectionGeneration
				})
			);
		}
	}

	[replicaCommandProjectionDelta](
		commandId: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[]
	): boolean {
		return replaceReplicaOptimisticLayerOn(
			this.#optimisticHost(),
			commandId,
			(writer) => update(this.#capturingOptimisticWriter(commandId, writer)),
			semanticChanges
		);
	}

	read<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): ReplicaSnapshot<TData> {
		this.#bindArtifact(artifact);
		const stableVariables = canonicalizeOperationVariables(artifact, variables);
		const key = operationKey(artifact, stableVariables);
		this.#rememberRenderedOperation(key, artifact, stableVariables, 'read');
		const materialized = this.#engine.read((reader) =>
			materializeReplicaOperation(reader, artifact, stableVariables)
		);
		return snapshotFrom(materialized, this.#queryState(key));
	}

	watch<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		options: WatchReplicaOptions = {}
	): ReplicaWatch<TData> {
		this.#bindArtifact(artifact);
		return new ReplicaWatchState(this, artifact, variables, options);
	}

	writeResult<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource
	): void {
		assertWriteSource(source);
		this.#bindArtifact(artifact);
		const stableVariables = canonicalizeOperationVariables(artifact, variables);
		this.#writeCanonicalResult(artifact, stableVariables, envelope, source);
	}

	revalidate(plan: ReplicaRevalidationPlan): Promise<void> {
		const matches = createReplicaRevalidationMatcher(plan);
		const generation = this.#protocolGenerationSequence;
		const active = new Map<
			string,
			ReplicaWatchState<unknown, GraphqlVariables>
		>();
		for (const [key, watches] of this.#watches) {
			const watch = watches.values().next().value;
			if (watch !== undefined && matches(watch.artifact)) {
				active.set(key, watch);
			}
		}
		/*
		 * A command-status observation may arrive while an older query is
		 * already in flight. Drain that request first; the authoritative
		 * revalidation below must start after the command fence.
		 */
		const prior = [...active.keys()].flatMap((key) => {
			const request = this.#inFlight.get(key);
			return request === undefined ? [] : [request];
		});
		return Promise.all(prior)
			.then(() => {
				if (this.#protocolGenerationSequence !== generation) return [];

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
				if (this.#protocolGenerationSequence !== generation) return;
				const failed = watches.some((watch) => {
					const state = this.#queryState(watch.key);
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
		this.#purgeProtocolGeneration();
	}

	dehydrate(): ReplicaDehydratedState {
		return dehydrateReplica(this.#hydrationHost());
	}

		hydrate(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope
	): boolean {
		return hydrateReplica(this.#hydrationHost(), state, authoritativeScope);
	}

	#bindArtifact<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void {
		const next = validatedArtifactBinding(artifact);
		this.#validateActiveArtifactTrustedPresets(next);
		const current = this.#artifactBinding;
		if (current === undefined) {
			this.#artifactBinding = Object.freeze({
				version: next.version,
				schemaHash: next.schemaHash,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
			return;
		}
		if (
			(
				current.surfaceIdentity === undefined ||
				current.trustedPresets === undefined
			) &&
			current.version === next.version &&
			current.schemaHash === next.schemaHash &&
			(
				current.surfaceIdentity === undefined ||
				current.surfaceIdentity === next.surfaceIdentity
			) &&
			(
				current.trustedPresets === undefined ||
				trustedPresetDescriptorFingerprint(current.trustedPresets) ===
					trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			this.#artifactBinding = Object.freeze({
				...current,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
			return;
		}
		if (
			current.version !== next.version ||
			current.schemaHash !== next.schemaHash ||
			(
				current.surfaceIdentity !== undefined &&
				current.surfaceIdentity !== next.surfaceIdentity
			) ||
			(
				current.trustedPresets !== undefined &&
				trustedPresetDescriptorFingerprint(
					current.trustedPresets
				) !== trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			throw new TypeError(
				'replica artifact schema does not match the active replica binding'
			);
		}
	}

	#validateActiveArtifactTrustedPresets(
		binding: ValidatedArtifactBinding
	): void {
		if (this.#protocolGeneration === undefined) {
			return;
		}
		try {
			matchReplicaTrustedPresetInventory(
				binding.trustedPresets,
				this.#trustedPresets
			);
		} catch (error) {
			this.#purgeProtocolGeneration();
			throw error;
		}
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
			this.#validateProtocolBinding(artifact, distributed, source);
			const previousProtocolGeneration = this.#protocolGeneration;
			const nextProtocolGeneration =
				this.#stageProtocolGeneration(distributed);
			const nextTrustedPresets = this.#stageTrustedPresets(
				distributed.trustedPresets,
				nextProtocolGeneration,
				artifact
			);

		const snapshot = distributed.snapshot;
		const live = distributed.live;
		if (live !== undefined && snapshot === undefined) {
			protocolInvalid('extensions.distributed.snapshot');
		}
		if (
			source === 'live' &&
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
			operation === undefined
				? undefined
				: this.#operationProtocol(key, operation, operationSource);
			if (snapshot === undefined || operationState === undefined) {
				this.#applyReceiptOnly(distributed.command);
				this.#trustedPresets = nextTrustedPresets;
				this.#protocolGeneration = nextProtocolGeneration;
				this.#resumeLiveWatches();
				this.#diagnosticScopeTransition(
					previousProtocolGeneration,
					nextProtocolGeneration
				);
				this.#syncDiagnostics();
				return true;
		}

		this.#validateLiveSnapshot(snapshot, live);
		const unsupportedLive =
			source === 'live' && live?.supported === false;
		const reset = live?.reset === true;
		const group = this.#operationProtocols.get(key)!;
		/*
		 * An unsupported subscription response is an authorized fallback
		 * snapshot, not a live source. In particular, a row-filtered snapshot
		 * has no comparable index vector, so retaining live ownership here
		 * would reject every later query handoff. Relinquish any prior live
		 * ownership without advancing the generation; the forced HTTP fallback
		 * starts against the generation that remains after this frame.
		 */
		if (unsupportedLive && group.active === 'live') {
			group.active = undefined;
		}
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
		const sharedDisposition = this.#sharedIndexDisposition(
			key,
			replicaResultIndexKeys(
				artifact,
				stableVariables,
				envelope,
				snapshot
			),
			snapshot,
			requestRevision,
			source
		);
		const handoffBlocked =
			handoff &&
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
			!unsupportedLive &&
			!handoffBlocked &&
			isComparableHandoffDisposition(disposition) &&
			this.#activateOperationSource(
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
				this.#resetOperationState(operationState);
			} else {
				this.#discardOperationSnapshot(
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
				this.#resetOperationState(operationState);
			} else {
				/*
				 * The server-authorized GraphQL payload is still an exact
				 * replacement result. Only its partition-wide causal vector is
				 * unavailable (most commonly because exposing that position
				 * would leak denied-row activity). Preserve local membership,
				 * while dropping every capability that requires comparison.
				 */
				this.#resetOperationCausalState(operationState);
			}
			disposition = rejectedHandoff
				? 'incomparable'
				: sharedDisposition.disposition === 'lower'
					? 'lower'
					: 'fresh';
		} else if (disposition === 'incomparable') {
			if (rejectedHandoff) {
				this.#resetOperationState(operationState);
			} else {
				this.#discardOperationSnapshot(
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
		/*
		 * Revision zero is the cache engine's lowest legal checkpoint. It lets
		 * an unsupported live response fill an empty cache immediately while
		 * guaranteeing that any HTTP request revision can replace it.
		 */
		const indexRevision =
			unsupportedLive
				? '0'
				: writeIndexes &&
					  (
							sourceSwitched ||
							sharedDisposition.disposition === 'higher'
						)
					? this.#allocateIndexRevision()
					: writeIndexes &&
						  sharedDisposition.disposition === 'equal' &&
						  sharedDisposition.indexRevision !== undefined
						? sharedDisposition.indexRevision
				: snapshot.indexesComparable &&
					  disposition === 'equal' &&
					  operationState.indexRevision !== undefined
					? operationState.indexRevision
					: (requestRevision ?? this.#allocateIndexRevision());
		const recordEvidence = prepareRecordEvidence(
			snapshot,
			distributed.command?.records ?? Object.freeze([])
		);
		const pendingRecordClocks = new Map<string, RecordProtocolClock>();
		const pendingRecordScopes = new Map<DistributedOpaqueString, string>();
		const pendingAnonymousRecordClocks = new Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>();
		const consumedAnonymousRecordClocks = new Set<
			DistributedOpaqueString
		>();
		const pendingPathRecords = new Map<string, string>();
		const pendingProjectedRecordFenceClears = new Map<
			string,
			ProjectedRecordFence
		>();
		const consumedRecordPaths = new Set<string>();
		const observationsAdmissible =
			snapshot.recordsComplete &&
				snapshot.indexesComparable &&
				disposition !== 'incomparable' &&
				(disposition !== 'lower' ||
					this.#operationProtocols.get(key)?.active ===
						operationSource);
		const receiptPlan = this.#planOptimisticReceipts(
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
				const projectedFence =
					this.#projectedRecordFences.get(recordKey);
				const projectedDisposition =
					projectedFence === undefined
						? undefined
						: compareProjectedRecordFields(
								projectedFence.fields,
								fields
							);
				const projectedClockComparison =
					projectedFence === undefined
						? undefined
						: compareEvidenceToProjectedFence(
								evidence,
								projectedFence
							);
				const newerPostProjectionRow =
					projectedFence !== undefined &&
					projectedDisposition === 'conflict' &&
					projectedClockComparison === 1 &&
					responseProjectionGeneration !== undefined &&
					responseProjectionGeneration >=
						projectedFence.projectionGeneration;
				/*
				 * Snapshot bodies can race: SQL may be read before a command
				 * while response evidence is stamped after it. Conflicting
				 * pre-command responses therefore cannot override a projected
				 * fence, even when their evidence revision is numerically later.
				 * A causally newer row from an HTTP request or live frame that
				 * began after the command is an atomic supersession and may
				 * release the fence without first echoing the projected body.
				 */
				if (projectedFence !== undefined) {
					const exactEcho =
						projectedDisposition === 'complete' &&
						(
							projectedClockComparison === 0 ||
							projectedClockComparison === 1
						);
					const newerPartial =
						projectedDisposition === 'partial' &&
						projectedClockComparison === 1;
					if (exactEcho || newerPartial || newerPostProjectionRow) {
						pendingProjectedRecordFenceClears.set(
							recordKey,
							projectedFence
						);
					}
				}
				const resolution = this.#resolveRecordEvidence(
					recordKey,
					evidence,
					pendingRecordClocks,
					pendingRecordScopes,
					pendingAnonymousRecordClocks,
					consumedAnonymousRecordClocks,
					projectedDisposition === 'conflict' &&
						!newerPostProjectionRow
				);
				pendingPathRecords.set(encodedPath, recordKey);
				return Object.freeze({ evidence, apply: resolution });
			}
		};

		const state = this.#queryState(key);
		const previousErrors = state.errors;
		state.errors = stableErrors(previousErrors, envelope.errors ?? []);
		let summary: ReturnType<typeof normalizeReplicaResult>;
		try {
			const update = (writer: BaseCacheWriter) => {
				const guarded = this.#guardIndexWriter(writer);
				this.#applyTombstoneEvidence(
					guarded,
					recordEvidence.tombstones,
					operationState,
					pendingRecordClocks,
					pendingRecordScopes,
					pendingAnonymousRecordClocks,
					consumedAnonymousRecordClocks,
					consumedRecordPaths,
					pendingProjectedRecordFenceClears
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
				this.#applyPathlessEvidence(
					guarded,
					recordEvidence.pathless,
					recordEvidence.byPath,
					consumedRecordPaths,
					pendingRecordClocks,
					pendingRecordScopes,
					pendingAnonymousRecordClocks,
					consumedAnonymousRecordClocks,
					pendingProjectedRecordFenceClears
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
				this.#discardOperationSnapshot(
					operationState,
					artifact,
					stableVariables
				);
			}
			this.#emitState(key, false);
			throw error;
		}

		for (const [recordKey, clock] of pendingRecordClocks) {
			this.#recordClocks.set(recordKey, clock);
			this.#recordKeysByScope.set(clock.scopeToken, recordKey);
		}
		for (const [scopeToken, recordKey] of pendingRecordScopes) {
			this.#recordKeysByScope.set(scopeToken, recordKey);
		}
		for (const [scopeToken, clock] of pendingAnonymousRecordClocks) {
			if (!consumedAnonymousRecordClocks.has(scopeToken)) {
				this.#anonymousRecordClocks.set(scopeToken, clock);
			}
		}
		for (const scopeToken of consumedAnonymousRecordClocks) {
			this.#anonymousRecordClocks.delete(scopeToken);
		}
		for (const [path, recordKey] of pendingPathRecords) {
			operationState.pathRecords.set(path, recordKey);
		}
		for (const [recordKey, projectedFields] of pendingProjectedRecordFenceClears) {
			if (this.#projectedRecordFences.get(recordKey) === projectedFields) {
				this.#projectedRecordFences.delete(recordKey);
			}
		}
		for (const [id, receipt] of receiptPlan.updates) {
			if (receiptPlan.satisfied.includes(id)) {
				this.#retireDiagnosticLayer(id, 'retired', 'atomic', receipt);
				this.#optimisticReceipts.delete(id);
			} else {
				this.#optimisticReceipts.set(id, receipt);
				this.#engine.markOptimisticLayerAccepted(id);
				const layer = this.#diagnosticLayers?.get(id);
				if (layer !== undefined) {
					this.#diagnosticLayers!.set(
						id,
						Object.freeze({ ...layer, state: 'accepted' as const })
					);
				}
				if (this.#diagnostics !== undefined) {
					const counts = diagnosticReceiptCounts(receipt);
					this.#diagnosticEvent(
						Object.freeze({
							kind: 'receipt',
							command: id,
							state:
								counts.obligations === 0
									? ('succeeded' as const)
									: ('succeeded_pending_projection' as const),
							obligations: counts.obligations,
							observed: counts.observed
						})
					);
				}
			}
		}
		if (writeIndexes) {
			operationState.indexRevision = indexRevision;
			for (const indexKey of summary.indexKeys) {
				operationState.indexKeys.add(indexKey);
			}
			if (snapshot.indexesComparable) {
				operationState.snapshotScope = snapshot.scopeToken;
				operationState.indexClocks = indexClockMap(snapshot.indexes);
				operationState.cursors = latestCursors(snapshot, live);
			} else {
				this.#resetOperationCausalState(operationState);
				operationState.indexRevision = indexRevision;
				for (const indexKey of summary.indexKeys) {
					operationState.indexKeys.add(indexKey);
				}
			}
		} else if (live?.reset === true || !snapshot.indexesComparable) {
			operationState.cursors = Object.freeze([]);
		}
		if (source === 'live' && !unsupportedLive) {
			this.#advanceOperationGeneration(key);
		}
			if (source !== 'live' && sourceSwitched) {
				this.#restartLive(key);
			}
			this.#trustedPresets = nextTrustedPresets;
			this.#protocolGeneration = nextProtocolGeneration;
		this.#resumeLiveWatches();
		this.#emitState(key, false);
		this.#flushDeferredMembershipConfirms();
		if (this.#diagnostics !== undefined) {
			const cache = this.#engine.extract();
			this.#diagnosticEvent(
				Object.freeze({
					kind: 'normalization',
					operation: artifact.id,
					source,
					records: cache.records.length,
					indexes: summary.indexKeys.length,
					partial:
						summary.partial ||
						disposition === 'lower' ||
						disposition === 'incomparable' ||
						rejectedHandoff
				})
			);
			for (const indexKey of summary.indexKeys) {
				const index = cache.indexes.find(
					(candidate) => candidate.key === indexKey && !candidate.deleted
				);
				const staleReason = index?.metadata?.staleReason;
				this.#diagnosticEvent(
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
			this.#diagnosticScopeTransition(
				previousProtocolGeneration,
				nextProtocolGeneration
			);
			this.#syncDiagnostics();
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
		createOptimisticLayerOn(this.#optimisticHost(), id, update, semanticChanges);
	}

	markOptimisticLayerAccepted(
		id: string,
		receipt?: DistributedCommandMetadata
	): boolean {
		return markOptimisticLayerAcceptedOn(this.#optimisticHost(), id, receipt);
	}

	confirmOptimisticLayer<T>(
		id: string,
		update: (writer: ReplicaBaseWriter) => T
	): T {
		if (this.#commandHasMembershipFence(id)) {
			this.#deferredMembershipConfirms.add(id);
			return this.#engine.batch((writer) => update(baseWriter(writer)));
		}
		return confirmOptimisticLayerOn(this.#optimisticHost(), id, update);
	}

	rejectOptimisticLayer(id: string): boolean {
		this.#clearMembershipFencesForCommand(id);
		this.#deferredMembershipConfirms.delete(id);
		return rejectOptimisticLayerOn(this.#optimisticHost(), id);
	}

	#capturingOptimisticWriter(
		commandId: string,
		writer: ReplicaOptimisticWriter
	): ReplicaOptimisticWriter {
		const touchedRecords = new Set<string>();
		return {
			writeRecord: (
				model: ReplicaModelArtifact,
				identity: ReplicaIdentity,
				patch: ReplicaRecordPatch
			) => {
				touchedRecords.add(replicaRecordKey(model, identity));
				writer.writeRecord(model, identity, patch);
			},
			tombstoneRecord: (model, identity) => {
				const recordKey = replicaRecordKey(model, identity);
				touchedRecords.delete(recordKey);
				this.#clearMembershipFenceOwner(commandId, recordKey);
				writer.tombstoneRecord(model, identity);
			},
			writeIndex: (target, records) => {
				const indexKey = indexKeyFromTarget(target);
				for (const recordKey of records) {
					if (!touchedRecords.has(recordKey)) continue;
					let recordsForIndex = this.#membershipFences.get(indexKey);
					if (recordsForIndex === undefined) {
						recordsForIndex = new Map();
						this.#membershipFences.set(indexKey, recordsForIndex);
					}
					let owners = recordsForIndex.get(recordKey);
					if (owners === undefined) {
						owners = new Set();
						recordsForIndex.set(recordKey, owners);
					}
					owners.add(commandId);
				}
				writer.writeIndex(target, records);
			},
			deleteIndex: (target) => {
				const indexKey = indexKeyFromTarget(target);
				const recordsForIndex = this.#membershipFences.get(indexKey);
				if (recordsForIndex !== undefined) {
					for (const [recordKey, owners] of recordsForIndex) {
						owners.delete(commandId);
						if (owners.size === 0) recordsForIndex.delete(recordKey);
					}
					if (recordsForIndex.size === 0) {
						this.#membershipFences.delete(indexKey);
					}
				}
				writer.deleteIndex(target);
			}
		};
	}

	#commandHasMembershipFence(commandId: string): boolean {
		for (const recordsForIndex of this.#membershipFences.values()) {
			for (const owners of recordsForIndex.values()) {
				if (owners.has(commandId)) return true;
			}
		}
		return false;
	}

	#clearMembershipFencesForCommand(commandId: string): void {
		for (const [indexKey, recordsForIndex] of this.#membershipFences) {
			for (const [recordKey, owners] of recordsForIndex) {
				owners.delete(commandId);
				if (owners.size === 0) recordsForIndex.delete(recordKey);
			}
			if (recordsForIndex.size === 0) {
				this.#membershipFences.delete(indexKey);
			}
		}
	}

	#clearMembershipFenceOwner(commandId: string, recordKey: string): void {
		for (const [indexKey, recordsForIndex] of this.#membershipFences) {
			const owners = recordsForIndex.get(recordKey);
			if (owners === undefined) continue;
			owners.delete(commandId);
			if (owners.size === 0) recordsForIndex.delete(recordKey);
			if (recordsForIndex.size === 0) {
				this.#membershipFences.delete(indexKey);
			}
		}
	}

	#guardIndexWriter(writer: BaseCacheWriter): BaseCacheWriter {
		return {
			recordClock: (key) => writer.recordClock(key),
			writeRecord: (write) => writer.writeRecord(write),
			tombstoneRecord: (key, revision, incarnation) =>
				writer.tombstoneRecord(key, revision, incarnation),
			discardRecord: (key) => writer.discardRecord(key),
			writeIndex: (write) => {
				const recordsForIndex = this.#membershipFences.get(write.key);
				if (write.complete === true && recordsForIndex !== undefined) {
					const visible =
						this.#engine.read(
							(reader) => reader.index(write.key)?.records
						) ?? [];
					for (const recordKey of recordsForIndex.keys()) {
						if (
							visible.includes(recordKey) &&
							!write.records.includes(recordKey)
						) {
							return false;
						}
					}
				}
				const wrote = writer.writeIndex(write);
				if (wrote && recordsForIndex !== undefined) {
					for (const recordKey of write.records) {
						recordsForIndex.delete(recordKey);
					}
					if (recordsForIndex.size === 0) {
						this.#membershipFences.delete(write.key);
					}
				}
				return wrote;
			},
			markIndexStale: (key, reason, revision) =>
				writer.markIndexStale(key, reason, revision),
			deleteIndex: (key, revision) => writer.deleteIndex(key, revision)
		};
	}

	#flushDeferredMembershipConfirms(): void {
		for (const commandId of [...this.#deferredMembershipConfirms]) {
			if (this.#commandHasMembershipFence(commandId)) continue;
			this.#deferredMembershipConfirms.delete(commandId);
			if (this.#engine.optimisticLayerState(commandId) === undefined) {
				continue;
			}
			confirmOptimisticLayerOn(
				this.#optimisticHost(),
				commandId,
				() => undefined
			);
		}
	}

	tombstoneRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		revision: ReplicaRevision
	): boolean {
		const wrote = this.#engine.batch((writer) =>
			writer.tombstoneRecord(replicaRecordKey(model, identity), revision)
		);
		if (wrote) this.#syncDiagnostics();
		return wrote;
	}

	markIndexStale(target: ReplicaIndexTarget, reason: string): boolean {
		const key = indexKeyFromTarget(target);
		const marked = this.#engine.batch((writer) =>
			writer.markIndexStale(key, reason)
		);
		if (marked) {
			if (this.#diagnostics !== undefined) {
				this.#diagnosticEvent(
					Object.freeze({
						kind: 'index-decision',
						index: key,
						decision: 'stale',
						reason
					})
				);
			}
			this.#syncDiagnostics();
		}
		return marked;
	}

	retainRecord(model: ReplicaModelArtifact, identity: ReplicaIdentity): void {
		this.#engine.retain(replicaRecordKey(model, identity));
	}

	releaseRecord(model: ReplicaModelArtifact, identity: ReplicaIdentity): void {
		this.#engine.release(replicaRecordKey(model, identity));
	}

	gc(): readonly string[] {
		const collected = this.#engine.gc();
		if (this.#diagnostics !== undefined) {
			this.#diagnosticEvent(
				Object.freeze({
					kind: 'gc',
					records: collected.length
				})
			);
		}
		this.#syncDiagnostics();
		return collected;
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

	#syncDiagnostics(): void {
		syncDiagnosticsOn(this.#diagnosticsHost());
	}

	#diagnosticEvent(event: ReplicaDiagnosticEventInput): void {
		diagnosticEventOn(this.#diagnosticsHost(), event);
	}

	#diagnosticOperation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void {
		diagnosticOperationOn(this.#diagnosticsHost(), artifact);
	}

	#diagnosticScopeTransition(
		previous: ProtocolGeneration | undefined,
		next: ProtocolGeneration
	): void {
		diagnosticScopeTransitionOn(this.#diagnosticsHost(), previous, next);
	}

	#retireDiagnosticLayer(
		id: string,
		action: 'retired' | 'rejected',
		receiptState: 'atomic' | 'rejected',
		receipt?: OptimisticReceiptState
	): void {
		retireDiagnosticLayerOn(this.#diagnosticsHost(), id, action, receiptState, receipt);
	}

	#validateProtocolBinding<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		envelope: DistributedProtocolEnvelope,
		source: ReplicaWriteSource
	): void {
		validateProtocolBindingOn(this.#protocolHost(), artifact, envelope, source);
	}

	#stageProtocolGeneration(
		envelope: DistributedProtocolEnvelope
	): ProtocolGeneration {
		return stageProtocolGenerationOn(this.#protocolHost(), envelope);
	}

	#stageTrustedPresets<TData, TVariables extends GraphqlVariables>(
		incoming: readonly DistributedTrustedPreset[],
		nextGeneration: ProtocolGeneration,
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): readonly DistributedTrustedPreset[] {
		return stageTrustedPresetsOn(
			this.#protocolHost(),
			incoming,
			nextGeneration,
			artifact
		);
	}

	#purgeProtocolGeneration(): void {
		purgeProtocolGenerationOn(this.#protocolHost());
	}

	#closeAuthorizationGeneration(): void {
		closeAuthorizationGenerationOn(this.#protocolHost());
	}

	#closeActiveTransports(): void {
		closeActiveTransportsOn(this.#fetchLiveHost());
	}

	#rememberRenderedOperation<TData, TVariables extends GraphqlVariables>(
		key: string,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		source: 'read' | 'watch'
	): void {
		this.#diagnosticOperation(artifact);
		if (!this.#indexPlanRegistrations.has(key)) {
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
		this.#renderedOperations.set(
			key,
			Object.freeze({
				artifact: artifact as ReplicaOperationArtifact<unknown, GraphqlVariables>,
				variables
			})
		);
		if (source === 'read') {
			this.#readOperationKeys.add(key);
		} else {
			this.#watchRenderCounts.set(
				key,
				(this.#watchRenderCounts.get(key) ?? 0) + 1
			);
		}
	}

	#forgetRenderedWatch(key: string): void {
		const count = this.#watchRenderCounts.get(key);
		if (count === undefined) return;
		if (count > 1) {
			this.#watchRenderCounts.set(key, count - 1);
			return;
		}
		this.#watchRenderCounts.delete(key);
		if (!this.#readOperationKeys.has(key)) {
			this.#renderedOperations.delete(key);
			this.#disposeIndexPlan(key);
		}
	}

	#finishDehydration(): void {
		finishDehydrationOn(this.#hydrationHost());
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
		confirmed: CacheEngineSnapshot,
		layers: readonly OptimisticLayerView[]
	): readonly DerivedIndexMutation[] {
		const snapshot = indexMaintenanceSnapshot(confirmed);
		const indexes = new Map(snapshot.indexes.map((index) => [index.key, index]));
		const semanticLayers = layers.map(indexSemanticLayer);
		const mutations: DerivedIndexMutation[] = [];
			for (const decision of this.#indexMaintenance.evaluate(
				snapshot,
				semanticLayers,
				this.#trustedPresets
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
			const index = indexes.get(decision.indexKey);
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
		return this.#deriveMaintainedIndexes(this.#engine.extract(), [layer]);
	}

	#operationProtocol(
		key: string,
		operation: string,
		source: OperationProtocolSource
	): OperationProtocolState {
		let group = this.#operationProtocols.get(key);
		if (group === undefined) {
			group = {};
			this.#operationProtocols.set(key, group);
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

	#activateOperationSource<TData, TVariables extends GraphqlVariables>(
		key: string,
		source: OperationProtocolSource,
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): boolean {
		const group = this.#operationProtocols.get(key);
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
			const confirmedFences = this.#confirmedIndexFences(
				revisionsByKey.keys()
			);
			const ownedKeys = [...revisionsByKey].flatMap(
				([indexKey, revisions]) => {
					const revision = confirmedFences.get(indexKey);
					return revision !== undefined &&
						revisions.has(revision) &&
						!this.#indexClaimedByAnotherOperationState(
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
		this.#advanceOperationGeneration(key);
		return previous !== undefined;
	}

	#operationGeneration(key: string): number {
		return this.#operationGenerations.get(key) ?? 0;
	}

	#indexClaimedByAnotherOperationState(
		indexKey: string,
		revision: string,
		excluded: ReadonlySet<OperationProtocolState>
	): boolean {
		for (const group of this.#operationProtocols.values()) {
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

	#confirmedIndexFences(
		indexKeys: Iterable<string>
	): ReadonlyMap<string, string> {
		return this.#engine.confirmedIndexFences([...indexKeys]);
	}

	#sharedIndexDisposition(
		currentKey: string,
		incomingIndexKeys: ReadonlySet<string>,
		snapshot: DistributedQuerySnapshot,
		requestRevision: string | undefined,
		source: ReplicaWriteSource
	): SharedIndexDisposition {
		if (incomingIndexKeys.size === 0) return { compared: false };
		const confirmedRevisions =
			this.#confirmedIndexFences(incomingIndexKeys);
		let compared = false;
		let lower = false;
		let higher = false;
		let incomparable = false;
		let equalRevision: string | undefined;
		let latestOwnerRevision: string | undefined;
		for (const [key, group] of this.#operationProtocols) {
			if (key === currentKey) continue;
			for (const state of [group.query, group.live]) {
				if (state?.indexRevision === undefined) continue;
				let ownsIncomingIndex = false;
				for (const indexKey of state.indexKeys) {
					if (
						incomingIndexKeys.has(indexKey) &&
						confirmedRevisions.get(indexKey) === state.indexRevision
					) {
						ownsIncomingIndex = true;
						break;
					}
				}
				if (!ownsIncomingIndex) continue;
				latestOwnerRevision =
					latestOwnerRevision === undefined ||
					compareCanonicalDecimalStrings(
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
					incomparable = true;
					continue;
				}
				compared = true;
				if (disposition === 'lower') lower = true;
				else if (disposition === 'higher') higher = true;
				else {
					equalRevision =
						equalRevision === undefined ||
						compareCanonicalDecimalStrings(
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
				return { compared: true, disposition: 'lower' };
			}
			if (requestRevision === undefined) return { compared: false };
			return latestOwnerRevision !== undefined &&
				compareCanonicalDecimalStrings(requestRevision, latestOwnerRevision) <
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

	#advanceOperationGeneration(key: string): void {
		this.#operationGenerations.set(
			key,
			this.#operationGeneration(key) + 1
		);
	}

	#resumeCursors(key: string): readonly DistributedLiveCursor[] {
		const group = this.#operationProtocols.get(key);
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

	#discardOperationSnapshot<TData, TVariables extends GraphqlVariables>(
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
		const confirmedFence = this.#confirmedIndexFences(keys);
		const ownedKeys =
			indexRevision === undefined
				? []
				: [...keys].filter(
						(key) =>
							confirmedFence.get(key) === indexRevision &&
							!this.#indexClaimedByAnotherOperationState(
								key,
								indexRevision,
								discardedStates
							)
					);
		this.#engine.discardIndexes(ownedKeys);
		this.#resetOperationState(state);
	}

	#resetOperationState(state: OperationProtocolState): void {
		this.#resetOperationCausalState(state);
		state.indexRevision = undefined;
		state.indexKeys.clear();
		state.pathRecords.clear();
	}

	#resetOperationCausalState(state: OperationProtocolState): void {
		state.snapshotScope = undefined;
		state.indexClocks = new Map();
		state.cursors = Object.freeze([]);
	}

	#allocateIndexRevision(): string {
		this.#nextIndexRevision = incrementCanonicalDecimal(
			this.#nextIndexRevision
		);
		return this.#nextIndexRevision;
	}

	#resolveRecordEvidence(
		recordKey: string,
		evidence: DistributedRecordRevision,
		pendingClocks: Map<string, RecordProtocolClock>,
		pendingScopes: Map<DistributedOpaqueString, string>,
		pendingAnonymousClocks: Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>,
		consumedAnonymousClocks: Set<DistributedOpaqueString>,
		projectedConflict = false
	): boolean {
		if (!recordKeyMatchesModel(recordKey, evidence.model)) {
			protocolInvalid('extensions.distributed.snapshot.records.model');
		}
		const scopedKey =
			pendingScopes.get(evidence.scopeToken) ??
			this.#recordKeysByScope.get(evidence.scopeToken);
		if (scopedKey !== undefined && scopedKey !== recordKey) {
			protocolInvalid('extensions.distributed.snapshot.records.scopeToken');
		}
		pendingScopes.set(evidence.scopeToken, recordKey);

		const incoming: RecordProtocolClock = {
			scopeToken: evidence.scopeToken,
			incarnation: evidence.incarnation,
			revision: evidence.revision,
			tombstone: evidence.tombstone
		};
		const pending = pendingClocks.get(recordKey);
		if (pending !== undefined) {
			if (!sameRecordClock(pending, incoming)) {
				protocolInvalid('extensions.distributed.snapshot.records');
			}
			return true;
		}
		let current = this.#recordClocks.get(recordKey);
		const anonymous =
			pendingAnonymousClocks.get(evidence.scopeToken) ??
			this.#anonymousRecordClocks.get(evidence.scopeToken);
		if (anonymous !== undefined) {
			if (anonymous.model !== evidence.model) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.model'
				);
			}
			consumedAnonymousClocks.add(evidence.scopeToken);
			if (current === undefined) {
				current = anonymous.clock;
			} else {
				if (current.scopeToken !== anonymous.clock.scopeToken) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.scopeToken'
					);
				}
				const anonymousComparison = compareRecordClock(
					anonymous.clock,
					current
				);
				if (
					anonymousComparison === 0 &&
					anonymous.clock.tombstone !== current.tombstone
				) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.tombstone'
					);
				}
				if (anonymousComparison > 0) current = anonymous.clock;
			}
		}
		if (current === undefined) {
			pendingClocks.set(recordKey, incoming);
			return true;
		}
		if (current.scopeToken !== incoming.scopeToken) {
			protocolInvalid('extensions.distributed.snapshot.records.scopeToken');
		}
		if (projectedConflict) {
			pendingClocks.set(recordKey, current);
			return false;
		}
		const comparison = compareRecordClock(incoming, current);
		if (comparison < 0) {
			pendingClocks.set(recordKey, current);
			return false;
		}
		if (comparison === 0) {
			if (current.tombstone !== incoming.tombstone) {
				protocolInvalid('extensions.distributed.snapshot.records.tombstone');
			}
			pendingClocks.set(recordKey, incoming);
			return true;
		}
		if (
			current.tombstone &&
			!incoming.tombstone &&
			compareDistributedDecimal(
				incoming.incarnation,
				current.incarnation
			) <= 0
		) {
			protocolInvalid('extensions.distributed.snapshot.records.incarnation');
		}
		pendingClocks.set(recordKey, incoming);
		return true;
	}

	#retainAnonymousRecordEvidence(
		evidence: DistributedRecordRevision,
		pendingAnonymousClocks: Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>
	): void {
		const incoming: AnonymousRecordProtocolClock = {
			model: evidence.model,
			clock: {
				scopeToken: evidence.scopeToken,
				incarnation: evidence.incarnation,
				revision: evidence.revision,
				tombstone: evidence.tombstone
			}
		};
		const current =
			pendingAnonymousClocks.get(evidence.scopeToken) ??
			this.#anonymousRecordClocks.get(evidence.scopeToken);
		if (current === undefined) {
			let retained = this.#anonymousRecordClocks.size;
			for (const scopeToken of pendingAnonymousClocks.keys()) {
				if (!this.#anonymousRecordClocks.has(scopeToken)) retained += 1;
			}
			if (retained >= MAX_ANONYMOUS_RECORD_CLOCKS) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.capacity'
				);
			}
			pendingAnonymousClocks.set(evidence.scopeToken, incoming);
			return;
		}
		if (current.model !== incoming.model) {
			protocolInvalid('extensions.distributed.snapshot.records.model');
		}
		const comparison = compareRecordClock(
			incoming.clock,
			current.clock
		);
		if (comparison < 0) return;
		if (comparison === 0) {
			if (current.clock.tombstone !== incoming.clock.tombstone) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.tombstone'
				);
			}
			return;
		}
		if (
			current.clock.tombstone &&
			!incoming.clock.tombstone &&
			compareDistributedDecimal(
				incoming.clock.incarnation,
				current.clock.incarnation
			) <= 0
		) {
			protocolInvalid(
				'extensions.distributed.snapshot.records.incarnation'
			);
		}
		pendingAnonymousClocks.set(evidence.scopeToken, incoming);
	}

	#applyTombstoneEvidence(
		writer: BaseCacheWriter,
		evidenceItems: readonly DistributedRecordRevision[],
		operation: OperationProtocolState,
		pendingClocks: Map<string, RecordProtocolClock>,
		pendingScopes: Map<DistributedOpaqueString, string>,
		pendingAnonymousClocks: Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>,
		consumedAnonymousClocks: Set<DistributedOpaqueString>,
		consumedPaths: Set<string>,
		pendingProjectedRecordFenceClears: Map<
			string,
			ProjectedRecordFence
		>
	): void {
		for (const evidence of evidenceItems) {
			const encodedPath =
				evidence.path === undefined
					? undefined
					: responsePathKey(evidence.path);
			const recordKey =
				this.#recordKeysByScope.get(evidence.scopeToken) ??
				(encodedPath === undefined
					? undefined
					: operation.pathRecords.get(encodedPath));
			if (recordKey === undefined) {
				this.#retainAnonymousRecordEvidence(
					evidence,
					pendingAnonymousClocks
				);
				continue;
			}
			if (encodedPath !== undefined) consumedPaths.add(encodedPath);
			const projectedFence =
				this.#projectedRecordFences.get(recordKey);
			const projectedClockComparison =
				projectedFence === undefined
					? undefined
					: compareEvidenceToProjectedFence(
							evidence,
							projectedFence
						);
			if (
				projectedFence !== undefined &&
				projectedClockComparison === 1
			) {
				pendingProjectedRecordFenceClears.set(
					recordKey,
					projectedFence
				);
			}
			if (
				this.#resolveRecordEvidence(
					recordKey,
					evidence,
					pendingClocks,
					pendingScopes,
					pendingAnonymousClocks,
					consumedAnonymousClocks,
					projectedFence !== undefined &&
						projectedClockComparison !== 1
				)
			) {
				writer.tombstoneRecord(
					recordKey,
					evidence.revision,
					evidence.incarnation
				);
			}
		}
	}

	#applyPathlessEvidence(
		writer: BaseCacheWriter,
		evidenceItems: readonly DistributedRecordRevision[],
		pathEvidence: ReadonlyMap<string, DistributedRecordRevision>,
		consumedPaths: ReadonlySet<string>,
		pendingClocks: Map<string, RecordProtocolClock>,
		pendingScopes: Map<DistributedOpaqueString, string>,
		pendingAnonymousClocks: Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>,
		consumedAnonymousClocks: Set<DistributedOpaqueString>,
		pendingProjectedRecordFenceClears: Map<
			string,
			ProjectedRecordFence
		>
	): void {
		for (const evidence of evidenceItems) {
			const recordKey =
				pendingScopes.get(evidence.scopeToken) ??
				this.#recordKeysByScope.get(evidence.scopeToken);
			if (recordKey === undefined) {
				this.#retainAnonymousRecordEvidence(
					evidence,
					pendingAnonymousClocks
				);
				continue;
			}
			if (!recordKeyMatchesModel(recordKey, evidence.model)) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.model'
				);
			}
			let certifiedByPath = false;
			for (const [path, candidate] of pathEvidence) {
				if (
					consumedPaths.has(path) &&
					candidate.scopeToken === evidence.scopeToken &&
					sameRecordRevision(candidate, evidence)
				) {
					certifiedByPath = true;
					break;
				}
			}
			if (certifiedByPath) continue;
			const projectedFence =
				this.#projectedRecordFences.get(recordKey);
			const projectedClockComparison =
				projectedFence === undefined
					? undefined
					: compareEvidenceToProjectedFence(
							evidence,
							projectedFence
						);
			if (
				projectedFence !== undefined &&
				projectedClockComparison === 1
			) {
				pendingProjectedRecordFenceClears.set(
					recordKey,
					projectedFence
				);
			}
			if (
				this.#resolveRecordEvidence(
					recordKey,
					evidence,
					pendingClocks,
					pendingScopes,
					pendingAnonymousClocks,
					consumedAnonymousClocks,
					projectedFence !== undefined &&
						projectedClockComparison !== 1
				)
			) {
				// A change-log upsert proves only that a newer row exists. It
				// advances the tuple fence, but cannot certify any cached field.
				writer.discardRecord(recordKey);
			}
		}
	}

	#validateLiveSnapshot(
		snapshot: DistributedQuerySnapshot,
		live: DistributedProtocolEnvelope['live']
	): void {
		if (live === undefined || !live.supported) return;
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

	#planOptimisticReceipts(
		command: DistributedCommandMetadata | undefined,
		observations: readonly DistributedProjectionObservation[],
		satisfactionAdmissible: boolean
	): {
		updates: Map<string, OptimisticReceiptState>;
		satisfied: string[];
	} {
		return planOptimisticReceiptsOn(
			this.#optimisticHost(),
			command,
			observations,
			satisfactionAdmissible
		);
	}

	#applyReceiptOnly(command: DistributedCommandMetadata | undefined): void {
		applyReceiptOnlyOn(this.#optimisticHost(), command);
	}

		/** Package-internal hook used by one watched operation. */
	_register<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>
	): () => void {
		this.#rememberRenderedOperation(
			watch.key,
			watch.artifact,
			watch.variables,
			'watch'
		);
		let watches = this.#watches.get(watch.key);
		if (!watches) {
			watches = new Set();
			this.#watches.set(
				watch.key,
				watches as Set<ReplicaWatchState<unknown, GraphqlVariables>>
			);
		}
		watches.add(watch as ReplicaWatchState<unknown, GraphqlVariables>);
		const unwatch = this.#engine.watch(
			(reader) => materializeReplicaOperation(reader, watch.artifact, watch.variables),
			(materialized) => watch._cacheChanged(materialized),
			{ immediate: true }
		);
		if (watch.liveRequested) this.#retainLive(watch);
		void this._fetch(watch, false);
		return () => {
			unwatch();
			watches?.delete(watch as ReplicaWatchState<unknown, GraphqlVariables>);
			if (watches?.size === 0) this.#watches.delete(watch.key);
			this.#forgetRenderedWatch(watch.key);
			if (watch.liveRequested) this.#releaseLive(watch.key);
		};
	}

	/** Package-internal query-state lookup. */
	_state(key: string): QueryState {
		return this.#queryState(key);
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
		return fetchWatch(this.#fetchLiveHost(), watch, force);
	}

		_reportObserverErrors(errors: unknown[]): void {
		if (errors.length === 0) return;
		reportSafely(
			this.#reportObserverError,
			new AggregateError(errors, 'replica observer delivery failed')
		);
	}

	#queryState(key: string): QueryState {
		let state = this.#queryStates.get(key);
		if (!state) {
			state = { fetching: false, errors: EMPTY_ERRORS, live: 'off' };
			this.#queryStates.set(key, state);
		}
		return state;
	}

	#emitState(key: string, allowFetch: boolean): void {
		emitWatchState(this.#fetchLiveHost(), key, allowFetch);
	}

	#retainLive<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>
	): void {
		retainLiveOn(this.#fetchLiveHost(), watch);
	}

	#restartLive(key: string): void {
		restartLiveOn(this.#fetchLiveHost(), key);
	}

	#resumeLiveWatches(): void {
		resumeLiveWatchesOn(this.#fetchLiveHost());
	}

	#releaseLive(key: string): void {
		releaseLiveOn(this.#fetchLiveHost(), key);
	}
}
