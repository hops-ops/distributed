import { CacheRevisionConflictError } from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import {
	parseGraphqlResponseExtensions,
	type DistributedLiveCursor,
	type DistributedProtocolEnvelope
} from '../../protocol.js';
import type { ReplicaDiagnosticEventInput } from '../diagnostics.js';
import type {
	ReplicaOperationArtifact,
	ReplicaResultEnvelope,
	ReplicaTransport,
	ReplicaWriteSource
} from '../types.js';
import {
	graphqlError,
	replicaClientRequestExtensions,
	stableErrors
} from './helpers.js';
import type {
	LiveEntry,
	OperationProtocolGroup,
	ProtocolGeneration,
	QueryState
} from './types.js';
import type { ReplicaWatchState } from './watch.js';

/**
 * Host accessors for fetch / live subscription orchestration.
 * Free functions never read private class fields; the class supplies state.
 */
export type FetchLiveHost = {
	readonly transport: ReplicaTransport | undefined;
	readonly inFlight: Map<string, Promise<void>>;
	readonly inFlightAborts: Map<string, AbortController>;
	readonly lives: Map<string, LiveEntry>;
	readonly watches: Map<
		string,
		Set<ReplicaWatchState<unknown, GraphqlVariables>>
	>;
	readonly operationProtocols: Map<string, OperationProtocolGroup>;
	readonly diagnostics: { readonly enabled: boolean };
	protocolGenerationSequence(): number;
	projectionGeneration(): number;
	protocolGeneration(): ProtocolGeneration | undefined;
	queryState(key: string): QueryState;
	emitState(key: string, allowFetch: boolean): void;
	operationGeneration(key: string): number;
	allocateIndexRevision(): string;
	writeCanonicalResult<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		stableVariables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource,
		requestRevision?: string,
		responseProjectionGeneration?: number
	): DistributedProtocolEnvelope;
	diagnosticEvent(event: ReplicaDiagnosticEventInput): void;
	resumeCursors(key: string): readonly DistributedLiveCursor[];
	freshness(artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>, key: string): Readonly<Record<string, unknown>> | undefined;
};

export function emitWatchState(host: FetchLiveHost, key: string, allowFetch: boolean): void {
	for (const watch of host.watches.get(key) ?? []) watch._stateChanged(allowFetch);
}

export function closeActiveTransports(host: FetchLiveHost): void {
	for (const controller of host.inFlightAborts.values()) controller.abort();
	host.inFlightAborts.clear();
	for (const entry of host.lives.values()) {
		entry.active = false;
		try {
			entry.unsubscribe();
		} catch {
			// The generation fence is already closed; transport cleanup is best effort.
		}
	}
	host.lives.clear();
	host.inFlight.clear();
}

export function fetchWatch<TData, TVariables extends GraphqlVariables>(
	host: FetchLiveHost,
	watch: ReplicaWatchState<TData, TVariables>,
	force: boolean
): Promise<void> {
	if (!host.transport) return Promise.resolve();
	if (!force && watch.materialized.complete && !watch.materialized.stale) {
		if (host.diagnostics.enabled) {
			host.diagnosticEvent(
				Object.freeze({
					kind: 'revalidation',
					operation: watch.artifact.id,
					action: 'skipped-complete',
					reason: 'watch'
				})
			);
		}
		return Promise.resolve();
	}
	const existing = host.inFlight.get(watch.key);
	if (existing) {
		if (host.diagnostics.enabled) {
			host.diagnosticEvent(
				Object.freeze({
					kind: 'revalidation',
					operation: watch.artifact.id,
					action: 'deduplicated',
					reason: force
						? ('refresh' as const)
						: watch.materialized.stale
							? ('stale' as const)
							: ('watch' as const)
				})
			);
		}
		return existing;
	}

	const state = host.queryState(watch.key);
	if (host.diagnostics.enabled) {
		host.diagnosticEvent(
			Object.freeze({
				kind: 'revalidation',
				operation: watch.artifact.id,
				action: 'requested',
				reason: force
					? ('refresh' as const)
					: watch.materialized.stale
						? ('stale' as const)
						: ('watch' as const)
			})
		);
	}
	state.fetching = true;
	host.emitState(watch.key, false);
	const operationGeneration = host.operationGeneration(watch.key);
	const authorizationGeneration = host.protocolGenerationSequence();
	const projectionGeneration = host.projectionGeneration();
	/*
	 * Reserve local index ordering and the projection-fence generation when
	 * the request starts, not when it finishes. Distinct operation artifacts
	 * may share the same semantic index key; a slower earlier request must
	 * not replace a later-started result merely because its response arrived
	 * last, nor may it supersede a direct projection installed in between.
	 */
	const requestRevision = host.allocateIndexRevision();
	const controller = new AbortController();
	const request = Object.freeze({
		operation: 'query' as const,
		operationId: watch.artifact.id,
		document: watch.artifact.document,
		variables: watch.variables,
		artifact: watch.artifact,
		extensions: requestExtensions(host, watch.artifact, watch.key),
		signal: controller.signal
	});
	let flight: Promise<void>;
	flight = Promise.resolve()
		.then(() => host.transport!.fetch(request))
		.then((result) => {
			if (host.protocolGenerationSequence() !== authorizationGeneration) {
				if (host.diagnostics.enabled) {
					host.diagnosticEvent(
						Object.freeze({
							kind: 'response-fenced',
							operation: watch.artifact.id,
							transport: 'http',
							reason: 'authorization-generation'
						})
					);
				}
				return;
			}
			if (host.inFlight.get(watch.key) !== flight) {
				if (host.diagnostics.enabled) {
					host.diagnosticEvent(
						Object.freeze({
							kind: 'response-fenced',
							operation: watch.artifact.id,
							transport: 'http',
							reason: 'superseded'
						})
					);
				}
				return;
			}
			if (host.operationGeneration(watch.key) !== operationGeneration) {
				if (host.diagnostics.enabled) {
					host.diagnosticEvent(
						Object.freeze({
							kind: 'response-fenced',
							operation: watch.artifact.id,
							transport: 'http',
							reason: 'operation-generation'
						})
					);
				}
				return;
			}
			host.writeCanonicalResult(
				watch.artifact,
				watch.variables,
				result,
				'network',
				requestRevision,
				projectionGeneration
			);
		})
		.catch((error: unknown) => {
			if (host.inFlight.get(watch.key) !== flight) return;
			if (controller.signal.aborted) return;
			state.errors = stableErrors(state.errors, [graphqlError(error)]);
		})
		.finally(() => {
			if (host.inFlight.get(watch.key) !== flight) return;
			host.inFlight.delete(watch.key);
			host.inFlightAborts.delete(watch.key);
			state.fetching = false;
			host.emitState(watch.key, false);
		});
	host.inFlight.set(watch.key, flight);
	host.inFlightAborts.set(watch.key, controller);
	return flight;
}

export function retainLive<TData, TVariables extends GraphqlVariables>(
	host: FetchLiveHost,
	watch: ReplicaWatchState<TData, TVariables>
): void {
	if (!watch.artifact.live || !host.transport?.subscribe) return;
	const existing = host.lives.get(watch.key);
	if (existing) {
		existing.count += 1;
		return;
	}
	const state = host.queryState(watch.key);
	state.live = 'connecting';
	const entry: LiveEntry = {
		count: 1,
		unsubscribe: () => undefined,
		active: true,
		protocolGeneration: host.protocolGenerationSequence()
	};
	host.lives.set(watch.key, entry);
	const resume = host.resumeCursors(watch.key);
	try {
		const unsubscribe = host.transport.subscribe(
			Object.freeze({
				operation: 'live' as const,
				operationId: watch.artifact.live.id,
				document: watch.artifact.live.document,
				variables: watch.variables,
				artifact: watch.artifact,
				extensions: requestExtensions(host, watch.artifact, watch.key),
				...(resume === undefined || resume.length === 0
					? {}
					: { resume })
			}),
			{
				next: (result) => {
					// A live frame begins at this synchronous ingress boundary.
					const projectionGeneration = host.projectionGeneration();
					if (
						entry.protocolGeneration !== host.protocolGenerationSequence()
					) {
						if (host.diagnostics.enabled) {
							host.diagnosticEvent(
								Object.freeze({
									kind: 'response-fenced',
									operation: watch.artifact.live!.id,
									transport: 'live',
									reason: 'authorization-generation'
								})
							);
						}
						return;
					}
					if (
						entry.operationGeneration !== undefined &&
						entry.operationGeneration !==
							host.operationGeneration(watch.key)
					) {
						if (host.diagnostics.enabled) {
							host.diagnosticEvent(
								Object.freeze({
									kind: 'response-fenced',
									operation: watch.artifact.live!.id,
									transport: 'live',
									reason: 'operation-generation'
								})
							);
						}
						return;
					}
					if (
						!entry.active ||
						host.lives.get(watch.key) !== entry
					) {
						if (host.diagnostics.enabled) {
							host.diagnosticEvent(
								Object.freeze({
									kind: 'response-fenced',
									operation: watch.artifact.live!.id,
									transport: 'live',
									reason: 'superseded'
								})
							);
						}
						return;
					}
					let unsupportedLive = false;
					try {
						unsupportedLive =
							parseGraphqlResponseExtensions(result.extensions)
								?.distributed?.live?.supported === false;
						if (unsupportedLive) state.live = 'off';
						const distributed = host.writeCanonicalResult(
							watch.artifact,
							watch.variables,
							result,
							'live',
							undefined,
							projectionGeneration
						);
						if (distributed.live?.supported === false) {
							fallbackFromLive(host, watch, entry);
							return;
						}
						state.live = 'active';
						entry.operationGeneration =
							host.operationGeneration(watch.key);
					} catch (error) {
						if (
							unsupportedLive &&
							error instanceof CacheRevisionConflictError
						) {
							/*
							 * Revision zero is shared by provisional fallbacks.
							 * Another operation may already have filled the same
							 * semantic index differently; HTTP remains authoritative.
							 */
							fallbackFromLive(host, watch, entry);
							return;
						}
						state.live = 'error';
						state.errors = stableErrors(state.errors, [graphqlError(error)]);
						host.emitState(watch.key, false);
					}
				},
				error: (error) => {
					if (!entry.active || host.lives.get(watch.key) !== entry) return;
					entry.active = false;
					host.lives.delete(watch.key);
					const unsub = entry.unsubscribe;
					entry.unsubscribe = () => undefined;
					try {
						unsub();
					} catch {
						// The terminal stream is fenced; cleanup is best effort.
					}
					state.live = 'error';
					state.errors = stableErrors(state.errors, [graphqlError(error)]);
					host.emitState(watch.key, false);
				},
				complete: () => {
					if (!entry.active || host.lives.get(watch.key) !== entry) return;
					fallbackFromLive(host, watch, entry);
				}
			}
		);
		entry.unsubscribe = unsubscribe;
		if (!entry.active || host.lives.get(watch.key) !== entry) {
			entry.unsubscribe = () => undefined;
			try {
				unsubscribe();
			} catch {
				// A closed/superseded subscription is already fenced.
			}
			return;
		}
		state.live = 'active';
	} catch (error) {
		entry.active = false;
		host.lives.delete(watch.key);
		state.live = 'error';
		state.errors = stableErrors(state.errors, [graphqlError(error)]);
	}
	host.emitState(watch.key, false);
}

export function fallbackFromLive<TData, TVariables extends GraphqlVariables>(
	host: FetchLiveHost,
	watch: ReplicaWatchState<TData, TVariables>,
	entry: LiveEntry
): void {
	if (host.lives.get(watch.key) !== entry) {
		void fetchWatch(host, watch, true);
		return;
	}
	const protocol = host.operationProtocols.get(watch.key);
	if (protocol?.active === 'live') protocol.active = undefined;
	entry.active = false;
	const unsubscribe = entry.unsubscribe;
	entry.unsubscribe = () => undefined;
	try {
		unsubscribe();
	} catch {
		// The inactive stream is fenced; transport cleanup is best effort.
	}
	const state = host.queryState(watch.key);
	state.live = 'off';
	host.emitState(watch.key, false);
	const authorizationGeneration = host.protocolGenerationSequence();
	const supersededFlight =
		entry.operationGeneration === undefined
			? undefined
			: host.inFlight.get(watch.key);
	const refresh = (): void => {
		if (
			host.protocolGenerationSequence() !== authorizationGeneration ||
			host.lives.get(watch.key) !== entry ||
			entry.active
		) {
			return;
		}
		void fetchWatch(host, watch, true);
	};
	/*
	 * Keep the inactive entry as an authorization-generation-scoped
	 * sentinel. Query ingestion calls resumeLiveWatches(); deleting this
	 * entry would otherwise reopen an unsupported or completed stream
	 * immediately. Authorization invalidation clears it and may retry.
	 *
	 * A supported live frame advances the operation generation. Any HTTP
	 * request that was already running is therefore doomed by its response
	 * fence; drain it before starting the authoritative fallback. A first
	 * unsupported frame never advances the generation, so its overlapping
	 * HTTP request remains valid and can be reused directly.
	 */
	if (supersededFlight === undefined) {
		refresh();
	} else {
		void supersededFlight.then(refresh, refresh);
	}
}

export function restartLive(host: FetchLiveHost, key: string): void {
	const previous = host.lives.get(key);
	if (previous === undefined) return;
	const count = previous.count;
	previous.active = false;
	host.lives.delete(key);
	try {
		previous.unsubscribe();
	} catch {
		// The old generation is already fenced; cleanup is best effort.
	}
	const watch = [...(host.watches.get(key) ?? [])].find(
		(candidate) => candidate.liveRequested
	);
	if (watch === undefined) {
		host.queryState(key).live = 'off';
		return;
	}
	retainLive(host, watch);
	const replacement = host.lives.get(key);
	if (replacement !== undefined) replacement.count = count;
}

export function resumeLiveWatches(host: FetchLiveHost): void {
	if (host.protocolGeneration() === undefined) return;
	for (const [key, watches] of host.watches) {
		if (host.lives.has(key)) continue;
		const liveWatches = [...watches].filter(
			(watch) => watch.liveRequested
		);
		const first = liveWatches[0];
		if (first === undefined) continue;
		retainLive(host, first);
		const entry = host.lives.get(key);
		if (entry !== undefined) entry.count = liveWatches.length;
	}
}

export function releaseLive(host: FetchLiveHost, key: string): void {
	const entry = host.lives.get(key);
	if (!entry) return;
	entry.count -= 1;
	if (entry.count > 0) return;
	entry.active = false;
	host.lives.delete(key);
	entry.unsubscribe();
	host.queryState(key).live = 'off';
	host.emitState(key, false);
}

function requestExtensions(
	host: FetchLiveHost,
	artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
	key: string
): Readonly<Record<string, unknown>> {
	const freshness = host.freshness(artifact, key);
	return Object.freeze({
		...replicaClientRequestExtensions(artifact).extensions,
		...(freshness === undefined ? {} : { gatewayFreshness: freshness })
	});
}
