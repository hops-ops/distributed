import type { GqlError, GraphqlVariables } from '../../types.js';
import type { DistributedProtocolEnvelope } from '../../protocol.js';
import { backoffDelay, type Scheduler } from '../../lib/backoff.js';
import type {
	ReplicaOperationArtifact,
	ReplicaResultEnvelope,
	ReplicaTransport,
	ReplicaWriteSource
} from '../types.js';
import {
	EMPTY_ERRORS,
	LIVE_FAILURE_RETRY_BASE_MS,
	LIVE_FAILURE_RETRY_JITTER,
	LIVE_FAILURE_RETRY_MAX_MS
} from './constants.js';
import {
	graphqlError,
	isGraphqlFailurePayload,
	replicaClientRequestExtensions,
	stableErrors
} from './helpers.js';
import type { ReplicaAuthority } from './authority.js';
import type { DiagnosticsReporter } from './diagnostics.js';
import type { OperationProtocols } from './operation-protocols.js';
import type { RecordClocks } from './record-clocks.js';
import type { ReplicaLiveConnection } from '../types.js';
import type { LiveEntry, QueryState } from './types.js';
import type { ReplicaWatchState } from './watch.js';

/**
 * Whether a live stream ended for a reason a later subscription can outlive.
 * The transport classifies what the server sent (`GraphqlWsError.retryable`);
 * an error it did not mark (a refused credential, a protocol violation, a
 * rejected operation) is terminal and never reopened here.
 */
function liveErrorIsRetryable(error: unknown): boolean {
	return (
		error !== null &&
		typeof error === 'object' &&
		(error as { retryable?: unknown }).retryable === true
	);
}

type Watch = ReplicaWatchState<unknown, GraphqlVariables>;

/** Admit one transport result into the replica; returns its envelope. */
type WriteResult = <TData, TVariables extends GraphqlVariables>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	stableVariables: TVariables,
	envelope: ReplicaResultEnvelope<TData>,
	source: ReplicaWriteSource,
	requestRevision?: string,
	responseProjectionGeneration?: number
) => DistributedProtocolEnvelope;

/**
 * The watches of every operation and the transport work done for them: the
 * deduplicated HTTP request per operation, its live subscription with reopen
 * backoff, and the fetching/error/live state watches render.
 *
 * It fences each response against the authorization generation and the
 * operation generation current when the request began, then hands it to
 * `write`.
 */
export class LiveCoordinator {
	readonly #transport: ReplicaTransport | undefined;
	readonly #scheduler: Scheduler;
	readonly #random: (() => number) | undefined;
	readonly #authority: ReplicaAuthority;
	readonly #operations: OperationProtocols;
	readonly #clocks: RecordClocks;
	readonly #diagnostics: DiagnosticsReporter;
	readonly #write: WriteResult;
	readonly #freshness: (
		artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
		key: string
	) => Readonly<Record<string, unknown>> | undefined;
	readonly #watches = new Map<string, Set<Watch>>();
	readonly #queryStates = new Map<string, QueryState>();
	readonly #inFlight = new Map<string, Promise<void>>();
	readonly #inFlightAborts = new Map<string, AbortController>();
	readonly #lives = new Map<string, LiveEntry>();
	readonly #connectionListeners = new Set<
		(connection: ReplicaLiveConnection) => void
	>();
	#connection: ReplicaLiveConnection = 'online';

	constructor(options: {
		transport: ReplicaTransport | undefined;
		scheduler: Scheduler;
		random: (() => number) | undefined;
		authority: ReplicaAuthority;
		operations: OperationProtocols;
		clocks: RecordClocks;
		diagnostics: DiagnosticsReporter;
		write: WriteResult;
		/** The causal delivery context a request for this operation carries. */
		freshness: (
			artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
			key: string
		) => Readonly<Record<string, unknown>> | undefined;
	}) {
		this.#transport = options.transport;
		this.#scheduler = options.scheduler;
		this.#random = options.random;
		this.#authority = options.authority;
		this.#operations = options.operations;
		this.#clocks = options.clocks;
		this.#diagnostics = options.diagnostics;
		this.#write = options.write;
		this.#freshness = options.freshness;
	}

	addWatch(watch: Watch): void {
		let watches = this.#watches.get(watch.key);
		if (!watches) {
			watches = new Set();
			this.#watches.set(watch.key, watches);
		}
		watches.add(watch);
	}

	/** `reconnecting` while a watched operation is reopening a lost stream. */
	get connection(): ReplicaLiveConnection {
		return this.#connection;
	}

	subscribeConnection(
		listener: (connection: ReplicaLiveConnection) => void
	): () => void {
		this.#connectionListeners.add(listener);
		return () => {
			this.#connectionListeners.delete(listener);
		};
	}

	#noteConnection(): void {
		let connection: ReplicaLiveConnection = 'online';
		for (const key of this.#watches.keys()) {
			if (this.#queryStates.get(key)?.live === 'reconnecting') {
				connection = 'reconnecting';
				break;
			}
		}
		if (connection === this.#connection) return;
		this.#connection = connection;
		for (const listener of [...this.#connectionListeners]) {
			try {
				listener(connection);
			} catch {
				// A listener's failure is its own; the others still hear.
			}
		}
	}

	removeWatch(watch: Watch): void {
		const watches = this.#watches.get(watch.key);
		watches?.delete(watch);
		if (watches?.size === 0) this.#watches.delete(watch.key);
		this.#noteConnection();
	}

	/** One watch of every watched operation. */
	*watchedOperations(): IterableIterator<Watch> {
		for (const watches of this.#watches.values()) {
			const watch = watches.values().next().value;
			if (watch !== undefined) yield watch;
		}
	}

	request(key: string): Promise<void> | undefined {
		return this.#inFlight.get(key);
	}

	/** An operation's query state, created on first use. */
	state(key: string): QueryState {
		let state = this.#queryStates.get(key);
		if (!state) {
			state = { fetching: false, errors: EMPTY_ERRORS, live: 'off' };
			this.#queryStates.set(key, state);
		}
		return state;
	}

	/** Whether an operation's live stream is open and delivering frames. */
	streaming(key: string): boolean {
		return this.#queryStates.get(key)?.live === 'active';
	}

	/** The query state an operation has, if anything created one. */
	peekState(key: string): QueryState | undefined {
		return this.#queryStates.get(key);
	}

	clearStates(): void {
		this.#queryStates.clear();
		this.#noteConnection();
	}

	/** Tell an operation's watches its state changed. */
	emit(key: string, allowFetch: boolean): void {
		for (const watch of this.#watches.get(key) ?? []) {
			watch._stateChanged(allowFetch);
		}
		this.#noteConnection();
	}

	/** Tell every watch its state changed; each may start a request. */
	refreshWatches(): void {
		for (const key of this.#watches.keys()) this.emit(key, true);
	}

	/** Abandon a collected operation: its request and its query state. */
	forget(key: string): void {
		const controller = this.#inFlightAborts.get(key);
		this.#inFlight.delete(key);
		this.#inFlightAborts.delete(key);
		controller?.abort();
		this.#queryStates.delete(key);
		this.#noteConnection();
	}

	/** The local revision at which an operation's open stream started. */
	liveStartRevision(key: string): string | undefined {
		return this.#lives.get(key)?.startRevision;
	}

	/**
	 * Remember the shared indexes whose independent owner disagreed with a
	 * stream's last frame.
	 */
	fenceLive(key: string, indexKeys: ReadonlySet<string> | undefined): void {
		const entry = this.#lives.get(key);
		if (entry !== undefined) entry.fencedIndexKeys = indexKeys;
	}

	/**
	 * Reopen live streams whose last frame was fenced by a shared index that
	 * another operation has now rewritten, so a fresh authoritative result is
	 * compared against the new owner membership.
	 */
	reopenFencedBy(writerKey: string, writtenKeys: readonly string[]): void {
		if (writtenKeys.length === 0) return;
		const reopen: string[] = [];
		for (const [liveKey, entry] of this.#lives) {
			if (liveKey === writerKey || !entry.active) continue;
			const fenced = entry.fencedIndexKeys;
			if (fenced === undefined) continue;
			if (writtenKeys.some((indexKey) => fenced.has(indexKey))) {
				reopen.push(liveKey);
			}
		}
		for (const liveKey of reopen) this.restartLive(liveKey);
	}

	sizes(): Record<string, number> {
		return {
			queryStates: this.#queryStates.size,
			watches: this.#watches.size,
			lives: this.#lives.size,
			inFlight: this.#inFlight.size,
			inFlightAborts: this.#inFlightAborts.size
		};
	}

	closeTransports(): void {
		for (const controller of this.#inFlightAborts.values()) controller.abort();
		this.#inFlightAborts.clear();
		for (const [key, entry] of this.#lives) {
			this.#operations.retireLive(key);
			entry.active = false;
			this.#cancelRetry(entry);
			try {
				entry.unsubscribe();
			} catch {
				// The generation fence is already closed; transport cleanup is best effort.
			}
		}
		this.#lives.clear();
		this.#inFlight.clear();
	}

	fetch<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>,
		force: boolean
	): Promise<void> {
		if (!this.#transport) return Promise.resolve();
		if (!force && watch.materialized.complete && !watch.materialized.stale) {
			if (this.#diagnostics.enabled) {
				this.#diagnostics.event(
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
		const existing = this.#inFlight.get(watch.key);
		if (existing) {
			if (this.#diagnostics.enabled) {
				this.#diagnostics.event(
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

		const state = this.state(watch.key);
		if (this.#diagnostics.enabled) {
			this.#diagnostics.event(
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
		this.emit(watch.key, false);
		const operationGeneration = this.#operations.generation(watch.key);
		const authorizationGeneration = this.#authority.sequence;
		const projectionGeneration = this.#clocks.projectionGeneration;
		/*
		 * Reserve local index ordering and the projection-fence generation when
		 * the request starts, not when it finishes. Distinct operation artifacts
		 * may share the same semantic index key; a slower earlier request must
		 * not replace a later-started result merely because its response arrived
		 * last, nor may it supersede a direct projection installed in between.
		 */
		const requestRevision = this.#operations.allocateIndexRevision();
		const controller = new AbortController();
		const request = Object.freeze({
			operation: 'query' as const,
			operationId: watch.artifact.id,
			document: watch.artifact.document,
			variables: watch.variables,
			artifact: watch.artifact,
			extensions: this.#requestExtensions(watch.artifact, watch.key),
			signal: controller.signal
		});
		let flight: Promise<void>;
		flight = Promise.resolve()
			.then(() => this.#transport!.fetch(request))
			.then((result) => {
				if (this.#authority.sequence !== authorizationGeneration) {
					if (this.#diagnostics.enabled) {
						this.#diagnostics.event(
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
				if (this.#inFlight.get(watch.key) !== flight) {
					if (this.#diagnostics.enabled) {
						this.#diagnostics.event(
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
				if (this.#operations.generation(watch.key) !== operationGeneration) {
					if (this.#diagnostics.enabled) {
						this.#diagnostics.event(
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
				this.#write(
					watch.artifact,
					watch.variables,
					result,
					'network',
					requestRevision,
					projectionGeneration
				);
			})
			.catch((error: unknown) => {
				if (this.#inFlight.get(watch.key) !== flight) return;
				if (controller.signal.aborted) return;
				// The reopen owns recovery: it refreshes again once it is back.
				if (state.live === 'reconnecting') return;
				state.errors = stableErrors(state.errors, [graphqlError(error)]);
			})
			.finally(() => {
				if (this.#inFlight.get(watch.key) !== flight) return;
				this.#inFlight.delete(watch.key);
				this.#inFlightAborts.delete(watch.key);
				state.fetching = false;
				this.emit(watch.key, false);
			});
		this.#inFlight.set(watch.key, flight);
		this.#inFlightAborts.set(watch.key, controller);
		return flight;
	}

	retainLive<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>
	): void {
		if (!watch.artifact.live || !this.#transport?.subscribe) return;
		const existing = this.#lives.get(watch.key);
		if (existing) {
			existing.count += 1;
			return;
		}
		this.#operations.reviveLive(watch.key);
		const state = this.state(watch.key);
		// A reopened stream is reconnecting until it delivers again.
		if (state.live !== 'reconnecting') state.live = 'connecting';
		const entry: LiveEntry = {
			count: 1,
			unsubscribe: () => undefined,
			active: true,
			protocolGeneration: this.#authority.sequence,
			startRevision: this.#operations.allocateIndexRevision()
		};
		this.#lives.set(watch.key, entry);
		const resume = this.#operations.resumeCursors(watch.key);
		try {
			const unsubscribe = this.#transport.subscribe(
				Object.freeze({
					operation: 'live' as const,
					operationId: watch.artifact.live.id,
					document: watch.artifact.live.document,
					variables: watch.variables,
					artifact: watch.artifact,
					extensions: this.#requestExtensions(watch.artifact, watch.key),
					...(resume === undefined || resume.length === 0
						? {}
						: { resume })
				}),
				{
					next: (result) => {
						// A live frame begins at this synchronous ingress boundary.
						const projectionGeneration = this.#clocks.projectionGeneration;
						if (
							entry.protocolGeneration !== this.#authority.sequence
						) {
							if (this.#diagnostics.enabled) {
								this.#diagnostics.event(
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
								this.#operations.generation(watch.key)
						) {
							if (this.#diagnostics.enabled) {
								this.#diagnostics.event(
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
							this.#lives.get(watch.key) !== entry
						) {
							if (this.#diagnostics.enabled) {
								this.#diagnostics.event(
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
						try {
							const distributed = this.#write(
								watch.artifact,
								watch.variables,
								result,
								'live',
								undefined,
								projectionGeneration
							);
							// Accepted without a snapshot, an error-only frame is the
							// receipt-only failure frame: it admitted nothing.
							if (
								distributed.snapshot === undefined &&
								isGraphqlFailurePayload(result)
							) {
								this.#failLive(watch, entry, result.errors);
								return;
							}
							const reconnected = state.live === 'reconnecting';
							state.live = 'active';
							entry.operationGeneration =
								this.#operations.generation(watch.key);
							entry.failures = undefined;
							if (reconnected) this.emit(watch.key, false);
						} catch (error) {
							state.live = 'error';
							state.errors = stableErrors(state.errors, [graphqlError(error)]);
							this.emit(watch.key, false);
						}
					},
					error: (error) => {
						if (!entry.active || this.#lives.get(watch.key) !== entry) return;
						if (liveErrorIsRetryable(error)) {
							// Lost, not failed: keep the data, reopen, and say so once
							// through the replica's live connection.
							this.#failLive(watch, entry, undefined);
							return;
						}
						this.#operations.retireLive(watch.key);
						entry.active = false;
						this.#lives.delete(watch.key);
						const unsub = entry.unsubscribe;
						entry.unsubscribe = () => undefined;
						try {
							unsub();
						} catch {
							// The terminal stream is fenced; cleanup is best effort.
						}
						state.live = 'error';
						state.errors = stableErrors(state.errors, [graphqlError(error)]);
						this.emit(watch.key, false);
					},
					complete: () => {
						if (!entry.active || this.#lives.get(watch.key) !== entry) return;
						this.#fallbackFromLive(watch, entry);
					}
				}
			);
			entry.unsubscribe = unsubscribe;
			if (!entry.active || this.#lives.get(watch.key) !== entry) {
				entry.unsubscribe = () => undefined;
				try {
					unsubscribe();
				} catch {
					// A closed/superseded subscription is already fenced.
				}
				return;
			}
			if (state.live !== 'reconnecting') state.live = 'active';
		} catch (error) {
			this.#operations.retireLive(watch.key);
			entry.active = false;
			this.#lives.delete(watch.key);
			state.live = 'error';
			state.errors = stableErrors(state.errors, [graphqlError(error)]);
		}
		this.emit(watch.key, false);
	}

	#fallbackFromLive<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>,
		entry: LiveEntry
	): void {
		if (this.#lives.get(watch.key) !== entry) {
			void this.fetch(watch, true);
			return;
		}
		this.#operations.retireLive(watch.key);
		this.#operations.releaseLiveOwnership(watch.key);
		entry.active = false;
		const unsubscribe = entry.unsubscribe;
		entry.unsubscribe = () => undefined;
		try {
			unsubscribe();
		} catch {
			// The inactive stream is fenced; transport cleanup is best effort.
		}
		const state = this.state(watch.key);
		state.live = 'off';
		this.emit(watch.key, false);
		const authorizationGeneration = this.#authority.sequence;
		const supersededFlight =
			entry.operationGeneration === undefined
				? undefined
				: this.#inFlight.get(watch.key);
		const refresh = (): void => {
			if (
				this.#authority.sequence !== authorizationGeneration ||
				this.#lives.get(watch.key) !== entry ||
				entry.active
			) {
				return;
			}
			void this.fetch(watch, true);
		};
		/*
		 * Keep the inactive entry as an authorization-generation-scoped
		 * sentinel. Query ingestion calls resumeLiveWatches(); deleting this
		 * entry would otherwise reopen a completed stream
		 * immediately. Authorization invalidation clears it and may retry.
		 *
		 * An accepted live frame advances the operation generation. Any HTTP
		 * request that was already running is therefore doomed by its response
		 * fence; drain it before starting the authoritative fallback. A stream
		 * that completed before its first frame can reuse the in-flight query.
		 */
		if (supersededFlight === undefined) {
			refresh();
		} else {
			void supersededFlight.then(refresh, refresh);
		}
	}

	restartLive(key: string): void {
		const previous = this.#lives.get(key);
		if (previous === undefined) return;
		const count = previous.count;
		this.#operations.retireLive(key);
		previous.active = false;
		this.#cancelRetry(previous);
		this.#lives.delete(key);
		try {
			previous.unsubscribe();
		} catch {
			// The old generation is already fenced; cleanup is best effort.
		}
		const watch = [...(this.#watches.get(key) ?? [])].find(
			(candidate) => candidate.liveRequested
		);
		if (watch === undefined) {
			this.state(key).live = 'off';
			this.#noteConnection();
			return;
		}
		this.retainLive(watch);
		const replacement = this.#lives.get(key);
		if (replacement !== undefined) replacement.count = count;
	}

	resumeLive(): void {
		if (this.#authority.scope === undefined) return;
		for (const [key, watches] of this.#watches) {
			if (this.#lives.has(key)) continue;
			const liveWatches = [...watches].filter(
				(watch) => watch.liveRequested
			);
			const first = liveWatches[0];
			if (first === undefined) continue;
			this.retainLive(first);
			const entry = this.#lives.get(key);
			if (entry !== undefined) entry.count = liveWatches.length;
		}
	}

	releaseLive(key: string): void {
		const entry = this.#lives.get(key);
		if (!entry) return;
		entry.count -= 1;
		if (entry.count > 0) return;
		this.#operations.retireLive(key);
		entry.active = false;
		this.#cancelRetry(entry);
		this.#lives.delete(key);
		entry.unsubscribe();
		this.state(key).live = 'off';
		this.emit(key, false);
	}

	/**
	 * Surface a failed live stream and reopen it after a backoff
	 * (`docs/live-query-delivery.md`). Both a failure frame and a retryable
	 * transport error end here: neither admitted anything, so no data, cursor,
	 * operation generation or ownership changed. The inactive entry stays
	 * registered, as in fallbackFromLive, so ingestion's resumeLiveWatches cannot
	 * bypass the backoff; the timer reopens the operation's current watches.
	 *
	 * Frames may have been missed while the stream was down. The reopen
	 * therefore also reads the operation over HTTP, started before the new
	 * stream so the stream may take the result over. Whichever answers first is
	 * admitted: an admitted live frame fences the HTTP response by operation
	 * generation, and a live frame behind an admitted HTTP result is ordered by
	 * its index and record clocks like any other.
	 */
	#failLive<TData, TVariables extends GraphqlVariables>(
		watch: ReplicaWatchState<TData, TVariables>,
		entry: LiveEntry,
		/** The failure frame's errors; absent when the stream was lost. */
		errors: readonly GqlError[] | undefined
	): void {
		if (!entry.active || this.#lives.get(watch.key) !== entry) return;
		const key = watch.key;
		this.#operations.retireLive(key);
		entry.active = false;
		const unsubscribe = entry.unsubscribe;
		entry.unsubscribe = () => undefined;
		try {
			unsubscribe();
		} catch {
			// The failed stream is fenced; transport cleanup is best effort.
		}
		const failures = (entry.failures ?? 0) + 1;
		entry.failures = failures;
		const delay = backoffDelay(failures, {
			baseMs: LIVE_FAILURE_RETRY_BASE_MS,
			maxMs: LIVE_FAILURE_RETRY_MAX_MS,
			jitter: LIVE_FAILURE_RETRY_JITTER,
			...(this.#random === undefined ? {} : { random: this.#random })
		});
		entry.retry = this.#scheduler.setTimeout(() => {
			entry.retry = undefined;
			if (
				this.#lives.get(key) !== entry ||
				entry.active ||
				entry.protocolGeneration !== this.#authority.sequence
			) {
				return;
			}
			this.#refreshLiveWatch(key);
			this.restartLive(key);
			const replacement = this.#lives.get(key);
			if (replacement !== undefined && replacement !== entry) {
				replacement.failures = failures;
			}
		}, delay);
		const state = this.state(key);
		if (errors === undefined) {
			state.live = 'reconnecting';
		} else {
			state.live = 'error';
			state.errors = stableErrors(state.errors, errors);
		}
		this.emit(key, false);
	}

	/**
	 * Read a live operation over HTTP for its current watches. A request that is
	 * already running may predate an admitted live frame, which fences its
	 * response, so the authoritative read starts once that request has settled.
	 */
	#refreshLiveWatch(key: string): void {
		const authorizationGeneration = this.#authority.sequence;
		const refresh = (): void => {
			if (this.#authority.sequence !== authorizationGeneration) return;
			const watch = [...(this.#watches.get(key) ?? [])].find(
				(candidate) => candidate.liveRequested
			);
			if (watch !== undefined) void this.fetch(watch, true);
		};
		const running = this.#inFlight.get(key);
		if (running === undefined) {
			refresh();
		} else {
			void running.then(refresh, refresh);
		}
	}

	#cancelRetry(entry: LiveEntry): void {
		if (entry.retry === undefined) return;
		this.#scheduler.clearTimeout(entry.retry);
		entry.retry = undefined;
	}

	#requestExtensions(
		artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
		key: string
	): Readonly<Record<string, unknown>> {
		const freshness = this.#freshness(artifact, key);
		return Object.freeze({
			...replicaClientRequestExtensions(artifact).extensions,
			...(freshness === undefined ? {} : { gatewayFreshness: freshness })
		});
	}
}
