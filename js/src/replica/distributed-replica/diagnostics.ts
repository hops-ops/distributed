import type {
	CacheEngine,
	CacheEngineSnapshot
} from '../../internal/cache-engine.js';
import type {
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticLayerInput,
	ReplicaDiagnosticReceiptInput,
	ReplicaDiagnosticsSink
} from '../diagnostics/types.js';
import type { GraphqlVariables } from '../../types.js';
import type { ReplicaOperationArtifact, ReplicaValue } from '../types.js';
import { modelFromRecordKey } from './clocks.js';
import {
	diagnosticReceiptCounts,
	diagnosticReceiptExpectations
} from './optimistic.js';
import { reportSafely } from './helpers.js';
import type {
	OptimisticReceiptState,
	ProtocolGeneration
} from './types.js';

/**
 * The diagnostics sink and the optimistic layers it mirrors. Without a sink
 * every method returns at once and no layer is kept.
 */
export class DiagnosticsReporter {
	readonly #sink: ReplicaDiagnosticsSink | undefined;
	readonly #engine: CacheEngine;
	readonly #reportObserverError: (error: AggregateError) => void;
	readonly #scope: () => ProtocolGeneration | undefined;
	readonly #generation: () => number;
	readonly #receipt: (id: string) => OptimisticReceiptState | undefined;
	readonly #layers: Map<string, ReplicaDiagnosticLayerInput> | undefined;
	#layerSequence = 0;

	constructor(options: {
		sink: ReplicaDiagnosticsSink | undefined;
		engine: CacheEngine;
		reportObserverError: (error: AggregateError) => void;
		/** The established authoritative scope, read at each sync. */
		scope: () => ProtocolGeneration | undefined;
		generation: () => number;
		/** The causal receipt of one optimistic layer, read at each sync. */
		receipt: (id: string) => OptimisticReceiptState | undefined;
	}) {
		this.#sink = options.sink;
		this.#engine = options.engine;
		this.#reportObserverError = options.reportObserverError;
		this.#scope = options.scope;
		this.#generation = options.generation;
		this.#receipt = options.receipt;
		this.#layers = options.sink === undefined ? undefined : new Map();
	}

	get enabled(): boolean {
		return this.#sink !== undefined;
	}

	/**
	 * Hand the sink the current state. Records and indexes are one pass over the
	 * whole cache, so they are computed when the sink first reads them, from the
	 * cache as it is then; a sink nobody is looking at costs a write nothing.
	 */
	sync(): void {
		const diagnostics = this.#sink;
		if (diagnostics === undefined) return;
		const whenRead = <T>(compute: () => readonly T[]) => {
			let value: readonly T[] | undefined;
			return () => {
				if (value !== undefined) return value;
				try {
					value = Object.freeze(compute());
				} catch (error) {
					value = Object.freeze([]);
					reportSafely(
						this.#reportObserverError,
						new AggregateError([error], 'replica diagnostics update failed')
					);
				}
				return value;
			};
		};
		try {
			let extracted: CacheEngineSnapshot | undefined;
			const cache = () => (extracted ??= this.#engine.extract());
			const records = whenRead(() =>
				cache().records.map((record) => {
					const model = modelFromRecordKey(record.key);
					const tombstone = record.tombstoneRevision !== undefined;
					const values: Record<string, ReplicaValue> = {};
					if (!tombstone && diagnostics.redactRecordValue !== undefined) {
						for (const [field, entry] of Object.entries(record.fields)) {
							const value = diagnostics.redactRecordValue(
								Object.freeze({
									recordKey: record.key,
									...(model === undefined ? {} : { model }),
									field,
									kind: 'field' as const
								}),
								entry.value as ReplicaValue
							);
							if (value !== undefined) values[field] = value;
						}
						for (const [field, entry] of Object.entries(record.links)) {
							const value = diagnostics.redactRecordValue(
								Object.freeze({
									recordKey: record.key,
									...(model === undefined ? {} : { model }),
									field,
									kind: 'link' as const
								}),
								entry.value as ReplicaValue
							);
							if (value !== undefined) values[field] = value;
						}
					}
					return Object.freeze({
						key: record.key,
						...(model === undefined ? {} : { model }),
						revision: record.revision,
						incarnation: record.incarnation ?? record.revision,
						tombstone,
						...(record.tombstoneRevision === undefined
							? {}
							: { tombstoneRevision: record.tombstoneRevision }),
						presentFields: Object.freeze(
							tombstone ? [] : Object.keys(record.fields).sort()
						),
						presentLinks: Object.freeze(
							tombstone ? [] : Object.keys(record.links).sort()
						),
						...(Object.keys(values).length === 0
							? {}
							: { values: Object.freeze(values) })
					});
				})
			);
			const indexes = whenRead(() =>
				cache().indexes.map((index) =>
					Object.freeze({
						key: index.key,
						revision: index.revision,
						...(index.staleRevision === undefined
							? {}
							: { staleRevision: index.staleRevision }),
						records: index.records,
						complete: index.complete,
						deleted: index.deleted,
						...(index.metadata === undefined
							? {}
							: {
									field: index.metadata.field,
									...(index.metadata.parent === undefined
										? {}
										: { parent: index.metadata.parent }),
									argumentNames: Object.freeze(
										Object.keys(index.metadata.arguments).sort()
									),
									...(diagnostics.includeStructuralIdentities
										? { arguments: index.metadata.arguments }
										: {}),
									coverage: index.metadata.coverage,
									dependencies: index.metadata.dependencies,
									...(index.metadata.staleReason === undefined
										? {}
										: { staleReason: index.metadata.staleReason }),
									nullValue: index.metadata.nullValue === true
								})
					})
				)
			);
			const receipts: ReplicaDiagnosticReceiptInput[] = [];
			for (const layer of this.#layers?.values() ?? []) {
				const receipt = this.#receipt(layer.id);
				receipts.push(
					Object.freeze({
						commandId: layer.id,
						state:
							receipt === undefined
								? ('optimistic' as const)
								: receipt.expectations.size === 0
									? ('succeeded' as const)
									: ('succeeded_pending_projection' as const),
						expectations:
							receipt === undefined
								? Object.freeze([])
								: diagnosticReceiptExpectations(receipt)
					})
				);
			}
			const scope = this.#scope();
			diagnostics.update(
				Object.freeze({
					scope:
						scope === undefined
							? Object.freeze({
									generation: this.#generation(),
									established: false
								})
							: Object.freeze({
									generation: this.#generation(),
									established: true,
									protocolVersion: 1 as const,
									schemaHash: scope.schemaHash
								}),
					get records() {
						return records();
					},
					get indexes() {
						return indexes();
					},
					layers: Object.freeze([
						...(this.#layers?.values() ?? [])
					]),
					receipts: Object.freeze(receipts)
				})
			);
		} catch (error) {
			reportSafely(
				this.#reportObserverError,
				new AggregateError([error], 'replica diagnostics update failed')
			);
		}
	}

	event(event: ReplicaDiagnosticEventInput): void {
		if (this.#sink === undefined) return;
		try {
			this.#sink.event(event);
		} catch (error) {
			reportSafely(
				this.#reportObserverError,
				new AggregateError([error], 'replica diagnostics event failed')
			);
		}
	}

	operation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void {
		if (this.#sink === undefined) return;
		try {
			this.#sink.operation(artifact);
		} catch (error) {
			reportSafely(
				this.#reportObserverError,
				new AggregateError([error], 'replica diagnostic artifact failed')
			);
		}
	}

	scopeTransition(
		previous: ProtocolGeneration | undefined,
		next: ProtocolGeneration
	): void {
		if (this.#sink === undefined) return;
		if (
			previous !== undefined &&
			previous.cacheScope === next.cacheScope &&
			previous.schemaHash === next.schemaHash &&
			previous.authorizationGeneration === next.authorizationGeneration
		) {
			return;
		}
		this.event(
			Object.freeze({
				kind: 'scope',
				action: previous === undefined ? 'established' : 'changed',
				generation: this.#generation(),
				schemaHash: next.schemaHash
			})
		);
	}

	retireLayer(
		id: string,
		action: 'retired' | 'rejected',
		receiptState: 'atomic' | 'rejected',
		receipt: OptimisticReceiptState | undefined
	): void {
		const layers = this.#layers;
		const removed = layers?.get(id);
		if (removed === undefined) return;
		layers!.delete(id);
		this.event(
			Object.freeze({
				kind: 'layer',
				layer: id,
				action,
				recordChanges: removed.recordChanges,
				indexChanges: removed.indexChanges
			})
		);
		const counts = diagnosticReceiptCounts(receipt);
		this.event(
			Object.freeze({
				kind: 'receipt',
				command: id,
				state: receiptState,
				obligations: counts.obligations,
				observed: counts.observed
			})
		);
		for (const layer of layers!.values()) {
			if (layer.sequence <= removed.sequence) continue;
			this.event(
				Object.freeze({
					kind: 'layer',
					layer: layer.id,
					action: 'rebased',
					recordChanges: layer.recordChanges,
					indexChanges: layer.indexChanges,
					reason: `${action}-earlier-layer`
				})
			);
		}
	}

	/** Mirror a new optimistic layer and announce it. */
	layerCreated(
		id: string,
		recordChanges: number,
		indexChanges: number,
		semanticChanges: number
	): void {
		if (this.#layers === undefined) return;
		this.#layerSequence += 1;
		this.#layers.set(
			id,
			Object.freeze({
				id,
				sequence: this.#layerSequence,
				state: 'optimistic' as const,
				recordChanges,
				indexChanges,
				semanticChanges
			})
		);
		this.event(
			Object.freeze({
				kind: 'layer',
				layer: id,
				action: 'created',
				recordChanges,
				indexChanges
			})
		);
		this.event(
			Object.freeze({
				kind: 'receipt',
				command: id,
				state: 'optimistic',
				obligations: 0,
				observed: 0
			})
		);
		this.sync();
	}

	/** Mark a mirrored layer accepted; `announce` also emits the layer event. */
	layerAccepted(id: string, announce: boolean): void {
		const layer = this.#layers?.get(id);
		if (layer === undefined) return;
		this.#layers!.set(
			id,
			Object.freeze({ ...layer, state: 'accepted' as const })
		);
		if (!announce) return;
		this.event(
			Object.freeze({
				kind: 'layer',
				layer: id,
				action: 'accepted',
				recordChanges: layer.recordChanges,
				indexChanges: layer.indexChanges
			})
		);
	}

	/** Emit the receipt progress of an accepted command. */
	receiptAccepted(id: string, receipt: OptimisticReceiptState | undefined): void {
		if (this.#sink === undefined) return;
		const counts = diagnosticReceiptCounts(receipt);
		this.event(
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

	clearLayers(): void {
		this.#layers?.clear();
		this.#layerSequence = 0;
	}
}
