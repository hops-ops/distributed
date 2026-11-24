import type {
	CacheEngine,
	CacheEngineSnapshot
} from '../../internal/cache-engine.js';
import type {
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticLayerInput,
	ReplicaDiagnosticReceiptInput,
	ReplicaDiagnosticsSink
} from '../diagnostics.js';
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
 * Host for diagnostics sync and event emission.
 */
export type DiagnosticsHost = {
	readonly engine: CacheEngine;
	readonly diagnostics: ReplicaDiagnosticsSink | undefined;
	readonly diagnosticLayers: Map<string, ReplicaDiagnosticLayerInput> | undefined;
	readonly optimisticReceipts: Map<string, OptimisticReceiptState>;
	readonly reportObserverError: (error: AggregateError) => void;
	getProtocolGeneration(): ProtocolGeneration | undefined;
	getProtocolGenerationSequence(): number;
};

/**
 * Hand the sink the current state. Records and indexes are one pass over the
 * whole cache, so they are computed when the sink first reads them, from the
 * cache as it is then; a sink nobody is looking at costs a write nothing.
 */
export function syncDiagnostics(host: DiagnosticsHost): void {
	const diagnostics = host.diagnostics;
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
					host.reportObserverError,
					new AggregateError([error], 'replica diagnostics update failed')
				);
			}
			return value;
		};
	};
	try {
		let extracted: CacheEngineSnapshot | undefined;
		const cache = () => (extracted ??= host.engine.extract());
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
		for (const layer of host.diagnosticLayers?.values() ?? []) {
			const receipt = host.optimisticReceipts.get(layer.id);
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
		const scope = host.getProtocolGeneration();
		diagnostics.update(
			Object.freeze({
				scope:
					scope === undefined
						? Object.freeze({
								generation: host.getProtocolGenerationSequence(),
								established: false
							})
						: Object.freeze({
								generation: host.getProtocolGenerationSequence(),
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
					...(host.diagnosticLayers?.values() ?? [])
				]),
				receipts: Object.freeze(receipts)
			})
		);
	} catch (error) {
		reportSafely(
			host.reportObserverError,
			new AggregateError([error], 'replica diagnostics update failed')
		);
	}
}

export function diagnosticEvent(
	host: DiagnosticsHost,
	event: ReplicaDiagnosticEventInput
): void {
	if (host.diagnostics === undefined) return;
	try {
		host.diagnostics.event(event);
	} catch (error) {
		reportSafely(
			host.reportObserverError,
			new AggregateError([error], 'replica diagnostics event failed')
		);
	}
}

export function diagnosticOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	host: DiagnosticsHost,
	artifact: ReplicaOperationArtifact<TData, TVariables>
): void {
	if (host.diagnostics === undefined) return;
	try {
		host.diagnostics.operation(artifact);
	} catch (error) {
		reportSafely(
			host.reportObserverError,
			new AggregateError([error], 'replica diagnostic artifact failed')
		);
	}
}

export function diagnosticScopeTransition(
	host: DiagnosticsHost,
	previous: ProtocolGeneration | undefined,
	next: ProtocolGeneration
): void {
	if (host.diagnostics === undefined) return;
	if (
		previous !== undefined &&
		previous.cacheScope === next.cacheScope &&
		previous.schemaHash === next.schemaHash &&
		previous.authorizationGeneration === next.authorizationGeneration
	) {
		return;
	}
	diagnosticEvent(
		host,
		Object.freeze({
			kind: 'scope',
			action: previous === undefined ? 'established' : 'changed',
			generation: host.getProtocolGenerationSequence(),
			schemaHash: next.schemaHash
		})
	);
}

export function retireDiagnosticLayer(
	host: DiagnosticsHost,
	id: string,
	action: 'retired' | 'rejected',
	receiptState: 'atomic' | 'rejected',
	receipt?: OptimisticReceiptState
): void {
	const layers = host.diagnosticLayers;
	const removed = layers?.get(id);
	if (removed === undefined) return;
	layers!.delete(id);
	diagnosticEvent(
		host,
		Object.freeze({
			kind: 'layer',
			layer: id,
			action,
			recordChanges: removed.recordChanges,
			indexChanges: removed.indexChanges
		})
	);
	const causal = receipt ?? host.optimisticReceipts.get(id);
	const counts = diagnosticReceiptCounts(causal);
	diagnosticEvent(
		host,
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
		diagnosticEvent(
			host,
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
