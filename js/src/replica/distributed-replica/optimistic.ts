import { compareCodeUnits } from '../../lib/compare-code-units.js';
import {
	type CacheIndexMetadata,
	type CacheValue,
	type OptimisticCacheWriter,
	type RecordLink
} from '../../internal/cache-engine.js';
import type {
	DistributedCommandMetadata,
	DistributedOpaqueString
} from '../../protocol.js';

import { cloneJsonValue } from '../identity/clone.js';
import { replicaRecordKey } from '../identity/keys.js';
import type { ReplicaIndexSemanticChange } from '../index-maintenance/types.js';
import type {
	ReplicaIdentity,
	ReplicaIndexTarget,
	ReplicaModelArtifact,
	ReplicaOptimisticWriter,
	ReplicaRecordPatch
} from '../types.js';
import { protocolInvalid } from './clocks.js';
import { indexKeyFromTarget, metadataFromTarget } from './helpers.js';
import type {
	CapturedReplicaOptimisticOperation,
	CapturedReplicaOptimisticUpdate,
	OptimisticReceiptState
} from './types.js';

export function optimisticReceiptState(
	command: DistributedCommandMetadata
): OptimisticReceiptState {
	const expectations = new Map<string, true>();
	for (const expectation of command.expects) {
		const key = expectationKey(expectation);
		if (expectations.has(key)) {
			protocolInvalid('extensions.distributed.command.expects');
		}
		expectations.set(key, true);
	}
	const observed = new Set<string>();
	for (const observation of command.observations) {
		if (observation.causationId !== command.causationId) {
			protocolInvalid('extensions.distributed.command.observations');
		}
		const key = expectationKey(observation);
		if (!expectations.has(key)) {
			protocolInvalid('extensions.distributed.command.observations');
		}
		observed.add(key);
	}
	return {
		causationId: command.causationId,
		expectations,
		observed
	};
}

export function cloneOptimisticReceipt(
	receipt: OptimisticReceiptState
): OptimisticReceiptState {
	return {
		causationId: receipt.causationId,
		expectations: new Map(receipt.expectations),
		observed: new Set(receipt.observed)
	};
}

export function sameReceipt(
	left: OptimisticReceiptState,
	right: OptimisticReceiptState
): boolean {
	return (
		left.causationId === right.causationId &&
		left.expectations.size === right.expectations.size &&
		[...left.expectations.keys()].every((key) =>
			right.expectations.has(key)
		)
	);
}

export function diagnosticReceiptExpectations(
	receipt: OptimisticReceiptState
): readonly {
	readonly projection: string;
	readonly model: string;
	readonly observed: boolean;
}[] {
	return Object.freeze(
		[...receipt.expectations.keys()]
			.map((key) => {
				const parsed = JSON.parse(key) as unknown;
				if (
					!Array.isArray(parsed) ||
					parsed.length !== 3 ||
					typeof parsed[0] !== 'string' ||
					typeof parsed[1] !== 'string'
				) {
					throw new TypeError('invalid internal projection expectation');
				}
				return Object.freeze({
					projection: parsed[0],
					model: parsed[1],
					observed: receipt.observed.has(key)
				});
			})
			.sort((left, right) =>
				compareCodeUnits(
					`${left.projection}\0${left.model}`,
					`${right.projection}\0${right.model}`
				)
			)
	);
}

export function diagnosticReceiptCounts(
	receipt: OptimisticReceiptState | undefined
): Readonly<{ obligations: number; observed: number }> {
	return Object.freeze({
		obligations: receipt?.expectations.size ?? 0,
		observed: receipt?.observed.size ?? 0
	});
}

export function expectationKey(value: {
	readonly projection: string;
	readonly model: string;
	readonly scopeToken: DistributedOpaqueString;
}): string {
	return JSON.stringify([value.projection, value.model, value.scopeToken]);
}

export function captureReplicaOptimisticUpdate(
	id: string,
	update: (writer: ReplicaOptimisticWriter) => void,
	semanticChanges: readonly ReplicaIndexSemanticChange[]
): CapturedReplicaOptimisticUpdate {
	if (!Array.isArray(semanticChanges)) {
		throw new TypeError('optimistic semantic changes must be an array');
	}
	const suppliedChanges = cloneJsonValue(
		semanticChanges
	) as unknown as readonly ReplicaIndexSemanticChange[];
	const operations: CapturedReplicaOptimisticOperation[] = [];
	const changes: ReplicaIndexSemanticChange[] = [];
	let active = true;
	const assertActive = () => {
		if (!active) throw new Error('replica optimistic writer is no longer active');
	};
	const writer: ReplicaOptimisticWriter = Object.freeze({
		writeRecord(
			model: ReplicaModelArtifact,
			identity: ReplicaIdentity,
			patch: ReplicaRecordPatch
		): void {
			assertActive();
			const key = replicaRecordKey(model, identity);
			const fields = cloneOptimisticFields(patch.fields);
			const links = cloneOptimisticLinks(patch.links);
			const unset = cloneOptimisticUnset(patch.unset, fields);
			if (
				Object.keys(fields).length === 0 &&
				Object.keys(links).length === 0 &&
				unset.length === 0
			) {
				return;
			}
			operations.push(
				Object.freeze({
					kind: 'write-record' as const,
					write: Object.freeze({
						key,
						fields,
						links,
						...(unset.length === 0 ? {} : { unset }),
						...(patch.ifPresent === true ? { ifPresent: true } : {})
					})
				})
			);
			changes.push(
				Object.freeze({
					kind: 'upsert' as const,
					model: model.id,
					key,
					fields,
					...(unset.length === 0 ? {} : { unset }),
					...(patch.ifPresent === true ? { ifPresent: true } : {})
				})
			);
		},
		tombstoneRecord(
			model: ReplicaModelArtifact,
			identity: ReplicaIdentity
		): void {
			assertActive();
			const key = replicaRecordKey(model, identity);
			operations.push(
				Object.freeze({ kind: 'tombstone-record' as const, key })
			);
			changes.push(
				Object.freeze({
					kind: 'delete' as const,
					model: model.id,
					key
				})
			);
		},
		writeIndex(target: ReplicaIndexTarget, records: readonly string[]): void {
			assertActive();
			if (!Array.isArray(records)) {
				throw new TypeError('index records must be an array');
			}
			const metadata = cloneJsonValue(
				metadataFromTarget(target)
			) as unknown as CacheIndexMetadata;
			operations.push(
				Object.freeze({
					kind: 'write-index' as const,
					write: Object.freeze({
						key: indexKeyFromTarget(target),
						records: Object.freeze([...records]),
						complete: target.complete ?? false,
						metadata
					})
				})
			);
		},
		deleteIndex(target: ReplicaIndexTarget): void {
			assertActive();
			operations.push(
				Object.freeze({
					kind: 'delete-index' as const,
					key: indexKeyFromTarget(target)
				})
			);
		}
	});
	try {
		const result = update(writer);
		assertReplicaOptimisticUpdateSynchronous(result);
	} finally {
		active = false;
	}
	changes.push(...suppliedChanges);
	return Object.freeze({
		operations: Object.freeze(operations),
		context: cloneJsonValue({
			id,
			changes
		}) as CacheValue
	});
}

export function replayReplicaOptimisticUpdate(
	writer: OptimisticCacheWriter,
	operations: readonly CapturedReplicaOptimisticOperation[]
): void {
	for (const operation of operations) {
		if (operation.kind === 'write-record') {
			writer.writeRecord(operation.write);
		} else if (operation.kind === 'tombstone-record') {
			writer.tombstoneRecord(operation.key);
		} else if (operation.kind === 'write-index') {
			writer.writeIndex(operation.write);
		} else {
			writer.deleteIndex(operation.key);
		}
	}
}

function cloneOptimisticFields(
	fields: ReplicaRecordPatch['fields']
): Readonly<Record<string, CacheValue>> {
	if (fields === undefined) return Object.freeze({});
	assertPlainReplicaRecord(fields, 'record fields');
	return Object.freeze(
		Object.fromEntries(
			Object.entries(fields).map(([name, value]) => {
				assertReplicaName(name, 'record field');
				return [name, cloneJsonValue(value) as CacheValue];
			})
		)
	);
}

function cloneOptimisticUnset(
	unset: ReplicaRecordPatch['unset'],
	fields: Readonly<Record<string, CacheValue>>
): readonly string[] {
	if (unset === undefined) return Object.freeze([]);
	if (!Array.isArray(unset)) throw new TypeError('record unset must be an array');
	const result = unset.map((name) => {
		assertReplicaName(name, 'record unset field');
		if (Object.hasOwn(fields, name)) {
			throw new TypeError('record field cannot be set and unset');
		}
		return name;
	});
	if (new Set(result).size !== result.length) {
		throw new TypeError('record unset fields must be unique');
	}
	return Object.freeze(result);
}

function cloneOptimisticLinks(
	links: ReplicaRecordPatch['links']
): Readonly<Record<string, RecordLink>> {
	if (links === undefined) return Object.freeze({});
	assertPlainReplicaRecord(links, 'record links');
	return Object.freeze(
		Object.fromEntries(
			Object.entries(links).map(([name, value]) => {
				assertReplicaName(name, 'record link');
				if (value === null) return [name, null];
				if (typeof value === 'string') {
					assertReplicaName(value, 'record key');
					return [name, value];
				}
				if (!Array.isArray(value)) {
					throw new TypeError(
						'record link must be a key, key array, or null'
					);
				}
				const keys = value.map((key) => {
					assertReplicaName(key, 'record key');
					return key;
				});
				return [name, Object.freeze(keys)];
			})
		)
	);
}

function assertPlainReplicaRecord(
	value: object,
	description: string
): void {
	const prototype = Object.getPrototypeOf(value);
	if (
		Array.isArray(value) ||
		(prototype !== Object.prototype && prototype !== null)
	) {
		throw new TypeError(`${description} must be a plain object`);
	}
}

export function assertReplicaOptimisticLayerId(id: string): void {
	assertReplicaName(id, 'optimistic layer id');
}

function assertReplicaName(value: string, description: string): void {
	if (typeof value !== 'string' || value.length === 0) {
		throw new TypeError(`${description} must be a non-empty string`);
	}
}

function assertReplicaOptimisticUpdateSynchronous(result: unknown): void {
	if (
		result !== null &&
		(typeof result === 'object' || typeof result === 'function') &&
		typeof (result as { then?: unknown }).then === 'function'
	) {
		void Promise.resolve(result).catch(() => undefined);
		throw new TypeError('optimistic layer update must be synchronous');
	}
}
