import type { CacheValue } from '../../internal/cache-engine.js';
import {
	compareDistributedDecimal,
	DistributedProtocolError,
	type DistributedIndexRevision,
	type DistributedLiveCursor,
	type DistributedProtocolEnvelope,
	type DistributedQuerySnapshot,
	type DistributedRecordRevision
} from '../../protocol.js';
import type { ReplicaValue } from '../types.js';
import type {
	IndexDisposition,
	IndexProtocolClock,
	OperationProtocolState,
	ProjectedRecordFence,
	RecordProtocolClock
} from './types.js';

import { deepEqual } from '../../lib/deep-equal.js';

export function sameRecordRevision(
	left: DistributedRecordRevision,
	right: DistributedRecordRevision
): boolean {
	return (
		left.model === right.model &&
		left.scopeToken === right.scopeToken &&
		left.incarnation === right.incarnation &&
		left.revision === right.revision &&
		left.tombstone === right.tombstone
	);
}

export function recordKeyMatchesModel(recordKey: string, model: string): boolean {
	return recordKey.startsWith(`record:${encodeURIComponent(model)}:`);
}

export function modelFromRecordKey(recordKey: string): string | undefined {
	if (!recordKey.startsWith('record:')) return undefined;
	const separator = recordKey.indexOf(':', 'record:'.length);
	if (separator === -1) return undefined;
	try {
		const model = decodeURIComponent(
			recordKey.slice('record:'.length, separator)
		);
		return model.length === 0 ? undefined : model;
	} catch {
		return undefined;
	}
}

export function compareIndexVector(
	current: ReadonlyMap<string, IndexProtocolClock>,
	incoming: readonly DistributedIndexRevision[]
): IndexDisposition {
	if (current.size === 0) return 'fresh';
	if (current.size !== incoming.length) return 'incomparable';
	let lower = false;
	let higher = false;
	for (const evidence of incoming) {
		const previous = current.get(evidence.projection);
		if (
			previous === undefined ||
			previous.scopeToken !== evidence.scopeToken
		) {
			return 'incomparable';
		}
		const comparison = compareDistributedDecimal(
			evidence.position,
			previous.position
		);
		lower ||= comparison < 0;
		higher ||= comparison > 0;
	}
	if (lower && higher) return 'incomparable';
	if (lower) return 'lower';
	if (higher) return 'higher';
	return 'equal';
}

export function compareSnapshotToOperationState(
	state: OperationProtocolState,
	snapshot: DistributedQuerySnapshot
): IndexDisposition {
	if (state.snapshotScope === undefined) return 'fresh';
	if (state.snapshotScope !== snapshot.scopeToken) return 'incomparable';
	if (state.indexClocks.size === 0 || snapshot.indexes.length === 0) {
		return state.indexClocks.size === snapshot.indexes.length
			? 'equal'
			: 'incomparable';
	}
	return compareIndexVector(state.indexClocks, snapshot.indexes);
}

export function isComparableHandoffDisposition(
	disposition: IndexDisposition
): boolean {
	return (
		disposition === 'fresh' ||
		disposition === 'equal' ||
		disposition === 'higher'
	);
}

export function indexClockMap(
	indexes: readonly DistributedIndexRevision[]
): Map<string, IndexProtocolClock> {
	return new Map(
		indexes.map((index) => [
			index.projection,
			{
				scopeToken: index.scopeToken,
				position: index.position
			}
		])
	);
}

export function latestCursors(
	snapshot: DistributedQuerySnapshot,
	live: DistributedProtocolEnvelope['live']
): readonly DistributedLiveCursor[] {
	if (live !== undefined) {
		return live.mode === 'resumable' ? live.cursors : Object.freeze([]);
	}
	return Object.freeze(
		snapshot.indexes.flatMap((index) =>
			index.resume === undefined ? [] : [index.resume]
		)
	);
}

export function responsePathKey(path: readonly string[]): string {
	return JSON.stringify(path);
}

export function compareRecordClock(
	left: RecordProtocolClock,
	right: RecordProtocolClock
): -1 | 0 | 1 {
	const incarnation = compareDistributedDecimal(
		left.incarnation,
		right.incarnation
	);
	return incarnation === 0
		? compareDistributedDecimal(left.revision, right.revision)
		: incarnation;
}

export function compareEvidenceToProjectedFence(
	evidence: DistributedRecordRevision,
	fence: ProjectedRecordFence
): -1 | 0 | 1 | undefined {
	if (evidence.scopeToken !== fence.clock.scopeToken) return undefined;
	return compareRecordClock(
		{
			scopeToken: evidence.scopeToken,
			incarnation: evidence.incarnation,
			revision: evidence.revision,
			tombstone: evidence.tombstone
		},
		fence.clock
	);
}

export function sameRecordClock(
	left: RecordProtocolClock,
	right: RecordProtocolClock
): boolean {
	return (
		left.scopeToken === right.scopeToken &&
		left.incarnation === right.incarnation &&
		left.revision === right.revision &&
		left.tombstone === right.tombstone
	);
}

export function incrementCanonicalDecimal(value: string): string {
	const digits = [...value];
	let carry = true;
	for (let index = digits.length - 1; index >= 0 && carry; index -= 1) {
		const digit = digits[index]!;
		if (digit === '9') {
			digits[index] = '0';
		} else {
			digits[index] = '0123456789'[
				'0123456789'.indexOf(digit) + 1
			]!;
			carry = false;
		}
	}
	if (carry) digits.unshift('1');
	return digits.join('');
}

export function compareCanonicalDecimalStrings(
	left: string,
	right: string
): number {
	return left.length === right.length
		? left.localeCompare(right)
		: left.length < right.length
			? -1
			: 1;
}

export function protocolInvalid(path: string): never {
	throw new DistributedProtocolError(
		'DISTRIBUTED_PROTOCOL_INVALID',
		path
	);
}

export function freezeRecordClock(clock: RecordProtocolClock): RecordProtocolClock {
	return Object.freeze({
		scopeToken: clock.scopeToken,
		incarnation: clock.incarnation,
		revision: clock.revision,
		tombstone: clock.tombstone
	});
}

export function compareProjectedRecordFields(
	projected: Readonly<Record<string, ReplicaValue>>,
	incoming: Readonly<Record<string, CacheValue>>
): 'conflict' | 'partial' | 'complete' {
	let complete = true;
	for (const [field, value] of Object.entries(projected)) {
		if (!Object.prototype.hasOwnProperty.call(incoming, field)) {
			complete = false;
			continue;
		}
		if (!deepEqual(value, incoming[field])) return 'conflict';
	}
	return complete ? 'complete' : 'partial';
}

export { deepEqual };
