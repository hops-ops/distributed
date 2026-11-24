import type { CacheEngineSnapshot, RecordLink } from '../../internal/cache-engine.js';
import { freezeRecord } from '../../lib/freeze-record.js';
import { modelFromRecordKey } from '../distributed-replica/clocks.js';
import {
	exactRecord,
	nonEmptyString,
	nonNegativeInteger,
	parseDehydratedState,
	plainRecord,
	type ParsedDehydratedState
} from '../distributed-replica/hydration.js';
import type { SerializedOperationProtocolState } from '../distributed-replica/types.js';
import type {
	ReplicaAuthoritativeScope,
	ReplicaDehydratedState
} from '../types.js';
import type {
	ReplicaPersistenceModelPolicy,
	ReplicaPersistencePolicy
} from './types.js';

/** v2 adds the durable command outbox store. */
export const DATABASE_VERSION = 2;
export const ENTRY_FORMAT_VERSION = 1;
export const STORE_NAME = 'confirmed-replicas';
export const DEFAULT_DATABASE_NAME = '@hops-ops/distributed:replica';
const GRAPHQL_NAME = /^[_A-Za-z][_0-9A-Za-z]*$/;

export type StoredReplicaEntry = {
	readonly formatVersion: 1;
	readonly identity: string;
	readonly storedAt: number;
	readonly state: ReplicaDehydratedState;
};

export type NormalizedPolicy = ReadonlyMap<string, ReplicaPersistenceModelPolicy>;

export function normalizePolicy(
	policy: ReplicaPersistencePolicy | undefined
): NormalizedPolicy {
	if (policy === undefined) return new Map();
	const raw = exactRecord(policy, 'persistence policy', ['models']);
	const models = plainRecord(raw.models, 'persistence policy.models');
	const result = new Map<string, ReplicaPersistenceModelPolicy>();
	for (const [model, value] of Object.entries(models)) {
		if (!GRAPHQL_NAME.test(model)) {
			throw new TypeError(`invalid persistence policy model: ${model}`);
		}
		const entry = exactRecord(
			value,
			`persistence policy.models.${model}`,
			['retention', 'sensitive']
		);
		if (
			entry.retention !== 'persist-confirmed' &&
			entry.retention !== 'memory-only'
		) {
			throw new TypeError(
				`invalid persistence retention for model ${model}`
			);
		}
		if (typeof entry.sensitive !== 'boolean') {
			throw new TypeError(
				`persistence sensitivity for model ${model} must be boolean`
			);
		}
		result.set(
			model,
			Object.freeze({
				retention: entry.retention,
				sensitive: entry.sensitive
			})
		);
	}
	return result;
}

export function filterReplicaState(
	parsed: ParsedDehydratedState,
	policy: NormalizedPolicy
): ReplicaDehydratedState | undefined {
	const allowsModel = (model: string): boolean => {
		const decision = policy.get(model);
		return (
			decision?.retention === 'persist-confirmed' &&
			decision.sensitive === false
		);
	};
	const allowedRecord = (key: string): boolean => {
		const model = modelFromRecordKey(key);
		return model !== undefined && allowsModel(model);
	};

	const recordClocks = parsed.payload.recordClocks.filter(([key]) =>
		allowedRecord(key)
	);
	const recordClockKeys = new Set(recordClocks.map(([key]) => key));
	const initialRecords = parsed.payload.cache.records.filter((record) =>
		allowedRecord(record.key)
	);
	const liveRecordKeys = new Set(
		initialRecords
			.filter((record) => record.tombstoneRevision === undefined)
			.map((record) => record.key)
	);
	const records = initialRecords.map((record) =>
		Object.freeze({
			...record,
			links: freezeRecord(
				Object.entries(record.links).filter(([, link]) =>
					linkReferencesOnly(link.value, liveRecordKeys)
				)
			)
		})
	);

	const candidateIndexes = new Map(
		parsed.payload.cache.indexes
			.filter(
				(index) =>
					!index.deleted &&
					index.records.length > 0 &&
					index.records.every((key) => liveRecordKeys.has(key)) &&
					(
						index.metadata?.parent === undefined ||
						liveRecordKeys.has(index.metadata.parent)
					)
			)
			.map((index) => [index.key, index] as const)
	);

	const initiallyEligible = new Map<
		SerializedOperationProtocolState,
		SerializedOperationProtocolState
	>();
	for (const group of parsed.payload.operations) {
		for (const state of [group.query, group.live]) {
			if (
				state !== undefined &&
				operationStateAllowed(
					state,
					candidateIndexes,
					recordClockKeys,
					allowedRecord
				)
			) {
				initiallyEligible.set(state, state);
			}
		}
	}

	let eligible = initiallyEligible;
	let finalIndexKeys = indexesProvenByOperations(
		candidateIndexes,
		eligible.values()
	);
	for (;;) {
		const nextEligible = new Map<
			SerializedOperationProtocolState,
			SerializedOperationProtocolState
		>();
		for (const state of eligible.values()) {
			if (state.indexKeys.every((key) => finalIndexKeys.has(key))) {
				nextEligible.set(state, state);
			}
		}
		const nextIndexKeys = indexesProvenByOperations(
			candidateIndexes,
			nextEligible.values()
		);
		if (
			nextEligible.size === eligible.size &&
			nextIndexKeys.size === finalIndexKeys.size
		) {
			eligible = nextEligible;
			finalIndexKeys = nextIndexKeys;
			break;
		}
		eligible = nextEligible;
		finalIndexKeys = nextIndexKeys;
	}

	const indexes = parsed.payload.cache.indexes.filter((index) =>
		finalIndexKeys.has(index.key)
	);
	const operations = parsed.payload.operations.flatMap((group) => {
		const query =
			group.query !== undefined && eligible.has(group.query)
				? group.query
				: undefined;
		const live =
			group.live !== undefined && eligible.has(group.live)
				? group.live
				: undefined;
		if (query === undefined && live === undefined) return [];
		const active =
			group.active === 'query' && query !== undefined
				? 'query'
				: group.active === 'live' && live !== undefined
					? 'live'
					: undefined;
		return [
			Object.freeze({
				key: group.key,
				...(query === undefined ? {} : { query }),
				...(live === undefined ? {} : { live }),
				...(active === undefined ? {} : { active }),
				generation: group.generation
			})
		];
	});
	const anonymousRecordClocks =
		parsed.payload.anonymousRecordClocks.filter(([, value]) =>
			allowsModel(value.model)
		);

	if (
		records.length === 0 &&
		indexes.length === 0 &&
		recordClocks.length === 0 &&
		anonymousRecordClocks.length === 0
	) {
		return undefined;
	}
	// What survives the policy is read back through the one parser, so a filter
	// that left a clock, an index or an operation without its counterpart cannot
	// produce a state that hydration would refuse.
	return Object.freeze({
		version: 1 as const,
		scope: parsed.scope,
		payload: parseDehydratedState({
			version: 1,
			scope: parsed.scope,
			payload: {
				cache: { version: 1, records, indexes },
				operations,
				recordClocks,
				anonymousRecordClocks,
				/*
				 * Presets are part of the exact server-issued cache scope contract. A
				 * restored cache cannot safely evaluate generated row policies or command
				 * effects without the matching inventory. The entry remains opt-in,
				 * confirmed-state-only, and keyed by that independently re-established
				 * opaque scope; transport credentials and command inputs are still absent.
				 */
				trustedPresets: parsed.payload.trustedPresets,
				nextIndexRevision: parsed.payload.nextIndexRevision
			}
		}).payload
	});
}

function operationStateAllowed(
	state: SerializedOperationProtocolState,
	indexes: ReadonlyMap<string, CacheEngineSnapshot['indexes'][number]>,
	recordClockKeys: ReadonlySet<string>,
	allowedRecord: (key: string) => boolean
): boolean {
	if (state.indexKeys.length === 0 && state.pathRecords.length === 0) {
		return false;
	}
	if (state.indexKeys.some((key) => !indexes.has(key))) return false;
	for (const [, key] of state.pathRecords) {
		if (!allowedRecord(key) || !recordClockKeys.has(key)) return false;
	}
	return true;
}

function indexesProvenByOperations(
	candidates: ReadonlyMap<string, CacheEngineSnapshot['indexes'][number]>,
	states: Iterable<SerializedOperationProtocolState>
): Set<string> {
	const result = new Set<string>();
	for (const state of states) {
		if (state.indexRevision === undefined) continue;
		for (const key of state.indexKeys) {
			if (candidates.get(key)?.revision === state.indexRevision) {
				result.add(key);
			}
		}
	}
	return result;
}

function linkReferencesOnly(
	link: RecordLink,
	allowedLiveRecordKeys: ReadonlySet<string>
): boolean {
	if (link === null) return true;
	if (typeof link === 'string') return allowedLiveRecordKeys.has(link);
	return link.every((key) => allowedLiveRecordKeys.has(key));
}

export function parseStoredEntry(value: unknown): StoredReplicaEntry {
	const entry = exactRecord(
		value,
		'persisted replica entry',
		['formatVersion', 'identity', 'storedAt', 'state']
	);
	if (entry.formatVersion !== ENTRY_FORMAT_VERSION) {
		throw new TypeError('unsupported persisted replica entry version');
	}
	const storedAt = nonNegativeInteger(entry.storedAt, 'persisted replica storedAt');
	return Object.freeze({
		formatVersion: 1 as const,
		identity: nonEmptyString(entry.identity, 'persisted replica identity'),
		storedAt,
		state: entry.state as ReplicaDehydratedState
	});
}

export function persistenceIdentity(scope: ReplicaAuthoritativeScope): string {
	// JSON tuple encoding is unambiguous even when opaque scope values contain
	// punctuation used by human-readable key formats.
	return JSON.stringify([
		'distributed-confirmed-replica',
		ENTRY_FORMAT_VERSION,
		scope.protocolVersion,
		scope.schemaHash,
		scope.authorizationGeneration,
		scope.cacheScope
	]);
}
