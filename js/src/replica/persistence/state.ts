import {
	createCacheEngine,
	type CacheEngineSnapshot,
	type RecordLink
} from '../../internal/cache-engine.js';
import {
	parseDistributedTrustedPresetInventory,
	type DistributedTrustedPreset
} from '../../protocol.js';
import type {
	ReplicaAuthoritativeScope,
	ReplicaDehydratedState
} from '../types.js';
import { freezeRecord } from '../../lib/freeze-record.js';
import type {
	ReplicaPersistenceModelPolicy,
	ReplicaPersistencePolicy
} from './types.js';

/** v2 adds the durable command outbox store. */
export const DATABASE_VERSION = 2;
export const ENTRY_FORMAT_VERSION = 1;
export const STORE_NAME = 'confirmed-replicas';
export const DEFAULT_DATABASE_NAME = '@hops-ops/distributed:replica';
export const GRAPHQL_NAME = /^[_A-Za-z][_0-9A-Za-z]*$/;
export const DECIMAL = /^(0|[1-9][0-9]*)$/;


export type RecordClockV1 = {
	readonly scopeToken: string;
	readonly incarnation: string;
	readonly revision: string;
	readonly tombstone: boolean;
};

export type AnonymousRecordClockV1 = {
	readonly model: string;
	readonly clock: RecordClockV1;
};

export type OperationProtocolStateV1 = {
	readonly operation: string;
	readonly snapshotScope?: string;
	readonly indexClocks: readonly (readonly [
		string,
		Readonly<{ scopeToken: string; position: string }>
	])[];
	readonly indexRevision?: string;
	readonly indexKeys: readonly string[];
	readonly pathRecords: readonly (readonly [string, string])[];
	readonly cursors: readonly Readonly<{
		projection: string;
		position: string;
		token: string;
	}>[];
};

export type OperationProtocolGroupV1 = {
	readonly key: string;
	readonly query?: OperationProtocolStateV1;
	readonly live?: OperationProtocolStateV1;
	readonly active?: 'query' | 'live';
	readonly generation: number;
};

export type ReplicaPersistencePayloadV1 = {
	readonly cache: CacheEngineSnapshot;
	readonly operations: readonly OperationProtocolGroupV1[];
	readonly recordClocks: readonly (readonly [string, RecordClockV1])[];
	readonly anonymousRecordClocks: readonly (readonly [
		string,
		AnonymousRecordClockV1
	])[];
	readonly trustedPresets: readonly DistributedTrustedPreset[];
	readonly nextIndexRevision: string;
};

export type ParsedReplicaState = {
	readonly state: ReplicaDehydratedState;
	readonly scope: ReplicaAuthoritativeScope;
	readonly payload: ReplicaPersistencePayloadV1;
};

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
	parsed: ParsedReplicaState,
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
		OperationProtocolStateV1,
		OperationProtocolStateV1
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
			OperationProtocolStateV1,
			OperationProtocolStateV1
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

	const cache = canonicalCacheSnapshot({
		version: 1,
		records,
		indexes
	});
	if (
		cache.records.length === 0 &&
		cache.indexes.length === 0 &&
		recordClocks.length === 0 &&
		anonymousRecordClocks.length === 0
	) {
		return undefined;
	}
	const payload: ReplicaPersistencePayloadV1 = Object.freeze({
		cache,
		operations: Object.freeze(operations),
		recordClocks: Object.freeze(recordClocks),
		anonymousRecordClocks: Object.freeze(anonymousRecordClocks),
		/*
		 * Presets are part of the exact server-issued cache scope contract. A
		 * restored cache cannot safely evaluate generated row policies or command
		 * effects without the matching inventory. The entry remains opt-in,
		 * confirmed-state-only, and keyed by that independently re-established
		 * opaque scope; transport credentials and command inputs are still absent.
		 */
		trustedPresets: parsed.payload.trustedPresets,
		nextIndexRevision: parsed.payload.nextIndexRevision
	});
	assertMetadataConsistent(payload);
	return Object.freeze({
		version: 1 as const,
		scope: parsed.scope,
		payload
	});
}

export function operationStateAllowed(
	state: OperationProtocolStateV1,
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

export function indexesProvenByOperations(
	candidates: ReadonlyMap<string, CacheEngineSnapshot['indexes'][number]>,
	states: Iterable<OperationProtocolStateV1>
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

export function linkReferencesOnly(
	link: RecordLink,
	allowedLiveRecordKeys: ReadonlySet<string>
): boolean {
	if (link === null) return true;
	if (typeof link === 'string') return allowedLiveRecordKeys.has(link);
	return link.every((key) => allowedLiveRecordKeys.has(key));
}

export function parseReplicaState(value: unknown): ParsedReplicaState {
	const state = exactRecord(value, 'state', ['version', 'scope', 'payload']);
	if (state.version !== 1) throw new TypeError('unsupported replica state version');
	const scope = parseAuthoritativeScope(state.scope);
	const payloadValue = exactRecord(
		state.payload,
		'state.payload',
		[
			'cache',
			'operations',
			'recordClocks',
			'anonymousRecordClocks',
			'trustedPresets',
			'nextIndexRevision'
		]
	);
	const cache = parseCacheSnapshot(payloadValue.cache);
	const operations = parseOperations(payloadValue.operations);
	const recordClocks = parseRecordClocks(payloadValue.recordClocks);
	const anonymousRecordClocks = parseAnonymousRecordClocks(
		payloadValue.anonymousRecordClocks,
		recordClocks
	);
	const trustedPresets = parseDistributedTrustedPresetInventory(
		payloadValue.trustedPresets,
		'state.payload.trustedPresets'
	);
	const nextIndexRevision = decimalString(
		payloadValue.nextIndexRevision,
		'state.payload.nextIndexRevision'
	);
	const payload: ReplicaPersistencePayloadV1 = Object.freeze({
		cache,
		operations,
		recordClocks,
		anonymousRecordClocks,
		trustedPresets,
		nextIndexRevision
	});
	assertMetadataConsistent(payload);
	const normalizedState: ReplicaDehydratedState = Object.freeze({
		version: 1 as const,
		scope,
		payload
	});
	return Object.freeze({ state: normalizedState, scope, payload });
}

export function parseAuthoritativeScope(value: unknown): ReplicaAuthoritativeScope {
	const scope = exactRecord(
		value,
		'authoritative scope',
		[
			'protocolVersion',
			'schemaHash',
			'authorizationGeneration',
			'cacheScope'
		]
	);
	if (scope.protocolVersion !== 1) {
		throw new TypeError('unsupported authoritative protocol version');
	}
	return Object.freeze({
		protocolVersion: 1 as const,
		schemaHash: nonEmptyString(scope.schemaHash, 'authoritative scope schemaHash'),
		authorizationGeneration: nonEmptyString(
			scope.authorizationGeneration,
			'authoritative scope authorizationGeneration'
		),
		cacheScope: nonEmptyString(scope.cacheScope, 'authoritative scope cacheScope')
	});
}

export function parseCacheSnapshot(value: unknown): CacheEngineSnapshot {
	const cache = exactRecord(value, 'state.payload.cache', [
		'version',
		'records',
		'indexes'
	]);
	if (cache.version !== 1) throw new TypeError('unsupported cache snapshot');
	for (const [index, value] of arrayValue(
		cache.records,
		'state.payload.cache.records'
	).entries()) {
		const path = `state.payload.cache.records[${index}]`;
		const record = exactRecord(
			value,
			path,
			['key', 'revision', 'incarnation', 'tombstoneRevision', 'fields', 'links'],
			['key', 'revision', 'fields', 'links']
		);
		nonEmptyString(record.key, `${path}.key`);
		decimalString(record.revision, `${path}.revision`);
		if (record.incarnation !== undefined) {
			decimalString(record.incarnation, `${path}.incarnation`);
		}
		if (record.tombstoneRevision !== undefined) {
			decimalString(record.tombstoneRevision, `${path}.tombstoneRevision`);
		}
		for (const [name, fieldValue] of Object.entries(
			plainRecord(record.fields, `${path}.fields`)
		)) {
			const field = exactRecord(
				fieldValue,
				`${path}.fields.${name}`,
				['revision', 'value']
			);
			decimalString(field.revision, `${path}.fields.${name}.revision`);
			assertJsonValue(field.value, `${path}.fields.${name}.value`);
		}
		for (const [name, linkValue] of Object.entries(
			plainRecord(record.links, `${path}.links`)
		)) {
			const link = exactRecord(
				linkValue,
				`${path}.links.${name}`,
				['revision', 'value']
			);
			decimalString(link.revision, `${path}.links.${name}.revision`);
			assertRecordLink(link.value, `${path}.links.${name}.value`);
		}
	}
	for (const [index, value] of arrayValue(
		cache.indexes,
		'state.payload.cache.indexes'
	).entries()) {
		const path = `state.payload.cache.indexes[${index}]`;
		const entry = exactRecord(
			value,
			path,
			[
				'key',
				'revision',
				'staleRevision',
				'records',
				'complete',
				'deleted',
				'metadata'
			],
			['key', 'revision', 'records', 'complete', 'deleted']
		);
		nonEmptyString(entry.key, `${path}.key`);
		decimalString(entry.revision, `${path}.revision`);
		if (entry.staleRevision !== undefined) {
			decimalString(entry.staleRevision, `${path}.staleRevision`);
		}
		const seen = new Set<string>();
		for (const [recordIndex, key] of arrayValue(
			entry.records,
			`${path}.records`
		).entries()) {
			const recordKey = nonEmptyString(key, `${path}.records[${recordIndex}]`);
			if (seen.has(recordKey)) {
				throw new TypeError(`duplicate record at ${path}.records[${recordIndex}]`);
			}
			seen.add(recordKey);
		}
		booleanValue(entry.complete, `${path}.complete`);
		booleanValue(entry.deleted, `${path}.deleted`);
		if (entry.metadata !== undefined) {
			assertIndexMetadata(entry.metadata, `${path}.metadata`);
		}
	}
	return canonicalCacheSnapshot(cache as unknown as CacheEngineSnapshot);
}

export function canonicalCacheSnapshot(
	value: CacheEngineSnapshot
): CacheEngineSnapshot {
	const engine = createCacheEngine();
	engine.restore(value);
	return engine.extract();
}

export function assertIndexMetadata(value: unknown, path: string): void {
	const metadata = exactRecord(
		value,
		path,
		[
			'parent',
			'parentRevision',
			'parentIncarnation',
			'field',
			'arguments',
			'coverage',
			'dependencies',
			'staleReason',
			'nullValue'
		],
		['field', 'arguments', 'coverage', 'dependencies']
	);
	if (metadata.parent !== undefined) nonEmptyString(metadata.parent, `${path}.parent`);
	if (metadata.parentRevision !== undefined) {
		decimalString(metadata.parentRevision, `${path}.parentRevision`);
	}
	if (metadata.parentIncarnation !== undefined) {
		decimalString(metadata.parentIncarnation, `${path}.parentIncarnation`);
	}
	nonEmptyString(metadata.field, `${path}.field`);
	const argumentsValue = plainRecord(metadata.arguments, `${path}.arguments`);
	assertJsonValue(argumentsValue, `${path}.arguments`);
	assertCoverage(metadata.coverage, `${path}.coverage`);
	const seen = new Set<string>();
	for (const [index, dependency] of arrayValue(
		metadata.dependencies,
		`${path}.dependencies`
	).entries()) {
		const name = nonEmptyString(
			dependency,
			`${path}.dependencies[${index}]`
		);
		if (seen.has(name)) {
			throw new TypeError(`duplicate dependency at ${path}.dependencies[${index}]`);
		}
		seen.add(name);
	}
	if (metadata.staleReason !== undefined) {
		nonEmptyString(metadata.staleReason, `${path}.staleReason`);
	}
	if (metadata.nullValue !== undefined) {
		booleanValue(metadata.nullValue, `${path}.nullValue`);
	}
}

export function assertCoverage(value: unknown, path: string): void {
	const coverage = plainRecord(value, path);
	if (coverage.kind === 'complete' || coverage.kind === 'unknown') {
		exactKeys(coverage, path, ['kind']);
		return;
	}
	if (coverage.kind === 'offset') {
		exactKeys(
			coverage,
			path,
			['kind', 'offset', 'limit', 'returned', 'hasNext'],
			['kind', 'offset']
		);
		nonNegativeInteger(coverage.offset, `${path}.offset`);
		if (coverage.limit !== undefined) nonNegativeInteger(coverage.limit, `${path}.limit`);
		if (coverage.returned !== undefined) {
			nonNegativeInteger(coverage.returned, `${path}.returned`);
		}
		if (coverage.hasNext !== undefined) booleanValue(coverage.hasNext, `${path}.hasNext`);
		return;
	}
	if (coverage.kind === 'cursor') {
		exactKeys(
			coverage,
			path,
			[
				'kind',
				'after',
				'before',
				'first',
				'last',
				'start',
				'end',
				'hasNext',
				'hasPrevious'
			],
			['kind']
		);
		for (const key of ['after', 'before', 'start', 'end'] as const) {
			if (coverage[key] !== undefined) {
				assertJsonValue(coverage[key], `${path}.${key}`);
			}
		}
		for (const key of ['first', 'last'] as const) {
			if (coverage[key] !== undefined) {
				nonNegativeInteger(coverage[key], `${path}.${key}`);
			}
		}
		for (const key of ['hasNext', 'hasPrevious'] as const) {
			if (coverage[key] !== undefined) {
				booleanValue(coverage[key], `${path}.${key}`);
			}
		}
		return;
	}
	throw new TypeError(`unsupported index coverage at ${path}`);
}

export function parseOperations(value: unknown): readonly OperationProtocolGroupV1[] {
	const groups: OperationProtocolGroupV1[] = [];
	const keys = new Set<string>();
	for (const [index, entry] of arrayValue(
		value,
		'state.payload.operations'
	).entries()) {
		const path = `state.payload.operations[${index}]`;
		const raw = exactRecord(
			entry,
			path,
			['key', 'query', 'live', 'active', 'generation'],
			['key', 'generation']
		);
		const key = nonEmptyString(raw.key, `${path}.key`);
		if (!key.startsWith('protocol:') || keys.has(key)) {
			throw new TypeError(`invalid or duplicate operation key at ${path}.key`);
		}
		keys.add(key);
		const query =
			raw.query === undefined
				? undefined
				: parseOperationState(raw.query, `${path}.query`);
		const live =
			raw.live === undefined
				? undefined
				: parseOperationState(raw.live, `${path}.live`);
		if (query === undefined && live === undefined) {
			throw new TypeError(`operation has no protocol state at ${path}`);
		}
		const active =
			raw.active === undefined
				? undefined
				: operationSource(raw.active, `${path}.active`);
		if (
			(active === 'query' && query === undefined) ||
			(active === 'live' && live === undefined)
		) {
			throw new TypeError(`invalid active operation source at ${path}.active`);
		}
		groups.push(
			Object.freeze({
				key,
				...(query === undefined ? {} : { query }),
				...(live === undefined ? {} : { live }),
				...(active === undefined ? {} : { active }),
				generation: nonNegativeInteger(raw.generation, `${path}.generation`)
			})
		);
	}
	return Object.freeze(groups);
}

export function parseOperationState(
	value: unknown,
	path: string
): OperationProtocolStateV1 {
	const raw = exactRecord(
		value,
		path,
		[
			'operation',
			'snapshotScope',
			'indexClocks',
			'indexRevision',
			'indexKeys',
			'pathRecords',
			'cursors'
		],
		['operation', 'indexClocks', 'indexKeys', 'pathRecords', 'cursors']
	);
	const indexClocks = parseUniquePairs(
		raw.indexClocks,
		`${path}.indexClocks`,
		(entry, entryPath) => {
			const clock = exactRecord(entry, entryPath, ['scopeToken', 'position']);
			return Object.freeze({
				scopeToken: nonEmptyString(clock.scopeToken, `${entryPath}.scopeToken`),
				position: decimalString(clock.position, `${entryPath}.position`)
			});
		}
	);
	const indexKeys = uniqueStrings(raw.indexKeys, `${path}.indexKeys`);
	const pathRecords = parseUniquePairs(
		raw.pathRecords,
		`${path}.pathRecords`,
		(entry, entryPath) => nonEmptyString(entry, entryPath)
	);
	const cursors = arrayValue(raw.cursors, `${path}.cursors`).map(
		(entry, index) => {
			const entryPath = `${path}.cursors[${index}]`;
			const cursor = exactRecord(
				entry,
				entryPath,
				['projection', 'position', 'token']
			);
			return Object.freeze({
				projection: nonEmptyString(cursor.projection, `${entryPath}.projection`),
				position: decimalString(cursor.position, `${entryPath}.position`),
				token: nonEmptyString(cursor.token, `${entryPath}.token`)
			});
		}
	);
	return Object.freeze({
		operation: nonEmptyString(raw.operation, `${path}.operation`),
		...(raw.snapshotScope === undefined
			? {}
			: {
					snapshotScope: nonEmptyString(
						raw.snapshotScope,
						`${path}.snapshotScope`
					)
				}),
		indexClocks,
		...(raw.indexRevision === undefined
			? {}
			: {
					indexRevision: decimalString(
						raw.indexRevision,
						`${path}.indexRevision`
					)
				}),
		indexKeys,
		pathRecords,
		cursors: Object.freeze(cursors)
	});
}

export function parseRecordClocks(
	value: unknown
): readonly (readonly [string, RecordClockV1])[] {
	return parseUniquePairs(
		value,
		'state.payload.recordClocks',
		(entry, path) => parseRecordClock(entry, path)
	);
}

export function parseAnonymousRecordClocks(
	value: unknown,
	recordClocks: readonly (readonly [string, RecordClockV1])[]
): readonly (readonly [string, AnonymousRecordClockV1])[] {
	const recordScopes = new Set(recordClocks.map(([, clock]) => clock.scopeToken));
	const result = parseUniquePairs(
		value,
		'state.payload.anonymousRecordClocks',
		(entry, path) => {
			const raw = exactRecord(entry, path, ['model', 'clock']);
			return Object.freeze({
				model: nonEmptyString(raw.model, `${path}.model`),
				clock: parseRecordClock(raw.clock, `${path}.clock`)
			});
		}
	);
	for (const [scopeToken, value] of result) {
		if (
			value.clock.scopeToken !== scopeToken ||
			recordScopes.has(scopeToken)
		) {
			throw new TypeError(
				`invalid anonymous record scope at state.payload.anonymousRecordClocks`
			);
		}
	}
	return result;
}

export function parseRecordClock(value: unknown, path: string): RecordClockV1 {
	const clock = exactRecord(
		value,
		path,
		['scopeToken', 'incarnation', 'revision', 'tombstone']
	);
	return Object.freeze({
		scopeToken: nonEmptyString(clock.scopeToken, `${path}.scopeToken`),
		incarnation: decimalString(clock.incarnation, `${path}.incarnation`),
		revision: decimalString(clock.revision, `${path}.revision`),
		tombstone: booleanValue(clock.tombstone, `${path}.tombstone`)
	});
}

export function assertMetadataConsistent(payload: ReplicaPersistencePayloadV1): void {
	const clocksByRecord = new Map(payload.recordClocks);
	const recordScopes = new Set<string>();
	for (const [key, clock] of payload.recordClocks) {
		if (recordScopes.has(clock.scopeToken)) {
			throw new TypeError(`duplicate record scope token for ${key}`);
		}
		recordScopes.add(clock.scopeToken);
	}
	for (const record of payload.cache.records) {
		if (modelFromRecordKey(record.key) === undefined) continue;
		const clock = clocksByRecord.get(record.key);
		const incarnation = record.incarnation ?? record.revision;
		if (
			clock === undefined ||
			clock.incarnation !== incarnation ||
			clock.revision !== record.revision ||
			(
				clock.tombstone
					? record.tombstoneRevision !== clock.revision
					: record.tombstoneRevision !== undefined
			)
		) {
			throw new TypeError(`inconsistent persisted record clock for ${record.key}`);
		}
	}
	const indexRevisions = new Map<string, Set<string>>();
	for (const group of payload.operations) {
		for (const state of [group.query, group.live]) {
			if (state === undefined) continue;
			if (
				state.indexRevision !== undefined &&
				compareDecimal(state.indexRevision, payload.nextIndexRevision) > 0
			) {
				throw new TypeError(`operation index revision exceeds checkpoint`);
			}
			if (state.indexRevision === undefined && state.indexKeys.length > 0) {
				throw new TypeError(`operation index keys lack a revision`);
			}
			for (const [, recordKey] of state.pathRecords) {
				if (
					modelFromRecordKey(recordKey) !== undefined &&
					!clocksByRecord.has(recordKey)
				) {
					throw new TypeError(`operation path lacks a record clock`);
				}
			}
			if (state.indexRevision !== undefined) {
				for (const key of state.indexKeys) {
					const revisions = indexRevisions.get(key) ?? new Set<string>();
					revisions.add(state.indexRevision);
					indexRevisions.set(key, revisions);
				}
			}
		}
	}
	for (const index of payload.cache.indexes) {
		if (
			compareDecimal(index.revision, payload.nextIndexRevision) > 0 ||
			!indexRevisions.get(index.key)?.has(index.revision)
		) {
			throw new TypeError(`index lacks matching operation checkpoint: ${index.key}`);
		}
	}
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

export function sameScope(
	left: ReplicaAuthoritativeScope,
	right: ReplicaAuthoritativeScope
): boolean {
	return (
		left.protocolVersion === right.protocolVersion &&
		left.schemaHash === right.schemaHash &&
		left.authorizationGeneration === right.authorizationGeneration &&
		left.cacheScope === right.cacheScope
	);
}

export function modelFromRecordKey(key: string): string | undefined {
	if (!key.startsWith('record:')) return undefined;
	const separator = key.indexOf(':', 'record:'.length);
	if (separator < 0 || separator === key.length - 1) return undefined;
	try {
		const model = decodeURIComponent(key.slice('record:'.length, separator));
		return GRAPHQL_NAME.test(model) ? model : undefined;
	} catch {
		return undefined;
	}
}

export function parseUniquePairs<T>(
	value: unknown,
	path: string,
	parseValue: (value: unknown, path: string) => T
): readonly (readonly [string, T])[] {
	const result: Array<readonly [string, T]> = [];
	const keys = new Set<string>();
	for (const [index, entry] of arrayValue(value, path).entries()) {
		const entryPath = `${path}[${index}]`;
		if (!Array.isArray(entry) || entry.length !== 2) {
			throw new TypeError(`invalid pair at ${entryPath}`);
		}
		const key = nonEmptyString(entry[0], `${entryPath}[0]`);
		if (keys.has(key)) throw new TypeError(`duplicate key at ${entryPath}[0]`);
		keys.add(key);
		result.push(
			Object.freeze([
				key,
				parseValue(entry[1], `${entryPath}[1]`)
			] as const)
		);
	}
	return Object.freeze(result);
}

export function uniqueStrings(value: unknown, path: string): readonly string[] {
	const result: string[] = [];
	const seen = new Set<string>();
	for (const [index, entry] of arrayValue(value, path).entries()) {
		const string = nonEmptyString(entry, `${path}[${index}]`);
		if (seen.has(string)) throw new TypeError(`duplicate value at ${path}[${index}]`);
		seen.add(string);
		result.push(string);
	}
	return Object.freeze(result);
}

export function assertRecordLink(value: unknown, path: string): void {
	if (value === null) return;
	if (typeof value === 'string' && value.length > 0) return;
	if (!Array.isArray(value)) throw new TypeError(`invalid record link at ${path}`);
	const seen = new Set<string>();
	for (const [index, entry] of value.entries()) {
		const key = nonEmptyString(entry, `${path}[${index}]`);
		if (seen.has(key)) throw new TypeError(`duplicate record link at ${path}[${index}]`);
		seen.add(key);
	}
}

export function assertJsonValue(
	value: unknown,
	path: string,
	ancestors = new Set<object>()
): void {
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	) {
		return;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) throw new TypeError(`non-finite number at ${path}`);
		return;
	}
	if (typeof value !== 'object') throw new TypeError(`non-JSON value at ${path}`);
	if (ancestors.has(value)) throw new TypeError(`cyclic JSON value at ${path}`);
	ancestors.add(value);
	if (Array.isArray(value)) {
		for (const [index, entry] of value.entries()) {
			assertJsonValue(entry, `${path}[${index}]`, ancestors);
		}
	} else {
		const record = plainRecord(value, path);
		for (const [key, entry] of Object.entries(record)) {
			assertJsonValue(entry, `${path}.${key}`, ancestors);
		}
	}
	ancestors.delete(value);
}

export function exactRecord(
	value: unknown,
	path: string,
	allowed: readonly string[],
	required: readonly string[] = allowed
): Record<string, unknown> {
	const record = plainRecord(value, path);
	exactKeys(record, path, allowed, required);
	return record;
}

export function exactKeys(
	record: Record<string, unknown>,
	path: string,
	allowed: readonly string[],
	required: readonly string[] = allowed
): void {
	const allowedKeys = new Set(allowed);
	for (const key of Object.keys(record)) {
		if (!allowedKeys.has(key)) throw new TypeError(`unknown field ${path}.${key}`);
	}
	for (const key of required) {
		if (!Object.prototype.hasOwnProperty.call(record, key)) {
			throw new TypeError(`missing field ${path}.${key}`);
		}
	}
}

export function plainRecord(value: unknown, path: string): Record<string, unknown> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		throw new TypeError(`expected object at ${path}`);
	}
	const prototype = Object.getPrototypeOf(value);
	if (prototype !== Object.prototype && prototype !== null) {
		throw new TypeError(`expected plain object at ${path}`);
	}
	return value as Record<string, unknown>;
}

export function arrayValue(value: unknown, path: string): unknown[] {
	if (!Array.isArray(value)) throw new TypeError(`expected array at ${path}`);
	return value;
}

export function nonEmptyString(value: unknown, path: string): string {
	if (typeof value !== 'string' || value.length === 0) {
		throw new TypeError(`expected non-empty string at ${path}`);
	}
	return value;
}

export function decimalString(value: unknown, path: string): string {
	const string = nonEmptyString(value, path);
	if (!DECIMAL.test(string)) throw new TypeError(`expected decimal at ${path}`);
	return string;
}

export function booleanValue(value: unknown, path: string): boolean {
	if (typeof value !== 'boolean') throw new TypeError(`expected boolean at ${path}`);
	return value;
}

export function operationSource(value: unknown, path: string): 'query' | 'live' {
	if (value !== 'query' && value !== 'live') {
		throw new TypeError(`expected operation source at ${path}`);
	}
	return value;
}

export function nonNegativeInteger(value: unknown, path: string): number {
	if (!Number.isSafeInteger(value) || (value as number) < 0) {
		throw new TypeError(`expected non-negative safe integer at ${path}`);
	}
	return value as number;
}

export function compareDecimal(left: string, right: string): -1 | 0 | 1 {
	const leftValue = BigInt(left);
	const rightValue = BigInt(right);
	return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}
