import { compareCodeUnits } from '../../lib/compare-code-units.js';
import {
	createCacheEngine,
	type CacheEngineSnapshot
} from '../../internal/cache-engine.js';
import {
	compareDistributedDecimal,
	parseDistributedTrustedPresetInventory,
	type DistributedDecimalString,
	type DistributedOpaqueString
} from '../../protocol.js';

import { modelFromRecordKey } from './clocks.js';

import type { GraphqlVariables } from '../../types.js';
import { replicaIndexKey, resolveArguments } from '../identity/keys.js';
import {
	runtimeRoot,
	type RuntimeObjectBranch,
	type RuntimeObjectSelection,
	type RuntimeRootSelection
} from '../selection.js';
import type { ReplicaAuthoritativeScope } from '../types.js';
import { isPlainRecord } from '../../lib/is-plain-record.js';
import { canonicalTrustedPresets } from './helpers.js';
import type {
	AnonymousRecordProtocolClock,
	OperationProtocolGroup,
	OperationProtocolSource,
	OperationProtocolState,
	ParsedReplicaHydration,
	ProtocolGeneration,
	RecordProtocolClock,
	RenderedOperation,
	ReplicaDehydratedPayloadV1,
	SerializedOperationProtocolGroup,
	SerializedOperationProtocolState
} from './types.js';

/**
 * The confirmed records and indexes the rendered operations reach, with only
 * the fields they select: what a dehydrated state carries.
 */
export function reachableConfirmedState(
	snapshot: CacheEngineSnapshot,
	rendered: ReadonlyMap<string, RenderedOperation>,
	group: (key: string) => Readonly<OperationProtocolGroup> | undefined
): {
	readonly cache: CacheEngineSnapshot;
	readonly recordKeys: ReadonlySet<string>;
	readonly clockRecordKeys: ReadonlySet<string>;
	readonly models: ReadonlySet<string>;
} {
	const indexes = new Map(snapshot.indexes.map((index) => [index.key, index]));
	const records = new Map(snapshot.records.map((record) => [record.key, record]));
	const indexKeys = new Set<string>();
	const recordKeys = new Set<string>();
	const clockRecordKeys = new Set<string>();
	const recordFields = new Map<string, Set<string>>();
	const models = new Set<string>();

	const rememberSelection = (selection: RuntimeObjectSelection): void => {
		if (selection.storage.kind === 'normalized') {
			models.add(selection.storage.model);
		}
		for (const member of selection.members) {
			if (member.kind === 'branch') rememberSelection(member.selection);
		}
	};
	const rememberRecord = (
		recordKey: string,
		selection: RuntimeObjectSelection
	): void => {
		recordKeys.add(recordKey);
		clockRecordKeys.add(recordKey);
		let selected = recordFields.get(recordKey);
		if (selected === undefined) {
			selected = new Set();
			recordFields.set(recordKey, selected);
		}
		for (const member of selection.members) {
			if (member.kind === 'scalar') selected.add(member.field);
		}
	};
	const visitObject = (
		selection: RuntimeObjectSelection,
		recordKey: string,
		variables: GraphqlVariables
	): void => {
		rememberRecord(recordKey, selection);
		for (const member of selection.members) {
			if (member.kind !== 'branch') continue;
			visitBranch(member, recordKey, variables);
		}
	};
	const visitBranch = (
		selection: RuntimeRootSelection | RuntimeObjectBranch,
		parent: string | undefined,
		variables: GraphqlVariables
	): void => {
		const key = replicaIndexKey({
			...(parent === undefined ? {} : { parent }),
			field: selection.field,
			arguments: resolveArguments(
				selection.arguments,
				variables,
				selection.coverage
			)
		});
		const index = indexes.get(key);
		if (index === undefined) return;
		indexKeys.add(key);
		for (const recordKey of index.records) {
			visitObject(selection.selection, recordKey, variables);
		}
	};

	for (const [key, operation] of rendered) {
		for (const rootArtifact of operation.artifact.roots) {
			const root = runtimeRoot(rootArtifact);
			rememberSelection(root.selection);
			visitBranch(root, undefined, operation.variables);
		}
		const protocol = group(key);
		for (const state of [protocol?.query, protocol?.live]) {
			for (const recordKey of state?.pathRecords.values() ?? []) {
				clockRecordKeys.add(recordKey);
			}
		}
	}

	return Object.freeze({
		cache: Object.freeze({
			version: 1 as const,
			records: Object.freeze(
				[...recordKeys]
					.sort()
					.flatMap((key) => {
						const record = records.get(key);
						if (record === undefined) return [];
						const selected = recordFields.get(key) ?? new Set();
						return [
							Object.freeze({
								...record,
								fields: Object.freeze(
									Object.fromEntries(
										Object.entries(record.fields).filter(([field]) =>
											selected.has(field)
										)
									)
								),
								links: Object.freeze({})
							})
						];
					})
			),
			indexes: Object.freeze(
				[...indexKeys]
					.sort()
					.flatMap((key) => {
						const index = indexes.get(key);
						return index === undefined ? [] : [index];
					})
			)
		}),
		recordKeys,
		clockRecordKeys,
		models
	});
}

export function serializeOperationProtocolGroup(
	key: string,
	group: OperationProtocolGroup,
	generation: number
): SerializedOperationProtocolGroup {
	return Object.freeze({
		key,
		...(group.query === undefined
			? {}
			: { query: serializeOperationProtocolState(group.query) }),
		...(group.live === undefined
			? {}
			: { live: serializeOperationProtocolState(group.live) }),
		...(group.active === undefined ? {} : { active: group.active }),
		generation
	});
}

function serializeOperationProtocolState(
	state: OperationProtocolState
): SerializedOperationProtocolState {
	return Object.freeze({
		operation: state.operation,
		...(state.snapshotScope === undefined
			? {}
			: { snapshotScope: state.snapshotScope }),
		indexClocks: Object.freeze(
			[...state.indexClocks]
				.sort(([left], [right]) => compareCodeUnits(left, right))
				.map(([projection, clock]) =>
					Object.freeze([
						projection,
						Object.freeze({
							scopeToken: clock.scopeToken,
							position: clock.position
						})
					] as const)
				)
		),
		...(state.indexRevision === undefined
			? {}
			: { indexRevision: state.indexRevision }),
		...(state.retiredAtRevision === undefined
			? {}
			: { retiredAtRevision: state.retiredAtRevision }),
		indexKeys: Object.freeze([...state.indexKeys].sort()),
		pathRecords: Object.freeze(
			[...state.pathRecords]
				.sort(([left], [right]) => compareCodeUnits(left, right))
				.map(([path, key]) => Object.freeze([path, key] as const))
		),
		cursors: Object.freeze(
			state.cursors.map((cursor) =>
				Object.freeze({
					projection: cursor.projection,
					position: cursor.position,
					token: cursor.token
				})
			)
		)
	});
}

/** A dehydrated state that passed {@link parseDehydratedState}. */
type ParsedDehydratedState = {
	readonly scope: ProtocolGeneration;
	readonly payload: ReplicaDehydratedPayloadV1;
};

const SCOPE_FIELDS = [
	'protocolVersion',
	'schemaHash',
	'authorizationGeneration',
	'cacheScope'
] as const;

/** Parse the scope a caller independently established. Throws `TypeError`. */
export function parseAuthoritativeScope(
	value: unknown,
	path = 'authoritativeScope'
): ProtocolGeneration {
	const scope = exactRecord(value, path, SCOPE_FIELDS);
	if (scope.protocolVersion !== 1) invalid(`${path}.protocolVersion`);
	return Object.freeze({
		protocolVersion: 1,
		schemaHash: nonEmptyString(scope.schemaHash, `${path}.schemaHash`),
		authorizationGeneration: nonEmptyString(
			scope.authorizationGeneration,
			`${path}.authorizationGeneration`
		),
		cacheScope: opaque(scope.cacheScope, `${path}.cacheScope`)
	});
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

/**
 * The one parser of the format `dehydrate()` writes: hydration reads a state
 * through it and nothing else.
 *
 * It admits exactly what `dehydrate()` can emit. Every object is plain and has
 * no unknown member, every revision is a canonical decimal string, the cache
 * restores into an engine, and the record clocks, operation states and indexes
 * agree with one another. Anything else throws `TypeError` naming the path.
 *
 * Protocol metadata is returned as fresh frozen values; the validated cache is
 * returned as given.
 */
export function parseDehydratedState(value: unknown): ParsedDehydratedState {
	const state = exactRecord(value, 'state', ['version', 'scope', 'payload']);
	if (state.version !== 1) invalid('state.version');
	const scope = parseAuthoritativeScope(state.scope, 'state.scope');
	const raw = exactRecord(state.payload, 'state.payload', [
		'cache',
		'operations',
		'recordClocks',
		'anonymousRecordClocks',
		'trustedPresets',
		'nextIndexRevision'
	]);
	const cache = parseCacheSnapshot(raw.cache);
	const operations = parseOperations(raw.operations);
	const recordClocks = parseUniquePairs(
		raw.recordClocks,
		'state.payload.recordClocks',
		parseRecordClock
	);
	const anonymousRecordClocks = parseUniquePairs(
		raw.anonymousRecordClocks,
		'state.payload.anonymousRecordClocks',
		(entry, path): AnonymousRecordProtocolClock => {
			const anonymous = exactRecord(entry, path, ['model', 'clock']);
			return Object.freeze({
				model: nonEmptyString(anonymous.model, `${path}.model`),
				clock: parseRecordClock(anonymous.clock, `${path}.clock`)
			});
		}
	);
	const payload: ReplicaDehydratedPayloadV1 = Object.freeze({
		cache,
		operations,
		recordClocks,
		anonymousRecordClocks,
		trustedPresets: canonicalTrustedPresets(
			parseDistributedTrustedPresetInventory(
				raw.trustedPresets,
				'state.payload.trustedPresets'
			)
		),
		nextIndexRevision: decimal(
			raw.nextIndexRevision,
			'state.payload.nextIndexRevision'
		)
	});
	assertMetadataConsistent(payload);
	return Object.freeze({ scope, payload });
}

/** The mutable protocol tables a replica keeps for a parsed state. */
export function hydrationTables(
	parsed: ParsedDehydratedState
): ParsedReplicaHydration {
	const { payload } = parsed;
	const operationProtocols = new Map<string, OperationProtocolGroup>();
	const operationGenerations = new Map<string, number>();
	for (const group of payload.operations) {
		operationProtocols.set(group.key, {
			...(group.query === undefined
				? {}
				: { query: operationProtocolState(group.query) }),
			...(group.live === undefined
				? {}
				: { live: operationProtocolState(group.live) }),
			...(group.active === undefined ? {} : { active: group.active })
		});
		operationGenerations.set(group.key, group.generation);
	}
	return {
		scope: parsed.scope,
		cache: payload.cache,
		operationProtocols,
		operationGenerations,
		recordClocks: new Map(payload.recordClocks),
		recordKeysByScope: new Map(
			payload.recordClocks.map(([key, clock]) => [clock.scopeToken, key])
		),
		anonymousRecordClocks: new Map(
			payload.anonymousRecordClocks as readonly (readonly [
				DistributedOpaqueString,
				AnonymousRecordProtocolClock
			])[]
		),
		trustedPresets: payload.trustedPresets,
		nextIndexRevision: payload.nextIndexRevision
	};
}

function operationProtocolState(
	state: SerializedOperationProtocolState
): OperationProtocolState {
	return {
		operation: state.operation,
		...(state.snapshotScope === undefined
			? {}
			: { snapshotScope: state.snapshotScope as DistributedOpaqueString }),
		indexClocks: new Map(
			state.indexClocks.map(([projection, clock]) => [
				projection,
				clock as {
					scopeToken: DistributedOpaqueString;
					position: DistributedDecimalString;
				}
			])
		),
		...(state.indexRevision === undefined
			? {}
			: { indexRevision: state.indexRevision }),
		...(state.retiredAtRevision === undefined
			? {}
			: { retiredAtRevision: state.retiredAtRevision }),
		indexKeys: new Set(state.indexKeys),
		pathRecords: new Map(state.pathRecords),
		cursors: state.cursors
	};
}

/**
 * Structure first, so that the engine only ever sees the shape it is typed
 * for; then the engine itself, which owns the cache's own invariants (key
 * shapes, field revisions within their record, tombstones without fields).
 */
function parseCacheSnapshot(value: unknown): CacheEngineSnapshot {
	const root = 'state.payload.cache';
	const cache = exactRecord(value, root, ['version', 'records', 'indexes']);
	if (cache.version !== 1) invalid(`${root}.version`);
	for (const [index, entry] of array(cache.records, `${root}.records`).entries()) {
		const path = `${root}.records[${index}]`;
		const record = exactRecord(
			entry,
			path,
			['key', 'revision', 'incarnation', 'tombstoneRevision', 'fields', 'links'],
			['key', 'revision', 'fields', 'links']
		);
		nonEmptyString(record.key, `${path}.key`);
		decimal(record.revision, `${path}.revision`);
		optional(record.incarnation, `${path}.incarnation`, decimal);
		optional(record.tombstoneRevision, `${path}.tombstoneRevision`, decimal);
		for (const member of ['fields', 'links'] as const) {
			for (const [name, stored] of Object.entries(
				plainRecord(record[member], `${path}.${member}`)
			)) {
				const storedPath = `${path}.${member}.${name}`;
				const cell = exactRecord(stored, storedPath, ['revision', 'value']);
				decimal(cell.revision, `${storedPath}.revision`);
				(member === 'fields' ? assertJsonValue : assertRecordLink)(
					cell.value,
					`${storedPath}.value`
				);
			}
		}
	}
	for (const [index, value] of array(cache.indexes, `${root}.indexes`).entries()) {
		const path = `${root}.indexes[${index}]`;
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
		decimal(entry.revision, `${path}.revision`);
		optional(entry.staleRevision, `${path}.staleRevision`, decimal);
		uniqueStrings(entry.records, `${path}.records`);
		boolean(entry.complete, `${path}.complete`);
		boolean(entry.deleted, `${path}.deleted`);
		optional(entry.metadata, `${path}.metadata`, assertIndexMetadata);
	}
	const snapshot = cache as unknown as CacheEngineSnapshot;
	try {
		createCacheEngine().restore(snapshot);
	} catch (error) {
		throw new TypeError(
			`invalid replica state at ${root}: ${(error as Error).message}`
		);
	}
	return snapshot;
}

function assertIndexMetadata(value: unknown, path: string): void {
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
	optional(metadata.parent, `${path}.parent`, nonEmptyString);
	optional(metadata.parentRevision, `${path}.parentRevision`, decimal);
	optional(metadata.parentIncarnation, `${path}.parentIncarnation`, decimal);
	nonEmptyString(metadata.field, `${path}.field`);
	assertJsonValue(
		plainRecord(metadata.arguments, `${path}.arguments`),
		`${path}.arguments`
	);
	assertCoverage(metadata.coverage, `${path}.coverage`);
	uniqueStrings(metadata.dependencies, `${path}.dependencies`);
	optional(metadata.staleReason, `${path}.staleReason`, nonEmptyString);
	optional(metadata.nullValue, `${path}.nullValue`, boolean);
}

function assertCoverage(value: unknown, path: string): void {
	const coverage = plainRecord(value, path);
	const members = (
		kinds: Readonly<Record<string, (value: unknown, path: string) => unknown>>,
		required: readonly string[] = []
	): void => {
		exactRecord(coverage, path, ['kind', ...Object.keys(kinds)], [
			'kind',
			...required
		]);
		for (const [key, check] of Object.entries(kinds)) {
			optional(coverage[key], `${path}.${key}`, check);
		}
	};
	switch (coverage.kind) {
		case 'complete':
		case 'unknown':
			return members({});
		case 'offset':
			return members(
				{
					offset: nonNegativeInteger,
					limit: nonNegativeInteger,
					returned: nonNegativeInteger,
					hasNext: boolean
				},
				['offset']
			);
		case 'cursor':
			return members({
				after: assertJsonValue,
				before: assertJsonValue,
				first: nonNegativeInteger,
				last: nonNegativeInteger,
				start: assertJsonValue,
				end: assertJsonValue,
				hasNext: boolean,
				hasPrevious: boolean
			});
		default:
			invalid(`${path}.kind`);
	}
}

function parseOperations(
	value: unknown
): readonly SerializedOperationProtocolGroup[] {
	const root = 'state.payload.operations';
	const keys = new Set<string>();
	return Object.freeze(
		array(value, root).map((entry, index) => {
			const path = `${root}[${index}]`;
			const raw = exactRecord(
				entry,
				path,
				['key', 'query', 'live', 'active', 'generation'],
				['key', 'generation']
			);
			const key = nonEmptyString(raw.key, `${path}.key`);
			if (!key.startsWith('protocol:') || keys.has(key)) invalid(`${path}.key`);
			keys.add(key);
			const query = optional(raw.query, `${path}.query`, parseOperationState);
			const live = optional(raw.live, `${path}.live`, parseOperationState);
			if (query === undefined && live === undefined) invalid(path);
			const active = optional(raw.active, `${path}.active`, operationSource);
			if (
				(active === 'query' && query === undefined) ||
				(active === 'live' && live === undefined)
			) {
				invalid(`${path}.active`);
			}
			return Object.freeze({
				key,
				...(query === undefined ? {} : { query }),
				...(live === undefined ? {} : { live }),
				...(active === undefined ? {} : { active }),
				generation: nonNegativeInteger(raw.generation, `${path}.generation`)
			});
		})
	);
}

function parseOperationState(
	value: unknown,
	path: string
): SerializedOperationProtocolState {
	const raw = exactRecord(
		value,
		path,
		[
			'operation',
			'snapshotScope',
			'indexClocks',
			'indexRevision',
			'retiredAtRevision',
			'indexKeys',
			'pathRecords',
			'cursors'
		],
		['operation', 'indexClocks', 'indexKeys', 'pathRecords', 'cursors']
	);
	const snapshotScope = optional(raw.snapshotScope, `${path}.snapshotScope`, opaque);
	const indexRevision = optional(raw.indexRevision, `${path}.indexRevision`, decimal);
	const retiredAtRevision = optional(
		raw.retiredAtRevision,
		`${path}.retiredAtRevision`,
		decimal
	);
	return Object.freeze({
		operation: nonEmptyString(raw.operation, `${path}.operation`),
		...(snapshotScope === undefined ? {} : { snapshotScope }),
		indexClocks: parseUniquePairs(
			raw.indexClocks,
			`${path}.indexClocks`,
			(entry, entryPath) => {
				const clock = exactRecord(entry, entryPath, ['scopeToken', 'position']);
				return Object.freeze({
					scopeToken: opaque(clock.scopeToken, `${entryPath}.scopeToken`),
					position: decimal(clock.position, `${entryPath}.position`)
				});
			}
		),
		...(indexRevision === undefined ? {} : { indexRevision }),
		...(retiredAtRevision === undefined ? {} : { retiredAtRevision }),
		indexKeys: uniqueStrings(raw.indexKeys, `${path}.indexKeys`),
		pathRecords: parseUniquePairs(raw.pathRecords, `${path}.pathRecords`, nonEmptyString),
		cursors: Object.freeze(
			array(raw.cursors, `${path}.cursors`).map((entry, index) => {
				const entryPath = `${path}.cursors[${index}]`;
				const cursor = exactRecord(entry, entryPath, [
					'projection',
					'position',
					'token'
				]);
				return Object.freeze({
					projection: nonEmptyString(cursor.projection, `${entryPath}.projection`),
					position: decimal(cursor.position, `${entryPath}.position`),
					token: opaque(cursor.token, `${entryPath}.token`)
				});
			})
		)
	});
}

function parseRecordClock(value: unknown, path: string): RecordProtocolClock {
	const raw = exactRecord(value, path, [
		'scopeToken',
		'incarnation',
		'revision',
		'tombstone'
	]);
	return Object.freeze({
		scopeToken: opaque(raw.scopeToken, `${path}.scopeToken`),
		incarnation: decimal(raw.incarnation, `${path}.incarnation`),
		revision: decimal(raw.revision, `${path}.revision`),
		tombstone: boolean(raw.tombstone, `${path}.tombstone`)
	});
}

/** What the separately valid parts of a payload must agree on. */
function assertMetadataConsistent(payload: ReplicaDehydratedPayloadV1): void {
	const clocks = new Map(payload.recordClocks);
	const scopes = new Set<string>();
	for (const [key, clock] of payload.recordClocks) {
		if (scopes.has(clock.scopeToken)) {
			throw new TypeError(`replica state repeats the record scope of ${key}`);
		}
		scopes.add(clock.scopeToken);
	}
	for (const [scopeToken, anonymous] of payload.anonymousRecordClocks) {
		if (anonymous.clock.scopeToken !== scopeToken || scopes.has(scopeToken)) {
			throw new TypeError(
				'replica state has an anonymous record clock under the wrong scope'
			);
		}
	}
	for (const record of payload.cache.records) {
		const disagrees = (): never => {
			throw new TypeError(`replica state record clock disagrees with ${record.key}`);
		};
		const clock = clocks.get(record.key);
		// Only records of a model carry a server clock.
		const modeled = modelFromRecordKey(record.key) !== undefined;
		const tombstoned = record.tombstoneRevision !== undefined;
		if (clock === undefined) {
			if (modeled) disagrees();
			continue;
		}
		if (
			tombstoned &&
			!(clock.tombstone && clock.revision === record.tombstoneRevision)
		) {
			disagrees();
		}
		if (
			modeled &&
			(clock.incarnation !== record.incarnation ||
				clock.revision !== record.revision ||
				clock.tombstone !== tombstoned)
		) {
			disagrees();
		}
	}
	const next = payload.nextIndexRevision as DistributedDecimalString;
	const beyondCheckpoint = (revision: string | undefined): boolean =>
		revision !== undefined &&
		compareDistributedDecimal(revision as DistributedDecimalString, next) > 0;
	const revisionsByIndex = new Map<string, Set<string>>();
	for (const group of payload.operations) {
		for (const state of [group.query, group.live]) {
			if (state === undefined) continue;
			if (
				beyondCheckpoint(state.indexRevision) ||
				beyondCheckpoint(state.retiredAtRevision)
			) {
				throw new TypeError('replica state operation revision exceeds its checkpoint');
			}
			if (state.indexRevision === undefined && state.indexKeys.length > 0) {
				throw new TypeError('replica state operation index keys lack a revision');
			}
			for (const [, recordKey] of state.pathRecords) {
				if (modelFromRecordKey(recordKey) !== undefined && !clocks.has(recordKey)) {
					throw new TypeError('replica state operation path lacks a record clock');
				}
			}
			if (state.indexRevision === undefined) continue;
			for (const key of state.indexKeys) {
				let revisions = revisionsByIndex.get(key);
				if (revisions === undefined) {
					revisions = new Set();
					revisionsByIndex.set(key, revisions);
				}
				revisions.add(state.indexRevision);
			}
		}
	}
	for (const index of payload.cache.indexes) {
		if (
			beyondCheckpoint(index.revision) ||
			!revisionsByIndex.get(index.key)?.has(index.revision)
		) {
			throw new TypeError(
				`replica state index lacks a matching operation checkpoint: ${index.key}`
			);
		}
	}
}

function parseUniquePairs<T>(
	value: unknown,
	path: string,
	parseValue: (value: unknown, path: string) => T
): readonly (readonly [string, T])[] {
	const keys = new Set<string>();
	return Object.freeze(
		array(value, path).map((entry, index) => {
			const entryPath = `${path}[${index}]`;
			if (!Array.isArray(entry) || entry.length !== 2) invalid(entryPath);
			const key = nonEmptyString(entry[0], `${entryPath}[0]`);
			if (keys.has(key)) invalid(`${entryPath}[0]`);
			keys.add(key);
			return Object.freeze([key, parseValue(entry[1], `${entryPath}[1]`)] as const);
		})
	);
}

function uniqueStrings(value: unknown, path: string): readonly string[] {
	const seen = new Set<string>();
	return Object.freeze(
		array(value, path).map((entry, index) => {
			const string = nonEmptyString(entry, `${path}[${index}]`);
			if (seen.has(string)) invalid(`${path}[${index}]`);
			seen.add(string);
			return string;
		})
	);
}

function assertRecordLink(value: unknown, path: string): void {
	if (value === null) return;
	if (typeof value === 'string') return void nonEmptyString(value, path);
	uniqueStrings(value, path);
}

function assertJsonValue(
	value: unknown,
	path: string,
	ancestors = new Set<object>()
): void {
	if (value === null || typeof value === 'string' || typeof value === 'boolean') {
		return;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) invalid(path);
		return;
	}
	if (typeof value !== 'object' || ancestors.has(value)) invalid(path);
	ancestors.add(value);
	if (Array.isArray(value)) {
		for (const [index, entry] of value.entries()) {
			assertJsonValue(entry, `${path}[${index}]`, ancestors);
		}
	} else {
		for (const [key, entry] of Object.entries(plainRecord(value, path))) {
			assertJsonValue(entry, `${path}.${key}`, ancestors);
		}
	}
	ancestors.delete(value);
}

function optional<T>(
	value: unknown,
	path: string,
	parse: (value: unknown, path: string) => T
): T | undefined {
	return value === undefined ? undefined : parse(value, path);
}

/** A plain object with only `allowed` members and every `required` one. */
function exactRecord(
	value: unknown,
	path: string,
	allowed: readonly string[],
	required: readonly string[] = allowed
): Record<string, unknown> {
	const record = plainRecord(value, path);
	for (const key of Object.keys(record)) {
		if (!allowed.includes(key)) invalid(`${path}.${key}`);
	}
	for (const key of required) {
		if (!Object.prototype.hasOwnProperty.call(record, key)) {
			invalid(`${path}.${key}`);
		}
	}
	return record;
}

function plainRecord(value: unknown, path: string): Record<string, unknown> {
	if (!isPlainRecord(value)) invalid(path);
	return value;
}

function array(value: unknown, path: string): unknown[] {
	if (!Array.isArray(value)) invalid(path);
	return value;
}

function nonEmptyString(value: unknown, path: string): string {
	if (typeof value !== 'string' || value.length === 0) invalid(path);
	return value;
}

function opaque(value: unknown, path: string): DistributedOpaqueString {
	return nonEmptyString(value, path) as DistributedOpaqueString;
}

function decimal(value: unknown, path: string): DistributedDecimalString {
	const string = nonEmptyString(value, path);
	if (!/^(0|[1-9][0-9]*)$/.test(string)) invalid(path);
	return string as DistributedDecimalString;
}

function boolean(value: unknown, path: string): boolean {
	if (typeof value !== 'boolean') invalid(path);
	return value;
}

function nonNegativeInteger(value: unknown, path: string): number {
	if (!Number.isSafeInteger(value) || (value as number) < 0) invalid(path);
	return value as number;
}

function operationSource(value: unknown, path: string): OperationProtocolSource {
	if (value !== 'query' && value !== 'live') invalid(path);
	return value;
}

function invalid(path: string): never {
	throw new TypeError(`invalid replica state at ${path}`);
}
