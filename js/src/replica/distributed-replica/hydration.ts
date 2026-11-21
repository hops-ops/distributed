import type { CacheEngineSnapshot } from '../../internal/cache-engine.js';
import {
	compareDistributedDecimal,
	parseDistributedTrustedPresetInventory,
	type DistributedDecimalString,
	type DistributedOpaqueString
} from '../../protocol.js';

import { modelFromRecordKey } from './clocks.js';

import { canonicalTrustedPresets } from './helpers.js';
import type {
	AnonymousRecordProtocolClock,
	IndexProtocolClock,
	OperationProtocolGroup,
	OperationProtocolSource,
	OperationProtocolState,
	ParsedReplicaHydration,
	ProtocolGeneration,
	RecordProtocolClock,
	SerializedOperationProtocolGroup,
	SerializedOperationProtocolState
} from './types.js';

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

export function serializeOperationProtocolState(
	state: OperationProtocolState
): SerializedOperationProtocolState {
	return Object.freeze({
		operation: state.operation,
		...(state.snapshotScope === undefined
			? {}
			: { snapshotScope: state.snapshotScope }),
		indexClocks: Object.freeze(
			[...state.indexClocks]
				.sort(([left], [right]) => left.localeCompare(right))
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
				.sort(([left], [right]) => left.localeCompare(right))
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

export function parseAuthoritativeScope(
	value: unknown
): ProtocolGeneration | undefined {
	try {
		const scope = hydrationRecord(
			value,
			'authoritativeScope',
			[
				'protocolVersion',
				'schemaHash',
				'authorizationGeneration',
				'cacheScope'
			]
		);
		if (scope.protocolVersion !== 1) {
			hydrationInvalid('authoritativeScope.protocolVersion');
		}
		return Object.freeze({
			protocolVersion: 1,
			schemaHash: hydrationString(
				scope.schemaHash,
				'authoritativeScope.schemaHash'
			),
			authorizationGeneration: hydrationString(
				scope.authorizationGeneration,
				'authoritativeScope.authorizationGeneration'
			),
			cacheScope: hydrationOpaque(
				scope.cacheScope,
				'authoritativeScope.cacheScope'
			)
		});
	} catch {
		return undefined;
	}
}

export function hydrationMetadataConsistent(
	parsed: ParsedReplicaHydration
): boolean {
	try {
		const recordByKey = new Map(
			parsed.cache.records.map((record) => [record.key, record])
		);
		for (const record of parsed.cache.records) {
			if (modelFromRecordKey(record.key) === undefined) continue;
			const clock = parsed.recordClocks.get(record.key);
			if (
				clock === undefined ||
				record.incarnation === undefined ||
				clock.incarnation !== record.incarnation ||
				clock.revision !== record.revision
			) {
				return false;
			}
			if (clock.tombstone) {
				if (record.tombstoneRevision !== clock.revision) return false;
			} else if (record.tombstoneRevision !== undefined) {
				return false;
			}
		}
		for (const [recordKey, clock] of parsed.recordClocks) {
			const record = recordByKey.get(recordKey);
			if (
				record?.tombstoneRevision !== undefined &&
				(
					!clock.tombstone ||
					record.tombstoneRevision !== clock.revision
				)
			) {
				return false;
			}
		}

		const revisionsByIndex = new Map<string, Set<string>>();
		for (const group of parsed.operationProtocols.values()) {
			for (const state of [group.query, group.live]) {
				if (state === undefined) continue;
				for (const recordKey of state.pathRecords.values()) {
					if (
						modelFromRecordKey(recordKey) !== undefined &&
						!parsed.recordClocks.has(recordKey)
					) {
						return false;
					}
				}
				if (state.indexRevision === undefined) {
					if (state.indexKeys.size > 0) return false;
					continue;
				}
				for (const indexKey of state.indexKeys) {
					let revisions = revisionsByIndex.get(indexKey);
					if (revisions === undefined) {
						revisions = new Set();
						revisionsByIndex.set(indexKey, revisions);
					}
					revisions.add(state.indexRevision);
				}
			}
		}
		const nextIndexRevision =
			parsed.nextIndexRevision as DistributedDecimalString;
		for (const index of parsed.cache.indexes) {
			const revision = hydrationDecimal(
				index.revision,
				'state.payload.cache.indexes.revision'
			);
			if (
				compareDistributedDecimal(revision, nextIndexRevision) > 0 ||
				!revisionsByIndex.get(index.key)?.has(index.revision)
			) {
				return false;
			}
		}
		return true;
	} catch {
		return false;
	}
}

export function parseReplicaHydration(
	value: unknown
): ParsedReplicaHydration | undefined {
	try {
		const state = hydrationRecord(
			value,
			'state',
			['version', 'scope', 'payload']
		);
		if (state.version !== 1) hydrationInvalid('state.version');
		const scopeValue = hydrationRecord(
			state.scope,
			'state.scope',
			[
				'protocolVersion',
				'schemaHash',
				'authorizationGeneration',
				'cacheScope'
			]
		);
		if (scopeValue.protocolVersion !== 1) {
			hydrationInvalid('state.scope.protocolVersion');
		}
		const scope: ProtocolGeneration = Object.freeze({
			protocolVersion: 1,
			schemaHash: hydrationString(
				scopeValue.schemaHash,
				'state.scope.schemaHash'
			),
			authorizationGeneration: hydrationString(
				scopeValue.authorizationGeneration,
				'state.scope.authorizationGeneration'
			),
			cacheScope: hydrationOpaque(
				scopeValue.cacheScope,
				'state.scope.cacheScope'
			)
		});
		const payload = hydrationRecord(
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
		const cache = payload.cache as CacheEngineSnapshot;
		if (!isHydrationRecord(cache)) hydrationInvalid('state.payload.cache');
		const operationsValue = hydrationArray(
			payload.operations,
			'state.payload.operations'
		);
		const operationProtocols = new Map<string, OperationProtocolGroup>();
		const operationGenerations = new Map<string, number>();
		for (const [index, entry] of operationsValue.entries()) {
			const path = `state.payload.operations[${index}]`;
			const raw = hydrationRecord(
				entry,
				path,
				['key', 'query', 'live', 'active', 'generation'],
				['key', 'generation']
			);
			const key = hydrationString(raw.key, `${path}.key`);
			if (!key.startsWith('protocol:') || operationProtocols.has(key)) {
				hydrationInvalid(`${path}.key`);
			}
			const query =
				raw.query === undefined
					? undefined
					: parseOperationProtocolState(raw.query, `${path}.query`);
			const live =
				raw.live === undefined
					? undefined
					: parseOperationProtocolState(raw.live, `${path}.live`);
			if (query === undefined && live === undefined) hydrationInvalid(path);
			const active =
				raw.active === undefined
					? undefined
					: hydrationOperationSource(raw.active, `${path}.active`);
			if (
				(active === 'query' && query === undefined) ||
				(active === 'live' && live === undefined)
			) {
				hydrationInvalid(`${path}.active`);
			}
			operationProtocols.set(key, {
				...(query === undefined ? {} : { query }),
				...(live === undefined ? {} : { live }),
				...(active === undefined ? {} : { active })
			});
			operationGenerations.set(
				key,
				hydrationGeneration(raw.generation, `${path}.generation`)
			);
		}

		const recordClocks = new Map<string, RecordProtocolClock>();
		const recordKeysByScope = new Map<DistributedOpaqueString, string>();
		for (const [index, entry] of hydrationArray(
			payload.recordClocks,
			'state.payload.recordClocks'
		).entries()) {
			const path = `state.payload.recordClocks[${index}]`;
			const pair = hydrationPair(entry, path);
			const key = hydrationString(pair[0], `${path}[0]`);
			if (recordClocks.has(key)) hydrationInvalid(`${path}[0]`);
			const clock = parseRecordClock(pair[1], `${path}[1]`);
			if (recordKeysByScope.has(clock.scopeToken)) {
				hydrationInvalid(`${path}[1].scopeToken`);
			}
			recordClocks.set(key, clock);
			recordKeysByScope.set(clock.scopeToken, key);
		}

		const anonymousRecordClocks = new Map<
			DistributedOpaqueString,
			AnonymousRecordProtocolClock
		>();
		for (const [index, entry] of hydrationArray(
			payload.anonymousRecordClocks,
			'state.payload.anonymousRecordClocks'
		).entries()) {
			const path = `state.payload.anonymousRecordClocks[${index}]`;
			const pair = hydrationPair(entry, path);
			const scopeToken = hydrationOpaque(pair[0], `${path}[0]`);
			if (
				anonymousRecordClocks.has(scopeToken) ||
				recordKeysByScope.has(scopeToken)
			) {
				hydrationInvalid(`${path}[0]`);
			}
			const raw = hydrationRecord(
				pair[1],
				`${path}[1]`,
				['model', 'clock']
			);
			const clock = parseRecordClock(raw.clock, `${path}[1].clock`);
			if (clock.scopeToken !== scopeToken) {
				hydrationInvalid(`${path}[1].clock.scopeToken`);
			}
			anonymousRecordClocks.set(
				scopeToken,
				Object.freeze({
					model: hydrationString(raw.model, `${path}[1].model`),
					clock
					})
				);
			}
			const trustedPresets = canonicalTrustedPresets(
				parseDistributedTrustedPresetInventory(
					payload.trustedPresets,
					'state.payload.trustedPresets'
				)
			);
			const nextIndexRevision = hydrationDecimal(
				payload.nextIndexRevision,
			'state.payload.nextIndexRevision'
		);
		for (const group of operationProtocols.values()) {
			for (const operation of [group.query, group.live]) {
				if (
					operation?.indexRevision !== undefined &&
					compareDistributedDecimal(
						operation.indexRevision as DistributedDecimalString,
						nextIndexRevision
					) > 0
				) {
					hydrationInvalid('state.payload.nextIndexRevision');
				}
				if (
					operation?.retiredAtRevision !== undefined &&
					compareDistributedDecimal(
						operation.retiredAtRevision as DistributedDecimalString,
						nextIndexRevision
					) > 0
				) {
					hydrationInvalid('state.payload.nextIndexRevision');
				}
			}
		}
		return {
			scope,
			cache,
			operationProtocols,
			operationGenerations,
				recordClocks,
				recordKeysByScope,
				anonymousRecordClocks,
				trustedPresets,
				nextIndexRevision
			};
	} catch {
		return undefined;
	}
}

export function parseOperationProtocolState(
	value: unknown,
	path: string
): OperationProtocolState {
	const raw = hydrationRecord(
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
	const indexClocks = new Map<string, IndexProtocolClock>();
	for (const [index, entry] of hydrationArray(
		raw.indexClocks,
		`${path}.indexClocks`
	).entries()) {
		const entryPath = `${path}.indexClocks[${index}]`;
		const pair = hydrationPair(entry, entryPath);
		const projection = hydrationString(pair[0], `${entryPath}[0]`);
		if (indexClocks.has(projection)) hydrationInvalid(`${entryPath}[0]`);
		const clock = hydrationRecord(
			pair[1],
			`${entryPath}[1]`,
			['scopeToken', 'position']
		);
		indexClocks.set(
			projection,
			Object.freeze({
					scopeToken: hydrationOpaque(
						clock.scopeToken,
						`${entryPath}[1].scopeToken`
					),
				position: hydrationDecimal(
					clock.position,
					`${entryPath}[1].position`
				)
			})
		);
	}
	const indexKeys = new Set<string>();
	for (const [index, entry] of hydrationArray(
		raw.indexKeys,
		`${path}.indexKeys`
	).entries()) {
		const key = hydrationString(entry, `${path}.indexKeys[${index}]`);
		if (indexKeys.has(key)) hydrationInvalid(`${path}.indexKeys[${index}]`);
		indexKeys.add(key);
	}
	const pathRecords = new Map<string, string>();
	for (const [index, entry] of hydrationArray(
		raw.pathRecords,
		`${path}.pathRecords`
	).entries()) {
		const entryPath = `${path}.pathRecords[${index}]`;
		const pair = hydrationPair(entry, entryPath);
		const responsePath = hydrationString(pair[0], `${entryPath}[0]`);
		if (pathRecords.has(responsePath)) hydrationInvalid(`${entryPath}[0]`);
		pathRecords.set(
			responsePath,
			hydrationString(pair[1], `${entryPath}[1]`)
		);
	}
	const cursors = hydrationArray(raw.cursors, `${path}.cursors`).map(
		(entry, index) => {
			const entryPath = `${path}.cursors[${index}]`;
			const cursor = hydrationRecord(
				entry,
				entryPath,
				['projection', 'position', 'token']
			);
			return Object.freeze({
				projection: hydrationString(
					cursor.projection,
					`${entryPath}.projection`
				),
				position: hydrationDecimal(
					cursor.position,
					`${entryPath}.position`
				),
				token: hydrationOpaque(cursor.token, `${entryPath}.token`)
			});
		}
	);
	return {
		operation: hydrationString(raw.operation, `${path}.operation`),
		...(raw.snapshotScope === undefined
			? {}
			: {
					snapshotScope: hydrationOpaque(
						raw.snapshotScope,
						`${path}.snapshotScope`
					)
				}),
		indexClocks,
		...(raw.indexRevision === undefined
			? {}
			: {
					indexRevision: hydrationDecimal(
						raw.indexRevision,
						`${path}.indexRevision`
					)
				}),
		...(raw.retiredAtRevision === undefined
			? {}
			: {
					retiredAtRevision: hydrationDecimal(
						raw.retiredAtRevision,
						`${path}.retiredAtRevision`
					)
				}),
		indexKeys,
		pathRecords,
		cursors: Object.freeze(cursors)
	};
}

export function parseRecordClock(value: unknown, path: string): RecordProtocolClock {
	const raw = hydrationRecord(
		value,
		path,
		['scopeToken', 'incarnation', 'revision', 'tombstone']
	);
	if (typeof raw.tombstone !== 'boolean') hydrationInvalid(`${path}.tombstone`);
	return Object.freeze({
		scopeToken: hydrationOpaque(raw.scopeToken, `${path}.scopeToken`),
		incarnation: hydrationDecimal(raw.incarnation, `${path}.incarnation`),
		revision: hydrationDecimal(raw.revision, `${path}.revision`),
		tombstone: raw.tombstone
	});
}

export function hydrationRecord(
	value: unknown,
	path: string,
	allowed: readonly string[],
	required: readonly string[] = allowed
): Record<string, unknown> {
	if (!isHydrationRecord(value)) hydrationInvalid(path);
	const allowedKeys = new Set(allowed);
	for (const key of Object.keys(value)) {
		if (!allowedKeys.has(key)) hydrationInvalid(`${path}.${key}`);
	}
	for (const key of required) {
		if (!Object.prototype.hasOwnProperty.call(value, key)) {
			hydrationInvalid(`${path}.${key}`);
		}
	}
	return value;
}

export function isHydrationRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function hydrationArray(value: unknown, path: string): unknown[] {
	if (!Array.isArray(value)) hydrationInvalid(path);
	return value;
}

export function hydrationPair(value: unknown, path: string): readonly [unknown, unknown] {
	if (!Array.isArray(value) || value.length !== 2) hydrationInvalid(path);
	return value as unknown as readonly [unknown, unknown];
}

export function hydrationString(value: unknown, path: string): string {
	if (typeof value !== 'string' || value.length === 0) hydrationInvalid(path);
	return value;
}

export function hydrationOpaque(
	value: unknown,
	path: string
): DistributedOpaqueString {
	return hydrationString(value, path) as DistributedOpaqueString;
}

export function hydrationDecimal(
	value: unknown,
	path: string
): DistributedDecimalString {
	const string = hydrationString(value, path);
	if (!/^(0|[1-9][0-9]*)$/.test(string)) hydrationInvalid(path);
	return string as DistributedDecimalString;
}

export function hydrationGeneration(value: unknown, path: string): number {
	if (!Number.isSafeInteger(value) || (value as number) < 0) {
		hydrationInvalid(path);
	}
	return value as number;
}

export function hydrationOperationSource(
	value: unknown,
	path: string
): OperationProtocolSource {
	if (value !== 'query' && value !== 'live') hydrationInvalid(path);
	return value;
}

export function hydrationInvalid(path: string): never {
	throw new TypeError(`invalid replica hydration state at ${path}`);
}
