import type {
	CacheEngineSnapshot,
	CacheIndexCoverage,
	CacheIndexMetadata,
	CacheReader,
	CacheValue,
	DerivedIndexMutation,
	DerivedIndexOperation,
	DerivedIndexReconciler,
	IndexKey,
	OptimisticLayerView,
	OverlayOperation,
	RecordKey,
	RecordLink,
	Revision,
	StoredField,
	StoredIndex,
	StoredRecord
} from './types.js';

import { assertName } from '../../lib/assert-name.js';
import { canonicalJson } from '../../lib/canonical-json.js';
import { compareCodeUnits } from '../../lib/compare-code-units.js';
import { deepEqual } from '../../lib/deep-equal.js';
import { freezeRecord } from '../../lib/freeze-record.js';
import { reportSafely, reportUnhandledError } from '../../lib/report.js';

export function parseSnapshot(snapshot: CacheEngineSnapshot): {
	records: Map<RecordKey, StoredRecord>;
	indexes: Map<IndexKey, StoredIndex>;
} {
	if (!snapshot || snapshot.version !== 1) throw new TypeError('unsupported cache snapshot');
	if (!Array.isArray(snapshot.records) || !Array.isArray(snapshot.indexes)) {
		throw new TypeError('invalid cache snapshot collections');
	}
	const records = new Map<RecordKey, StoredRecord>();
	for (const input of snapshot.records) {
		validateRecordKey(input.key);
		if (records.has(input.key)) throw new TypeError(`duplicate snapshot record: ${input.key}`);
		const revision = revisionToken(input.revision);
		const incarnation = revisionToken(input.incarnation ?? input.revision);
		const tombstoneRevision =
			input.tombstoneRevision === undefined
				? undefined
				: revisionToken(input.tombstoneRevision);
		if (tombstoneRevision !== undefined && tombstoneRevision !== revision) {
			throw new TypeError(`invalid tombstone revision for ${input.key}`);
		}
		const fields = new Map<string, StoredField<CacheValue>>();
		for (const [name, field] of Object.entries(input.fields) as Array<
			[string, { readonly revision: string; readonly value: CacheValue }]
		>) {
			assertName(name, 'record field');
			const fieldRevision = revisionToken(field.revision);
			if (fieldRevision > revision) throw new TypeError(`field revision exceeds ${input.key}`);
			fields.set(name, { revision: fieldRevision, value: cloneCacheValue(field.value) });
		}
		const links = new Map<string, StoredField<RecordLink>>();
		for (const [name, link] of Object.entries(input.links) as Array<
			[string, { readonly revision: string; readonly value: RecordLink }]
		>) {
			assertName(name, 'record link');
			const linkRevision = revisionToken(link.revision);
			if (linkRevision > revision) throw new TypeError(`link revision exceeds ${input.key}`);
			links.set(name, { revision: linkRevision, value: cloneLink(link.value) });
		}
		if (tombstoneRevision !== undefined && (fields.size > 0 || links.size > 0)) {
			throw new TypeError(`tombstone record contains live fields: ${input.key}`);
		}
		records.set(input.key, {
			revision,
			incarnation,
			tombstoneRevision,
			fields,
			links
		});
	}

	const indexes = new Map<IndexKey, StoredIndex>();
	for (const input of snapshot.indexes) {
		validateIndexWrite(input);
		if (indexes.has(input.key)) throw new TypeError(`duplicate snapshot index: ${input.key}`);
		validateRecordKeys(input.records);
		const revision = revisionToken(input.revision);
		const staleRevision =
			input.staleRevision === undefined
				? undefined
				: revisionToken(input.staleRevision);
		if (staleRevision !== undefined && staleRevision < revision) {
			throw new TypeError(`stale index revision precedes its snapshot: ${input.key}`);
		}
		indexes.set(input.key, {
			revision,
			staleRevision,
			records: Object.freeze([...input.records]),
			complete: Boolean(input.complete),
			deleted: Boolean(input.deleted),
			metadata:
				input.metadata === undefined ? undefined : cloneIndexMetadata(input.metadata)
		});
	}
	return { records, indexes };
}

export function runDerivedIndexReconciler(
	reconciler: DerivedIndexReconciler,
	confirmed: CacheReader,
	layers: readonly OptimisticLayerView[]
): readonly DerivedIndexMutation[] {
	const result = reconciler(confirmed, Object.freeze([...layers]));
	assertSynchronousResult(result, 'derived index reconciler');
	if (!Array.isArray(result)) {
		throw new TypeError('derived index reconciler must return an array');
	}
	return result;
}

export function cloneDerivedIndexOperations(
	input: readonly DerivedIndexMutation[]
): readonly DerivedIndexOperation[] {
	const operations: DerivedIndexOperation[] = [];
	const keys = new Set<IndexKey>();
	for (const [index, value] of input.entries()) {
		const path = `derived index mutation ${index}`;
		if (
			value === null ||
			typeof value !== 'object' ||
			Array.isArray(value) ||
			(Object.getPrototypeOf(value) !== Object.prototype &&
				Object.getPrototypeOf(value) !== null)
		) {
			throw new TypeError(`${path} must be a plain object`);
		}
		if (value.kind === 'write') {
			assertExactKeys(value, ['kind', 'write'], path);
			const write = value.write;
			if (
				write === null ||
				typeof write !== 'object' ||
				Array.isArray(write) ||
				(Object.getPrototypeOf(write) !== Object.prototype &&
					Object.getPrototypeOf(write) !== null)
			) {
				throw new TypeError(`${path}.write must be a plain object`);
			}
			assertExactKeys(
				write as unknown as Record<string, unknown>,
				['key', 'records', 'complete', 'metadata'],
				`${path}.write`,
				['key', 'records']
			);
			validateIndexWrite(write);
			if (
				write.complete !== undefined &&
				typeof write.complete !== 'boolean'
			) {
				throw new TypeError(`${path}.write.complete must be a boolean`);
			}
			if (keys.has(write.key)) {
				throw new TypeError(`duplicate derived index mutation: ${write.key}`);
			}
			keys.add(write.key);
			operations.push(
				Object.freeze({
					kind: 'write-index' as const,
					write: Object.freeze({
						key: write.key,
						records: Object.freeze([...write.records]),
						complete: write.complete ?? false,
						...(write.metadata === undefined
							? {}
							: { metadata: cloneIndexMetadata(write.metadata) })
					})
				})
			);
			continue;
		}
		if (value.kind === 'stale') {
			assertExactKeys(value, ['kind', 'key', 'reason'], path);
			assertName(value.key, `${path} key`);
			assertName(value.reason, `${path} reason`);
			if (keys.has(value.key)) {
				throw new TypeError(`duplicate derived index mutation: ${value.key}`);
			}
			keys.add(value.key);
			operations.push(
				Object.freeze({
					kind: 'mark-index-stale' as const,
					key: value.key,
					reason: value.reason
				})
			);
			continue;
		}
		if (value.kind === 'delete') {
			assertExactKeys(value, ['kind', 'key'], path);
			assertName(value.key, `${path} key`);
			if (keys.has(value.key)) {
				throw new TypeError(`duplicate derived index mutation: ${value.key}`);
			}
			keys.add(value.key);
			operations.push(
				Object.freeze({ kind: 'delete-index' as const, key: value.key })
			);
			continue;
		}
		throw new TypeError(`${path} has unsupported kind`);
	}
	return Object.freeze(
		operations.sort((left, right) =>
			compareCodeUnits(
				derivedIndexOperationKey(left),
				derivedIndexOperationKey(right)
			)
		)
	);
}

export function derivedIndexKeys(
	operations: readonly DerivedIndexOperation[]
): readonly IndexKey[] {
	return Object.freeze(
		[...new Set(operations.map(derivedIndexOperationKey))].sort()
	);
}

export function derivedIndexOperationKey(
	operation: DerivedIndexOperation
): IndexKey {
	return operation.kind === 'write-index'
		? operation.write.key
		: operation.key;
}

export function assertExactKeys(
	value: Record<string, unknown>,
	allowed: readonly string[],
	description: string,
	required: readonly string[] = allowed
): void {
	const allowedKeys = new Set(allowed);
	for (const key of Object.keys(value)) {
		if (!allowedKeys.has(key)) {
			throw new TypeError(`${description} contains unknown field ${key}`);
		}
	}
	for (const key of required) {
		if (!Object.prototype.hasOwnProperty.call(value, key)) {
			throw new TypeError(`${description} is missing field ${key}`);
		}
	}
}

export function operationDependencies(operation: OverlayOperation): readonly string[] {
	if (operation.kind === 'write-record') {
		return [
			recordSeenDependency(operation.write.key),
			...Object.keys(operation.write.fields ?? {}).map((name) =>
				recordFieldDependency(operation.write.key, `field:${name}`)
			),
			...Object.keys(operation.write.links ?? {}).map((name) =>
				recordFieldDependency(operation.write.key, `link:${name}`)
			)
		];
	}
	if (operation.kind === 'tombstone-record') {
		return [recordSeenDependency(operation.key), recordWildcardDependency(operation.key)];
	}
	return [indexDependency(operation.kind === 'write-index' ? operation.write.key : operation.key)];
}

export function recordSeenDependency(key: RecordKey): string {
	return JSON.stringify(['record-seen', key]);
}

export function recordWildcardDependency(key: RecordKey): string {
	return JSON.stringify(['record', key, '*']);
}

export function recordFieldDependency(key: RecordKey, field: string): string {
	return JSON.stringify(['record', key, field]);
}

export function indexDependency(key: IndexKey): string {
	return JSON.stringify(['index', key]);
}

/** Read by `CacheReader.indexKeys`; changes whenever any index does. */
export const indexKeysDependency = JSON.stringify(['indexes']);

export function validateRecordKey(key: RecordKey): void {
	assertName(key, 'record key');
}

export function validateIndexWrite(write: {
	key: IndexKey;
	records: readonly RecordKey[];
	metadata?: CacheIndexMetadata;
}): void {
	assertName(write.key, 'index key');
	if (!Array.isArray(write.records)) throw new TypeError('index records must be an array');
	validateRecordKeys(write.records);
	if ('metadata' in write && write.metadata !== undefined) {
		const metadata = cloneIndexMetadata(write.metadata);
		const expectedKey = cacheIndexKey({
			...(metadata.parent === undefined ? {} : { parent: metadata.parent }),
			field: metadata.field,
			arguments: metadata.arguments
		});
		if (write.key !== expectedKey) {
			throw new TypeError(`index key does not match its metadata: expected ${expectedKey}`);
		}
	}
}

export function indexMetadataWithoutStaleReason(
	metadata: CacheIndexMetadata | undefined
): Omit<CacheIndexMetadata, 'staleReason'> | undefined {
	if (metadata === undefined) return undefined;
	const { staleReason: _staleReason, ...rest } = metadata;
	return rest;
}

export function isOrderedSubsequence(
	known: readonly RecordKey[],
	complete: readonly RecordKey[]
): boolean {
	let knownIndex = 0;
	for (const key of complete) {
		if (key === known[knownIndex]) knownIndex += 1;
	}
	return knownIndex === known.length;
}

export function refinementMetadataCompatible(
	current: CacheIndexMetadata | undefined,
	next: CacheIndexMetadata | undefined
): boolean {
	if (current === undefined || next === undefined) return current === next;
	return deepEqual(refinementMetadataIdentity(current), refinementMetadataIdentity(next));
}

export function refinementMetadataIdentity(metadata: CacheIndexMetadata): unknown {
	return {
		parent: metadata.parent,
		parentRevision: metadata.parentRevision,
		parentIncarnation: metadata.parentIncarnation,
		field: metadata.field,
		arguments: metadata.arguments,
		dependencies: metadata.dependencies,
		nullValue: metadata.nullValue,
		coverage: coverageRequestIdentity(metadata.coverage)
	};
}

export function coverageRequestIdentity(coverage: CacheIndexCoverage): unknown {
	if (coverage.kind === 'complete' || coverage.kind === 'unknown') {
		return { kind: coverage.kind };
	}
	if (coverage.kind === 'offset') {
		return { kind: coverage.kind, offset: coverage.offset, limit: coverage.limit };
	}
	return {
		kind: coverage.kind,
		after: coverage.after,
		before: coverage.before,
		first: coverage.first,
		last: coverage.last
	};
}

export function cloneIndexMetadata(metadata: CacheIndexMetadata): CacheIndexMetadata {
	if (!metadata || typeof metadata !== 'object') {
		throw new TypeError('index metadata must be an object');
	}
	assertName(metadata.field, 'index field');
	if (metadata.parent !== undefined) validateRecordKey(metadata.parent);
	if (metadata.parentRevision !== undefined) revisionToken(metadata.parentRevision);
	if (metadata.parentIncarnation !== undefined) revisionToken(metadata.parentIncarnation);
	const argumentsValue = cloneCacheValue(metadata.arguments);
	if (
		argumentsValue === null ||
		Array.isArray(argumentsValue) ||
		typeof argumentsValue !== 'object'
	) {
		throw new TypeError('index arguments must be a plain object');
	}
	if (!Array.isArray(metadata.dependencies)) {
		throw new TypeError('index dependencies must be an array');
	}
	const dependencies = [...metadata.dependencies];
	const seen = new Set<string>();
	for (const dependency of dependencies) {
		assertName(dependency, 'index dependency');
		if (seen.has(dependency)) {
			throw new TypeError(`duplicate index dependency: ${dependency}`);
		}
		seen.add(dependency);
	}
	if (metadata.staleReason !== undefined) {
		assertName(metadata.staleReason, 'index stale reason');
	}
	if (metadata.nullValue !== undefined && typeof metadata.nullValue !== 'boolean') {
		throw new TypeError('index nullValue must be a boolean');
	}

	const coverage = cloneIndexCoverage(metadata.coverage);
	return Object.freeze({
		...(metadata.parent === undefined ? {} : { parent: metadata.parent }),
		...(metadata.parentRevision === undefined
			? {}
			: { parentRevision: metadata.parentRevision }),
		...(metadata.parentIncarnation === undefined
			? {}
			: { parentIncarnation: metadata.parentIncarnation }),
		field: metadata.field,
		arguments: argumentsValue as Readonly<Record<string, CacheValue>>,
		coverage,
		dependencies: Object.freeze(dependencies),
		...(metadata.staleReason === undefined
			? {}
			: { staleReason: metadata.staleReason }),
		...(metadata.nullValue === undefined ? {} : { nullValue: metadata.nullValue })
	});
}

export function cloneIndexCoverage(coverage: CacheIndexCoverage): CacheIndexCoverage {
	if (!coverage || typeof coverage !== 'object') {
		throw new TypeError('index coverage must be an object');
	}
	if (coverage.kind === 'complete' || coverage.kind === 'unknown') {
		return Object.freeze({ kind: coverage.kind });
	}
	if (coverage.kind === 'offset') {
		assertNonNegativeSafeInteger(coverage.offset, 'offset coverage offset');
		if (coverage.limit !== undefined) {
			assertNonNegativeSafeInteger(coverage.limit, 'offset coverage limit');
		}
		if (coverage.returned !== undefined) {
			assertNonNegativeSafeInteger(coverage.returned, 'offset coverage returned');
		}
		if (coverage.hasNext !== undefined && typeof coverage.hasNext !== 'boolean') {
			throw new TypeError('offset coverage hasNext must be a boolean');
		}
		return Object.freeze({
			kind: 'offset' as const,
			offset: coverage.offset,
			...(coverage.limit === undefined ? {} : { limit: coverage.limit }),
			...(coverage.returned === undefined ? {} : { returned: coverage.returned }),
			...(coverage.hasNext === undefined ? {} : { hasNext: coverage.hasNext })
		});
	}
	if (coverage.kind === 'cursor') {
		if (coverage.first !== undefined) {
			assertNonNegativeSafeInteger(coverage.first, 'cursor coverage first');
		}
		if (coverage.last !== undefined) {
			assertNonNegativeSafeInteger(coverage.last, 'cursor coverage last');
		}
		if (coverage.hasNext !== undefined && typeof coverage.hasNext !== 'boolean') {
			throw new TypeError('cursor coverage hasNext must be a boolean');
		}
		if (
			coverage.hasPrevious !== undefined &&
			typeof coverage.hasPrevious !== 'boolean'
		) {
			throw new TypeError('cursor coverage hasPrevious must be a boolean');
		}
		return Object.freeze({
			kind: 'cursor' as const,
			...(coverage.after === undefined
				? {}
				: { after: cloneCacheValue(coverage.after) }),
			...(coverage.before === undefined
				? {}
				: { before: cloneCacheValue(coverage.before) }),
			...(coverage.first === undefined ? {} : { first: coverage.first }),
			...(coverage.last === undefined ? {} : { last: coverage.last }),
			...(coverage.start === undefined
				? {}
				: { start: cloneCacheValue(coverage.start) }),
			...(coverage.end === undefined ? {} : { end: cloneCacheValue(coverage.end) }),
			...(coverage.hasNext === undefined ? {} : { hasNext: coverage.hasNext }),
			...(coverage.hasPrevious === undefined
				? {}
				: { hasPrevious: coverage.hasPrevious })
		});
	}
	throw new TypeError('unsupported index coverage kind');
}

export function assertNonNegativeSafeInteger(value: number, description: string): void {
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new TypeError(`${description} must be a non-negative safe integer`);
	}
}

export function assertWriterActive(active: boolean): void {
	if (!active) throw new Error('cache writer is no longer active');
}

export function assertSynchronousResult(result: unknown, description: string): void {
	if (
		result !== null &&
		(typeof result === 'object' || typeof result === 'function') &&
		typeof (result as { then?: unknown }).then === 'function'
	) {
		void Promise.resolve(result).catch(() => undefined);
		throw new TypeError(`${description} must be synchronous`);
	}
}

export function validateRecordKeys(keys: readonly RecordKey[]): void {
	const seen = new Set<RecordKey>();
	for (const key of keys) {
		validateRecordKey(key);
		if (seen.has(key)) throw new TypeError(`duplicate record in index: ${key}`);
		seen.add(key);
	}
}

export function revisionToken(value: Revision): bigint {
	if (typeof value === 'bigint') {
		if (value < 0n) throw new TypeError('revision must be an unsigned integer');
		return value;
	}
	if (typeof value === 'number') {
		if (!Number.isSafeInteger(value) || value < 0) {
			throw new TypeError('numeric revision must be an unsigned safe integer');
		}
		return BigInt(value);
	}
	if (!/^(0|[1-9][0-9]*)$/.test(value)) {
		throw new TypeError('string revision must be a canonical unsigned integer');
	}
	return BigInt(value);
}

export function compareRecordTuple(
	leftIncarnation: bigint,
	leftRevision: bigint,
	rightIncarnation: bigint,
	rightRevision: bigint
): -1 | 0 | 1 {
	if (leftIncarnation < rightIncarnation) return -1;
	if (leftIncarnation > rightIncarnation) return 1;
	if (leftRevision < rightRevision) return -1;
	if (leftRevision > rightRevision) return 1;
	return 0;
}

export function revisionString(value: bigint): string {
	return value.toString(10);
}

export function cloneFields(
	fields: Readonly<Record<string, CacheValue>> | undefined
): Readonly<Record<string, CacheValue>> {
	if (fields === undefined) return Object.freeze({});
	return freezeRecord(
		Object.entries(fields).map(([name, value]) => {
			assertName(name, 'record field');
			return [name, cloneCacheValue(value)];
		})
	);
}

export function cloneLinks(
	links: Readonly<Record<string, RecordLink>> | undefined
): Readonly<Record<string, RecordLink>> {
	if (links === undefined) return Object.freeze({});
	return freezeRecord(
		Object.entries(links).map(([name, value]) => {
			assertName(name, 'record link');
			return [name, cloneLink(value)];
		})
	);
}

export function cloneLink(value: RecordLink): RecordLink {
	if (value === null) return null;
	if (typeof value === 'string') {
		validateRecordKey(value);
		return value;
	}
	if (!Array.isArray(value)) throw new TypeError('record link must be a key, key array, or null');
	validateRecordKeys(value);
	return Object.freeze([...value]);
}

export function linkKeys(value: RecordLink): readonly RecordKey[] {
	if (value === null) return [];
	return typeof value === 'string' ? [value] : value;
}

export function cloneCacheValue(value: CacheValue, ancestors = new Set<object>()): CacheValue {
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean' ||
		typeof value === 'number'
	) {
		if (typeof value === 'number' && !Number.isFinite(value)) {
			throw new TypeError('cache numbers must be finite');
		}
		return value;
	}
	if (typeof value !== 'object') {
		throw new TypeError('cache fields must contain JSON-compatible values; omit absent fields');
	}
	if (ancestors.has(value)) throw new TypeError('cache fields must not contain cycles');
	ancestors.add(value);
	let cloned: CacheValue;
	if (Array.isArray(value)) {
		cloned = Object.freeze(value.map((entry) => cloneCacheValue(entry, ancestors)));
	} else {
		const prototype = Object.getPrototypeOf(value);
		if (prototype !== Object.prototype && prototype !== null) {
			throw new TypeError('cache objects must be plain JSON objects');
		}
		cloned = freezeRecord(
			Object.entries(value).map(([key, entry]) => [key, cloneCacheValue(entry, ancestors)])
		);
	}
	ancestors.delete(value);
	return cloned;
}

export function cacheIndexKey(input: {
	parent?: RecordKey;
	field: string;
	arguments?: Readonly<Record<string, CacheValue>>;
}): IndexKey {
	assertName(input.field, 'index field');
	if (input.parent !== undefined) validateRecordKey(input.parent);
	const argumentsValue = cloneCacheValue(input.arguments ?? {});
	return `${input.parent ?? '$root'}.${input.field}(${canonicalJson(argumentsValue)})`;
}

export { assertName, deepEqual, freezeRecord, reportSafely };
export const reportUnhandledWatcherError = reportUnhandledError;
