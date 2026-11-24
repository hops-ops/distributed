import {
	assertName,
	cloneIndexMetadata,
	deepEqual,
	freezeRecord,
	indexDependency,
	indexKeysDependency,
	recordFieldDependency,
	recordSeenDependency,
	recordWildcardDependency,
	revisionString,
	revisionToken
} from './helpers.js';
import type {
	CacheGraph,
	CacheIndex,
	CacheIndexMetadata,
	CachePresence,
	CacheReader,
	CacheValue,
	DerivedIndexOperation,
	IndexKey,
	OptimisticLayer,
	OverlayOperation,
	RecordKey,
	RecordLink,
	SparseRecord,
	SparseRecordMeta,
	StoredRecord,
	VisibleIndex,
	VisibleRecord
} from './types.js';
import { cacheEngineWork } from './work.js';

const EMPTY_KEYS: readonly RecordKey[] = Object.freeze([]);
const ABSENT: CachePresence<never> = Object.freeze({ present: false });
const DELETED_INDEX: VisibleIndex = Object.freeze({
	revision: 0n,
	staleRevision: undefined,
	records: EMPTY_KEYS,
	complete: false,
	deleted: true,
	metadata: undefined
});

/*
 * Stored and visible values never change after they are built, so what is
 * derived from one is computed once and found again by identity.
 */
const visibleRecords = new WeakMap<StoredRecord, VisibleRecord>();
const sparseRecords = new WeakMap<VisibleRecord, SparseRecord>();
const cacheIndexes = new WeakMap<VisibleIndex, CacheIndex>();

export function visibleRecord(
	stored: StoredRecord | undefined
): VisibleRecord | undefined {
	if (stored === undefined) return undefined;
	let visible = visibleRecords.get(stored);
	if (visible === undefined) {
		const fields = new Map<string, CacheValue>();
		for (const [name, field] of stored.fields) fields.set(name, field.value);
		const links = new Map<string, RecordLink>();
		for (const [name, link] of stored.links) links.set(name, link.value);
		visible = {
			revision: stored.revision,
			incarnation: stored.incarnation,
			tombstoned: stored.tombstoneRevision !== undefined,
			fields,
			links
		};
		visibleRecords.set(stored, visible);
	}
	return visible;
}

export function isVisibleRecordLive(record: VisibleRecord | undefined): boolean {
	return record !== undefined && !record.tombstoned;
}

export function sameVisibleIndex(
	left: VisibleIndex | undefined,
	right: VisibleIndex | undefined
): boolean {
	if (left === right) return true;
	if (left === undefined || right === undefined) return false;
	return (
		left.revision === right.revision &&
		left.staleRevision === right.staleRevision &&
		left.complete === right.complete &&
		left.deleted === right.deleted &&
		deepEqual(left.records, right.records) &&
		deepEqual(left.metadata, right.metadata)
	);
}

/**
 * The optimistic layers and derived index operations replayed over the base
 * graph. It holds only the keys the replay read or wrote; every other key
 * reads through to the base, so building one costs the operations replayed,
 * not the cache.
 */
export class Overlay implements CacheGraph {
	/** Keys the replay read or wrote; `undefined` is "absent". */
	readonly records = new Map<RecordKey, VisibleRecord | undefined>();
	readonly indexes = new Map<IndexKey, VisibleIndex | undefined>();
	readonly #base: CacheGraph;
	readonly #floors: ReadonlyMap<string, number>;

	constructor(
		base: CacheGraph,
		readonly layers: readonly OptimisticLayer[],
		readonly derivedIndexOperations: readonly DerivedIndexOperation[],
		floors: ReadonlyMap<string, number>
	) {
		this.#base = base;
		this.#floors = floors;
		for (const layer of layers) {
			for (const operation of layer.operations) {
				cacheEngineWork.overlayOperations += 1;
				this.#apply(layer.sequence, operation);
			}
		}
		for (const operation of derivedIndexOperations) {
			cacheEngineWork.overlayOperations += 1;
			this.#applyDerived(operation);
		}
	}

	get floors(): ReadonlyMap<string, number> {
		return this.#floors;
	}

	record(key: RecordKey): VisibleRecord | undefined {
		return this.records.has(key) ? this.records.get(key) : this.#base.record(key);
	}

	index(key: IndexKey): VisibleIndex | undefined {
		return this.indexes.has(key) ? this.indexes.get(key) : this.#base.index(key);
	}

	*indexKeys(): Iterable<IndexKey> {
		yield* this.#base.indexKeys();
		for (const key of this.indexes.keys()) {
			if (this.#base.index(key) === undefined) yield key;
		}
	}

	/** Replay reads remember the key: a later base write to it outdates this overlay. */
	#record(key: RecordKey): VisibleRecord | undefined {
		if (!this.records.has(key)) this.records.set(key, this.#base.record(key));
		return this.records.get(key);
	}

	#index(key: IndexKey): VisibleIndex | undefined {
		if (!this.indexes.has(key)) this.indexes.set(key, this.#base.index(key));
		return this.indexes.get(key);
	}

	#floor(dependency: string): number {
		return this.#floors.get(dependency) ?? 0;
	}

	#recordFieldFloor(key: RecordKey, field: string): number {
		return Math.max(
			this.#floor(recordWildcardDependency(key)),
			this.#floor(recordFieldDependency(key, field))
		);
	}

	#apply(sequence: number, operation: OverlayOperation): void {
		if (operation.kind === 'write-record') {
			const { key, fields = {}, links = {}, unset = [] } = operation.write;
			const record = this.#record(key);
			if (operation.write.ifPresent === true && (!record || record.tombstoned)) {
				return;
			}
			let nextFields: Map<string, CacheValue> | undefined;
			let nextLinks: Map<string, RecordLink> | undefined;
			for (const [name, value] of Object.entries(fields)) {
				if (sequence <= this.#recordFieldFloor(key, `field:${name}`)) continue;
				(nextFields ??= new Map(record?.fields)).set(name, value);
			}
			for (const [name, value] of Object.entries(links)) {
				if (sequence <= this.#recordFieldFloor(key, `link:${name}`)) continue;
				(nextLinks ??= new Map(record?.links)).set(name, value);
			}
			for (const name of unset) {
				if (sequence <= this.#recordFieldFloor(key, `field:${name}`)) continue;
				if (!(nextFields ?? record?.fields)?.has(name)) continue;
				(nextFields ??= new Map(record?.fields)).delete(name);
			}
			if (nextFields === undefined && nextLinks === undefined) return;
			this.records.set(key, {
				revision: record?.revision ?? 0n,
				incarnation: record?.incarnation ?? 0n,
				tombstoned: false,
				fields: nextFields ?? record?.fields ?? new Map(),
				links: nextLinks ?? record?.links ?? new Map()
			});
			return;
		}

		if (operation.kind === 'tombstone-record') {
			if (sequence <= this.#floor(recordSeenDependency(operation.key))) return;
			const record = this.#record(operation.key);
			this.records.set(operation.key, {
				revision: record?.revision ?? 0n,
				incarnation: record?.incarnation ?? 0n,
				tombstoned: true,
				fields: new Map(),
				links: new Map()
			});
			return;
		}

		const key = operation.kind === 'write-index' ? operation.write.key : operation.key;
		if (sequence <= this.#floor(indexDependency(key))) return;
		if (operation.kind === 'write-index') this.#writeIndex(operation.write);
		else this.#deleteIndex(key);
	}

	#applyDerived(operation: DerivedIndexOperation): void {
		if (operation.kind === 'write-index') {
			this.#writeIndex(operation.write);
			return;
		}
		if (operation.kind === 'delete-index') {
			this.#deleteIndex(operation.key);
			return;
		}
		const index = this.#index(operation.key);
		if (!index || index.deleted) return;
		/*
		 * Staleness is a freshness claim, not structural data loss. Keep a
		 * previously complete visible index renderable while the owner
		 * revalidates it. Operations that actually remove membership or
		 * records explicitly clear `complete` in their own lifecycle path.
		 */
		this.indexes.set(operation.key, {
			...index,
			staleRevision:
				index.staleRevision === undefined || index.staleRevision < index.revision
					? index.revision
					: index.staleRevision,
			metadata:
				index.metadata === undefined
					? undefined
					: cloneIndexMetadata({
							...index.metadata,
							staleReason: operation.reason
						})
		});
	}

	#writeIndex(write: {
		readonly key: IndexKey;
		readonly records: readonly RecordKey[];
		readonly complete?: boolean;
		readonly metadata?: CacheIndexMetadata;
	}): void {
		let metadata = write.metadata;
		if (metadata?.parent !== undefined) {
			const parent = this.#record(metadata.parent);
			if (!parent || parent.tombstoned) return;
			if (
				metadata.parentRevision !== undefined &&
				revisionToken(metadata.parentRevision) !== parent.revision
			) {
				return;
			}
			if (
				metadata.parentIncarnation !== undefined &&
				revisionToken(metadata.parentIncarnation) !== parent.incarnation
			) {
				return;
			}
			metadata = cloneIndexMetadata({
				...metadata,
				parentIncarnation: revisionString(parent.incarnation)
			});
		}
		const current = this.#index(write.key);
		this.indexes.set(write.key, {
			revision: current?.revision ?? 0n,
			staleRevision: current?.staleRevision,
			records: write.records,
			complete: write.complete ?? false,
			deleted: false,
			metadata
		});
	}

	#deleteIndex(key: IndexKey): void {
		this.indexes.set(key, {
			...(this.#index(key) ?? DELETED_INDEX),
			deleted: true,
			records: EMPTY_KEYS
		});
	}
}

/**
 * Read one graph. Values come back by identity: an unchanged record, field,
 * link or index is the same frozen object on every read.
 */
export function graphReader(
	graph: CacheGraph,
	dependencies?: Set<string>
): CacheReader {
	return Object.freeze({
		recordMeta(key: RecordKey): SparseRecordMeta | undefined {
			dependencies?.add(recordSeenDependency(key));
			const record = graph.record(key);
			if (!record || record.tombstoned) return undefined;
			return Object.freeze({
				key,
				incarnation: revisionString(record.incarnation)
			});
		},
		field(key: RecordKey, name: string): CachePresence<CacheValue> {
			assertName(name, 'record field');
			dependencies?.add(recordSeenDependency(key));
			dependencies?.add(recordFieldDependency(key, `field:${name}`));
			const record = graph.record(key);
			if (!record || record.tombstoned || !record.fields.has(name)) return ABSENT;
			return Object.freeze({ present: true, value: record.fields.get(name)! });
		},
		link(key: RecordKey, name: string): CachePresence<RecordLink> {
			assertName(name, 'record link');
			dependencies?.add(recordSeenDependency(key));
			dependencies?.add(recordFieldDependency(key, `link:${name}`));
			const record = graph.record(key);
			if (!record || record.tombstoned || !record.links.has(name)) return ABSENT;
			return Object.freeze({ present: true, value: record.links.get(name)! });
		},
		record(key: RecordKey): SparseRecord | undefined {
			dependencies?.add(recordWildcardDependency(key));
			const record = graph.record(key);
			if (!record || record.tombstoned) return undefined;
			let sparse = sparseRecords.get(record);
			if (sparse === undefined) {
				sparse = Object.freeze({
					key,
					revision: revisionString(record.revision),
					incarnation: revisionString(record.incarnation),
					fields: freezeRecord([...record.fields]),
					links: freezeRecord([...record.links])
				});
				sparseRecords.set(record, sparse);
			}
			return sparse;
		},
		index(key: IndexKey): CacheIndex | undefined {
			dependencies?.add(indexDependency(key));
			const index = graph.index(key);
			if (!index || index.deleted) return undefined;
			let visible = cacheIndexes.get(index);
			if (visible === undefined) {
				visible = Object.freeze({
					key,
					revision: revisionString(index.revision),
					...(index.staleRevision === undefined
						? {}
						: { staleRevision: revisionString(index.staleRevision) }),
					records: index.records,
					complete: index.complete,
					...(index.metadata === undefined ? {} : { metadata: index.metadata })
				});
				cacheIndexes.set(index, visible);
			}
			return visible;
		},
		indexKeys(): Iterable<IndexKey> {
			dependencies?.add(indexKeysDependency);
			return listed(graph.indexKeys());
		}
	});
}

function* listed(keys: Iterable<IndexKey>): Iterable<IndexKey> {
	for (const key of keys) {
		cacheEngineWork.indexKeysListed += 1;
		yield key;
	}
}
