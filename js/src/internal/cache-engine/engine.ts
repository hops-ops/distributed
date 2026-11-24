import {
	CacheRevisionConflictError,
	OptimisticLayerNotFoundError
} from './errors.js';
import {
	Overlay,
	graphReader,
	isVisibleRecordLive,
	sameVisibleIndex,
	visibleRecord
} from './graph.js';
import {
	assertName,
	assertSynchronousResult,
	assertWriterActive,
	cloneCacheValue,
	cloneDerivedIndexOperations,
	cloneFields,
	cloneIndexMetadata,
	cloneLinks,
	compareRecordTuple,
	deepEqual,
	derivedIndexKeys,
	freezeRecord,
	indexDependency,
	indexKeysDependency,
	indexMetadataWithoutStaleReason,
	isOrderedSubsequence,
	linkKeys,
	operationDependencies,
	parseSnapshot,
	recordFieldDependency,
	recordSeenDependency,
	recordWildcardDependency,
	refinementMetadataCompatible,
	reportSafely,
	reportUnhandledWatcherError,
	revisionString,
	revisionToken,
	runDerivedIndexReconciler,
	validateIndexWrite,
	validateRecordKey
} from './helpers.js';
import { compareCodeUnits } from '../../lib/compare-code-units.js';
import type {
	BaseCacheWriter,
	CacheEngine,
	CacheEngineOptions,
	CacheEngineSnapshot,
	CacheGraph,
	CacheListener,
	CacheReader,
	CacheSelector,
	CacheValue,
	DerivedIndexOperation,
	DerivedIndexReconciler,
	IndexKey,
	IndexWrite,
	OptimisticCacheWriter,
	OptimisticIndexWrite,
	OptimisticLayer,
	OptimisticLayerContext,
	OptimisticLayerReplacement,
	OptimisticLayerState,
	OptimisticRecordWrite,
	OverlayOperation,
	RecordKey,
	RecordLink,
	RecordWrite,
	Revision,
	StoredField,
	StoredIndex,
	StoredRecord,
	TransactionUndo,
	VisibleRecord,
	WatchOptions,
	Watcher
} from './types.js';
import { cacheEngineWork } from './work.js';

const byKey = <T>([left]: readonly [string, T], [right]: readonly [string, T]): number =>
	compareCodeUnits(left, right);

/**
 * Minimum purpose-built implementation selected by the executable spike.
 *
 * It stores authoritative sparse records and exact indexes, then replays
 * named optimistic operation layers above them. Confirmation advances a
 * per-dependency causal floor before removing the layer, so an older pending
 * layer cannot become visible again after a newer command confirms.
 *
 * Cost model: stored records, indexes and layers are immutable values, and a
 * write replaces the value under its key. A transaction therefore costs what
 * it writes: rollback puts the replaced values back, the optimistic overlay
 * holds only the keys its operations touch, and a commit re-selects only the
 * watchers that read a changed key.
 */
export class PurposeBuiltCacheEngine implements CacheEngine {
	#records = new Map<RecordKey, StoredRecord>();
	#indexes = new Map<IndexKey, StoredIndex>();
	/** Live base indexes holding each record as a member or as their parent. */
	#indexReferences = new Map<RecordKey, Set<IndexKey>>();
	#layers: readonly OptimisticLayer[] = [];
	#derivedIndexOperations: readonly DerivedIndexOperation[] = [];
	#derivedIndexReconciler: DerivedIndexReconciler | undefined;
	#reconcilingDerivedIndexes = false;
	#confirmedFloors: ReadonlyMap<string, number> = new Map();
	#retained = new Map<RecordKey, number>();
	#nextLayerSequence = 0;
	#watchers = new Set<Watcher>();
	#watchersByDependency = new Map<string, Set<Watcher>>();
	#nextWatcherOrder = 0;
	#undo: TransactionUndo | undefined;
	#transactionLifecycleIndexes = new Set<IndexKey>();
	#dirty = false;
	#changedDependencies = new Set<string>();
	/** The current overlay, kept until its layers or a key it read change. */
	#overlay: Overlay | undefined;
	readonly #base: CacheGraph = {
		record: (key) => visibleRecord(this.#records.get(key)),
		index: (key) => this.#indexes.get(key),
		indexKeys: () => this.#indexes.keys()
	};
	readonly #reportWatcherError: (error: AggregateError) => void;

	constructor(options: CacheEngineOptions = {}) {
		this.#reportWatcherError = options.onWatcherError ?? reportUnhandledWatcherError;
	}

	read<T>(selector: CacheSelector<T>): T {
		if (typeof selector !== 'function') throw new TypeError('cache selector must be a function');
		return selector(graphReader(this.#view()));
	}

	readConfirmed<T>(selector: CacheSelector<T>): T {
		if (typeof selector !== 'function') throw new TypeError('cache selector must be a function');
		return selector(graphReader(this.#base));
	}

	confirmedIndexFences(
		keys: readonly IndexKey[]
	): ReadonlyMap<IndexKey, string> {
		const fences = new Map<IndexKey, string>();
		for (const key of new Set(keys)) {
			assertName(key, 'index key');
			const index = this.#indexes.get(key);
			if (index === undefined) continue;
			const fence =
				index.staleRevision !== undefined &&
				index.staleRevision > index.revision
					? index.staleRevision
					: index.revision;
			fences.set(key, revisionString(fence));
		}
		return fences;
	}

	watch<T>(
		selector: CacheSelector<T>,
		listener: CacheListener<T>,
		options: WatchOptions = {}
	): () => void {
		if (typeof listener !== 'function') throw new TypeError('cache listener must be a function');
		const { value, dependencies } = this.#select(selector);
		const watcher: Watcher<T> = {
			selector,
			listener,
			order: ++this.#nextWatcherOrder,
			value,
			dependencies: new Set()
		};
		this.#watchers.add(watcher as Watcher);
		this.#watchDependencies(watcher as Watcher, dependencies);
		if (options.immediate) {
			try {
				listener(value, undefined);
			} catch (error) {
				this.#removeWatcher(watcher as Watcher);
				reportSafely(
					this.#reportWatcherError,
					new AggregateError([error], 'initial cache watcher delivery failed')
				);
			}
		}
		return () => this.#removeWatcher(watcher as Watcher);
	}

	batch<T>(update: (writer: BaseCacheWriter) => T): T {
		if (typeof update !== 'function') throw new TypeError('cache update must be a function');
		if (this.#undo !== undefined) {
			throw new Error('nested cache batches are not supported');
		}
		return this.#transaction(() => {
			const result = this.#runBaseUpdate(update);
			this.#reconcileDerivedIndexes();
			return result;
		});
	}

	createOptimisticLayer(
		id: string,
		update: (writer: OptimisticCacheWriter) => void,
		context?: OptimisticLayerContext
	): void {
		assertName(id, 'optimistic layer id');
		if (this.#layers.some((layer) => layer.id === id)) {
			throw new Error(`optimistic layer already exists: ${id}`);
		}

		if (typeof update !== 'function') {
			throw new TypeError('optimistic layer update must be a function');
		}
		const operations: OverlayOperation[] = [];
		this.#runOptimisticUpdate(update, operations);
		const stableContext =
			context === undefined ? undefined : cloneCacheValue(context);
		this.#transaction(() => {
			const before = this.#view();
			this.#layers = [
				...this.#layers,
				{
					id,
					sequence: ++this.#nextLayerSequence,
					state: 'optimistic',
					operations,
					...(stableContext === undefined ? {} : { context: stableContext })
				}
			];
			this.#markOverlayChanges(operations, before, this.#view());
			this.#dirty = true;
			this.#reconcileDerivedIndexes();
		});
	}

	replaceOptimisticLayer(
		id: string,
		replacement: OptimisticLayerReplacement,
		context?: OptimisticLayerContext
	): boolean {
		assertName(id, 'optimistic layer id');
		if (typeof replacement !== 'function') {
			throw new TypeError('optimistic layer replacement must be a function');
		}
		return this.#transaction(() => {
			const targetIndex = this.#layers.findIndex(
				(candidate) => candidate.id === id
			);
			if (targetIndex === -1) {
				throw new OptimisticLayerNotFoundError(id);
			}
			const target = this.#layers[targetIndex]!;
			const before = this.#view();
			const prefixLayers = this.#layers.slice(0, targetIndex);
			const prefix = new Overlay(
				this.#base,
				prefixLayers,
				this.#deriveIndexOperations(prefixLayers),
				this.#confirmedFloors
			);
			const operations: OverlayOperation[] = [];
			this.#runOptimisticReplacement(
				replacement,
				graphReader(prefix),
				operations
			);
			const stableContext =
				context === undefined ? target.context : cloneCacheValue(context);
			if (
				deepEqual(operations, target.operations) &&
				deepEqual(stableContext, target.context)
			) return false;

			const layers = [...this.#layers];
			layers[targetIndex] = {
				id: target.id,
				sequence: target.sequence,
				state: target.state,
				operations,
				...(stableContext === undefined ? {} : { context: stableContext })
			};
			this.#layers = layers;
			this.#reconcileDerivedIndexes();
			const after = this.#view();
			this.#markOverlayChanges(
				[...target.operations, ...operations],
				before,
				after
			);
			// The base is untouched here, so only overlaid indexes can differ.
			for (const key of new Set([
				...overlaidIndexKeys(before),
				...overlaidIndexKeys(after)
			])) {
				if (!sameVisibleIndex(before.index(key), after.index(key))) {
					this.#indexChanged(key);
				}
			}
			this.#dirty = true;
			return true;
		});
	}

	setDerivedIndexReconciler(
		reconciler: DerivedIndexReconciler | undefined
	): void {
		if (reconciler !== undefined && typeof reconciler !== 'function') {
			throw new TypeError('derived index reconciler must be a function');
		}
		const previous = this.#derivedIndexReconciler;
		try {
			this.#transaction(() => {
				this.#derivedIndexReconciler = reconciler;
				this.#reconcileDerivedIndexes();
			});
		} catch (error) {
			this.#derivedIndexReconciler = previous;
			throw error;
		}
	}

	markOptimisticLayerAccepted(id: string): boolean {
		if (!this.#layers.some((candidate) => candidate.id === id)) return false;
		this.#transaction(() => {
			this.#layers = this.#layers.map((layer) =>
				layer.id === id && layer.state !== 'accepted'
					? { ...layer, state: 'accepted' }
					: layer
			);
			this.#reconcileDerivedIndexes();
		});
		return true;
	}

	confirmOptimisticLayer<T>(id: string, update: (writer: BaseCacheWriter) => T): T {
		return this.confirmOptimisticLayers([id], update);
	}

	confirmOptimisticLayers<T>(
		ids: readonly string[],
		update: (writer: BaseCacheWriter) => T
	): T {
		const unique = [...new Set(ids)];
		if (unique.length !== ids.length) {
			throw new Error('optimistic layer confirmation contains duplicate ids');
		}
		const layers = unique.map((id) => {
			const layer = this.#layers.find((candidate) => candidate.id === id);
			if (!layer) throw new Error(`unknown optimistic layer: ${id}`);
			return layer;
		});

		return this.#transaction(() => {
			const operations = layers.flatMap((layer) => layer.operations);
			// The base update below changes what a later read of these keys sees.
			const view = this.#view();
			const before = new Map<RecordKey, VisibleRecord | undefined>();
			for (const operation of operations) {
				if (operation.kind !== 'write-record') continue;
				before.set(operation.write.key, view.record(operation.write.key));
			}
			const layerDependencies = new Map<string, number>();
			for (const layer of layers) {
				for (const operation of layer.operations) {
					for (const dependency of operationDependencies(operation)) {
						layerDependencies.set(
							dependency,
							Math.max(layerDependencies.get(dependency) ?? 0, layer.sequence)
						);
					}
				}
			}

			// A command result may contain a full row, but only fields owned by this
			// optimistic layer are confirmed. Advancing floors for every field in the
			// server payload would incorrectly erase independent, older overlays.
			const result = this.#runBaseUpdate(update);
			const removed = new Set(layers);
			this.#layers = this.#layers.filter((candidate) => !removed.has(candidate));
			const floors = new Map(this.#confirmedFloors);
			for (const [dependency, sequence] of layerDependencies) {
				if (sequence > (floors.get(dependency) ?? 0)) floors.set(dependency, sequence);
			}
			this.#confirmedFloors = floors;
			this.#markOverlayChanges(
				operations,
				{ record: (key) => before.get(key) },
				this.#view()
			);
			if (this.#layers.length === 0) this.#confirmedFloors = new Map();
			this.#dirty = true;
			this.#reconcileDerivedIndexes();
			return result;
		});
	}

	rejectOptimisticLayer(id: string): boolean {
		const layer = this.#layers.find((candidate) => candidate.id === id);
		if (!layer) return false;
		this.#transaction(() => {
			const before = this.#view();
			this.#layers = this.#layers.filter((candidate) => candidate !== layer);
			this.#markOverlayChanges(layer.operations, before, this.#view());
			if (this.#layers.length === 0) this.#confirmedFloors = new Map();
			this.#dirty = true;
			this.#reconcileDerivedIndexes();
		});
		return true;
	}

	optimisticLayerState(id: string): OptimisticLayerState | undefined {
		return this.#layers.find((layer) => layer.id === id)?.state;
	}

	get recordCount(): number {
		return this.#records.size;
	}

	extract(): CacheEngineSnapshot {
		cacheEngineWork.extracts += 1;
		const stored = <T>([name, field]: readonly [string, StoredField<T>]) =>
			[
				name,
				Object.freeze({
					revision: revisionString(field.revision),
					value: field.value
				})
			] as const;
		return Object.freeze({
			version: 1 as const,
			records: Object.freeze(
				[...this.#records].sort(byKey).map(([key, record]) =>
					Object.freeze({
						key,
						revision: revisionString(record.revision),
						incarnation: revisionString(record.incarnation),
						...(record.tombstoneRevision === undefined
							? {}
							: { tombstoneRevision: revisionString(record.tombstoneRevision) }),
						fields: freezeRecord([...record.fields].sort(byKey).map(stored)),
						links: freezeRecord([...record.links].sort(byKey).map(stored))
					})
				)
			),
			indexes: Object.freeze(
				[...this.#indexes].sort(byKey).map(([key, index]) =>
					Object.freeze({
						key,
						revision: revisionString(index.revision),
						...(index.staleRevision === undefined
							? {}
							: { staleRevision: revisionString(index.staleRevision) }),
						records: index.records,
						complete: index.complete,
						deleted: index.deleted,
						...(index.metadata === undefined ? {} : { metadata: index.metadata })
					})
				)
			)
		});
	}

	restore(snapshot: CacheEngineSnapshot): void {
		const restored = parseSnapshot(snapshot);
		this.#transaction(() => {
			this.#replaceBase(restored);
			this.#layers = [];
			this.#derivedIndexOperations = [];
			this.#confirmedFloors = new Map();
			this.#nextLayerSequence = 0;
			this.#changedDependencies.add('*');
			this.#dirty = true;
		});
	}

	restoreConfirmed(snapshot: CacheEngineSnapshot): void {
		const restored = parseSnapshot(snapshot);
		this.#transaction(() => {
			this.#replaceBase(restored);
			this.#derivedIndexOperations = [];
			this.#changedDependencies.add('*');
			this.#dirty = true;
			this.#reconcileDerivedIndexes();
		});
	}

	mergeConfirmed(snapshot: CacheEngineSnapshot): void {
		const restored = parseSnapshot(snapshot);
		this.#transaction(() => {
			// Seed is authoritative for keys it contains; keys it omits stay.
			for (const [key, record] of restored.records) {
				this.#putRecord(key, record);
				this.#changedDependencies.add(recordSeenDependency(key));
				this.#changedDependencies.add(recordWildcardDependency(key));
			}
			for (const [key, index] of restored.indexes) {
				this.#putIndex(key, index);
				this.#indexChanged(key);
			}
			this.#derivedIndexOperations = [];
			this.#dirty = true;
			this.#reconcileDerivedIndexes();
		});
	}

	discardIndexes(keys: readonly IndexKey[]): void {
		const unique = new Set<IndexKey>();
		for (const key of keys) {
			assertName(key, 'index key');
			unique.add(key);
		}
		if (unique.size === 0) return;
		this.#transaction(() => {
			let changed = false;
			for (const key of unique) {
				if (!this.#indexes.has(key)) continue;
				this.#putIndex(key, undefined);
				this.#indexChanged(key);
				changed = true;
			}
			if (changed) this.#dirty = true;
			this.#reconcileDerivedIndexes();
		});
	}

	retain(key: RecordKey): void {
		assertName(key, 'record key');
		this.#retained.set(key, (this.#retained.get(key) ?? 0) + 1);
	}

	release(key: RecordKey): void {
		const count = this.#retained.get(key);
		if (count === undefined) return;
		if (count <= 1) this.#retained.delete(key);
		else this.#retained.set(key, count - 1);
	}

	gc(): readonly RecordKey[] {
		return this.#transaction(() => {
			// A destructive optimistic overlay must not make confirmed state
			// collectible: rejecting that layer has to reveal the complete base
			// graph again. Conversely, an optimistic index/link may be the only
			// current root for an authoritative record. Preserve the union.
			const optimisticRoots = this.#optimisticRecordRoots();
			const visible = this.#view();
			const reachable = new Set<RecordKey>([
				...this.#reachableRecords(this.#base, optimisticRoots),
				...this.#reachableRecords(visible, optimisticRoots)
			]);

			// Decide everything from the graphs as they are, then remove.
			const collected: RecordKey[] = [];
			for (const [key, record] of this.#records) {
				// Tombstones are revision fences and cannot be collected like data rows.
				if (record.tombstoneRevision === undefined && !reachable.has(key)) {
					collected.push(key);
				}
			}
			const collectedIndexes: IndexKey[] = [];
			for (const [key, index] of this.#indexes) {
				const parent = index.metadata?.parent;
				if (parent === undefined) continue;
				const parentIsLive =
					isVisibleRecordLive(this.#base.record(parent)) ||
					isVisibleRecordLive(visible.record(parent));
				if (reachable.has(parent) && parentIsLive) continue;
				collectedIndexes.push(key);
			}
			for (const key of collected) {
				this.#putRecord(key, undefined);
				this.#changedDependencies.add(recordSeenDependency(key));
				this.#changedDependencies.add(recordWildcardDependency(key));
			}
			for (const key of collectedIndexes) {
				this.#putIndex(key, undefined);
				this.#indexChanged(key);
			}
			if (collected.length > 0 || collectedIndexes.length > 0) this.#dirty = true;
			this.#reconcileDerivedIndexes();
			return Object.freeze(collected.sort());
		});
	}

	/** The graph selectors see: the base, under the overlay when there is one. */
	#view(): CacheGraph {
		if (this.#layers.length === 0 && this.#derivedIndexOperations.length === 0) {
			return this.#base;
		}
		const overlay = this.#overlay;
		if (
			overlay !== undefined &&
			overlay.layers === this.#layers &&
			overlay.derivedIndexOperations === this.#derivedIndexOperations &&
			overlay.floors === this.#confirmedFloors
		) {
			return overlay;
		}
		return (this.#overlay = new Overlay(
			this.#base,
			this.#layers,
			this.#derivedIndexOperations,
			this.#confirmedFloors
		));
	}

	#select<T>(selector: CacheSelector<T>): { value: T; dependencies: Set<string> } {
		const dependencies = new Set<string>();
		return { value: selector(graphReader(this.#view(), dependencies)), dependencies };
	}

	#optimisticRecordRoots(): Set<RecordKey> {
		const roots = new Set<RecordKey>();
		for (const layer of this.#layers) {
			for (const operation of layer.operations) {
				if (operation.kind === 'write-record') {
					roots.add(operation.write.key);
					for (const link of Object.values(operation.write.links ?? {})) {
						for (const key of linkKeys(link)) roots.add(key);
					}
				} else if (operation.kind === 'tombstone-record') {
					roots.add(operation.key);
				} else if (operation.kind === 'write-index') {
					for (const key of operation.write.records) roots.add(key);
				}
			}
		}
		for (const operation of this.#derivedIndexOperations) {
			if (operation.kind !== 'write-index') continue;
			for (const key of operation.write.records) roots.add(key);
		}
		return roots;
	}

	#reachableRecords(
		graph: CacheGraph,
		extraRoots: ReadonlySet<RecordKey>
	): Set<RecordKey> {
		const reachable = new Set<RecordKey>([...this.#retained.keys(), ...extraRoots]);
		const pending = [...reachable];
		const relationshipIndexes = new Map<RecordKey, StoredIndex[]>();
		for (const indexKey of graph.indexKeys()) {
			const index = graph.index(indexKey);
			if (index === undefined || index.deleted) continue;
			const parent = index.metadata?.parent;
			if (parent !== undefined) {
				const indexes = relationshipIndexes.get(parent) ?? [];
				indexes.push(index);
				relationshipIndexes.set(parent, indexes);
				continue;
			}
			for (const key of index.records) {
				if (reachable.has(key)) continue;
				reachable.add(key);
				pending.push(key);
			}
		}
		while (pending.length > 0) {
			const key = pending.pop()!;
			const record = graph.record(key);
			if (!record || record.tombstoned) continue;
			for (const link of record.links.values()) {
				for (const key of linkKeys(link)) {
					if (reachable.has(key)) continue;
					reachable.add(key);
					pending.push(key);
				}
			}
			for (const index of relationshipIndexes.get(key) ?? []) {
				for (const child of index.records) {
					if (reachable.has(child)) continue;
					reachable.add(child);
					pending.push(child);
				}
			}
		}
		return reachable;
	}

	#indexChanged(key: IndexKey): void {
		this.#changedDependencies.add(indexDependency(key));
		this.#changedDependencies.add(indexKeysDependency);
	}

	#markOverlayChanges(
		operations: readonly OverlayOperation[],
		before: Pick<CacheGraph, 'record'>,
		after: Pick<CacheGraph, 'record'>
	): void {
		for (const operation of operations) {
			if (operation.kind === 'write-record') {
				const key = operation.write.key;
				this.#changedDependencies.add(recordWildcardDependency(key));
				for (const name of Object.keys(operation.write.fields ?? {})) {
					this.#changedDependencies.add(recordFieldDependency(key, `field:${name}`));
				}
				for (const name of operation.write.unset ?? []) {
					this.#changedDependencies.add(recordFieldDependency(key, `field:${name}`));
				}
				for (const name of Object.keys(operation.write.links ?? {})) {
					this.#changedDependencies.add(recordFieldDependency(key, `link:${name}`));
				}
				const previous = before.record(key);
				const next = after.record(key);
				if (
					isVisibleRecordLive(previous) !== isVisibleRecordLive(next) ||
					previous?.incarnation !== next?.incarnation
				) {
					this.#changedDependencies.add(recordSeenDependency(key));
				}
				continue;
			}
			if (operation.kind === 'tombstone-record') {
				this.#changedDependencies.add(recordSeenDependency(operation.key));
				this.#changedDependencies.add(recordWildcardDependency(operation.key));
				continue;
			}
			this.#indexChanged(
				operation.kind === 'write-index' ? operation.write.key : operation.key
			);
		}
	}

	#reconcileDerivedIndexes(): void {
		const next = this.#deriveIndexOperations(this.#layers);
		const previous = this.#derivedIndexOperations;
		// The same operations over the same base show the same indexes.
		if (deepEqual(previous, next)) return;
		const before = this.#view();
		this.#derivedIndexOperations = next;
		const after = this.#view();
		let changed = false;
		for (const key of derivedIndexKeys([...previous, ...next])) {
			if (sameVisibleIndex(before.index(key), after.index(key))) continue;
			this.#indexChanged(key);
			changed = true;
		}
		if (changed) this.#dirty = true;
	}

	#deriveIndexOperations(
		layers: readonly OptimisticLayer[]
	): readonly DerivedIndexOperation[] {
		if (this.#reconcilingDerivedIndexes) {
			throw new Error('derived index reconciliation cannot be re-entered');
		}
		const reconciler = this.#derivedIndexReconciler;
		if (reconciler === undefined) return [];
		this.#reconcilingDerivedIndexes = true;
		try {
			return cloneDerivedIndexOperations(
				runDerivedIndexReconciler(
					reconciler,
					graphReader(this.#base),
					layers.map((layer) =>
						Object.freeze({
							id: layer.id,
							sequence: layer.sequence,
							state: layer.state,
							...(layer.context === undefined
								? {}
								: { context: layer.context })
						})
					)
				)
			);
		} finally {
			this.#reconcilingDerivedIndexes = false;
		}
	}

	#baseWriter(touched?: Set<string>, isActive: () => boolean = () => true): BaseCacheWriter {
		return Object.freeze({
			recordClock: (key: RecordKey) => {
				assertWriterActive(isActive());
				validateRecordKey(key);
				const record = this.#records.get(key);
				if (!record) return undefined;
				return Object.freeze({
					revision: revisionString(record.revision),
					incarnation: revisionString(record.incarnation),
					tombstoned: record.tombstoneRevision !== undefined
				});
			},
			writeRecord: (write: RecordWrite) => {
				assertWriterActive(isActive());
				return this.#writeBaseRecord(write, touched);
			},
			tombstoneRecord: (
				key: RecordKey,
				revision: Revision,
				incarnation?: Revision
			) => {
				assertWriterActive(isActive());
				return this.#tombstoneBaseRecord(key, revision, incarnation, touched);
			},
			discardRecord: (key: RecordKey) => {
				assertWriterActive(isActive());
				return this.#discardBaseRecord(key, touched);
			},
			writeIndex: (write: IndexWrite) => {
				assertWriterActive(isActive());
				return this.#writeBaseIndex(write, touched);
			},
			markIndexStale: (key: IndexKey, reason: string, revision?: Revision) => {
				assertWriterActive(isActive());
				return this.#markBaseIndexStale(key, reason, revision, touched);
			},
			deleteIndex: (key: IndexKey, revision: Revision) => {
				assertWriterActive(isActive());
				return this.#deleteBaseIndex(key, revision, touched);
			}
		});
	}

	#optimisticWriter(
		operations: OverlayOperation[],
		isActive: () => boolean = () => true
	): OptimisticCacheWriter {
		return Object.freeze({
			writeRecord(write: OptimisticRecordWrite): void {
				assertWriterActive(isActive());
				validateRecordKey(write.key);
				const fields = cloneFields(write.fields);
				const links = cloneLinks(write.links);
				const unset = Object.freeze([...(write.unset ?? [])]);
				for (const name of unset) assertName(name, 'record field');
				if (new Set(unset).size !== unset.length) {
					throw new TypeError('record unset fields must be unique');
				}
				if (unset.some((name) => Object.hasOwn(fields, name))) {
					throw new TypeError('record field cannot be set and unset');
				}
				if (
					Object.keys(fields).length === 0 &&
					Object.keys(links).length === 0 &&
					unset.length === 0
				) return;
				operations.push({
					kind: 'write-record',
					write: {
						key: write.key,
						fields,
						links,
						...(unset.length === 0 ? {} : { unset }),
						...(write.ifPresent === true ? { ifPresent: true } : {})
					}
				});
			},
			tombstoneRecord(key: RecordKey): void {
				assertWriterActive(isActive());
				validateRecordKey(key);
				operations.push({ kind: 'tombstone-record', key });
			},
			writeIndex(write: OptimisticIndexWrite): void {
				assertWriterActive(isActive());
				validateIndexWrite(write);
				operations.push({
					kind: 'write-index',
					write: {
						key: write.key,
						records: Object.freeze([...write.records]),
						complete: write.complete ?? false,
						...(write.metadata === undefined
							? {}
							: { metadata: cloneIndexMetadata(write.metadata) })
					}
				});
			},
			deleteIndex(key: IndexKey): void {
				assertWriterActive(isActive());
				assertName(key, 'index key');
				operations.push({ kind: 'delete-index', key });
			}
		});
	}

	#runBaseUpdate<T>(
		update: (writer: BaseCacheWriter) => T,
		touched?: Set<string>
	): T {
		let active = true;
		const writer = this.#baseWriter(touched, () => active);
		try {
			const result = update(writer);
			assertSynchronousResult(result, 'cache update');
			return result;
		} finally {
			active = false;
		}
	}

	#runOptimisticUpdate(
		update: (writer: OptimisticCacheWriter) => void,
		operations: OverlayOperation[]
	): void {
		let active = true;
		const writer = this.#optimisticWriter(operations, () => active);
		try {
			const result = update(writer);
			assertSynchronousResult(result, 'optimistic layer update');
		} finally {
			active = false;
		}
	}

	#runOptimisticReplacement(
		replacement: OptimisticLayerReplacement,
		reader: CacheReader,
		operations: OverlayOperation[]
	): void {
		let active = true;
		const writer = this.#optimisticWriter(operations, () => active);
		try {
			const result = replacement(reader, writer);
			assertSynchronousResult(result, 'optimistic layer replacement');
		} finally {
			active = false;
		}
	}

	#putRecord(key: RecordKey, record: StoredRecord | undefined): void {
		const undo = this.#undo!;
		if (this.#records === undo.records && !undo.recordLog.has(key)) {
			undo.recordLog.set(key, this.#records.get(key));
		}
		if (record === undefined) this.#records.delete(key);
		else this.#records.set(key, record);
		if (this.#overlay?.records.has(key)) this.#overlay = undefined;
		cacheEngineWork.recordsWritten += 1;
	}

	#putIndex(key: IndexKey, index: StoredIndex | undefined): void {
		const undo = this.#undo!;
		const previous = this.#indexes.get(key);
		if (this.#indexes === undo.indexes && !undo.indexLog.has(key)) {
			undo.indexLog.set(key, previous);
		}
		this.#moveIndexReferences(key, previous, index);
		if (index === undefined) this.#indexes.delete(key);
		else this.#indexes.set(key, index);
		if (this.#overlay?.indexes.has(key)) this.#overlay = undefined;
		cacheEngineWork.indexesWritten += 1;
	}

	#replaceBase(base: {
		records: Map<RecordKey, StoredRecord>;
		indexes: Map<IndexKey, StoredIndex>;
	}): void {
		this.#records = base.records;
		this.#indexes = base.indexes;
		this.#overlay = undefined;
		this.#indexReferences = new Map();
		for (const [key, index] of this.#indexes) {
			this.#moveIndexReferences(key, undefined, index);
		}
	}

	#moveIndexReferences(
		key: IndexKey,
		previous: StoredIndex | undefined,
		next: StoredIndex | undefined
	): void {
		const before = previous?.deleted === false ? previous : undefined;
		const after = next?.deleted === false ? next : undefined;
		if (
			before !== undefined &&
			after !== undefined &&
			before.records === after.records &&
			before.metadata?.parent === after.metadata?.parent
		) {
			return;
		}
		for (const record of indexReferences(before)) {
			const keys = this.#indexReferences.get(record);
			if (keys?.delete(key) && keys.size === 0) this.#indexReferences.delete(record);
		}
		for (const record of indexReferences(after)) {
			let keys = this.#indexReferences.get(record);
			if (keys === undefined) this.#indexReferences.set(record, (keys = new Set()));
			keys.add(key);
		}
	}

	#writeBaseRecord(write: RecordWrite, touched?: Set<string>): boolean {
		validateRecordKey(write.key);
		const revision = revisionToken(write.revision);
		const requestedIncarnation =
			write.incarnation === undefined ? undefined : revisionToken(write.incarnation);
		const fields = cloneFields(write.fields);
		const links = cloneLinks(write.links);
		for (const name of Object.keys(fields)) {
			touched?.add(recordFieldDependency(write.key, `field:${name}`));
		}
		for (const name of Object.keys(links)) {
			touched?.add(recordFieldDependency(write.key, `link:${name}`));
		}
		if (Object.keys(fields).length > 0 || Object.keys(links).length > 0) {
			touched?.add(recordSeenDependency(write.key));
		}

		const record = this.#records.get(write.key);
		if (
			requestedIncarnation === undefined &&
			record?.tombstoneRevision !== undefined
		) {
			if (revision < record.revision) return false;
			if (revision === record.revision) {
				throw new CacheRevisionConflictError(
					recordSeenDependency(write.key),
					revision
				);
			}
		}
		const incarnation =
			requestedIncarnation ??
			(record === undefined
				? revision
				: record.tombstoneRevision === undefined
					? record.incarnation
					: revision);
		if (record !== undefined) {
			const comparison = compareRecordTuple(
				incarnation,
				revision,
				record.incarnation,
				record.revision
			);
			if (comparison < 0) return false;
			if (comparison === 0 && record.tombstoneRevision !== undefined) {
				throw new CacheRevisionConflictError(
					recordSeenDependency(write.key),
					revision
				);
			}
			if (
				comparison > 0 &&
				record.tombstoneRevision !== undefined &&
				incarnation === record.incarnation
			) {
				throw new CacheRevisionConflictError(
					recordSeenDependency(write.key),
					revision
				);
			}
		}

		// A new record or a new incarnation starts empty; otherwise the write
		// lands on a copy, taken only once a field or link actually changes.
		const fresh = record === undefined || incarnation > record.incarnation;
		let changed = fresh;
		let nextRevision = fresh ? revision : record.revision;
		let nextFields: Map<string, StoredField<CacheValue>> | undefined = fresh
			? new Map()
			: undefined;
		let nextLinks: Map<string, StoredField<RecordLink>> | undefined = fresh
			? new Map()
			: undefined;
		for (const [name, value] of Object.entries(fields)) {
			const current = (nextFields ?? record!.fields).get(name);
			if (current && revision < current.revision) continue;
			if (current && revision === current.revision) {
				if (deepEqual(current.value, value)) continue;
				throw new CacheRevisionConflictError(
					recordFieldDependency(write.key, `field:${name}`),
					revision
				);
			}
			(nextFields ??= new Map(record!.fields)).set(name, { revision, value });
			changed = true;
		}
		for (const [name, value] of Object.entries(links)) {
			const current = (nextLinks ?? record!.links).get(name);
			if (current && revision < current.revision) continue;
			if (current && revision === current.revision) {
				if (deepEqual(current.value, value)) continue;
				throw new CacheRevisionConflictError(
					recordFieldDependency(write.key, `link:${name}`),
					revision
				);
			}
			(nextLinks ??= new Map(record!.links)).set(name, { revision, value });
			changed = true;
		}
		if (!fresh && revision > record.revision) {
			nextRevision = revision;
			changed = true;
		}
		if (!changed) return false;

		if (fresh) this.#invalidateIndexesForRecordLifecycle(write.key, touched);
		this.#putRecord(write.key, {
			revision: nextRevision,
			incarnation,
			tombstoneRevision: fresh ? undefined : record.tombstoneRevision,
			fields: nextFields ?? record!.fields,
			links: nextLinks ?? record!.links
		});
		this.#changedDependencies.add(recordWildcardDependency(write.key));
		if (fresh) this.#changedDependencies.add(recordSeenDependency(write.key));
		for (const name of Object.keys(fields)) {
			this.#changedDependencies.add(
				recordFieldDependency(write.key, `field:${name}`)
			);
		}
		for (const name of Object.keys(links)) {
			this.#changedDependencies.add(
				recordFieldDependency(write.key, `link:${name}`)
			);
		}
		this.#dirty = true;
		return true;
	}

	#tombstoneBaseRecord(
		key: RecordKey,
		revisionValue: Revision,
		incarnationValue?: Revision,
		touched?: Set<string>
	): boolean {
		validateRecordKey(key);
		const revision = revisionToken(revisionValue);
		touched?.add(recordWildcardDependency(key));
		touched?.add(recordSeenDependency(key));
		const record = this.#records.get(key);
		const incarnation =
			incarnationValue === undefined
				? (record?.incarnation ?? revision)
				: revisionToken(incarnationValue);
		if (record) {
			const comparison = compareRecordTuple(
				incarnation,
				revision,
				record.incarnation,
				record.revision
			);
			if (comparison < 0) return false;
			if (comparison === 0) {
				if (record.tombstoneRevision !== undefined) return false;
				throw new CacheRevisionConflictError(recordSeenDependency(key), revision);
			}
		}
		this.#putRecord(key, {
			revision,
			incarnation,
			tombstoneRevision: revision,
			fields: new Map(),
			links: new Map()
		});
		this.#invalidateIndexesForRecordLifecycle(key, touched);
		this.#changedDependencies.add(recordSeenDependency(key));
		this.#changedDependencies.add(recordWildcardDependency(key));
		this.#dirty = true;
		return true;
	}

	#discardBaseRecord(key: RecordKey, touched?: Set<string>): boolean {
		validateRecordKey(key);
		const hadRecord = this.#records.has(key);
		if (!hadRecord && !this.#indexReferences.has(key)) return false;
		if (hadRecord) this.#putRecord(key, undefined);
		touched?.add(recordSeenDependency(key));
		touched?.add(recordWildcardDependency(key));
		this.#invalidateIndexesForRecordLifecycle(key, touched);
		this.#changedDependencies.add(recordSeenDependency(key));
		this.#changedDependencies.add(recordWildcardDependency(key));
		this.#dirty = true;
		return true;
	}

	#invalidateIndexesForRecordLifecycle(
		recordKey: RecordKey,
		touched?: Set<string>
	): void {
		for (const key of [...(this.#indexReferences.get(recordKey) ?? [])]) {
			const index = this.#indexes.get(key)!;
			const ownedByRecord = index.metadata?.parent === recordKey;
			this.#transactionLifecycleIndexes.add(key);
			this.#putIndex(key, {
				revision: index.revision,
				staleRevision:
					index.staleRevision === undefined || index.staleRevision < index.revision
						? index.revision
						: index.staleRevision,
				records: ownedByRecord
					? EMPTY_KEYS
					: Object.freeze(
							index.records.filter((candidate) => candidate !== recordKey)
						),
				complete: false,
				deleted: ownedByRecord,
				metadata:
					index.metadata === undefined
						? undefined
						: cloneIndexMetadata({
								...index.metadata,
								staleReason: 'record-lifecycle-changed'
							})
			});
			touched?.add(indexDependency(key));
			this.#indexChanged(key);
			this.#dirty = true;
		}
	}

	#writeBaseIndex(write: IndexWrite, touched?: Set<string>): boolean {
		validateIndexWrite(write);
		const revision = revisionToken(write.revision);
		touched?.add(indexDependency(write.key));
		const current = this.#indexes.get(write.key);
		if (current?.staleRevision !== undefined && revision < current.staleRevision) {
			return false;
		}
		if (current && revision < current.revision) return false;
		const records = Object.freeze([...write.records]);
		const complete = write.complete ?? false;
		let metadata =
			write.metadata === undefined ? undefined : cloneIndexMetadata(write.metadata);
		if (metadata?.parent !== undefined) {
			const parent = this.#records.get(metadata.parent);
			if (!parent || parent.tombstoneRevision !== undefined) return false;
			if (
				metadata.parentRevision !== undefined &&
				revisionToken(metadata.parentRevision) !== parent.revision
			) {
				throw new CacheRevisionConflictError(
					recordSeenDependency(metadata.parent),
					revisionToken(metadata.parentRevision)
				);
			}
			if (
				metadata.parentIncarnation !== undefined &&
				revisionToken(metadata.parentIncarnation) !== parent.incarnation
			) {
				throw new CacheRevisionConflictError(
					recordSeenDependency(metadata.parent),
					revisionToken(metadata.parentIncarnation)
				);
			}
			metadata = cloneIndexMetadata({
				...metadata,
				parentIncarnation: revisionString(parent.incarnation)
			});
		}
		const staleRevision = metadata?.staleReason === undefined ? undefined : revision;
		const put = (index: StoredIndex): true => {
			this.#putIndex(write.key, index);
			this.#indexChanged(write.key);
			this.#dirty = true;
			return true;
		};
		if (current && revision === current.revision) {
			if (
				current.deleted &&
				current.metadata === undefined &&
				current.records.length === 0 &&
				current.staleRevision === revision
			) {
				// A hidden fence uses an empty deleted index so it materializes no
				// membership. Revision zero is valid, so an equal-checkpoint success
				// must be able to replace that sentinel state.
				return put({
					revision,
					staleRevision,
					records,
					complete,
					deleted: false,
					metadata
				});
			}
			if (
				complete &&
				metadata?.staleReason === undefined &&
				(this.#transactionLifecycleIndexes.has(write.key) ||
					(!current.deleted &&
						!current.complete &&
						current.metadata?.staleReason !== 'record-lifecycle-changed' &&
						isOrderedSubsequence(current.records, records) &&
						refinementMetadataCompatible(current.metadata, metadata)))
			) {
				// A partial GraphQL result may be retried without the underlying read
				// model advancing. Permit only the monotonic incomplete -> complete
				// refinement; an already-authoritative membership still conflicts on
				// any same-revision disagreement.
				return put({
					revision,
					staleRevision: undefined,
					records,
					complete: true,
					deleted: false,
					metadata
				});
			}
			const currentComparable = indexMetadataWithoutStaleReason(current.metadata);
			const nextComparable = indexMetadataWithoutStaleReason(metadata);
			if (
				!current.deleted &&
				current.complete === complete &&
				deepEqual(current.records, records) &&
				deepEqual(currentComparable, nextComparable)
			) {
				if (
					deepEqual(current.metadata, metadata) &&
					current.staleRevision === staleRevision
				) {
					return false;
				}
				return put({ ...current, staleRevision, metadata });
			}
			throw new CacheRevisionConflictError(indexDependency(write.key), revision);
		}
		return put({
			revision,
			staleRevision,
			records,
			complete,
			deleted: false,
			metadata
		});
	}

	#markBaseIndexStale(
		key: IndexKey,
		reason: string,
		revisionValue?: Revision,
		touched?: Set<string>
	): boolean {
		assertName(key, 'index key');
		assertName(reason, 'index stale reason');
		const current = this.#indexes.get(key);
		if (!current) {
			if (revisionValue === undefined) return false;
			this.#putIndex(key, {
				revision: 0n,
				staleRevision: revisionToken(revisionValue),
				records: EMPTY_KEYS,
				complete: false,
				deleted: true,
				metadata: undefined
			});
			touched?.add(indexDependency(key));
			this.#indexChanged(key);
			this.#dirty = true;
			return true;
		}
		if (current.deleted && revisionValue === undefined) return false;
		const revision =
			revisionValue === undefined ? current.revision : revisionToken(revisionValue);
		const fence =
			current.staleRevision !== undefined && current.staleRevision > current.revision
				? current.staleRevision
				: current.revision;
		if (revision < fence) return false;
		if (
			current.staleRevision === revision &&
			(current.metadata === undefined || current.metadata.staleReason === reason)
		) {
			return false;
		}
		touched?.add(indexDependency(key));
		this.#putIndex(key, {
			...current,
			staleRevision: revision,
			metadata:
				current.metadata === undefined
					? undefined
					: cloneIndexMetadata({ ...current.metadata, staleReason: reason })
		});
		this.#indexChanged(key);
		this.#dirty = true;
		return true;
	}

	#deleteBaseIndex(key: IndexKey, revisionValue: Revision, touched?: Set<string>): boolean {
		assertName(key, 'index key');
		const revision = revisionToken(revisionValue);
		touched?.add(indexDependency(key));
		const current = this.#indexes.get(key);
		if (current?.staleRevision !== undefined && revision < current.staleRevision) {
			return false;
		}
		if (current && revision < current.revision) return false;
		if (current && revision === current.revision) {
			if (current.deleted) return false;
			throw new CacheRevisionConflictError(indexDependency(key), revision);
		}
		this.#putIndex(key, {
			revision,
			staleRevision: undefined,
			records: EMPTY_KEYS,
			complete: false,
			deleted: true,
			metadata: current?.metadata
		});
		this.#indexChanged(key);
		this.#dirty = true;
		return true;
	}

	/**
	 * Run one all-or-nothing update. The outermost call owns the undo log;
	 * a failure puts every replaced value back before it propagates.
	 */
	#transaction<T>(update: () => T): T {
		if (this.#undo !== undefined) return update();
		const undo: TransactionUndo = (this.#undo = {
			records: this.#records,
			indexes: this.#indexes,
			recordLog: new Map(),
			indexLog: new Map(),
			layers: this.#layers,
			derivedIndexOperations: this.#derivedIndexOperations,
			confirmedFloors: this.#confirmedFloors,
			nextLayerSequence: this.#nextLayerSequence
		});
		this.#transactionLifecycleIndexes = new Set();
		let result: T;
		try {
			result = update();
		} catch (error) {
			this.#rollback(undo);
			throw error;
		} finally {
			this.#undo = undefined;
		}
		const shouldFlush = this.#dirty;
		const changedDependencies = this.#changedDependencies;
		this.#dirty = false;
		this.#changedDependencies = new Set();
		this.#transactionLifecycleIndexes = new Set();
		if (shouldFlush) this.#flushWatchers(changedDependencies);
		return result;
	}

	#rollback(undo: TransactionUndo): void {
		const replaced =
			this.#records !== undo.records || this.#indexes !== undo.indexes;
		this.#records = undo.records;
		for (const [key, record] of undo.recordLog) {
			if (record === undefined) this.#records.delete(key);
			else this.#records.set(key, record);
		}
		if (replaced) {
			for (const [key, index] of undo.indexLog) {
				if (index === undefined) undo.indexes.delete(key);
				else undo.indexes.set(key, index);
			}
			this.#replaceBase({ records: undo.records, indexes: undo.indexes });
		} else {
			for (const [key, index] of undo.indexLog) {
				this.#moveIndexReferences(key, this.#indexes.get(key), index);
				if (index === undefined) this.#indexes.delete(key);
				else this.#indexes.set(key, index);
			}
		}
		this.#layers = undo.layers;
		this.#derivedIndexOperations = undo.derivedIndexOperations;
		this.#confirmedFloors = undo.confirmedFloors;
		this.#nextLayerSequence = undo.nextLayerSequence;
		this.#overlay = undefined;
		this.#dirty = false;
		this.#changedDependencies = new Set();
		this.#transactionLifecycleIndexes = new Set();
	}

	#watchDependencies(watcher: Watcher, dependencies: Set<string>): void {
		for (const dependency of watcher.dependencies) {
			if (dependencies.has(dependency)) continue;
			const watchers = this.#watchersByDependency.get(dependency);
			if (watchers?.delete(watcher) && watchers.size === 0) {
				this.#watchersByDependency.delete(dependency);
			}
		}
		for (const dependency of dependencies) {
			if (watcher.dependencies.has(dependency)) continue;
			let watchers = this.#watchersByDependency.get(dependency);
			if (watchers === undefined) {
				this.#watchersByDependency.set(dependency, (watchers = new Set()));
			}
			watchers.add(watcher);
		}
		watcher.dependencies = dependencies;
	}

	#removeWatcher(watcher: Watcher): boolean {
		if (!this.#watchers.delete(watcher)) return false;
		this.#watchDependencies(watcher, new Set());
		return true;
	}

	/** Re-select the watchers that read a changed dependency, in watch order. */
	#flushWatchers(changedDependencies: ReadonlySet<string>): void {
		let affected: Watcher[];
		if (changedDependencies.size === 0 || changedDependencies.has('*')) {
			affected = [...this.#watchers];
		} else {
			const readers = new Set<Watcher>();
			for (const dependency of changedDependencies) {
				for (const watcher of this.#watchersByDependency.get(dependency) ?? []) {
					readers.add(watcher);
				}
			}
			affected = [...readers].sort((left, right) => left.order - right.order);
		}
		const errors: unknown[] = [];
		for (const watcher of affected) {
			// An earlier listener may have removed this watcher.
			if (!this.#watchers.has(watcher)) continue;
			try {
				cacheEngineWork.reselections += 1;
				const { value: next, dependencies } = this.#select(watcher.selector);
				this.#watchDependencies(watcher, dependencies);
				if (deepEqual(next, watcher.value)) continue;
				const previous = watcher.value;
				watcher.value = next;
				watcher.listener(next, previous);
			} catch (error) {
				errors.push(error);
			}
		}
		if (errors.length > 0) {
			reportSafely(
				this.#reportWatcherError,
				new AggregateError(
					errors,
					'cache transaction committed, but watcher delivery failed'
				)
			);
		}
	}
}

const EMPTY_KEYS: readonly RecordKey[] = Object.freeze([]);

function* indexReferences(index: StoredIndex | undefined): Iterable<RecordKey> {
	if (index === undefined) return;
	yield* index.records;
	if (index.metadata?.parent !== undefined) yield index.metadata.parent;
}

function overlaidIndexKeys(graph: CacheGraph): Iterable<IndexKey> {
	return graph instanceof Overlay ? graph.indexes.keys() : [];
}
