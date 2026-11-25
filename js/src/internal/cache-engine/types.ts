export type Revision = number | string | bigint;
export type RecordKey = string;
export type IndexKey = string;

export type CacheValue =
	| null
	| boolean
	| number
	| string
	| readonly CacheValue[]
	| { readonly [key: string]: CacheValue };

export type RecordLink = RecordKey | readonly RecordKey[] | null;

export type RecordWrite = {
	key: RecordKey;
	revision: Revision;
	/** Stable incarnation fence. Defaults to the first live revision. */
	incarnation?: Revision;
	/** Omitted fields stay absent. A present `null` remains present. */
	fields?: Readonly<Record<string, CacheValue>>;
	/** Optimistic-only field removals; base writes never use this member. */
	unset?: readonly string[];
	/** Optimistic-only guard which suppresses the whole write when absent. */
	ifPresent?: boolean;
	/** Relationship identities are stored separately from scalar/JSON fields. */
	links?: Readonly<Record<string, RecordLink>>;
};

export type OptimisticRecordWrite = Omit<RecordWrite, 'revision' | 'incarnation'>;

export type CacheIndexCoverage =
	| { readonly kind: 'complete' }
	| { readonly kind: 'unknown' }
	| {
			readonly kind: 'offset';
			readonly offset: number;
			readonly limit?: number;
			readonly returned?: number;
			readonly hasNext?: boolean;
	  }
	| {
			readonly kind: 'cursor';
			readonly after?: CacheValue;
			readonly before?: CacheValue;
			readonly first?: number;
			readonly last?: number;
			readonly start?: CacheValue;
			readonly end?: CacheValue;
			readonly hasNext?: boolean;
			readonly hasPrevious?: boolean;
	  };

export type CacheIndexMetadata = {
	readonly parent?: RecordKey;
	/** Parent row clock claimed by the response that produced this relationship. */
	readonly parentRevision?: string;
	/** Parent lifecycle fence stamped by the engine. */
	readonly parentIncarnation?: string;
	readonly field: string;
	readonly arguments: Readonly<Record<string, CacheValue>>;
	readonly coverage: CacheIndexCoverage;
	readonly dependencies: readonly string[];
	readonly staleReason?: string;
	/** Distinguishes a present GraphQL `null` from an empty collection. */
	readonly nullValue?: boolean;
};

export type IndexWrite = {
	key: IndexKey;
	revision: Revision;
	records: readonly RecordKey[];
	complete?: boolean;
	metadata?: CacheIndexMetadata;
};

export type OptimisticIndexWrite = Omit<IndexWrite, 'revision'>;

export type SparseRecord = {
	readonly key: RecordKey;
	readonly revision: string;
	readonly incarnation: string;
	readonly fields: Readonly<Record<string, CacheValue>>;
	readonly links: Readonly<Record<string, RecordLink>>;
};

export type CacheIndex = {
	readonly key: IndexKey;
	readonly revision: string;
	readonly staleRevision?: string;
	readonly records: readonly RecordKey[];
	readonly complete: boolean;
	readonly metadata?: CacheIndexMetadata;
};

export type CachePresence<T> =
	| { readonly present: false }
	| { readonly present: true; readonly value: T };

export type SparseRecordMeta = {
	readonly key: RecordKey;
	readonly incarnation: string;
};

export type BaseRecordClock = {
	readonly revision: string;
	readonly incarnation: string;
	readonly tombstoned: boolean;
};

/** Raised when one source revision claims two different values. */

export interface CacheReader {
	recordMeta(key: RecordKey): SparseRecordMeta | undefined;
	/**
	 * Whether this record is visible only through an optimistic layer: an
	 * unsettled command created it and the base (server data) does not hold
	 * it yet. A layer that edits a record the base holds does not make it
	 * provisional.
	 */
	provisional(key: RecordKey): boolean;
	field(key: RecordKey, name: string): CachePresence<CacheValue>;
	link(key: RecordKey, name: string): CachePresence<RecordLink>;
	record(key: RecordKey): SparseRecord | undefined;
	index(key: IndexKey): CacheIndex | undefined;
	/** Every index key this reader may hold; `index(key)` says whether it is live. */
	indexKeys(): Iterable<IndexKey>;
}

export interface BaseCacheWriter {
	recordClock(key: RecordKey): BaseRecordClock | undefined;
	writeRecord(write: RecordWrite): boolean;
	tombstoneRecord(key: RecordKey, revision: Revision, incarnation?: Revision): boolean;
	/** Drop uncertified fields while preserving higher protocol clocks externally. */
	discardRecord(key: RecordKey): boolean;
	writeIndex(write: IndexWrite): boolean;
	markIndexStale(key: IndexKey, reason: string, revision?: Revision): boolean;
	deleteIndex(key: IndexKey, revision: Revision): boolean;
}

export interface OptimisticCacheWriter {
	writeRecord(write: OptimisticRecordWrite): void;
	tombstoneRecord(key: RecordKey): void;
	writeIndex(write: OptimisticIndexWrite): void;
	deleteIndex(key: IndexKey): void;
}

export type OptimisticLayerReplacement = (
	reader: CacheReader,
	writer: OptimisticCacheWriter
) => void;

export type CacheSelector<T> = (reader: CacheReader) => T;
export type CacheListener<T> = (value: T, previous: T | undefined) => void;

export type WatchOptions = {
	immediate?: boolean;
};

export type CacheEngineOptions = {
	/** Observer failures are reported after every eligible watcher was delivered. */
	onWatcherError?: (error: AggregateError) => void;
};

export type OptimisticLayerState = 'optimistic' | 'accepted';

export type CacheEngineSnapshot = {
	readonly version: 1;
	readonly records: readonly {
		readonly key: RecordKey;
		readonly revision: string;
		readonly incarnation?: string;
		readonly tombstoneRevision?: string;
		readonly fields: Readonly<
			Record<string, { readonly revision: string; readonly value: CacheValue }>
		>;
		readonly links: Readonly<
			Record<string, { readonly revision: string; readonly value: RecordLink }>
		>;
	}[];
	readonly indexes: readonly {
		readonly key: IndexKey;
		readonly revision: string;
		readonly staleRevision?: string;
		readonly records: readonly RecordKey[];
		readonly complete: boolean;
		readonly deleted: boolean;
		readonly metadata?: CacheIndexMetadata;
	}[];
};

/**
 * Opaque, detached JSON context owned by the replica and carried with one
 * optimistic layer. The cache engine never interprets it.
 */
export type OptimisticLayerContext = CacheValue;

export type OptimisticLayerView = {
	readonly id: string;
	readonly sequence: number;
	readonly state: OptimisticLayerState;
	readonly context?: OptimisticLayerContext;
};

export type DerivedIndexMutation =
	| {
			readonly kind: 'write';
			readonly write: OptimisticIndexWrite;
	  }
	| {
			readonly kind: 'stale';
			readonly key: IndexKey;
			readonly reason: string;
	  }
	| {
			readonly kind: 'delete';
			readonly key: IndexKey;
	  };

/**
 * Rebuild the complete derived-index overlay from confirmed state and the
 * ordered semantic contexts of every surviving optimistic layer.
 *
 * `confirmed` reads the authoritative base graph by key and is valid only
 * during the call: the reconciler pays for what it reads, not for the cache.
 */
export type DerivedIndexReconciler = (
	confirmed: CacheReader,
	layers: readonly OptimisticLayerView[]
) => readonly DerivedIndexMutation[];

export interface CacheEngine {
	read<T>(selector: CacheSelector<T>): T;
	/**
	 * Read only the authoritative base graph, excluding optimistic records and
	 * their derived-index overlay.
	 */
	readConfirmed<T>(selector: CacheSelector<T>): T;
	/**
	 * Return the authoritative write fence for requested base indexes.
	 * Deleted sentinels remain visible here even though CacheReader hides them.
	 */
	confirmedIndexFences(
		keys: readonly IndexKey[]
	): ReadonlyMap<IndexKey, string>;
	watch<T>(
		selector: CacheSelector<T>,
		listener: CacheListener<T>,
		options?: WatchOptions
	): () => void;
	batch<T>(update: (writer: BaseCacheWriter) => T): T;
	createOptimisticLayer(
		id: string,
		update: (writer: OptimisticCacheWriter) => void,
		context?: OptimisticLayerContext
	): void;
	/**
	 * Atomically replace a layer in place. The replacement evaluates against
	 * confirmed state plus only the optimistic layers below the target.
	 */
	replaceOptimisticLayer(
		id: string,
		replacement: OptimisticLayerReplacement,
		context?: OptimisticLayerContext
	): boolean;
	setDerivedIndexReconciler(reconciler: DerivedIndexReconciler | undefined): void;
	markOptimisticLayerAccepted(id: string): boolean;
	confirmOptimisticLayer<T>(id: string, update: (writer: BaseCacheWriter) => T): T;
	confirmOptimisticLayers<T>(
		ids: readonly string[],
		update: (writer: BaseCacheWriter) => T
	): T;
	rejectOptimisticLayer(id: string): boolean;
	optimisticLayerState(id: string): OptimisticLayerState | undefined;
	/** Whether this layer shows a record the base does not hold yet. */
	optimisticLayerProvisional(id: string): boolean;
	extract(): CacheEngineSnapshot;
	restore(snapshot: CacheEngineSnapshot): void;
	/**
	 * Replace only confirmed base state while preserving local optimistic layers,
	 * their acceptance state, and causal sequencing floors.
	 */
	restoreConfirmed(snapshot: CacheEngineSnapshot): void;
	/**
	 * Upsert confirmed records/indexes from a same-scope seed without deleting
	 * keys the seed omitted. Used when a warm SPA client re-receives a
	 * route-local SSR dehydrate that is a subset of the session cache.
	 */
	mergeConfirmed(snapshot: CacheEngineSnapshot): void;
	/**
	 * Drop incomparable/reset base indexes without assigning them a fabricated
	 * revision. Pending optimistic overlays remain layered above the new gap.
	 */
	discardIndexes(keys: readonly IndexKey[]): void;
	retain(key: RecordKey): void;
	release(key: RecordKey): void;
	gc(): readonly RecordKey[];
}

/*
 * Stored and visible values are immutable: a write replaces the value under
 * its key. That is what lets a transaction roll back by putting old values
 * back, and lets readers hand out the same frozen object until it changes.
 */
export type StoredField<T> = {
	readonly revision: bigint;
	readonly value: T;
};

export type StoredRecord = {
	readonly revision: bigint;
	readonly incarnation: bigint;
	readonly tombstoneRevision?: bigint;
	readonly fields: ReadonlyMap<string, StoredField<CacheValue>>;
	readonly links: ReadonlyMap<string, StoredField<RecordLink>>;
};

export type StoredIndex = {
	readonly revision: bigint;
	readonly staleRevision?: bigint;
	readonly records: readonly RecordKey[];
	readonly complete: boolean;
	readonly deleted: boolean;
	readonly metadata?: CacheIndexMetadata;
};

export type OverlayOperation =
	| { kind: 'write-record'; write: OptimisticRecordWrite }
	| { kind: 'tombstone-record'; key: RecordKey }
	| { kind: 'write-index'; write: OptimisticIndexWrite }
	| { kind: 'delete-index'; key: IndexKey };

export type OptimisticLayer = {
	readonly id: string;
	readonly sequence: number;
	readonly state: OptimisticLayerState;
	readonly operations: readonly OverlayOperation[];
	readonly context?: OptimisticLayerContext;
};

export type DerivedIndexOperation =
	| { kind: 'write-index'; write: OptimisticIndexWrite }
	| { kind: 'mark-index-stale'; key: IndexKey; reason: string }
	| { kind: 'delete-index'; key: IndexKey };

export type VisibleRecord = {
	readonly revision: bigint;
	readonly incarnation: bigint;
	readonly tombstoned: boolean;
	readonly fields: ReadonlyMap<string, CacheValue>;
	readonly links: ReadonlyMap<string, RecordLink>;
};

/** A stored index is already in its visible shape. */
export type VisibleIndex = StoredIndex;

/** The base graph, or the base graph under the optimistic overlay. */
export type CacheGraph = {
	record(key: RecordKey): VisibleRecord | undefined;
	index(key: IndexKey): VisibleIndex | undefined;
	indexKeys(): Iterable<IndexKey>;
	/** Only an optimistic overlay holds provisional records. */
	provisional?(key: RecordKey): boolean;
};

export type Watcher<T = unknown> = {
	readonly selector: CacheSelector<T>;
	readonly listener: CacheListener<T>;
	/** Registration order; delivery follows it. */
	readonly order: number;
	value: T;
	dependencies: Set<string>;
};

/** What one transaction needs to put back if it fails. */
export type TransactionUndo = {
	readonly layers: readonly OptimisticLayer[];
	readonly derivedIndexOperations: readonly DerivedIndexOperation[];
	readonly confirmedFloors: ReadonlyMap<string, number>;
	readonly nextLayerSequence: number;
};
