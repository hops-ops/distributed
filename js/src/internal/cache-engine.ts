/** Private cache engine; implementation lives in ./cache-engine/. */
export { CacheRevisionConflictError, OptimisticLayerNotFoundError } from './cache-engine/errors.js';
export { cacheEngineWork } from './cache-engine/work.js';
export { cacheIndexKey, createCacheEngine } from './cache-engine/create.js';
export type {
	BaseCacheWriter,
	BaseRecordClock,
	CacheEngine,
	CacheEngineOptions,
	CacheEngineSnapshot,
	CacheIndex,
	CacheIndexCoverage,
	CacheIndexMetadata,
	CacheListener,
	CachePresence,
	CacheReader,
	CacheSelector,
	CacheValue,
	DerivedIndexMutation,
	DerivedIndexReconciler,
	IndexKey,
	IndexWrite,
	OptimisticCacheWriter,
	OptimisticIndexWrite,
	OptimisticLayerContext,
	OptimisticLayerReplacement,
	OptimisticLayerState,
	OptimisticLayerView,
	OptimisticRecordWrite,
	RecordKey,
	RecordLink,
	RecordWrite,
	Revision,
	SparseRecord,
	SparseRecordMeta,
	WatchOptions
} from './cache-engine/types.js';
