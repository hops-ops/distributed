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
} from './types.js';
export {
	CacheRevisionConflictError,
	OptimisticLayerNotFoundError
} from './errors.js';
export { cacheIndexKey, createCacheEngine } from './create.js';
export { cacheEngineWork } from './work.js';
