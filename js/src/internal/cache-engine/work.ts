/**
 * Counts of the cache engine's work, summed over every engine in the process.
 * The scaling test and benchmark read them to show that a transaction costs
 * what it changed, not what the cache holds; nothing else reads them.
 */
export const cacheEngineWork = {
	/** Full `extract()` snapshots: one pass over every record and index. */
	extracts: 0,
	/** Stored records replaced or removed. */
	recordsWritten: 0,
	/** Stored indexes replaced or removed. */
	indexesWritten: 0,
	/** Optimistic and derived operations replayed over the base graph. */
	overlayOperations: 0,
	/** Index keys handed to a reader that listed them. */
	indexKeysListed: 0,
	/** Watcher selector runs caused by a committed transaction. */
	reselections: 0
};
