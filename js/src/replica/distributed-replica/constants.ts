import type { GqlError } from '../../types.js';
import type { DistributedTrustedPreset } from '../../protocol.js';

export const EMPTY_ERRORS: readonly GqlError[] = Object.freeze([]);
/** Matches protocol.ts MAX_EVIDENCE_ITEMS without making it public API. */
export const MAX_ANONYMOUS_RECORD_CLOCKS = 4_096;
export const SHA256 = /^sha256:[0-9a-f]{64}$/;
export const EMPTY_TRUSTED_PRESETS: readonly DistributedTrustedPreset[] = Object.freeze([]);
export const EMPTY_CACHE_SNAPSHOT = Object.freeze({
	version: 1 as const,
	records: Object.freeze([]),
	indexes: Object.freeze([])
});
/** Reopen backoff after a failed live stream (`docs/live-query-delivery.md`). */
export const LIVE_FAILURE_RETRY_BASE_MS = 1_000;
export const LIVE_FAILURE_RETRY_MAX_MS = 30_000;
/** Up to half of each reopen delay is subtracted at random. */
export const LIVE_FAILURE_RETRY_JITTER = 0.5;
/**
 * How long an operation's protocol state and cached result outlive its last
 * watch, so that a remount (a navigation back, a keyed re-render) finds them.
 */
export const OPERATION_RETENTION_GRACE_MS = 30_000;
/** Query state of an operation nothing watches. */
export const IDLE_QUERY_STATE = Object.freeze({
	fetching: false,
	errors: EMPTY_ERRORS,
	live: 'off' as const
});
