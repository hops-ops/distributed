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
/** Reopen backoff after a live failure frame (`docs/live-query-delivery.md`). */
export const LIVE_FAILURE_RETRY_BASE_MS = 1_000;
export const LIVE_FAILURE_RETRY_MAX_MS = 30_000;
