/** Clock and timers (injectable for deterministic tests). */
export type Scheduler = Readonly<{
	now(): number;
	setTimeout(callback: () => void, ms: number): unknown;
	clearTimeout(handle: unknown): void;
}>;

export const defaultScheduler: Scheduler = Object.freeze({
	now: () => globalThis.performance?.now() ?? Date.now(),
	setTimeout: (callback: () => void, ms: number) => globalThis.setTimeout(callback, ms),
	clearTimeout: (handle: unknown) =>
		globalThis.clearTimeout(handle as ReturnType<typeof globalThis.setTimeout>)
});

export type BackoffOptions = Readonly<{
	/** Delay before the first retry. */
	baseMs: number;
	/** Upper bound of every delay. */
	maxMs: number;
	/**
	 * Fraction of the delay that `random` may subtract, in [0, 1]. Default 0
	 * (exact delays). Jitter keeps clients that failed together from retrying
	 * together.
	 */
	jitter?: number;
	/** Uniform source in [0, 1). Default `Math.random`. */
	random?: () => number;
}>;

/**
 * Delay before retry `attempt` (1-based): `baseMs` doubling per attempt,
 * capped at `maxMs`, less the jittered fraction.
 */
export function backoffDelay(attempt: number, options: BackoffOptions): number {
	const exponential = Math.min(
		options.maxMs,
		options.baseMs * 2 ** Math.min(Math.max(attempt, 1) - 1, 30)
	);
	const jitter = options.jitter ?? 0;
	if (jitter === 0) return exponential;
	return exponential * (1 - jitter * (options.random ?? Math.random)());
}

/**
 * Mark a housekeeping timer so that, where the runtime has the notion
 * (Node), it does not keep the process alive.
 */
export function unrefTimer(handle: unknown): void {
	(handle as { unref?: () => void } | null | undefined)?.unref?.();
}
