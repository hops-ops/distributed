/**
 * Timing helpers for game clients.
 *
 * - `AdaptiveDelay`: interpolation delay that follows the measured network.
 *   The clock estimates the server's *present*, so a fixed delay smaller
 *   than the one-way latency asks for states that have not arrived yet;
 *   the adaptive delay targets `rtt / 2 + patchIntervals × patch interval +
 *   jitterFactor × jitter`, clamped to `[minMs, maxMs]` and slewed slowly
 *   so the rendered timeline never jumps. `GameClient` uses one per zone by
 *   default.
 * - `InputPacer`: sends one input per server tick from a render loop
 *   (accumulator with a catch-up cap), so inputs match the server's command
 *   frames instead of the display refresh rate.
 */

/** What `AdaptiveDelay` reads from a zone (a `GameZoneView` fits). */
export type DelaySource = Readonly<{
	connection: Readonly<{
		clock: Readonly<{ rtt: number; jitter: number; synced: boolean }>;
		welcome?: Readonly<{ patchRate: number }>;
	}>;
}>;

export type AdaptiveDelayOptions = Readonly<{
	/** Lower bound (ms). Default: `patchIntervals` patch intervals of the zone. */
	minMs?: number;
	/** Upper bound (ms). Default 800. */
	maxMs?: number;
	/** Largest change per 100 ms of wall time (ms). Default 25. */
	slewMs?: number;
	/** Patch intervals of buffer. Default 2. */
	patchIntervals?: number;
	/** Jitter multiplier. Default 2. */
	jitterFactor?: number;
}>;

/** Interpolation delay that follows the measured one-way latency. */
export class AdaptiveDelay {
	readonly maxMs: number;
	readonly slewMs: number;
	readonly #minMs: number | undefined;
	readonly #patchIntervals: number;
	readonly #jitterFactor: number;
	#value: number | undefined;
	#at: number | undefined;

	constructor(options: AdaptiveDelayOptions = {}) {
		this.#minMs = options.minMs;
		this.maxMs = options.maxMs ?? 800;
		this.slewMs = options.slewMs ?? 25;
		this.#patchIntervals = options.patchIntervals ?? 2;
		this.#jitterFactor = options.jitterFactor ?? 2;
	}

	/** Current delay (ms); before the first `sample`, the lower bound at 20 Hz. */
	get value(): number {
		return this.#value ?? this.#minMs ?? this.#patchIntervals * 50;
	}

	/** Lower bound for a zone. */
	minMs(zone: DelaySource): number {
		return this.#minMs ?? this.#patchIntervals * patchMs(zone);
	}

	/** Delay the zone's network calls for right now (before slewing). */
	target(zone: DelaySource): number {
		const clock = zone.connection.clock;
		const patch = patchMs(zone);
		const wanted = clock.synced
			? clock.rtt / 2 + this.#patchIntervals * patch + this.#jitterFactor * clock.jitter
			: this.#patchIntervals * patch;
		const min = this.minMs(zone);
		return Math.min(Math.max(min, this.maxMs), Math.max(min, wanted));
	}

	/**
	 * Delay to render with at local time `now`: starts at the target, then
	 * moves toward it by at most `slewMs` per 100 ms. Calling it several
	 * times with the same `now` returns the same value.
	 */
	sample(zone: DelaySource, now: number): number {
		const target = this.target(zone);
		if (this.#value === undefined || this.#at === undefined) {
			this.#value = target;
			this.#at = now;
			return target;
		}
		const budget = (this.slewMs * Math.max(0, now - this.#at)) / 100;
		this.#at = Math.max(this.#at, now);
		const delta = target - this.#value;
		this.#value += Math.max(-budget, Math.min(budget, delta));
		return this.#value;
	}

	/** True when within `toleranceMs` of the target. */
	settled(zone: DelaySource, toleranceMs = 5): boolean {
		return Math.abs(this.target(zone) - this.value) <= toleranceMs;
	}

	/** Start over (next `sample` jumps to the target). */
	reset(): void {
		this.#value = undefined;
		this.#at = undefined;
	}
}

function patchMs(zone: DelaySource): number {
	return 1000 / (zone.connection.welcome?.patchRate || 20);
}

export type InputPacerOptions = Readonly<{
	/** Inputs per second. Default: `tickRate()` or 30. */
	rate?: number;
	/** Current server tick rate (e.g. `() => client.authority?.connection.welcome?.tickRate`). */
	tickRate?: () => number | undefined;
	/** Most inputs sent by one `update` (after a stall the backlog is dropped). Default 4. */
	maxPerUpdate?: number;
}>;

/**
 * Fires `send` once per input interval from a render loop: call
 * `update(now, send)` every frame. One input per server tick is what an
 * authoritative zone consumes (see the server's `InputCredits`); sending
 * per display frame would overflow its input buffer at 120 Hz or starve
 * it at 30 Hz.
 */
export class InputPacer {
	readonly #rate: number | undefined;
	readonly #tickRate: (() => number | undefined) | undefined;
	readonly #maxPerUpdate: number;
	#acc = 0;
	#last: number | undefined;

	constructor(options: InputPacerOptions = {}) {
		this.#rate = options.rate;
		this.#tickRate = options.tickRate;
		this.#maxPerUpdate = Math.max(1, options.maxPerUpdate ?? 4);
	}

	/** Current input interval (ms). */
	get intervalMs(): number {
		const rate = this.#rate ?? this.#tickRate?.() ?? 30;
		return 1000 / (rate > 0 ? rate : 30);
	}

	/** Call every frame with a monotonic time; returns the inputs sent. */
	update(now: number, send: (dtMs: number) => void): number {
		if (this.#last === undefined) this.#last = now;
		this.#acc += Math.max(0, now - this.#last);
		this.#last = now;
		const interval = this.intervalMs;
		let sent = 0;
		// Tolerate float drift from summing frame times.
		while (this.#acc >= interval - 1e-6 && sent < this.#maxPerUpdate) {
			this.#acc -= interval;
			send(interval);
			sent += 1;
		}
		if (sent === this.#maxPerUpdate) this.#acc = Math.min(this.#acc, interval);
		this.#acc = Math.max(0, this.#acc);
		return sent;
	}

	/** Forget accumulated time (e.g. after a pause). */
	reset(): void {
		this.#acc = 0;
		this.#last = undefined;
	}
}
