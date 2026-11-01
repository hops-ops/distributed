/**
 * Ping/Pong clock synchronization (`PROTOCOL.md` §3).
 *
 * Per sample: `rtt = now - pong.clientTime`,
 * `offset = pong.serverTime + rtt/2 - now`. RTT and jitter are smoothed like
 * TCP's SRTT/RTTVAR (RFC 6298 gains 1/8 and 1/4). The offset estimate uses
 * the lowest-RTT samples of a sliding window (they carry the least queueing
 * asymmetry) and moves toward new estimates gradually so the rendered
 * server timeline never jumps by more than `maxSlewPerSample`.
 */

export type ClockSyncOptions = Readonly<{
	/** Samples kept for the offset estimate. Default 16. */
	window?: number;
	/** Largest offset correction applied per sample once synced (ms). Default 25. */
	maxSlewPerSample?: number;
}>;

export type ClockSample = Readonly<{ rtt: number; offset: number; at: number }>;

export class ClockSync {
	readonly #window: number;
	readonly #maxSlew: number;
	#samples: ClockSample[] = [];
	#offset = 0;
	#rtt = 0;
	#jitter = 0;
	#synced = false;
	#seeded = false;

	constructor(options: ClockSyncOptions = {}) {
		this.#window = Math.max(1, options.window ?? 16);
		this.#maxSlew = options.maxSlewPerSample ?? 25;
	}

	/** Estimated `serverClock - localClock` in ms. */
	get offset(): number {
		return this.#offset;
	}

	/** Smoothed round-trip time in ms. */
	get rtt(): number {
		return this.#rtt;
	}

	/** Smoothed RTT variation (jitter) in ms. */
	get jitter(): number {
		return this.#jitter;
	}

	/** True once at least one Pong was observed. */
	get synced(): boolean {
		return this.#synced;
	}

	get samples(): readonly ClockSample[] {
		return this.#samples;
	}

	/** Coarse seed from a Welcome (`serverTime` at receipt, latency unknown). */
	seed(serverTime: number, now: number): void {
		if (this.#synced) return;
		this.#offset = serverTime - now;
		this.#seeded = true;
	}

	/** Forget samples (a new connection may face a different path or server). */
	reset(): void {
		this.#samples = [];
		this.#synced = false;
		this.#seeded = false;
		this.#rtt = 0;
		this.#jitter = 0;
	}

	/** Fold in one Pong; returns the raw sample. */
	observe(clientTime: number, serverTime: number, now: number): ClockSample | undefined {
		const rtt = now - clientTime;
		if (!(rtt >= 0) || !Number.isFinite(serverTime)) return undefined;
		const sample = { rtt, offset: serverTime + rtt / 2 - now, at: now };
		this.#samples.push(sample);
		if (this.#samples.length > this.#window) this.#samples.shift();

		if (!this.#synced) {
			this.#rtt = rtt;
			this.#jitter = rtt / 2;
		} else {
			this.#jitter += (Math.abs(rtt - this.#rtt) - this.#jitter) / 4;
			this.#rtt += (rtt - this.#rtt) / 8;
		}

		const best = [...this.#samples].sort((a, b) => a.rtt - b.rtt);
		const keep = best.slice(0, Math.max(1, Math.ceil(best.length / 2)));
		const offsets = keep.map((s) => s.offset).sort((a, b) => a - b);
		const target = offsets[Math.floor(offsets.length / 2)];
		if (!this.#synced) {
			this.#offset = target;
			this.#synced = true;
		} else {
			const delta = target - this.#offset;
			this.#offset += Math.max(-this.#maxSlew, Math.min(this.#maxSlew, delta));
		}
		this.#seeded = true;
		return sample;
	}

	/** Server clock estimate for local time `now`. */
	serverNow(now: number): number {
		return now + (this.#seeded ? this.#offset : 0);
	}
}
