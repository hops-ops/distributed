/**
 * Keep a set of zones around the authoritative one observed — e.g. the
 * neighbouring rooms of a room grid — and merged into `render()`.
 *
 * Zones hosted as celld objects treat observers as **passive watchers**: a
 * watcher never keeps a zone awake and never wakes a sleeping one (it is
 * refused with `ASLEEP`, and a zone that falls asleep closes its watchers).
 * `watchZones` re-syncs on every authority change, lets go of closed
 * watchers (nothing is drawn for a sleeping zone) and tries them again
 * after `retryMs`, backing off (doubling up to `maxRetryMs`) while a zone
 * stays asleep — every attempt costs an admission ticket.
 *
 * **Near zones** (proximity wake): `setNear(zones)` marks watched zones the
 * player is close to. A near watcher says so to the zone (Hello `near`,
 * then `Near` without reconnecting); on celld, when its ticket grants
 * `wake` (the issuer decides, e.g. only rooms next to the player's own), it
 * wakes the zone and keeps it awake. A sleeping zone that becomes near is
 * retried at once. Zones that stop being near are passive watchers again.
 * A watcher whose ticket did not grant `wake` (`WAKE_DENIED`, e.g. issued
 * before the issuer knew where the player is) is reopened with a fresh
 * ticket while its zone stays near — at once the first time, then backing
 * off like a sleeping zone.
 */
import type { GameClient } from './client.js';
import { GameErrorCode } from './messages.js';

/** Per watched zone: connected, (re)connecting, or refused / closed (asleep). */
export type WatchState = 'watching' | 'connecting' | 'asleep';

export type WatchZonesOptions = Readonly<{
	/** First retry of a zone that is not watched (default 3000 ms). */
	retryMs?: number;
	/** Longest retry back-off (default 30000 ms). */
	maxRetryMs?: number;
	/** Clock (ms; tests). */
	now?: () => number;
	/** Timer functions (tests). */
	setInterval?: (fn: () => void, ms: number) => unknown;
	clearInterval?: (handle: unknown) => void;
}>;

export type ZoneWatch = Readonly<{
	/** State of every wanted zone. */
	states(): ReadonlyMap<string, WatchState>;
	/** Re-sync now (also runs on authority changes and every `retryMs`). */
	sync(): void;
	/** Stop syncing (observers already open stay until `unobserve`/`close`). */
	stop(): void;
	/**
	 * The watched zones that are near (replaces the previous set; zones not
	 * watched are kept and apply once they are).
	 */
	setNear(zones: Iterable<string>): void;
	/** The zones last passed to `setNear`. */
	near(): ReadonlySet<string>;
}>;

/**
 * Observe `around(authorityZone)` (the authoritative zone itself is skipped)
 * for as long as the watch runs.
 */
export function watchZones(
	client: GameClient<any>,
	around: (authorityZone: string) => readonly string[],
	options: WatchZonesOptions = {}
): ZoneWatch {
	const states = new Map<string, WatchState>();
	/** Per zone: failed attempts in a row and when to try again. */
	const backoff = new Map<string, { failures: number; at: number }>();
	const now = options.now ?? (() => Date.now());
	const retryMs = options.retryMs ?? 3000;
	const maxRetryMs = options.maxRetryMs ?? 30_000;
	const failed = (zone: string) => {
		const failures = (backoff.get(zone)?.failures ?? 0) + 1;
		backoff.set(zone, { failures, at: now() + Math.min(maxRetryMs, retryMs * 2 ** (failures - 1)) });
	};
	let wanted = new Set<string>();
	let near = new Set<string>();
	/**
	 * Near zones whose watcher's ticket did not grant wake: refusals in a
	 * row (kept across Welcomes) and when to ask for a fresh ticket.
	 */
	const denied = new Map<string, { failures: number; at: number; retry: boolean }>();
	let stopped = false;
	const letGo = (zone: string) => {
		if (client.zone(zone)?.role === 'observer') client.unobserve(zone);
	};
	const sync = () => {
		if (stopped) return;
		const here = client.authority?.zoneId;
		wanted = new Set(here === undefined ? [] : around(here).filter((z) => z !== here));
		for (const zone of client.zones()) {
			if (zone.role === 'observer' && !wanted.has(zone.zoneId)) client.unobserve(zone.zoneId);
		}
		for (const zone of [...states.keys()]) if (!wanted.has(zone)) states.delete(zone);
		for (const zone of [...backoff.keys()]) if (!wanted.has(zone)) backoff.delete(zone);
		for (const zone of [...denied.keys()]) if (!wanted.has(zone) || !near.has(zone)) denied.delete(zone);
		for (const zone of wanted) {
			const existing = client.zone(zone);
			if (existing?.role === 'authority') continue;
			const refusal = denied.get(zone);
			if (existing?.role === 'observer' && existing.connection.state !== 'closed') {
				if (!(refusal?.retry === true && refusal.at <= now())) {
					client.setNear(zone, near.has(zone));
					continue;
				}
				// Ask the issuer again: a fresh ticket may grant wake.
				refusal.retry = false;
			}
			const due = (backoff.get(zone)?.at ?? 0) <= now();
			// A closed watcher (refused or let go by a sleeping zone): anew,
			// once its back-off is over.
			if (!due) continue;
			letGo(zone);
			if (states.get(zone) !== 'asleep') states.set(zone, 'connecting');
			let welcomed: Promise<unknown>;
			try {
				welcomed = client.observe(zone, { near: near.has(zone) });
			} catch {
				return; // the client closed
			}
			welcomed.then(
				() => {
					backoff.delete(zone);
					if (wanted.has(zone)) states.set(zone, 'watching');
				},
				() => {
					failed(zone);
					if (wanted.has(zone)) states.set(zone, 'asleep');
					// Nothing to show of a sleeping zone.
					letGo(zone);
				}
			);
		}
	};
	const offs = [
		client.on('authority', () => sync()),
		client.on('error', (zone, error) => {
			if (error.code !== GameErrorCode.WAKE_DENIED || !wanted.has(zone) || !near.has(zone)) return;
			const failures = (denied.get(zone)?.failures ?? 0) + 1;
			const wait = failures === 1 ? 0 : Math.min(maxRetryMs, retryMs * 2 ** (failures - 2));
			denied.set(zone, { failures, at: now() + wait, retry: true });
			if (wait === 0) queueMicrotask(sync);
		}),
		client.on('zoneClosed', (zone) => {
			if (!wanted.has(zone) || client.zone(zone)?.role !== 'observer') return;
			failed(zone);
			states.set(zone, 'asleep');
			letGo(zone);
		})
	];
	const every = options.setInterval ?? ((fn: () => void, ms: number) => globalThis.setInterval(fn, ms));
	const clear = options.clearInterval ?? ((handle: unknown) => globalThis.clearInterval(handle as ReturnType<typeof setInterval>));
	const timer = every(sync, Math.min(1000, retryMs));
	// Never keep a Node process alive for this.
	(timer as { unref?: () => void } | undefined)?.unref?.();
	sync();
	return {
		states: () => states,
		sync,
		setNear: (zones) => {
			const next = new Set(zones);
			let retry = false;
			for (const zone of next) {
				// A sleeping zone that becomes near is tried again at once
				// (the near watcher may wake it).
				if (!near.has(zone) && wanted.has(zone) && states.get(zone) === 'asleep') {
					backoff.delete(zone);
					retry = true;
				}
			}
			const changed = next.size !== near.size || [...next].some((z) => !near.has(z));
			near = next;
			if (!changed) return;
			if (retry) sync();
			else for (const zone of wanted) client.setNear(zone, near.has(zone));
		},
		near: () => near,
		stop: () => {
			stopped = true;
			clear(timer);
			for (const off of offs) off();
		}
	};
}
