/**
 * Shooting Gallery networking without rendering (Node-testable).
 *
 * - Targets are interpolated like any remote entity, but with an adaptive
 *   delay (`AdaptiveDelay`): the SDK's clock estimates the server's
 *   *present*, so a fixed 100 ms delay would ask for states that have not
 *   arrived yet once one-way latency exceeds it; the delay follows
 *   `rtt / 2 + 2 patch intervals + 2 × jitter`, slewed slowly.
 * - `shoot(dir)` ray-tests the rendered targets locally (client-predicted
 *   hit marker) and sends one shot input carrying the exact view time:
 *   `client_tick` (input header) plus `delay = tickTime(client_tick) -
 *   renderTime` in the payload, so the server's `hit_test_shot` rewinds to
 *   precisely the instant that was on screen (clamped to `maxRewindMs`).
 * - The server answers every shot with a verdict event; `stats` compares
 *   predicted and confirmed hits.
 */
import { GameClient, type GameSocketFactory, type GameZoneView, type RenderedEntity } from '@hops-ops/distributed/game';

import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { onJsonEvent } from '../shared/events.ts';

export const DEFAULT_ZONE = 'gallery-1';
/** Event type ids (server `gallery::EVENT_*`). */
export const EVENT_HELLO = 1;
export const EVENT_SHOT = 2;
export const FLAG_FIRE = 1;
export const FLAG_NO_COMP = 2;
export const KIND = { shooter: 'shooter', target: 'target' } as const;

export type Vec3 = [number, number, number];

export type GalleryHello = Readonly<{ entity: number; eye: Vec3; maxRewindMs: number; range: number; half: number }>;

export type ShotVerdict = Readonly<{
	shot: number;
	hit: number | null;
	compensated: boolean;
	ageMs: number;
	requestedAgeMs: number;
	clamped: boolean;
	tick: number;
}>;

export type ShotRecord = {
	id: number;
	dir: Vec3;
	compensated: boolean;
	/** Target the client saw the ray hit (undefined = predicted miss). */
	predicted: number | undefined;
	/** Where the ray met the predicted target (for markers). */
	point: Vec3 | undefined;
	delayMs: number;
	sentAt: number;
	verdict?: ShotVerdict;
};

export type GalleryStats = {
	shots: number;
	predictedHits: number;
	confirmedHits: number;
	/** Shots where client and server agree (hit/miss and target). */
	agreed: number;
	/** Verdicts received. */
	resolved: number;
	clamped: number;
	/** Mean rewind age of compensated verdicts (ms). */
	meanAgeMs: number;
	lastAgeMs: number;
};

/** Interpolation delay that follows the measured one-way latency. */
export class AdaptiveDelay {
	readonly minMs: number;
	readonly maxMs: number;
	/** Largest change per 100 ms of wall time (keeps the timeline smooth). */
	readonly slewMs: number;
	#value: number;
	#at: number | undefined;

	constructor(options: Readonly<{ minMs?: number; maxMs?: number; slewMs?: number }> = {}) {
		this.minMs = options.minMs ?? 100;
		this.maxMs = options.maxMs ?? 800;
		this.slewMs = options.slewMs ?? 25;
		this.#value = this.minMs;
	}

	get value(): number {
		return this.#value;
	}

	/** Target delay for a zone right now. */
	target(zone: GameZoneView): number {
		const clock = zone.connection.clock;
		const patchMs = 1000 / (zone.connection.welcome?.patchRate || 20);
		const wanted = clock.synced ? clock.rtt / 2 + 2 * patchMs + 2 * clock.jitter : 2 * patchMs;
		return Math.min(this.maxMs, Math.max(this.minMs, wanted));
	}

	/** Current delay, moved toward `target(zone)` by at most `slewMs` / 100 ms. */
	sample(zone: GameZoneView, now: number): number {
		const last = this.#at ?? now;
		this.#at = now;
		const budget = (this.slewMs * Math.max(0, now - last)) / 100;
		const delta = this.target(zone) - this.#value;
		this.#value += Math.max(-budget, Math.min(budget, delta));
		return this.#value;
	}

	/** True when within `toleranceMs` of the target. */
	settled(zone: GameZoneView, toleranceMs = 5): boolean {
		return Math.abs(this.target(zone) - this.#value) <= toleranceMs;
	}
}

export function encodeShot(flags: number, shot: number, delayMs: number, dir: Vec3): Uint8Array {
	const bytes = new Uint8Array(11);
	const view = new DataView(bytes.buffer);
	view.setUint8(0, flags);
	view.setUint16(1, shot & 0xffff, true);
	view.setUint16(3, Math.max(0, Math.min(0xffff, Math.round(delayMs))), true);
	const len = Math.hypot(dir[0], dir[1], dir[2]) || 1;
	for (let i = 0; i < 3; i += 1) {
		view.setInt16(5 + i * 2, Math.round(Math.max(-1, Math.min(1, dir[i] / len)) * 32767), true);
	}
	return bytes;
}

/** Entry distance of a ray (unit `dir`) into a sphere, if hit in front. */
export function raySphere(origin: Vec3, dir: Vec3, center: Vec3, radius: number): number | undefined {
	const oc: Vec3 = [origin[0] - center[0], origin[1] - center[1], origin[2] - center[2]];
	const c = oc[0] ** 2 + oc[1] ** 2 + oc[2] ** 2 - radius * radius;
	if (c <= 0) return 0;
	const b = oc[0] * dir[0] + oc[1] * dir[1] + oc[2] * dir[2];
	const h = b * b - c;
	if (h < 0 || b > 0) return undefined;
	return -b - Math.sqrt(h);
}

export function targetPosition(entity: Pick<RenderedEntity, 'fields'>): Vec3 {
	const f = entity.fields;
	return [Number(f.x ?? 0), Number(f.y ?? 0), Number(f.z ?? 0)];
}

export type GalleryClient = Readonly<{
	client: GameClient<never>;
	delay: AdaptiveDelay;
	hello(): GalleryHello | undefined;
	ready: Promise<GalleryHello>;
	stats: GalleryStats;
	shots: Map<number, ShotRecord>;
	/** Targets as rendered now. */
	targets(): RenderedEntity[];
	/** Nearest rendered target on the ray from the eye along `dir`. */
	predict(dir: Vec3): { target: number | undefined; point: Vec3 | undefined };
	/** Fire along `dir` (game coordinates, z up) from the eye; undefined if not sent. */
	shoot(dir: Vec3, options?: { compensated?: boolean }): ShotRecord | undefined;
	/** Resolves with the verdict of shot `id`. */
	verdict(id: number): Promise<ShotVerdict>;
	onVerdict(listener: (shot: ShotRecord) => void): () => void;
}>;

export type GalleryClientOptions = Readonly<{
	api: DevApi;
	base?: string;
	socket?: GameSocketFactory;
	delay?: AdaptiveDelay;
}>;

export function createGalleryClient(options: GalleryClientOptions): GalleryClient {
	const delay = options.delay ?? new AdaptiveDelay();
	const client = new GameClient<never>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 250,
		interpolationDelayMs: (zone) => delay.sample(zone, zone.connection.now())
	});
	let hello: GalleryHello | undefined;
	let resolveReady: (h: GalleryHello) => void = () => undefined;
	const ready = new Promise<GalleryHello>((resolve) => (resolveReady = resolve));
	onJsonEvent<GalleryHello>(client, EVENT_HELLO, (value) => {
		hello = value;
		resolveReady(value);
	});

	const stats: GalleryStats = {
		shots: 0,
		predictedHits: 0,
		confirmedHits: 0,
		agreed: 0,
		resolved: 0,
		clamped: 0,
		meanAgeMs: 0,
		lastAgeMs: 0
	};
	const shots = new Map<number, ShotRecord>();
	const waiters = new Map<number, (v: ShotVerdict) => void>();
	const listeners = new Set<(shot: ShotRecord) => void>();
	let next = 0;
	let compensatedVerdicts = 0;

	onJsonEvent<ShotVerdict>(client, EVENT_SHOT, (verdict) => {
		const shot = shots.get(verdict.shot);
		if (shot === undefined || shot.verdict !== undefined) return;
		shot.verdict = verdict;
		stats.resolved += 1;
		if (verdict.hit !== null) stats.confirmedHits += 1;
		if ((verdict.hit ?? undefined) === shot.predicted) stats.agreed += 1;
		if (verdict.clamped) stats.clamped += 1;
		if (verdict.compensated) {
			compensatedVerdicts += 1;
			stats.meanAgeMs += (verdict.ageMs - stats.meanAgeMs) / compensatedVerdicts;
			stats.lastAgeMs = verdict.ageMs;
		}
		waiters.get(verdict.shot)?.(verdict);
		waiters.delete(verdict.shot);
		for (const listener of listeners) listener(shot);
	});

	const targets = () => client.render().filter((e) => e.kindName === KIND.target && !e.stale);

	function predict(dir: Vec3): { target: number | undefined; point: Vec3 | undefined } {
		const eye = hello?.eye;
		if (eye === undefined) return { target: undefined, point: undefined };
		const len = Math.hypot(dir[0], dir[1], dir[2]) || 1;
		const unit: Vec3 = [dir[0] / len, dir[1] / len, dir[2] / len];
		let target: number | undefined;
		let point: Vec3 | undefined;
		let best = Infinity;
		for (const entity of targets()) {
			const d = raySphere(eye, unit, targetPosition(entity), Number(entity.fields.r ?? 0.5));
			if (d !== undefined && d < best && d <= (hello?.range ?? 80)) {
				best = d;
				target = entity.id;
				point = [eye[0] + unit[0] * d, eye[1] + unit[1] * d, eye[2] + unit[2] * d];
			}
		}
		return { target, point };
	}

	function shoot(dir: Vec3, opts: { compensated?: boolean } = {}): ShotRecord | undefined {
		const zone = client.authority;
		const eye = hello?.eye;
		if (zone === undefined || eye === undefined || !zone.connection.canSendInput()) return undefined;
		const len = Math.hypot(dir[0], dir[1], dir[2]) || 1;
		const unit: Vec3 = [dir[0] / len, dir[1] / len, dir[2] / len];
		// Exact view time: the delay is measured from client_tick's time to
		// the instant render() samples, so the server rewinds to what is drawn.
		const context = client.inputContext(zone)!;
		const renderTime = zone.connection.serverNow() - context.interpolationDelayMs;
		const delayMs = Math.max(0, zone.connection.tickTime(context.clientTick) - renderTime);
		const { target: predicted, point } = predict(unit);
		const compensated = opts.compensated ?? true;
		next = (next + 1) & 0xffff;
		const id = next;
		const flags = FLAG_FIRE | (compensated ? 0 : FLAG_NO_COMP);
		const seq = zone.connection.sendInput(encodeShot(flags, id, delayMs, unit), context.clientTick);
		if (seq === undefined) return undefined;
		const record: ShotRecord = {
			id,
			dir: unit,
			compensated,
			predicted,
			point,
			delayMs,
			sentAt: zone.connection.now()
		};
		shots.set(id, record);
		if (shots.size > 512) shots.delete(shots.keys().next().value!);
		stats.shots += 1;
		if (predicted !== undefined) stats.predictedHits += 1;
		return record;
	}

	return {
		client,
		delay,
		hello: () => hello,
		ready,
		stats,
		shots,
		targets,
		predict,
		shoot,
		verdict(id) {
			const known = shots.get(id)?.verdict;
			if (known !== undefined) return Promise.resolve(known);
			return new Promise((resolve) => waiters.set(id, resolve));
		},
		onVerdict(listener) {
			listeners.add(listener);
			return () => listeners.delete(listener);
		}
	};
}
