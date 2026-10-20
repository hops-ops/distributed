/**
 * Shooting Gallery networking without rendering (Node-testable).
 *
 * - Targets are interpolated like any remote entity with the SDK's default
 *   adaptive delay (`AdaptiveDelay`): the clock estimates the server's
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
import {
	type AdaptiveDelay,
	type AdaptiveDelayOptions,
	type GameClient,
	type GameSocketFactory,
	type GameZoneView,
	type RenderedEntity
} from '@hops-ops/distributed/game';

import * as gallery from '../generated/gallery/index.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { latest } from '../shared/latest.ts';

export const DEFAULT_ZONE = 'gallery-1';

export type Vec3 = [number, number, number];

export type GalleryHello = gallery.HelloEvent;
export type ShotVerdict = gallery.ShotEvent;

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

function vec3(v: readonly number[]): Vec3 {
	return [v[0] ?? 0, v[1] ?? 0, v[2] ?? 0];
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
	const { x, y, z } = gallery.targetFields(entity);
	return [x, y, z];
}

export type GalleryClient = Readonly<{
	client: GameClient<gallery.ShotInput>;
	/** The SDK's adaptive interpolation delay of `zone` (default: the authority). */
	delay(zone?: GameZoneView): AdaptiveDelay;
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
	adaptiveDelay?: AdaptiveDelayOptions;
}>;

export function createGalleryClient(options: GalleryClientOptions): GalleryClient {
	const typed = gallery.createGalleryClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 250,
		adaptiveDelay: options.adaptiveDelay
	});
	const { client } = typed;
	const delay = (zone: GameZoneView | undefined = client.authority): AdaptiveDelay => {
		if (zone === undefined) throw new Error('not joined');
		return client.adaptiveDelay(zone)!;
	};
	const hello = latest<GalleryHello>((h) => typed.on('hello', h));

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

	typed.on('shot', (verdict) => {
		const shot = shots.get(verdict.shot);
		if (shot === undefined || shot.verdict !== undefined) return;
		shot.verdict = verdict;
		stats.resolved += 1;
		if (verdict.hit !== undefined) stats.confirmedHits += 1;
		if (verdict.hit === shot.predicted) stats.agreed += 1;
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

	const targets = () => client.render().filter((e) => gallery.isTarget(e) && !e.stale);

	function predict(dir: Vec3): { target: number | undefined; point: Vec3 | undefined } {
		const eye = hello.get()?.eye;
		if (eye === undefined) return { target: undefined, point: undefined };
		const len = Math.hypot(dir[0], dir[1], dir[2]) || 1;
		const unit: Vec3 = [dir[0] / len, dir[1] / len, dir[2] / len];
		let target: number | undefined;
		let point: Vec3 | undefined;
		let best = Infinity;
		for (const entity of targets()) {
			const d = raySphere(vec3(eye), unit, targetPosition(entity), gallery.targetFields(entity).r);
			if (d !== undefined && d < best && d <= (hello.get()?.range ?? 80)) {
				best = d;
				target = entity.id;
				point = [eye[0] + unit[0] * d, eye[1] + unit[1] * d, eye[2] + unit[2] * d];
			}
		}
		return { target, point };
	}

	function shoot(dir: Vec3, opts: { compensated?: boolean } = {}): ShotRecord | undefined {
		const zone = client.authority;
		const eye = hello.get()?.eye;
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
		const flags = gallery.BUTTON.fire | (compensated ? 0 : gallery.BUTTON.noComp);
		const payload = gallery.encodeInput({
			flags,
			shot: id,
			delayMs: Math.min(0xffff, Math.round(delayMs)),
			dx: unit[0],
			dy: unit[1],
			dz: unit[2]
		});
		const seq = zone.connection.sendInput(payload, context.clientTick);
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
		hello: hello.get,
		ready: hello.ready,
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
