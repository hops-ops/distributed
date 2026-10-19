/**
 * Mob Pit networking without rendering (Node-testable): the generated
 * `createMobpitClient` with the Arena's predicted movement for the own
 * capsule, the zone's typed events folded into a small state object, and
 * the `/dev/mobpit/*` routes.
 *
 * Event flow of one kill: `kill` (broadcast, the tick the mob dies — the
 * fact is only *queued*) → `saved` (to the killer, when the durable cell
 * commit settled; `waitedMs` shows the storage latency) → `profile`.
 */
import { GameClient, type GameSocketFactory } from '@hops-ops/distributed/game';

import { predictPlayer } from '../arena/movement.ts';
import * as pit from '../generated/mobpit/index.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { latest } from '../shared/latest.ts';

export const DEFAULT_ZONE = 'pit-1';
export const MOB_STATE = ['idle', 'chase', 'windup', 'recover', 'return'] as const;
/** Per tier: display name, body radius, telegraph radius (server `tier_stats`). */
export const TIERS: Readonly<Record<number, { name: string; radius: number; tele: number; color: string }>> = {
	1: { name: 'Grunt', radius: 0.5, tele: 1.8, color: '#9ad16b' },
	2: { name: 'Brute', radius: 0.7, tele: 2.2, color: '#b48cf2' },
	3: { name: 'Elite', radius: 0.8, tele: 2.6, color: '#ff6b6b' }
};

/** `/dev/mobpit/leaderboard` neighbourhood (HTTP only, not a zone event). */
export type BoardPosition = Readonly<{ rank: number | null; score: number | null; around: pit.BoardRow[] }>;

export type MobPitClient = pit.MobpitClient &
	Readonly<{
		hello(): pit.HelloEvent | undefined;
		profile(): pit.Profile | undefined;
		board(): pit.BoardSnapshot | undefined;
		/** Kills by me whose fact has not settled yet (key → event). */
		unsaved: ReadonlyMap<string, pit.KillEvent>;
		/** Resolves on the first hello. */
		ready: Promise<pit.HelloEvent>;
	}>;

export type MobPitClientOptions = Readonly<{
	api: DevApi;
	base?: string;
	socket?: GameSocketFactory;
}>;

export function createMobPitClient(options: MobPitClientOptions): MobPitClient {
	const typed = pit.createMobpitClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		inputDtMs: 1000 / 30,
		// The movement part of the frame is the Arena's (attack only adds a bit).
		prediction: { predict: (fields, input) => predictPlayer(fields, input) }
	});
	const hello = latest<pit.HelloEvent>(
		(h) => typed.on('hello', h),
		(value) => typed.client.setControlledEntity(value.entity)
	);
	const profile = latest<pit.Profile>((h) => typed.on('profile', h));
	const board = latest<pit.BoardSnapshot>((h) => typed.on('board', h));
	const unsaved = new Map<string, pit.KillEvent>();
	typed.on('kill', (kill) => {
		if (kill.killer === hello.get()?.entity) unsaved.set(kill.key, kill);
	});
	typed.on('saved', (saved) => unsaved.delete(saved.key));
	return {
		...typed,
		hello: hello.get,
		profile: profile.get,
		board: board.get,
		unsaved,
		ready: hello.ready
	};
}

/** Observer-only client for the leaderboard page (no prediction, no inputs). */
export function createBoardWatcher(options: MobPitClientOptions): Readonly<{
	client: GameClient<pit.PitInput>;
	board(): pit.BoardSnapshot | undefined;
	on(handler: (value: pit.BoardSnapshot) => void): () => void;
}> {
	const typed = pit.createMobpitClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 1000
	});
	const board = latest<pit.BoardSnapshot>((h) => typed.on('board', h));
	return { client: typed.client, board: board.get, on: (handler) => typed.on('board', handler) };
}

/** `/dev/mobpit/*` (dev only; none of these take an identity or a reward). */
export class MobPitApi {
	readonly base: string;

	constructor(base = '') {
		this.base = base;
	}

	storage(): Promise<{ latencyMs: number }> {
		return this.#json('/dev/mobpit/storage');
	}

	/** Artificial latency in front of every durable commit (server-wide). */
	setStorage(latencyMs: number): Promise<{ latencyMs: number }> {
		return this.#json('/dev/mobpit/storage', { latencyMs });
	}

	stats(): Promise<Record<string, unknown>> {
		return this.#json('/dev/mobpit/stats');
	}

	crawlers(): Promise<{ crawlers: { principal: string; name: string; xp: number; kills: number; level: number }[] }> {
		return this.#json('/dev/mobpit/crawlers');
	}

	leaderboard(member?: string): Promise<{ snapshot: pit.BoardSnapshot; me: { kills: BoardPosition; xp: BoardPosition } | null }> {
		const query = member === undefined ? '' : `?member=${encodeURIComponent(member)}`;
		return this.#json(`/dev/mobpit/leaderboard${query}`);
	}

	rebuild(): Promise<pit.RebuildReport> {
		return this.#json('/dev/mobpit/leaderboard/rebuild', {});
	}

	async #json<T>(path: string, body?: unknown): Promise<T> {
		const response = await fetch(`${this.base}${path}`, {
			method: body === undefined ? 'GET' : 'POST',
			headers: body === undefined ? undefined : { 'content-type': 'application/json' },
			body: body === undefined ? undefined : JSON.stringify(body)
		});
		if (!response.ok) throw new Error(`${path}: HTTP ${response.status} ${await response.text()}`);
		return (await response.json()) as T;
	}
}
