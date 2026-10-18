/**
 * Mob Pit networking without rendering (Node-testable): the `GameClient`
 * with the Arena's predicted movement for the own capsule, the zone's JSON
 * events folded into a small state object, and the `/dev/mobpit/*` routes.
 *
 * Event flow of one kill: `kill` (broadcast, the tick the mob dies — the
 * fact is only *queued*) → `saved` (to the killer, when the durable cell
 * commit settled; `waitedMs` shows the storage latency) → `profile`.
 */
import { GameClient, type GameSocketFactory } from '@hops-ops/distributed/game';

import { encodeMoveInput, predictPlayer, type MoveInput } from '../arena/movement.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { onJsonEvent } from '../shared/events.ts';

export const DEFAULT_ZONE = 'pit-1';
/** Event type ids (server `mobpit::zone::EVENT_*`). */
export const EVENT = { hello: 1, profile: 2, kill: 3, saved: 4, board: 5, died: 6 } as const;
/** Input bits: the Arena's dash (1) plus attack. */
export const BUTTON_DASH = 1;
export const BUTTON_ATTACK = 2;
export const KIND = { player: 'player', mob: 'mob', pillar: 'pillar', pit: 'pit' } as const;
export const MOB_STATE = ['idle', 'chase', 'windup', 'recover', 'return'] as const;
/** Per tier: display name, body radius, telegraph radius (server `tier_stats`). */
export const TIERS: Readonly<Record<number, { name: string; radius: number; tele: number; color: string }>> = {
	1: { name: 'Grunt', radius: 0.5, tele: 1.8, color: '#9ad16b' },
	2: { name: 'Brute', radius: 0.7, tele: 2.2, color: '#b48cf2' },
	3: { name: 'Elite', radius: 0.8, tele: 2.6, color: '#ff6b6b' }
};

export type PitHello = Readonly<{ entity: number; half: number; boot: string; attackRange: number; attackArcCos: number }>;
export type Profile = Readonly<{
	xp: number;
	level: number;
	kills: number;
	levelXp: number;
	nextLevelXp: number;
	source: 'cell' | 'settled';
}>;
export type KillEvent = Readonly<{ key: string; mob: number; tier: number; x: number; y: number; killer: number; killerName: string }>;
export type SavedEvent = Readonly<{
	key: string;
	ok: boolean;
	replayed?: boolean;
	xp?: number;
	totalXp?: number;
	kills?: number;
	level?: number;
	waitedMs: number;
	x: number;
	y: number;
	error?: string;
}>;
export type BoardRow = Readonly<{ rank: number; member: string; name: string; score: number }>;
export type BoardSnapshot = Readonly<{
	version: number;
	backend?: 'memory' | 'redis';
	members?: number;
	kills: BoardRow[];
	xp: BoardRow[];
	background?: Readonly<{ enqueued: number; applied: number; dropped: number; failed: number }>;
	lastRebuild?: RebuildReport | null;
}>;
export type RebuildReport = Readonly<{
	backend: string;
	crawlers: number;
	events: number;
	applied: number;
	duplicates: number;
	identicalTop: boolean;
	matchesCells: boolean;
	mismatches: unknown[];
	tookMs: number;
}>;
export type BoardPosition = Readonly<{ rank: number | null; score: number | null; around: BoardRow[] }>;

export type MobPitEvents = {
	hello: PitHello;
	profile: Profile;
	kill: KillEvent;
	saved: SavedEvent;
	board: BoardSnapshot;
	died: { respawnMs: number };
};

export type MobPitClient = Readonly<{
	client: GameClient<MoveInput>;
	hello(): PitHello | undefined;
	profile(): Profile | undefined;
	board(): BoardSnapshot | undefined;
	/** Kills by me whose fact has not settled yet (key → event). */
	unsaved: ReadonlyMap<string, KillEvent>;
	/** Resolves on the first hello. */
	ready: Promise<PitHello>;
	on<K extends keyof MobPitEvents>(type: K, handler: (value: MobPitEvents[K]) => void): () => void;
}>;

export type MobPitClientOptions = Readonly<{
	api: DevApi;
	base?: string;
	socket?: GameSocketFactory;
}>;

export function createMobPitClient(options: MobPitClientOptions): MobPitClient {
	const ease = { mode: 'ease', durationMs: 150 } as const;
	const snap = { mode: 'snap' } as const;
	const replace = { mode: 'replace' } as const;
	const client = new GameClient<MoveInput>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		inputDtMs: 1000 / 30,
		prediction: {
			encodeInput: (input) => encodeMoveInput(input),
			predict: (fields, input) => predictPlayer(fields, input),
			corrections: {
				[KIND.player]: {
					x: ease,
					y: ease,
					vx: snap,
					vy: snap,
					dash: snap,
					cd: snap,
					name: replace,
					hp: replace,
					face: replace,
					swing: replace,
					level: replace,
					dead: replace
				}
			}
		}
	});
	const listeners = new Map<string, Set<(value: unknown) => void>>();
	const emit = (type: string, value: unknown) => {
		for (const handler of listeners.get(type) ?? []) handler(value);
	};
	let hello: PitHello | undefined;
	let profile: Profile | undefined;
	let board: BoardSnapshot | undefined;
	const unsaved = new Map<string, KillEvent>();
	let resolveReady: (h: PitHello) => void = () => undefined;
	const ready = new Promise<PitHello>((resolve) => (resolveReady = resolve));

	onJsonEvent<PitHello>(client, EVENT.hello, (value) => {
		hello = value;
		client.setControlledEntity(value.entity);
		resolveReady(value);
		emit('hello', value);
	});
	onJsonEvent<Profile>(client, EVENT.profile, (value) => {
		profile = value;
		emit('profile', value);
	});
	onJsonEvent<KillEvent>(client, EVENT.kill, (value) => {
		if (hello !== undefined && value.killer === hello.entity) unsaved.set(value.key, value);
		emit('kill', value);
	});
	onJsonEvent<SavedEvent>(client, EVENT.saved, (value) => {
		unsaved.delete(value.key);
		emit('saved', value);
	});
	onJsonEvent<BoardSnapshot>(client, EVENT.board, (value) => {
		board = value;
		emit('board', value);
	});
	onJsonEvent<{ respawnMs: number }>(client, EVENT.died, (value) => emit('died', value));

	return {
		client,
		hello: () => hello,
		profile: () => profile,
		board: () => board,
		unsaved,
		ready,
		on(type, handler) {
			let set = listeners.get(type);
			if (set === undefined) listeners.set(type, (set = new Set()));
			const h = handler as (value: unknown) => void;
			set.add(h);
			return () => set.delete(h);
		}
	};
}

/** Observer-only client for the leaderboard page (no prediction, no inputs). */
export function createBoardWatcher(options: MobPitClientOptions): Readonly<{
	client: GameClient<never>;
	board(): BoardSnapshot | undefined;
	on(handler: (value: BoardSnapshot) => void): () => void;
}> {
	const client = new GameClient<never>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 1000
	});
	let board: BoardSnapshot | undefined;
	const handlers = new Set<(value: BoardSnapshot) => void>();
	onJsonEvent<BoardSnapshot>(client, EVENT.board, (value) => {
		board = value;
		for (const h of handlers) h(value);
	});
	return {
		client,
		board: () => board,
		on(handler) {
			handlers.add(handler);
			return () => handlers.delete(handler);
		}
	};
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

	leaderboard(member?: string): Promise<{ snapshot: BoardSnapshot; me: { kills: BoardPosition; xp: BoardPosition } | null }> {
		const query = member === undefined ? '' : `?member=${encodeURIComponent(member)}`;
		return this.#json(`/dev/mobpit/leaderboard${query}`);
	}

	rebuild(): Promise<RebuildReport> {
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
