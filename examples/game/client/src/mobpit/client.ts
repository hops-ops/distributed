/**
 * Mob Pit networking without rendering (Node-testable): the generated
 * `createMobpitClient` (the own capsule predicted by `pit_step`, the Rust
 * Arena movement compiled to WebAssembly), the room's typed events folded
 * into a small state object, the rooms' generated contents
 * (`createMobpitPures().pitRoom`, the Rust room generator as WebAssembly),
 * and the `/dev/mobpit/*` routes.
 *
 * Every room (`pit.<x>.<y>`) is a celld Durable Object reached at
 * `/rooms/{room}`; celld forwards the socket to whichever node runs it.
 * Walking through a doorway, the room hands the player to the next room
 * (`Handoff`), and the SDK reconnects there with the transfer ticket.
 *
 * One continuous world: room `(x, y)` sits at world `(x, y) × ROOM_SIZE`
 * and entity positions are room-local, so `worldOf` places anything from
 * any room (the doorway arrival is the same world point, see
 * `game_sim::mobpit`).
 *
 * Neighbours: the client also watches the 8 rooms around its own as an
 * observer (`watchZones` over `neighbourRooms`), so their mobs are drawn where they are. On
 * celld an observer is a passive watcher: it keeps no room awake and wakes
 * none (a sleeping room answers `ASLEEP`; it is tried again every few
 * seconds and shows nothing meanwhile).
 *
 * Event flow of one kill: `kill` (broadcast, the tick the mob dies — the
 * fact is only *queued*) → `saved` (to the killer, when the durable cell
 * commit settled) → `profile`.
 */
import { watchZones, type EntityFields, type GameSocketFactory, type RenderedEntity, type WatchState } from '@hops-ops/distributed/game';

import * as pit from '../generated/mobpit/index.ts';
import type { DevApi } from '../shared/dev-api.ts';
import { latest } from '../shared/latest.ts';

export const DEFAULT_ZONE = 'pit.0.0';

/** `pit.<x>.<y>` → `[x, y]` (undefined for anything else). */
export function parseRoom(zone: string): [number, number] | undefined {
	const m = /^pit\.(-?\d+)\.(-?\d+)$/.exec(zone);
	return m === null ? undefined : [Number(m[1]), Number(m[2])];
}

/** Room edge length (m): room `(x, y)` is centred at world `(x, y) × ROOM_SIZE`. */
export const ROOM_SIZE = pit.PIT_WORLD.roomSize;

/** World position (m) of a room-local point of `zone` (undefined for a non-room zone). */
export function toWorldXY(zone: string, x: number, y: number): { x: number; y: number } | undefined {
	const room = parseRoom(zone);
	return room === undefined ? undefined : { x: room[0] * ROOM_SIZE + x, y: room[1] * ROOM_SIZE + y };
}

/** World position (m) of a rendered entity (every Mob Pit kind has local `x` / `y`). */
export function worldOf(entity: RenderedEntity): { x: number; y: number } | undefined {
	const { x, y } = entity.fields as { x?: unknown; y?: unknown };
	return typeof x === 'number' && typeof y === 'number' ? toWorldXY(entity.zoneId, x, y) : undefined;
}

/**
 * The own player's predicted fields re-expressed in the next room's frame
 * (the same world point): lets the SDK keep predicting through a doorway
 * handoff instead of holding the player still (`handoffFrame`).
 */
export function pitHandoffFrame(fields: EntityFields, from: string, to: string): EntityFields | undefined {
	const a = parseRoom(from);
	const b = parseRoom(to);
	const { x, y } = fields as { x?: unknown; y?: unknown };
	if (a === undefined || b === undefined || typeof x !== 'number' || typeof y !== 'number') return undefined;
	return { ...fields, x: x + (a[0] - b[0]) * ROOM_SIZE, y: y + (a[1] - b[1]) * ROOM_SIZE };
}

/** `[x, y]` → `pit.<x>.<y>`. */
export function roomZone(x: number, y: number): string {
	return `pit.${x}.${y}`;
}

/** The 8 rooms around `zone`. */
export function neighbourRooms(zone: string): string[] {
	const at = parseRoom(zone);
	if (at === undefined) return [];
	const out: string[] = [];
	for (let dy = -1; dy <= 1; dy += 1) {
		for (let dx = -1; dx <= 1; dx += 1) if (dx !== 0 || dy !== 0) out.push(roomZone(at[0] + dx, at[1] + dy));
	}
	return out;
}


/**
 * WebSocket URL of a room. `base` is the http(s) origin rooms are reached
 * at (celld, in Node tests); in the browser the page's own origin (Vite
 * proxies `/rooms` to celld).
 */
export function roomUrl(zone: string, base?: string): string {
	const origin = base ?? globalThis.location.origin;
	return `${origin.replace(/^http/, 'ws')}/rooms/${encodeURIComponent(zone)}`;
}
/** `state` names of the `mob` kind, by value (generated from the Rust `MOB_STATES`). */
export const MOB_STATE = pit.PIT_RULES.states;
/** Presentation of each tier (client only). */
const TIER_LOOK: Readonly<Record<number, { name: string; color: string }>> = {
	1: { name: 'Grunt', color: '#9ad16b' },
	2: { name: 'Brute', color: '#b48cf2' },
	3: { name: 'Elite', color: '#ff6b6b' }
};
/**
 * Per tier: display name and color (client presentation), body and
 * telegraph radius (the server's `tier_stats`, generated `PIT_RULES`).
 */
export const TIERS: Readonly<Record<number, { name: string; radius: number; tele: number; color: string }>> = Object.fromEntries(
	pit.PIT_RULES.tiers.map((t) => [t.tier, { ...(TIER_LOOK[t.tier] ?? { name: `Tier ${t.tier}`, color: '#9ad16b' }), radius: t.radius, tele: t.tele }])
);

/** One leaderboard row (`/dev/mobpit/leaderboard`). */
export type BoardRow = Readonly<{ rank: number; member: string; name: string; score: number }>;
/** The leaderboard snapshot the services keep (top-N, store, rebuilds). */
export type BoardSnapshot = Readonly<{
	version: number;
	backend: string;
	members: number;
	kills: BoardRow[];
	xp: BoardRow[];
	background: { enqueued: number; applied: number; dropped: number; failed: number };
	lastRebuild: RebuildReport | null;
}>;
export type RebuildReport = Readonly<{
	backend: string;
	crawlers: number;
	events: number;
	applied: number;
	duplicates: number;
	identicalTop: boolean;
	matchesCells: boolean;
	mismatches: { member: string; cell: { kills: number; xp: number }; board: { kills: number; xp: number } }[];
	tookMs: number;
}>;
/** `/dev/mobpit/leaderboard` neighbourhood. */
export type BoardPosition = Readonly<{ rank: number | null; score: number | null; around: BoardRow[] }>;

/** A room as the `zone_status` read model knows it (`/dev/mobpit/rooms`). */
export type RoomStatus = Readonly<{
	zone: string;
	x: number;
	y: number;
	/** `active` · `dormant` · `hibernated` (unknown rooms are cold). */
	status: 'active' | 'dormant' | 'hibernated';
	players: number;
	/** The current activation restored a snapshot. */
	restored: boolean;
	activations: number;
	/** celld node of the activation — a debug detail (empty when unknown). */
	node: string;
	changedAtMs: number;
}>;

/** A room object's debug JSON (`/rooms/{room}/info`). */
export type RoomInfo = Readonly<{
	zone: string;
	status: string;
	awake: boolean;
	activation: string;
	restored: boolean;
	wakes: number;
	node: string;
	isolate: string;
	players: number;
	tick: number;
	hz: number;
	lateMs: { p50: number; p99: number };
	droppedTicks: number;
}>;

export type MobPitClient = pit.MobpitClient &
	Readonly<{
		hello(): pit.HelloEvent | undefined;
		profile(): pit.Profile | undefined;
		/** The room the player is in (authority zone). */
		room(): string | undefined;
		/** The own player as rendered now (predicted; held through a handoff). */
		me(): RenderedEntity | undefined;
		/** World position (m) of the own player as rendered now. */
		position(): { x: number; y: number } | undefined;
		/** Kills by me whose fact has not settled yet (key → event). */
		unsaved: ReadonlyMap<string, pit.KillEvent>;
		/** The rooms' generated contents (the Rust generator as WASM). */
		pures: ReturnType<typeof pit.createMobpitPures>;
		/** Resolves on the first hello, once the predicted step is loaded. */
		ready: Promise<pit.HelloEvent>;
		/** The neighbour rooms watched (`watchZones` over `neighbourRooms`), when enabled. */
		neighbours(): ReadonlyMap<string, WatchState>;
		/** Stop watching neighbours and close the client. */
		close(): void;
	}>;

export type MobPitClientOptions = Readonly<{
	api: DevApi;
	/** http(s) origin of the rooms (celld); default the page's origin. */
	roomsBase?: string;
	socket?: GameSocketFactory;
	/** Watch the 8 neighbour rooms (default true). */
	neighbours?: boolean;
}>;

export function createMobPitClient(options: MobPitClientOptions): MobPitClient {
	const typed = pit.createMobpitClient({
		url: (zone) => roomUrl(zone, options.roomsBase),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		inputDtMs: 1000 / 30,
		// Prediction: the generated default, `pit_step` compiled to WASM,
		// carried on through doorways in the next room's frame.
		prediction: { handoffFrame: pitHandoffFrame }
	});
	const hello = latest<pit.HelloEvent>(
		(h) => typed.on('hello', h),
		(value) => typed.client.setControlledEntity(value.entity)
	);
	const profile = latest<pit.Profile>((h) => typed.on('profile', h));
	const unsaved = new Map<string, pit.KillEvent>();
	typed.on('kill', (kill, zoneId) => {
		// Entity ids are per room: only kills in your own room can be yours.
		if (zoneId === typed.client.authority?.zoneId && kill.killer === hello.get()?.entity) unsaved.set(kill.key, kill);
	});
	typed.on('saved', (saved) => unsaved.delete(saved.key));
	const pures = pit.createMobpitPures();
	// The own player: the predicted entity, or (mid-handoff) the SDK's
	// bridge, the last predicted state in the room just left.
	const me = () => {
		const id = hello.get()?.entity;
		// Never a watched neighbour's player (entity ids are per room).
		const rendered = typed.client.render().filter((e) => pit.isPlayer(e) && typed.client.zone(e.zoneId)?.role !== 'observer');
		return rendered.find((e) => e.predicted) ?? rendered.find((e) => e.id === id && e.zoneId === typed.client.authority?.zoneId) ?? rendered.find((e) => e.id === id);
	};
	const watcher = options.neighbours === false ? undefined : watchZones(typed.client, neighbourRooms);
	return {
		...typed,
		hello: hello.get,
		profile: profile.get,
		room: () => typed.client.authority?.zoneId,
		me,
		position: () => {
			const entity = me();
			return entity === undefined ? undefined : worldOf(entity);
		},
		unsaved,
		pures,
		ready: Promise.all([hello.ready, typed.prediction?.ready, pures.ready]).then(([h]) => h),
		neighbours: () => watcher?.states() ?? new Map(),
		close: () => {
			watcher?.stop();
			typed.client.close();
		}
	};
}

/** Polls the services' leaderboard snapshot (the board is a projection of the cells). */
export function createBoardWatcher(pitApi: MobPitApi, everyMs = 500): Readonly<{
	board(): BoardSnapshot | undefined;
	on(handler: (value: BoardSnapshot) => void): () => void;
	stop(): void;
}> {
	let current: BoardSnapshot | undefined;
	const handlers = new Set<(value: BoardSnapshot) => void>();
	let stopped = false;
	const poll = async () => {
		while (!stopped) {
			try {
				const { snapshot } = await pitApi.leaderboard();
				if (current === undefined || snapshot.version !== current.version) {
					current = snapshot;
					for (const h of handlers) h(snapshot);
				}
			} catch {
				// Services restarting: try again.
			}
			await new Promise((r) => setTimeout(r, everyMs));
		}
	};
	void poll();
	return {
		board: () => current,
		on: (handler) => {
			handlers.add(handler);
			return () => handlers.delete(handler);
		},
		stop: () => {
			stopped = true;
		}
	};
}

/** `/dev/mobpit/*` (dev only; none of these take an identity or a reward). */
export class MobPitApi {
	readonly base: string;
	readonly roomsBase: string;

	/** `base`: the services' origin; `roomsBase`: celld's (both default to the page's). */
	constructor(base = '', roomsBase = base) {
		this.base = base;
		this.roomsBase = roomsBase;
	}

	/**
	 * The room a returning guest starts in: its last room (presence cell,
	 * verified on the server from the guest token), else the entrance.
	 */
	async startRoom(guest: string | undefined): Promise<string> {
		const { zone } = await this.#json<{ zone: string; resumed: boolean }>('/dev/mobpit/start', guest === undefined ? {} : { guest });
		return parseRoom(zone) === undefined ? DEFAULT_ZONE : zone;
	}

	/** Every room the zone status read model knows. */
	rooms(): Promise<{ rooms: RoomStatus[] }> {
		return this.#json('/dev/mobpit/rooms');
	}

	/** A room object's debug JSON, straight from the room on celld (does not wake it). */
	async roomInfo(zone: string): Promise<RoomInfo> {
		const response = await fetch(`${this.roomsBase}/rooms/${encodeURIComponent(zone)}/info`);
		if (!response.ok) throw new Error(`room info ${zone}: HTTP ${response.status}`);
		return (await response.json()) as RoomInfo;
	}

	/** The player's presence cell. */
	presence(principal: string): Promise<{ presence: { zone: string; status: string } | null }> {
		return this.#json(`/dev/mobpit/presence?principal=${encodeURIComponent(principal)}`);
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
