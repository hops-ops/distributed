/**
 * Mob Pit networking without rendering (Node-testable): the generated
 * `createMobpitClient` (the own capsule predicted by `pit_step`, the Rust
 * Arena movement compiled to WebAssembly), the room's typed events folded
 * into a small state object, the rooms' generated contents
 * (`createMobpitPures().pitRoom`, the Rust room generator as WebAssembly),
 * and the `/dev/mobpit/*` routes.
 *
 * Every room (`pit.<x>.<y>`) is a celld Durable Object reached at
 * `/zones/{room}`; celld forwards the socket to whichever node runs it.
 * Walking through a doorway, the room hands the player to the next room
 * (`Handoff`), and the SDK reconnects there with the transfer ticket.
 *
 * One continuous world: room `(x, y)` sits at world `(x, y) × ROOM_SIZE`
 * and entity positions are room-local, so `worldOf` places anything from
 * any room (the doorway arrival is the same world point, see
 * `game_sim::mobpit`).
 *
 * Neighbours: the client also watches the 8 rooms around its own as an
 * observer (`watchZones` over `neighbourRooms`), so their mobs are drawn where they are.
 * The client tells the SDK where the player is (`setPosition`); every room
 * says where it sits in the world (Welcome origin), so each watcher is
 * focused on the player in that room's coordinates, and the room
 * replicates only what is within the player's own view radius around it. On
 * celld an observer is a passive watcher: it keeps no room awake and wakes
 * none (a sleeping room answers `ASLEEP`; it is tried again every few
 * seconds and shows nothing meanwhile).
 *
 * Proximity wake ("Wake rooms in reach", `wake`, default on): rooms connect
 * only through the four mid-wall doorways, so only those four neighbours
 * can be woken. Within `PIT_WORLD.wakeReach` (11 m) of a doorway — from the
 * predicted position — the watcher of the room behind it says it is *near*
 * (`watchZones(...).setNear`): the room wakes and stays live until the
 * player is beyond `wakeRelease` (19 m) of that doorway (`rooms_in_reach`,
 * shared Rust as WASM: at most the two doorways of a corner, nothing from
 * the middle of the room, corner rooms never). The services grant `wake`
 * only on observer tickets for rooms within two doorways of the player's
 * last room. Off: every neighbour is a passive watcher.
 *
 * Live content updates: the page starts on the world's live content
 * version (`/dev/mobpit/content`). An admin rollout freezes every room
 * (`Freeze`, inputs held; the change notes on screen), switches them to the
 * new pack and resumes them (`Resume` with the version). A normal client
 * patches live (`setContentVersion`); a pinned one (`pinContent`, the page's
 * `?old=1`) stays on its pack and is `stale`: the room keeps it from every
 * doorway but the entrance (`stale` event) and reloads it there
 * (`Kick(RELOAD)` → `reloadContent` → back with the new version).
 *
 * Event flow of one kill: `kill` (broadcast, the tick the mob dies — the
 * fact is only *queued*) → `saved` (to the killer, when the durable cell
 * commit settled) → `profile`.
 */
import {
	watchZones,
	type EntityFields,
	type FreezeMessage,
	type GameSocketFactory,
	type RenderedEntity,
	type ResumeMessage,
	type WatchState
} from '@hops-ops/distributed/game';

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

/** The doorway neighbours of `room` behind `near` bits (`rooms_in_reach`: east, west, north, south). */
export function reachRooms(room: readonly [number, number], near: number): string[] {
	const doors: [number, [number, number]][] = [
		[1, [1, 0]],
		[2, [-1, 0]],
		[4, [0, 1]],
		[8, [0, -1]]
	];
	return doors.filter(([bit]) => (near & bit) !== 0).map(([, [dx, dy]]) => roomZone(room[0] + dx, room[1] + dy));
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
 * at (the services' gateway, or celld itself in Node tests); in the browser
 * the page's own origin (Vite proxies `/zones` to the gateway).
 */
export function roomUrl(zone: string, base?: string): string {
	const origin = base ?? globalThis.location.origin;
	return `${origin.replace(/^http/, 'ws')}/zones/${encodeURIComponent(zone)}`;
}
/** `state` names of the `mob` kind, by value (generated from the Rust `MOB_STATES`). */
export const MOB_STATE = pit.PIT_RULES.states;
/** Presentation of each tier (client only). */
const TIER_LOOK: Readonly<Record<number, { name: string; color: string }>> = {
	1: { name: 'Grunt', color: '#9ad16b' },
	2: { name: 'Brute', color: '#b48cf2' },
	3: { name: 'Elite', color: '#ff6b6b' },
	4: { name: 'Boss', color: '#ff2d55' }
};
/**
 * Per tier: display name and color (client presentation), body and
 * telegraph radius (the server's `tier_stats`, generated `PIT_RULES`).
 */
export const TIERS: Readonly<Record<number, { name: string; radius: number; tele: number; color: string }>> = Object.fromEntries(
	pit.PIT_RULES.tiers.map((t) => [t.tier, { ...(TIER_LOOK[t.tier] ?? { name: `Tier ${t.tier}`, color: '#9ad16b' }), radius: t.radius, tele: t.tele }])
);

export type Vec3 = [number, number, number];

/** One ranged shot: what the client saw, then the room's verdict. */
export type ShotRecord = {
	id: number;
	/** Room-local eye position and unit aim (z up). */
	origin: Vec3;
	dir: Vec3;
	zone: string;
	compensated: boolean;
	/** The mob the client saw the arrow hit (undefined: a predicted miss). */
	predicted: number | undefined;
	/** Where it met the predicted mob, or where it stops. */
	point: Vec3;
	delayMs: number;
	verdict?: pit.ShotEvent;
};

/** Ranged numbers for the HUD and tests. */
export type RangedStats = {
	shots: number;
	predictedHits: number;
	confirmedHits: number;
	/** Client and room agree (same mob, or both a miss). */
	agreed: number;
	resolved: number;
	clamped: number;
	lastAgeMs: number;
};

/** Entry distance of a ray (unit `dir`) into a vertical capsule standing on the floor. */
export function rayCapsule(origin: Vec3, dir: Vec3, x: number, y: number, radius: number, height: number): number | undefined {
	// Horizontal circle first, then the height at the entry.
	const flat = Math.hypot(dir[0], dir[1]);
	if (flat < 1e-6) return undefined;
	const ux = dir[0] / flat;
	const uy = dir[1] / flat;
	const ox = x - origin[0];
	const oy = y - origin[1];
	const along = ox * ux + oy * uy;
	const off2 = ox * ox + oy * oy - along * along;
	if (along <= 0 || off2 > radius * radius) return undefined;
	const entry = (along - Math.sqrt(radius * radius - off2)) / flat;
	const z = origin[2] + dir[2] * entry;
	return z >= 0 && z <= height ? entry : undefined;
}

/** Unit aim from yaw (radians from +x) and pitch (up positive). */
export function aimDir(yaw: number, pitch: number): Vec3 {
	const flat = Math.cos(pitch);
	return [Math.cos(yaw) * flat, Math.sin(yaw) * flat, Math.sin(pitch)];
}

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

/** The tiers around a room (`/dev/mobpit/tiers`): hoods, their boroughs, the city. */
export type TiersAround = Readonly<{
	hoods: { hood: string; x: number; y: number; bossRoom: [number, number]; bossDown: boolean; tier: number }[];
	boroughs: { borough: string; x: number; y: number; bossesDown: number; weather: string }[];
	city: { bosses_down: number; boroughs_cleared: number; banner: string } | null;
}>;

/** A room object's debug JSON (`/zones/{room}/info`). */
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
	/** Observers connected, and how many of them keep the room awake (near). */
	observers?: number;
	near?: number;
	/** The room is ticking (active). */
	ticking?: boolean;
	tick: number;
	hz: number;
	lateMs: { p50: number; p99: number };
	droppedTicks: number;
	/** The room's content version and freeze state (awake rooms only). */
	content: { version: string; freeze: 'running' | 'freezing' | 'frozen' } | null;
}>;

/** The pack of a content version (unknown versions draw the built-in one). */
export function contentPack(version: string): pit.ContentPack {
	return pit.CONTENT_PACKS.find((p) => p.version === version) ?? pit.CONTENT_PACKS[0]!;
}

/** What the client knows about live content. */
export type ContentView = Readonly<{
	/** The content version this client runs (its pack). */
	version: string;
	pack: pit.ContentPack;
	/** The world's version (last Welcome / Resume of the own room). */
	worldVersion: string;
	/** Running older content than the world. */
	stale: boolean;
	/** The room's Freeze notice while frozen. */
	frozen: FreezeMessage | undefined;
	lastResume: ResumeMessage | undefined;
	/** The room's last `stale` notice (only the entrance lets you through). */
	staleNotice: pit.StaleEvent | undefined;
	/** Reloads the rooms required (room, version). */
	reloads: readonly { zone: string; version: string }[];
}>;

/** The world's live content as `/dev/mobpit/content` reports it. */
export type ContentInfo = Readonly<{
	world: string;
	state: Readonly<{ version: string; target: string; phase: string; notes: string; since_ms: number; hard: boolean }> | null;
	packs: readonly pit.ContentPack[];
}>;

export type MobPitClient = pit.MobpitClient &
	Readonly<{
		hello(): pit.HelloEvent | undefined;
		/** The tiers above the room (hood, borough, city), latest. */
		world(): pit.WorldTiers | undefined;
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
		/** Ranged shots by id (latest 256). */
		shots: Map<number, ShotRecord>;
		rangedStats: RangedStats;
		/**
		 * Prepare a ranged shot along `yaw`/`pitch` from the own eye: the
		 * predicted hit (ray against the mobs as rendered) and the input
		 * fields to send this frame (shot id, view delay, the fire bits).
		 */
		aimShot(yaw: number, pitch: number, compensated: boolean, firstPerson?: boolean): { record: ShotRecord; input: Pick<pit.PitInput, 'buttons' | 'shot' | 'delayMs' | 'yaw' | 'pitch'> } | undefined;
		/** Resolves with the verdict of shot `id`. */
		verdict(id: number): Promise<pit.ShotEvent>;
		/** The neighbour rooms watched (`watchZones` over `neighbourRooms`), when enabled. */
		neighbours(): ReadonlyMap<string, WatchState>;
		/** The doorway neighbours kept awake now (proximity wake; empty when off). */
		near(): ReadonlySet<string>;
		/** Proximity wake ("Wake rooms in reach") on? */
		wakeRooms(): boolean;
		/** Turn proximity wake on or off (off: every neighbour passive). */
		setWakeRooms(on: boolean): void;
		/** Live content (version, pack, freeze, staleness). */
		content(): ContentView;
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
	/** Proximity wake: keep the doorway neighbours in reach awake (default true). */
	wake?: boolean;
	/** Content version to start on (the world's live one; default ''). */
	contentVersion?: string;
	/** Simulate an old client: never patch content live on Resume. */
	pinContent?: boolean;
	/** The world's live version, for a reload no room named (default ''). */
	liveVersion?: () => Promise<string>;
}>;

export function createMobPitClient(options: MobPitClientOptions): MobPitClient {
	const reloads: { zone: string; version: string }[] = [];
	let typedRef: pit.MobpitClient | undefined;
	const typed = pit.createMobpitClient({
		contentVersion: options.contentVersion ?? '',
		// A room required a reload (the entrance, for a stale client; or a
		// fresh join on old content): "load" the pack of the version it
		// named (the packs are built in), else the world's live one.
		reloadContent: async (request) => {
			const version = request.version ?? (await options.liveVersion?.()) ?? '';
			reloads.push({ zone: typedRef?.client.authority?.zoneId ?? '', version });
			return version;
		},
		url: (zone) => roomUrl(zone, options.roomsBase),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		inputDtMs: 1000 / 30,
		// Prediction: the generated default, `pit_step` compiled to WASM,
		// carried on through doorways in the next room's frame.
		prediction: { handoffFrame: pitHandoffFrame }
	});
	typedRef = typed;
	// ---- live content ----
	let worldVersion = options.contentVersion ?? '';
	let frozen: FreezeMessage | undefined;
	let lastResume: ResumeMessage | undefined;
	let staleNotice: pit.StaleEvent | undefined;
	typed.client.on('authority', (_zone, welcome) => {
		worldVersion = welcome.contentVersion;
		frozen = typed.client.authority?.connection.frozen ? frozen : undefined;
		staleNotice = undefined;
	});
	typed.client.on('freeze', (zone, message) => {
		if (zone === typed.client.authority?.zoneId) frozen = message;
	});
	typed.client.on('resume', (zone, message) => {
		if (zone !== typed.client.authority?.zoneId) return;
		frozen = undefined;
		lastResume = message;
		worldVersion = message.version;
		// Patch live (the packs are built in) unless simulating an old build.
		if (options.pinContent !== true && message.version !== typed.client.contentVersion) typed.client.setContentVersion(message.version);
	});
	typed.on('stale', (notice, zoneId) => {
		if (zoneId === typed.client.authority?.zoneId) staleNotice = notice;
	});
	const content = (): ContentView => {
		const version = typed.client.contentVersion;
		return {
			version,
			pack: contentPack(version),
			worldVersion,
			stale: version !== worldVersion,
			frozen,
			lastResume,
			staleNotice: version !== worldVersion ? staleNotice : undefined,
			reloads
		};
	};
	const hello = latest<pit.HelloEvent>(
		(h) => typed.on('hello', h),
		(value) => typed.client.setControlledEntity(value.entity)
	);
	// The predicted step's context is the room (its seeded obstacles are
	// generated inside the step): follow the authority and the world seed.
	const setRoomContext = () => {
		const room = parseRoom(typed.client.authority?.zoneId ?? '');
		if (room === undefined) return;
		typed.prediction?.setContext({ seed: hello.get()?.seed ?? pit.PIT_WORLD.defaultSeed, x: room[0], y: room[1] });
	};
	typed.client.on('authority', setRoomContext);
	typed.on('hello', setRoomContext);
	const profile = latest<pit.Profile>((h) => typed.on('profile', h));
	// Only the own room's view of the tiers (watched neighbours send theirs too).
	let world: pit.WorldTiers | undefined;
	typed.on('world', (w, zoneId) => {
		if (zoneId === typed.client.authority?.zoneId) world = w;
	});
	const unsaved = new Map<string, pit.KillEvent>();
	typed.on('kill', (kill, zoneId) => {
		// Entity ids are per room: only kills in your own room can be yours.
		if (zoneId === typed.client.authority?.zoneId && kill.killer === hello.get()?.entity) unsaved.set(kill.key, kill);
	});
	typed.on('saved', (saved) => unsaved.delete(saved.key));
	const pures = pit.createMobpitPures();

	// ---- ranged: predicted hits, verdicts ----
	const shots = new Map<number, ShotRecord>();
	const waiters = new Map<number, (v: pit.ShotEvent) => void>();
	const rangedStats: RangedStats = { shots: 0, predictedHits: 0, confirmedHits: 0, agreed: 0, resolved: 0, clamped: 0, lastAgeMs: 0 };
	let nextShot = 0;
	typed.on('shot', (verdict) => {
		const shot = shots.get(verdict.shot);
		if (shot === undefined || shot.verdict !== undefined) return;
		shot.verdict = verdict;
		rangedStats.resolved += 1;
		if (verdict.hit !== undefined) rangedStats.confirmedHits += 1;
		if (verdict.hit === shot.predicted) rangedStats.agreed += 1;
		if (verdict.clamped) rangedStats.clamped += 1;
		rangedStats.lastAgeMs = verdict.ageMs;
		waiters.get(verdict.shot)?.(verdict);
		waiters.delete(verdict.shot);
	});
	const heights = pit.PIT_RULES.mobHeights;
	const range = pit.PIT_RULES.range;
	const aimShot = (yaw: number, pitch: number, compensated: boolean, firstPerson = false) => {
		const zone = typed.client.authority;
		const meNow = me();
		if (zone === undefined || meNow === undefined || !zone.connection.canSendInput()) return undefined;
		const p = pit.playerFields(meNow);
		// First person: from the eye along the look; else level at the hip.
		const origin: Vec3 = [p.x, p.y, p.z + (firstPerson ? pit.PIT_RULES.eyeHeight : pit.PIT_RULES.hipHeight)];
		const dir = aimDir(yaw, firstPerson ? pitch : 0);
		// The view time: the room rewinds to exactly what is drawn.
		const context = typed.client.inputContext(zone)!;
		const renderTime = zone.connection.serverNow() - context.interpolationDelayMs;
		const delayMs = Math.max(0, zone.connection.tickTime(context.clientTick) - renderTime);
		let predicted: number | undefined;
		let best = range;
		for (const e of typed.client.render()) {
			if (!pit.isMob(e) || e.stale || e.zoneId !== zone.zoneId) continue;
			const m = pit.mobFields(e);
			const t = TIERS[m.tier];
			const d = rayCapsule(origin, dir, m.x, m.y, t?.radius ?? 0.5, heights[m.tier] ?? 1);
			if (d !== undefined && d < best) {
				best = d;
				predicted = e.id;
			}
		}
		nextShot = (nextShot % 0xffff) + 1;
		const record: ShotRecord = {
			id: nextShot,
			origin,
			dir,
			zone: zone.zoneId,
			compensated,
			predicted,
			point: [origin[0] + dir[0] * best, origin[1] + dir[1] * best, origin[2] + dir[2] * best],
			delayMs
		};
		shots.set(record.id, record);
		if (shots.size > 256) shots.delete(shots.keys().next().value!);
		rangedStats.shots += 1;
		if (predicted !== undefined) rangedStats.predictedHits += 1;
		const buttons = pit.BUTTON.fire | (compensated ? 0 : pit.BUTTON.noComp) | (firstPerson ? pit.BUTTON.aim : 0);
		return { record, input: { buttons, shot: record.id, delayMs: Math.min(0xffff, Math.round(delayMs)), yaw, pitch } };
	};
	// The own player: the predicted entity, or (mid-handoff) the SDK's
	// bridge, the last predicted state in the room just left.
	const me = () => {
		const id = hello.get()?.entity;
		// Never a watched neighbour's player (entity ids are per room).
		const rendered = typed.client.render().filter((e) => pit.isPlayer(e) && typed.client.zone(e.zoneId)?.role !== 'observer');
		return rendered.find((e) => e.predicted) ?? rendered.find((e) => e.id === id && e.zoneId === typed.client.authority?.zoneId) ?? rendered.find((e) => e.id === id);
	};
	const watcher = options.neighbours === false ? undefined : watchZones(typed.client, neighbourRooms);
	// ---- proximity wake ----
	let wake = options.wake ?? true;
	let reachRoom: string | undefined;
	let reachBits = 0;
	let near: ReadonlySet<string> = new Set();
	const updateReach = () => {
		if (watcher === undefined) return;
		const room = typed.client.authority?.zoneId;
		const at = parseRoom(room ?? '');
		// The hysteresis is per room (a doorway re-frames everything).
		if (room !== reachRoom) {
			reachRoom = room;
			reachBits = 0;
		}
		const meNow = me();
		// Each watched room (placed in the world by its Welcome origin)
		// applies the player's own view around it: its interest radius
		// carries on across the wall.
		if (meNow !== undefined) {
			const p = pit.playerFields(meNow);
			typed.client.setPosition(p.x, p.y, meNow.zoneId);
		}
		let bits = 0;
		if (wake && at !== undefined && meNow !== undefined && meNow.zoneId === room && pures.loaded()) {
			const p = pit.playerFields(meNow);
			bits = pures.roomsInReach({ x: p.x, y: p.y, near: reachBits }).near;
		}
		reachBits = bits;
		const rooms = at === undefined ? [] : reachRooms(at, bits);
		if (rooms.length === near.size && rooms.every((z) => near.has(z))) return;
		near = new Set(rooms);
		watcher.setNear(rooms);
	};
	const reachTimer = setInterval(updateReach, 100);
	// Never keep a Node process alive for this.
	(reachTimer as { unref?: () => void }).unref?.();
	return {
		...typed,
		hello: hello.get,
		world: () => world,
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
		near: () => near,
		wakeRooms: () => wake,
		setWakeRooms(on) {
			wake = on;
			updateReach();
		},
		content,
		shots,
		rangedStats,
		aimShot,
		verdict(id) {
			const known = shots.get(id)?.verdict;
			if (known !== undefined) return Promise.resolve(known);
			return new Promise((resolve) => waiters.set(id, resolve));
		},
		close: () => {
			clearInterval(reachTimer);
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

	/** The hoods around `room`, their boroughs and the city (tier cells). */
	tiers(room: string): Promise<TiersAround> {
		return this.#json(`/dev/mobpit/tiers?room=${encodeURIComponent(room)}`);
	}

	/** Every room the zone status read model knows. */
	rooms(): Promise<{ rooms: RoomStatus[] }> {
		return this.#json('/dev/mobpit/rooms');
	}

	/** A room object's debug JSON, straight from the room on celld (does not wake it). */
	async roomInfo(zone: string): Promise<RoomInfo> {
		const response = await fetch(`${this.roomsBase}/zones/${encodeURIComponent(zone)}/info`);
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

	/** The world's live content and every pack. */
	content(): Promise<ContentInfo> {
		return this.#json('/dev/mobpit/content');
	}

	/**
	 * **Admin** (the services' admin secret): roll `version` out to every
	 * room (freeze → apply → resume with `notes`; `hard` reloads old
	 * clients at once instead of at the entrance).
	 */
	async rollout(secret: string, request: { version: string; notes?: string; hard?: boolean; freezeMs?: number }): Promise<{ version: string; notes: string }> {
		const response = await fetch(`${this.base}/dev/mobpit/rollout`, {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'x-game-admin-secret': secret },
			body: JSON.stringify(request)
		});
		if (!response.ok) throw new Error(`rollout: HTTP ${response.status} ${await response.text()}`);
		return (await response.json()) as { version: string; notes: string };
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
