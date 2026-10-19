/**
 * The Freeze networking without rendering (Node-testable).
 *
 * The client "build" runs one content pack (`pack()`), fetched from the
 * demo's content CDN (`GET /dev/freeze/content/{version}`). What happens
 * at a world barrier:
 *
 * - `Freeze`: `frozen()` holds the notice; inputs are not sent and are
 *   counted in `droppedWhileFrozen` (the server also drops and counts any
 *   that were in flight).
 * - `Resume` with a new version: a normal client patches live (fetches the
 *   pack, `client.setContentVersion`); a **pinned** client (simulated old
 *   build) keeps its pack and is `stale()` until the server makes it
 *   reload — at once in a Hard rollout, or when it enters the hub gate in a
 *   Rolling (safe-room) rollout.
 * - `Kick(RELOAD)` / `VERSION_MISMATCH`: `reloadContent` fetches the
 *   required pack; the SDK reconnects with the reconnect token.
 * - `Resume` with reason `content_update_aborted`: `lastResume().aborted`.
 */
import {
	type GameClient,
	type GameSocketFactory,
	type ResumeMessage,
	type FreezeMessage
} from '@hops-ops/distributed/game';

import * as freeze from '../generated/freeze/index.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';

export const ZONES = { hub: 'freeze-hub', meadow: 'freeze-meadow', ember: 'freeze-ember' } as const;
export const DEFAULT_ZONE = ZONES.hub;

/** Provenance of an event or fact (generated from the Rust `Stamp` record). */
export type Stamp = freeze.Stamp;

export type ZoneLayout = Readonly<{
	title: string;
	gate: boolean;
	portals: readonly Readonly<{ x: number; y: number; to: string; title: string }>[];
	pools: readonly Readonly<{ x: number; y: number; r: number }>[];
	ritual: Readonly<{ x: number; y: number }>;
}>;

export type Pack = Readonly<{
	version: string;
	name: string;
	notes: readonly string[];
	hazard: 'lava' | 'ice';
	slimeSpeed: number;
	slimeColor: string;
	slimeScale: number;
	crystalColor: string;
	crystalLayout: number;
	gemValue: number;
	zones: Readonly<Record<string, ZoneLayout>> & { crystals: readonly (readonly [number, number])[][] };
}>;

export type FreezeHello = freeze.HelloEvent;

export type LogEntry = Readonly<{ at: number; zone: string; kind: string; text: string; stamp?: Stamp }>;

export type ResumeInfo = Readonly<{
	version: string;
	notes: string[];
	aborted: boolean;
	/** Applied live (normal client) vs ignored (pinned client). */
	patched: boolean;
	zone: string;
}>;

/** The content CDN (any version up to the world's current one). */
export async function fetchPack(base: string, version: string): Promise<Pack> {
	const response = await fetch(`${base}/dev/freeze/content/${encodeURIComponent(version)}`);
	if (!response.ok) throw new Error(`content ${version}: HTTP ${response.status}`);
	return (await response.json()) as Pack;
}

/** Admin side of the demo (`x-game-admin-secret`). */
export class FreezeAdmin {
	readonly base: string;
	secret: string;

	constructor(base: string, secret: string) {
		this.base = base;
		this.secret = secret;
	}

	rollout(mode: 'hard' | 'rolling'): Promise<Record<string, unknown>> {
		return this.#post('/dev/freeze/rollout', { mode });
	}

	setStuck(zone: string, stuck: boolean): Promise<Record<string, unknown>> {
		return this.#post('/dev/freeze/stuck', { zone, stuck });
	}

	async world(): Promise<WorldStatus> {
		const response = await fetch(`${this.base}/dev/freeze/world`);
		if (!response.ok) throw new Error(`world: HTTP ${response.status}`);
		return (await response.json()) as WorldStatus;
	}

	async #post(path: string, body: unknown): Promise<Record<string, unknown>> {
		const response = await fetch(`${this.base}${path}`, {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'x-game-admin-secret': this.secret },
			body: JSON.stringify(body)
		});
		const json = (await response.json().catch(() => ({}))) as Record<string, unknown>;
		if (!response.ok) throw new Error(`${path}: HTTP ${response.status} ${String(json.error ?? '')}`);
		return json;
	}
}

export type ZoneStatus = Readonly<{
	zone: string;
	title: string;
	state: 'running' | 'freezing' | 'frozen';
	players: number;
	droppedFrozen: number;
	contentVersion: string;
	pack: string;
	stuck: boolean;
}>;

export type WorldStatus = Readonly<{
	version: string;
	hosts: readonly Readonly<{
		host: string;
		version?: string;
		zones?: readonly ZoneStatus[];
		facts?: readonly Readonly<{ command: string; key: string; stamp: Stamp; input: unknown }>[];
		error?: string;
	}>[];
	rollouts: readonly Record<string, unknown>[];
}>;

export type FreezeClient = Readonly<{
	client: GameClient<freeze.MoveInput>;
	/** Content pack the client renders. */
	pack(): Pack;
	hello(): FreezeHello | undefined;
	/** Resolves on the next hello (each zone sends one), once the predicted step is loaded. */
	nextHello(): Promise<FreezeHello>;
	/** Freeze notice while frozen. */
	frozen(): FreezeMessage | undefined;
	lastResume(): ResumeInfo | undefined;
	/** World content version (last Welcome / Resume of the authority). */
	worldVersion(): string | undefined;
	/** Running older content than the world. */
	stale(): boolean;
	/** Inputs not sent because the zone was frozen. */
	droppedWhileFrozen(): number;
	/** Content reloads forced by the server (zone, version). */
	reloads: { zone: string; version: string }[];
	log: LogEntry[];
	/** Send one command frame (counted when frozen). */
	sendInput(input: freeze.MoveInput): number | undefined;
	onChange(listener: (what: string) => void): () => void;
}>;

export type FreezeClientOptions = Readonly<{
	api: DevApi;
	/** HTTP origin (Node); '' = the page origin. */
	base?: string;
	socket?: GameSocketFactory;
	/** Simulate an old client: never patch content live. */
	pinned?: () => boolean;
}>;

export async function createFreezeClient(options: FreezeClientOptions): Promise<FreezeClient> {
	const base = options.base ?? '';
	const pinned = options.pinned ?? (() => false);
	let pack = await fetchPack(base, 'current');
	const listeners = new Set<(what: string) => void>();
	const changed = (what: string) => {
		for (const listener of listeners) listener(what);
	};
	const reloads: { zone: string; version: string }[] = [];
	const log: LogEntry[] = [];
	const note = (zone: string, kind: string, text: string, stamp?: Stamp) => {
		log.push({ at: Date.now(), zone, kind, text, stamp });
		if (log.length > 50) log.shift();
		changed('log');
	};
	let reloadingZone = '';
	const typed = freeze.createFreezeClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		inputDtMs: 1000 / 30,
		contentVersion: pack.version,
		reloadContent: async (request) => {
			pack = await fetchPack(base, request.version ?? 'current');
			reloads.push({ zone: reloadingZone, version: pack.version });
			note(reloadingZone, 'reload', `server required content ${pack.version}: reloaded "${pack.name}"`);
			changed('pack');
			return pack.version;
		}
		// Prediction: the generated default, `arena_step` compiled to WASM.
	});
	const { client } = typed;
	client.on('reloadRequired', (zone) => (reloadingZone = zone));

	let hello: FreezeHello | undefined;
	let helloWaiters: ((h: FreezeHello) => void)[] = [];
	let frozen: FreezeMessage | undefined;
	let lastResume: ResumeInfo | undefined;
	let worldVersion: string | undefined;
	let dropped = 0;

	typed.on('hello', (value) => {
		hello = value;
		client.setControlledEntity(value.entity);
		const waiters = helloWaiters;
		helloWaiters = [];
		for (const w of waiters) w(value);
		changed('hello');
	});
	client.on('authority', (_zone, welcome) => {
		worldVersion = welcome.contentVersion;
		frozen = undefined;
		changed('welcome');
	});
	client.on('freeze', (zone, message) => {
		if (zone !== client.authority?.zoneId) return;
		frozen = message;
		note(zone, 'freeze', `world paused for update ${message.version}`);
		changed('freeze');
	});
	client.on('resume', (zone, message: ResumeMessage) => {
		if (zone !== client.authority?.zoneId) return;
		frozen = undefined;
		const aborted = message.reason === 'content_update_aborted';
		const notes = message.notes.split('\n').filter((l) => l.length > 0);
		worldVersion = message.version;
		const update = !aborted && message.version !== client.contentVersion;
		const patched = update && !pinned();
		lastResume = { version: message.version, notes, aborted, patched, zone };
		note(zone, aborted ? 'aborted' : 'resume', aborted ? `update aborted (${notes.join(' ')}); still ${message.version}` : `resumed on ${message.version}`);
		changed('resume');
		if (patched) {
			// A client that can patch live: load the pack, present it from now on.
			fetchPack(base, message.version)
				.then((next) => {
					pack = next;
					client.setContentVersion(next.version);
					note(zone, 'patch', `patched live to ${next.version} "${next.name}"`);
					changed('pack');
				})
				.catch((error: unknown) => note(zone, 'error', String(error)));
		}
	});
	typed.on('gem', (v, zone) => note(zone, 'gem', `${v.by} collected a gem (+${v.value})`, v.stamp));
	typed.on('burned', (v, zone) => note(zone, 'burned', `${v.name} stepped into lava`, v.stamp));
	typed.on('content', (v, zone) => note(zone, 'content', `zone switched ${v.from} → ${v.to} "${v.name}"`, v.stamp));

	return {
		client,
		pack: () => pack,
		hello: () => hello,
		nextHello: () =>
			Promise.all([
				new Promise<FreezeHello>((resolve) => helloWaiters.push(resolve)),
				typed.prediction?.ready
			]).then(([h]) => h),
		frozen: () => frozen,
		lastResume: () => lastResume,
		worldVersion: () => worldVersion,
		stale: () => worldVersion !== undefined && worldVersion !== client.contentVersion,
		droppedWhileFrozen: () => dropped,
		reloads,
		log,
		sendInput(input) {
			if (client.authority?.connection.frozen === true) {
				dropped += 1;
				return undefined;
			}
			return client.sendInput(input);
		},
		onChange(listener) {
			listeners.add(listener);
			return () => listeners.delete(listener);
		}
	};
}
