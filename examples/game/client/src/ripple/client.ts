/**
 * Boss Ripple networking without rendering (Node-testable):
 *
 * - the player's authoritative zone is one neighborhood (predicted with
 *   the Arena movement step; the boss stands on the central pillar);
 * - every other zone of the hierarchy is observed (realm, city, boroughs
 *   and the other hoods), so the page shows each tier's live state and
 *   receives what upper tiers broadcast: ripple events, the city's
 *   weather, the realm's banner.
 */
import { GameClient, type GameSocketFactory } from '@hops-ops/distributed/game';

import { encodeMoveInput, predictPlayer } from '../arena/movement.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { onJsonEvent } from '../shared/events.ts';

export const REALM = 'realm';
export const CITY = 'realm.city';
export const BOROUGHS = ['realm.city.borough-1', 'realm.city.borough-2'] as const;
export const HOODS_PER_BOROUGH = 3;
export const DEFAULT_ZONE = 'realm.city.borough-1.hood-1';
/** Event type ids (server `ripple::EVENT_*`). */
export const EVENT = { hello: 1, ripple: 2, weather: 3, banner: 4, boss: 5 } as const;
export const ACTION_ATTACK = 1;
export const ACTION_SMITE = 2;
export const WEATHER = ['storm', 'rain', 'clearing', 'clear'] as const;

export type HoodInput = Readonly<{ mx: number; my: number; buttons: number; action: number }>;
export type TierName = 'realm' | 'city' | 'borough' | 'hood';

export type RippleHello = Readonly<{
	zone: string;
	tier: TierName;
	parent: string | null;
	children: string[];
	host: string;
	tickRate: number;
	entity: number | null;
	weather: Readonly<{ weather: string; remaining: number }> | null;
	banner: string;
}>;

export type RippleEvent = Readonly<{ at: string; kind: string; from: string; origin: string; originTick: number; tick: number }>;

/** Live state of one tier, from its replicated `tier` (and `boss`) entity. */
export type TierView = Readonly<{
	zone: string;
	hello: RippleHello | undefined;
	connected: boolean;
	defeated: number;
	total: number;
	cleared: number;
	liberated: boolean;
	weather: string;
	clock: number;
	events: number;
	boss?: Readonly<{ hp: number; max: number; by: string }>;
}>;

export function hoods(borough: string): string[] {
	return Array.from({ length: HOODS_PER_BOROUGH }, (_, i) => `${borough}.hood-${i + 1}`);
}

/** Every zone, root first (tree order). */
export function allZones(): string[] {
	const out = [REALM, CITY];
	for (const b of BOROUGHS) out.push(b, ...hoods(b));
	return out;
}

export function tierOf(zone: string): TierName | undefined {
	if (zone === REALM) return 'realm';
	if (zone === CITY) return 'city';
	if ((BOROUGHS as readonly string[]).includes(zone)) return 'borough';
	return BOROUGHS.some((b) => hoods(b).includes(zone)) ? 'hood' : undefined;
}

export function encodeHoodInput(input: HoodInput): Uint8Array {
	const move = encodeMoveInput(input);
	return new Uint8Array([move[0]!, move[1]!, move[2]!, input.action & 0xff]);
}

export type RippleClientOptions = Readonly<{
	api: DevApi;
	base?: string;
	socket?: GameSocketFactory;
	/** Observe every other zone of the hierarchy (default true). */
	observeAll?: boolean;
}>;

export type RippleClient = Readonly<{
	client: GameClient<HoodInput>;
	/** Resolves on the first hello of the player's hood. */
	ready: Promise<RippleHello>;
	tier(zone: string): TierView;
	/** Ripple events received (newest last, bounded). */
	ripples(): readonly RippleEvent[];
	weather(): string | undefined;
	banner(): string | undefined;
	onRipple(listener: (event: RippleEvent) => void): () => void;
	onBanner(listener: (text: string) => void): () => void;
	onWeather(listener: (weather: string) => void): () => void;
	onBoss(listener: (zone: string, event: { kind: string; by?: string }) => void): () => void;
	/** Join `hood` as the player's zone (switches authority). */
	enter(hood: string): Promise<unknown>;
	/** One input with the smite bit (demo shortcut). */
	smite(): void;
	close(): void;
}>;

export function createRippleClient(options: RippleClientOptions): RippleClient {
	const hellos = new Map<string, RippleHello>();
	const ripples: RippleEvent[] = [];
	let weather: string | undefined;
	let banner: string | undefined;
	const listeners = {
		ripple: new Set<(e: RippleEvent) => void>(),
		banner: new Set<(t: string) => void>(),
		weather: new Set<(w: string) => void>(),
		boss: new Set<(z: string, e: { kind: string; by?: string }) => void>()
	};
	const ease = { mode: 'ease', durationMs: 150 } as const;
	const snap = { mode: 'snap' } as const;
	const client = new GameClient<HoodInput>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 1000,
		inputDtMs: 1000 / 30,
		prediction: {
			encodeInput: (input) => encodeHoodInput(input),
			predict: (fields, input) => predictPlayer(fields, input),
			corrections: { player: { x: ease, y: ease, vx: snap, vy: snap, dash: snap, cd: snap, name: { mode: 'replace' } } }
		}
	});
	let resolveReady: (h: RippleHello) => void = () => undefined;
	const ready = new Promise<RippleHello>((resolve) => (resolveReady = resolve));

	onJsonEvent<RippleHello>(client, EVENT.hello, (hello, zone) => {
		hellos.set(zone, hello);
		if (hello.weather !== null) weather = hello.weather.weather;
		if (hello.tier === 'realm' && hello.banner !== '') banner = hello.banner;
		if (hello.entity !== null && zone === client.authority?.zoneId) {
			client.setControlledEntity(hello.entity);
			resolveReady(hello);
		}
	});
	onJsonEvent<RippleEvent>(client, EVENT.ripple, (event) => {
		ripples.push(event);
		if (ripples.length > 200) ripples.shift();
		for (const l of listeners.ripple) l(event);
	});
	onJsonEvent<{ weather: string }>(client, EVENT.weather, (value) => {
		weather = value.weather;
		for (const l of listeners.weather) l(value.weather);
	});
	onJsonEvent<{ text: string }>(client, EVENT.banner, (value) => {
		banner = value.text;
		for (const l of listeners.banner) l(value.text);
	});
	onJsonEvent<{ kind: string; by?: string }>(client, EVENT.boss, (value, zone) => {
		for (const l of listeners.boss) l(zone, value);
	});

	const observeOthers = () => {
		if (options.observeAll === false) return;
		const authority = client.authority?.zoneId;
		for (const zone of allZones()) {
			if (zone === authority || client.zone(zone)?.role === 'observer') continue;
			client.observe(zone).catch(() => undefined);
		}
	};
	client.on('authority', observeOthers);

	const on = <T>(set: Set<T>, l: T) => {
		set.add(l);
		return () => set.delete(l);
	};
	const n = (v: unknown) => (typeof v === 'number' ? v : typeof v === 'boolean' ? Number(v) : 0);

	return {
		client,
		ready,
		tier(zone) {
			const view = client.zone(zone);
			const tier = view?.replica.get(1)?.fields ?? {};
			const boss = view?.replica.get(2)?.fields;
			return {
				zone,
				hello: hellos.get(zone),
				connected: view?.connection.state === 'open',
				defeated: n(tier.defeated),
				total: n(tier.total),
				cleared: n(tier.cleared),
				liberated: tier.liberated === true,
				weather: WEATHER[n(tier.weather)] ?? 'storm',
				clock: n(tier.clock),
				events: n(tier.events),
				boss: boss === undefined ? undefined : { hp: n(boss.hp), max: n(boss.max), by: String(boss.by ?? '') }
			};
		},
		ripples: () => ripples,
		weather: () => weather,
		banner: () => banner,
		onRipple: (l) => on(listeners.ripple, l),
		onBanner: (l) => on(listeners.banner, l),
		onWeather: (l) => on(listeners.weather, l),
		onBoss: (l) => on(listeners.boss, l),
		enter: (hood) => client.connect(hood),
		smite() {
			client.sendInput({ mx: 0, my: 0, buttons: 0, action: ACTION_SMITE });
		},
		close: () => client.close()
	};
}
