/**
 * Endless Grid networking without rendering (Node-testable):
 *
 * - one `GameClient` whose authority is the cell the walker is in; it
 *   follows every Handoff (and the Redirect when the next cell lives on
 *   another host) — the SDK keeps rendering the old cell until the new one
 *   has content;
 * - observer connections to the 8 neighbouring cells, re-synced whenever
 *   the authority changes, so what is across a border is visible before
 *   crossing;
 * - prediction of the own walker with the shared step (`world.ts`) against
 *   the props of the current cell;
 * - per-zone hello info (host, restored-from-hibernation …) and the
 *   `/dev/grid/map` world map.
 */
import { GameClient, type GameSocketFactory, type RenderedEntity } from '@hops-ops/distributed/game';

import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { onJsonEvent } from '../shared/events.ts';
import {
	cellProps,
	encodeMoveInput,
	parseZone,
	step,
	zoneId,
	CELL,
	DEFAULT_SEED,
	type Cell,
	type MoveInput,
	type Prop
} from './world.ts';

export const DEFAULT_ZONE = 'grid.0.0';
/** Event type ids (server `grid::EVENT_*`). */
export const EVENT_HELLO = 1;
export const KIND = { walker: 'walker', critter: 'critter', marker: 'marker' } as const;

export type GridHello = Readonly<{
	zone: string;
	cell: [number, number];
	host: string;
	seed: number;
	contentVersion: number;
	cellSize: number;
	entity: number | null;
	restored: boolean;
	restores: number;
	hibernatedForMs: number | null;
	restoredCritters: number;
	restoredMarkers: number;
	activatedAt: number;
}>;

export type MapCell = Readonly<{
	x: number;
	y: number;
	zone: string;
	owner: string;
	state: 'active' | 'hibernated' | 'cold' | 'unknown';
	tier?: string;
	players?: number;
	observers?: number;
}>;

export type WorldMap = Readonly<{
	host: string;
	hosts: readonly Readonly<{ id: string; wsUrl: string; reachable: boolean; activeTotal: number | null; active: number; hibernated: number }>[];
	cells: readonly MapCell[];
}>;

export type GridClientOptions = Readonly<{
	api: DevApi;
	/** HTTP origin of the first host (Node); default: the page origin. */
	base?: string;
	socket?: GameSocketFactory;
	/** Observe the 8 neighbours (default true). */
	neighbours?: boolean;
}>;

export type GridClient = Readonly<{
	client: GameClient<MoveInput>;
	/** Cell of the authoritative zone. */
	cell(): Cell | undefined;
	/** Latest hello per zone. */
	info(zone: string): GridHello | undefined;
	/** Seed announced by the zones (falls back to the default). */
	seed(): number;
	/** Props of a cell (memoized). */
	props(cell: Cell): readonly Prop[];
	/** World position (m) of a rendered entity. */
	worldOf(entity: RenderedEntity): { x: number; y: number } | undefined;
	/** Handoffs completed so far. */
	handoffs(): number;
	/** Resolves on the first hello with an own entity. */
	ready: Promise<GridHello>;
	/** Called once per restored activation of any zone we see. */
	onRestored(listener: (hello: GridHello) => void): () => void;
	/** Fetch the world map around a cell (`/dev/grid/map`). */
	map(center: Cell, radius: number): Promise<WorldMap>;
	close(): void;
}>;

export function neighbours(cell: Cell): string[] {
	const out: string[] = [];
	for (let dy = -1; dy <= 1; dy += 1) {
		for (let dx = -1; dx <= 1; dx += 1) {
			if (dx !== 0 || dy !== 0) out.push(zoneId([cell[0] + dx, cell[1] + dy]));
		}
	}
	return out;
}

export function createGridClient(options: GridClientOptions): GridClient {
	const infos = new Map<string, GridHello>();
	const propsCache = new Map<string, readonly Prop[]>();
	let seed = DEFAULT_SEED;
	let authorityCell: Cell | undefined;
	let handoffs = 0;
	const restoredSeen = new Set<string>();
	const restoredListeners = new Set<(hello: GridHello) => void>();

	const props = (cell: Cell): readonly Prop[] => {
		const key = `${seed}:${cell[0]}:${cell[1]}`;
		let list = propsCache.get(key);
		if (list === undefined) {
			list = cellProps(seed, cell);
			if (propsCache.size > 256) propsCache.clear();
			propsCache.set(key, list);
		}
		return list;
	};
	const num = (v: unknown) => (typeof v === 'number' ? v : 0);
	const ease = { mode: 'ease', durationMs: 120 } as const;

	const client = new GameClient<MoveInput>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 1000,
		inputDtMs: 1000 / 30,
		prediction: {
			encodeInput: (input) => encodeMoveInput(input),
			predict: (fields, input) => {
				const cell = authorityCell ?? [0, 0];
				const [x, y] = step(num(fields.x), num(fields.y), input, props(cell));
				return { x, y };
			},
			corrections: { [KIND.walker]: { x: ease, y: ease, name: { mode: 'replace' }, hue: { mode: 'replace' } } }
		}
	});

	let resolveReady: (h: GridHello) => void = () => undefined;
	const ready = new Promise<GridHello>((resolve) => (resolveReady = resolve));

	const syncNeighbours = () => {
		if (options.neighbours === false || authorityCell === undefined) return;
		const authority = client.authority?.zoneId;
		const wanted = new Set(neighbours(authorityCell));
		for (const zone of client.zones()) {
			if (zone.role === 'observer' && !wanted.has(zone.zoneId)) client.unobserve(zone.zoneId);
		}
		for (const zone of wanted) {
			if (zone === authority) continue;
			const existing = client.zone(zone);
			if (existing?.role === 'observer') continue;
			client.observe(zone).catch(() => undefined);
		}
	};

	client.on('authority', (zone) => {
		authorityCell = parseZone(zone);
		syncNeighbours();
	});
	client.on('handoffComplete', () => {
		handoffs += 1;
	});
	onJsonEvent<GridHello>(client, EVENT_HELLO, (hello, zone) => {
		infos.set(zone, hello);
		seed = hello.seed;
		if (hello.entity !== null && zone === client.authority?.zoneId) {
			client.setControlledEntity(hello.entity);
			resolveReady(hello);
		}
		const key = `${zone}@${hello.activatedAt}`;
		if (hello.restored && !restoredSeen.has(key)) {
			restoredSeen.add(key);
			for (const listener of restoredListeners) listener(hello);
		}
	});

	return {
		client,
		cell: () => authorityCell,
		info: (zone) => infos.get(zone),
		seed: () => seed,
		props,
		worldOf(entity) {
			const cell = parseZone(entity.zoneId);
			if (cell === undefined) return undefined;
			return { x: cell[0] * CELL + num(entity.fields.x), y: cell[1] * CELL + num(entity.fields.y) };
		},
		handoffs: () => handoffs,
		ready,
		onRestored(listener) {
			restoredListeners.add(listener);
			return () => restoredListeners.delete(listener);
		},
		async map(center, radius) {
			const base = options.base ?? '';
			const response = await fetch(`${base}/dev/grid/map?x=${center[0]}&y=${center[1]}&r=${radius}`);
			if (!response.ok) throw new Error(`/dev/grid/map: HTTP ${response.status}`);
			return (await response.json()) as WorldMap;
		},
		close: () => client.close()
	};
}
