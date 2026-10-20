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
 * - prediction of the own walker by `grid_step`, the Rust step compiled to
 *   WebAssembly, which collides against the props of the cell named in its
 *   context (world seed + authoritative cell, set here);
 * - world content for rendering — terrain lattice, biomes, props
 *   (`grid_cell`) and point samples (`grid_sample`) — from the Rust world
 *   generation compiled into the same module (`#[game_pure]`); nothing
 *   static comes over the wire and there is no TypeScript twin;
 * - per-zone hello info (host, restored-from-hibernation …) and the
 *   `/dev/grid/map` world map.
 */
import { fieldNumber, type GameClient, type GameSocketFactory, type RenderedEntity } from '@hops-ops/distributed/game';

import * as grid from '../generated/grid/index.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';

export type MoveInput = grid.MoveInput;

/** Static world facts (cell size, biome / prop names, lattice), generated from Rust. */
export const WORLD = grid.GRID_WORLD;
export const CELL = WORLD.cell;
export const BUTTON_SPRINT = grid.BUTTON.sprint;
export const BUTTON_MARK = grid.BUTTON.mark;

export type Cell = readonly [number, number];
/** A cell's terrain lattice and props (`grid_cell`). */
export type CellContent = grid.GridCellContent;
export type Prop = grid.Prop;
/** Terrain at one point (`grid_sample`). */
export type Sample = grid.GridSample;

export const DEFAULT_ZONE = 'grid.0.0';

/** `grid.<x>.<y>` (the zone id naming scheme of `game_sim::grid::zone_id`). */
export function zoneId(cell: Cell): string {
	return `grid.${cell[0]}.${cell[1]}`;
}

/** Inverse of `zoneId` (canonical integers within ±`WORLD.maxCell` only). */
export function parseZone(zone: string): [number, number] | undefined {
	const m = /^grid\.(-?\d+)\.(-?\d+)$/.exec(zone);
	if (m === null) return undefined;
	const x = Number(m[1]);
	const y = Number(m[2]);
	if (String(x) !== m[1] || String(y) !== m[2] || Math.abs(x) > WORLD.maxCell || Math.abs(y) > WORLD.maxCell) return undefined;
	return [x, y];
}

/** A zone's hello (generated from the Rust `GridHello` event). */
export type GridHello = grid.HelloEvent;

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
	/** The compiled `grid_step` (context: seed + authoritative cell). */
	prediction: grid.GridPrediction | undefined;
	/** Cell of the authoritative zone. */
	cell(): Cell | undefined;
	/** Latest hello per zone. */
	info(zone: string): GridHello | undefined;
	/** Seed announced by the zones (falls back to the default). */
	seed(): number;
	/** The compiled world generation (`grid_cell`, `grid_sample`). */
	pures: grid.GridPures;
	/** Terrain lattice and props of a cell (memoized); undefined until the module loaded. */
	content(cell: Cell): CellContent | undefined;
	/** Props of a cell; empty until the module loaded. */
	props(cell: Cell): readonly Prop[];
	/** Terrain at a local point (clamped into the cell); undefined until the module loaded. */
	sample(cell: Cell, x: number, y: number): Sample | undefined;
	/** World position (m) of a rendered entity. */
	worldOf(entity: RenderedEntity): { x: number; y: number } | undefined;
	/** Handoffs completed so far. */
	handoffs(): number;
	/** Resolves on the first hello with an own entity, once the compiled module is loaded. */
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
	const contentCache = new Map<string, CellContent>();
	let seed = WORLD.defaultSeed;
	let authorityCell: Cell | undefined;
	let handoffs = 0;
	const restoredSeen = new Set<string>();
	const restoredListeners = new Set<(hello: GridHello) => void>();

	// Same module URL as the predicted step: one fetch, one instance.
	const pures = grid.createGridPures();
	const content = (cell: Cell): CellContent | undefined => {
		if (!pures.loaded()) return undefined;
		const key = `${seed}:${cell[0]}:${cell[1]}`;
		let cached = contentCache.get(key);
		if (cached === undefined) {
			cached = pures.gridCell({ seed, cx: cell[0], cy: cell[1] });
			if (contentCache.size > 256) contentCache.clear();
			contentCache.set(key, cached);
		}
		return cached;
	};
	const typed = grid.createGridClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 1000,
		inputDtMs: 1000 / 30
		// Prediction: the generated default, `grid_step` compiled to WASM;
		// its context (seed + cell) follows the authority below.
	});
	const { client } = typed;
	const setContext = () => {
		const cell = authorityCell ?? [0, 0];
		typed.prediction?.setContext({ seed, cx: cell[0], cy: cell[1] });
	};

	let resolveReady: (h: GridHello) => void = () => undefined;
	const ready = Promise.all([
		new Promise<GridHello>((resolve) => (resolveReady = resolve)),
		typed.prediction?.ready,
		pures.ready
	]).then(([h]) => h);

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
		setContext();
		syncNeighbours();
	});
	client.on('handoffComplete', () => {
		handoffs += 1;
	});
	typed.on('hello', (hello, zone) => {
		infos.set(zone, hello);
		if (hello.seed !== seed) {
			seed = hello.seed;
			setContext();
		}
		if (hello.entity !== undefined && zone === client.authority?.zoneId) {
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
		prediction: typed.prediction,
		cell: () => authorityCell,
		info: (zone) => infos.get(zone),
		seed: () => seed,
		pures,
		content,
		props: (cell) => content(cell)?.props ?? [],
		sample: (cell, x, y) => (pures.loaded() ? pures.gridSample({ seed, cx: cell[0], cy: cell[1], x, y }) : undefined),
		worldOf(entity) {
			const cell = parseZone(entity.zoneId);
			if (cell === undefined) return undefined;
			// Every grid kind has local x / y.
			return { x: cell[0] * CELL + fieldNumber(entity.fields, 'x'), y: cell[1] * CELL + fieldNumber(entity.fields, 'y') };
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
