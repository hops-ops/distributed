/**
 * Procedural world *content* of the Endless Grid, for rendering: terrain
 * height, biomes, props, zone ids.
 *
 * **Twin of the generation in `examples/game/server/sim/src/grid.rs`**,
 * bit-identical by construction (`u32` hashing with `Math.imul` / `>>> 0`,
 * binary32 arithmetic with `f()` (= `Math.fround`) after every operation,
 * only `+ - * /`, `sqrt` and `floor`) and checked against the Rust-written
 * fixture `examples/game/fixtures/grid-world.json` by
 * `tests/grid-world-parity.test.mjs`. Serving world content from WASM too
 * would retire this file; it is a separate follow-up.
 *
 * Movement is **not** here: the own walker is predicted by the Rust step
 * compiled to WebAssembly (`grid_step`, which generates the cell's props
 * itself), wired by the generated client (`../generated/grid`).
 */
import { BUTTON } from '../generated/grid/index.ts';

export type { MoveInput } from '../generated/grid/index.ts';

const f = Math.fround;

export const CELL = 32;
export const CONTENT_VERSION = 1;
export const DEFAULT_SEED = 0x00c0ffee;
export const SUB = 4;
export const SUB_SIZE = 8;
export const PROP_MARGIN = 2;
export const PROP_SPAN = 4;
/** Walker radius (m, for drawing; collision is in the WASM step). */
export const PLAYER_RADIUS = f(0.45);
export const BUTTON_SPRINT = BUTTON.sprint;
export const BUTTON_MARK = BUTTON.mark;

export const BIOMES = ['highlands', 'steppe', 'forest', 'meadow'] as const;
const PROP_DENSITY = [0.4375, 0.1875, 0.75, 0.3125];
const SALT_HEIGHT = [1, 2, 3];
const SALT_MOISTURE = 7;
const SALT_PROP = 100;

export const PROP_TREE = 0;
export const PROP_ROCK = 1;
export const PROP_BUSH = 2;

export type Prop = Readonly<{ kind: number; x: number; y: number; r: number; solid: boolean; variant: number }>;
export type Cell = readonly [number, number];

/** lowbias32. */
export function hash32(v: number): number {
	let x = v >>> 0;
	x = (x ^ (x >>> 16)) >>> 0;
	x = Math.imul(x, 0x7feb352d) >>> 0;
	x = (x ^ (x >>> 15)) >>> 0;
	x = Math.imul(x, 0x846ca68b) >>> 0;
	x = (x ^ (x >>> 16)) >>> 0;
	return x;
}

export function mix(seed: number, salt: number, a: number, b: number): number {
	const salted = ((salt >>> 0) + (Math.imul(CONTENT_VERSION, 0x9e3779b9) >>> 0)) >>> 0;
	let h = hash32((seed ^ hash32(salted)) >>> 0);
	h = hash32((h ^ (a >>> 0)) >>> 0);
	return hash32((h + (Math.imul(b | 0, 0x85ebca6b) >>> 0)) >>> 0);
}

/** Top 24 bits in [0, 1) (exact binary32). */
export function unit(h: number): number {
	return (h >>> 8) * (1 / 16777216);
}

function lattice(c: number, l: number, spacing: number): [number, number] {
	const s = spacing;
	if (spacing <= CELL) {
		const per = CELL / spacing;
		const k = Math.max(0, Math.min(per, Math.floor(f(l / s))));
		const frac = f(f(l - k * spacing) / s);
		return [(Math.imul(c, per) + k) | 0, frac];
	}
	const n = spacing / CELL;
	let i = Math.floor(c / n);
	let off = f(f((c - i * n) * CELL) + l);
	if (off >= s) {
		i = (i + 1) | 0;
		off = f(off - s);
	}
	return [i, f(off / s)];
}

function smooth(t: number): number {
	return f(f(t * t) * f(3 - f(2 * t)));
}

function lerp(a: number, b: number, t: number): number {
	return f(a + f(f(b - a) * t));
}

/** Value noise in [0, 1) with lattice `spacing` (m). */
export function noise(seed: number, salt: number, cell: Cell, lx: number, ly: number, spacing: number): number {
	const [ix, fx] = lattice(cell[0], lx, spacing);
	const [iy, fy] = lattice(cell[1], ly, spacing);
	const v = (dx: number, dy: number) => unit(mix(seed, salt, (ix + dx) | 0, (iy + dy) | 0));
	const sx = smooth(fx);
	const sy = smooth(fy);
	return lerp(lerp(v(0, 0), v(1, 0), sx), lerp(v(0, 1), v(1, 1), sx), sy);
}

/** Terrain height in [0, 1). */
export function height(seed: number, cell: Cell, lx: number, ly: number): number {
	const a = f(noise(seed, SALT_HEIGHT[0]!, cell, lx, ly, 64) * 0.5625);
	const b = f(noise(seed, SALT_HEIGHT[1]!, cell, lx, ly, 16) * 0.3125);
	const c = f(noise(seed, SALT_HEIGHT[2]!, cell, lx, ly, 8) * 0.125);
	return f(f(a + b) + c);
}

export function moisture(seed: number, cell: Cell, lx: number, ly: number): number {
	return noise(seed, SALT_MOISTURE, cell, lx, ly, 128);
}

/** Biome index into BIOMES (given height and moisture, or computed). */
export function biome(seed: number, cell: Cell, lx: number, ly: number): number {
	return biomeOf(height(seed, cell, lx, ly), moisture(seed, cell, lx, ly));
}

export function biomeOf(h: number, m: number): number {
	if (h > 0.6875) return 0;
	if (m < 0.375) return 1;
	if (m > 0.625) return 2;
	return 3;
}

/** The props of a cell, sub-cell by sub-cell (row-major). */
export function cellProps(seed: number, cell: Cell): Prop[] {
	const props: Prop[] = [];
	for (let j = 0; j < SUB; j += 1) {
		for (let i = 0; i < SUB; i += 1) {
			const gx = (Math.imul(cell[0], SUB) + i) | 0;
			const gy = (Math.imul(cell[1], SUB) + j) | 0;
			const h0 = mix(seed, SALT_PROP, gx, gy);
			const b = biome(seed, cell, i * SUB_SIZE + SUB_SIZE / 2, j * SUB_SIZE + SUB_SIZE / 2);
			if (unit(h0) >= PROP_DENSITY[b]!) continue;
			const h1 = hash32((h0 ^ 0x68e31da4) >>> 0);
			const h2 = hash32((h1 ^ 0xb5297a4d) >>> 0);
			const h3 = hash32((h2 ^ 0x1b56c4e9) >>> 0);
			const h4 = hash32((h3 ^ 0x2545f491) >>> 0);
			const x = f(f(i * SUB_SIZE + PROP_MARGIN) + f(unit(h1) * PROP_SPAN));
			const y = f(f(j * SUB_SIZE + PROP_MARGIN) + f(unit(h2) * PROP_SPAN));
			const k = unit(h3);
			let kind: number;
			if (b === 0) kind = k < 0.8125 ? PROP_ROCK : PROP_TREE;
			else if (b === 1) kind = k < 0.5 ? PROP_ROCK : PROP_BUSH;
			else if (b === 2) kind = k < 0.8125 ? PROP_TREE : k < 0.875 ? PROP_ROCK : PROP_BUSH;
			else kind = k < 0.375 ? PROP_TREE : k < 0.5625 ? PROP_ROCK : PROP_BUSH;
			const u = unit(h4);
			const r =
				kind === PROP_TREE
					? f(0.5 + f(u * 0.3125))
					: kind === PROP_ROCK
						? f(0.625 + f(u * 0.375))
						: f(0.5 + f(u * 0.25));
			props.push({ kind, x, y, r, solid: kind !== PROP_BUSH, variant: h4 >>> 24 });
		}
	}
	return props;
}

export const MAX_CELL = 1_000_000;

export function zoneId(cell: Cell): string {
	return `grid.${cell[0]}.${cell[1]}`;
}

export function parseZone(zone: string): [number, number] | undefined {
	const m = /^grid\.(-?\d+)\.(-?\d+)$/.exec(zone);
	if (m === null) return undefined;
	const x = Number(m[1]);
	const y = Number(m[2]);
	if (String(x) !== m[1] || String(y) !== m[2] || Math.abs(x) > MAX_CELL || Math.abs(y) > MAX_CELL) return undefined;
	return [x, y];
}
