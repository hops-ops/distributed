/**
 * Shared deterministic movement step for the Arena demo.
 *
 * **Twin of `examples/game/server/src/arena/movement.rs`** — the server runs
 * that one, the client predicts its own capsule with this one. They must
 * produce identical numbers: every operation is rounded to binary32 with
 * `f()` (= `Math.fround`), only `+ - * /` and `sqrt` are used, one input is
 * one command frame of exactly `DT`, and positions/velocities are snapped
 * to their wire quantization at the end of each step.
 *
 * `tests/movement-parity.test.mjs` replays the Rust-generated fixture
 * `examples/game/fixtures/arena-movement.json` through this file.
 *
 * No imports except the SDK quantizers so Node can run it directly.
 */
import { dequantizeLinear, quantizeLinear, type EntityFields } from '@hops-ops/distributed/game';

const f = Math.fround;

export const DT = f(1 / 30);
export const HALF = 20;
export const PLAYER_RADIUS = 0.5;
export const MAX_SPEED = 5;
export const ACCEL = 40;
export const DASH_SPEED = 14;
export const DASH_FRAMES = 6;
export const DASH_COOLDOWN = 45;
export const BUTTON_DASH = 1;

/** Static circular pillars `[x, y, radius]`. */
export const PILLARS: readonly (readonly [number, number, number])[] = [
	[0, 0, 2.5],
	[-9, -9, 1.5],
	[9, -9, 1.5],
	[-9, 9, 1.5],
	[9, 9, 1.5],
	[0, -14, 1],
	[0, 14, 1],
	[-14, 0, 1],
	[14, 0, 1]
];

export type Quant = Readonly<{ min: number; max: number; bits: number }>;

/** `FieldType::quant_centered(range, bits)`: zero is on the grid. */
export function quantCentered(range: number, bits: number): Quant {
	return { min: -range, max: f(range - range / 2 ** (bits - 1)), bits };
}

/** Wire quantization of `x`/`y` and `vx`/`vy` (must match the Rust schema). */
export const POS = quantCentered(HALF, 18);
export const VEL = quantCentered(16, 12);

/** One command frame: stick per axis in [-127, 127] and button bits. */
export type MoveInput = Readonly<{ mx: number; my: number; buttons: number }>;

export type MoveState = Readonly<{
	x: number;
	y: number;
	vx: number;
	vy: number;
	dash: number;
	cooldown: number;
}>;

/** Wire payload `[mx, my, buttons]` (as the server decodes it). */
export function encodeMoveInput(input: MoveInput): Uint8Array {
	return new Uint8Array([input.mx & 0xff, input.my & 0xff, input.buttons & 0xff]);
}

/** The server's sanitization: -128 → -127, unknown buttons cleared. */
export function sanitizeMoveInput(input: MoveInput): MoveInput {
	const axis = (v: number) => Math.max(-127, Math.min(127, Math.trunc(v)));
	return { mx: axis(input.mx), my: axis(input.my), buttons: input.buttons & BUTTON_DASH };
}

function snap(q: Quant, v: number): number {
	return dequantizeLinear(quantizeLinear(v, q.min, q.max, q.bits), q.min, q.max, q.bits);
}

/** Advance one command frame (see the module docs). */
export function step(state: MoveState, raw: MoveInput): MoveState {
	const input = sanitizeMoveInput(raw);
	let { x, y, vx, vy, dash, cooldown } = state;
	let dx = f(input.mx / 127);
	let dy = f(input.my / 127);
	const l2 = f(f(dx * dx) + f(dy * dy));
	if (l2 > 1) {
		const inv = f(1 / f(Math.sqrt(l2)));
		dx = f(dx * inv);
		dy = f(dy * inv);
	}

	cooldown = Math.max(0, cooldown - 1);
	if ((input.buttons & BUTTON_DASH) !== 0 && cooldown === 0 && dash === 0) {
		const v2 = f(f(vx * vx) + f(vy * vy));
		let dir: [number, number] | undefined;
		if (l2 > f(0.01)) {
			const l = f(Math.sqrt(f(f(dx * dx) + f(dy * dy))));
			dir = [f(dx / l), f(dy / l)];
		} else if (v2 > f(0.01)) {
			const l = f(Math.sqrt(v2));
			dir = [f(vx / l), f(vy / l)];
		}
		if (dir !== undefined) {
			vx = f(dir[0] * DASH_SPEED);
			vy = f(dir[1] * DASH_SPEED);
			dash = DASH_FRAMES;
			cooldown = DASH_COOLDOWN;
		}
	}

	if (dash > 0) {
		dash -= 1;
	} else {
		const ex = f(f(dx * MAX_SPEED) - vx);
		const ey = f(f(dy * MAX_SPEED) - vy);
		const e2 = f(f(ex * ex) + f(ey * ey));
		const maxDv = f(ACCEL * DT);
		if (e2 > f(maxDv * maxDv)) {
			const k = f(maxDv / f(Math.sqrt(e2)));
			vx = f(vx + f(ex * k));
			vy = f(vy + f(ey * k));
		} else {
			vx = f(vx + ex);
			vy = f(vy + ey);
		}
	}

	x = f(x + f(vx * DT));
	y = f(y + f(vy * DT));

	for (const [px, py, pr] of PILLARS) {
		const ox = f(x - px);
		const oy = f(y - py);
		const d2 = f(f(ox * ox) + f(oy * oy));
		const min = f(PLAYER_RADIUS + pr);
		if (d2 < f(min * min)) {
			const d = f(Math.sqrt(d2));
			const [nx, ny] = d > f(1e-6) ? [f(ox / d), f(oy / d)] : [1, 0];
			x = f(px + f(nx * min));
			y = f(py + f(ny * min));
			const vn = f(f(vx * nx) + f(vy * ny));
			if (vn < 0) {
				vx = f(vx - f(vn * nx));
				vy = f(vy - f(vn * ny));
			}
		}
	}

	const lim = f(HALF - PLAYER_RADIUS);
	if (x < -lim) {
		x = -lim;
		vx = Math.max(vx, 0);
	} else if (x > lim) {
		x = lim;
		vx = Math.min(vx, 0);
	}
	if (y < -lim) {
		y = -lim;
		vy = Math.max(vy, 0);
	} else if (y > lim) {
		y = lim;
		vy = Math.min(vy, 0);
	}
	return {
		x: snap(POS, x),
		y: snap(POS, y),
		vx: snap(VEL, vx),
		vy: snap(VEL, vy),
		dash,
		cooldown
	};
}

/** Replicated `player` fields → state. */
export function stateFromFields(fields: EntityFields): MoveState {
	const n = (name: string) => {
		const v = fields[name];
		return typeof v === 'number' ? v : 0;
	};
	return { x: n('x'), y: n('y'), vx: n('vx'), vy: n('vy'), dash: n('dash'), cooldown: n('cd') };
}

/** `Predictor` `predict` function for the `player` kind. */
export function predictPlayer(fields: EntityFields, input: MoveInput): Record<string, number> {
	const s = step(stateFromFields(fields), input);
	return { x: s.x, y: s.y, vx: s.vx, vy: s.vy, dash: s.dash, cd: s.cooldown };
}
