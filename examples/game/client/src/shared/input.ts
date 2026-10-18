/**
 * Keyboard + gamepad sampling and a fixed-rate input loop.
 *
 * `InputSampler.sample()` returns a stick in [-1, 1]² (+x right, +y forward,
 * length ≤ 1) and named buttons. `FixedRate` turns render frames into
 * command frames at the server tick rate: each command frame is one input
 * the server simulates for exactly one tick, which is what makes client
 * prediction line up with the authoritative simulation.
 */

export type Sample = Readonly<{
	x: number;
	y: number;
	/** Primary action (dash, fire …): Space / Shift / gamepad A or RB. */
	action: boolean;
	/** Secondary action: E / F / gamepad B or LB. */
	alt: boolean;
}>;

const UP = ['KeyW', 'ArrowUp'];
const DOWN = ['KeyS', 'ArrowDown'];
const LEFT = ['KeyA', 'ArrowLeft'];
const RIGHT = ['KeyD', 'ArrowRight'];
const ACTION = ['Space', 'ShiftLeft', 'ShiftRight'];
const ALT = ['KeyE', 'KeyF'];
const DEADZONE = 0.15;

export class InputSampler {
	readonly #down = new Set<string>();
	readonly #dispose: () => void;

	constructor(target: Window = window) {
		const down = (e: KeyboardEvent) => {
			if (isTyping(e)) return;
			this.#down.add(e.code);
			if ([...UP, ...DOWN, ...LEFT, ...RIGHT, 'Space'].includes(e.code)) e.preventDefault();
		};
		const up = (e: KeyboardEvent) => this.#down.delete(e.code);
		const blur = () => this.#down.clear();
		target.addEventListener('keydown', down);
		target.addEventListener('keyup', up);
		target.addEventListener('blur', blur);
		this.#dispose = () => {
			target.removeEventListener('keydown', down);
			target.removeEventListener('keyup', up);
			target.removeEventListener('blur', blur);
		};
	}

	#any(codes: readonly string[]): boolean {
		return codes.some((c) => this.#down.has(c));
	}

	sample(): Sample {
		let x = (this.#any(RIGHT) ? 1 : 0) - (this.#any(LEFT) ? 1 : 0);
		let y = (this.#any(UP) ? 1 : 0) - (this.#any(DOWN) ? 1 : 0);
		let action = this.#any(ACTION);
		let alt = this.#any(ALT);
		const pad = firstGamepad();
		if (pad !== undefined) {
			const gx = pad.axes[0] ?? 0;
			const gy = -(pad.axes[1] ?? 0);
			if (Math.hypot(gx, gy) > DEADZONE) {
				x = gx;
				y = gy;
			}
			action ||= pad.buttons[0]?.pressed === true || pad.buttons[5]?.pressed === true;
			alt ||= pad.buttons[1]?.pressed === true || pad.buttons[4]?.pressed === true;
		}
		const length = Math.hypot(x, y);
		if (length > 1) {
			x /= length;
			y /= length;
		}
		return { x, y, action, alt };
	}

	dispose(): void {
		this.#dispose();
	}
}

function firstGamepad(): Gamepad | undefined {
	const pads = globalThis.navigator?.getGamepads?.() ?? [];
	for (const pad of pads) if (pad?.connected) return pad;
	return undefined;
}

function isTyping(e: KeyboardEvent): boolean {
	const t = e.target as HTMLElement | null;
	return t !== null && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable);
}

/** Rotate a stick by the camera so "forward" is away from the camera. */
export function cameraRelative(
	stick: Readonly<{ x: number; y: number }>,
	forward: Readonly<{ x: number; y: number }>
): { x: number; y: number } {
	const l = Math.hypot(forward.x, forward.y) || 1;
	const fx = forward.x / l;
	const fy = forward.y / l;
	// right = forward rotated -90° on the ground plane.
	return { x: fy * stick.x + fx * stick.y, y: -fx * stick.x + fy * stick.y };
}

/** Map [-1, 1] to an i8 axis in [-127, 127]. */
export function toAxis(v: number): number {
	return Math.round(Math.max(-1, Math.min(1, v)) * 127);
}

/**
 * Accumulates frame time and fires `step` once per `1000 / hz` ms. Backlog
 * is capped (a hidden tab must not burst dozens of inputs on return).
 */
export class FixedRate {
	readonly intervalMs: number;
	readonly #maxSteps: number;
	#acc = 0;
	#last: number | undefined;

	constructor(hz: number, maxStepsPerFrame = 4) {
		this.intervalMs = 1000 / hz;
		this.#maxSteps = maxStepsPerFrame;
	}

	/** Call every frame with a monotonic time; returns steps fired. */
	advance(now: number, step: () => void): number {
		if (this.#last === undefined) this.#last = now;
		this.#acc += now - this.#last;
		this.#last = now;
		let steps = 0;
		while (this.#acc >= this.intervalMs && steps < this.#maxSteps) {
			this.#acc -= this.intervalMs;
			step();
			steps += 1;
		}
		if (steps === this.#maxSteps) this.#acc = Math.min(this.#acc, this.intervalMs);
		return steps;
	}
}
