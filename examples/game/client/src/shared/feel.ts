/**
 * Presentation feel: easing, trauma screen shake and object pools. All of
 * it is drawing only; nothing here touches the predicted simulation.
 */

/** Fast start, gentle stop. */
export const easeOutCubic = (t: number): number => 1 - (1 - t) ** 3;

const BACK = 1.70158;
/** Like easeOutCubic, but overshoots the end a little and settles back. */
export const easeOutBack = (t: number): number =>
  1 + (BACK + 1) * (t - 1) ** 3 + BACK * (t - 1) ** 2;

/** `start`..`end` at `elapsed` of `duration` (clamped), shaped by `ease`. */
export function tween(
  start: number,
  end: number,
  elapsed: number,
  duration: number,
  ease = easeOutCubic,
): number {
  const t = Math.max(0, Math.min(1, elapsed / duration));
  return start + (end - start) * ease(t);
}

/** A fixed pseudo-random gradient in -1..1 at lattice point `i` of channel `seed`. */
function gradient(seed: number, i: number): number {
  let h = Math.imul(i ^ Math.imul(seed, 0x9e3779b1), 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return ((h >>> 0) / 0xffffffff) * 2 - 1;
}

/** 1D Perlin (gradient) noise: smooth, about -1..1, 0 at every integer. */
export function perlin(seed: number, x: number): number {
  const i = Math.floor(x);
  const f = x - i;
  const fade = f * f * f * (f * (f * 6 - 15) + 10);
  const a = gradient(seed, i) * f;
  const b = gradient(seed, i + 1) * (f - 1);
  return 2 * (a + (b - a) * fade);
}

/** The most a shake moves the view: metres sideways and up, radians of roll. */
export type ShakeMax = { x: number; y: number; roll: number };
export type Shake = { x: number; y: number; roll: number };

/** Trauma lost per second. */
const TRAUMA_DECAY = 1;
/** How fast the noise runs (lattice points per second). */
const SHAKE_HZ = 18;

/**
 * Screen shake from a trauma value: hits add to it (capped at 1), it decays
 * linearly, and the shake is trauma² (small hits barely move the view, big
 * ones jolt it) times smooth noise, so it wobbles instead of jittering.
 */
export class Trauma {
  readonly max: ShakeMax;
  #value = 0;
  #time = 0;

  constructor(max: ShakeMax) {
    this.max = max;
  }

  get value(): number {
    return this.#value;
  }

  add(amount: number): void {
    this.#value = Math.min(1, this.#value + amount);
  }

  /** Advance `dtS` seconds; the shake to apply this frame. */
  update(dtS: number): Shake {
    this.#value = Math.max(0, this.#value - TRAUMA_DECAY * dtS);
    this.#time += dtS;
    const s = this.#value * this.#value;
    const t = this.#time * SHAKE_HZ;
    return {
      x: this.max.x * s * perlin(1, t),
      y: this.max.y * s * perlin(2, t),
      roll: this.max.roll * s * perlin(3, t),
    };
  }
}

/**
 * Preallocated objects reused instead of made and thrown away: `take` hands
 * out an inactive one (the caller resets it), `release` puts it back (once;
 * a second release is ignored). Running out makes another and warns, so the
 * peak count can be raised.
 */
export class Pool<T> {
  readonly name: string;
  readonly #make: () => T;
  readonly #sleep: (item: T) => void;
  readonly #free: T[] = [];
  readonly #active = new Set<T>();
  #size = 0;

  constructor(
    name: string,
    peak: number,
    make: () => T,
    sleep: (item: T) => void,
  ) {
    this.name = name;
    this.#make = make;
    this.#sleep = sleep;
    for (let i = 0; i < peak; i += 1) {
      const item = make();
      sleep(item);
      this.#free.push(item);
    }
    this.#size = peak;
  }

  get size(): number {
    return this.#size;
  }

  get active(): number {
    return this.#active.size;
  }

  take(): T {
    let item = this.#free.pop();
    if (item === undefined) {
      this.#size += 1;
      console.warn(`pool ${this.name} ran out: growing to ${this.#size}`);
      item = this.#make();
    }
    this.#active.add(item);
    return item;
  }

  release(item: T): void {
    if (!this.#active.delete(item)) return;
    this.#sleep(item);
    this.#free.push(item);
  }
}
