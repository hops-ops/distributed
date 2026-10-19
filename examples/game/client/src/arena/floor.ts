/**
 * Drawing constants of the Arena floor, shared by the pages with an Arena
 * capsule (Arena, Mob Pit, The Freeze, Boss Ripple).
 *
 * Rendering only: movement, collision and the dash run in Rust
 * (`game_sim::arena`), on the server and — compiled to WebAssembly — in the
 * client's prediction. `tests/prediction-wasm.test.mjs` checks these values
 * against the native build so the drawn floor matches the simulated one.
 */

/** Floor half extent (m). */
export const HALF = 20;
/** Capsule radius (m). */
export const PLAYER_RADIUS = 0.5;
/** Dash cooldown in command frames (for the HUD meter). */
export const DASH_COOLDOWN = 45;

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
