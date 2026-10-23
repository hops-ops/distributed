/**
 * Headless Mob Pit helpers: the rooms and cells on their own `celld dev`,
 * the services (`game-demos mobpit`) on a throwaway data dir, and a
 * scripted hunter that walks to the nearest mob, faces it and swings until
 * `done()` is true.
 */
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BUTTON, isMob, isPlayer, mobFields, playerFields } from '../../src/generated/mobpit/index.ts';
import { freePort, startCelld } from './celld.mjs';
import { sleep, startDemo } from './demo-server.mjs';
import { startFleet } from './fleet.mjs';

/** The services' admin secret in the tests (live content rollouts). */
export const ADMIN_SECRET = 'mobpit-test-admin-secret';

export function tempDataDir() {
	return mkdtempSync(join(tmpdir(), 'mobpit-js-'));
}

/** Rooms go dormant and to sleep fast (tests wait for both). */
const FAST_ROOMS = { ZONE_DORMANT_AFTER_MS: '1000', ZONE_HIBERNATE_AFTER_MS: '1500' };

/**
 * celld (rooms + cells) + `game-demos <demo>` (mobpit / leaderboard: the
 * services) on a fixed port that celld's outbox relay targets.
 * `roomsUrl` is where rooms are reached: the services' gateway, which
 * routes `/zones/{room}` to celld. `restart()` restarts only the
 * services (same port, data dir and cells); `restartCelld()` kills celld
 * and starts it again on its state (rooms, cells); `stop()` stops both.
 */
export async function startPit(demo = 'mobpit', vars = {}) {
	const port = await freePort();
	const celld = await startCelld({ relayPort: port, vars: { ...FAST_ROOMS, ...vars } });
	const env = { ...celld.env, MOBPIT_DATA_DIR: tempDataDir(), DG_DEMO_ADMIN_SECRET: ADMIN_SECRET };
	let server;
	try {
		server = await startDemo(demo, { env, port });
	} catch (error) {
		await celld.stop();
		throw error;
	}
	return {
		celld,
		get roomsUrl() {
			return server.httpUrl;
		},
		get httpUrl() {
			return server.httpUrl;
		},
		async restart() {
			await server.stop();
			server = await startDemo(demo, { env, port });
		},
		restartCelld: () => celld.restart(),
		async stop() {
			await server.stop();
			await celld.stop();
		}
	};
}

/**
 * `startPit` on a local celld fleet (`nodes` nodes on one RustFS bucket,
 * see `fleet.mjs`): rooms are entered through the gateway, which sends
 * them to the first node; celld places them. `redeploy(vars)` deploys the
 * worker again with `vars` over the worker's own (not over the earlier
 * ones). Null when the fleet's tools are missing (docker, aws CLI, celld).
 */
export async function startPitFleet(demo = 'mobpit', vars = {}, nodes = 3) {
	const port = await freePort();
	const fleet = await startFleet({ relayPort: port, nodes, vars: { ...FAST_ROOMS, ...vars } });
	if (fleet === null) return null;
	const env = { ...fleet.env, MOBPIT_DATA_DIR: tempDataDir(), DG_DEMO_ADMIN_SECRET: ADMIN_SECRET };
	let server;
	try {
		server = await startDemo(demo, { env, port });
	} catch (error) {
		await fleet.stop();
		throw error;
	}
	return {
		fleet,
		get roomsUrl() {
			return server.httpUrl;
		},
		get httpUrl() {
			return server.httpUrl;
		},
		redeploy: (more) => fleet.redeploy({ ...FAST_ROOMS, ...more }),
		async stop() {
			await server.stop();
			await fleet.stop();
		}
	};
}

/**
 * Walk to the doorway on side `[dx, dy]` and through it (one command frame
 * per tick) until the client's authority is the next room and the own
 * player is predicted there again. Returns the new room and the own world
 * position sampled every frame on the way (`{ t, room, x, y }`).
 */
export async function walkThroughDoor(pit, [dx, dy], timeoutMs = 30_000) {
	const { client } = pit;
	const from = pit.room();
	const deadline = Date.now() + timeoutMs;
	const trace = [];
	const settled = () => pit.room() !== from && pit.me()?.predicted === true && pit.me()?.zoneId === pit.room();
	for (let after = 0; after < 10; after += settled() ? 1 : 0) {
		if (Date.now() > deadline) throw new Error(`walk through door ${dx},${dy} timed out`);
		const me = pit.me();
		const at = pit.position();
		if (at !== undefined) trace.push({ t: Date.now(), room: me.zoneId, predicted: me.predicted, ...at });
		let input = { mx: 0, my: 0, buttons: 0 };
		if (at !== undefined) {
			// Beside the pillar on the door's axis, through the gap, and on
			// into the next room (world coordinates: the rooms are one world).
			const [rx, ry] = from.split('.').slice(1).map(Number);
			const size = 2 * 20;
			const tx = rx * size + (dx !== 0 ? dx * 24 : 1.7);
			const ty = ry * size + (dy !== 0 ? dy * 24 : 1.7);
			const ddx = tx - at.x;
			const ddy = ty - at.y;
			const len = Math.max(Math.hypot(ddx, ddy), 1e-3);
			input = { mx: Math.round((ddx / len) * 127), my: Math.round((ddy / len) * 127), buttons: 0 };
		}
		client.sendInput(input);
		await sleep(1000 / 30);
	}
	return { room: pit.room(), trace };
}

/** Largest move (m) between consecutive samples of a `walkThroughDoor` trace. */
export function largestStep(trace) {
	let max = 0;
	for (let i = 1; i < trace.length; i += 1) {
		max = Math.max(max, Math.hypot(trace[i].x - trace[i - 1].x, trace[i].y - trace[i - 1].y));
	}
	return max;
}

/** Hunt mobs (one command frame per 30 Hz tick) until `done()` or timeout. */
export async function hunt(pit, done, timeoutMs = 45_000) {
	const { client } = pit;
	const deadline = Date.now() + timeoutMs;
	while (!done()) {
		if (Date.now() > deadline) throw new Error('hunt timed out');
		const rendered = client.render();
		const room = pit.room?.();
		const meEntity = rendered.find((e) => e.id === pit.hello()?.entity && isPlayer(e) && (room === undefined || e.zoneId === room));
		const me = meEntity && playerFields(meEntity);
		const mobs = rendered.filter((e) => isMob(e) && !e.stale && (room === undefined || e.zoneId === room)).map(mobFields);
		let input = { mx: 0, my: 0, buttons: 0 };
		if (me !== undefined && mobs.length > 0 && !me.dead) {
			const target = mobs
				.map((m) => ({ m, d: Math.hypot(m.x - me.x, m.y - me.y) }))
				.sort((a, b) => a.d - b.d)[0];
			const dx = target.m.x - me.x;
			const dy = target.m.y - me.y;
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			input = { mx: Math.round(dx * k), my: Math.round(dy * k), buttons: near ? BUTTON.attack : 0 };
		}
		client.sendInput(input);
		await sleep(1000 / 30);
	}
}
