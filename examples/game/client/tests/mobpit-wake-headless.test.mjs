/**
 * Headless Mob Pit proximity wake (Node): rooms on their own `celld dev`.
 *
 * "Wake rooms in reach": walking toward a doorway, the room behind it
 * wakes (its watcher becomes *near*) before the player reaches the doorway
 * and stays live; back in the middle of the room it is released after the
 * hysteresis and goes dormant, then to sleep. Standing in a corner keeps
 * at most the two doorway rooms of that corner awake — the corner
 * (diagonal) room is never woken. With the toggle off nothing wakes.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { PIT_WORLD, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit } from './helpers/mobpit.mjs';

/**
 * Walk to room-local `(tx, ty)` (one command frame per tick), steering
 * around the room's obstacles, and record how many rooms were near.
 */
async function walkTo(pit, obstacles, [tx, ty], seen, timeoutMs = 20_000) {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const me = pit.me();
		const p = me && me.zoneId === pit.room() ? playerFields(me) : undefined;
		seen.max = Math.max(seen.max, pit.near().size);
		if (p !== undefined && Math.hypot(tx - p.x, ty - p.y) < 0.4) break;
		if (Date.now() > deadline) throw new Error(`walk to ${tx},${ty} timed out at ${p?.x},${p?.y}`);
		let input = { mx: 0, my: 0, buttons: 0 };
		if (p !== undefined && !p.dead) {
			let dx = tx - p.x;
			let dy = ty - p.y;
			const len = Math.max(Math.hypot(dx, dy), 1e-3);
			dx /= len;
			dy /= len;
			// Push away from obstacles close ahead (slide around them).
			for (const o of obstacles) {
				const ox = p.x - o.x;
				const oy = p.y - o.y;
				const d = Math.hypot(ox, oy);
				const clear = d - o.r - PIT_WORLD.playerRadius;
				if (clear < 1.5 && ox * dx + oy * dy < 0) {
					const k = 0.8 / Math.max(clear, 0.15);
					// Sideways around it, on the side the target is.
					const side = Math.sign(-oy * (tx - o.x) + ox * (ty - o.y)) || 1;
					dx += ((side * -oy) / d) * k + (ox / d) * k * 0.3;
					dy += ((side * ox) / d) * k + (oy / d) * k * 0.3;
				}
			}
			const n = Math.max(Math.hypot(dx, dy), 1e-3);
			const slow = Math.min(1, len / 1.5);
			input = { mx: Math.round((dx / n) * 127 * Math.max(slow, 0.35)), my: Math.round((dy / n) * 127 * Math.max(slow, 0.35)), buttons: 0 };
		}
		pit.client.sendInput(input);
		await sleep(1000 / 30);
	}
	pit.client.sendInput({ mx: 0, my: 0, buttons: 0 });
}

/** Keep sending idle frames for `ms` (the room keeps simulating us). */
async function idle(pit, ms, seen) {
	const end = Date.now() + ms;
	while (Date.now() < end) {
		seen.max = Math.max(seen.max, pit.near().size);
		pit.client.sendInput({ mx: 0, my: 0, buttons: 0 });
		await sleep(1000 / 30);
	}
}

test('proximity wake: doorway rooms in reach wake, release with hysteresis, corners never, toggle off wakes nothing', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const status = async (zone) => (await pitApi.rooms()).rooms.find((r) => r.zone === zone)?.status;
		const pit = createMobPitClient({
			api: new DevApi({ base: server.httpUrl, name: () => 'Waker', storage: memoryStorage() }),
			roomsBase: server.roomsUrl
		});
		clients.push(pit);
		assert.equal(pit.wakeRooms(), true, 'on by default');
		await pit.client.connect('pit.0.0');
		const hello = await pit.ready;
		const obstacles = pit.pures.pitRoom({ seed: hello.seed, x: 0, y: 0 }).obstacles;
		const seen = { max: 0 };
		await until(() => [...pit.neighbours().values()].filter((s) => s === 'asleep').length === 8, 15_000, 'neighbours asleep');

		// The middle of the room: nothing in reach.
		await walkTo(pit, obstacles, [-3, 3], seen);
		await idle(pit, 1000, seen);
		assert.equal(pit.near().size, 0, 'nothing from the middle');

		// Toward the east doorway, short of it (7 m away, still in pit.0.0).
		await walkTo(pit, obstacles, [13, 2.5], seen);
		assert.equal(pit.room(), 'pit.0.0', 'not through the doorway');
		await until(() => pit.near().has('pit.1.0') && pit.near().size === 1, 5_000, 'pit.1.0 in reach');
		await eventually(async () => (await pitApi.roomInfo('pit.1.0')).ticking === true, 15_000, 'pit.1.0 woke');
		await eventually(async () => (await status('pit.1.0')) === 'active', 15_000, 'pit.1.0 active (zone_status)');
		const woken = await pitApi.roomInfo('pit.1.0');
		assert.equal(woken.players, 0, 'kept awake by the near watcher, no player');
		assert.equal(woken.near, 1);
		await until(() => pit.neighbours().get('pit.1.0') === 'watching', 10_000, 'watching pit.1.0');
		// Stays live past the hibernation delay (1.5 s here).
		await idle(pit, 3000, seen);
		assert.equal((await pitApi.roomInfo('pit.1.0')).ticking, true, 'kept awake');
		console.error(`pit.1.0 woke at 7 m from its doorway (reach ${PIT_WORLD.wakeReach} m, release ${PIT_WORLD.wakeRelease} m)`);

		// Back to the middle: released (beyond the hysteresis), dormant, asleep.
		await walkTo(pit, obstacles, [-3, 3], seen);
		await until(() => pit.near().size === 0, 5_000, 'released');
		await eventually(async () => (await status('pit.1.0')) === 'dormant' || (await status('pit.1.0')) === 'hibernated', 15_000, 'pit.1.0 dormant');
		await eventually(async () => (await status('pit.1.0')) === 'hibernated', 15_000, 'pit.1.0 asleep again');

		// A corner: east, then north doorway — both in reach (hysteresis), the
		// corner room pit.1.1 never.
		await walkTo(pit, obstacles, [15, 3], seen);
		await walkTo(pit, obstacles, [5, 6], seen);
		await walkTo(pit, obstacles, [5, 11], seen);
		await until(() => pit.near().has('pit.1.0') && pit.near().has('pit.0.1'), 5_000, 'two doorways in reach');
		await walkTo(pit, obstacles, [17, 17], seen);
		await idle(pit, 2500, seen);
		assert.deepEqual([...pit.near()].sort(), ['pit.0.1', 'pit.1.0'], 'the two doorway rooms of the corner');
		await eventually(async () => (await pitApi.roomInfo('pit.0.1')).ticking === true, 15_000, 'pit.0.1 woke');
		await eventually(async () => (await pitApi.roomInfo('pit.1.0')).ticking === true, 15_000, 'pit.1.0 woke');
		const corner = await pitApi.roomInfo('pit.1.1');
		assert.equal(corner.awake, false, 'the corner room is never woken');
		assert.equal(corner.wakes, 0);
		assert.ok(seen.max <= 2, `at most two rooms near at once (saw ${seen.max})`);

		// Toggle off: both released, they go to sleep; walking up to a doorway
		// wakes nothing.
		pit.setWakeRooms(false);
		assert.equal(pit.near().size, 0);
		await eventually(async () => (await status('pit.1.0')) === 'hibernated' && (await status('pit.0.1')) === 'hibernated', 20_000, 'both asleep with the toggle off');
		const before = (await pitApi.roomInfo('pit.1.0')).wakes;
		await walkTo(pit, obstacles, [15, 3], seen);
		await walkTo(pit, obstacles, [13, 2.5], seen);
		await idle(pit, 3000, seen);
		assert.equal(pit.near().size, 0, 'off: nothing near');
		const after = await pitApi.roomInfo('pit.1.0');
		assert.equal(after.awake, false, 'off: the doorway room stays asleep');
		assert.equal(after.wakes, before);
		assert.equal((await pitApi.roomInfo('pit.1.1')).wakes, 0, 'the corner room never woke');
		console.error(`proximity wake: at most ${seen.max} rooms near at once`);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
