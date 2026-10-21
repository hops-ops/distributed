/**
 * Headless Mob Pit: a doorway into a sleeping room right after a fight
 * with the bow (Node, rooms on their own `celld dev`).
 *
 * The player hunts with arrows in first person (every frame carries the
 * aim, some loose an arrow), walks into the next room, waits until the room
 * it left is asleep, and walks back into it. Prediction must stay exact
 * the whole way: no correction of the own player, and the drawn position
 * never moves more than one tick's walk between frames — across both
 * doorways too.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { BUTTON, isMob, mobFields, PIT_RULES, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { largestStep, startPit, walkThroughDoor } from './helpers/mobpit.mjs';

/** One tick's walk (5 m/s at 30 Hz) plus a dash frame's worth of slack. */
const STEP = 0.5;

/** Hunt with the bow for `ms`: chase the nearest mob, aim at it in first person, loose arrows. */
async function bowFight(pit, ms) {
	const deadline = Date.now() + ms;
	let shots = 0;
	while (Date.now() < deadline) {
		await sleep(1000 / 30);
		const me = pit.me();
		if (me === undefined) continue;
		const at = playerFields(me);
		const mob = pit.client
			.render()
			.filter((e) => isMob(e) && !e.stale && e.zoneId === pit.room())
			.map(mobFields)
			.sort((a, b) => Math.hypot(a.x - at.x, a.y - at.y) - Math.hypot(b.x - at.x, b.y - at.y))[0];
		if (mob === undefined || at.dead) {
			pit.client.sendInput({ mx: 0, my: 0, buttons: 0 });
			continue;
		}
		const d = Math.hypot(mob.x - at.x, mob.y - at.y);
		const yaw = Math.atan2(mob.y - at.y, mob.x - at.x);
		const pitch = Math.atan2((PIT_RULES.mobHeights[mob.tier] ?? 1) / 2 - (at.z + PIT_RULES.eyeHeight), d);
		// Keep some distance and shoot; first person faces the aim.
		// (Away from the walls: the fight stays in this room.)
		const inside = Math.abs(at.x) < 14 && Math.abs(at.y) < 14;
		const away = d < 6 ? -1 : d > 10 ? 1 : 0;
		const centre = Math.atan2(-at.y, -at.x);
		const move = inside
			? { mx: Math.round(Math.cos(yaw) * 127 * away), my: Math.round(Math.sin(yaw) * 127 * away) }
			: { mx: Math.round(Math.cos(centre) * 127), my: Math.round(Math.sin(centre) * 127) };
		const shot = pit.aimShot(yaw, pitch, true, true);
		if (shot !== undefined && shots * 300 < ms - (deadline - Date.now())) {
			shots += 1;
			pit.client.sendInput({ ...move, ...shot.input, buttons: shot.input.buttons });
		} else {
			pit.client.sendInput({ ...move, buttons: BUTTON.aim, yaw, pitch: 0, shot: 0, delayMs: 0 });
		}
	}
	return shots;
}

test('a doorway into a sleeping room after a bow fight: no correction, no jump', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Ana', storage: memoryStorage() });
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit);
		const corrections = [];
		pit.client.on('correction', (c) => corrections.push({ room: pit.room(), at: Date.now(), m: Math.hypot(c.errors.x ?? 0, c.errors.y ?? 0), errors: c.errors }));
		await pit.client.connect('pit.0.0');
		await pit.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');

		for (let round = 1; round <= 3; round += 1) {
			const shots = await bowFight(pit, 6_000);
			const before = corrections.length;
			const east = await walkThroughDoor(pit, [1, 0]);
			assert.equal(east.room, 'pit.1.0');
			// The room left goes to sleep (the watcher does not keep it awake).
			await eventually(async () => (await pitApi.roomInfo('pit.0.0')).awake === false, 30_000, 'pit.0.0 asleep');
			const west = await walkThroughDoor(pit, [-1, 0]);
			assert.equal(west.room, 'pit.0.0');
			await sleep(500);
			const fresh = corrections.slice(before).filter((c) => c.m > 0.05);
			console.error(
				`round ${round}: ${shots} arrows; east step ${largestStep(east.trace).toFixed(3)} m, west step ${largestStep(west.trace).toFixed(3)} m; corrections ${JSON.stringify(fresh.map((c) => ({ room: c.room, m: +c.m.toFixed(3) })))}`
			);
			assert.ok(largestStep(east.trace) <= STEP, `east: ${largestStep(east.trace)} m`);
			assert.ok(largestStep(west.trace) <= STEP, `west: ${largestStep(west.trace)} m`);
			assert.deepEqual(fresh, [], 'no correction of the own player across the doorways');
		}
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
