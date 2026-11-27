/**
 * Headless Mob Pit: dying and respawning (Node, rooms on their own
 * `celld dev`).
 *
 * The player walks up to a mob and lets it win. The room respawns it at a
 * spawn point of the room: the drawn position must cut there in one frame
 * (a respawn is a teleport, not a misprediction to smooth over), and no
 * frame may slide part of the way — that slide is what a player sees as a
 * jump (it was the "~1.5 m jump" walking back into the entrance after a
 * fight: a death in the room being left, eased over 150 ms).
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob, mobFields, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit } from './helpers/mobpit.mjs';

/** One tick's walk at 5 m/s, with slack for a frame that takes two inputs. */
const STEP = 0.5;

test('a respawn cuts to the spawn point in one frame, no slide', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Victim', storage: memoryStorage() });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl, neighbours: false });
		clients.push(pit);
		let died = 0;
		pit.on('died', () => (died += 1));
		await pit.client.connect('pit.1.0');
		await pit.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');

		// Sample what is drawn at 60 Hz while walking into the nearest mob
		// (no swing) until it kills us, then until respawned and settled.
		const frames = [];
		let respawnedAt;
		const deadline = Date.now() + 240_000;
		for (let i = 0; ; i += 1) {
			assert.ok(Date.now() < deadline, 'killed and respawned in time');
			await sleep(1000 / 60);
			const me = pit.me();
			if (me === undefined) continue;
			const p = playerFields(me);
			frames.push({ t: Date.now(), x: p.x, y: p.y, dead: p.dead, died });
			if (respawnedAt === undefined && died > 0 && !p.dead && frames.some((f) => f.dead)) respawnedAt = frames.length - 1;
			if (respawnedAt !== undefined && frames.length - respawnedAt > 60) break;
			if (i % 2 !== 0) continue;
			// One input per tick: towards the nearest mob, never swinging.
			const mob = pit.client
				.render()
				.filter((e) => isMob(e) && !e.stale && e.zoneId === pit.room())
				.map(mobFields)
				.sort((a, b) => Math.hypot(a.x - p.x, a.y - p.y) - Math.hypot(b.x - p.x, b.y - p.y))[0];
			const d = mob === undefined ? 0 : Math.hypot(mob.x - p.x, mob.y - p.y);
			const k = d > 1.2 ? 127 / d : 0;
			pit.client.sendInput(mob === undefined || p.dead ? { mx: 0, my: 0, buttons: 0 } : { mx: Math.round((mob.x - p.x) * k), my: Math.round((mob.y - p.y) * k), buttons: 0 });
		}
		const steps = frames.slice(1).map((f, i) => ({ d: Math.hypot(f.x - frames[i].x, f.y - frames[i].y), dt: f.t - frames[i].t }));
		const big = steps.filter((s) => s.d > STEP);
		console.error(`died ${died}×; frame steps above ${STEP} m: ${JSON.stringify(big.map((s) => +s.d.toFixed(3)))}`);
		assert.ok(big.length >= 1, 'the respawn moved the player');
		assert.equal(big.length, 1, 'one cut to the spawn point, no slide across frames');
		// The cut lands on a spawn point of the room (±4.5, ±4.5 or 0, ±5.5).
		const landed = frames[respawnedAt];
		const spots = [[-4.5, -4.5], [4.5, -4.5], [4.5, 4.5], [-4.5, 4.5], [0, -5.5], [0, 5.5]];
		assert.ok(
			spots.some(([x, y]) => Math.hypot(landed.x - x, landed.y - y) < 1),
			`respawned at a spawn point: ${landed.x.toFixed(2)},${landed.y.toFixed(2)}`
		);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
