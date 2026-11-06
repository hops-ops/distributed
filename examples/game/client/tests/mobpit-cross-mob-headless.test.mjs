/**
 * Headless Mob Pit: a mob chasing a player follows them through a doorway
 * into the next room and keeps chasing there (Node, rooms on their own
 * `celld dev`).
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob, mobFields, PIT_RULES, PIT_WORLD, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit, walkThroughDoor, walkTo } from './helpers/mobpit.mjs';

const CHASE = 1;

/** Visitors in `zone`: guest slots, past the room's own mobs (ids 1..=its layout's). */
function guestsIn(pit, zone) {
	const [x, y] = zone.split('.').slice(1).map(Number);
	const own = pit.pures.pitRoom({ seed: PIT_WORLD.defaultSeed, x, y }).mobs.length;
	return mobsIn(pit, zone).filter((m) => Number(m.key.split(':').at(-1)) > own);
}

/** Live mobs of `zone` as `{ key, x, y, state, tier }`. */
function mobsIn(pit, zone) {
	return pit.client
		.render()
		.filter((e) => isMob(e) && !e.stale && e.zoneId === zone)
		.map((e) => ({ key: e.key, ...mobFields(e) }))
		.filter((m) => m.hp > 0);
}

test('a chasing mob follows through a doorway and keeps chasing', { timeout: 300_000 }, async () => {
	const server = await startPit('mobpit');
	const pit = createMobPitClient({ api: new DevApi({ base: server.httpUrl, name: () => 'Ana', storage: memoryStorage() }), roomsBase: server.roomsUrl });
	try {
		await pit.client.connect('pit.0.0');
		await pit.ready;
		await pit.pures.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');
		// Get chased: walk at the nearest grunt until it chases.
		const deadline = Date.now() + 30_000;
		let chaser;
		while (chaser === undefined) {
			if (Date.now() > deadline) throw new Error('no mob chased');
			const at = pit.position();
			const mobs = mobsIn(pit, 'pit.0.0');
			chaser = mobs.find((m) => m.state === CHASE && m.tier === 1);
			const near = mobs.filter((m) => m.tier === 1).sort((a, b) => Math.hypot(a.x - at.x, a.y - at.y) - Math.hypot(b.x - at.x, b.y - at.y))[0];
			if (near !== undefined && at !== undefined) {
				const d = Math.max(Math.hypot(near.x - at.x, near.y - at.y), 1e-3);
				pit.client.sendInput({ mx: Math.round(((near.x - at.x) / d) * 127), my: Math.round(((near.y - at.y) / d) * 127), buttons: 0 });
			}
			await sleep(1000 / 30);
		}
		console.error(`chaser ${chaser.key} at ${chaser.x.toFixed(1)},${chaser.y.toFixed(1)}`);
		const east = await walkThroughDoor(pit, [1, 0]);
		assert.equal(east.room, 'pit.1.0');
		// In pit.1.0: keep moving about near the doorway for 15 s.
		const log = [];
		const start = Date.now();
		while (Date.now() - start < 15_000) {
			const t = Date.now() - start;
			const visitors = guestsIn(pit, 'pit.1.0');
			const home = mobsIn(pit, 'pit.0.0').find((m) => m.key === chaser.key);
			log.push({ t, visitors: visitors.map((m) => `${m.key}@${m.x.toFixed(1)},${m.y.toFixed(1)} s${m.state}`), home: home ? `s${home.state}@${home.x.toFixed(1)},${home.y.toFixed(1)}` : '-' });
			// Kite: a slow loop through the room's middle, the mob behind.
			const at = pit.position();
			if (at !== undefined) {
				const a = t / 4000;
				const [tx, ty] = [40 - 8 + 6 * Math.cos(a), 6 * Math.sin(a)];
				const d = Math.max(Math.hypot(tx - at.x, ty - at.y), 1e-3);
				pit.client.sendInput({ mx: Math.round(((tx - at.x) / d) * 90), my: Math.round(((ty - at.y) / d) * 90), buttons: 0 });
			}
			await sleep(100);
		}
		let last = '';
		for (const l of log) {
			const line = `visitors ${JSON.stringify(l.visitors)} home ${l.home}`;
			if (l.t % 1000 < 100 || line.replace(/@[^ ]+/g, '') !== last) console.error(`${(l.t / 1000).toFixed(1)}s ${line}`);
			last = line.replace(/@[^ ]+/g, '');
		}
		console.error(server.celld.output().split('\n').filter((l) => /zone message|refused/i.test(l)).slice(-20).join('\n'));
		assert.ok(!log.some((l) => l.visitors.length > 0 && l.home !== '-'), 'never in two rooms');
		const seen = log.filter((l) => l.visitors.length > 0);
		assert.ok(seen.length > 0, 'the chaser followed into pit.1.0');
		assert.ok(log.at(-1).visitors.length > 0, 'still chasing after 15 s');
	} finally {
		pit.close();
		await server.stop();
	}
});

test('a mob shot through a doorway comes after the shooter', { timeout: 300_000 }, async () => {
	const server = await startPit('mobpit');
	const pit = createMobPitClient({ api: new DevApi({ base: server.httpUrl, name: () => 'Ana', storage: memoryStorage() }), roomsBase: server.roomsUrl });
	try {
		const beams = [];
		pit.on('beam', (b, zoneId) => beams.push({ zoneId, ...b }));
		await pit.client.connect('pit.0.0');
		await pit.ready;
		await pit.pures.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');
		await walkTo(pit, [], [17, 1.7], undefined, 30_000);
		await until(() => pit.near().has('pit.1.0'), 20_000, 'pit.1.0 watched');
		await sleep(1000);
		// Shoot whichever grunt of pit.1.0 is in line through the doorway gap.
		const deadline = Date.now() + 60_000;
		let target;
		while (target === undefined) {
			if (Date.now() > deadline) throw new Error(`no hit through the doorway (${beams.length} beams)`);
			const at = pit.position();
			const me = playerFields(pit.me());
			const aims = mobsIn(pit, 'pit.1.0')
				.filter((m) => m.tier <= 2)
				.map((m) => {
					const [wx, wy] = [40 + m.x, m.y];
					// Where the line meets the shared side (x = 20).
					const k = (20 - at.x) / (wx - at.x);
					return { m, wx, wy, side: at.y + (wy - at.y) * k, d: Math.hypot(wx - at.x, wy - at.y) };
				})
				.filter((a) => a.wx > at.x && Math.abs(a.side) < 2 && Math.abs(a.side) > 0.8)
				.sort((a, b) => a.d - b.d);
			const aim = aims[0];
			if (aim !== undefined) {
				const yaw = Math.atan2(aim.wy - at.y, aim.wx - at.x);
				const pitch = Math.atan2((PIT_RULES.mobHeights[aim.m.tier] ?? 1) / 2 - (me.z + PIT_RULES.eyeHeight), aim.d);
				const shot = pit.aimShot(yaw, pitch, true, true);
				if (shot !== undefined) pit.client.sendInput({ mx: 0, my: 0, ...shot.input, buttons: shot.input.buttons });
			}
			await sleep(450);
			const hit = beams.find((b) => b.zoneId === 'pit.1.0' && b.hit !== undefined);
			if (hit !== undefined) target = `pit.1.0:${hit.hit}`;
		}
		console.error(`hit ${target}`);
		// It comes through the doorway after Ana: a newcomer in pit.0.0, closing in.
		const start = Date.now();
		let closest = Infinity;
		let arrived;
		while (Date.now() - start < 15_000) {
			const at = pit.position();
			const visitor = guestsIn(pit, 'pit.0.0')[0];
			if (visitor !== undefined && at !== undefined) {
				arrived ??= Date.now() - start;
				closest = Math.min(closest, Math.hypot(visitor.x - at.x, visitor.y - at.y));
				// Its home no longer draws it.
				assert.ok(!mobsIn(pit, 'pit.1.0').some((m) => m.key === target), 'never in two rooms');
			}
			pit.client.sendInput({ mx: 0, my: 0, buttons: 0 });
			await sleep(100);
		}
		console.error(`came through after ${arrived} ms; closest ${closest.toFixed(2)} m`);
		assert.ok(arrived !== undefined, 'the shot mob came into pit.0.0');
		assert.ok(closest < 2.5, `it reached Ana: ${closest} m`);
	} finally {
		pit.close();
		await server.stop();
	}
});
