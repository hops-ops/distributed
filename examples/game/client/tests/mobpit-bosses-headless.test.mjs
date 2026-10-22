/**
 * Headless Mob Pit hierarchy (Node): rooms and tier cells on their own
 * `celld dev`, the services binary, a party of scripted players.
 *
 * A party fells the boss of hood 0.0 in its seeded boss room: the room
 * tells everyone (`bossDown`) and reports the fall to the hood's cell (a
 * zone fact); the hood's tier rises and the worker's saga ripples it into
 * the borough and the city (`/dev/mobpit/tiers` reads the cells). Another
 * room of the same hood learns its tier from its hood (the rooms read
 * their tiers off the tick) — it is tougher now.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { BUTTON, isMob, mobFields, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit } from './helpers/mobpit.mjs';

/** Boss room of hood `(hx, hy)`: the room whose generated layout says so. */
function bossRoomOf(pures, seed, hx, hy) {
	for (let k = 0; k < 9; k += 1) {
		const [x, y] = [hx * 3 + (k % 3), hy * 3 + Math.floor(k / 3)];
		if (pures.pitRoom({ seed, x, y }).boss) return [x, y];
	}
	throw new Error('no boss room');
}

test('a boss falls: its hood rises, the saga ripples it up, the hood gets tougher', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const player = (name) => {
			const pit = createMobPitClient({
				api: new DevApi({ base: server.httpUrl, name: () => name, storage: memoryStorage() }),
				roomsBase: server.roomsUrl,
				neighbours: false
			});
			clients.push(pit);
			return pit;
		};
		const scout = player('Scout');
		await scout.pures.ready;
		const seed = (await import('../src/generated/mobpit/index.ts')).PIT_WORLD.defaultSeed;
		const [bx, by] = bossRoomOf(scout.pures, seed, 0, 0);
		const bossZone = `pit.${bx}.${by}`;
		const party = ['Ana', 'Bob', 'Cy', 'Di'].map(player);
		await Promise.all(party.map((p) => p.client.connect(bossZone)));
		await Promise.all(party.map((p) => p.ready));
		const downs = [];
		party[0].on('bossDown', (b, zone) => zone === bossZone && downs.push(b));
		await until(() => party[0].world() !== undefined, 10_000, 'world tiers on join');
		assert.equal(party[0].world().hoodTier, 0);
		const boss = () => party[0].client.render().find((e) => isMob(e) && e.zoneId === bossZone && mobFields(e).tier === 4 && !e.stale);
		await until(() => boss() !== undefined, 10_000, 'the boss');

		// The party hacks at the boss until it falls.
		const deadline = Date.now() + 120_000;
		while (downs.length === 0) {
			assert.ok(Date.now() < deadline, 'the party fells the boss');
			const b = boss();
			for (const p of party) {
				const me = p.me();
				let input = { mx: 0, my: 0, buttons: 0 };
				if (b !== undefined && me !== undefined) {
					const at = playerFields(me);
					const target = mobFields(b);
					const dx = target.x - at.x;
					const dy = target.y - at.y;
					const d = Math.max(Math.hypot(dx, dy), 1e-3);
					const near = d < 2.6;
					const k = (near ? 40 : 127) / d;
					input = { mx: Math.round(dx * k), my: Math.round(dy * k), buttons: near ? BUTTON.attack : 0 };
				}
				p.client.sendInput(input);
			}
			await sleep(1000 / 30);
		}
		assert.equal(downs[0].hoodTier, 1);
		console.error(`boss of hood 0.0 felled in ${bossZone} by ${downs[0].by}`);

		// Up the hierarchy: the hood's cell, then the saga into borough and city.
		const tiers = await eventually(async () => {
			const t = await pitApi.tiers(bossZone);
			const hood = t.hoods.find((h) => h.hood === '0.0');
			return hood?.bossDown && hood.tier === 1 && t.boroughs.find((b) => b.borough === '0.0')?.bossesDown === 1 && t.city?.bosses_down === 1 && t;
		}, 30_000, 'hood → borough → city');
		console.error(`city: ${tiers.city.banner}`);

		// Down again: another room of the hood reads its hood's tier.
		const other = [0, 1, 2].flatMap((x) => [0, 1, 2].map((y) => [x, y])).find(([x, y]) => x !== bx || y !== by);
		const visitor = player('Visitor');
		await visitor.client.connect(`pit.${other[0]}.${other[1]}`);
		await visitor.ready;
		await until(() => visitor.world()?.hoodTier === 1 && visitor.world()?.bossDown === true, 15_000, 'the hood tier reaches its other rooms');
		assert.equal(visitor.world().boroughBossesDown, 1);
		assert.ok(visitor.world().banner.length > 0, 'the city banner');
		// The boss stays down in its room meanwhile.
		await sleep(5000);
		assert.equal(boss(), undefined, 'still down');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
