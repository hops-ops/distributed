/**
 * Headless Mob Pit neighbours (Node): rooms on their own `celld dev`.
 *
 * A player watches the 8 rooms around its own as a passive observer: a
 * sleeping room is not woken (it answers `ASLEEP`, nothing is drawn), an
 * awake one streams its mobs, and the watcher does not keep it awake — when
 * its last player leaves it goes dormant and then hibernates as if nobody
 * looked, and the watcher shows nothing again. A neighbour applies the
 * player's own view radius around the player (`Focus`): only its mobs
 * within reach of the player are sent, also after the player moves.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob, mobFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi, neighbourRooms } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, until } from './helpers/demo-server.mjs';
import { startPit, walkTo } from './helpers/mobpit.mjs';

test('neighbour rooms: passive watchers see mobs, never wake or keep a room awake', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const player = (name, neighbours = true) => {
			const pit = createMobPitClient({
				api: new DevApi({ base: server.httpUrl, name: () => name, storage: memoryStorage() }),
				roomsBase: server.roomsUrl,
				neighbours,
				// Passive watchers only (proximity wake has its own test).
				wake: false
			});
			clients.push(pit);
			return pit;
		};
		const watcher = player('Watcher');
		await watcher.client.connect('pit.0.0');
		const hello = await watcher.ready;
		assert.deepEqual(neighbourRooms('pit.0.0').length, 8);
		const mobsIn = (zone) => watcher.client.render().filter((e) => isMob(e) && !e.stale && e.zoneId === zone).length;

		// Nobody ever played next door: every neighbour stays asleep (not woken).
		await until(() => [...watcher.neighbours().values()].filter((s) => s === 'asleep').length === 8, 15_000, 'all 8 asleep');
		const east = await pitApi.roomInfo('pit.1.0');
		assert.equal(east.awake, false, 'watching did not wake the room');
		assert.equal(east.wakes, 0);
		assert.equal(mobsIn('pit.1.0'), 0);

		// A player wakes pit.1.0: the watcher sees its mobs next door.
		const other = player('Other', false);
		await other.client.connect('pit.1.0');
		await other.ready;
		const layout = watcher.pures.pitRoom({ seed: hello.seed, x: 1, y: 0 });
		await until(() => watcher.neighbours().get('pit.1.0') === 'watching', 15_000, 'watching pit.1.0');
		// Only what the player's own view covers, measured from the player
		// (pit.1.0's frame: world x − 40), leaving at +10 %.
		const leave = hello.viewRadius * 1.1;
		const eastMobs = () =>
			watcher.client
				.render()
				.filter((e) => isMob(e) && !e.stale && e.zoneId === 'pit.1.0')
				.map((e) => mobFields(e));
		const focus = () => {
			const at = watcher.position();
			return { x: at.x - 40, y: at.y };
		};
		const withinView = (slack) => {
			const f = focus();
			return eastMobs().every((m) => Math.hypot(m.x - f.x, m.y - f.y) <= leave + slack);
		};
		const farMobs = () => {
			const f = focus();
			return layout.mobs.filter((m) => Math.hypot(m.x - f.x, m.y - f.y) > leave + 6).length;
		};
		await until(() => withinView(0.5), 10_000, 'pit.1.0 sends only what the player sees');
		if (farMobs() > 0) assert.ok(eastMobs().length < layout.mobs.length, 'far mobs of pit.1.0 are not sent');
		console.error(`watching pit.1.0 from the room's middle: ${mobsIn('pit.1.0')} of ${layout.mobs.length} mobs`);

		// Walking up to the shared wall brings pit.1.0's west half into view.
		const obstacles = watcher.pures.pitRoom({ seed: hello.seed, x: 0, y: 0 }).obstacles;
		const middle = mobsIn('pit.1.0');
		await walkTo(watcher, obstacles, [15, 3]);
		await until(() => withinView(0.5) && mobsIn('pit.1.0') > middle, 10_000, 'more of pit.1.0 near its wall');
		console.error(`watching pit.1.0 from beside its wall: ${mobsIn('pit.1.0')} of ${layout.mobs.length} mobs`);
		await walkTo(watcher, obstacles, [-3, 3]);

		// Its player leaves: the room sleeps although it is watched.
		other.close();
		await eventually(async () => (await pitApi.rooms()).rooms.find((r) => r.zone === 'pit.1.0')?.status === 'dormant', 15_000, 'pit.1.0 dormant');
		await eventually(async () => (await pitApi.rooms()).rooms.find((r) => r.zone === 'pit.1.0')?.status === 'hibernated', 15_000, 'pit.1.0 hibernated while watched');
		assert.equal((await pitApi.roomInfo('pit.1.0')).awake, false);
		await until(() => watcher.neighbours().get('pit.1.0') === 'asleep' && mobsIn('pit.1.0') === 0, 15_000, 'watcher lets go of the sleeping room');
		// And it stays asleep while the watcher keeps trying.
		await new Promise((r) => setTimeout(r, 4000));
		const after = await pitApi.roomInfo('pit.1.0');
		assert.equal(after.awake, false, 'still asleep');
		assert.ok(after.wakes <= 1, 'woken only by the player');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
