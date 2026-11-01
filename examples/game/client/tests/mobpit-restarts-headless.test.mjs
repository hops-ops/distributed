/**
 * Headless Mob Pit: the same browser comes back as the same player — same
 * principal and name, the room it was last in, the place it stood, its
 * durable stats — after every kind of restart:
 *
 * - a page reload (fresh ticket for the slot it still holds: the room keeps
 *   the body and greets the new page),
 * - a network drop (reconnect token: the live body),
 * - closing the page past the reconnect grace (the room parks the place;
 *   it hibernates with it),
 * - a services restart (the guest token still verifies: persisted secret),
 * - a celld restart while away (hibernated room snapshot) and while
 *   connected (the room's periodic snapshot).
 *
 * The page's start room comes from `/dev/mobpit/start` (the presence cell,
 * verified from the guest token), as `src/mobpit/main.ts` does.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { hunt, startPit, walkThroughDoor } from './helpers/mobpit.mjs';

/** How far (m) a returning player may be from where it stood. */
const SAME_PLACE = 0.3;
const GRACE_MS = 2000;

test('the same browser comes back as the same player after reloads and restarts', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit', { ZONE_RECONNECT_GRACE_MS: String(GRACE_MS), ZONE_PERSIST_MS: '300' });
	const storage = memoryStorage();
	const clients = [];
	const pitApi = () => new MobPitApi(server.httpUrl, server.roomsUrl);

	/** What a page load does: ask where to start, join there. */
	async function openPage(pageStorage = storage) {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Hunter', storage: pageStorage });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		const zone = await pitApi().startRoom(api.guest);
		await pit.client.connect(zone);
		await pit.ready;
		await until(() => pit.me()?.predicted === true && pit.position() !== undefined && pit.profile() !== undefined, 10_000, 'own player and profile');
		return { api, pit };
	}

	/** Walk with `[mx, my]` for `frames` command frames, then stand still. */
	async function walk(pit, [mx, my], frames) {
		for (let i = 0; i < frames; i += 1) {
			pit.client.sendInput({ mx, my, buttons: 0 });
			await sleep(1000 / 30);
		}
		for (let i = 0; i < 15; i += 1) {
			pit.client.sendInput({ mx: 0, my: 0, buttons: 0 });
			await sleep(1000 / 30);
		}
	}

	/** The player as the page shows it. */
	const seen = ({ api, pit }) => ({
		principal: api.last.principal,
		name: api.last.name,
		room: pit.room(),
		kills: pit.profile().kills,
		xp: pit.profile().xp,
		...pit.position()
	});

	function assertSame(what, now, before) {
		const moved = Math.hypot(now.x - before.x, now.y - before.y);
		console.error(`${what}: ${now.room} (${now.x.toFixed(2)}, ${now.y.toFixed(2)}), ${moved.toFixed(3)} m from where it stood`);
		assert.equal(now.principal, before.principal, `${what}: principal`);
		assert.equal(now.name, before.name, `${what}: name`);
		assert.equal(now.room, before.room, `${what}: room`);
		assert.ok(moved < SAME_PLACE, `${what}: (${before.x}, ${before.y}) → (${now.x}, ${now.y})`);
		assert.equal(now.kills, before.kills, `${what}: kills`);
		assert.equal(now.xp, before.xp, `${what}: xp`);
	}

	/** Presence follows the player (settled lifecycle facts) before a page asks for it. */
	const presenceIn = (principal, zone) =>
		eventually(async () => (await pitApi().presence(principal)).presence?.zone === zone, 15_000, `presence in ${zone}`);

	const hibernated = (zone) =>
		eventually(async () => (await pitApi().rooms()).rooms.find((r) => r.zone === zone)?.status === 'hibernated', 30_000, `${zone} hibernated`);

	try {
		// First visit: the entrance. A kill (durable stats), then east.
		let page = await openPage();
		assert.equal(page.pit.room(), 'pit.0.0');
		await hunt(page.pit, () => (page.pit.profile()?.kills ?? 0) >= 1 && page.pit.unsaved.size === 0);
		assert.equal((await walkThroughDoor(page.pit, [1, 0])).room, 'pit.1.0');
		await walk(page.pit, [127, 40], 20);
		let before = seen(page);
		await presenceIn(before.principal, 'pit.1.0');

		// 1. Reload: same body in pit.1.0 (not a new one at the entrance).
		page.pit.client.close();
		page = await openPage();
		assertSame('reload', seen(page), before);
		assert.equal((await pitApi().roomInfo('pit.1.0')).players, 1, 'one body, not two');

		// 2. Network drop: the reconnect token resumes the live body.
		await walk(page.pit, [0, 127], 15);
		before = seen(page);
		const entity = page.pit.hello().entity;
		const connection = page.pit.client.authority.connection;
		connection.dropTransport();
		await until(() => connection.state === 'open' && connection.welcome !== undefined, 10_000, 'reconnected');
		await sleep(300);
		assertSame('network drop', seen(page), before);
		assert.equal(page.pit.hello().entity, entity, 'same entity');

		// 3. Closed past the grace: the room parks the place and sleeps with it.
		page.pit.client.close();
		await sleep(GRACE_MS + 500);
		await hibernated('pit.1.0');
		page = await openPage();
		assertSame('back after the grace (room hibernated)', seen(page), before);
		assert.equal(page.pit.hello().restored, true, 'the room woke from its snapshot');

		// 4. Services restart.
		await walk(page.pit, [-127, -60], 12);
		before = seen(page);
		page.pit.client.close();
		await server.restart();
		page = await openPage();
		assertSame('services restart', seen(page), before);

		// 5. celld restart while away (the hibernated snapshot).
		await walk(page.pit, [60, 127], 12);
		before = seen(page);
		page.pit.client.close();
		await sleep(GRACE_MS + 500);
		await hibernated('pit.1.0');
		await server.restartCelld();
		page = await openPage();
		assertSame('celld restart while away', seen(page), before);

		// 6. celld restart while connected (the periodic snapshot): the page
		// reconnects on its own.
		await walk(page.pit, [-127, 0], 12);
		before = seen(page);
		await sleep(1000);
		const restarted = server.restartCelld();
		await restarted;
		await until(() => page.pit.client.authority?.connection.state === 'open', 30_000, 'the page reconnected');
		await until(() => page.pit.me()?.predicted === true && page.pit.position() !== undefined, 10_000, 'own player again');
		await sleep(300);
		assertSame('celld restart while connected', seen(page), before);

		// Another browser is another player (and starts at the entrance).
		const other = await openPage(memoryStorage());
		assert.notEqual(other.api.last.principal, before.principal);
		assert.equal(other.pit.room(), 'pit.0.0');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
