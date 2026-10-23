/**
 * Headless Mob Pit idle rooms (Node): rooms on their own `celld dev`.
 *
 * A room that went to sleep writes nothing more. Each hibernated room once
 * kept its 10 s outbox alarm forever on an activity report that had long
 * settled (its queue was only drained by the tick loop): ~360 writes per
 * idle room-hour, most of a long-lived dev store.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { storeObjects } from './helpers/celld.mjs';
import { startPit } from './helpers/mobpit.mjs';

const ROOMS = ['pit.0.0', 'pit.3.0', 'pit.0.3', 'pit.3.3'];
/** The rooms' cells in celld's store (the game's cells write on their own). */
const ROOM_OBJECTS = 'cells/PitRoom:';
/** Longer than three outbox retries (10 s each). */
const QUIET_MS = 35_000;

test('hibernated rooms stop writing', { timeout: 180_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		for (const [k, zone] of ROOMS.entries()) {
			const api = new DevApi({ base: server.httpUrl, name: () => `Idler${k}`, storage: memoryStorage() });
			const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
			clients.push(pit.client);
			await pit.client.connect(zone);
			await pit.ready;
			await until(() => pit.me()?.predicted === true, 20_000, `Idler${k} in ${zone}`);
			pit.client.close();
		}
		const api = new MobPitApi(server.httpUrl, server.roomsUrl);
		for (const zone of ROOMS) {
			await eventually(async () => (await api.roomInfo(zone)).status === 'hibernated', 30_000, `${zone} asleep`);
		}
		// The first outbox alarm after hibernation settles the last report.
		await sleep(12_000);
		const before = storeObjects(server.celld.project, ROOM_OBJECTS);
		await sleep(QUIET_MS);
		const grown = storeObjects(server.celld.project, ROOM_OBJECTS) - before;
		console.log(`${ROOMS.length} hibernated rooms wrote ${grown} objects in ${QUIET_MS / 1000} s`);
		assert.ok(Number.isFinite(grown), 'store readable');
		assert.equal(grown, 0, `hibernated rooms kept writing: ${grown} objects in ${QUIET_MS / 1000} s`);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
