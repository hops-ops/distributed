/**
 * Headless Mob Pit: a laser shot through a doorway carries on into the room
 * next door (Node, rooms on their own `celld dev`).
 *
 * The player stands by the east doorway of pit.0.0 and fires east through
 * the gap. The room hands the rest of the beam to pit.1.0 as a zone message
 * (`POST /pit.1.0/__zone`, internal secret); pit.1.0 resolves its leg and
 * broadcasts it, so the shooter's page (watching its neighbours) sees a
 * `beam` from pit.1.0 by them.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createMobPitClient } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit, walkTo } from './helpers/mobpit.mjs';

test('a laser through a doorway reaches the room next door', { timeout: 300_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Ana', storage: memoryStorage() });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit);
		const beams = [];
		pit.on('beam', (b, zoneId) => beams.push({ zoneId, ...b }));
		await pit.client.connect('pit.0.0');
		await pit.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');
		// Beside the pillar on the door's axis, in line with the gap.
		await walkTo(pit, [], [17, 1.7], undefined, 30_000);
		// Proximity wake: standing by the doorway wakes pit.1.0.
		await until(() => pit.near().has('pit.1.0'), 20_000, 'pit.1.0 watched');
		const deadline = Date.now() + 30_000;
		let fired = 0;
		while (beams.length === 0 && Date.now() < deadline) {
			const shot = pit.aimShot(0, -0.03, true, true);
			if (shot !== undefined) {
				fired += 1;
				pit.client.sendInput({ mx: 0, my: 0, ...shot.input, buttons: shot.input.buttons });
			}
			await sleep(400);
		}
		console.error(`${fired} shots; beams ${JSON.stringify(beams.map((b) => ({ zone: b.zoneId, by: b.by, x: +b.x.toFixed(2), y: +b.y.toFixed(2), hit: b.hit })))}`);
		if (beams.length === 0) console.error(server.celld.output().split('\n').filter((l) => /zone|refused|panic|error/i.test(l)).slice(-30).join('\n'));
		assert.ok(beams.length > 0, 'a beam came out the other side');
		assert.equal(beams[0].zoneId, 'pit.1.0');
		assert.equal(beams[0].by, 'Ana');
		// It enters on pit.1.0's west side.
		assert.ok(beams[0].x < -19, `enters at x = ${beams[0].x}`);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
