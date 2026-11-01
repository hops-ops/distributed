/**
 * Headless Mob Pit world events (Node): rooms and tier cells on their own
 * `celld dev`, the services binary, a scripted player.
 *
 * The room tells its borough it woke (a zone fact); the borough's cell
 * arms its event cycle and, on its own schedule (`ctx.schedule`), starts
 * an invasion: the room reads it off the tick and tells its players
 * (`world.event`), the services see it in the borough cell, and the effect
 * is real — a mob slain during the invasion is back within a second or so
 * (normally four).
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, until } from './helpers/demo-server.mjs';
import { hunt, startPit } from './helpers/mobpit.mjs';

test('a borough schedules a world event; its rooms feel it', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const pit = createMobPitClient({
			api: new DevApi({ base: server.httpUrl, name: () => 'Eventer', storage: memoryStorage() }),
			roomsBase: server.roomsUrl,
			neighbours: false
		});
		clients.push(pit);
		await pit.client.connect('pit.0.0');
		const hello = await pit.ready;
		await until(() => pit.world()?.event === 'invasion', 40_000, 'the borough starts an invasion');
		const w = pit.world();
		assert.ok(w.eventUntilMs > Date.now(), 'it runs for a while');
		console.error(`world event: ${w.event} in borough ${w.boroughX},${w.boroughY}, ${Math.round((w.eventUntilMs - Date.now()) / 1000)} s left`);
		const tiers = await eventually(async () => {
			const t = await pitApi.tiers('pit.0.0');
			return t.boroughs.find((b) => b.borough === '0.0')?.event === 'invasion' && t;
		}, 10_000, 'the borough cell shows it');
		assert.ok(tiers);

		// The effect: the fallen rise again at once.
		const layout = pit.pures.pitRoom({ seed: hello.seed, x: 0, y: 0 });
		const mobs = () => pit.client.render().filter((e) => isMob(e) && e.zoneId === 'pit.0.0' && !e.stale).length;
		let killedAt = 0;
		pit.on('kill', () => (killedAt ||= Date.now()));
		await hunt(pit, () => killedAt > 0, 45_000);
		await until(() => mobs() === layout.mobs.length, 3_000, 'the slain mob is back within seconds');
		console.error(`back after ${Date.now() - killedAt} ms`);
		assert.ok(Date.now() - killedAt < 3_500);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
