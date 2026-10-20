/**
 * Headless Mob Pit (Node, real `game-demos mobpit` binary on its own
 * `celld dev`): join → profile read from the celld Crawler cell → kill a mob
 * → the fact settles ("saved") with server-decided XP → slow storage makes
 * the settlement wait while the tick stays flat → restart the server → XP is
 * still there.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isPit, pitFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, until } from './helpers/demo-server.mjs';
import { hunt, startPit } from './helpers/mobpit.mjs';

const XP = new Set([10, 25, 60]);

test('kills become durable facts without stalling the tick, and survive a restart', { timeout: 600_000 }, async () => {
	const storage = memoryStorage();
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Hunter', storage });
		const pitApi = new MobPitApi(server.httpUrl);
		const pit = createMobPitClient({ api, base: server.httpUrl });
		clients.push(pit.client);
		const welcome = await pit.client.connect('pit-1');
		assert.deepEqual(welcome.schema.kinds.map((k) => k.name), ['player', 'mob', 'pillar', 'pit']);
		await pit.ready;
		await until(() => pit.profile() !== undefined, 5_000, 'profile from the cell');
		assert.equal(pit.profile().source, 'cell');
		assert.equal(pit.profile().kills, 0);

		// First kill at normal speed.
		const saved = [];
		const kills = [];
		pit.on('saved', (s) => saved.push(s));
		pit.on('kill', (k) => k.killer === pit.hello().entity && kills.push(k));
		await hunt(pit, () => saved.length >= 1);
		assert.ok(saved[0].ok && XP.has(saved[0].xp), JSON.stringify(saved[0]));
		assert.equal(saved[0].key, kills[0].key);
		assert.match(
			saved[0].key,
			/^crawler\.record_kill:pit-1:\d+:m\d+:\d+$/,
			'{command}:{zone}:{boot_epoch}:m{mob}:{generation}'
		);
		// One swing can kill several mobs: wait until every own kill settled.
		await until(() => pit.unsaved.size === 0 && saved.length === kills.length, 5_000, 'settlements');
		await until(() => pit.profile().source === 'settled' && pit.profile().kills === kills.length, 5_000, `profile after settlement ${JSON.stringify(pit.profile())}`);

		// Slow storage: the next kill stays unsaved for ~1.5 s, the tick does not slow down.
		assert.equal((await pitApi.setStorage(1500)).latencyMs, 1500);
		const pitStats = () => {
			const entity = pit.client.render().find(isPit);
			return entity && pitFields(entity);
		};
		const fastKills = kills.length;
		await hunt(pit, () => kills.length > fastKills);
		await until(() => (pitStats()?.pending ?? 0) >= 1, 2_000, 'pending fact replicated');
		assert.ok(pit.unsaved.size >= 1, 'kill not yet saved');
		const ticks = [];
		const sampleUntil = Date.now() + 900;
		while (Date.now() < sampleUntil) {
			ticks.push(pitStats()?.tickMaxMs ?? 0);
			await new Promise((r) => setTimeout(r, 100));
		}
		assert.ok(Math.max(...ticks) < 80, `tick interval stays ~33 ms while storage is slow: ${ticks}`);
		await until(() => saved.length === kills.length, 15_000, 'slow settlements');
		assert.ok(saved[fastKills].waitedMs >= 1400, `waited ${saved[fastKills].waitedMs} ms`);
		await pitApi.setStorage(0);
		await until(() => pit.profile().kills === kills.length, 5_000, 'profile');
		const before = pit.profile();

		// Graceful restart on the same data dir (the smoke test covers kill -9):
		// the guest token stays valid (secret persisted in the data dir) and
		// the profile is read back from the cell.
		pit.client.close();
		await server.restart();
		const again = createMobPitClient({ api: new DevApi({ base: server.httpUrl, storage }), base: server.httpUrl });
		clients.push(again.client);
		await again.client.connect('pit-1');
		await until(() => again.profile() !== undefined, 5_000, 'profile after restart');
		assert.equal(again.profile().source, 'cell');
		assert.equal(again.profile().kills, before.kills);
		assert.equal(again.profile().xp, before.xp);
		const crawlers = (await new MobPitApi(server.httpUrl).crawlers()).crawlers;
		assert.equal(crawlers.length, 1);
		assert.equal(crawlers[0].kills, kills.length);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
