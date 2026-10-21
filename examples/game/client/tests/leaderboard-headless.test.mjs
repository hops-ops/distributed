/**
 * Headless Leaderboard: a hunter scores kills in the pit, an observer
 * receives pushed board snapshots whose scores equal the celld Crawler cells;
 * rebuilding from the cells' events yields the identical board.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createBoardWatcher, createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, until } from './helpers/demo-server.mjs';
import { hunt, startPit } from './helpers/mobpit.mjs';

test('board pushes match the cells and rebuild is identical', { timeout: 600_000 }, async () => {
	const server = await startPit('leaderboard');
	const clients = [];
	try {
		const storage = memoryStorage();
		const api = new DevApi({ base: server.httpUrl, name: () => 'Ranker', storage });
		const pitApi = new MobPitApi(server.httpUrl);

		// Observer page first: it gets the current (empty) snapshot on observe.
		const watcher = createBoardWatcher({ api: new DevApi({ base: server.httpUrl, storage }), base: server.httpUrl });
		clients.push(watcher.client);
		const pushes = [];
		watcher.on((b) => pushes.push(b));
		await watcher.client.observe('pit-1');
		await until(() => pushes.length >= 1, 5_000, 'initial board push');
		assert.equal(watcher.board().backend, 'memory');

		const pit = createMobPitClient({ api, base: server.httpUrl });
		clients.push(pit.client);
		await pit.client.connect('pit-1');
		await pit.ready;
		const me = api.last.principal;
		const kills = [];
		pit.on('kill', (k) => k.killer === pit.hello().entity && kills.push(k));
		await hunt(pit, () => (pit.profile()?.kills ?? 0) >= 2);
		// One swing can kill several mobs, and kills keep landing from inputs
		// sent before the hunt saw the second settlement: wait until every
		// input is acknowledged and every own kill settled before comparing.
		await until(() => pit.client.predictor.pending.length === 0, 5_000, 'inputs acknowledged');
		await until(
			() => pit.unsaved.size === 0 && pit.profile()?.kills === kills.length,
			10_000,
			`settled profile ${JSON.stringify(pit.profile())} for ${kills.length} kills`
		);
		const profile = pit.profile();

		// Pushed, not polled: the observer sees my kills without asking.
		const row = (b, board) => b[board].find((r) => r.member === me);
		await until(() => row(watcher.board(), 'kills')?.score === profile.kills, 10_000, 'pushed kills');
		assert.equal(row(watcher.board(), 'kills').name, 'Ranker');
		assert.equal(row(watcher.board(), 'xp').score, profile.xp);
		// The crawler list (and so the rebuild) is a projection of the cells'
		// kill events: eventually equal to the cells.
		const listed = async () => (await pitApi.crawlers()).crawlers.map((c) => [c.principal, c.kills, c.xp]);
		await eventually(
			async () => JSON.stringify(await listed()) === JSON.stringify([[me, profile.kills, profile.xp]]),
			15_000,
			'crawler directory'
		);

		const { me: mine } = await pitApi.leaderboard(me);
		assert.equal(mine.kills.rank, 1);
		assert.equal(mine.kills.around[0].member, me);

		const top = [watcher.board().kills, watcher.board().xp];
		const report = await pitApi.rebuild();
		assert.equal(report.events, profile.kills);
		assert.equal(report.matchesCells, true);
		assert.equal(report.identicalTop, true);
		const after = (await pitApi.leaderboard()).snapshot;
		assert.deepEqual([after.kills, after.xp], top, 'rebuild from events = live board');
		await until(() => watcher.board().lastRebuild?.events === profile.kills, 5_000, 'rebuild pushed');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
