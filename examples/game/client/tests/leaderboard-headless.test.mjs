/**
 * Headless Leaderboard: a hunter scores kills in a room on celld; the
 * services' board (a projection of the Crawler cells) follows, its scores
 * equal the cells; rebuilding from the cells' events yields the identical
 * board.
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
	let watcher;
	try {
		const storage = memoryStorage();
		const api = new DevApi({ base: server.httpUrl, name: () => 'Ranker', storage });
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);

		watcher = createBoardWatcher(pitApi, 200);
		const pushes = [];
		watcher.on((b) => pushes.push(b));
		await until(() => pushes.length >= 1, 5_000, 'initial board');
		assert.equal(watcher.board().backend, 'memory');

		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		await pit.client.connect('pit.0.0');
		await pit.ready;
		const me = api.last.principal;
		await hunt(pit, () => (pit.profile()?.kills ?? 0) >= 2);
		const profile = pit.profile();

		// The board follows the cells (via the projected crawler list).
		const row = (b, board) => b[board].find((r) => r.member === me);
		await until(() => row(watcher.board(), 'kills')?.score === profile.kills, 20_000, 'board kills');
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
		await until(() => watcher.board().lastRebuild?.events === profile.kills, 5_000, 'rebuild shown');
	} finally {
		watcher?.stop();
		for (const c of clients) c.close();
		await server.stop();
	}
});
