/**
 * Headless Endless Grid client (Node, no browser) against two real
 * `game-demos grid` processes (hosts A and B on a hash ring):
 *
 * - joins grid.0.0 and observes its 8 neighbours (critters across the
 *   border are rendered before crossing);
 * - walks east along a trail over several borders, at least one of them
 *   between hosts, following every Handoff/Redirect with the SDK — the
 *   session survives and the walker ends in the expected cell;
 * - the cell left behind hibernates (world map) and a new visitor gets
 *   the "restored from hibernation" hello;
 * - the world content each crossed cell renders (terrain lattice, props)
 *   comes from the compiled Rust generation (`grid_cell`).
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { BUTTON_SPRINT, createGridClient, WORLD, zoneId } from '../src/grid/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { startCluster } from './helpers/cluster.mjs';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';

test('grid walker crosses cells and hosts; empty cells hibernate and restore', { timeout: 600_000 }, async () => {
	const cluster = await startCluster('grid', { hibernateAfterMs: 800 });
	const clients = [];
	try {
		const base = cluster.httpUrl;
		const api = new DevApi({ base, name: () => 'Walker', storage: memoryStorage() });
		const grid = createGridClient({ api, base });
		clients.push(grid);
		const { client } = grid;
		let failed = 0;
		client.on('handoffFailed', () => (failed += 1));

		await client.connect('grid.0.0');
		const hello = await grid.ready;
		assert.deepEqual(hello.cell, [0, 0]);
		// The module is loaded with the step: content is there for every cell.
		const origin = grid.content([0, 0]);
		assert.equal(origin.heights.length, WORLD.samples * WORLD.samples);
		assert.ok(grid.props([0, 0]).length > 0, 'the origin cell has props');
		assert.equal(grid.sample([0, 0], 32, 32).height, grid.content([0, 0]).heights.at(-1));
		const principal = api.last.principal;
		await until(() => client.predictor.active, 5_000, 'walker bound');
		await until(
			() => client.zones().filter((z) => z.role === 'observer' && z.connection.welcome !== undefined).length === 8,
			10_000,
			'8 neighbours observed'
		);
		await until(
			() => client.render().some((e) => e.kindName === 'critter' && e.zoneId === 'grid.1.0'),
			5_000,
			'critters across the east border'
		);

		// Pick a route east that crosses between hosts.
		const map = await grid.map([0, 0], 8);
		const owner = (x) => map.cells.find((c) => c.x === x && c.y === 0).owner;
		assert.equal(owner(0), hello.host);
		let last = 2;
		while (![...Array(last).keys()].some((i) => owner(i) !== owner(i + 1))) last += 1;
		assert.ok(last <= 8, 'a host border within 8 cells');
		const target = zoneId([last, 0]);

		const deadline = Date.now() + 60_000;
		const hosts = new Set();
		while (!(client.authority?.zoneId === target && client.predictor.active && client.predictor.predicted().x > 4)) {
			assert.ok(Date.now() < deadline, `walk timed out in ${client.authority?.zoneId}`);
			client.sendInput({ mx: 127, my: 0, buttons: BUTTON_SPRINT });
			const zone = client.authority?.zoneId;
			if (zone !== undefined && grid.info(zone) !== undefined) hosts.add(grid.info(zone).host);
			await sleep(1000 / 30);
		}
		assert.equal(failed, 0, 'no failed handoff');
		assert.equal(grid.handoffs(), last, 'one handoff per border');
		assert.equal(grid.info(target).host, owner(last));
		assert.ok(hosts.size === 2, `visited both hosts: ${[...hosts]}`);
		const ownerUrl = map.hosts.find((h) => h.id === owner(last)).wsUrl;
		assert.ok(client.authority.connection.url.startsWith(ownerUrl), 'connected to the owning host');
		assert.equal(api.last.principal, principal, 'same guest throughout');
		await until(() => client.predictor.pending.length === 0, 5_000, 'acks');
		const me = client.authority.replica.get(grid.info(target).entity);
		assert.equal(me.fields.name, 'Walker', 'name carried across hosts');
		assert.ok(Math.abs(me.fields.y - 16) < 0.01, 'stayed on the trail');
		assert.deepEqual(client.predictor.predicted().x, me.fields.x, 'prediction matches the server');
		// Neighbours follow the walker.
		await until(
			() => client.zones().filter((z) => z.role === 'observer').map((z) => z.zoneId).includes(zoneId([last + 1, 0])),
			5_000,
			'new east neighbour observed'
		);

		// grid.0.0 is no longer observed by anyone: Dormant, then hibernated.
		let state = 'active';
		const hibernateDeadline = Date.now() + 15_000;
		while (state !== 'hibernated') {
			assert.ok(Date.now() < hibernateDeadline, `grid.0.0 stayed ${state}`);
			await sleep(200);
			state = (await grid.map([0, 0], 0)).cells[0].state;
		}

		const visitor = createGridClient({
			api: new DevApi({ base, name: () => 'Visitor', storage: memoryStorage() }),
			base,
			neighbours: false
		});
		clients.push(visitor);
		const restored = new Promise((resolve) => visitor.onRestored(resolve));
		await visitor.client.connect('grid.0.0');
		const back = await restored;
		assert.equal(back.zone, 'grid.0.0');
		assert.equal(back.restores, 1);
		assert.ok(back.restoredCritters >= 2);
		assert.ok(back.hibernatedForMs >= 0);
	} finally {
		for (const c of clients) c.close();
		await cluster.stop();
	}
});
