/**
 * Headless Boss Ripple client against two real `game-demos ripple`
 * processes (placement alternates by depth, so each hop crosses hosts):
 * the client plays a neighborhood, observes every tier, smites the bosses
 * of borough-1 and sees the cascade arrive at the borough, the city (rain)
 * and the realm, each tier running its own tick rate.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { allZones, BOROUGHS, CITY, createRippleClient, hoods, REALM } from '../src/ripple/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { startCluster } from './helpers/cluster.mjs';
import { memoryStorage, until } from './helpers/demo-server.mjs';

test('defeating a borough ripples up every tier across hosts', { timeout: 600_000 }, async () => {
	const cluster = await startCluster('ripple');
	const base = cluster.httpUrl;
	const ripple = createRippleClient({ api: new DevApi({ base, name: () => 'Hero', storage: memoryStorage() }), base });
	try {
		const { client } = ripple;
		const [first, ...rest] = hoods(BOROUGHS[0]);
		await client.connect(first);
		const hello = await ripple.ready;
		assert.equal(hello.tier, 'hood');
		assert.equal(hello.tickRate, 30);
		await until(() => allZones().every((z) => ripple.tier(z).connected && ripple.tier(z).hello !== undefined), 10_000, 'every tier observed');
		assert.equal(ripple.tier(REALM).hello.tickRate, 2);
		assert.equal(ripple.tier(CITY).hello.tickRate, 5);
		assert.equal(ripple.tier(BOROUGHS[0]).hello.tickRate, 10);
		assert.notEqual(ripple.tier(BOROUGHS[0]).hello.host, hello.host, 'parent on another host');
		assert.equal(ripple.tier(first).boss.hp, ripple.tier(first).boss.max);
		await until(() => ripple.weather() === 'storm', 5_000, 'stormy city');

		const bosses = [];
		ripple.onBoss((zone, event) => bosses.push([zone, event.kind]));
		for (const hood of [first, ...rest]) {
			if (hood !== first) await ripple.enter(hood);
			await until(() => client.authority?.zoneId === hood && client.authority.connection.canSendInput(), 5_000, `in ${hood}`);
			ripple.smite();
			await until(() => ripple.tier(hood).boss?.hp === 0, 5_000, `${hood} boss down`);
		}
		await until(() => ripple.tier(BOROUGHS[0]).cleared === 1, 10_000, 'borough cleared');
		await until(() => ripple.tier(CITY).cleared === 1 && ripple.tier(CITY).defeated === 3, 10_000, 'city tally');
		await until(() => ripple.tier(REALM).defeated === 3, 10_000, 'realm tally');
		await until(() => ripple.weather() === 'rain', 5_000, 'weather broadcast down to the players');
		const atBorough = ripple.ripples().filter((r) => r.at === BOROUGHS[0] && r.kind === 'boss_defeated');
		assert.deepEqual(new Set(atBorough.map((r) => r.origin)), new Set([first, ...rest]));
		assert.ok(ripple.ripples().some((r) => r.at === CITY && r.kind === 'borough_cleared'));
		assert.ok(ripple.ripples().some((r) => r.at === REALM && r.kind === 'borough_cleared' && r.from === CITY));
		assert.ok(bosses.filter(([, kind]) => kind === 'defeated').length >= 3);
		assert.equal(ripple.tier(BOROUGHS[1]).cleared, 0, 'the other borough is untouched');
	} finally {
		ripple.close();
		await cluster.stop();
	}
});
