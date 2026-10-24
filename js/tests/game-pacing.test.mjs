import assert from 'node:assert/strict';
import test from 'node:test';

import { AdaptiveDelay, GameClient, InputPacer } from '@hops-ops/distributed/game';

import { FakeNetwork, FakeScheduler, avatar, delta, welcome } from './fixtures/game-harness.mjs';

const zoneWith = (clock, patchRate = 20) => ({ connection: { clock, welcome: { patchRate } } });

test('AdaptiveDelay targets rtt/2 + 2 patches + 2 jitter, clamped and slewed', () => {
	const delay = new AdaptiveDelay();
	const unsynced = zoneWith({ rtt: 0, jitter: 0, synced: false });
	assert.equal(delay.target(unsynced), 100, 'two 20 Hz patches before any Pong');
	assert.equal(delay.sample(unsynced, 0), 100, 'first sample jumps to the target');

	const slow = zoneWith({ rtt: 400, jitter: 20, synced: true });
	assert.equal(delay.target(slow), 200 + 100 + 40);
	assert.equal(delay.sample(slow, 100), 125, 'at most 25 ms per 100 ms');
	assert.equal(delay.sample(slow, 100), 125, 'same instant, same value');
	assert.equal(delay.settled(slow), false);
	for (let t = 200; t <= 1_200; t += 100) delay.sample(slow, t);
	assert.equal(delay.value, 340);
	assert.equal(delay.settled(slow), true);

	// Clamped to maxMs and to the patch-based minimum.
	assert.equal(delay.target(zoneWith({ rtt: 5_000, jitter: 0, synced: true })), 800);
	assert.equal(delay.target(zoneWith({ rtt: 0, jitter: 0, synced: true }, 10)), 200);
	const tuned = new AdaptiveDelay({ minMs: 50, maxMs: 300, patchIntervals: 1, jitterFactor: 0 });
	assert.equal(tuned.target(zoneWith({ rtt: 10, jitter: 99, synced: true })), 55);
	assert.equal(tuned.target(zoneWith({ rtt: 1_000, jitter: 0, synced: true })), 300);
});

test('InputPacer sends one input per tick from any frame rate and caps catch-up', () => {
	let tickRate = 30;
	const pacer = new InputPacer({ tickRate: () => tickRate, maxPerUpdate: 3 });
	let sent = 0;
	const send = (dt) => {
		assert.ok(Math.abs(dt - 1000 / tickRate) < 1e-9);
		sent += 1;
	};
	// 120 Hz display for one second → 30 inputs.
	for (let i = 0; i <= 120; i += 1) pacer.update((i * 1000) / 120, send);
	assert.equal(sent, 30);
	// A 2 s stall sends at most 3 and drops the rest of the backlog.
	sent = 0;
	assert.equal(pacer.update(3_000, send), 3);
	assert.equal(pacer.update(3_001, send), 1, 'at most one interval carried over');
	// Rate follows the server tick rate.
	tickRate = 60;
	pacer.reset();
	sent = 0;
	for (let i = 0; i <= 60; i += 1) pacer.update(10_000 + (i * 1000) / 60, send);
	assert.equal(sent, 60);
	assert.equal(new InputPacer({ rate: 10 }).intervalMs, 100);
});

test('GameClient: adaptive delay by default, fixed when configured, prediction toggles at runtime', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type === 'Hello') conn.send(welcome({ serverTime: 10_000 }));
	};
	const client = new GameClient({
		url: (zone) => `ws://game.test/zones/${zone}`,
		ticket: () => 't',
		socket: network.factory,
		scheduler,
		pingIntervalMs: 60_000,
		prediction: {
			encodeInput: (dx) => new Uint8Array([dx & 0xff]),
			predict: (s, dx, dt) => ({ x: s.x + (dx * dt) / 1000 }),
			controlledEntity: () => 1,
			smoothSteps: false
		}
	});
	await client.connect('z1');
	const zone = client.authority;
	const delay = client.adaptiveDelay(zone);
	assert.ok(delay instanceof AdaptiveDelay);
	assert.equal(client.interpolationDelay(zone), 100);
	// A measured 300 ms RTT moves the delay up gradually.
	zone.connection.clock.observe(0, 10_150, 300);
	scheduler.time = 400;
	const moved = client.interpolationDelay(zone);
	assert.ok(moved > 100 && moved < delay.target(zone), `slewing: ${moved}`);

	network.last('z1').send(delta({ creates: [avatar(1, 0)] }));
	await scheduler.advance(1);
	client.sendInput(50, 1000);
	const predicted = () => client.render().find((e) => e.id === 1);
	assert.equal(predicted().predicted, true);
	assert.ok(Math.abs(predicted().fields.x - 50) < 0.01, `${predicted().fields.x}`);
	client.setPredictionEnabled(false);
	assert.equal(client.predictionEnabled, false);
	assert.equal(predicted().predicted, false);
	assert.ok(Math.abs(predicted().fields.x) < 0.01, 'authoritative state when prediction is off');
	client.sendInput(50, 1000);
	client.setPredictionEnabled(true);
	assert.ok(Math.abs(predicted().fields.x - 100) < 0.01, 'inputs kept recording while off');
	client.close();

	const fixed = new GameClient({
		url: (zone) => `ws://game.test/zones/${zone}`,
		ticket: () => 't',
		socket: network.factory,
		scheduler,
		interpolationDelayMs: 70
	});
	await fixed.connect('z1');
	assert.equal(fixed.interpolationDelay(fixed.authority), 70);
	assert.equal(fixed.adaptiveDelay(fixed.authority), undefined);
	fixed.close();
});

test('GameClient: the controlled entity moves smoothly through each predicted step (smoothSteps)', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type === 'Hello') conn.send(welcome({ serverTime: 10_000 }));
	};
	const client = new GameClient({
		url: (zone) => `ws://game.test/zones/${zone}`,
		ticket: () => 't',
		socket: network.factory,
		scheduler,
		pingIntervalMs: 60_000,
		prediction: {
			encodeInput: (dx) => new Uint8Array([dx & 0xff]),
			predict: (s, dx, dt) => ({ x: s.x + (dx * dt) / 1000 }),
			controlledEntity: () => 1
		}
	});
	await client.connect('z1');
	network.last('z1').send(delta({ creates: [avatar(1, 0)] }));
	await scheduler.advance(1);
	const start = scheduler.time;
	client.sendInput(50, 1000); // +50 over the next 1000 ms
	const x = () => client.render().find((e) => e.id === 1).fields.x;
	assert.ok(Math.abs(x()) < 0.01, `drawn from where it was: ${x()}`);
	scheduler.time = start + 500;
	assert.ok(Math.abs(x() - 25) < 0.01, `halfway through the step: ${x()}`);
	scheduler.time = start + 1000;
	assert.ok(Math.abs(x() - 50) < 0.01, `the prediction at the end of the step: ${x()}`);
	client.close();
});
