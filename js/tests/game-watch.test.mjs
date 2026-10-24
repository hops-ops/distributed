import assert from 'node:assert/strict';
import test from 'node:test';

import { GameErrorCode, watchZones } from '../dist/game/index.js';

/** A GameClient stand-in: zones, observe / unobserve, events. */
function stubClient() {
	const zones = new Map();
	const handlers = new Map();
	const answers = new Map(); // zone → 'ok' | 'asleep'
	const client = {
		authority: undefined,
		observed: [],
		zones: () => [...zones.values()],
		zone: (id) => zones.get(id),
		nearCalls: [],
		observe(id, options = {}) {
			client.observed.push(id);
			const zone = { zoneId: id, role: 'observer', near: options.near === true, connection: { state: 'connecting' } };
			zones.set(id, zone);
			// A near watcher wakes a sleeping zone (its ticket grants wake).
			if ((answers.get(id) ?? 'asleep') === 'ok' || zone.near) {
				zone.connection.state = 'open';
				return Promise.resolve({});
			}
			zone.connection.state = 'closed';
			return Promise.reject(new Error('ASLEEP'));
		},
		setNear(id, near) {
			const zone = zones.get(id);
			if (zone?.role !== 'observer') return;
			if (zone.near !== near) client.nearCalls.push([id, near]);
			zone.near = near;
		},
		unobserve(id) {
			if (zones.get(id)?.role === 'observer') zones.delete(id);
		},
		on(type, fn) {
			handlers.set(type, [...(handlers.get(type) ?? []), fn]);
			return () => handlers.set(type, (handlers.get(type) ?? []).filter((h) => h !== fn));
		},
		emit(type, ...args) {
			for (const h of handlers.get(type) ?? []) h(...args);
		},
		join(id) {
			client.authority = { zoneId: id };
			zones.set(id, { zoneId: id, role: 'authority', connection: { state: 'open' } });
			client.emit('authority', id);
		},
		answers
	};
	return client;
}

const flush = () => new Promise((r) => setImmediate(r));

test('watchZones observes the zones around the authority, lets sleeping ones go and retries', async () => {
	const client = stubClient();
	let tick = () => {};
	const around = (zone) => {
		const n = Number(zone.slice(1));
		return [`z${n - 1}`, `z${n}`, `z${n + 1}`];
	};
	let clock = 0;
	const watch = watchZones(client, around, { now: () => clock, setInterval: (fn) => ((tick = fn), 1), clearInterval: () => {} });
	assert.equal(watch.states().size, 0, 'no authority yet');
	client.answers.set('z1', 'ok');
	client.join('z2');
	await flush();
	assert.deepEqual(Object.fromEntries(watch.states()), { z1: 'watching', z3: 'asleep' });
	assert.equal(client.zone('z3'), undefined, 'a refused watcher is let go (nothing drawn)');
	assert.equal(client.zone('z2').role, 'authority', 'the own zone is not observed');
	// z3 wakes up later: the retry (after its back-off) finds it.
	client.answers.set('z3', 'ok');
	const tries = client.observed.length;
	tick();
	assert.equal(client.observed.length, tries, 'still backing off');
	clock += 3000;
	tick();
	await flush();
	assert.equal(watch.states().get('z3'), 'watching');
	// z1 falls asleep: its watcher closes and is let go.
	client.zone('z1').connection.state = 'closed';
	client.emit('zoneClosed', 'z1', { reason: 'closed' });
	assert.equal(watch.states().get('z1'), 'asleep');
	assert.equal(client.zone('z1'), undefined);
	// Moving on: zones no longer around are unobserved.
	client.join('z5');
	await flush();
	assert.deepEqual([...watch.states().keys()].sort(), ['z4', 'z6']);
	assert.equal(client.zone('z3'), undefined);
	watch.stop();
	const before = client.observed.length;
	client.join('z9');
	tick();
	assert.equal(client.observed.length, before, 'stopped');
});

test('watchZones: near zones are observed near, a sleeping one is retried at once, and released', async () => {
	const client = stubClient();
	let clock = 0;
	const around = (zone) => {
		const n = Number(zone.slice(1));
		return [`z${n - 1}`, `z${n + 1}`];
	};
	const watch = watchZones(client, around, { now: () => clock, setInterval: () => 1, clearInterval: () => {} });
	client.answers.set('z1', 'ok');
	client.join('z2');
	await flush();
	assert.deepEqual(Object.fromEntries(watch.states()), { z1: 'watching', z3: 'asleep' });
	// Near z1 (awake, open): told without reconnecting.
	const opened = client.observed.length;
	watch.setNear(['z1']);
	assert.equal(client.zone('z1').near, true);
	assert.equal(client.observed.length, opened, 'no reconnect');
	// Near z3 (asleep, backing off): retried at once, near, and it wakes.
	watch.setNear(['z1', 'z3']);
	await flush();
	assert.equal(client.zone('z3').near, true);
	assert.equal(watch.states().get('z3'), 'watching');
	assert.deepEqual([...watch.near()].sort(), ['z1', 'z3']);
	// Released: passive again, still watched.
	watch.setNear(['z3']);
	assert.equal(client.zone('z1').near, false);
	assert.equal(watch.states().get('z1'), 'watching');
	assert.deepEqual(client.nearCalls, [['z1', true], ['z1', false]]);
	// Nothing near: z3 passive too.
	watch.setNear([]);
	assert.equal(client.zone('z3').near, false);
	watch.stop();
});

test('watchZones: a near watcher refused WAKE_DENIED asks for a fresh ticket, at once then backing off', async () => {
	const client = stubClient();
	let clock = 0;
	let tick = () => {};
	const watch = watchZones(client, (zone) => [zone === 'z2' ? 'z3' : 'z2'], {
		now: () => clock,
		setInterval: (fn) => ((tick = fn), 1),
		clearInterval: () => {}
	});
	client.answers.set('z3', 'ok');
	client.join('z2');
	await flush();
	assert.equal(watch.states().get('z3'), 'watching');
	const deny = () => client.emit('error', 'z3', { type: 'Error', code: GameErrorCode.WAKE_DENIED, message: 'no grant' });
	watch.setNear(['z3']);
	const opened = client.observed.length;
	// Not near-ready (the ticket had no grant): reopened at once.
	deny();
	await flush();
	assert.equal(client.observed.length, opened + 1, 'a fresh ticket at once');
	assert.equal(client.zone('z3').near, true, 'reopened near');
	// Refused again: backs off (3 s), then tries once more.
	deny();
	await flush();
	tick();
	assert.equal(client.observed.length, opened + 1, 'backing off');
	clock += 3000;
	tick();
	await flush();
	assert.equal(client.observed.length, opened + 2);
	// A refusal for a zone that is no longer near is ignored.
	watch.setNear([]);
	deny();
	clock += 60_000;
	tick();
	await flush();
	assert.equal(client.observed.length, opened + 2);
	assert.equal(client.zone('z3').near, false);
	watch.stop();
});
