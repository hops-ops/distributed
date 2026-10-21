import assert from 'node:assert/strict';
import test from 'node:test';

import { watchZones } from '../dist/game/index.js';

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
		observe(id) {
			client.observed.push(id);
			const zone = { zoneId: id, role: 'observer', connection: { state: 'connecting' } };
			zones.set(id, zone);
			if ((answers.get(id) ?? 'asleep') === 'ok') {
				zone.connection.state = 'open';
				return Promise.resolve({});
			}
			zone.connection.state = 'closed';
			return Promise.reject(new Error('ASLEEP'));
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
