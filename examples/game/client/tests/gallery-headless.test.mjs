/**
 * Headless Shooting Gallery client against the real `game-demos gallery`
 * binary: a scripted shooter aims at the fastest target *as rendered*
 * under simulated latency. With lag compensation the server confirms the
 * hits; without it the same aim mostly misses; beyond the max rewind the
 * shots are clamped.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createGalleryClient, encodeShot, raySphere, targetPosition } from '../src/gallery/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, startDemo, until } from './helpers/demo-server.mjs';

const FAST_TARGET = 5; // 12 m/s rail

/** Fire `n` shots at the rendered fast target, wait for every verdict. */
async function volley(gallery, n, compensated) {
	const records = [];
	while (records.length < n) {
		await sleep(80);
		const eye = gallery.hello().eye;
		const target = gallery.targets().find((t) => t.id === FAST_TARGET);
		const at = targetPosition(target);
		const dir = [at[0] - eye[0], at[1] - eye[1], at[2] - eye[2]];
		// Another target in the line of fire: wait for a clear shot.
		if (gallery.predict(dir).target !== FAST_TARGET) continue;
		const shot = gallery.shoot(dir, { compensated });
		assert.ok(shot, 'shot sent');
		assert.equal(shot.predicted, FAST_TARGET, 'aimed at what is rendered');
		records.push(shot);
	}
	const verdicts = await Promise.all(records.map((r) => gallery.verdict(r.id)));
	return {
		hits: verdicts.filter((v) => v.hit === FAST_TARGET).length,
		clamped: verdicts.filter((v) => v.clamped).length,
		ages: verdicts.map((v) => v.ageMs),
		requested: verdicts.map((v) => v.requestedAgeMs)
	};
}

test('shot payload and local ray test', () => {
	const bytes = encodeShot(3, 0x1234, 287.4, [0, 2, 0]);
	assert.deepEqual([...bytes.slice(0, 5)], [3, 0x34, 0x12, 31, 1]);
	assert.equal(new DataView(bytes.buffer).getInt16(7, true), 32767);
	assert.equal(raySphere([0, 0, 0], [0, 1, 0], [0, 10, 0], 1), 9);
	assert.equal(raySphere([0, 0, 0], [0, 1, 0], [0, -10, 0], 1), undefined, 'behind');
	assert.equal(raySphere([0, 0, 0], [0, 1, 0], [2, 10, 0], 1), undefined, 'beside');
});

test('lag compensation confirms what the shooter saw; off misses; beyond max rewind clamps', { timeout: 600_000 }, async (t) => {
	const server = await startDemo('gallery');
	const api = new DevApi({ base: server.httpUrl, name: () => 'Sharpshooter', storage: memoryStorage() });
	const gallery = createGalleryClient({ api, base: server.httpUrl });
	try {
		await gallery.client.connect('gallery-1');
		const hello = await gallery.ready;
		assert.equal(hello.maxRewindMs, 500);
		await until(() => gallery.targets().length === 6, 5_000, 'targets');

		// 100 ms one way (RTT ≈ 200 ms): the client draws targets ~300 ms in the past.
		await api.setNetSim({ latencyMs: 100, jitterMs: 0, loss: 0 });
		const zone = gallery.client.authority;
		await until(() => zone.connection.clock.rtt > 180, 10_000, 'rtt with latency');
		await until(() => gallery.targets() && gallery.delay.settled(zone, 10), 15_000, 'adaptive delay');
		assert.ok(gallery.delay.value > 190, `delay ${gallery.delay.value}`);

		const on = await volley(gallery, 20, true);
		assert.ok(on.hits >= 18, `compensated hits ${on.hits}/20`);
		assert.equal(on.clamped, 0);
		const meanAge = on.ages.reduce((a, b) => a + b, 0) / on.ages.length;
		assert.ok(meanAge > 250 && meanAge < 500, `rewind age ${meanAge}`);

		const off = await volley(gallery, 20, false);
		assert.ok(off.hits <= 6, `uncompensated hits ${off.hits}/20`);
		assert.ok(off.ages.every((a) => a === 0), 'no rewind without compensation');

		// 300 ms one way: the view is ~700 ms old, more than the 500 ms clamp.
		await api.setNetSim({ latencyMs: 300, jitterMs: 0, loss: 0 });
		await until(() => zone.connection.clock.rtt > 550, 15_000, 'rtt 600');
		await until(() => gallery.targets() && gallery.delay.settled(zone, 10), 20_000, 'adaptive delay 2');
		const far = await volley(gallery, 10, true);
		t.diagnostic(
			`comp on ${on.hits}/20 (mean rewind ${meanAge.toFixed(0)} ms) · off ${off.hits}/20 · 300 ms: clamped ${far.clamped}/10, hits ${far.hits}/10`
		);
		assert.ok(far.clamped >= 9, `clamped ${far.clamped}/10`);
		assert.ok(far.requested.every((r) => r > 520), `requested ${far.requested}`);
		assert.ok(far.ages.every((a) => a <= 501), `ages ${far.ages}`);

		assert.equal(gallery.stats.resolved, 50);
		assert.equal(gallery.stats.predictedHits, 50);
		const me = gallery.client.render().find((e) => e.id === hello.entity);
		await until(
			() => gallery.client.render().find((e) => e.id === hello.entity)?.fields.shots === 50,
			5_000,
			'replicated shot counter'
		);
		assert.equal(me.fields.name, 'Sharpshooter');
	} finally {
		await api.setNetSim({ latencyMs: 0, jitterMs: 0, loss: 0 }).catch(() => undefined);
		gallery.client.close();
		await server.stop();
	}
});
