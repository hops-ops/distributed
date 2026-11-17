/**
 * Headless Mob Pit ranged combat (Node): rooms on their own `celld dev`,
 * the services binary, a scripted archer under simulated latency.
 *
 * The archer aims at mobs *as rendered* (interpolated, behind the room by
 * the round trip and the interpolation delay) and looses arrows. With lag
 * compensation the room rewinds the mobs to the archer's view time and
 * confirms what the archer saw; without it the verdict uses the latest
 * state (no rewind). Predicted (client) and confirmed (room) hits are
 * compared shot by shot.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob, mobFields, PIT_RULES, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit } from './helpers/mobpit.mjs';

/** Loose `n` arrows at the nearest rendered mob, each when the client sees a clear hit. */
async function volley(pit, n, compensated) {
	const records = [];
	const deadline = Date.now() + 60_000;
	while (records.length < n) {
		assert.ok(Date.now() < deadline, 'volley');
		await sleep(1000 / 30);
		const me = pit.me();
		if (me === undefined) continue;
		const at = playerFields(me);
		const mob = pit.client
			.render()
			.filter((e) => isMob(e) && !e.stale && e.zoneId === pit.room())
			.map((e) => ({ e, m: mobFields(e) }))
			.sort((a, b) => Math.hypot(a.m.x - at.x, a.m.y - at.y) - Math.hypot(b.m.x - at.x, b.m.y - at.y))[0];
		if (mob === undefined) continue;
		const eye = at.z + PIT_RULES.eyeHeight;
		const mid = (PIT_RULES.mobHeights[mob.m.tier] ?? 1) / 2;
		const d = Math.hypot(mob.m.x - at.x, mob.m.y - at.y);
		const yaw = Math.atan2(mob.m.y - at.y, mob.m.x - at.x);
		const pitch = Math.atan2(mid - eye, d);
		// First person: from the eye, pitched at the mob's middle.
		const shot = pit.aimShot(yaw, pitch, compensated, true);
		if (shot === undefined) continue;
		pit.client.sendInput({ mx: 0, my: 0, ...shot.input });
		if (shot.record.predicted === mob.e.id) records.push(shot.record);
		// The bow's cooldown.
		await sleep((PIT_RULES.fireCooldown * 1000) / 30 + 20);
	}
	const verdicts = await Promise.all(records.map((r) => pit.verdict(r.id)));
	return records.map((r, i) => ({ predicted: r.predicted, ...verdicts[i] }));
}

test('arrows: lag compensation confirms what the archer saw', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Archer', storage: memoryStorage() });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl, neighbours: false });
		clients.push(pit);
		await pit.client.connect('pit.0.0');
		await pit.ready;
		await until(() => pit.client.predictor?.active, 5_000, 'bound');
		await api.setNetSim({ latencyMs: 80, jitterMs: 0, loss: 0 });
		await sleep(2500);

		const on = await volley(pit, 8, true);
		const hits = on.filter((v) => v.hit === v.predicted).length;
		console.error(`compensated: ${hits}/${on.length} confirmed as seen, rewinds ${on.map((v) => v.ageMs.toFixed(0)).join(' ')} ms`);
		assert.ok(hits >= 6, `${hits}/${on.length} compensated arrows confirmed`);
		assert.ok(on.every((v) => v.compensated && v.ageMs > 60), 'rewound to the view time');
		assert.ok(pit.rangedStats.agreed >= hits);

		const off = await volley(pit, 4, false);
		console.error(`uncompensated: ${off.filter((v) => v.hit === v.predicted).length}/${off.length} confirmed`);
		assert.ok(off.every((v) => !v.compensated && v.ageMs === 0), 'no rewind without compensation');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
