/**
 * Headless run of The Freeze against the real `game-demos freeze` binary
 * (primary process + the second host it spawns). Two players in Ember
 * Fields (second host): Ana patches content live, Bo is a pinned old
 * client.
 *
 * 1. Rolling rollout v1 → v2 across both hosts: Ana patches, Bo keeps
 *    playing v1 in the field and reloads only when it walks into the hub
 *    gate (Ana walks in without a reload).
 * 2. Hard rollout v2 → v3: inputs during the freeze are dropped and
 *    counted, every client reloads at once.
 * 3. A stuck zone (ritual in Ember Fields) aborts the next rollout:
 *    everyone resumes on v3 with "update aborted".
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createFreezeClient, FreezeAdmin, ZONES } from '../src/freeze/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, startDemo, until } from './helpers/demo-server.mjs';

const ADMIN = 'freeze-test-admin';

async function player(base, name, pinned) {
	const api = new DevApi({ base, name: () => name, storage: memoryStorage() });
	return createFreezeClient({ api, base, pinned: () => pinned });
}

/** Steer with command frames toward (x, y) until `done()`. */
async function walk(p, [x, y], done, timeoutMs = 15_000) {
	const deadline = Date.now() + timeoutMs;
	while (!done()) {
		assert.ok(Date.now() < deadline, `walk to ${x},${y} timed out`);
		const fields = p.client.predictor?.active ? p.client.predictor.predicted() : undefined;
		if (fields !== undefined) {
			const at = { x: Number(fields.x), y: Number(fields.y) };
			const dx = x - at.x;
			const dy = y - at.y;
			const d = Math.hypot(dx, dy);
			const k = d > 0.01 ? 127 / d : 0;
			p.sendInput({ mx: Math.round(dx * k), my: Math.round(dy * k), buttons: 0 });
		}
		await sleep(1000 / 30);
	}
}

test('world barrier across two hosts: rolling + safe room, hard reload, aborted rollout', { timeout: 600_000 }, async (t) => {
	const server = await startDemo('freeze', { env: { DG_DEMO_ADMIN_SECRET: ADMIN, DG_FREEZE_STAGE_MS: '700' } });
	const base = server.httpUrl;
	const admin = new FreezeAdmin(base, ADMIN);
	const players = [];
	try {
		const ana = await player(base, 'Ana', false);
		const bo = await player(base, 'Bo', true);
		players.push(ana, bo);
		assert.equal(ana.pack().version, 'v1');
		assert.equal(ana.pack().hazard, 'lava');
		for (const p of [ana, bo]) {
			const hello = p.nextHello();
			await p.client.connect(ZONES.ember);
			const h = await hello;
			assert.equal(h.host, 'beta', 'Ember Fields runs on the second process');
			assert.equal(h.contentVersion, 'v1');
			await until(() => p.client.predictor.active, 5_000, 'bound');
		}
		assert.match(bo.client.authority.connection.url, /\/zones\/freeze-ember$/);
		assert.notEqual(new URL(bo.client.authority.connection.url).port, new URL(base).port, 'redirected to beta');
		await assert.rejects(new FreezeAdmin(base, 'wrong').rollout('hard'), /401/);

		// ---- 1. Rolling (safe room) v1 → v2 ----
		const rolling = await admin.rollout('rolling');
		assert.equal(rolling.ok, true, JSON.stringify(rolling));
		assert.equal(rolling.version, 'v2');
		await until(() => ana.lastResume()?.version === 'v2' && bo.lastResume()?.version === 'v2', 5_000, 'resume v2');
		assert.ok(ana.lastResume().notes.some((n) => n.includes('Lava is now ice')));
		await until(() => ana.client.contentVersion === 'v2' && ana.pack().hazard === 'ice', 5_000, 'ana patched');
		assert.equal(ana.lastResume().patched, true);
		assert.equal(bo.lastResume().patched, false);
		assert.equal(bo.client.contentVersion, 'v1', 'pinned client keeps its build');
		assert.equal(bo.pack().hazard, 'lava');
		assert.ok(bo.stale());
		assert.deepEqual([ana.reloads.length, bo.reloads.length], [0, 0], 'nobody reloaded in the field');
		// The stale client keeps playing in the field zone: inputs are acked.
		const ack = bo.client.predictor.lastAck;
		for (let i = 0; i < 10; i += 1) {
			bo.sendInput({ mx: 0, my: 40, buttons: 0 });
			await sleep(1000 / 30);
		}
		await until(() => bo.client.predictor.lastAck >= ack + 10, 5_000, 'stale inputs acked');
		assert.equal(bo.client.authority.zoneId, ZONES.ember);

		// Both walk west into the hub gate (cross-host handoff beta → alpha).
		const portal = [-16.5, 5];
		await Promise.all(
			[ana, bo].map((p) => walk(p, portal, () => p.hello()?.zone === ZONES.hub && p.client.authority?.connection.state === 'open'))
		);
		await until(() => bo.reloads.length === 1 && bo.client.contentVersion === 'v2', 10_000, 'bo reloaded at the gate');
		assert.deepEqual(bo.reloads, [{ zone: ZONES.hub, version: 'v2' }], 'reload happened at the hub');
		await until(() => bo.client.authority.connection.state === 'open' && !bo.stale(), 10_000, 'bo back');
		assert.equal(bo.pack().hazard, 'ice');
		assert.equal(ana.reloads.length, 0, 'an up-to-date client enters the gate without reloading');
		assert.equal(bo.hello().host, 'alpha');

		// ---- 2. Hard v2 → v3: inputs during the freeze are dropped ----
		const hard = admin.rollout('hard');
		await until(() => ana.frozen() !== undefined && bo.frozen() !== undefined, 5_000, 'frozen');
		assert.equal(ana.frozen().version, 'v3');
		assert.equal(ana.client.authority.connection.canSendInput(), false);
		for (let i = 0; i < 5; i += 1) {
			assert.equal(ana.sendInput({ mx: 127, my: 0, buttons: 0 }), undefined);
			await sleep(20);
		}
		assert.ok(ana.droppedWhileFrozen() >= 5);
		const hardResult = await hard;
		assert.equal(hardResult.ok, true, JSON.stringify(hardResult));
		assert.equal(hardResult.version, 'v3');
		await until(
			() => [ana, bo].every((p) => p.client.contentVersion === 'v3' && p.client.authority?.connection.state === 'open'),
			10_000,
			'everyone reloaded to v3'
		);
		assert.deepEqual(ana.reloads, [{ zone: ZONES.hub, version: 'v3' }]);
		assert.deepEqual(bo.reloads.at(-1), { zone: ZONES.hub, version: 'v3' });
		assert.equal(ana.pack().name, 'Bloom Update');

		// Facts on both hosts carry the version they were produced under.
		const world = await admin.world();
		assert.equal(world.version, 'v3');
		const facts = world.hosts.flatMap((h) => (h.facts ?? []).map((f) => ({ host: h.host, ...f })));
		for (const [host, zone] of [
			['alpha', ZONES.hub],
			['beta', ZONES.ember]
		]) {
			for (const version of ['v2', 'v3']) {
				assert.ok(
					facts.some(
						(f) => f.host === host && f.command === 'freeze.content_applied' && f.stamp.zone === zone && f.stamp.contentVersion === version
					),
					`${host}/${zone} ${version} fact: ${JSON.stringify(facts)}`
				);
			}
		}
		assert.ok(world.hosts.every((h) => h.version === 'v3' && h.zones.every((z) => z.state === 'running')));
		assert.ok(ana.log.some((e) => e.kind === 'content' && e.stamp?.contentVersion === 'v3'), 'stamped content event');

		// ---- 3. Stuck zone → aborted rollout, everyone stays on v3 ----
		const stuck = await admin.setStuck(ZONES.ember, true);
		assert.equal(stuck.host, 'beta');
		const aborted = await admin.rollout('hard');
		assert.equal(aborted.ok, false);
		assert.equal(aborted.error, 'not_acked');
		assert.deepEqual(aborted.missing, [`beta/${ZONES.ember}`]);
		await until(() => ana.lastResume()?.aborted === true && bo.lastResume()?.aborted === true, 5_000, 'abort notice');
		assert.equal(ana.lastResume().version, 'v3');
		assert.equal(ana.client.contentVersion, 'v3');
		assert.equal(ana.reloads.length, 1, 'no reload on abort');
		const after = await admin.world();
		assert.ok(after.hosts.every((h) => h.version === 'v3'));
		await admin.setStuck(ZONES.ember, false);
		t.diagnostic(`dropped while frozen: ana ${ana.droppedWhileFrozen()} · rollouts ${after.rollouts.map((r) => `${r.version}${r.ok ? '' : ' (aborted)'}`).join(', ')}`);
	} finally {
		for (const p of players) p.client.close();
		await server.stop();
	}
});
