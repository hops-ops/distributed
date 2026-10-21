/**
 * Headless Mob Pit network panel (Node): the rooms on their own `celld
 * dev`, the services binary, a scripted player.
 *
 * Prediction: the own capsule runs `pit_step` compiled to WebAssembly and
 * the room on celld agrees with it bit for bit. Network simulation: the
 * services' `/dev/netsim` writes the rooms' dev settings and every room
 * delays its sockets both ways (the measured RTT rises, the room's debug
 * info reports it, and it drops again when switched off). Interest: the
 * room replicates only what is inside the `viewRadius` ring the page draws.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { BUTTON, isMob, playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit } from './helpers/mobpit.mjs';

/** The predicted `player` fields (the Rust `MoveState`). */
const stateFromFields = (f) => ({ x: f.x, y: f.y, vx: f.vx, vy: f.vy, dash: f.dash, cd: f.cd, z: f.z, vz: f.vz });

test('Mob Pit network panel: exact prediction, NetSim on celld rooms, interest ring', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Netter', storage: memoryStorage() });
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		const { client } = pit;
		await client.connect('pit.0.0');
		const hello = await pit.ready;
		assert.equal(hello.viewRadius, 24, 'the interest ring the page draws');
		await until(() => client.predictor?.active, 5_000, 'own entity bound');

		// Prediction: what the client showed equals the room's state bit for bit.
		const input = { mx: 90, my: -60, buttons: 0 };
		for (let i = 0; i < 30; i += 1) {
			client.sendInput(input);
			await sleep(1000 / 30);
		}
		const expected = stateFromFields(client.predictor.predicted());
		await until(() => client.predictor.pending.length === 0, 5_000, 'acks');
		const authoritative = stateFromFields(client.authority.replica.get(hello.entity).fields);
		assert.deepEqual(authoritative, expected, 'the room agrees with the WASM prediction');
		assert.deepEqual(stateFromFields(client.predictor.authoritative()), authoritative, 'the ghost is the replica');

		// A jump: up and down, predicted exactly too.
		let apex = 0;
		for (let i = 0; i < 30; i += 1) {
			client.sendInput({ mx: 40, my: 0, buttons: i < 3 ? BUTTON.jump : 0 });
			apex = Math.max(apex, client.predictor.predicted().z);
			await sleep(1000 / 30);
		}
		assert.ok(apex > 1, `jumped (apex ${apex})`);
		const landed = stateFromFields(client.predictor.predicted());
		await until(() => client.predictor.pending.length === 0, 5_000, 'acks');
		assert.deepEqual(stateFromFields(client.authority.replica.get(hello.entity).fields), landed, 'the jump agrees bit for bit');
		assert.equal(landed.z, 0, 'back on the floor');

		// NetSim: the rooms' setting raises the RTT through celld.
		const connection = client.authority.connection;
		await until(() => connection.clock.synced, 5_000, 'clock');
		const base = connection.clock.rtt;
		assert.deepEqual(await api.getNetSim(), { latencyMs: 0, jitterMs: 0, loss: 0 });
		assert.deepEqual(await api.setNetSim({ latencyMs: 80, jitterMs: 0, loss: 0 }), { latencyMs: 80, jitterMs: 0, loss: 0 });
		await until(() => connection.clock.samples.at(-1)?.rtt > base + 100, 20_000, 'rtt with netsim');
		const info = await eventually(async () => {
			const i = await pitApi.roomInfo('pit.0.0');
			return i.netsim?.latencyMs === 80 && i;
		}, 5_000, 'room reports the simulation');
		console.error(`rtt ${base.toFixed(1)} ms → ${connection.clock.samples.at(-1).rtt.toFixed(1)} ms with 80 ms one way (room ${info.zone})`);
		// Inputs still reach the room in order under the simulation.
		for (let i = 0; i < 15; i += 1) {
			client.sendInput({ mx: -90, my: 60, buttons: 0 });
			await sleep(1000 / 30);
		}
		const late = stateFromFields(client.predictor.predicted());
		await until(() => client.predictor.pending.length === 0, 5_000, 'acks under latency');
		assert.deepEqual(stateFromFields(client.authority.replica.get(hello.entity).fields), late, 'exact under latency too');
		await api.setNetSim({ latencyMs: 0, jitterMs: 0, loss: 0 });
		await until(() => connection.clock.samples.at(-1)?.rtt < base + 60, 10_000, 'rtt back down');

		// Interest: walk into a corner; every replicated mob is near the ring.
		const deadline = Date.now() + 15_000;
		while (Date.now() < deadline) {
			// Along +x first, then +y: beside the pillars, into the corner.
			const me = playerFields(pit.me());
			if (me.x > 17 && me.y > 17) break;
			client.sendInput(me.x <= 17 ? { mx: 127, my: 0, buttons: 0 } : { mx: 0, my: 127, buttons: 0 });
			await sleep(1000 / 30);
		}
		await sleep(500);
		const me = playerFields(pit.me());
		const mobs = client.render().filter((e) => isMob(e) && !e.stale && e.zoneId === 'pit.0.0');
		const layout = pit.pures.pitRoom({ seed: hello.seed, x: 0, y: 0 });
		assert.ok(mobs.length < layout.mobs.length, `far mobs culled (${mobs.length} of ${layout.mobs.length} replicated)`);
		for (const m of mobs) {
			assert.ok(Math.hypot(m.fields.x - me.x, m.fields.y - me.y) <= hello.viewRadius * 1.25 + 1, 'replicated mobs are inside the ring');
		}
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
