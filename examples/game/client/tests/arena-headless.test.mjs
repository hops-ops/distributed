/**
 * Headless Arena client (Node, no browser): dev ticket → join → move with
 * prediction; the server agrees with the predicted position; NetSim control
 * changes the measured RTT. Starts the real `game-demos arena` binary.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createArenaClient } from '../src/arena/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { stateFromFields, step } from '../src/arena/movement.ts';
import { memoryStorage, sleep, startDemo, until } from './helpers/demo-server.mjs';

test('headless client joins the arena and moves with exact prediction', { timeout: 600_000 }, async () => {
	const server = await startDemo('arena');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Headless', storage: memoryStorage() });
		const arena = createArenaClient({ api, base: server.httpUrl });
		clients.push(arena.client);
		const { client } = arena;
		const welcome = await client.connect('arena-1');
		assert.equal(welcome.tickRate, 30);
		assert.deepEqual(welcome.schema.kinds.map((k) => k.name), ['player', 'drone', 'pillar']);
		const hello = await arena.ready;
		assert.equal(hello.viewRadius, 14);
		assert.ok(api.last.principal.startsWith('guest-'), 'server-generated principal');
		await until(() => client.predictor.active, 5_000, 'own entity bound');
		const start = stateFromFields(client.predictor.predicted());

		// 30 command frames forward + right at the server's tick rate.
		const input = { mx: 90, my: 90, buttons: 0 };
		let expected = start;
		for (let i = 0; i < 30; i += 1) {
			client.sendInput(input);
			expected = step(expected, input);
			await sleep(1000 / 30);
		}
		const predicted = stateFromFields(client.predictor.predicted());
		assert.deepEqual(predicted, expected, 'prediction applies the shared step immediately');
		await until(() => client.predictor.lastAck >= 30 && client.predictor.pending.length === 0, 5_000, 'acks');
		const server_ = stateFromFields(client.authority.replica.get(hello.entity).fields);
		assert.deepEqual(server_, expected, 'authoritative state equals the prediction bit for bit');
		assert.ok(Math.hypot(server_.x - start.x, server_.y - start.y) > 3, 'moved');

		// Interpolated world: name label, drones and global pillars are visible.
		await sleep(150);
		const rendered = client.render();
		const me = rendered.find((e) => e.id === hello.entity);
		assert.equal(me.fields.name, 'Headless');
		assert.ok(me.predicted);
		assert.equal(rendered.filter((e) => e.kindName === 'pillar').length, 9);
		assert.ok(rendered.some((e) => e.kindName === 'drone'));
		const stats = client.authority.connection.stats;
		assert.ok(stats.deltasIn > 10 && stats.deltaBytesIn > 0);

		// Server-side NetSim raises the RTT the clock measures.
		const connection = client.authority.connection;
		await until(() => connection.clock.synced, 5_000, 'clock');
		const base = connection.clock.rtt;
		await api.setNetSim({ latencyMs: 60, jitterMs: 0, loss: 0 });
		await until(() => connection.clock.samples.at(-1)?.rtt >= 110, 10_000, 'rtt with netsim');
		assert.ok(connection.clock.samples.at(-1).rtt > base + 100);
		await api.setNetSim({ latencyMs: 0, jitterMs: 0, loss: 0 });
		assert.deepEqual(await api.getNetSim(), { latencyMs: 0, jitterMs: 0, loss: 0 });

		// A second guest: interest management hides the first one while it is
		// beyond the view radius; walking toward it makes it appear,
		// interpolated (not predicted) with its display name.
		const other = createArenaClient({
			api: new DevApi({ base: server.httpUrl, name: () => 'Other', storage: memoryStorage() }),
			base: server.httpUrl
		});
		clients.push(other.client);
		await other.client.connect('arena-1');
		const otherHello = await other.ready;
		await until(() => other.client.predictor.active, 5_000, 'second entity bound');
		const sees = () =>
			other.client.render().some((e) => e.id === hello.entity && !e.predicted && e.fields.name === 'Headless');
		const deadline = Date.now() + 10_000;
		while (!sees() && Date.now() < deadline) {
			const at = stateFromFields(other.client.predictor.predicted());
			const dx = server_.x - at.x;
			const dy = server_.y - at.y;
			const d = Math.hypot(dx, dy);
			if (d > otherHello.viewRadius + 2) {
				assert.ok(!other.client.authority.replica.has(hello.entity), 'out of view radius: not replicated');
			}
			const k = d > 1 ? 127 / d : 0;
			other.client.sendInput({ mx: Math.round(dx * k), my: Math.round(dy * k), buttons: 0 });
			await sleep(1000 / 30);
		}
		assert.ok(sees(), 'remote player visible once within the view radius');
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
