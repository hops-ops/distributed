/**
 * End-to-end against the real Rust zone server
 * (`distributed_game/examples/echo_arena.rs`, feature `net`).
 *
 * Opt-in: `DISTRIBUTED_GAME_E2E=1 npm test` (spawns `cargo run`, which may
 * compile on first use). Skipped otherwise.
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHmac, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

import { GameClient } from '@hops-ops/distributed/game';

const ENABLED = process.env.DISTRIBUTED_GAME_E2E === '1';
const SECRET = 'e2e-secret';
const REPO = fileURLToPath(new URL('../../', import.meta.url));

const b64url = (value) => Buffer.from(value).toString('base64url');

/** HS256 admission ticket exactly as PROTOCOL.md §8 specifies. */
function mintTicket(principal, zone, role = 'player') {
	const header = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
	const claims = b64url(
		JSON.stringify({
			sub: principal,
			zone,
			exp: Math.floor(Date.now() / 1000) + 300,
			nonce: randomUUID(),
			role
		})
	);
	const signature = createHmac('sha256', SECRET).update(`${header}.${claims}`).digest('base64url');
	return `${header}.${claims}.${signature}`;
}

function startServer() {
	const child = spawn(
		'cargo',
		['run', '-q', '-p', 'distributed_game', '--features', 'net', '--example', 'echo_arena'],
		{ cwd: REPO, env: { ...process.env, DISTRIBUTED_GAME_SECRET: SECRET }, stdio: ['pipe', 'pipe', 'inherit'] }
	);
	const url = new Promise((resolve, reject) => {
		let buffered = '';
		child.stdout.on('data', (chunk) => {
			buffered += chunk;
			const match = /LISTENING (ws:\/\/\S+)/.exec(buffered);
			if (match) resolve(match[1]);
		});
		child.on('exit', (code) => reject(new Error(`echo_arena exited with ${code}`)));
	});
	return { child, url };
}

async function until(predicate, timeoutMs = 10_000, what = 'condition') {
	const start = Date.now();
	while (!predicate()) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('game client against the real Rust zone server', { skip: !ENABLED, timeout: 600_000 }, async () => {
	const server = startServer();
	try {
		const base = await server.url;
		const DT = 1000 / 30; // server applies each input over one 30 Hz tick
		const client = new GameClient({
			url: (zone) => `${base}/zones/${zone}`,
			ticket: ({ zoneId, role }) => mintTicket('alice', zoneId, role),
			pingIntervalMs: 200,
			inputDtMs: DT,
			prediction: {
				encodeInput: ({ dx, dy }) => new Uint8Array([dx & 0xff, dy & 0xff]),
				predict: (s, i, dt) => ({
					x: s.x + (i.dx * 10 * dt) / 1000,
					y: s.y + (i.dy * 10 * dt) / 1000
				}),
				corrections: { avatar: { x: { mode: 'snap' }, y: { mode: 'snap' } } }
			}
		});
		let controlled;
		client.on('event', (_zone, event) => {
			if (event.typeId === 1) {
				controlled = new DataView(event.payload.buffer, event.payload.byteOffset).getUint32(0, true);
				client.setControlledEntity(controlled);
			}
		});
		const welcome = await client.connect('arena-1');
		assert.equal(welcome.role, 'player');
		assert.equal(welcome.schema.kinds[0].name, 'avatar');
		await until(() => controlled !== undefined && client.predictor.active, 10_000, 'avatar');

		// Prediction: inputs apply locally at once and agree with the server.
		for (let i = 0; i < 10; i += 1) {
			assert.equal(client.sendInput({ dx: 1, dy: 0 }), i + 1);
			await sleep(40);
		}
		const predictedNow = client.predictor.predicted().x;
		assert.ok(Math.abs(predictedNow - (10 * 10) / 30) < 0.01, `predicted ${predictedNow}`);
		await until(() => client.predictor.lastAck >= 10 && client.predictor.pending.length === 0, 10_000, 'acks');
		const authoritative = client.authority.replica.get(controlled).fields.x;
		assert.ok(Math.abs(authoritative - (10 * 10) / 30) < 0.01, `server x ${authoritative}`);
		assert.ok(Math.abs(client.predictor.predicted().x - authoritative) < 1e-6);

		// Clock sync converged on a real socket.
		const connection = client.authority.connection;
		await until(() => connection.clock.synced, 5_000, 'clock sync');
		assert.ok(connection.clock.rtt >= 0 && connection.clock.rtt < 1000);

		// Abrupt drop → resume with the reconnect token (same session).
		const session = connection.sessionId;
		const resumed = new Promise((resolve) =>
			connection.on('welcome', (w, info) => resolve({ w, info }))
		);
		connection.dropTransport();
		const { w, info } = await resumed;
		assert.equal(info.resumed, true);
		assert.equal(w.sessionId, session);
		await until(() => client.authority.replica.has(controlled), 5_000, 'recreate');
		assert.equal(client.sendInput({ dx: 0, dy: 1 }), 1, 'sequence restarted on the new socket');
		await until(() => client.predictor.lastAck >= 1 && client.predictor.pending.length === 0, 5_000, 'ack after resume');

		// Observer connection to a neighbouring zone, merged by zone tag.
		const observed = await client.observe('arena-2');
		assert.equal(observed.role, 'observer');
		await sleep(150);
		const rendered = client.render();
		const mine = rendered.find((e) => e.key === `arena-1:${controlled}`);
		assert.ok(mine?.predicted, 'own avatar rendered from prediction');
		assert.ok(rendered.every((e) => e.zoneId === 'arena-1' || e.zoneId === 'arena-2'));
		client.close();
	} finally {
		server.child.stdin.end();
		await new Promise((resolve) => {
			const timer = setTimeout(() => {
				server.child.kill('SIGKILL');
				resolve();
			}, 5_000);
			server.child.on('exit', () => {
				clearTimeout(timer);
				resolve();
			});
		});
	}
});
