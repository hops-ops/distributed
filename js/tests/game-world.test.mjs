import assert from 'node:assert/strict';
import test from 'node:test';

import {
	GameClient,
	GameConnection,
	GameErrorCode,
	GameKickCode
} from '@hops-ops/distributed/game';

import { FakeNetwork, FakeScheduler, avatar, delta, flush, welcome } from './fixtures/game-harness.mjs';

const host = (conn) => new URL(conn.url).host;

test('Redirect resends the same Hello to the owning host, at most 3 times', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, message) => {
		if (message.type !== 'Hello') return;
		if (host(conn) === 'lobby.test') {
			conn.send({ type: 'Redirect', zoneId: 'z1', url: 'ws://h2.test/zones/z1' });
			conn.close();
		} else if (host(conn) === 'h2.test') {
			conn.send(welcome());
		}
	};
	let tickets = 0;
	const conn = new GameConnection({
		url: 'ws://lobby.test/zones/z1',
		zoneId: 'z1',
		ticket: () => `ticket-${++tickets}`,
		socket: network.factory,
		scheduler
	});
	const redirects = [];
	conn.on('redirect', (r) => redirects.push(r.url));
	await conn.connect();
	assert.deepEqual(redirects, ['ws://h2.test/zones/z1']);
	assert.equal(conn.url, 'ws://h2.test/zones/z1');
	const [first, second] = network.connections;
	assert.deepEqual(second.lastHello, first.lastHello, 'identical Hello after redirect');
	assert.equal(tickets, 1, 'ticket not re-fetched');
	conn.close();

	// A redirect loop gives up after 3 hops.
	const loop = new FakeNetwork();
	loop.onMessage = (c, m) => {
		if (m.type === 'Hello') {
			c.send({ type: 'Redirect', zoneId: 'z1', url: `ws://h${loop.connections.length}.test/zones/z1` });
			c.close();
		}
	};
	const looping = new GameConnection({
		url: 'ws://h0.test/zones/z1',
		zoneId: 'z1',
		ticket: () => 't',
		socket: loop.factory,
		scheduler
	});
	await assert.rejects(looping.connect(), (e) => e.info.reason === 'too-many-redirects');
	assert.equal(loop.connections.length, 4);
});

test('Kick(RELOAD) waits for the app to reload, then resumes with the token and new version', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	let issued = 0;
	network.onMessage = (conn, m) => {
		if (m.type !== 'Hello') return;
		if (m.contentVersion !== 'c1' && m.contentVersion !== 'c2') {
			conn.send({ type: 'Error', code: GameErrorCode.VERSION_MISMATCH, message: 'content c2 required: reload' });
			conn.close(1008);
			return;
		}
		issued += 1;
		conn.send(welcome({ reconnectToken: `rt-${issued}`, contentVersion: m.contentVersion }));
	};
	const reloads = [];
	const conn = new GameConnection({
		url: 'ws://game.test/zones/z1',
		zoneId: 'z1',
		ticket: () => 'fresh',
		contentVersion: 'c1',
		socket: network.factory,
		scheduler,
		reloadContent: async (request) => {
			reloads.push(request);
			return request.version ?? 'c2';
		}
	});
	const closes = [];
	conn.on('close', (i) => closes.push(i));
	await conn.connect();
	const server = network.last();
	server.send({ type: 'Freeze', reason: 'content_update', version: 'c2', notes: 'patching' });
	server.send({ type: 'Resume', reason: 'content_update', version: 'c2', notes: '' });
	server.send({ type: 'Kick', code: GameKickCode.RELOAD, message: 'content c2 required' });
	server.close();
	await scheduler.advance(10);
	assert.deepEqual(reloads, [{ version: 'c2', reason: 'kick', message: 'content c2 required' }]);
	const hello = network.last().lastHello;
	assert.equal(hello.contentVersion, 'c2');
	assert.equal(hello.reconnectToken, 'rt-1', 'player held for the grace window');
	assert.equal(hello.ticket, '');
	assert.equal(conn.state, 'open');
	assert.deepEqual(closes, [], 'RELOAD is not session-terminal');
	conn.close();
});

test('a stale fresh join gets VERSION_MISMATCH and rejoins after a manual reload', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type !== 'Hello') return;
		if (m.contentVersion !== 'c2') {
			conn.send({ type: 'Error', code: GameErrorCode.VERSION_MISMATCH, message: 'stale content: reload' });
			conn.close(1008);
			return;
		}
		conn.send(welcome({ contentVersion: 'c2' }));
	};
	const conn = new GameConnection({
		url: 'ws://game.test/zones/z1',
		zoneId: 'z1',
		ticket: () => 'join-ticket',
		initialTicket: 'join-ticket',
		contentVersion: 'c1',
		socket: network.factory,
		scheduler
	});
	const required = [];
	conn.on('reloadRequired', (r) => required.push(r.reason));
	const joined = conn.connect();
	await scheduler.advance(10);
	assert.equal(conn.state, 'reload-required');
	assert.deepEqual(required, ['version-mismatch']);
	conn.resumeAfterReload('c2');
	const w = await joined;
	assert.equal(w.contentVersion, 'c2');
	assert.equal(network.last().lastHello.ticket, 'join-ticket', 'unconsumed ticket reused');
	conn.close();
});

function handoffClient(network, scheduler) {
	return new GameClient({
		url: (zone) => `ws://game.test/zones/${zone}`,
		ticket: ({ zoneId }) => `fresh:${zoneId}`,
		socket: network.factory,
		scheduler,
		pingIntervalMs: 60_000,
		handoffRetainMs: 100,
		handoffTimeoutMs: 2000,
		prediction: {
			encodeInput: (dx, ctx) => new Uint8Array([dx & 0xff, ctx.interpolationDelayMs]),
			predict: (s, dx, dt) => ({ x: s.x + (dx * dt) / 1000 }),
			controlledEntity: () => 1
		}
	});
}

test('refused handoff returns to the source with the same Hello, then falls back to a fresh ticket', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	let sourceAcceptsReturn = true;
	network.onMessage = (conn, m) => {
		if (m.type !== 'Hello') return;
		if (conn.zoneId === 'z2') {
			conn.send({ type: 'Error', code: GameErrorCode.ZONE_FULL, message: 'full' });
			conn.close(1008);
		} else if (m.ticket === 'transfer:t1' && !sourceAcceptsReturn) {
			conn.send({ type: 'Error', code: GameErrorCode.UNAUTHORIZED, message: 'used' });
			conn.close(1008);
		} else {
			conn.send(welcome({ zoneId: conn.zoneId }));
		}
	};
	const client = handoffClient(network, scheduler);
	const failures = [];
	client.on('handoffFailed', (f) => failures.push([f.stage, f.to]));
	await client.connect('z1');
	network.last('z1').send(delta({ creates: [avatar(1, 5)] }));

	network.last('z1').send({ type: 'Handoff', targetZone: 'z2', transferTicket: 'transfer:t1' });
	network.last('z1').close();
	await scheduler.advance(10);
	assert.equal(network.last('z2').lastHello.ticket, 'transfer:t1');
	assert.equal(network.last('z1').lastHello.ticket, 'transfer:t1', 'same Hello back to the source');
	assert.deepEqual(failures, [['target', 'z2']]);
	assert.equal(client.authority.zoneId, 'z1');
	assert.equal(client.authority.connection.state, 'open');
	assert.ok(client.render().some((e) => e.key === 'z1:1'), 'world kept rendering');
	client.close();

	// Source refuses the return too → fresh ticket from the app.
	sourceAcceptsReturn = false;
	const net2 = new FakeNetwork();
	net2.onMessage = network.onMessage;
	const client2 = handoffClient(net2, scheduler);
	const failures2 = [];
	client2.on('handoffFailed', (f) => failures2.push(f.stage));
	await client2.connect('z1');
	net2.last('z1').send({ type: 'Handoff', targetZone: 'z2', transferTicket: 'transfer:t1' });
	net2.last('z1').close();
	await scheduler.advance(10);
	assert.deepEqual(failures2, ['target', 'return']);
	assert.equal(net2.last('z1').lastHello.ticket, 'fresh:z1');
	assert.equal(client2.authority.connection.state, 'open');
	client2.close();
});

test('a silent handoff target times out into the return path', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type === 'Hello' && conn.zoneId !== 'z2') conn.send(welcome({ zoneId: conn.zoneId }));
	};
	const client = handoffClient(network, scheduler);
	const failures = [];
	client.on('handoffFailed', (f) => failures.push(f.reason));
	await client.connect('z1');
	network.last('z1').send({ type: 'Handoff', targetZone: 'z2', transferTicket: 'transfer:t9' });
	network.last('z1').close();
	await scheduler.advance(1000);
	assert.deepEqual(failures, []);
	await scheduler.advance(1500);
	assert.deepEqual(failures, ['timeout']);
	assert.equal(network.last('z2').closed, true);
	assert.equal(client.authority.zoneId, 'z1');
	assert.equal(network.last('z1').lastHello.ticket, 'transfer:t9');
	client.close();
});

test('interpolation delay and view tick are exposed for lag compensation', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type === 'Hello') conn.send(welcome({ serverTime: 10_000 }));
	};
	const client = handoffClient(network, scheduler);
	await client.connect('z1');
	assert.equal(client.authority.replica.interpolationDelayMs, 100, '2 patches at 20 Hz');
	network.last('z1').send(delta({ creates: [avatar(1, 0)] }));
	scheduler.time = 1000; // serverNow = 11000 → tick 100 + 30
	const ctx = client.inputContext();
	assert.deepEqual(ctx, { zoneId: 'z1', interpolationDelayMs: 100, viewTick: 127, clientTick: 130 });
	client.sendInput(1);
	const [input] = network.last('z1').inputs();
	assert.equal(input.clientTick, 130);
	assert.deepEqual([...input.payload], [1, 100], 'encodeInput received the context');
	client.close();
});
