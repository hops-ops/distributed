import assert from 'node:assert/strict';
import test from 'node:test';

import {
	GAME_CLOSE_RESTARTED,
	GAME_PROTOCOL_VERSION,
	GameClient,
	GameConnection,
	GameErrorCode,
	GameFeature,
	GameKickCode
} from '@hops-ops/distributed/game';

import {
	FakeNetwork,
	FakeScheduler,
	avatar,
	delta,
	flush,
	welcome
} from './fixtures/game-harness.mjs';

/** A scripted zone: tickets `t:<zone>:<n>` admit; tokens rotate per Welcome. */
function scriptedServer(network, { graceTokens = true, closeOnError = true } = {}) {
	const tokens = new Set();
	let issued = 0;
	network.onMessage = (conn, message) => {
		if (message.type === 'Hello') {
			if (message.reconnectToken !== '') {
				if (!graceTokens || !tokens.delete(message.reconnectToken)) {
					conn.send({ type: 'Error', code: GameErrorCode.RECONNECT_FAILED, message: 'expired' });
					if (closeOnError) conn.close(1008);
					return;
				}
			} else if (!message.ticket.startsWith(`t:${conn.zoneId}`) && !message.ticket.startsWith('transfer:')) {
				conn.send({ type: 'Error', code: GameErrorCode.UNAUTHORIZED, message: 'bad ticket' });
				if (closeOnError) conn.close(1008);
				return;
			}
			issued += 1;
			const token = `rt-${issued}`;
			tokens.add(token);
			conn.send(welcome({ zoneId: conn.zoneId, reconnectToken: token, serverTime: 10_000 }));
		}
		if (message.type === 'Ping') {
			conn.send({ type: 'Pong', clientTime: message.clientTime, serverTime: 10_000 + message.clientTime });
		}
	};
	return tokens;
}

function connection(network, scheduler, overrides = {}) {
	const tickets = [];
	const conn = new GameConnection({
		url: 'ws://game.test/zones/z1',
		zoneId: 'z1',
		ticket: (request) => {
			tickets.push(request);
			return `t:z1:${tickets.length}`;
		},
		contentVersion: 'c1',
		capabilities: 5,
		socket: network.factory,
		scheduler,
		...overrides
	});
	return { conn, tickets };
}

test('handshake sends Hello with versions, capabilities and the app ticket', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const { conn, tickets } = connection(network, scheduler);
	const states = [];
	conn.on('state', (s) => states.push(s));
	const w = await conn.connect();
	assert.equal(w.zoneId, 'z1');
	assert.deepEqual(network.last().lastHello, {
		type: 'Hello',
		protocolVersion: GAME_PROTOCOL_VERSION,
		contentVersion: 'c1',
		capabilities: 5,
		ticket: 't:z1:1',
		reconnectToken: '',
		near: false
	});
	assert.deepEqual(tickets, [{ zoneId: 'z1', role: 'player', reason: 'join' }]);
	assert.deepEqual(states, ['connecting', 'open']);
	assert.equal(conn.reconnectToken, 'rt-1');
	conn.close();
	await flush();
	assert.deepEqual(network.last().inbox.at(-1), { type: 'Leave' }, 'clean close sends Leave');
	assert.equal(conn.state, 'closed');
});

test('an observer says it is near in Hello and with Near, without reconnecting', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, message) => {
		if (message.type === 'Hello') conn.send(welcome({ zoneId: conn.zoneId, role: 'observer', serverTime: 10_000 }));
	};
	const { conn } = connection(network, scheduler, { role: 'observer', near: true });
	await conn.connect();
	const server = network.last();
	assert.equal(server.lastHello.near, true);
	assert.equal(conn.near, true);
	conn.setNear(false);
	conn.setNear(false);
	conn.setNear(true);
	assert.deepEqual(
		server.inbox.filter((m) => m.type === 'Near'),
		[
			{ type: 'Near', near: false },
			{ type: 'Near', near: true }
		]
	);
	assert.equal(network.connections.length, 1, 'same socket');
	conn.close();
	// A player's nearness is never sent.
	network.onMessage = (c, message) => {
		if (message.type === 'Hello') c.send(welcome({ zoneId: c.zoneId, serverTime: 10_000 }));
	};
	const player = connection(network, scheduler).conn;
	await player.connect();
	player.setNear(true);
	assert.deepEqual(network.last().inbox.filter((m) => m.type === 'Near'), []);
	player.close();
});

test('an observer sends Focus throttled, and again after a reconnect', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, message) => {
		if (message.type === 'Hello') conn.send(welcome({ zoneId: conn.zoneId, role: 'observer', serverTime: 10_000 }));
	};
	const { conn } = connection(network, scheduler, { role: 'observer' });
	await conn.connect();
	const focus = () => network.last().inbox.filter((m) => m.type === 'Focus');
	conn.setFocus(-25, 0);
	// Within the interval, or moved too little: not sent.
	conn.setFocus(-24, 0);
	await scheduler.advance(250);
	conn.setFocus(-25.125, 0);
	conn.setFocus(-23, 1);
	assert.deepEqual(focus(), [
		{ type: 'Focus', x: -25, y: 0 },
		{ type: 'Focus', x: -23, y: 1 }
	]);
	conn.setFocus(Number.NaN, 0);
	assert.equal(focus().length, 2, 'non-finite focus is ignored');
	// A new connection hears the last focus right after its Welcome.
	network.last().close(1006, "lost");
	await scheduler.advance(5_000);
	assert.ok(network.connections.length >= 2, 'reconnected');
	assert.deepEqual(focus(), [{ type: 'Focus', x: -23, y: 1 }]);
	conn.close();
	// A player's focus is never sent.
	network.onMessage = (c, message) => {
		if (message.type === 'Hello') c.send(welcome({ zoneId: c.zoneId, serverTime: 10_000 }));
	};
	const player = connection(network, scheduler).conn;
	await player.connect();
	player.setFocus(1, 2);
	assert.deepEqual(network.last().inbox.filter((m) => m.type === 'Focus'), []);
	player.close();
});

/**
 * A server built before protocol 2 (what `distributed_game` answered then):
 * any Hello but version 1 gets `Error(VERSION_MISMATCH)` and a close,
 * before the ticket is looked at (`message`: native "protocol 2 != 1",
 * celld "protocol version"). Records the tickets it admitted.
 */
function v1OnlyServer(network, { message = 'protocol version', closeCode = 1008, role = 'player' } = {}) {
	const admitted = [];
	network.onMessage = (conn, m) => {
		if (m.type !== 'Hello') {
			// An old celld zone refuses unknown messages (e.g. Focus) and closes.
			if (m.type === 'Focus') {
				conn.send({ type: 'Error', code: GameErrorCode.BAD_MESSAGE, message: 'bad message' });
				conn.close(1008);
			}
			return;
		}
		if (m.protocolVersion !== 1) {
			conn.send({ type: 'Error', code: GameErrorCode.VERSION_MISMATCH, message });
			conn.close(closeCode);
			return;
		}
		admitted.push(m.ticket || m.reconnectToken);
		conn.send(
			welcome({ zoneId: conn.zoneId, role, protocolVersion: 1, features: 0, reconnectToken: `rt-${admitted.length}`, serverTime: 10_000 })
		);
	};
	return admitted;
}

test('offers protocol 2 and falls back once to 1 against a v1-only server', async () => {
	for (const [message, closeCode] of [
		['protocol version', 1008],
		['protocol 2 != 1', 1005]
	]) {
		const network = new FakeNetwork();
		const scheduler = new FakeScheduler();
		const admitted = v1OnlyServer(network, { message, closeCode, role: 'observer' });
		const url = `ws://old-${closeCode}.test/zones/z1`;
		const { conn, tickets } = connection(network, scheduler, { url, role: 'observer' });
		const errors = [];
		const disconnects = [];
		conn.on('error', (e) => errors.push(e));
		conn.on('disconnect', (d) => disconnects.push(d));
		const w = await conn.connect();
		assert.equal(w.protocolVersion, 1);
		assert.equal(conn.protocolVersion, 1);
		assert.deepEqual(
			network.connections.map((c) => c.lastHello.protocolVersion),
			[2, 1],
			'the same Hello again with 1'
		);
		assert.equal(network.connections[1].lastHello.ticket, network.connections[0].lastHello.ticket, 'same ticket');
		assert.equal(tickets.length, 1, 'no new ticket fetched');
		assert.deepEqual(admitted, ['t:z1:1']);
		assert.deepEqual(errors, [], 'the refusal is not surfaced');
		assert.deepEqual(disconnects, []);
		assert.equal(conn.state, 'open');
		// A v1 server has no FOCUS feature: Focus is never sent (an old
		// celld zone would close the connection on it).
		conn.setFocus(3, 4);
		await scheduler.advance(300);
		assert.deepEqual(network.last().inbox.filter((m) => m.type === 'Focus'), []);
		assert.equal(conn.hasFeature(GameFeature.FOCUS), false);
		// Remembered per host: a reconnect offers 1 at once.
		network.last().close(1006, 'lost');
		await scheduler.advance(5_000);
		assert.equal(conn.state, 'open');
		assert.deepEqual(
			network.connections.slice(2).map((c) => c.lastHello.protocolVersion),
			[1]
		);
		conn.close();
	}
});

test('a content VERSION_MISMATCH still asks for a reload, without a protocol fallback', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	network.onMessage = (conn, m) => {
		if (m.type === 'Hello') {
			conn.send({ type: 'Error', code: GameErrorCode.VERSION_MISMATCH, message: 'content version c1 != c2: reload' });
			conn.close(1008);
		}
	};
	const { conn } = connection(network, scheduler, { url: 'ws://new.test/zones/z1' });
	const reloads = [];
	conn.on('reloadRequired', (r) => reloads.push(r.reason));
	void conn.connect().catch(() => undefined);
	await scheduler.advance(10);
	assert.deepEqual(reloads, ['version-mismatch']);
	assert.deepEqual(network.connections.map((c) => c.lastHello.protocolVersion), [2]);
	conn.close();
});

test('setPosition focuses every observed zone that declares its origin', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	// Rooms 48 m wide: pit.<x>.<y> centred at (x, y) × 48; pit.9.9 is unplaced.
	const origin = (zone) => {
		const m = /^pit\.(-?\d+)\.(-?\d+)$/.exec(zone);
		return m === null || zone === 'pit.9.9' ? undefined : { x: Number(m[1]) * 48, y: Number(m[2]) * 48 };
	};
	network.onMessage = (conn, m) => {
		if (m.type !== 'Hello') return;
		const role = m.ticket.endsWith(':observer') ? 'observer' : 'player';
		const at = origin(conn.zoneId);
		conn.send(welcome({ zoneId: conn.zoneId, role, serverTime: 10_000, ...(at ? { origin: at } : {}) }));
	};
	const client = gameClient(network, scheduler);
	const focus = (zone) => network.last(zone).inbox.filter((m) => m.type === 'Focus').map(({ x, y }) => [x, y]);
	await client.connect('pit.0.0');
	await client.observe('pit.1.0');
	await client.observe('pit.9.9');
	// Local to the player's own room: (5, 3) there is (-43, 3) east.
	client.setPosition(5, 3, 'pit.0.0');
	assert.deepEqual(client.position, { x: 5, y: 3 });
	assert.deepEqual(focus('pit.1.0'), [[-43, 3]]);
	assert.deepEqual(focus('pit.9.9'), [], 'an unplaced zone is focused by hand only');
	assert.deepEqual(focus('pit.0.0'), [], 'the own zone is not an observer');
	// A zone observed later is focused right after its Welcome.
	await client.observe('pit.0.-1');
	assert.deepEqual(focus('pit.0.-1'), [[5, 51]]);
	// World coordinates work too.
	await scheduler.advance(250);
	client.setPosition(40, -10);
	assert.deepEqual(focus('pit.1.0').at(-1), [-8, -10]);
	assert.deepEqual(focus('pit.0.-1').at(-1), [40, 38]);
	// A zone whose origin is unknown cannot place a local position.
	client.setPosition(1, 1, 'pit.7.7');
	assert.deepEqual(client.position, { x: 40, y: -10 });
	client.close();
});

test('ping/pong estimates offset, rtt and jitter', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	const pongs = [];
	// Server clock = client clock + 10000; replies after a scripted delay.
	let delay = 40;
	network.onMessage = (conn, message) => {
		if (message.type === 'Hello') conn.send(welcome({ serverTime: 10_000 + scheduler.now() }));
		if (message.type === 'Ping') {
			const d = delay;
			scheduler.setTimeout(() => {
				conn.send({
					type: 'Pong',
					clientTime: message.clientTime,
					serverTime: message.clientTime + d / 2 + 10_000
				});
			}, d);
		}
	};
	const { conn } = connection(network, scheduler, { pingIntervalMs: 500 });
	conn.on('pong', (p) => pongs.push(p));
	await conn.connect();
	await scheduler.advance(600);
	assert.equal(pongs.length, 2);
	assert.equal(conn.clock.rtt, 40);
	assert.equal(conn.clock.offset, 10_000);
	delay = 80;
	await scheduler.advance(2000);
	assert.ok(conn.clock.rtt > 40 && conn.clock.rtt < 80, `smoothed rtt ${conn.clock.rtt}`);
	assert.ok(conn.clock.jitter > 0);
	assert.equal(conn.clock.offset, 10_000, 'symmetric delay keeps the offset exact');
	assert.equal(conn.serverNow(), scheduler.now() + 10_000);
	conn.close();
});

test('typed events surface Event/Freeze/Resume/Error and inputs are sequenced', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const { conn } = connection(network, scheduler);
	const seen = [];
	for (const type of ['event', 'freeze', 'resume', 'error', 'delta']) {
		conn.on(type, (m) => seen.push(m.type));
	}
	await conn.connect();
	const server = network.last();
	assert.equal(conn.sendInput(new Uint8Array([1, 0]), 7), 1);
	assert.equal(conn.sendInput(new Uint8Array([1, 1]), 8), 2);
	server.send({ type: 'Freeze', reason: 'content_update', version: 'c2', notes: '' });
	assert.equal(conn.sendInput(new Uint8Array([9])), undefined, 'no inputs while frozen');
	server.send({ type: 'Resume', reason: 'content_update', version: 'c2', notes: '' });
	server.send({ type: 'Event', typeId: 3, payload: new Uint8Array([1]) });
	server.send({ type: 'Error', code: GameErrorCode.RATE_LIMITED, message: 'slow down' });
	server.send(delta({ creates: [avatar(1, 3)] }));
	assert.equal(conn.state, 'open', 'post-join errors are advisory');
	assert.deepEqual(seen, ['Freeze', 'Resume', 'Event', 'Error', 'Delta']);
	const stats = conn.stats;
	assert.equal(stats.deltasIn, 1);
	assert.ok(stats.deltaBytesIn > 0 && stats.deltaBytesIn < stats.bytesIn);
	assert.equal(stats.messagesIn, 7, 'Welcome, Pong, Freeze, Resume, Event, Error, Delta');
	assert.equal(stats.messagesOut, 4, 'Hello, Ping and two inputs');
	assert.ok(stats.bytesOut > 0);
	assert.deepEqual(
		server.inputs().map((i) => [i.seq, i.clientTick, [...i.payload]]),
		[
			[1, 7, [1, 0]],
			[2, 8, [1, 1]]
		]
	);
	conn.close();
});

test('reconnects with the token inside the grace window, then falls back to a fresh ticket', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	const tokens = scriptedServer(network);
	const { conn, tickets } = connection(network, scheduler, {
		reconnect: { graceMs: 15_000, maxAttempts: 100, backoffMs: () => 1000 }
	});
	const welcomes = [];
	conn.on('welcome', (w, info) => welcomes.push({ token: w.reconnectToken, ...info }));
	await conn.connect();
	conn.sendInput(new Uint8Array([1]));

	// 1) Abrupt drop: resume with the token, empty ticket.
	network.last().close(1006);
	await scheduler.advance(10);
	let hello = network.last().lastHello;
	assert.equal(hello.ticket, '');
	assert.equal(hello.reconnectToken, 'rt-1');
	assert.deepEqual(welcomes.at(-1), { token: 'rt-2', resumed: true, reconnected: true });
	assert.equal(conn.lastSeq, 0, 'sequence numbers restart per connection');
	assert.equal(tickets.length, 1, 'no new ticket needed');

	// 2) Server unreachable longer than the grace window → fresh ticket.
	network.down = true;
	network.last().close(1006);
	await scheduler.advance(16_000);
	network.down = false;
	await scheduler.advance(1000);
	hello = network.last().lastHello;
	assert.equal(hello.reconnectToken, '');
	// Past the grace window every attempt uses a freshly fetched ticket.
	assert.ok(tickets.length > 2);
	assert.equal(hello.ticket, `t:z1:${tickets.length}`);
	assert.deepEqual(tickets.at(-1), { zoneId: 'z1', role: 'player', reason: 'rejoin' });
	assert.equal(conn.state, 'open');

	// 3) Token rejected by the server (RECONNECT_FAILED) → fresh ticket.
	tokens.clear();
	network.last().close(1006);
	await scheduler.advance(10);
	assert.equal(network.last().lastHello.reconnectToken, 'rt-3');
	assert.equal(conn.state, 'reconnecting');
	await scheduler.advance(1000);
	hello = network.last().lastHello;
	assert.equal(hello.reconnectToken, '');
	assert.equal(hello.ticket, `t:z1:${tickets.length}`);
	assert.equal(conn.state, 'open');
	assert.equal(welcomes.at(-1).resumed, false);
	conn.close();
});

test('a refused Hello ends the attempt even when the server never closes the socket', async () => {
	// E.g. a restarted zone refusing the old session's token: the error
	// arrives, the close may not (seen with celld room objects).
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	const tokens = scriptedServer(network, { closeOnError: false });
	const { conn, tickets } = connection(network, scheduler);
	await conn.connect();
	tokens.clear();
	network.last().close(1006);
	await scheduler.advance(10);
	assert.equal(network.last().lastHello.reconnectToken, 'rt-1', 'tried the token');
	await scheduler.advance(1000);
	const hello = network.last().lastHello;
	assert.equal(hello.reconnectToken, '', 'then a fresh ticket');
	assert.equal(hello.ticket, `t:z1:${tickets.length}`);
	assert.equal(conn.state, 'open');
	conn.close();

	// A refused first Hello rejects connect() instead of hanging.
	const refused = new GameConnection({
		url: 'ws://game.test/zones/z1',
		zoneId: 'z1',
		ticket: () => 'forged',
		socket: network.factory,
		scheduler,
		reconnect: false
	});
	await assert.rejects(refused.connect(), /bad ticket|rejected/);
	assert.equal(refused.state, 'closed');
});

test('a zone that restarted (close 1012) is rejoined at once with a fresh ticket', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const { conn, tickets } = connection(network, scheduler, {
		reconnect: { graceMs: 15_000, maxAttempts: 100, backoffMs: () => 1000 }
	});
	await conn.connect();
	assert.equal(conn.reconnectToken, 'rt-1');
	network.last().close(GAME_CLOSE_RESTARTED);
	await scheduler.advance(10);
	const hello = network.last().lastHello;
	assert.equal(hello.reconnectToken, '', 'the old session is gone: no token');
	assert.equal(hello.ticket, 't:z1:2');
	assert.deepEqual(tickets.at(-1), { zoneId: 'z1', role: 'player', reason: 'rejoin' });
	assert.equal(conn.state, 'open');
	conn.close();
});

test('kick and handshake rejection close without reconnecting', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const { conn } = connection(network, scheduler);
	const closes = [];
	conn.on('close', (info) => closes.push(info));
	await conn.connect();
	network.last().send({ type: 'Kick', code: GameKickCode.REPLACED, message: 'again' });
	network.last().close();
	await scheduler.advance(5000);
	assert.equal(conn.state, 'closed');
	assert.deepEqual(closes, [{ reason: 'kick', code: 2, message: 'again' }]);
	assert.equal(network.connections.length, 1);

	const bad = new GameConnection({
		url: 'ws://game.test/zones/z1',
		zoneId: 'z1',
		ticket: () => 'forged',
		socket: network.factory,
		scheduler
	});
	await assert.rejects(bad.connect(), (error) => error.info.reason === 'rejected' && error.info.code === 2);
});

test('a protocol violation drops the socket and resynchronizes via reconnect', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const { conn } = connection(network, scheduler);
	const errors = [];
	conn.on('protocolError', (e) => errors.push(e.code));
	await conn.connect();
	// Update for an entity the client never saw.
	network.last().send(delta({ creates: [avatar(1, 0)] }));
	const server = network.last();
	server.send({ type: 'Event', typeId: 1, payload: new Uint8Array() });
	conn.mirror.entities.clear(); // simulate divergence
	server.send(delta({ updates: [{ id: 1, mask: 1, fields: [3] }] }));
	await scheduler.advance(10);
	assert.deepEqual(errors, ['UNKNOWN_ENTITY']);
	assert.equal(network.last().lastHello.reconnectToken, 'rt-1');
	assert.equal(conn.state, 'open');
	conn.close();
});

/** Game client used for the multi-zone tests. */
function gameClient(network, scheduler, extra = {}) {
	return new GameClient({
		url: (zone) => `ws://game.test/zones/${zone}`,
		ticket: ({ zoneId, role }) => (role === 'observer' ? `t:${zoneId}:observer` : `t:${zoneId}:player`),
		socket: network.factory,
		scheduler,
		pingIntervalMs: 60_000,
		prediction: {
			encodeInput: (dx) => new Uint8Array([dx & 0xff, 0]),
			predict: (s, dx, dt) => ({ x: s.x + (dx * 10 * dt) / 1000 }),
			corrections: { avatar: { x: { mode: 'ease', durationMs: 100 }, hp: { mode: 'replace' } } },
			controlledEntity: ({ zoneId }) => (zoneId === 'z1' ? 7 : 70),
			// These tests check predicted values the instant they are made.
			smoothSteps: false
		},
		...extra
	});
}

test('game client predicts its entity, reconciles on ack and interpolates others', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const client = gameClient(network, scheduler, { inputDtMs: 100 });
	const corrections = [];
	client.on('correction', (c) => corrections.push(c));
	await client.connect('z1');
	const server = network.last('z1');
	// Welcome anchor: tick 100 @ 10000 ms, 30 Hz.
	server.send(delta({ serverTick: 100, creates: [avatar(7, 0), avatar(8, 0)] }));
	assert.equal(client.sendInput(1), 1);
	assert.equal(client.sendInput(1), 2);
	assert.deepEqual(server.inputs().map((i) => i.seq), [1, 2]);
	let me = client.render().find((e) => e.id === 7);
	assert.equal(me.predicted, true);
	assert.ok(Math.abs(me.fields.x - 2) < 1e-3, `predicted ${me.fields.x}`);

	// Server consumed seq 1 but the avatar only moved 0.5 (it hit something).
	server.send(
		delta({ serverTick: 103, ackSeq: 1, updates: [{ id: 7, mask: 1, fields: [0.5] }, { id: 8, mask: 1, fields: [30] }] })
	);
	assert.equal(corrections.length, 1);
	assert.ok(Math.abs(corrections[0].errors.x - 0.5) < 1e-3);
	assert.deepEqual(client.predictor.pending.map((p) => p.seq), [2]);
	me = client.render().find((e) => e.id === 7);
	assert.ok(Math.abs(me.fields.x - 2) < 1e-3, 'ease: no visual pop');
	await scheduler.advance(100);
	me = client.render().find((e) => e.id === 7);
	assert.ok(Math.abs(me.fields.x - 1.5) < 1e-3, `eased to rebased prediction, got ${me.fields.x}`);

	// Entity 8 is interpolated at serverNow - 100 ms between tick 100 and 103.
	const other = (t) => {
		scheduler.time = t;
		return client.render().find((e) => e.id === 8).fields.x;
	};
	// serverNow = local + 10000; render at serverNow - 100 ms (2 patches at
	// 20 Hz); tick 100 ↦ 10000 ms, tick 103 ↦ 10100 ms.
	assert.ok(Math.abs(other(150) - 15) < 1e-3, `midway ${other(150)}`);
	assert.ok(Math.abs(other(500) - 30) < 1e-3);
	client.close();
});

test('observer zones merge into the rendered world tagged by zone', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const client = gameClient(network, scheduler);
	await client.connect('z1');
	await client.observe('z2');
	assert.equal(network.last('z2').lastHello.ticket, 't:z2:observer');
	network.last('z1').send(delta({ creates: [avatar(1, 0)] }));
	network.last('z2').send(delta({ creates: [avatar(1, 900)] }));
	scheduler.time = 1000;
	const keys = client.render().map((e) => [e.key, e.authoritative]).sort();
	assert.deepEqual(keys, [
		['z1:1', true],
		['z2:1', false]
	]);
	client.unobserve('z2');
	await flush();
	assert.deepEqual(client.render().map((e) => e.key), ['z1:1']);
	client.close();
});

test('handoff connects to the target with the transfer ticket without blanking the world', async () => {
	const network = new FakeNetwork();
	const scheduler = new FakeScheduler();
	scriptedServer(network);
	const client = gameClient(network, scheduler, { handoffRetainMs: 300 });
	const events = [];
	client.on('handoff', (h) => events.push(['handoff', h.from, h.to]));
	client.on('handoffComplete', (h) => events.push(['complete', h.from, h.to]));
	client.on('authority', (zone) => events.push(['authority', zone]));
	await client.connect('z1');
	await client.observe('z2');
	const z1 = network.last('z1');
	const z2Observer = network.last('z2');
	z1.send(delta({ creates: [avatar(7, 99), avatar(8, 50)] }));
	z2Observer.send(delta({ creates: [avatar(70, 101), avatar(71, 120)] }));
	scheduler.time = 1000;
	assert.equal(client.render().length, 4);

	// Zone 1 hands the player to zone 2.
	z1.send({ type: 'Handoff', targetZone: 'z2', transferTicket: 'transfer:abc' });
	z1.close();
	await flush();
	const target = network.last('z2');
	assert.notEqual(target, z2Observer);
	assert.equal(target.lastHello.ticket, 'transfer:abc');
	assert.equal(client.authority.zoneId, 'z2');
	assert.equal(z2Observer.closed, true, 'observer replaced once the authority was welcomed');
	// Nothing disappears while the new authority has not sent creates yet.
	assert.deepEqual(
		client.render().map((e) => e.key).sort(),
		['z1:7', 'z1:8', 'z2:70', 'z2:71']
	);
	assert.ok(client.render().filter((e) => e.zoneId === 'z2').every((e) => e.stale));

	target.send(delta({ creates: [avatar(70, 102), avatar(71, 120)] }));
	assert.deepEqual(events, [
		['authority', 'z1'],
		['handoff', 'z1', 'z2'],
		['authority', 'z2'],
		['complete', 'z1', 'z2']
	]);
	const after = client.render();
	assert.ok(after.find((e) => e.key === 'z2:70').predicted, 'prediction rebinds to the new zone entity');
	assert.ok(after.some((e) => e.key === 'z1:8'), 'old zone retained briefly');
	await scheduler.advance(400);
	assert.deepEqual(client.render().map((e) => e.key).sort(), ['z2:70', 'z2:71']);
	assert.equal(client.sendInput(1), 1);
	assert.equal(target.inputs().length, 1);
	client.close();
});
