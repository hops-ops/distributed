import assert from 'node:assert/strict';
import test from 'node:test';

import { createReplicaGraphqlTransport } from '../dist/replica/index.js';
import {
	createDistributedLeaderboardStore,
	loadDistributedLeaderboard
} from '../dist/sveltekit/index.js';
import { FakeScheduler } from './fixtures/game-harness.mjs';
import { createWaiter, startGraphqlServer } from './fixtures/graphql-ws-server.mjs';

const op = Object.freeze({
	leaderboard: 'xp',
	operation: 'top',
	document: 'query XpLeaderboard($first: Int) { xpLeaderboard(first: $first) { rank member score } }',
	liveDocument:
		'subscription XpLeaderboard_Live($first: Int) { xpLeaderboard(first: $first) { rank member score } }'
});

function jsonResponse(body, status = 200) {
	return { status, statusText: 'OK', text: async () => JSON.stringify(body) };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('replica-free documents share the transport endpoint and credential', async () => {
	const calls = [];
	const transport = createReplicaGraphqlTransport({
		getUrl: () => 'http://api.test/graphql',
		getAuth: async () => ({ accessToken: 'token-1' }),
		fetch: async (url, init) => {
			calls.push({ url, init, body: JSON.parse(init.body) });
			return jsonResponse({ data: { xpLeaderboard: [{ rank: 0, member: 'ana', score: 10 }] } });
		}
	});
	const result = await transport.query(op.document, { first: 5 });
	assert.deepEqual(result.data.xpLeaderboard[0].member, 'ana');
	assert.equal(calls[0].url, 'http://api.test/graphql');
	assert.equal(calls[0].init.headers.authorization ?? calls[0].init.headers.Authorization, 'Bearer token-1');
	assert.deepEqual(calls[0].body.variables, { first: 5 });

	const loaded = await loadDistributedLeaderboard(op, { first: 5 }, {
		url: 'http://api.test/graphql',
		auth: { accessToken: 'token-1' },
		fetch: async () => jsonResponse({ data: { xpLeaderboard: [] } })
	});
	assert.deepEqual(loaded, { xpLeaderboard: [] });
	await assert.rejects(
		loadDistributedLeaderboard(op, {}, {
			url: 'http://api.test/graphql',
			auth: {},
			fetch: async () => jsonResponse({ errors: [{ message: 'FORBIDDEN' }] })
		}),
		/FORBIDDEN/
	);
});

test('a leaderboard store starts from the server result and follows live pushes', async () => {
	let observer;
	let closed = 0;
	const transport = {
		query: async () => {
			throw new Error('initial data was given; no query expected');
		},
		live(document, variables, next) {
			assert.equal(document, op.liveDocument);
			assert.deepEqual(variables, { first: 2 });
			observer = next;
			return () => {
				closed += 1;
			};
		}
	};
	const initial = { xpLeaderboard: [{ rank: 0, member: 'ana', score: 10 }] };
	const store = createDistributedLeaderboardStore(transport, op, { first: 2 }, { initial, browser: true });
	const seen = [];
	const unsubscribe = store.subscribe((value) => seen.push(value));
	assert.equal(seen.at(-1).data, initial);
	assert.equal(seen.at(-1).live, 'connecting');
	observer.next({ data: { xpLeaderboard: [{ rank: 0, member: 'bob', score: 30 }] } });
	assert.equal(seen.at(-1).data.xpLeaderboard[0].member, 'bob');
	assert.equal(seen.at(-1).live, 'active');
	observer.error(new Error('socket closed'));
	assert.equal(seen.at(-1).live, 'error');
	unsubscribe();
	assert.equal(closed, 1);
});

test('without initial data the browser store queries; on the server it stays idle', async () => {
	let queries = 0;
	const transport = {
		query: async () => {
			queries += 1;
			return { data: { xpLeaderboard: [{ rank: 0, member: 'cat', score: 5 }] } };
		},
		live: () => () => undefined
	};
	const server = createDistributedLeaderboardStore(transport, op, {}, { browser: false });
	const serverValues = [];
	server.subscribe((value) => serverValues.push(value))();
	assert.equal(queries, 0);
	assert.equal(serverValues[0].data, undefined);

	const browser = createDistributedLeaderboardStore(transport, op, {}, { browser: true, live: false });
	const values = [];
	const stop = browser.subscribe((value) => values.push(value));
	await tick();
	assert.equal(queries, 1);
	assert.equal(values.at(-1).data.xpLeaderboard[0].member, 'cat');
	assert.equal(values.at(-1).live, 'off');
	stop();
});

/** A lost socket, as the WebSocket transport reports it. */
const lost = () => Object.assign(new Error('socket lost'), { retryable: true });
const board = (member) => ({ xpLeaderboard: [{ rank: 0, member, score: 1 }] });

/** A store over a scripted transport and a manual clock. */
function scriptedStore(options = {}) {
	const scheduler = new FakeScheduler();
	const streams = [];
	const queries = [];
	const transport = {
		query: () => new Promise((resolve) => queries.push(resolve)),
		live(_document, _variables, observer) {
			const stream = { observer, closed: false };
			streams.push(stream);
			return () => {
				stream.closed = true;
			};
		}
	};
	const store = createDistributedLeaderboardStore(transport, op, {}, {
		browser: true,
		scheduler,
		random: () => 0,
		...options
	});
	const seen = [];
	const unsubscribe = store.subscribe((value) => seen.push(value));
	return { scheduler, streams, queries, seen, unsubscribe };
}

test('a lost leaderboard stream reopens after a backoff and shows the new board', async () => {
	const { scheduler, streams, queries, seen, unsubscribe } = scriptedStore({
		initial: board('ana')
	});
	streams[0].observer.next({ data: board('bob') });
	streams[0].observer.error(lost());
	assert.equal(seen.at(-1).live, 'error');
	assert.equal(seen.at(-1).error, 'Error: socket lost');
	assert.deepEqual(seen.at(-1).data, board('bob'), 'the last board stays readable');
	assert.equal(streams[0].closed, true);

	await scheduler.advance(999);
	assert.equal(streams.length, 1);
	await scheduler.advance(1);
	assert.equal(streams.length, 2);
	assert.equal(seen.at(-1).live, 'connecting');
	// The stream's first frame is the current board: no HTTP refresh.
	assert.equal(queries.length, 0);
	streams[1].observer.next({ data: board('cat') });
	assert.deepEqual(seen.at(-1), { data: board('cat'), error: null, live: 'active' });
	// A late frame of the lost stream cannot replace the newer board.
	streams[0].observer.next({ data: board('bob') });
	assert.deepEqual(seen.at(-1).data, board('cat'));
	unsubscribe();
	assert.equal(streams[1].closed, true);
});

test('consecutive leaderboard losses back off with jitter; a frame resets the backoff', async () => {
	let random = 0;
	const { scheduler, streams, unsubscribe } = scriptedStore({
		initial: board('ana'),
		random: () => random
	});
	for (const [index, delay] of [1000, 2000, 4000].entries()) {
		streams[index].observer.error(lost());
		await scheduler.advance(delay - 1);
		assert.equal(streams.length, index + 1, `attempt ${index + 1} waits ${delay} ms`);
		await scheduler.advance(1);
		assert.equal(streams.length, index + 2);
	}
	random = 0.5;
	streams[3].observer.error(lost());
	await scheduler.advance(5999);
	assert.equal(streams.length, 4);
	await scheduler.advance(1);
	assert.equal(streams.length, 5, '8 s shortened by a quarter');
	random = 0;
	streams[4].observer.next({ data: board('bob') });
	streams[4].observer.error(lost());
	await scheduler.advance(1000);
	assert.equal(streams.length, 6);
	unsubscribe();
});

test('a terminal leaderboard error is not reopened, and the last unsubscribe cancels a reopen', async () => {
	const terminal = scriptedStore({ initial: board('ana') });
	terminal.streams[0].observer.error(
		Object.assign(new Error('unauthorized'), { retryable: false })
	);
	await terminal.scheduler.advance(60_000);
	assert.equal(terminal.streams.length, 1);
	assert.equal(terminal.seen.at(-1).live, 'error');
	terminal.unsubscribe();

	const stopped = scriptedStore({ initial: board('ana') });
	stopped.streams[0].observer.error(lost());
	stopped.unsubscribe();
	await stopped.scheduler.advance(60_000);
	assert.equal(stopped.streams.length, 1);
});

test('the initial query never replaces a board the live stream already delivered', async () => {
	const { scheduler, streams, queries, seen, unsubscribe } = scriptedStore();
	assert.equal(queries.length, 1);
	streams[0].observer.next({ data: board('live') });
	// The stream is lost before the slow query answers with an older board.
	streams[0].observer.error(lost());
	queries[0]({ data: board('stale') });
	await tick();
	assert.deepEqual(seen.at(-1).data, board('live'));
	await scheduler.advance(1000);
	assert.equal(queries.length, 1, 'a reopen does not query again');
	unsubscribe();
});

test('a leaderboard follows a server restart over a real WebSocket', { timeout: 60_000 }, async (t) => {
	const { wake, until } = createWaiter();
	const scheduler = new FakeScheduler();
	let server = await startGraphqlServer({ http: () => ({ data: board('http') }), notify: wake });
	// Registered first: a failure anywhere below must not leave the server open.
	t.after(() => server.stop());
	const { port } = server;
	const transport = createReplicaGraphqlTransport({
		getUrl: () => `http://127.0.0.1:${port}/graphql`,
		getAuth: () => ({ userId: 'alice', role: 'user' }),
		scheduler,
		random: () => 0
	});
	const store = createDistributedLeaderboardStore(transport, op, { first: 3 }, {
		initial: board('ana'),
		browser: true,
		scheduler,
		random: () => 0
	});
	let current;
	const unsubscribe = store.subscribe((value) => {
		current = value;
		wake();
	});
	t.after(() => unsubscribe());
	const accepted = async () => {
		await until(() => server.sockets[0]?.received[0]?.type === 'connection_init');
		server.sockets[0].send({ type: 'connection_ack' });
		await until(() => server.sockets[0].subscriptions.length === 1);
		return server.sockets[0];
	};
	const push = (socket, member) =>
		socket.send({
			type: 'next',
			id: socket.subscriptions[0].id,
			payload: { data: board(member) }
		});

	const first = await accepted();
	assert.equal(first.subscriptions[0].payload.query, op.liveDocument);
	push(first, 'bob');
	await until(() => current.live === 'active');
	assert.deepEqual(current.data, board('bob'));

	// The server process dies: no close frame, nothing listening.
	await server.stop();
	await until(() => current.live === 'error');
	assert.deepEqual(current.data, board('bob'));

	server = await startGraphqlServer({ port, http: () => ({ data: board('http') }), notify: wake });
	await scheduler.advance(1000);
	const second = await accepted();
	assert.deepEqual(second.subscriptions[0].payload.variables, { first: 3 });
	push(second, 'cat');
	await until(() => current.live === 'active');
	assert.deepEqual(current, { data: board('cat'), error: null, live: 'active' });
	assert.equal(server.requests.length, 0, 'the stream itself brings the current board');
});
