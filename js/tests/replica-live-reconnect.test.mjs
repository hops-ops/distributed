import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createDistributedReplica,
	createReplicaGraphqlTransport
} from '../dist/replica/index.js';
import { FakeScheduler, flush } from './fixtures/game-harness.mjs';
import { createWaiter, startGraphqlServer } from './fixtures/graphql-ws-server.mjs';
import { scripted, Todos, wireFrame } from './fixtures/replica-todos.mjs';

const row = (title) => [{ id: 'todo-1', title }];
const liveFrame = (position, title, live = {}) =>
	wireFrame({
		operation: 'live:todos',
		position,
		rows: row(title),
		live: { mode: 'resumable', ...live }
	});
const resumeAt = (position) => [
	{ projection: 'todos-projector', position, token: `resume:${position}` }
];
/** A lost socket, as the WebSocket transport reports it. */
const lost = () => Object.assign(new Error('socket lost'), { retryable: true });

test('a lost live stream reopens with its cursors and an HTTP refresh, without the caller', async () => {
	const { replica, scheduler, fetches, subscriptions } = scripted();
	const watch = replica.watch(Todos, {}, { live: true });
	const seen = [];
	watch.subscribe((snapshot) => seen.push(snapshot.live));
	const connection = [];
	const stopConnection = replica.subscribeLiveConnection((state) => connection.push(state));
	await flush();
	fetches[0].resolve(wireFrame({ position: '1', rows: row('first') }));
	await flush();
	subscriptions[0].observer.next(liveFrame('2', 'before', { reset: true }));
	assert.equal(watch.get().live, 'active');
	const fetchesBefore = fetches.length;

	subscriptions[0].observer.error(lost());
	assert.equal(watch.get().live, 'reconnecting');
	assert.deepEqual(watch.get().errors, [], 'a lost stream is not a query error');
	assert.equal(watch.get().status, 'ready');
	assert.deepEqual(watch.get().data.todos, row('before'), 'admitted data stays readable');
	assert.equal(replica.liveConnection, 'reconnecting');
	assert.deepEqual(connection, ['reconnecting']);
	assert.equal(subscriptions[0].closed, true);
	// The transport's trailing completion is ignored, as after a failure frame.
	subscriptions[0].observer.complete();
	assert.equal(fetches.length, fetchesBefore);

	await scheduler.advance(999);
	assert.equal(subscriptions.length, 1);
	await scheduler.advance(1);
	assert.equal(subscriptions.length, 2);
	assert.deepEqual(subscriptions[1].request.resume, resumeAt('2'));
	assert.equal(fetches.length, fetchesBefore + 1, 'the reopen refreshes over HTTP');
	assert.equal(watch.get().live, 'reconnecting', 'reopened, not yet delivering');
	fetches.at(-1).reject(new TypeError('fetch failed'));
	await flush();
	assert.deepEqual(watch.get().errors, [], 'a refresh failing while reconnecting is not a query error');

	subscriptions[1].observer.next(liveFrame('3', 'after'));
	assert.deepEqual(watch.get().data.todos, row('after'));
	assert.equal(watch.get().live, 'active');
	assert.deepEqual(watch.get().errors, []);
	assert.equal(seen.includes('error'), false);
	assert.equal(seen.includes('reconnecting'), true);
	assert.equal(seen.at(-1), 'active');
	assert.equal(replica.liveConnection, 'online');
	assert.deepEqual(connection, ['reconnecting', 'online']);
	stopConnection();

	// The admitted frame reset the backoff.
	subscriptions[1].observer.error(lost());
	await scheduler.advance(1000);
	assert.equal(subscriptions.length, 3);
	watch.destroy();
});

test('consecutive losses back off exponentially with jitter up to the cap', async () => {
	let random = 0;
	const { replica, scheduler, subscriptions } = scripted({ random: () => random });
	const watch = replica.watch(Todos, {}, { live: true });
	const expected = [1000, 2000, 4000, 8000, 16000, 30000, 30000];
	for (const [index, delay] of expected.entries()) {
		subscriptions[index].observer.error(lost());
		await scheduler.advance(delay - 1);
		assert.equal(subscriptions.length, index + 1, `attempt ${index + 1} waits ${delay} ms`);
		await scheduler.advance(1);
		assert.equal(subscriptions.length, index + 2);
	}
	random = 0.5;
	subscriptions.at(-1).observer.error(lost());
	const before = subscriptions.length;
	await scheduler.advance(22_499);
	assert.equal(subscriptions.length, before, 'half of the jitter span is subtracted');
	await scheduler.advance(1);
	assert.equal(subscriptions.length, before + 1);
	watch.destroy();
});

test('terminal live errors are never reopened', async () => {
	for (const error of [
		new Error('unauthorized'),
		Object.assign(new Error('refused'), { retryable: false }),
		'subscription error'
	]) {
		const { replica, scheduler, subscriptions } = scripted();
		const watch = replica.watch(Todos, {}, { live: true });
		subscriptions[0].observer.error(error);
		assert.equal(watch.get().live, 'error');
		await scheduler.advance(120_000);
		assert.equal(subscriptions.length, 1);
		watch.destroy();
	}
});

test('a pending reopen ends with the last watch and with the authorization generation', async () => {
	const { replica, scheduler, subscriptions } = scripted();
	const first = replica.watch(Todos, {}, { live: true });
	const second = replica.watch(Todos, {}, { live: true });
	subscriptions[0].observer.error(lost());
	first.destroy();
	await scheduler.advance(1000);
	assert.equal(subscriptions.length, 2, 'a remaining watch keeps the reopen');
	subscriptions[1].observer.error(lost());
	assert.equal(replica.liveConnection, 'reconnecting');
	second.destroy();
	assert.equal(replica.liveConnection, 'online', 'an unwatched operation is not reconnecting');
	await scheduler.advance(120_000);
	assert.equal(subscriptions.length, 2, 'no watch, no reopen');

	const third = replica.watch(Todos, {}, { live: true });
	assert.equal(subscriptions.length, 3);
	subscriptions[2].observer.error(lost());
	replica.invalidateAuthorization();
	assert.equal(replica.liveConnection, 'online');
	await scheduler.advance(120_000);
	assert.equal(subscriptions.length, 3);
	third.destroy();
});

test('whichever of the reopened stream and its HTTP refresh answers first, the cache never regresses', async () => {
	// Live first: the HTTP response started before it is fenced.
	{
		const { replica, scheduler, fetches, subscriptions } = scripted();
		const watch = replica.watch(Todos, {}, { live: true });
		await flush();
		fetches[0].resolve(wireFrame({ position: '1', rows: row('one') }));
		await flush();
		subscriptions[0].observer.next(liveFrame('2', 'two', { reset: true }));
		subscriptions[0].observer.error(lost());
		await scheduler.advance(1000);
		subscriptions[1].observer.next(liveFrame('4', 'four'));
		fetches.at(-1).resolve(wireFrame({ position: '3', rows: row('three') }));
		await flush();
		assert.deepEqual(watch.get().data.todos, row('four'));
		assert.equal(watch.get().live, 'active');
		watch.destroy();
	}
	// HTTP first: the stream is reopened from the HTTP result's cursors, and a
	// replayed frame of the stream it replaced changes nothing.
	{
		const { replica, scheduler, fetches, subscriptions } = scripted();
		const watch = replica.watch(Todos, {}, { live: true });
		await flush();
		fetches[0].resolve(wireFrame({ position: '1', rows: row('one') }));
		await flush();
		subscriptions[0].observer.next(liveFrame('2', 'two', { reset: true }));
		subscriptions[0].observer.error(lost());
		await scheduler.advance(1000);
		assert.equal(subscriptions.length, 2);
		fetches.at(-1).resolve(wireFrame({ position: '4', rows: row('four') }));
		await flush();
		assert.deepEqual(watch.get().data.todos, row('four'));
		assert.equal(subscriptions[1].closed, true);
		assert.deepEqual(subscriptions[2].request.resume, resumeAt('4'));
		subscriptions[1].observer.next(liveFrame('3', 'three'));
		assert.deepEqual(watch.get().data.todos, row('four'));
		subscriptions[2].observer.next(liveFrame('5', 'five'));
		assert.deepEqual(watch.get().data.todos, row('five'));
		assert.deepEqual(watch.get().errors, []);
		watch.destroy();
	}
});

test('a server restart resumes live updates over a real WebSocket without a manual refresh', { timeout: 60_000 }, async (t) => {
	const { wake, until } = createWaiter();
	const scheduler = new FakeScheduler();
	let http = () => wireFrame({ position: '1', rows: row('one') });
	let server = await startGraphqlServer({ http: (body) => http(body), notify: wake });
	// Registered first: a failure anywhere below must not leave the server open.
	t.after(() => server.stop());
	const { port } = server;
	const transport = createReplicaGraphqlTransport({
		getUrl: () => `http://127.0.0.1:${port}/graphql`,
		getAuth: () => ({ userId: 'alice', role: 'user' }),
		scheduler,
		random: () => 0
	});
	const replica = createDistributedReplica({ transport, scheduler, random: () => 0 });
	/** Finish the handshake of the server's only socket. */
	const accepted = async () => {
		await until(() => server.sockets[0]?.received[0]?.type === 'connection_init');
		server.sockets[0].send({ type: 'connection_ack' });
		return server.sockets[0];
	};
	const title = () => watch.get().data.todos?.[0]?.title;

	const watch = replica.watch(Todos, {}, { live: true });
	const snapshots = [];
	watch.subscribe((snapshot) => {
		snapshots.push(snapshot);
		wake();
	});
	// A leaderboard-style live document shares the socket.
	const board = [];
	const closeBoard = transport.live('subscription Board { board }', {}, {
		next: (result) => {
			board.push(result.data.board);
			wake();
		},
		error: () => wake(),
		complete: () => undefined
	});
	t.after(() => {
		watch.destroy();
		closeBoard();
	});

	const socket = await accepted();
	await until(() => socket.subscriptions.length === 2);
	assert.equal(server.sockets.length, 1, 'one socket carries both subscriptions');
	assert.deepEqual(socket.received[0].payload, {
		'x-user-id': 'alice',
		'x-roles': 'user'
	});
	const [todos, boardSubscription] = socket.subscriptions;
	assert.match(todos.payload.query, /TodosLive/);
	assert.notEqual(todos.id, boardSubscription.id);
	await until(() => title() === 'one');
	socket.send({
		type: 'next',
		id: todos.id,
		payload: liveFrame('2', 'two', { reset: true })
	});
	socket.send({ type: 'next', id: boardSubscription.id, payload: { data: { board: 1 } } });
	await until(() => title() === 'two' && board.length === 1);
	socket.send({ type: 'next', id: todos.id, payload: liveFrame('3', 'three') });
	await until(() => title() === 'three');
	assert.equal(watch.get().live, 'active');

	// The server process dies: no close frame, nothing listening.
	await server.stop();
	await until(() => watch.get().live === 'reconnecting');
	assert.deepEqual(watch.get().data.todos, row('three'));
	assert.deepEqual(watch.get().errors, []);

	// First reopen: still down. The dial and the refresh fail; the backoff doubles.
	const before = snapshots.length;
	await scheduler.advance(1000);
	await until(
		() =>
			snapshots.length > before &&
			watch.get().live === 'reconnecting' &&
			!watch.get().fetching
	);
	assert.deepEqual(watch.get().data.todos, row('three'));
	assert.deepEqual(watch.get().errors, [], 'the failed dial and refresh are not query errors');

	// The server is back with changes the client missed. Its HTTP answer is
	// held until the resubscription arrived, so the order is the test's.
	let answer;
	http = () => new Promise((resolve) => (answer = resolve));
	server = await startGraphqlServer({ port, http: (body) => http(body), notify: wake });
	await scheduler.advance(2000);
	const reopened = await accepted();
	await until(() => reopened.subscriptions.length === 1 && server.requests.length === 1);
	assert.deepEqual(
		reopened.subscriptions[0].payload.extensions.distributed.resume,
		{ cursors: resumeAt('3') },
		'the reopened stream resumes from the last admitted cursor'
	);
	assert.match(server.requests[0].query, /query Todos/, 'the reopen refreshes over HTTP');
	answer(wireFrame({ position: '5', rows: row('five') }));
	await until(() => title() === 'five');

	// The refresh moved the cursors: the stream continues from them, on the
	// same socket.
	await until(() => reopened.subscriptions.length === 2);
	assert.equal(server.sockets.length, 1);
	const current = reopened.subscriptions[1];
	assert.deepEqual(current.payload.extensions.distributed.resume, {
		cursors: resumeAt('5')
	});
	reopened.send({ type: 'next', id: current.id, payload: liveFrame('6', 'six') });
	await until(() => title() === 'six');
	assert.equal(watch.get().live, 'active');
	assert.deepEqual(watch.get().errors, []);
});
