import assert from 'node:assert/strict';
import test from 'node:test';

import { createGraphqlWsConnection, GraphqlWsError } from '../dist/index.js';
import { FakeScheduler, flush } from './fixtures/game-harness.mjs';

function fakeWebSocket() {
	return class FakeWebSocket {
		static OPEN = 1;
		static instances = [];
		readyState = 0;
		sent = [];
		closeCount = 0;

		constructor(url, protocol) {
			this.url = url;
			this.protocol = protocol;
			FakeWebSocket.instances.push(this);
		}

		send(value) {
			this.sent.push(JSON.parse(value));
		}

		close() {
			this.closeCount += 1;
			this.readyState = 3;
		}

		/** Complete the handshake as the server does. */
		accept() {
			this.readyState = FakeWebSocket.OPEN;
			this.onopen?.({});
			this.message({ type: 'connection_ack' });
		}

		message(message) {
			this.onmessage?.({ data: JSON.stringify(message) });
		}

		/** The server (or the network) closed the socket. */
		drop(code = 1006, reason = '') {
			this.readyState = 3;
			this.onclose?.({ code, reason, wasClean: code !== 1006 });
		}

		subscribed() {
			return this.sent.filter((message) => message.type === 'subscribe');
		}
	};
}

function harness(options = {}) {
	const WebSocket = fakeWebSocket();
	const scheduler = new FakeScheduler();
	const connection = createGraphqlWsConnection({
		getUrl: () => '/graphql',
		webSocket: WebSocket,
		scheduler,
		random: () => 0,
		...options
	});
	const subscribe = (document = 'subscription A { a }', subscribeOptions) => {
		const events = [];
		const stop = connection.subscribe(
			document,
			{
				onNext: (result) => events.push({ next: result }),
				onError: (error) => events.push({ error }),
				onComplete: () => events.push({ complete: true })
			},
			subscribeOptions
		);
		return { events, stop };
	};
	return { connection, scheduler, sockets: WebSocket.instances, subscribe };
}

test('subscriptions with one credential share one socket under their own operation ids', async () => {
	const { scheduler, sockets, subscribe } = harness();
	const a = subscribe('subscription A { a }', { variables: { room: 'x' } });
	const b = subscribe('subscription B { b }');
	await flush();
	assert.equal(sockets.length, 1);
	const socket = sockets[0];
	assert.equal(socket.protocol, 'graphql-transport-ws');
	assert.deepEqual(socket.sent, [], 'nothing is sent before the socket opens');
	socket.accept();
	assert.deepEqual(socket.sent, [
		{ type: 'connection_init', payload: {} },
		{
			type: 'subscribe',
			id: '1',
			payload: { query: 'subscription A { a }', variables: { room: 'x' } }
		},
		{
			type: 'subscribe',
			id: '2',
			payload: { query: 'subscription B { b }', variables: {} }
		}
	]);

	// A subscription made on the open socket is sent at once.
	const c = subscribe('subscription C { c }');
	await flush();
	assert.equal(sockets.length, 1);
	assert.equal(socket.sent.at(-1).id, '3');

	socket.message({ type: 'next', id: '2', payload: { data: { b: 1 } } });
	socket.message({ type: 'next', id: '9', payload: { data: { stray: 1 } } });
	assert.deepEqual(a.events, []);
	assert.deepEqual(b.events, [{ next: { data: { b: 1 } } }]);

	// The server ends one operation; the others and the socket stay.
	socket.message({ type: 'complete', id: '1' });
	assert.deepEqual(a.events, [{ complete: true }]);
	a.stop();
	assert.equal(socket.closeCount, 0);

	b.stop();
	b.stop();
	assert.deepEqual(socket.sent.at(-1), { type: 'complete', id: '2' });
	assert.equal(socket.closeCount, 0);
	socket.message({ type: 'next', id: '2', payload: { data: { b: 2 } } });
	assert.equal(b.events.length, 1, 'an unsubscribed operation receives nothing');

	c.stop();
	assert.deepEqual(socket.sent.at(-1), { type: 'complete', id: '3' });

	// A replacement arriving while the socket idles reuses it.
	await scheduler.advance(999);
	assert.equal(socket.closeCount, 0);
	const d = subscribe('subscription D { d }');
	await scheduler.advance(5_000);
	assert.equal(sockets.length, 1);
	assert.equal(socket.sent.at(-1).id, '4');
	assert.equal(socket.closeCount, 0);

	d.stop();
	await scheduler.advance(999);
	assert.equal(socket.closeCount, 0);
	await scheduler.advance(1);
	assert.equal(socket.closeCount, 1, 'an idle socket closes');
	subscribe();
	await flush();
	assert.equal(sockets.length, 2);
});

test('a different credential gets its own socket', async () => {
	let auth = { userId: 'alice' };
	const { sockets, subscribe } = harness({ getAuth: () => auth });
	subscribe();
	await flush();
	auth = { userId: 'bob' };
	subscribe();
	subscribe();
	await flush();
	assert.equal(sockets.length, 2);
	sockets[0].accept();
	sockets[1].accept();
	assert.deepEqual(sockets[0].sent[0].payload, {
		'x-user-id': 'alice',
		'x-roles': 'user'
	});
	assert.deepEqual(sockets[1].sent[0].payload, {
		'x-user-id': 'bob',
		'x-roles': 'user'
	});
	assert.equal(sockets[0].subscribed().length, 1);
	assert.equal(sockets[1].subscribed().length, 2);
});

test('a lost socket ends every subscription once and is redialed once however many resubscribe', async () => {
	const { scheduler, sockets, subscribe } = harness();
	const first = [subscribe(), subscribe(), subscribe()];
	await flush();
	sockets[0].accept();
	sockets[0].drop(1006);
	for (const { events } of first) {
		assert.equal(events.length, 1);
		assert(events[0].error instanceof GraphqlWsError);
		assert.equal(events[0].error.retryable, true);
		assert.equal(events[0].error.code, 1006);
	}
	assert.equal(sockets[0].closeCount, 1);

	// Three owners subscribe again at once: one dial, after the delay.
	const second = [subscribe(), subscribe(), subscribe()];
	await flush();
	assert.equal(sockets.length, 1, 'no redial before the delay');
	await scheduler.advance(999);
	assert.equal(sockets.length, 1);
	await scheduler.advance(1);
	assert.equal(sockets.length, 2);

	// The server is still down: the delay doubles, still one dial per round.
	sockets[1].drop(1006);
	for (const { events } of second) assert.equal(events.length, 1);
	const third = [subscribe(), subscribe()];
	await scheduler.advance(1999);
	assert.equal(sockets.length, 2);
	await scheduler.advance(1);
	assert.equal(sockets.length, 3);
	sockets[2].accept();
	assert.deepEqual(
		sockets[2].subscribed().map((message) => message.id),
		['7', '8']
	);

	// A result proves the endpoint healthy: the next loss waits 1 s again.
	sockets[2].message({ type: 'next', id: '7', payload: { data: { a: 1 } } });
	sockets[2].drop(1006);
	subscribe();
	await scheduler.advance(1000);
	assert.equal(sockets.length, 4);
	for (const { stop } of third) stop();
});

test('an error event without a close is a lost socket', async () => {
	const { scheduler, sockets, subscribe } = harness();
	const a = subscribe();
	await flush();
	sockets[0].accept();
	sockets[0].onerror({});
	assert.equal(a.events.length, 1);
	assert.equal(a.events[0].error.retryable, true);
	assert.equal(sockets[0].closeCount, 1, 'the broken socket is closed');
	assert.equal(sockets[0].onclose, null, 'and can report nothing more');
	subscribe();
	await scheduler.advance(1000);
	assert.equal(sockets.length, 2);
});

test('the redial delay is jittered downwards by at most half', async () => {
	let random = 0.999;
	const { scheduler, sockets, subscribe } = harness({ random: () => random });
	subscribe();
	await flush();
	sockets[0].drop(1006);
	subscribe();
	await scheduler.advance(500);
	assert.equal(sockets.length, 1);
	await scheduler.advance(1);
	assert.equal(sockets.length, 2);
	random = 0.5;
	sockets[1].drop(1006);
	subscribe();
	await scheduler.advance(1499);
	assert.equal(sockets.length, 2);
	await scheduler.advance(1);
	assert.equal(sockets.length, 3);
});

test('no socket is dialed for subscriptions that ended during the delay', async () => {
	const { scheduler, sockets, subscribe } = harness();
	subscribe();
	await flush();
	sockets[0].drop(1006);
	const waiting = subscribe();
	await flush();
	waiting.stop();
	await scheduler.advance(60_000);
	assert.equal(sockets.length, 1);
});

test('closes and operation errors are classified by what the server sent', async () => {
	const { scheduler, sockets, subscribe } = harness();
	const cases = [
		// Engine: refused connection_init or a protocol violation.
		[1002, 'unauthorized', false],
		// Gateway: refused credential; duplicate or excessive operation ids.
		[4401, 'unauthorized', false],
		[4409, 'duplicate or excessive operation ID', false],
		// The accepted token ran out: a fresh credential may be accepted.
		[4401, 'token expired', true],
		[1000, 'connection lifetime reached', true],
		[1013, 'LIVE_RESET_REQUIRED', true],
		[1006, '', true]
	];
	for (const [code, reason, retryable] of cases) {
		const { events } = subscribe();
		await scheduler.advance(60_000);
		sockets.at(-1).accept();
		sockets.at(-1).drop(code, reason);
		assert.equal(events.length, 1);
		assert.equal(events[0].error.retryable, retryable, `${code} ${reason}`);
		assert.equal(events[0].error.code, code);
	}

	const operations = [
		[[{ message: 'LIVE_RESET_REQUIRED', extensions: { code: 'LIVE_RESET_REQUIRED' } }], true],
		[[{ message: 'FRESHNESS_SCOPE_CHANGED', extensions: { code: 'FRESHNESS_SCOPE_CHANGED' } }], true],
		[[{ message: 'Unknown field "nope"' }], false],
		[[{ message: 'PERSISTED_QUERY_NOT_FOUND', extensions: { code: 'PERSISTED_QUERY_NOT_FOUND' } }], false]
	];
	const keep = subscribe();
	await scheduler.advance(60_000);
	const socket = sockets.at(-1);
	socket.accept();
	for (const [payload, retryable] of operations) {
		const { events } = subscribe();
		await flush();
		const { id } = socket.subscribed().at(-1);
		socket.message({ type: 'error', id, payload });
		assert.equal(events.length, 1);
		assert.equal(events[0].error.retryable, retryable, payload[0].message);
		assert.equal(events[0].error.message, payload[0].message);
	}
	assert.equal(socket.closeCount, 0, 'an operation error leaves the socket open');
	assert.deepEqual(keep.events, []);
	keep.stop();
});

test('a refused credential source ends only its subscription', async () => {
	const { sockets, subscribe } = harness({
		getAuth: () => Promise.reject(new Error('no session'))
	});
	const { events } = subscribe();
	await flush();
	assert.equal(sockets.length, 0);
	assert.equal(events.length, 1);
	assert.equal(events[0].error.message, 'no session');
	assert.notEqual(events[0].error.retryable, true);
});
