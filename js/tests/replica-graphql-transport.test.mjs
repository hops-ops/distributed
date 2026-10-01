import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createReplicaGraphqlTransport
} from '../dist/replica/index.js';

function jsonResponse(body, status = 200) {
	return {
		status,
		statusText: status === 200 ? 'OK' : 'Error',
		text: async () => JSON.stringify(body)
	};
}

function tick() {
	return new Promise((resolve) => setImmediate(resolve));
}

test('replica GraphQL HTTP and command work share auth and preserve exact extensions and abort signals', async () => {
	const calls = [];
	const controller = new AbortController();
	let authCalls = 0;
	const transport = createReplicaGraphqlTransport({
		getUrl: () => '/graphql',
		getAuth: async () => {
			authCalls += 1;
			return { accessToken: 'access-token' };
		},
		fetch: async (url, init) => {
			calls.push({ url, init, body: JSON.parse(init.body) });
			return jsonResponse({ data: { ok: true } });
		}
	});
	const clientExtensions = {
		distributed: {
			client: {
				surface: { kind: 'role', name: 'user' },
				schemaHash: `sha256:${'a'.repeat(64)}`
			}
		}
	};

	const query = await transport.fetch({
		operation: 'query',
		operationId: 'query:todos',
		document: 'query Todos { todos { id } }',
		variables: {},
		artifact: { id: 'query:todos', document: '', roots: [] },
		extensions: clientExtensions,
		signal: controller.signal
	});
	assert.deepEqual(query.data, { ok: true });

	const dispatched = await transport.dispatch({
		operation: 'mutation',
		commandName: 'todo.complete',
		commandId: 'command-1',
		mutationField: 'todoComplete',
		document: 'mutation Complete { todoComplete }',
		operationHash: `sha256:${'b'.repeat(64)}`,
		variables: { commandId: 'command-1', input: { id: 'todo-1' } },
		extensions: clientExtensions,
		signal: controller.signal
	});
	assert.equal(dispatched.status, 200);

	const status = await transport.status({
		operation: 'status',
		commandId: 'command-1',
		name: 'CommandStatus',
		document: 'query CommandStatus { commandStatus }',
		operationHash: `sha256:${'c'.repeat(64)}`,
		variables: { commandId: 'command-1' },
		extensions: clientExtensions,
		signal: controller.signal
	});
	assert.equal(status.status, 200);

	assert.equal(authCalls, 3);
	assert.equal(calls.length, 3);
	for (const call of calls) {
		assert.equal(call.url, '/graphql');
		assert.equal(call.init.headers.authorization, 'Bearer access-token');
		assert.equal(call.init.signal, controller.signal);
		assert.deepEqual(call.body.extensions, clientExtensions);
	}
	assert.equal(calls[0].body.query, 'query Todos { todos { id } }');
	assert.deepEqual(calls[1].body.variables, {
		commandId: 'command-1',
		input: { id: 'todo-1' }
	});
});

test('replica GraphQL live work merges surface binding with resume and closes on abort', async () => {
	class FakeWebSocket {
		static CONNECTING = 0;
		static OPEN = 1;
		static CLOSING = 2;
		static CLOSED = 3;
		static instances = [];

		readyState = FakeWebSocket.CONNECTING;
		sent = [];
		closed = false;

		constructor(url, protocol) {
			this.url = url;
			this.protocol = protocol;
			FakeWebSocket.instances.push(this);
		}

		send(value) {
			this.sent.push(JSON.parse(value));
		}

		close() {
			this.closed = true;
			this.readyState = FakeWebSocket.CLOSED;
		}

		open() {
			this.readyState = FakeWebSocket.OPEN;
			this.onopen?.();
		}

		message(value) {
			this.onmessage?.({ data: JSON.stringify(value) });
		}
	}

	const controller = new AbortController();
	const next = [];
	const errors = [];
	let completions = 0;
	const transport = createReplicaGraphqlTransport({
		getUrl: () => '/graphql',
		getAuth: () => ({ userId: 'alice', role: 'user' }),
		webSocket: FakeWebSocket
	});
	const extensions = {
		distributed: {
			client: {
				surface: { kind: 'application', name: 'e2e-ui', roles: ['admin', 'user'] },
				schemaHash: `sha256:${'d'.repeat(64)}`
			}
		}
	};
	const resume = [
		{ projection: 'todos', position: '7', token: 'resume-token' }
	];
	const liveRequest = {
		operation: 'live',
		operationId: 'live:todos',
		document: 'subscription TodosLive { todos { id } }',
		variables: { room: 'lobby' },
		artifact: { id: 'query:todos', document: '', roots: [] },
		extensions,
		resume,
		signal: controller.signal
	};
	const observer = {
		next: (value) => next.push(value),
		error: (error) => errors.push(error),
		complete: () => {
			completions += 1;
		}
	};
	const stop = transport.subscribe(
		liveRequest,
		observer
	);
	await tick();

	assert.equal(FakeWebSocket.instances.length, 1);
	const socket = FakeWebSocket.instances[0];
	assert.equal(socket.protocol, 'graphql-transport-ws');
	assert.match(socket.url, /graphql\/ws/);
	assert.match(socket.url, /x-user-id=alice/);
	socket.open();
	assert.deepEqual(socket.sent[0], {
		type: 'connection_init',
		payload: { 'x-user-id': 'alice', 'x-roles': 'user' }
	});
	socket.message({ type: 'connection_ack' });
	assert.deepEqual(socket.sent[1], {
		type: 'subscribe',
		id: '1',
		payload: {
			query: 'subscription TodosLive { todos { id } }',
			variables: { room: 'lobby' },
			extensions: {
				distributed: {
					client: extensions.distributed.client,
					resume: { cursors: resume }
				}
			}
		}
	});
	const credit = { gatewayOperation: '1', gatewayDeliveryAck: 'opaque-credit' };
	socket.message({ type: 'ping', payload: credit });
	assert.deepEqual(socket.sent.at(-1), { type: 'pong', payload: credit });
	socket.message({ type: 'ping' });
	assert.deepEqual(socket.sent.at(-1), { type: 'pong' });
	socket.message({ type: 'next', payload: { data: { todos: [{ id: '1' }] } } });
	assert.deepEqual(next, [{ data: { todos: [{ id: '1' }] } }]);
	assert.deepEqual(errors, []);
	socket.message({ type: 'complete', id: '1' });
	assert.equal(completions, 1);
	assert.equal(socket.closed, true);

	const stopError = transport.subscribe(liveRequest, observer);
	await tick();
	const errorSocket = FakeWebSocket.instances[1];
	errorSocket.open();
	errorSocket.message({ type: 'error', id: '1', payload: 'terminal error' });
	assert.deepEqual(errors, ['terminal error']);
	assert.equal(errorSocket.closed, true);
	assert.equal(completions, 1);

	controller.abort();
	assert.equal(socket.closed, true);
	stop();
	stopError();
	assert.equal(socket.closed, true);
	assert.equal(errorSocket.closed, true);
	assert.equal(completions, 1);
});

test('canceling a live request before async auth resolves prevents a socket', async () => {
	let release;
	const auth = new Promise((resolve) => {
		release = resolve;
	});
	class ForbiddenWebSocket {
		constructor() {
			throw new Error('socket must not open');
		}
	}
	const transport = createReplicaGraphqlTransport({
		getUrl: () => '/graphql',
		getAuth: () => auth,
		webSocket: ForbiddenWebSocket
	});
	const stop = transport.subscribe(
		{
			operation: 'live',
			operationId: 'live',
			document: 'subscription Live { items { id } }',
			variables: {},
			artifact: { id: 'query', document: '', roots: [] }
		},
		{
			next: () => undefined,
			error: () => undefined,
			complete: () => undefined
		}
	);
	stop();
	release({});
	await tick();
});
