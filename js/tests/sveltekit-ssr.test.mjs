import assert from 'node:assert/strict';
import {createLazyReplicaCommandRuntime} from '../dist/replica/command-runtime/lazy.js';
import test from 'node:test';

import {
	createDistributedSvelteKit,
	createPageDataSessionSource,
	createDistributedSvelteKitServer,
	defineDistributedBoundaryBinding,
	defineDistributedBoundaryOperation
} from '../dist/sveltekit/index.js';
import {
	REACT_FIXTURE_SCHEMA,
	TodosArtifact,
	TodoModel,
	todoFrame
} from './fixtures/adapter-conformance.mjs';

function jsonResponse(body, status = 200) {
	return {
		status,
		statusText: status === 200 ? 'OK' : 'Error',
		text: async () => JSON.stringify(body)
	};
}

async function flushMicrotasks() {
	for (let iteration = 0; iteration < 16; iteration += 1) {
		await Promise.resolve();
	}
}

class SsrWebSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	static instances = [];

	readyState = SsrWebSocket.CONNECTING;
	closed = false;
	sent = [];

	constructor(url, protocol) {
		this.url = url;
		this.protocol = protocol;
		SsrWebSocket.instances.push(this);
	}

	send(value) {
		this.sent.push(JSON.parse(value));
	}

	close() {
		this.closed = true;
		this.readyState = SsrWebSocket.CLOSED;
	}

	open() {
		this.readyState = SsrWebSocket.OPEN;
		this.onopen?.();
	}

	receive(message) {
		this.onmessage?.({ data: JSON.stringify(message) });
	}
}

const todosBoundary = defineDistributedBoundaryOperation(
	{
		operation: 'Todos',
		route: '/todos',
		kind: 'page',
		discovery: 'route_document'
	},
	TodosArtifact,
	defineDistributedBoundaryBinding(TodosArtifact, {})
);

const forwardedTodosArtifact = Object.freeze({
	...TodosArtifact,
	id: 'forwarded-todos-v1',
	document:
		'query ForwardedTodos($payload: JSON) { todos { id title status } }',
	protocol: Object.freeze({
		...TodosArtifact.protocol,
		operation: 'forwarded-todos-v1'
	}),
	variableCodec: Object.freeze({
		version: 2,
		limits: Object.freeze({ maxDepth: 64, maxBoolWidth: 256, maxInList: 1000 }),
		variables: Object.freeze({
			payload: Object.freeze({
				kind: 'scalar', scalar: 'JSON', codec: 'json', nullable: true
			})
		}),
		defaults: Object.freeze({}),
		inputs: Object.freeze({})
	})
});

const forwardedTodosBoundary = defineDistributedBoundaryOperation(
	{
		operation: 'ForwardedTodos',
		route: '/todos',
		kind: 'page',
		discovery: 'component',
		sourcePath: 'src/lib/ForwardedTodos.graphql'
	},
	forwardedTodosArtifact,
	defineDistributedBoundaryBinding(forwardedTodosArtifact, {
		payload: { kind: 'forwarded_prop', path: ['filters'] }
	})
);

function serverHarness(mode = 'resumable') {
	const calls = [];
	const server = createDistributedSvelteKitServer({
		boundaries: [todosBoundary],
		getSession: async (event) => event.locals.session,
		getRole: () => 'user'
	});
	const event = (token, position = '1') => ({
		locals: {
			session: {
				accessToken: token,
				user: { id: token }
			}
		},
		route: { id: '/todos' },
		url: new URL('https://app.example/todos'),
		async fetch(url, init) {
			const body = JSON.parse(init.body);
			const authorization = init.headers.authorization;
			calls.push({ url, init, body, authorization });
			assert.deepEqual(body.extensions.distributed.client, {
				surface: { kind: 'role', name: 'user' },
				schemaHash: REACT_FIXTURE_SCHEMA
			});
			assert.equal(authorization, `Bearer ${token}`);
			return jsonResponse(
				todoFrame(
					TodosArtifact,
					[
						{
							id: `todo-${token}`,
							title: `${token}:${position}`,
							status: 'open'
						}
					],
					{
						cacheScope: `cache:${token}`,
						mode,
						position
					}
				)
			);
		}
	});
	return {
		server,
		event,
		calls
	};
}

for (const mode of ['resumable', 'snapshot']) {
test(`static @load SSR is isolated and hydration avoids a duplicate fetch (${mode})`, async () => {
	const harness = serverHarness(mode);
	const [alice, bob] = await Promise.all([
		harness.server.load(harness.event('alice')),
		harness.server.load(harness.event('bob'))
	]);

	assert.equal(harness.calls.length, 2);
	assert.equal(alice.gqlError, null);
	assert.equal(bob.gqlError, null);
	assert.equal(alice.distributed.state.scope.cacheScope, 'cache:alice');
	assert.equal(bob.distributed.state.scope.cacheScope, 'cache:bob');
	assert.equal(JSON.stringify(alice.distributed).includes('bob'), false);
	assert.equal(JSON.stringify(bob.distributed).includes('alice'), false);

	SsrWebSocket.instances.length = 0;
	let browserFetches = 0;
	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary],
		session: { getAuth: () => ({ accessToken: 'alice' }) },
		hydration: alice.distributed,
		authority: alice.distributedAuthority,
		fetch: async () => {
			browserFetches += 1;
			throw new Error('hydrated first render must not fetch');
		},
		webSocket: SsrWebSocket
	});
	const todos = client.operation(TodosArtifact).use();
	assert.equal(todos.get().status, 'ready');
	assert.equal(todos.get().data.todos[0].title, 'alice:1');
	assert.equal(browserFetches, 0);
	assert.equal(SsrWebSocket.instances.length, 0);

	const unsubscribe = todos.subscribe(() => undefined);
	await flushMicrotasks();
	assert.equal(browserFetches, 0);
	assert.equal(SsrWebSocket.instances.length, 1, '@live attaches after hydration');
	const socket = SsrWebSocket.instances[0];
	socket.open();
	socket.receive({ type: 'connection_ack' });
	const subscribed = socket.sent.find(({ type }) => type === 'subscribe');
	assert.ok(subscribed, 'the live handoff must open a subscription');
	socket.receive({
		type: 'next',
		id: subscribed.id,
		payload: todoFrame(
			TodosArtifact,
			[{ id: 'todo-alice', title: 'alice:live', status: 'open' }],
			{ cacheScope: 'cache:alice', position: '2', source: 'live', mode }
		)
	});
	await flushMicrotasks();
	assert.equal(
		todos.get().data.todos[0].title,
		'alice:live',
		'the live frame must be applied through the active subscription'
	);
	assert.equal(
		browserFetches,
		0,
		'query-to-live handoff must not fetch through transient cache state'
	);
	unsubscribe();
	assert.equal(socket.closed, true);
	client.destroy();
});
}

test('SSR only awaits parent data for forwarded-prop bindings', async () => {
	const harness = serverHarness();
	let parentCalls = 0;
	const page = await harness.server.load({
		...harness.event('alice'),
		async parent() {
			parentCalls += 1;
			throw new Error('a boundary without forwarded props must not await parent data');
		}
	});

	assert.equal(page.gqlError, null);
	assert.equal(parentCalls, 0);
});

test('SSR abort settles while forwarded parent data remains pending', async () => {
	const controller = new AbortController();
	let parentCalls = 0;
	const server = createDistributedSvelteKitServer({
		boundaries: [forwardedTodosBoundary],
		getSession: async (event) => event.locals.session,
		getRole: () => 'user'
	});
	const pendingParent = new Promise(() => undefined);
	const load = server.load({
		locals: {
			session: { accessToken: 'alice', user: { id: 'alice' } }
		},
		route: { id: '/todos' },
		url: new URL('https://app.example/todos'),
		request: { signal: controller.signal },
		parent() {
			parentCalls += 1;
			return pendingParent;
		}
	});
	await flushMicrotasks();
	controller.abort();

	await assert.rejects(load, (error) => error?.name === 'AbortError');
	assert.equal(parentCalls, 1);
});

test('client-side data requests skip GraphQL so navigation stays SPA', async () => {
	const harness = serverHarness();
	const document = await harness.server.load(harness.event('alice'));
	assert.equal(harness.calls.length, 1);
	assert.ok(document.distributed);

	const dataNav = await harness.server.load({
		...harness.event('alice'),
		isDataRequest: true
	});
	assert.equal(harness.calls.length, 1, 'SPA data request must not seed a new replica');
	assert.equal(dataNav.distributed, undefined);
	assert.equal(dataNav.gqlError, null);
	assert.equal(dataNav.accessToken, 'alice');
});

test('replica state never self-authorizes hydration scope', async () => {
	const harness = serverHarness();
	const [alice, bob] = await Promise.all([
		harness.server.load(harness.event('alice')),
		harness.server.load(harness.event('bob'))
	]);

	assert.throws(
		() =>
			createDistributedSvelteKit({
				boundaries: [todosBoundary],
				session: { getAuth: () => ({ accessToken: 'alice' }) },
				hydration: alice.distributed
			}),
		/separate trusted SSR authority/
	);

	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary],
		session: { getAuth: () => ({ accessToken: 'bob' }) }
	});
	assert.equal(
		client.hydrate(alice.distributed, bob.distributedAuthority),
		false,
		'a replayed user-A state fails against current user-B page authority'
	);
	assert.equal(client.replica.scope, undefined);

	const tampered = structuredClone(alice.distributed);
	tampered.state.scope.cacheScope = 'cache:forged';
	assert.equal(
		client.hydrate(tampered, alice.distributedAuthority),
		false,
		'tampered state scope fails against the separately carried authority'
	);
	assert.equal(client.replica.scope, undefined);
	client.destroy();
});

test('failed reload registration disposes partially constructed client resources', () => {
	let unsubscribed = false;
	let runtimeDisposed = false;
	assert.throws(
		() =>
			createDistributedSvelteKit({
				boundaries: [],
				session: {
					getAuth: () => ({}),
					subscribe: () => () => {
						unsubscribed = true;
					}
				},
				browser: true,
				reload: { key: '' },
				createCommands: () => ({
					commands: Object.freeze({}),
					dispose() {
						runtimeDisposed = true;
					}
				})
			}),
		/reload participant key/
	);
	assert.equal(runtimeDisposed, true);
	assert.equal(unsubscribed, true);
});

test('route misses do no GraphQL work and same-scope soft-nav merges overlapping seed', async () => {
	const harness = serverHarness();
	const missed = await harness.server.load({
		...harness.event('alice'),
		route: { id: '/session' },
		url: new URL('https://app.example/session')
	});
	assert.equal(harness.calls.length, 0);
	assert.equal(missed.distributed, undefined);
	assert.equal(missed.gqlError, null);

	const first = await harness.server.load(harness.event('alice', '1'));
	const second = await harness.server.load(harness.event('alice', '2'));
	let browserFetches = 0;
	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary],
		session: { getAuth: () => ({ accessToken: 'alice' }) },
		hydration: first.distributed,
		authority: first.distributedAuthority,
		fetch: async () => {
			browserFetches += 1;
			throw new Error('navigation hydration must not fetch');
		},
		webSocket: SsrWebSocket
	});
	const todos = client.operation(TodosArtifact).use({}, { live: false });
	const values = [];
	const unsubscribe = todos.subscribe((snapshot) => {
		values.push(snapshot.data.todos?.[0]?.title);
	});
	assert.equal(todos.get().data.todos[0].title, 'alice:1');
	// Overlapping same-scope seed upserts the route window (position/title update)
	// without requiring a browser GraphQL round-trip.
	assert.equal(
		client.hydrate(second.distributed, second.distributedAuthority),
		true
	);
	assert.equal(todos.get().data.todos[0].title, 'alice:2');
	assert.equal(values.at(-1), 'alice:2');
	assert.equal(browserFetches, 0);
	unsubscribe();
	client.destroy();
});

test('auth changes purge old data, abort live work, and reject cross-scope hydration', async () => {
	const harness = serverHarness();
	const [alice, bob] = await Promise.all([
		harness.server.load(harness.event('alice')),
		harness.server.load(harness.event('bob'))
	]);
	let credential = { accessToken: 'alice' };
	const listeners = new Set();
	const requests = [];
	SsrWebSocket.instances.length = 0;
	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary],
		session: {
			getAuth: () => credential,
			subscribe(listener) {
				listeners.add(listener);
				return () => listeners.delete(listener);
			}
		},
		hydration: alice.distributed,
		authority: alice.distributedAuthority,
		fetch: (url, init) => {
			requests.push({ url, init });
			return new Promise(() => undefined);
		},
		webSocket: SsrWebSocket
	});
	const todos = client.operation(TodosArtifact).use();
	const unsubscribe = todos.subscribe(() => undefined);
	await flushMicrotasks();
	assert.equal(todos.get().data.todos[0].title, 'alice:1');
	assert.equal(SsrWebSocket.instances.length, 1);

	assert.equal(
		client.hydrate(bob.distributed, alice.distributedAuthority),
		false,
		'replayed state cannot authorize itself against current page authority'
	);
	assert.equal(todos.get().complete, false);
	assert.equal(SsrWebSocket.instances[0].closed, true);

	credential = {};
	for (const listener of listeners) listener();
	await flushMicrotasks();
	assert.equal(todos.get().complete, false);
	assert.deepEqual(todos.get().data, {});
	assert.equal(requests.length, 2);
	assert.equal(requests[0].init.signal.aborted, true);
	assert.equal(requests[1].init.headers.authorization, undefined);

	unsubscribe();
	client.destroy();
});

test('one browser replica refuses to mix user and elevated generated surfaces', async () => {
	const harness = serverHarness();
	const alice = await harness.server.load(harness.event('alice'));
	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary],
		session: { getAuth: () => ({ accessToken: 'alice' }) },
		hydration: alice.distributed,
		authority: alice.distributedAuthority
	});
	client.operation(TodosArtifact).read({});

	const adminOperation = Object.freeze({
		...TodosArtifact,
		id: 'query:admin-todos',
		document: 'query AdminTodos { todos { id title status } }',
		protocol: Object.freeze({
			...TodosArtifact.protocol,
			surface: Object.freeze({ kind: 'role', name: 'admin' }),
			operation: 'query:admin-todos'
		}),
		live: undefined
	});
	assert.throws(
		() => client.operation(adminOperation).read({}),
		/active replica binding/
	);
	client.destroy();
});


test('lazy command authority preserves isolated SSR hydration and query prefetch without importing commands', async () => {
 const harness = serverHarness();
 const [alice, bob] = await Promise.all([harness.server.load(harness.event('alice')), harness.server.load(harness.event('bob'))]);
 const hash = `sha256:${'d'.repeat(64)}`;
 const status = {name:'Status',document:'query Status { commandStatus { state } }', operationHash:hash, protocol:{...TodosArtifact.protocol, protocolHash:`sha256:${'c'.repeat(64)}`, operation:hash}};
 let imports=0;let fetches=0;
 const clients=[alice,bob].map((data,i)=>createDistributedSvelteKit({
  browser:false, boundaries:[todosBoundary], session:{getAuth:()=>({accessToken:i===0?'alice':'bob'})},
  hydration:data.distributed, authority:data.distributedAuthority,
  fetch:async()=>{fetches++;throw Error('hydrated selection must not refetch')},
  createCommands:(replica,transport)=>createLazyReplicaCommandRuntime(replica,transport,
   {commands:{'todo.ping':{operationHash:hash,hasInput:false}},status},async()=>{imports++;throw Error('commands must stay deferred')})
 }));
 for(const [i,client] of clients.entries()){
  const store=client.operation(TodosArtifact).use();
  const release=store.subscribe(()=>{});
  assert.equal(store.get().data.todos[0].id,i===0?'todo-alice':'todo-bob');
  await client.prefetchLocation('/todos',{search:new URLSearchParams(),session:{},props:{}});
  release();
 }
 assert.equal(imports,0);assert.equal(fetches,0);
 clients.forEach(client=>client.destroy());
 await assert.rejects(clients[0].preloadCommands(),/destroyed/);
});


test('fresh same-scope SSR authority rotates credentials without emptying the visible replica', async () => {
	const harness = serverHarness();
	const first = await harness.server.load(harness.event('alice', '1'));
	const second = await harness.server.load(harness.event('alice', '2'));
	const pageData = createPageDataSessionSource(first);
	const requests = [];
	SsrWebSocket.instances.length = 0;
	const client = createDistributedSvelteKit({
		boundaries: [todosBoundary], session: pageData.session,
		hydration: first.distributed, authority: first.distributedAuthority,
		fetch: (url, init) => new Promise(resolve => requests.push({init, resolve})),
		webSocket: SsrWebSocket
	});
	const todos = client.operation(TodosArtifact).use();
	const values = [];
	const unsubscribe = todos.subscribe(snapshot => values.push(snapshot.data.todos?.[0]?.title));
	await flushMicrotasks();
	const oldSocket = SsrWebSocket.instances[0];
	const oldRequest = todos.refetch();
	await flushMicrotasks();
	assert.equal(requests.length, 1);
	client.replica.writeResult(TodosArtifact, {}, todoFrame(TodosArtifact, [{id:'todo-alice', title:'confirmed ahead', status:'open'}], {cacheScope:'cache:alice', position:'3'}), 'network');
	client.replica.createOptimisticLayer('pending-edit', writer => writer.writeRecord(TodoModel, 'todo-alice', {fields:{title:'optimistic edit'}}));
	pageData.set({...second, accessToken: 'rotated-alice'});
	await flushMicrotasks();
	assert.ok(values.every(value => value === 'alice:1' || value === 'confirmed ahead' || value === 'optimistic edit'), JSON.stringify(values));
	assert.equal(todos.get().data.todos[0].title, 'optimistic edit');
	assert.equal(requests[0].init.signal.aborted, true);
	assert.equal(oldSocket.closed, true);
	assert.equal(requests.length, 1, 'fresh SSR data needs no replacement browser query');
	const nextSocket = SsrWebSocket.instances.at(-1);
	assert.notEqual(nextSocket, oldSocket);
	nextSocket.open();
	await flushMicrotasks();
	assert.equal(nextSocket.sent[0].payload.authorization, 'Bearer rotated-alice');
	requests[0].resolve(jsonResponse(todoFrame(TodosArtifact, [{id:'todo-alice',title:'late old credential',status:'open'}], {cacheScope:'cache:alice',position:'3'})));
	await oldRequest;
	assert.equal(todos.get().data.todos[0].title, 'optimistic edit', 'closed credential work cannot overwrite the new seed');
	client.replica.rejectOptimisticLayer('pending-edit');
	assert.equal(todos.get().data.todos[0].title, 'confirmed ahead', 'the refresh seed cannot overwrite a newer confirmed command result');
	unsubscribe(); client.destroy();
});

for (const kind of ['missing', 'replayed', 'historical', 'tampered', 'different-scope', 'logout']) {
	test(`credential change with ${kind} hydration still purges the old replica`, async () => {
		const harness = serverHarness();
		const first = await harness.server.load(harness.event('alice'));
		const second = await harness.server.load(harness.event(kind === 'different-scope' ? 'bob' : 'alice', '2'));
		const pageData = createPageDataSessionSource(first);
		const client = createDistributedSvelteKit({
			boundaries: [todosBoundary], session: pageData.session,
			hydration: first.distributed, authority: first.distributedAuthority,
			fetch: () => new Promise(() => {}), webSocket: SsrWebSocket
		});
		const todos = client.operation(TodosArtifact).use({}, {live:false});
		const unsubscribe = todos.subscribe(() => {});
		await flushMicrotasks();
		let next = {...second, accessToken:'new-credential'};
		if(kind === 'missing') {delete next.distributed;delete next.distributedAuthority;}
		if(kind === 'historical') {pageData.set({...first, distributed:undefined, distributedAuthority:undefined});await flushMicrotasks();}
		if(kind === 'replayed' || kind === 'historical') next = {...first, accessToken:'new-credential'};
		if(kind === 'tampered') {next.distributed = structuredClone(next.distributed);next.distributed.state.scope.cacheScope = 'cache:forged';}
		if(kind === 'logout') next = {session:null};
		pageData.set(next);
		await flushMicrotasks();
		assert.equal(todos.get().complete, false);
		assert.deepEqual(todos.get().data, {});
		assert.equal(client.replica.scope, undefined);
		unsubscribe(); client.destroy();
	});
}
