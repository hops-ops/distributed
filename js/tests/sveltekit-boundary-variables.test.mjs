import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createDistributedSvelteKit,
	createDistributedSvelteKitServer,
	DistributedSvelteKitBoundaryController,
	defineDistributedBoundaryBinding,
	defineDistributedBoundaryOperation,
	searchParam
} from '../dist/sveltekit/index.js';
import { createDistributedReplica } from '../dist/replica/index.js';
import {
	REACT_FIXTURE_SCHEMA,
	TodosArtifact,
	todoFrame
} from './fixtures/adapter-conformance.mjs';

const BoundTodosArtifact = Object.freeze({
	...TodosArtifact,
	id: 'bound-todos-v1',
	document:
		'query BoundTodos($id: ID!, $limit: Int!, $nil: String, $omitted: String, $owner: String, $payload: JSON, $tags: [String!]) { todos { id title status } }',
	protocol: Object.freeze({
		...TodosArtifact.protocol,
		operation: 'bound-todos-v1'
	}),
	variableCodec: Object.freeze({
		version: 2,
		limits: Object.freeze({ maxDepth: 64, maxBoolWidth: 256, maxInList: 1000 }),
		variables: Object.freeze({
			id: Object.freeze({
				kind: 'scalar', scalar: 'ID', codec: 'string', nullable: false
			}),
			limit: Object.freeze({
				kind: 'scalar', scalar: 'Int', codec: 'int32', nullable: false
			}),
			nil: Object.freeze({
				kind: 'scalar', scalar: 'String', codec: 'string', nullable: true
			}),
			omitted: Object.freeze({
				kind: 'scalar', scalar: 'String', codec: 'string', nullable: true
			}),
			owner: Object.freeze({
				kind: 'scalar', scalar: 'String', codec: 'string', nullable: true
			}),
			payload: Object.freeze({
				kind: 'scalar', scalar: 'JSON', codec: 'json', nullable: true
			}),
			tags: Object.freeze({
				kind: 'list',
				nullable: true,
				item: Object.freeze({
					kind: 'scalar', scalar: 'String', codec: 'string', nullable: false
				})
			})
		}),
		defaults: Object.freeze({}),
		inputs: Object.freeze({})
	})
});

function binding(limit = 20) {
	return defineDistributedBoundaryBinding(BoundTodosArtifact, {
		id: { kind: 'route_param', name: 'itemId' },
		limit: typeof limit === 'number' ? { kind: 'constant', value: limit } : limit,
		nil: { kind: 'constant', value: null },
		omitted: { kind: 'omit' },
		owner: { kind: 'trusted_session', path: ['user', 'id'] },
		payload: { kind: 'forwarded_prop', path: ['filters'] },
		tags: searchParam('tag', 'String', 'all')
	});
}

function operation(bound = binding()) {
	return defineDistributedBoundaryOperation(
		{
			operation: 'BoundTodos',
			route: '/items/[itemId]',
			kind: 'page',
			discovery: 'component',
			sourcePath: 'src/lib/ItemList.graphql'
		},
		BoundTodosArtifact,
		bound
	);
}

function operationAt({
	name,
	route,
	kind,
	limit
}) {
	return defineDistributedBoundaryOperation(
		{
			operation: name,
			route,
			kind,
			discovery: 'explicit'
		},
		BoundTodosArtifact,
		binding(limit)
	);
}

function context() {
	return Object.freeze({
		params: Object.freeze({ itemId: 'café/東京' }),
		search: new URLSearchParams('tag=%CE%B2&tag=%CE%B1'),
		session: Object.freeze({ user: Object.freeze({ id: 'owner-1' }) }),
		props: Object.freeze({
			filters: Object.freeze({ z: 2, a: Object.freeze(['雪', 1]) })
		})
	});
}

function jsonResponse(body) {
	return {
		status: 200,
		statusText: 'OK',
		text: async () => JSON.stringify(body)
	};
}

test('one boundary binding canonicalizes route, search, session, constant, and forwarded props', () => {
	const first = binding();
	const variables = first.resolve(context());
	assert.deepEqual(variables, {
		id: 'café/東京',
		limit: 20,
		nil: null,
		owner: 'owner-1',
		payload: { a: ['雪', 1], z: 2 },
		tags: ['β', 'α']
	});
	assert.equal(Object.hasOwn(variables, 'omitted'), false, 'omission remains distinct from null');
	assert.equal(first.canonicalBytes(context()), JSON.stringify(variables));
	assert.deepEqual(
		first.resolve({ ...context(), search: {} }).tags,
		undefined,
		'an absent multi-value search parameter is omitted so operation defaults can apply'
	);
	assert.equal(binding().id, first.id, 'equivalent bindings have stable fingerprints');
	assert.notEqual(binding(21).id, first.id, 'binding contract changes alter hydration identity');
});

test('boundary binding fails closed for missing values, hostile keys, accessors, and forgery paths', () => {
	assert.throws(
		() =>
			defineDistributedBoundaryBinding(BoundTodosArtifact, {
				limit: { kind: 'constant', value: 20 }
			}),
		/missing required variable id/
	);
	assert.throws(
		() =>
			defineDistributedBoundaryBinding(BoundTodosArtifact, {
				id: { kind: 'route_param', name: 'itemId' },
				limit: { kind: 'constant', value: 20 },
				owner: { kind: 'trusted_session', path: ['__proto__', 'owner'] }
			}),
		/invalid path/
	);
	const hostile = Object.create(null);
	Object.defineProperty(hostile, '__proto__', {
		value: 'forged', enumerable: true
	});
	assert.throws(
		() =>
			defineDistributedBoundaryBinding(BoundTodosArtifact, {
				id: { kind: 'route_param', name: 'itemId' },
				limit: { kind: 'constant', value: 20 },
				payload: { kind: 'constant', value: hostile }
			}),
		/hostile object key/
	);
	const params = {};
	Object.defineProperty(params, 'itemId', { get: () => 'secret', enumerable: true });
	assert.throws(
		() => binding().resolve({ ...context(), params }),
		/contains an accessor/
	);
	assert.throws(
		() => binding().resolve({ ...context(), params: {} }),
		/required variable is missing/
	);
});

test('SSR, hydration, component use, navigation read, and prefetch share canonical boundary variables', async () => {
	const boundary = operation(binding(searchParam('pageSize', 'Int')));
	const sent = [];
	const server = createDistributedSvelteKitServer({
		boundaries: [boundary],
		getSession: async (event) => event.locals.session,
		getRole: () => 'user'
	});
	const serverContext = context();
	serverContext.search.set('pageSize', '20');
	const page = await server.load({
		locals: { session: serverContext.session },
		route: { id: '/items/[itemId]' },
		params: serverContext.params,
		url: new URL(`https://app.example/items/value?${serverContext.search}`),
		parent: async () => serverContext.props,
		async fetch(_url, init) {
			const body = JSON.parse(init.body);
			sent.push(body.variables);
			assert.deepEqual(body.extensions.distributed.client, {
				surface: { kind: 'role', name: 'user' },
				schemaHash: REACT_FIXTURE_SCHEMA
			});
			return jsonResponse(
				todoFrame(
					BoundTodosArtifact,
					[{ id: 'todo-1', title: 'bound', status: 'open' }],
					{ cacheScope: 'cache:owner-1', position: '1' }
				)
			);
		}
	});
	assert.equal(page.gqlError, null);
	assert.deepEqual(page.distributed.bindings, [boundary.binding.id]);
	assert.deepEqual(sent, [boundary.binding.resolve(serverContext)]);
	assert.equal(sent[0].limit, 20, 'URL text becomes a GraphQL Int before SSR transport');

	let browserFetches = 0;
	const client = createDistributedSvelteKit({
		session: { getAuth: () => ({ accessToken: 'browser' }) },
		boundaries: [boundary],
		hydration: page.distributed,
		authority: page.distributedAuthority,
		fetch: async () => {
			browserFetches += 1;
			throw new Error('complete hydrated boundary must not refetch');
		},
		browser: false
	});
	const bound = client.boundary(boundary);
	const lifecycleBytes = [
		boundary.binding.canonicalBytes(serverContext),
		JSON.stringify(bound.variables(serverContext)),
		JSON.stringify(sent[0])
	];
	assert.equal(new Set(lifecycleBytes).size, 1);
	assert.equal(bound.read(serverContext).status, 'ready');
	assert.equal(bound.use(serverContext, { live: false }).get().status, 'ready');
	await bound.prefetch(serverContext);
	assert.equal(browserFetches, 0);
	client.destroy();

	const changedBoundary = operation(binding(21));
	const stale = createDistributedSvelteKit({
		session: { getAuth: () => ({ accessToken: 'browser' }) },
		boundaries: [changedBoundary],
		hydration: page.distributed,
		authority: page.distributedAuthority,
		browser: false
	});
	assert.equal(stale.replica.scope, undefined, 'changed binding rejects stale hydration');
	stale.destroy();
});

test('location orchestration derives one canonical key for layout, page, and hover prefetch', async () => {
	const layout = operationAt({
		name: 'LocationLayout',
		route: '/items/[itemId]',
		kind: 'layout',
		limit: 20
	});
	const page = operationAt({
		name: 'LocationPage',
		route: '/items/[itemId]',
		kind: 'page',
		limit: 20
	});
	const requests = [];
	const selected = context();
	const locationContext = Object.freeze({
		search: selected.search,
		session: selected.session,
		props: selected.props
	});
	const client = createDistributedSvelteKit({
		boundaries: [layout, page],
		session: { getAuth: () => ({ accessToken: 'browser' }) },
		async fetch(_url, init) {
			const request = JSON.parse(init.body);
			requests.push(request);
			return jsonResponse(
				todoFrame(
					BoundTodosArtifact,
					[{ id: 'todo-location', title: 'location', status: 'open' }],
					{ cacheScope: 'cache:location', position: '1' }
				)
			);
		}
	});

	await client.prefetchLocation('/items/caf%C3%A9', locationContext);
	assert.equal(
		requests.length,
		1,
		'exact layout/page occurrences share one replica prefetch identity'
	);
	assert.deepEqual(requests[0].variables, {
		...binding().resolve({ ...selected, params: { itemId: 'café' } })
	});
	const warm = page.binding.resolve({
		...selected,
		params: { itemId: 'café' }
	});
	assert.equal(client.replica.read(BoundTodosArtifact, warm).complete, true);
	await client.prefetchLocation('/items/caf%C3%A9', locationContext);
	assert.equal(requests.length, 1, 'navigation reuses the hover-prefetched key');

	const absent = client.retainLocation(
		{ id: 'no-island-page', pathname: '/unrelated', kind: 'page' },
		locationContext
	);
	absent.release();
	client.destroy();

	let matcherFetches = 0;
	const matched = operationAt({
		name: 'ApplicationMatcher',
		route: '/matched/[itemId=owned]',
		kind: 'page',
		limit: 20
	});
	const matcherClient = createDistributedSvelteKit({
		boundaries: [matched],
		session: { getAuth: () => ({ accessToken: 'browser' }) },
		async fetch() {
			matcherFetches += 1;
			throw new Error('an application matcher cannot be guessed by the adapter');
		}
	});
	await matcherClient.prefetchLocation('/matched/value', locationContext);
	assert.equal(matcherFetches, 0);
	await assert.rejects(
		matcherClient.prefetchLocation(`/${'x'.repeat(8_192)}`, locationContext),
		/exceeds adapter limits/
	);
	matcherClient.destroy();
});

test('location orchestration matches optional, rest, optional-rest, and group segments', async () => {
	const optional = operationAt({
		name: 'OptionalRoute',
		route: '/optional/[[locale]]/[itemId]',
		kind: 'page',
		limit: 31
	});
	const rest = operationAt({
		name: 'RestRoute',
		route: '/rest/[...itemId]',
		kind: 'page',
		limit: 32
	});
	const optionalRest = operationAt({
		name: 'OptionalRestRoute',
		route: '/optional-rest/[[...itemId]]',
		kind: 'page',
		limit: 33
	});
	const optionalRestEmpty = defineDistributedBoundaryOperation(
		{
			operation: 'OptionalRestEmptyRoute',
			route: '/optional-rest-empty/[[...unused]]',
			kind: 'page',
			discovery: 'explicit'
		},
		BoundTodosArtifact,
		defineDistributedBoundaryBinding(BoundTodosArtifact, {
			id: { kind: 'constant', value: 'empty' },
			limit: { kind: 'constant', value: 35 }
		})
	);
	const grouped = operationAt({
		name: 'GroupedRoute',
		route: '/(catalog)/grouped/[itemId]',
		kind: 'page',
		limit: 34
	});
	const selected = context();
	const locationContext = Object.freeze({
		search: selected.search,
		session: selected.session,
		props: selected.props
	});
	const variablesFor = async (boundary, pathname) => {
		const requests = [];
		const client = createDistributedSvelteKit({
			boundaries: [boundary],
			session: { getAuth: () => ({ accessToken: 'browser' }) },
			async fetch(_url, init) {
				const request = JSON.parse(init.body);
				requests.push(request.variables);
				return jsonResponse(
					todoFrame(
						BoundTodosArtifact,
						[{ id: 'todo-route', title: 'route', status: 'open' }],
						{ cacheScope: 'cache:routes', position: '1' }
					)
				);
			}
		});
		await client.prefetchLocation(pathname, locationContext);
		client.destroy();
		assert.equal(requests.length, 1, `${pathname} must select its boundary`);
		return requests[0];
	};

	assert.deepEqual(
		await variablesFor(optional, '/optional/final'),
		{ ...binding(31).resolve({ ...selected, params: { itemId: 'final' } }) }
	);
	assert.deepEqual(
		await variablesFor(optional, '/optional/en/final'),
		{ ...binding(31).resolve({ ...selected, params: { locale: 'en', itemId: 'final' } }) }
	);
	assert.deepEqual(
		await variablesFor(rest, '/rest/a/b'),
		{ ...binding(32).resolve({ ...selected, params: { itemId: 'a/b' } }) }
	);
	assert.deepEqual(
		await variablesFor(optionalRest, '/optional-rest/a/b'),
		{ ...binding(33).resolve({ ...selected, params: { itemId: 'a/b' } }) }
	);
	assert.deepEqual(
		await variablesFor(optionalRestEmpty, '/optional-rest-empty'),
		{ id: 'empty', limit: 35 }
	);
	assert.deepEqual(
		await variablesFor(grouped, '/grouped/value'),
		{ ...binding(34).resolve({ ...selected, params: { itemId: 'value' } }) }
	);
});

test('boundary SSR selects parent layouts, deduplicates exact work, and bounds distinct refreshes', async () => {
	const boundaries = [
		operationAt({ name: 'RootItems', route: '/', kind: 'layout', limit: 20 }),
		operationAt({
			name: 'ItemPage',
			route: '/items/[itemId]',
			kind: 'page',
			limit: 20
		}),
		operationAt({
			name: 'ItemSibling',
			route: '/items',
			kind: 'layout',
			limit: 21
		}),
		operationAt({
			name: 'ItemSiblingTwo',
			route: '/items',
			kind: 'layout',
			limit: 22
		})
	];
	let active = 0;
	let maximum = 0;
	const calls = [];
	const server = createDistributedSvelteKitServer({
		boundaries,
		maxConcurrency: 2,
		getSession: async (event) => event.locals.session,
		getRole: () => 'user'
	});
	const selectedContext = context();
	const page = await server.load({
		locals: { session: selectedContext.session },
		route: { id: '/items/[itemId]' },
		params: selectedContext.params,
		url: new URL(`https://app.example/items/value?${selectedContext.search}`),
		parent: async () => selectedContext.props,
		async fetch(_url, init) {
			const body = JSON.parse(init.body);
			calls.push(body.variables.limit);
			active += 1;
			maximum = Math.max(maximum, active);
			await new Promise((resolve) => setTimeout(resolve, 10));
			active -= 1;
			return jsonResponse(
				todoFrame(
					BoundTodosArtifact,
					[{ id: `todo-${body.variables.limit}`, title: 'bound', status: 'open' }],
					{ cacheScope: 'cache:owner-1', position: String(body.variables.limit) }
				)
			);
		}
	});
	assert.deepEqual(calls.sort((left, right) => left - right), [20, 21, 22]);
	assert.equal(maximum, 2, 'request scheduling never exceeds configured concurrency');
	assert.ok(page.distributed, 'one request-local replica publishes one final seed');
	assert.equal(page.distributed.operations.length, 4, 'logical duplicate ownership remains inspectable');
	assert.equal(
		new Set(page.distributed.bindings).size,
		3,
		'exact duplicate binding identities are represented once'
	);
	assert.equal(page.distributed.bindings.length, 3);
});

test('boundary SSR preserves successful siblings on partial failure and aborts held work', async () => {
	const boundaries = [
		operationAt({ name: 'First', route: '/items', kind: 'layout', limit: 20 }),
		operationAt({ name: 'Second', route: '/items/[itemId]', kind: 'page', limit: 21 })
	];
	const selectedContext = context();
	const server = createDistributedSvelteKitServer({
		boundaries,
		getSession: async (event) => event.locals.session,
		getRole: () => 'user'
	});
	const sharedEvent = {
		locals: { session: selectedContext.session },
		route: { id: '/items/[itemId]' },
		params: selectedContext.params,
		url: new URL(`https://app.example/items/value?${selectedContext.search}`),
		parent: async () => selectedContext.props
	};
	const partial = await server.load({
		...sharedEvent,
		async fetch(_url, init) {
			const body = JSON.parse(init.body);
			if (body.variables.limit === 21) throw new Error('sensitive upstream detail');
			return jsonResponse(
				todoFrame(
					BoundTodosArtifact,
					[{ id: 'todo-20', title: 'valid sibling', status: 'open' }],
					{ cacheScope: 'cache:owner-1', position: '20' }
				)
			);
		}
	});
	assert.ok(partial.distributed, 'successful sibling data remains dehydrated');
	assert.equal(partial.gqlError, 'Distributed GraphQL island refresh failed');
	assert.doesNotMatch(partial.gqlError, /sensitive upstream detail/);

	const controller = new AbortController();
	let transportAborted = false;
	const held = server.load({
		...sharedEvent,
		request: { signal: controller.signal },
		fetch(_url, init) {
			return new Promise((_resolve, reject) => {
				init.signal.addEventListener(
					'abort',
					() => {
						transportAborted = true;
						const error = new Error('transport aborted');
						error.name = 'AbortError';
						reject(error);
					},
					{ once: true }
				);
			});
		}
	});
	await new Promise((resolve) => setTimeout(resolve, 0));
	controller.abort();
	await assert.rejects(held, (error) => error.name === 'AbortError');
	assert.equal(transportAborted, true);
});

test('boundary controller retains layouts, replaces pages, and closes one shared live subscription', async () => {
	const layout = operationAt({
		name: 'LayoutItems', route: '/items', kind: 'layout', limit: 20
	});
	const page = operationAt({
		name: 'PageItems', route: '/items/[itemId]', kind: 'page', limit: 20
	});
	const nonLiveArtifact = Object.freeze({
		...Object.fromEntries(
			Object.entries(BoundTodosArtifact).filter(([key]) => key !== 'live')
		),
		id: 'bound-todos-static-v1',
		protocol: Object.freeze({
			...BoundTodosArtifact.protocol,
			operation: 'bound-todos-static-v1'
		})
	});
	const nonLive = defineDistributedBoundaryOperation(
		{
			operation: 'StaticItems',
			route: '/static',
			kind: 'page',
			discovery: 'explicit'
		},
		nonLiveArtifact,
		defineDistributedBoundaryBinding(nonLiveArtifact, layout.binding.sources)
	);
	const subscriptions = [];
	let unsubscribes = 0;
	const diagnostics = [];
	const replica = createDistributedReplica({
		transport: {
			async fetch(request) {
				return todoFrame(
					request.artifact,
					[{ id: 'todo-live', title: 'live', status: 'open' }],
					{ cacheScope: 'cache:owner-1', position: '1' }
				);
			},
			subscribe(request, observer) {
				const subscription = { request, observer, closed: false };
				subscriptions.push(subscription);
				return () => {
					if (subscription.closed) return;
					subscription.closed = true;
					unsubscribes += 1;
				};
			}
		}
	});
	const controller = new DistributedSvelteKitBoundaryController(
		replica,
		[layout, page, nonLive],
		(event) => diagnostics.push(event)
	);
	const selected = context();
	const layoutOwner = controller.retain(
		{ id: 'layout-instance', route: '/items', kind: 'layout' },
		selected
	);
	assert.equal(subscriptions.length, 1);
	const pageOwner = controller.retain(
		{ id: 'page-instance-a', route: '/items/[itemId]', kind: 'page' },
		selected
	);
	assert.equal(
		subscriptions.length,
		1,
		'layout and page reuse the core canonical live subscription'
	);
	const hmrOwner = controller.retain(
		{ id: 'layout-instance', route: '/items', kind: 'layout' },
		selected
	);
	pageOwner.release();
	pageOwner.release();
	assert.equal(unsubscribes, 0, 'releasing one owner preserves the layout watch');
	layoutOwner.release();
	assert.equal(unsubscribes, 0, 'a repeated instance retain is independently released');
	hmrOwner.release();
	assert.equal(unsubscribes, 1, 'the final canonical owner closes exactly once');
	const staticOwner = controller.retain(
		{ id: 'static-page', route: '/static', kind: 'page' },
		selected
	);
	assert.equal(
		subscriptions.length,
		1,
		'load-only boundaries retain selection without opening live transport work'
	);
	staticOwner.release();
	assert.equal(unsubscribes, 1);

	const nextContext = Object.freeze({
		...selected,
		params: Object.freeze({ itemId: 'next-item' })
	});
	const retainedLayout = controller.retain(
		{ id: 'layout-instance-b', route: '/items', kind: 'layout' },
		selected
	);
	const nextPage = controller.retain(
		{ id: 'page-instance-b', route: '/items/[itemId]', kind: 'page' },
		nextContext
	);
	assert.equal(subscriptions.length, 3, 'a new canonical page identity gets its own live work');
	controller.disposeScope();
	assert.equal(unsubscribes, 3, 'scope disposal closes layout and page work before reuse');
	retainedLayout.release();
	nextPage.release();

	const afterScope = controller.retain(
		{ id: 'layout-instance-b', route: '/items', kind: 'layout' },
		selected
	);
	afterScope.release();
	assert.equal(unsubscribes, 4, 'the controller is reusable for the new scope generation');
	assert.ok(diagnostics.some(({ action }) => action === 'retain'));
	assert.ok(diagnostics.some(({ action }) => action === 'scope-dispose'));
	assert.equal(
		diagnostics.filter(({ action }) => action === 'final-unsubscribe').length,
		4
	);
	assert.doesNotMatch(JSON.stringify(diagnostics), /owner-1|next-item/);
	controller.destroy();
});

test('server scope transition disposes retained boundaries before old work can continue', async () => {
	class FakeWebSocket {
		static CONNECTING = 0;
		static OPEN = 1;
		static CLOSING = 2;
		static CLOSED = 3;
		static instances = [];
		readyState = FakeWebSocket.CONNECTING;
		closed = false;

		constructor() {
			FakeWebSocket.instances.push(this);
		}

		send() {}

		close() {
			this.closed = true;
			this.readyState = FakeWebSocket.CLOSED;
		}

		message(value) {
			this.onmessage?.({ data: JSON.stringify(value) });
		}
	}

	const diagnostics = [];
	const boundary = operationAt({
		name: 'ScopedItems', route: '/items', kind: 'layout', limit: 20
	});
	const client = createDistributedSvelteKit({
		session: { getAuth: () => ({ userId: 'owner-1', role: 'user' }) },
		boundaries: [boundary],
		webSocket: FakeWebSocket,
		onBoundaryDiagnostic: (event) => diagnostics.push(event),
		async fetch(_url, init) {
			const request = JSON.parse(init.body);
			return jsonResponse(
				todoFrame(
					BoundTodosArtifact,
					[{ id: `todo-${request.variables.limit}`, title: 'scope', status: 'open' }],
					{ cacheScope: 'cache:owner-1', position: '1' }
				)
			);
		}
	});
	const retained = client.retainBoundary(
		{ id: 'scope-layout', route: '/items', kind: 'layout' },
		context()
	);
	await new Promise((resolve) => setTimeout(resolve, 0));
	assert.equal(FakeWebSocket.instances.length, 1);
	assert.equal(client.replica.scope?.cacheScope, 'cache:owner-1');
	const variables = boundary.binding.resolve(context());
	client.replica.writeResult(
		BoundTodosArtifact,
		variables,
		todoFrame(
			BoundTodosArtifact,
			[{ id: 'todo-next', title: 'new scope', status: 'open' }],
			{ cacheScope: 'cache:owner-2', authorizationGeneration: 'auth-2', position: '2' }
		),
		'network'
	);
	assert.equal(FakeWebSocket.instances[0].closed, true);
	assert.equal(client.replica.scope?.cacheScope, 'cache:owner-2');
	assert.deepEqual(
		diagnostics.slice(-2).map(({ action }) => action),
		['scope-dispose', 'final-unsubscribe']
	);
	FakeWebSocket.instances[0].message({
		type: 'next',
		id: '1',
		payload: todoFrame(
			BoundTodosArtifact,
			[{ id: 'todo-old', title: 'late old scope', status: 'open' }],
			{ cacheScope: 'cache:owner-1', position: '3', source: 'live' }
		)
	});
	assert.equal(
		client.replica.read(BoundTodosArtifact, variables).data.todos?.[0]?.title,
		'new scope'
	);
	retained.release();
	client.destroy();
});
