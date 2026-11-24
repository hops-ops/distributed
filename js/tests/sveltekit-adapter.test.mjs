import assert from 'node:assert/strict';
import test from 'node:test';

import {
	bindSveltekitOperation,
	createPageDataSessionSource,
	createDistributedSvelteKit,
	defineDistributedSvelteKitOperation,
	provideDistributedSvelteKitClient,
	useDistributedSvelteKitClient,
	useDistributedSvelteKitCommands
} from '../dist/sveltekit/index.js';
import { getAllContexts } from 'svelte';
import { render } from 'svelte/server';
import { COMMAND_CONSISTENCY } from './fixtures/command-protocol.mjs';
import {
	assertReplicaAdapterConformance,
	ControlledReplicaTransport,
	REACT_FIXTURE_SCHEMA,
	TodosArtifact,
	todoFrame
} from './fixtures/adapter-conformance.mjs';
import {
	createDistributedReplica,
	createReplicaCommandRuntime,
	createReplicaDiagnostics
} from '../dist/replica/index.js';
import { replicaCommandProjectedLifecycle } from '../dist/replica/command-runtime/symbols.js';

async function flushMicrotasks() {
	for (let iteration = 0; iteration < 12; iteration += 1) {
		await Promise.resolve();
	}
}

function jsonResponse(body, status = 200) {
	return {
		status,
		statusText: status === 200 ? 'OK' : 'Error',
		text: async () => JSON.stringify(body)
	};
}

test('generated wrappers resolve the nearest tree-local client and never capture module state', () => {
	const wrapper = defineDistributedSvelteKitOperation(TodosArtifact);
	const calls = [];
	const client = (label) => ({
		commands: Object.freeze({ label }),
		operation(artifact) {
			assert.equal(artifact, TodosArtifact);
			return {
				artifact,
				use(...args) {
					calls.push({ label, kind: 'use', args });
					return { label, args };
				},
				read(variables) {
					calls.push({ label, kind: 'read', variables });
					return { label, variables };
				}
			};
		}
	});
	const user = client('user');
	const admin = client('admin');

	void render(() => {
		assert.equal(provideDistributedSvelteKitClient(user), user);
		assert.equal(useDistributedSvelteKitClient(), user);
		assert.deepEqual(useDistributedSvelteKitCommands(), { label: 'user' });
		assert.deepEqual(wrapper.use({ limit: 1 }), {
			label: 'user',
			args: [{ limit: 1 }]
		});

		void render(
			() => {
				assert.equal(
					useDistributedSvelteKitClient(),
					user,
					'a child inherits the nearest parent client'
				);
				provideDistributedSvelteKitClient(admin);
				assert.deepEqual(useDistributedSvelteKitCommands(), { label: 'admin' });
				assert.deepEqual(wrapper.read({ limit: 2 }), {
					label: 'admin',
					variables: { limit: 2 }
				});
			},
			{ context: new Map(getAllContexts()) }
		).body;

		assert.equal(
			useDistributedSvelteKitClient(),
			user,
			'leaving an elevated subtree restores the user-safe boundary'
		);
	}).body;
	assert.deepEqual(
		calls.map(({ label, kind }) => `${label}:${kind}`),
		['user:use', 'admin:read']
	);
});

test('page-data session source feeds one mutable auth lifecycle', () => {
	const page = createPageDataSessionSource({
		accessToken: 'token-a',
		engineRole: 'user'
	});
	const notifications = [];
	const unsubscribe = page.session.subscribe(() => {
		notifications.push(page.get().accessToken);
	});

	assert.deepEqual(page.session.getAuth(), {
		accessToken: 'token-a',
		userId: undefined,
		role: 'user'
	});
	page.set({
		accessToken: null,
		engineRole: 'admin',
		session: { user: { id: 'dev-admin' } }
	});
	assert.deepEqual(notifications, [null]);
	assert.deepEqual(page.session.getAuth(), {
		accessToken: null,
		userId: 'dev-admin',
		role: 'admin'
	});
	unsubscribe();
	page.set({ accessToken: 'ignored-after-unsubscribe' });
	assert.deepEqual(notifications, [null]);
});

async function mountSveltekitQuery({
	replica,
	artifact,
	variables,
	options
}) {
	const operation = bindSveltekitOperation(replica, artifact);
	const store = operation.use(variables, options);
	let current = store.get();
	const unsubscribe = store.subscribe((snapshot) => {
		current = snapshot;
	});

	return {
		getSnapshot() {
			assert.ok(current, 'SvelteKit query must expose a snapshot');
			return current;
		},
		async settle(action) {
			action?.();
			await flushMicrotasks();
		},
		refetch() {
			return store.refetch();
		},
		async dispose() {
			unsubscribe();
			store.destroy();
			await flushMicrotasks();
		}
	};
}

test('SvelteKit passes the shared replica adapter conformance contract', async () => {
	await assertReplicaAdapterConformance({ mount: mountSveltekitQuery });
});

test('Svelte navigation owns exactly one lazy watch and live subscription', async () => {
	const transport = new ControlledReplicaTransport();
	const replica = createDistributedReplica({ transport });
	const store = bindSveltekitOperation(replica, TodosArtifact).use(
		{},
		{ live: true }
	);

	assert.equal(store.get().status, 'loading');
	assert.equal(transport.fetches.length, 0, 'constructing a store is side-effect free');
	assert.equal(transport.lives.length, 0);

	const first = store.subscribe(() => undefined);
	const second = store.subscribe(() => undefined);
	await flushMicrotasks();
	assert.equal(transport.fetches.length, 1);
	assert.equal(transport.lives.length, 1);
	assert.equal(transport.lives[0].closed, false);

	first();
	assert.equal(transport.lives[0].closed, false, 'one subscriber still owns the watch');
	second();
	assert.equal(transport.lives[0].closed, true, 'route teardown retires live work');

	const third = store.subscribe(() => undefined);
	await flushMicrotasks();
	assert.equal(
		transport.fetches.length,
		1,
		'navigation back coalesces the still-pending HTTP fetch'
	);
	assert.equal(transport.lives.length, 2, 'navigation back starts one new live stream');
	assert.equal(transport.lives[1].closed, false);
	third();
	assert.equal(transport.lives[1].closed, true);
	store.destroy();
	assert.throws(() => store.subscribe(() => undefined), /query is destroyed/);
});

test('component SSR subscriptions stay transport-free when browser is false', async () => {
	let fetches = 0;
	class ForbiddenWebSocket {
		constructor() {
			throw new Error('component SSR must not open WebSocket');
		}
	}
	const client = createDistributedSvelteKit({
		boundaries: [],
		session: { getAuth: () => ({ accessToken: 'server-token' }) },
		browser: false,
		fetch: async () => {
			fetches += 1;
			throw new Error('component SSR must not fetch');
		},
		webSocket: ForbiddenWebSocket
	});
	const store = client.operation(TodosArtifact).use();
	const values = [];
	const unsubscribe = store.subscribe((snapshot) => values.push(snapshot.status));
	await flushMicrotasks();
	assert.deepEqual(values, ['loading']);
	assert.equal(fetches, 0);
	unsubscribe();
	client.destroy();
});

test('caller projection cancellation does not hide a globally pending command', async () => {
	let resolveGlobal;
	let rejectCaller;
	const globallyProjected = new Promise((resolve) => {
		resolveGlobal = resolve;
	});
	const callerProjected = new Promise((_resolve, reject) => {
		rejectCaller = reject;
	});
	const receiptValue = {
		commandId: 'command-pending-after-caller-deadline',
		state: 'succeeded_pending_projection',
		result: { accepted: true },
		metadata: {},
		status: () => Promise.resolve({ state: 'succeeded_pending_projection' }),
		projected: callerProjected
	};
	Object.defineProperty(receiptValue, replicaCommandProjectedLifecycle, {
		value: globallyProjected
	});
	const receipt = Object.freeze(receiptValue);
	const client = createDistributedSvelteKit({
		boundaries: [],
		session: { getAuth: () => ({ accessToken: 'token' }) },
		browser: false,
		createCommands() {
			return {
				commands: {
					save: () => Promise.resolve(receipt)
				},
				dispose() {}
			};
		}
	});
	const query = client.operation(TodosArtifact).use();
	const unsubscribe = query.subscribe(() => undefined);

	const returned = await client.commands.save();
	assert.equal(returned, receipt);
	assert.deepEqual(query.get().pending, [receipt]);

	const callerOutcome = assert.rejects(
		callerProjected,
		/caller deadline/
	);
	rejectCaller(new Error('caller deadline'));
	await callerOutcome;
	await flushMicrotasks();
	assert.deepEqual(
		query.get().pending,
		[receipt],
		'the adapter must observe the command-wide causal lifecycle'
	);

	resolveGlobal({
		commandId: receipt.commandId,
		state: 'atomic'
	});
	await flushMicrotasks();
	assert.deepEqual(query.get().pending, []);
	unsubscribe();
	query.destroy();
	client.destroy();
});

test('pre-subscribe refetch uses one temporary HTTP watch and never opens live', async () => {
	const transport = new ControlledReplicaTransport();
	const replica = createDistributedReplica({ transport });
	const store = bindSveltekitOperation(replica, TodosArtifact).use(
		{},
		{ live: true }
	);

	const refetch = store.refetch();
	await flushMicrotasks();
	assert.equal(transport.fetches.length, 1);
	assert.equal(transport.lives.length, 0);
	transport.fetches[0].response.resolve(
		todoFrame(
			TodosArtifact,
			[{ id: 'todo-1', title: 'prefetched', status: 'open' }],
			{ position: '1' }
		)
	);
	await refetch;
	assert.equal(store.get().data.todos[0].title, 'prefetched');

	const unsubscribe = store.subscribe(() => undefined);
	await flushMicrotasks();
	assert.equal(transport.lives.length, 1);
	assert.equal(transport.fetches.length, 1, 'hydrated cache avoids a duplicate first fetch');
	unsubscribe();
	store.destroy();
});

test('Svelte uses replica-owned revalidation with an undecorated GraphQL transport', async () => {
	const requests = [];
	let position = 0;
	let commandTransport;
	const client = createDistributedSvelteKit({
		boundaries: [],
		session: { getAuth: () => ({ accessToken: 'token' }) },
		fetch: async (_url, init) => {
			requests.push(JSON.parse(init.body));
			position += 1;
			return jsonResponse(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-1', title: `server-${position}`, status: 'open' }],
					{ position: String(position) }
				)
			);
		},
		createCommands(_replica, transport) {
			commandTransport = transport;
			return {
				commands: Object.freeze({}),
				dispose() {}
			};
		}
	});
	const store = client.operation(TodosArtifact).use({}, { live: false });
	const unsubscribe = store.subscribe(() => undefined);
	await store.refetch();
	assert.equal(requests.length, 1);
	assert.equal(store.get().status, 'ready');
	assert.equal(commandTransport, client.transport);
	assert.equal(commandTransport.revalidate, undefined);

	const plan = (dependencies, models = []) => ({
		dependencies,
		models,
		relationships: []
	});
	await client.replica.revalidate(plan(['unrelated']));
	assert.equal(
		requests.length,
		1,
		'an unrelated dependency inventory must not refresh this operation'
	);

	await Promise.all([
		client.replica.revalidate(plan(['todos'])),
		client.replica.revalidate(plan(['todos']))
	]);
	assert.equal(requests.length, 2, 'matching concurrent plans share one HTTP request');
	assert.equal(store.get().data.todos[0].title, 'server-2');

	await client.replica.revalidate(plan([], ['TodoView']));
	assert.equal(requests.length, 3, 'model inventory also targets the operation');

	unsubscribe();
	store.destroy();
	client.destroy();
});

test('Svelte composition shares one diagnostics sink with operations and generated commands', () => {
	const diagnostics = createReplicaDiagnostics();
	const operationHash = `sha256:${'b'.repeat(64)}`;
	const RenameTodo = Object.freeze({
		version: 1,
		name: 'todo.rename',
		mutationField: 'renameTodo',
		document:
			'mutation RenameTodo($commandId: ID!, $input: RenameTodoInput!) { renameTodo(commandId: $commandId, input: $input) }',
		operationHash,
		protocol: Object.freeze({
			version: 1,
			schemaHash: REACT_FIXTURE_SCHEMA,
			protocolHash: `sha256:${'c'.repeat(64)}`,
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: operationHash,
			trustedPresets: Object.freeze([])
		}),
		input: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: 'RenameTodoInput',
				fields: Object.freeze([
					Object.freeze({
						name: 'id',
						typeName: 'ID',
						nullable: false,
						list: false,
						itemNullable: false,
						codec: 'string'
					})
				])
			})
		}),
		output: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: 'RenameTodoResult',
				fields: Object.freeze([
					Object.freeze({
						name: 'succeeded',
						typeName: 'Boolean',
						nullable: false,
						list: false,
						itemNullable: false,
						codec: 'boolean'
					})
				])
			})
		}),
		consistency: COMMAND_CONSISTENCY.SUCCEEDED,
		effects: Object.freeze({
			version: 1,
			operations: Object.freeze([
				Object.freeze({
					kind: 'invalidate_model',
					model: 'TodoView'
				})
			]),
			fallback: 'revalidate'
		}),
		revalidation: Object.freeze({
			version: 1,
			required: true,
			dependencies: Object.freeze(['todos']),
			models: Object.freeze(['TodoView']),
			relationships: Object.freeze([])
		}),
		trustedPresets: Object.freeze([])
	});
	let factoryOptions;
	const client = createDistributedSvelteKit({
		boundaries: [],
		session: { getAuth: () => ({ accessToken: 'token' }) },
		replica: { diagnostics },
		createCommands(replica, transport, options) {
			factoryOptions = options;
			return createReplicaCommandRuntime(
				replica,
				transport,
				{ renameTodo: RenameTodo },
				options
			);
		}
	});

	client.replica.retainOperation(TodosArtifact, {});
	const snapshot = diagnostics.snapshot();
	assert.equal(factoryOptions.diagnostics, diagnostics);
	assert.deepEqual(
		snapshot.artifacts.operations.map(({ id }) => id),
		[TodosArtifact.id]
	);
	assert.deepEqual(
		snapshot.artifacts.commands.map(({ name }) => name),
		[RenameTodo.name]
	);
	client.destroy();
});
