import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createDistributedReplica,
	replicaRecordKey
} from '../dist/replica/index.js';
import { replicaCommandAuthority } from '../dist/replica/command-runtime.js';

const AUTH_SCHEMA_HASH = `sha256:${'a'.repeat(64)}`;
const AUTH_PROTOCOL_HASH = `sha256:${'b'.repeat(64)}`;
const USER_SURFACE = Object.freeze({ kind: 'role', name: 'user' });

const Todo = Object.freeze({
	id: 'TodoView',
	identityFields: Object.freeze(['id'])
});

const NoVariables = Object.freeze({
	version: 2,
	limits: Object.freeze({
		maxDepth: 8,
		maxBoolWidth: 256,
		maxInList: 1000
	}),
	variables: Object.freeze({}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({})
});

function operation(id, field = 'todos', schemaHash = 'schema-a') {
	return Object.freeze({
		id,
		document: `query ${id.replaceAll(':', '_')} { ${field} { id title } }`,
		protocol: Object.freeze({
			version: 1,
			schemaHash,
			surface: USER_SURFACE,
			operation: id,
			trustedPresets: Object.freeze([])
		}),
		variableCodec: NoVariables,
		roots: Object.freeze([
			Object.freeze({
				responseKey: field,
				field,
				cardinality: 'many',
				nullable: false,
				dependencies: Object.freeze([field]),
				selection: Object.freeze({
					typename: Todo.id,
					storage: Object.freeze({
						kind: 'normalized',
						model: Todo.id,
						identityFields: Todo.identityFields
					}),
					members: Object.freeze([
						Object.freeze({
							kind: 'scalar',
							responseKey: 'id',
							field: 'id',
							codec: 'ID',
							nullable: false
						}),
						Object.freeze({
							kind: 'scalar',
							responseKey: 'title',
							field: 'title',
							codec: 'String',
							nullable: false
						})
					])
				})
			})
		])
	});
}

const Todos = operation('query:todos');
const HiddenTodos = operation('query:hidden-todos', 'hidden_todos');
const ElevatedTodos = operation('query:elevated-todos', 'todos', 'schema-elevated');
const AuthorizedTodosBase = operation(
	'query:authorized-todos',
	'todos',
	AUTH_SCHEMA_HASH
);
const AuthorizedTodos = Object.freeze({
	...AuthorizedTodosBase,
	protocol: Object.freeze({
		...AuthorizedTodosBase.protocol,
		trustedPresets: Object.freeze([
			Object.freeze({ name: 'owner', codec: 'string' })
		])
	})
});
const QueryScopedTodos = Object.freeze({
	...operation('query:scoped-todos', 'todos', AUTH_SCHEMA_HASH),
	protocol: Object.freeze({
		...operation('query:scoped-todos', 'todos', AUTH_SCHEMA_HASH).protocol,
		trustedPresets: Object.freeze([
			Object.freeze({ name: 'owner', codec: 'string' })
		])
	})
});
const TodosWithSecret = Object.freeze({
	...operation('query:todos-with-secret'),
	roots: Object.freeze([
		Object.freeze({
			...Todos.roots[0],
			selection: Object.freeze({
				...Todos.roots[0].selection,
				members: Object.freeze([
					...Todos.roots[0].selection.members,
					Object.freeze({
						kind: 'scalar',
						responseKey: 'secret',
						field: 'secret',
						codec: 'String',
						nullable: false
					})
				])
			})
		})
	])
});

function frame(
	artifact,
	rows,
	{
		cacheScope = 'cache:a',
		authorizationGeneration = 'auth-1',
		position = '1',
		revision = position,
		recordScope = (row) => `record:${row.id}`,
		records,
		snapshotScope = `snapshot:${artifact.id}`,
		indexScope = `index:${artifact.id}`,
		trustedPresets = []
	} = {}
) {
	const field = artifact.roots[0].field;
	const recordEvidence =
		records ??
		rows.map((row, index) => ({
			path: [field, String(index)],
			model: Todo.id,
			scopeToken: recordScope(row),
			incarnation: '1',
			revision,
			tombstone: false
		}));
	return {
		data: { [field]: rows },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: artifact.protocol.schemaHash,
				authorizationGeneration,
				cacheScope,
				operation: artifact.protocol.operation,
				trustedPresets,
				snapshot: {
					scopeToken: snapshotScope,
					recordsComplete: true,
					indexesComparable: true,
					records: recordEvidence,
					indexes: [
						{
							projection: `${field}-projector`,
							scopeToken: indexScope,
							position
						}
					],
					observations: []
				}
			}
		}
	};
}

function write(replica, artifact, rows, options) {
	replica.writeResult(artifact, {}, frame(artifact, rows, options), 'network');
}

function jsonClone(value) {
	return JSON.parse(JSON.stringify(value));
}

test('only a server response establishes a scope and permits SSR dehydration', () => {
	const replica = createDistributedReplica();

	assert.equal(replica.scope, undefined);
	assert.throws(
		() => replica.dehydrate(),
		/server establishes an authoritative scope/
	);

	write(replica, Todos, [{ id: 'todo-1', title: 'one' }]);
	assert.deepEqual(replica.scope, {
		protocolVersion: 1,
		schemaHash: 'schema-a',
		authorizationGeneration: 'auth-1',
		cacheScope: 'cache:a'
	});
	assert.equal(Object.isFrozen(replica.scope), true);
});

test('query artifacts independently bind exact scope-preset inventories', () => {
	const replica = createDistributedReplica();
	write(replica, QueryScopedTodos, [{ id: 'todo-1', title: 'authorized' }], {
		trustedPresets: [
			{ name: 'owner', codec: 'string', value: 'user-1' }
		]
	});
	assert.equal(replica.scope.cacheScope, 'cache:a');

	assert.throws(
		() =>
			write(replica, QueryScopedTodos, [{ id: 'todo-2', title: 'forged' }], {
				position: '2',
				revision: '2',
				trustedPresets: [
					{ name: 'owner', codec: 'string', value: 'user-2' }
				]
			}),
		(error) =>
			error?.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
			error?.path === 'extensions.distributed.trustedPresets'
	);
	assert.equal(
		replica.scope,
		undefined,
		'a changed value inside one authoritative scope purges the generation'
	);

	for (const trustedPresets of [
		[],
		[
			{ name: 'owner', codec: 'string', value: 'user-1' },
			{ name: 'forged-extra', codec: 'string', value: 'forged' }
		],
		[{ name: 'owner', codec: 'int32', value: 1 }]
	]) {
		const candidate = createDistributedReplica();
		assert.throws(
			() =>
				write(candidate, QueryScopedTodos, [], {
					trustedPresets
				}),
			(error) =>
				error?.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
				error?.path === 'extensions.distributed.trustedPresets'
		);
		assert.equal(candidate.scope, undefined);
	}
});

test('bound command authority is scope-derived, hydration-safe, and generation-fenced', () => {
	const contract = Object.freeze({
		protocolVersion: 1,
		schemaHash: AUTH_SCHEMA_HASH,
		protocolHash: AUTH_PROTOCOL_HASH,
		surface: USER_SURFACE,
		trustedPresets: Object.freeze([
			Object.freeze({ name: 'owner', codec: 'string' })
		])
	});
	const replica = createDistributedReplica();
	const registration = replica[replicaCommandAuthority](contract);
	const initial = registration.read();
	assert.equal(initial.scope, undefined);
	assert.deepEqual(initial.trustedPresets, []);
	assert.equal(initial.signal.aborted, false);

	write(replica, AuthorizedTodos, [{ id: 'todo-1', title: 'authorized' }], {
		trustedPresets: [
			{ name: 'owner', codec: 'string', value: 'user-1' }
		]
	});
	const established = registration.read();
	assert.equal(established.scope.cacheScope, 'cache:a');
	assert.deepEqual(established.trustedPresets, [
		{ name: 'owner', codec: 'string', value: 'user-1' }
	]);
	assert.equal(Object.isFrozen(established.trustedPresets), true);

	replica.read(AuthorizedTodos, {});
	const state = replica.dehydrate();
	const browser = createDistributedReplica();
	const browserRegistration = browser[replicaCommandAuthority](contract);
	assert.equal(browser.hydrate(jsonClone(state), state.scope), true);
	assert.deepEqual(browserRegistration.read().trustedPresets, [
		{ name: 'owner', codec: 'string', value: 'user-1' }
	]);

	assert.throws(
		() =>
			write(replica, AuthorizedTodos, [{ id: 'todo-2', title: 'invalid' }], {
				cacheScope: 'cache:b',
				position: '2',
				revision: '2',
				trustedPresets: []
			}),
		(error) =>
			error?.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
			error?.path === 'extensions.distributed.trustedPresets'
	);
	assert.equal(established.signal.aborted, true);
	assert.equal(replica.scope, undefined);
	assert.deepEqual(registration.read().trustedPresets, []);

	assert.throws(
		() =>
			replica[replicaCommandAuthority]({
				...contract,
				surface: { kind: 'role', name: 'admin' }
			}),
		/does not match the active replica client surface/
	);
	registration.dispose();
	browserRegistration.dispose();
});

test('SSR dehydration includes confirmed reachable state and excludes optimistic or unrendered data', () => {
	const replica = createDistributedReplica();
	write(replica, HiddenTodos, [{ id: 'hidden-1', title: 'private hidden row' }]);
	write(replica, TodosWithSecret, [
		{ id: 'todo-1', title: 'older', secret: 'not rendered' }
	]);
	write(replica, Todos, [{ id: 'todo-1', title: 'confirmed' }], {
		position: '2',
		revision: '2'
	});
	replica.read(Todos, {});
	replica.createOptimisticLayer('cmd-1', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic' }
		});
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'optimistic');

	const state = replica.dehydrate();
	const payload = state.payload;
	assert.deepEqual(
		payload.cache.records.map((record) => record.key),
		[replicaRecordKey(Todo, 'todo-1')]
	);
	assert.equal(
		payload.cache.records.some((record) => record.key.includes('hidden-1')),
		false
	);
	assert.equal(
		Object.prototype.hasOwnProperty.call(
			payload.cache.records[0].fields,
			'secret'
		),
		false
	);
	assert.equal(payload.cache.indexes.length, 1);

	const browserReplica = createDistributedReplica();
	assert.equal(browserReplica.hydrate(jsonClone(state), state.scope), true);
	assert.equal(
		browserReplica.read(Todos, {}).data.todos[0].title,
		'confirmed'
	);
	assert.deepEqual(browserReplica.scope, state.scope);
});

test('same-scope re-hydrate retains confirmed indexes omitted from a later route seed', () => {
	// Soft-nav SSR only dehydrates the destination route. A warm SPA client may
	// already hold confirmed windows from other routes; those must stay.
	const warm = createDistributedReplica();
	write(warm, Todos, [{ id: 'todo-1', title: 'todos-row' }], { position: '1' });
	write(warm, HiddenTodos, [{ id: 'hidden-1', title: 'history-page' }], {
		position: '1'
	});
	warm.read(Todos, {});
	warm.read(HiddenTodos, {});
	assert.equal(warm.read(Todos, {}).data.todos[0].title, 'todos-row');
	assert.equal(
		warm.read(HiddenTodos, {}).data.hidden_todos[0].title,
		'history-page'
	);

	const pageSeed = createDistributedReplica();
	write(
		pageSeed,
		Todos,
		[{ id: 'todo-1', title: 'todos-row-v2' }],
		{ position: '2' }
	);
	pageSeed.read(Todos, {});
	// Intentionally omit HiddenTodos from the second seed (route subset).
	const seed = pageSeed.dehydrate();
	assert.equal(
		seed.payload.cache.records.some((record) => record.key.includes('hidden-1')),
		false,
		'fixture seed must omit the retained index'
	);

	assert.equal(warm.hydrate(jsonClone(seed), seed.scope), true);
	// Overlapping route window upserts.
	assert.equal(warm.read(Todos, {}).data.todos[0].title, 'todos-row-v2');
	// Omitted confirmed index remains materializable without a forced wipe.
	assert.equal(warm.read(HiddenTodos, {}).complete, true);
	assert.equal(
		warm.read(HiddenTodos, {}).data.hidden_todos[0].title,
		'history-page'
	);
});

test('older same-scope route seed cannot regress confirmed query membership or record clocks', () => {
	const old = createDistributedReplica();
	write(old, Todos, [{ id: 'todo-1', title: 'old' }], { position: '1' });
	old.read(Todos, {});
	const seed = old.dehydrate();
	const warm = createDistributedReplica();
	write(warm, Todos, [{ id: 'todo-1', title: 'current' }, { id: 'todo-2', title: 'new member' }], { position: '3' });
	warm.read(Todos, {});
	const before = warm.dehydrate();
	assert.equal(warm.hydrate(seed, seed.scope), true);
	assert.deepEqual(warm.read(Todos, {}).data.todos, [{ id: 'todo-1', title: 'current' }, { id: 'todo-2', title: 'new member' }]);
	const after = warm.dehydrate();
	assert.deepEqual(after.payload.operations, before.payload.operations);
	assert.deepEqual(after.payload.recordClocks, before.payload.recordClocks);
});

test('hydration rejects malformed, cross-scope, and elevated-schema state atomically', () => {
	const current = createDistributedReplica();
	write(current, Todos, [{ id: 'todo-1', title: 'current' }]);
	current.read(Todos, {});
	const currentState = current.dehydrate();
	assert.equal(createDistributedReplica().hydrate(currentState, undefined), false);

	const otherScope = createDistributedReplica();
	write(
		otherScope,
		Todos,
		[{ id: 'todo-1', title: 'other tenant' }],
		{ cacheScope: 'cache:b', recordScope: () => 'record:b' }
	);
	otherScope.read(Todos, {});
	assert.equal(
		current.hydrate(otherScope.dehydrate(), current.scope),
		false
	);
	assert.equal(current.read(Todos, {}).data.todos[0].title, 'current');
	assert.equal(current.scope.cacheScope, 'cache:a');

	const otherAuthorization = createDistributedReplica();
	write(
		otherAuthorization,
		Todos,
		[{ id: 'todo-1', title: 'other authorization' }],
		{ authorizationGeneration: 'auth-2' }
	);
	otherAuthorization.read(Todos, {});
	assert.equal(
		current.hydrate(otherAuthorization.dehydrate(), current.scope),
		false
	);
	assert.equal(current.read(Todos, {}).data.todos[0].title, 'current');

	const malformed = jsonClone(current.dehydrate());
	malformed.payload.cache.records[0].revision = 'not-a-decimal';
	assert.equal(current.hydrate(malformed, current.scope), false);
	assert.equal(current.read(Todos, {}).data.todos[0].title, 'current');
	const inconsistent = jsonClone(current.dehydrate());
	inconsistent.payload.recordClocks[0][1].revision = '99';
	assert.equal(current.hydrate(inconsistent, current.scope), false);
	assert.equal(current.read(Todos, {}).data.todos[0].title, 'current');

	const elevated = createDistributedReplica();
	elevated.read(ElevatedTodos, {});
	assert.equal(elevated.hydrate(current.dehydrate(), current.scope), false);
	assert.equal(elevated.scope, undefined);
	assert.equal(elevated.read(ElevatedTodos, {}).complete, false);
});

test('dehydration drops historical list members and destroyed watch reachability', () => {
	const replica = createDistributedReplica();
	const watch = replica.watch(Todos, {});
	write(
		replica,
		Todos,
		[
			{ id: 'todo-a', title: 'a' },
			{ id: 'todo-b', title: 'b' }
		],
		{ position: '1', revision: '1' }
	);
	write(replica, Todos, [{ id: 'todo-a', title: 'a2' }], {
		position: '2',
		revision: '2'
	});
	const state = replica.dehydrate();
	assert.deepEqual(
		state.payload.cache.records.map((record) => record.key),
		[replicaRecordKey(Todo, 'todo-a')]
	);

	watch.destroy();
	const afterDestroy = replica.dehydrate();
	assert.deepEqual(afterDestroy.payload.cache.records, []);
	assert.deepEqual(afterDestroy.payload.cache.indexes, []);
});

test('authorization invalidation aborts old HTTP and starts one generation-fenced replacement', async () => {
	let request;
	const requests = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(next) {
				request = next;
				let resolve;
				const result = new Promise((done) => {
					resolve = done;
				});
				requests.push({ request: next, result, resolve });
				return result;
			}
		}
	});
	const watch = replica.watch(Todos, {});
	await Promise.resolve();
	assert.equal(request.signal.aborted, false);
	assert.equal(requests.length, 1);
	const generation = replica.authorizationGeneration;

	replica.invalidateAuthorization();
	assert.equal(requests[0].request.signal.aborted, true);
	assert.equal(replica.authorizationGeneration, generation + 1);
	await Promise.resolve();
	assert.equal(requests.length, 2);
	requests[0].resolve(
		frame(Todos, [{ id: 'todo-late', title: 'must not appear' }])
	);
	await requests[0].result;
	await Promise.resolve();

	assert.equal(watch.get().complete, false);
	assert.deepEqual(watch.get().data, {});
	assert.equal(replica.scope, undefined);
	requests[1].resolve(
		frame(Todos, [{ id: 'todo-current', title: 'current generation' }])
	);
	await requests[1].result;
	await Promise.resolve();
	assert.equal(watch.get().data.todos[0].title, 'current generation');
	watch.destroy();
});

test('a malformed response purges the prior scope without publishing its replacement', () => {
	const replica = createDistributedReplica();
	write(replica, Todos, [{ id: 'todo-a', title: 'scope a' }]);
	assert.equal(replica.scope.cacheScope, 'cache:a');
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'scope a');

	assert.throws(
		() =>
			write(replica, Todos, [{ id: 'todo-b', title: 'invalid scope b' }], {
				cacheScope: 'cache:b',
				position: '2',
				revision: '2',
				records: []
			}),
		(error) =>
			error?.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
			error?.path === 'extensions.distributed.snapshot.records'
	);
	assert.equal(replica.scope, undefined);
	assert.equal(replica.read(Todos, {}).complete, false);
	assert.deepEqual(replica.read(Todos, {}).data, {});
});

test('hydrated protocol clocks reject stale rows and tombstone resurrection', () => {
	const serverReplica = createDistributedReplica();
	write(serverReplica, Todos, [{ id: 'todo-1', title: 'first' }]);
	serverReplica.read(Todos, {});
	write(serverReplica, Todos, [], {
		position: '9',
		revision: '9',
		records: [
			{
				path: ['todos', '0'],
				model: Todo.id,
				scopeToken: 'record:todo-1',
				incarnation: '1',
				revision: '9',
				tombstone: true
			}
		]
	});
	assert.deepEqual(serverReplica.read(Todos, {}).data.todos, []);

	const browserReplica = createDistributedReplica();
	const state = serverReplica.dehydrate();
	assert.equal(browserReplica.hydrate(jsonClone(state), state.scope), true);
	assert.deepEqual(browserReplica.read(Todos, {}).data.todos, []);

	write(
		browserReplica,
		Todos,
		[{ id: 'todo-1', title: 'stale resurrection' }],
		{
			position: '8',
			revision: '8',
			recordScope: () => 'record:todo-1'
		}
	);
	assert.deepEqual(browserReplica.read(Todos, {}).data.todos, []);
	assert.equal(browserReplica.inspectRecord(Todo, 'todo-1'), undefined);
});

test('SSR callers get isolated replicas rather than process-wide state', () => {
	const firstRequest = createDistributedReplica();
	const secondRequest = createDistributedReplica();
	write(firstRequest, Todos, [{ id: 'todo-1', title: 'request one' }]);

	assert.equal(firstRequest.read(Todos, {}).complete, true);
	assert.equal(secondRequest.read(Todos, {}).complete, false);
	assert.equal(secondRequest.scope, undefined);
});
