import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createReplicaIndexedDbPersistence,
	REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED
} from '../dist/replica/persistence.js';
import {
	createDistributedReplica,
	replicaIndexKey,
	replicaRecordKey
} from '../dist/replica/index.js';
import { replicaCommandAuthority } from '../dist/replica/command-runtime.js';

const DATABASE = 'replica-persistence-test';
const Todo = Object.freeze({
	id: 'TodoView',
	identityFields: Object.freeze(['id'])
});
const Secret = Object.freeze({
	id: 'SecretView',
	identityFields: Object.freeze(['id'])
});

const TodoKey = replicaRecordKey(Todo, 'todo-1');
const SecretKey = replicaRecordKey(Secret, 'secret-1');
const TodoIndex = replicaIndexKey({ field: 'todos' });
const SecretIndex = replicaIndexKey({ field: 'secrets' });
const MixedIndex = replicaIndexKey({ field: 'mixed' });
const EmptyIndex = replicaIndexKey({ field: 'emptyTodos' });

const TodoPolicy = Object.freeze({
	models: Object.freeze({
		TodoView: Object.freeze({
			retention: 'persist-confirmed',
			sensitive: false
		}),
		SecretView: Object.freeze({
			retention: 'persist-confirmed',
			sensitive: true
		})
	})
});

function scope(
	cacheScope = 'cache:tenant-a',
	schemaHash = 'schema:a',
	authorizationGeneration = 'auth-1'
) {
	return {
		protocolVersion: 1,
		schemaHash,
		authorizationGeneration,
		cacheScope
	};
}

function field(revision, value) {
	return { revision, value };
}

function record(key, revision, fields, links = {}) {
	return {
		key,
		revision,
		incarnation: '1',
		fields,
		links
	};
}

function index(key, fieldName, revision, records) {
	return {
		key,
		revision,
		records,
		complete: true,
		deleted: false,
		metadata: {
			field: fieldName,
			arguments: {},
			coverage: { kind: 'complete' },
			dependencies: [fieldName]
		}
	};
}

function protocolState(operation, fieldName, revision, indexKey, pathRecord) {
	return {
		operation,
		snapshotScope: `snapshot:${operation}`,
		indexClocks: [
			[
				`${fieldName}-projector`,
				{
					scopeToken: `index:${operation}`,
					position: revision
				}
			]
		],
		indexRevision: revision,
		indexKeys: [indexKey],
		pathRecords:
			pathRecord === undefined ? [] : [[`${fieldName}.0`, pathRecord]],
		cursors: []
	};
}

function operation(key, state) {
	return {
		key: `protocol:${key}`,
		query: state,
		active: 'query',
		generation: 0
	};
}

function clock(scopeToken, revision = '1', tombstone = false) {
	return {
		scopeToken,
		incarnation: '1',
		revision,
		tombstone
	};
}

function state(authoritativeScope = scope()) {
	return {
		version: 1,
		scope: authoritativeScope,
		payload: {
			cache: {
				version: 1,
				records: [
					record(
						TodoKey,
						'1',
						{
							id: field('1', 'todo-1'),
							title: field('1', 'visible')
						},
						{
							self: field('1', TodoKey),
							secret: field('1', SecretKey)
						}
					),
					record(SecretKey, '1', {
						id: field('1', 'secret-1'),
						value: field('1', 'must stay memory-only')
					})
				],
				indexes: [
					index(TodoIndex, 'todos', '1', [TodoKey]),
					index(SecretIndex, 'secrets', '2', [SecretKey]),
					index(MixedIndex, 'mixed', '3', [TodoKey, SecretKey]),
					index(EmptyIndex, 'emptyTodos', '4', [])
				]
			},
			operations: [
				operation(
					'todos',
					protocolState('query:todos', 'todos', '1', TodoIndex, TodoKey)
				),
				operation(
					'secrets',
					protocolState(
						'query:secrets',
						'secrets',
						'2',
						SecretIndex,
						SecretKey
					)
				),
				operation(
					'mixed',
					protocolState('query:mixed', 'mixed', '3', MixedIndex, TodoKey)
				),
				operation(
					'empty',
					protocolState('query:empty', 'emptyTodos', '4', EmptyIndex)
				)
			],
			recordClocks: [
				[TodoKey, clock('record:todo')],
				[SecretKey, clock('record:secret')]
			],
			anonymousRecordClocks: [
				[
					'record:anonymous:todo',
					{
						model: 'TodoView',
						clock: clock('record:anonymous:todo')
					}
				],
				[
					'record:anonymous:secret',
					{
						model: 'SecretView',
						clock: clock('record:anonymous:secret')
					}
				]
			],
			trustedPresets: [
					{
						name: 'current_user_id',
						codec: 'string',
						value: 'user-1'
					}
				],
			nextIndexRevision: '4'
		}
	};
}

test('persistence is explicit, IndexedDB-only, and fails closed without model policy', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE
	});

	// The opt-in command outbox exists, but nothing is durable without policy.
	assert.equal(REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED, true);
	assert.equal(persistence.supportsOfflineCommandOutbox, true);
	assert.equal(await persistence.save(state()), false);
	assert.equal(factory.entries(DATABASE).size, 0);
	assert.equal('localStorage' in factory, false);
	persistence.close();
});

test('confirmed state is policy-filtered before a second instance restores it', async () => {
	const factory = new FakeIndexedDbFactory();
	const first = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	assert.equal(await first.save(state()), true);

	const [stored] = [...factory.entries(DATABASE).values()];
	assert.deepEqual(
		stored.state.payload.cache.records.map((entry) => entry.key),
		[TodoKey]
	);
	assert.deepEqual(
		Object.keys(stored.state.payload.cache.records[0].links),
		['self']
	);
	assert.deepEqual(
		stored.state.payload.cache.indexes.map((entry) => entry.key),
		[TodoIndex]
	);
	assert.deepEqual(
		stored.state.payload.operations.map((entry) => entry.key),
		['protocol:todos']
	);
	assert.deepEqual(
		stored.state.payload.recordClocks.map(([key]) => key),
		[TodoKey]
	);
	assert.deepEqual(
		stored.state.payload.anonymousRecordClocks.map(([, entry]) => entry.model),
		['TodoView']
	);
	assert.deepEqual(
		stored.state.payload.trustedPresets,
		state().payload.trustedPresets
	);

	const second = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	const restored = await second.restore(scope());
	assert.deepEqual(restored, stored.state);
	assert.equal(Object.isFrozen(restored), true);
	assert.equal(Object.isFrozen(restored.payload), true);
	assert.equal(
		createDistributedReplica().hydrate(restored, scope()),
		true,
		JSON.stringify(restored, null, 2)
	);

	// Looking under a caller-supplied or decoded scope cannot find the entry.
	assert.equal(await second.restore(scope('cache:tenant-b')), undefined);
	assert.equal(factory.entries(DATABASE).size, 1);
	first.close();
	second.close();
});

test('restored trusted presets satisfy the generated command authority contract', async () => {
	const factory = new FakeIndexedDbFactory();
	const schemaHash = `sha256:${'a'.repeat(64)}`;
	const authoritativeScope = scope('cache:tenant-a', schemaHash);
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	assert.equal(await persistence.save(state(authoritativeScope)), true);
	const restored = await persistence.restore(authoritativeScope);
	const replica = createDistributedReplica();

	assert.equal(replica.hydrate(restored, authoritativeScope), true);
	const registration = replica[replicaCommandAuthority]({
		protocolVersion: 1,
		schemaHash,
		protocolHash: `sha256:${'b'.repeat(64)}`,
		surface: { kind: 'role', name: 'user' },
		trustedPresets: [
			{ name: 'current_user_id', codec: 'string' }
		]
	});
	assert.deepEqual(registration.read().trustedPresets, [
		{ name: 'current_user_id', codec: 'string', value: 'user-1' }
	]);
	registration.dispose();
	persistence.close();
});

test('empty and mixed-policy indexes are conservatively dropped', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	await persistence.save(state());
	const restored = await persistence.restore(scope());

	assert.deepEqual(
		restored.payload.cache.indexes.map((entry) => entry.key),
		[TodoIndex]
	);
	assert.equal(
		restored.payload.operations.some(
			(entry) =>
				entry.key === 'protocol:mixed' || entry.key === 'protocol:empty'
		),
		false
	);
	persistence.close();
});

test('a stricter current policy rewrites and removes previously durable data', async () => {
	const factory = new FakeIndexedDbFactory();
	const permissive = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: {
			models: {
				TodoView: {
					retention: 'persist-confirmed',
					sensitive: false
				},
				SecretView: {
					retention: 'persist-confirmed',
					sensitive: false
				}
			}
		}
	});
	await permissive.save(state());
	assert.deepEqual(
		[...factory.entries(DATABASE).values()][0].state.payload.cache.records.map(
			(entry) => entry.key
		),
		[SecretKey, TodoKey]
	);

	const restricted = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	await restricted.restore(scope());
	assert.deepEqual(
		[...factory.entries(DATABASE).values()][0].state.payload.cache.records.map(
			(entry) => entry.key
		),
		[TodoKey]
	);
	assert.deepEqual(
		[...factory.entries(DATABASE).values()][0].state.payload.trustedPresets,
		state().payload.trustedPresets
	);
	permissive.close();
	restricted.close();
});

test('confirmed tombstone fences survive persistence for an allowed model', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	const deletedKey = replicaRecordKey(Todo, 'todo-deleted');
	const input = state();
	input.payload.cache.records.push({
		key: deletedKey,
		revision: '5',
		incarnation: '1',
		tombstoneRevision: '5',
		fields: {},
		links: {}
	});
	input.payload.recordClocks.push([
		deletedKey,
		clock('record:todo-deleted', '5', true)
	]);

	assert.equal(await persistence.save(input), true);
	const restored = await persistence.restore(scope());
	assert.deepEqual(
		restored.payload.cache.records.find((entry) => entry.key === deletedKey),
		{
			key: deletedKey,
			revision: '5',
			incarnation: '1',
			tombstoneRevision: '5',
			fields: {},
			links: {}
		}
	);
	assert.equal(createDistributedReplica().hydrate(restored, scope()), true);
	persistence.close();
});

test('corrupt, unsupported, and internally mismatched entries are discarded', async (t) => {
	for (const [name, corruption] of [
		[
			'unsupported persistence version',
			(entry) => {
				entry.formatVersion = 2;
			}
		],
		[
			'unsupported dehydration version',
			(entry) => {
				entry.state.version = 99;
			}
		],
		[
			'misfiled authoritative scope',
			(entry) => {
				entry.state.scope.cacheScope = 'cache:other';
			}
		],
		[
			'malformed causal revision',
			(entry) => {
				entry.state.payload.cache.records[0].revision = 'not-a-decimal';
			}
		]
	]) {
		await t.test(name, async () => {
			const factory = new FakeIndexedDbFactory();
			const persistence = createReplicaIndexedDbPersistence({
				indexedDB: factory,
				databaseName: DATABASE,
				policy: TodoPolicy
			});
			await persistence.save(state());
			const [entry] = [...factory.entries(DATABASE).values()];
			corruption(entry);

			assert.equal(await persistence.restore(scope()), undefined);
			assert.equal(factory.entries(DATABASE).size, 0);
			persistence.close();
		});
	}
});

test('malformed or command-like dehydration fields never enter IndexedDB', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	const malformed = state();
	malformed.payload.optimisticLayers = [{ id: 'command-1' }];

	await assert.rejects(
		() => persistence.save(malformed),
		/unknown field state\.payload\.optimisticLayers/
	);
	assert.equal(factory.entries(DATABASE).size, 0);
	persistence.close();
});

test('scope identity includes schema and authorization fingerprints exactly', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: TodoPolicy
	});
	await persistence.save(state(scope('cache:tenant-a', 'schema:a')));
	await persistence.save(state(scope('cache:tenant-a', 'schema:b')));
	await persistence.save(
		state(scope('cache:tenant-a', 'schema:a', 'auth-2'))
	);

	assert.equal(factory.entries(DATABASE).size, 3);
	assert.notEqual(
		[...factory.entries(DATABASE).keys()][0],
		[...factory.entries(DATABASE).keys()][1]
	);
	assert.equal(
		(await persistence.restore(scope('cache:tenant-a', 'schema:a'))).scope
			.schemaHash,
		'schema:a'
	);
	assert.equal(
		(await persistence.restore(scope('cache:tenant-a', 'schema:b'))).scope
			.schemaHash,
		'schema:b'
	);
	assert.equal(
		(
			await persistence.restore(
				scope('cache:tenant-a', 'schema:a', 'auth-2')
			)
		).scope.authorizationGeneration,
		'auth-2'
	);
	persistence.close();
});

test('sensitive and memory-only models cannot be made durable accidentally', async () => {
	const factory = new FakeIndexedDbFactory();
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE,
		policy: {
			models: {
				TodoView: {
					retention: 'memory-only',
					sensitive: false
				},
				SecretView: {
					retention: 'persist-confirmed',
					sensitive: true
				}
			}
		}
	});

	assert.equal(await persistence.save(state()), false);
	assert.equal(factory.entries(DATABASE).size, 0);
	persistence.close();
});

class FakeIndexedDbFactory {
	#databases = new Map();

	open(name, version) {
		const request = fakeRequest();
		queueMicrotask(() => {
			let database = this.#databases.get(name);
			const upgrade = database === undefined;
			if (upgrade) {
				database = {
					version,
					stores: new Map()
				};
				this.#databases.set(name, database);
			}
			request.result = new FakeDatabase(database);
			if (upgrade) request.onupgradeneeded?.({ target: request });
			queueMicrotask(() => request.onsuccess?.({ target: request }));
		});
		return request;
	}

	entries(name) {
		const database = this.#databases.get(name);
		if (database === undefined) return new Map();
		return database.stores.get('confirmed-replicas') ?? new Map();
	}
}

class FakeDatabase {
	constructor(state) {
		this.state = state;
		this.objectStoreNames = {
			contains: (name) => this.state.stores.has(name)
		};
	}

	createObjectStore(name) {
		const entries = new Map();
		this.state.stores.set(name, entries);
		return entries;
	}

	transaction(name) {
		const entries = this.state.stores.get(name);
		if (entries === undefined) throw new Error(`missing object store ${name}`);
		return new FakeTransaction(entries);
	}

	close() {}
}

class FakeTransaction {
	constructor(entries) {
		this.entries = entries;
		this.error = null;
		this.completed = false;
	}

	objectStore() {
		return {
			get: (key) =>
				this.#request(() => {
					const value = this.entries.get(key);
					return value === undefined ? undefined : structuredClone(value);
				}),
			put: (value) =>
				this.#request(() => {
					const cloned = structuredClone(value);
					this.entries.set(cloned.identity, cloned);
					return cloned.identity;
				}),
			delete: (key) =>
				this.#request(() => {
					this.entries.delete(key);
					return undefined;
				})
		};
	}

	abort() {
		if (this.completed) throw new Error('transaction already completed');
		this.completed = true;
		this.onabort?.({ target: this });
	}

	#request(work) {
		const request = fakeRequest();
		queueMicrotask(() => {
			if (this.completed) return;
			try {
				request.result = work();
				request.onsuccess?.({ target: request });
				queueMicrotask(() => {
					if (this.completed) return;
					this.completed = true;
					this.oncomplete?.({ target: this });
				});
			} catch (error) {
				request.error = error;
				this.error = error;
				request.onerror?.({ target: request });
				this.onerror?.({ target: this });
			}
		});
		return request;
	}
}

function fakeRequest() {
	return {
		result: undefined,
		error: null,
		onsuccess: null,
		onerror: null,
		onupgradeneeded: null,
		onblocked: null
	};
}
