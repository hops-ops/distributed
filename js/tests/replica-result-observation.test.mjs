import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createDistributedReplica
} from '../dist/replica/index.js';
import {
	replicaResultObservation
} from '../dist/replica/command-runtime.js';

const Item = Object.freeze({ id: 'Item', identityFields: ['id'] });
const SCHEMA_HASH = `sha256:${'a'.repeat(64)}`;
const Query = Object.freeze({
	id: 'query:items',
	document: 'query Items { items { id value } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: SCHEMA_HASH,
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'query:items',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: Object.freeze({
		version: 2,
		limits: Object.freeze({
			maxDepth: 8,
			maxBoolWidth: 32,
			maxInList: 64
		}),
		variables: Object.freeze({}),
		defaults: Object.freeze({}),
		inputs: Object.freeze({})
	}),
	live: Object.freeze({
		id: 'live:items',
		document: 'subscription ItemsLive { items { id value } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'items',
			field: 'items',
			cardinality: 'many',
			nullable: false,
			dependencies: Object.freeze(['items']),
			selection: Object.freeze({
				typename: Item.id,
				storage: Object.freeze({
					kind: 'normalized',
					model: Item.id,
					identityFields: Item.identityFields
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
						responseKey: 'value',
						field: 'value',
						codec: 'String',
						nullable: false
					})
				])
			})
		})
	])
});

function frame(revision, value, operation = Query.id) {
	return {
		data: { items: [{ id: 'item-1', value }] },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: SCHEMA_HASH,
				authorizationGeneration: 'auth-1',
				cacheScope: 'scope:user',
				operation,
				trustedPresets: [],
				snapshot: {
					scopeToken: 'snapshot:items',
					recordsComplete: true,
					indexesComparable: true,
					records: [
						{
							path: ['items', '0'],
							model: Item.id,
							scopeToken: 'record:item-1',
							incarnation: '1',
							revision: String(revision),
							tombstone: false
						}
					],
					indexes: [
						{
							projection: 'items',
							scopeToken: 'index:items',
							position: String(revision),
							...(operation === Query.live.id
								? {
										resume: {
											projection: 'items',
											position: String(revision),
											token: `cursor:${revision}`
										}
									}
								: {})
						}
					],
					observations: []
				},
				...(operation === Query.live.id
					? {
							live: {
								mode: 'resumable',
								reset: false,
								cursors: [
									{
										projection: 'items',
										position: String(revision),
										token: `cursor:${revision}`
									}
								]
							}
						}
					: {})
			}
		}
	};
}

function deferred() {
	let resolve;
	const promise = new Promise((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function waitFor(predicate) {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (predicate()) return;
		await new Promise((resolve) => setImmediate(resolve));
	}
	assert.fail('condition was not reached');
}

test('post-commit observations cover explicit, network, and live writes and ignore stale frames', async () => {
	let liveObserver;
	const replica = createDistributedReplica({
		transport: {
			fetch: () => Promise.resolve(frame(2, 'network')),
			subscribe: (_request, observer) => {
				liveObserver = observer;
				return () => undefined;
			}
		}
	});
	const observed = [];
	const registration = replica[replicaResultObservation]((envelope) => {
		observed.push({
			revision:
				envelope.extensions.distributed.snapshot.records[0].revision,
			value: replica.read(Query, {}).data.items?.[0]?.value
		});
	});

	replica.writeResult(Query, {}, frame(1, 'explicit'), 'network');
	assert.deepEqual(observed, [{ revision: '1', value: 'explicit' }]);

	const watch = replica.watch(Query, {}, { live: true });
	await watch.refresh();
	await waitFor(() => watch.get().data.items?.[0]?.value === 'network');
	assert.deepEqual(observed.at(-1), { revision: '2', value: 'network' });

	liveObserver.next(frame(3, 'live', Query.live.id));
	assert.equal(watch.get().data.items[0].value, 'live');
	assert.deepEqual(observed.at(-1), { revision: '3', value: 'live' });

	replica.writeResult(Query, {}, frame(2, 'stale'), 'network');
	assert.equal(observed.length, 3);
	assert.equal(watch.get().data.items[0].value, 'live');

	registration.dispose();
	replica.writeResult(Query, {}, frame(4, 'after-dispose'), 'network');
	assert.equal(observed.length, 3);
	watch.destroy();
});

test('observer failures are isolated after commit and invalid or scope-fenced work is never observed', async () => {
	const response = deferred();
	const reported = [];
	let fetchCalls = 0;
	const replica = createDistributedReplica({
		transport: {
			fetch: () => {
				fetchCalls += 1;
				return fetchCalls === 1
					? response.promise
					: new Promise(() => undefined);
			}
		},
		onObserverError: (error) => reported.push(error)
	});
	let safeCalls = 0;
	replica[replicaResultObservation](() => {
		throw new Error('observer failed');
	});
	replica[replicaResultObservation](() => {
		safeCalls += 1;
	});

	replica.writeResult(Query, {}, frame(1, 'committed'), 'network');
	assert.equal(safeCalls, 1);
	assert.equal(replica.read(Query, {}).data.items[0].value, 'committed');
	assert.equal(reported.length, 1);
	assert.match(reported[0].message, /replica observer delivery failed/);

	assert.throws(
		() => replica.writeResult(Query, {}, { data: { items: [] } }, 'network'),
		(error) => error?.code === 'DISTRIBUTED_PROTOCOL_INVALID'
	);
	assert.equal(safeCalls, 1);

	const watch = replica.watch(Query, {});
	const flight = watch.refresh();
	await new Promise((resolve) => setImmediate(resolve));
	replica.invalidateAuthorization();
	response.resolve(frame(2, 'late'));
	await flight;
	assert.equal(safeCalls, 1, 'late work from the closed generation is fenced');
	watch.destroy();
});

test('receipt-only protocol frames notify only after the authoritative scope commits', () => {
	const schemaHash = `sha256:${'a'.repeat(64)}`;
	const operationHash = `sha256:${'b'.repeat(64)}`;
	const artifact = Object.freeze({
		id: operationHash,
		document: 'query Scope { __typename }',
		protocol: Object.freeze({
			version: 1,
			schemaHash,
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: operationHash,
			trustedPresets: Object.freeze([])
		}),
		variableCodec: Object.freeze({
			version: 2,
			limits: Object.freeze({
				maxDepth: 8,
				maxBoolWidth: 32,
				maxInList: 64
			}),
			variables: Object.freeze({}),
			defaults: Object.freeze({}),
			inputs: Object.freeze({})
		}),
		roots: Object.freeze([])
	});
	const replica = createDistributedReplica();
	const scopes = [];
	replica[replicaResultObservation](() => {
		scopes.push(replica.scope?.cacheScope);
	});

	replica.writeResult(
		artifact,
		{},
		{
			extensions: {
				distributed: {
					protocolVersion: 1,
					schemaHash,
					authorizationGeneration: 'auth-1',
					cacheScope: 'scope:user',
					operation: operationHash,
					trustedPresets: []
				}
			}
		},
		'network'
	);
	assert.deepEqual(scopes, ['scope:user']);

	assert.throws(
		() =>
			replica.writeResult(
				artifact,
				{},
				{
					extensions: {
						distributed: {
							protocolVersion: 1,
							schemaHash: `sha256:${'c'.repeat(64)}`,
							authorizationGeneration: 'auth-1',
							cacheScope: 'scope:forged',
							operation: operationHash,
							trustedPresets: []
						}
					}
				},
				'network'
			),
		(error) => error?.code === 'DISTRIBUTED_PROTOCOL_INVALID'
	);
	assert.deepEqual(scopes, ['scope:user']);
});
