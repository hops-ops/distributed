/**
 * One parser reads the dehydrated-state format. SSR hydration and IndexedDB
 * persistence used to parse it separately and disagreed on the inputs below;
 * both now give the same answer for each, and both accept everything
 * `dehydrate()` writes.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createDistributedReplica } from '../dist/replica/index.js';
import { createReplicaIndexedDbPersistence } from '../dist/replica/persistence.js';
import { FakeIndexedDb } from './fixtures/fake-indexeddb.mjs';
import { flush } from './fixtures/game-harness.mjs';
import { scripted, Todos, wireFrame } from './fixtures/replica-todos.mjs';

const SCOPE = Object.freeze({
	protocolVersion: 1,
	schemaHash: 'schema-a',
	authorizationGeneration: 'auth-1',
	cacheScope: 'cache:a'
});
const POLICY = Object.freeze({
	models: { TodoView: { retention: 'persist-confirmed', sensitive: false } }
});

/** A replica holding one query result and one admitted live frame. */
async function liveReplica() {
	const harness = scripted();
	const watch = harness.replica.watch(Todos, {}, { live: true });
	await flush();
	harness.fetches[0].resolve(wireFrame({ position: '1' }));
	await flush();
	harness.subscriptions[0].observer.next(
		wireFrame({
			operation: 'live:todos',
			position: '2',
			live: { mode: 'resumable', reset: true }
		})
	);
	return { ...harness, watch };
}

/** How each consumer of the format answers `state`. */
async function consumers(state) {
	const hydrated = createDistributedReplica().hydrate(state, SCOPE);
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: new FakeIndexedDb(),
		databaseName: 'dehydrated-state',
		policy: POLICY
	});
	let saved;
	try {
		saved = await persistence.save(state);
	} catch (error) {
		assert.ok(error instanceof TypeError, String(error));
		saved = false;
	}
	const restored = saved ? await persistence.restore(SCOPE) : undefined;
	persistence.close();
	return { hydrated, saved, restored };
}

test('a state whose live stream was lost is hydrated, saved and restored', async () => {
	const { replica, subscriptions, watch } = await liveReplica();
	subscriptions[0].observer.error(Object.assign(new Error('lost'), { retryable: true }));
	const state = replica.dehydrate();
	assert.equal(
		state.payload.operations[0].live.retiredAtRevision,
		state.payload.nextIndexRevision,
		'the retired live operation is part of what dehydrate() writes'
	);

	const { hydrated, saved, restored } = await consumers(state);
	assert.equal(hydrated, true);
	assert.equal(saved, true, 'persistence used to refuse retiredAtRevision as an unknown field');
	assert.deepEqual(restored, JSON.parse(JSON.stringify(state)));

	const second = createDistributedReplica();
	assert.equal(second.hydrate(restored, SCOPE), true);
	assert.deepEqual(second.read(Todos, {}).data, watch.get().data);
	watch.destroy();
});

const REJECTED = {
	'a numeric revision': (state) => {
		for (const record of state.payload.cache.records) {
			for (const field of Object.values(record.fields)) {
				field.revision = Number(field.revision);
			}
		}
	},
	'an unknown cache record member': (state) => {
		state.payload.cache.records[0].extra = 1;
	},
	'an unknown index metadata member': (state) => {
		state.payload.cache.indexes[0].metadata.extra = 1;
	},
	'a non-boolean index flag': (state) => {
		state.payload.cache.indexes[0].complete = 1;
	},
	'an object that is not plain': (state) => {
		state.payload = Object.assign(Object.create({ inherited: true }), state.payload);
	},
	'a live clock on a tombstoned record outside any model': (state) => {
		state.payload.cache.records.push({
			key: 'object:detached',
			revision: '1',
			incarnation: '1',
			tombstoneRevision: '1',
			fields: {},
			links: {}
		});
		state.payload.recordClocks.push([
			'object:detached',
			{ scopeToken: 'record:detached', incarnation: '1', revision: '1', tombstone: false }
		]);
	},
	'a retirement past the index checkpoint': (state) => {
		state.payload.operations[0].live.retiredAtRevision = '999';
	},
	'a modeled record without its incarnation': (state) => {
		const record = state.payload.cache.records[0];
		for (const [key, clock] of state.payload.recordClocks) {
			if (key === record.key) clock.incarnation = record.revision;
		}
		delete record.incarnation;
	}
};

test('hydration and persistence refuse the same malformed states', async (t) => {
	const { replica, watch } = await liveReplica();
	const valid = JSON.parse(JSON.stringify(replica.dehydrate()));
	watch.destroy();
	assert.deepEqual(await consumers(valid).then(({ hydrated, saved }) => [hydrated, saved]), [
		true,
		true
	]);
	for (const [name, corrupt] of Object.entries(REJECTED)) {
		await t.test(name, async () => {
			const state = structuredClone(valid);
			corrupt(state);
			const { hydrated, saved } = await consumers(state);
			assert.equal(hydrated, false, 'hydration');
			assert.equal(saved, false, 'persistence');
		});
	}
});
