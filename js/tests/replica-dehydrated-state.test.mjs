/**
 * One parser reads the dehydrated-state format: hydration accepts everything
 * `dehydrate()` writes, including after a JSON round trip, and refuses each
 * malformed input below.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { createDistributedReplica } from '../dist/replica/index.js';
import { flush } from './fixtures/game-harness.mjs';
import { scripted, Todos, wireFrame } from './fixtures/replica-todos.mjs';

const SCOPE = Object.freeze({
	protocolVersion: 1,
	schemaHash: 'schema-a',
	authorizationGeneration: 'auth-1',
	cacheScope: 'cache:a'
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

test('a state whose live stream was lost hydrates, also after a JSON round trip', async () => {
	const { replica, subscriptions, watch } = await liveReplica();
	subscriptions[0].observer.error(Object.assign(new Error('lost'), { retryable: true }));
	const state = replica.dehydrate();
	assert.equal(
		state.payload.operations[0].live.retiredAtRevision,
		state.payload.nextIndexRevision,
		'the retired live operation is part of what dehydrate() writes'
	);

	assert.equal(createDistributedReplica().hydrate(state, SCOPE), true);

	const second = createDistributedReplica();
	assert.equal(second.hydrate(JSON.parse(JSON.stringify(state)), SCOPE), true);
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

test('hydration refuses malformed states', async (t) => {
	const { replica, watch } = await liveReplica();
	const valid = JSON.parse(JSON.stringify(replica.dehydrate()));
	watch.destroy();
	assert.equal(createDistributedReplica().hydrate(structuredClone(valid), SCOPE), true);
	for (const [name, corrupt] of Object.entries(REJECTED)) {
		await t.test(name, () => {
			const state = structuredClone(valid);
			corrupt(state);
			assert.equal(createDistributedReplica().hydrate(state, SCOPE), false);
		});
	}
});
