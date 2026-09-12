import assert from 'node:assert/strict';
import test from 'node:test';
import { createDistributedReplica } from '../dist/replica/index.js';
import { ControlledReplicaTransport } from './fixtures/adapter-conformance.mjs';

import { artifact, owner, frame } from './fixtures/unique-key-artifact.mjs';

test('Rust-generated candidate-key relationship survives hydration and live reference changes', async () => {
	assert.deepEqual(owner.relationship.keyMapping, {
		kind: 'direct', local: ['tenantId', 'ownerTitle'], remote: ['tenantId', 'title']
	});
	assert.deepEqual(owner.selection.storage.identityFields, ['tenantId', 'id']);
	const transport = new ControlledReplicaTransport();
	const server = createDistributedReplica({ transport });
	const watch = server.watch(artifact, {}, { live: false });
	const pending = watch.refresh();
	await Promise.resolve();
	transport.fetches[0].response.resolve(frame('1', 'target-one', 'first'));
	await pending;
	assert.deepEqual(server.read(artifact, {}).data.todos[0], {
		id: 'source-id', title: 'source', owner: { id: 'target-one', title: 'first' }
	});
	watch.destroy();
	const seed = server.dehydrate();
	const browserTransport = new ControlledReplicaTransport();
	const browser = createDistributedReplica({ transport: browserTransport });
	assert.equal(browser.hydrate(seed, seed.scope), true);
	assert.deepEqual(browser.read(artifact, {}).data, server.read(artifact, {}).data);
	const live = browser.watch(artifact, {}, { live: true });
	const unsubscribe = live.subscribe(() => {});
	assert.equal(browserTransport.fetches.length, 0);
	assert.equal(browserTransport.lives.length, 1);
	browserTransport.lives[0].observer.next(frame('2', 'target-two', 'second'));
	assert.deepEqual(live.get().data.todos[0], {
		id: 'source-id', title: 'source', owner: { id: 'target-two', title: 'second' }
	});
	browserTransport.lives[0].observer.next(frame('3', null, null));
	assert.equal(live.get().data.todos[0].owner, null);
	assert.equal(live.get().data.todos[0].id, 'source-id');
	unsubscribe();
	live.destroy();
	assert.equal(browserTransport.lives[0].closed, true);
});
