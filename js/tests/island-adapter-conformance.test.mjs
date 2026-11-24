import assert from 'node:assert/strict';
import test from 'node:test';

import { createDistributedReplica } from '../dist/replica/index.js';
import {
	ControlledReplicaTransport,
	TodosArtifact,
	todoFrame
} from './fixtures/adapter-conformance.mjs';

const VanillaIsland = Object.freeze({
	plan: Object.freeze({
		version: 1,
		id: 'island:vanilla-todos',
		operation: 'VanillaTodos',
		operationHash: `sha256:${'b'.repeat(64)}`,
		modulePath: 'operations/vanilla-todos.ts',
		exportName: 'Operation_VanillaTodos',
		source: Object.freeze({
			path: 'src/widgets/vanilla-todos.graphql',
			line: 1,
			column: 1
		}),
		directives: Object.freeze({ load: true, live: true }),
		variableSchema: Object.freeze({
			reference: `sha256:${'b'.repeat(64)}#variable-codec-v2`,
			codecVersion: 2,
			variables: Object.freeze([])
		}),
		liveCoverage: Object.freeze({
			requested: true,
			finite: true,
			kind: 'complete'
		})
	}),
	artifact: TodosArtifact
});

test('framework-neutral island performs SSR, hydration, reactive selection, and live release', async () => {
	const serverTransport = new ControlledReplicaTransport();
	const server = createDistributedReplica({ transport: serverTransport });
	const serverWatch = server.watch(VanillaIsland.artifact, {}, { live: false });
	const refreshed = serverWatch.refresh();
	await Promise.resolve();
	assert.equal(serverTransport.fetches.length, 1);
	serverTransport.fetches[0].response.resolve(
		todoFrame(
			VanillaIsland.artifact,
			[{ id: 'todo-1', title: 'server island', status: 'open' }],
			{ cacheScope: 'cache:vanilla', position: '1' }
		)
	);
	await refreshed;
	assert.equal(serverWatch.get().data.todos[0].title, 'server island');
	server.retainOperation(VanillaIsland.artifact, {});
	serverWatch.destroy();
	const seed = server.dehydrate();

	const browserTransport = new ControlledReplicaTransport();
	const browser = createDistributedReplica({ transport: browserTransport });
	assert.equal(browser.hydrate(seed, seed.scope), true);
	assert.equal(
		browser.read(VanillaIsland.artifact, {}).data.todos[0].title,
		'server island'
	);

	const live = browser.watch(VanillaIsland.artifact, {}, { live: true });
	const snapshots = [];
	const unsubscribe = live.subscribe((snapshot) => {
		snapshots.push(snapshot);
	});
	assert.equal(browserTransport.fetches.length, 0, 'complete hydration skips mount HTTP');
	assert.equal(browserTransport.lives.length, 1);
	browserTransport.lives[0].observer.next(
		todoFrame(
			VanillaIsland.artifact,
			[{ id: 'todo-1', title: 'live island', status: 'open' }],
			{ cacheScope: 'cache:vanilla', position: '2', source: 'live' }
		)
	);
	assert.equal(snapshots.at(-1).data.todos[0].title, 'live island');
	unsubscribe();
	live.destroy();
	assert.equal(browserTransport.lives[0].closed, true);
	assert.deepEqual(Object.keys(VanillaIsland.plan).sort(), [
		'directives',
		'exportName',
		'id',
		'liveCoverage',
		'modulePath',
		'operation',
		'operationHash',
		'source',
		'variableSchema',
		'version'
	]);
	assert.doesNotMatch(JSON.stringify(VanillaIsland.plan), /svelte|react/i);
});
