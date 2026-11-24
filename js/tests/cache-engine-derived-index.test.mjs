import assert from 'node:assert/strict';
import test from 'node:test';
import {
	cacheIndexKey,
	createCacheEngine
} from '../dist/internal/cache-engine.js';

const BASE = 'Todo:base';
const A = 'Todo:a';
const B = 'Todo:b';
const SERVER = 'Todo:server';
const TODOS = cacheIndexKey({ field: 'todos', arguments: {} });
const METADATA = Object.freeze({
	field: 'todos',
	arguments: Object.freeze({}),
	coverage: Object.freeze({ kind: 'complete' }),
	dependencies: Object.freeze(['todos'])
});

function seed(engine) {
	engine.batch((writer) => {
		writer.writeRecord({
			key: BASE,
			revision: 1,
			fields: { id: 'base', title: 'base' }
		});
		writer.writeIndex({
			key: TODOS,
			revision: 1,
			records: [BASE],
			complete: true,
			metadata: METADATA
		});
	});
}

function membershipReconciler(observations = []) {
	return (confirmed, layers) => {
		const index = confirmed.index(TODOS);
		observations.push({
			confirmedRecords: [BASE, A, B, SERVER].filter(
				(key) => confirmed.record(key) !== undefined
			),
			layers
		});
		if (index === undefined) return [];
		return [
			{
				kind: 'write',
				write: {
					key: TODOS,
					records: [
						...index.records,
						...layers.flatMap((layer) => layer.context?.records ?? [])
					],
					complete: true,
					metadata: index.metadata
				}
			}
		];
	};
}

test('derived indexes rebase from confirmed state plus surviving semantic layers', () => {
	const engine = createCacheEngine();
	seed(engine);
	const observations = [];
	engine.setDerivedIndexReconciler(membershipReconciler(observations));

	const contextA = { records: [A] };
	const contextB = { records: [B] };
	engine.createOptimisticLayer(
		'A',
		(writer) =>
			writer.writeRecord({
				key: A,
				fields: { id: 'a', title: 'from A' }
			}),
		contextA
	);
	engine.createOptimisticLayer(
		'B',
		(writer) =>
			writer.writeRecord({
				key: B,
				fields: { id: 'b', title: 'from B' }
			}),
		contextB
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A, B]
	);

	// Context is detached at layer creation; caller mutation cannot alter a
	// future full-overlay reconciliation.
	contextB.records.push(A);
	const delivered = [];
	engine.watch(
		(reader) => reader.index(TODOS)?.records,
		(value) => delivered.push(value)
	);
	assert.equal(engine.rejectOptimisticLayer('A'), true);

	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, B]
	);
	assert.deepEqual(delivered, [[BASE, B]]);
	assert.equal(engine.read((reader) => reader.record(A)), undefined);
	assert.equal(engine.read((reader) => reader.record(B))?.fields.title, 'from B');
	const last = observations.at(-1);
	assert.deepEqual(last.confirmedRecords, [BASE]);
	assert.deepEqual(last.layers.map(({ id }) => id), ['B']);
	assert.equal(Object.isFrozen(last.layers), true);
	assert.equal(Object.isFrozen(last.layers[0]), true);
	assert.equal(Object.isFrozen(last.layers[0].context), true);
	assert.equal(Object.isFrozen(last.layers[0].context.records), true);

	delivered.length = 0;
	engine.confirmOptimisticLayer('B', (writer) => {
		writer.writeRecord({
			key: B,
			revision: 2,
			fields: { id: 'b', title: 'confirmed B' }
		});
		writer.writeIndex({
			key: TODOS,
			revision: 2,
			records: [BASE, B],
			complete: true,
			metadata: METADATA
		});
	});
	assert.equal(engine.optimisticLayerState('B'), undefined);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, B]
	);
	assert.deepEqual(delivered, []);
	assert.deepEqual(observations.at(-1).layers, []);
});

test('replacement prefix derives indexes without target or suffix contexts', () => {
	const engine = createCacheEngine();
	seed(engine);
	const observations = [];
	engine.setDerivedIndexReconciler(membershipReconciler(observations));
	engine.createOptimisticLayer(
		'lower',
		(writer) =>
			writer.writeRecord({
				key: A,
				fields: { id: 'a', title: 'lower' }
			}),
		{ records: [A], marker: 'lower' }
	);
	engine.createOptimisticLayer(
		'target',
		(writer) =>
			writer.writeRecord({
				key: B,
				fields: { id: 'b', title: 'preview' }
			}),
		{ records: [B], marker: 'preserved-target' }
	);
	assert.equal(engine.markOptimisticLayerAccepted('target'), true);
	engine.createOptimisticLayer(
		'suffix',
		(writer) =>
			writer.writeRecord({
				key: SERVER,
				fields: { id: 'server', title: 'suffix' }
			}),
		{ records: [SERVER], marker: 'suffix' }
	);
	const layersBefore = observations.at(-1).layers;
	const observationCount = observations.length;
	const deliveredRecords = [];
	const deliveredIndexes = [];
	engine.watch(
		(reader) => reader.record(B)?.fields.title,
		(value) => deliveredRecords.push(value)
	);
	engine.watch(
		(reader) => reader.index(TODOS)?.records,
		(value) => deliveredIndexes.push(value)
	);

	let prefixIndex;
	let prefixTarget;
	assert.equal(
		engine.replaceOptimisticLayer('target', (reader, writer) => {
			prefixIndex = reader.index(TODOS)?.records;
			prefixTarget = reader.record(B);
			writer.writeRecord({
				key: B,
				fields: { id: 'b', title: 'actual' }
			});
		}),
		true
	);

	assert.deepEqual(prefixIndex, [BASE, A]);
	assert.equal(prefixTarget, undefined);
	assert.deepEqual(deliveredRecords, ['actual']);
	assert.deepEqual(deliveredIndexes, []);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A, B, SERVER]
	);
	assert.equal(observations.length, observationCount + 2);
	assert.deepEqual(
		observations.at(-2).layers.map(({ id }) => id),
		['lower']
	);
	assert.deepEqual(observations.at(-1).layers, layersBefore);

	const noOpObservationCount = observations.length;
	assert.equal(
		engine.replaceOptimisticLayer('target', (_reader, writer) =>
			writer.writeRecord({
				key: B,
				fields: { id: 'b', title: 'actual' }
			})
		),
		false
	);
	assert.equal(observations.length, noOpObservationCount + 1);
	assert.deepEqual(
		observations.at(-1).layers.map(({ id }) => id),
		['lower']
	);
	assert.deepEqual(deliveredRecords, ['actual']);
	assert.deepEqual(deliveredIndexes, []);
});

test('replacement publishes derived-index lifecycle changes from the final graph', () => {
	const engine = createCacheEngine();
	const relationship = cacheIndexKey({
		parent: B,
		field: 'related',
		arguments: {}
	});
	const relationshipMetadata = {
		parent: B,
		field: 'related',
		arguments: {},
		coverage: { kind: 'complete' },
		dependencies: ['related']
	};
	engine.batch((writer) => {
		writer.writeRecord({
			key: B,
			revision: 1,
			fields: { id: 'b', title: 'base parent' }
		});
		writer.writeRecord({
			key: A,
			revision: 1,
			fields: { id: 'a', title: 'base child' }
		});
	});
	engine.setDerivedIndexReconciler((_confirmed, layers) =>
		layers.some(({ id }) => id === 'target')
			? [
					{
						kind: 'write',
						write: {
							key: relationship,
							records: [A],
							complete: true,
							metadata: relationshipMetadata
						}
					}
				]
			: []
	);
	engine.createOptimisticLayer(
		'target',
		(writer) =>
			writer.writeRecord({
				key: B,
				fields: { title: 'preview parent' }
			}),
		{ stable: 'target-context' }
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(relationship)?.records),
		[A]
	);
	const delivered = [];
	engine.watch(
		(reader) => reader.index(relationship)?.records,
		(value) => delivered.push(value)
	);
	let prefixIndex;
	assert.equal(
		engine.replaceOptimisticLayer('target', (reader, writer) => {
			prefixIndex = reader.index(relationship);
			writer.tombstoneRecord(B);
		}),
		true
	);

	assert.equal(prefixIndex, undefined);
	assert.equal(engine.read((reader) => reader.record(B)), undefined);
	assert.equal(engine.read((reader) => reader.index(relationship)), undefined);
	assert.deepEqual(delivered, [undefined]);
});

test('authoritative base writes reconcile pending derived indexes before one watcher flush', () => {
	const engine = createCacheEngine();
	seed(engine);
	const observations = [];
	engine.setDerivedIndexReconciler(membershipReconciler(observations));
	engine.createOptimisticLayer(
		'B',
		(writer) =>
			writer.writeRecord({
				key: B,
				fields: { id: 'b', title: 'pending B' }
			}),
		{ records: [B] }
	);

	const delivered = [];
	engine.watch(
		(reader) => reader.index(TODOS)?.records,
		(value) => delivered.push(value)
	);
	engine.batch((writer) => {
		writer.writeRecord({
			key: SERVER,
			revision: 2,
			fields: { id: 'server', title: 'from server' }
		});
		writer.writeIndex({
			key: TODOS,
			revision: 2,
			records: [BASE, SERVER],
			complete: true,
			metadata: METADATA
		});
	});

	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, SERVER, B]
	);
	assert.deepEqual(delivered, [[BASE, SERVER, B]]);
	const last = observations.at(-1);
	assert.deepEqual(
		last.confirmedRecords.sort(),
		[BASE, SERVER].sort()
	);
	assert.deepEqual(last.layers.map(({ id }) => id), ['B']);
});

test('derived stale/delete decisions are overlays and accepted-state changes reconcile atomically', () => {
	const engine = createCacheEngine();
	seed(engine);
	engine.setDerivedIndexReconciler((_confirmed, layers) => {
		const layer = layers.at(-1);
		if (layer?.context?.mode === 'delete') {
			return [{ kind: 'delete', key: TODOS }];
		}
		if (layer?.state === 'accepted') {
			return [{ kind: 'stale', key: TODOS, reason: 'awaiting-projection' }];
		}
		return [];
	});
	engine.createOptimisticLayer('lifecycle', () => undefined, {
		mode: 'stale-after-accept'
	});
	assert.equal(engine.read((reader) => reader.index(TODOS)?.complete), true);

	const delivered = [];
	engine.watch(
		(reader) => reader.index(TODOS),
		(value) => delivered.push(value)
	);
	assert.equal(engine.markOptimisticLayerAccepted('lifecycle'), true);
	const stale = engine.read((reader) => reader.index(TODOS));
	assert.equal(
		stale.complete,
		true,
		'freshness uncertainty must retain structurally complete visible data'
	);
	assert.equal(stale.metadata.staleReason, 'awaiting-projection');
	assert.equal(delivered.length, 1);

	assert.equal(engine.rejectOptimisticLayer('lifecycle'), true);
	const restored = engine.read((reader) => reader.index(TODOS));
	assert.equal(restored.complete, true);
	assert.equal(restored.metadata.staleReason, undefined);

	engine.createOptimisticLayer('delete', () => undefined, { mode: 'delete' });
	assert.equal(engine.read((reader) => reader.index(TODOS)), undefined);
	engine.rejectOptimisticLayer('delete');
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE]
	);
});

test('reconciler failures and invalid mutations roll back base and layer lifecycle state', async () => {
	const engine = createCacheEngine();
	seed(engine);
	const stable = membershipReconciler();
	let rejectBlocked = false;
	let replacementBlocked = false;
	engine.setDerivedIndexReconciler((confirmed, layers) => {
		const title = confirmed.record(BASE)?.fields.title;
		if (title === 'bad') throw new Error('cannot derive bad base');
		if (rejectBlocked && layers.length === 0) {
			throw new Error('cannot derive rejected layer');
		}
		if (replacementBlocked && layers.some(({ id }) => id === 'A')) {
			throw new Error('cannot derive replacement');
		}
		return stable(confirmed, layers);
	});
	engine.createOptimisticLayer(
		'A',
		(writer) => writer.writeRecord({ key: A, fields: { id: 'a' } }),
		{ records: [A] }
	);
	const delivered = [];
	engine.watch(
		(reader) => reader.index(TODOS)?.records,
		(value) => delivered.push(value)
	);

	assert.throws(
		() =>
			engine.batch((writer) =>
				writer.writeRecord({
					key: BASE,
					revision: 2,
					fields: { title: 'bad' }
				})
			),
		/cannot derive bad base/
	);
	assert.equal(
		engine.read((reader) => reader.record(BASE)?.fields.title),
		'base'
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A]
	);
	assert.deepEqual(delivered, []);

	rejectBlocked = true;
	assert.throws(
		() => engine.rejectOptimisticLayer('A'),
		/cannot derive rejected layer/
	);
	assert.equal(engine.optimisticLayerState('A'), 'optimistic');
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A]
	);
	assert.deepEqual(delivered, []);
	rejectBlocked = false;

	assert.throws(
		() =>
			engine.replaceOptimisticLayer('A', (_reader, writer) => {
				writer.writeRecord({
					key: A,
					fields: { id: 'a', title: 'rolled-back replacement' }
				});
				replacementBlocked = true;
			}),
		/cannot derive replacement/
	);
	replacementBlocked = false;
	assert.equal(engine.optimisticLayerState('A'), 'optimistic');
	assert.equal(engine.read((reader) => reader.record(A))?.fields.title, undefined);
	assert.deepEqual(delivered, []);

	assert.throws(
		() =>
			engine.setDerivedIndexReconciler(() => [
				{
					kind: 'write',
					write: {
						key: TODOS,
						records: [BASE],
						complete: true,
						metadata: METADATA
					}
				},
				{ kind: 'stale', key: TODOS, reason: 'duplicate' }
			]),
		/duplicate derived index mutation/
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A]
	);

	assert.throws(
		() =>
			engine.createOptimisticLayer('invalid-context', () => undefined, {
				invalid: () => undefined
			}),
		/JSON-compatible/
	);
	assert.equal(engine.optimisticLayerState('invalid-context'), undefined);

	assert.throws(
		() =>
			engine.setDerivedIndexReconciler(async () => {
				await Promise.resolve();
				return [];
			}),
		/derived index reconciler must be synchronous/
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A]
	);
	await Promise.resolve();
});

test('restore clears optimistic contexts and the derived overlay without serializing either', () => {
	const engine = createCacheEngine();
	seed(engine);
	const confirmed = engine.extract();
	engine.setDerivedIndexReconciler(membershipReconciler());
	engine.createOptimisticLayer(
		'A',
		(writer) => writer.writeRecord({ key: A, fields: { secret: 'optimistic' } }),
		{ records: [A], secret: 'semantic-context' }
	);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE, A]
	);
	assert.doesNotMatch(JSON.stringify(engine.extract()), /optimistic|semantic-context/);

	engine.restore(confirmed);
	assert.equal(engine.optimisticLayerState('A'), undefined);
	assert.equal(engine.read((reader) => reader.record(A)), undefined);
	assert.deepEqual(
		engine.read((reader) => reader.index(TODOS)?.records),
		[BASE]
	);
});
