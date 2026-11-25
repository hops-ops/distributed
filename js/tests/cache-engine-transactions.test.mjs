import assert from 'node:assert/strict';
import test from 'node:test';
import { cacheIndexKey, createCacheEngine } from '../dist/internal/cache-engine.js';

const TODO = 'Todo:todo-1';
const OTHER = 'Todo:todo-2';
const DONE = 'Todo:todo-3';
const USER = 'User:user-1';
const OPEN = cacheIndexKey({ field: 'todos', arguments: { status: 'open' } });
const CLOSED = cacheIndexKey({ field: 'todos', arguments: { status: 'closed' } });
const USER_TODOS = cacheIndexKey({ parent: USER, field: 'todos', arguments: {} });

/** A cache with records, links, a root index, a parent-scoped index and a layer. */
function seeded() {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: USER, revision: 1, fields: { name: 'Ada' } });
		writer.writeRecord({
			key: TODO,
			revision: 1,
			fields: { title: 'one' },
			links: { owner: USER }
		});
		writer.writeRecord({ key: OTHER, revision: 1, fields: { title: 'two' } });
		writer.writeRecord({ key: DONE, revision: 1, fields: { title: 'three' } });
		writer.writeIndex({ key: OPEN, revision: 1, records: [TODO], complete: true });
		writer.writeIndex({ key: CLOSED, revision: 1, records: [DONE], complete: true });
		writer.writeIndex({
			key: USER_TODOS,
			revision: 1,
			records: [TODO, OTHER],
			complete: true,
			metadata: {
				parent: USER,
				field: 'todos',
				arguments: {},
				coverage: { kind: 'complete' },
				dependencies: ['todos']
			}
		});
	});
	engine.createOptimisticLayer('pending', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'optimistic' } });
	});
	return engine;
}

/** Every value a reader can see, keyed by what it is. */
function everything(reader) {
	return Object.fromEntries([
		...[TODO, OTHER, DONE, USER].map((key) => [key, reader.record(key)]),
		...[...reader.indexKeys()].map((key) => [key, reader.index(key)])
	]);
}

function abort() {
	throw new Error('abort');
}

test('a throw mid-transaction leaves exactly the old state, nested transactions included', () => {
	const engine = seeded();
	const snapshot = engine.extract();
	const confirmed = engine.readConfirmed(everything);
	const visible = engine.read(everything);

	assert.throws(
		() =>
			engine.confirmOptimisticLayers(['pending'], (writer) => {
				writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'confirmed' } });
				writer.writeRecord({ key: 'Todo:new', revision: 2, fields: { title: 'new' } });
				writer.tombstoneRecord(OTHER, 2);
				writer.discardRecord(USER);
				writer.writeIndex({ key: OPEN, revision: 2, records: ['Todo:new'], complete: true });
				writer.deleteIndex(CLOSED, 2);
				writer.markIndexStale(cacheIndexKey({ field: 'fresh', arguments: {} }), 'x', 2);
				// Nested transactions run inside the outer one and roll back with it.
				engine.createOptimisticLayer('nested', (layer) => {
					layer.writeRecord({ key: DONE, fields: { title: 'nested layer' } });
				});
				engine.mergeConfirmed({
					version: 1,
					records: [
						{
							key: 'Todo:merged',
							revision: '3',
							incarnation: '3',
							fields: { title: { revision: '3', value: 'merged' } },
							links: {}
						}
					],
					indexes: []
				});
				// A nested replacement of the whole confirmed root, then more writes.
				engine.restoreConfirmed({ version: 1, records: [], indexes: [] });
				writer.writeRecord({ key: TODO, revision: 4, fields: { title: 'after restore' } });
				abort();
			}),
		/abort/
	);

	assert.deepEqual(engine.extract(), snapshot);
	assert.deepEqual(engine.readConfirmed(everything), confirmed);
	assert.deepEqual(engine.read(everything), visible);
	assert.equal(engine.optimisticLayerState('pending'), 'optimistic');
	assert.equal(engine.optimisticLayerState('nested'), undefined);
	// Not equal copies: the very values the old root held.
	for (const [key, value] of Object.entries(engine.readConfirmed(everything))) {
		assert.equal(value, confirmed[key], `${key} kept its identity`);
	}
});

test('a rolled-back index write leaves the old membership in charge of lifecycle invalidation', () => {
	const engine = seeded();
	assert.throws(
		() =>
			engine.batch((writer) => {
				writer.writeIndex({ key: OPEN, revision: 2, records: [OTHER], complete: true });
				abort();
			}),
		/abort/
	);

	// OTHER was a member only inside the failed transaction.
	engine.batch((writer) => writer.tombstoneRecord(OTHER, 2));
	const open = engine.readConfirmed((reader) => reader.index(OPEN));
	assert.deepEqual(open.records, [TODO]);
	assert.equal(open.complete, true);
	assert.equal(open.staleRevision, undefined);

	// TODO is a member of the committed index, so its tombstone invalidates it.
	engine.batch((writer) => writer.tombstoneRecord(TODO, 2));
	const invalidated = engine.readConfirmed((reader) => reader.index(OPEN));
	assert.deepEqual(invalidated.records, []);
	assert.equal(invalidated.complete, false);
});

test('a rolled-back restore keeps the old root and its index references', () => {
	const engine = seeded();
	const before = engine.readConfirmed(everything);
	assert.throws(
		() =>
			engine.batch(() => {
				engine.restoreConfirmed({ version: 1, records: [], indexes: [] });
				abort();
			}),
		/abort/
	);
	for (const [key, value] of Object.entries(engine.readConfirmed(everything))) {
		assert.equal(value, before[key], `${key} kept its identity`);
	}
	// Discarding the parent still deletes the index it owns.
	engine.batch((writer) => writer.discardRecord(USER));
	assert.equal(engine.readConfirmed((reader) => reader.index(USER_TODOS)), undefined);
});

test('a committed transaction shares every key it did not write', () => {
	const engine = seeded();
	const before = engine.readConfirmed(everything);
	engine.batch((writer) => {
		writer.writeRecord({ key: OTHER, revision: 2, fields: { title: 'changed' } });
		writer.writeIndex({ key: CLOSED, revision: 2, records: [DONE, OTHER], complete: true });
	});
	const after = engine.readConfirmed(everything);
	const changed = new Set([OTHER, CLOSED]);
	for (const [key, value] of Object.entries(after)) {
		if (changed.has(key)) assert.notEqual(value, before[key], `${key} changed`);
		else assert.equal(value, before[key], `${key} is the same object`);
	}
	assert.equal(after[OTHER].fields.title, 'changed');
	assert.deepEqual(after[CLOSED].records, [DONE, OTHER]);
});

test('reads inside a transaction see its writes over the shared root', () => {
	const engine = seeded();
	const seen = engine.batch((writer) => {
		writer.writeRecord({ key: 'Todo:new', revision: 2, fields: { title: 'new' } });
		writer.discardRecord(OTHER);
		writer.writeIndex({ key: OPEN, revision: 2, records: [TODO, 'Todo:new'], complete: true });
		return {
			clock: writer.recordClock('Todo:new'),
			gone: writer.recordClock(OTHER),
			indexes: engine.readConfirmed((reader) => [...reader.indexKeys()]),
			open: engine.readConfirmed((reader) => reader.index(OPEN).records)
		};
	});
	assert.equal(seen.clock.revision, '2');
	assert.equal(seen.gone, undefined);
	assert.deepEqual(seen.indexes, [OPEN, CLOSED, USER_TODOS]);
	assert.deepEqual(seen.open, [TODO, 'Todo:new']);
	assert.equal(engine.readConfirmed((reader) => reader.record(OTHER)), undefined);
});
