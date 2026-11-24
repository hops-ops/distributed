import assert from 'node:assert/strict';
import test from 'node:test';

import { flush } from './fixtures/game-harness.mjs';
import { scripted, Todo, Todos, wireFrame } from './fixtures/replica-todos.mjs';

const GRACE = 30_000;
const rows = (...titles) =>
	titles.map((title, index) => ({ id: `todo-${index + 1}`, title }));
const liveFrame = (position, list, live = {}) =>
	wireFrame({
		operation: 'live:todos',
		position,
		rows: list,
		live: { mode: 'resumable', ...live }
	});

/** A second operation over the same model, with its own root index. */
const Archived = Object.freeze({
	...Todos,
	id: 'query:archived',
	document: 'query Archived { archived { id title } }',
	protocol: Object.freeze({ ...Todos.protocol, operation: 'query:archived' }),
	live: undefined,
	roots: Object.freeze([
		Object.freeze({
			...Todos.roots[0],
			responseKey: 'archived',
			field: 'archived',
			dependencies: Object.freeze(['archived'])
		})
	])
});
const archivedFrame = (position, list) => {
	const frame = wireFrame({
		operation: 'query:archived',
		position,
		rows: list,
		snapshotScope: 'snapshot:archived',
		indexScope: 'index:archived'
	});
	return {
		...frame,
		data: { archived: list },
		extensions: {
			distributed: {
				...frame.extensions.distributed,
				snapshot: {
					...frame.extensions.distributed.snapshot,
					records: frame.extensions.distributed.snapshot.records.map(
						(record, index) => ({
							...record,
							path: ['archived', String(index)]
						})
					)
				}
			}
		}
	};
};

/** Watch the list live, admit an HTTP result and live frames, run a command layer. */
async function useTodos({ replica, fetches, subscriptions }) {
	const watch = replica.watch(Todos, {}, { live: true });
	const unsubscribe = watch.subscribe(() => {});
	await flush();
	fetches.at(-1).resolve(wireFrame({ position: '1', rows: rows('one') }));
	await flush();
	const stream = subscriptions.at(-1);
	stream.observer.next(liveFrame('2', rows('one', 'two'), { reset: true }));
	stream.observer.next(liveFrame('3', rows('one', 'two', 'three')));
	replica.createOptimisticLayer('command-1', (writer) => {
		writer.writeRecord(Todo, 'todo-1', { fields: { title: 'optimistic' } });
	});
	assert.equal(watch.get().data.todos[0].title, 'optimistic');
	replica.rejectOptimisticLayer('command-1');
	assert.deepEqual(watch.get().data.todos, rows('one', 'two', 'three'));
	return () => {
		unsubscribe();
		watch.destroy();
	};
}

test('state returns to baseline after the last watch is destroyed and the grace period ends', async () => {
	const harness = scripted();
	const { replica, scheduler, subscriptions } = harness;
	const baseline = replica._sizes();

	const release = await useTodos(harness);
	const used = replica._sizes();
	assert.equal(used.records, 3);
	assert.equal(used.indexes, 1);
	assert.equal(used.operationProtocols, 1);
	assert.equal(used.recordClocks, 3);

	release();
	assert.equal(subscriptions.at(-1).closed, true);
	assert.equal(replica._sizes().lives, 0, 'the stream closes with the watch');
	assert.equal(replica._sizes().records, 3, 'the result stays for the grace period');

	await scheduler.advance(GRACE - 1);
	assert.equal(replica._sizes().records, 3);
	await scheduler.advance(1);
	assert.deepEqual(replica._sizes(), baseline);
	assert.equal(replica.read(Todos, {}).complete, false);
	assert.deepEqual(replica._sizes(), baseline, 'a read registers nothing');
});

test('repeated mount and unmount cycles do not accumulate state', async () => {
	const harness = scripted();
	const baseline = harness.replica._sizes();
	for (let cycle = 0; cycle < 5; cycle += 1) {
		const release = await useTodos(harness);
		release();
		await harness.scheduler.advance(GRACE);
		assert.deepEqual(harness.replica._sizes(), baseline, `cycle ${cycle}`);
	}
});

test('a remount within the grace period keeps the cached result and restarts nothing early', async () => {
	const harness = scripted();
	const { replica, scheduler, fetches } = harness;
	const release = await useTodos(harness);
	release();
	await scheduler.advance(GRACE - 1);

	const watch = replica.watch(Todos, {}, { live: false });
	assert.deepEqual(
		watch.get().data.todos,
		rows('one', 'two', 'three'),
		'the remount renders from the cache'
	);
	await scheduler.advance(GRACE * 2);
	assert.equal(replica._sizes().records, 3, 'a watched operation is never collected');

	// The grace period starts again from the new last destroy.
	watch.destroy();
	await scheduler.advance(GRACE - 1);
	assert.equal(replica._sizes().records, 3);
	await scheduler.advance(1);
	assert.equal(replica._sizes().records, 0);
	assert.equal(replica._sizes().operationProtocols, 0);
	for (const fetch of fetches) fetch.resolve(wireFrame({ position: '3' }));
});

test('collecting one operation keeps the records another operation still lists', async () => {
	const harness = scripted();
	const { replica, scheduler, fetches } = harness;
	const archived = replica.watch(Archived, {}, { live: false });
	await flush();
	fetches.at(-1).resolve(
		archivedFrame('1', [{ id: 'todo-1', title: 'one' }])
	);
	await flush();
	assert.equal(archived.get().data.archived[0].title, 'one');
	const alone = replica._sizes();

	const release = await useTodos(harness);
	assert.equal(replica._sizes().records, 3);
	release();
	await scheduler.advance(GRACE);

	assert.equal(archived.get().data.archived[0].id, 'todo-1');
	const after = replica._sizes();
	assert.equal(after.records, 1, 'todo-1 is still listed by the watched operation');
	assert.equal(after.recordClocks, 1);
	assert.equal(after.indexes, alone.indexes);
	assert.equal(after.operationProtocols, alone.operationProtocols);
	assert.equal(after.queryStates, alone.queryStates);
	archived.destroy();
});

test('a retained operation is held like a watched one until its handle is released', async () => {
	const harness = scripted();
	const { replica, scheduler } = harness;
	const baseline = replica._sizes();
	const hold = replica.retainOperation(Todos, {});
	const release = await useTodos(harness);
	release();
	await scheduler.advance(GRACE * 3);
	assert.equal(replica._sizes().records, 3);
	assert.equal(replica.dehydrate().payload.cache.records.length, 3);

	hold();
	hold();
	await scheduler.advance(GRACE);
	assert.deepEqual(replica._sizes(), baseline);
});

test('a result that arrives during the grace period restarts it', async () => {
	const harness = scripted();
	const { replica, scheduler } = harness;
	const baseline = replica._sizes();
	const release = await useTodos(harness);
	release();
	await scheduler.advance(GRACE - 1);
	// A route seed for a page that is about to mount.
	replica.writeResult(
		Todos,
		{},
		wireFrame({ position: '4', rows: rows('seeded') }),
		'ssr'
	);
	await scheduler.advance(1);
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'seeded');
	await scheduler.advance(GRACE - 1);
	assert.equal(replica._sizes().operationProtocols, 1);
	await scheduler.advance(1);
	assert.deepEqual(replica._sizes(), baseline);
});

test('a request still in flight when its operation is collected is aborted and ignored', async () => {
	const { replica, scheduler, fetches } = scripted();
	const baseline = replica._sizes();
	const watch = replica.watch(Todos, {}, { live: false });
	await flush();
	assert.equal(fetches.length, 1);
	watch.destroy();
	await scheduler.advance(GRACE);
	assert.equal(fetches[0].request.signal.aborted, true);
	fetches[0].resolve(wireFrame({ position: '1', rows: rows('late') }));
	await flush();
	assert.deepEqual(replica._sizes(), baseline);
	assert.equal(replica.read(Todos, {}).complete, false);
});

test('retentionGraceMs: Infinity never collects', async () => {
	const harness = scripted({ retentionGraceMs: Infinity });
	const release = await useTodos(harness);
	release();
	await harness.scheduler.advance(GRACE * 100);
	assert.equal(harness.replica._sizes().records, 3);
	assert.equal(harness.replica._sizes().retired, 0);
});
