import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createDistributedReplica,
	replicaRecordKey
} from '../dist/replica/index.js';
import { Todo, Todos, wireFrame } from './fixtures/replica-todos.mjs';

const TODOS_INDEX = Object.freeze({
	field: 'todos',
	arguments: {},
	dependencies: ['todos'],
	complete: true
});

function seeded() {
	const replica = createDistributedReplica();
	replica.writeResult(
		Todos,
		{},
		wireFrame({ position: '1', rows: [{ id: 'todo-1', title: 'first' }] }),
		'network'
	);
	return replica;
}

function post(replica, layer, fields) {
	replica.createOptimisticLayer(layer, (writer) => {
		writer.writeRecord(Todo, 'todo-2', { fields });
		writer.writeIndex(TODOS_INDEX, [
			replicaRecordKey(Todo, 'todo-1'),
			replicaRecordKey(Todo, 'todo-2')
		]);
	});
}

test('a row an unsettled command creates is pending until its layer settles', () => {
	for (const settle of ['confirm', 'reject']) {
		const replica = seeded();
		const watch = replica.watch(Todos, {});
		const seen = [];
		const unsubscribe = watch.subscribe((snapshot) => seen.push(snapshot));
		assert.equal(watch.get().pending.size, 0);

		post(replica, 'cmd-post', { id: 'todo-2', title: 'posted' });
		const optimistic = watch.get();
		const [first, second] = optimistic.data.todos;
		assert.equal(optimistic.pending.has(first), false);
		assert.equal(optimistic.pending.has(second), true);
		assert.equal(optimistic.pending.size, 1);
		// Only rows of this snapshot are members; anything else is not.
		assert.equal(optimistic.pending.has({ ...second }), false);
		assert.equal(optimistic.pending.has(undefined), false);

		// Acceptance alone settles nothing: the row is still a preview.
		replica.markOptimisticLayerAccepted('cmd-post');
		assert.equal(watch.get().pending.has(watch.get().data.todos[1]), true);

		if (settle === 'confirm') {
			// The server's row carries its own values (e.g. a `now()` field the
			// server stamped); they replace the provisional ones.
			replica.confirmOptimisticLayer('cmd-post', (writer) => {
				writer.writeRecord(Todo, 'todo-2', '2', {
					fields: { id: 'todo-2', title: 'posted (server)' }
				});
				writer.writeIndex(
					TODOS_INDEX,
					[replicaRecordKey(Todo, 'todo-1'), replicaRecordKey(Todo, 'todo-2')],
					'2'
				);
			});
			const settled = watch.get();
			assert.deepEqual(settled.data.todos, [
				{ id: 'todo-1', title: 'first' },
				{ id: 'todo-2', title: 'posted (server)' }
			]);
			assert.equal(settled.pending.size, 0);
			assert.equal(settled.pending.has(settled.data.todos[1]), false);
		} else {
			// A failed command's preview rolls back: no row stays pending.
			replica.rejectOptimisticLayer('cmd-post');
			const settled = watch.get();
			assert.deepEqual(settled.data.todos, [{ id: 'todo-1', title: 'first' }]);
			assert.equal(settled.pending.size, 0);
		}
		// Every transition was published, so a page re-renders its markers.
		assert.ok(seen.length >= 3, `${settle}: ${seen.length} publications`);
		unsubscribe();
		watch.destroy();
	}
});

test('a command editing a row the server returned never makes it pending', () => {
	const replica = seeded();
	const watch = replica.watch(Todos, {});
	replica.createOptimisticLayer('cmd-rename', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { id: 'todo-1', title: 'renamed' }
		});
	});
	const snapshot = watch.get();
	// The edit's preview shows, and the row stays usable.
	assert.deepEqual(snapshot.data.todos, [{ id: 'todo-1', title: 'renamed' }]);
	assert.equal(snapshot.pending.size, 0);
	assert.equal(snapshot.pending.has(snapshot.data.todos[0]), false);
	watch.destroy();
});

test('a created row stops being pending once the server row reaches the base', () => {
	const replica = seeded();
	const watch = replica.watch(Todos, {});
	post(replica, 'cmd-post', { id: 'todo-2', title: 'posted' });
	replica.markOptimisticLayerAccepted('cmd-post');
	assert.equal(watch.get().pending.size, 1);

	// A refetch (or live frame) returns the server row while the command's
	// layer is still unsettled: the row is no longer only a preview.
	replica.writeResult(
		Todos,
		{},
		wireFrame({
			position: '2',
			rows: [
				{ id: 'todo-1', title: 'first' },
				{ id: 'todo-2', title: 'posted' }
			]
		}),
		'network'
	);
	const snapshot = watch.get();
	assert.equal(snapshot.data.todos.length, 2);
	assert.equal(snapshot.pending.size, 0);
	assert.equal(snapshot.pending.has(snapshot.data.todos[1]), false);
	watch.destroy();
});

test('a typed result never surfaces a partially shaped row', () => {
	const replica = seeded();
	// A preview that knows only some selected fields of a new row.
	post(replica, 'cmd-partial', { id: 'todo-2' });
	const snapshot = replica.read(Todos, {});
	assert.equal(snapshot.complete, false);
	assert.deepEqual(snapshot.data.todos, [{ id: 'todo-1', title: 'first' }]);
	for (const row of snapshot.data.todos) {
		assert.equal(typeof row.title, 'string');
	}
});
