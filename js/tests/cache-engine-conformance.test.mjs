import assert from 'node:assert/strict';
import test from 'node:test';
import {
	CacheRevisionConflictError,
	OptimisticLayerNotFoundError,
	cacheIndexKey,
	createCacheEngine
} from '../dist/internal/cache-engine.js';

const TODO = 'Todo:todo-1';
const OTHER_TODO = 'Todo:todo-2';
const USER = 'User:user-1';
const OPEN = cacheIndexKey({ field: 'todos', arguments: { status: 'open', first: 20 } });
const OWNED_OPEN = cacheIndexKey({
	field: 'todos',
	arguments: { status: 'open', owner: 'user-1', first: 20 }
});
const ALL = cacheIndexKey({ field: 'todos', arguments: { first: 20 } });
const USER_TODOS = cacheIndexKey({ parent: USER, field: 'todos', arguments: { first: 20 } });

function todoFields(reader) {
	return reader.record(TODO)?.fields;
}

function todoAt(reader, indexKey) {
	const index = reader.index(indexKey);
	return index?.records.map((key) => reader.record(key)?.fields);
}

/** Generated-like response plan: aliases and fragments map to canonical fields. */
function normalizeAliasedTodo(writer, payload, revision) {
	writer.writeRecord({
		key: `Todo:${payload.todoId}`,
		revision,
		fields: {
			id: payload.todoId,
			title: payload.headline,
			status: payload.state,
			description: payload.description
		},
		links: { owner: `User:${payload.ownerId}` }
	});
	writer.writeRecord({
		key: `User:${payload.ownerId}`,
		revision,
		fields: { id: payload.ownerId, name: payload.ownerName },
		links: { todos: [`Todo:${payload.todoId}`] }
	});
	writer.writeIndex({ key: OPEN, revision, records: [TODO], complete: true });
	writer.writeIndex({ key: OWNED_OPEN, revision, records: [TODO], complete: true });
	writer.writeIndex({ key: ALL, revision, records: [TODO], complete: true });
	writer.writeIndex({ key: USER_TODOS, revision, records: [TODO], complete: true });
}

test('purpose-built engine normalizes sparse records, links, and exact indexes in one batch', () => {
	const engine = createCacheEngine();
	const notifications = new Map();
	const watch = (name, selector) =>
		engine.watch(selector, () => notifications.set(name, (notifications.get(name) ?? 0) + 1));

	watch('by-pk', (reader) => reader.record(TODO));
	watch('alias-fragment', (reader) => reader.record(TODO)?.fields.title);
	watch('open', (reader) => todoAt(reader, OPEN));
	watch('owned-open', (reader) => todoAt(reader, OWNED_OPEN));
	watch('all', (reader) => todoAt(reader, ALL));
	watch('relationship', (reader) => todoAt(reader, USER_TODOS));
	watch('unrelated', (reader) => reader.record(OTHER_TODO));

	engine.batch((writer) => {
		normalizeAliasedTodo(
			writer,
			{
				todoId: 'todo-1',
				headline: 'original',
				state: 'open',
				description: null,
				ownerId: 'user-1',
				ownerName: 'Ada'
			},
			1
		);
	});

	assert.deepEqual(Object.fromEntries(notifications), {
		'by-pk': 1,
		'alias-fragment': 1,
		open: 1,
		'owned-open': 1,
		all: 1,
		relationship: 1
	});
	const initial = engine.read((reader) => reader.record(TODO));
	assert.equal(initial.fields.title, 'original');
	assert.equal(initial.fields.description, null);
	assert.equal(Object.hasOwn(initial.fields, 'description'), true);
	assert.equal(Object.hasOwn(initial.fields, 'estimate'), false);
	assert.equal(initial.links.owner, USER);

	for (const key of notifications.keys()) notifications.set(key, 0);
	engine.batch((writer) => {
		// A partial fragment updates only fields it selected.
		writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'updated' } });
	});

	assert.deepEqual(Object.fromEntries(notifications), {
		'by-pk': 1,
		'alias-fragment': 1,
		open: 1,
		'owned-open': 1,
		all: 1,
		relationship: 1
	});
	const updated = engine.read((reader) => reader.record(TODO));
	assert.equal(updated.fields.title, 'updated');
	assert.equal(updated.fields.description, null);
	assert.equal(Object.hasOwn(updated.fields, 'estimate'), false);
});

test('purpose-built engine keeps argument-sensitive roots distinct and batches watchers once', () => {
	const engine = createCacheEngine();
	const canonicalA = cacheIndexKey({
		field: 'todos',
		arguments: { where: { status: 'open', owner: 'one' }, first: 20 }
	});
	const canonicalB = cacheIndexKey({
		field: 'todos',
		arguments: { first: 20, where: { owner: 'one', status: 'open' } }
	});
	const closed = cacheIndexKey({
		field: 'todos',
		arguments: { first: 20, where: { owner: 'one', status: 'closed' } }
	});
	assert.equal(canonicalA, canonicalB);
	assert.notEqual(canonicalA, closed);

	let calls = 0;
	engine.watch((reader) => todoAt(reader, canonicalA), () => calls++);
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { id: 'todo-1', status: 'open' } });
		writer.writeIndex({ key: canonicalA, revision: 1, records: [TODO], complete: true });
		writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'batched' } });
	});
	assert.equal(calls, 1);
	assert.equal(engine.read((reader) => reader.index(closed)), undefined);
});

test('tracked field dependencies skip selectors for unrelated record fields', () => {
	const engine = createCacheEngine();
	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 1,
			fields: { title: 'one', status: 'open', description: 'initial' }
		})
	);
	let titleSelections = 0;
	let statusSelections = 0;
	engine.watch(
		(reader) => {
			titleSelections += 1;
			const field = reader.field(TODO, 'title');
			return field.present ? field.value : undefined;
		},
		() => undefined
	);
	engine.watch(
		(reader) => {
			statusSelections += 1;
			const field = reader.field(TODO, 'status');
			return field.present ? field.value : undefined;
		},
		() => undefined
	);
	titleSelections = 0;
	statusSelections = 0;

	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 2, fields: { description: 'unrelated' } })
	);
	assert.equal(titleSelections, 0);
	assert.equal(statusSelections, 0);

	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 3, fields: { title: 'two' } })
	);
	assert.equal(titleSelections, 1);
	assert.equal(statusSelections, 0);
});

test('named optimistic layers survive acceptance and stale base responses', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 4, fields: { status: 'open' } });
	});
	engine.createOptimisticLayer('complete-1', (writer) => {
		writer.writeRecord({ key: TODO, fields: { status: 'completed-optimistic' } });
	});
	assert.equal(engine.markOptimisticLayerAccepted('complete-1'), true);
	assert.equal(engine.optimisticLayerState('complete-1'), 'accepted');

	engine.batch((writer) => {
		assert.equal(
			writer.writeRecord({ key: TODO, revision: 3, fields: { status: 'open-stale' } }),
			false
		);
	});
	assert.equal(engine.read((reader) => todoFields(reader).status), 'completed-optimistic');
	assert.equal(engine.optimisticLayerState('complete-1'), 'accepted');
});

test('optimistic replacement reads only its prefix and publishes one rebased final state', () => {
	const engine = createCacheEngine();
	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 1,
			fields: { title: 'base', status: 'open' }
		})
	);
	const confirmed = engine.extract();
	engine.createOptimisticLayer('lower', (writer) =>
		writer.writeRecord({
			key: TODO,
			fields: { title: 'lower', status: 'lower-open' }
		})
	);
	engine.createOptimisticLayer(
		'target',
		(writer) =>
			writer.writeRecord({
				key: TODO,
				fields: { status: 'preview' }
			}),
		{ stable: 'target-context' }
	);
	assert.equal(engine.markOptimisticLayerAccepted('target'), true);
	engine.createOptimisticLayer('suffix', (writer) =>
		writer.writeRecord({
			key: TODO,
			fields: { title: 'suffix', estimate: 3 }
		})
	);

	const delivered = [];
	engine.watch(
		(reader) => reader.record(TODO)?.fields,
		(value) => delivered.push(value)
	);
	let prefixFields;
	assert.equal(
		engine.replaceOptimisticLayer('target', (reader, writer) => {
			prefixFields = reader.record(TODO)?.fields;
			writer.writeRecord({
				key: TODO,
				fields: { status: 'actual' }
			});
		}),
		true
	);

	assert.deepEqual(prefixFields, {
		title: 'lower',
		status: 'lower-open'
	});
	assert.deepEqual(delivered, [
		{ title: 'suffix', status: 'actual', estimate: 3 }
	]);
	assert.equal(engine.optimisticLayerState('target'), 'accepted');
	assert.deepEqual(engine.extract(), confirmed);

	assert.equal(engine.rejectOptimisticLayer('suffix'), true);
	assert.deepEqual(engine.read((reader) => reader.record(TODO)?.fields), {
		title: 'lower',
		status: 'actual'
	});
	assert.equal(engine.rejectOptimisticLayer('target'), true);
	assert.deepEqual(engine.read((reader) => reader.record(TODO)?.fields), {
		title: 'lower',
		status: 'lower-open'
	});
});

test('identical optimistic replacement is a no-op and missing layers fail typed without mutation', () => {
	const engine = createCacheEngine();
	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 7,
			fields: { title: 'base' }
		})
	);
	engine.createOptimisticLayer('target', (writer) =>
		writer.writeRecord({ key: TODO, fields: { title: 'preview' } })
	);
	assert.equal(engine.markOptimisticLayerAccepted('target'), true);
	const confirmed = engine.extract();
	const visible = engine.read((reader) => reader.record(TODO));
	let calls = 0;
	engine.watch(
		(reader) => reader.record(TODO),
		() => calls++
	);

	assert.equal(
		engine.replaceOptimisticLayer('target', (_reader, writer) =>
			writer.writeRecord({ key: TODO, fields: { title: 'preview' } })
		),
		false
	);
	let replacementInvoked = false;
	assert.throws(
		() =>
			engine.replaceOptimisticLayer('missing', () => {
				replacementInvoked = true;
			}),
		(error) =>
			error instanceof OptimisticLayerNotFoundError &&
			error.layerId === 'missing'
	);

	assert.equal(replacementInvoked, false);
	assert.equal(calls, 0);
	assert.equal(engine.optimisticLayerState('target'), 'accepted');
	assert.deepEqual(engine.extract(), confirmed);
	assert.deepEqual(engine.read((reader) => reader.record(TODO)), visible);
});

test('conditional replacement falls back when its earlier optimistic create is gone', () => {
	const engine = createCacheEngine();
	engine.createOptimisticLayer('earlier-create', (writer) =>
		writer.writeRecord({
			key: TODO,
			fields: { id: 'todo-1', title: 'created' }
		})
	);
	engine.createOptimisticLayer('later-patch', (writer) =>
		writer.writeRecord({
			key: TODO,
			fields: { status: 'preview' }
		})
	);
	assert.equal(engine.rejectOptimisticLayer('earlier-create'), true);
	assert.deepEqual(engine.read((reader) => reader.record(TODO)?.fields), {
		status: 'preview'
	});

	const delivered = [];
	engine.watch(
		(reader) => reader.record(TODO),
		(value) => delivered.push(value)
	);
	assert.equal(
		engine.replaceOptimisticLayer('later-patch', (reader, writer) => {
			if (reader.record(TODO) === undefined) return;
			writer.writeRecord({
				key: TODO,
				fields: { status: 'actual' }
			});
		}),
		true
	);

	assert.equal(engine.read((reader) => reader.record(TODO)), undefined);
	assert.deepEqual(delivered, [undefined]);
	assert.deepEqual(engine.extract().records, []);
	assert.equal(engine.optimisticLayerState('later-patch'), 'optimistic');
});

test('causal confirmation writes base and retires its layer atomically', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { status: 'open' } });
	});
	engine.createOptimisticLayer('complete-1', (writer) => {
		writer.writeRecord({ key: TODO, fields: { status: 'optimistic' } });
	});
	let calls = 0;
	const values = [];
	engine.watch(
		(reader) => reader.record(TODO)?.fields.status,
		(value) => {
			calls++;
			values.push(value);
		}
	);

	engine.confirmOptimisticLayer('complete-1', (writer) => {
		writer.writeRecord({ key: TODO, revision: 2, fields: { status: 'atomic' } });
	});
	assert.equal(calls, 1);
	assert.deepEqual(values, ['atomic']);
	assert.equal(engine.optimisticLayerState('complete-1'), undefined);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.status), 'atomic');
});

test('same-entity optimistic layers confirm and reject out of order without cross-rollback', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'base', status: 'open' } });
	});

	engine.createOptimisticLayer('title-a', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'A' } });
	});
	engine.createOptimisticLayer('title-b', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'B' } });
	});
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'B');

	// B confirms first. A stays tracked but cannot reappear above B's projection.
	engine.confirmOptimisticLayer('title-b', (writer) => {
		writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'B projected' } });
	});
	assert.equal(engine.optimisticLayerState('title-a'), 'optimistic');
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'B projected');
	engine.confirmOptimisticLayer('title-a', (writer) => {
		assert.equal(
			writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'A stale projection' } }),
			false
		);
	});
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'B projected');

	engine.createOptimisticLayer('status-c', (writer) => {
		writer.writeRecord({ key: TODO, fields: { status: 'C' } });
	});
	engine.createOptimisticLayer('status-d', (writer) => {
		writer.writeRecord({ key: TODO, fields: { status: 'D' } });
	});
	// C confirms first, but the causally later pending D remains visible.
	engine.confirmOptimisticLayer('status-c', (writer) => {
		writer.writeRecord({ key: TODO, revision: 3, fields: { status: 'C projected' } });
	});
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.status), 'D');
	engine.rejectOptimisticLayer('status-d');
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.status), 'C projected');

	engine.createOptimisticLayer('title-e', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'E' } });
	});
	engine.createOptimisticLayer('title-f', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'F' } });
	});
	engine.rejectOptimisticLayer('title-f');
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'E');
	engine.confirmOptimisticLayer('title-e', (writer) => {
		writer.writeRecord({ key: TODO, revision: 4, fields: { title: 'E projected' } });
	});
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'E projected');
});

test('confirming one field does not retire an independent optimistic field', () => {
	const engine = createCacheEngine();
	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 1,
			fields: { title: 'base title', status: 'base status' }
		})
	);
	engine.createOptimisticLayer('title-layer', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'pending title' } });
	});
	engine.createOptimisticLayer('status-layer', (writer) => {
		writer.writeRecord({ key: TODO, fields: { status: 'pending status' } });
	});

	engine.confirmOptimisticLayer('status-layer', (writer) => {
		// Projectors commonly return a full row, including fields owned by other
		// in-flight commands.
		writer.writeRecord({
			key: TODO,
			revision: 2,
			fields: { title: 'server title', status: 'projected status' }
		});
	});
	assert.deepEqual(engine.read((reader) => reader.record(TODO)?.fields), {
		title: 'pending title',
		status: 'projected status'
	});
	assert.equal(engine.optimisticLayerState('title-layer'), 'optimistic');
	engine.rejectOptimisticLayer('title-layer');
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'server title');
});

test('field and link selectors observe base and optimistic tombstones', () => {
	const engine = createCacheEngine();
	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 1,
			fields: { title: 'visible' },
			links: { owner: USER }
		})
	);
	const fieldPresence = [];
	const linkPresence = [];
	engine.watch((reader) => reader.field(TODO, 'title'), (value) => fieldPresence.push(value));
	engine.watch((reader) => reader.link(TODO, 'owner'), (value) => linkPresence.push(value));

	engine.batch((writer) => writer.tombstoneRecord(TODO, 2));
	assert.deepEqual(fieldPresence, [{ present: false }]);
	assert.deepEqual(linkPresence, [{ present: false }]);

	engine.batch((writer) =>
		writer.writeRecord({
			key: TODO,
			revision: 3,
			fields: { title: 'recreated' },
			links: { owner: USER }
		})
	);
	fieldPresence.length = 0;
	linkPresence.length = 0;
	engine.createOptimisticLayer('delete-visible-record', (writer) => {
		writer.tombstoneRecord(TODO);
	});
	assert.deepEqual(fieldPresence, [{ present: false }]);
	assert.deepEqual(linkPresence, [{ present: false }]);
	engine.rejectOptimisticLayer('delete-visible-record');
	assert.equal(engine.read((reader) => reader.field(TODO, 'title')).present, true);
});

test('newer stale markers fence older index writes and deletes', () => {
	const engine = createCacheEngine();
	const metadata = {
		field: 'todos',
		arguments: { first: 20 },
		coverage: { kind: 'complete' },
		dependencies: ['todos']
	};
	engine.batch((writer) =>
		writer.writeIndex({ key: ALL, revision: 10, records: [TODO], complete: true, metadata })
	);
	engine.batch((writer) => {
		assert.equal(writer.markIndexStale(ALL, 'late-error', 9), false);
		assert.equal(writer.markIndexStale(ALL, 'new-error', 11), true);
		assert.equal(
			writer.writeIndex({
				key: ALL,
				revision: 10,
				records: [OTHER_TODO],
				complete: true,
				metadata
			}),
			false
		);
		assert.equal(writer.deleteIndex(ALL, 10), false);
	});
	assert.equal(engine.read((reader) => reader.index(ALL)?.revision), '10');
	assert.equal(engine.read((reader) => reader.index(ALL)?.staleRevision), '11');
	assert.equal(engine.read((reader) => reader.index(ALL)?.metadata.staleReason), 'new-error');

	engine.batch((writer) =>
		writer.writeIndex({ key: ALL, revision: 11, records: [TODO], complete: true, metadata })
	);
	assert.equal(engine.read((reader) => reader.index(ALL)?.staleRevision), undefined);
	assert.equal(engine.read((reader) => reader.index(ALL)?.metadata.staleReason), undefined);
});

test('missing index checkpoints survive extract/restore without inventing membership', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		assert.equal(writer.markIndexStale(ALL, 'uncached-error', 10), true);
	});
	assert.equal(engine.read((reader) => reader.index(ALL)), undefined);
	const restored = createCacheEngine();
	restored.restore(JSON.parse(JSON.stringify(engine.extract())));
	assert.equal(restored.read((reader) => reader.index(ALL)), undefined);
	restored.batch((writer) => {
		assert.equal(
			writer.writeIndex({ key: ALL, revision: 9, records: [TODO], complete: true }),
			false
		);
	});
	assert.equal(restored.read((reader) => reader.index(ALL)), undefined);

	const zero = cacheIndexKey({ field: 'zero_revision', arguments: {} });
	const zeroFenced = createCacheEngine();
	zeroFenced.batch((writer) => writer.markIndexStale(zero, 'revision-zero-error', 0));
	const zeroRestored = createCacheEngine();
	zeroRestored.restore(JSON.parse(JSON.stringify(zeroFenced.extract())));
	zeroRestored.batch((writer) => {
		assert.equal(
			writer.writeIndex({ key: zero, revision: 0, records: [TODO], complete: true }),
			true
		);
	});
	assert.deepEqual(zeroRestored.read((reader) => reader.index(zero)?.records), [TODO]);
});

test('confirmed index fences include visible, stale, and hidden base checkpoints', () => {
	const engine = createCacheEngine();
	const stale = cacheIndexKey({ field: 'stale_todos', arguments: {} });
	const deleted = cacheIndexKey({ field: 'deleted_todos', arguments: {} });
	const hiddenStale = cacheIndexKey({
		field: 'hidden_stale_todos',
		arguments: {}
	});
	const missing = cacheIndexKey({ field: 'missing_todos', arguments: {} });
	engine.batch((writer) => {
		writer.writeIndex({
			key: ALL,
			revision: 5,
			records: [TODO],
			complete: true
		});
		writer.writeIndex({
			key: stale,
			revision: 4,
			records: [TODO],
			complete: true
		});
		writer.markIndexStale(stale, 'partial-error', 7);
		writer.deleteIndex(deleted, 9);
		writer.markIndexStale(hiddenStale, 'missing-result', 11);
	});

	assert.deepEqual(
		[...engine.confirmedIndexFences([
			ALL,
			stale,
			deleted,
			hiddenStale,
			missing
		])],
		[
			[ALL, '5'],
			[stale, '7'],
			[deleted, '9'],
			[hiddenStale, '11']
		]
	);
	assert.equal(engine.read((reader) => reader.index(deleted)), undefined);
	assert.equal(engine.read((reader) => reader.index(hiddenStale)), undefined);
});

test('one authoritative batch may rewrite lifecycle-invalidated membership at its checkpoint', () => {
	const engine = createCacheEngine();
	const metadata = {
		field: 'todos',
		arguments: { first: 20 },
		coverage: { kind: 'complete' },
		dependencies: ['todos']
	};
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { id: 'todo-1' } });
		writer.writeRecord({ key: OTHER_TODO, revision: 1, fields: { id: 'todo-2' } });
		writer.writeRecord({ key: USER, revision: 1, fields: { id: 'user-1' } });
		writer.writeIndex({
			key: ALL,
			revision: 2,
			records: [TODO, OTHER_TODO, USER],
			complete: true,
			metadata
		});
	});
	engine.batch((writer) => {
		writer.tombstoneRecord(TODO, 3);
		assert.equal(
			writer.writeIndex({
				key: ALL,
				revision: 2,
				records: [USER, OTHER_TODO],
				complete: true,
				metadata
			}),
			true
		);
	});
	const index = engine.read((reader) => reader.index(ALL));
	assert.deepEqual(index.records, [USER, OTHER_TODO]);
	assert.equal(index.complete, true);
	assert.equal(index.staleRevision, undefined);
});

test('record revisions and tombstones reject stale overwrite and resurrection', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: '8', fields: { title: 'latest', status: 'open' } });
		assert.equal(writer.tombstoneRecord(TODO, 9n), true);
	});
	assert.equal(engine.read((reader) => reader.record(TODO)), undefined);

	engine.batch((writer) => {
		assert.equal(writer.writeRecord({ key: TODO, revision: 8, fields: { title: 'stale' } }), false);
		assert.equal(writer.tombstoneRecord(TODO, 7), false);
	});
	assert.throws(
		() =>
			engine.batch((writer) =>
				writer.writeRecord({ key: TODO, revision: 9, fields: { title: 'same' } })
			),
		CacheRevisionConflictError
	);
	assert.equal(engine.read((reader) => reader.record(TODO)), undefined);

	engine.batch((writer) => {
		assert.equal(
			writer.writeRecord({ key: TODO, revision: 10, fields: { id: 'todo-1', title: 'recreated' } }),
			true
		);
	});
	const recreated = engine.read((reader) => reader.record(TODO));
	assert.equal(recreated.fields.title, 'recreated');
	assert.equal(Object.hasOwn(recreated.fields, 'status'), false);
});

test('SSR extract and restore contain confirmed base only', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'confirmed' } });
		writer.writeIndex({ key: ALL, revision: 1, records: [TODO], complete: true });
	});
	engine.createOptimisticLayer('rename-1', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'optimistic secret' } });
		writer.writeRecord({ key: OTHER_TODO, fields: { title: 'optimistic insert' } });
		writer.writeIndex({ key: ALL, records: [TODO, OTHER_TODO], complete: true });
	});

	const snapshot = engine.extract();
	const serialized = JSON.stringify(snapshot);
	assert.doesNotMatch(serialized, /optimistic/);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'optimistic secret');

	const restored = createCacheEngine();
	restored.restore(JSON.parse(serialized));
	assert.equal(restored.read((reader) => reader.record(TODO)?.fields.title), 'confirmed');
	assert.equal(restored.read((reader) => reader.record(OTHER_TODO)), undefined);
	assert.deepEqual(restored.read((reader) => reader.index(ALL)?.records), [TODO]);
	assert.equal(restored.optimisticLayerState('rename-1'), undefined);
});

test('GC traverses confirmed indexes and relationship links while retaining tombstone fences', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: USER, revision: 1, fields: { name: 'Ada' }, links: { todos: [TODO] } });
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'reachable' } });
		writer.writeRecord({ key: OTHER_TODO, revision: 1, fields: { title: 'orphan' } });
		writer.writeRecord({ key: 'Todo:deleted', revision: 1, fields: { title: 'deleted' } });
		writer.tombstoneRecord('Todo:deleted', 2);
		writer.writeIndex({ key: ALL, revision: 1, records: [USER], complete: true });
	});
	assert.deepEqual(engine.gc(), [OTHER_TODO]);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'reachable');

	engine.batch((writer) => writer.deleteIndex(ALL, 2));
	engine.retain(USER);
	assert.deepEqual(engine.gc(), []);
	engine.release(USER);
	assert.deepEqual(engine.gc(), [TODO, USER]);
	assert.match(JSON.stringify(engine.extract()), /Todo:deleted/);
});

test('GC treats parent-scoped indexes as edges, not immortal roots', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: USER, revision: 1, fields: { id: 'user-1' } });
		writer.writeRecord({ key: TODO, revision: 1, fields: { id: 'todo-1' } });
		writer.writeIndex({ key: ALL, revision: 1, records: [USER], complete: true });
		writer.writeIndex({
			key: USER_TODOS,
			revision: 1,
			records: [TODO],
			complete: true,
			metadata: {
				parent: USER,
				parentRevision: '1',
				field: 'todos',
				arguments: { first: 20 },
				coverage: { kind: 'complete' },
				dependencies: ['todos']
			}
		});
	});
	assert.deepEqual(engine.gc(), []);
	engine.batch((writer) => writer.deleteIndex(ALL, 2));
	assert.deepEqual(engine.gc(), [TODO, USER]);
	assert.equal(engine.read((reader) => reader.index(USER_TODOS)), undefined);
});

test('GC cannot corrupt rollback beneath destructive optimistic overlays', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: USER, revision: 1, fields: { name: 'Ada' }, links: { todos: [TODO] } });
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'confirmed child' } });
		writer.writeRecord({ key: OTHER_TODO, revision: 1, fields: { title: 'optimistic root' } });
		writer.writeIndex({ key: ALL, revision: 1, records: [USER], complete: true });
	});

	engine.createOptimisticLayer('rewrite-roots', (writer) => {
		writer.tombstoneRecord(USER);
		writer.writeIndex({ key: ALL, records: [OTHER_TODO], complete: true });
	});
	assert.deepEqual(engine.gc(), []);
	engine.rejectOptimisticLayer('rewrite-roots');
	assert.deepEqual(engine.read((reader) => reader.index(ALL)?.records), [USER]);
	assert.deepEqual(engine.read((reader) => reader.record(USER)?.links.todos), [TODO]);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'confirmed child');
	assert.deepEqual(engine.gc(), [OTHER_TODO]);

	engine.createOptimisticLayer('delete-root', (writer) => writer.deleteIndex(ALL));
	assert.deepEqual(engine.gc(), []);
	engine.rejectOptimisticLayer('delete-root');
	assert.deepEqual(engine.read((reader) => reader.index(ALL)?.records), [USER]);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'confirmed child');
});

test('GC preserves every record that can reappear from a non-current optimistic layer', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: USER, revision: 1, fields: { name: 'Ada' } });
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'hidden by top layer' } });
		writer.writeIndex({ key: USER_TODOS, revision: 1, records: [USER], complete: true });
	});

	engine.createOptimisticLayer('older-root-and-link', (writer) => {
		writer.writeRecord({ key: USER, links: { todos: [TODO] } });
		writer.writeIndex({ key: ALL, records: [TODO], complete: true });
	});
	engine.createOptimisticLayer('newer-hides-root-and-link', (writer) => {
		writer.writeRecord({ key: USER, links: { todos: [] } });
		writer.writeIndex({ key: ALL, records: [], complete: true });
	});
	assert.deepEqual(engine.read((reader) => reader.record(USER)?.links.todos), []);
	assert.deepEqual(engine.read((reader) => reader.index(ALL)?.records), []);
	assert.deepEqual(engine.gc(), []);

	engine.rejectOptimisticLayer('newer-hides-root-and-link');
	assert.deepEqual(engine.read((reader) => reader.record(USER)?.links.todos), [TODO]);
	assert.deepEqual(engine.read((reader) => reader.index(ALL)?.records), [TODO]);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'hidden by top layer');

	engine.rejectOptimisticLayer('older-root-and-link');
	assert.deepEqual(engine.gc(), [TODO]);
});

test('failed batches restore base state without notifying observers', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'base' } }));
	let calls = 0;
	engine.watch((reader) => reader.record(TODO)?.fields.title, () => calls++);
	assert.throws(
		() =>
			engine.batch((writer) => {
				writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'should roll back' } });
				throw new Error('abort');
			}),
		/abort/
	);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'base');
	assert.equal(calls, 0);
});

test('nested batches are rejected before inner writes and rollback clears dependency dirt', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'outer' } });
		assert.throws(
			() =>
				engine.batch((inner) => {
					inner.writeRecord({ key: TODO, revision: 2, fields: { status: 'leaked' } });
				}),
			/nested cache batches/
		);
		writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'outer committed' } });
	});
	assert.equal(Object.hasOwn(engine.read((reader) => reader.record(TODO).fields), 'status'), false);

	let statusSelections = 0;
	engine.watch(
		(reader) => {
			statusSelections += 1;
			return reader.field(TODO, 'status');
		},
		() => undefined
	);
	statusSelections = 0;
	assert.throws(
		() =>
			engine.batch((writer) => {
				writer.writeRecord({ key: TODO, revision: 3, fields: { status: 'rolled back' } });
				throw new Error('rollback dependency set');
			}),
		/rollback dependency set/
	);
	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 3, fields: { title: 'unrelated' } })
	);
	assert.equal(statusSelections, 0);
});

test('async base and optimistic updaters are rejected, rolled back, and revoked', async () => {
	const engine = createCacheEngine();
	let releaseBase;
	const baseGate = new Promise((resolve) => {
		releaseBase = resolve;
	});
	let baseLateError;
	let finishBase;
	const baseFinished = new Promise((resolve) => {
		finishBase = resolve;
	});
	assert.throws(
		() =>
			engine.batch(async (writer) => {
				writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'escaped' } });
				await baseGate;
				try {
					writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'late' } });
				} catch (error) {
					baseLateError = error;
				} finally {
					finishBase();
				}
			}),
		/cache update must be synchronous/
	);
	assert.equal(engine.read((reader) => reader.record(TODO)), undefined);
	releaseBase();
	await baseFinished;
	assert.match(String(baseLateError), /writer is no longer active/);
	assert.equal(engine.read((reader) => reader.record(TODO)), undefined);

	let releaseLayer;
	const layerGate = new Promise((resolve) => {
		releaseLayer = resolve;
	});
	let layerLateError;
	let finishLayer;
	const layerFinished = new Promise((resolve) => {
		finishLayer = resolve;
	});
	assert.throws(
		() =>
			engine.createOptimisticLayer('async-layer', async (writer) => {
				writer.writeRecord({ key: TODO, fields: { title: 'escaped optimistic' } });
				await layerGate;
				try {
					writer.writeRecord({ key: TODO, fields: { title: 'late optimistic' } });
				} catch (error) {
					layerLateError = error;
				} finally {
					finishLayer();
				}
			}),
		/optimistic layer update must be synchronous/
	);
	assert.equal(engine.optimisticLayerState('async-layer'), undefined);
	releaseLayer();
	await layerFinished;
	assert.match(String(layerLateError), /writer is no longer active/);

	engine.createOptimisticLayer('confirm-async', (writer) => {
		writer.writeRecord({ key: TODO, fields: { title: 'pending confirmation' } });
	});
	assert.throws(
		() =>
			engine.confirmOptimisticLayer('confirm-async', async (writer) => {
				writer.writeRecord({ key: TODO, revision: 3, fields: { title: 'escaped confirm' } });
			}),
		/cache update must be synchronous/
	);
	assert.equal(engine.optimisticLayerState('confirm-async'), 'optimistic');
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'pending confirmation');
});

test('watcher failures are isolated after commit and reported as one aggregate', () => {
	const reported = [];
	const engine = createCacheEngine({ onWatcherError: (error) => reported.push(error) });
	const delivered = [];
	engine.watch(
		(reader) => reader.record(TODO)?.fields.title,
		() => {
			delivered.push('first');
			throw new Error('first watcher failed');
		}
	);
	engine.watch(
		(reader) => reader.record(TODO)?.fields.title,
		() => delivered.push('second')
	);

	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'committed' } })
	);
	assert.deepEqual(delivered, ['first', 'second']);
	assert.equal(reported.length, 1);
	assert.ok(reported[0] instanceof AggregateError);
	assert.match(reported[0].message, /transaction committed/);
	assert.equal(reported[0].errors.length, 1);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'committed');
});

test('a failing immediate watcher is reported and unregistered', () => {
	const reported = [];
	const engine = createCacheEngine({ onWatcherError: (error) => reported.push(error) });
	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 1, fields: { title: 'initial' } })
	);
	let calls = 0;
	engine.watch(
		(reader) => reader.field(TODO, 'title'),
		() => {
			calls += 1;
			throw new Error('immediate failed');
		},
		{ immediate: true }
	);
	assert.equal(calls, 1);
	assert.equal(reported.length, 1);
	engine.batch((writer) =>
		writer.writeRecord({ key: TODO, revision: 2, fields: { title: 'later' } })
	);
	assert.equal(calls, 1);
});

test('same-revision disagreement raises a deterministic conflict and rolls back', () => {
	const engine = createCacheEngine();
	engine.batch((writer) => {
		writer.writeRecord({ key: TODO, revision: 4, fields: { title: 'canonical' } });
		writer.writeIndex({ key: ALL, revision: 4, records: [TODO], complete: true });
	});

	assert.throws(
		() =>
			engine.batch((writer) => {
				writer.writeRecord({ key: OTHER_TODO, revision: 1, fields: { title: 'rolled back' } });
				writer.writeRecord({ key: TODO, revision: 4, fields: { title: 'conflict' } });
			}),
		(error) =>
			error instanceof CacheRevisionConflictError &&
			error.revision === '4' &&
			error.dependency.includes('field:title')
	);
	assert.equal(engine.read((reader) => reader.record(OTHER_TODO)), undefined);
	assert.equal(engine.read((reader) => reader.record(TODO)?.fields.title), 'canonical');

	assert.throws(
		() =>
			engine.batch((writer) =>
				writer.writeIndex({ key: ALL, revision: 4, records: [], complete: true })
			),
		CacheRevisionConflictError
	);
	assert.deepEqual(engine.read((reader) => reader.index(ALL)?.records), [TODO]);
	assert.throws(
		() => engine.batch((writer) => writer.tombstoneRecord(TODO, 4)),
		CacheRevisionConflictError
	);
});

test('structured index metadata must agree with its canonical key', () => {
	const engine = createCacheEngine();
	assert.throws(
		() =>
			engine.batch((writer) =>
				writer.writeIndex({
					key: ALL,
					revision: 1,
					records: [],
					metadata: {
						field: 'different_root',
						arguments: {},
						coverage: { kind: 'complete' },
						dependencies: []
					}
				})
			),
		/index key does not match its metadata/
	);
});
