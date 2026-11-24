import assert from 'node:assert/strict';
import test from 'node:test';
import { cacheEngineWork } from '../dist/internal/cache-engine.js';
import {
	canonicalizeOperationVariables,
	createReplicaDiagnostics
} from '../dist/replica/index.js';
import { Game, scalingReplica } from './fixtures/cache-scaling.mjs';
import { NoVariables, Todos } from './fixtures/replica-todos.mjs';

const WATCHERS = 50;
const DELTAS = 100;
/** The review's size and a fifth of it: the work per delta must not differ. */
const SIZES = [2_000, 10_000];

/**
 * Apply `DELTAS` live frames that each change one record and return the
 * engine's work per frame. Counts, not wall time: they do not depend on the
 * machine or its load. `scripts/bench-cache-scaling.mjs` reports the times.
 */
async function measure(records, scenario, replicaOptions) {
	const harness = await scalingReplica({ records, watchers: WATCHERS, replicaOptions });
	const kind = scenario === 'insert' ? 'insert' : 'update';
	if (scenario === 'pending') {
		harness.replica.createOptimisticLayer('pending-command', (writer) => {
			writer.writeRecord(Game, 'game-49-0', { fields: { title: 'optimistic' } });
		});
	}
	harness.notifications.fill(0);
	const results = harness.watches.map((watch) => watch.get().data);
	const before = { ...cacheEngineWork };
	const changedLists = new Set();
	for (let n = 0; n < DELTAS; n += 1) {
		const delta = harness.prepare(n, kind);
		changedLists.add(delta.list);
		delta.apply();
	}
	const work = Object.fromEntries(
		Object.entries(cacheEngineWork).map(([name, value]) => [name, value - before[name]])
	);
	return { harness, work, results, changedLists };
}

for (const scenario of ['update', 'insert', 'pending']) {
	test(`a one-record ${scenario} delta costs the same in a cache five times larger`, async () => {
		const [small, large] = [
			await measure(SIZES[0], scenario),
			await measure(SIZES[1], scenario)
		];
		assert.ok(large.harness.records >= 5 * small.harness.records);
		assert.equal(large.harness.records, 10_000);
		assert.deepEqual(large.work, small.work, 'work per delta grew with the cache');

		const { work, harness, results, changedLists } = large;
		assert.equal(work.extracts, 0, 'no pass over the whole cache');
		assert.equal(work.indexKeysListed, 0, 'no listing of every index');
		assert.equal(work.recordsWritten, DELTAS, 'one stored record per delta');
		assert.equal(work.reselections, DELTAS, 'only the changed list is selected again');
		assert.ok(
			work.indexesWritten <= DELTAS * 40,
			`${work.indexesWritten} index writes: more than the frame's own list and relationships`
		);
		// One rebuild of the overlay per delta: the layer's write, and the index
		// each watched list derives from it. Bounded by the watchers, not the cache.
		if (scenario === 'pending') {
			assert.ok(work.overlayOperations >= DELTAS);
			assert.ok(work.overlayOperations <= DELTAS * (WATCHERS + 1));
		} else {
			assert.equal(work.overlayOperations, 0);
		}

		harness.notifications.forEach((count, list) => {
			assert.equal(
				count,
				changedLists.has(list) ? DELTAS / changedLists.size : 0,
				`list ${list} notifications`
			);
		});
		harness.watches.forEach((watch, list) => {
			if (changedLists.has(list)) return;
			assert.equal(
				watch.get().data,
				results[list],
				`list ${list} did not change and must keep its result object`
			);
		});
	});
}

test('diagnostics read the cache when asked for a snapshot, not on every write', async () => {
	const diagnostics = createReplicaDiagnostics();
	const { work, harness } = await measure(SIZES[0], 'update', { diagnostics });
	assert.equal(work.extracts, 0);
	const extractsBefore = cacheEngineWork.extracts;
	const snapshot = diagnostics.snapshot();
	assert.equal(cacheEngineWork.extracts, extractsBefore + 1);
	assert.equal(snapshot.records.length, harness.records);
	assert.ok(snapshot.events.some((event) => event.kind === 'normalization'));
	const extracts = cacheEngineWork.extracts;
	assert.equal(diagnostics.snapshot(), snapshot);
	assert.equal(cacheEngineWork.extracts, extracts, 'a second snapshot reads nothing');
});

/** An object whose every property read is counted. */
function counted(source, reads) {
	return new Proxy(source, {
		get(target, key, receiver) {
			reads.count += 1;
			return Reflect.get(target, key, receiver);
		}
	});
}

test('an operation artifact is validated once, not on every call', () => {
	const reads = { count: 0 };
	const artifact = Object.freeze({
		...Todos,
		protocol: counted(Todos.protocol, reads),
		variableCodec: counted(NoVariables, reads)
	});
	canonicalizeOperationVariables(artifact, {});
	const first = reads.count;
	assert.ok(first > 0, 'the first call validates the protocol and the codec');
	for (let call = 0; call < 100; call += 1) canonicalizeOperationVariables(artifact, {});
	assert.equal(reads.count, first, 'later calls read neither again');

	// A different object is a different artifact, however alike.
	canonicalizeOperationVariables(
		Object.freeze({ ...artifact, variableCodec: counted(NoVariables, reads) }),
		{}
	);
	assert.ok(reads.count > first);
	assert.throws(() =>
		canonicalizeOperationVariables(Object.freeze({ ...Todos, variableCodec: {} }), {})
	);
});
