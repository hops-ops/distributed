import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { DISTRIBUTED_PROTOCOL_VERSION } from '../dist/index.js';
import { bindSveltekitOperation } from '../dist/sveltekit/index.js';
import { flush } from './fixtures/game-harness.mjs';
import { scripted } from './fixtures/replica-todos.mjs';

// `query Feed($limit: Int! = 10) @live @window` ordered `[{priority: desc}]`
// over a `(tenantId, id)` key, exactly as the Rust compiler emits it.
const artifact = JSON.parse(
	readFileSync(
		new URL('../../distributed_cli/tests/fixtures/window-bridge-operation.json', import.meta.url),
		'utf8'
	)
);
const root = artifact.roots[0];

const id = (rank) => `m${String(rank).padStart(2, '0')}`;
const wireRow = (rank) => ({
	id: id(rank),
	tenantId: 't',
	rank,
	title: `#${rank}`,
	_distributed_typename: root.selection.typename,
	_distributed_completed: false
});
/** Ranks from `newest` down to `oldest`, the window's order. */
const ranks = (newest, oldest) =>
	Array.from({ length: newest - oldest + 1 }, (_, index) => newest - index);

let positions = 0;
function frame(rankList, options = {}) {
	positions += 1;
	const position = String(positions);
	const projection = 'feed-projector';
	const resume = { projection, position, token: `resume:${position}` };
	return {
		data: { [root.responseKey]: rankList.map(wireRow) },
		extensions: {
			distributed: {
				protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
				schemaHash: artifact.protocol.schemaHash,
				authorizationGeneration: 'auth-1',
				cacheScope: 'cache:a',
				operation: options.operation ?? artifact.id,
				snapshot: {
					scopeToken: 'snapshot:feed',
					recordsComplete: true,
					indexesComparable: true,
					records: rankList.map((rank, index) => ({
						path: [root.responseKey, String(index)],
						model: root.selection.storage.model,
						scopeToken: `record:${id(rank)}`,
						incarnation: '1',
						revision: position,
						tombstone: false
					})),
					indexes: [{ projection, scopeToken: 'index:feed', position, resume }],
					observations: []
				},
				...(options.live ? { live: { mode: 'resumable', reset: false, cursors: [resume] } } : {})
			}
		}
	};
}

const rankOf = (snapshot) => snapshot.rows.map((row) => row.rank);

/** A subscribed window whose head (ranks `newest`..`newest - 9`) has loaded. */
async function openWindow(newest = 30) {
	positions = 0;
	const harness = scripted();
	const window = bindSveltekitOperation(harness.replica, artifact).useWindow();
	const unsubscribe = window.subscribe(() => {});
	await flush();
	assert.equal(harness.fetches.length, 1, 'the head is one request');
	assert.deepEqual(harness.fetches[0].request.variables, { limit: 10 });
	harness.fetches[0].resolve(frame(ranks(newest, newest - 9)));
	await flush();
	assert.equal(harness.subscriptions.length, 1, 'only the head is live');
	const live = (rankList) =>
		harness.subscriptions[0].observer.next(
			frame(rankList, { operation: artifact.live.id, live: true })
		);
	return { ...harness, window, unsubscribe, live };
}

async function fetchMore(harness, rankList) {
	const before = harness.fetches.length;
	const loading = harness.window.fetchMore();
	assert.equal(harness.window.get().fetchingMore, true);
	await flush();
	assert.equal(harness.fetches.length, before + 1, 'fetchMore is one request');
	const request = harness.fetches.at(-1);
	request.resolve(frame(rankList));
	await loading;
	return request.request.variables;
}

test('a window keeps retained pages while the live head slides, as one ordered list', async () => {
	const harness = await openWindow(30);
	const { window } = harness;
	assert.deepEqual(rankOf(window.get()), ranks(30, 21));
	assert.equal(window.get().hasMore, true, 'a full head may have older rows');

	// The first page starts after the head's newest row and covers the rest
	// of the head plus one page, so the head can slide without a gap.
	const first = await fetchMore(harness, ranks(29, 11));
	assert.deepEqual(first, {
		cursor_id: 'm30',
		cursor_priority: 30,
		cursor_tenantId: 't',
		limit: 19
	});
	let snapshot = window.get();
	assert.deepEqual(rankOf(snapshot), ranks(30, 11), 'head and page overlap once');
	assert.equal(new Set(snapshot.rows.map((row) => row.id)).size, snapshot.rows.length);
	assert.equal(snapshot.hasMore, true);
	assert.equal(snapshot.fetchingMore, false);
	assert.equal(snapshot.complete, true);

	// A new head row does not stale, change or refetch the retained page.
	const fetches = harness.fetches.length;
	harness.live(ranks(31, 22));
	await flush();
	snapshot = window.get();
	assert.deepEqual(rankOf(snapshot), ranks(31, 11));
	assert.equal(snapshot.complete, true);
	assert.equal(snapshot.stale, false);
	assert.equal(harness.fetches.length, fetches, 'no page refetched');

	// Later pages continue after the last retained row; a short page ends it.
	const second = await fetchMore(harness, ranks(10, 5));
	assert.deepEqual(second, {
		cursor_id: 'm11',
		cursor_priority: 11,
		cursor_tenantId: 't',
		limit: 10
	});
	snapshot = window.get();
	assert.deepEqual(rankOf(snapshot), ranks(31, 5));
	assert.equal(snapshot.hasMore, false, 'a short page is the end');
	await window.fetchMore();
	await flush();
	assert.equal(harness.fetches.length, fetches + 1, 'nothing after the end');

	harness.unsubscribe();
	window.destroy();
});

test('a head shorter than a page has nothing more and fetches nothing', async () => {
	positions = 0;
	const harness = scripted();
	const window = bindSveltekitOperation(harness.replica, artifact).useWindow();
	const unsubscribe = window.subscribe(() => {});
	await flush();
	harness.fetches[0].resolve(frame(ranks(4, 1)));
	await flush();
	assert.deepEqual(rankOf(window.get()), ranks(4, 1));
	assert.equal(window.get().hasMore, false);
	await window.fetchMore();
	await flush();
	assert.equal(harness.fetches.length, 1);
	unsubscribe();
	window.destroy();
});

test('a failed fetchMore keeps the rows, reports the error and retries', async () => {
	const harness = await openWindow(30);
	const { window } = harness;
	const loading = window.fetchMore();
	// A second call while one is in flight shares it.
	assert.equal(window.fetchMore(), loading);
	await flush();
	assert.equal(harness.fetches.length, 2);
	harness.fetches[1].reject(new TypeError('fetch failed'));
	await loading;
	let snapshot = window.get();
	assert.ok(snapshot.fetchMoreError instanceof Error, 'the failure is in the snapshot');
	assert.equal(snapshot.fetchingMore, false);
	assert.deepEqual(rankOf(snapshot), ranks(30, 21));
	assert.equal(snapshot.hasMore, true);

	const retried = await fetchMore(harness, ranks(29, 11));
	assert.equal(retried.cursor_id, 'm30', 'the retry anchors where the failure did');
	snapshot = window.get();
	assert.equal(snapshot.fetchMoreError, undefined);
	assert.deepEqual(rankOf(snapshot), ranks(30, 11));
	harness.unsubscribe();
	window.destroy();
});

test('a head that slides past the first page drops retained pages instead of a gap', async () => {
	const harness = await openWindow(30);
	const { window } = harness;
	await fetchMore(harness, ranks(29, 11));

	// Nine arrivals: the head still holds the anchor row, so no gap.
	harness.live(ranks(39, 30));
	await flush();
	assert.deepEqual(rankOf(window.get()), ranks(39, 11));

	// The tenth pushes the anchor out of the head: rows 30.. are in no page
	// that starts at the head, so the window resets to the head.
	harness.live(ranks(40, 31));
	await flush();
	let snapshot = window.get();
	assert.deepEqual(rankOf(snapshot), ranks(40, 31));
	assert.equal(snapshot.hasMore, true);

	// Loading again anchors at the new head and stays contiguous.
	const anchored = await fetchMore(harness, ranks(39, 21));
	assert.equal(anchored.cursor_id, 'm40');
	snapshot = window.get();
	assert.deepEqual(rankOf(snapshot), ranks(40, 21));
	harness.unsubscribe();
	window.destroy();
});

test('a page size must fit two pages in the root max limit', () => {
	const { replica } = scripted();
	const operation = bindSveltekitOperation(replica, artifact);
	assert.throws(() => operation.useWindow({}, { pageSize: 51 }), RangeError);
	assert.throws(() => operation.useWindow({}, { pageSize: 0 }), RangeError);
	operation.useWindow({}, { pageSize: 50 }).destroy();
});
