import assert from 'node:assert/strict';
import test from 'node:test';

import { createReplicaGraphqlTransport } from '../dist/replica/index.js';
import {
	createDistributedLeaderboardStore,
	loadDistributedLeaderboard
} from '../dist/sveltekit/index.js';

const op = Object.freeze({
	leaderboard: 'xp',
	operation: 'top',
	document: 'query XpLeaderboard($first: Int) { xpLeaderboard(first: $first) { rank member score } }',
	liveDocument:
		'subscription XpLeaderboard_Live($first: Int) { xpLeaderboard(first: $first) { rank member score } }'
});

function jsonResponse(body, status = 200) {
	return { status, statusText: 'OK', text: async () => JSON.stringify(body) };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('replica-free documents share the transport endpoint and credential', async () => {
	const calls = [];
	const transport = createReplicaGraphqlTransport({
		getUrl: () => 'http://api.test/graphql',
		getAuth: async () => ({ accessToken: 'token-1' }),
		fetch: async (url, init) => {
			calls.push({ url, init, body: JSON.parse(init.body) });
			return jsonResponse({ data: { xpLeaderboard: [{ rank: 0, member: 'ana', score: 10 }] } });
		}
	});
	const result = await transport.query(op.document, { first: 5 });
	assert.deepEqual(result.data.xpLeaderboard[0].member, 'ana');
	assert.equal(calls[0].url, 'http://api.test/graphql');
	assert.equal(calls[0].init.headers.authorization ?? calls[0].init.headers.Authorization, 'Bearer token-1');
	assert.deepEqual(calls[0].body.variables, { first: 5 });

	const loaded = await loadDistributedLeaderboard(op, { first: 5 }, {
		url: 'http://api.test/graphql',
		auth: { accessToken: 'token-1' },
		fetch: async () => jsonResponse({ data: { xpLeaderboard: [] } })
	});
	assert.deepEqual(loaded, { xpLeaderboard: [] });
	await assert.rejects(
		loadDistributedLeaderboard(op, {}, {
			url: 'http://api.test/graphql',
			auth: {},
			fetch: async () => jsonResponse({ errors: [{ message: 'FORBIDDEN' }] })
		}),
		/FORBIDDEN/
	);
});

test('a leaderboard store starts from the server result and follows live pushes', async () => {
	let observer;
	let closed = 0;
	const transport = {
		query: async () => {
			throw new Error('initial data was given; no query expected');
		},
		live(document, variables, next) {
			assert.equal(document, op.liveDocument);
			assert.deepEqual(variables, { first: 2 });
			observer = next;
			return () => {
				closed += 1;
			};
		}
	};
	const initial = { xpLeaderboard: [{ rank: 0, member: 'ana', score: 10 }] };
	const store = createDistributedLeaderboardStore(transport, op, { first: 2 }, { initial, browser: true });
	const seen = [];
	const unsubscribe = store.subscribe((value) => seen.push(value));
	assert.equal(seen.at(-1).data, initial);
	assert.equal(seen.at(-1).live, 'connecting');
	observer.next({ data: { xpLeaderboard: [{ rank: 0, member: 'bob', score: 30 }] } });
	assert.equal(seen.at(-1).data.xpLeaderboard[0].member, 'bob');
	assert.equal(seen.at(-1).live, 'active');
	observer.error(new Error('socket closed'));
	assert.equal(seen.at(-1).live, 'error');
	unsubscribe();
	assert.equal(closed, 1);
});

test('without initial data the browser store queries; on the server it stays idle', async () => {
	let queries = 0;
	const transport = {
		query: async () => {
			queries += 1;
			return { data: { xpLeaderboard: [{ rank: 0, member: 'cat', score: 5 }] } };
		},
		live: () => () => undefined
	};
	const server = createDistributedLeaderboardStore(transport, op, {}, { browser: false });
	const serverValues = [];
	server.subscribe((value) => serverValues.push(value))();
	assert.equal(queries, 0);
	assert.equal(serverValues[0].data, undefined);

	const browser = createDistributedLeaderboardStore(transport, op, {}, { browser: true, live: false });
	const values = [];
	const stop = browser.subscribe((value) => values.push(value));
	await tick();
	assert.equal(queries, 1);
	assert.equal(values.at(-1).data.xpLeaderboard[0].member, 'cat');
	assert.equal(values.at(-1).live, 'off');
	stop();
});
