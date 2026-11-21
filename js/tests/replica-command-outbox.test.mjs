import assert from 'node:assert/strict';
import test from 'node:test';

import {
	createReplicaIndexedDbPersistence,
	REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED
} from '../dist/replica/persistence.js';
import { classifyOutboxResult } from '../dist/replica/persistence/outbox.js';
import { FakeIndexedDb } from './fixtures/fake-indexeddb.mjs';
import { COMMAND_CONSISTENCY, commandReceipt } from './fixtures/command-protocol.mjs';

const DATABASE = 'outbox-test';
const OUTBOX = 'command-outbox';

const scopeA = Object.freeze({
	protocolVersion: 1,
	schemaHash: 'sha256:schema',
	authorizationGeneration: 'auth-1',
	cacheScope: 'scope:alice'
});
const scopeB = Object.freeze({ ...scopeA, cacheScope: 'scope:bob' });

const POLICY = Object.freeze({
	commands: {
		addTodo: { durable: true, sensitive: false },
		changePassword: { durable: true, sensitive: true },
		ephemeral: { durable: false, sensitive: false }
	}
});

function request(commandName, commandId, variables = {}) {
	return Object.freeze({
		operation: 'mutation',
		commandName,
		commandId,
		mutationField: commandName,
		document: `mutation ${commandName} { ${commandName} }`,
		operationHash: 'sha256:op',
		variables,
		extensions: { distributed: { client: { schemaHash: 'sha256:schema' } } }
	});
}

function accepted(req, state = 'succeeded_pending_projection') {
	return {
		status: 200,
		data: { [req.mutationField]: { ok: true } },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: 'sha256:schema',
				authorizationGeneration: 'auth-1',
				cacheScope: 'scope:alice',
				operation: req.operationHash,
				command: commandReceipt({
					commandId: req.commandId,
					causationId: `cause:${req.commandId}`,
					state,
					consistency: COMMAND_CONSISTENCY.EVENTUAL
				})
			}
		}
	};
}

function rejected(message = 'title taken') {
	return {
		status: 200,
		data: null,
		errors: [{ message, extensions: { code: 'REJECTED' } }]
	};
}

/** A scripted network: `online` toggles, `answer(request)` decides. */
function network(answer) {
	const sent = [];
	const net = {
		online: true,
		sent,
		transport: {
			async dispatch(req) {
				sent.push(req.commandId);
				if (!net.online) throw new TypeError('Failed to fetch');
				return answer(req);
			}
		}
	};
	return net;
}

function outboxFor(factory, net, options = {}) {
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: factory,
		databaseName: DATABASE
	});
	const settled = [];
	const outbox = persistence.commandOutbox({
		transport: net.transport,
		scope: () => options.scope ?? scopeA,
		policy: POLICY,
		isOnline: () => net.online,
		onSettled: (s) => settled.push(s)
	});
	return { persistence, outbox, settled };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('offline outbox support is advertised by IndexedDB persistence', () => {
	const persistence = createReplicaIndexedDbPersistence({
		indexedDB: new FakeIndexedDb(),
		databaseName: DATABASE
	});
	assert.equal(REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED, true);
	assert.equal(persistence.supportsOfflineCommandOutbox, true);
	assert.equal(typeof persistence.commandOutbox, 'function');
	persistence.close();
});

test('commands persist before send and drain in order after a reload, reusing ids', async () => {
	const factory = new FakeIndexedDb();
	const net = network((req) => (req.commandId === 'cmd-2' ? rejected() : accepted(req)));
	net.online = false;
	const first = outboxFor(factory, net);

	// Offline: durable commands are written, nothing reaches the network.
	const live1 = first.outbox.transport.dispatch(request('addTodo', 'cmd-1', { title: 'a' }));
	const live2 = first.outbox.transport.dispatch(request('addTodo', 'cmd-2', { title: 'b' }));
	const live3 = first.outbox.transport.dispatch(request('addTodo', 'cmd-3', { title: 'c' }));
	await tick();
	assert.deepEqual(net.sent, []);
	const stored = [...factory.store(DATABASE, OUTBOX).values()].sort((a, b) => a.order - b.order);
	assert.deepEqual(stored.map((e) => e.commandId), ['cmd-1', 'cmd-2', 'cmd-3']);
	assert.deepEqual(stored[0].request.variables, { title: 'a' });
	assert.equal('signal' in stored[0].request, false);
	assert.deepEqual((await first.outbox.pending()).map((p) => p.commandId), ['cmd-1', 'cmd-2', 'cmd-3']);

	// Non-durable and sensitive commands are never written; they go straight
	// to the (offline) network and fail there.
	await assert.rejects(first.outbox.transport.dispatch(request('changePassword', 'pw-1', { secret: 'x' })));
	await assert.rejects(first.outbox.transport.dispatch(request('ephemeral', 'e-1')));
	await assert.rejects(first.outbox.transport.dispatch(request('unlisted', 'u-1')));
	assert.equal(factory.store(DATABASE, OUTBOX).size, 3);
	assert.equal(
		JSON.stringify([...factory.store(DATABASE, OUTBOX).values()]).includes('secret'),
		false
	);

	// "Reload": the live callers are gone; a fresh instance drains the queue.
	void live1;
	void live2;
	void live3;
	first.persistence.close();
	net.sent.length = 0;
	net.online = true;
	const second = outboxFor(factory, net);
	const report = await second.outbox.drain();
	assert.deepEqual(net.sent, ['cmd-1', 'cmd-2', 'cmd-3'], 'same ids, enqueue order');
	assert.deepEqual(
		report.settled.map((s) => [s.commandId, s.outcome, s.live]),
		[
			['cmd-1', 'accepted', false],
			['cmd-2', 'rejected', false],
			['cmd-3', 'accepted', false]
		]
	);
	assert.equal(report.remaining, 0);
	assert.deepEqual(second.settled.map((s) => s.outcome), ['accepted', 'rejected', 'accepted']);
	assert.equal(factory.store(DATABASE, OUTBOX).size, 0);
	second.persistence.close();
});

test('a live caller waits through an outage and receives the deduplicated result', async () => {
	const factory = new FakeIndexedDb();
	const processed = new Map();
	// The server dedupes on command id: a resend returns the first receipt.
	const net = network((req) => {
		if (!processed.has(req.commandId)) processed.set(req.commandId, accepted(req));
		return processed.get(req.commandId);
	});
	const { outbox, persistence } = outboxFor(factory, net);

	// Online but the request dies in flight (server may have processed it).
	net.online = true;
	const original = net.transport.dispatch;
	let failNext = true;
	net.transport.dispatch = async (req) => {
		const result = await original(req);
		if (failNext) {
			failNext = false;
			throw new TypeError('connection reset after send');
		}
		return result;
	};
	let resolved;
	const live = outbox.transport
		.dispatch(request('addTodo', 'cmd-9'))
		.then((r) => (resolved = r));
	await tick();
	await tick();
	assert.equal(resolved, undefined, 'outcome unknown: caller keeps waiting');
	const [entry] = factory.store(DATABASE, OUTBOX).values();
	assert.equal(entry.attempts, 1);

	// Reconnect: drain resends the same id, the server answers from its receipt.
	const report = await outbox.drain();
	await live;
	assert.deepEqual(net.sent, ['cmd-9', 'cmd-9']);
	assert.equal(processed.size, 1);
	assert.equal(resolved.extensions.distributed.command.commandId, 'cmd-9');
	assert.equal(report.settled[0].live, true);
	assert.equal(report.settled[0].attempts, 2);
	persistence.close();
});

test('retryable responses keep order; aborting a never-sent command cancels it', async () => {
	const factory = new FakeIndexedDb();
	let busy = true;
	const net = network((req) =>
		busy ? { status: 503, data: null, errors: [{ message: 'busy' }] } : accepted(req)
	);
	const { outbox, persistence } = outboxFor(factory, net);
	const a = outbox.transport.dispatch(request('addTodo', 'a'));
	await tick();
	await tick();
	assert.deepEqual(net.sent, ['a']);
	net.online = false;
	const controller = new AbortController();
	const b = outbox.transport.dispatch({ ...request('addTodo', 'b'), signal: controller.signal });
	await tick();
	controller.abort(new Error('user cancelled'));
	await assert.rejects(b, /user cancelled/);
	assert.deepEqual((await outbox.pending()).map((p) => p.commandId), ['a']);

	net.online = true;
	busy = false;
	const report = await outbox.drain();
	assert.deepEqual(report.settled.map((s) => s.commandId), ['a']);
	assert.equal((await a).status, 200);
	assert.deepEqual(net.sent, ['a', 'a']);
	persistence.close();
});

test('entries are bound to their scope and can be discarded on sign-out', async () => {
	const factory = new FakeIndexedDb();
	const net = network((req) => accepted(req));
	net.online = false;
	const alice = outboxFor(factory, net, { scope: scopeA });
	void alice.outbox.transport.dispatch(request('addTodo', 'alice-1'));
	await tick();
	alice.persistence.close();

	net.online = true;
	const bob = outboxFor(factory, net, { scope: scopeB });
	const report = await bob.outbox.drain();
	assert.deepEqual(report.settled, [], 'never replayed for another principal');
	assert.deepEqual(net.sent, []);
	await bob.outbox.discardScope(scopeA);
	assert.equal(factory.store(DATABASE, OUTBOX).size, 0);
	bob.persistence.close();
});

test('result classification', () => {
	const req = request('addTodo', 'x');
	assert.equal(classifyOutboxResult(accepted(req)), 'accepted');
	assert.equal(classifyOutboxResult(accepted(req, 'in_progress')), 'accepted');
	assert.equal(classifyOutboxResult(rejected()), 'rejected');
	assert.equal(classifyOutboxResult({ status: 502, data: null }), 'retry');
	assert.equal(classifyOutboxResult({ status: 429, data: null }), 'retry');
	assert.equal(
		classifyOutboxResult({ status: 503, data: null, errors: [{ message: 'r', extensions: { code: 'APPLICATION_RELOADING' } }] }),
		'retry'
	);
	assert.equal(classifyOutboxResult({ status: 400, data: null, errors: [{ message: 'bad' }] }), 'failed');
	assert.equal(classifyOutboxResult({ status: 200, extensions: { distributed: 'garbage' } }), 'failed');
});
