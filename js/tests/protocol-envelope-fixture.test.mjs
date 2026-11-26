/**
 * The response envelope as the Rust server serializes it
 * (`tests/fixtures/protocol-envelope-v1.json`, written by
 * `src/graphql/protocol/envelope_fixture.rs`), read by this client's own
 * parser, replica and command runtime. A field renamed or added on either
 * side fails here or in the Rust drift check.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
	distributedLiveResumeExtensions,
	parseDistributedProtocolEnvelope,
	parseGraphqlResponseExtensions
} from '../dist/index.js';
import { createReplicaCommandRuntime } from '../dist/replica/index.js';
import { flush } from './fixtures/game-harness.mjs';
import { scripted, Todos } from './fixtures/replica-todos.mjs';

const fixture = JSON.parse(
	readFileSync(
		new URL('../../tests/fixtures/protocol-envelope-v1.json', import.meta.url),
		'utf8'
	)
);
const { scope, limits, operations, live, command } = fixture;

const Query = Object.freeze({
	...Todos,
	id: operations.query,
	protocol: Object.freeze({
		...Todos.protocol,
		schemaHash: scope.schemaHash,
		operation: operations.query
	}),
	live: Object.freeze({ ...Todos.live, id: operations.live })
});

/** Every member the client reads, by envelope object. */
const KNOWN = {
	envelope: [
		'protocolVersion',
		'schemaHash',
		'authorizationGeneration',
		'cacheScope',
		'generation',
		'operation',
		'command',
		'snapshot',
		'live',
		'trustedPresets'
	],
	command: [
		'commandId',
		'causationId',
		'state',
		'consistency',
		'projectionDisposition',
		'expects',
		'projection',
		'observations',
		'records'
	],
	snapshot: [
		'scopeToken',
		'recordsComplete',
		'indexesComparable',
		'records',
		'indexes',
		'observations'
	],
	live: ['mode', 'reset', 'cursors'],
	record: ['path', 'model', 'scopeToken', 'incarnation', 'revision', 'tombstone'],
	index: ['projection', 'scopeToken', 'position', 'resume'],
	cursor: ['projection', 'position', 'token'],
	observation: ['causationId', 'projection', 'model', 'scopeToken']
};

function assertKnown(kind, value, where) {
	for (const key of Object.keys(value)) {
		assert.ok(
			KNOWN[kind].includes(key),
			`${where}: the server sent ${kind}.${key}, which this client does not read`
		);
	}
}

function assertOnlyKnownMembers(envelope, where) {
	assertKnown('envelope', envelope, where);
	for (const cursor of envelope.live?.cursors ?? []) assertKnown('cursor', cursor, where);
	if (envelope.live !== undefined) assertKnown('live', envelope.live, where);
	const evidence = [envelope.snapshot, envelope.command].filter(Boolean);
	if (envelope.snapshot !== undefined) assertKnown('snapshot', envelope.snapshot, where);
	if (envelope.command !== undefined) assertKnown('command', envelope.command, where);
	for (const owner of evidence) {
		for (const record of owner.records ?? []) assertKnown('record', record, where);
		for (const observation of owner.observations ?? []) {
			assertKnown('observation', observation, where);
		}
		for (const index of owner.indexes ?? []) {
			assertKnown('index', index, where);
			if (index.resume !== undefined) assertKnown('cursor', index.resume, where);
		}
	}
}

const responses = Object.entries({
	query: fixture.query,
	...Object.fromEntries(Object.entries(live).map(([name, value]) => [`live.${name}`, value])),
	...Object.fromEntries(
		Object.entries(command.dispatch).map(([name, value]) => [`dispatch.${name}`, value])
	),
	...Object.fromEntries(
		Object.entries(command.status).map(([name, value]) => [`status.${name}`, value])
	)
});

test('every server response parses, and carries nothing the client does not read', () => {
	assert.equal(fixture.version, 1);
	for (const [name, response] of responses) {
		const wire = response.extensions.distributed;
		assertOnlyKnownMembers(wire, name);
		const parsed = parseGraphqlResponseExtensions(response.extensions).distributed;
		// Parsing adds the defaults of omitted empty lists and changes nothing else.
		assert.deepEqual(
			JSON.parse(JSON.stringify(parsed)),
			{
				...wire,
				trustedPresets: [],
				...(wire.command === undefined
					? {}
					: { command: { observations: [], records: [], ...wire.command } })
			},
			name
		);
		for (const member of ['protocolVersion', 'schemaHash', 'authorizationGeneration', 'cacheScope']) {
			assert.equal(parsed[member], scope[member], `${name}.${member}`);
		}
	}
});

test('query and live frames are admitted through the replica in server order', async () => {
	const { replica, scheduler, fetches, subscriptions } = scripted();
	const watch = replica.watch(Query, {}, { live: true });
	await flush();
	fetches[0].resolve(fixture.query);
	await flush();
	assert.deepEqual(watch.get().data, fixture.query.data);
	assert.equal(watch.get().complete, true);
	assert.deepEqual(replica.dehydrate().scope, scope);

	const observer = subscriptions[0].observer;
	for (const name of ['opening', 'delta', 'reset']) {
		observer.next(live[name]);
		assert.deepEqual(watch.get().data, live[name].data, name);
		assert.equal(watch.get().live, 'active', name);
		assert.deepEqual(watch.get().errors, [], name);
	}

	// The failure frame admits nothing, keeps the last data and reopens from
	// the last admitted cursors: a position past 2^53 survives as a string.
	observer.next(live.failure);
	assert.equal(watch.get().live, 'error');
	assert.deepEqual(
		watch.get().errors.map((error) => error.message),
		live.failure.errors.map((error) => error.message)
	);
	assert.deepEqual(watch.get().data, live.reset.data);
	assert.equal(subscriptions[0].closed, true);
	await scheduler.advance(1000);
	assert.equal(subscriptions.length, 2);
	const cursors = live.reset.extensions.distributed.live.cursors;
	assert.equal(cursors[0].position, '9007199254740993');
	assert.deepEqual(subscriptions[1].request.resume, cursors);
	watch.destroy();
});

test('a frame without a comparable position replaces the result and is not resumable', async () => {
	const { replica, scheduler, subscriptions } = scripted();
	const watch = replica.watch(Query, {}, { live: true });
	await flush();
	subscriptions[0].observer.next(live.unresumable);
	assert.deepEqual(watch.get().data, live.unresumable.data);
	assert.equal(watch.get().live, 'active');
	subscriptions[0].observer.error(Object.assign(new Error('socket lost'), { retryable: true }));
	await scheduler.advance(1000);
	assert.equal(subscriptions.length, 2);
	assert.equal(subscriptions[1].request.resume, undefined);
	watch.destroy();
});

test('the client accepts everything the server limits allow and no more evidence', () => {
	const base = fixture.query.extensions.distributed;
	const index = base.snapshot.indexes[0];
	const record = base.snapshot.records[0];
	const withSnapshot = (snapshot) =>
		parseDistributedProtocolEnvelope({ ...base, snapshot: { ...base.snapshot, ...snapshot } });
	const records = (count) =>
		Array.from({ length: count }, (_, at) => ({ ...record, path: ['todos', String(at)] }));
	const indexes = (count, resumable) =>
		Array.from({ length: count }, (_, at) => {
			const projection = `p${at}`;
			return {
				...index,
				projection,
				...(at < resumable ? { resume: { ...index.resume, projection } } : { resume: undefined })
			};
		});

	withSnapshot({ records: records(limits.evidenceItems) });
	assert.throws(() => withSnapshot({ records: records(limits.evidenceItems + 1) }));
	withSnapshot({ indexes: indexes(limits.evidenceItems, limits.liveResumeCursors) });
	assert.throws(() =>
		withSnapshot({ indexes: indexes(limits.evidenceItems, limits.liveResumeCursors + 1) })
	);

	const cursors = (count) =>
		indexes(count, count).map((entry) => entry.resume);
	distributedLiveResumeExtensions(cursors(limits.liveResumeCursors));
	assert.throws(() => distributedLiveResumeExtensions(cursors(limits.liveResumeCursors + 1)));

	// Tokens and projector names up to the server's own bounds are admitted.
	const token = 't'.repeat(limits.opaqueTokenBytes);
	const projection = 'p'.repeat(limits.liveResumeProjectionBytes);
	withSnapshot({
		scopeToken: token,
		indexes: [
			{
				projection,
				scopeToken: token,
				position: index.position,
				resume: { projection, position: index.position, token }
			}
		]
	});
	distributedLiveResumeExtensions([{ projection, position: '1', token }]);
});

const scalar = (name) =>
	Object.freeze({
		name,
		typeName: 'String',
		codec: 'string',
		nullable: false,
		list: false,
		itemNullable: false
	});
const protocol = (operation) =>
	Object.freeze({
		version: 1,
		schemaHash: scope.schemaHash,
		protocolHash: `sha256:${'c'.repeat(64)}`,
		surface: Query.protocol.surface,
		operation,
		trustedPresets: Object.freeze([])
	});
const STATUS = Object.freeze({
	name: 'Distributed_CommandStatus',
	document:
		'query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }',
	operationHash: operations.status,
	protocol: protocol(operations.status)
});

function commandArtifact(consistency) {
	const atomic = consistency === 'atomic';
	return Object.freeze({
		version: 2,
		name: command.name,
		mutationField: command.mutationField,
		document: `mutation Client_complete($commandId: ID!, $input: CompleteTodoInput!) { ${command.mutationField}(commandId: $commandId, input: $input) { id${atomic ? ' title' : ''} } }`,
		operationHash: operations.command,
		protocol: protocol(operations.command),
		input: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: 'CompleteTodoInput',
				fields: Object.freeze([scalar('id')])
			})
		}),
		output: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: atomic ? 'TodoView' : 'CompleteTodoPayload',
				fields: Object.freeze(atomic ? [scalar('id'), scalar('title')] : [scalar('id')])
			})
		}),
		consistency,
		...(atomic
			? {
					directProjection: Object.freeze({
						topology: Object.freeze({
							version: 1,
							name: 'todos-projector',
							digest: `sha256:${'11'.repeat(32)}`
						}),
						model: 'TodoView',
						identityFields: Object.freeze(['id'])
					})
				}
			: {}),
		revalidation: Object.freeze({
			version: 1,
			// Without a modeled projection only refetching shows the outcome.
			required: !atomic,
			dependencies: Object.freeze(['todos']),
			models: Object.freeze(['TodoView']),
			relationships: Object.freeze([])
		})
	});
}

/** A replica already in the fixture's scope, and one command over `transport`. */
function commands(consistency, transport) {
	const { replica } = scripted();
	replica.writeResult(Query, {}, fixture.query, 'network');
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) => Promise.resolve(transport.dispatch(request)),
			status: (request) => Promise.resolve(transport.status(request))
		},
		{ complete: commandArtifact(consistency) },
		{ status: STATUS }
	);
	const invoke = () =>
		runtime.commands.complete({ id: 'todo-1' }, { commandId: command.commandId });
	return { replica, runtime, invoke };
}

const http = (response) => ({ status: 200, ...response });
const consistencyOf = (response) => response.extensions.distributed.command.consistency;

for (const state of ['succeeded', 'succeeded_pending_projection', 'atomic']) {
	test(`a dispatched command answered "${state}" yields a receipt`, async () => {
		const response = command.dispatch[state];
		const { replica, runtime, invoke } = commands(consistencyOf(response), {
			dispatch(request) {
				assert.equal(request.operationHash, operations.command);
				assert.equal(request.commandId, command.commandId);
				return http(response);
			}
		});
		const receipt = await invoke();
		assert.equal(receipt.state, state);
		assert.equal(receipt.commandId, command.commandId);
		assert.equal(receipt.metadata.causationId, command.causationId);
		assert.deepEqual(receipt.result, response.data[command.mutationField]);
		if (state === 'atomic') {
			// The sealed row is in the cache at the receipt's own revision.
			replica.writeResult(Query, {}, fixture.query, 'network');
			assert.deepEqual(replica.read(Query, {}).data, {
				todos: [response.data[command.mutationField]]
			});
		}
		runtime.dispose();
	});
}

test('a rejected dispatch is a domain rejection, not a protocol failure', async () => {
	const { runtime, invoke } = commands('eventual', {
		dispatch: () => http(command.dispatch.rejected)
	});
	await assert.rejects(invoke(), (error) => {
		assert.equal(error.code, 'REPLICA_COMMAND_REJECTED');
		return true;
	});
	runtime.dispose();
});

for (const [state, response] of Object.entries(command.status)) {
	test(`command status "${state}" is accepted after an in-progress dispatch`, async () => {
		const consistency =
			response.extensions.distributed.command?.consistency ?? 'eventual';
		const { runtime, invoke } = commands(consistency, {
			dispatch: () => {
				const pending = structuredClone(command.dispatch.in_progress);
				pending.extensions.distributed.command.consistency = consistency;
				return http(pending);
			},
			status(request) {
				assert.equal(request.operationHash, operations.status);
				assert.equal(request.commandId, command.commandId);
				return http(response);
			}
		});
		let recovery;
		await assert.rejects(invoke(), (error) => {
			assert.equal(error.code, 'REPLICA_COMMAND_OUTCOME_PENDING');
			recovery = error.recovery;
			return true;
		});
		if (state === 'unknown') {
			// The server already acknowledged this command: `unknown` afterwards
			// would lose its receipt, so the client refuses it.
			await assert.rejects(recovery.status(), { code: 'REPLICA_COMMAND_PROTOCOL_INVALID' });
		} else {
			const status = await recovery.status();
			assert.equal(status.state, state);
			assert.equal(status.commandId, command.commandId);
			assert.equal(status.metadata?.state, response.extensions.distributed.command?.state);
		}
		runtime.dispose();
	});
}

test('command status "unknown" is accepted for a command the server never acknowledged', async () => {
	const { runtime, invoke } = commands('eventual', {
		dispatch: () => Promise.reject(new Error('connection reset')),
		status: () => http(command.status.unknown)
	});
	let recovery;
	await assert.rejects(invoke(), (error) => {
		recovery = error.recovery;
		return recovery !== undefined;
	});
	assert.deepEqual(await recovery.status(), {
		commandId: command.commandId,
		state: 'unknown'
	});
	runtime.dispose();
});
