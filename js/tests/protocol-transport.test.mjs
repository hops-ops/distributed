import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
	DISTRIBUTED_PROTOCOL_VERSION,
	DistributedProtocolError,
	parseDistributedProtocolEnvelope,
	parseGraphqlResponseExtensions,
	requestGraphql,
	subscribe
} from '@hops-ops/distributed';
import {
	COMMAND_CONSISTENCY,
	COMMAND_STATE,
	commandReceipt
} from './fixtures/command-protocol.mjs';

function distributedEnvelope() {
	return {
		protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
		schemaHash: 'sha256:schema-v2',
		authorizationGeneration: 'auth-1',
		cacheScope: 'opaque:principal-and-grants',
		operation: 'sha256:operation',
		command: commandReceipt({
			commandId: 'opaque-command-id',
			causationId: 'opaque-causation-id',
			state: COMMAND_STATE.PENDING_PROJECTION,
			consistency: COMMAND_CONSISTENCY.EVENTUAL,
			expects: [
				{
					projection: 'todos',
					model: 'TodoView',
					scopeToken: 'opaque:tenant-key-and-generation'
				}
			],
			observations: [
				{
					causationId: 'opaque-causation-id',
					projection: 'todos',
					model: 'TodoView',
					scopeToken: 'opaque:tenant-key-and-generation'
				}
			],
			records: [
				{
					model: 'TodoView',
					scopeToken: 'opaque:record-scope',
					incarnation: '18446744073709551614',
					revision: '18446744073709551615',
					tombstone: false
				}
			]
		}),
		snapshot: {
			scopeToken: 'opaque:query-snapshot',
			recordsComplete: true,
			indexesComparable: true,
			records: [
				{
					path: ['live'],
					model: 'TodoView',
					scopeToken: 'opaque:record-scope',
					incarnation: '18446744073709551614',
					revision: '18446744073709551615',
					tombstone: false
				}
			],
			indexes: [
				{
					projection: 'todos',
					scopeToken: 'opaque:index-scope',
					position: '18446744073709551615',
					resume: {
						projection: 'todos',
						position: '18446744073709551615',
						token: 'opaque:resume'
					}
				}
			],
			observations: []
		},
		live: {
			mode: "resumable",
			reset: false,
			cursors: [
				{
					projection: 'todos',
					position: '18446744073709551615',
					token: 'opaque:resume'
				}
			]
		},
		trustedPresets: [],
		futureMetadata: { retained: true }
	};
}

function responseExtensions() {
	return {
		traceId: 'trace-1',
		distributed: distributedEnvelope()
	};
}

test('protocol parser validates receipts while retaining future metadata opaquely', () => {
	assert.deepEqual(COMMAND_CONSISTENCY, {
		SUCCEEDED: 'succeeded',
		EVENTUAL: 'eventual',
		ATOMIC: 'atomic'
	});
	const extensions = parseGraphqlResponseExtensions(responseExtensions());
	assert.deepEqual(extensions, responseExtensions());
	assert.equal(
		typeof extensions.distributed.snapshot.records[0].incarnation,
		'string'
	);
	assert.equal(
		extensions.distributed.snapshot.records[0].revision,
		'18446744073709551615'
	);
	assert.equal(
		extensions.distributed.command.expects[0].scopeToken,
		'opaque:tenant-key-and-generation'
	);
	assert.equal(Object.isFrozen(extensions.distributed.command.expects), true);
	assert.equal(Object.isFrozen(extensions.distributed.command.records), true);
	assert.equal(Object.isFrozen(extensions.distributed.snapshot.indexes), true);
	assert.equal(Object.isFrozen(extensions.distributed.live.cursors), true);
	assert.equal(Object.isFrozen(extensions.distributed), true);

	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...distributedEnvelope(),
				protocolVersion: 3
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.code === 'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED' &&
			!error.message.includes('3')
	);
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...distributedEnvelope(),
				command: {
					...distributedEnvelope().command,
					expects: [
						{
							projection: 'todos',
							model: 'TodoView',
							scopeToken: 9007199254740992
						}
					]
				}
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
			error.path.endsWith('.scopeToken')
	);
	for (const [field, value] of [
		['state', 'accepted_pending_projection'],
		['consistency', 'fact']
	]) {
		assert.throws(
			() =>
				parseDistributedProtocolEnvelope({
					...distributedEnvelope(),
					command: {
						...distributedEnvelope().command,
						[field]: value
					}
				}),
			(error) =>
				error instanceof DistributedProtocolError &&
				error.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
				error.path.endsWith(`.${field}`)
		);
	}
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...distributedEnvelope(),
				snapshot: {
					...distributedEnvelope().snapshot,
					indexes: [
						{
							...distributedEnvelope().snapshot.indexes[0],
							position: '18446744073709551616'
						}
					]
				}
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path.endsWith('.position')
	);

	const {
		recordsComplete: _recordsComplete,
		indexesComparable: _indexesComparable,
		...legacySnapshot
	} = distributedEnvelope().snapshot;
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...distributedEnvelope(),
				snapshot: { ...legacySnapshot, complete: true }
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path ===
				'extensions.distributed.snapshot.recordsComplete'
	);
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...distributedEnvelope(),
				snapshot: {
					...distributedEnvelope().snapshot,
					indexesComparable: undefined
				}
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path ===
				'extensions.distributed.snapshot.indexesComparable'
	);
});

test('protocol parser requires exact live snapshot alignment', () => {
	const envelope = distributedEnvelope();
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...envelope,
				snapshot: undefined,
				live: { mode: "snapshot", reset: true, cursors: [] }
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path === 'extensions.distributed.snapshot'
	);
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...envelope,
				snapshot: {
					...envelope.snapshot,
					indexesComparable: false,
					indexes: [],
					observations: []
				},
				live: { mode: "resumable", reset: true, cursors: [] }
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path ===
				'extensions.distributed.snapshot.indexesComparable'
	);

	const mismatches = [
		[
			'empty supported cursor inventory',
			{
				...envelope,
				snapshot: { ...envelope.snapshot, indexes: [] },
				live: { ...envelope.live, cursors: [] }
			}
		],
		[
			'cursor count',
			{
				...envelope,
				live: { ...envelope.live, cursors: [] }
			}
		],
		[
			'cursor projection',
			{
				...envelope,
				live: {
					...envelope.live,
					cursors: [
						{ ...envelope.live.cursors[0], projection: 'other-projector' }
					]
				}
			}
		],
		[
			'cursor position',
			{
				...envelope,
				live: {
					...envelope.live,
					cursors: [{ ...envelope.live.cursors[0], position: '1' }]
				}
			}
		],
		[
			'cursor token',
			{
				...envelope,
				live: {
					...envelope.live,
					cursors: [{ ...envelope.live.cursors[0], token: 'opaque:other' }]
				}
			}
		],
		[
			'missing snapshot resume',
			{
				...envelope,
				snapshot: {
					...envelope.snapshot,
					indexes: envelope.snapshot.indexes.map(
						({ resume: _resume, ...index }) => index
					)
				}
			}
		]
	];
	for (const [description, invalidEnvelope] of mismatches) {
		assert.throws(
			() => parseDistributedProtocolEnvelope(invalidEnvelope),
			(error) =>
				error instanceof DistributedProtocolError &&
				error.path === 'extensions.distributed.live.cursors',
			description
		);
	}
});

test('snapshot delivery requires an explicit cursorless reset and fails closed on old modes', () => {
	const base = distributedEnvelope();
	const envelope = {
		...base,
		snapshot: { ...base.snapshot, indexesComparable: false, indexes: [], observations: [] },
		live: { mode: 'snapshot', reset: true, cursors: [] }
	};
	assert.equal(parseDistributedProtocolEnvelope(envelope).live.mode, 'snapshot');
	for (const live of [
		{ supported: false, reset: true, cursors: [] },
		{ ...envelope.live, supported: false },
		{ ...envelope.live, mode: 'unknown' },
		{ ...envelope.live, reset: false },
		{ ...envelope.live, cursors: base.live.cursors }
	]) {
		assert.throws(() => parseDistributedProtocolEnvelope({ ...envelope, live }), DistributedProtocolError);
	}
	assert.throws(() => parseDistributedProtocolEnvelope({
		...envelope, snapshot: base.snapshot
	}), DistributedProtocolError);
});

test('protocol parser accepts 64 resume cursors and rejects 65', () => {
	const cursors = Array.from({ length: 65 }, (_, index) => ({
		projection: `projection-${index}`,
		position: String(index + 1),
		token: `opaque:resume-${index}`
	}));
	const indexes = cursors.map((cursor, index) => ({
		projection: cursor.projection,
		scopeToken: `opaque:index-${index}`,
		position: cursor.position,
		resume: cursor
	}));
	const envelope = distributedEnvelope();
	const accepted = parseDistributedProtocolEnvelope({
		...envelope,
		snapshot: { ...envelope.snapshot, indexes: indexes.slice(0, 64) },
		live: { mode: "resumable", reset: false, cursors: cursors.slice(0, 64) }
	});
	assert.equal(accepted.live.cursors.length, 64);
	assert.equal(accepted.snapshot.indexes.length, 64);

	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...envelope,
				live: { mode: "resumable", reset: false, cursors }
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path === 'extensions.distributed.live.cursors'
	);
	assert.throws(
		() =>
			parseDistributedProtocolEnvelope({
				...envelope,
				snapshot: { ...envelope.snapshot, indexes },
				live: { mode: "snapshot", reset: true, cursors: [] }
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path === 'extensions.distributed.snapshot.indexes'
	);
});

test('HTTP preserves a valid Distributed envelope without adding metadata to data', async () => {
	const extensions = responseExtensions();
	const result = await requestGraphql(
		'/graphql',
		'mutation Change { change { ok } }',
		{},
		{},
		{
			fetch: async () => ({
				status: 202,
				json: async () => ({
					data: { change: { ok: true } },
					extensions
				})
			})
		}
	);

	assert.deepEqual(result, {
		data: { change: { ok: true } },
		errors: undefined,
		extensions,
		status: 202
	});
	assert.deepEqual(result.data.change, { ok: true });
	assert.equal('distributed' in result.data.change, false);
});

test('HTTP rejects malformed or incompatible Distributed envelopes fail closed', async () => {
	const incompatible = await requestGraphql(
		'/graphql',
		'query Read { item { id } }',
		{},
		{},
		{
			fetch: async () => ({
				status: 200,
				json: async () => ({
					data: { item: { id: 'must-not-be-trusted' } },
					extensions: {
						distributed: {
							...distributedEnvelope(),
							protocolVersion: 99
						}
					}
				})
			})
		}
	);
	assert.equal(incompatible.data, undefined);
	assert.equal(
		incompatible.errors[0].extensions.code,
		'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED'
	);
	assert.equal('extensions' in incompatible, false);

	const malformed = await requestGraphql(
		'/graphql',
		'query Read { item { id } }',
		{},
		{},
		{
			fetch: async () => ({
				status: 200,
				json: async () => ({
					data: { item: { id: 'must-not-be-trusted' } },
					extensions: {
						distributed: {
							...distributedEnvelope(),
							cacheScope: { tenant: 'hidden' }
						}
					}
				})
			})
		}
	);
	assert.equal(malformed.data, undefined);
	assert.equal(
		malformed.errors[0].extensions.code,
		'DISTRIBUTED_PROTOCOL_INVALID'
	);
	assert.match(malformed.errors[0].message, /cacheScope$/);
	assert.equal(malformed.errors[0].message.includes('hidden'), false);
});

class FakeWebSocket {
	static OPEN = 1;
	static instances = [];

	constructor(url, protocol) {
		this.url = url;
		this.protocol = protocol;
		this.readyState = 0;
		this.sent = [];
		this.closeCount = 0;
		FakeWebSocket.instances.push(this);
	}

	send(value) {
		this.sent.push(JSON.parse(value));
	}

	close() {
		this.closeCount += 1;
		this.readyState = 3;
	}

	open() {
		this.readyState = FakeWebSocket.OPEN;
		this.onopen?.({});
	}

	message(message) {
		this.onmessage?.({ data: JSON.stringify(message) });
	}
}

test('GraphQL-WS preserves valid envelopes and terminates on incompatible frames', () => {
	FakeWebSocket.instances.length = 0;
	const next = [];
	const errors = [];
	const stop = subscribe(
		'subscription Live { live { id } }',
		{},
		{
			onNext: (result) => next.push(result),
			onError: (error) => errors.push(error)
		},
		{
			webSocket: FakeWebSocket,
			httpUrl: '/graphql',
			resume: distributedEnvelope().live.cursors
		}
	);
	const socket = FakeWebSocket.instances[0];
	socket.open();
	socket.message({ type: 'connection_ack' });
	assert.deepEqual(socket.sent[1].payload.extensions, {
		distributed: {
			resume: {
				cursors: distributedEnvelope().live.cursors
			}
		}
	});
	socket.message({
		type: 'next',
		id: '1',
		payload: {
			data: { live: { id: 'row-1' } },
			extensions: responseExtensions()
		}
	});
	assert.deepEqual(next[0], {
		data: { live: { id: 'row-1' } },
		extensions: responseExtensions()
	});
	assert.deepEqual(errors, []);
	stop();

	const rejectedNext = [];
	const rejectedErrors = [];
	subscribe(
		'subscription Live { live { id } }',
		{},
		{
			onNext: (result) => rejectedNext.push(result),
			onError: (error) => rejectedErrors.push(error)
		},
		{ webSocket: FakeWebSocket, httpUrl: '/graphql' }
	);
	const rejectedSocket = FakeWebSocket.instances[1];
	rejectedSocket.open();
	rejectedSocket.message({ type: 'connection_ack' });
	rejectedSocket.message({
		type: 'next',
		id: '1',
		payload: {
			data: { live: { id: 'must-not-be-trusted' } },
			extensions: {
				distributed: {
					...distributedEnvelope(),
					protocolVersion: 7
				}
			}
		}
	});

	assert.deepEqual(rejectedNext, []);
	assert.equal(rejectedErrors.length, 1);
	assert.equal(
		rejectedErrors[0].code,
		'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED'
	);
	assert.deepEqual(rejectedSocket.sent.at(-1), {
		type: 'complete',
		id: '1'
	});
	assert.equal(rejectedSocket.closeCount, 1);
});
