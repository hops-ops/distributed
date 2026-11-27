import assert from 'node:assert/strict';
import test from 'node:test';

import {
	DistributedProtocolError,
	parseDistributedProtocolEnvelope
} from '../dist/index.js';
import { matchReplicaTrustedPresetInventory } from '../dist/replica/commands/presets.js';
import { prepareReplicaCommandWithTrustedPresets } from '../dist/replica/commands/prepare.js';
import {
	prepareReplicaCommand,
	ReplicaCommandContractError
} from '../dist/replica/index.js';
import { COMMAND_CONSISTENCY } from './fixtures/command-protocol.mjs';

const HASH_A = `sha256:${'a'.repeat(64)}`;
const HASH_B = `sha256:${'b'.repeat(64)}`;
const HASH_C = `sha256:${'c'.repeat(64)}`;
const COMMAND_ID = '018f47de-3d2a-7abc-8abc-0123456789ab';

function envelope(overrides = {}) {
	return {
		protocolVersion: 1,
		schemaHash: HASH_A,
		authorizationGeneration: 'auth-1',
		cacheScope: 'opaque:scope',
		...overrides
	};
}

function trusted(name) {
	return { kind: 'trusted_preset', name, codec: 'string' };
}

function presetArtifact(overrides = {}) {
	const event = {
		id: 'todo-assigned',
		name: 'todo.assigned',
		version: 1
	};
	return {
		version: 2,
		name: 'todo.assign',
		mutationField: 'assignTodo',
		document:
			'mutation Client_assignTodo($commandId: ID!) { assignTodo(commandId: $commandId) }',
		operationHash: HASH_A,
		protocol: {
			version: 1,
			schemaHash: HASH_B,
			protocolHash: HASH_C,
			surface: { kind: 'role', name: 'user' },
			operation: HASH_A,
			trustedPresets: [
				{ name: 'x-default-status', codec: 'string' },
				{ name: 'x-tenant', codec: 'string' }
			]
		},
		input: { kind: 'none' },
		output: {
			kind: 'object',
			definition: {
				name: 'AssignTodoResult',
				fields: [
					{
						name: 'succeeded',
						typeName: 'Boolean',
						nullable: false,
						list: false,
						itemNullable: false,
						codec: 'boolean'
					}
				]
			}
		},
		consistency: COMMAND_CONSISTENCY.SUCCEEDED,
		projection: {
			version: 2,
			deltaWireVersion: 1,
			projectionProgramVersion: 2,
			operationSemanticsVersion: 1,
			projections: [
				{
					programId: `pp1:sha256:${'1'.repeat(64)}`,
					bindingId: `pb1:sha256:${'2'.repeat(64)}`,
					programIrVersion: 1,
					operationSemanticsVersion: 1
				}
			],
			eventSet: [event],
			capabilities: {
				version: 1,
				arms: [
					{
						event,
						projection_ref: 0,
						arm: 'todo_assigned',
						partition: { kind: 'unit' },
						mutations: [
							{
								kind: 'record',
								model: 'Todo',
								key: ['tenantId'],
								fields: ['status'],
								replace: [],
								upsert: false,
								patch: true,
								delete: false
							}
						]
					}
				]
			},
			preview: {
				version: 1,
				occurrences: [{ ordinal: 0, event }],
				operations: [
					{
						occurrence_ordinal: 0,
						projection_refs: [0],
						mutation: {
							op: 'patch',
							scope: {
								partition: { kind: 'unit' },
								model: 'Todo',
								key: [
									{
										ordinal: 0,
										field: 'tenantId',
										value: trusted('x-tenant')
									}
								]
							},
							set: [
								{
									field: 'status',
									value: trusted('x-default-status')
								}
							],
							unset: [],
							if_present: true
						}
					}
				],
				recoveries: []
			},
			fallback: 'revalidate'
		},
		trustedPresets: [
			{ name: 'x-default-status', codec: 'string' },
			{ name: 'x-tenant', codec: 'string' }
		],
		revalidation: {
			version: 1,
			required: true,
			dependencies: ['todo_rows'],
			models: ['Todo'],
			relationships: []
		},
		...overrides
	};
}

test('protocol parses every trusted-preset codec into a deep immutable inventory', () => {
	const callerJson = {
		nested: [{ enabled: true }],
		safe: 9_007_199_254_740_991
	};
	const parsed = parseDistributedProtocolEnvelope(
		envelope({
			trustedPresets: [
				{ name: 'string', codec: 'string', value: 'tenant-1' },
				{
					name: 'timestamp',
					codec: 'string_unvalidated_timestamp',
					value: '2026-07-23T00:00:00Z'
				},
				{ name: 'bytes', codec: 'base64', value: 'dGVuYW50LTE=' },
				{ name: 'bool', codec: 'boolean', value: true },
				{ name: 'int', codec: 'int32', value: -2_147_483_648 },
				{ name: 'float', codec: 'float64', value: 1.25 },
				{
					name: 'bigint',
					codec: 'json_number_precision_limited',
					value: 9_007_199_254_740_991
				},
				{ name: 'json', codec: 'json', value: callerJson }
			]
		})
	);

	callerJson.nested[0].enabled = false;
	assert.equal(parsed.trustedPresets.length, 8);
	assert.deepEqual(parsed.trustedPresets[7].value, {
		nested: [{ enabled: true }],
		safe: 9_007_199_254_740_991
	});
	assert.equal(Object.isFrozen(parsed.trustedPresets), true);
	assert.equal(Object.isFrozen(parsed.trustedPresets[0]), true);
	assert.equal(Object.isFrozen(parsed.trustedPresets[7].value), true);
	assert.equal(Object.isFrozen(parsed.trustedPresets[7].value.nested), true);
	assert.equal(
		Object.isFrozen(parsed.trustedPresets[7].value.nested[0]),
		true
	);

	const omitted = parseDistributedProtocolEnvelope(envelope());
	assert.deepEqual(omitted.trustedPresets, []);
	assert.equal(Object.isFrozen(omitted.trustedPresets), true);
});

test('protocol rejects duplicate, unsupported, and codec-invalid trusted presets', () => {
	const rejected = [
		[
			[
				{ name: 'same', codec: 'string', value: 'first' },
				{ name: 'same', codec: 'string', value: 'second' }
			],
			'.name'
		],
		[[{ name: 'tenant', codec: 'uuid', value: 'tenant-1' }], '.codec'],
		[[{ name: ' tenant', codec: 'string', value: 'tenant-1' }], '.name'],
		[[{ name: 'bytes', codec: 'base64', value: 'not base64' }], '.value'],
		[[{ name: 'flag', codec: 'boolean', value: 'true' }], '.value'],
		[[{ name: 'count', codec: 'int32', value: 2_147_483_648 }], '.value'],
		[
			[
				{
					name: 'count',
					codec: 'json_number_precision_limited',
					value: 9_007_199_254_740_992
				}
			],
			'.value'
		],
		[[{ name: 'float', codec: 'float64', value: Infinity }], '.value'],
		[[{ name: 'json', codec: 'json', value: { invalid: undefined } }], '.invalid']
	];

	for (const [trustedPresets, suffix] of rejected) {
		assert.throws(
			() =>
				parseDistributedProtocolEnvelope(
					envelope({ trustedPresets })
				),
			(error) =>
				error instanceof DistributedProtocolError &&
				error.code === 'DISTRIBUTED_PROTOCOL_INVALID' &&
				error.path.endsWith(suffix)
		);
	}
});

test('exact inventory matching rejects missing, extra, and codec drift', () => {
	const expected = [
		{ name: 'x-default-status', codec: 'string' },
		{ name: 'x-tenant', codec: 'string' }
	];
	const authoritative = parseDistributedProtocolEnvelope(
		envelope({
			trustedPresets: [
				{ name: 'x-tenant', codec: 'string', value: 'tenant-1' },
				{
					name: 'x-default-status',
					codec: 'string',
					value: 'assigned'
				}
			]
		})
	).trustedPresets;
	const matched = matchReplicaTrustedPresetInventory(expected, authoritative);

	assert.equal(matched.resolve('x-tenant'), 'tenant-1');
	assert.equal(matched.resolve('x-default-status'), 'assigned');
	assert.equal(Object.isFrozen(matched), true);
	assert.equal(Object.isFrozen(matched.descriptors), true);
	assert.equal(Object.isFrozen(matched.values), true);

	for (const candidate of [
		authoritative.slice(0, 1),
		[
			...authoritative,
			{ name: 'x-unrelated', codec: 'string', value: 'other' }
		],
		[
			{ name: 'x-tenant', codec: 'json', value: 'tenant-1' },
			authoritative[1]
		]
	]) {
		assert.throws(
			() => matchReplicaTrustedPresetInventory(expected, candidate),
			(error) =>
				error instanceof ReplicaCommandContractError &&
				error.code === 'REPLICA_COMMAND_TRUSTED_PRESET_MISMATCH'
		);
	}
});

test('replica-bound preparation resolves only exact command descriptors while standalone stays closed', () => {
	const artifact = presetArtifact();
	const authoritative = parseDistributedProtocolEnvelope(
		envelope({
			trustedPresets: [
				{
					name: 'x-default-status',
					codec: 'string',
					value: 'assigned'
				},
				{ name: 'x-tenant', codec: 'string', value: 'tenant-1' },
				{
					name: 'x-other-command',
					codec: 'boolean',
					value: true
				}
			]
		})
	).trustedPresets;

	assert.throws(
		() =>
			prepareReplicaCommand(artifact, undefined, {
				commandId: COMMAND_ID
			}),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID' &&
			error.path.endsWith('.value')
	);

	const prepared = prepareReplicaCommandWithTrustedPresets(
		artifact,
		undefined,
		authoritative,
		{ commandId: COMMAND_ID }
	);
	assert.deepEqual(prepared.optimistic.operations, [
		{
			kind: 'patch',
			scope: {
				model: 'Todo',
				key: [{ field: 'tenantId', value: 'tenant-1' }]
			},
			fields: { status: 'assigned' },
			unset: [],
			ifPresent: true
		}
	]);

	for (const malformedArtifact of [
		presetArtifact({
			trustedPresets: [
				{ name: 'x-tenant', codec: 'string' },
				{ name: 'x-tenant', codec: 'string' }
			]
		}),
		presetArtifact({
			trustedPresets: [
				{ name: 'x-default-status', codec: 'uuid' },
				{ name: 'x-tenant', codec: 'string' }
			]
		}),
		presetArtifact({
			trustedPresets: [
				{ name: 'x-default-status', codec: 'string' },
				{ name: 'x-tenant', codec: 'string' },
				{ name: 'x-unused', codec: 'string' }
			]
		})
	]) {
		assert.throws(
			() =>
				prepareReplicaCommandWithTrustedPresets(
					malformedArtifact,
					undefined,
					authoritative,
					{ commandId: COMMAND_ID }
				),
			(error) =>
				error instanceof ReplicaCommandContractError &&
				error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID'
		);
	}

	assert.throws(
		() =>
			prepareReplicaCommandWithTrustedPresets(
				artifact,
				undefined,
				authoritative.filter(({ name }) => name !== 'x-tenant'),
				{ commandId: COMMAND_ID }
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_TRUSTED_PRESET_MISMATCH'
	);
});
