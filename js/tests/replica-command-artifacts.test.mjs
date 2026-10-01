import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
	createReplicaUuidV7,
	prepareReplicaCommand,
	ReplicaCommandContractError,
	verifyReplicaCommandReceipt
} from '../dist/replica/index.js';
import {
	COMMAND_CONSISTENCY,
	commandReceipt
} from './fixtures/command-protocol.mjs';

const HASH_A = `sha256:${'a'.repeat(64)}`;
const HASH_B = `sha256:${'b'.repeat(64)}`;
const HASH_C = `sha256:${'c'.repeat(64)}`;
const PROGRAM = `pp1:sha256:${'1'.repeat(64)}`;
const BINDING = `pb1:sha256:${'2'.repeat(64)}`;
const COMMAND_ID = '018f47de-3d2a-7abc-8abc-0123456789ab';
const OTHER_COMMAND_ID = '018f47de-3d2a-7def-8def-0123456789ab';
const GENERATED_UUID = '018f47de-3d2a-7123-8123-0123456789ab';
const GENERATED_ULID = '01J0Z6YV6E0000000000000000';
const GENERATED_DRAINING_COMMAND = JSON.parse(
	readFileSync(
		new URL(
			'../../tests/fixtures/generated-draining-command-v2.json',
			import.meta.url
		),
		'utf8'
	)
);

const GENERATED_UNSIGNED_COMMAND = JSON.parse(readFileSync(new URL(
	'../../distributed_cli/tests/fixtures/generated-unsigned-command.json', import.meta.url
), 'utf8'));

test('generated unsigned command preserves exact optimistic and transport values', () => {
	for (const priority of [0, 1, Number.MAX_SAFE_INTEGER]) {
		const prepared = prepareReplicaCommand(GENERATED_UNSIGNED_COMMAND, {
			id: GENERATED_UUID, tenantId: 'tenant-1', title: 'Update', priority
		}, { commandId: COMMAND_ID });
		assert.equal(prepared.transport.variables.input.priority, priority);
		assert.equal(prepared.optimistic.operations[0].fields.priority, priority);
		assert.equal(prepared.optimistic.operations[0].kind, 'upsert');
	}
});

test('unsigned command preparation rejects invalid values before producing optimism', () => {
	for (const [codec, maximum] of [['uint8', 255], ['uint16', 65_535],
		['uint32', 4_294_967_295], ['uint64_safe_integer', Number.MAX_SAFE_INTEGER]]) {
		const artifact = structuredClone(GENERATED_UNSIGNED_COMMAND);
		artifact.input.definition.fields.find(field => field.name === 'priority').codec = codec;
		const prepare = priority => prepareReplicaCommand(artifact, {
			id: GENERATED_UUID, tenantId: 'tenant-1', title: 'Update', priority
		}, { commandId: COMMAND_ID });
		assert.equal(prepare(maximum).optimistic.operations[0].fields.priority, maximum);
		for (const invalid of [-1, -0, 0.5, NaN, Infinity, -Infinity, maximum + 1,
			2 ** 64, '1', 1n, null, undefined]) {
			assert.throws(() => prepare(invalid), error =>
				error instanceof ReplicaCommandContractError && /input.priority/.test(error.message));
		}
	}
});

test('compiled recovery-only command drops unused presets without weakening validation', () => {
	const artifact = JSON.parse(readFileSync(new URL(
		'../../distributed_cli/tests/fixtures/generated-recovery-preset-command.json', import.meta.url
	), 'utf8'));
	assert.deepEqual(artifact.protocol.trustedPresets, [{ name: 'priority', codec: 'int32' }]);
	assert.equal(artifact.trustedPresets, undefined);
	assert.deepEqual(artifact.projection.preview.operations, []);
	assert.ok(artifact.projection.preview.recoveries.length > 0);
	const input = { id: GENERATED_UUID, tenantId: 'tenant-1', title: 'Prepare' };
	assert.doesNotThrow(() => prepareReplicaCommand(artifact, input, { commandId: COMMAND_ID }));
	const invalid = { ...artifact, trustedPresets: artifact.protocol.trustedPresets };
	assert.throws(
		() => prepareReplicaCommand(invalid, input, { commandId: COMMAND_ID }),
		/artifact.trustedPresets\[0\]/
	);
});

const scalarField = (
	name,
	typeName = 'String',
	codec = 'string',
	overrides = {}
) =>
	Object.freeze({
		name,
		typeName,
		nullable: false,
		list: false,
		itemNullable: false,
		codec,
		...overrides
	});

const nestedField = (name, definition, overrides = {}) =>
	Object.freeze({
		name,
		typeName: definition.name,
		nullable: false,
		list: false,
		itemNullable: false,
		nested: definition,
		...overrides
	});

const META_TYPE = Object.freeze({
	name: 'CreateMetaInput',
	fields: Object.freeze([
		scalarField('count', 'Int', 'int32'),
		scalarField('note', 'String', 'string', { nullable: true })
	])
});

const CREATE_TYPE = Object.freeze({
	name: 'CreateTodoInput',
	fields: Object.freeze([
		scalarField('code', 'ID'),
		scalarField('id', 'ID'),
		nestedField('meta', META_TYPE),
		scalarField('title')
	])
});

const OUTPUT = Object.freeze({
	kind: 'object',
	definition: Object.freeze({
		name: 'CommandResult',
		fields: Object.freeze([
			scalarField('value', 'JSON', 'json', { nullable: true })
		])
	})
});
const INPUT = Object.freeze({
	kind: 'object',
	definition: CREATE_TYPE
});
const JSON_IMPORT_INPUT = Object.freeze({
	kind: 'object',
	definition: Object.freeze({
		name: 'ImportTodosInput',
		fields: Object.freeze([scalarField('payload', 'JSON', 'json')])
	})
});
const PROJECTED_OUTPUT = Object.freeze({
	kind: 'object',
	definition: Object.freeze({
		name: 'Todo',
		fields: Object.freeze([
			scalarField('id', 'ID'),
			scalarField('title')
		])
	})
});

const inputValue = (path) => Object.freeze({ kind: 'input', path: Object.freeze(path) });
const constantValue = (value) =>
	Object.freeze({
		kind: 'constant',
		value:
			typeof value === 'string'
				? Object.freeze({ type: 'string', value })
				: Object.freeze({ type: 'i64', value: String(value) })
	});
const projectionField = (field, value) => Object.freeze({ field, value });
const unit = Object.freeze({ kind: 'unit' });
const EVENT = Object.freeze({
	id: 'todo-event',
	name: 'todo.changed',
	version: 1
});

function projectionScope(model, ...fields) {
	return Object.freeze({
		partition: unit,
		model,
		key: Object.freeze(
			fields.map(({ field, value }, ordinal) =>
				Object.freeze({ ordinal, field, value })
			)
		)
	});
}

function projectionArtifact(operations, capabilities, recoveries = []) {
	return Object.freeze({
		version: 2,
		deltaWireVersion: 1,
		projectionProgramVersion: 2,
		operationSemanticsVersion: 1,
		projections: Object.freeze([
			Object.freeze({
				programId: PROGRAM,
				bindingId: BINDING,
				epoch: 'todos-v1',
				programIrVersion: 1,
				operationSemanticsVersion: 1
			})
		]),
		eventSet: Object.freeze([EVENT]),
		capabilities: Object.freeze({
			version: 1,
			arms: Object.freeze([
				Object.freeze({
					event: EVENT,
					projection_ref: 0,
					arm: 'todo_changed',
					partition: unit,
					mutations: Object.freeze(capabilities)
				})
			])
		}),
		preview: Object.freeze({
			version: 1,
			occurrences: Object.freeze([
				Object.freeze({ ordinal: 0, event: EVENT })
			]),
			operations: Object.freeze(
				operations.map((mutation) =>
					Object.freeze({
						occurrence_ordinal: 0,
						projection_refs: Object.freeze([0]),
						mutation
					})
				)
			),
			recoveries: Object.freeze(recoveries)
		}),
		fallback: 'revalidate'
	});
}

function baseProjection() {
	const scope = projectionScope(
		'Todo',
		projectionField('id', inputValue(['id']))
	);
	return projectionArtifact(
		[
			Object.freeze({
				op: 'upsert',
				scope,
				fields: Object.freeze([
					projectionField('count', inputValue(['meta', 'count'])),
					projectionField('title', inputValue(['title']))
				]),
				replace: Object.freeze(['count', 'title'])
			})
		],
		[
			Object.freeze({
				kind: 'record',
				model: 'Todo',
				key: Object.freeze(['id']),
				fields: Object.freeze(['count', 'title']),
				replace: Object.freeze(['count', 'title']),
				upsert: true,
				patch: false,
				delete: false
			})
		]
	);
}

const TODO_RELATIONSHIP = Object.freeze({
	sourceModel: 'Todo',
	field: 'related',
	targetModel: 'Todo'
});

function baseArtifact(overrides = {}) {
	return {
		version: 2,
		name: 'todo.create',
		mutationField: 'createTodo',
		document:
			'mutation Client_createTodo($commandId: ID!, $input: CreateTodoInput!) { createTodo(commandId: $commandId, input: $input) }',
		operationHash: HASH_A,
		protocol: {
			version: 1,
			schemaHash: HASH_B,
			protocolHash: HASH_C,
			surface: { kind: 'role', name: 'user' },
			operation: HASH_A,
			trustedPresets: []
		},
		input: INPUT,
		output: OUTPUT,
		inputDefaults: {
			version: 1,
			defaults: [
				{ path: ['code'], generator: 'ulid' },
				{ path: ['id'], generator: 'uuid_v7' }
			]
		},
		consistency: COMMAND_CONSISTENCY.EVENTUAL,
		projection: baseProjection(),
		revalidation: {
			version: 1,
			required: false,
			dependencies: ['todo_rows'],
			models: ['Todo'],
			relationships: []
		},
		...overrides
	};
}

function projectedArtifact(directOverrides = {}) {
	return baseArtifact({
		output: PROJECTED_OUTPUT,
		consistency: COMMAND_CONSISTENCY.ATOMIC,
		projection: undefined,
		directProjection: {
			topology: {
				version: 1,
				name: 'todos',
				digest: HASH_C
			},
			model: 'Todo',
			identityFields: ['id'],
			partition: inputValue(['meta', 'count']),
			changeEpoch: 'todos-v1',
			...directOverrides
		}
	});
}

function receipt(prepared, state, overrides = {}) {
	return commandReceipt({
		commandId: prepared.commandId,
		causationId: 'opaque-causation',
		state,
		consistency: prepared.consistency,
		expects: [],
		observations: [],
		records: [],
		...overrides
	});
}

function expectation(projection, model, token) {
	return { projection, model, scopeToken: token };
}

test('preparation fills omitted UUIDv7/ULID defaults once and closes every input expression', () => {
	let uuidCalls = 0;
	let ulidCalls = 0;
	const callerInput = {
		title: 'Ship it',
		meta: { count: 3 }
	};
	const prepared = prepareReplicaCommand(baseArtifact(), callerInput, {
		commandId: COMMAND_ID,
		generators: {
			uuidV7: () => {
				uuidCalls += 1;
				return GENERATED_UUID;
			},
			ulid: () => {
				ulidCalls += 1;
				return GENERATED_ULID;
			}
		}
	});

	assert.deepEqual(callerInput, { title: 'Ship it', meta: { count: 3 } });
	assert.equal(prepared.input.id, GENERATED_UUID);
	assert.equal(prepared.input.code, GENERATED_ULID);
	assert.equal(prepared.transport.variables.input, prepared.input);
	assert.deepEqual(prepared.transport.variables, {
		commandId: COMMAND_ID,
		input: {
			code: GENERATED_ULID,
			id: GENERATED_UUID,
			meta: { count: 3 },
			title: 'Ship it'
		}
	});
	assert.deepEqual(prepared.optimistic.operations[0], {
		kind: 'upsert',
		scope: {
			model: 'Todo',
			key: [{ field: 'id', value: GENERATED_UUID }]
		},
		fields: { count: 3, title: 'Ship it' },
		replace: ['count', 'title']
	});
	assert.equal(uuidCalls, 1);
	assert.equal(ulidCalls, 1);

	// The prepared value itself is the retry unit; reading/reusing it performs no work.
	assert.equal(prepared.transport.variables.input.id, GENERATED_UUID);
	assert.equal(
		prepared.optimistic.operations[0].scope.key[0].value,
		GENERATED_UUID
	);
	assert.equal(uuidCalls, 1);
	assert.equal(ulidCalls, 1);
});

test('explicit defaulted fields are retained and their generators never run', () => {
	let calls = 0;
	const prepared = prepareReplicaCommand(
		baseArtifact(),
		{
			code: 'caller-code',
			id: 'caller-id',
			meta: { count: 1 },
			title: 'Explicit'
		},
		{
			commandId: COMMAND_ID.toUpperCase(),
			generators: {
				uuidV7: () => {
					calls += 1;
					return GENERATED_UUID;
				},
				ulid: () => {
					calls += 1;
					return GENERATED_ULID;
				}
			}
		}
	);

	assert.equal(prepared.commandId, COMMAND_ID);
	assert.equal(prepared.input.id, 'caller-id');
	assert.equal(prepared.input.code, 'caller-code');
	assert.equal(calls, 0);
});

test('compact generated preview patches canonicalize an omitted unset list', () => {
	const scope = projectionScope(
		'Todo',
		projectionField('id', inputValue(['id']))
	);
	const artifact = baseArtifact({
		projection: projectionArtifact(
			[
				Object.freeze({
					op: 'patch',
					scope,
					set: Object.freeze([
						projectionField('title', inputValue(['title']))
					]),
					if_present: true
				})
			],
			[
				Object.freeze({
					kind: 'record',
					model: 'Todo',
					key: Object.freeze(['id']),
					fields: Object.freeze(['title']),
					replace: Object.freeze([]),
					upsert: false,
					patch: true,
					delete: false
				})
			],
			[
				Object.freeze({
					occurrence_ordinal: 0,
					projection_refs: Object.freeze([0]),
					condition: 'if_record_missing',
					target: Object.freeze({ kind: 'record', scope })
				})
			]
		)
	});
	const prepared = prepareReplicaCommand(
		artifact,
		{ meta: { count: 1 }, title: 'Compact patch' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	assert.deepEqual(prepared.optimistic.operations, [
		{
			kind: 'patch',
			scope: {
				model: 'Todo',
				key: [{ field: 'id', value: GENERATED_UUID }]
			},
			fields: { title: 'Compact patch' },
			unset: [],
			ifPresent: true
		}
	]);
	const unset = prepared.optimistic.operations[0].unset;
	assert.equal(Object.isFrozen(unset), true);
	assert.throws(() => unset.push('title'), TypeError);
	assert.equal(
		prepared.projection.revalidate,
		false,
		'a conditional missing-record fallback is not unconditional revalidation'
	);
});

test('real default generators produce canonical values', () => {
	const prepared = prepareReplicaCommand(
		baseArtifact(),
		{ meta: { count: 1 }, title: 'Generated' },
		{ commandId: COMMAND_ID }
	);

	assert.match(prepared.input.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7/);
	assert.match(prepared.input.code, /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
});

test('public UUIDv7 generation supports caller-correlated optimistic record ids', () => {
	const id = createReplicaUuidV7();
	assert.match(
		id,
		/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
	);
});

test('none inputs and typed JSON fields produce exact canonical transport variables', () => {
	const noInput = prepareReplicaCommand(
		baseArtifact({
			name: 'todo.rebuild',
			mutationField: 'rebuildTodos',
			document:
				'mutation Client_rebuildTodos($commandId: ID!) { rebuildTodos(commandId: $commandId) }',
			input: { kind: 'none' },
			inputDefaults: undefined,
			consistency: COMMAND_CONSISTENCY.SUCCEEDED,
			projection: undefined,
			revalidation: {
				version: 1,
				required: true,
				dependencies: ['todo_rows'],
				models: ['Todo'],
				relationships: []
			}
		}),
		undefined,
		{ commandId: COMMAND_ID }
	);
	assert.deepEqual(noInput.transport.variables, { commandId: COMMAND_ID });
	assert.equal(noInput.input, undefined);

	const json = prepareReplicaCommand(
		baseArtifact({
			name: 'todo.import',
			mutationField: 'importTodos',
			document:
				'mutation Client_importTodos($commandId: ID!, $input: ImportTodosInput!) { importTodos(commandId: $commandId, input: $input) }',
			input: JSON_IMPORT_INPUT,
			inputDefaults: undefined,
			consistency: COMMAND_CONSISTENCY.SUCCEEDED,
			projection: undefined,
			revalidation: {
				version: 1,
				required: true,
				dependencies: ['todo_rows'],
				models: ['Todo'],
				relationships: []
			}
		}),
		{ payload: { z: [2, 1], a: { y: true, x: null } } },
		{ commandId: COMMAND_ID }
	);
	assert.deepEqual(Object.keys(json.input), ['payload']);
	assert.deepEqual(Object.keys(json.input.payload), ['a', 'z']);
	assert.deepEqual(Object.keys(json.input.payload.a), ['x', 'y']);
	assert.equal(json.transport.variables.input, json.input);

	const cyclic = {};
	cyclic.self = cyclic;
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					name: 'todo.import',
					mutationField: 'importTodos',
					document:
						'mutation Client_importTodos($commandId: ID!, $input: ImportTodosInput!) { importTodos(commandId: $commandId, input: $input) }',
					input: JSON_IMPORT_INPUT,
					inputDefaults: undefined,
					consistency: COMMAND_CONSISTENCY.SUCCEEDED,
					projection: undefined,
					revalidation: {
						version: 1,
						required: true,
						dependencies: ['todo_rows'],
						models: ['Todo'],
						relationships: []
					}
				}),
				{ payload: cyclic },
				{ commandId: COMMAND_ID }
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_INPUT_INVALID'
	);
});

test('nested paths resolve exactly and missing or type-invalid values fail closed', () => {
	const nullableMeta = {
		...META_TYPE,
		fields: [
			scalarField('count', 'Int', 'int32'),
			scalarField('note', 'String', 'string', { nullable: true })
		]
	};
	const artifact = baseArtifact({
		input: {
			kind: 'object',
			definition: {
				...CREATE_TYPE,
				fields: [
					scalarField('code', 'ID'),
					scalarField('id', 'ID'),
					nestedField('meta', nullableMeta),
					scalarField('title')
				]
			}
		},
		projection: projectionArtifact(
			[
				Object.freeze({
					op: 'patch',
					scope: projectionScope(
						'Todo',
						projectionField('id', inputValue(['id']))
					),
					set: Object.freeze([
						projectionField('note', inputValue(['meta', 'note']))
					]),
					unset: Object.freeze([]),
					if_present: true
				})
			],
			[
				Object.freeze({
					kind: 'record',
					model: 'Todo',
					key: Object.freeze(['id']),
					fields: Object.freeze(['note']),
					replace: Object.freeze([]),
					upsert: false,
					patch: true,
					delete: false
				})
			]
		)
	});

	const unresolved = prepareReplicaCommand(
		artifact,
		{ meta: { count: 1 }, title: 'Absent note' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);
	assert.deepEqual(unresolved.optimistic.operations, []);
	assert.equal(unresolved.projection.revalidate, true);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact(),
				{ meta: { count: 'one' }, title: 'Wrong type' },
				{
					commandId: COMMAND_ID,
					generators: {
						uuidV7: () => GENERATED_UUID,
						ulid: () => GENERATED_ULID
					}
				}
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_INPUT_INVALID' &&
			error.path === 'input.meta.count'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact(),
				{ extra: true, meta: { count: 1 }, title: 'Unknown field' },
				{
					commandId: COMMAND_ID,
					generators: {
						uuidV7: () => GENERATED_UUID,
						ulid: () => GENERATED_ULID
					}
				}
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'input.extra'
	);
});

test('the closed optimistic IR supports every declared effect kind', () => {
	const artifact = baseArtifact({
		projection: projectionArtifact(
			[
				Object.freeze({
					op: 'upsert',
					scope: projectionScope(
						'TodoCreate',
						projectionField('id', inputValue(['id']))
					),
					fields: Object.freeze([
						projectionField('title', inputValue(['title']))
					]),
					replace: Object.freeze(['title'])
				}),
				Object.freeze({
					op: 'patch',
					scope: projectionScope(
						'TodoPatch',
						projectionField('id', inputValue(['id']))
					),
					set: Object.freeze([
						projectionField('rank', constantValue(2))
					]),
					unset: Object.freeze([]),
					if_present: true
				}),
				Object.freeze({
					op: 'delete',
					scope: projectionScope(
						'TodoRemove',
						projectionField('id', inputValue(['id']))
					)
				}),
				Object.freeze({
					op: 'link',
					relationship: TODO_RELATIONSHIP.field,
					source: projectionScope(
						'Todo',
						projectionField('id', inputValue(['id']))
					),
					target: projectionScope(
						'Todo',
						projectionField('id', constantValue('target-a'))
					)
				}),
				Object.freeze({
					op: 'unlink',
					relationship: TODO_RELATIONSHIP.field,
					source: projectionScope(
						'Todo',
						projectionField('id', inputValue(['id']))
					),
					target: projectionScope(
						'Todo',
						projectionField('id', constantValue('target-b'))
					)
				}),
				Object.freeze({ op: 'invalidate_model', model: 'Todo' }),
				Object.freeze({
					op: 'invalidate_relationship',
					relationship: TODO_RELATIONSHIP.field,
					source: projectionScope(
						'Todo',
						projectionField('id', inputValue(['id']))
					)
				})
			],
			[
				Object.freeze({
					kind: 'record',
					model: 'TodoCreate',
					key: Object.freeze(['id']),
					fields: Object.freeze(['title']),
					replace: Object.freeze(['title']),
					upsert: true,
					patch: false,
					delete: false
				}),
				Object.freeze({
					kind: 'record',
					model: 'TodoPatch',
					key: Object.freeze(['id']),
					fields: Object.freeze(['rank']),
					replace: Object.freeze([]),
					upsert: false,
					patch: true,
					delete: false
				}),
				Object.freeze({
					kind: 'record',
					model: 'TodoRemove',
					key: Object.freeze(['id']),
					fields: Object.freeze([]),
					replace: Object.freeze([]),
					upsert: false,
					patch: false,
					delete: true
				}),
				Object.freeze({
					kind: 'relationship',
					relationship: TODO_RELATIONSHIP.field,
					source_model: 'Todo',
					source_key: Object.freeze(['id']),
					target_model: 'Todo',
					target_key: Object.freeze(['id']),
					link: true,
					unlink: true
				}),
				Object.freeze({ kind: 'model', model: 'Todo' })
			]
		),
		revalidation: {
			version: 1,
			required: false,
			dependencies: ['todo_rows', 'todo_links'],
			models: ['Todo', 'TodoCreate', 'TodoPatch', 'TodoRemove'],
			relationships: [TODO_RELATIONSHIP]
		}
	});
	const prepared = prepareReplicaCommand(
		artifact,
		{ meta: { count: 4 }, title: 'All effects' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	assert.deepEqual(
		prepared.optimistic.operations.map(({ kind }) => kind),
		[
			'upsert',
			'patch',
			'delete',
			'link',
			'unlink',
			'invalidate_model',
			'invalidate_relationship'
		]
	);
	assert.equal(
		prepared.optimistic.operations[0].scope.key[0].value,
		GENERATED_UUID
	);
	assert.equal(prepared.optimistic.operations[1].fields.rank, 2);
	assert.equal(
		prepared.optimistic.operations[3].target.key[0].value,
		'target-a'
	);
});

test('unknown generators/effects and unresolved trusted presets fail before optimism', () => {
	const options = {
		commandId: COMMAND_ID,
		generators: {
			uuidV7: () => GENERATED_UUID,
			ulid: () => GENERATED_ULID
		}
	};
	const input = { meta: { count: 1 }, title: 'Invalid IR' };

	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					protocol: {
						version: 1,
						schemaHash: HASH_B,
						protocolHash: HASH_C,
						operation: HASH_A,
						trustedPresets: []
					}
				}),
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'artifact.protocol.surface'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					protocol: {
						version: 1,
						schemaHash: HASH_B,
						protocolHash: HASH_C,
						surface: { kind: 'role', name: 'user' },
						operation: HASH_B,
						trustedPresets: []
					}
				}),
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'artifact.protocol.operation'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					inputDefaults: {
						version: 1,
						defaults: [{ path: ['id'], generator: 'uuid_v4' }]
					}
				}),
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					projection: projectionArtifact(
						[Object.freeze({ op: 'explode' })],
						[Object.freeze({ kind: 'model', model: 'Todo' })]
					)
				}),
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path ===
				'artifact.projection.preview.operations[0].mutation.op'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(
				baseArtifact({
					projection: projectionArtifact(
						[
							Object.freeze({
								op: 'patch',
								scope: projectionScope(
									'Todo',
									projectionField('id', inputValue(['id']))
								),
								set: Object.freeze([
									projectionField(
										'owner',
										Object.freeze({
											kind: 'trusted_preset',
											name: 'current_tenant',
											codec: 'string'
										})
									)
								]),
								unset: Object.freeze([]),
								if_present: true
							})
						],
						[
							Object.freeze({
								kind: 'record',
								model: 'Todo',
								key: Object.freeze(['id']),
								fields: Object.freeze(['owner']),
								replace: Object.freeze([]),
								upsert: false,
								patch: true,
								delete: false
							})
						]
					)
				}),
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID'
	);
	assert.throws(
		() =>
			prepareReplicaCommand(baseArtifact(), input, {
				commandId: COMMAND_ID,
				generators: {
					uuidV7: () => 'not-a-uuid',
					ulid: () => GENERATED_ULID
				}
			}),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_INPUT_INVALID'
	);
});

test('modeled receipts defer without a delta and take obligations from server metadata', () => {
	const prepared = prepareReplicaCommand(
		baseArtifact(),
		{ meta: { count: 1 }, title: 'Receipt' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	assert.deepEqual(
		verifyReplicaCommandReceipt(prepared, receipt(prepared, 'in_progress')),
		{ kind: 'deferred', revalidate: false }
	);
	assert.deepEqual(
		verifyReplicaCommandReceipt(
			prepared,
			receipt(prepared, 'succeeded_pending_projection', {
				expects: [
					expectation('search', 'TodoSearch', 'token-3'),
					expectation('todos', 'Todo', 'token-1'),
					expectation('todos', 'Todo', 'token-2')
				],
				projection: {}
			})
		),
		{ kind: 'matched', revalidate: false }
	);
	assert.deepEqual(
		verifyReplicaCommandReceipt(
			prepared,
			receipt(prepared, 'succeeded_pending_projection', {
				expects: [
					expectation('search', 'TodoSearch', 'token-3'),
					expectation('todos', 'Todo', 'token-1')
				],
				projection: {}
			})
		),
		{ kind: 'matched', revalidate: false }
	);
	assert.throws(
		() =>
			verifyReplicaCommandReceipt(
				prepared,
				receipt(prepared, 'succeeded_pending_projection')
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'receipt.expects'
	);
	assert.throws(
		() =>
			verifyReplicaCommandReceipt(prepared, {
				...receipt(prepared, 'succeeded_pending_projection'),
				commandId: OTHER_COMMAND_ID
			}),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'receipt.commandId'
	);
	assert.throws(
		() =>
			verifyReplicaCommandReceipt(prepared, {
				...receipt(prepared, 'succeeded_pending_projection'),
				consistency: COMMAND_CONSISTENCY.SUCCEEDED
			}),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'receipt.consistency'
	);
});

test('unavailable confirmation contracts always force conservative revalidation', () => {
	const artifact = baseArtifact({
		consistency: COMMAND_CONSISTENCY.SUCCEEDED,
		projection: undefined,
		revalidation: {
			version: 1,
			required: true,
			dependencies: ['todo_rows'],
			models: ['Todo'],
			relationships: []
		}
	});
	const prepared = prepareReplicaCommand(
		artifact,
		{ meta: { count: 1 }, title: 'Unavailable' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	assert.deepEqual(
		verifyReplicaCommandReceipt(
			prepared,
			receipt(prepared, 'succeeded')
		),
		{ kind: 'matched', revalidate: true }
	);
});

test('generated Draining command authorizes lifecycle revalidation without projection application authority', () => {
	assert.equal(GENERATED_DRAINING_COMMAND.consistency, 'eventual');
	assert.equal(GENERATED_DRAINING_COMMAND.projection, undefined);
	assert.equal(GENERATED_DRAINING_COMMAND.revalidation.required, true);
	const prepared = prepareReplicaCommand(
		GENERATED_DRAINING_COMMAND,
		{ todo_id: 'todo-1' },
		{ commandId: COMMAND_ID }
	);

	assert.deepEqual(
		verifyReplicaCommandReceipt(
			prepared,
			receipt(prepared, 'succeeded', {
				projectionDisposition: 'revalidate'
			})
		),
		{ kind: 'matched', revalidate: true }
	);
});

test('revalidation disposition without projection or command-level capability fails closed', () => {
	const prepared = prepareReplicaCommand(
		projectedArtifact(),
		{ meta: { count: 7 }, title: 'No lifecycle capability' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);
	assert.equal(prepared.projection, undefined);
	assert.equal(prepared.revalidation.required, false);

	assert.throws(
		() =>
			verifyReplicaCommandReceipt(
				prepared,
				receipt(prepared, 'atomic', {
					projectionDisposition: 'revalidate'
				})
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'receipt.projectionDisposition'
	);
});

test('succeeded commands without finite confirmations require canonical revalidation', () => {
	const invalid = baseArtifact({
		consistency: COMMAND_CONSISTENCY.SUCCEEDED,
		projection: undefined
	});
	const input = { meta: { count: 1 }, title: 'No finite fence' };
	const options = {
		commandId: COMMAND_ID,
		generators: {
			uuidV7: () => GENERATED_UUID,
			ulid: () => GENERATED_ULID
		}
	};

	assert.throws(
		() => prepareReplicaCommand(invalid, input, options),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'artifact.revalidation.required'
	);

	const valid = baseArtifact({
		consistency: COMMAND_CONSISTENCY.SUCCEEDED,
		projection: undefined,
		revalidation: {
			...invalid.revalidation,
			required: true
		}
	});
	assert.equal(
		prepareReplicaCommand(valid, input, options).revalidation.required,
		true
	);
});

test('projected commands close the direct projection partition from finalized input', () => {
	const artifact = projectedArtifact();
	const prepared = prepareReplicaCommand(
		artifact,
		{ meta: { count: 7 }, title: 'Projected' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	assert.equal(prepared.directProjection.partition, 7);
	assert.equal(prepared.directProjection.topology.name, 'todos');
	assert.deepEqual(prepared.directProjection.identityFields, ['id']);
	assert.notEqual(
		prepared.directProjection.identityFields,
		artifact.directProjection.identityFields
	);
	assert.equal(Object.isFrozen(prepared.directProjection), true);
	assert.equal(Object.isFrozen(prepared.directProjection.topology), true);
	assert.equal(Object.isFrozen(prepared.directProjection.identityFields), true);
	assert.deepEqual(
		verifyReplicaCommandReceipt(prepared, receipt(prepared, 'atomic')),
		{ kind: 'matched', revalidate: false }
	);
});

test('projected command identity facts reject missing, unknown, and duplicate fields', () => {
	const input = { meta: { count: 7 }, title: 'Projected' };
	const options = {
		commandId: COMMAND_ID,
		generators: {
			uuidV7: () => GENERATED_UUID,
			ulid: () => GENERATED_ULID
		}
	};
	const cases = [
		{
			identityFields: undefined,
			path: 'artifact.directProjection.identityFields'
		},
		{ identityFields: [], path: 'artifact.directProjection.identityFields' },
		{
			identityFields: ['missing'],
			path: 'artifact.directProjection.identityFields[0]'
		},
		{
			identityFields: ['id', 'id'],
			path: 'artifact.directProjection.identityFields[1]'
		}
	];

	for (const { identityFields, path } of cases) {
		assert.throws(
			() =>
				prepareReplicaCommand(
					projectedArtifact({ identityFields }),
					input,
					options
				),
			(error) =>
				error instanceof ReplicaCommandContractError &&
				error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID' &&
				error.path === path
		);
	}

	assert.throws(
		() =>
			prepareReplicaCommand(
				{
					...projectedArtifact(),
					output: OUTPUT
				},
				input,
				options
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.code === 'REPLICA_COMMAND_ARTIFACT_INVALID' &&
			error.path === 'artifact.directProjection.identityFields[0]'
	);
});

test('prepared command input, variables, projection, and plans are deeply frozen', () => {
	const prepared = prepareReplicaCommand(
		baseArtifact(),
		{ meta: { count: 2, note: 'nested' }, title: 'Frozen' },
		{
			commandId: COMMAND_ID,
			generators: {
				uuidV7: () => GENERATED_UUID,
				ulid: () => GENERATED_ULID
			}
		}
	);

	for (const value of [
		prepared,
		prepared.input,
		prepared.input.meta,
		prepared.transport,
		prepared.transport.protocol,
		prepared.transport.variables,
		prepared.optimistic,
		prepared.optimistic.operations,
		prepared.optimistic.operations[0],
		prepared.optimistic.operations[0].scope,
		prepared.optimistic.operations[0].scope.key,
		prepared.optimistic.operations[0].fields,
		prepared.projection,
		prepared.projection.contract,
		prepared.projection.contract.capabilities,
		prepared.projection.contract.capabilities.arms,
		prepared.revalidation,
		prepared.revalidation.dependencies,
		prepared.revalidation.models,
		prepared.revalidation.relationships
	]) {
		assert.equal(Object.isFrozen(value), true);
	}
	assert.throws(() => {
		prepared.input.meta.count = 9;
	}, TypeError);
});
