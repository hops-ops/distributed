import { createLazyReplicaCommandRuntime } from '../dist/replica/command-runtime/lazy.js';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { createCacheEngine } from '../dist/internal/cache-engine.js';
import {
	createReplicaCommandRuntime,
	replicaCommandAuthority,
	replicaCommandDirectProjection,
	replicaCommandProjectionDelta,
	ReplicaCommandRuntimeError
} from '../dist/replica/command-runtime.js';
import {
	prepareReplicaCommand,
	replicaRecordKey,
	ReplicaCommandContractError
} from '../dist/replica/index.js';
import {
	COMMAND_CONSISTENCY,
	COMMAND_STATE,
	commandReceipt
} from './fixtures/command-protocol.mjs';

const HASH_A = `sha256:${'a'.repeat(64)}`;
const HASH_B = `sha256:${'b'.repeat(64)}`;
const HASH_C = `sha256:${'c'.repeat(64)}`;
const HASH_D = `sha256:${'d'.repeat(64)}`;
const PROGRAM = `pp1:sha256:${'1'.repeat(64)}`;
const BINDING = `pb1:sha256:${'2'.repeat(64)}`;
const COMMAND_A = '018f47de-3d2a-7abc-8abc-0123456789ab';
const COMMAND_B = '018f47de-3d2a-7def-8def-0123456789ab';
const COMMAND_C = '018f47de-3d2a-7123-8123-0123456789ab';
const GENERATED_DRAINING_COMMAND = JSON.parse(
	readFileSync(
		new URL(
			'../../tests/fixtures/generated-draining-command-v2.json',
			import.meta.url
		),
		'utf8'
	)
);
const SURFACE = Object.freeze({ kind: 'role', name: 'user' });
const CACHE_SCOPE = token('cache-scope', 1);
const Todo = Object.freeze({
	id: 'Todos',
	identityFields: Object.freeze(['id'])
});
const Audit = Object.freeze({
	id: 'Audits',
	identityFields: Object.freeze(['id'])
});

const scalar = (name, typeName = 'String', codec = 'string') =>
	Object.freeze({
		name,
		typeName,
		codec,
		nullable: false,
		list: false,
		itemNullable: false
	});

const TodoInput = Object.freeze({
	name: 'TodoInput',
	fields: Object.freeze([scalar('id', 'ID'), scalar('title')])
});
const ResultOutput = Object.freeze({
	name: 'CommandResult',
	fields: Object.freeze([scalar('ok', 'Boolean', 'boolean')])
});

const input = (path) =>
	Object.freeze({ kind: 'input', path: Object.freeze(path) });
const unit = Object.freeze({ kind: 'unit' });

function scope(value, model = Todo.id) {
	return Object.freeze({
		partition: unit,
		model,
		key: Object.freeze([
			Object.freeze({
				ordinal: 0,
				field: 'id',
				value
			})
		])
	});
}

function projection(operation = 'upsert') {
	const event = Object.freeze({ id: 'event-1', name: 'todo.changed', version: 1 });
	const previewScope = scope(input(['id']));
	const targetScope = scope(
		Object.freeze({
			kind: 'constant',
			value: Object.freeze({ type: 'string', value: 'target' })
		})
	);
	let mutation;
	if (operation === 'upsert') {
		mutation = Object.freeze({
			op: 'upsert',
			scope: previewScope,
			fields: Object.freeze([
				Object.freeze({ field: 'title', value: input(['title']) })
			]),
			replace: Object.freeze(['title'])
		});
	} else if (operation === 'patch') {
		mutation = Object.freeze({
			op: 'patch',
			scope: previewScope,
			set: Object.freeze([
				Object.freeze({ field: 'title', value: input(['title']) })
			]),
			unset: Object.freeze([]),
			if_present: true
		});
	} else if (operation === 'delete') {
		mutation = Object.freeze({ op: 'delete', scope: previewScope });
	} else if (operation === 'link' || operation === 'unlink') {
		mutation = Object.freeze({
			op: operation,
			relationship: 'related',
			source: previewScope,
			target: targetScope
		});
	} else if (operation === 'invalidate_model') {
		mutation = Object.freeze({
			op: 'invalidate_model',
			partition: unit,
			model: Todo.id
		});
	} else {
		mutation = Object.freeze({
			op: 'invalidate_relationship',
			relationship: 'related',
			source: previewScope
		});
	}
	const capability =
		operation === 'link' || operation === 'unlink'
			? Object.freeze({
					kind: 'relationship',
					relationship: 'related',
					source_model: Todo.id,
					source_key: Object.freeze(['id']),
					target_model: Todo.id,
					target_key: Object.freeze(['id']),
					link: operation === 'link',
					unlink: operation === 'unlink'
				})
			: operation === 'invalidate_model'
				? Object.freeze({ kind: 'model', model: Todo.id })
				: operation === 'invalidate_relationship'
					? Object.freeze({
							kind: 'relationship',
							relationship: 'related',
							source_model: Todo.id,
							source_key: Object.freeze(['id']),
							target_model: Todo.id,
							target_key: Object.freeze(['id']),
							link: false,
							unlink: false
						})
					: Object.freeze({
							kind: 'record',
							model: Todo.id,
							key: Object.freeze(['id']),
							fields: Object.freeze(
								operation === 'delete' ? [] : ['title']
							),
							replace: Object.freeze(
								operation === 'upsert' ? ['title'] : []
							),
							upsert: operation === 'upsert',
							patch: operation === 'patch',
							delete: operation === 'delete'
						});
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
		eventSet: Object.freeze([event]),
		capabilities: Object.freeze({
			version: 1,
			arms: Object.freeze([
				Object.freeze({
					event,
					projection_ref: 0,
					arm: `todo_${operation}`,
					partition: unit,
					mutations: Object.freeze([capability])
				})
			])
		}),
		preview: Object.freeze({
			version: 1,
			occurrences: Object.freeze([
				Object.freeze({ ordinal: 0, event })
			]),
			operations: Object.freeze([
				Object.freeze({
					occurrence_ordinal: 0,
					projection_refs: Object.freeze([0]),
					mutation
				})
			]),
			recoveries: Object.freeze([])
		}),
		fallback: 'revalidate'
	});
}

function artifact(options = {}) {
	const operation = options.operation ?? 'upsert';
	return Object.freeze({
		version: 2,
		name: options.name ?? `todo.${operation}`,
		mutationField: options.mutationField ?? 'changeTodo',
		document:
			'mutation Client_changeTodo($commandId: ID!, $input: TodoInput!) { changeTodo(commandId: $commandId, input: $input) { ok } }',
		operationHash: HASH_A,
		protocol: Object.freeze({
			version: 1,
			schemaHash: HASH_B,
			protocolHash: HASH_C,
			surface: SURFACE,
			operation: HASH_A,
			trustedPresets: Object.freeze([])
		}),
		input: Object.freeze({ kind: 'object', definition: TodoInput }),
		output: Object.freeze({ kind: 'object', definition: ResultOutput }),
		consistency: options.consistency ?? COMMAND_CONSISTENCY.EVENTUAL,
		...(options.modeled === false ? {} : { projection: projection(operation) }),
		...(options.directProjection === undefined
			? {}
			: { directProjection: options.directProjection }),
		revalidation: Object.freeze({
			version: 1,
			required: options.revalidate ?? false,
			dependencies: Object.freeze(['todos']),
			models: Object.freeze([Todo.id]),
			relationships: Object.freeze(
				operation === 'link' ||
					operation === 'unlink' ||
					operation === 'invalidate_relationship'
					? [
							Object.freeze({
								sourceModel: Todo.id,
								field: 'related',
								targetModel: Todo.id
							})
						]
					: []
			)
		})
	});
}

function directProjectionArtifact() {
	return Object.freeze({
		...artifact({
			name: 'todo.project',
			consistency: COMMAND_CONSISTENCY.ATOMIC,
			modeled: false,
			directProjection: Object.freeze({
				topology: Object.freeze({
					version: 1,
					name: 'todos',
					digest: HASH_D
				}),
				model: Todo.id,
				identityFields: Todo.identityFields,
				changeEpoch: 'todos-v1'
			})
		}),
		output: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: Todo.id,
				fields: Object.freeze([scalar('id', 'ID'), scalar('title')])
			})
		})
	});
}

function modeledArtifactWithAuditArm() {
	const modeled = structuredClone(artifact());
	const event = { id: 'event-2', name: 'audit.recorded', version: 1 };
	modeled.projection.eventSet.push(event);
	modeled.projection.capabilities.arms.push({
		event,
		projection_ref: 0,
		arm: 'audit_upsert',
		partition: { kind: 'unit' },
		mutations: [
			{
				kind: 'record',
				model: Audit.id,
				key: ['id'],
				fields: ['title'],
				replace: ['title'],
				upsert: true,
				patch: false,
				delete: false
			}
		]
	});
	return modeled;
}

function deltaMutation(request, options = {}) {
	const operation = options.operation ?? 'upsert';
	const actualScope = scope({
		type: 'string',
		value: request.variables.input.id
	});
	if (operation === 'upsert') {
		return {
			op: 'upsert',
			scope: actualScope,
			fields: [
				{
					field: 'title',
					value: {
						type: 'string',
						value: options.actualTitle ?? request.variables.input.title
					}
				}
			],
			replace: ['title']
		};
	}
	if (operation === 'patch') {
		return {
			op: 'patch',
			scope: actualScope,
			set: [
				{
					field: 'title',
					value: {
						type: 'string',
						value: options.actualTitle ?? request.variables.input.title
					}
				}
			],
			unset: [],
			if_present: true
		};
	}
	if (operation === 'delete') return { op: 'delete', scope: actualScope };
	if (operation === 'link' || operation === 'unlink') {
		return {
			op: operation,
			relationship: 'related',
			source: actualScope,
			target: scope({ type: 'string', value: 'target' })
		};
	}
	if (operation === 'invalidate_model') {
		return { op: 'invalidate_model', partition: unit, model: Todo.id };
	}
	return {
		op: 'invalidate_relationship',
		relationship: 'related',
		source: actualScope
	};
}

function commandMetadata(request, options = {}) {
	const state = options.state ?? COMMAND_STATE.PENDING_PROJECTION;
	const causationId = options.causationId ?? `cause:${request.commandId}`;
	if (state === 'in_progress' && options.projection === false) {
		return commandReceipt({
			commandId: request.commandId,
			causationId,
			state,
			consistency: options.consistency ?? COMMAND_CONSISTENCY.EVENTUAL,
			expects: [],
			observations: [],
			records: []
		});
	}
	const obligations = Array.from(
		{ length: options.obligations ?? 1 },
		(_, index) => ({
			projectionRef: 0,
			model: options.obligationModel ?? Todo.id,
			scopeToken: token('projection-obligation', index + 3)
		})
	);
	const projectionMetadata = {
		wireVersion: 1,
		issuedAtUnixMs: Date.now() - 1_000,
		expiresAtUnixMs: Date.now() + 60_000,
		delta: {
			wire_version: 1,
			identity: {
				manifest_version: 2,
				client_protocol_version: 1,
				surface: options.surface ?? SURFACE,
				schema_fingerprint: options.schemaHash ?? HASH_B,
				protocol_fingerprint: options.protocolHash ?? HASH_C,
				authorization_generation: options.authorizationGeneration ?? 'auth-1',
				cache_scope_token: options.cacheScope ?? CACHE_SCOPE,
				command_causation_id: causationId
			},
			projections: [
				{
					program_id: PROGRAM,
					binding_id: options.bindingId ?? BINDING,
					epoch: 'todos-v1',
					program_ir_version: 1,
					operation_semantics_version: 1
				}
			],
			occurrences: [
				{
					causation_id: causationId,
					ordinal: 0,
					occurrence_id: `occurrence:${request.commandId}`
				}
			],
			operations: options.emptyDelta
				? []
				: [
						{
							occurrence_ordinal: 0,
							projection_refs: [0],
							mutation:
								options.mutation ?? deltaMutation(request, options)
						}
					]
		},
		lifecycleProofs: [
			{
				projectionRef: 0,
				token: token('projection-lifecycle', 2)
			}
		],
		obligations,
		revalidate: options.revalidate ?? false
	};
	return commandReceipt({
		commandId: request.commandId,
		causationId,
		state,
		consistency: options.consistency ?? COMMAND_CONSISTENCY.EVENTUAL,
		expects: obligations.map((obligation) => ({
			projection: PROGRAM,
			model: obligation.model,
			scopeToken: obligation.scopeToken
		})),
		observations: options.observations ?? [],
		records: options.records ?? [],
		projection: projectionMetadata
	});
}

function envelope(request, options = {}) {
	const metadata =
		options.command ??
		commandMetadata(request, options);
	return {
		status: 200,
		...(options.errors === undefined ? {} : { errors: options.errors }),
		data:
			options.data ??
			{ [request.mutationField]: { ok: true } },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: HASH_B,
				authorizationGeneration:
					options.envelopeAuthorizationGeneration ?? 'auth-1',
				cacheScope: CACHE_SCOPE,
				operation: request.operationHash,
				trustedPresets: [],
				command: metadata
			}
		}
	};
}

function statusEnvelope(
	request,
	metadata,
	authorizationGeneration = 'auth-1'
) {
	return {
		status: 200,
		data: { commandStatus: { state: metadata.state } },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: HASH_B,
				authorizationGeneration,
				cacheScope: CACHE_SCOPE,
				operation: HASH_D,
				trustedPresets: [],
				command: metadata
			}
		}
	};
}

const STATUS = Object.freeze({
	name: 'Distributed_CommandStatus',
	document:
		'query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }',
	operationHash: HASH_D,
	protocol: Object.freeze({
		version: 1,
		schemaHash: HASH_B,
		protocolHash: HASH_C,
		surface: SURFACE,
		operation: HASH_D,
		trustedPresets: Object.freeze([])
	})
});

class TestReplica {
	engine = createCacheEngine();
	authorizationGeneration = 1;
	scope = Object.freeze({
		protocolVersion: 1,
		schemaHash: HASH_B,
		authorizationGeneration: 'auth-1',
		cacheScope: CACHE_SCOPE
	});
	revalidations = [];
	replacements = [];
	direct = [];
	semanticChanges = [];
	acceptances = 0;
	confirmations = 0;
	rejections = 0;
	#controller = new AbortController();

	[replicaCommandAuthority]() {
		return Object.freeze({
			read: () => ({
				generation: this.authorizationGeneration,
				scope: this.scope,
				trustedPresets: Object.freeze([]),
				signal: this.#controller.signal
			}),
			dispose() {}
		});
	}

	createOptimisticLayer(id, update, semanticChanges = []) {
		this.semanticChanges.push(...semanticChanges);
		this.engine.createOptimisticLayer(id, (writer) =>
			update(replicaWriter(writer))
		);
	}

	[replicaCommandProjectionDelta](id, update, semanticChanges) {
		this.replacements.push({ id, semanticChanges });
		return this.engine.replaceOptimisticLayer(id, (_reader, writer) =>
			update(replicaWriter(writer))
		);
	}

	[replicaCommandDirectProjection](id, projectionValue) {
		this.direct.push(projectionValue);
		this.engine.confirmOptimisticLayer(id, (writer) =>
			writer.writeRecord({
				key: replicaRecordKey(projectionValue.model, projectionValue.identity),
				revision: projectionValue.evidence.revision,
				incarnation: projectionValue.evidence.incarnation,
				fields: projectionValue.fields
			})
		);
	}

	markOptimisticLayerAccepted(id) {
		this.acceptances += 1;
		return this.engine.markOptimisticLayerAccepted(id);
	}

	confirmOptimisticLayer(id, update) {
		this.confirmations += 1;
		return this.engine.confirmOptimisticLayer(id, update);
	}

	rejectOptimisticLayer(id) {
		this.rejections += 1;
		return this.engine.rejectOptimisticLayer(id);
	}

	revalidate(plan) {
		this.revalidations.push(plan);
		return Promise.resolve();
	}

	record(id) {
		return this.engine.read((reader) =>
			reader.record(replicaRecordKey(Todo, [id]))
		);
	}

	layer(id) {
		return this.engine.optimisticLayerState(id);
	}

	invalidate() {
		this.authorizationGeneration += 1;
		this.#controller.abort(new Error('scope changed'));
	}
}

function replicaWriter(writer) {
	return Object.freeze({
		writeRecord(model, identity, patch) {
			writer.writeRecord({
				key: replicaRecordKey(model, identity),
				fields: patch.fields,
				unset: patch.unset,
				ifPresent: patch.ifPresent
			});
		},
		tombstoneRecord(model, identity) {
			writer.tombstoneRecord(replicaRecordKey(model, identity));
		},
		writeIndex() {},
		deleteIndex() {}
	});
}

function deferred() {
	let resolve;
	let reject;
	const promise = new Promise((yes, no) => {
		resolve = yes;
		reject = no;
	});
	return { promise, resolve, reject };
}

function token(purpose, byte) {
	return `v1.${purpose}.${Buffer.alloc(32, byte).toString('base64url')}`;
}

function tick() {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

test('artifact v1 is rejected at the public boundary', () => {
	assert.throws(
		() =>
			prepareReplicaCommand(
				{
					...artifact(),
					version: 1,
					effects: { version: 1, operations: [], fallback: 'revalidate' }
				},
				{ id: 'todo-1', title: 'preview' },
				{ commandId: COMMAND_A }
			),
		(error) =>
			error instanceof ReplicaCommandContractError &&
			error.path === 'artifact.version'
	);
});

test('coherent reload gate rejects before optimism or transport dispatch', async () => {
	const replica = new TestReplica();
	let dispatches = 0;
	let blocked = true;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch() {
				dispatches += 1;
				throw new Error('must not dispatch while reloading');
			}
		},
		{ change: artifact() },
		{
			lifecycle: {
				assertDispatchOpen() {
					if (blocked) throw new Error('reload pending');
				}
			}
		}
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'blocked' },
			{ commandId: COMMAND_A }
		),
		(error) =>
			error instanceof ReplicaCommandRuntimeError &&
			error.code === 'REPLICA_COMMAND_RELOADING'
	);
	assert.equal(dispatches, 0);
	assert.equal(replica.layer(COMMAND_A), undefined);
	blocked = false;
	runtime.dispose();
});

test('HTTP reload gate rolls back optimism and reports a reload, not an invalid receipt', async () => {
	for (const status of [503, 200]) {
		const replica = new TestReplica();
		const runtime = createReplicaCommandRuntime(replica, {
			dispatch() {
				assert.ok(replica.layer(COMMAND_A), 'optimism exists before the response');
				return { status, errors: [{message: 'application generation is reloading',
					extensions: {code: 'APPLICATION_RELOADING'}}] };
			}
		}, {change: artifact()});
		await assert.rejects(runtime.commands.change({id: 'todo-1', title: 'blocked'}, {commandId: COMMAND_A}),
			{code: status === 503 ? 'REPLICA_COMMAND_RELOADING' : 'REPLICA_COMMAND_PROTOCOL_INVALID'});
		assert.equal(replica.layer(COMMAND_A), undefined);
		runtime.dispose();
	}
});

test('actual delta rebases later optimism while same-record dispatch retains invocation order', async () => {
	const replica = new TestReplica();
	const first = deferred();
	const second = deferred();
	let dispatches = 0;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				dispatches += 1;
				return dispatches === 1 ? first.promise : second.promise;
			}
		},
		{ change: artifact() }
	);
	const a = runtime.commands.change(
		{ id: 'todo-1', title: 'preview-a' },
		{ commandId: COMMAND_A }
	);
	const b = runtime.commands.change(
		{ id: 'todo-1', title: 'preview-b' },
		{ commandId: COMMAND_B }
	);
	assert.equal(replica.record('todo-1').fields.title, 'preview-b');
	await tick();
	assert.equal(dispatches, 1);

	first.resolve(
		envelope(
			{
				commandId: COMMAND_A,
				mutationField: 'changeTodo',
				operationHash: HASH_A,
				variables: { input: { id: 'todo-1', title: 'preview-a' } }
			},
			{ actualTitle: 'actual-a' }
		)
	);
	const receipt = await a;
	assert.equal(receipt.metadata.expects.length, 1);
	assert.equal(replica.layer(COMMAND_A), 'accepted');
	assert.equal(replica.record('todo-1').fields.title, 'preview-b');
	await tick();
	assert.equal(dispatches, 2);

	second.resolve(
		envelope(
			{
				commandId: COMMAND_B,
				mutationField: 'changeTodo',
				operationHash: HASH_A,
				variables: { input: { id: 'todo-1', title: 'preview-b' } }
			},
			{ state: 'rejected', projection: false, obligations: 0 }
		)
	);
	await assert.rejects(b, { code: 'REPLICA_COMMAND_REJECTED' });
	assert.equal(replica.record('todo-1').fields.title, 'actual-a');
	assert.equal(replica.replacements.length, 1);
	runtime.dispose();
});

test('rejecting an earlier create suppresses a dependent conditional patch', async () => {
	const replica = new TestReplica();
	const createResult = deferred();
	const patchResult = deferred();
	const independentResult = deferred();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				if (request.commandName === 'todo.upsert') {
					return request.commandId === COMMAND_C
						? independentResult.promise
						: createResult.promise;
				}
				return patchResult.promise;
			}
		},
		{
			create: artifact(),
			patch: artifact({
				name: 'todo.patch',
				mutationField: 'patchTodo',
				operation: 'patch'
			})
		}
	);
	const create = runtime.commands.create(
		{ id: 'dependent', title: 'create' },
		{ commandId: COMMAND_A }
	);
	const patch = runtime.commands.patch(
		{ id: 'dependent', title: 'patch' },
		{ commandId: COMMAND_B }
	);
	const independent = runtime.commands.create(
		{ id: 'independent', title: 'safe' },
		{ commandId: COMMAND_C }
	);
	assert.equal(replica.record('dependent').fields.title, 'patch');
	assert.equal(replica.record('independent').fields.title, 'safe');

	createResult.resolve(
		envelope(
			{
				commandId: COMMAND_A,
				mutationField: 'changeTodo',
				operationHash: HASH_A,
				variables: { input: { id: 'dependent', title: 'create' } }
			},
			{ state: 'rejected', projection: false }
		)
	);
	await assert.rejects(create, { code: 'REPLICA_COMMAND_REJECTED' });
	assert.equal(replica.record('dependent'), undefined);
	assert.equal(replica.record('independent').fields.title, 'safe');

	runtime.dispose();
	await assert.rejects(patch, { code: 'REPLICA_COMMAND_DISPOSED' });
	await assert.rejects(independent, { code: 'REPLICA_COMMAND_DISPOSED' });
});

test('status replay is idempotent only for byte-identical actual metadata', async () => {
	const replica = new TestReplica();
	let statusCalls = 0;
	let actual;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						command: commandMetadata(request, {
							state: 'in_progress',
							projection: false
						})
					})
				),
			status(request) {
				statusCalls += 1;
				const commandRequest = {
					commandId: request.commandId,
					mutationField: 'changeTodo',
					operationHash: HASH_A,
					variables: { input: { id: 'todo-1', title: 'preview' } }
				};
				actual ??= commandMetadata(commandRequest, {
					actualTitle: 'actual'
				});
				const metadata =
					statusCalls < 3
						? actual
						: commandMetadata(commandRequest, {
								actualTitle: 'changed'
							});
				return Promise.resolve(statusEnvelope(request, metadata));
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return (
				error instanceof ReplicaCommandRuntimeError &&
				error.code === 'REPLICA_COMMAND_OUTCOME_PENDING'
			);
		}
	);
	assert.equal((await recovery.status()).state, 'succeeded_pending_projection');
	assert.equal(replica.record('todo-1').fields.title, 'actual');
	assert.equal((await recovery.status()).state, 'succeeded_pending_projection');
	await assert.rejects(recovery.status(), {
		code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
	});
	runtime.dispose();
});

test('status causation is validated before actual delta mutation and rolls back preview', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						command: commandMetadata(request, {
							state: 'in_progress',
							projection: false
						})
					})
				),
			status(request) {
				const commandRequest = {
					commandId: request.commandId,
					mutationField: 'changeTodo',
					operationHash: HASH_A,
					variables: { input: { id: 'todo-1', title: 'preview' } }
				};
				return Promise.resolve(
					statusEnvelope(
						request,
						commandMetadata(commandRequest, {
							causationId: 'cause:wrong',
							actualTitle: 'must-not-apply'
						})
					)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return error.code === 'REPLICA_COMMAND_OUTCOME_PENDING';
		}
	);
	await assert.rejects(recovery.status(), {
		code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
	});
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(replica.replacements.length, 0);
	runtime.dispose();
});

test('draining lifecycle status revalidates without applying old-scope delta and retires only after refresh', async () => {
	const replica = new TestReplica();
	const terminalRefresh = deferred();
	let refreshCalls = 0;
	replica.revalidate = (plan) => {
		replica.revalidations.push(plan);
		refreshCalls += 1;
		return refreshCalls === 1
			? Promise.resolve()
			: terminalRefresh.promise;
	};
	let commandRequest;
	let statusCalls = 0;
	const disposition = (state) => ({
		commandId: COMMAND_A,
		causationId: `cause:${COMMAND_A}`,
		state,
		consistency: COMMAND_CONSISTENCY.EVENTUAL,
		projectionDisposition: 'revalidate',
		expects: [],
		observations: [],
		records: []
	});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				commandRequest = request;
				return Promise.resolve(
					envelope(request, {
						command: commandMetadata(request, {
							state: 'in_progress',
							projection: false
						})
					})
				);
			},
			status(request) {
				statusCalls += 1;
				assert.equal(request.commandId, commandRequest.commandId);
				return Promise.resolve(
					statusEnvelope(
						request,
						disposition(
							statusCalls === 1
								? 'succeeded_pending_projection'
								: 'atomic'
						)
					)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return error.code === 'REPLICA_COMMAND_OUTCOME_PENDING';
		}
	);
	assert.equal(replica.replacements.length, 0);
	assert.equal(replica.record('todo-1').fields.title, 'preview');

	const pendingStatus = await recovery.status();
	assert.equal(pendingStatus.state, 'succeeded_pending_projection');
	assert.equal(replica.replacements.length, 0);
	assert.notEqual(replica.layer(COMMAND_A), undefined);
	await tick();
	assert.equal(refreshCalls, 1);
	assert.notEqual(replica.layer(COMMAND_A), undefined);

	let terminalSettled = false;
	const terminalStatus = recovery.status().then((status) => {
		terminalSettled = true;
		return status;
	});
	await tick();
	assert.equal(refreshCalls, 2);
	assert.equal(terminalSettled, false);
	assert.equal(replica.replacements.length, 0);
	assert.notEqual(replica.layer(COMMAND_A), undefined);

	terminalRefresh.resolve();
	assert.equal((await terminalStatus).state, 'atomic');
	assert.equal(replica.replacements.length, 0);
	assert.equal(replica.layer(COMMAND_A), undefined);
	assert.equal(replica.record('todo-1'), undefined);
	runtime.dispose();
});

test('generated Draining command handles a fresh succeeded response through current revalidation then retirement', async () => {
	const replica = new TestReplica();
	const terminalRefresh = deferred();
	replica.scope = Object.freeze({
		protocolVersion: 1,
		schemaHash: GENERATED_DRAINING_COMMAND.protocol.schemaHash,
		authorizationGeneration: 'auth-1',
		cacheScope: CACHE_SCOPE
	});
	replica.revalidate = (plan) => {
		replica.revalidations.push(plan);
		return terminalRefresh.promise;
	};
	const generatedStatus = Object.freeze({
		...STATUS,
		protocol: Object.freeze({
			...GENERATED_DRAINING_COMMAND.protocol,
			operation: HASH_D
		})
	});
	const terminalMetadata = (commandId) => ({
		commandId,
		causationId: `cause:${commandId}`,
		state: 'succeeded',
		consistency: COMMAND_CONSISTENCY.EVENTUAL,
		projectionDisposition: 'revalidate',
		expects: [],
		observations: [],
		records: []
	});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				return Promise.resolve({
					status: 200,
					data: {
						[request.mutationField]: {
							todo_id: request.variables.input.todo_id
						}
					},
					extensions: {
						distributed: {
							protocolVersion: 1,
							schemaHash: GENERATED_DRAINING_COMMAND.protocol.schemaHash,
							authorizationGeneration: 'auth-1',
							cacheScope: CACHE_SCOPE,
							operation: request.operationHash,
							trustedPresets: [],
							command: terminalMetadata(request.commandId)
						}
					}
				});
			},
			status(request) {
				return Promise.resolve({
					status: 200,
					data: { commandStatus: { state: 'succeeded' } },
					extensions: {
						distributed: {
							protocolVersion: 1,
							schemaHash: GENERATED_DRAINING_COMMAND.protocol.schemaHash,
							authorizationGeneration: 'auth-1',
							cacheScope: CACHE_SCOPE,
							operation: request.operationHash,
							trustedPresets: [],
							command: terminalMetadata(request.commandId)
						}
					}
				});
			}
		},
		{ complete: GENERATED_DRAINING_COMMAND },
		{ status: generatedStatus }
	);

	const receipt = await runtime.commands.complete(
		{ todo_id: 'todo-1' },
		{ commandId: COMMAND_A }
	);
	assert.equal(receipt.state, 'succeeded');
	assert.equal(receipt.metadata.projection, undefined);
	assert.equal(replica.replacements.length, 0);
	assert.deepEqual(replica.revalidations, [
		GENERATED_DRAINING_COMMAND.revalidation
	]);
	assert.equal(replica.confirmations, 0);
	assert.notEqual(replica.layer(COMMAND_A), undefined);

	let terminalSettled = false;
	const terminalStatus = receipt.status().then((status) => {
		terminalSettled = true;
		return status;
	});
	await tick();
	assert.equal(terminalSettled, false);
	terminalRefresh.resolve();
	assert.equal((await terminalStatus).state, 'succeeded');
	assert.equal(terminalSettled, true);
	assert.equal(replica.confirmations, 1);
	assert.equal(replica.layer(COMMAND_A), undefined);
	assert.equal(replica.replacements.length, 0);
	runtime.dispose();
});

test('draining lifecycle live frames keep polling while pending and retire only after terminal refresh', async () => {
	const replica = new TestReplica();
	const terminalRefresh = deferred();
	const backgroundErrors = [];
	let commandRequest;
	let terminalObserved = false;
	let terminalRefreshAttempts = 0;
	let statusCalls = 0;
	replica.revalidate = (plan) => {
		replica.revalidations.push(plan);
		if (terminalObserved) {
			terminalRefreshAttempts += 1;
			if (terminalRefreshAttempts === 1) {
				return Promise.reject(new Error('first terminal refresh failed'));
			}
			return terminalRefresh.promise;
		}
		return Promise.resolve();
	};
	const disposition = (state) => ({
		commandId: commandRequest.commandId,
		causationId: `cause:${commandRequest.commandId}`,
		state,
		consistency: COMMAND_CONSISTENCY.EVENTUAL,
		projectionDisposition: 'revalidate',
		expects: [],
		observations: [],
		records: []
	});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				commandRequest = request;
				return Promise.resolve(
					envelope(request, { actualTitle: 'accepted' })
				);
			},
			status(request) {
				statusCalls += 1;
				return Promise.resolve(
					statusEnvelope(
						request,
						disposition('succeeded_pending_projection')
					)
				);
			}
		},
		{ change: artifact() },
		{
			status: STATUS,
			onBackgroundError: (error) => backgroundErrors.push(error)
		}
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	let projectedSettled = false;
	const projected = receipt.projected.then((outcome) => {
		projectedSettled = true;
		return outcome;
	});
	assert.equal(replica.replacements.length, 1);
	assert.equal(replica.record('todo-1').fields.title, 'accepted');
	assert.notEqual(replica.layer(COMMAND_A), undefined);

	runtime.observeResult({
		extensions: envelope(commandRequest, {
			command: disposition('succeeded_pending_projection')
		}).extensions
	});
	await new Promise((resolve) => setTimeout(resolve, 40));
	assert.ok(statusCalls >= 1);
	assert.equal(projectedSettled, false);
	assert.equal(replica.replacements.length, 1);
	assert.notEqual(replica.layer(COMMAND_A), undefined);
	assert.deepEqual(backgroundErrors, []);

	terminalObserved = true;
	runtime.observeResult({
		extensions: envelope(commandRequest, {
			command: disposition('atomic')
		}).extensions
	});
	await tick();
	assert.equal(terminalRefreshAttempts, 1);
	assert.equal(projectedSettled, false);
	assert.equal(replica.replacements.length, 1);
	assert.notEqual(replica.layer(COMMAND_A), undefined);
	assert.equal(backgroundErrors.length, 1);
	assert.equal(backgroundErrors[0].message, 'first terminal refresh failed');
	assert.notEqual(
		backgroundErrors[0].code,
		'REPLICA_COMMAND_PROTOCOL_INVALID'
	);

	runtime.observeResult({
		extensions: envelope(commandRequest, {
			command: disposition('atomic')
		}).extensions
	});
	await tick();
	assert.equal(terminalRefreshAttempts, 2);
	assert.equal(projectedSettled, false);
	assert.equal(replica.replacements.length, 1);
	assert.notEqual(replica.layer(COMMAND_A), undefined);
	assert.equal(backgroundErrors.length, 1);

	terminalRefresh.resolve();
	assert.equal((await projected).state, 'atomic');
	assert.equal(replica.replacements.length, 1);
	assert.equal(replica.layer(COMMAND_A), undefined);
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(backgroundErrors.length, 1);
	runtime.dispose();
});

test('an invalid initial result does not poison corrected status recovery', async () => {
	const modeled = modeledArtifactWithAuditArm();
	const replica = new TestReplica();
	let commandRequest;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				commandRequest = request;
				return Promise.resolve(
					envelope(request, {
						bindingId: `pb1:sha256:${'9'.repeat(64)}`,
						obligationModel: Audit.id,
						mutation: {
							op: 'upsert',
							scope: scope(
								{
									type: 'string',
									value: request.variables.input.id
								},
								Audit.id
							),
							fields: [
								{
									field: 'title',
									value: {
										type: 'string',
										value: 'invalid initial'
									}
								}
							],
							replace: ['title']
						}
					})
				);
			},
			status(request) {
				return Promise.resolve(
					statusEnvelope(request, commandMetadata(commandRequest))
				);
			}
		},
		{ change: modeled },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return error.code === 'REPLICA_COMMAND_PROTOCOL_INVALID';
		}
	);
	/*
	 * The initial protocol failure correctly retired the old preview. Recreate
	 * only the cache seam so this regression isolates the status tracker's
	 * authority to accept corrected server metadata on retry.
	 */
	replica.createOptimisticLayer(COMMAND_A, () => undefined);
	assert.equal(
		(await recovery.status()).state,
		'succeeded_pending_projection'
	);
	assert.equal(replica.replacements.length, 1);
	runtime.dispose();
});

test('live causation is validated before actual delta mutation and rejects its layer', async () => {
	const replica = new TestReplica();
	let request;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(candidate) {
				request = candidate;
				return Promise.resolve(envelope(candidate, { actualTitle: 'accepted' }));
			}
		},
		{ change: artifact() }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	const projected = assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
	});
	runtime.observeResult({
		extensions: envelope(request, {
			causationId: 'cause:wrong',
			actualTitle: 'must-not-apply'
		}).extensions
	});
	await projected;
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(replica.replacements.length, 1);
	runtime.dispose();
});

test('live command state cannot regress before actual projection mutation', async () => {
	const replica = new TestReplica();
	let request;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(candidate) {
				request = candidate;
				return Promise.resolve(envelope(candidate));
			}
		},
		{ change: artifact() }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	const projected = assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
	});
	runtime.observeResult({
		extensions: envelope(request, {
			command: commandMetadata(request, {
				state: 'in_progress',
				projection: false
			})
		}).extensions
	});
	await projected;
	assert.equal(replica.replacements.length, 1);
	assert.equal(replica.record('todo-1'), undefined);
	runtime.dispose();
});

for (const statusState of ['atomic', 'succeeded_pending_projection']) {
	test(`live terminal progression ${
		statusState === 'atomic'
			? 'permits an idempotent status replay'
			: 'rejects a later status regression'
	}`, async () => {
		const replica = new TestReplica();
		let request;
		let liveMetadata;
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch(candidate) {
					request = candidate;
					return Promise.resolve(envelope(candidate));
				},
				status(statusRequest) {
					return Promise.resolve(
						statusEnvelope(statusRequest, {
							...liveMetadata,
							state: statusState
						})
					);
				}
			},
			{ change: artifact() },
			{ status: STATUS }
		);
		const receipt = await runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		);
		liveMetadata = Object.freeze({
			...receipt.metadata,
			state: 'atomic'
		});
		const projected =
			statusState === 'atomic'
				? receipt.projected
				: assert.rejects(receipt.projected, {
						code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
					});
		runtime.observeResult({
			extensions: envelope(request, {
				command: liveMetadata
			}).extensions
		});
		if (statusState === 'atomic') {
			assert.equal((await receipt.status()).state, 'atomic');
			assert.equal((await projected).state, 'atomic');
		} else {
			await assert.rejects(receipt.status(), {
				code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
			});
			await projected;
		}
		runtime.dispose();
	});
}

test('terminal exact projection status settles without command-triggered revalidation', async () => {
	const replica = new TestReplica();
	let pendingMetadata;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				pendingMetadata = commandMetadata(request, {
					actualTitle: 'accepted'
				});
				return Promise.resolve(
					envelope(request, { command: pendingMetadata })
				);
			},
			status(request) {
				const terminalMetadata = Object.freeze({
					...pendingMetadata,
					state: 'atomic',
					observations: Object.freeze(
						pendingMetadata.expects.map((expectation) =>
							Object.freeze({
								...expectation,
								causationId: pendingMetadata.causationId
							})
						)
					)
				});
				return Promise.resolve(
					statusEnvelope(request, terminalMetadata)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);

	assert.equal((await receipt.status()).state, 'atomic');
	assert.equal((await receipt.projected).state, 'atomic');
	assert.deepEqual(replica.revalidations, []);
	assert.equal(replica.layer(COMMAND_A), 'accepted');
	assert.equal(replica.record('todo-1').fields.title, 'accepted');
	runtime.dispose();
});

test('invalid live progression cannot poison a later valid status transition', async () => {
	const replica = new TestReplica();
	let request;
	let projectedMetadata;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(candidate) {
				request = candidate;
				return Promise.resolve(envelope(candidate));
			},
			status(statusRequest) {
				return Promise.resolve(
					statusEnvelope(statusRequest, projectedMetadata)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	projectedMetadata = Object.freeze({
		...receipt.metadata,
		state: 'atomic'
	});
	const projected = assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
	});
	runtime.observeResult({
		extensions: envelope(request, {
			command: {
				...receipt.metadata,
				state: 'rejected'
			}
		}).extensions
	});
	await projected;
	assert.equal((await receipt.status()).state, 'atomic');
	runtime.dispose();
});

test('terminal live deltas never replace optimism before rollback', async () => {
	const replica = new TestReplica();
	let request;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(candidate) {
				request = candidate;
				return Promise.resolve(envelope(candidate, { actualTitle: 'accepted' }));
			}
		},
		{ change: artifact() }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	const projected = assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_PROJECTION_FAILED'
	});
	runtime.observeResult({
		extensions: envelope(request, {
			state: 'projection_failed',
			actualTitle: 'must-not-apply'
		}).extensions
	});
	await projected;
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(replica.replacements.length, 1);
	runtime.dispose();
});

test('an allowed unpreviewed event arm can authoritatively replace the preview', async () => {
	const modeled = structuredClone(artifact());
	const event = { id: 'event-2', name: 'todo.corrected', version: 1 };
	modeled.projection.eventSet.push(event);
	modeled.projection.capabilities.arms.push({
		event,
		projection_ref: 0,
		arm: 'todo_patch',
		partition: { kind: 'unit' },
		mutations: [
			{
				kind: 'record',
				model: Todo.id,
				key: ['id'],
				fields: ['title'],
				replace: [],
				upsert: false,
				patch: true,
				delete: false
			}
		]
	});
	const replica = new TestReplica();
	replica.engine.batch((writer) =>
		writer.writeRecord({
			key: replicaRecordKey(Todo, ['todo-1']),
			revision: 1,
			fields: { __typename: Todo.id, id: 'todo-1', title: 'base' }
		})
	);
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						operation: 'patch',
						actualTitle: 'server-correction'
					})
				)
		},
		{ change: modeled }
	);
	await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(replica.record('todo-1').fields.title, 'server-correction');
	assert.equal(replica.replacements.length, 1);
	runtime.dispose();
});

test('explicit revalidation includes actual unpreviewed target models', async () => {
	const modeled = modeledArtifactWithAuditArm();
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						obligations: 0,
						revalidate: true,
						mutation: {
							op: 'upsert',
							scope: scope(
								{
									type: 'string',
									value: request.variables.input.id
								},
								Audit.id
							),
							fields: [
								{
									field: 'title',
									value: {
										type: 'string',
										value: 'authoritative audit'
									}
								}
							],
							replace: ['title']
						}
					})
				)
		},
		{ change: modeled }
	);
	const receipt = await runtime.commands.change(
		{ id: 'audit-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(receipt.projected, undefined);
	await tick();
	assert.deepEqual(replica.revalidations[0].models, [Audit.id, Todo.id]);
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('actual deltas outside every selected-arm capability fail closed', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						operation: 'patch',
						actualTitle: 'forged'
					})
				)
		},
		{ change: artifact() }
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		{ code: 'REPLICA_COMMAND_PROTOCOL_INVALID' }
	);
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(replica.replacements.length, 0);
	runtime.dispose();
});

test('surface, digest, scope, causation, and expiry mismatches fail before mutation', async () => {
	for (const override of [
		{ surface: { kind: 'role', name: 'admin' } },
		{ bindingId: `pb1:sha256:${'9'.repeat(64)}` },
		{ cacheScope: token('cache-scope', 9) },
		{ protocolHash: HASH_D },
		{ authorizationGeneration: 'auth-wrong' },
		{ envelopeAuthorizationGeneration: 'auth-wrong' }
	]) {
		const replica = new TestReplica();
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch: (request) =>
					Promise.resolve(envelope(request, override))
			},
			{ change: artifact() }
		);
		await assert.rejects(
			runtime.commands.change(
				{ id: 'todo-1', title: 'preview' },
				{ commandId: COMMAND_A }
			),
			{ code: 'REPLICA_COMMAND_PROTOCOL_INVALID' }
		);
		assert.equal(replica.record('todo-1'), undefined);
		assert.equal(replica.replacements.length, 0);
		runtime.dispose();
	}
});

test('status and live ingress enforce authorization generation before mutation', async () => {
	{
		const replica = new TestReplica();
		let commandRequest;
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch(request) {
					commandRequest = request;
					return Promise.resolve(
						envelope(request, {
							command: commandMetadata(request, {
								state: 'in_progress',
								projection: false
							})
						})
					);
				},
				status(request) {
					return Promise.resolve(
						statusEnvelope(
							request,
							commandMetadata(commandRequest),
							'auth-wrong'
						)
					);
				}
			},
			{ change: artifact() },
			{ status: STATUS }
		);
		let recovery;
		await assert.rejects(
			runtime.commands.change(
				{ id: 'todo-1', title: 'preview' },
				{ commandId: COMMAND_A }
			),
			(error) => {
				recovery = error.recovery;
				return error.code === 'REPLICA_COMMAND_OUTCOME_PENDING';
			}
		);
		await assert.rejects(recovery.status(), {
			code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
		});
		assert.equal(replica.replacements.length, 0);
		runtime.dispose();
	}

	{
		const replica = new TestReplica();
		let request;
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch(candidate) {
					request = candidate;
					return Promise.resolve(envelope(candidate));
				}
			},
			{ change: artifact() }
		);
		const receipt = await runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		);
		const projected = assert.rejects(receipt.projected, {
			code: 'REPLICA_COMMAND_PROTOCOL_INVALID'
		});
		runtime.observeResult({
			extensions: envelope(request, {
				envelopeAuthorizationGeneration: 'auth-wrong'
			}).extensions
		});
		assert.equal(replica.layer(COMMAND_A), 'accepted');
		runtime.observeResult({
			extensions: envelope(request, {
				authorizationGeneration: 'auth-wrong'
			}).extensions
		});
		await projected;
		assert.equal(replica.replacements.length, 1);
		assert.equal(replica.record('todo-1'), undefined);
		runtime.dispose();
	}
});

test('zero, one, and many obligations are server-derived and never predicted keys', async () => {
	for (const count of [0, 1, 3]) {
		const replica = new TestReplica();
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch: (request) =>
					Promise.resolve(envelope(request, { obligations: count }))
			},
			{ change: artifact() }
		);
		const receipt = await runtime.commands.change(
			{ id: `todo-${count}`, title: 'preview' },
			{ commandId: [COMMAND_A, COMMAND_B, COMMAND_C][count === 3 ? 2 : count] }
		);
		assert.equal(receipt.metadata.expects.length, count);
		assert.equal(receipt.projected === undefined, count === 0);
		if (count === 0) {
			await tick();
			assert.equal(replica.revalidations.length, 0);
			assert.equal(replica.layer(receipt.commandId), 'accepted');
		}
		runtime.dispose();
	}
});

test('delete remains a provisional tombstone while its causal obligation is pending', async () => {
	const replica = new TestReplica();
	replica.engine.batch((writer) =>
		writer.writeRecord({
			key: replicaRecordKey(Todo, ['todo-1']),
			revision: 1,
			fields: { __typename: Todo.id, id: 'todo-1', title: 'base' }
		})
	);
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(envelope(request, { operation: 'delete' }))
		},
		{
			remove: artifact({
				name: 'todo.delete',
				operation: 'delete'
			})
		}
	);
	const receipt = await runtime.commands.remove(
		{ id: 'todo-1', title: 'unused' },
		{ commandId: COMMAND_A }
	);
	assert.equal(replica.record('todo-1'), undefined);
	assert.equal(replica.layer(COMMAND_A), 'accepted');
	assert.equal(receipt.projected instanceof Promise, true);
	runtime.dispose();
});

test('direct Atomic results retain the canonical record-clock path', async () => {
	const replica = new TestReplica();
	const directArtifact = directProjectionArtifact();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				const metadata = {
					commandId: request.commandId,
					causationId: `cause:${request.commandId}`,
					state: 'atomic',
					consistency: COMMAND_CONSISTENCY.ATOMIC,
					expects: [],
					observations: [],
					records: [
						{
							model: Todo.id,
							scopeToken: token('record-scope', 7),
							incarnation: '1',
							revision: '2',
							tombstone: false
						}
					]
				};
				return Promise.resolve(
					envelope(request, {
						command: metadata,
						data: {
							[request.mutationField]: {
								id: request.variables.input.id,
								title: 'canonical'
							}
						}
					})
				);
			}
		},
		{ project: directArtifact }
	);
	const receipt = await runtime.commands.project(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(receipt.state, 'atomic');
	assert.equal(replica.direct.length, 1);
	assert.equal(replica.record('todo-1').fields.title, 'canonical');
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('authorization rollover aborts the captured generation and rolls back preview', async () => {
	const replica = new TestReplica();
	const pending = deferred();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: () => pending.promise },
		{ change: artifact() }
	);
	const command = runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(replica.record('todo-1').fields.title, 'preview');
	replica.invalidate();
	await assert.rejects(command, { code: 'REPLICA_COMMAND_SCOPE_INVALIDATED' });
	assert.equal(replica.record('todo-1'), undefined);
	runtime.dispose();
});

test('pre-abort and disposal cancel work without leaking optimistic layers', async () => {
	const replica = new TestReplica();
	const pending = deferred();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: () => pending.promise },
		{ change: artifact() }
	);
	const aborted = new AbortController();
	aborted.abort(new Error('caller cancelled'));
	await assert.rejects(
		runtime.commands.change(
			{ id: 'never', title: 'never' },
			{ commandId: COMMAND_A, signal: aborted.signal }
		),
		{ code: 'REPLICA_COMMAND_ABORTED' }
	);
	assert.equal(replica.layer(COMMAND_A), undefined);

	const inFlight = runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_B }
	);
	assert.equal(replica.layer(COMMAND_B), 'optimistic');
	runtime.dispose();
	await assert.rejects(inFlight, { code: 'REPLICA_COMMAND_DISPOSED' });
	assert.equal(replica.layer(COMMAND_B), undefined);
});

for (const operation of [
	'link',
	'unlink',
	'invalidate_model',
	'invalidate_relationship'
]) {
	test(`${operation} remains a semantic projection change`, async () => {
		const replica = new TestReplica();
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch: (request) =>
					Promise.resolve(
						envelope(request, {
							operation,
							...(
								operation === 'invalidate_model' ||
								operation === 'invalidate_relationship'
									? { obligations: 0 }
									: {}
							)
						})
					)
			},
			{
				change: artifact({
					name: `todo.${operation}`,
					operation
				})
			}
		);
		await runtime.commands.change(
			{ id: 'todo-1', title: 'unused' },
			{ commandId: COMMAND_A }
		);
		assert.deepEqual(
			replica.semanticChanges.map(({ kind }) => kind),
			[
				operation === 'link' || operation === 'unlink'
					? operation
					: 'invalidate'
			]
		);
		assert.equal(replica.record('todo-1'), undefined);
		runtime.dispose();
	});
}

test('transport retries reuse one frozen prepared command unit', async () => {
	const replica = new TestReplica();
	const requests = [];
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				requests.push(request);
				if (requests.length === 1) return Promise.reject(new Error('retry'));
				return Promise.resolve(envelope(request));
			}
		},
		{ change: artifact() }
	);
	await runtime.commands.change(
		{ id: 'todo-1', title: 'once' },
		{ commandId: COMMAND_A, transportRetries: 1 }
	);
	assert.equal(requests.length, 2);
	assert.equal(requests[0].commandId, requests[1].commandId);
	assert.equal(requests[0].variables, requests[1].variables);
	assert.equal(replica.semanticChanges.length, 0);
	runtime.dispose();
});

test('generated status reads coalesce while one exact read is in flight', async () => {
	const replica = new TestReplica();
	const statusResult = deferred();
	let statusCalls = 0;
	let metadata;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				metadata = commandMetadata(request);
				return Promise.resolve(envelope(request, { command: metadata }));
			},
			status(request) {
				statusCalls += 1;
				return statusResult.promise.then(() =>
					statusEnvelope(request, metadata)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A }
	);
	const first = receipt.status();
	const second = receipt.status();
	assert.equal(first, second);
	assert.equal(statusCalls, 1);
	statusResult.resolve();
	assert.equal((await first).state, 'succeeded_pending_projection');
	runtime.dispose();
});

test('ambiguous dispatch exposes generated recovery and retains optimism', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: () => Promise.reject(new Error('ambiguous')),
			status: () => Promise.reject(new Error('not read'))
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return error.code === 'REPLICA_COMMAND_TRANSPORT_AMBIGUOUS';
		}
	);
	assert.equal(typeof recovery.status, 'function');
	assert.equal(replica.layer(COMMAND_A), 'optimistic');
	assert.deepEqual(runtime.pendingCommandIds(), [COMMAND_A]);
	runtime.dispose();
});

test('terminal status rolls back only its tracked optimistic layer', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						command: commandMetadata(request, {
							state: 'in_progress',
							projection: false
						})
					})
				),
			status(request) {
				const commandRequest = {
					commandId: request.commandId,
					mutationField: 'changeTodo',
					operationHash: HASH_A,
					variables: { input: { id: 'todo-1', title: 'preview' } }
				};
				return Promise.resolve(
					statusEnvelope(
						request,
						commandMetadata(commandRequest, { state: 'rejected' })
					)
				);
			}
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	let recovery;
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) => {
			recovery = error.recovery;
			return error.code === 'REPLICA_COMMAND_OUTCOME_PENDING';
		}
	);
	assert.deepEqual(runtime.pendingCommandIds(), [COMMAND_A]);
	assert.equal((await recovery.status()).state, 'rejected');
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('caller abort after acceptance only bounds its projected awaitable', async () => {
	const replica = new TestReplica();
	const caller = new AbortController();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: (request) => Promise.resolve(envelope(request)) },
		{ change: artifact() }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A, signal: caller.signal }
	);
	caller.abort(new Error('caller stopped waiting'));
	await assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_ABORTED'
	});
	assert.equal(replica.layer(COMMAND_A), 'accepted');
	runtime.dispose();
});

test('required revalidation requires a replica coordinator at construction', () => {
	const replica = new TestReplica();
	replica.revalidate = undefined;
	assert.throws(
		() =>
			createReplicaCommandRuntime(
				replica,
				{ dispatch: () => Promise.reject(new Error('unused')) },
				{
					change: artifact({
						modeled: false,
						revalidate: true
					})
				}
			),
		/required revalidation plan/
	);
});

test('every modeled projection requires a coordinator before dispatch or layers', () => {
	const replica = new TestReplica();
	replica.revalidate = undefined;
	const modeled = modeledArtifactWithAuditArm();
	let dispatches = 0;
	assert.throws(
		() =>
			createReplicaCommandRuntime(
				replica,
				{
					dispatch(request) {
						dispatches += 1;
						return Promise.resolve(
							envelope(request, {
								obligations: 0,
								mutation: {
									op: 'upsert',
									scope: scope(
										{
											type: 'string',
											value: request.variables.input.id
										},
										Audit.id
									),
									fields: [
										{
											field: 'title',
											value: {
												type: 'string',
												value: 'widened actual'
											}
										}
									],
									replace: ['title']
								}
							})
						);
					}
				},
				{ change: modeled }
			),
		/generated modeled projection/
	);
	assert.equal(dispatches, 0);
	assert.equal(replica.layer(COMMAND_A), undefined);
	assert.deepEqual(replica.semanticChanges, []);
});

test('unmodeled direct Atomic commands do not require a revalidation coordinator', () => {
	const replica = new TestReplica();
	replica.revalidate = undefined;
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: () => Promise.reject(new Error('unused')) },
		{ project: directProjectionArtifact() }
	);
	runtime.dispose();
});

test('generated status artifacts require a status transport at construction', () => {
	const replica = new TestReplica();
	assert.throws(
		() =>
			createReplicaCommandRuntime(
				replica,
				{ dispatch: () => Promise.reject(new Error('unused')) },
				{ change: artifact() },
				{ status: STATUS }
			),
		/requires transport.status/
	);
});

test('disposing aborts an in-flight generated status read', async () => {
	const replica = new TestReplica();
	const statusResult = deferred();
	let metadata;
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				metadata = commandMetadata(request);
				return Promise.resolve(envelope(request, { command: metadata }));
			},
			status: () => statusResult.promise
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A }
	);
	const status = receipt.status();
	runtime.dispose();
	await assert.rejects(status, { code: 'REPLICA_COMMAND_DISPOSED' });
	assert.equal(replica.layer(COMMAND_A), undefined);
});

test('authority invalidation aborts an in-flight generated status read', async () => {
	const replica = new TestReplica();
	const statusResult = deferred();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) => Promise.resolve(envelope(request)),
			status: () => statusResult.promise
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A }
	);
	const status = receipt.status();
	replica.invalidate();
	await assert.rejects(status, {
		code: 'REPLICA_COMMAND_SCOPE_INVALIDATED'
	});
	runtime.dispose();
});

test('a throwing background reporter cannot reject successful command work', async () => {
	const replica = new TestReplica();
	replica.revalidate = () => Promise.reject(new Error('refresh failed'));
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(envelope(request, { obligations: 0 }))
		},
		{ change: artifact() },
		{
			onBackgroundError() {
				throw new Error('reporter failed');
			}
		}
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A }
	);
	assert.equal(receipt.state, 'succeeded_pending_projection');
	await tick();
	runtime.dispose();
});

test('dotted generated names become deeply frozen command namespaces', () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: () => Promise.reject(new Error('unused')) },
		{
			'todos.create': artifact({ name: 'todos.create' }),
			'todos.admin.remove': artifact({
				name: 'todos.admin.remove',
				operation: 'delete'
			})
		}
	);
	assert.equal(typeof runtime.commands.todos.create, 'function');
	assert.equal(typeof runtime.commands.todos.admin.remove, 'function');
	assert.equal(Object.isFrozen(runtime.commands.todos.admin), true);
	assert.equal(Object.isFrozen(runtime.commands.todos), true);
	runtime.dispose();
});

test('generated command namespaces reject prefix collisions', () => {
	const replica = new TestReplica();
	assert.throws(() =>
		createReplicaCommandRuntime(
			replica,
			{ dispatch: () => Promise.reject(new Error('unused')) },
			{
				todos: artifact({ name: 'todos' }),
				'todos.create': artifact({ name: 'todos.create' })
			}
		)
	);
});

test('post-dispatch protocol failure retains recovery while rolling back its layer', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						command: commandMetadata(request, {
							bindingId: `pb1:sha256:${'9'.repeat(64)}`
						})
					})
				),
			status: () => Promise.reject(new Error('unused'))
		},
		{ change: artifact() },
		{ status: STATUS }
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		(error) =>
			error.code === 'REPLICA_COMMAND_PROTOCOL_INVALID' &&
			typeof error.recovery?.status === 'function'
	);
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

for (const state of ['projection_failed', 'expired']) {
	test(`${state} dispatch rolls back optimism and requests canonical recovery`, async () => {
		const replica = new TestReplica();
		const runtime = createReplicaCommandRuntime(
			replica,
			{
				dispatch: (request) =>
					Promise.resolve(envelope(request, { state }))
			},
			{ change: artifact() }
		);
		await assert.rejects(
			runtime.commands.change(
				{ id: 'todo-1', title: 'preview' },
				{ commandId: COMMAND_A }
			),
			{ code: 'REPLICA_COMMAND_PROJECTION_FAILED' }
		);
		assert.equal(replica.layer(COMMAND_A), undefined);
		assert.equal(replica.revalidations.length, 1);
		runtime.dispose();
	});
}

test('GraphQL errors attached to a successful command receipt fail closed', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						errors: [{ message: 'unexpected resolver error' }]
					})
				)
		},
		{ change: artifact() }
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		{ code: 'REPLICA_COMMAND_PROTOCOL_INVALID' }
	);
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('malformed command output rolls back its optimistic layer', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(envelope(request, { data: { changeTodo: null } }))
		},
		{ change: artifact() }
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		{ code: 'REPLICA_COMMAND_PROTOCOL_INVALID' }
	);
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('onSucceeded failures are reported without changing a valid receipt', async () => {
	const replica = new TestReplica();
	const reported = [];
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: (request) => Promise.resolve(envelope(request)) },
		{ change: artifact() },
		{ onBackgroundError: (error) => reported.push(error) }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{
			commandId: COMMAND_A,
			onSucceeded() {
				throw new Error('observer failed');
			}
		}
	);
	assert.equal(receipt.commandId, COMMAND_A);
	assert.equal(reported.length, 1);
	runtime.dispose();
});

test('disposing an accepted command rejects its causal lifecycle and layer', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: (request) => Promise.resolve(envelope(request)) },
		{ change: artifact() }
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'accepted' },
		{ commandId: COMMAND_A }
	);
	const projected = assert.rejects(receipt.projected, {
		code: 'REPLICA_COMMAND_DISPOSED'
	});
	runtime.dispose();
	await projected;
	assert.equal(replica.layer(COMMAND_A), undefined);
});

test('commands fail before optimism when no authoritative scope is available', async () => {
	const replica = new TestReplica();
	replica.scope = undefined;
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: () => Promise.reject(new Error('must not dispatch')) },
		{ change: artifact() }
	);
	await assert.rejects(
		runtime.commands.change(
			{ id: 'todo-1', title: 'preview' },
			{ commandId: COMMAND_A }
		),
		{ code: 'REPLICA_COMMAND_AUTHORITY_UNAVAILABLE' }
	);
	assert.equal(replica.layer(COMMAND_A), undefined);
	runtime.dispose();
});

test('Atomic direct path normalizes a new record with its generated model identity', async () => {
	const { replica, runtime } = await directProjectionRuntime();
	assert.deepEqual(replica.record('todo-1').fields, {
		__typename: Todo.id,
		id: 'todo-1',
		title: 'canonical'
	});
	runtime.dispose();
});

for (const scenario of ['older-row', 'newer-row', 'newer-tombstone']) {
	test(`Atomic direct path fences ${scenario}`, async () => {
		const { replica, runtime } = await directProjectionRuntime();
		replica.engine.batch((writer) => {
			if (scenario === 'newer-tombstone') {
				writer.tombstoneRecord(
					replicaRecordKey(Todo, ['todo-1']),
					'3'
				);
				return;
			}
			writer.writeRecord({
				key: replicaRecordKey(Todo, ['todo-1']),
				revision: scenario === 'older-row' ? '1' : '3',
				fields: {
					__typename: Todo.id,
					id: 'todo-1',
					title: scenario
				}
			});
		});
		const record = replica.record('todo-1');
		if (scenario === 'older-row') {
			assert.equal(record.fields.title, 'canonical');
		} else if (scenario === 'newer-row') {
			assert.equal(record.fields.title, 'newer-row');
		} else {
			assert.equal(record, undefined);
		}
		runtime.dispose();
	});
}

test('Atomic with portable preview IR does not require an eventual projection-delta response', async () => {
	// Direct commands may export the same mutation program for `.applies`
	// previews. The response still seals via confirmDirectProjection only —
	// no async projection-delta envelope.
	const replica = new TestReplica();
	const directWithPreview = Object.freeze({
		...artifact({
			name: 'todo.project',
			consistency: COMMAND_CONSISTENCY.ATOMIC,
			modeled: true,
			directProjection: Object.freeze({
				topology: Object.freeze({
					version: 1,
					name: 'todos',
					digest: HASH_D
				}),
				model: Todo.id,
				identityFields: Todo.identityFields,
				changeEpoch: 'todos-v1'
			})
		}),
		output: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: Todo.id,
				fields: Object.freeze([scalar('id', 'ID'), scalar('title')])
			})
		})
	});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				return Promise.resolve(
					envelope(request, {
						command: {
							commandId: request.commandId,
							causationId: `cause:${request.commandId}`,
							state: 'atomic',
							consistency: COMMAND_CONSISTENCY.ATOMIC,
							expects: [],
							observations: [],
							records: [
								{
									model: Todo.id,
									scopeToken: token('record-scope', 7),
									incarnation: '1',
									revision: '2',
									tombstone: false
								}
							]
						},
						data: {
							[request.mutationField]: {
								id: 'todo-1',
								title: 'from-handler'
							}
						}
					})
				);
			}
		},
		{ project: directWithPreview }
	);
	const receipt = await runtime.commands.project(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(receipt.result.title, 'from-handler');
	assert.equal((await receipt.projected).state, 'atomic');
	assert.equal(replica.record('todo-1').fields.title, 'from-handler');
	runtime.dispose();
});

test('link obligations may name any server-selected affected model', async () => {
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: (request) =>
				Promise.resolve(
					envelope(request, {
						operation: 'link',
						obligationModel: 'RelationshipSummary'
					})
				)
		},
		{
			change: artifact({
				name: 'todo.link',
				operation: 'link'
			})
		}
	);
	const receipt = await runtime.commands.change(
		{ id: 'todo-1', title: 'unused' },
		{ commandId: COMMAND_A }
	);
	assert.equal(
		receipt.metadata.projection.obligations[0].model,
		'RelationshipSummary'
	);
	runtime.dispose();
});

test('equivalent event arms may fan out to the same read-model mutation', async () => {
	const modeled = structuredClone(artifact());
	const event = { id: 'event-2', name: 'todo.duplicated', version: 1 };
	modeled.projection.eventSet.push(event);
	modeled.projection.capabilities.arms.push({
		...structuredClone(modeled.projection.capabilities.arms[0]),
		event,
		arm: 'todo_upsert_duplicate'
	});
	const replica = new TestReplica();
	const runtime = createReplicaCommandRuntime(
		replica,
		{ dispatch: (request) => Promise.resolve(envelope(request)) },
		{ change: modeled }
	);
	await runtime.commands.change(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	assert.equal(replica.replacements.length, 1);
	assert.equal(replica.record('todo-1').fields.title, 'preview');
	runtime.dispose();
});

async function directProjectionRuntime() {
	const replica = new TestReplica();
	const directArtifact = Object.freeze({
		...artifact({
			name: 'todo.project',
			consistency: COMMAND_CONSISTENCY.ATOMIC,
			modeled: false,
			directProjection: Object.freeze({
				topology: Object.freeze({
					version: 1,
					name: 'todos',
					digest: HASH_D
				}),
				model: Todo.id,
				identityFields: Todo.identityFields,
				changeEpoch: 'todos-v1'
			})
		}),
		output: Object.freeze({
			kind: 'object',
			definition: Object.freeze({
				name: Todo.id,
				fields: Object.freeze([scalar('id', 'ID'), scalar('title')])
			})
		})
	});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch(request) {
				return Promise.resolve(
					envelope(request, {
						command: {
							commandId: request.commandId,
							causationId: `cause:${request.commandId}`,
							state: 'atomic',
							consistency: COMMAND_CONSISTENCY.ATOMIC,
							expects: [],
							observations: [],
							records: [
								{
									model: Todo.id,
									scopeToken: token('record-scope', 7),
									incarnation: '1',
									revision: '2',
									tombstone: false
								}
							]
						},
						data: {
							[request.mutationField]: {
								id: 'todo-1',
								title: 'canonical'
							}
						}
					})
				);
			}
		},
		{ project: directArtifact }
	);
	await runtime.commands.project(
		{ id: 'todo-1', title: 'preview' },
		{ commandId: COMMAND_A }
	);
	return { replica, runtime };
}

function lazyRuntime(replica, transport, load, options) {
	return createLazyReplicaCommandRuntime(replica, transport,
		{ commands: { 'todo.upsert': { operationHash: HASH_A, hasInput: true } }, status: STATUS },
		load ?? (async () => ({ entries: { 'todo.upsert': artifact() } })), options);
}

test('lazy commands register authority before loading, share imports, and snapshot invocation inputs', async () => {
	const replica = new TestReplica();
	let registrations = 0;
	const register = replica[replicaCommandAuthority].bind(replica);
	replica[replicaCommandAuthority] = (contract) => { registrations++; return register(contract); };
	const gate = deferred();
	let loads = 0;
	const requests = [];
	const runtime = lazyRuntime(replica, {
		dispatch: async request => { requests.push(request); return envelope(request); },
		status: async () => { throw new Error('not requested'); }
	}, () => { loads++; return gate.promise; });
	assert.equal(registrations, 1);
	assert.equal(loads, 0);
	assert.deepEqual(runtime.pendingCommandIds(), []);
	const input = { id: 'todo-1', title: 'original' };
	const first = runtime.commands.todo.upsert(input, { commandId: COMMAND_A });
	input.title = 'edited while loading';
	const second = runtime.commands.todo.upsert({ id: 'todo-2', title: 'second' }, { commandId: COMMAND_B });
	await tick();
	assert.equal(loads, 1);
	assert.equal(requests.length, 0);
	gate.resolve({ entries: { 'todo.upsert': artifact() } });
	const receipts = await Promise.all([first, second]);
	assert.deepEqual(receipts.map(r => r.commandId), [COMMAND_A, COMMAND_B]);
	assert.equal(requests[0].variables.input.title, 'original');
	assert.equal(replica.record('todo-1').fields.title, 'original');
	await runtime.preload();
	assert.equal(loads, 1);
	runtime.dispose();
});

for (const failure of ['scope', 'dispose', 'abort']) {
	test(`lazy commands reject ${failure} while the chunk is still loading without dispatch`, async () => {
		const replica = new TestReplica();
		const gate = deferred();
		const abort = new AbortController();
		let dispatches = 0;
		const runtime = lazyRuntime(replica, {
			dispatch: async request => { dispatches++; return envelope(request); }, status: async () => {}
		}, () => gate.promise);
		const pending = runtime.commands.todo.upsert({ id: 'todo-1', title: 'stale' }, { commandId: COMMAND_A, signal: abort.signal });
		if (failure === 'scope') replica.invalidate();
		if (failure === 'dispose') runtime.dispose();
		if (failure === 'abort') abort.abort();
		await assert.rejects(pending, { code: failure === 'scope' ? 'REPLICA_COMMAND_SCOPE_INVALIDATED' : failure === 'dispose' ? 'REPLICA_COMMAND_DISPOSED' : 'REPLICA_COMMAND_ABORTED' });
		assert.equal(dispatches, 0);
		gate.resolve({ entries: { 'todo.upsert': artifact() } });
		await tick();
		assert.equal(dispatches, 0);
		assert.equal(replica.record('todo-1'), undefined);
		runtime.dispose();
	});
}

test('lazy chunk failures retry without dispatch and reject mismatched inventory', async () => {
	const replica = new TestReplica();
	let loads = 0;
	let dispatches = 0;
	const runtime = lazyRuntime(replica, {
		dispatch: async request => { dispatches++; return envelope(request); }, status: async () => {}
	}, async () => {
		if (++loads === 1) throw new Error('chunk unavailable');
		return { entries: { 'todo.upsert': artifact() } };
	});
	await assert.rejects(runtime.preload(), /chunk unavailable/);
	await runtime.preload();
	assert.equal(loads, 2);
	assert.equal(dispatches, 0);
	runtime.dispose();
	const mismatch = lazyRuntime(replica, { status: async () => {} }, async () => ({ entries: { 'todo.upsert': { ...artifact(), operationHash: HASH_D } } }));
	await assert.rejects(mismatch.preload(), /does not match its catalog/);
	mismatch.dispose();
});

test('lazy commands preserve transport retry identity, status recovery and pending IDs', async () => {
	const replica = new TestReplica();
	const requests = [];
	const runtime = lazyRuntime(replica, {
		dispatch(request) { requests.push(request); return Promise.reject(new Error('ambiguous')); },
		status(request) {
			return Promise.resolve(statusEnvelope(request, commandMetadata(requests[0], { state: 'rejected', projection: false })));
		}
	});
	let recovery;
	await assert.rejects(runtime.commands.todo.upsert({ id: 'todo-1', title: 'preview' }, { commandId: COMMAND_A, transportRetries: 1 }), error => {
		recovery = error.recovery;
		return error.code === 'REPLICA_COMMAND_TRANSPORT_AMBIGUOUS';
	});
	assert.equal(requests.length, 2);
	assert.equal(requests[0].variables, requests[1].variables);
	assert.deepEqual(runtime.pendingCommandIds(), [COMMAND_A]);
	assert.equal(replica.layer(COMMAND_A), 'optimistic');
	await runtime.preload();
	assert.equal((await recovery.status()).state, 'rejected');
	assert.equal(replica.record('todo-1'), undefined);
	assert.deepEqual(runtime.pendingCommandIds(), []);
	runtime.dispose();
});

test('lazy commands recheck the reload dispatch gate after import', async () => {
	const replica = new TestReplica();
	const gate = deferred();
	let reloading = false;
	let dispatches = 0;
	const runtime = lazyRuntime(replica, { dispatch: async () => { dispatches++; }, status: async () => {} }, () => gate.promise,
		{ lifecycle: { assertDispatchOpen() { if (reloading) throw new Error('reload'); } } });
	const pending = runtime.commands.todo.upsert({ id: 'todo-1', title: 'preview' }, { commandId: COMMAND_A });
	reloading = true;
	gate.resolve({ entries: { 'todo.upsert': artifact() } });
	await assert.rejects(pending, { code: 'REPLICA_COMMAND_RELOADING' });
	assert.equal(dispatches, 0);
	assert.equal(replica.record('todo-1'), undefined);
	runtime.dispose();
});

test('lazy no-input commands keep options, cancellation, command ID and callbacks in the first argument', async () => {
	const replica = new TestReplica();
	const gate = deferred();
	const noInput = { ...artifact({ modeled: false, revalidate: true }), name: 'todo.ping', input: { kind: 'none' } };
	let calls = 0;
	let succeeded = 0;
	const runtime = createLazyReplicaCommandRuntime(replica, {
		dispatch: async request => { calls++; assert.equal(request.commandId, COMMAND_A); return envelope(request, { command: commandReceipt({ commandId: request.commandId, causationId: `cause:${request.commandId}`, state: 'succeeded', consistency: 'eventual', expects: [], observations: [], records: [] }) }); },
		status: async () => {}
	}, { commands: { 'todo.ping': { operationHash: HASH_A, hasInput: false } }, status: STATUS }, () => gate.promise);
	const abort = new AbortController();
	const cancelled = runtime.commands.todo.ping({ signal: abort.signal, commandId: COMMAND_B });
	abort.abort();
	await assert.rejects(cancelled, { code: 'REPLICA_COMMAND_ABORTED' });
	gate.resolve({ entries: { 'todo.ping': noInput } });
	await runtime.preload();
	const receipt = await runtime.commands.todo.ping({ commandId: COMMAND_A, onSucceeded() { succeeded++; } });
	assert.equal(receipt.commandId, COMMAND_A);
	assert.equal(calls, 1);
	assert.equal(succeeded, 1);
	runtime.dispose();
});

test('calls made after preload cannot overtake earlier commands waiting for the same import', async () => {
	const replica = new TestReplica();
	const gate = deferred();
	const requests = [];
	const runtime = lazyRuntime(replica, {
		dispatch: async request => { requests.push(request.commandId); return envelope(request); }, status: async () => {}
	}, () => gate.promise);
	const preload = runtime.preload();
	// This continuation runs before the cold callers' import continuations.
	const warm = preload.then(() => runtime.commands.todo.upsert({ id: 'todo-3', title: 'third' }, { commandId: COMMAND_C }));
	const first = runtime.commands.todo.upsert({ id: 'todo-1', title: 'first' }, { commandId: COMMAND_A });
	const second = runtime.commands.todo.upsert({ id: 'todo-2', title: 'second' }, { commandId: COMMAND_B });
	gate.resolve({ entries: { 'todo.upsert': artifact() } });
	await Promise.all([first, second, warm]);
	assert.deepEqual(requests, [COMMAND_A, COMMAND_B, COMMAND_C]);
	runtime.dispose();
});
