import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import test from 'node:test';

import { build } from 'esbuild';

import { FakeScheduler } from './fixtures/game-harness.mjs';

import {
	createDistributedReplica,
	createReplicaCommandRuntime,
	createReplicaDevelopmentCapability,
	createReplicaDiagnostics,
	inspectReplicaCommandArtifact,
	inspectReplicaOperationArtifact
} from '../dist/replica/index.js';
import {
	COMMAND_CONSISTENCY,
	COMMAND_STATE,
	commandReceipt
} from './fixtures/command-protocol.mjs';

const Todo = Object.freeze({
	id: 'Todo',
	identityFields: Object.freeze(['id'])
});

const DIAGNOSTIC_SCHEMA_HASH = `sha256:${'b'.repeat(64)}`;

const Todos = Object.freeze({
	id: 'Todos.v1',
	document: 'query Todos { todos { id title } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: DIAGNOSTIC_SCHEMA_HASH,
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'Todos.v1',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: Object.freeze({
		version: 2,
		limits: Object.freeze({
			maxDepth: 8,
			maxBoolWidth: 32,
			maxInList: 64
		}),
		variables: Object.freeze({}),
		defaults: Object.freeze({}),
		inputs: Object.freeze({})
	}),
	source: Object.freeze({
		path: 'src/routes/todos/+page.graphql',
		line: 2,
		column: 1
	}),
	live: Object.freeze({
		id: 'Todos.live.v1',
		document: 'subscription TodosLive { todos { id title } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'todos',
			field: 'todos',
			cardinality: 'many',
			nullable: false,
			dependencies: Object.freeze(['todos']),
			coverage: Object.freeze({ kind: 'complete' }),
			selection: Object.freeze({
				typename: 'todo',
				storage: Object.freeze({
					kind: 'normalized',
					model: 'Todo',
					identityFields: Object.freeze(['id'])
				}),
				members: Object.freeze([
					Object.freeze({
						kind: 'scalar',
						responseKey: 'id',
						field: 'id',
						codec: 'ID',
						nullable: false
					}),
					Object.freeze({
						kind: 'scalar',
						responseKey: 'title',
						field: 'title',
						codec: 'String',
						nullable: false
					}),
					Object.freeze({
						kind: 'scalar',
						responseKey: '_distributed_revision',
						field: '__row_revision',
						codec: 'BigInt',
						nullable: false,
						expose: false
					})
				])
			})
		})
	])
});

function todosFrame(revision, data, errors = undefined) {
	const position = String(revision);
	const rows = Array.isArray(data?.todos) ? data.todos : [];
	return {
		data,
		...(errors === undefined ? {} : { errors }),
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: DIAGNOSTIC_SCHEMA_HASH,
				authorizationGeneration: 'auth-1',
				cacheScope: 'scope:diagnostics-user',
				operation: Todos.id,
				trustedPresets: [],
				snapshot: {
					scopeToken: 'snapshot:todos',
					recordsComplete: true,
					indexesComparable: true,
					records: rows.map((row, index) => ({
						path: ['todos', String(index)],
						model: Todo.id,
						scopeToken: `record:todo:${row.id}`,
						incarnation: '1',
						revision: String(row._distributed_revision),
						tombstone: false
					})),
					indexes: [
						{
							projection: 'todos',
							scopeToken: 'index:todos',
							position
						}
					],
					observations: []
				}
			}
		}
	};
}

const commandArtifact = Object.freeze({
	version: 2,
	name: 'todo.rename',
	mutationField: 'renameTodo',
	document: 'mutation RenameTodo { renameTodo }',
	operationHash: `sha256:${'a'.repeat(64)}`,
	protocol: Object.freeze({
		version: 1,
		schemaHash: DIAGNOSTIC_SCHEMA_HASH,
		protocolHash: `sha256:${'c'.repeat(64)}`,
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: `sha256:${'a'.repeat(64)}`,
		trustedPresets: Object.freeze([
			Object.freeze({ name: 'x-private-claim-name', codec: 'string' })
		])
	}),
	input: Object.freeze({
		kind: 'object',
		definition: Object.freeze({
			name: 'RenameTodoInput',
			fields: Object.freeze([
				Object.freeze({
					name: 'id',
					typeName: 'ID',
					nullable: false,
					list: false,
					itemNullable: false,
					codec: 'string'
				})
			])
		})
	}),
	output: Object.freeze({
		kind: 'object',
		definition: Object.freeze({
			name: 'RenameTodoResult',
			fields: Object.freeze([
				Object.freeze({
					name: 'accepted',
					typeName: 'Boolean',
					nullable: false,
					list: false,
					itemNullable: false,
					codec: 'boolean'
				})
			])
		})
	}),
	consistency: COMMAND_CONSISTENCY.EVENTUAL,
	projection: Object.freeze({
		version: 2,
		deltaWireVersion: 1,
		projectionProgramVersion: 2,
		operationSemanticsVersion: 1,
		projections: Object.freeze([
			Object.freeze({
				programId: `pp1:sha256:${'1'.repeat(64)}`,
				bindingId: `pb1:sha256:${'2'.repeat(64)}`,
				epoch: 'todos-v1',
				programIrVersion: 1,
				operationSemanticsVersion: 1
			})
		]),
		eventSet: Object.freeze([
			Object.freeze({
				id: 'todo-renamed',
				name: 'todo.renamed',
				version: 1
			})
		]),
		capabilities: Object.freeze({
			version: 1,
			arms: Object.freeze([
				Object.freeze({
					event: Object.freeze({
						id: 'todo-renamed',
						name: 'todo.renamed',
						version: 1
					}),
					projection_ref: 0,
					arm: 'todo_renamed',
					partition: Object.freeze({ kind: 'unit' }),
					mutations: Object.freeze([
						Object.freeze({
							kind: 'record',
							model: 'Todo',
							key: Object.freeze(['id']),
							fields: Object.freeze(['owner_id', 'title']),
							replace: Object.freeze([]),
							upsert: false,
							patch: true,
							delete: false
						})
					])
				})
			])
		}),
		preview: Object.freeze({
			version: 1,
			occurrences: Object.freeze([
				Object.freeze({
					ordinal: 0,
					event: Object.freeze({
						id: 'todo-renamed',
						name: 'todo.renamed',
						version: 1
					})
				})
			]),
			operations: Object.freeze([
				Object.freeze({
					occurrence_ordinal: 0,
					projection_refs: Object.freeze([0]),
					mutation: Object.freeze({
						op: 'patch',
						scope: Object.freeze({
							partition: Object.freeze({ kind: 'unit' }),
							model: 'Todo',
							key: Object.freeze([
								Object.freeze({
									ordinal: 0,
									field: 'id',
									value: Object.freeze({
										kind: 'input',
										path: Object.freeze(['id'])
									})
								})
							])
						}),
						set: Object.freeze([
							Object.freeze({
								field: 'owner_id',
								value: Object.freeze({
									kind: 'trusted_preset',
									name: 'x-private-claim-name',
									codec: 'string'
								})
							}),
							Object.freeze({
								field: 'title',
								value: Object.freeze({
									kind: 'constant',
									value: Object.freeze({
										type: 'string',
										value: 'must-never-appear-in-inspection'
									})
								})
							})
						]),
						unset: Object.freeze([]),
						if_present: true
					})
				})
			]),
			recoveries: Object.freeze([])
		}),
		fallback: 'revalidate'
	}),
	trustedPresets: Object.freeze([
		Object.freeze({ name: 'x-private-claim-name', codec: 'string' })
	]),
	revalidation: Object.freeze({
		version: 1,
		required: false,
		dependencies: Object.freeze(['todos']),
		models: Object.freeze(['Todo']),
		relationships: Object.freeze([])
	})
});

function diagnosticState(overrides = {}) {
	return Object.freeze({
		scope: Object.freeze({ generation: 0, established: false }),
		records: Object.freeze([]),
		indexes: Object.freeze([]),
		layers: Object.freeze([]),
		receipts: Object.freeze([]),
		...overrides
	});
}

test('default snapshots redact identities, arguments, field values, and scope material', () => {
	const diagnostics = createReplicaDiagnostics({ now: () => 10 });
	diagnostics.update(
		diagnosticState({
			scope: Object.freeze({
				generation: 1,
				established: true,
				protocolVersion: 1,
				schemaHash: `sha256:${'d'.repeat(64)}`
			}),
			records: Object.freeze([
				Object.freeze({
					key: 'record:Todo:["private-todo-id"]',
					model: 'Todo',
					revision: '4',
					incarnation: '1',
					tombstone: false,
					presentFields: Object.freeze(['id', 'title']),
					presentLinks: Object.freeze([]),
					values: Object.freeze({
						id: 'private-todo-id',
						title: 'private title'
					})
				})
			]),
			indexes: Object.freeze([
				Object.freeze({
					key: 'index:private-owner-filter',
					revision: '8',
					records: Object.freeze(['record:Todo:["private-todo-id"]']),
					complete: true,
					deleted: false,
					field: 'todos',
					argumentNames: Object.freeze(['where']),
					arguments: Object.freeze({
						where: Object.freeze({ owner_id: 'private-user-id' })
					}),
					coverage: Object.freeze({ kind: 'complete' }),
					dependencies: Object.freeze(['todos'])
				})
			]),
			layers: Object.freeze([
				Object.freeze({
					id: 'private-command-id',
					sequence: 1,
					state: 'accepted',
					recordChanges: 1,
					indexChanges: 0,
					semanticChanges: 1
				})
			]),
			receipts: Object.freeze([
				Object.freeze({
					commandId: 'private-command-id',
					state: 'succeeded_pending_projection',
					expectations: Object.freeze([
						Object.freeze({
							projection: 'todos',
							model: 'Todo',
							observed: false
						})
					])
				})
			])
		})
	);

	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.mode, 'redacted');
	assert.equal(snapshot.records[0].key, 'record#1');
	assert.equal(snapshot.indexes[0].key, 'index#1');
	assert.deepEqual(snapshot.indexes[0].records, ['record#1']);
	assert.deepEqual(snapshot.indexes[0].argumentNames, ['where']);
	assert.equal(snapshot.indexes[0].arguments, undefined);
	assert.equal(snapshot.records[0].values, undefined);
	assert.equal(snapshot.layers[0].id, snapshot.receipts[0].commandId);
	const encoded = JSON.stringify(snapshot);
	for (const secret of [
		'private-todo-id',
		'private title',
		'private-user-id',
		'private-owner-filter',
		'private-command-id'
	]) {
		assert.equal(encoded.includes(secret), false, secret);
	}
	assert.equal(encoded.includes('cacheScope'), false);
});

test('snapshot identity is stable for Svelte stores and external-store readers', () => {
	const diagnostics = createReplicaDiagnostics({ now: () => 10 });
	const initial = diagnostics.snapshot();
	assert.equal(diagnostics.snapshot(), initial);

	const svelteEmissions = [];
	const { subscribe, getSnapshot } = diagnostics;
	const unsubscribe = subscribe((snapshot) =>
		svelteEmissions.push(snapshot)
	);
	const externalSnapshot = getSnapshot();
	assert.equal(svelteEmissions[0], externalSnapshot);

	diagnostics.event({ kind: 'gc', records: 0 });
	const changed = diagnostics.snapshot();
	assert.notEqual(changed, initial);
	assert.equal(diagnostics.snapshot(), changed);
	assert.equal(svelteEmissions.at(-1), changed);

	unsubscribe();
	diagnostics.event({ kind: 'gc', records: 0 });
	assert.equal(svelteEmissions.length, 2);
});

test('field values require both a development capability and an explicit redactor', () => {
	const capability = createReplicaDevelopmentCapability();
	assert.throws(
		() =>
			createReplicaDiagnostics({
				fieldValues: {
					capability: Object.freeze({ version: 1 }),
					allow: () => true,
					redact: (value) => value
				}
			}),
		/development capability/
	);
	const diagnostics = createReplicaDiagnostics({
		development: capability,
		fieldValues: {
			capability,
			allow: ({ field }) => field === 'title',
			redact: () => '[support-redacted]'
		}
	});
	const replica = createDistributedReplica({ diagnostics });
	replica.writeResult(
		Todos,
		{},
		todosFrame(1, {
				todos: [
					{
						id: 'todo-1',
						title: 'private title',
						_distributed_revision: 1
					}
				]
			}),
		'network'
	);
	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.mode, 'development');
	assert.match(snapshot.records[0].key, /todo-1/);
	assert.deepEqual(snapshot.records[0].values, {
		title: '[support-redacted]'
	});
	assert.equal(JSON.stringify(snapshot).includes('private title'), false);
});

test('free-form reasons require a development capability and explicit redactor', () => {
	const secret = 'private claim value embedded by application code';
	assert.throws(
		() =>
			createReplicaDiagnostics({
				reasons: {
					capability: Object.freeze({ version: 1 }),
					redact: () => '[safe]'
				}
			}),
		/development capability/
	);
	const capability = createReplicaDevelopmentCapability();
	const diagnostics = createReplicaDiagnostics({
		development: capability,
		reasons: {
			capability,
			redact: (_reason, context) => `[support:${context.kind}]`
		}
	});
	diagnostics.update(
		diagnosticState({
			indexes: Object.freeze([
				Object.freeze({
					key: 'index:private',
					revision: '1',
					records: Object.freeze([]),
					complete: false,
					deleted: false,
					staleReason: secret
				})
			])
		})
	);
	diagnostics.event({
		kind: 'index-decision',
		index: 'index:private',
		decision: 'stale',
		reason: secret
	});
	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.indexes[0].staleReason, '[support:index-stale]');
	assert.equal(snapshot.events[0].reason, '[support:index-stale]');
	assert.equal(JSON.stringify(snapshot).includes(secret), false);
});

test('artifact inspection explains source, injected fields, dependencies, live plan, and effects without values', () => {
	const operation = inspectReplicaOperationArtifact(Todos);
	assert.deepEqual(operation.source, {
		path: 'src/routes/todos/+page.graphql',
		line: 2,
		column: 1
	});
	assert.deepEqual(operation.dependencies, ['todos']);
	assert.deepEqual(operation.live, { operation: 'Todos.live.v1' });
	assert.deepEqual(operation.injectedFields, [
		{
			path: 'todos._distributed_revision',
			responseKey: '_distributed_revision',
			field: '__row_revision'
		}
	]);
	assert.deepEqual(operation.indexes[0], {
		path: 'todos',
		field: 'todos',
		cardinality: 'many',
		dependencies: ['todos'],
		coverage: 'complete',
		filtered: false,
		ordered: false
	});
	const unsafeSource = inspectReplicaOperationArtifact(
		Object.freeze({
			...Todos,
			source: Object.freeze({
				path: '/private/user\nclaim-secret.graphql',
				line: 1,
				column: 1
			})
		})
	);
	assert.equal(unsafeSource.source, undefined);
	assert.equal(JSON.stringify(unsafeSource).includes('claim-secret'), false);
	const ambiguousWindowsSource = inspectReplicaOperationArtifact(
		Object.freeze({
			...Todos,
			source: Object.freeze({
				path: '..\\private\\claim-secret.graphql',
				line: 1,
				column: 1
			})
		})
	);
	assert.equal(ambiguousWindowsSource.source, undefined);
	assert.equal(
		JSON.stringify(ambiguousWindowsSource).includes('claim-secret'),
		false
	);

	const command = inspectReplicaCommandArtifact(commandArtifact);
	assert.deepEqual(command.effects, [
		{
			kind: 'patch',
			models: ['Todo'],
			fields: ['id', 'owner_id', 'title'],
			valueSources: ['constant', 'input', 'trusted_preset']
		}
	]);
	const encoded = JSON.stringify(command);
	assert.equal(encoded.includes('must-never-appear'), false);
	assert.equal(encoded.includes('x-private-claim-name'), false);
});

test('one diagnostics store receives both replica state and generated command artifacts', () => {
	const diagnostics = createReplicaDiagnostics();
	const replica = createDistributedReplica({ diagnostics });
	const operationHash = `sha256:${'d'.repeat(64)}`;
	const protocolTodos = Object.freeze({
		...Todos,
		id: operationHash,
		live: undefined,
		variableCodec: Object.freeze({
			version: 2,
			limits: Object.freeze({
				maxDepth: 8,
				maxBoolWidth: 32,
				maxInList: 64
			}),
			variables: Object.freeze({}),
			defaults: Object.freeze({}),
			inputs: Object.freeze({})
		}),
		protocol: Object.freeze({
			version: 1,
			schemaHash: commandArtifact.protocol.schemaHash,
			surface: commandArtifact.protocol.surface,
			operation: operationHash,
			trustedPresets: commandArtifact.protocol.trustedPresets
		})
	});
	replica.retainOperation(protocolTodos, {});
	const runtime = createReplicaCommandRuntime(
		replica,
		{
			dispatch: () => Promise.reject(new Error('not dispatched in this test'))
		},
		{ rename: commandArtifact },
		{ diagnostics }
	);

	const snapshot = diagnostics.snapshot();
	assert.deepEqual(
		snapshot.artifacts.operations.map((operation) => operation.id),
		[protocolTodos.id]
	);
	assert.deepEqual(
		snapshot.artifacts.commands.map((command) => command.name),
		[commandArtifact.name]
	);
	assert.equal(snapshot.records.length, 0);
	runtime.dispose();
});

const OtherTodos = Object.freeze({
	...Todos,
	id: 'Todos.v2',
	protocol: Object.freeze({ ...Todos.protocol, operation: 'Todos.v2' }),
	roots: Object.freeze([
		Object.freeze({
			...Todos.roots[0],
			responseKey: 'otherTodos',
			field: 'otherTodos'
		})
	])
});

test('replica integration exposes structural normalization, index, layer, receipt, rebase, and GC reasons', async () => {
	let tick = 0;
	const diagnostics = createReplicaDiagnostics({
		maxEvents: 50,
		now: () => ++tick
	});
	const scheduler = new FakeScheduler();
	const replica = createDistributedReplica({
		diagnostics,
		scheduler,
		retentionGraceMs: 10
	});
	replica.retainOperation(Todos, {});
	replica.writeResult(
		Todos,
		{},
		todosFrame(1, {
				todos: [
					{
						id: 'todo-1',
						title: 'private title',
						_distributed_revision: 1
					}
				]
			}),
		'network'
	);
	const maliciousReason =
		'claim=user-private-claim; row=todo-private-row; token=private-token';
	replica.markIndexStale({ field: 'todos' }, maliciousReason);
	replica.createOptimisticLayer('command-private-a', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic private title' }
		});
	});
	replica.createOptimisticLayer('command-private-b', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'later private title' }
		});
	});
	replica.markOptimisticLayerAccepted(
		'command-private-a',
		commandReceipt({
			commandId: 'command-private-a',
			causationId: 'private-causation',
			state: COMMAND_STATE.PENDING_PROJECTION,
			consistency: COMMAND_CONSISTENCY.EVENTUAL,
			expects: [
				{
					projection: 'todos',
					model: 'Todo',
					scopeToken: 'private-obligation-token'
				}
			],
			observations: [],
			records: []
		})
	);
	const acceptedReceipt = diagnostics
		.snapshot()
		.receipts.find(
			(receipt) => receipt.state === 'succeeded_pending_projection'
		);
	assert.equal(acceptedReceipt?.state, 'succeeded_pending_projection');
	assert.equal(acceptedReceipt?.expectations[0].projection, 'todos');
	replica.rejectOptimisticLayer('command-private-a');
	// An operation that was held and released is collected after the grace
	// period; the one still held keeps its index and record.
	replica.retainOperation(OtherTodos, {})();
	await scheduler.advance(10);

	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.records.length, 1);
	assert.equal(snapshot.indexes.length, 1);
	assert.equal(snapshot.layers.length, 1);
	assert.equal(snapshot.receipts[0].state, 'optimistic');
	assert.equal(snapshot.receipts[0].consistency, undefined);
	assert.equal(snapshot.indexes[0].staleReason, 'application-stale');
	assert.equal(snapshot.artifacts.operations[0].id, Todos.id);
	assert(snapshot.events.some((event) => event.kind === 'normalization'));
	assert(
		snapshot.events.some(
			(event) =>
				event.kind === 'index-decision' &&
				event.decision === 'maintained'
		)
	);
	assert(
		snapshot.events.some(
			(event) =>
				event.kind === 'index-decision' &&
				event.decision === 'stale' &&
				event.reason === 'application-stale'
		)
	);
	assert(
		snapshot.events.some(
			(event) => event.kind === 'layer' && event.action === 'rebased'
		)
	);
	assert(snapshot.events.some((event) => event.kind === 'receipt'));
	assert(snapshot.events.some((event) => event.kind === 'gc'));
	const encoded = JSON.stringify(snapshot);
	for (const secret of [
		'command-private-a',
		'command-private-b',
			'private-causation',
			'private-obligation-token',
			'private title',
			maliciousReason,
			'user-private-claim',
			'todo-private-row',
			'private-token'
	]) {
		assert.equal(encoded.includes(secret), false, secret);
	}
});

test('event log is bounded and a scope generation change removes cross-scope state', () => {
	let tick = 0;
	const diagnostics = createReplicaDiagnostics({
		maxEvents: 2,
		now: () => ++tick
	});
	diagnostics.inspectOperation(Todos);
	for (let index = 0; index < 3; index += 1) {
		diagnostics.event({
			kind: 'gc',
			records: index
		});
	}
	assert.deepEqual(
		diagnostics.snapshot().events.map((event) => event.sequence),
		[2, 3]
	);
	diagnostics.update(
		diagnosticState({
			scope: Object.freeze({
				generation: 2,
				established: true,
				protocolVersion: 1,
				schemaHash: `sha256:${'e'.repeat(64)}`
			})
		})
	);
	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.events.length, 0);
	assert.equal(snapshot.artifacts.operations.length, 0);
	assert.equal(snapshot.records.length, 0);
});

test('late HTTP results are recorded as authorization-fenced without merging data', async () => {
	let resolveFetch;
	let startedResolve;
	const started = new Promise((resolveStarted) => {
		startedResolve = resolveStarted;
	});
	const diagnostics = createReplicaDiagnostics();
	const replica = createDistributedReplica({
		diagnostics,
		transport: {
			fetch: () =>
				new Promise((resolveResult) => {
					resolveFetch = resolveResult;
					startedResolve();
				})
		}
	});
	const watch = replica.watch(Todos, {});
	await started;
	replica.invalidateAuthorization();
	resolveFetch(
		todosFrame(1, {
			todos: [
				{
					id: 'late-private-id',
					title: 'late private title',
					_distributed_revision: 1
				}
			]
		})
	);
	await new Promise((resolveTurn) => setImmediate(resolveTurn));
	await new Promise((resolveTurn) => setImmediate(resolveTurn));
	const snapshot = diagnostics.snapshot();
	assert.equal(snapshot.records.length, 0);
	assert(
		snapshot.events.some(
			(event) =>
				event.kind === 'response-fenced' &&
				event.transport === 'http' &&
				event.reason === 'authorization-generation'
		)
	);
	assert.equal(JSON.stringify(snapshot).includes('late-private-id'), false);
	watch.destroy();
});

test('diagnostics code is tree-shaken unless explicitly imported', async () => {
	const replicaEntry = resolve('dist/replica/index.js');
	const withoutDiagnostics = await build({
		stdin: {
			contents: `import { createDistributedReplica } from ${JSON.stringify(
				replicaEntry
			)}; console.log(createDistributedReplica);`,
			resolveDir: process.cwd(),
			sourcefile: 'without-diagnostics.mjs'
		},
		bundle: true,
		format: 'esm',
		platform: 'browser',
		target: 'es2022',
		minify: true,
		treeShaking: true,
		write: false
	});
	assert.equal(
		withoutDiagnostics.outputFiles[0].text.includes(
			'distributed-replica-diagnostics-v1'
		),
		false
	);

	const withDiagnostics = await build({
		stdin: {
			contents: `import { createReplicaDiagnostics } from ${JSON.stringify(
				replicaEntry
			)}; console.log(createReplicaDiagnostics().snapshot().marker);`,
			resolveDir: process.cwd(),
			sourcefile: 'with-diagnostics.mjs'
		},
		bundle: true,
		format: 'esm',
		platform: 'browser',
		target: 'es2022',
		minify: true,
		treeShaking: true,
		write: false
	});
	assert.equal(
		withDiagnostics.outputFiles[0].text.includes(
			'distributed-replica-diagnostics-v1'
		),
		true
	);
});
