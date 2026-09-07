import assert from 'node:assert/strict';
import test from 'node:test';

import {
	DISTRIBUTED_PROTOCOL_VERSION,
	DistributedProtocolError,
	parseDistributedProtocolEnvelope
} from '../dist/index.js';
import {
	createDistributedReplica,
	replicaRecordKey
} from '../dist/replica/index.js';
import {
	replicaCommandDirectProjection,
	replicaCommandProjectionDelta
} from '../dist/replica/command-runtime.js';
import {
	COMMAND_CONSISTENCY,
	COMMAND_STATE,
	commandReceipt
} from './fixtures/command-protocol.mjs';

const Todo = Object.freeze({
	id: 'TodoView',
	identityFields: Object.freeze(['id'])
});

const NoVariables = Object.freeze({
	version: 2,
	limits: Object.freeze({
		maxDepth: 8,
		maxBoolWidth: 256,
		maxInList: 1000
	}),
	variables: Object.freeze({}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({})
});

const Todos = Object.freeze({
	id: 'query:todos',
	document: 'query Todos { todos { id title } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: 'schema-a',
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'query:todos',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: NoVariables,
	live: Object.freeze({
		id: 'live:todos',
		document: 'subscription TodosLive { todos { id title } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'todos',
			field: 'todos',
			cardinality: 'many',
			nullable: false,
			dependencies: Object.freeze(['todos']),
			selection: Object.freeze({
				typename: Todo.id,
				storage: Object.freeze({
					kind: 'normalized',
					model: Todo.id,
					identityFields: Todo.identityFields
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
					})
				])
			})
		})
	])
});

const TodosOtherOperation = Object.freeze({
	...Todos,
	id: 'query:todos-other',
	protocol: Object.freeze({
		version: 1,
		schemaHash: 'schema-a',
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'query:todos-other',
		trustedPresets: Object.freeze([])
	}),
	live: undefined
});

const TodosOtherLiveOperation = Object.freeze({
	...TodosOtherOperation,
	live: Object.freeze({
		id: 'live:todos-other',
		document: 'subscription TodosOtherLive { todos { id title } }'
	})
});

const TodosOpen = Object.freeze({
	...Todos,
	id: 'query:todos-open',
	document: 'query TodosOpen { todos(where: {status: {_eq: "open"}}) { id title } }',
	protocol: Object.freeze({
		...Todos.protocol,
		operation: 'query:todos-open'
	}),
	live: Object.freeze({
		id: 'live:todos-open',
		document:
			'subscription TodosOpenLive { todos(where: {status: {_eq: "open"}}) { id title } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			...Todos.roots[0],
			arguments: Object.freeze({
				where: Object.freeze({
					kind: 'literal',
					value: Object.freeze({
						status: Object.freeze({ _eq: 'open' })
					})
				})
			})
		})
	])
});

/*
 * Mirrors the generated Todos artifact's material index semantics: an exact,
 * offset-backed collection whose authorization policy is server-only.
 */
const TodosServerOnly = Object.freeze({
	...TodosOtherOperation,
	id: 'query:todos-server-only',
	document:
		'query TodosServerOnly { todos(order_by: [{id: asc}]) { id title } }',
	protocol: Object.freeze({
		...TodosOtherOperation.protocol,
		operation: 'query:todos-server-only'
	}),
	roots: Object.freeze([
		Object.freeze({
			...Todos.roots[0],
			arguments: Object.freeze({
				order_by: Object.freeze({
					kind: 'literal',
					value: Object.freeze([Object.freeze({ id: 'asc' })])
				})
			}),
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				limitArgument: 'limit',
				defaultLimit: 100,
				maxLimit: 1000
			}),
			filter: Object.freeze({
				fields: Object.freeze([
					Object.freeze({
						field: 'id',
						scalar: 'ID',
						codec: 'string',
						nullable: false,
						operators: Object.freeze(['_eq'])
					}),
					Object.freeze({
						field: 'title',
						scalar: 'String',
						codec: 'string',
						nullable: false,
						operators: Object.freeze(['_eq'])
					})
				]),
				relationships: Object.freeze([]),
				rowPolicy: Object.freeze({ kind: 'server_only' })
			}),
			order: Object.freeze({
				input: Object.freeze({
					kind: 'literal',
					value: Object.freeze([Object.freeze({ id: 'asc' })])
				}),
				fields: Object.freeze([
					Object.freeze({
						field: 'id',
						scalar: 'ID',
						codec: 'string',
						nullable: false
					}),
					Object.freeze({
						field: 'title',
						scalar: 'String',
						codec: 'string',
						nullable: false
					})
				]),
				tieBreakers: Object.freeze([
					Object.freeze({
						field: 'id',
						scalar: 'ID',
						codec: 'string',
						nullable: false
					})
				])
			}),
			pagination: Object.freeze({
				kind: 'offset',
				insert: 'local',
				delete: 'local',
				reorder: 'local',
				stableUpdate: 'local'
			})
		})
	])
});

const TodosLocallyMaintainable = Object.freeze({
	...TodosServerOnly,
	id: 'query:todos-local',
	protocol: Object.freeze({
		...TodosServerOnly.protocol,
		operation: 'query:todos-local'
	}),
	roots: Object.freeze([
		Object.freeze({
			...TodosServerOnly.roots[0],
			filter: Object.freeze({
				...TodosServerOnly.roots[0].filter,
				rowPolicy: Object.freeze({ kind: 'unrestricted' })
			})
		})
	])
});

const GamesWithOwner = Object.freeze({
	id: 'query:games-with-owner',
	document: 'query GamesWithOwner { games { id owner_id owner { id name } } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: 'schema-a',
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'query:games-with-owner',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: NoVariables,
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'games',
			field: 'games',
			cardinality: 'many',
			nullable: false,
			dependencies: Object.freeze(['games']),
			selection: Object.freeze({
				typename: 'GameView',
				storage: Object.freeze({
					kind: 'normalized',
					model: 'GameView',
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
						responseKey: 'owner_id',
						field: 'owner_id',
						codec: 'ID',
						nullable: false
					}),
					Object.freeze({
						kind: 'branch',
						semantic: 'relationship',
						responseKey: 'owner',
						field: 'owner',
						cardinality: 'one',
						nullable: false,
						dependencies: Object.freeze(['games', 'users']),
						relationship: Object.freeze({
							field: 'owner',
							targetModel: 'UserView',
							kind: 'belongs_to',
							keyMapping: Object.freeze({
								kind: 'direct',
								local: Object.freeze(['owner_id']),
								remote: Object.freeze(['id'])
							}),
							maintenance: 'revalidate',
							dependencies: Object.freeze(['games', 'users'])
						}),
						selection: Object.freeze({
							typename: 'UserView',
							storage: Object.freeze({
								kind: 'normalized',
								model: 'UserView',
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
									responseKey: 'name',
									field: 'name',
									codec: 'String',
									nullable: false
								})
							])
						})
					})
				])
			})
		})
	])
});

const FeaturedGamesWithOwner = Object.freeze({
	...GamesWithOwner,
	id: 'query:featured-games-with-owner',
	document:
		'query FeaturedGamesWithOwner { featuredGames { id owner_id owner { id name } } }',
	protocol: Object.freeze({
		...GamesWithOwner.protocol,
		operation: 'query:featured-games-with-owner'
	}),
	roots: Object.freeze([
		Object.freeze({
			...GamesWithOwner.roots[0],
			responseKey: 'featuredGames',
			field: 'featured_games'
		})
	])
});

const GamesWithOwnerOtherOperation = Object.freeze({
	...GamesWithOwner,
	id: 'query:games-with-owner-other',
	protocol: Object.freeze({
		...GamesWithOwner.protocol,
		operation: 'query:games-with-owner-other'
	})
});

const GamesWithOwnerLiveOperation = Object.freeze({
	...GamesWithOwner,
	id: 'query:games-with-owner-live',
	protocol: Object.freeze({
		...GamesWithOwner.protocol,
		operation: 'query:games-with-owner-live'
	}),
	live: Object.freeze({
		id: 'live:games-with-owner',
		document:
			'subscription GamesWithOwnerLive { games { id owner_id owner { id name } } }'
	})
});

function wireFrame(options = {}) {
	const rows = options.rows ?? [{ id: 'todo-1', title: 'one' }];
	const position = options.position ?? '1';
	const projection = options.projection ?? 'todos-projector';
	const resume = {
		projection,
		position,
		token: options.resumeToken ?? `resume:${position}`
	};
	const records =
		options.records ??
		rows.map((row, index) => ({
			path: ['todos', String(index)],
			model: 'TodoView',
			scopeToken: options.recordScope ?? `record:${row.id}`,
			incarnation: options.incarnation ?? '1',
			revision: options.revision ?? position,
			tombstone: false
		}));
	const snapshot = {
		scopeToken: options.snapshotScope ?? 'snapshot:query',
		recordsComplete: options.recordsComplete ?? true,
		indexesComparable: options.indexesComparable ?? true,
		records,
		indexes:
			options.indexes ??
			(options.indexesComparable === false
				? []
				: [
						{
							projection,
							scopeToken: options.indexScope ?? 'index:query',
							position,
							resume
						}
					]),
		observations: options.observations ?? []
	};
	const live =
		options.live === undefined
			? undefined
			: {
					supported: options.live.supported ?? true,
					reset: options.live.reset ?? false,
					cursors:
						options.live.cursors ??
						(options.live.supported === false ? [] : [resume])
				};
	return {
		data: { todos: rows },
		extensions: {
			distributed: {
				protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
				schemaHash: options.schemaHash ?? 'schema-a',
				authorizationGeneration: 'auth-1',
				cacheScope: options.cacheScope ?? 'cache:a',
				operation: options.operation ?? 'query:todos',
				...(options.command === undefined
					? {}
					: { command: options.command }),
				snapshot,
				...(live === undefined ? {} : { live })
			}
		}
	};
}

function gamesFrame({
	artifact,
	responseKey,
	position,
	ownerId,
	ownerName,
	projection = 'games-owner-projector',
	operation = artifact.id,
	live,
	indexesComparable = true
}) {
	return {
		data: {
			[responseKey]: [
				{
					id: 'game-1',
					owner_id: ownerId,
					owner: { id: ownerId, name: ownerName }
				}
			]
		},
		extensions: {
			distributed: {
				protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
				schemaHash: 'schema-a',
				authorizationGeneration: 'auth-1',
				cacheScope: 'cache:a',
				operation,
				snapshot: {
					scopeToken: `snapshot:${artifact.id}`,
					recordsComplete: true,
					indexesComparable,
					records: [
						{
							path: [responseKey, '0'],
							model: 'GameView',
							scopeToken: 'record:game-1',
							incarnation: '1',
							revision: position,
							tombstone: false
						},
						{
							path: [responseKey, '0', 'owner'],
							model: 'UserView',
							scopeToken: `record:${ownerId}`,
							incarnation: '1',
							revision: position,
							tombstone: false
						}
					],
					indexes: indexesComparable
						? [
								{
									projection,
									scopeToken: 'index:games',
									position,
									resume: {
										projection,
										position,
										token: `resume:${position}`
									}
								}
							]
						: [],
					observations: []
				},
				...(live === undefined ? {} : { live })
			}
		}
	};
}

function write(replica, options = {}, source = 'network', artifact = Todos) {
	replica.writeResult(artifact, {}, wireFrame(options), source);
}

function commandMetadata(options = {}) {
	return parseDistributedProtocolEnvelope({
		protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
		schemaHash: 'schema-a',
		authorizationGeneration: 'auth-1',
		cacheScope: 'cache:a',
		operation: 'command:todo',
		command: commandReceipt({
			commandId: options.commandId ?? 'cmd-1',
			causationId: options.causationId ?? 'cause-1',
			state: options.state ?? COMMAND_STATE.PENDING_PROJECTION,
			consistency: COMMAND_CONSISTENCY.EVENTUAL,
			expects: [
				{
					projection: 'todos-projector',
					model: 'TodoView',
					scopeToken: options.expectationToken ?? 'expect:todo-1'
				}
			],
			...(options.observations === undefined
				? {}
				: { observations: options.observations })
		})
	}).command;
}

test('authorized replacement snapshots render without a causal index vector', () => {
	const replica = createDistributedReplica();
	write(replica, {
		recordsComplete: true,
		indexesComparable: false,
		rows: [{ id: 'todo-1', title: 'first authorized result' }]
	});

	const first = replica.read(Todos, {});
	assert.equal(first.status, 'ready');
	assert.equal(first.complete, true);
	assert.deepEqual(first.data.todos, [
		{ id: 'todo-1', title: 'first authorized result' }
	]);
	const firstIndex = replica.inspectIndex({ field: 'todos', arguments: {} });
	assert.equal(firstIndex.complete, true);

	write(replica, {
		recordsComplete: true,
		indexesComparable: false,
		rows: [{ id: 'todo-2', title: 'replacement result' }],
		recordScope: 'record:todo-2'
	});

	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-2', title: 'replacement result' }
	]);
	const replacementIndex = replica.inspectIndex({
		field: 'todos',
		arguments: {}
	});
	assert.notEqual(replacementIndex.revision, firstIndex.revision);
});

test('authoritative revalidation succeeds against confirmed data while a server-only optimistic index remains stale', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	const frame = (position, rows) =>
		wireFrame({
			operation: TodosServerOnly.id,
			position,
			recordsComplete: true,
			indexesComparable: false,
			rows
		});
	const watch = replica.watch(TodosServerOnly, {});
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	fetches[0].resolve(frame('1', [{ id: 'todo-1', title: 'base' }]));
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(watch.get().complete, true);

	replica.createOptimisticLayer('cmd-server-only', (writer) => {
		writer.writeRecord(Todo, 'todo-2', {
			fields: { id: 'todo-2', title: 'optimistic' }
		});
	});
	replica.markOptimisticLayerAccepted('cmd-server-only');
	assert.equal(
		watch.get().complete,
		true,
		'the structurally complete optimistic view must remain renderable'
	);
	assert.equal(watch.get().stale, true);

	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(fetches.length, 2);
	fetches[1].resolve(
		frame('2', [
			{ id: 'todo-1', title: 'base' },
			{ id: 'todo-2', title: 'atomic' }
		])
	);
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(watch.get().complete, true);
	assert.equal(watch.get().stale, true);
	assert.deepEqual(watch.get().data.todos, [
		{ id: 'todo-1', title: 'base' },
		{ id: 'todo-2', title: 'optimistic' }
	]);

	const revalidation = replica.revalidate({
		dependencies: ['todos'],
		models: ['TodoView'],
		relationships: []
	});
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(fetches.length, 3);
	fetches[2].resolve(
		frame('3', [
			{ id: 'todo-1', title: 'base' },
			{ id: 'todo-2', title: 'atomic' }
		])
	);
	await revalidation;

	assert.equal(
		watch.get().complete,
		true,
		'the visible policy-safe overlay remains renderable until command retirement'
	);
	assert.equal(watch.get().stale, true);
	replica.confirmOptimisticLayer('cmd-server-only', () => undefined);
	assert.equal(watch.get().complete, true);
	assert.equal(watch.get().stale, false);
	assert.deepEqual(watch.get().data.todos, [
		{ id: 'todo-1', title: 'base' },
		{ id: 'todo-2', title: 'atomic' }
	]);
	watch.destroy();
});

test('comparable live snapshot cannot drop an Eventual list row after confirmation', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		rows: [{ id: 'todo-1', title: 'first' }]
	});
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-1', title: 'first' }
	]);

	replica.createOptimisticLayer('cmd-eventual-post', (writer) => {
		writer.writeRecord(Todo, 'todo-2', {
			fields: { id: 'todo-2', title: 'posted' }
		});
		writer.writeIndex(
			{
				field: 'todos',
				arguments: {},
				dependencies: ['todos'],
				complete: true
			},
			[
				replicaRecordKey(Todo, 'todo-1'),
				replicaRecordKey(Todo, 'todo-2')
			]
		);
	});
	replica[replicaCommandProjectionDelta](
		'cmd-eventual-post',
		(writer) => {
			writer.writeRecord(Todo, 'todo-2', {
				fields: { id: 'todo-2', title: 'posted' }
			});
			writer.writeIndex(
				{
					field: 'todos',
					arguments: {},
					dependencies: ['todos'],
					complete: true
				},
				[
					replicaRecordKey(Todo, 'todo-1'),
					replicaRecordKey(Todo, 'todo-2')
				]
			);
		},
		[]
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'posted' }
	]);

	write(
		replica,
		{
			position: '2',
			operation: Todos.live.id,
			rows: [{ id: 'todo-1', title: 'first' }],
			live: { supported: true }
		},
		'live'
	);
	replica.confirmOptimisticLayer('cmd-eventual-post', () => undefined);

	assert.equal(replica.read(Todos, {}).complete, true);
	assert.deepEqual(
		replica.read(Todos, {}).data.todos,
		[
			{ id: 'todo-1', title: 'first' },
			{ id: 'todo-2', title: 'posted' }
		],
		'SQL @live that has not projected the confirmed Eventual row must not shrink the list'
	);

	write(
		replica,
		{
			position: '3',
			operation: Todos.live.id,
			rows: [
				{ id: 'todo-1', title: 'first' },
				{ id: 'todo-2', title: 'posted' }
			],
			live: { supported: true },
			records: [
				{
					path: ['todos', '0'],
					model: 'TodoView',
					scopeToken: 'record:todo-1',
					incarnation: '1',
					revision: '3',
					tombstone: false
				},
				{
					path: ['todos', '1'],
					model: 'TodoView',
					scopeToken: 'record:todo-2',
					incarnation: '1',
					revision: '3',
					tombstone: false
				}
			]
		},
		'live'
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'posted' }
	]);
});

test('Eventual membership fences are independent per index', () => {
	const replica = createDistributedReplica();
	const baseRows = [{ id: 'todo-1', title: 'first' }];
	write(replica, { position: '1', rows: baseRows });
	write(
		replica,
		{
			position: '1',
			operation: TodosOpen.id,
			projection: 'todos-open-projector',
			indexScope: 'index:todos-open',
			snapshotScope: 'snapshot:todos-open',
			rows: baseRows
		},
		'network',
		TodosOpen
	);
	const allTarget = {
		field: 'todos',
		arguments: {},
		dependencies: ['todos'],
		complete: true
	};
	const openTarget = {
		...allTarget,
		arguments: { where: { status: { _eq: 'open' } } }
	};
	const recordKeys = [
		replicaRecordKey(Todo, 'todo-1'),
		replicaRecordKey(Todo, 'todo-2')
	];
	replica.createOptimisticLayer('cmd-two-indexes', () => undefined);
	replica[replicaCommandProjectionDelta](
		'cmd-two-indexes',
		(writer) => {
			writer.writeRecord(Todo, 'todo-2', {
				fields: { id: 'todo-2', title: 'posted' }
			});
			writer.writeIndex(allTarget, recordKeys);
			writer.writeIndex(openTarget, recordKeys);
		},
		[]
	);
	replica.confirmOptimisticLayer('cmd-two-indexes', () => undefined);

	write(
		replica,
		{
			position: '2',
			operation: Todos.live.id,
			rows: [
				{ id: 'todo-1', title: 'first' },
				{ id: 'todo-2', title: 'posted' }
			],
			live: { supported: true }
		},
		'live'
	);
	write(
		replica,
		{
			position: '2',
			operation: TodosOpen.live.id,
			projection: 'todos-open-projector',
			indexScope: 'index:todos-open',
			snapshotScope: 'snapshot:todos-open',
			rows: [{ id: 'todo-1', title: 'first' }],
			live: { supported: true }
		},
		'live',
		TodosOpen
	);
	assert.deepEqual(replica.read(TodosOpen, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'posted' }
	]);

	write(
		replica,
		{
			position: '3',
			operation: TodosOpen.live.id,
			projection: 'todos-open-projector',
			indexScope: 'index:todos-open',
			snapshotScope: 'snapshot:todos-open',
			rows: [
				{ id: 'todo-1', title: 'first' },
				{ id: 'todo-2', title: 'posted' }
			],
			live: { supported: true }
		},
		'live',
		TodosOpen
	);
	assert.deepEqual(replica.read(TodosOpen, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'posted' }
	]);
});

test('overlapping Eventual commands retain every membership-fence owner', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		rows: [{ id: 'todo-1', title: 'first' }]
	});
	const target = {
		field: 'todos',
		arguments: {},
		dependencies: ['todos'],
		complete: true
	};
	const records = [
		replicaRecordKey(Todo, 'todo-1'),
		replicaRecordKey(Todo, 'todo-2')
	];
	for (const commandId of ['cmd-owner-a', 'cmd-owner-b']) {
		replica.createOptimisticLayer(commandId, () => undefined);
		replica[replicaCommandProjectionDelta](
			commandId,
			(writer) => {
				writer.writeRecord(Todo, 'todo-2', {
					fields: { id: 'todo-2', title: 'posted' }
				});
				writer.writeIndex(target, records);
			},
			[]
		);
	}
	replica.rejectOptimisticLayer('cmd-owner-b');
	replica.confirmOptimisticLayer('cmd-owner-a', () => undefined);
	write(
		replica,
		{
			position: '2',
			operation: Todos.live.id,
			rows: [{ id: 'todo-1', title: 'first' }],
			live: { supported: true }
		},
		'live'
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'posted' }
	]);
});

test('Atomic direct projection does not hold later complete @load membership', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		rows: [{ id: 'todo-1', title: 'first' }]
	});
	replica.createOptimisticLayer('cmd-atomic-start', (writer) => {
		writer.writeRecord(Todo, 'todo-2', {
			fields: { id: 'todo-2', title: 'started' }
		});
		writer.writeIndex(
			{
				field: 'todos',
				arguments: {},
				dependencies: ['todos'],
				complete: true
			},
			[replicaRecordKey(Todo, 'todo-1'), replicaRecordKey(Todo, 'todo-2')]
		);
	});
	replica[replicaCommandDirectProjection]('cmd-atomic-start', {
		model: Todo,
		identity: 'todo-2',
		evidence: {
			model: Todo.id,
			scopeToken: 'record:todo-2',
			incarnation: '1',
			revision: '2',
			tombstone: false
		},
		fields: { id: 'todo-2', title: 'started', __typename: Todo.id }
	});
	write(replica, {
		position: '2',
		rows: [
			{ id: 'todo-1', title: 'first' },
			{ id: 'todo-2', title: 'started' }
		],
		records: [
			{
				path: ['todos', '0'],
				model: Todo.id,
				scopeToken: 'record:todo-1',
				incarnation: '1',
				revision: '2',
				tombstone: false
			},
			{
				path: ['todos', '1'],
				model: Todo.id,
				scopeToken: 'record:todo-2',
				incarnation: '1',
				revision: '2',
				tombstone: false
			}
		]
	});
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'started' }
	]);
});

test('Atomic direct projection commits locally provable collection membership', () => {
	const replica = createDistributedReplica();
	write(
		replica,
		{
			operation: TodosLocallyMaintainable.id,
			position: '1',
			rows: [{ id: 'todo-1', title: 'first' }]
		},
		'network',
		TodosLocallyMaintainable
	);
	// Render once so the operation's compiler plan owns collection maintenance.
	replica.read(TodosLocallyMaintainable, {});
	replica.createOptimisticLayer('cmd-atomic-create', (writer) => {
		// Generated partial previews fail closed when the record is not known yet.
		writer.writeRecord(Todo, 'todo-2', {
			fields: { id: 'todo-2', title: 'preview' },
			ifPresent: true
		});
	});
	replica[replicaCommandDirectProjection]('cmd-atomic-create', {
		model: Todo,
		identity: 'todo-2',
		evidence: {
			model: Todo.id,
			scopeToken: 'record:todo-2',
			incarnation: '1',
			revision: '2',
			tombstone: false
		},
		fields: { id: 'todo-2', title: 'canonical', __typename: Todo.id }
	});

	assert.deepEqual(replica.read(TodosLocallyMaintainable, {}).data.todos, [
		{ id: 'todo-1', title: 'first' },
		{ id: 'todo-2', title: 'canonical' }
	]);
});

test('Atomic direct projection keeps unprovable collection membership stale', () => {
	const replica = createDistributedReplica();
	write(
		replica,
		{
			operation: TodosServerOnly.id,
			position: '1',
			rows: [{ id: 'todo-1', title: 'first' }]
		},
		'network',
		TodosServerOnly
	);
	replica.read(TodosServerOnly, {});
	replica.createOptimisticLayer('cmd-atomic-server-only', (writer) => {
		writer.writeRecord(Todo, 'todo-2', {
			fields: { id: 'todo-2', title: 'preview' },
			ifPresent: true
		});
	});
	replica[replicaCommandDirectProjection]('cmd-atomic-server-only', {
		model: Todo,
		identity: 'todo-2',
		evidence: {
			model: Todo.id,
			scopeToken: 'record:todo-2',
			incarnation: '1',
			revision: '2',
			tombstone: false
		},
		fields: { id: 'todo-2', title: 'canonical', __typename: Todo.id }
	});

	const snapshot = replica.read(TodosServerOnly, {});
	assert.equal(snapshot.stale, true);
	assert.deepEqual(snapshot.data.todos, [{ id: 'todo-1', title: 'first' }]);
});

test('shared non-comparable membership follows request-start order across operations', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	const first = replica.watch(Todos, {});
	const second = replica.watch(TodosOtherOperation, {});
	await Promise.resolve();
	assert.equal(fetches.length, 2);

	fetches[1].resolve(
		wireFrame({
			operation: TodosOtherOperation.id,
			recordsComplete: true,
			indexesComparable: false,
			rows: [{ id: 'todo-new', title: 'later request' }],
			recordScope: 'record:todo-new'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	fetches[0].resolve(
		wireFrame({
			operation: Todos.id,
			recordsComplete: true,
			indexesComparable: false,
			rows: [{ id: 'todo-old', title: 'slower earlier request' }],
			recordScope: 'record:todo-old'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	/*
	 * These artifacts describe the same field/arguments membership. Sharing is
	 * intentional, but local request-start order—not response arrival—decides
	 * which exact authorized replacement is newer when no server vector exists.
	 */
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-new', title: 'later request' }
	]);
	assert.deepEqual(replica.read(TodosOtherOperation, {}).data.todos, [
		{ id: 'todo-new', title: 'later request' }
	]);
	first.destroy();
	second.destroy();
});

test('non-comparable request order is atomic across shared root and nested indexes', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	replica.writeResult(
		GamesWithOwnerOtherOperation,
		{},
		gamesFrame({
			artifact: GamesWithOwnerOtherOperation,
			responseKey: 'games',
			position: '1',
			ownerId: 'user-root',
			ownerName: 'root owner'
		}),
		'network'
	);
	const rootBefore = replica.inspectIndex({ field: 'games', arguments: {} });
	const incoming = replica.watch(GamesWithOwner, {});
	const incomingRefresh = incoming.refresh();
	const nestedOwner = replica.watch(FeaturedGamesWithOwner, {});
	await Promise.resolve();
	assert.equal(fetches.length, 2);

	fetches[1].resolve(
		gamesFrame({
			artifact: FeaturedGamesWithOwner,
			responseKey: 'featuredGames',
			position: '3',
			ownerId: 'user-nested',
			ownerName: 'later nested owner',
			indexesComparable: false
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	fetches[0].resolve(
		gamesFrame({
			artifact: GamesWithOwner,
			responseKey: 'games',
			position: '2',
			ownerId: 'user-incoming',
			ownerName: 'earlier whole graph',
			indexesComparable: false
		})
	);
	await incomingRefresh;

	assert.equal(
		replica.inspectIndex({ field: 'games', arguments: {} }).revision,
		rootBefore.revision,
		'the earlier request cannot update only its older root'
	);
	assert.equal(
		replica.read(GamesWithOwner, {}).data.games[0].owner.name,
		'later nested owner'
	);
	incoming.destroy();
	nestedOwner.destroy();
});

test('shared comparable membership follows server vectors across operations', async (t) => {
	const scenario = async ({
		firstPosition,
		firstTitle,
		secondPosition,
		secondTitle,
		resolveSecondFirst,
		expectedTitle
	}) => {
		const fetches = [];
		const replica = createDistributedReplica({
			transport: {
				fetch(request) {
					let resolve;
					const promise = new Promise((done) => {
						resolve = done;
					});
					fetches.push({ request, resolve });
					return promise;
				}
			}
		});
		const first = replica.watch(Todos, {});
		const second = replica.watch(TodosOtherOperation, {});
		await Promise.resolve();
		assert.equal(fetches.length, 2);
		const firstFrame = wireFrame({
			operation: Todos.id,
			snapshotScope: 'snapshot:todos',
			position: firstPosition,
			rows: [{ id: 'todo-first', title: firstTitle }]
		});
		const secondFrame = wireFrame({
			operation: TodosOtherOperation.id,
			snapshotScope: 'snapshot:todos-other',
			position: secondPosition,
			rows: [{ id: 'todo-second', title: secondTitle }]
		});
		const order = resolveSecondFirst
			? [
					[fetches[1], secondFrame],
					[fetches[0], firstFrame]
				]
			: [
					[fetches[0], firstFrame],
					[fetches[1], secondFrame]
				];
		for (const [fetch, frame] of order) {
			fetch.resolve(frame);
			await new Promise((resolve) => setImmediate(resolve));
		}
		assert.equal(replica.read(Todos, {}).data.todos[0].title, expectedTitle);
		assert.equal(
			replica.read(TodosOtherOperation, {}).data.todos[0].title,
			expectedTitle
		);
		first.destroy();
		second.destroy();
	};

	await t.test('a slower earlier request with a higher vector wins', () =>
		scenario({
			firstPosition: '10',
			firstTitle: 'server position 10',
			secondPosition: '9',
			secondTitle: 'server position 9',
			resolveSecondFirst: true,
			expectedTitle: 'server position 10'
		})
	);
	await t.test('a later-started lower vector cannot clobber', () =>
		scenario({
			firstPosition: '10',
			firstTitle: 'server position 10',
			secondPosition: '8',
			secondTitle: 'server position 8',
			resolveSecondFirst: false,
			expectedTitle: 'server position 10'
		})
	);
});

test('a stale index fence retains its newer shared server vector', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	write(replica, {
		position: '10',
		rows: [{ id: 'todo-10', title: 'server position 10' }]
	});
	const partial = wireFrame({
		position: '11',
		rows: []
	});
	partial.data = {};
	partial.errors = [{ message: 'root failed', path: ['todos'] }];
	partial.extensions.distributed.snapshot.records = [];
	replica.writeResult(Todos, {}, partial, 'network');
	const staleFence = replica.inspectIndex({
		field: 'todos',
		arguments: {}
	});
	assert.equal(staleFence.staleRevision, '2');

	const lower = replica.watch(TodosOtherOperation, {});
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	fetches[0].resolve(
		wireFrame({
			operation: TodosOtherOperation.id,
			snapshotScope: 'snapshot:todos-other',
			position: '9',
			rows: [{ id: 'todo-9', title: 'lower server position' }]
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	const after = replica.inspectIndex({ field: 'todos', arguments: {} });
	assert.equal(after.revision, staleFence.revision);
	assert.equal(after.staleRevision, staleFence.staleRevision);
	assert.equal(replica.read(TodosOtherOperation, {}).stale, true);
	lower.destroy();
});

test('comparable vectors govern nested indexes shared by distinct roots', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	const games = replica.watch(GamesWithOwner, {});
	const featured = replica.watch(FeaturedGamesWithOwner, {});
	await Promise.resolve();
	assert.equal(fetches.length, 2);

	fetches[1].resolve(
		gamesFrame({
			artifact: FeaturedGamesWithOwner,
			responseKey: 'featuredGames',
			position: '9',
			ownerId: 'user-old',
			ownerName: 'old owner'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	fetches[0].resolve(
		gamesFrame({
			artifact: GamesWithOwner,
			responseKey: 'games',
			position: '10',
			ownerId: 'user-new',
			ownerName: 'new owner'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	assert.equal(
		replica.read(GamesWithOwner, {}).data.games[0].owner.name,
		'new owner'
	);
	games.destroy();
	featured.destroy();
});

test('a comparable shared root cannot promote an incomparable nested sibling', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			}
		}
	});
	const incoming = replica.watch(GamesWithOwner, {});
	const rootOwner = replica.watch(GamesWithOwnerOtherOperation, {});
	const nestedOwner = replica.watch(FeaturedGamesWithOwner, {});
	await Promise.resolve();
	assert.equal(fetches.length, 3);

	fetches[1].resolve(
		gamesFrame({
			artifact: GamesWithOwnerOtherOperation,
			responseKey: 'games',
			position: '5',
			ownerId: 'user-root',
			ownerName: 'root owner'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	fetches[2].resolve(
		gamesFrame({
			artifact: FeaturedGamesWithOwner,
			responseKey: 'featuredGames',
			position: '9',
			ownerId: 'user-nested',
			ownerName: 'incomparable nested owner',
			projection: 'featured-games-projector'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	fetches[0].resolve(
		gamesFrame({
			artifact: GamesWithOwner,
			responseKey: 'games',
			position: '6',
			ownerId: 'user-incoming',
			ownerName: 'should remain fenced'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	assert.equal(
		replica.read(GamesWithOwner, {}).data.games[0].owner.name,
		'incomparable nested owner'
	);
	replica.writeResult(
		GamesWithOwnerLiveOperation,
		{},
		gamesFrame({
			artifact: GamesWithOwnerLiveOperation,
			responseKey: 'games',
			position: '7',
			ownerId: 'user-live',
			ownerName: 'unfenced live frame',
			operation: GamesWithOwnerLiveOperation.live.id,
			live: {
				supported: true,
				reset: false,
				cursors: [
					{
						projection: 'games-owner-projector',
						position: '7',
						token: 'resume:7'
					}
				]
			}
		}),
		'live'
	);
	assert.equal(
		replica.read(GamesWithOwner, {}).data.games[0].owner.name,
		'incomparable nested owner',
		'a live frame without a request-start fence cannot promote the graph'
	);
	incoming.destroy();
	rootOwner.destroy();
	nestedOwner.destroy();
});

test('an older operation reset cannot erase a shared index owned by a newer artifact', () => {
	const replica = createDistributedReplica();
	write(
		replica,
		{
			operation: Todos.live.id,
			position: '5',
			rows: [{ id: 'todo-live', title: 'old live owner' }],
			live: { supported: true }
		},
		'live'
	);
	write(
		replica,
		{
			operation: TodosOtherOperation.id,
			snapshotScope: 'snapshot:todos-other',
			position: '9',
			rows: [{ id: 'todo-new', title: 'new artifact owner' }]
		},
		'network',
		TodosOtherOperation
	);
	write(
		replica,
		{
			operation: Todos.live.id,
			position: '8',
			rows: [{ id: 'todo-reset', title: 'older reset' }],
			live: { supported: true, reset: true }
		},
		'live'
	);

	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-new', title: 'new artifact owner' }
	]);
});

test('reset preserves an equal-vector index with another operation co-owner', () => {
	const replica = createDistributedReplica();
	write(
		replica,
		{
			operation: Todos.live.id,
			position: '5',
			rows: [{ id: 'todo-shared', title: 'shared snapshot' }],
			live: { supported: true }
		},
		'live'
	);
	write(
		replica,
		{
			operation: TodosOtherOperation.id,
			snapshotScope: 'snapshot:todos-other',
			position: '5',
			rows: [{ id: 'todo-shared', title: 'shared snapshot' }]
		},
		'network',
		TodosOtherOperation
	);
	write(
		replica,
		{
			operation: Todos.live.id,
			position: '4',
			rows: [{ id: 'todo-reset', title: 'older reset' }],
			live: { supported: true, reset: true }
		},
		'live'
	);

	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-shared', title: 'shared snapshot' }
	]);
});

test('snapshot-only nested records do not invalidate exact membership', () => {
	const replica = createDistributedReplica();
	replica.writeResult(
		GamesWithOwner,
		{},
		{
			data: {
				games: [
					{
						id: 'game-1',
						owner_id: 'user-1',
						owner: { id: 'user-1', name: 'Pat' }
					}
				]
			},
			extensions: {
				distributed: {
					protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
					schemaHash: 'schema-a',
					authorizationGeneration: 'auth-1',
					cacheScope: 'cache:a',
					operation: GamesWithOwner.id,
					snapshot: {
						scopeToken: 'snapshot:games',
						recordsComplete: false,
						indexesComparable: false,
						records: [
							{
								path: ['games', '0'],
								model: 'GameView',
								scopeToken: 'record:game-1',
								incarnation: '1',
								revision: '1',
								tombstone: false
							}
						],
						indexes: [],
						observations: []
					}
				}
			}
		},
		'network'
	);

	const snapshot = replica.read(GamesWithOwner, {});
	assert.equal(snapshot.status, 'ready');
	assert.equal(snapshot.complete, true);
	assert.deepEqual(snapshot.data.games, [
		{
			id: 'game-1',
			owner_id: 'user-1',
			owner: { id: 'user-1', name: 'Pat' }
		}
	]);
});

test('v1 replica ingress rejects tampered decimals before exposing data', () => {
	const replica = createDistributedReplica();
	const tampered = wireFrame();
	tampered.extensions.distributed.snapshot.indexes[0].position = 1;

	assert.throws(
		() => replica.writeResult(Todos, {}, tampered, 'network'),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path.endsWith('.position')
	);
	assert.equal(replica.read(Todos, {}).complete, false);
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);
});

test('record and index clocks reject lower or incomparable evidence without numeric coercion', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '18446744073709551615',
		revision: '18446744073709551615',
		rows: [{ id: 'todo-1', title: 'newest' }]
	});
	write(replica, {
		position: '9',
		revision: '9',
		rows: [{ id: 'todo-1', title: 'late-old' }]
	});

	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'newest');
	assert.equal(
		replica.inspectRecord(Todo, 'todo-1').revision,
		'18446744073709551615'
	);

	assert.throws(
		() =>
			write(replica, {
				position: '18446744073709551615',
				revision: '18446744073709551615',
				recordScope: 'record:incomparable',
				rows: [{ id: 'todo-1', title: 'must-not-win' }]
			}),
		DistributedProtocolError
	);
	assert.equal(
		replica.inspectRecord(Todo, 'todo-1').revision,
		'18446744073709551615'
	);
	assert.equal(replica.inspectIndex({ field: 'todos', arguments: {} }), undefined);
});

test('tombstone and explicit recreate fences reject stale resurrection', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'first lifecycle' }]
	});
	write(replica, {
		position: '9',
		rows: [],
		records: [
			{
				path: ['todos', '0'],
				model: 'TodoView',
				scopeToken: 'record:todo-1',
				incarnation: '1',
				revision: '9',
				tombstone: true
			}
		]
	});
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);
	assert.deepEqual(replica.read(Todos, {}).data.todos, []);

	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'delayed pre-delete' }]
	});
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);

	assert.throws(
		() =>
			write(replica, {
				position: '10',
				incarnation: '1',
				revision: '10',
				rows: [{ id: 'todo-1', title: 'implicit resurrection' }]
			}),
		DistributedProtocolError
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);

	write(replica, {
		position: '11',
		incarnation: '2',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'explicit recreate' }]
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'explicit recreate');
	assert.equal(replica.inspectRecord(Todo, 'todo-1').incarnation, '2');
	assert.equal(replica.inspectRecord(Todo, 'todo-1').revision, '1');

	write(replica, {
		position: '12',
		incarnation: '1',
		revision: '12',
		rows: [{ id: 'todo-1', title: 'stale prior lifecycle' }]
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'explicit recreate');
});

test('live reset replaces its snapshot and a later query refetch may hand back', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '5',
		rows: [{ id: 'todo-old', title: 'old snapshot' }],
		recordScope: 'record:old'
	});
	write(
		replica,
		{
			operation: 'live:todos',
			position: '6',
			rows: [{ id: 'todo-new', title: 'fresh live snapshot' }],
			recordScope: 'record:new',
			live: { reset: true }
		},
		'live'
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-new', title: 'fresh live snapshot' }
	]);

	write(replica, {
		position: '7',
		rows: [{ id: 'todo-old', title: 'later query refetch' }],
		recordScope: 'record:old'
	});
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-old', title: 'later query refetch' }
	]);
});

test('a live handoff fences an HTTP response launched in the prior generation', async () => {
	let resolveFetch;
	let liveObserver;
	const pendingFetch = new Promise((resolve) => {
		resolveFetch = resolve;
	});
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				return pendingFetch;
			},
			subscribe(_request, observer) {
				liveObserver = observer;
				return () => {};
			}
		}
	});
	const watch = replica.watch(Todos, {}, { live: true });
	await Promise.resolve();

	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '1',
			rows: [{ id: 'todo-live', title: 'live wins' }],
			recordScope: 'record:live',
			live: { reset: true }
		})
	);
	resolveFetch(
		wireFrame({
			position: '99',
			rows: [{ id: 'todo-http', title: 'stale HTTP' }],
			recordScope: 'record:http'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'live wins' }
	]);
	watch.destroy();
});

test('unsupported live fallback cannot fence HTTP membership or later revalidation', async () => {
	const fetches = [];
	const subscriptions = [];
	let unsubscribeCount = 0;
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ request, resolve });
				return promise;
			},
			subscribe(request, observer) {
				subscriptions.push({ request, observer });
				return () => {
					unsubscribeCount += 1;
				};
			}
		}
	});
	const watch = replica.watch(Todos, {}, { live: true });
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	assert.equal(subscriptions.length, 1);

	subscriptions[0].observer.next(
		wireFrame({
			operation: 'live:todos',
			position: '1',
			rows: [{ id: 'todo-live', title: 'provisional live fallback' }],
			recordScope: 'record:live',
			indexesComparable: false,
			live: { supported: false, reset: true }
		})
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'provisional live fallback' }
	]);
	assert.equal(watch.get().live, 'off');
	assert.equal(unsubscribeCount, 1);

	await Promise.resolve();
	fetches[0].resolve(
		wireFrame({
			position: '2',
			rows: [{ id: 'todo-http', title: 'newer HTTP membership' }],
			recordScope: 'record:http',
			indexesComparable: false
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-http', title: 'newer HTTP membership' }
	]);
	assert.equal(
		subscriptions.length,
		1,
		'query fallback must not immediately reopen an unsupported stream'
	);

	const revalidation = replica.revalidate({
		dependencies: ['todos'],
		models: [],
		relationships: []
	});
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(fetches.length, 2);
	fetches[1].resolve(
		wireFrame({
			position: '3',
			rows: [{ id: 'todo-revalidated', title: 'revalidated membership' }],
			recordScope: 'record:revalidated',
			indexesComparable: false
		})
	);
	await revalidation;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-revalidated', title: 'revalidated membership' }
	]);
	assert.equal(subscriptions.length, 1);

	watch.destroy();
	assert.equal(unsubscribeCount, 1);
});

test('conflicting provisional live fallbacks still close and yield to HTTP', async () => {
	const fetches = [];
	let liveObserver;
	let unsubscribeCount = 0;
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ resolve });
				return promise;
			},
			subscribe(_request, observer) {
				liveObserver = observer;
				return () => {
					unsubscribeCount += 1;
				};
			}
		}
	});
	replica.writeResult(
		TodosOtherLiveOperation,
		{},
		wireFrame({
			operation: 'live:todos-other',
			rows: [{ id: 'todo-other', title: 'other provisional membership' }],
			recordScope: 'record:other',
			indexesComparable: false,
			live: { supported: false, reset: true }
		}),
		'live'
	);

	const watch = replica.watch(Todos, {}, { live: true });
	await Promise.resolve();
	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			rows: [{ id: 'todo-live', title: 'conflicting provisional membership' }],
			recordScope: 'record:live',
			indexesComparable: false,
			live: { supported: false, reset: true }
		})
	);
	assert.equal(watch.get().live, 'off');
	assert.equal(unsubscribeCount, 1);

	await Promise.resolve();
	fetches[0].resolve(
		wireFrame({
			rows: [{ id: 'todo-http', title: 'authoritative HTTP membership' }],
			recordScope: 'record:http',
			indexesComparable: false
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-http', title: 'authoritative HTTP membership' }
	]);

	watch.destroy();
	assert.equal(unsubscribeCount, 1);
});

test('completed supported live streams relinquish ownership and fall back to HTTP', async () => {
	const fetches = [];
	const subscriptions = [];
	let unsubscribeCount = 0;
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ resolve });
				return promise;
			},
			subscribe(_request, observer) {
				subscriptions.push(observer);
				return () => {
					unsubscribeCount += 1;
				};
			}
		}
	});
	const watch = replica.watch(Todos, {}, { live: true });
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	assert.equal(watch.get().live, 'active');

	subscriptions[0].next(
		wireFrame({
			operation: 'live:todos',
			position: '2',
			rows: [{ id: 'todo-live', title: 'supported live membership' }],
			recordScope: 'record:live',
			live: { supported: true, reset: true }
		})
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'supported live membership' }
	]);
	subscriptions[0].complete();
	assert.equal(watch.get().live, 'off');
	assert.equal(unsubscribeCount, 1);

	fetches[0].resolve(
		wireFrame({
			position: '99',
			rows: [{ id: 'todo-old-http', title: 'superseded HTTP membership' }],
			recordScope: 'record:old-http'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'supported live membership' }
	]);
	assert.equal(fetches.length, 2);

	fetches[1].resolve(
		wireFrame({
			position: '3',
			rows: [{ id: 'todo-http', title: 'completion fallback' }],
			recordScope: 'record:http',
			indexesComparable: false
		})
	);
	await new Promise((resolve) => setImmediate(resolve));
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-http', title: 'completion fallback' }
	]);
	assert.equal(subscriptions.length, 1);

	watch.destroy();
	assert.equal(unsubscribeCount, 1);
});

test('terminal live errors close their stream and allow a later HTTP retry', async () => {
	const fetches = [];
	const subscriptions = [];
	let unsubscribeCount = 0;
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ resolve });
				return promise;
			},
			subscribe(_request, observer) {
				subscriptions.push(observer);
				return () => {
					unsubscribeCount += 1;
				};
			}
		}
	});
	write(replica, {
		rows: [{ id: 'todo-query', title: 'initial query' }],
		recordScope: 'record:query'
	});
	const watch = replica.watch(Todos, {}, { live: true });

	subscriptions[0].error(new Error('terminal live failure'));
	assert.equal(watch.get().live, 'error');
	assert.equal(unsubscribeCount, 1);

	const retry = watch.refresh();
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	fetches[0].resolve(
		wireFrame({
			position: '2',
			rows: [{ id: 'todo-http', title: 'retry membership' }],
			recordScope: 'record:http'
		})
	);
	await retry;
	assert.equal(subscriptions.length, 2);
	assert.equal(watch.get().live, 'active');
	assert.deepEqual(watch.get().errors, []);

	watch.destroy();
	assert.equal(unsubscribeCount, 2);
});

test('live advancement fences an overlapping refresh while a later clean refresh succeeds', async () => {
	const fetches = [];
	const subscriptions = [];
	let liveObserver;
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ promise, resolve });
				return promise;
			},
			subscribe(request, observer) {
				subscriptions.push({ request, observer });
				liveObserver = observer;
				return () => {};
			}
		}
	});
	const watch = replica.watch(Todos, {}, { live: true });
	await Promise.resolve();
	assert.equal(fetches.length, 1);
	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '1',
			rows: [{ id: 'todo-live', title: 'live one' }],
			recordScope: 'record:live',
			live: { reset: true }
		})
	);
	fetches[0].resolve(
		wireFrame({
			position: '90',
			rows: [{ id: 'todo-old-http', title: 'old HTTP' }],
			recordScope: 'record:old-http'
		})
	);
	await new Promise((resolve) => setImmediate(resolve));

	const overlapping = watch.refresh();
	await Promise.resolve();
	assert.equal(fetches.length, 2);
	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '2',
			revision: '2',
			rows: [{ id: 'todo-live', title: 'live two' }],
			recordScope: 'record:live',
			live: { reset: false }
		})
	);
	fetches[1].resolve(
		wireFrame({
			position: '99',
			rows: [{ id: 'todo-racing-http', title: 'racing HTTP' }],
			recordScope: 'record:racing-http'
		})
	);
	await overlapping;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'live two' }
	]);

	const clean = watch.refresh();
	const preRefreshObserver = liveObserver;
	await Promise.resolve();
	assert.equal(fetches.length, 3);
	fetches[2].resolve(
		wireFrame({
			position: '100',
			rows: [{ id: 'todo-refreshed', title: 'clean refresh' }],
			recordScope: 'record:refreshed'
		})
	);
	await clean;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-refreshed', title: 'clean refresh' }
	]);
	assert.equal(subscriptions.length, 2);
	assert.notEqual(liveObserver, preRefreshObserver);
	assert.deepEqual(subscriptions[1].request.resume, [
		{
			projection: 'todos-projector',
			position: '100',
			token: 'resume:100'
		}
	]);

	preRefreshObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '999',
			revision: '999',
			rows: [{ id: 'todo-live', title: 'queued old subscription' }],
			recordScope: 'record:live',
			live: { reset: false }
		})
	);
	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '101',
			revision: '3',
			rows: [{ id: 'todo-live', title: 'rebased live' }],
			recordScope: 'record:live',
			live: { reset: false }
		})
	);
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'rebased live' }
	]);
	watch.destroy();
});

test('HTTP handoff requires shared scope and an equal-or-newer active index vector', async () => {
	const fetches = [];
	const subscriptions = [];
	let liveObserver;
	const replica = createDistributedReplica({
		transport: {
			fetch() {
				let resolve;
				const promise = new Promise((done) => {
					resolve = done;
				});
				fetches.push({ resolve });
				return promise;
			},
			subscribe(request, observer) {
				subscriptions.push(request);
				liveObserver = observer;
				return () => {};
			}
		}
	});
	write(replica, {
		position: '5',
		rows: [{ id: 'todo-query', title: 'query five' }],
		recordScope: 'record:query'
	});
	const watch = replica.watch(Todos, {}, { live: true });
	liveObserver.next(
		wireFrame({
			operation: 'live:todos',
			position: '10',
			rows: [{ id: 'todo-live', title: 'live ten' }],
			recordScope: 'record:live',
			live: { reset: true }
		})
	);

	const lagging = watch.refresh();
	await Promise.resolve();
	fetches[0].resolve(
		wireFrame({
			position: '9',
			rows: [{ id: 'todo-lagging', title: 'lagging HTTP' }],
			recordScope: 'record:lagging'
		})
	);
	await lagging;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'live ten' }
	]);
	assert.equal(subscriptions.length, 1);

	const incomparable = watch.refresh();
	await Promise.resolve();
	fetches[1].resolve(
		wireFrame({
			position: '11',
			snapshotScope: 'snapshot:other',
			indexScope: 'index:other',
			rows: [{ id: 'todo-other', title: 'incomparable HTTP' }],
			recordScope: 'record:other'
		})
	);
	await incomparable;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-live', title: 'live ten' }
	]);
	assert.equal(subscriptions.length, 1);

	const newer = watch.refresh();
	await Promise.resolve();
	fetches[2].resolve(
		wireFrame({
			position: '11',
			rows: [{ id: 'todo-newer', title: 'newer HTTP' }],
			recordScope: 'record:newer'
		})
	);
	await newer;
	assert.deepEqual(replica.read(Todos, {}).data.todos, [
		{ id: 'todo-newer', title: 'newer HTTP' }
	]);
	assert.equal(subscriptions.length, 2);
	watch.destroy();
});

test('scope and schema generations purge old state before accepting fresh evidence', () => {
	const replica = createDistributedReplica();
	write(replica, {
		rows: [{ id: 'todo-1', title: 'scope a' }],
		recordScope: 'record:a'
	});
	write(replica, {
		cacheScope: 'cache:b',
		rows: [{ id: 'todo-1', title: 'scope b' }],
		recordScope: 'record:b'
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'scope b');

	assert.throws(
		() =>
			write(replica, {
				schemaHash: 'schema-tampered',
				authorizationGeneration: 'auth-1',
				cacheScope: 'cache:b',
				rows: [{ id: 'todo-1', title: 'wrong schema' }]
			}),
		DistributedProtocolError
	);
	assert.equal(replica.read(Todos, {}).complete, false);
});

test('protocol-bound artifacts reject results without a v1 envelope', () => {
	const replica = createDistributedReplica();
	assert.throws(
		() =>
			replica.writeResult(
				Todos,
				{},
				{
					data: { todos: [{ id: 'todo-1', title: 'unscoped' }] },
					revision: '99'
				},
				'network'
			),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path === 'extensions.distributed'
	);
	assert.equal(replica.read(Todos, {}).complete, false);
});

test('only exact causation and expectation observations retire optimism', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'base' }]
	});
	replica.createOptimisticLayer('cmd-1', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic' }
		});
	});
	replica.markOptimisticLayerAccepted('cmd-1', commandMetadata());

	write(replica, {
		position: '2',
		revision: '2',
		rows: [{ id: 'todo-1', title: 'server before observation' }],
		observations: [
			{
				causationId: 'cause-1',
				projection: 'todos-projector',
				model: 'TodoView',
				scopeToken: 'expect:other-record'
			}
		]
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'optimistic');

	write(replica, {
		position: '3',
		revision: '3',
		rows: [{ id: 'todo-1', title: 'atomic' }],
		observations: [
			{
				causationId: 'cause-1',
				projection: 'todos-projector',
				model: 'TodoView',
				scopeToken: 'expect:todo-1'
			}
		]
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'atomic');
});

test('discarded or incomplete snapshots cannot use observations to retire optimism', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'base' }]
	});
	replica.createOptimisticLayer('cmd-1', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic' }
		});
	});
	replica.markOptimisticLayerAccepted('cmd-1', commandMetadata());
	const observation = [
		{
			causationId: 'cause-1',
			projection: 'todos-projector',
			model: 'TodoView',
			scopeToken: 'expect:todo-1'
		}
	];

	write(replica, {
		position: '2',
		revision: '2',
		snapshotScope: 'snapshot:incomparable',
		indexScope: 'index:incomparable',
		rows: [{ id: 'todo-1', title: 'incomparable base' }],
		command: commandMetadata({ observations: observation })
	});
	write(replica, {
		position: '3',
		revision: '3',
		snapshotScope: 'snapshot:incomparable',
		indexScope: 'index:incomparable',
		rows: [{ id: 'todo-1', title: 'canonical after discard' }]
	});
	assert.equal(
		replica.read(Todos, {}).data.todos[0].title,
		'canonical after discard'
	);

	replica.createOptimisticLayer('cmd-2', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic incomplete' }
		});
	});
	const secondReceipt = commandMetadata({
		commandId: 'cmd-2',
		causationId: 'cause-2'
	});
	replica.markOptimisticLayerAccepted('cmd-2', secondReceipt);
	const secondObservation = [
		{
			causationId: 'cause-2',
			projection: 'todos-projector',
			model: 'TodoView',
			scopeToken: 'expect:todo-1'
		}
	];

	write(replica, {
		position: '4',
		revision: '4',
		recordsComplete: false,
		indexesComparable: false,
		rows: [{ id: 'todo-1', title: 'incomplete base' }],
		command: commandMetadata({
			commandId: 'cmd-2',
			causationId: 'cause-2',
			observations: secondObservation
		})
	});
	assert.throws(
		() => replica.createOptimisticLayer('cmd-2', () => {}),
		/optimistic layer already exists/,
		'incomplete command observation must retain its optimistic layer'
	);
	write(replica, {
		position: '5',
		revision: '5',
		snapshotScope: 'snapshot:incomparable',
		indexScope: 'index:incomparable',
		rows: [{ id: 'todo-1', title: 'canonical after incomplete' }]
	});
	assert.equal(
		replica.read(Todos, {}).data.todos[0].title,
		'canonical after incomplete'
	);

	replica.createOptimisticLayer('cmd-3', (writer) => {
		writer.writeRecord(Todo, 'todo-1', {
			fields: { title: 'optimistic snapshot observation' }
		});
	});
	replica.markOptimisticLayerAccepted(
		'cmd-3',
		commandMetadata({ commandId: 'cmd-3', causationId: 'cause-3' })
	);
	const thirdObservation = [
		{
			causationId: 'cause-3',
			projection: 'todos-projector',
			model: 'TodoView',
			scopeToken: 'expect:todo-1'
		}
	];

	write(replica, {
		position: '6',
		revision: '6',
		recordsComplete: false,
		indexesComparable: true,
		rows: [{ id: 'todo-1', title: 'incomplete snapshot observation' }],
		observations: thirdObservation
	});
	write(replica, {
		position: '7',
		revision: '7',
		snapshotScope: 'snapshot:incomparable',
		indexScope: 'index:incomparable',
		rows: [{ id: 'todo-1', title: 'clean without observation' }]
	});
	assert.equal(
		replica.read(Todos, {}).data.todos[0].title,
		'optimistic snapshot observation'
	);
	write(replica, {
		position: '8',
		revision: '8',
		snapshotScope: 'snapshot:incomparable',
		indexScope: 'index:incomparable',
		rows: [{ id: 'todo-1', title: 'canonical base' }],
		observations: thirdObservation
	});
	assert.equal(replica.read(Todos, {}).data.todos[0].title, 'canonical base');
});

test('pathless upserts advance the global fence without certifying stale fields', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'cached one' }]
	});

	for (const revision of ['2', '3']) {
		write(replica, {
			position: revision,
			recordsComplete: false,
			indexesComparable: false,
			rows: [],
			records: [
				{
					model: 'TodoView',
					scopeToken: 'record:todo-1',
					incarnation: '1',
					revision,
					tombstone: false
				}
			]
		});
		assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);
	}

	write(
		replica,
		{
			operation: 'query:todos-other',
			position: '2',
			revision: '2',
			rows: [{ id: 'todo-1', title: 'late other operation' }]
		},
		'network',
		TodosOtherOperation
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);
});

test('an unseen pathless delete fences delayed identity discovery until recreation', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '9',
		recordsComplete: false,
		indexesComparable: false,
		rows: [],
		records: [
			{
				model: 'TodoView',
				scopeToken: 'record:unseen',
				incarnation: '1',
				revision: '9',
				tombstone: true
			}
		]
	});

	write(
		replica,
		{
			operation: 'query:todos-other',
			position: '1',
			incarnation: '1',
			revision: '1',
			recordScope: 'record:unseen',
			rows: [{ id: 'todo-unseen', title: 'delayed before delete' }]
		},
		'network',
		TodosOtherOperation
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-unseen'), undefined);
	assert.equal(replica.read(TodosOtherOperation, {}).complete, false);

	write(
		replica,
		{
			operation: 'query:todos-other',
			position: '2',
			incarnation: '2',
			revision: '1',
			recordScope: 'record:unseen',
			rows: [{ id: 'todo-unseen', title: 'recreated' }]
		},
		'network',
		TodosOtherOperation
	);
	assert.equal(
		replica.read(TodosOtherOperation, {}).data.todos[0].title,
		'recreated'
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-unseen').incarnation, '2');
	assert.equal(replica.inspectRecord(Todo, 'todo-unseen').revision, '1');
});

test('anonymous pathless clock capacity fails closed without evicting retained fences', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		recordsComplete: false,
		indexesComparable: false,
		rows: [],
		records: Array.from({ length: 4_096 }, (_, index) => ({
			model: 'TodoView',
			scopeToken: `record:anonymous:${index}`,
			incarnation: '1',
			revision: '1',
			tombstone: true
		}))
	});

	assert.throws(
		() =>
			write(replica, {
				position: '2',
				recordsComplete: false,
				indexesComparable: false,
				rows: [],
				records: [
					{
						model: 'TodoView',
						scopeToken: 'record:anonymous:overflow',
						incarnation: '1',
						revision: '2',
						tombstone: true
					}
				]
			}),
		(error) =>
			error instanceof DistributedProtocolError &&
			error.path.endsWith('.records.capacity')
	);

	write(
		replica,
		{
			operation: 'query:todos-other',
			position: '1',
			incarnation: '1',
			revision: '0',
			recordScope: 'record:anonymous:0',
			rows: [{ id: 'todo-delayed', title: 'must stay deleted' }]
		},
		'network',
		TodosOtherOperation
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-delayed'), undefined);
});

test('pathless delete and recreation handle reset revisions and duplicate final evidence', () => {
	const replica = createDistributedReplica();
	write(replica, {
		position: '1',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'first lifecycle' }]
	});
	write(replica, {
		position: '9',
		recordsComplete: false,
		indexesComparable: false,
		rows: [],
		records: [
			{
				model: 'TodoView',
				scopeToken: 'record:todo-1',
				incarnation: '1',
				revision: '9',
				tombstone: true
			}
		]
	});
	assert.equal(replica.inspectRecord(Todo, 'todo-1'), undefined);

	write(replica, {
		position: '10',
		recordsComplete: false,
		indexesComparable: false,
		rows: [],
		records: [
			{
				model: 'TodoView',
				scopeToken: 'record:todo-1',
				incarnation: '2',
				revision: '1',
				tombstone: false
			}
		]
	});
	write(replica, {
		position: '11',
		incarnation: '2',
		revision: '1',
		rows: [{ id: 'todo-1', title: 'second lifecycle' }],
		records: [
			{
				path: ['todos', '0'],
				model: 'TodoView',
				scopeToken: 'record:todo-1',
				incarnation: '2',
				revision: '1',
				tombstone: false
			},
			{
				model: 'TodoView',
				scopeToken: 'record:todo-1',
				incarnation: '2',
				revision: '1',
				tombstone: false
			}
		]
	});
	assert.equal(
		replica.read(Todos, {}).data.todos[0].title,
		'second lifecycle'
	);
	assert.equal(replica.inspectRecord(Todo, 'todo-1').incarnation, '2');
	assert.equal(replica.inspectRecord(Todo, 'todo-1').revision, '1');
});

test('replica transport resumes from the latest server-issued cursor', () => {
	const subscriptions = [];
	const replica = createDistributedReplica({
		transport: {
			async fetch() {
				throw new Error('complete cache must not refetch');
			},
			subscribe(request) {
				subscriptions.push(request);
				return () => {};
			}
		}
	});
	write(replica, {
		position: '7',
		resumeToken: 'resume:latest',
		rows: [{ id: 'todo-1', title: 'cached' }]
	});
	const watch = replica.watch(Todos, {}, { live: true });
	assert.equal(subscriptions.length, 1);
	assert.deepEqual(subscriptions[0].resume, [
		{
			projection: 'todos-projector',
			position: '7',
			token: 'resume:latest'
		}
	]);
	watch.destroy();
});

test('protocol record scopes remain opaque and never become replica identities', () => {
	const replica = createDistributedReplica();
	write(replica, {
		recordScope: 'opaque:tenant/key/partition',
		rows: [{ id: 'public-id', title: 'visible' }]
	});
	assert.equal(
		replica.inspectRecord(Todo, 'public-id').key,
		replicaRecordKey(Todo, 'public-id')
	);
	assert.equal(
		replica.inspectRecord(Todo, 'opaque:tenant/key/partition'),
		undefined
	);
});

test('issuing-user freshness keeps pending effects separate from confirmed floors', async () => {
	const { replicaCommandFreshness } = await import('../dist/replica/command-runtime/symbols.js');
	const artifact = { ...Todos, protocol: { ...Todos.protocol, protocolHash: 'protocol-a' } };
	const requests = [];
	let candidate = wireFrame({ position: '1', rows: [{ id: 'todo-1', title: 'base' }] });
	const replica = createDistributedReplica({ transport: { async fetch(request) { requests.push(request); return candidate; } } });
	write(replica, { position: '1', rows: [{ id: 'todo-1', title: 'base' }] }, 'network', artifact);
	const watch = replica.watch(artifact, {});
	const plan = { dependencies: ['todos'], models: [Todo.id], relationships: [] };
	replica[replicaCommandFreshness]('cmd-1', plan);
	replica.createOptimisticLayer('cmd-1', writer => writer.writeRecord(Todo, 'todo-1', { fields: { title: 'preview' } }));
	replica.markOptimisticLayerAccepted('cmd-1', commandMetadata());
	await watch.refresh();
	assert.equal(requests.at(-1).extensions.gatewayFreshness.pending.length, 1);
	assert.equal(replica.read(artifact, {}).data.todos[0].title, 'preview');
	candidate = wireFrame({ position: '2', revision: '2', rows: [{ id: 'todo-1', title: 'committed' }], observations: [{ causationId: 'cause-1', projection: 'todos-projector', model: Todo.id, scopeToken: 'expect:todo-1' }] });
	await watch.refresh();
	assert.equal(replica.read(artifact, {}).data.todos[0].title, 'committed');
	candidate = wireFrame({ position: '1', rows: [{ id: 'todo-1', title: 'lagging' }] });
	await watch.refresh();
	const context = requests.at(-1).extensions.gatewayFreshness;
	assert.equal(context.pending.length, 0);
	assert(context.minimum.some(floor => floor.kind === 'index' && floor.position === '2'));
	assert.equal(replica.read(artifact, {}).data.todos[0].title, 'committed');
	watch.destroy();
});

test('Atomic installs a retained outgoing floor without an automatic fetch', async () => {
	const { replicaCommandFreshness } = await import('../dist/replica/command-runtime/symbols.js');
	const artifact = { ...Todos, protocol: { ...Todos.protocol, protocolHash: 'protocol-a' } };
	const requests = [];
	const replica = createDistributedReplica({ transport: { async fetch(request) { requests.push(request); return wireFrame({ position: '1', rows: [{ id: 'todo-1', title: 'lagging' }] }); } } });
	write(replica, { position: '1', rows: [{ id: 'todo-1', title: 'base' }] }, 'network', artifact);
	const watch = replica.watch(artifact, {});
	replica[replicaCommandFreshness]('atomic', { dependencies: ['todos'], models: [Todo.id], relationships: [] });
	replica.createOptimisticLayer('atomic', writer => writer.writeRecord(Todo, 'todo-1', { fields: { title: 'preview' } }));
	replica[replicaCommandDirectProjection]('atomic', { model: Todo, identity: 'todo-1', evidence: { model: Todo.id, scopeToken: 'record:todo-1', incarnation: '1', revision: '2', tombstone: false }, fields: { id: 'todo-1', title: 'committed', __typename: Todo.id } });
	assert.equal(requests.length, 0);
	assert.equal(replica.read(artifact, {}).data.todos[0].title, 'committed');
	await watch.refresh();
	const context = requests.at(-1).extensions.gatewayFreshness;
	assert.equal(context.pending.length, 0);
	assert(context.minimum.some(floor => floor.kind === 'record' && floor.revision === '2'));
	assert.equal(replica.read(artifact, {}).data.todos[0].title, 'committed');
	watch.destroy();
});
