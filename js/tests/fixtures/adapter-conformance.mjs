import assert from 'node:assert/strict';

import {
	createDistributedReplica,
	createReplicaCommandRuntime
} from '../../dist/replica/index.js';
import { COMMAND_CONSISTENCY } from './command-protocol.mjs';

export const REACT_FIXTURE_SCHEMA = `sha256:${'a'.repeat(64)}`;
export const REACT_FIXTURE_SURFACE = Object.freeze({
	kind: 'role',
	name: 'user'
});

export const TodoModel = Object.freeze({
	id: 'TodoView',
	identityFields: Object.freeze(['id'])
});

const RequiredRevalidationCommand = Object.freeze({
	version: 1,
	name: 'todo.revalidate',
	mutationField: 'revalidateTodos',
	document:
		'mutation RevalidateTodos($commandId: ID!) { revalidateTodos(commandId: $commandId) }',
	operationHash: `sha256:${'b'.repeat(64)}`,
	protocol: Object.freeze({
		version: 1,
		schemaHash: REACT_FIXTURE_SCHEMA,
		protocolHash: `sha256:${'c'.repeat(64)}`,
		surface: REACT_FIXTURE_SURFACE,
		operation: `sha256:${'b'.repeat(64)}`,
		trustedPresets: Object.freeze([])
	}),
	input: Object.freeze({ kind: 'none' }),
	output: Object.freeze({
		kind: 'object',
		definition: Object.freeze({
			name: 'RevalidateTodosResult',
			fields: Object.freeze([
				Object.freeze({
					name: 'succeeded',
					typeName: 'Boolean',
					nullable: false,
					list: false,
					itemNullable: false,
					codec: 'boolean'
				})
			])
		})
	}),
	consistency: COMMAND_CONSISTENCY.SUCCEEDED,
	effects: Object.freeze({
		version: 1,
		operations: Object.freeze([]),
		fallback: 'revalidate'
	}),
	revalidation: Object.freeze({
		version: 1,
		required: true,
		dependencies: Object.freeze(['todos']),
		models: Object.freeze([TodoModel.id]),
		relationships: Object.freeze([])
	}),
	trustedPresets: Object.freeze([])
});

const NoVariables = Object.freeze({
	version: 2,
	limits: Object.freeze({
		maxDepth: 8,
		maxBoolWidth: 256,
		maxInList: 1_000
	}),
	variables: Object.freeze({}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({})
});

const TodoByIdVariables = Object.freeze({
	version: 2,
	limits: NoVariables.limits,
	variables: Object.freeze({
		id: Object.freeze({
			kind: 'scalar',
			scalar: 'ID',
			codec: 'string',
			nullable: false
		})
	}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({})
});

const todoSelection = Object.freeze({
	typename: TodoModel.id,
	storage: Object.freeze({
		kind: 'normalized',
		model: TodoModel.id,
		identityFields: TodoModel.identityFields
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
			responseKey: 'status',
			field: 'status',
			codec: 'String',
			nullable: false
		})
	])
});

export const TodosArtifact = Object.freeze({
	id: 'query:react-fixture-todos',
	document: 'query ReactFixtureTodos { todos { id title status } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: REACT_FIXTURE_SCHEMA,
		surface: REACT_FIXTURE_SURFACE,
		operation: 'query:react-fixture-todos',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: NoVariables,
	live: Object.freeze({
		id: 'live:react-fixture-todos',
		document:
			'subscription ReactFixtureTodosLive { todos { id title status } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'todos',
			field: 'todos',
			cardinality: 'many',
			nullable: false,
			dependencies: Object.freeze(['todos']),
			selection: todoSelection
		})
	])
});

export const OpenTodosArtifact = Object.freeze({
	...TodosArtifact,
	id: 'query:react-fixture-open-todos',
	document:
		'query ReactFixtureOpenTodos { todos(where: { completed: { _eq: false } }) { id title status completed } }',
	protocol: Object.freeze({
		...TodosArtifact.protocol,
		operation: 'query:react-fixture-open-todos'
	}),
	live: Object.freeze({
		id: 'live:react-fixture-open-todos',
		document:
			'subscription ReactFixtureOpenTodosLive { todos(where: { completed: { _eq: false } }) { id title status completed } }'
	}),
	roots: Object.freeze([
		Object.freeze({
			...TodosArtifact.roots[0],
			selection: Object.freeze({
				...todoSelection,
				members: Object.freeze([
					...todoSelection.members,
					Object.freeze({
						kind: 'scalar',
						responseKey: 'completed',
						field: 'completed',
						codec: 'Boolean',
						nullable: false
					})
				])
			}),
			coverage: Object.freeze({ kind: 'complete' }),
			filter: Object.freeze({
				input: Object.freeze({
					kind: 'literal',
					value: Object.freeze({
						completed: Object.freeze({ _eq: false })
					})
				}),
				fields: Object.freeze([
					Object.freeze({
						field: 'completed',
						scalar: 'Boolean',
						codec: 'boolean',
						nullable: false,
						operators: Object.freeze(['_eq'])
					})
				]),
				relationships: Object.freeze([]),
				rowPolicy: Object.freeze({ kind: 'unrestricted' })
			}),
			pagination: Object.freeze({
				kind: 'complete',
				insert: 'local',
				delete: 'local',
				reorder: 'local',
				stableUpdate: 'local'
			})
		})
	])
});

export const TodoByIdArtifact = Object.freeze({
	id: 'query:react-fixture-todo-by-id',
	document:
		'query ReactFixtureTodoById($id: ID!) { todo(id: $id) { id title status } }',
	protocol: Object.freeze({
		version: 1,
		schemaHash: REACT_FIXTURE_SCHEMA,
		surface: REACT_FIXTURE_SURFACE,
		operation: 'query:react-fixture-todo-by-id',
		trustedPresets: Object.freeze([])
	}),
	variableCodec: TodoByIdVariables,
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'todo',
			field: 'todo',
			cardinality: 'one',
			nullable: true,
			arguments: Object.freeze({
				id: Object.freeze({ kind: 'variable', name: 'id' })
			}),
			dependencies: Object.freeze(['todos']),
			selection: todoSelection
		})
	])
});

function deferred() {
	let resolveDeferred;
	let rejectDeferred;
	let settled = false;
	const promise = new Promise((resolve, reject) => {
		resolveDeferred = resolve;
		rejectDeferred = reject;
	});
	return {
		promise,
		get settled() {
			return settled;
		},
		resolve(value) {
			if (settled) return;
			settled = true;
			resolveDeferred(value);
		},
		reject(error) {
			if (settled) return;
			settled = true;
			rejectDeferred(error);
		}
	};
}

export class ControlledReplicaTransport {
	fetches = [];
	lives = [];

	fetch(request) {
		const response = deferred();
		const entry = { request, response };
		this.fetches.push(entry);
		return response.promise;
	}

	subscribe(request, observer) {
		const entry = {
			request,
			observer,
			closed: false
		};
		this.lives.push(entry);
		return () => {
			entry.closed = true;
		};
	}

	latestOpenLive() {
		return [...this.lives].reverse().find((entry) => !entry.closed);
	}
}

export function todoFrame(
	artifact,
	rows,
	{
		cacheScope = 'cache:user-a',
		authorizationGeneration = 'auth-1',
		position = '1',
		source = 'query',
		mode = 'resumable',
		reset = false,
		errors
	} = {}
) {
	const root = artifact.roots[0];
	const isMany = root.cardinality === 'many';
	const operation =
		source === 'live' ? artifact.live?.id : artifact.protocol.operation;
	if (operation === undefined) {
		throw new TypeError('live fixture frame requires a live artifact');
	}
	const dataValue = isMany ? rows : (rows[0] ?? null);
	const records = rows.map((row, index) => ({
		path: isMany
			? [root.responseKey, String(index)]
			: [root.responseKey],
		model: TodoModel.id,
		scopeToken: `record:${row.id}`,
		incarnation: '1',
		revision: position,
		tombstone: false
	}));
	const resume = {
		projection: 'todos-projector',
		position,
		token: `resume:${cacheScope}:${position}`
	};
	return {
		data: { [root.responseKey]: dataValue },
		...(errors === undefined ? {} : { errors }),
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: artifact.protocol.schemaHash,
				authorizationGeneration,
				cacheScope,
				operation,
				trustedPresets: [],
				snapshot: {
					scopeToken: `snapshot:${root.field}`,
					recordsComplete: true,
					indexesComparable: mode === 'resumable',
					records,
					indexes: mode === 'snapshot' ? [] : [
						{
							projection: 'todos-projector',
							scopeToken: `index:${root.field}`,
							position,
							resume
						}
					],
					observations: []
				},
				...(source === 'live'
					? {
							live: {
								mode,
								reset: mode === 'snapshot' || reset,
								cursors: mode === 'snapshot' ? [] : [resume]
							}
						}
					: {})
			}
		}
	};
}

function currentTitle(snapshot) {
	return snapshot.data.todos?.[0]?.title;
}

/**
 * Framework-neutral adapter behavioral contract.
 *
 * `mount` is the only framework-specific input. It binds one operation and
 * returns the familiar external-store controls, allowing Svelte and React to
 * execute the same state-transition assertions.
 */
export async function assertReplicaAdapterConformance({ mount }) {
	const transport = new ControlledReplicaTransport();
	const replica = createDistributedReplica({ transport });
	const commandRuntime = createReplicaCommandRuntime(
		replica,
		Object.freeze({
			dispatch: () => Promise.reject(new Error('unused command transport'))
		}),
		{ revalidateTodos: RequiredRevalidationCommand }
	);
	const adapter = await mount({
		replica,
		artifact: TodosArtifact,
		variables: {},
		options: { live: true }
	});

	try {
		assert.equal(adapter.getSnapshot().status, 'loading');
		assert.equal(adapter.getSnapshot().fetching, true);
		assert.equal(transport.fetches.length, 1);
		assert.equal(transport.lives.length, 1);

		await adapter.settle(() => {
			transport.fetches[0].response.resolve(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-1', title: 'server', status: 'open' }],
					{ position: '1' }
				)
			);
		});
		assert.equal(adapter.getSnapshot().status, 'ready');
		assert.equal(adapter.getSnapshot().live, 'active');
		assert.equal(currentTitle(adapter.getSnapshot()), 'server');

		const fetchesBeforeOptimism = transport.fetches.length;
		await adapter.settle(() => {
			replica.createOptimisticLayer('cmd-pending', (writer) => {
				writer.writeRecord(TodoModel, 'todo-1', {
					fields: { title: 'optimistic' }
				});
			});
			assert.equal(replica.markOptimisticLayerAccepted('cmd-pending'), true);
		});
		assert.equal(currentTitle(adapter.getSnapshot()), 'optimistic');
		const optimisticRevalidation =
			transport.fetches.length > fetchesBeforeOptimism
				? transport.fetches.at(-1)
				: undefined;

		const firstLive = transport.latestOpenLive();
		assert.ok(firstLive, 'live subscription must remain attached');
		await adapter.settle(() => {
			firstLive.observer.next(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-1', title: 'stale-server', status: 'open' }],
					{ position: '2', source: 'live', reset: true }
				)
			);
		});
		assert.equal(
			currentTitle(adapter.getSnapshot()),
			'optimistic',
			'accepted optimism must remain above a stale projected read'
		);
		if (optimisticRevalidation !== undefined) {
			await adapter.settle(() => {
				optimisticRevalidation.response.resolve(
					todoFrame(
						TodosArtifact,
						[{ id: 'todo-1', title: 'superseded-query', status: 'open' }],
						{ position: '3' }
					)
				);
			});
			assert.equal(
				currentTitle(adapter.getSnapshot()),
				'optimistic',
				'a query from before the live handoff must be generation-fenced'
			);
		}

		await adapter.settle(() => {
			replica.confirmOptimisticLayer('cmd-pending', (writer) =>
				writer.writeRecord(TodoModel, 'todo-1', '3', {
					fields: { title: 'atomic' }
				})
			);
		});
		assert.equal(
			currentTitle(adapter.getSnapshot()),
			'atomic',
			'Atomic confirmation must replace the pending layer atomically'
		);

		await adapter.settle(() => {
			replica.createOptimisticLayer('cmd-rejected', (writer) => {
				writer.writeRecord(TodoModel, 'todo-1', {
					fields: { title: 'will-roll-back' }
				});
			});
		});
		assert.equal(currentTitle(adapter.getSnapshot()), 'will-roll-back');
		await adapter.settle(() => {
			assert.equal(replica.rejectOptimisticLayer('cmd-rejected'), true);
		});
		assert.equal(currentTitle(adapter.getSnapshot()), 'atomic');

		const backgroundFetches = transport.fetches.filter(
			(entry) => !entry.response.settled
		);
		if (backgroundFetches.length > 0) {
			await adapter.settle(() => {
				for (const entry of backgroundFetches) {
					entry.response.resolve(
						todoFrame(
							TodosArtifact,
							[{ id: 'todo-1', title: 'atomic', status: 'done' }],
							{ position: '30' }
						)
					);
				}
			});
		}
		assert.equal(
			transport.fetches.some((entry) => !entry.response.settled),
			false,
			'pre-existing conservative revalidation must be drained'
		);

		const activeLive = transport.latestOpenLive();
		assert.ok(activeLive, 'live subscription must survive query handoff');
		await adapter.settle(() => {
			activeLive.observer.next(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-1', title: 'live', status: 'done' }],
					{ position: '40', source: 'live', reset: true }
				)
			);
		});
		assert.equal(currentTitle(adapter.getSnapshot()), 'live');

		const liveHandoffFetches = transport.fetches.filter(
			(entry) => !entry.response.settled
		);
		if (liveHandoffFetches.length > 0) {
			await adapter.settle(() => {
				for (const entry of liveHandoffFetches) {
					entry.response.resolve(
						todoFrame(
							TodosArtifact,
							[{ id: 'todo-1', title: 'live', status: 'done' }],
							{ position: '45' }
						)
					);
				}
			});
		}
		assert.equal(
			transport.fetches.some((entry) => !entry.response.settled),
			false,
			'live handoff revalidation must settle before explicit stale coverage'
		);

		const fetchesBeforeStale = transport.fetches.length;
		await adapter.settle(() => {
			assert.equal(
				replica.markIndexStale({ field: 'todos' }, 'adapter-conformance'),
				true
			);
		});
		assert.equal(
			transport.fetches.length,
			fetchesBeforeStale + 1,
			'stale transition must start a fresh revalidation'
		);
		assert.equal(adapter.getSnapshot().status, 'stale');
		assert.equal(
			adapter.getSnapshot().complete,
			true,
			'stale complete data must remain renderable during revalidation'
		);
		assert.equal(
			currentTitle(adapter.getSnapshot()),
			'live',
			'stale-while-revalidate must retain the last materialized view'
		);
		assert.equal(adapter.getSnapshot().fetching, true);
		const staleFetch = transport.fetches.at(-1);
		await adapter.settle(() => {
			staleFetch.response.resolve(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-1', title: 'revalidated', status: 'done' }],
					{ position: '50' }
				)
			);
		});
		assert.equal(adapter.getSnapshot().status, 'ready');
		assert.equal(currentTitle(adapter.getSnapshot()), 'revalidated');

		await adapter.settle(() => {
			void adapter.refetch();
		});
		const failedFetch = transport.fetches.at(-1);
		await adapter.settle(() => {
			failedFetch.response.reject(new Error('adapter conformance network failure'));
		});
		assert.equal(adapter.getSnapshot().status, 'error');
		assert.equal(adapter.getSnapshot().errors.length, 1);

		await adapter.settle(() => {
			void adapter.refetch();
		});
		const oldIdentityFetch = transport.fetches.at(-1);
		await adapter.settle(() => {
			replica.invalidateAuthorization();
		});
		assert.equal(oldIdentityFetch.request.signal.aborted, true);
		assert.equal(adapter.getSnapshot().status, 'loading');
		const newIdentityFetch = transport.fetches.at(-1);
		assert.notEqual(newIdentityFetch, oldIdentityFetch);

		await adapter.settle(() => {
			oldIdentityFetch.response.resolve(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-old', title: 'late-old-user', status: 'open' }],
					{ cacheScope: 'cache:user-a', position: '51' }
				)
			);
		});
		assert.notEqual(currentTitle(adapter.getSnapshot()), 'late-old-user');

		await adapter.settle(() => {
			newIdentityFetch.response.resolve(
				todoFrame(
					TodosArtifact,
					[{ id: 'todo-new', title: 'new-user', status: 'open' }],
					{ cacheScope: 'cache:user-b', position: '1' }
				)
			);
		});
		assert.equal(adapter.getSnapshot().status, 'ready');
		assert.equal(currentTitle(adapter.getSnapshot()), 'new-user');
		assert.equal(replica.scope.cacheScope, 'cache:user-b');
	} finally {
		commandRuntime.dispose();
		await adapter.dispose();
	}
}
