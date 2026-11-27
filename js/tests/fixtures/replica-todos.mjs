/** One normalized list operation and its wire frames, shared by replica tests. */
import { DISTRIBUTED_PROTOCOL_VERSION } from '../../dist/index.js';
import { createDistributedReplica } from '../../dist/replica/index.js';
import { FakeScheduler } from './game-harness.mjs';

export const Todo = Object.freeze({
	id: 'TodoView',
	identityFields: Object.freeze(['id'])
});

export const NoVariables = Object.freeze({
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

export const Todos = Object.freeze({
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

export function wireFrame(options = {}) {
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
					mode: options.live.mode ?? 'resumable',
					reset: options.live.reset ?? false,
					cursors:
						options.live.cursors ??
						(options.live.mode === 'snapshot' ? [] : [resume])
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


/** A replica over a scripted transport and a manual clock. */
export function scripted(options = {}) {
	const scheduler = new FakeScheduler();
	const fetches = [];
	const subscriptions = [];
	const replica = createDistributedReplica({
		scheduler,
		random: () => 0,
		...options,
		transport: {
			fetch(request) {
				return new Promise((resolve, reject) => {
					fetches.push({ request, resolve, reject });
				});
			},
			subscribe(request, observer) {
				const subscription = { request, observer, closed: false };
				subscriptions.push(subscription);
				return () => {
					subscription.closed = true;
				};
			}
		}
	});
	return { replica, scheduler, fetches, subscriptions };
}
