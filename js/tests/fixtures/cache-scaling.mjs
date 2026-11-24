/**
 * A replica holding many small watched and unwatched lists, driven by live
 * frames that each change one record. Shared by the scaling test and
 * `scripts/bench-cache-scaling.mjs`.
 */
import { DISTRIBUTED_PROTOCOL_VERSION } from '../../dist/index.js';
import { createDistributedReplica } from '../../dist/replica/index.js';
import { NoVariables } from './replica-todos.mjs';

/** Games per list; every list also holds its one owner. */
export const LIST_GAMES = 19;
export const Game = Object.freeze({
	id: 'GameView',
	identityFields: Object.freeze(['id'])
});
const LIST_RECORDS = LIST_GAMES + 1;

const scalar = (field, codec) =>
	Object.freeze({ kind: 'scalar', responseKey: field, field, codec, nullable: false });

/** `games_<n> { id title owner_id owner { id name } }`, with its live twin. */
function listArtifact(list) {
	const field = `games_${list}`;
	return Object.freeze({
		id: `query:${field}`,
		document: `query Games${list} { ${field} { id title owner_id owner { id name } } }`,
		protocol: Object.freeze({
			version: 1,
			schemaHash: 'schema-a',
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: `query:${field}`,
			trustedPresets: Object.freeze([])
		}),
		variableCodec: NoVariables,
		live: Object.freeze({
			id: `live:${field}`,
			document: `subscription Games${list}Live { ${field} { id title owner_id owner { id name } } }`
		}),
		roots: Object.freeze([
			Object.freeze({
				responseKey: field,
				field,
				cardinality: 'many',
				nullable: false,
				dependencies: Object.freeze([field]),
				selection: Object.freeze({
					typename: 'GameView',
					storage: Object.freeze({
						kind: 'normalized',
						model: 'GameView',
						identityFields: Object.freeze(['id'])
					}),
					members: Object.freeze([
						scalar('id', 'ID'),
						scalar('title', 'String'),
						scalar('owner_id', 'ID'),
						Object.freeze({
							kind: 'branch',
							semantic: 'relationship',
							responseKey: 'owner',
							field: 'owner',
							cardinality: 'one',
							nullable: false,
							dependencies: Object.freeze([field, 'users']),
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
								dependencies: Object.freeze([field, 'users'])
							}),
							selection: Object.freeze({
								typename: 'UserView',
								storage: Object.freeze({
									kind: 'normalized',
									model: 'UserView',
									identityFields: Object.freeze(['id'])
								}),
								members: Object.freeze([scalar('id', 'ID'), scalar('name', 'String')])
							})
						})
					])
				})
			})
		])
	});
}

/** One list's rows and the revision each record was last written at. */
function listState(list) {
	const owner = `user-${list}`;
	return {
		list,
		field: `games_${list}`,
		artifact: listArtifact(list),
		position: 1,
		ownerRevision: 1,
		rows: Array.from({ length: LIST_GAMES }, (_, row) => ({
			id: `game-${list}-${row}`,
			title: `title ${row}`,
			owner_id: owner,
			owner: { id: owner, name: `owner ${list}` },
			revision: 1
		}))
	};
}

function frame(state, operation, live) {
	const position = String(state.position);
	const projection = `${state.field}-projector`;
	const resume = { projection, position, token: `resume:${position}` };
	return {
		data: {
			[state.field]: state.rows.map(({ revision: _revision, ...row }) => row)
		},
		extensions: {
			distributed: {
				protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
				schemaHash: 'schema-a',
				authorizationGeneration: 'auth-1',
				cacheScope: 'cache:a',
				operation,
				snapshot: {
					scopeToken: `snapshot:${operation}`,
					recordsComplete: true,
					indexesComparable: true,
					records: state.rows.flatMap((row, index) => [
						{
							path: [state.field, String(index)],
							model: 'GameView',
							scopeToken: `record:${row.id}`,
							incarnation: '1',
							revision: String(row.revision),
							tombstone: false
						},
						{
							path: [state.field, String(index), 'owner'],
							model: 'UserView',
							scopeToken: `record:${row.owner_id}`,
							incarnation: '1',
							revision: String(state.ownerRevision),
							tombstone: false
						}
					]),
					indexes: [
						{ projection, scopeToken: `index:${state.field}`, position, resume }
					],
					observations: []
				},
				...(live === undefined
					? {}
					: { live: { mode: 'resumable', reset: live.reset, cursors: [resume] } })
			}
		}
	};
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

/**
 * Build a replica with about `records` records in lists of twenty, the first
 * `watchers` lists watched live and the rest only written.
 *
 * `delta(n)` sends the live frame that retitles one game of watched list
 * `n % hot`: one record changes, so one watcher's result changes.
 */
export async function scalingReplica({
	records,
	watchers = 50,
	hot = 5,
	replicaOptions = {}
}) {
	const lists = Math.max(watchers, Math.ceil(records / LIST_RECORDS));
	const subscriptions = new Map();
	const replica = createDistributedReplica({
		...replicaOptions,
		transport: {
			fetch: () => new Promise(() => {}),
			subscribe(request, observer) {
				subscriptions.set(request.operationName ?? request.query, observer);
				subscriptions.set(subscriptions.size, observer);
				return () => {};
			}
		}
	});
	const states = [];
	const notifications = new Array(watchers).fill(0);
	const watches = [];
	for (let list = 0; list < lists; list += 1) {
		const state = listState(list);
		states.push(state);
		if (list >= watchers) {
			replica.writeResult(
				state.artifact,
				{},
				frame(state, state.artifact.id),
				'network'
			);
			continue;
		}
		const before = subscriptions.size;
		const watch = replica.watch(state.artifact, {}, { live: true });
		watch.subscribe(() => {
			notifications[list] += 1;
		});
		watches.push(watch);
		await flush();
		if (subscriptions.size === before) {
			throw new Error(`list ${list} opened no live stream`);
		}
		state.observer = [...subscriptions.values()].at(-1);
		state.observer.next(frame(state, state.artifact.live.id, { reset: true }));
	}
	notifications.fill(0);
	return {
		replica,
		watches,
		states,
		notifications,
		lists,
		records: lists * LIST_RECORDS,
		/**
		 * Build the next single-record live frame without applying it:
		 * `update` retitles one game, `insert` appends a new game.
		 */
		prepare(n, kind = 'update') {
			const state = states[n % hot];
			state.position += 1;
			let row;
			if (kind === 'insert') {
				const owner = state.rows[0].owner;
				row = {
					id: `game-${state.list}-new-${n}`,
					title: `inserted ${n}`,
					owner_id: owner.id,
					owner,
					revision: state.position
				};
				state.rows.push(row);
			} else {
				row = state.rows[n % LIST_GAMES];
				row.revision = state.position;
				row.title = `title ${n} at ${state.position}`;
			}
			const next = frame(state, state.artifact.live.id, { reset: false });
			return { list: state.list, row, apply: () => state.observer.next(next) };
		}
	};
}
