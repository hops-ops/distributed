import assert from 'node:assert/strict';
import test from 'node:test';

import { createCacheEngine } from '../dist/internal/cache-engine.js';
import { relationshipMembership } from '../dist/replica/index-maintenance/engine.js';
import { replaceOptimisticLayerOn } from '../dist/replica/distributed-replica/impl-optimistic.js';
import {
	createReplicaIndexMaintenanceRegistry,
	formatReplicaIndexStaleReason,
	replicaIndexKey,
	replicaRecordKey
} from '../dist/replica/index.js';

const Todo = Object.freeze({ id: 'Todo', identityFields: Object.freeze(['id']) });
const Board = Object.freeze({ id: 'Board', identityFields: Object.freeze(['id']) });
const Card = Object.freeze({ id: 'Card', identityFields: Object.freeze(['id']) });

test('direct candidate-key membership preserves row identity and SQL null semantics', () => {
	const plan = { path: ['object'], relationship: {
		keyMapping: { kind: 'direct', local: ['namespace', 'revision'], remote: ['scope', 'version'] }
	} };
	const parent = { fields: { id: 'parent-id', namespace: 'team-a', revision: 'v1' } };
	const records = new Map([['parent-key', parent]]);
	const branch = { metadata: { parent: 'parent-key' } };
	const target = { fields: { id: 'opaque-object-id', scope: 'team-a', version: 'v1' } };
	const membership = () => relationshipMembership(plan, branch, target, records);
	assert.deepEqual(membership(), { related: true });
	target.fields.scope = 'team-b';
	assert.deepEqual(membership(), { related: false });
	target.fields.scope = 'team-a';
	parent.fields.revision = 'v2';
	assert.deepEqual(membership(), { related: false });
	target.fields.version = 'v2';
	assert.deepEqual(membership(), { related: true });
	assert.equal(target.fields.id, 'opaque-object-id');
	parent.fields.revision = null;
	target.fields.version = null;
	assert.deepEqual(membership(), { related: false });
	delete target.fields.version;
	assert.equal(membership().reason.code, 'missing_field');
});

const COMPLETE = Object.freeze({ kind: 'complete' });
const COMPLETE_PAGINATION = Object.freeze({
	kind: 'complete',
	insert: 'local',
	delete: 'local',
	reorder: 'local',
	stableUpdate: 'local'
});
const OFFSET_PAGINATION = Object.freeze({
	kind: 'offset',
	insert: 'local',
	delete: 'local',
	reorder: 'local',
	stableUpdate: 'local'
});

const literal = (value) => Object.freeze({ kind: 'literal', value });
const variable = (name) => Object.freeze({ kind: 'variable', name });
const scalar = (responseKey, field) =>
	Object.freeze({
		kind: 'scalar',
		responseKey,
		field,
		codec: 'string',
		nullable: false
	});
const record = (model, identity, fields) =>
	Object.freeze({
		key: replicaRecordKey(model, identity),
		model: model.id,
		fields: Object.freeze({ ...fields })
	});
const index = (
	field,
	records,
	{
		parent,
		arguments: argumentsValue = {},
		coverage = COMPLETE,
		dependencies = ['todos'],
		complete = true,
		staleReason
	} = {}
) =>
	Object.freeze({
		key: replicaIndexKey({
			...(parent === undefined ? {} : { parent }),
			field,
			arguments: argumentsValue
		}),
		records: Object.freeze([...records]),
		complete,
		metadata: Object.freeze({
			...(parent === undefined ? {} : { parent }),
			field,
			arguments: Object.freeze(argumentsValue),
			coverage,
			dependencies: Object.freeze([...dependencies]),
			...(staleReason === undefined ? {} : { staleReason })
		})
	});

const FILTER_FIELDS = Object.freeze([
	Object.freeze({
		field: 'active',
		scalar: 'Boolean',
		codec: 'boolean',
		nullable: false,
		operators: Object.freeze(['_eq'])
	}),
	Object.freeze({
		field: 'id',
		scalar: 'ID',
		codec: 'string',
		nullable: false,
		operators: Object.freeze(['_eq'])
	}),
	Object.freeze({
		field: 'rank',
		scalar: 'Int',
		codec: 'int32',
		nullable: false,
		operators: Object.freeze(['_eq', '_gt', '_gte', '_lt', '_lte'])
	}),
	Object.freeze({
		field: 'tenantId',
		scalar: 'ID',
		codec: 'string',
		nullable: false,
		operators: Object.freeze(['_eq'])
	})
]);

const ORDER = Object.freeze({
	input: literal(Object.freeze([Object.freeze({ rank: 'asc' })])),
	fields: Object.freeze(
		FILTER_FIELDS.map(({ field, scalar: scalarName, codec, nullable }) =>
			Object.freeze({ field, scalar: scalarName, codec, nullable })
		)
	),
	tieBreakers: Object.freeze([
		Object.freeze({
			field: 'id',
			scalar: 'ID',
			codec: 'string',
			nullable: false
		})
	])
});

const unrestrictedFilter = (input = literal({ active: { _eq: true } })) =>
	Object.freeze({
		input,
		fields: FILTER_FIELDS,
		relationships: Object.freeze([]),
		rowPolicy: Object.freeze({ kind: 'unrestricted' })
	});

const TODO_SELECTION = Object.freeze({
	typename: Todo.id,
	storage: Object.freeze({
		kind: 'normalized',
		model: Todo.id,
		identityFields: Todo.identityFields
	}),
	members: Object.freeze([
		scalar('id', 'id'),
		scalar('active', 'active'),
		scalar('rank', 'rank'),
		scalar('tenantId', 'tenantId')
	])
});

const VARIABLE_CODEC = Object.freeze({
	version: 2,
	limits: Object.freeze({
		maxDepth: 8,
		maxBoolWidth: 256,
		maxInList: 1_000
	}),
	variables: Object.freeze({
		owner: Object.freeze({
			kind: 'scalar',
			scalar: 'ID',
			codec: 'string',
			nullable: false
		})
	}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({})
});

const NO_VARIABLES = Object.freeze({
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

function todoArtifact({
	id = 'Todos',
	responseKey = 'todos',
	where = unrestrictedFilter(),
	order = ORDER,
	pagination = COMPLETE_PAGINATION,
	coverage = COMPLETE,
	arguments: argumentsValue = Object.freeze({ owner: variable('owner') }),
	rowPolicy,
	trustedPresets = Object.freeze([]),
	extraRoots = []
} = {}) {
	const filter =
		rowPolicy === undefined
			? where
			: Object.freeze({ ...where, rowPolicy: Object.freeze(rowPolicy) });
	const root = Object.freeze({
		responseKey,
		field: 'todos',
		cardinality: 'many',
		nullable: false,
		arguments: argumentsValue,
		dependencies: Object.freeze(['todos']),
		coverage,
		filter,
		order,
		pagination,
		selection: TODO_SELECTION
	});
	return Object.freeze({
		id,
		document: `query ${id} { ${responseKey}: todos { id } }`,
		protocol: Object.freeze({
			version: 1,
			schemaHash: `sha256:${'1'.repeat(64)}`,
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: id,
			trustedPresets
		}),
		variableCodec: VARIABLE_CODEC,
		roots: Object.freeze([root, ...extraRoots])
	});
}

function snapshot(records, indexes) {
	return Object.freeze({
		records: Object.freeze(records),
		indexes: Object.freeze(indexes)
	});
}

function layer(id, changes) {
	return Object.freeze({ id, changes: Object.freeze(changes) });
}

function upsert(model, identity, fields, dependencies = ['todos']) {
	return Object.freeze({
		kind: 'upsert',
		model: model.id,
		key: replicaRecordKey(model, identity),
		fields: Object.freeze(fields),
		dependencies: Object.freeze(dependencies)
	});
}

test('canonical operation instances deduplicate aliases and maintain complete membership/order', () => {
	const baseA = record(Todo, 'a', {
		id: 'a',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const rootAlias = Object.freeze({
		...todoArtifact().roots[0],
		responseKey: 'sameTodos'
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(
		todoArtifact({ extraRoots: [rootAlias] }),
		{ owner: 1 }
	);
	const rootIndex = index('todos', [baseA.key], {
		arguments: { owner: '1' }
	});
	const insertB = upsert(Todo, 'b', {
		id: 'b',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});

	let decisions = registry.evaluate(
		snapshot([baseA], [rootIndex]),
		[layer('insert-b', [insertB])]
	);
	assert.deepEqual(decisions, [
		{
			kind: 'write',
			indexKey: rootIndex.key,
			records: [insertB.key, baseA.key],
			complete: true
		}
	]);

	decisions = registry.evaluate(
		snapshot([baseA], [rootIndex]),
		[
			layer('close-a', [
				upsert(Todo, 'a', {
					active: false
				})
			])
		]
	);
	assert.deepEqual(decisions[0], {
		kind: 'write',
		indexKey: rootIndex.key,
		records: [],
		complete: true
	});
});

test('maintenance registration rejects an unbound or mismatched operation artifact', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	const artifact = todoArtifact();
	assert.throws(
		() =>
			registry.registerOperation(
				Object.freeze({
					...artifact,
					protocol: Object.freeze({
						...artifact.protocol,
						operation: 'OtherOperation'
					})
				}),
				{ owner: 'tenant-1' }
			),
		/replica artifact protocol binding is invalid/
	);
});

test('semantic layers recompute from confirmed state when an earlier layer disappears', () => {
	const base = record(Todo, 'base', {
		id: 'base',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(todoArtifact(), { owner: 'owner-1' });
	const root = index('todos', [base.key], {
		arguments: { owner: 'owner-1' }
	});
	const insertA = upsert(Todo, 'a', {
		id: 'a',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const insertB = upsert(Todo, 'b', {
		id: 'b',
		active: true,
		rank: 3,
		tenantId: 'tenant-1'
	});

	assert.deepEqual(
		registry.evaluate(snapshot([base], [root]), [
			layer('A', [insertA]),
			layer('B', [insertB])
		])[0].records,
		[base.key, insertA.key, insertB.key]
	);
	assert.deepEqual(
		registry.evaluate(snapshot([base], [root]), [layer('B', [insertB])])[0]
			.records,
		[base.key, insertB.key]
	);
});

test('package-private replacement seam preserves replica receipt and diagnostic identity', () => {
	const engine = createCacheEngine();
	const key = replicaRecordKey(Todo, 'replacement-seam');
	engine.batch((writer) =>
		writer.writeRecord({
			key,
			revision: 11,
			fields: { title: 'base', status: 'open' }
		})
	);
	engine.createOptimisticLayer('lower', (writer) =>
		writer.writeRecord({ key, fields: { title: 'lower' } })
	);
	engine.createOptimisticLayer(
		'command-1',
		(writer) =>
			writer.writeRecord({ key, fields: { status: 'preview' } }),
		{ id: 'command-1', changes: [] }
	);
	assert.equal(engine.markOptimisticLayerAccepted('command-1'), true);
	engine.createOptimisticLayer('suffix', (writer) =>
		writer.writeRecord({ key, fields: { title: 'suffix' } })
	);

	const receipt = {
		causationId: 'causation-1',
		expectations: new Map(),
		observed: new Set()
	};
	const diagnostic = Object.freeze({
		id: 'command-1',
		sequence: 2,
		state: 'accepted',
		recordChanges: 1,
		indexChanges: 0,
		semanticChanges: 1
	});
	const events = [];
	let syncCalls = 0;
	let diagnosticSequence = 3;
	const host = {
		engine,
		optimisticReceipts: new Map([['command-1', receipt]]),
		diagnosticLayers: new Map([['command-1', diagnostic]]),
		diagnostics: { enabled: true },
		getDiagnosticLayerSequence: () => diagnosticSequence,
		setDiagnosticLayerSequence: (value) => {
			diagnosticSequence = value;
		},
		diagnosticEvent: (event) => events.push(event),
		syncDiagnostics: () => {
			syncCalls += 1;
		},
		retireDiagnosticLayer: () => {
			throw new Error('replacement must not retire its layer');
		}
	};
	const confirmed = engine.extract();
	let prefix;
	assert.equal(
		replaceOptimisticLayerOn(host, 'command-1', (reader, writer) => {
			prefix = reader.record(key)?.fields;
			writer.writeRecord({ key, fields: { status: 'actual' } });
		}),
		true
	);

	assert.deepEqual(prefix, { title: 'lower', status: 'open' });
	assert.deepEqual(engine.read((reader) => reader.record(key)?.fields), {
		title: 'suffix',
		status: 'actual'
	});
	assert.equal(engine.optimisticLayerState('command-1'), 'accepted');
	assert.equal(host.optimisticReceipts.get('command-1'), receipt);
	assert.equal(host.diagnosticLayers.get('command-1'), diagnostic);
	assert.equal(syncCalls, 0);
	assert.deepEqual(events, []);
	assert.deepEqual(engine.extract(), confirmed);
});

test('a stale complete index keeps a later patch ordered during rebase', () => {
	const earlier = record(Todo, 'earlier', {
		id: 'earlier',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const target = record(Todo, 'target', {
		id: 'target',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(todoArtifact(), { owner: 'owner-1' });
	const root = index('todos', [earlier.key, target.key], {
		arguments: { owner: 'owner-1' },
		staleReason: 'command-authoritative-revalidation'
	});

	const decisions = registry.evaluate(snapshot([earlier, target], [root]), [
		layer('later-rank-change', [
			upsert(Todo, 'target', {
				rank: 0
			})
		])
	]);

	assert.deepEqual(decisions[0], {
		kind: 'write',
		indexKey: root.key,
		records: [target.key, earlier.key],
		complete: true
	});
});

test('one record change updates every distinct exact root index once', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	const openArtifact = todoArtifact({
		id: 'OpenTodos',
		arguments: Object.freeze({
			owner: variable('owner'),
			state: literal('open')
		})
	});
	const allArtifact = todoArtifact({
		id: 'AllTodos',
		arguments: Object.freeze({
			owner: variable('owner'),
			state: literal('all')
		}),
		where: unrestrictedFilter(literal({}))
	});
	registry.registerOperation(openArtifact, { owner: 'one' });
	registry.registerOperation(allArtifact, { owner: 'one' });
	const open = index('todos', [], {
		arguments: { owner: 'one', state: 'open' }
	});
	const all = index('todos', [], {
		arguments: { owner: 'one', state: 'all' }
	});
	const inserted = upsert(Todo, 'new', {
		id: 'new',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const decisions = registry.evaluate(
		snapshot([], [open, all]),
		[layer('insert', [inserted])]
	);
	assert.equal(decisions.length, 2);
	assert.deepEqual(
		decisions.map(({ indexKey }) => indexKey).sort(),
		[all.key, open.key].sort()
	);
	assert.ok(decisions.every(({ records }) => records[0] === inserted.key));
});

test('reorders are deterministic and duplicates fail closed', () => {
	const a = record(Todo, 'a', {
		id: 'a',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const b = record(Todo, 'b', {
		id: 'b',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(todoArtifact(), { owner: 'one' });
	const root = index('todos', [a.key, b.key], {
		arguments: { owner: 'one' }
	});
	const reordered = registry.evaluate(
		snapshot([a, b], [root]),
		[layer('reorder', [upsert(Todo, 'a', { rank: 3 })])]
	);
	assert.deepEqual(reordered[0].records, [b.key, a.key]);

	const duplicate = registry.evaluate(
		snapshot(
			[a, b],
			[
				Object.freeze({
					...root,
					records: Object.freeze([a.key, a.key])
				})
			]
		),
		[layer('touch', [upsert(Todo, 'a', { rank: 3 })])]
	);
	assert.equal(duplicate[0].kind, 'stale');
	assert.equal(duplicate[0].reason.code, 'duplicate_index_record');
});

test('missing dependencies, claims, and offset windows become precise stale decisions', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(todoArtifact(), { owner: 'one' });
	const complete = index('todos', [], { arguments: { owner: 'one' } });
	const missing = upsert(Todo, 'missing', {
		id: 'missing',
		rank: 1,
		tenantId: 'tenant-1'
	});
	let decision = registry.evaluate(
		snapshot([], [complete]),
		[layer('missing', [missing])]
	)[0];
	assert.equal(decision.kind, 'stale');
	assert.equal(decision.reason.code, 'missing_field');
	assert.match(formatReplicaIndexStaleReason(decision.reason), /^query-plan:missing_field:/);

	const claimPolicy = Object.freeze({
		kind: 'predicate',
		expression: Object.freeze({
			kind: 'cmp',
			value: Object.freeze({
				column: 'rank',
				op: 'eq',
				rhs: Object.freeze({
					kind: 'claim',
					value: Object.freeze({ header: 'x-distributed-rank' })
				})
			})
		})
	});
	const claimDescriptor = Object.freeze({
		name: 'x-distributed-rank',
		codec: 'int32'
	});
	const claimed = createReplicaIndexMaintenanceRegistry();
	claimed.registerOperation(
		todoArtifact({
			id: 'ClaimedTodos',
			rowPolicy: claimPolicy,
			trustedPresets: Object.freeze([claimDescriptor])
		}),
		{ owner: 'one' }
	);
	decision = claimed.evaluate(
		snapshot([], [complete]),
		[
			layer('claimed', [
				upsert(Todo, 'claimed', {
					id: 'claimed',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		]
	)[0];
	assert.equal(decision.reason.code, 'claim_inventory');

	const scopedRank = (value) =>
		Object.freeze([
			Object.freeze({
				...claimDescriptor,
				value
			})
		]);
	decision = claimed.evaluate(
		snapshot([], [complete]),
		[
			layer('claimed', [
				upsert(Todo, 'claimed', {
					id: 'claimed',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		],
		scopedRank(1)
	)[0];
	assert.deepEqual(decision, {
		kind: 'write',
		indexKey: complete.key,
		records: [replicaRecordKey(Todo, 'claimed')],
		complete: true
	});

	decision = claimed.evaluate(
		snapshot([], [complete]),
		[
			layer('claimed', [
				upsert(Todo, 'claimed', {
					id: 'claimed',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		],
		scopedRank(2)
	)[0];
	assert.equal(decision.kind, 'unchanged');

	decision = claimed.evaluate(
		snapshot([], [complete]),
		[
			layer('claimed', [
				upsert(Todo, 'claimed', {
					id: 'claimed',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		],
		[
			...scopedRank(1),
			{ name: 'x-forged-extra', codec: 'string', value: 'forged' }
		]
	)[0];
	assert.equal(decision.kind, 'stale');
	assert.equal(decision.reason.code, 'claim_inventory');

	const offsetRegistry = createReplicaIndexMaintenanceRegistry();
	offsetRegistry.registerOperation(
		todoArtifact({
			id: 'OffsetTodos',
			pagination: OFFSET_PAGINATION,
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				limitArgument: 'limit'
			}),
			arguments: Object.freeze({
				owner: variable('owner'),
				offset: literal(0),
				limit: literal(10)
			})
		}),
		{ owner: 'one' }
	);
	const offset = index('todos', [], {
		arguments: { owner: 'one', offset: 0, limit: 10 },
		coverage: { kind: 'offset', offset: 0, limit: 10, returned: 0 }
	});
	decision = offsetRegistry.evaluate(
		snapshot([], [offset]),
		[
			layer('offset-insert', [
				upsert(Todo, 'offset', {
					id: 'offset',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		]
	)[0];
	assert.deepEqual(decision, {
		kind: 'write',
		indexKey: offset.key,
		records: [replicaRecordKey(Todo, 'offset')],
		complete: true
	});

	const full = Object.freeze({
		...offset,
		records: Object.freeze(
			Array.from({ length: 10 }, (_, item) => replicaRecordKey(Todo, `base-${item}`))
		),
		metadata: Object.freeze({
			...offset.metadata,
			coverage: Object.freeze({
				kind: 'offset',
				offset: 0,
				limit: 10,
				returned: 10
			})
		})
	});
	decision = offsetRegistry.evaluate(
		snapshot(
			Array.from({ length: 10 }, (_, item) =>
				record(Todo, `base-${item}`, {
					id: `base-${item}`,
					active: true,
					rank: item + 2,
					tenantId: 'tenant-1'
				})
			),
			[full]
		),
		[
			layer('full-offset-insert', [
				upsert(Todo, 'offset', {
					id: 'offset',
					active: true,
					rank: 1,
					tenantId: 'tenant-1'
				})
			])
		]
	)[0];
	// Full first page still accepts inserts: re-sort + truncate to limit.
	assert.equal(decision.kind, 'write');
	assert.equal(decision.records.length, 10);
	assert.equal(decision.records[0], replicaRecordKey(Todo, 'offset'));
	assert.equal(
		decision.records.includes(replicaRecordKey(Todo, 'base-9')),
		false,
		'drops the worst page member after the optimistic insert'
	);

	const first = record(Todo, 'first', {
		id: 'first',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const boundary = record(Todo, 'boundary', {
		id: 'boundary',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const fullBoundary = index('todos', [first.key, boundary.key], {
		arguments: { owner: 'one', offset: 0, limit: 10 },
		coverage: { kind: 'offset', offset: 0, limit: 10, returned: 2 }
	});
	const forgedCoverage = (coverage) =>
		Object.freeze({
			...fullBoundary,
			metadata: Object.freeze({
				...fullBoundary.metadata,
				coverage: Object.freeze(coverage)
			})
		});
	// Mismatched offset / returned still fail closed.
	for (const coverage of [
		{ kind: 'offset', offset: 1, limit: 10, returned: 2 },
		{ kind: 'offset', offset: 0, limit: 10, returned: 1 }
	]) {
		decision = offsetRegistry.evaluate(
			snapshot([first, boundary], [forgedCoverage(coverage)]),
			[
				layer('coverage-mismatch', [
					upsert(Todo, 'third', {
						id: 'third',
						active: true,
						rank: 3,
						tenantId: 'tenant-1'
					})
				])
			]
		)[0];
		assert.equal(decision.kind, 'stale');
		assert.equal(decision.reason.code, 'invalid_index_metadata');
	}
	// hasNext on the first page does not block optimistic inserts.
	decision = offsetRegistry.evaluate(
		snapshot(
			[first, boundary],
			[
				forgedCoverage({
					kind: 'offset',
					offset: 0,
					limit: 10,
					returned: 2,
					hasNext: true
				})
			]
		),
		[
			layer('has-next-insert', [
				upsert(Todo, 'third', {
					id: 'third',
					active: true,
					rank: 0,
					tenantId: 'tenant-1'
				})
			])
		]
	)[0];
	assert.equal(decision.kind, 'write');
	assert.deepEqual(decision.records, [
		replicaRecordKey(Todo, 'third'),
		first.key,
		boundary.key
	]);
});

test('stacked local offset inserts preserve the exact first-page limit', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(
		todoArtifact({
			id: 'SafeOffsetTodos',
			pagination: OFFSET_PAGINATION,
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				limitArgument: 'limit'
			}),
			arguments: Object.freeze({
				owner: variable('owner'),
				offset: literal(0),
				limit: literal(2)
			})
		}),
		{ owner: 'one' }
	);
	const base = record(Todo, 'base', {
		id: 'base',
		active: true,
		rank: 3,
		tenantId: 'tenant-1'
	});
	const root = index('todos', [base.key], {
		arguments: { owner: 'one', offset: 0, limit: 2 },
		coverage: { kind: 'offset', offset: 0, limit: 2, returned: 1 }
	});
	const first = upsert(Todo, 'first', {
		id: 'first',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const second = upsert(Todo, 'second', {
		id: 'second',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const decision = registry.evaluate(
		snapshot([base], [root]),
		[layer('first', [first]), layer('second', [second])]
	)[0];
	assert.deepEqual(decision.records, [first.key, second.key]);

	assert.deepEqual(
		registry.evaluate(
			snapshot([base], [root]),
			[layer('second', [second])]
		)[0].records,
		[second.key, base.key]
	);
	assert.deepEqual(
		registry.evaluate(
			snapshot([base], [root]),
			[layer('first', [first])]
		)[0].records,
		[first.key, base.key]
	);
});

test('offset order changes use net tuples and full windows fail closed at unseen boundaries', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(
		todoArtifact({
			id: 'OffsetOrderTodos',
			pagination: OFFSET_PAGINATION,
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				limitArgument: 'limit'
			}),
			arguments: Object.freeze({
				owner: variable('owner'),
				offset: literal(0),
				limit: literal(2)
			})
		}),
		{ owner: 'one' }
	);
	const a = record(Todo, 'a', {
		id: 'a',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const b = record(Todo, 'b', {
		id: 'b',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const full = index('todos', [a.key, b.key], {
		arguments: { owner: 'one', offset: 0, limit: 2 },
		coverage: { kind: 'offset', offset: 0, limit: 2, returned: 2 }
	});

	let decision = registry.evaluate(
		snapshot([a, b], [full]),
		[layer('boundary', [upsert(Todo, 'b', { rank: 100 })])]
	)[0];
	assert.equal(decision.kind, 'stale');
	assert.equal(decision.reason.code, 'reorder_changes_offset_window');

	decision = registry.evaluate(
		snapshot([a, b], [full]),
		[
			layer('away', [upsert(Todo, 'b', { rank: 100 })]),
			layer('restore', [upsert(Todo, 'b', { rank: 2 })])
		]
	)[0];
	assert.equal(decision.kind, 'unchanged');

	const nonFull = Object.freeze({
		...full,
		metadata: Object.freeze({
			...full.metadata,
			coverage: Object.freeze({
				kind: 'offset',
				offset: 0,
				limit: 3,
				returned: 2
			}),
			arguments: Object.freeze({ owner: 'one', offset: 0, limit: 3 })
		}),
		key: replicaIndexKey({
			field: 'todos',
			arguments: { owner: 'one', offset: 0, limit: 3 }
		})
	});
	const nonFullRegistry = createReplicaIndexMaintenanceRegistry();
	nonFullRegistry.registerOperation(
		todoArtifact({
			id: 'NonFullOffsetOrderTodos',
			pagination: OFFSET_PAGINATION,
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				limitArgument: 'limit'
			}),
			arguments: Object.freeze({
				owner: variable('owner'),
				offset: literal(0),
				limit: literal(3)
			})
		}),
		{ owner: 'one' }
	);
	decision = nonFullRegistry.evaluate(
		snapshot([a, b], [nonFull]),
		[layer('local-reorder', [upsert(Todo, 'a', { rank: 3 })])]
	)[0];
	assert.deepEqual(decision.records, [b.key, a.key]);

	decision = nonFullRegistry.evaluate(
		snapshot([a, b], [nonFull]),
		[layer('local-delete', [Object.freeze({
			kind: 'delete',
			model: Todo.id,
			key: a.key
		})])]
	)[0];
	assert.deepEqual(decision.records, [b.key]);
});

test('cursor windows remain stale at integration boundaries without compiler proof IR', () => {
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(
		todoArtifact({
			id: 'CursorTodos',
			pagination: Object.freeze({
				kind: 'cursor',
				// Neither a forged flag nor local dispositions constitute a
				// versioned compiler proof understood by this runtime.
				certified: true,
				insert: 'local',
				delete: 'local',
				reorder: 'local',
				stableUpdate: 'local'
			}),
			coverage: Object.freeze({
				kind: 'cursor',
				afterArgument: 'after',
				firstArgument: 'first'
			}),
			arguments: Object.freeze({
				owner: variable('owner'),
				after: literal('cursor-a'),
				first: literal(10)
			})
		}),
		{ owner: 'one' }
	);
	const window = index('todos', [], {
		arguments: { owner: 'one', after: 'cursor-a', first: 10 },
		coverage: {
			kind: 'cursor',
			after: 'cursor-a',
			first: 10,
			start: 'cursor-b',
			end: 'cursor-z',
			hasNext: true
		}
	});
	const decision = registry.evaluate(
		snapshot([], [window]),
		[
			layer('cursor-boundary-insert', [
				upsert(Todo, 'cursor-c', {
					id: 'cursor-c',
					active: true,
					rank: 3,
					tenantId: 'tenant-1'
				})
			])
		]
	)[0];
	assert.equal(decision.kind, 'stale');
	assert.equal(decision.reason.code, 'cursor_not_certified');
});

function relationshipArtifact(
	relationship,
	{
		id = 'BoardCards',
		filter = unrestrictedFilter(),
		order = ORDER
	} = {}
) {
	const selection = Object.freeze({
		typename: Board.id,
		storage: Object.freeze({
			kind: 'normalized',
			model: Board.id,
			identityFields: Board.identityFields
		}),
		members: Object.freeze([
			scalar('id', 'id'),
			Object.freeze({
				kind: 'branch',
				semantic: 'relationship',
				responseKey: 'cards',
				field: 'cards',
				cardinality: 'many',
				nullable: false,
				dependencies: relationship.dependencies,
				coverage: COMPLETE,
				filter,
				order,
				pagination: COMPLETE_PAGINATION,
				relationship,
				selection: Object.freeze({
					typename: Card.id,
					storage: Object.freeze({
						kind: 'normalized',
						model: Card.id,
						identityFields: Card.identityFields
					}),
					members: TODO_SELECTION.members
				})
			})
		])
	});
	return Object.freeze({
		id,
		document: `query ${id} { board { cards { id } } }`,
		protocol: Object.freeze({
			version: 1,
			schemaHash: `sha256:${'1'.repeat(64)}`,
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: id,
			trustedPresets: Object.freeze([])
		}),
		variableCodec: NO_VARIABLES,
		roots: Object.freeze([
			Object.freeze({
				responseKey: 'board',
				field: 'board_by_pk',
				cardinality: 'one',
				nullable: false,
				dependencies: Object.freeze(['boards']),
				selection
			})
		])
	});
}

const M2M = Object.freeze({
	field: 'cards',
	targetModel: 'Card',
	kind: 'many_to_many',
	keyMapping: Object.freeze({
		kind: 'through',
		local: Object.freeze(['id']),
		remote: Object.freeze(['id']),
		table: 'board_cards',
		sourceForeignKey: Object.freeze(['board_id']),
		targetForeignKey: Object.freeze(['card_id'])
	}),
	maintenance: 'local',
	dependencies: Object.freeze(['board_cards', 'boards', 'cards'])
});

test('many-to-many link/unlink recomputes semantically and requires join dependencies', () => {
	const board = record(Board, 'board-1', { id: 'board-1' });
	const first = record(Card, 'card-1', {
		id: 'card-1',
		active: true,
		rank: 1,
		tenantId: 'tenant-1'
	});
	const second = record(Card, 'card-2', {
		id: 'card-2',
		active: true,
		rank: 2,
		tenantId: 'tenant-1'
	});
	const cards = index('cards', [first.key], {
		parent: board.key,
		dependencies: M2M.dependencies
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(relationshipArtifact(M2M), {});
	const link = Object.freeze({
		kind: 'link',
		sourceModel: 'Board',
		field: 'cards',
		targetModel: 'Card',
		sourceKey: board.key,
		targetKey: second.key,
		dependencies: M2M.dependencies
	});
	let decision = registry.evaluate(
		snapshot([board, first, second], [cards]),
		[layer('link', [link])]
	)[0];
	assert.deepEqual(decision.records, [first.key, second.key]);

	decision = registry.evaluate(
		snapshot([board, first, second], [cards]),
		[
			layer('unlink', [
				Object.freeze({ ...link, kind: 'unlink', targetKey: first.key })
			])
		]
	)[0];
	assert.deepEqual(decision.records, []);

	decision = registry.evaluate(
		snapshot([board, first, second], [cards]),
		[
			layer('unsafe-link', [
				Object.freeze({
					...link,
					dependencies: Object.freeze(['boards', 'cards'])
				})
			])
		]
	)[0];
	assert.equal(decision.kind, 'stale');
	assert.equal(decision.reason.code, 'relationship_dependency_missing');
});

test('relationship target changes and opaque mappings fail closed without explicit proof', () => {
	const board = record(Board, 'board-1', { id: 'board-1' });
	const cards = index('cards', [], {
		parent: board.key,
		dependencies: M2M.dependencies
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(relationshipArtifact(M2M), {});
	let decision = registry.evaluate(
		snapshot([board], [cards]),
		[
			layer('unproven-target', [
				upsert(
					Card,
					'card-1',
					{
						id: 'card-1',
						active: true,
						rank: 1,
						tenantId: 'tenant-1'
					},
					['cards']
				)
			])
		]
	)[0];
	assert.equal(decision.reason.code, 'relationship_mapping_unknown');

	const opaque = Object.freeze({
		...M2M,
		keyMapping: Object.freeze({
			kind: 'through_opaque',
			local: Object.freeze(['id']),
			remote: Object.freeze(['id']),
			dependency: 'private_membership'
		}),
		maintenance: 'revalidate',
		dependencies: Object.freeze([
			'private_membership',
			'boards',
			'cards'
		])
	});
	const opaqueRegistry = createReplicaIndexMaintenanceRegistry();
	opaqueRegistry.registerOperation(
		relationshipArtifact(opaque, { id: 'OpaqueCards' }),
		{}
	);
	const opaqueCards = index('cards', [], {
		parent: board.key,
		dependencies: opaque.dependencies
	});
	decision = opaqueRegistry.evaluate(
		snapshot([board], [opaqueCards]),
		[
			layer('opaque-link', [
				Object.freeze({
					kind: 'link',
					sourceModel: 'Board',
					field: 'cards',
					targetModel: 'Card',
					sourceKey: board.key,
					targetKey: replicaRecordKey(Card, 'card-1'),
					dependencies: opaque.dependencies
				})
			])
		]
	)[0];
	assert.equal(decision.reason.code, 'relationship_maintenance_revalidate');
});

test('aggregate snapshots invalidate from declared dependencies and registry clear fences scope changes', () => {
	const aggregateArtifact = Object.freeze({
		id: 'TodoAggregate',
		document: 'query TodoAggregate { todos_aggregate { aggregate { count } } }',
		protocol: Object.freeze({
			version: 1,
			schemaHash: `sha256:${'1'.repeat(64)}`,
			surface: Object.freeze({ kind: 'role', name: 'user' }),
			operation: 'TodoAggregate',
			trustedPresets: Object.freeze([])
		}),
		variableCodec: NO_VARIABLES,
		roots: Object.freeze([
			Object.freeze({
				responseKey: 'todos_aggregate',
				field: 'todos_aggregate',
				cardinality: 'one',
				nullable: false,
				dependencies: Object.freeze(['todos']),
				selection: Object.freeze({
					typename: 'todo_aggregate',
					storage: Object.freeze({ kind: 'embedded' }),
					members: Object.freeze([])
				})
			})
		])
	});
	const aggregate = index('todos_aggregate', ['embedded:aggregate'], {
		dependencies: ['todos']
	});
	const registry = createReplicaIndexMaintenanceRegistry();
	registry.registerOperation(aggregateArtifact, {});
	let decisions = registry.evaluate(
		snapshot([], [aggregate]),
		[
			layer('invalidate', [
				Object.freeze({
					kind: 'invalidate',
					dependencies: Object.freeze(['todos'])
				})
			])
		]
	);
	assert.equal(decisions[0].kind, 'stale');
	assert.equal(decisions[0].reason.code, 'aggregate_dependency_changed');

	registry.clear();
	decisions = registry.evaluate(
		snapshot([], [aggregate]),
		[
			layer('old-scope', [
				Object.freeze({
					kind: 'invalidate',
					dependencies: Object.freeze(['todos'])
				})
			])
		]
	);
	assert.deepEqual(decisions, []);
});

test('conditional patches do not synthesize a row when an earlier create is rejected', () => {
	const engine = createCacheEngine();
	const dependent = 'Todo:["dependent"]';
	const independent = 'Todo:["independent"]';
	engine.createOptimisticLayer('create', (writer) =>
		writer.writeRecord({
			key: dependent,
			fields: { id: 'dependent', title: 'preview' }
		})
	);
	engine.createOptimisticLayer('patch', (writer) =>
		writer.writeRecord({
			key: dependent,
			fields: { status: 'completed' },
			unset: ['title'],
			ifPresent: true
		})
	);
	engine.createOptimisticLayer('independent', (writer) =>
		writer.writeRecord({
			key: independent,
			fields: { id: 'independent', title: 'safe' }
		})
	);
	assert.deepEqual(engine.read((reader) => reader.record(dependent)?.fields), {
		id: 'dependent',
		status: 'completed'
	});

	assert.equal(engine.rejectOptimisticLayer('create'), true);
	assert.equal(engine.read((reader) => reader.record(dependent)), undefined);
	assert.deepEqual(engine.read((reader) => reader.record(independent)?.fields), {
		id: 'independent',
		title: 'safe'
	});
});
