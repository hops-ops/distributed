import assert from 'node:assert/strict';
import test from 'node:test';

import {
	compareReplicaOrder,
	decideReplicaPaginationMaintenance,
	evaluateReplicaFilter
} from '../dist/replica/index.js';
import { resolveArguments } from '../dist/replica/identity.js';

const FILTER_FIELDS = Object.freeze([
	Object.freeze({
		field: 'id',
		scalar: 'ID',
		codec: 'string',
		nullable: false,
		operators: Object.freeze(['_eq', '_neq', '_in', '_nin'])
	}),
	Object.freeze({
		field: 'priority',
		scalar: 'Int',
		codec: 'int32',
		nullable: true,
		operators: Object.freeze([
			'_eq',
			'_neq',
			'_gt',
			'_gte',
			'_lt',
			'_lte',
			'_in',
			'_nin',
			'_is_null'
		])
	}),
	Object.freeze({
		field: 'active',
		scalar: 'Boolean',
		codec: 'boolean',
		nullable: false,
		operators: Object.freeze(['_eq', '_neq'])
	}),
	Object.freeze({
		field: 'title',
		scalar: 'String',
		codec: 'string',
		nullable: true,
		operators: Object.freeze([
			'_eq',
			'_neq',
			'_gt',
			'_in',
			'_nin',
			'_like',
			'_ilike',
			'_icontains'
		])
	}),
	Object.freeze({
		field: 'payload',
		scalar: 'JSON',
		codec: 'json',
		nullable: true,
		operators: Object.freeze(['_eq', '_contains'])
	})
]);

const OWNER_RELATIONSHIP = Object.freeze({
	field: 'owner',
	targetModel: 'User',
	kind: 'belongs_to',
	keyMapping: Object.freeze({
		kind: 'direct',
		local: Object.freeze(['ownerId']),
		remote: Object.freeze(['id'])
	}),
	maintenance: 'local',
	dependencies: Object.freeze(['todo_rows', 'user_rows'])
});

function literal(value) {
	return Object.freeze({ kind: 'literal', value: Object.freeze(value) });
}

function filterArtifact(
	input,
	rowPolicy = Object.freeze({ kind: 'unrestricted' }),
	relationships = Object.freeze([OWNER_RELATIONSHIP])
) {
	return Object.freeze({
		...(input === undefined ? {} : { input }),
		fields: FILTER_FIELDS,
		relationships,
		rowPolicy
	});
}

function result(value) {
	return value.result;
}

test('caller where uses SQL three-valued boolean composition', () => {
	const record = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	const falseDominatesUnknown = filterArtifact(
		literal({
			_and: [
				{ priority: { _eq: null } },
				{ active: { _eq: false } }
			]
		})
	);
	assert.equal(result(evaluateReplicaFilter(falseDominatesUnknown, record)), 'no_match');

	const trueDominatesUnknown = filterArtifact(
		literal({
			_or: [
				{ priority: { _eq: null } },
				{ active: { _eq: true } }
			]
		})
	);
	assert.equal(result(evaluateReplicaFilter(trueDominatesUnknown, record)), 'match');

	const stillUnknown = filterArtifact(
		literal({ _not: { priority: { _eq: null } } })
	);
	assert.deepEqual(evaluateReplicaFilter(stillUnknown, record), {
		result: 'unknown',
		reason: {
			code: 'sql_null',
			path: ['where', '_not', 'priority', '_eq'],
			message: 'SQL comparison with null is unknown'
		}
	});
});

test('caller and row-policy empty boolean lists preserve their distinct server semantics', () => {
	const record = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	assert.equal(
		result(evaluateReplicaFilter(filterArtifact(literal({ _and: [] })), record)),
		'match'
	);
	assert.equal(
		result(evaluateReplicaFilter(filterArtifact(literal({ _or: [] })), record)),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(undefined, {
					kind: 'predicate',
					expression: { kind: 'and', value: [] }
				}),
				record
			)
		),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(undefined, {
					kind: 'predicate',
					expression: { kind: 'or', value: [] }
				}),
				record
			)
		),
		'no_match'
	);
});

test('portable scalar filters handle numeric comparisons and exact SQL IN null behavior', () => {
	const base = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _gte: 3, _lt: 4 } })),
				base
			)
		),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _in: [] } })),
				{ ...base, priority: null }
			)
		),
		'no_match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _nin: [] } })),
				{ ...base, priority: null }
			)
		),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _in: [3, null] } })),
				base
			)
		),
		'match'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ priority: { _in: [2, null] } })),
			base
		).reason.code,
		'sql_null'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ priority: { _nin: [2, null] } })),
			base
		).reason.code,
		'sql_null'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ priority: { _in: ['bad', 3] } })),
			base
		).reason.code,
		'invalid_filter_value'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ priority: { _nin: ['bad', 2] } })),
			base
		).reason.code,
		'invalid_filter_value'
	);
});

test('BigInt plans compare only JavaScript-safe integer values', () => {
	const bigintField = Object.freeze({
		field: 'sequence',
		scalar: 'BigInt',
		codec: 'json_number_precision_limited',
		nullable: false,
		operators: Object.freeze(['_eq', '_gt', '_gte', '_lt', '_lte', '_in'])
	});
	const artifact = Object.freeze({
		input: literal({ sequence: { _gte: 9_007_199_254_740_990 } }),
		fields: Object.freeze([bigintField]),
		relationships: Object.freeze([]),
		rowPolicy: Object.freeze({ kind: 'unrestricted' })
	});
	assert.equal(
		evaluateReplicaFilter(artifact, {
			sequence: 9_007_199_254_740_991
		}).result,
		'match'
	);
	assert.equal(
		evaluateReplicaFilter(artifact, {
			sequence: 9_007_199_254_740_992
		}).reason.code,
		'invalid_filter_value'
	);

	const order = Object.freeze({
		input: literal([{ sequence: 'asc' }]),
		fields: Object.freeze([bigintField]),
		tieBreakers: Object.freeze([
			Object.freeze({
				field: 'id',
				scalar: 'ID',
				codec: 'string',
				nullable: false
			})
		])
	});
	assert.equal(
		compareReplicaOrder(
			order,
			{ id: 'same', sequence: 9_007_199_254_740_990 },
			{ id: 'same', sequence: 9_007_199_254_740_991 }
		).result,
		'less'
	);
});

test('unequal string equality and IN require a collation contract', () => {
	const base = {
		id: 'todo-1',
		priority: 3,
		active: true,
		title: 'one',
		payload: null
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ title: { _eq: 'one' } })),
			base
		).result,
		'match'
	);
	for (const where of [
		{ title: { _eq: 'ONE' } },
		{ title: { _neq: 'ONE' } },
		{ title: { _in: ['ONE'] } },
		{ title: { _nin: ['ONE'] } }
	]) {
		assert.equal(
			evaluateReplicaFilter(filterArtifact(literal(where)), base).reason.code,
			'collation_not_portable'
		);
	}
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ title: { _in: ['ONE', 'one'] } })),
			base
		).result,
		'match'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ title: { _nin: ['ONE', 'one'] } })),
			base
		).result,
		'no_match'
	);
});

test('query-plan artifacts require exact scalar-codec pairs', () => {
	const invalidFilter = Object.freeze({
		...filterArtifact(literal({ active: { _eq: true } })),
		fields: Object.freeze([
			Object.freeze({
				...FILTER_FIELDS[2],
				scalar: 'String',
				codec: 'boolean'
			})
		])
	});
	assert.equal(
		evaluateReplicaFilter(invalidFilter, { active: true }).reason.code,
		'invalid_artifact'
	);
	const invalidOperator = Object.freeze({
		...filterArtifact(literal({ active: { _like: 't%' } })),
		fields: Object.freeze([
			Object.freeze({
				...FILTER_FIELDS[2],
				operators: Object.freeze(['_like'])
			})
		])
	});
	assert.equal(
		evaluateReplicaFilter(invalidOperator, { active: true }).reason.code,
		'invalid_artifact'
	);

	const invalidOrder = Object.freeze({
		input: literal([{ priority: 'asc' }]),
		fields: Object.freeze([
			Object.freeze({
				field: 'priority',
				scalar: 'BigInt',
				codec: 'int32',
				nullable: false
			})
		]),
		tieBreakers: Object.freeze([
			Object.freeze({
				field: 'id',
				scalar: 'ID',
				codec: 'string'
			})
		])
	});
	assert.equal(
		compareReplicaOrder(
			invalidOrder,
			{ id: 'one', priority: 1 },
			{ id: 'two', priority: 2 }
		).reason.code,
		'invalid_artifact'
	);
});

test('unsafe operators, codecs, and absent canonical fields explain why evaluation is unknown', () => {
	const record = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	assert.equal(
		evaluateReplicaFilter(filterArtifact(literal({ title: { _icontains: 'ONE' } })), record).reason.code,
		'unsupported_operator'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ title: { _like: 'o%' } })),
			record
		).reason.code,
		'unsupported_operator'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ payload: { _eq: { a: 1 } } })),
			{ ...record, payload: { a: 1 } }
		).reason.code,
		'unsupported_codec'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ priority: { _eq: 3 } })),
			{ id: 'todo-1', active: true, title: 'one', payload: null }
		).reason.code,
		'missing_field'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(literal({ title: { _gt: 'a' } })),
			record
		).reason.code,
		'unsupported_operator'
	);
});

test('recursive argument sources resolve nested variables without runtime GraphQL parsing', () => {
	const artifact = filterArtifact({
		kind: 'object',
		fields: {
			_and: {
				kind: 'list',
				items: [
					{
						kind: 'object',
						fields: {
							priority: {
								kind: 'object',
								fields: {
									_gte: { kind: 'variable', name: 'minimum' }
								}
							}
						}
					}
				]
			}
		}
	});
	const record = { id: 'todo-1', priority: 4, active: true, title: 'one', payload: null };
	assert.equal(result(evaluateReplicaFilter(artifact, record, { minimum: 3 })), 'match');
	assert.equal(result(evaluateReplicaFilter(artifact, record, { minimum: 5 })), 'no_match');
	// Missing nested variables omit their input-object field, exactly like GraphQL.
	assert.equal(result(evaluateReplicaFilter(artifact, record, {})), 'match');
});

test('recursive argument sources preserve the same canonical cache identity inputs', () => {
	const source = {
		where: {
			kind: 'object',
			fields: {
				priority: {
					kind: 'object',
					fields: {
						_gte: { kind: 'variable', name: 'minimum' },
						_lte: { kind: 'literal', value: 9 }
					}
				},
				ids: {
					kind: 'list',
					items: [
						{ kind: 'variable', name: 'firstId' },
						{ kind: 'variable', name: 'optionalId' }
					]
				}
			}
		}
	};
	assert.deepEqual(resolveArguments(source, { minimum: 3, firstId: 'one' }), {
		where: {
			ids: ['one', null],
			priority: { _gte: 3, _lte: 9 }
		}
	});
	assert.deepEqual(resolveArguments(source, { firstId: 'one' }), {
		where: {
			ids: ['one', null],
			priority: { _lte: 9 }
		}
	});
});

test('row policies are conjoined and never read ambient claims', () => {
	const record = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	const policy = {
		kind: 'predicate',
		expression: {
			kind: 'cmp',
			value: {
				column: 'active',
				op: 'eq',
				rhs: { kind: 'lit', value: { kind: 'bool', value: true } }
			}
		}
	};
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _gt: 2 } }), policy),
				record
			)
		),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(literal({ priority: { _gt: 4 } }), policy),
				record
			)
		),
		'no_match'
	);

	const claimPolicy = {
		kind: 'predicate',
		expression: {
			kind: 'cmp',
			value: {
				column: 'priority',
				op: 'eq',
				rhs: {
					kind: 'claim',
					value: { header: 'x-distributed-priority' }
				}
			}
		}
	};
	assert.equal(
		evaluateReplicaFilter(filterArtifact(undefined, claimPolicy), record).reason.code,
		'claim_operand'
	);
	const descriptor = Object.freeze({
		name: 'x-distributed-priority',
		codec: 'int32'
	});
	const scoped = (value) =>
		Object.freeze({
			trustedPresets: Object.freeze({
				descriptors: Object.freeze([descriptor]),
				values: Object.freeze([
					Object.freeze({
						...descriptor,
						value
					})
				])
			})
		});
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(undefined, claimPolicy),
				record,
				{},
				scoped(3)
			)
		),
		'match'
	);
	assert.equal(
		result(
			evaluateReplicaFilter(
				filterArtifact(undefined, claimPolicy),
				record,
				{},
				scoped(4)
			)
		),
		'no_match'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, claimPolicy),
			record,
			{},
			{
				trustedPresets: {
					descriptors: [descriptor],
					values: []
				}
			}
		).reason.code,
		'claim_inventory'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, claimPolicy),
			record,
			{},
			{
				trustedPresets: {
					descriptors: [descriptor],
					values: [
						{ ...descriptor, value: 3 },
						{ name: 'x-forged-extra', codec: 'string', value: 'forged' }
					]
				}
			}
		).reason.code,
		'claim_inventory'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, claimPolicy),
			record,
			{},
			{
				trustedPresets: {
					descriptors: [descriptor],
					values: [
						{
							name: descriptor.name,
							codec: 'string',
							value: '3'
						}
					]
				}
			}
		).reason.code,
		'claim_inventory'
	);
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, { kind: 'server_only' }),
			record
		).reason.code,
		'server_only_policy'
	);
});

test('row-policy literal tags are checked before comparison', () => {
	const record = {
		id: 'todo-1',
		priority: 3,
		active: true,
		title: 'one',
		payload: { tenant: true }
	};
	const jsonAgainstString = {
		kind: 'predicate',
		expression: {
			kind: 'cmp',
			value: {
				column: 'title',
				op: 'eq',
				rhs: { kind: 'lit', value: { kind: 'json', value: 'one' } }
			}
		}
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, jsonAgainstString),
			record
		).reason.code,
		'invalid_artifact'
	);

	const jsonHasKey = {
		kind: 'predicate',
		expression: {
			kind: 'cmp',
			value: {
				column: 'payload',
				op: 'has_key',
				rhs: { kind: 'lit', value: { kind: 'string', value: 'tenant' } }
			}
		}
	};
	assert.equal(
		evaluateReplicaFilter(filterArtifact(undefined, jsonHasKey), record).reason.code,
		'unsupported_operator'
	);

	const floatFromInteger = Object.freeze({
		fields: Object.freeze([
			Object.freeze({
				field: 'score',
				scalar: 'Float',
				codec: 'float64',
				nullable: false,
				operators: Object.freeze(['_gt'])
			})
		]),
		relationships: Object.freeze([]),
		rowPolicy: Object.freeze({
			kind: 'predicate',
			expression: Object.freeze({
				kind: 'cmp',
				value: Object.freeze({
					column: 'score',
					op: 'gt',
					rhs: Object.freeze({
						kind: 'lit',
						value: Object.freeze({ kind: 'i64', value: 1 })
					})
				})
			})
		})
	});
	assert.equal(
		evaluateReplicaFilter(floatFromInteger, { score: 1.5 }).result,
		'match'
	);
});

test('malformed row-policy booleans fail closed', () => {
	const record = {
		id: 'todo-1',
		priority: 3,
		active: true,
		title: 'one',
		payload: null
	};
	const invalidIsNull = {
		kind: 'predicate',
		expression: {
			kind: 'is_null',
			value: { column: 'priority', is_null: 'yes' }
		}
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, invalidIsNull),
			record
		).reason.code,
		'invalid_artifact'
	);

	const invalidNegated = {
		kind: 'predicate',
		expression: {
			kind: 'in',
			value: {
				column: 'priority',
				values: [{ kind: 'lit', value: { kind: 'i64', value: 3 } }],
				negated: 'false'
			}
		}
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(undefined, invalidNegated),
			record
		).reason.code,
		'invalid_artifact'
	);
});

test('relationship predicates require an explicit resolver carrying target-policy semantics', () => {
	const artifact = filterArtifact(literal({ owner: { id: { _eq: 'user-1' } } }));
	const record = { id: 'todo-1', priority: 3, active: true, title: 'one', payload: null };
	assert.equal(
		evaluateReplicaFilter(artifact, record).reason.code,
		'relationship_resolver_required'
	);
	let request;
	const evaluated = evaluateReplicaFilter(artifact, record, {}, {
		resolveRelationship(value) {
			request = value;
			return { result: 'match' };
		}
	});
	assert.equal(evaluated.result, 'match');
	assert.equal(request.source, 'caller');
	assert.deepEqual(request.relationship, OWNER_RELATIONSHIP);
	assert.deepEqual(request.predicate, { id: { _eq: 'user-1' } });
});

test('relationship resolvers receive exact direct, belongs-to, has-many, m2m, and opaque plans', () => {
	const relationships = Object.freeze([
		OWNER_RELATIONSHIP,
		Object.freeze({
			field: 'children',
			targetModel: 'Todo',
			kind: 'has_many',
			keyMapping: Object.freeze({
				kind: 'direct',
				local: Object.freeze(['id']),
				remote: Object.freeze(['parentId'])
			}),
			maintenance: 'local',
			dependencies: Object.freeze(['todo_rows'])
		}),
		Object.freeze({
			field: 'members',
			targetModel: 'User',
			kind: 'many_to_many',
			keyMapping: Object.freeze({
				kind: 'through',
				local: Object.freeze(['id']),
				remote: Object.freeze(['id']),
				table: 'todo_members',
				sourceForeignKey: Object.freeze(['todo_id']),
				targetForeignKey: Object.freeze(['user_id'])
			}),
			maintenance: 'local',
			dependencies: Object.freeze(['todo_members', 'todo_rows', 'user_rows'])
		}),
		Object.freeze({
			field: 'opaqueMembers',
			targetModel: 'User',
			kind: 'many_to_many',
			keyMapping: Object.freeze({
				kind: 'through_opaque',
				local: Object.freeze(['id']),
				remote: Object.freeze(['id']),
				dependency: 'private_membership'
			}),
			maintenance: 'revalidate',
			dependencies: Object.freeze([
				'private_membership',
				'todo_rows',
				'user_rows'
			])
		}),
		Object.freeze({
			field: 'embeddedOwner',
			targetModel: 'User',
			kind: 'belongs_to',
			keyMapping: Object.freeze({ kind: 'embedded' }),
			maintenance: 'revalidate',
			dependencies: Object.freeze(['todo_rows', 'user_rows'])
		})
	]);
	const record = {
		id: 'todo-1',
		priority: 3,
		active: true,
		title: 'one',
		payload: null
	};
	for (const expected of relationships) {
		let request;
		const evaluated = evaluateReplicaFilter(
			filterArtifact(
				literal({ [expected.field]: { id: { _eq: 'target-1' } } }),
				Object.freeze({ kind: 'unrestricted' }),
				relationships
			),
			record,
			{},
			{
				resolveRelationship(value) {
					request = value;
					return { result: 'match' };
				}
			}
		);
		assert.equal(evaluated.result, 'match');
		assert.deepEqual(request.relationship, expected);
		assert.equal(Object.isFrozen(request.relationship), true);
		assert.equal(Object.isFrozen(request.relationship.dependencies), true);
	}
});

test('relationship descriptors fail closed when maintenance or key facts are forged', () => {
	const record = {
		id: 'todo-1',
		priority: 3,
		active: true,
		title: 'one',
		payload: null
	};
	const forgedOpaque = {
		...OWNER_RELATIONSHIP,
		keyMapping: {
			kind: 'through_opaque',
			local: ['ownerId'],
			remote: ['id'],
			dependency: 'private_membership'
		},
		maintenance: 'local',
		dependencies: ['private_membership', 'todo_rows', 'user_rows']
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(
				literal({ owner: { id: { _eq: 'user-1' } } }),
				Object.freeze({ kind: 'unrestricted' }),
				[forgedOpaque]
			),
			record
		).reason.code,
		'invalid_artifact'
	);

	const conservativeDirect = Object.freeze({
		...OWNER_RELATIONSHIP,
		maintenance: 'revalidate'
	});
	let request;
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(
				literal({ owner: { id: { _eq: 'user-1' } } }),
				Object.freeze({ kind: 'unrestricted' }),
				[conservativeDirect]
			),
			record,
			{},
			{
				resolveRelationship(value) {
					request = value;
					return { result: 'match' };
				}
			}
		).result,
		'match'
	);
	assert.equal(request.relationship.maintenance, 'revalidate');

	const mismatchedKeys = {
		...OWNER_RELATIONSHIP,
		keyMapping: {
			kind: 'direct',
			local: ['ownerId', 'tenantId'],
			remote: ['id']
		}
	};
	assert.equal(
		evaluateReplicaFilter(
			filterArtifact(
				literal({ owner: { id: { _eq: 'user-1' } } }),
				Object.freeze({ kind: 'unrestricted' }),
				[mismatchedKeys]
			),
			record
		).reason.code,
		'invalid_artifact'
	);
});

const ORDER_FIELDS = Object.freeze([
	Object.freeze({
		field: 'priority',
		scalar: 'Int',
		codec: 'int32',
		nullable: false
	}),
	Object.freeze({
		field: 'score',
		scalar: 'Float',
		codec: 'float64',
		nullable: true
	}),
	Object.freeze({
		field: 'title',
		scalar: 'String',
		codec: 'string',
		nullable: false
	}),
	Object.freeze({
		field: 'payload',
		scalar: 'JSON',
		codec: 'json',
		nullable: false
	})
]);

function orderArtifact(
	input,
	tieBreakers = [
		{
			field: 'sequence',
			scalar: 'BigInt',
			codec: 'json_number_precision_limited',
			nullable: false
		}
	]
) {
	return Object.freeze({
		...(input === undefined ? {} : { input }),
		fields: ORDER_FIELDS,
		tieBreakers: Object.freeze(tieBreakers)
	});
}

test('order comparison follows declared priority then exact identity tie-breakers', () => {
	const artifact = orderArtifact({ kind: 'variable', name: 'order' });
	const left = { priority: 3, score: 1, title: 'same', payload: {}, sequence: 2 };
	const right = { priority: 2, score: 1, title: 'same', payload: {}, sequence: 1 };
	assert.equal(
		compareReplicaOrder(artifact, left, right, {
			order: [{ priority: 'desc' }]
		}).result,
		'less'
	);
	assert.equal(
		compareReplicaOrder(artifact, { ...left, priority: 2 }, right, {
			order: [{ priority: 'desc' }]
		}).result,
		'greater'
	);
	assert.equal(
		compareReplicaOrder(
			orderArtifact(undefined),
			{ ...left, sequence: 1 },
			{ ...right, sequence: 2 }
		).result,
		'less'
	);
});

test('order comparison validates entry shape, directions, and null placement', () => {
	const left = { priority: 1, score: null, title: 'a', payload: {}, sequence: 1 };
	const right = { priority: 1, score: 2, title: 'b', payload: {}, sequence: 2 };
	assert.equal(
		compareReplicaOrder(
			orderArtifact(literal([{ score: 'asc_nulls_last' }])),
			left,
			right
		).result,
		'greater'
	);
	assert.equal(
		compareReplicaOrder(orderArtifact(literal([{ score: 'asc' }])), left, right)
			.reason.code,
		'implicit_null_order'
	);
	assert.equal(
		compareReplicaOrder(
			orderArtifact(literal([{ priority: 'asc', title: 'asc' }])),
			left,
			right
		).reason.code,
		'ambiguous_order_entry'
	);
	assert.equal(
		compareReplicaOrder(
			orderArtifact(literal([{ priority: 'sideways' }])),
			left,
			right
		).reason.code,
		'invalid_order_direction'
	);
	assert.equal(
		compareReplicaOrder(orderArtifact(literal([{ title: 'asc' }])), left, right)
			.result,
		'less'
	);
	assert.equal(
		compareReplicaOrder(orderArtifact(literal([{ payload: 'asc' }])), left, right)
			.reason.code,
		'unsupported_codec'
	);
});

test('string order uses unsigned UTF-8 bytes including supplementary code points', () => {
	const order = orderArtifact(literal([{ title: 'asc' }]));
	assert.equal(
		compareReplicaOrder(
			order,
			{ title: '\u{10000}', sequence: 1 },
			{ title: '\u{e000}', sequence: 2 }
		).result,
		'greater'
	);
	assert.equal(
		compareReplicaOrder(
			order,
			{ title: 'same', sequence: 1 },
			{ title: 'same', sequence: 2 }
		).result,
		'less'
	);
});

test('tied comparisons without identity tie-breakers remain unknown', () => {
	const noTieBreaker = orderArtifact(literal([{ priority: 'asc' }]), []);
	const left = { priority: 1, score: 1, title: 'a', payload: {}, sequence: 1 };
	const right = { priority: 1, score: 1, title: 'b', payload: {}, sequence: 2 };
	assert.equal(
		compareReplicaOrder(noTieBreaker, left, right).reason.code,
		'invalid_artifact'
	);
	assert.equal(
		compareReplicaOrder(noTieBreaker, left, { ...right, priority: 2 }).result,
		'less'
	);
});

test('order input supports GraphQL singleton-list coercion and recursive sources', () => {
	const left = { priority: 1, score: 1, title: 'same', payload: {}, sequence: 1 };
	const right = { priority: 2, score: 1, title: 'same', payload: {}, sequence: 2 };
	assert.equal(
		compareReplicaOrder(
			orderArtifact({ kind: 'variable', name: 'order' }),
			left,
			right,
			{ order: { priority: 'asc' } }
		).result,
		'less'
	);
	assert.equal(
		compareReplicaOrder(
			orderArtifact({
				kind: 'list',
				items: [
					{
						kind: 'object',
						fields: {
							priority: { kind: 'variable', name: 'direction' }
						}
					}
				]
			}),
			left,
			right,
			{ direction: 'desc' }
		).result,
		'greater'
	);
});

const COMPLETE_PAGINATION = Object.freeze({
	kind: 'complete',
	insert: 'local',
	delete: 'local',
	reorder: 'local',
	stableUpdate: 'local'
});
const OFFSET_PAGINATION = Object.freeze({
	kind: 'offset',
	insert: 'revalidate',
	delete: 'revalidate',
	reorder: 'revalidate',
	stableUpdate: 'local'
});
const LOCAL_OFFSET_PAGINATION = Object.freeze({
	kind: 'offset',
	insert: 'local',
	delete: 'local',
	reorder: 'local',
	stableUpdate: 'local'
});

test('pagination plans make complete and offset maintenance decisions explicit', () => {
	const complete = { kind: 'complete' };
	for (const kind of ['insert', 'delete', 'reorder', 'stable_update']) {
		assert.equal(
			decideReplicaPaginationMaintenance(COMPLETE_PAGINATION, complete, { kind })
				.decision,
			'local'
		);
	}

	const offset = { kind: 'offset', offset: 0, limit: 10, returned: 10 };
	assert.equal(
		decideReplicaPaginationMaintenance(OFFSET_PAGINATION, offset, {
			kind: 'stable_update'
		}).decision,
		'local'
	);
	// OFFSET_PAGINATION marks insert/delete/reorder as revalidate (policy gate).
	assert.equal(
		decideReplicaPaginationMaintenance(OFFSET_PAGINATION, offset, {
			kind: 'insert'
		}).reason.code,
		'insert_changes_offset_window'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(OFFSET_PAGINATION, offset, {
			kind: 'delete'
		}).reason.code,
		'delete_changes_offset_window'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(OFFSET_PAGINATION, offset, {
			kind: 'reorder'
		}).reason.code,
		'reorder_changes_offset_window'
	);
});

test('pagination refuses unknown, mismatched, unsafe offset, and unproven cursor plans', () => {
	assert.equal(
		decideReplicaPaginationMaintenance(
			OFFSET_PAGINATION,
			{ kind: 'unknown' },
			{ kind: 'stable_update' }
		).reason.code,
		'unknown_coverage'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(
			OFFSET_PAGINATION,
			{ kind: 'complete' },
			{ kind: 'stable_update' }
		).reason.code,
		'coverage_mismatch'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(
			LOCAL_OFFSET_PAGINATION,
			{ kind: 'offset', offset: 0 },
			{ kind: 'insert' }
		).reason.code,
		'insert_changes_offset_window'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(
			{
				kind: 'cursor',
				insert: 'revalidate',
				delete: 'revalidate',
				reorder: 'revalidate',
				stableUpdate: 'local'
			},
			{ kind: 'cursor', after: 'cursor-1' },
			{ kind: 'stable_update' }
		).reason.code,
		'cursor_not_certified'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(
			{
				kind: 'cursor',
				// A forged boolean is not a versioned proof IR.
				certified: true,
				insert: 'local',
				delete: 'local',
				reorder: 'local',
				stableUpdate: 'local'
			},
			{ kind: 'cursor', after: 'cursor-1' },
			{ kind: 'insert' }
		).reason.code,
		'cursor_not_certified'
	);
	assert.equal(
		decideReplicaPaginationMaintenance(
			{
				kind: 'forged',
				insert: 'local',
				delete: 'local',
				reorder: 'local',
				stableUpdate: 'local'
			},
			{ kind: 'complete' },
			{ kind: 'insert' }
		).reason.code,
		'invalid_pagination_policy'
	);
});

test('offset locality: first-page insert always; delete/reorder need non-full page', () => {
	const safe = { kind: 'offset', offset: 0, limit: 10, returned: 9 };
	for (const kind of ['insert', 'delete', 'reorder', 'stable_update']) {
		assert.equal(
			decideReplicaPaginationMaintenance(
				LOCAL_OFFSET_PAGINATION,
				safe,
				{ kind }
			).decision,
			'local'
		);
	}

	// Full first page: insert stays local; delete/reorder still fail closed.
	assert.equal(
		decideReplicaPaginationMaintenance(
			LOCAL_OFFSET_PAGINATION,
			{ kind: 'offset', offset: 0, limit: 10, returned: 10 },
			{ kind: 'insert' }
		).decision,
		'local'
	);

	for (const [coverage, kind, code] of [
		[
			{ kind: 'offset', offset: 0, limit: 10, returned: 10 },
			'delete',
			'delete_changes_offset_window'
		],
		[
			{ kind: 'offset', offset: 1, limit: 10, returned: 1 },
			'delete',
			'delete_changes_offset_window'
		],
		[
			{ kind: 'offset', offset: 0, limit: 10 },
			'reorder',
			'reorder_changes_offset_window'
		],
		[
			{ kind: 'offset', offset: 1, limit: 10, returned: 1 },
			'insert',
			'insert_changes_offset_window'
		]
	]) {
		assert.equal(
			decideReplicaPaginationMaintenance(
				LOCAL_OFFSET_PAGINATION,
				coverage,
				{ kind }
			).reason.code,
			code
		);
	}
});
