import assert from 'node:assert/strict';
import test from 'node:test';

import {
	canonicalizeOperationVariables,
	createDistributedReplica
} from '../dist/replica/index.js';

const Item = Object.freeze({ id: 'item-view', identityFields: Object.freeze(['id']) });

const codecLimits = Object.freeze({
	maxDepth: 64,
	maxBoolWidth: 256,
	maxInList: 1000
});

const variableCodec = Object.freeze({
	version: 2,
	limits: codecLimits,
	variables: Object.freeze({
		big: Object.freeze({
			kind: 'scalar',
			scalar: 'BigInt',
			codec: 'json_number_precision_limited',
			nullable: true
		}),
		blob: Object.freeze({
			kind: 'scalar',
			scalar: 'Bytea',
			codec: 'base64',
			nullable: true
		}),
		direction: Object.freeze({
			kind: 'enum',
			name: 'order_by',
			values: Object.freeze(['asc', 'desc']),
			nullable: true
		}),
		id: Object.freeze({
			kind: 'scalar',
			scalar: 'ID',
			codec: 'string',
			nullable: false
		}),
		offset: Object.freeze({
			kind: 'scalar',
			scalar: 'Int',
			codec: 'int32',
			nullable: true
		}),
		order: Object.freeze({
			kind: 'list',
			nullable: true,
			item: Object.freeze({ kind: 'input', name: 'item_order_by', nullable: false })
		}),
		payload: Object.freeze({
			kind: 'scalar',
			scalar: 'JSON',
			codec: 'json',
			nullable: true
		}),
		ratio: Object.freeze({
			kind: 'scalar',
			scalar: 'Float',
			codec: 'float64',
			nullable: true
		}),
		where: Object.freeze({
			kind: 'input',
			name: 'item_bool_exp',
			nullable: true,
			filterBaseDepth: 0
		})
	}),
	defaults: Object.freeze({}),
	inputs: Object.freeze({
		item_bool_exp: Object.freeze({
			kind: 'filter',
			model: 'item-view',
			fields: Object.freeze([
				Object.freeze({
					field: 'id',
					scalar: 'ID',
					codec: 'string',
					nullable: false,
					operators: Object.freeze(['_eq', '_in', '_nin'])
				}),
				Object.freeze({
					field: 'payload',
					scalar: 'JSON',
					codec: 'json',
					nullable: true,
					operators: Object.freeze(['_eq', '_contains', '_has_key'])
				}),
				Object.freeze({
					field: 'priority',
					scalar: 'Int',
					codec: 'int32',
					nullable: false,
					operators: Object.freeze(['_eq', '_in', '_is_null'])
				}),
				Object.freeze({
					field: 'title',
					scalar: 'String',
					codec: 'string',
					nullable: false,
					operators: Object.freeze(['_eq', '_like', '_ilike', '_icontains'])
				})
			]),
			relationships: Object.freeze([
				Object.freeze({
					field: 'owner',
					target: Object.freeze({ kind: 'input', name: 'user_bool_exp' })
				}),
				Object.freeze({
					field: 'unplanned',
					target: Object.freeze({ kind: 'opaque' })
				})
			])
		}),
		item_order_by: Object.freeze({
			kind: 'order',
			model: 'item-view',
			fields: Object.freeze([
				Object.freeze({
					field: 'id',
					scalar: 'ID',
					codec: 'string',
					nullable: false
				}),
				Object.freeze({
					field: 'priority',
					scalar: 'Int',
					codec: 'int32',
					nullable: false
				})
			]),
			values: Object.freeze(['asc', 'desc'])
		}),
		user_bool_exp: Object.freeze({
			kind: 'filter',
			model: 'user-view',
			fields: Object.freeze([
				Object.freeze({
					field: 'id',
					scalar: 'ID',
					codec: 'string',
					nullable: false,
					operators: Object.freeze(['_eq'])
				})
			]),
			// A named registry cycle is valid and must not recurse during validation.
			relationships: Object.freeze([
				Object.freeze({
					field: 'items',
					target: Object.freeze({ kind: 'input', name: 'item_bool_exp' })
				})
			])
		})
	})
});

const CodecArtifact = Object.freeze({
	id: 'query:codec-items',
	document: 'query CodecItems { items { id title } }',
	variableCodec,
	protocol: Object.freeze({
		version: 1,
		schemaHash: 'schema-one',
		surface: Object.freeze({ kind: 'role', name: 'user' }),
		operation: 'query:codec-items',
		trustedPresets: Object.freeze([])
	}),
	roots: Object.freeze([
		Object.freeze({
			responseKey: 'items',
			field: 'items',
			cardinality: 'many',
			nullable: false,
			arguments: Object.freeze({
				id: Object.freeze({ kind: 'variable', name: 'id' }),
				offset: Object.freeze({ kind: 'variable', name: 'offset' }),
				order_by: Object.freeze({ kind: 'variable', name: 'order' }),
				where: Object.freeze({ kind: 'variable', name: 'where' })
			}),
			coverage: Object.freeze({
				kind: 'offset',
				offsetArgument: 'offset',
				defaultLimit: 25,
				maxLimit: 100
			}),
			dependencies: Object.freeze(['items']),
			selection: Object.freeze({
				typename: Item.id,
				storage: Object.freeze({
					kind: 'normalized',
					model: Item.id,
					identityFields: Item.identityFields
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

const singletonVariables = Object.freeze({
	id: 7,
	offset: -1,
	order: Object.freeze({ priority: 'asc' }),
	where: Object.freeze({
		_and: Object.freeze({
			priority: Object.freeze({ _eq: -0 }),
			id: Object.freeze({ _in: 7 })
		})
	})
});

const expandedVariables = Object.freeze({
	where: Object.freeze({
		_and: Object.freeze([
			Object.freeze({
				id: Object.freeze({ _in: Object.freeze(['7']) }),
				priority: Object.freeze({ _eq: 0 })
			})
		])
	}),
	order: Object.freeze([Object.freeze({ priority: 'asc' })]),
	offset: -1,
	id: '7'
});

function codecWireFrame(title = 'schema one') {
	const projection = 'items-projector';
	const position = '1';
	const resume = {
		projection,
		position,
		token: 'resume:1'
	};
	return {
		data: { items: [{ id: 'item-1', title }] },
		extensions: {
			distributed: {
				protocolVersion: 1,
				schemaHash: 'schema-one',
				authorizationGeneration: 'auth-1',
				cacheScope: 'cache:one',
				operation: CodecArtifact.id,
				snapshot: {
					scopeToken: 'snapshot:codec-items',
					recordsComplete: true,
					indexesComparable: true,
					records: [
						{
							path: ['items', '0'],
							model: Item.id,
							scopeToken: 'record:item-1',
							incarnation: '1',
							revision: position,
							tombstone: false
						}
					],
					indexes: [
						{
							projection,
							scopeToken: 'index:codec-items',
							position,
							resume
						}
					],
					observations: []
				}
			}
		}
	};
}

function assertDeepFrozen(value) {
	if (value === null || typeof value !== 'object') return;
	assert.equal(Object.isFrozen(value), true);
	for (const entry of Object.values(value)) assertDeepFrozen(entry);
}

async function flushMicrotasks() {
	await Promise.resolve();
	await Promise.resolve();
	await new Promise((resolve) => setImmediate(resolve));
}

test('compiler variable codec canonicalizes ID, lists, filters, order, and key order', () => {
	const singleton = canonicalizeOperationVariables(CodecArtifact, singletonVariables);
	const expanded = canonicalizeOperationVariables(CodecArtifact, expandedVariables);

	assert.deepEqual(singleton, expanded);
	assert.deepEqual(singleton, {
		id: '7',
		offset: -1,
		order: [{ priority: 'asc' }],
		where: {
			_and: [{ id: { _in: ['7'] }, priority: { _eq: 0 } }]
		}
	});
	assertDeepFrozen(singleton);
	assert.equal(JSON.stringify(singleton), JSON.stringify(expanded));
});

test('literal text filters preserve operands and reject non-string values', () => {
	for (const q of ['', '%_!\\\'OR 1=1', 'Älice']) {
		const variables = { id: '1', where: { title: { _icontains: q } } };
		assert.deepEqual(canonicalizeOperationVariables(CodecArtifact, variables), variables);
	}
	for (const q of [12, true, {}, []]) {
		assert.throws(() => canonicalizeOperationVariables(CodecArtifact, {
			id: '1', where: { title: { _icontains: q } }
		}), /expected string/);
	}
	assert.throws(() => canonicalizeOperationVariables(CodecArtifact, {
		id: '1', where: { priority: { _icontains: '1' } }
	}));
});

test('scalar canonicalization is deterministic and preserves omission versus null', () => {
	const canonical = canonicalizeOperationVariables(CodecArtifact, {
		id: -0,
		big: -0,
		blob: 'YQ==',
		direction: 'asc',
		payload: { z: -0, a: [true, 1] },
		ratio: -0
	});
	assert.deepEqual(canonical, {
		big: 0,
		blob: 'YQ==',
		direction: 'asc',
		id: '0',
		payload: { a: [true, 1], z: 0 },
		ratio: 0
	});
	assertDeepFrozen(canonical);

	const omitted = canonicalizeOperationVariables(CodecArtifact, {
		id: '1',
		where: undefined
	});
	const explicitNull = canonicalizeOperationVariables(CodecArtifact, {
		id: '1',
		where: null
	});
	assert.deepEqual(omitted, { id: '1' });
	assert.deepEqual(explicitNull, { id: '1', where: null });
	assert.notEqual(JSON.stringify(omitted), JSON.stringify(explicitNull));
});

test('GraphQL defaults are canonical identity inputs and explicit values win', () => {
	const defaultedArtifact = Object.freeze({
		...CodecArtifact,
		variableCodec: Object.freeze({
			...variableCodec,
			defaults: Object.freeze({ id: 7, offset: 25 })
		})
	});
	assert.deepEqual(canonicalizeOperationVariables(defaultedArtifact, {}), {
		id: '7',
		offset: 25
	});
	assert.deepEqual(
		canonicalizeOperationVariables(defaultedArtifact, {
			id: 9,
			offset: 0
		}),
		{ id: '9', offset: 0 }
	);
	assert.deepEqual(
		canonicalizeOperationVariables(defaultedArtifact, {
			id: undefined,
			offset: undefined
		}),
		{ id: '7', offset: 25 }
	);
	assert.throws(
		() =>
			canonicalizeOperationVariables(
				{
					...defaultedArtifact,
					variableCodec: { ...defaultedArtifact.variableCodec, defaults: { id: null } }
				},
				{}
			),
		/invalid GraphQL operation input at artifact\.variableCodec\.defaults\.id/
	);
});

test('canonical write and read variables share normalized index identity', () => {
	const replica = createDistributedReplica();
	replica.writeResult(
		CodecArtifact,
		singletonVariables,
		codecWireFrame('canonical'),
		'network'
	);
	const snapshot = replica.read(CodecArtifact, expandedVariables);
	assert.equal(snapshot.status, 'ready');
	assert.deepEqual(snapshot.data, {
		items: [{ id: 'item-1', title: 'canonical' }]
	});
});

test('invalid values, unknown inputs, sparse arrays, and accessors fail closed', () => {
	for (const variables of [
		{ id: Number.MAX_SAFE_INTEGER + 1 },
		{ id: '1', blob: 'YQ' },
		{ id: '1', blob: 'AB==' },
		{ id: '1', offset: 2_147_483_648 },
		{ id: '1', ratio: Number.POSITIVE_INFINITY },
		{ id: '1', big: Number.MAX_SAFE_INTEGER + 1 },
		{ id: '1', payload: { unsafe: Number.MAX_SAFE_INTEGER + 1 } },
		{ id: '1', direction: 'sideways' },
		{ id: '1', extra: undefined },
		{ id: '1', where: { missing: { _eq: 1 } } },
		{ id: '1', where: { priority: { _gt: 1 } } },
		{ id: '1', where: { id: { _in: null } } },
		{ id: '1', where: { id: { _in: [null] } } },
		{ id: '1', order: { id: 'asc', priority: 'desc' } },
		{ id: '1', where: { unplanned: 'not-an-object' } },
		{ id: '1', payload: new Date() }
	]) {
		assert.throws(
			() => canonicalizeOperationVariables(CodecArtifact, variables),
			TypeError
		);
	}

	const sparse = new Array(1);
	assert.throws(
		() =>
			canonicalizeOperationVariables(CodecArtifact, {
				id: '1',
				where: { id: { _in: sparse } }
			}),
		/dense data-only input array/
	);

	let getterCalls = 0;
	const accessor = [];
	Object.defineProperty(accessor, '0', {
		enumerable: true,
		get() {
			getterCalls += 1;
			return '1';
		}
	});
	accessor.length = 1;
	assert.throws(
		() =>
			canonicalizeOperationVariables(CodecArtifact, {
				id: '1',
				where: { id: { _in: accessor } }
			}),
		/dense data-only input array/
	);
	assert.equal(getterCalls, 0);

	const cyclicValue = {};
	cyclicValue._not = cyclicValue;
	assert.throws(
		() =>
			canonicalizeOperationVariables(CodecArtifact, {
				id: '1',
				where: cyclicValue
			}),
		/must not contain cycles/
	);
});

test('deep acyclic filters fail with a typed depth error', () => {
	const safetyArtifact = {
		...CodecArtifact,
		variableCodec: {
			...variableCodec,
			limits: { ...codecLimits, maxDepth: 100 }
		}
	};
	let where = { id: { _eq: '1' } };
	for (let depth = 0; depth < 70; depth += 1) {
		where = { _not: where };
	}
	assert.throws(
		() =>
			canonicalizeOperationVariables(safetyArtifact, {
				id: '1',
				where
			}),
		(error) =>
			error instanceof TypeError &&
			error.message.startsWith('invalid GraphQL operation input at ') &&
			error.message.endsWith('input nesting exceeds the supported depth')
	);
});

test('filter semantic depth enforces exact and max-plus-one boundaries', () => {
	const depthArtifact = {
		...CodecArtifact,
		variableCodec: {
			...variableCodec,
			limits: { ...codecLimits, maxDepth: 2 }
		}
	};
	assert.deepEqual(
		canonicalizeOperationVariables(depthArtifact, {
			id: '1',
			where: { _not: { _not: { id: { _eq: '1' }, _and: [] } } }
		}).where,
		{ _not: { _not: { _and: [], id: { _eq: '1' } } } }
	);
	for (const where of [
		{ _not: { _not: { _not: { id: { _eq: '1' } } } } },
		{ _not: { _not: { _not: null } } }
	]) {
		assert.throws(
			() => canonicalizeOperationVariables(depthArtifact, { id: '1', where }),
			/exceeding maxDepth 2/
		);
	}

	const relationshipAtMax = {
		...CodecArtifact,
		variableCodec: {
			...variableCodec,
			limits: { ...codecLimits, maxDepth: 1 }
		}
	};
	assert.deepEqual(
		canonicalizeOperationVariables(relationshipAtMax, {
			id: '1',
			where: { owner: null }
		}).where,
		{ owner: null }
	);
	const relationshipPastMax = {
		...relationshipAtMax,
		variableCodec: {
			...relationshipAtMax.variableCodec,
			limits: { ...codecLimits, maxDepth: 0 }
		}
	};
	assert.throws(
		() =>
			canonicalizeOperationVariables(relationshipPastMax, {
				id: '1',
				where: { owner: null }
			}),
		/exceeding maxDepth 0/
	);
	assert.deepEqual(
		canonicalizeOperationVariables(relationshipPastMax, {
			id: '1',
			where: { _and: null, _or: [] }
		}).where,
		{ _and: null, _or: [] }
	);
});

test('boolean and IN widths are checked after singleton coercion', () => {
	const widthArtifact = {
		...CodecArtifact,
		variableCodec: {
			...variableCodec,
			limits: { maxDepth: 64, maxBoolWidth: 1, maxInList: 1 }
		}
	};
	assert.deepEqual(
		canonicalizeOperationVariables(widthArtifact, {
			id: '1',
			where: {
				_and: { id: { _eq: 1 } },
				id: { _in: 1 }
			}
		}).where,
		{ _and: [{ id: { _eq: '1' } }], id: { _in: ['1'] } }
	);
	for (const where of [
		{ _and: [{ id: { _eq: 1 } }, { id: { _eq: 2 } }] },
		{ id: { _in: [1, 2] } }
	]) {
		assert.throws(
			() => canonicalizeOperationVariables(widthArtifact, { id: '1', where }),
			/exceeding max(BoolWidth|InList) 1/
		);
	}
});

test('per-variable maxItems applies after coercion while null skips it', () => {
	const listArtifact = {
		...CodecArtifact,
		variableCodec: {
			...variableCodec,
			variables: {
				...variableCodec.variables,
				ids: {
					kind: 'list',
					nullable: true,
					maxItems: 2,
					item: {
						kind: 'scalar',
						scalar: 'ID',
						codec: 'string',
						nullable: false
					}
				}
			}
		}
	};
	assert.deepEqual(
		canonicalizeOperationVariables(listArtifact, { id: '1', ids: 2 }).ids,
		['2']
	);
	assert.deepEqual(
		canonicalizeOperationVariables(listArtifact, { id: '1', ids: [2, 3] }).ids,
		['2', '3']
	);
	assert.equal(
		canonicalizeOperationVariables(listArtifact, { id: '1', ids: null }).ids,
		null
	);
	assert.throws(
		() =>
			canonicalizeOperationVariables(listArtifact, {
				id: '1',
				ids: [1, 2, 3]
			}),
		/exceeding maxItems 2/
	);
});

test('codec limits and per-variable constraints must be exact JS integers', () => {
	const invalidCodecs = [
		{ ...variableCodec, version: 3 },
		{
			...variableCodec,
			limits: { ...codecLimits, maxDepth: Number.MAX_SAFE_INTEGER + 1 }
		},
		{
			...variableCodec,
			limits: { ...codecLimits, maxBoolWidth: -1 }
		},
		{
			...variableCodec,
			limits: { ...codecLimits, maxInList: 1.5 }
		},
		{
			...variableCodec,
			variables: {
				...variableCodec.variables,
				where: {
					...variableCodec.variables.where,
					filterBaseDepth: Number.MAX_SAFE_INTEGER + 1
				}
			}
		},
		{
			...variableCodec,
			variables: {
				...variableCodec.variables,
				values: {
					kind: 'list',
					nullable: true,
					maxItems: Number.POSITIVE_INFINITY,
					item: {
						kind: 'scalar',
						scalar: 'ID',
						codec: 'string',
						nullable: false
					}
				}
			}
		}
	];
	for (const invalidCodec of invalidCodecs) {
		assert.throws(
			() =>
				canonicalizeOperationVariables(
					{ ...CodecArtifact, variableCodec: invalidCodec },
					{ id: '1' }
				),
			/invalid replica variable codec/
		);
	}
});

test('cyclic or incompatible codec artifacts fail without walking forever', () => {
	const cyclicRef = { kind: 'list', nullable: true };
	cyclicRef.item = cyclicRef;
	const cyclicArtifact = {
		...CodecArtifact,
		variableCodec: {
			version: 2,
			limits: codecLimits,
			variables: { value: cyclicRef },
			defaults: {},
			inputs: {}
		}
	};
	assert.throws(
		() => canonicalizeOperationVariables(cyclicArtifact, { value: [] }),
		/invalid replica variable codec/
	);

	const incompatibleArtifact = {
		...CodecArtifact,
		variableCodec: {
			version: 2,
			limits: codecLimits,
			variables: {
				where: {
					kind: 'input',
					name: 'bad_filter',
					nullable: true,
					filterBaseDepth: 0
				}
			},
			defaults: {},
			inputs: {
				bad_filter: {
					kind: 'filter',
					model: 'bad',
					fields: [
						{
							field: 'count',
							scalar: 'Int',
							codec: 'int32',
							nullable: false,
							operators: ['_contains']
						}
					],
					relationships: []
				}
			}
		}
	};
	assert.throws(
		() => canonicalizeOperationVariables(incompatibleArtifact, {}),
		/invalid replica variable codec/
	);
});

test('protocol artifacts require a codec before binding, cache access, or transport', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				fetches.push(request);
				return new Promise(() => undefined);
			}
		}
	});
	const missingCodec = Object.freeze({
		...CodecArtifact,
		variableCodec: undefined
	});
	const unboundWithCodec = Object.freeze({
		...CodecArtifact,
		protocol: undefined
	});
	assert.throws(
		() =>
			canonicalizeOperationVariables(
				unboundWithCodec,
				singletonVariables
			),
		/replica artifact protocol binding is invalid/
	);
	for (const useArtifact of [
		() => canonicalizeOperationVariables(missingCodec, singletonVariables),
		() => replica.read(missingCodec, singletonVariables),
		() =>
			replica.writeResult(
				missingCodec,
				singletonVariables,
				codecWireFrame('must not write'),
				'network'
			),
		() => replica.watch(missingCodec, singletonVariables)
	]) {
		assert.throws(useArtifact, /protocol-v1 replica artifact requires variableCodec/);
	}
	await flushMicrotasks();
	assert.equal(fetches.length, 0);
	assert.equal(replica.inspectRecord(Item, 'item-1'), undefined);

	const otherSchema = Object.freeze({
		...CodecArtifact,
		protocol: Object.freeze({
			...CodecArtifact.protocol,
			schemaHash: 'schema-two'
		})
	});
	assert.equal(replica.read(otherSchema, expandedVariables).complete, false);
});

test('generated protocol identity is mandatory before cache identity or transport', async () => {
	const fetches = [];
	const replica = createDistributedReplica({
		transport: {
			fetch(request) {
				fetches.push(request);
				return new Promise(() => undefined);
			}
		}
	});
	const malformed = [
		[
			Object.freeze({ ...CodecArtifact, protocol: undefined }),
			/replica artifact protocol binding is invalid/
		],
		[
			Object.freeze({
				...CodecArtifact,
				protocol: Object.freeze({
					...CodecArtifact.protocol,
					surface: undefined
				})
			}),
			/replica artifact client surface is invalid/
		],
		[
			Object.freeze({
				...CodecArtifact,
				protocol: Object.freeze({
					...CodecArtifact.protocol,
					trustedPresets: undefined
				})
			}),
			/replica artifact trusted preset contract is invalid/
		],
		[
			Object.freeze({
				...CodecArtifact,
				protocol: Object.freeze({
					...CodecArtifact.protocol,
					operation: 'query:not-the-artifact'
				})
			}),
			/replica artifact protocol binding is invalid/
		]
	];
	for (const [artifact, expected] of malformed) {
		assert.throws(
			() => canonicalizeOperationVariables(artifact, singletonVariables),
			expected
		);
		assert.throws(() => replica.read(artifact, singletonVariables), expected);
		assert.throws(() => replica.watch(artifact, singletonVariables), expected);
	}
	await flushMicrotasks();
	assert.equal(fetches.length, 0);
});

test('replica rejects schema and unbound artifact mixing before cache reads without purging', () => {
	const replica = createDistributedReplica();
	replica.writeResult(
		CodecArtifact,
		singletonVariables,
		codecWireFrame(),
		'network'
	);
	assert.deepEqual(replica.read(CodecArtifact, expandedVariables).data, {
		items: [{ id: 'item-1', title: 'schema one' }]
	});

	const otherSchema = Object.freeze({
		...CodecArtifact,
		protocol: Object.freeze({
			...CodecArtifact.protocol,
			schemaHash: 'schema-two'
		})
	});
	assert.throws(
		() => replica.read(otherSchema, expandedVariables),
		/does not match the active replica binding/
	);
	const unboundArtifact = Object.freeze({
		...CodecArtifact,
		protocol: undefined
	});
	assert.throws(
		() => replica.read(unboundArtifact, expandedVariables),
		/replica artifact protocol binding is invalid/
	);
	assert.throws(
		() =>
			replica.writeResult(
				otherSchema,
				expandedVariables,
				codecWireFrame('wrong schema'),
				'network'
			),
		/does not match the active replica binding/
	);
	assert.deepEqual(replica.read(CodecArtifact, expandedVariables).data, {
		items: [{ id: 'item-1', title: 'schema one' }]
	});
});

test('an invalid write source cannot bind a replica', () => {
	const replica = createDistributedReplica();
	assert.throws(
		() =>
			replica.writeResult(
				CodecArtifact,
				singletonVariables,
				codecWireFrame(),
				'invalid-source'
			),
		/unsupported replica write source/
	);
	const otherSchema = Object.freeze({
		...CodecArtifact,
		protocol: Object.freeze({
			...CodecArtifact.protocol,
			schemaHash: 'schema-two'
		})
	});
	assert.equal(replica.read(otherSchema, expandedVariables).complete, false);
});

test('watch validates before transport and sends only canonical frozen variables', async () => {
	const fetches = [];
	const transport = {
		fetch(request) {
			fetches.push(request);
			return new Promise(() => undefined);
		}
	};
	const replica = createDistributedReplica({ transport });

	assert.throws(
		() =>
			replica.watch(CodecArtifact, {
				id: '1',
				where: { unknown: {} }
			}),
		/unknown filter field/
	);
	await flushMicrotasks();
	assert.equal(fetches.length, 0);

	const first = replica.watch(CodecArtifact, singletonVariables);
	const second = replica.watch(CodecArtifact, expandedVariables);
	await flushMicrotasks();
	assert.equal(fetches.length, 1);
	assert.deepEqual(fetches[0].variables, canonicalizeOperationVariables(
		CodecArtifact,
		singletonVariables
	));
	assertDeepFrozen(fetches[0].variables);

	const sameSchemaOtherOperation = Object.freeze({
		...CodecArtifact,
		id: 'query:codec-items-other',
		protocol: Object.freeze({
			...CodecArtifact.protocol,
			operation: 'query:codec-items-other'
		})
	});
	const third = replica.watch(sameSchemaOtherOperation, expandedVariables);
	await flushMicrotasks();
	assert.equal(fetches.length, 2);
	assert.equal(fetches[1].operationId, sameSchemaOtherOperation.id);

	const otherSchema = Object.freeze({
		...CodecArtifact,
		protocol: Object.freeze({
			...CodecArtifact.protocol,
			schemaHash: 'schema-two'
		})
	});
	assert.throws(
		() => replica.watch(otherSchema, expandedVariables),
		/does not match the active replica binding/
	);
	await flushMicrotasks();
	assert.equal(fetches.length, 2);

	const mismatchedOperation = {
		...CodecArtifact,
		protocol: { ...CodecArtifact.protocol, operation: 'query:not-the-artifact' }
	};
	assert.throws(
		() => replica.watch(mismatchedOperation, expandedVariables),
		/protocol binding is invalid/
	);
	assert.equal(fetches.length, 2);

	first.destroy();
	second.destroy();
	third.destroy();
});
