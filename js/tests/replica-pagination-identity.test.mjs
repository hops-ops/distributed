import assert from 'node:assert/strict';
import test from 'node:test';

import { coverageFromArtifact, resolveArguments } from '../dist/replica/identity/keys.js';
import { replicaIndexKey } from '../dist/replica/index.js';

const OFFSET_COVERAGE = Object.freeze({
	kind: 'offset',
	limitArgument: 'limit',
	offsetArgument: 'offset',
	defaultLimit: 25,
	maxLimit: 100
});

const PAGINATION_ARGUMENTS = Object.freeze({
	limit: Object.freeze({ kind: 'variable', name: 'limit' }),
	offset: Object.freeze({ kind: 'variable', name: 'offset' })
});

function indexKey(variables) {
	return replicaIndexKey({
		field: 'todos',
		arguments: resolveArguments(
			PAGINATION_ARGUMENTS,
			variables,
			OFFSET_COVERAGE
		)
	});
}

test('offset index identity uses the exact effective server window', () => {
	const defaultWindow = { limit: 25, offset: 0 };
	for (const variables of [
		{},
		{ limit: null, offset: null },
		{ limit: -1, offset: -1 },
		{ limit: 25, offset: 0 }
	]) {
		assert.deepEqual(
			resolveArguments(PAGINATION_ARGUMENTS, variables, OFFSET_COVERAGE),
			defaultWindow
		);
		assert.equal(indexKey(variables), indexKey({ limit: 25, offset: 0 }));
	}

	assert.deepEqual(resolveArguments(undefined, {}, OFFSET_COVERAGE), defaultWindow);
	assert.equal(indexKey({ limit: 1_000, offset: 4 }), indexKey({ limit: 100, offset: 4 }));
});

test('offset coverage accepts every pagination value accepted by the server', () => {
	for (const argumentsValue of [
		{},
		{ limit: null, offset: null },
		{ limit: -1, offset: -1 },
		{ limit: 25, offset: 0 }
	]) {
		assert.deepEqual(coverageFromArtifact(OFFSET_COVERAGE, argumentsValue, 3), {
			kind: 'offset',
			offset: 0,
			limit: 25,
			returned: 3
		});
	}

	assert.deepEqual(
		coverageFromArtifact(OFFSET_COVERAGE, { limit: 1_000, offset: 4 }, 3),
		{
			kind: 'offset',
			offset: 4,
			limit: 100,
			returned: 3
		}
	);
	assert.throws(
		() => coverageFromArtifact(OFFSET_COVERAGE, { limit: '25' }, 0),
		/pagination argument limit must be an integer or null/
	);
});
