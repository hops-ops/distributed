import assert from 'node:assert/strict';
import test from 'node:test';
import { defineDistributedBoundaryBinding, searchParam } from '../dist/sveltekit/index.js';
import { TodosArtifact } from './fixtures/adapter-conformance.mjs';

function artifact(scalar, { list = false, nullable = false, defaults = {} } = {}) {
	const item = { kind: 'scalar', scalar, codec: { Int: 'int32', Float: 'float64', Boolean: 'boolean', String: 'string', ID: 'string' }[scalar], nullable: false };
	return { ...TodosArtifact, variableCodec: {
		version: 2,
		limits: { maxDepth: 64, maxBoolWidth: 256, maxInList: 3 },
		variables: { value: list ? { kind: 'list', item, nullable, maxItems: 3 } : { ...item, nullable } },
		defaults, inputs: {}
	} };
}

function resolve(scalar, search, options = {}) {
	return defineDistributedBoundaryBinding(artifact(scalar, options), {
		value: searchParam('q', scalar, options.list ? 'all' : 'first')
	}).resolve({ params: {}, search, session: null, props: {} });
}

test('typed search scalars decode URL and record inputs with the same canonical result', () => {
	for (const [scalar, text, expected] of [
		['Int', '2147483647', 2147483647], ['Int', '-2147483648', -2147483648],
		['Int', '0', 0], ['Int', '-0', 0], ['Float', '1.25e2', 125],
		['Float', '-0.125', -0.125], ['Boolean', 'true', true], ['Boolean', 'false', false],
		['String', '雪 & café', '雪 & café'], ['ID', '001', '001'], ['String', '', '']
	]) {
		assert.deepEqual(resolve(scalar, new URLSearchParams({ q: text })), { value: expected });
		assert.deepEqual(resolve(scalar, { q: text }), { value: expected });
	}
	assert.deepEqual(resolve('Int', new URLSearchParams('q=2&q=1'), { list: true }), { value: [2, 1] });
	assert.deepEqual(resolve('Int', { q: ['2', '1'] }, { list: true }), { value: [2, 1] });
	assert.deepEqual(resolve('Int', { q: ['2', '1'] }), { value: 2 }, 'first mode is explicit and deterministic');
});

test('missing search inputs preserve defaults, nullable omission and required errors', () => {
	for (const search of [new URLSearchParams(), {}, { q: [] }]) {
		assert.deepEqual(resolve('Int', search, { defaults: { value: 25 } }), { value: 25 });
		assert.deepEqual(resolve('Int', search, { list: true, defaults: { value: [25] } }), { value: [25] });
		assert.deepEqual(resolve('Int', search, { nullable: true }), {});
		assert.deepEqual(resolve('Int', search, { list: true, nullable: true }), {});
		assert.throws(() => resolve('Int', search), /required variable is missing/);
	}
});

test('numeric and boolean URL decoding fails closed without exposing values', () => {
	for (const scalar of ['Int', 'Float', 'Boolean']) {
		for (const value of ['', ' ', ' 1', '1 ', '+1', '01', '0x10', 'NaN', 'Infinity', 'true-secret', 'null']) {
			assert.throws(() => resolve(scalar, { q: value }), (error) => {
				assert.match(error.message, /Distributed search scalar/);
				assert.doesNotMatch(error.message, /true-secret/);
				return true;
			});
		}
	}
	for (const value of ['1.5', '1e2', '1x', '2147483648', '-2147483649']) {
		assert.throws(() => resolve('Int', { q: value }));
	}
	for (const value of ['.5', '1.', '1e999']) assert.throws(() => resolve('Float', { q: value }));
	for (const value of ['1', '0', 'TRUE', 'False']) assert.throws(() => resolve('Boolean', { q: value }));
	for (const value of [1, true, null, {}, [1], ['1', null]]) {
		assert.throws(() => resolve('Int', { q: value }, { list: true }), /requires URL text/);
	}
	assert.throws(() => resolve('Int', { q: ['1', '2', '3', '4'] }, { list: true }), /limit|maximum|exceed/i);
});

test('search source registration rejects mismatched, untyped and unsafe contracts', () => {
	for (const source of [
		{ kind: 'search_param', name: 'q' },
		searchParam('q', 'String'), searchParam('q', 'Int', 'all'),
		{ ...searchParam('q', 'Int'), fallback: 0 },
		{ ...searchParam('q', 'Int'), scalar: 'JSON' },
		{ ...searchParam('q', 'Int'), mode: 'last' }
	]) assert.throws(() => defineDistributedBoundaryBinding(artifact('Int'), { value: source }));
	for (const enumerable of [true, false]) {
		const getter = { ...searchParam('q', 'Int') };
		Object.defineProperty(getter, 'scalar', { enumerable, get() { throw new Error('must not execute'); } });
		assert.throws(() => defineDistributedBoundaryBinding(artifact('Int'), { value: getter }), /invalid field/);
	}
	assert.throws(() => defineDistributedBoundaryBinding(artifact('Int', { list: true }), { value: searchParam('q', 'Int') }));
	const bound = defineDistributedBoundaryBinding(artifact('Int'), { value: searchParam('q', 'Int') });
	assert.equal(bound.version, 2);
	assert.match(bound.id, /^boundary-v2:/);
});
