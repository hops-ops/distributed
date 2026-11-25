import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
	compareReplicaOrder,
	evaluateReplicaFilter
} from '../dist/replica/index.js';

// The shared statement of what a `where`, a row policy and an `order_by`
// mean. `tests/graphql/graphql_compile/mod.rs` holds SQLite, Postgres and the Rust
// evaluator to the same file, and checks `fields` against the manifest.
const corpus = JSON.parse(
	await readFile(
		new URL('../../tests/fixtures/client-query-plan-corpus.json', import.meta.url),
		'utf8'
	)
);

const literal = (value) => Object.freeze({ kind: 'literal', value });
const orderFields = corpus.fields.map(({ field, scalar, codec, nullable }) => ({
	field,
	scalar,
	codec,
	nullable
}));
const tieBreakers = orderFields
	.filter(({ field }) => field === 'id')
	.map((field) => ({ ...field, nullable: false }));
const trustedPresets = {
	descriptors: corpus.claims.map(({ name, codec }) => ({ name, codec })),
	values: corpus.claims
};

for (const entry of corpus.cases) {
	test(`corpus: ${entry.name}`, () => {
		const filter = {
			...(entry.where === undefined ? {} : { input: literal(entry.where) }),
			fields: corpus.fields,
			relationships: [],
			rowPolicy:
				entry.rowPolicy === undefined
					? { kind: 'unrestricted' }
					: { kind: 'predicate', expression: entry.rowPolicy }
		};
		const undecided = entry.undecided ?? [];
		const matched = [];
		for (const record of corpus.records) {
			const expected =
				entry.unknown.includes(record.id) ||
				undecided.includes(record.id) ||
				(undecided.includes('*') && !entry.unknown.includes(record.id))
					? 'unknown'
					: entry.matches.includes(record.id)
						? 'match'
						: 'no_match';
			const evaluation = evaluateReplicaFilter(filter, record, {}, {
				trustedPresets
			});
			assert.equal(
				evaluation.result,
				expected,
				`row ${record.id}: ${evaluation.reason?.message ?? ''}`
			);
			if (evaluation.result === 'match') matched.push(record);
		}
		if (undecided.length > 0) return;

		const order = {
			input: literal(entry.orderBy),
			fields: orderFields,
			tieBreakers
		};
		matched.sort((left, right) => {
			const comparison = compareReplicaOrder(order, left, right);
			assert.notEqual(comparison.result, 'unknown', comparison.reason?.message);
			return comparison.result === 'less'
				? -1
				: comparison.result === 'greater'
					? 1
					: 0;
		});
		assert.deepEqual(
			matched
				.slice(entry.offset, entry.offset + entry.limit)
				.map((record) => record.id),
			entry.expected
		);
	});
}
