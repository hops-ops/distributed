import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { canonicalJson } from '../dist/lib/canonical-json.js';

// `tests/fixtures/resolved-projection-plan-v1.json` maps each occurrence,
// through its program, to the resolved plan Rust produces. Rust
// (`src/projection/tests.rs`, `resolved_plans_match_the_shared_fixture`)
// asserts its canonical bytes equal this file; this suite asserts the
// TypeScript canonicalizer reproduces the same bytes, and that the program
// identities and key/partition encodings inside it are those bytes'
// domain-separated encodings. Equal bytes imply equal digests, so no digest
// is pinned on either side.
async function fixtureText() {
	const text = await readFile(
		new URL('../../tests/fixtures/resolved-projection-plan-v1.json', import.meta.url),
		'utf8'
	);
	assert.ok(text.endsWith('\n') && !text.endsWith('\n\n'), 'fixture ends with one newline');
	return text.slice(0, -1);
}

const utf8 = new TextEncoder();

/** `domain || u64 big-endian length || canonical JSON`, as Rust encodes it. */
function domainSeparated(domain, value) {
	const canonical = utf8.encode(canonicalJson(value));
	const length = new Uint8Array(8);
	new DataView(length.buffer).setBigUint64(0, BigInt(canonical.length));
	return Buffer.concat([utf8.encode(`${domain}\0`), length, canonical]);
}

function programId(program) {
	const digest = createHash('sha256')
		.update(domainSeparated('distributed.projection-program/v1', program))
		.digest('hex');
	return `pp1:sha256:${digest}`;
}

/** Every resolved key (`fields`) and partition (`partition`) in a plan. */
function encodedValues(value, found = []) {
	if (value === null || typeof value !== 'object') return found;
	if (Array.isArray(value)) {
		for (const item of value) encodedValues(item, found);
		return found;
	}
	if ('canonical_bytes' in value) found.push(value);
	for (const child of Object.values(value)) encodedValues(child, found);
	return found;
}

test('TypeScript canonicalizes the resolved-plan fixture to the bytes Rust wrote', async () => {
	const text = await fixtureText();
	const fixture = JSON.parse(text);
	assert.equal(canonicalJson(fixture), text);
	assert.equal(fixture.fixture, 'resolved-projection-plan');
	assert.equal(fixture.version, 1);
	assert.deepEqual(
		fixture.cases.map(({ name }) => name),
		[
			'expression-keys-upsert-invalidations',
			'expression-keys-patch-unset',
			'composite-key-delete-unlink',
			'related-insert-link',
			'input-keys-body-partition',
			'constant-key-unit-partition'
		]
	);
});

test('each plan names the digest of its program and carries its own occurrence', async () => {
	const fixture = JSON.parse(await fixtureText());
	for (const { name, program, occurrence, plan } of fixture.cases) {
		const id = programId(fixture.programs[program]);
		assert.equal(plan.program_id, id, name);
		for (const mutation of plan.mutations) {
			assert.equal(mutation.provenance.program_id, id, name);
			assert.equal(mutation.provenance.occurrence.occurrence_id, occurrence.id, name);
		}
		assert.deepEqual(plan.occurrence, occurrence, name);
	}
});

test('key and partition bytes are the domain-separated canonical encodings', async () => {
	const fixture = JSON.parse(await fixtureText());
	let keys = 0;
	let partitions = 0;
	for (const { name, plan } of fixture.cases) {
		for (const encoded of encodedValues(plan)) {
			let expected;
			if ('fields' in encoded) {
				expected = domainSeparated('distributed.projection-key/v1', encoded.fields);
				keys += 1;
			} else {
				expected = domainSeparated('distributed.projection-partition/v1', encoded.partition);
				partitions += 1;
			}
			assert.deepEqual(Buffer.from(encoded.canonical_bytes), expected, name);
		}
	}
	// Every case has a plan partition and a scope partition per mutation;
	// keys include relationship-effect source and target keys.
	assert.ok(keys >= fixture.cases.length);
	assert.ok(partitions >= 2 * fixture.cases.length);
	assert.ok(
		fixture.cases.some(({ plan }) => plan.partition.partition.kind === 'unit'),
		'covers the unit partition'
	);
});
