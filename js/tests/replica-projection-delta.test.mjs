import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { canonicalCommandProjectionMetadata, canonicalProjectionDelta } from '../dist/replica/projection-delta/canonical.js';
import {
	parseCommandProjectionMetadata,
	parseProjectionDelta,
	validateCommandProjectionArtifact,
	validateProjectionMetadataAuthority
} from '../dist/replica/projection-delta/validate.js';

const clone = (value) => structuredClone(value);

// The shared fixtures are the cross-language wire contract: Rust
// (`src/graphql/projection_delta/vector_tests.rs`,
// `src/graphql/protocol/projection_metadata.rs`) asserts its canonical bytes
// equal these files, and this suite asserts the TypeScript canonicalizer
// reproduces them exactly. Equal bytes imply an equal digest.
async function fixtureText(name) {
	const text = await readFile(new URL(`../../tests/fixtures/${name}`, import.meta.url), 'utf8');
	return text.endsWith('\n') ? text.slice(0, -1) : text;
}

async function vector() {
	return JSON.parse(await fixtureText('projection-delta-v1.json'));
}

async function metadataVector() {
	return JSON.parse(await fixtureText('command-projection-metadata-v1.json'));
}

test('Rust projection vector is byte-identical across every op and tagged value', async () => {
	const parsed = parseProjectionDelta(await vector());
	assert.equal(canonicalProjectionDelta(parsed), await fixtureText('projection-delta-v1.json'));
	assert.equal(parsed.operations.length, 8);
	assert.deepEqual(
		parsed.operations.map(({ mutation }) => mutation.op),
		[
			'upsert',
			'patch',
			'delete',
			'link',
			'link',
			'unlink',
			'invalidate_model',
			'invalidate_relationship'
		]
	);
});

test('metadata validates identity, expiry, obligations, and canonical replay bytes', async () => {
	const parsed = parseCommandProjectionMetadata(await metadataVector());
	const projection = parsed.delta.projections[0];
	const contract = {
		version: 2,
		deltaWireVersion: 1,
		projectionProgramVersion: 2,
		operationSemanticsVersion: 1,
		projections: [
			{
				programId: projection.program_id,
				bindingId: projection.binding_id,
				epoch: projection.epoch,
				programIrVersion: 1,
				operationSemanticsVersion: 1
			}
		],
		eventSet: [],
		capabilities: { version: 1, arms: [] },
		preview: { version: 1, occurrences: [], operations: [], recoveries: [] },
		fallback: 'revalidate'
	};
	const canonical = validateProjectionMetadataAuthority(
		parsed,
		contract,
		{
			surface: { kind: 'role', name: 'member' },
			schemaHash: 'sha256:schema',
			protocolHash: 'sha256:protocol',
			authorizationGeneration: 'auth-generation-1',
			cacheScope:
				'v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
			causationId: 'cause-1'
		},
		1_700_000_000_001
	);
	assert.equal(canonical, canonicalCommandProjectionMetadata(parsed));
	assert.equal(canonical, await fixtureText('command-projection-metadata-v1.json'));
	assert.throws(() =>
		validateProjectionMetadataAuthority(
			parsed,
			contract,
			{
				surface: { kind: 'role', name: 'member' },
				schemaHash: 'sha256:schema',
				protocolHash: 'sha256:protocol',
				authorizationGeneration: 'auth-generation-2',
				cacheScope:
					'v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
				causationId: 'cause-1'
			},
			1_700_000_000_001
		)
	);
	assert.throws(() =>
		validateProjectionMetadataAuthority(
			parsed,
			contract,
			{
				surface: { kind: 'role', name: 'member' },
				schemaHash: 'sha256:schema',
				protocolHash: 'sha256:protocol',
				cacheScope:
					'v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
				causationId: 'cause-1'
			},
			1_700_000_000_001
		)
	);
	assert.throws(() =>
		validateProjectionMetadataAuthority(
			parsed,
			contract,
			{
				surface: { kind: 'role', name: 'member' },
				schemaHash: 'sha256:schema',
				protocolHash: 'sha256:protocol',
				authorizationGeneration: 1,
				cacheScope:
					'v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
				causationId: 'cause-1'
			},
			1_700_000_000_001
		)
	);
});

test('lifecycle proofs are dense, ordered, bounded opaque data and never client authority', async () => {
	const accepted = await metadataVector();
	const missing = clone(accepted);
	missing.lifecycleProofs = [];
	assert.throws(
		() => parseCommandProjectionMetadata(missing),
		/lifecycleProofs/
	);

	const wrongPurpose = clone(accepted);
	wrongPurpose.lifecycleProofs[0].token =
		`v1.projection-obligation.${Buffer.alloc(32, 3).toString('base64url')}`;
	assert.throws(
		() => parseCommandProjectionMetadata(wrongPurpose),
		/lifecycleProofs/
	);

	const tamperedShape = clone(accepted);
	tamperedShape.lifecycleProofs[0].route = 'client-must-not-see-or-trust-this';
	assert.throws(
		() => parseCommandProjectionMetadata(tamperedShape),
		/lifecycleProofs/
	);

	const oversized = clone(accepted);
	oversized.lifecycleProofs = Array.from({ length: 129 }, (_, index) => ({
		projectionRef: index,
		token: `v1.projection-lifecycle.${Buffer.alloc(32, index).toString('base64url')}`
	}));
	assert.throws(
		() => parseCommandProjectionMetadata(oversized),
		/lifecycleProofs/
	);

	const unordered = clone(accepted);
	unordered.delta = await vector();
	unordered.lifecycleProofs = [
		{
			projectionRef: 1,
			token: `v1.projection-lifecycle.${Buffer.alloc(32, 4).toString('base64url')}`
		},
		{
			projectionRef: 0,
			token: `v1.projection-lifecycle.${Buffer.alloc(32, 5).toString('base64url')}`
		}
	];
	assert.throws(
		() => parseCommandProjectionMetadata(unordered),
		/lifecycleProofs/
	);
});

test('strict hostile inputs fail before allocation or cache mutation', async () => {
	const original = await vector();
	const unknown = clone(original);
	unknown.identity.surprise = true;
	assert.throws(() => parseProjectionDelta(unknown), /identity\.surprise/);

	const reordered = clone(original);
	[reordered.operations[0], reordered.operations[1]] = [
		reordered.operations[1],
		reordered.operations[0]
	];
	assert.throws(() => parseProjectionDelta(reordered), /operations/);

	const oversizedKey = clone(original);
	oversizedKey.operations[0].mutation.scope.key[0].value.value = 'x'.repeat(4097);
	assert.throws(() => parseProjectionDelta(oversizedKey), /key/);

	const forgedToken = clone(original);
	forgedToken.identity.cache_scope_token =
		'v1.projection-partition.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
	assert.throws(() => parseProjectionDelta(forgedToken), /cache_scope_token/);

	const tooMany = clone(original);
	tooMany.occurrences = Array.from({ length: 129 }, (_, ordinal) => ({
		causation_id: original.identity.command_causation_id,
		ordinal,
		occurrence_id: `occurrence-${ordinal}`
	}));
	assert.throws(() => parseProjectionDelta(tooMany), /occurrences/);
});

test('preflight reserves dense-array and broad-object visits before enqueue', () => {
	const traversalBudget = 1024 * 1024;
	let denseDescriptorReads = 0;
	const dense = new Proxy(Array(traversalBudget).fill(null), {
		getOwnPropertyDescriptor(target, property) {
			denseDescriptorReads += 1;
			return Reflect.getOwnPropertyDescriptor(target, property);
		}
	});
	assert.throws(
		() => parseProjectionDelta(dense),
		(error) => error?.path === 'projection.delta'
	);
	assert.equal(
		denseDescriptorReads,
		0,
		'array length must exhaust the remaining visit budget before descriptors enqueue'
	);

	const broadTarget = Object.fromEntries(
		Array.from({ length: 64 }, (_, index) => [`field${index}`, null])
	);
	let broadDescriptorReads = 0;
	const broad = new Proxy(broadTarget, {
		getOwnPropertyDescriptor(target, property) {
			broadDescriptorReads += 1;
			return Reflect.getOwnPropertyDescriptor(target, property);
		}
	});
	const carrierTarget = [];
	carrierTarget.length = traversalBudget - 32;
	const carrier = new Proxy(carrierTarget, {
		getOwnPropertyDescriptor(_target, property) {
			const index = Number(property);
			return {
				configurable: true,
				enumerable: true,
				writable: true,
				value: index === 0 ? broad : null
			};
		}
	});
	assert.throws(
		() => parseProjectionDelta(carrier),
		(error) => error?.path === 'projection.delta'
	);
	assert.equal(
		broadDescriptorReads,
		0,
		'object width must exhaust the remaining visit budget before descriptors enqueue'
	);
});

test('f64 strings use Rust serde_json/ryu notation at fixed/scientific boundaries', async () => {
	const original = await vector();
	const float = (value) => {
		const candidate = clone(original);
		candidate.operations[0].mutation.fields[0].value.value[0].value.value =
			value;
		return candidate;
	};
	assert.doesNotThrow(() => parseProjectionDelta(float('1e+20')));
	assert.doesNotThrow(() => parseProjectionDelta(float('1e-6')));
	assert.doesNotThrow(() => parseProjectionDelta(float('0.00001')));
	assert.doesNotThrow(() =>
		parseProjectionDelta(float('1000000000000000.0'))
	);
	assert.throws(() =>
		parseProjectionDelta(float('100000000000000000000.0'))
	);
	assert.throws(() => parseProjectionDelta(float('0.000001')));
	assert.throws(() => parseProjectionDelta(float('1e20')));
});

function projectionArtifact() {
	const event = { id: 'event-1', name: 'todo.changed', version: 1 };
	const scope = {
		partition: { kind: 'unit' },
		model: 'Todo',
		key: [
			{
				ordinal: 0,
				field: 'id',
				value: {
					kind: 'constant',
					value: { type: 'string', value: 'todo-1' }
				}
			}
		]
	};
	return {
		version: 2,
		deltaWireVersion: 1,
		projectionProgramVersion: 2,
		operationSemanticsVersion: 1,
		projections: [
			{
				programId: `pp1:sha256:${'1'.repeat(64)}`,
				bindingId: `pb1:sha256:${'2'.repeat(64)}`,
				epoch: 'todos-v1',
				programIrVersion: 1,
				operationSemanticsVersion: 1
			}
		],
		eventSet: [event],
		capabilities: {
			version: 1,
			arms: [
				{
					event,
					projection_ref: 0,
					arm: 'todo_changed',
					partition: { kind: 'unit' },
					mutations: [
						{
							kind: 'record',
							model: 'Todo',
							key: ['id'],
							fields: ['title'],
							replace: [],
							upsert: false,
							patch: true,
							delete: false
						}
					]
				}
			]
		},
		preview: {
			version: 1,
			occurrences: [{ ordinal: 0, event }],
			operations: [
				{
					occurrence_ordinal: 0,
					projection_refs: [0],
					mutation: {
						op: 'patch',
						scope,
						set: [
							{
								field: 'title',
								value: {
									kind: 'constant',
									value: { type: 'string', value: 'updated' }
								}
							}
						],
						unset: [],
						if_present: true
					}
				}
			],
			recoveries: []
		},
		fallback: 'revalidate'
	};
}

test('projection artifact enforces Rust-aligned epoch/model and 4 KiB preview bounds', () => {
	assert.doesNotThrow(() =>
		validateCommandProjectionArtifact(projectionArtifact())
	);

	const epoch = projectionArtifact();
	epoch.projections[0].epoch = 'e'.repeat(129);
	assert.throws(() => validateCommandProjectionArtifact(epoch), /epoch/);

	const controlEpoch = projectionArtifact();
	controlEpoch.projections[0].epoch = `todos\u0000v1`;
	assert.throws(() =>
		validateCommandProjectionArtifact(controlEpoch)
	);

	const model = projectionArtifact();
	model.preview.operations[0].mutation.scope.model = 'M'.repeat(129);
	assert.throws(() => validateCommandProjectionArtifact(model), /model/);

	const key = projectionArtifact();
	key.preview.operations[0].mutation.scope.key[0].value.value.value =
		'x'.repeat(4097);
	assert.throws(() => validateCommandProjectionArtifact(key), /key/);

	const partition = projectionArtifact();
	partition.preview.operations[0].mutation.scope.partition = {
		kind: 'expression',
		expression: {
			kind: 'constant',
			value: { type: 'string', value: 'x'.repeat(4097) }
		},
		requires: 'current_cache_partition'
	};
	assert.throws(() =>
		validateCommandProjectionArtifact(partition)
	);
});

test('projection metadata keeps obligation models at the Rust 255-byte boundary', async () => {
	const accepted = await metadataVector();
	accepted.delta = await vector();
	accepted.lifecycleProofs.push({
		projectionRef: 1,
		token: `v1.projection-lifecycle.${Buffer.alloc(32, 2).toString('base64url')}`
	});
	accepted.revalidate = true;
	accepted.obligations[0].projectionRef = 0;
	accepted.obligations[0].model = 'M'.repeat(255);
	assert.doesNotThrow(() => parseCommandProjectionMetadata(accepted));

	const rejected = clone(accepted);
	rejected.obligations[0].model = 'M'.repeat(256);
	assert.throws(() => parseCommandProjectionMetadata(rejected), /model/);
});

test('projection artifacts reject aggregate payloads above one MiB', () => {
	const artifact = projectionArtifact();
	const mutations = Array.from({ length: 128 }, (_, index) => ({
		kind: 'relationship',
		relationship: `r${String(index).padStart(3, '0')}`.padEnd(4096, 'x'),
		source_model: 'Todo',
		source_key: ['id'],
		target_model: 'Todo',
		target_key: ['id'],
		link: true,
		unlink: true
	}));
	artifact.capabilities.arms = ['a', 'b', 'c'].map((arm) => ({
		event: artifact.eventSet[0],
		projection_ref: 0,
		arm,
		partition: { kind: 'unit' },
		mutations
	}));
	assert.ok(Buffer.byteLength(JSON.stringify(artifact)) > 1024 * 1024);
	assert.throws(() => validateCommandProjectionArtifact(artifact));
});

for (const operationCount of [65, 128]) {
	test(`projection capabilities accept ${operationCount} operations after record/model expansion`, () => {
		const artifact = projectionArtifact();
		const records = Array.from({ length: operationCount }, (_, index) => ({
			kind: 'record',
			model: `Model${String(index).padStart(3, '0')}`,
			key: ['id'],
			fields: ['title'],
			replace: ['title'],
			upsert: true,
			patch: true,
			delete: false
		}));
		const models = Array.from({ length: operationCount }, (_, index) => ({
			kind: 'model',
			model: `Model${String(index).padStart(3, '0')}`
		}));
		artifact.capabilities.arms[0].mutations = [...records, ...models];
		assert.equal(
			artifact.capabilities.arms[0].mutations.length,
			operationCount * 2
		);
		assert.doesNotThrow(() =>
			validateCommandProjectionArtifact(artifact)
		);
	});
}

test('aggregate byte budget rejects before constructing a late hostile artifact member', () => {
	const artifact = projectionArtifact();
	artifact.capabilities.arms[0].mutations = Array.from(
		{ length: 128 },
		(_, index) => ({
			kind: 'relationship',
			relationship: `r${String(index).padStart(3, '0')}`.padEnd(9_000, 'x'),
			source_model: 'Todo',
			source_key: ['id'],
			target_model: 'Todo',
			target_key: ['id'],
			link: true,
			unlink: true,
			...(index === 127 ? { hostile: true } : {})
		})
	);
	assert.throws(
		() => validateCommandProjectionArtifact(artifact),
		(error) => error.path === 'artifact.projection'
	);
});
