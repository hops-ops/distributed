import { isPlainRecord } from '../../lib/is-plain-record.js';
import { compareUtf8 } from '../../lib/compare-utf8.js';
import { oncePerIdentity } from '../../lib/once-per-identity.js';
import {
	canonicalCommandProjectionMetadata,
	canonicalProjectionDelta,
	commandProjectionMetadataByteLength,
	projectionDeltaByteLength
} from './canonical.js';
import {
	COMMAND_PROJECTION_ARTIFACT_VERSION,
	COMMAND_PROJECTION_METADATA_WIRE_VERSION,
	PROJECTION_DELTA_WIRE_VERSION,
	PROJECTION_OPERATION_SEMANTICS_VERSION,
	PROJECTION_PROGRAM_IR_VERSION,
	PROJECTION_PROGRAM_VERSION,
	type CommandProjectionMetadata,
	type CommandProjectionObligation,
	type ProjectionCapabilityArm,
	type ProjectionCapabilityMutation,
	type ProjectionCapabilityPartition,
	type ProjectionDelta,
	type ProjectionDeltaField,
	type ProjectionDeltaMutation,
	type ProjectionDeltaPartition,
	type ProjectionDeltaProjectionIdentity,
	type ProjectionDeltaRecovery,
	type ProjectionDeltaRecoveryTarget,
	type ProjectionDeltaScope,
	type ProjectionDeltaSurface,
	type ProjectionDeltaValue,
	type ProjectionPreviewMutation,
	type ProjectionPreviewPartition,
	type ProjectionPreviewRecoveryTarget,
	type ProjectionPreviewScope,
	type ProjectionPreviewValue,
	type ReplicaCommandProjection
} from './types.js';

const MAX_ITEMS = 128;
const MAX_DEPTH = 64;
const MAX_BODY_BYTES = 1024 * 1024;
const MAX_TRAVERSAL_NODES = MAX_BODY_BYTES;
const MAX_TRAVERSAL_DEPTH = MAX_DEPTH + 64;
const MAX_KEY_BYTES = 4 * 1024;
const MAX_PARTITION_BYTES = 4 * 1024;
const MAX_IDENTITY_BYTES = 4 * 1024;
const PROGRAM_ID = /^pp1:sha256:[0-9a-f]{64}$/;
const BINDING_ID = /^pb1:sha256:[0-9a-f]{64}$/;
const SHA256 = /^sha256:[0-9a-f]{64}$/;
const U64_MAX = 18_446_744_073_709_551_615n;
const I64_MIN = -9_223_372_036_854_775_808n;
const I64_MAX = 9_223_372_036_854_775_807n;
const encoder = new TextEncoder();

export class ProjectionDeltaValidationError extends TypeError {
	readonly path: string;

	constructor(path: string) {
		super(`Invalid projection delta at ${path}`);
		this.name = 'ProjectionDeltaValidationError';
		this.path = path;
	}
}

export function parseProjectionDelta(value: unknown): ProjectionDelta {
	preflightProjectionPayload(value, 'projection.delta');
	const delta = exactRecord(
		value,
		['wire_version', 'identity', 'projections', 'occurrences', 'operations'],
		['recoveries'],
		'projection.delta'
	);
	if (delta.wire_version !== PROJECTION_DELTA_WIRE_VERSION) {
		invalid('projection.delta.wire_version');
	}
	const identityValue = exactRecord(
		delta.identity,
		[
			'manifest_version',
			'client_protocol_version',
			'surface',
			'schema_fingerprint',
			'protocol_fingerprint',
			'authorization_generation',
			'cache_scope_token',
			'command_causation_id'
		],
		[],
		'projection.delta.identity'
	);
	if (
		identityValue.manifest_version !== 2 ||
		identityValue.client_protocol_version !== 1
	) {
		invalid('projection.delta.identity');
	}
	const identity = Object.freeze({
		manifest_version: 2 as const,
		client_protocol_version: 1 as const,
		surface: parseSurface(
			identityValue.surface,
			'projection.delta.identity.surface'
		),
		schema_fingerprint: identityString(
			identityValue.schema_fingerprint,
			'projection.delta.identity.schema_fingerprint'
		),
		protocol_fingerprint: identityString(
			identityValue.protocol_fingerprint,
			'projection.delta.identity.protocol_fingerprint'
		),
		authorization_generation: identityString(
			identityValue.authorization_generation,
			'projection.delta.identity.authorization_generation'
		),
		cache_scope_token: protocolToken(
			identityValue.cache_scope_token,
			'cache-scope',
			'projection.delta.identity.cache_scope_token'
		),
		command_causation_id: identityString(
			identityValue.command_causation_id,
			'projection.delta.identity.command_causation_id'
		)
	});
	const projections = boundedArray(
		delta.projections,
		'projection.delta.projections'
	).map((item, index) =>
		parseProjectionIdentity(item, `projection.delta.projections[${index}]`)
	);
	assertStrictOrder(
		projections,
		compareProjectionIdentity,
		'projection.delta.projections'
	);
	const occurrences = boundedArray(
		delta.occurrences,
		'projection.delta.occurrences'
	).map((item, index) => {
		const path = `projection.delta.occurrences[${index}]`;
		const occurrence = exactRecord(
			item,
			['causation_id', 'ordinal', 'occurrence_id'],
			[],
			path
		);
		const causationId = identityString(
			occurrence.causation_id,
			`${path}.causation_id`
		);
		const ordinal = boundedOrdinal(occurrence.ordinal, `${path}.ordinal`);
		const occurrenceId = identityString(
			occurrence.occurrence_id,
			`${path}.occurrence_id`
		);
		if (ordinal !== index || causationId !== identity.command_causation_id) {
			invalid(path);
		}
		return Object.freeze({
			causation_id: causationId,
			ordinal,
			occurrence_id: occurrenceId
		});
	});
	assertUnique(
		occurrences.map(({ occurrence_id }) => occurrence_id),
		'projection.delta.occurrences'
	);
	const operations = boundedArray(
		delta.operations,
		'projection.delta.operations'
	).map((item, index) => {
		const path = `projection.delta.operations[${index}]`;
		const operation = exactRecord(
			item,
			['occurrence_ordinal', 'projection_refs', 'mutation'],
			[],
			path
		);
		const occurrenceOrdinal = boundedOrdinal(
			operation.occurrence_ordinal,
			`${path}.occurrence_ordinal`
		);
		if (occurrenceOrdinal >= occurrences.length) {
			invalid(`${path}.occurrence_ordinal`);
		}
			return Object.freeze({
				occurrence_ordinal: occurrenceOrdinal,
				projection_refs: projectionRefs(
					operation.projection_refs,
					projections.length,
					`${path}.projection_refs`
				),
				mutation: parseMutation(operation.mutation, `${path}.mutation`)
			});
		});
	assertStrictOrder(
		operations,
		(left, right) =>
			compareTuple(
				[operationScopeKey(left.mutation), left.occurrence_ordinal],
				[operationScopeKey(right.mutation), right.occurrence_ordinal]
			),
		'projection.delta.operations'
	);
	assertUnique(
		operations.map(({ mutation }) => JSON.stringify(operationScopeKey(mutation))),
		'projection.delta.operations'
	);
	const recoveries = boundedArray(
		delta.recoveries ?? [],
		'projection.delta.recoveries'
	).map((item, index) =>
		parseRecovery(
			item,
			index,
			projections.length,
			occurrences.length
		)
	);
	assertStrictOrder(
		recoveries,
		(left, right) =>
			compareTuple(
				[recoveryTargetKey(left.target), left.occurrence_ordinal],
				[recoveryTargetKey(right.target), right.occurrence_ordinal]
			),
		'projection.delta.recoveries'
	);
	assertUnique(
		recoveries.map(({ target }) => JSON.stringify(recoveryTargetKey(target))),
		'projection.delta.recoveries'
	);
	for (const recovery of recoveries) {
		if (recovery.condition !== 'if_record_missing') continue;
		if (
			recovery.target.kind !== 'record' ||
			!operations.some(
				({ mutation }) =>
					mutation.op === 'patch' &&
					compareTuple(
						scopeKey(mutation.scope),
						scopeKey(recovery.target.kind === 'record'
							? recovery.target.scope
							: mutation.scope)
					) === 0
			)
		) {
			invalid('projection.delta.recoveries');
		}
	}
	if (
		projections.length === 0 &&
		(occurrences.length !== 0 ||
			operations.length !== 0 ||
			recoveries.length !== 0)
	) {
		invalid('projection.delta.projections');
	}
	const parsed = Object.freeze({
		wire_version: 1 as const,
		identity,
		projections: Object.freeze(projections),
		occurrences: Object.freeze(occurrences),
		operations: Object.freeze(operations),
		recoveries: Object.freeze(recoveries)
	});
	if (projectionDeltaByteLength(parsed) > MAX_BODY_BYTES) {
		invalid('projection.delta');
	}
	return parsed;
}

export function parseCommandProjectionMetadata(
	value: unknown
): CommandProjectionMetadata {
	preflightProjectionPayload(value, 'projection');
	const metadata = exactRecord(
		value,
		[
			'wireVersion',
			'issuedAtUnixMs',
			'expiresAtUnixMs',
			'delta',
			'lifecycleProofs',
			'obligations',
			'revalidate'
		],
		[],
		'projection'
	);
	if (
		metadata.wireVersion !== COMMAND_PROJECTION_METADATA_WIRE_VERSION ||
		typeof metadata.revalidate !== 'boolean'
	) {
		invalid('projection');
	}
	const issuedAtUnixMs = safeUnsignedInteger(
		metadata.issuedAtUnixMs,
		'projection.issuedAtUnixMs'
	);
	const expiresAtUnixMs = safeUnsignedInteger(
		metadata.expiresAtUnixMs,
		'projection.expiresAtUnixMs'
	);
	if (issuedAtUnixMs >= expiresAtUnixMs) invalid('projection.expiresAtUnixMs');
	const delta = parseProjectionDelta(metadata.delta);
	const lifecycleProofs = boundedArray(
		metadata.lifecycleProofs,
		'projection.lifecycleProofs'
	).map((item, index) => {
		const path = `projection.lifecycleProofs[${index}]`;
		const proof = exactRecord(item, ['projectionRef', 'token'], [], path);
		const projectionRef = boundedOrdinal(
			proof.projectionRef,
			`${path}.projectionRef`
		);
		if (projectionRef !== index || projectionRef >= delta.projections.length) {
			invalid(path);
		}
		return Object.freeze({
			projectionRef,
			token: protocolToken(
				proof.token,
				'projection-lifecycle',
				`${path}.token`
			)
		});
	});
	if (lifecycleProofs.length !== delta.projections.length) {
		invalid('projection.lifecycleProofs');
	}
	const obligations = boundedArray(
		metadata.obligations,
		'projection.obligations'
	).map((item, index) =>
		parseObligation(item, index, delta)
	);
	assertStrictOrder(
		obligations,
		(left, right) =>
			compareTuple(
				[left.projectionRef, left.model, left.scopeToken],
				[right.projectionRef, right.model, right.scopeToken]
			),
		'projection.obligations'
	);
	const scopeModels = new Map<string, string>();
	for (const obligation of obligations) {
		const key = JSON.stringify([
			obligation.projectionRef,
			obligation.scopeToken
		]);
		const previous = scopeModels.get(key);
		if (previous !== undefined && previous !== obligation.model) {
			invalid('projection.obligations');
		}
		scopeModels.set(key, obligation.model);
	}
	if (!metadata.revalidate && delta.recoveries.length !== 0) {
		invalid('projection.revalidate');
	}
	const parsed = Object.freeze({
		wireVersion: 1 as const,
		issuedAtUnixMs,
		expiresAtUnixMs,
		delta,
		lifecycleProofs: Object.freeze(lifecycleProofs),
		obligations: Object.freeze(obligations),
		revalidate: metadata.revalidate
	});
	if (commandProjectionMetadataByteLength(parsed) > MAX_BODY_BYTES) {
		invalid('projection');
	}
	return parsed;
}

/**
 * A command's projection is an immutable generated constant, validated once.
 * A caller that names its own error path is validated every time.
 */
export function validateCommandProjectionArtifact(
	value: unknown,
	path = 'artifact.projection'
): ReplicaCommandProjection {
	return path === 'artifact.projection'
		? oncePerIdentity(validatedProjections, value, (projection) =>
				parseCommandProjectionArtifact(projection, path)
			)
		: parseCommandProjectionArtifact(value, path);
}

const validatedProjections = new WeakMap<object, ReplicaCommandProjection>();

function parseCommandProjectionArtifact(
	value: unknown,
	path: string
): ReplicaCommandProjection {
	preflightProjectionPayload(value, path);
	const projection = exactRecord(
		value,
		[
			'version',
			'deltaWireVersion',
			'projectionProgramVersion',
			'operationSemanticsVersion',
			'projections',
			'eventSet',
			'capabilities',
			'preview',
			'fallback'
		],
		['pureReduces'],
		path
	);
	if (
		projection.version !== COMMAND_PROJECTION_ARTIFACT_VERSION ||
		projection.deltaWireVersion !== PROJECTION_DELTA_WIRE_VERSION ||
		projection.projectionProgramVersion !== PROJECTION_PROGRAM_VERSION ||
		projection.operationSemanticsVersion !==
			PROJECTION_OPERATION_SEMANTICS_VERSION ||
		projection.fallback !== 'revalidate'
	) {
		invalid(path);
	}
	const projections = boundedArray(
		projection.projections,
		`${path}.projections`
	).map((item, index) => {
		const itemPath = `${path}.projections[${index}]`;
		const identity = exactRecord(
			item,
			[
				'programId',
				'bindingId',
				'epoch',
				'programIrVersion',
				'operationSemanticsVersion'
			],
			[],
			itemPath
		);
		if (
			!PROGRAM_ID.test(identity.programId as string) ||
			!BINDING_ID.test(identity.bindingId as string) ||
			identity.programIrVersion !== PROJECTION_PROGRAM_IR_VERSION ||
			identity.operationSemanticsVersion !==
				PROJECTION_OPERATION_SEMANTICS_VERSION
		) {
			invalid(itemPath);
		}
			return Object.freeze({
				programId: identityString(identity.programId, `${itemPath}.programId`),
				bindingId: identityString(identity.bindingId, `${itemPath}.bindingId`),
				epoch: projectionEpoch(identity.epoch, `${itemPath}.epoch`),
				programIrVersion: 1 as const,
				operationSemanticsVersion: 1 as const
			});
		});
	assertStrictOrder(
		projections,
		(left, right) =>
			compareTuple(
				[
					left.programId,
					left.bindingId,
					left.epoch,
					left.programIrVersion,
					left.operationSemanticsVersion
				],
				[
					right.programId,
					right.bindingId,
					right.epoch,
					right.programIrVersion,
					right.operationSemanticsVersion
				]
			),
		`${path}.projections`
	);
	const eventSet = boundedArray(projection.eventSet, `${path}.eventSet`).map(
		(item, index) => parseEventRef(item, `${path}.eventSet[${index}]`)
	);
	assertStrictOrder(
		eventSet,
		(left, right) =>
			compareTuple(
				[left.id, left.name, left.version],
				[right.id, right.name, right.version]
			),
		`${path}.eventSet`
	);
	const capabilitiesValue = exactRecord(
		projection.capabilities,
		['version', 'arms'],
		[],
		`${path}.capabilities`
	);
	if (capabilitiesValue.version !== 1) {
		invalid(`${path}.capabilities.version`);
	}
	const capabilityArms = boundedArray(
		capabilitiesValue.arms,
		`${path}.capabilities.arms`
	).map((item, index) =>
		parseCapabilityArm(
			item,
			index,
			projections.length,
			eventSet,
			`${path}.capabilities.arms[${index}]`
		)
	);
	if (
		capabilityArms.length === 0 ||
		eventSet.some(
			(event) =>
				!capabilityArms.some((arm) => sameEventRef(arm.event, event))
		)
	) {
		invalid(`${path}.capabilities.arms`);
	}
	assertStrictOrder(
		capabilityArms,
		(left, right) =>
			compareTuple(
				[
					left.event.id,
					left.event.name,
					left.event.version,
					left.projection_ref,
					left.arm
				],
				[
					right.event.id,
					right.event.name,
					right.event.version,
					right.projection_ref,
					right.arm
				]
			),
		`${path}.capabilities.arms`
	);
	const previewValue = exactRecord(
		projection.preview,
		['version', 'occurrences', 'operations', 'recoveries'],
		[],
		`${path}.preview`
	);
	if (previewValue.version !== 1) invalid(`${path}.preview.version`);
	const occurrences = boundedArray(
		previewValue.occurrences,
		`${path}.preview.occurrences`
	).map((item, index) => {
		const itemPath = `${path}.preview.occurrences[${index}]`;
		const occurrence = exactRecord(item, ['ordinal', 'event'], [], itemPath);
		const ordinal = boundedOrdinal(occurrence.ordinal, `${itemPath}.ordinal`);
		const event = parseEventRef(occurrence.event, `${itemPath}.event`);
		if (
			ordinal !== index ||
			!eventSet.some(
				(candidate) =>
					candidate.id === event.id &&
					candidate.name === event.name &&
					candidate.version === event.version
			)
		) {
			invalid(itemPath);
		}
		return Object.freeze({ ordinal, event });
	});
	const operations = boundedArray(
		previewValue.operations,
		`${path}.preview.operations`
	).map((item, index) => {
		const itemPath = `${path}.preview.operations[${index}]`;
		const operation = exactRecord(
			item,
			['occurrence_ordinal', 'projection_refs', 'mutation'],
			[],
			itemPath
		);
		const occurrenceOrdinal = boundedOrdinal(
			operation.occurrence_ordinal,
			`${itemPath}.occurrence_ordinal`
		);
		if (occurrenceOrdinal >= occurrences.length) invalid(itemPath);
		return Object.freeze({
			occurrence_ordinal: occurrenceOrdinal,
			projection_refs: projectionRefs(
				operation.projection_refs,
				projections.length,
				`${itemPath}.projection_refs`
			),
			mutation: parsePreviewMutation(
				operation.mutation,
				`${itemPath}.mutation`
			)
			});
		});
		assertStrictOrder(
			operations,
			(left, right) =>
				compareTuple(
					[
						previewOperationScopeKey(left.mutation),
						left.occurrence_ordinal
					],
					[
						previewOperationScopeKey(right.mutation),
						right.occurrence_ordinal
					]
				),
			`${path}.preview.operations`
		);
		assertUnique(
			operations.map(({ mutation }) =>
				JSON.stringify(previewOperationScopeKey(mutation))
			),
			`${path}.preview.operations`
		);
		const recoveries = boundedArray(
		previewValue.recoveries,
		`${path}.preview.recoveries`
	).map((item, index) => {
		const itemPath = `${path}.preview.recoveries[${index}]`;
		const recovery = exactRecord(
			item,
			['occurrence_ordinal', 'projection_refs', 'condition', 'target'],
			[],
			itemPath
		);
		const occurrenceOrdinal = boundedOrdinal(
			recovery.occurrence_ordinal,
			`${itemPath}.occurrence_ordinal`
		);
		if (
			occurrenceOrdinal >= occurrences.length ||
			(recovery.condition !== 'always' &&
				recovery.condition !== 'if_record_missing')
		) {
			invalid(itemPath);
		}
		return Object.freeze({
			occurrence_ordinal: occurrenceOrdinal,
			projection_refs: projectionRefs(
				recovery.projection_refs,
				projections.length,
				`${itemPath}.projection_refs`
			),
			condition: recovery.condition,
			target: parsePreviewRecoveryTarget(
				recovery.target,
				`${itemPath}.target`
			)
			});
		});
		assertStrictOrder(
			recoveries,
			(left, right) =>
				compareTuple(
					[
						previewRecoveryTargetKey(left.target),
						left.occurrence_ordinal
					],
					[
						previewRecoveryTargetKey(right.target),
						right.occurrence_ordinal
					]
				),
			`${path}.preview.recoveries`
		);
		assertUnique(
			recoveries.map(({ target }) =>
				JSON.stringify(previewRecoveryTargetKey(target))
			),
			`${path}.preview.recoveries`
		);
		for (const recovery of recoveries) {
			if (
				recovery.condition === 'if_record_missing' &&
				(recovery.target.kind !== 'record' ||
					!operations.some(
						({ mutation }) =>
							mutation.op === 'patch' &&
							compareTuple(
								previewScopeKey(mutation.scope),
								previewScopeKey(recovery.target.kind === 'record'
									? recovery.target.scope
									: mutation.scope)
							) === 0
					))
			) {
				invalid(`${path}.preview.recoveries`);
			}
		}
			const pureReduces = parsePureReduces(
				projection.pureReduces,
				occurrences.length,
				projections.length,
				`${path}.pureReduces`
			);
			const parsed = Object.freeze({
			version: 2 as const,
			deltaWireVersion: 1 as const,
			projectionProgramVersion: 2 as const,
		operationSemanticsVersion: 1 as const,
			projections: Object.freeze(projections),
			eventSet: Object.freeze(eventSet),
			capabilities: Object.freeze({
				version: 1 as const,
				arms: Object.freeze(capabilityArms)
			}),
			preview: Object.freeze({
			version: 1 as const,
			occurrences: Object.freeze(occurrences),
			operations: Object.freeze(operations),
			recoveries: Object.freeze(recoveries)
		}),
			...(pureReduces.length === 0 ? {} : { pureReduces }),
			fallback: 'revalidate' as const
			});
			if (encoder.encode(JSON.stringify(parsed)).byteLength > MAX_BODY_BYTES) {
				invalid(path);
			}
			return parsed;
	}

function parsePureReduces(
	value: unknown,
	occurrenceCount: number,
	projectionCount: number,
	path: string
): readonly import('./types.js').ProjectionPreviewPureReduce[] {
	if (value === undefined) return Object.freeze([]);
	return Object.freeze(
		boundedArray(value, path).map((item, index) => {
			const itemPath = `${path}[${index}]`;
			const reduce = exactRecord(
				item,
				[
					'fn',
					'scope',
					'args',
					'assign',
					'occurrence_ordinal',
					'projection_refs'
				],
				['clientModule', 'clientExport'],
				itemPath
			);
			const fn = reduce.fn;
			if (typeof fn !== 'string' || fn.length === 0 || fn.length > 128) {
				invalid(`${itemPath}.fn`);
			}
			const occurrenceOrdinal = boundedOrdinal(
				reduce.occurrence_ordinal,
				`${itemPath}.occurrence_ordinal`
			);
			if (occurrenceOrdinal >= occurrenceCount) invalid(itemPath);
			const assign = boundedArray(reduce.assign, `${itemPath}.assign`).map(
				(field, fieldIndex) => {
					if (typeof field !== 'string' || field.length === 0) {
						invalid(`${itemPath}.assign[${fieldIndex}]`);
					}
					return field as string;
				}
			);
			if (assign.length === 0) invalid(`${itemPath}.assign`);
			const args = boundedArray(reduce.args, `${itemPath}.args`).map(
				(arg, argIndex) => {
					const argPath = `${itemPath}.args[${argIndex}]`;
					const entry = exactRecord(arg, ['name', 'value'], [], argPath);
					if (typeof entry.name !== 'string' || entry.name.length === 0) {
						invalid(`${argPath}.name`);
					}
					return Object.freeze({
						name: entry.name as string,
						value: parsePreviewValue(entry.value, `${argPath}.value`, 0)
					});
				}
			);
			const clientModule =
				typeof reduce.clientModule === 'string'
					? (reduce.clientModule as string)
					: undefined;
			const clientExport =
				typeof reduce.clientExport === 'string'
					? (reduce.clientExport as string)
					: undefined;
			return Object.freeze({
				fn: fn as string,
				...(clientModule === undefined ? {} : { clientModule }),
				...(clientExport === undefined ? {} : { clientExport }),
				scope: parsePreviewScope(reduce.scope, `${itemPath}.scope`),
				args: Object.freeze(args),
				assign: Object.freeze(assign),
				occurrence_ordinal: occurrenceOrdinal,
				projection_refs: projectionRefs(
					reduce.projection_refs,
					projectionCount,
					`${itemPath}.projection_refs`
				)
			});
		})
	);
}

export function validateProjectionMetadataAuthority(
	metadata: CommandProjectionMetadata,
	contract: ReplicaCommandProjection,
	authority: Readonly<{
		surface: ProjectionDeltaSurface;
		schemaHash: string;
		protocolHash: string;
		authorizationGeneration: string;
		cacheScope: string;
		causationId: string;
	}>,
	nowUnixMs = Date.now()
): string {
	const identity = metadata.delta.identity;
	if (
		nowUnixMs >= metadata.expiresAtUnixMs ||
		identity.surface.kind !== authority.surface.kind ||
		identity.surface.name !== authority.surface.name ||
		(identity.surface.kind === 'application' &&
			(authority.surface.kind !== 'application' ||
				compareStringArrays(
					identity.surface.eligible_roles,
					authority.surface.eligible_roles
				) !== 0 ||
				compareStringArrays(
					identity.surface.schema_roles,
					authority.surface.schema_roles
				) !== 0)) ||
		identity.schema_fingerprint !== authority.schemaHash ||
		identity.protocol_fingerprint !== authority.protocolHash ||
		identity.authorization_generation !== authority.authorizationGeneration ||
		identity.cache_scope_token !== authority.cacheScope ||
		identity.command_causation_id !== authority.causationId
	) {
		invalid('projection.delta.identity');
	}
	if (metadata.delta.projections.length !== contract.projections.length) {
		invalid('projection.delta.projections');
	}
	for (let index = 0; index < contract.projections.length; index += 1) {
		const expected = contract.projections[index]!;
		const actual = metadata.delta.projections[index]!;
		if (
			expected.programId !== actual.program_id ||
			expected.bindingId !== actual.binding_id ||
			expected.epoch !== actual.epoch ||
			expected.programIrVersion !== actual.program_ir_version ||
			expected.operationSemanticsVersion !==
				actual.operation_semantics_version
		) {
			invalid(`projection.delta.projections[${index}]`);
		}
	}
	return canonicalCommandProjectionMetadata(metadata);
}

function parseProjectionIdentity(
	value: unknown,
	path: string
): ProjectionDeltaProjectionIdentity {
	const identity = exactRecord(
		value,
		[
			'program_id',
			'binding_id',
			'epoch',
			'program_ir_version',
			'operation_semantics_version'
		],
		[],
		path
	);
	if (
		typeof identity.program_id !== 'string' ||
		!PROGRAM_ID.test(identity.program_id) ||
		typeof identity.binding_id !== 'string' ||
		!BINDING_ID.test(identity.binding_id) ||
		identity.program_ir_version !== PROJECTION_PROGRAM_IR_VERSION ||
		identity.operation_semantics_version !==
			PROJECTION_OPERATION_SEMANTICS_VERSION
	) {
		invalid(path);
	}
	return Object.freeze({
		program_id: identity.program_id,
		binding_id: identity.binding_id,
		epoch: projectionEpoch(identity.epoch, `${path}.epoch`),
		program_ir_version: 1 as const,
		operation_semantics_version: 1 as const
	});
}

function parseSurface(value: unknown, path: string): ProjectionDeltaSurface {
	if (!isPlainRecord(value)) invalid(path);
	if (value.kind === 'role') {
		const role = exactRecord(value, ['kind', 'name'], [], path);
		return Object.freeze({
			kind: 'role' as const,
			name: identityString(role.name, `${path}.name`)
		});
	}
	if (value.kind === 'application') {
		const application = exactRecord(
			value,
			['kind', 'name', 'eligible_roles', 'schema_roles'],
			[],
			path
		);
		const eligibleRoles = boundedArray(
			application.eligible_roles,
			`${path}.eligible_roles`
		).map(
			(role, index) =>
				identityString(role, `${path}.eligible_roles[${index}]`)
		);
		const schemaRoles = boundedArray(
			application.schema_roles,
			`${path}.schema_roles`
		).map((role, index) =>
			identityString(role, `${path}.schema_roles[${index}]`)
		);
		if (eligibleRoles.length === 0) invalid(`${path}.eligible_roles`);
		if (schemaRoles.length === 0) invalid(`${path}.schema_roles`);
		assertStrictOrder(
			eligibleRoles,
			compareUtf8,
			`${path}.eligible_roles`
		);
		assertStrictOrder(schemaRoles, compareUtf8, `${path}.schema_roles`);
		if (schemaRoles.some((role) => !eligibleRoles.includes(role))) {
			invalid(`${path}.schema_roles`);
		}
		return Object.freeze({
			kind: 'application' as const,
			name: identityString(application.name, `${path}.name`),
			eligible_roles: Object.freeze(eligibleRoles),
			schema_roles: Object.freeze(schemaRoles)
		});
	}
	invalid(`${path}.kind`);
}

function parseMutation(value: unknown, path: string): ProjectionDeltaMutation {
	if (!isPlainRecord(value) || typeof value.op !== 'string') {
		invalid(path);
	}
	switch (value.op) {
		case 'upsert': {
			const mutation = exactRecord(
				value,
				['op', 'scope', 'fields', 'replace'],
				[],
				path
			);
			const fields = parseFields(mutation.fields, `${path}.fields`);
			const replace = sortedNames(mutation.replace, `${path}.replace`);
			if (fields.some(({ field }) => !replace.includes(field))) invalid(path);
			return Object.freeze({
				op: 'upsert',
				scope: parseScope(mutation.scope, `${path}.scope`),
				fields,
				replace
			});
		}
		case 'patch': {
			const mutation = exactRecord(
				value,
				['op', 'scope', 'if_present'],
				['set', 'unset'],
				path
			);
			if (mutation.if_present !== true) invalid(`${path}.if_present`);
			const set = parseFields(mutation.set ?? [], `${path}.set`);
			const unset = sortedNames(mutation.unset ?? [], `${path}.unset`);
			if (
				(set.length === 0 && unset.length === 0) ||
				set.some(({ field }) => unset.includes(field))
			) {
				invalid(path);
			}
			return Object.freeze({
				op: 'patch',
				scope: parseScope(mutation.scope, `${path}.scope`),
				set,
				unset,
				if_present: true as const
			});
		}
		case 'delete': {
			const mutation = exactRecord(value, ['op', 'scope'], [], path);
			return Object.freeze({
				op: 'delete',
				scope: parseScope(mutation.scope, `${path}.scope`)
			});
		}
		case 'link':
		case 'unlink': {
			const mutation = exactRecord(
				value,
				['op', 'relationship', 'source', 'target'],
				[],
				path
			);
			return Object.freeze({
				op: value.op,
				relationship: identityString(
					mutation.relationship,
					`${path}.relationship`
				),
				source: parseScope(mutation.source, `${path}.source`),
				target: parseScope(mutation.target, `${path}.target`)
			});
		}
		case 'invalidate_model': {
			const mutation = exactRecord(
				value,
				['op', 'model'],
				['partition'],
				path
			);
			return Object.freeze({
				op: 'invalidate_model',
				...(mutation.partition === undefined
					? {}
					: {
							partition: parsePartition(
								mutation.partition,
								`${path}.partition`
							)
						}),
				model: projectionModel(mutation.model, `${path}.model`)
			});
		}
		case 'invalidate_relationship': {
			const mutation = exactRecord(
				value,
				['op', 'relationship', 'source'],
				[],
				path
			);
			return Object.freeze({
				op: 'invalidate_relationship',
				relationship: identityString(
					mutation.relationship,
					`${path}.relationship`
				),
				source: parseScope(mutation.source, `${path}.source`)
			});
		}
		default:
			invalid(`${path}.op`);
	}
}

function parseScope(value: unknown, path: string): ProjectionDeltaScope {
	const scope = exactRecord(value, ['partition', 'model', 'key'], [], path);
	const key = boundedArray(scope.key, `${path}.key`).map((item, index) => {
		const itemPath = `${path}.key[${index}]`;
		const field = exactRecord(
			item,
			['ordinal', 'field', 'value'],
			[],
			itemPath
		);
		const ordinal = boundedOrdinal(field.ordinal, `${itemPath}.ordinal`);
		const name = identityString(field.field, `${itemPath}.field`);
		const fieldValue = parseValue(field.value, `${itemPath}.value`, 1);
		if (
			ordinal !== index ||
			fieldValue.type === 'null' ||
			fieldValue.type === 'list' ||
			fieldValue.type === 'object'
		) {
			invalid(itemPath);
		}
		return Object.freeze({ ordinal, field: name, value: fieldValue });
	});
	if (key.length === 0) invalid(`${path}.key`);
	assertUnique(
		key.map(({ field }) => field),
		`${path}.key`
	);
	if (encoder.encode(JSON.stringify(key)).byteLength > MAX_KEY_BYTES) {
		invalid(`${path}.key`);
	}
	return Object.freeze({
		partition: parsePartition(scope.partition, `${path}.partition`),
		model: projectionModel(scope.model, `${path}.model`),
		key: Object.freeze(key)
	});
}

function parsePartition(
	value: unknown,
	path: string
): ProjectionDeltaPartition {
	if (!isPlainRecord(value)) invalid(path);
	let partition: ProjectionDeltaPartition;
	if (value.kind === 'unit') {
		exactRecord(value, ['kind'], [], path);
		partition = Object.freeze({ kind: 'unit' as const });
	} else if (value.kind === 'opaque') {
		const opaque = exactRecord(value, ['kind', 'token'], [], path);
		partition = Object.freeze({
			kind: 'opaque' as const,
			token: protocolToken(
				opaque.token,
				'projection-partition',
				`${path}.token`
			)
		});
	} else {
		invalid(`${path}.kind`);
	}
	if (encoder.encode(JSON.stringify(partition)).byteLength > MAX_PARTITION_BYTES) {
		invalid(path);
	}
	return partition;
}

function parseFields(value: unknown, path: string): readonly ProjectionDeltaField[] {
	const fields = boundedArray(value, path).map((item, index) => {
		const itemPath = `${path}[${index}]`;
		const field = exactRecord(item, ['field', 'value'], [], itemPath);
		return Object.freeze({
			field: identityString(field.field, `${itemPath}.field`),
			value: parseValue(field.value, `${itemPath}.value`, 1)
		});
	});
	assertStrictOrder(
		fields,
		(left, right) => compareUtf8(left.field, right.field),
		path
	);
	return Object.freeze(fields);
}

function parseValue(
	value: unknown,
	path: string,
	depth: number
): ProjectionDeltaValue {
	if (depth > MAX_DEPTH) invalid(path);
	if (!isPlainRecord(value) || typeof value.type !== 'string') invalid(path);
	switch (value.type) {
		case 'null':
			exactRecord(value, ['type'], [], path);
			return Object.freeze({ type: 'null' as const });
		case 'boolean': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			if (typeof typed.value !== 'boolean') invalid(`${path}.value`);
			return Object.freeze({ type: 'boolean' as const, value: typed.value });
		}
		case 'i64':
		case 'u64': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			const decimal = canonicalInteger(
				typed.value,
				value.type,
				`${path}.value`
			);
			return Object.freeze({ type: value.type, value: decimal });
		}
		case 'f64': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			const decimal = canonicalFloat(typed.value, `${path}.value`);
			return Object.freeze({ type: 'f64' as const, value: decimal });
		}
		case 'string': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			if (
				typeof typed.value !== 'string' ||
				encoder.encode(typed.value).byteLength > MAX_BODY_BYTES
			) {
				invalid(`${path}.value`);
			}
			return Object.freeze({ type: 'string' as const, value: typed.value });
		}
		case 'enum': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			const enumValue = exactRecord(
				typed.value,
				['enum_type', 'variant'],
				[],
				`${path}.value`
			);
			return Object.freeze({
				type: 'enum' as const,
				value: Object.freeze({
					enum_type: identityString(
						enumValue.enum_type,
						`${path}.value.enum_type`
					),
					variant: identityString(
						enumValue.variant,
						`${path}.value.variant`
					)
				})
			});
		}
		case 'list': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			const values = boundedArray(typed.value, `${path}.value`).map(
				(item, index) =>
					parseValue(item, `${path}.value[${index}]`, depth + 1)
			);
			return Object.freeze({
				type: 'list' as const,
				value: Object.freeze(values)
			});
		}
		case 'object': {
			const typed = exactRecord(value, ['type', 'value'], [], path);
			const fields = boundedArray(typed.value, `${path}.value`).map(
				(item, index) => {
					const itemPath = `${path}.value[${index}]`;
					const field = exactRecord(
						item,
						['field', 'value'],
						[],
						itemPath
					);
					return Object.freeze({
						field: identityString(field.field, `${itemPath}.field`),
						value: parseValue(
							field.value,
							`${itemPath}.value`,
							depth + 1
						)
					});
				}
			);
			assertStrictOrder(
				fields,
				(left, right) => compareUtf8(left.field, right.field),
				`${path}.value`
			);
			return Object.freeze({
				type: 'object' as const,
				value: Object.freeze(fields)
			});
		}
		default:
			invalid(`${path}.type`);
	}
}

function parseRecovery(
	value: unknown,
	index: number,
	projectionCount: number,
	occurrenceCount: number
): ProjectionDeltaRecovery {
	const path = `projection.delta.recoveries[${index}]`;
	const recovery = exactRecord(
		value,
		['occurrence_ordinal', 'projection_refs', 'condition', 'target'],
		[],
		path
	);
	const occurrenceOrdinal = boundedOrdinal(
		recovery.occurrence_ordinal,
		`${path}.occurrence_ordinal`
	);
	if (
		occurrenceOrdinal >= occurrenceCount ||
		(recovery.condition !== 'always' &&
			recovery.condition !== 'if_record_missing')
	) {
		invalid(path);
	}
	return Object.freeze({
		occurrence_ordinal: occurrenceOrdinal,
		projection_refs: projectionRefs(
			recovery.projection_refs,
			projectionCount,
			`${path}.projection_refs`
		),
		condition: recovery.condition,
		target: parseRecoveryTarget(recovery.target, `${path}.target`)
	});
}

function parseRecoveryTarget(
	value: unknown,
	path: string
): ProjectionDeltaRecoveryTarget {
	if (!isPlainRecord(value) || typeof value.kind !== 'string') invalid(path);
	switch (value.kind) {
		case 'record': {
			const target = exactRecord(value, ['kind', 'scope'], [], path);
			return Object.freeze({
				kind: 'record' as const,
				scope: parseScope(target.scope, `${path}.scope`)
			});
		}
		case 'relationship': {
			const target = exactRecord(
				value,
				['kind', 'relationship', 'source'],
				[],
				path
			);
			return Object.freeze({
				kind: 'relationship' as const,
				relationship: identityString(
					target.relationship,
					`${path}.relationship`
				),
				source: parseScope(target.source, `${path}.source`)
			});
		}
		case 'model': {
			const target = exactRecord(value, ['kind', 'model'], ['partition'], path);
			return Object.freeze({
				kind: 'model' as const,
				...(target.partition === undefined
					? {}
					: {
							partition: parsePartition(
								target.partition,
								`${path}.partition`
							)
						}),
				model: projectionModel(target.model, `${path}.model`)
			});
		}
		default:
			invalid(`${path}.kind`);
	}
}

function parseObligation(
	value: unknown,
	index: number,
	delta: ProjectionDelta
): CommandProjectionObligation {
	const path = `projection.obligations[${index}]`;
	const obligation = exactRecord(
		value,
		['projectionRef', 'model', 'scopeToken'],
		[],
		path
	);
	const projectionRef = boundedOrdinal(
		obligation.projectionRef,
		`${path}.projectionRef`
	);
	const model = obligationModel(obligation.model, `${path}.model`);
	if (
		projectionRef >= delta.projections.length ||
		!delta.operations.some(
			({ projection_refs, mutation }) =>
				projection_refs.includes(projectionRef) &&
				operationCanObserveModel(mutation, model)
		)
	) {
		invalid(path);
	}
	return Object.freeze({
		projectionRef,
		model,
		scopeToken: protocolToken(
			obligation.scopeToken,
			'projection-obligation',
			`${path}.scopeToken`
		)
	});
}

function operationCanObserveModel(
	mutation: ProjectionDeltaMutation,
	model: string
): boolean {
	switch (mutation.op) {
		case 'upsert':
		case 'patch':
		case 'delete':
			return mutation.scope.model === model;
			case 'link':
			case 'unlink':
				return true;
			case 'invalidate_model':
			case 'invalidate_relationship':
				return false;
		}
	}

function parseCapabilityArm(
	value: unknown,
	_index: number,
	projectionCount: number,
	eventSet: readonly Readonly<{ id: string; name: string; version: number }>[],
	path: string
): ProjectionCapabilityArm {
	const arm = exactRecord(
		value,
		['event', 'projection_ref', 'arm', 'partition', 'mutations'],
		[],
		path
	);
	const event = parseEventRef(arm.event, `${path}.event`);
	const projectionRef = boundedOrdinal(
		arm.projection_ref,
		`${path}.projection_ref`
	);
	if (
		projectionRef >= projectionCount ||
		!eventSet.some((candidate) => sameEventRef(candidate, event))
	) {
		invalid(path);
	}
	const mutations = capabilityArray(arm.mutations, `${path}.mutations`).map(
		(item, index) =>
		parseCapabilityMutation(item, `${path}.mutations[${index}]`)
	);
	if (mutations.length === 0) invalid(`${path}.mutations`);
	assertStrictOrder(
		mutations,
		(left, right) =>
			compareTuple(
				capabilityMutationKey(left),
				capabilityMutationKey(right)
			),
		`${path}.mutations`
	);
	return Object.freeze({
		event,
		projection_ref: projectionRef,
		arm: identityString(arm.arm, `${path}.arm`),
		partition: parseCapabilityPartition(arm.partition, `${path}.partition`),
		mutations: Object.freeze(mutations)
	});
}

function parseCapabilityPartition(
	value: unknown,
	path: string
): ProjectionCapabilityPartition {
	if (!isPlainRecord(value) || typeof value.kind !== 'string') invalid(path);
	if (value.kind === 'unit') {
		exactRecord(value, ['kind'], [], path);
		return Object.freeze({ kind: 'unit' as const });
	}
	if (value.kind === 'opaque') {
		const partition = exactRecord(
			value,
			['kind', 'expression_fingerprint'],
			[],
			path
		);
		if (
			typeof partition.expression_fingerprint !== 'string' ||
			!SHA256.test(partition.expression_fingerprint)
		) {
			invalid(`${path}.expression_fingerprint`);
		}
		return Object.freeze({
			kind: 'opaque' as const,
			expression_fingerprint: partition.expression_fingerprint
		});
	}
	invalid(`${path}.kind`);
}

function parseCapabilityMutation(
	value: unknown,
	path: string
): ProjectionCapabilityMutation {
	if (!isPlainRecord(value) || typeof value.kind !== 'string') invalid(path);
	switch (value.kind) {
		case 'record': {
			const mutation = exactRecord(
				value,
				[
					'kind',
					'model',
					'key',
					'fields',
					'replace',
					'upsert',
					'patch',
					'delete'
				],
				[],
				path
			);
			if (
				typeof mutation.upsert !== 'boolean' ||
				typeof mutation.patch !== 'boolean' ||
				typeof mutation.delete !== 'boolean'
			) {
				invalid(path);
			}
			const key = uniqueNames(mutation.key, `${path}.key`);
			const fields = sortedNames(mutation.fields, `${path}.fields`);
			const replace = sortedNames(mutation.replace, `${path}.replace`);
			if (
				key.length === 0 ||
				(mutation.patch && fields.length === 0)
			) {
				invalid(path);
			}
			return Object.freeze({
				kind: 'record' as const,
				model: projectionModel(mutation.model, `${path}.model`),
				key,
				fields,
				replace,
				upsert: mutation.upsert,
				patch: mutation.patch,
				delete: mutation.delete
			});
		}
		case 'relationship': {
			const mutation = exactRecord(
				value,
				[
					'kind',
					'relationship',
					'source_model',
					'source_key',
					'target_model',
					'target_key',
					'link',
					'unlink'
				],
				[],
				path
			);
			if (
				typeof mutation.link !== 'boolean' ||
				typeof mutation.unlink !== 'boolean'
			) {
				invalid(path);
			}
			const sourceKey = uniqueNames(
				mutation.source_key,
				`${path}.source_key`
			);
			const targetKey = uniqueNames(
				mutation.target_key,
				`${path}.target_key`
			);
			if (sourceKey.length === 0 || targetKey.length === 0) invalid(path);
			return Object.freeze({
				kind: 'relationship' as const,
				relationship: identityString(
					mutation.relationship,
					`${path}.relationship`
				),
				source_model: projectionModel(
					mutation.source_model,
					`${path}.source_model`
				),
				source_key: sourceKey,
				target_model: projectionModel(
					mutation.target_model,
					`${path}.target_model`
				),
				target_key: targetKey,
				link: mutation.link,
				unlink: mutation.unlink
			});
		}
		case 'model': {
			const mutation = exactRecord(value, ['kind', 'model'], [], path);
			return Object.freeze({
				kind: 'model' as const,
				model: projectionModel(mutation.model, `${path}.model`)
			});
		}
		default:
			invalid(`${path}.kind`);
	}
}

function capabilityMutationKey(
	mutation: ProjectionCapabilityMutation
): readonly unknown[] {
	switch (mutation.kind) {
		case 'record':
			return [
				0,
				mutation.model,
				mutation.key,
				mutation.fields,
				mutation.replace,
				mutation.upsert,
				mutation.patch,
				mutation.delete
			];
		case 'relationship':
			return [
				1,
				mutation.relationship,
				mutation.source_model,
				mutation.source_key,
				mutation.target_model,
				mutation.target_key,
				mutation.link,
				mutation.unlink
			];
		case 'model':
			return [2, mutation.model];
	}
}

function sameEventRef(
	left: Readonly<{ id: string; name: string; version: number }>,
	right: Readonly<{ id: string; name: string; version: number }>
): boolean {
	return (
		left.id === right.id &&
		left.name === right.name &&
		left.version === right.version
	);
}

function parsePreviewMutation(
	value: unknown,
	path: string
): ProjectionPreviewMutation {
	if (!isPlainRecord(value) || typeof value.op !== 'string') invalid(path);
	switch (value.op) {
		case 'upsert': {
			const mutation = exactRecord(
				value,
				['op', 'scope', 'fields', 'replace'],
				[],
				path
			);
			const fields = parsePreviewFields(mutation.fields, `${path}.fields`);
			const replace = sortedNames(mutation.replace, `${path}.replace`);
			if (fields.some(({ field }) => !replace.includes(field))) invalid(path);
			return Object.freeze({
				op: 'upsert',
				scope: parsePreviewScope(mutation.scope, `${path}.scope`),
				fields,
				replace
			});
		}
		case 'patch': {
			const mutation = exactRecord(
				value,
				['op', 'scope', 'if_present'],
				['set', 'unset'],
				path
			);
			if (mutation.if_present !== true) invalid(`${path}.if_present`);
			const set = parsePreviewFields(mutation.set ?? [], `${path}.set`);
			const unset = sortedNames(mutation.unset ?? [], `${path}.unset`);
			if (
				(set.length === 0 && unset.length === 0) ||
				set.some(({ field }) => unset.includes(field))
			) {
				invalid(path);
			}
			return Object.freeze({
				op: 'patch',
				scope: parsePreviewScope(mutation.scope, `${path}.scope`),
				set,
				unset,
				if_present: true as const
			});
		}
		case 'delete': {
			const mutation = exactRecord(value, ['op', 'scope'], [], path);
			return Object.freeze({
				op: 'delete',
				scope: parsePreviewScope(mutation.scope, `${path}.scope`)
			});
		}
		case 'link':
		case 'unlink': {
			const mutation = exactRecord(
				value,
				['op', 'relationship', 'source', 'target'],
				[],
				path
			);
			return Object.freeze({
				op: value.op,
				relationship: identityString(
					mutation.relationship,
					`${path}.relationship`
				),
				source: parsePreviewScope(mutation.source, `${path}.source`),
				target: parsePreviewScope(mutation.target, `${path}.target`)
			});
		}
		case 'invalidate_model': {
			const mutation = exactRecord(
				value,
				['op', 'model'],
				['partition'],
				path
			);
			return Object.freeze({
				op: 'invalidate_model',
				...(mutation.partition === undefined
					? {}
					: {
							partition: parsePreviewPartition(
								mutation.partition,
								`${path}.partition`
							)
						}),
				model: projectionModel(mutation.model, `${path}.model`)
			});
		}
		case 'invalidate_relationship': {
			const mutation = exactRecord(
				value,
				['op', 'relationship', 'source'],
				[],
				path
			);
			return Object.freeze({
				op: 'invalidate_relationship',
				relationship: identityString(
					mutation.relationship,
					`${path}.relationship`
				),
				source: parsePreviewScope(mutation.source, `${path}.source`)
			});
		}
		default:
			invalid(`${path}.op`);
	}
}

function parsePreviewScope(
	value: unknown,
	path: string
): ProjectionPreviewScope {
	const scope = exactRecord(value, ['partition', 'model', 'key'], [], path);
	const key = boundedArray(scope.key, `${path}.key`).map((item, index) => {
		const itemPath = `${path}.key[${index}]`;
		const field = exactRecord(
			item,
			['ordinal', 'field', 'value'],
			[],
			itemPath
		);
		const ordinal = boundedOrdinal(field.ordinal, `${itemPath}.ordinal`);
		if (ordinal !== index) invalid(`${itemPath}.ordinal`);
		return Object.freeze({
			ordinal,
			field: identityString(field.field, `${itemPath}.field`),
			value: parsePreviewValue(field.value, `${itemPath}.value`, 1)
		});
	});
	if (key.length === 0) invalid(`${path}.key`);
	assertUnique(
		key.map(({ field }) => field),
		`${path}.key`
	);
	if (encoder.encode(JSON.stringify(key)).byteLength > MAX_KEY_BYTES) {
		invalid(`${path}.key`);
	}
	const partition = parsePreviewPartition(
		scope.partition,
		`${path}.partition`
	);
	return Object.freeze({
		partition,
		model: projectionModel(scope.model, `${path}.model`),
		key: Object.freeze(key)
	});
}

function parsePreviewPartition(
	value: unknown,
	path: string
): ProjectionPreviewPartition {
	if (!isPlainRecord(value) || typeof value.kind !== 'string') invalid(path);
	if (value.kind === 'unit') {
		exactRecord(value, ['kind'], [], path);
		const parsed = Object.freeze({ kind: 'unit' as const });
		if (encoder.encode(JSON.stringify(parsed)).byteLength > MAX_PARTITION_BYTES) {
			invalid(path);
		}
		return parsed;
	}
	if (value.kind === 'expression') {
		const partition = exactRecord(
			value,
			['kind', 'expression', 'requires'],
			[],
			path
		);
		if (partition.requires !== 'current_cache_partition') {
			invalid(`${path}.requires`);
		}
		const parsed = Object.freeze({
			kind: 'expression' as const,
			expression: parsePreviewValue(
				partition.expression,
				`${path}.expression`,
				1
			),
			requires: 'current_cache_partition' as const
		});
		if (encoder.encode(JSON.stringify(parsed)).byteLength > MAX_PARTITION_BYTES) {
			invalid(path);
		}
		return parsed;
	}
	invalid(`${path}.kind`);
}

function parsePreviewValue(
	value: unknown,
	path: string,
	depth: number
): ProjectionPreviewValue {
	if (depth > MAX_DEPTH || !isPlainRecord(value) || typeof value.kind !== 'string') {
		invalid(path);
	}
	switch (value.kind) {
		case 'input':
		case 'generated_default': {
			const expression = exactRecord(value, ['kind', 'path'], [], path);
			return Object.freeze({
				kind: value.kind,
				path: stringPath(expression.path, `${path}.path`)
			});
		}
		case 'trusted_preset': {
			const expression = exactRecord(
				value,
				['kind', 'name', 'codec'],
				[],
				path
			);
			return Object.freeze({
				kind: 'trusted_preset' as const,
				name: identityString(expression.name, `${path}.name`),
				codec: identityString(expression.codec, `${path}.codec`)
			});
		}
		case 'constant': {
			const expression = exactRecord(value, ['kind', 'value'], [], path);
			return Object.freeze({
				kind: 'constant' as const,
				value: parseValue(expression.value, `${path}.value`, depth)
			});
		}
		case 'null':
			exactRecord(value, ['kind'], [], path);
			return Object.freeze({ kind: 'null' as const });
		case 'list': {
			const expression = exactRecord(value, ['kind', 'values'], [], path);
			return Object.freeze({
				kind: 'list' as const,
				values: Object.freeze(
					boundedArray(expression.values, `${path}.values`).map(
						(item, index) =>
							parsePreviewValue(
								item,
								`${path}.values[${index}]`,
								depth + 1
							)
					)
				)
			});
		}
		case 'object': {
			const expression = exactRecord(value, ['kind', 'fields'], [], path);
			const fields = boundedArray(
				expression.fields,
				`${path}.fields`
			).map((item, index) => {
				const itemPath = `${path}.fields[${index}]`;
				const field = exactRecord(item, ['name', 'value'], [], itemPath);
				return Object.freeze({
					name: identityString(field.name, `${itemPath}.name`),
					value: parsePreviewValue(
						field.value,
						`${itemPath}.value`,
						depth + 1
					)
				});
			});
			assertStrictOrder(
				fields,
				(left, right) => compareUtf8(left.name, right.name),
				`${path}.fields`
			);
			return Object.freeze({
				kind: 'object' as const,
				fields: Object.freeze(fields)
			});
		}
		case 'transform': {
			const expression = exactRecord(
				value,
				['kind', 'transform', 'arguments'],
				[],
				path
			);
			if (
				expression.transform !== 'string_concat' &&
				expression.transform !== 'first_present'
			) {
				invalid(`${path}.transform`);
			}
			const args = boundedArray(
				expression.arguments,
				`${path}.arguments`
			).map((item, index) =>
				parsePreviewValue(
					item,
					`${path}.arguments[${index}]`,
					depth + 1
				)
			);
			if (args.length === 0) invalid(`${path}.arguments`);
			return Object.freeze({
				kind: 'transform' as const,
				transform: expression.transform,
				arguments: Object.freeze(args)
			});
		}
		default:
			invalid(`${path}.kind`);
	}
}

function parsePreviewFields(
	value: unknown,
	path: string
): readonly Readonly<{ field: string; value: ProjectionPreviewValue }>[] {
	const fields = boundedArray(value, path).map((item, index) => {
		const itemPath = `${path}[${index}]`;
		const field = exactRecord(item, ['field', 'value'], [], itemPath);
		return Object.freeze({
			field: identityString(field.field, `${itemPath}.field`),
			value: parsePreviewValue(field.value, `${itemPath}.value`, 1)
		});
	});
	assertStrictOrder(
		fields,
		(left, right) => compareUtf8(left.field, right.field),
		path
	);
	return Object.freeze(fields);
}

function parsePreviewRecoveryTarget(
	value: unknown,
	path: string
): ProjectionPreviewRecoveryTarget {
	if (!isPlainRecord(value) || typeof value.kind !== 'string') invalid(path);
	if (value.kind === 'record') {
		const target = exactRecord(value, ['kind', 'scope'], [], path);
		return Object.freeze({
			kind: 'record' as const,
			scope: parsePreviewScope(target.scope, `${path}.scope`)
		});
	}
	if (value.kind === 'relationship') {
		const target = exactRecord(
			value,
			['kind', 'relationship', 'source'],
			[],
			path
		);
		return Object.freeze({
			kind: 'relationship' as const,
			relationship: identityString(
				target.relationship,
				`${path}.relationship`
			),
			source: parsePreviewScope(target.source, `${path}.source`)
		});
	}
	if (value.kind === 'model') {
		const target = exactRecord(value, ['kind', 'model'], ['partition'], path);
		return Object.freeze({
			kind: 'model' as const,
			...(target.partition === undefined
				? {}
				: {
						partition: parsePreviewPartition(
							target.partition,
							`${path}.partition`
						)
					}),
			model: projectionModel(target.model, `${path}.model`)
		});
	}
	invalid(`${path}.kind`);
}

function parseEventRef(
	value: unknown,
	path: string
): Readonly<{ id: string; name: string; version: number }> {
	const event = exactRecord(value, ['id', 'name', 'version'], [], path);
	const version = boundedOrdinal(event.version, `${path}.version`);
	if (version === 0) invalid(`${path}.version`);
	return Object.freeze({
		id: identityString(event.id, `${path}.id`),
		name: identityString(event.name, `${path}.name`),
		version
	});
}

function projectionRefs(
	value: unknown,
	projectionCount: number,
	path: string
): readonly number[] {
	const refs = boundedArray(value, path).map((item, index) =>
		boundedOrdinal(item, `${path}[${index}]`)
	);
	if (
		refs.length === 0 ||
		refs.some((reference) => reference >= projectionCount)
	) {
		invalid(path);
	}
	assertStrictOrder(refs, (left, right) => left - right, path);
	return Object.freeze(refs);
}

function previewOperationScopeKey(
	mutation: ProjectionPreviewMutation
): readonly unknown[] {
	switch (mutation.op) {
		case 'upsert':
		case 'patch':
		case 'delete':
			return [0, previewScopeKey(mutation.scope)];
		case 'link':
		case 'unlink':
			return [
				1,
				mutation.relationship,
				previewScopeKey(mutation.source),
				previewScopeKey(mutation.target)
			];
		case 'invalidate_model':
			return [
				2,
				mutation.partition === undefined
					? [0]
					: [1, previewPartitionKey(mutation.partition)],
				mutation.model
			];
		case 'invalidate_relationship':
			return [3, mutation.relationship, previewScopeKey(mutation.source)];
	}
}

function previewRecoveryTargetKey(
	target: ProjectionPreviewRecoveryTarget
): readonly unknown[] {
	switch (target.kind) {
		case 'record':
			return [0, previewScopeKey(target.scope)];
		case 'relationship':
			return [1, target.relationship, previewScopeKey(target.source)];
		case 'model':
			return [
				2,
				target.partition === undefined
					? [0]
					: [1, previewPartitionKey(target.partition)],
				target.model
			];
	}
}

function previewScopeKey(scope: ProjectionPreviewScope): readonly unknown[] {
	return [
		previewPartitionKey(scope.partition),
		scope.model,
		scope.key.map((field) => [
			field.ordinal,
			field.field,
			JSON.stringify(field.value)
		])
	];
}

function previewPartitionKey(
	partition: ProjectionPreviewPartition
): readonly unknown[] {
	return partition.kind === 'unit'
		? [0]
		: [1, JSON.stringify(partition.expression), partition.requires];
}

function operationScopeKey(mutation: ProjectionDeltaMutation): readonly unknown[] {
	switch (mutation.op) {
		case 'upsert':
		case 'patch':
		case 'delete':
			return [0, scopeKey(mutation.scope)];
		case 'link':
		case 'unlink':
			return [
				1,
				mutation.relationship,
				scopeKey(mutation.source),
				scopeKey(mutation.target)
			];
		case 'invalidate_model':
			return [
				2,
				mutation.partition === undefined
					? [0]
					: [1, partitionKey(mutation.partition)],
				mutation.model
			];
		case 'invalidate_relationship':
			return [3, mutation.relationship, scopeKey(mutation.source)];
	}
}

function recoveryTargetKey(
	target: ProjectionDeltaRecoveryTarget
): readonly unknown[] {
	switch (target.kind) {
		case 'record':
			return [0, scopeKey(target.scope)];
		case 'relationship':
			return [1, target.relationship, scopeKey(target.source)];
		case 'model':
			return [
				2,
				target.partition === undefined
					? [0]
					: [1, partitionKey(target.partition)],
				target.model
			];
	}
}

function scopeKey(scope: ProjectionDeltaScope): readonly unknown[] {
	return [
		partitionKey(scope.partition),
		scope.model,
		scope.key.map((field) => [
			field.ordinal,
			field.field,
			valueKey(field.value)
		])
	];
}

function partitionKey(
	partition: ProjectionDeltaPartition
): readonly unknown[] {
	return partition.kind === 'unit' ? [0] : [1, partition.token];
}

function valueKey(value: ProjectionDeltaValue): readonly unknown[] {
	switch (value.type) {
		case 'null':
			return [0];
		case 'boolean':
			return [1, value.value];
		case 'i64':
			return [2, value.value];
		case 'u64':
			return [3, value.value];
		case 'f64':
			return [4, value.value];
		case 'string':
			return [5, value.value];
		case 'enum':
			return [6, value.value.enum_type, value.value.variant];
		case 'list':
			return [7, value.value.map(valueKey)];
		case 'object':
			return [
				8,
				value.value.map((field) => [field.field, valueKey(field.value)])
			];
	}
}

function compareProjectionIdentity(
	left: ProjectionDeltaProjectionIdentity,
	right: ProjectionDeltaProjectionIdentity
): number {
	return compareTuple(
		[
			left.program_id,
			left.binding_id,
			left.epoch,
			left.program_ir_version,
			left.operation_semantics_version
		],
		[
			right.program_id,
			right.binding_id,
			right.epoch,
			right.program_ir_version,
			right.operation_semantics_version
		]
	);
}

function compareTuple(left: readonly unknown[], right: readonly unknown[]): number {
	for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
		const compared = compareValue(left[index], right[index]);
		if (compared !== 0) return compared;
	}
	return left.length - right.length;
}

function compareValue(left: unknown, right: unknown): number {
	if (Array.isArray(left) && Array.isArray(right)) return compareTuple(left, right);
	if (typeof left === 'string' && typeof right === 'string') {
		return compareUtf8(left, right);
	}
	if (typeof left === 'number' && typeof right === 'number') return left - right;
	if (typeof left === 'boolean' && typeof right === 'boolean') {
		return Number(left) - Number(right);
	}
	return compareUtf8(JSON.stringify(left), JSON.stringify(right));
}

function compareStringArrays(
	left: readonly string[],
	right: readonly string[]
): number {
	return compareTuple(left, right);
}

/** Rust `String::cmp` is lexicographic over UTF-8 bytes, not UTF-16 units. */
function exactRecord(
	value: unknown,
	required: readonly string[],
	optional: readonly string[],
	path: string
): Record<string, unknown> {
	if (!isPlainRecord(value)) invalid(path);
	const keys = Reflect.ownKeys(value);
	if (keys.some((key) => typeof key !== 'string')) invalid(path);
	const allowed = new Set([...required, ...optional]);
	for (const key of keys as string[]) {
		if (!allowed.has(key)) invalid(`${path}.${key}`);
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !('value' in descriptor)) {
			invalid(`${path}.${key}`);
		}
	}
	for (const key of required) {
		if (!Object.prototype.hasOwnProperty.call(value, key)) {
			invalid(`${path}.${key}`);
		}
	}
	return value as Record<string, unknown>;
}

/**
 * Bound hostile already-decoded JSON before cloning/freezing/canonicalizing it.
 *
 * The byte counter is a lower bound (UTF-8 strings and property names only), so
 * it never rejects a wire body that could satisfy the exact final one-MiB
 * canonical check. The independent node/depth fences bound punctuation-heavy
 * or deeply nested inputs whose strings alone would stay below that byte limit.
 */
function preflightProjectionPayload(value: unknown, path: string): void {
	type Work =
		| Readonly<{ kind: 'visit'; value: unknown; depth: number }>
		| Readonly<{ kind: 'leave'; value: object }>;
	const active = new WeakSet<object>();
	const work: Work[] = [{ kind: 'visit', value, depth: 0 }];
	let scheduledVisits = 1;
	let stringBytes = 0;
	const reserveVisits = (count: number): void => {
		if (count > MAX_TRAVERSAL_NODES - scheduledVisits) invalid(path);
		scheduledVisits += count;
	};
	const consumeString = (candidate: string): void => {
		stringBytes += utf8ByteLength(candidate, MAX_BODY_BYTES - stringBytes);
		if (stringBytes > MAX_BODY_BYTES) invalid(path);
	};
	const enqueueVisit = (candidate: unknown, depth: number): void => {
		if (depth > MAX_TRAVERSAL_DEPTH) invalid(path);
		if (typeof candidate === 'string') {
			consumeString(candidate);
		} else if (
			candidate !== null &&
			(typeof candidate === 'object' || typeof candidate === 'function')
		) {
			work.push({ kind: 'visit', value: candidate, depth });
		}
	};
	while (work.length !== 0) {
		const item = work.pop()!;
		if (item.kind === 'leave') {
			active.delete(item.value);
			continue;
		}
		if (item.depth > MAX_TRAVERSAL_DEPTH) {
			invalid(path);
		}
		const candidate = item.value;
		if (typeof candidate === 'string') {
			consumeString(candidate);
			continue;
		}
		if (
			candidate === null ||
			(typeof candidate !== 'object' && typeof candidate !== 'function')
		) {
			continue;
		}
		if (active.has(candidate)) invalid(path);
		active.add(candidate);
		work.push({ kind: 'leave', value: candidate });
		if (Array.isArray(candidate)) {
			reserveVisits(candidate.length);
			for (let index = candidate.length - 1; index >= 0; index -= 1) {
				const descriptor = Object.getOwnPropertyDescriptor(candidate, index);
				if (descriptor === undefined || !('value' in descriptor)) invalid(path);
				enqueueVisit(descriptor.value, item.depth + 1);
			}
			continue;
		}
		const keys = Reflect.ownKeys(candidate);
		reserveVisits(keys.length);
		for (let index = keys.length - 1; index >= 0; index -= 1) {
			const key = keys[index]!;
			if (typeof key !== 'string') invalid(path);
			consumeString(key);
			const descriptor = Object.getOwnPropertyDescriptor(candidate, key);
			if (descriptor === undefined || !('value' in descriptor)) invalid(path);
			enqueueVisit(descriptor.value, item.depth + 1);
		}
	}
}

function utf8ByteLength(value: string, remaining: number): number {
	let bytes = 0;
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code <= 0x7f) bytes += 1;
		else if (code <= 0x7ff) bytes += 2;
		else if (
			code >= 0xd800 &&
			code <= 0xdbff &&
			index + 1 < value.length &&
			value.charCodeAt(index + 1) >= 0xdc00 &&
			value.charCodeAt(index + 1) <= 0xdfff
		) {
			bytes += 4;
			index += 1;
		} else {
			bytes += 3;
		}
		if (bytes > remaining) return bytes;
	}
	return bytes;
}

function boundedArray(value: unknown, path: string): readonly unknown[] {
	if (!Array.isArray(value) || value.length > MAX_ITEMS) invalid(path);
	return value;
}

function capabilityArray(value: unknown, path: string): readonly unknown[] {
	if (!Array.isArray(value) || value.length > MAX_TRAVERSAL_NODES) invalid(path);
	return value;
}

function boundedOrdinal(value: unknown, path: string): number {
	if (
		typeof value !== 'number' ||
		!Number.isSafeInteger(value) ||
		value < 0 ||
		value > 0xffff_ffff
	) {
		invalid(path);
	}
	return value;
}

function safeUnsignedInteger(value: unknown, path: string): number {
	if (
		typeof value !== 'number' ||
		!Number.isSafeInteger(value) ||
		value < 0
	) {
		invalid(path);
	}
	return value;
}

function identityString(value: unknown, path: string): string {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value.trim() !== value ||
		encoder.encode(value).byteLength > MAX_IDENTITY_BYTES
	) {
		invalid(path);
	}
	return value;
}

function projectionEpoch(value: unknown, path: string): string {
	const epoch = identityString(value, path);
	if (
		encoder.encode(epoch).byteLength > 128 ||
		[...epoch].some((character) => /\p{Cc}/u.test(character))
	) {
		invalid(path);
	}
	return epoch;
}

function projectionModel(value: unknown, path: string): string {
	const model = identityString(value, path);
	if (
		encoder.encode(model).byteLength > 128 ||
		[...model].some(
			(character) =>
				/\p{Cc}/u.test(character) || /\p{White_Space}/u.test(character)
		)
	) {
		invalid(path);
	}
	return model;
}

function obligationModel(value: unknown, path: string): string {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value.trim() !== value ||
		encoder.encode(value).byteLength > 255
	) {
		invalid(path);
	}
	return value;
}

function protocolToken(
	value: unknown,
	purpose: string,
	path: string
): string {
	const token = identityString(value, path);
	const prefix = `v1.${purpose}.`;
	const encoded = token.startsWith(prefix) ? token.slice(prefix.length) : '';
	if (!/^[A-Za-z0-9_-]{43}$/.test(encoded)) invalid(path);
	try {
		const standard = `${encoded.replace(/-/g, '+').replace(/_/g, '/')}=`;
		const decoded = atob(standard);
		if (decoded.length !== 32) invalid(path);
		const bytes = Uint8Array.from(decoded, (character) =>
			character.charCodeAt(0)
		);
		let canonical = '';
		for (const byte of bytes) canonical += String.fromCharCode(byte);
		const roundTrip = btoa(canonical)
			.replace(/\+/g, '-')
			.replace(/\//g, '_')
			.replace(/=+$/, '');
		if (roundTrip !== encoded) invalid(path);
	} catch {
		invalid(path);
	}
	return token;
}

function canonicalInteger(
	value: unknown,
	type: 'i64' | 'u64',
	path: string
): string {
	if (typeof value !== 'string' || !/^-?(?:0|[1-9][0-9]*)$/.test(value)) {
		invalid(path);
	}
	let parsed: bigint;
	try {
		parsed = BigInt(value);
	} catch {
		invalid(path);
	}
	if (
		parsed.toString() !== value ||
		(type === 'i64' && (parsed < I64_MIN || parsed > I64_MAX)) ||
		(type === 'u64' && (parsed < 0n || parsed > U64_MAX))
	) {
		invalid(path);
	}
	return value;
}

function canonicalFloat(value: unknown, path: string): string {
	if (typeof value !== 'string' || value.length === 0) invalid(path);
	const parsed = Number(value);
	if (!Number.isFinite(parsed)) invalid(path);
	const canonical = rustRyuFloat(parsed);
	if (canonical !== value) invalid(path);
	return value;
}

/**
 * ECMAScript and Rust Ryu choose the same shortest round-tripping digits but
 * use different fixed/scientific notation windows. serde_json's finite f64
 * representation is fixed for decimal exponents -5..=15 and scientific
 * outside that range; integral fixed values retain `.0`.
 */
function rustRyuFloat(value: number): string {
	if (Object.is(value, -0) || value === 0) return '0.0';
	const scientific = value.toExponential();
	const match = /^(-?)([0-9])(?:\.([0-9]+))?e([+-][0-9]+)$/.exec(scientific);
	if (match === null) throw new TypeError('invalid finite float');
	const sign = match[1]!;
	const digits = `${match[2]!}${match[3] ?? ''}`;
	const exponent = Number(match[4]);
	if (exponent < -5 || exponent > 15) {
		const fraction = digits.slice(1);
		return `${sign}${digits[0]!}${fraction.length === 0 ? '' : `.${fraction}`}e${
			exponent >= 0 ? '+' : ''
		}${exponent}`;
	}
	const point = exponent + 1;
	if (point <= 0) {
		return `${sign}0.${'0'.repeat(-point)}${digits}`;
	}
	if (point >= digits.length) {
		return `${sign}${digits}${'0'.repeat(point - digits.length)}.0`;
	}
	return `${sign}${digits.slice(0, point)}.${digits.slice(point)}`;
}

function sortedNames(value: unknown, path: string): readonly string[] {
	const names = boundedArray(value, path).map((item, index) =>
		identityString(item, `${path}[${index}]`)
	);
	assertStrictOrder(names, compareUtf8, path);
	return Object.freeze(names);
}

function uniqueNames(value: unknown, path: string): readonly string[] {
	const names = boundedArray(value, path).map((item, index) =>
		identityString(item, `${path}[${index}]`)
	);
	assertUnique(names, path);
	return Object.freeze(names);
}

function stringPath(value: unknown, path: string): readonly string[] {
	const values = boundedArray(value, path).map((item, index) =>
		identityString(item, `${path}[${index}]`)
	);
	if (values.length === 0) invalid(path);
	return Object.freeze(values);
}

function assertUnique(values: readonly string[], path: string): void {
	if (new Set(values).size !== values.length) invalid(path);
}

function assertStrictOrder<T>(
	values: readonly T[],
	compare: (left: T, right: T) => number,
	path: string
): void {
	for (let index = 1; index < values.length; index += 1) {
		if (compare(values[index - 1]!, values[index]!) >= 0) invalid(path);
	}
}

function invalid(path: string): never {
	throw new ProjectionDeltaValidationError(path);
}

export {
	canonicalCommandProjectionMetadata,
	canonicalProjectionDelta
};
