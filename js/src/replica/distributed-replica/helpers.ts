import {
	type BaseCacheWriter,
	type CacheEngineSnapshot,
	type CacheIndexCoverage,
	type CacheIndexMetadata,
	type CacheValue,
	type OptimisticLayerView
} from '../../internal/cache-engine.js';
import type { GqlError, GraphqlVariables } from '../../types.js';
import {
	isDistributedTrustedPresetCodec,
	type DistributedQuerySnapshot,
	type DistributedRecordRevision,
	type DistributedTrustedPreset
} from '../../protocol.js';
import type { ReplicaTrustedPresetDescriptor } from '../commands.js';
import type { ReplicaCommandSurfaceContract } from '../command-runtime.js';
import {
	canonicalVariables,
	cloneJsonValue,
	replicaIndexKey,
	replicaRecordKey,
	resolveArguments
} from '../identity.js';
import {
	type ReplicaIndexMaintenanceSnapshot,
	type ReplicaIndexSemanticChange,
	type ReplicaIndexSemanticLayer
} from '../index-maintenance.js';
import type { MaterializedReplicaResult } from '../materialize.js';
import { validateReplicaOperationBinding as validatedArtifactBinding } from '../operation-binding.js';
import {
	embeddedRecordKey,
	runtimeRoot,
	type RuntimeObjectBranch,
	type RuntimeObjectSelection,
	type RuntimeRootSelection
} from '../selection.js';
import type {
	ReplicaBaseWriter,
	ReplicaIdentity,
	ReplicaIndexTarget,
	ReplicaModelArtifact,
	ReplicaOperationArtifact,
	ReplicaRecordPatch,
	ReplicaResultEnvelope,
	ReplicaRevision,
	ReplicaSnapshot,
	ReplicaStatus,
	ReplicaWriteSource
} from '../types.js';
import {
	deepEqual,
	modelFromRecordKey,
	protocolInvalid,
	responsePathKey
} from './clocks.js';
import { EMPTY_ERRORS, SHA256 } from './constants.js';
import type {
	OperationProtocolSource,
	QueryState,
	RegisteredCommandAuthorityContract
} from './types.js';

import { reportSafely, reportUnhandledError } from '../../lib/report.js';

export { deepEqual } from './clocks.js';

export function prepareRecordEvidence(
	snapshot: DistributedQuerySnapshot,
	commandRecords: readonly DistributedRecordRevision[]
): {
	byPath: Map<string, DistributedRecordRevision>;
	tombstones: readonly DistributedRecordRevision[];
	pathless: readonly DistributedRecordRevision[];
	livePaths: ReadonlySet<string>;
} {
	const byPath = new Map<string, DistributedRecordRevision>();
	const tombstones: DistributedRecordRevision[] = [];
	const pathless: DistributedRecordRevision[] = [];
	const livePaths = new Set<string>();
	for (const evidence of snapshot.records) {
		if (evidence.path === undefined) {
			if (evidence.tombstone) tombstones.push(evidence);
			else pathless.push(evidence);
			continue;
		}
		const key = responsePathKey(evidence.path);
		if (byPath.has(key)) {
			protocolInvalid('extensions.distributed.snapshot.records.path');
		}
		byPath.set(key, evidence);
		if (evidence.tombstone) tombstones.push(evidence);
		else livePaths.add(key);
	}
	for (const evidence of commandRecords) {
		if (evidence.tombstone) tombstones.push(evidence);
		else pathless.push(evidence);
	}
	return {
		byPath,
		tombstones: Object.freeze(tombstones),
		pathless: Object.freeze(pathless),
		livePaths
	};
}

export function replicaResultIndexKeys<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables,
	envelope: ReplicaResultEnvelope<TData>,
	snapshot: DistributedQuerySnapshot
): ReadonlySet<string> {
	const keys = new Set<string>();
	if (
		envelope.data === undefined ||
		envelope.data === null ||
		!isReplicaResultObject(envelope.data) ||
		(envelope.errors ?? []).some(
			(error) => !Array.isArray(error.path) || error.path.length === 0
		)
	) {
		return keys;
	}
	const errorPaths = (envelope.errors ?? []).flatMap((error) =>
		Array.isArray(error.path) && error.path.length > 0
			? [error.path]
			: []
	);
	const evidencePaths = new Set(
		snapshot.records.flatMap((record) =>
			record.path === undefined || record.tombstone
				? []
				: [responsePathKey(record.path)]
		)
	);
	for (const artifactRoot of artifact.roots) {
		const root = runtimeRoot(artifactRoot);
		const rootPath: readonly (string | number)[] = [root.responseKey];
		const rootKey = replicaIndexKey({
			field: root.field,
			arguments: resolveArguments(
				root.arguments,
				variables,
				root.coverage
			)
		});
		keys.add(rootKey);
		if (
			resultPathBlocked(errorPaths, rootPath) ||
			!Object.prototype.hasOwnProperty.call(
				envelope.data,
				root.responseKey
			)
		) {
			continue;
		}
		const value = envelope.data[root.responseKey];
		if (
			value === null &&
			resultPathHasErrors(errorPaths, rootPath)
		) {
			continue;
		}
		collectResultBranchIndexKeys(
			artifact.id,
			root,
			value,
			rootPath,
			rootKey,
			variables,
			errorPaths,
			evidencePaths,
			keys
		);
	}
	return keys;
}

export function collectResultBranchIndexKeys(
	artifactId: string,
	selection: RuntimeRootSelection | RuntimeObjectBranch,
	value: unknown,
	path: readonly (string | number)[],
	enclosingIndexKey: string,
	variables: GraphqlVariables,
	errorPaths: readonly (readonly (string | number)[])[],
	evidencePaths: ReadonlySet<string>,
	keys: Set<string>
): void {
	if (value === null || value === undefined) return;
	if (selection.cardinality === 'one') {
		collectResultObjectIndexKeys(
			artifactId,
			selection.selection,
			value,
			path,
			enclosingIndexKey,
			undefined,
			variables,
			errorPaths,
			evidencePaths,
			keys
		);
		return;
	}
	if (!Array.isArray(value)) return;
	for (const [ordinal, entry] of value.entries()) {
		if (entry === null || entry === undefined) continue;
		collectResultObjectIndexKeys(
			artifactId,
			selection.selection,
			entry,
			[...path, ordinal],
			enclosingIndexKey,
			ordinal,
			variables,
			errorPaths,
			evidencePaths,
			keys
		);
	}
}

export function collectResultObjectIndexKeys(
	artifactId: string,
	selection: RuntimeObjectSelection,
	value: unknown,
	path: readonly (string | number)[],
	enclosingIndexKey: string,
	ordinal: number | undefined,
	variables: GraphqlVariables,
	errorPaths: readonly (readonly (string | number)[])[],
	evidencePaths: ReadonlySet<string>,
	keys: Set<string>
): void {
	if (resultPathBlocked(errorPaths, path) || !isReplicaResultObject(value)) {
		return;
	}
	const fields = new Map<string, CacheValue>();
	for (const member of selection.members) {
		if (member.kind !== 'scalar') continue;
		const fieldPath = [...path, member.responseKey];
		if (
			resultPathBlocked(errorPaths, fieldPath) ||
			!Object.prototype.hasOwnProperty.call(value, member.responseKey)
		) {
			continue;
		}
		const rawValue = value[member.responseKey];
		if (rawValue === null && !member.nullable) continue;
		if (!fields.has(member.field)) {
			fields.set(
				member.field,
				cloneJsonValue(rawValue) as CacheValue
			);
		}
	}
	let parentKey: string;
	if (
		selection.storage.kind === 'normalized' &&
		evidencePaths.has(responsePathKey(path.map(String)))
	) {
		const identity = selection.storage.identityFields.flatMap((field) => {
			const value = fields.get(field);
			return value === undefined || value === null ? [] : [value];
		});
		if (identity.length !== selection.storage.identityFields.length) return;
		parentKey = replicaRecordKey(
			{
				id: selection.storage.model,
				identityFields: selection.storage.identityFields
			},
			identity
		);
	} else {
		parentKey = embeddedRecordKey(
			artifactId,
			enclosingIndexKey,
			ordinal
		);
	}
	for (const member of selection.members) {
		if (member.kind !== 'branch') continue;
		const branchPath = [...path, member.responseKey];
		const branchKey = replicaIndexKey({
			parent: parentKey,
			field: member.field,
			arguments: resolveArguments(
				member.arguments,
				variables,
				member.coverage
			)
		});
		keys.add(branchKey);
		if (
			resultPathBlocked(errorPaths, branchPath) ||
			!Object.prototype.hasOwnProperty.call(value, member.responseKey)
		) {
			continue;
		}
		const branchValue = value[member.responseKey];
		if (
			branchValue === null &&
			resultPathHasErrors(errorPaths, branchPath)
		) {
			continue;
		}
		collectResultBranchIndexKeys(
			artifactId,
			member,
			branchValue,
			branchPath,
			branchKey,
			variables,
			errorPaths,
			evidencePaths,
			keys
		);
	}
}

export function resultPathBlocked(
	errorPaths: readonly (readonly (string | number)[])[],
	path: readonly (string | number)[]
): boolean {
	return errorPaths.some((errorPath) => resultPathPrefix(errorPath, path));
}

export function resultPathHasErrors(
	errorPaths: readonly (readonly (string | number)[])[],
	path: readonly (string | number)[]
): boolean {
	return errorPaths.some(
		(errorPath) =>
			resultPathPrefix(path, errorPath) ||
			resultPathPrefix(errorPath, path)
	);
}

export function resultPathPrefix(
	prefix: readonly (string | number)[],
	value: readonly (string | number)[]
): boolean {
	return (
		prefix.length <= value.length &&
		prefix.every((entry, index) => entry === value[index])
	);
}

export function isReplicaResultObject(
	value: unknown
): value is Readonly<Record<string, unknown>> {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function indexMaintenanceSnapshot(
	confirmed: CacheEngineSnapshot
): ReplicaIndexMaintenanceSnapshot {
	return Object.freeze({
		records: Object.freeze(
			confirmed.records.flatMap((record) => {
				if (record.tombstoneRevision !== undefined) return [];
				const model = modelFromRecordKey(record.key);
				if (model === undefined) return [];
				return [
					Object.freeze({
						key: record.key,
						model,
						fields: Object.freeze(
							Object.fromEntries(
								Object.entries(record.fields).map(([name, field]) => [
									name,
									field.value
								])
							)
						)
					})
				];
			})
		),
		indexes: Object.freeze(
			confirmed.indexes.flatMap((index) => {
				if (index.deleted || index.metadata === undefined) return [];
				return [
					Object.freeze({
						key: index.key,
						records: index.records,
						complete: index.complete,
						metadata: index.metadata
					})
				];
			})
		)
	});
}

export function indexSemanticLayer(
	layer: OptimisticLayerView
): ReplicaIndexSemanticLayer {
	const context = layer.context;
	if (
		context === null ||
		Array.isArray(context) ||
		typeof context !== 'object'
	) {
		throw new TypeError(
			`optimistic layer ${layer.id} has invalid index-maintenance context`
		);
	}
	const record = context as Readonly<Record<string, CacheValue>>;
	if (record.id !== layer.id || !Array.isArray(record.changes)) {
		throw new TypeError(
			`optimistic layer ${layer.id} has invalid index-maintenance context`
		);
	}
	return Object.freeze({
		id: layer.id,
		changes: record.changes as unknown as readonly ReplicaIndexSemanticChange[]
	});
}

export function baseWriter(writer: BaseCacheWriter): ReplicaBaseWriter {
	return Object.freeze({
		writeRecord(
			model: ReplicaModelArtifact,
			identity: ReplicaIdentity,
			revision: ReplicaRevision,
			patch: ReplicaRecordPatch & { readonly incarnation?: ReplicaRevision }
		): boolean {
			return writer.writeRecord({
				key: replicaRecordKey(model, identity),
				revision,
				...patch
			});
		},
		tombstoneRecord(
			model: ReplicaModelArtifact,
			identity: ReplicaIdentity,
			revision: ReplicaRevision
		): boolean {
			return writer.tombstoneRecord(replicaRecordKey(model, identity), revision);
		},
		writeIndex(
			target: ReplicaIndexTarget,
			records: readonly string[],
			revision: ReplicaRevision
		): boolean {
			return writer.writeIndex({
				key: indexKeyFromTarget(target),
				revision,
				records,
				complete: target.complete ?? false,
				metadata: metadataFromTarget(target)
			});
		},
		deleteIndex(target: ReplicaIndexTarget, revision: ReplicaRevision): boolean {
			return writer.deleteIndex(indexKeyFromTarget(target), revision);
		}
	});
}

export function metadataFromTarget(target: ReplicaIndexTarget): CacheIndexMetadata {
	const dependencies = [...new Set(target.dependencies ?? [])].sort();
	return Object.freeze({
		...(target.parent === undefined ? {} : { parent: target.parent }),
		field: target.field,
		arguments: target.arguments ?? Object.freeze({}),
		coverage: target.coverage ?? ({ kind: 'unknown' } as CacheIndexCoverage),
		dependencies: Object.freeze(dependencies),
		...(target.staleReason === undefined ? {} : { staleReason: target.staleReason }),
		...(target.nullValue === undefined ? {} : { nullValue: target.nullValue })
	});
}

export function indexKeyFromTarget(target: ReplicaIndexTarget): string {
	return replicaIndexKey({
		...(target.parent === undefined ? {} : { parent: target.parent }),
		field: target.field,
		arguments: target.arguments ?? {}
	});
}

export function replicaClientRequestExtensions<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>
): { readonly extensions?: Readonly<Record<string, unknown>> } {
	validatedArtifactBinding(artifact);
	const protocol = artifact.protocol;
	const surface =
		protocol.surface.kind === 'role'
			? Object.freeze({
					kind: 'role' as const,
					name: protocol.surface.name
				})
			: Object.freeze({
					kind: 'application' as const,
					name: protocol.surface.name,
					eligible_roles: Object.freeze([
						...protocol.surface.eligible_roles
					]),
					schema_roles: Object.freeze([...protocol.surface.schema_roles])
				});
	return Object.freeze({
		extensions: Object.freeze({
			distributed: Object.freeze({
				client: Object.freeze({
					surface,
					schemaHash: protocol.schemaHash
				})
			})
		})
	});
}

export function validatedCommandAuthorityContract(
	value: ReplicaCommandSurfaceContract
): RegisteredCommandAuthorityContract {
	if (
		value === null ||
		typeof value !== 'object' ||
		value.protocolVersion !== 1 ||
		typeof value.schemaHash !== 'string' ||
		!SHA256.test(value.schemaHash) ||
		typeof value.protocolHash !== 'string' ||
		!SHA256.test(value.protocolHash) ||
		!Array.isArray(value.trustedPresets)
	) {
		throw new TypeError('replica command authority contract is invalid');
	}
	const surfaceIdentity = validatedSurfaceIdentity(value.surface);
	const names = new Set<string>();
	const trustedPresets = Object.freeze(
		value.trustedPresets
			.map((descriptor) => {
				if (
					descriptor === null ||
					typeof descriptor !== 'object' ||
					typeof descriptor.name !== 'string' ||
					descriptor.name.length === 0 ||
					descriptor.name.length > 128 ||
					descriptor.name.trim() !== descriptor.name ||
					/[\u0000-\u001f\u007f-\u009f]/.test(descriptor.name) ||
					names.has(descriptor.name) ||
					!isDistributedTrustedPresetCodec(descriptor.codec)
				) {
					throw new TypeError(
						'replica command trusted preset contract is invalid'
					);
				}
				names.add(descriptor.name);
				return Object.freeze({
					name: descriptor.name,
					codec: descriptor.codec
				});
			})
			.sort(({ name: left }, { name: right }) =>
				left < right ? -1 : left > right ? 1 : 0
			)
	);
	const fingerprint = JSON.stringify([
		2,
		value.schemaHash,
		value.protocolHash,
		surfaceIdentity,
		trustedPresets
	]);
	return Object.freeze({
		schemaHash: value.schemaHash,
		protocolHash: value.protocolHash,
		surfaceIdentity,
		trustedPresets,
		fingerprint
	});
}

export function canonicalTrustedPresets(
	value: readonly DistributedTrustedPreset[]
): readonly DistributedTrustedPreset[] {
	return Object.freeze(
		[...value].sort(({ name: left }, { name: right }) =>
			left < right ? -1 : left > right ? 1 : 0
		)
	);
}

export function trustedPresetInventoryFingerprint(
	value: readonly DistributedTrustedPreset[]
): string {
	return JSON.stringify(canonicalTrustedPresets(value));
}

export function trustedPresetDescriptorFingerprint(
	value: readonly ReplicaTrustedPresetDescriptor[]
): string {
	return JSON.stringify(value);
}

export function operationKey<TData, TVariables extends GraphqlVariables>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables
): string {
	const binding = validatedArtifactBinding(artifact);
	const artifactIdentity = JSON.stringify([
		binding.version,
		binding.schemaHash,
		binding.surfaceIdentity,
		binding.operation,
		artifact.id
	]);
	return `protocol:${artifactIdentity}:${canonicalVariables(variables)}`;
}

export function validatedSurfaceIdentity(
	value: NonNullable<ReplicaOperationArtifact['protocol']>['surface']
): string {
	if (
		value === null ||
		typeof value !== 'object' ||
		typeof value.name !== 'string' ||
		value.name.length === 0
	) {
		throw new TypeError('replica artifact client surface is invalid');
	}
	if (value.kind === 'role') {
		return JSON.stringify(['role', value.name]);
	}
	if (
		value.kind !== 'application' ||
		!Array.isArray(value.eligible_roles) ||
		!Array.isArray(value.schema_roles) ||
		value.eligible_roles.length === 0 ||
		value.schema_roles.length === 0 ||
		value.eligible_roles.some(
			(role) => typeof role !== 'string' || role.length === 0
		) ||
		value.schema_roles.some(
			(role) => typeof role !== 'string' || role.length === 0
		) ||
		new Set(value.eligible_roles).size !== value.eligible_roles.length ||
		new Set(value.schema_roles).size !== value.schema_roles.length ||
		[...value.eligible_roles].sort().some(
			(role, index) => role !== value.eligible_roles[index]
		) ||
		[...value.schema_roles].sort().some(
			(role, index) => role !== value.schema_roles[index]
		) ||
		value.schema_roles.some((role) => !value.eligible_roles.includes(role))
	) {
		throw new TypeError('replica artifact client surface is invalid');
	}
	return JSON.stringify([
		'application',
		value.name,
		value.eligible_roles,
		value.schema_roles
	]);
}

export function snapshotFrom<TData>(
	materialized: MaterializedReplicaResult<TData>,
	state: QueryState
): ReplicaSnapshot<TData> {
	const status: ReplicaStatus =
		state.errors.length > 0
			? 'error'
			: materialized.stale
				? 'stale'
				: materialized.complete
					? 'ready'
					: 'loading';
	const snapshot = Object.freeze({
		data: materialized.data,
		status,
		fetching: state.fetching,
		stale: materialized.stale,
		complete: materialized.complete,
		errors: state.errors,
		live: state.live
	});
	// `materializeReplicaOperation` only reports complete after every generated
	// selection is present; that runtime invariant is what promotes sparse data
	// to the generated result type in ReplicaSnapshot's discriminated union.
	return snapshot as ReplicaSnapshot<TData>;
}

export function snapshotEqual<TData>(
	left: ReplicaSnapshot<TData>,
	right: ReplicaSnapshot<TData>
): boolean {
	return (
		left.data === right.data &&
		left.status === right.status &&
		left.fetching === right.fetching &&
		left.stale === right.stale &&
		left.complete === right.complete &&
		left.errors === right.errors &&
		left.live === right.live
	);
}

export function freezeErrors(errors: readonly GqlError[]): readonly GqlError[] {
	if (errors.length === 0) return EMPTY_ERRORS;
	return Object.freeze(
		errors.map((error) =>
			(Object.freeze({
				message: error.message,
				...(error.locations === undefined
					? {}
					: {
							locations: Object.freeze(
								error.locations.map((location) => Object.freeze({ ...location }))
							)
						}),
				...(error.path === undefined ? {} : { path: Object.freeze([...error.path]) }),
				...(error.extensions === undefined
					? {}
					: {
							extensions: cloneJsonValue(error.extensions) as GqlError['extensions']
						})
			}) as GqlError)
		)
	);
}

export function stableErrors(
	current: readonly GqlError[],
	next: readonly GqlError[]
): readonly GqlError[] {
	if (next.length === 0) return EMPTY_ERRORS;
	return deepEqual(current, next) ? current : freezeErrors(next);
}

/**
 * GraphQL payload of a live failure frame (`docs/live-query-delivery.md`): no
 * result data and a nonempty list of errors that each carry a string message.
 * Callers also require absent snapshot, live and command metadata. Partial
 * data with errors is never a failure payload.
 */
export function isGraphqlFailurePayload(envelope: {
	readonly data?: unknown;
	readonly errors?: unknown;
}): envelope is { readonly data?: null; readonly errors: readonly GqlError[] } {
	return (
		(envelope.data === undefined || envelope.data === null) &&
		Array.isArray(envelope.errors) &&
		envelope.errors.length > 0 &&
		envelope.errors.every(
			(error: unknown) =>
				error !== null &&
				typeof error === 'object' &&
				!Array.isArray(error) &&
				typeof (error as { message?: unknown }).message === 'string'
		)
	);
}

export function graphqlError(error: unknown): GqlError {
	return Object.freeze({
		message: error instanceof Error ? error.message : String(error),
		extensions: Object.freeze({ code: 'REPLICA_TRANSPORT' })
	});
}

export function assertWriteSource(source: ReplicaWriteSource): void {
	if (!['network', 'live', 'ssr', 'restore', 'atomic'].includes(source)) {
		throw new TypeError(`unsupported replica write source: ${source}`);
	}
}

export function protocolOperationSource(
	source: ReplicaWriteSource
): OperationProtocolSource {
	return source === 'live' ? 'live' : 'query';
}

export { reportSafely };
export const reportUnhandledObserverError = reportUnhandledError;
