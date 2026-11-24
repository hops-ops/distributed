import type {
	BaseCacheWriter,
	CacheIndexMetadata,
	CacheValue,
	RecordKey,
	Revision
} from '../internal/cache-engine.js';
import type { DistributedRecordRevision } from '../protocol.js';
import type { GqlError, GraphqlVariables } from '../types.js';
import { cloneJsonValue } from './identity/clone.js';
import {
	coverageFromArtifact,
	replicaIndexKey,
	replicaRecordKey,
	resolveArguments
} from './identity/keys.js';
import {
	embeddedRecordKey,
	runtimeRoot,
	type RuntimeObjectBranch,
	type RuntimeObjectSelection,
	type RuntimeRootSelection
} from './selection.js';
import type {
	ReplicaOperationArtifact,
	ReplicaRevision,
	ReplicaResultEnvelope
} from './types.js';

import { deepEqual } from '../lib/deep-equal.js';

export type ReplicaNormalizationSummary = {
	readonly wrote: boolean;
	readonly partial: boolean;
	readonly indexKeys: readonly string[];
};

export type ReplicaProtocolRecordResolution = {
	readonly evidence: DistributedRecordRevision;
	/** False means a same-scope newer base record already won. */
	readonly apply: boolean;
};

export type ReplicaNormalizationProtocol = {
	readonly indexRevision: ReplicaRevision;
	readonly writeIndexes: boolean;
	readonly indexesComplete: boolean;
	/**
	 * Missing causal record evidence may use operation-local snapshot storage.
	 * Such rows remain renderable without impersonating a shared normalized
	 * identity.
	 */
	readonly allowSnapshotOnlyRecords: boolean;
	readonly record: (
		path: readonly string[],
		model: string,
		key: string,
		fields: Readonly<Record<string, CacheValue>>
	) => ReplicaProtocolRecordResolution | undefined;
};

type NormalizedBranch = {
	keys: RecordKey[];
	nullValue: boolean;
	partial: boolean;
};

type ErrorPaths = {
	global: boolean;
	paths: readonly (readonly (string | number)[])[];
};

type RuntimeBranchSelection = RuntimeRootSelection | RuntimeObjectBranch;

export function normalizeReplicaResult<
	TData,
	TVariables extends GraphqlVariables
>(
	writer: BaseCacheWriter,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables,
	envelope: ReplicaResultEnvelope<TData>,
	protocol: ReplicaNormalizationProtocol
): ReplicaNormalizationSummary {
	validateArtifact(artifact);
	const snapshotRevision = protocol.indexRevision;
	const errors = collectErrorPaths(envelope.errors ?? []);
	if (errors.global || envelope.data === undefined) {
		return Object.freeze({ wrote: false, partial: errors.global, indexKeys: [] });
	}
	if (envelope.data === null && (envelope.errors?.length ?? 0) > 0) {
		// `data: null` is a valid GraphQL error result after non-null propagation.
		// The ingress coordinator will stale the operation roots without replacing
		// the last known-good memberships.
		return Object.freeze({ wrote: false, partial: true, indexKeys: [] });
	}
	if (envelope.data === null || !isObject(envelope.data)) {
		throw new TypeError(`operation ${artifact.id} returned a non-object GraphQL data value`);
	}

	let wrote = false;
	let partial = false;
	const indexKeys: string[] = [];
	for (const artifactRoot of artifact.roots) {
		const root = runtimeRoot(artifactRoot);
		const path: readonly (string | number)[] = [root.responseKey];
		const hasValue = Object.prototype.hasOwnProperty.call(envelope.data, root.responseKey);
		const blocked = pathBlocked(errors, path);
		const argumentsValue = resolveArguments(root.arguments, variables, root.coverage);
		const key = replicaIndexKey({ field: root.field, arguments: argumentsValue });
		const hasErrors = pathHasErrors(errors, path);
		const rawValue = hasValue ? envelope.data[root.responseKey] : undefined;
		if (blocked || !hasValue || (rawValue === null && hasErrors)) {
			wrote =
				protocol.writeIndexes
					? writer.markIndexStale(
							key,
							hasErrors ? 'graphql-partial-error' : 'incomplete-result',
							snapshotRevision
						) || wrote
					: wrote;
			indexKeys.push(key);
			partial = true;
			continue;
		}
		const branch = normalizeBranch(
			writer,
			artifact.id,
			root,
			rawValue,
			path,
			key,
			snapshotRevision,
			variables,
			errors,
			protocol,
			indexKeys
		);
		const rootPartial =
			branch.partial ||
			hasErrors ||
			!protocol.indexesComplete;
		const metadata = indexMetadata(
			root,
			argumentsValue,
			branch,
			rootPartial,
			hasErrors
		);
		if (
			protocol.writeIndexes &&
			writer.writeIndex({
				key,
				revision: snapshotRevision,
				records: branch.keys,
				complete: !rootPartial,
				metadata
			})
		) {
			wrote = true;
		}
		indexKeys.push(key);
		partial ||= rootPartial;
	}
	return Object.freeze({
		wrote,
		partial,
		indexKeys: Object.freeze(indexKeys)
	});
}

function normalizeBranch(
	writer: BaseCacheWriter,
	artifactId: string,
	selection: RuntimeBranchSelection,
	value: unknown,
	path: readonly (string | number)[],
	indexKey: string,
	snapshotRevision: Revision,
	variables: GraphqlVariables,
	errors: ErrorPaths,
	protocol: ReplicaNormalizationProtocol,
	indexKeys: string[]
): NormalizedBranch {
	if (value === null) {
		if (!selection.nullable && !pathHasErrors(errors, path)) {
			throw new TypeError(
				`operation ${artifactId} returned null for non-null field ${pathLabel(path)}`
			);
		}
		return { keys: [], nullValue: true, partial: false };
	}
	if (value === undefined) return { keys: [], nullValue: false, partial: true };
	if (selection.cardinality === 'one') {
		const object = normalizeObject(
			writer,
			artifactId,
			selection.selection,
			value,
			path,
			indexKey,
			undefined,
			snapshotRevision,
			variables,
			errors,
			protocol,
			indexKeys
		);
		return {
			keys: object.key === undefined ? [] : [object.key],
			nullValue: false,
			partial: object.partial
		};
	}
	if (!Array.isArray(value)) {
		throw new TypeError(
			`operation ${artifactId} returned a non-list value for ${pathLabel(path)}`
		);
	}
	const keys: RecordKey[] = [];
	let partial = false;
	for (const [ordinal, entry] of value.entries()) {
		const entryPath = [...path, ordinal];
		if (entry === null || entry === undefined) {
			if (pathHasErrors(errors, entryPath)) {
				partial = true;
				continue;
			}
			throw new TypeError(
				`operation ${artifactId} returned null for non-null list item ${pathLabel(
					entryPath
				)}`
			);
		}
		const object = normalizeObject(
			writer,
			artifactId,
			selection.selection,
			entry,
			entryPath,
			indexKey,
			ordinal,
			snapshotRevision,
			variables,
			errors,
			protocol,
			indexKeys
		);
		if (object.key === undefined) partial = true;
		else keys.push(object.key);
		partial ||= object.partial;
	}
	return { keys, nullValue: false, partial };
}

function normalizeObject(
	writer: BaseCacheWriter,
	artifactId: string,
	selection: RuntimeObjectSelection,
	value: unknown,
	path: readonly (string | number)[],
	enclosingIndexKey: string,
	ordinal: number | undefined,
	snapshotRevision: Revision,
	variables: GraphqlVariables,
	errors: ErrorPaths,
	protocol: ReplicaNormalizationProtocol,
	indexKeys: string[]
): { key?: RecordKey; partial: boolean } {
	if (pathBlocked(errors, path)) return { partial: true };
	if (!isObject(value)) {
		throw new TypeError(
			`operation ${artifactId} returned a non-object value for ${pathLabel(path)}`
		);
	}

	const fields: Record<string, CacheValue> = Object.create(null) as Record<
		string,
		CacheValue
	>;
	let partial = false;
	for (const member of selection.members) {
		if (member.kind !== 'scalar') continue;
		const fieldPath = [...path, member.responseKey];
		const hasValue = Object.prototype.hasOwnProperty.call(value, member.responseKey);
		const rawValue = hasValue ? value[member.responseKey] : undefined;
		if (
			pathBlocked(errors, fieldPath) ||
			!hasValue
		) {
			partial = true;
			continue;
		}
		if (rawValue === null && !member.nullable) {
			if (pathHasErrors(errors, fieldPath)) {
				partial = true;
				continue;
			}
			throw new TypeError(
				`operation ${artifactId} returned null for non-null field ${pathLabel(
					fieldPath
				)}`
			);
		}
		const next = cloneJsonValue(rawValue);
		if (Object.prototype.hasOwnProperty.call(fields, member.field)) {
			if (!deepEqual(fields[member.field], next)) {
				throw new TypeError(
					`operation aliases disagree for ${selection.typename}.${member.field}`
				);
			}
			continue;
		}
		fields[member.field] = next;
	}

	let key: RecordKey;
	let revision: Revision;
	let incarnation: Revision | undefined;
	let resolution: ReplicaProtocolRecordResolution | undefined;
	if (selection.storage.kind === 'normalized') {
		const identity: CacheValue[] = [];
		for (const field of selection.storage.identityFields) {
			if (
				!Object.prototype.hasOwnProperty.call(fields, field) ||
				fields[field] === null
			) {
				return { partial: true };
			}
			identity.push(fields[field]!);
		}
		key = replicaRecordKey(
			{
				id: selection.storage.model,
				identityFields: selection.storage.identityFields
			},
			identity
		);
		resolution = protocol.record(
			path.map(String),
			selection.storage.model,
			key,
			fields
		);
		if (resolution === undefined) {
			if (!protocol.allowSnapshotOnlyRecords) {
				return { key, partial: true };
			}
			/*
			 * This row came from an exact authorized query, but the selected
			 * surface has no safely comparable record clock for its model.
			 * Scope the storage to this operation/index position so a later
			 * snapshot can replace it without corrupting causally normalized
			 * state shared by another operation.
			 */
			key = embeddedRecordKey(artifactId, enclosingIndexKey, ordinal);
			revision = snapshotRevision;
			incarnation = snapshotRevision;
		} else {
			if (resolution.evidence.tombstone) {
				throw new TypeError('live GraphQL row carries tombstone record evidence');
			}
			revision = resolution.evidence.revision;
			incarnation = resolution.evidence.incarnation;
		}
	} else {
		// Embedded output is an operation-local replacement snapshot, not a
		// normalized server record. Its synthetic incarnation deliberately
		// advances with the enclosing response so removed sparse fields disappear.
		key = embeddedRecordKey(artifactId, enclosingIndexKey, ordinal);
		revision = snapshotRevision;
		incarnation = snapshotRevision;
	}

	if (resolution?.apply !== false) {
		writer.writeRecord({
			key,
			revision,
			...(incarnation === undefined ? {} : { incarnation }),
			fields
		});
	}
	const storedClock = writer.recordClock(key);
	if (
		storedClock === undefined ||
		storedClock.tombstoned ||
		(resolution?.apply !== false &&
			(storedClock.revision !== String(revision) ||
				(incarnation !== undefined &&
					storedClock.incarnation !== String(incarnation))))
	) {
		return { partial: true };
	}

	for (const member of selection.members) {
		if (member.kind !== 'branch') continue;
		const branchPath = [...path, member.responseKey];
		const hasValue = Object.prototype.hasOwnProperty.call(value, member.responseKey);
		const blocked = pathBlocked(errors, branchPath);
		const hasErrors = pathHasErrors(errors, branchPath);
		const rawValue = hasValue ? value[member.responseKey] : undefined;
		const argumentsValue = resolveArguments(
			member.arguments,
			variables,
			member.coverage
		);
		const branchIndexKey = replicaIndexKey({
			parent: key,
			field: member.field,
			arguments: argumentsValue
		});
		indexKeys.push(branchIndexKey);
		if (!hasValue || blocked || (rawValue === null && hasErrors)) {
			if (protocol.writeIndexes) {
				writer.markIndexStale(
					branchIndexKey,
					hasErrors ? 'graphql-partial-error' : 'incomplete-result',
					snapshotRevision
				);
			}
			partial = true;
			continue;
		}
		const branch = normalizeBranch(
			writer,
			artifactId,
			member,
			rawValue,
			branchPath,
			branchIndexKey,
			snapshotRevision,
			variables,
			errors,
			protocol,
			indexKeys
		);
		const branchPartial =
			branch.partial ||
			hasErrors ||
			!protocol.indexesComplete;
		if (protocol.writeIndexes) {
			writer.writeIndex({
				key: branchIndexKey,
				revision: snapshotRevision,
				records: branch.keys,
				complete: !branchPartial,
				metadata: indexMetadata(
					member,
					argumentsValue,
					branch,
					branchPartial,
					hasErrors,
					key,
					storedClock.revision
				)
			});
		}
		partial ||= branchPartial;
	}

	return { key, partial };
}

function indexMetadata(
	selection: RuntimeBranchSelection,
	argumentsValue: Readonly<Record<string, CacheValue>>,
	branch: NormalizedBranch,
	partial: boolean,
	hasErrors: boolean,
	parent?: RecordKey,
	parentRevision?: Revision
): CacheIndexMetadata {
	return Object.freeze({
		...(parent === undefined ? {} : { parent }),
		...(parentRevision === undefined ? {} : { parentRevision: String(parentRevision) }),
		field: selection.field,
		arguments: argumentsValue,
		coverage: coverageFromArtifact(
			selection.coverage,
			argumentsValue,
			branch.keys.length
		),
		dependencies: Object.freeze([...new Set(selection.dependencies)].sort()),
		...(partial
			? { staleReason: hasErrors ? 'graphql-partial-error' : 'incomplete-result' }
			: {}),
		nullValue: branch.nullValue
	});
}

function collectErrorPaths(errors: readonly GqlError[]): ErrorPaths {
	const paths: Array<readonly (string | number)[]> = [];
	let global = false;
	for (const error of errors) {
		if (!Array.isArray(error.path) || error.path.length === 0) {
			global = true;
			continue;
		}
		paths.push(Object.freeze([...error.path]));
	}
	return { global, paths };
}

function pathBlocked(errors: ErrorPaths, path: readonly (string | number)[]): boolean {
	return errors.global || errors.paths.some((errorPath) => isPrefix(errorPath, path));
}

function pathHasErrors(errors: ErrorPaths, path: readonly (string | number)[]): boolean {
	return (
		errors.global ||
		errors.paths.some(
			(errorPath) => isPrefix(path, errorPath) || isPrefix(errorPath, path)
		)
	);
}

function isPrefix(
	prefix: readonly (string | number)[],
	value: readonly (string | number)[]
): boolean {
	return (
		prefix.length <= value.length &&
		prefix.every((entry, index) => entry === value[index])
	);
}

function validateArtifact(artifact: {
	readonly id: string;
	readonly roots: readonly unknown[];
}): void {
	if (!artifact || typeof artifact !== 'object') {
		throw new TypeError('replica artifact is required');
	}
	if (typeof artifact.id !== 'string' || artifact.id.length === 0) {
		throw new TypeError('replica artifact id must be a non-empty string');
	}
	if (!Array.isArray(artifact.roots) || artifact.roots.length === 0) {
		throw new TypeError(`operation ${artifact.id} must contain at least one root selection`);
	}
}

function pathLabel(path: readonly (string | number)[]): string {
	return path.map(String).join('.');
}

function isObject(value: unknown): value is Readonly<Record<string, unknown>> {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

