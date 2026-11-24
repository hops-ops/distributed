import type { CacheIndex, CacheReader } from '../internal/cache-engine.js';
import type { GraphqlVariables } from '../types.js';
import { replicaIndexKey, resolveArguments } from './identity/keys.js';
import {
	runtimeRoot,
	type RuntimeObjectBranch,
	type RuntimeObjectSelection,
	type RuntimeRootSelection
} from './selection.js';
import type { ReplicaOperationArtifact, ReplicaSparse } from './types.js';

export type MaterializedReplicaResult<TData> = {
	readonly data: ReplicaSparse<TData>;
	readonly complete: boolean;
	readonly stale: boolean;
	readonly identitySignature: string;
};

type MaterializedObject = {
	value?: Readonly<Record<string, unknown>>;
	complete: boolean;
	stale: boolean;
	identitySignature: string;
};

type MaterializedBranch = {
	value?: unknown;
	present: boolean;
	complete: boolean;
	stale: boolean;
	identitySignature: string;
};

type RuntimeBranchSelection = RuntimeRootSelection | RuntimeObjectBranch;

export function materializeReplicaOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	reader: CacheReader,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	variables: TVariables
): MaterializedReplicaResult<TData> {
	const output: Record<string, unknown> = {};
	let complete = true;
	let stale = false;
	const signatures: string[] = [];
	for (const artifactRoot of artifact.roots) {
		const root = runtimeRoot(artifactRoot);
		const argumentsValue = resolveArguments(root.arguments, variables, root.coverage);
		const indexKey = replicaIndexKey({ field: root.field, arguments: argumentsValue });
		const index = reader.index(indexKey);
		const branch = materializeBranch(reader, root, index, variables);
		if (branch.present) {
			defineOutputValue(output, root.responseKey, branch.value);
		}
		complete &&= branch.complete;
		stale ||= branch.stale;
		signatures.push(`${indexKey}:${branch.identitySignature}`);
	}
	return Object.freeze({
		data: Object.freeze(output) as ReplicaSparse<TData>,
		complete,
		stale,
		identitySignature: signatures.join('|')
	});
}

function materializeBranch(
	reader: CacheReader,
	selection: RuntimeBranchSelection,
	index: CacheIndex | undefined,
	variables: GraphqlVariables
): MaterializedBranch {
	if (!index || !index.metadata) {
		return {
			present: false,
			complete: false,
			stale: false,
			identitySignature: 'missing-index'
		};
	}
	const stale = index.metadata.staleReason !== undefined;
	/*
	 * Completeness and freshness are independent. A stale complete index still
	 * has a structurally materializable result and remains visible during
	 * stale-while-revalidate. Partial/lifecycle-invalidated indexes explicitly
	 * carry `complete=false` and remain sparse.
	 */
	const indexComplete = index.complete;
	if (index.metadata.nullValue) {
		return {
			value: null,
			present: true,
			complete: indexComplete && selection.nullable,
			stale,
			identitySignature: `null:${index.metadata.staleReason ?? ''}`
		};
	}

	if (selection.cardinality === 'one') {
		const key = index.records[0];
		if (key === undefined) {
			// Empty relationship edge: null when the selection allows it.
			if (selection.nullable) {
				return {
					value: null,
					present: true,
					complete: indexComplete,
					stale,
					identitySignature: `null:${index.metadata.staleReason ?? ''}`
				};
			}
			return {
				present: false,
				complete: false,
				stale,
				identitySignature: `missing-record:${index.metadata.staleReason ?? ''}`
			};
		}
		const object = materializeObject(reader, selection.selection, key, variables);
		return {
			value: object.value,
			present: object.value !== undefined,
			complete: indexComplete && index.records.length === 1 && object.complete,
			stale: stale || object.stale,
			identitySignature: object.identitySignature
		};
	}

	const values: Readonly<Record<string, unknown>>[] = [];
	let childrenComplete = true;
	let childrenStale = false;
	const signatures: string[] = [];
	for (const key of index.records) {
		const object = materializeObject(reader, selection.selection, key, variables);
		if (object.value !== undefined) values.push(object.value);
		else childrenComplete = false;
		childrenComplete &&= object.complete;
		childrenStale ||= object.stale;
		signatures.push(object.identitySignature);
	}
	return {
		value: Object.freeze(values),
		present: true,
		complete: indexComplete && childrenComplete,
		stale: stale || childrenStale,
		identitySignature: signatures.join(',')
	};
}

function materializeObject(
	reader: CacheReader,
	selection: RuntimeObjectSelection,
	key: string,
	variables: GraphqlVariables
): MaterializedObject {
	const record = reader.recordMeta(key);
	if (!record) {
		return {
			complete: false,
			stale: false,
			identitySignature: `${key}:missing`
		};
	}
	const output: Record<string, unknown> = {};
	let complete = true;
	let stale = false;
	const nestedSignatures: string[] = [];
	for (const member of selection.members) {
		if (member.kind !== 'scalar') continue;
		const presence = reader.field(key, member.field);
		if (!presence.present || (presence.value === null && !member.nullable)) {
			complete = false;
			continue;
		}
		if (member.expose !== false) {
			defineOutputValue(output, member.responseKey, presence.value);
		}
	}

	for (const member of selection.members) {
		if (member.kind !== 'branch') continue;
		const branchResult = materializeNestedBranch(
			reader,
			key,
			record.incarnation,
			member,
			variables
		);
		if (member.expose !== false && branchResult.present) {
			defineOutputValue(output, member.responseKey, branchResult.value);
		}
		complete &&= branchResult.complete;
		stale ||= branchResult.stale;
		nestedSignatures.push(`${member.field}:${branchResult.identitySignature}`);
	}

	return {
		value: Object.freeze(output),
		complete,
		stale,
		identitySignature: `${key}@${record.incarnation}[${nestedSignatures.join('|')}]`
	};
}

function materializeNestedBranch(
	reader: CacheReader,
	parentKey: string,
	parentIncarnation: string,
	selection: RuntimeObjectBranch,
	variables: GraphqlVariables
): MaterializedBranch {
	const argumentsValue = resolveArguments(
		selection.arguments,
		variables,
		selection.coverage
	);
	const indexKey = replicaIndexKey({
		parent: parentKey,
		field: selection.field,
		arguments: argumentsValue
	});
	const index = reader.index(indexKey);
	if (index?.metadata?.parentIncarnation !== parentIncarnation) {
		/*
		 * Optimistic parent upserts often write the FK row before a relationship
		 * index exists. For nullable joins (e.g. ChatMessages.author), treat the
		 * missing edge as an authorized null so the parent list stays complete
		 * and visible. Required relationships still fail closed as incomplete.
		 */
		if (selection.nullable) {
			return {
				value: null,
				present: true,
				complete: true,
				stale: index !== undefined,
				identitySignature: `null-missing-edge:${selection.field}`
			};
		}
		return {
			present: false,
			complete: false,
			stale: index !== undefined,
			identitySignature: `parent-incarnation-mismatch:${parentIncarnation}`
		};
	}
	return materializeBranch(reader, selection, index, variables);
}

function defineOutputValue(
	output: Record<string, unknown>,
	key: string,
	value: unknown
): void {
	Object.defineProperty(output, key, {
		value,
		enumerable: true,
		configurable: false,
		writable: false
	});
}
