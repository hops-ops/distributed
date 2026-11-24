import type { GraphqlVariables } from '../../types.js';
import type { ReplicaCommandArtifact } from '../commands.js';
import type {
	ReplicaIndexCoverage,
	ReplicaOperationArtifact,
	ReplicaValue,
	ReplicaWriteSource
} from '../types.js';

export const DIAGNOSTICS_BUNDLE_MARKER = 'distributed-replica-diagnostics-v1' as const;

declare const developmentCapabilityBrand: unique symbol;

export type ReplicaDevelopmentCapability = Readonly<{
	readonly version: 1;
	readonly [developmentCapabilityBrand]: true;
}>;

export type ReplicaDiagnosticFieldValueContext = Readonly<{
	recordKey: string;
	model?: string;
	field: string;
	kind: 'field' | 'link';
}>;

export type ReplicaDiagnosticFieldValuePolicy = Readonly<{
	capability: ReplicaDevelopmentCapability;
	/** A field must be explicitly authorized before its value reaches the redactor. */
	allow(context: ReplicaDiagnosticFieldValueContext): boolean;
	/**
	 * Return a support-safe replacement. Returning undefined omits the value.
	 * Raw values are never retained before this function returns.
	 */
	redact(
		value: ReplicaValue,
		context: ReplicaDiagnosticFieldValueContext
	): ReplicaValue | undefined;
}>;

export type ReplicaDiagnosticReasonContext =
	| Readonly<{
			kind: 'index-stale';
			indexKey: string;
	  }>
	| Readonly<{
			kind: 'layer';
			layerId: string;
			action: 'created' | 'rebased' | 'accepted' | 'retired' | 'rejected';
	  }>;

export type ReplicaDiagnosticReasonPolicy = Readonly<{
	capability: ReplicaDevelopmentCapability;
	/**
	 * Return a bounded support-safe replacement. Returning undefined keeps the
	 * structural default (`application-stale`, for example).
	 */
	redact(
		reason: string,
		context: ReplicaDiagnosticReasonContext
	): string | undefined;
}>;

export type ReplicaDiagnosticsOptions = Readonly<{
	maxEvents?: number;
	development?: ReplicaDevelopmentCapability;
	fieldValues?: ReplicaDiagnosticFieldValuePolicy;
	reasons?: ReplicaDiagnosticReasonPolicy;
	now?: () => number;
	onObserverError?: (error: AggregateError) => void;
}>;

export type ReplicaDiagnosticScopeInput = Readonly<{
	generation: number;
	established: boolean;
	protocolVersion?: 1;
	schemaHash?: string;
}>;

export type ReplicaDiagnosticRecordInput = Readonly<{
	key: string;
	model?: string;
	revision: string;
	incarnation: string;
	tombstone: boolean;
	tombstoneRevision?: string;
	presentFields: readonly string[];
	presentLinks: readonly string[];
	/**
	 * Values in this object must already be support-safe redactor output. Replica
	 * core obtains them only through `redactRecordValue`.
	 */
	values?: Readonly<Record<string, ReplicaValue>>;
}>;

export type ReplicaDiagnosticIndexInput = Readonly<{
	key: string;
	revision: string;
	staleRevision?: string;
	records: readonly string[];
	complete: boolean;
	deleted: boolean;
	field?: string;
	parent?: string;
	argumentNames?: readonly string[];
	arguments?: Readonly<Record<string, ReplicaValue>>;
	coverage?: ReplicaIndexCoverage;
	dependencies?: readonly string[];
	staleReason?: string;
	nullValue?: boolean;
}>;

export type ReplicaDiagnosticLayerInput = Readonly<{
	id: string;
	sequence: number;
	state: 'optimistic' | 'accepted';
	recordChanges: number;
	indexChanges: number;
	semanticChanges: number;
}>;

export type ReplicaDiagnosticReceiptExpectationInput = Readonly<{
	projection: string;
	model: string;
	observed: boolean;
}>;

export type ReplicaDiagnosticReceiptInput = Readonly<{
	commandId: string;
	state: 'optimistic' | 'succeeded' | 'succeeded_pending_projection';
	consistency?: 'succeeded' | 'eventual' | 'atomic';
	expectations: readonly ReplicaDiagnosticReceiptExpectationInput[];
}>;

/**
 * Engine-independent state contract implemented by DistributedReplica.
 *
 * DistributedReplica computes `records` and `indexes` when they are first
 * read, from the cache as it is then: one pass over the whole cache. A sink
 * reads them when someone asks for its snapshot, not on every update.
 */
export type ReplicaDiagnosticStateInput = Readonly<{
	scope: ReplicaDiagnosticScopeInput;
	records: readonly ReplicaDiagnosticRecordInput[];
	indexes: readonly ReplicaDiagnosticIndexInput[];
	layers: readonly ReplicaDiagnosticLayerInput[];
	receipts: readonly ReplicaDiagnosticReceiptInput[];
}>;

export type ReplicaDiagnosticEventInput =
	| Readonly<{
			kind: 'normalization';
			operation: string;
			source: ReplicaWriteSource;
			records: number;
			indexes: number;
			partial: boolean;
	  }>
	| Readonly<{
			kind: 'index-decision';
			index: string;
			decision: 'maintained' | 'stale' | 'revalidate';
			reason?: string;
	  }>
	| Readonly<{
			kind: 'revalidation';
			operation: string;
			action: 'requested' | 'deduplicated' | 'skipped-complete';
			reason: 'watch' | 'refresh' | 'stale';
	  }>
	| Readonly<{
			kind: 'layer';
			layer: string;
			action: 'created' | 'rebased' | 'accepted' | 'retired' | 'rejected';
			recordChanges?: number;
			indexChanges?: number;
			reason?: string;
	  }>
	| Readonly<{
			kind: 'receipt';
			command: string;
			state:
				| 'optimistic'
				| 'succeeded'
				| 'succeeded_pending_projection'
				| 'atomic'
				| 'rejected';
			obligations: number;
			observed: number;
	  }>
	| Readonly<{
			kind: 'response-fenced';
			operation: string;
			transport: 'http' | 'live';
			reason: 'authorization-generation' | 'operation-generation' | 'superseded';
	  }>
	| Readonly<{
			kind: 'gc';
			records: number;
	  }>
	| Readonly<{
			kind: 'hydration';
			action: 'accepted' | 'rejected';
			reason:
				| 'accepted'
				| 'same-scope-merge'
				| 'invalid'
				| 'scope-mismatch'
				| 'artifact-mismatch'
				| 'active-scope-mismatch';
	  }>
	| Readonly<{
			kind: 'scope';
			action: 'established' | 'changed' | 'invalidated';
			generation: number;
			schemaHash?: string;
	  }>;

export type ReplicaDiagnosticEvent = ReplicaDiagnosticEventInput &
	Readonly<{
		sequence: number;
		at: number;
	}>;

export type ReplicaDiagnosticRecord = Readonly<{
	key: string;
	model?: string;
	revision: string;
	incarnation: string;
	tombstone: boolean;
	tombstoneRevision?: string;
	presentFields: readonly string[];
	presentLinks: readonly string[];
	values?: Readonly<Record<string, ReplicaValue>>;
}>;

export type ReplicaDiagnosticIndex = Readonly<{
	key: string;
	revision: string;
	staleRevision?: string;
	records: readonly string[];
	complete: boolean;
	deleted: boolean;
	field?: string;
	parent?: string;
	argumentNames: readonly string[];
	arguments?: Readonly<Record<string, ReplicaValue>>;
	coverage?: ReplicaIndexCoverage;
	dependencies: readonly string[];
	staleReason?: string;
	nullValue: boolean;
}>;

export type ReplicaDiagnosticLayer = Readonly<{
	id: string;
	sequence: number;
	state: 'optimistic' | 'accepted';
	recordChanges: number;
	indexChanges: number;
	semanticChanges: number;
}>;

export type ReplicaDiagnosticReceipt = Readonly<{
	commandId: string;
	state: 'optimistic' | 'succeeded' | 'succeeded_pending_projection';
	consistency?: 'succeeded' | 'eventual' | 'atomic';
	expectations: readonly ReplicaDiagnosticReceiptExpectationInput[];
}>;

export type ReplicaArtifactSourceLocation = Readonly<{
	path: string;
	line: number;
	column: number;
}>;

export type ReplicaOperationInjectedFieldInspection = Readonly<{
	path: string;
	responseKey: string;
	field: string;
}>;

export type ReplicaOperationIndexInspection = Readonly<{
	path: string;
	field: string;
	cardinality: 'one' | 'many';
	dependencies: readonly string[];
	coverage?: ReplicaIndexCoverage['kind'];
	filtered: boolean;
	ordered: boolean;
	pagination?: 'complete' | 'offset' | 'cursor';
}>;

export type ReplicaOperationArtifactInspection = Readonly<{
	kind: 'operation';
	id: string;
	source?: ReplicaArtifactSourceLocation;
	rootFields: readonly string[];
	injectedFields: readonly ReplicaOperationInjectedFieldInspection[];
	dependencies: readonly string[];
	indexes: readonly ReplicaOperationIndexInspection[];
	live?: Readonly<{ operation: string }>;
}>;

export type ReplicaCommandEffectInspection = Readonly<{
	kind:
		| 'upsert'
		| 'patch'
		| 'delete'
		| 'link'
		| 'unlink'
		| 'invalidate_model'
		| 'invalidate_relationship';
	models: readonly string[];
	fields: readonly string[];
	valueSources: readonly (
		| 'input'
		| 'trusted_preset'
		| 'constant'
		| 'null'
	)[];
}>;

export type ReplicaCommandArtifactInspection = Readonly<{
	kind: 'command';
	name: string;
	operation: string;
	consistency: 'succeeded' | 'eventual' | 'atomic';
	effects: readonly ReplicaCommandEffectInspection[];
	revalidation: Readonly<{
		required: boolean;
		dependencies: readonly string[];
		models: readonly string[];
	}>;
}>;

export type ReplicaDiagnosticsSnapshot = Readonly<{
	version: 1;
	marker: typeof DIAGNOSTICS_BUNDLE_MARKER;
	mode: 'redacted' | 'development';
	sequence: number;
	scope: ReplicaDiagnosticScopeInput;
	records: readonly ReplicaDiagnosticRecord[];
	indexes: readonly ReplicaDiagnosticIndex[];
	layers: readonly ReplicaDiagnosticLayer[];
	receipts: readonly ReplicaDiagnosticReceipt[];
	artifacts: Readonly<{
		operations: readonly ReplicaOperationArtifactInspection[];
		commands: readonly ReplicaCommandArtifactInspection[];
	}>;
	events: readonly ReplicaDiagnosticEvent[];
}>;

/**
 * Small core-facing contract. Implementations must not throw into replica
 * transactions; DistributedReplica also catches sink failures defensively.
 */
export interface ReplicaDiagnosticsSink {
	readonly includeStructuralIdentities: boolean;
	redactRecordValue?(
		context: ReplicaDiagnosticFieldValueContext,
		value: ReplicaValue
	): ReplicaValue | undefined;
	update(state: ReplicaDiagnosticStateInput): void;
	event(event: ReplicaDiagnosticEventInput): void;
	operation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void;
	command<TInput, TOutput>(artifact: ReplicaCommandArtifact<TInput, TOutput>): void;
}

export interface ReplicaDiagnostics extends ReplicaDiagnosticsSink {
	snapshot(): ReplicaDiagnosticsSnapshot;
	/** React/useSyncExternalStore-compatible alias with stable referential output. */
	getSnapshot(): ReplicaDiagnosticsSnapshot;
	subscribe(listener: (snapshot: ReplicaDiagnosticsSnapshot) => void): () => void;
	inspectOperation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): ReplicaOperationArtifactInspection;
	inspectCommand<TInput, TOutput>(
		artifact: ReplicaCommandArtifact<TInput, TOutput>
	): ReplicaCommandArtifactInspection;
	clear(): void;
}

/** Create an unforgeable, process-local opt-in to development identities. */
