import type { GqlError, GraphqlVariables } from '../../types.js';
import type {
	CacheEngineSnapshot,
	CacheValue,
	OptimisticIndexWrite,
	OptimisticRecordWrite
} from '../../internal/cache-engine.js';
import type {
	DistributedDecimalString,
	DistributedLiveCursor,
	DistributedOpaqueString,
	DistributedTrustedPreset
} from '../../protocol.js';
import type { ReplicaTrustedPresetDescriptor } from '../commands.js';
import type { ValidatedReplicaOperationBinding } from '../operation-binding.js';
import type {
	ReplicaLiveState,
	ReplicaOperationArtifact,
	ReplicaValue
} from '../types.js';

export type QueryState = {
	fetching: boolean;
	errors: readonly GqlError[];
	live: ReplicaLiveState;
};

export type LiveEntry = {
	count: number;
	unsubscribe: () => void;
	active: boolean;
	protocolGeneration: number;
	/** Local fence for taking over query snapshots that preceded this stream. */
	startRevision: string;
	operationGeneration?: number;
};

export type ProtocolGeneration = {
	protocolVersion: 1;
	cacheScope: DistributedOpaqueString;
	schemaHash: string;
	authorizationGeneration: string;
};

export type RenderedOperation = {
	readonly artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>;
	readonly variables: GraphqlVariables;
};

export type SerializedOperationProtocolState = {
	readonly operation: string;
	readonly snapshotScope?: string;
	readonly indexClocks: readonly (readonly [
		string,
		Readonly<{ scopeToken: string; position: string }>
	])[];
	readonly indexRevision?: string;
	readonly indexKeys: readonly string[];
	readonly pathRecords: readonly (readonly [string, string])[];
	readonly cursors: readonly DistributedLiveCursor[];
};

export type SerializedOperationProtocolGroup = {
	readonly key: string;
	readonly query?: SerializedOperationProtocolState;
	readonly live?: SerializedOperationProtocolState;
	readonly active?: OperationProtocolSource;
	readonly generation: number;
};

export type ReplicaDehydratedPayloadV1 = {
	readonly cache: CacheEngineSnapshot;
	readonly operations: readonly SerializedOperationProtocolGroup[];
	readonly recordClocks: readonly (readonly [string, RecordProtocolClock])[];
	readonly anonymousRecordClocks: readonly (readonly [
		string,
		AnonymousRecordProtocolClock
	])[];
	readonly trustedPresets: readonly DistributedTrustedPreset[];
	readonly nextIndexRevision: string;
};

export type ParsedReplicaHydration = {
	readonly scope: ProtocolGeneration;
	readonly cache: CacheEngineSnapshot;
	readonly operationProtocols: Map<string, OperationProtocolGroup>;
	readonly operationGenerations: Map<string, number>;
	readonly recordClocks: Map<string, RecordProtocolClock>;
	readonly recordKeysByScope: Map<DistributedOpaqueString, string>;
	readonly anonymousRecordClocks: Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>;
	readonly trustedPresets: readonly DistributedTrustedPreset[];
	readonly nextIndexRevision: string;
};

export type RegisteredCommandAuthorityContract = {
	readonly schemaHash: string;
	readonly protocolHash: string;
	readonly surfaceIdentity: string;
	readonly trustedPresets: readonly ReplicaTrustedPresetDescriptor[];
	readonly fingerprint: string;
};

export type ReplicaArtifactBinding = {
	version: 1;
	schemaHash: string;
	surfaceIdentity?: string;
	trustedPresets?: readonly ReplicaTrustedPresetDescriptor[];
};

export type ValidatedArtifactBinding = ValidatedReplicaOperationBinding;

export type RecordProtocolClock = {
	scopeToken: DistributedOpaqueString;
	incarnation: DistributedDecimalString;
	revision: DistributedDecimalString;
	tombstone: boolean;
};

export type ProjectedRecordFence = {
	readonly fields: Readonly<Record<string, ReplicaValue>>;
	readonly clock: RecordProtocolClock;
	readonly projectionGeneration: number;
};

export type AnonymousRecordProtocolClock = {
	model: string;
	clock: RecordProtocolClock;
};

export type IndexProtocolClock = {
	scopeToken: DistributedOpaqueString;
	position: DistributedDecimalString;
};

export type OperationProtocolState = {
	operation: string;
	snapshotScope?: DistributedOpaqueString;
	indexClocks: Map<string, IndexProtocolClock>;
	indexRevision?: string;
	indexKeys: Set<string>;
	pathRecords: Map<string, string>;
	cursors: readonly DistributedLiveCursor[];
};

export type OperationProtocolSource = 'query' | 'live';

export type OperationProtocolGroup = {
	query?: OperationProtocolState;
	live?: OperationProtocolState;
	active?: OperationProtocolSource;
};

export type OptimisticReceiptState = {
	causationId: DistributedOpaqueString;
	expectations: ReadonlyMap<string, true>;
	observed: Set<string>;
};

export type IndexDisposition = 'fresh' | 'equal' | 'higher' | 'lower' | 'incomparable';

export type SharedIndexDisposition = {
	readonly compared: boolean;
	readonly disposition?: 'equal' | 'higher' | 'lower';
	readonly indexRevision?: string;
};

export type CapturedReplicaOptimisticOperation =
	| {
			readonly kind: 'write-record';
			readonly write: OptimisticRecordWrite;
	  }
	| {
			readonly kind: 'tombstone-record';
			readonly key: string;
	  }
	| {
			readonly kind: 'write-index';
			readonly write: OptimisticIndexWrite;
	  }
	| {
			readonly kind: 'delete-index';
			readonly key: string;
	  };

export type CapturedReplicaOptimisticUpdate = {
	readonly operations: readonly CapturedReplicaOptimisticOperation[];
	readonly context: CacheValue;
};
