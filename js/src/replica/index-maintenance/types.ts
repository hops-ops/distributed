import type { GraphqlVariables } from '../../types.js';
import type { DistributedTrustedPreset } from '../../protocol.js';
import type {
	ReplicaIndexCoverage,
	ReplicaIndexSemanticChange,
	ReplicaOperationArtifact,
	ReplicaValue
} from '../types.js';

export type {
	ReplicaIndexDependencyChange,
	ReplicaIndexRecordChange,
	ReplicaIndexRelationshipChange,
	ReplicaIndexSemanticChange
} from '../types.js';

export type ReplicaIndexMaintenanceRecord = {
	readonly key: string;
	readonly model: string;
	readonly fields: Readonly<Record<string, ReplicaValue>>;
};

export type ReplicaIndexMaintenanceIndex = {
	readonly key: string;
	readonly records: readonly string[];
	readonly complete: boolean;
	readonly metadata: {
		readonly parent?: string;
		readonly field: string;
		readonly arguments: Readonly<Record<string, ReplicaValue>>;
		readonly coverage: ReplicaIndexCoverage;
		readonly dependencies: readonly string[];
		readonly staleReason?: string;
	};
};

export type ReplicaIndexSemanticLayer = {
	readonly id: string;
	readonly changes: readonly ReplicaIndexSemanticChange[];
};

/**
 * Confirmed state, read by key. Evaluation looks up the records its layers
 * and affected indexes name, so its cost does not follow the cache size.
 */
export type ReplicaIndexMaintenanceSnapshot = {
	record(key: string): ReplicaIndexMaintenanceRecord | undefined;
	index(key: string): ReplicaIndexMaintenanceIndex | undefined;
	/** Listed only when a change may affect every index of one parent field. */
	indexes(): Iterable<ReplicaIndexMaintenanceIndex>;
};

export type ReplicaIndexMaintenanceReasonCode =
	| 'aggregate_dependency_changed'
	| 'conflicting_index_plan'
	| 'dependency_changed'
	| 'duplicate_index_record'
	| 'index_incomplete'
	| 'invalid_index_metadata'
	| 'missing_index_record'
	| 'missing_order_plan'
	| 'missing_parent_record'
	| 'non_unique_order'
	| 'relationship_dependency_missing'
	| 'relationship_maintenance_revalidate'
	| 'relationship_mapping_unknown'
	| 'unsupported_cardinality'
	| import('../query-plan.js').ReplicaQueryPlanReasonCode;

export type ReplicaIndexMaintenanceReason = {
	readonly code: ReplicaIndexMaintenanceReasonCode;
	readonly path: readonly (string | number)[];
	readonly message: string;
};

export type ReplicaIndexMaintenanceDecision =
	| {
			readonly kind: 'write';
			readonly indexKey: string;
			readonly records: readonly string[];
			readonly complete: true;
	  }
	| {
			readonly kind: 'stale';
			readonly indexKey: string;
			readonly reason: ReplicaIndexMaintenanceReason;
	  }
	| {
			readonly kind: 'unchanged';
			readonly indexKey: string;
	  };

export type ReplicaIndexPlanRegistration = {
	readonly id: string;
	dispose(): void;
};

export interface ReplicaIndexMaintenanceRegistry {
	registerOperation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): ReplicaIndexPlanRegistration;
	evaluate(
		snapshot: ReplicaIndexMaintenanceSnapshot,
		layers: readonly ReplicaIndexSemanticLayer[],
		trustedPresets?: readonly DistributedTrustedPreset[]
	): readonly ReplicaIndexMaintenanceDecision[];
	clear(): void;
}
