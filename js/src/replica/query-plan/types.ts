
import type { DistributedTrustedPreset } from '../../protocol.js';
import type {
	ReplicaFilterExpression,
	ReplicaFilterFieldArtifact,
	ReplicaFilterLiteral,
	ReplicaRelationshipArtifact,
	ReplicaSurfaceTrustedPresetDescriptor,
	ReplicaValue
} from '../types.js';

export type ReplicaQueryPlanPath = readonly (string | number)[];

export type ReplicaQueryPlanReasonCode =
	| 'ambiguous_order_entry'
	| 'claim_operand'
	| 'claim_inventory'
	| 'coverage_mismatch'
	| 'cursor_not_certified'
	| 'delete_changes_offset_window'
	| 'implicit_null_order'
	| 'invalid_argument_value'
	| 'insert_changes_offset_window'
	| 'invalid_artifact'
	| 'invalid_filter_input'
	| 'invalid_filter_value'
	| 'invalid_order_direction'
	| 'invalid_order_input'
	| 'invalid_order_value'
	| 'invalid_pagination_policy'
	| 'missing_field'
	| 'relationship_resolver_required'
	| 'reorder_changes_offset_window'
	| 'server_only_policy'
	| 'sql_null'
	| 'unknown_coverage'
	| 'unknown_field'
	| 'unknown_relationship'
	| 'unsupported_codec'
	| 'unsupported_operator';

export type ReplicaQueryPlanReason = {
	readonly code: ReplicaQueryPlanReasonCode;
	readonly path: ReplicaQueryPlanPath;
	readonly message: string;
};

export type ReplicaFilterEvaluation =
	| { readonly result: 'match' | 'no_match' }
	| {
			readonly result: 'unknown';
			readonly reason: ReplicaQueryPlanReason;
	  };

export type ReplicaRelationshipFilterRequest =
	| {
			readonly source: 'caller';
			readonly relationship: ReplicaRelationshipArtifact;
			readonly predicate: ReplicaValue;
			readonly record: Readonly<Record<string, ReplicaValue>>;
			readonly path: ReplicaQueryPlanPath;
	  }
	| {
			readonly source: 'row_policy';
			readonly relationship: ReplicaRelationshipArtifact;
			readonly predicate: ReplicaFilterExpression;
			readonly record: Readonly<Record<string, ReplicaValue>>;
			readonly path: ReplicaQueryPlanPath;
	  };

export type ReplicaFilterEvaluationOptions = {
	/**
	 * Resolves the server's EXISTS semantics for a relationship predicate.
	 * The resolver is responsible for the target model's row policy as well as
	 * the supplied predicate; omitting it deliberately produces `unknown`.
	 */
	readonly resolveRelationship?: (
		request: ReplicaRelationshipFilterRequest
	) => ReplicaFilterEvaluation;
	/**
	 * Static compiler-owned descriptor union paired with the current
	 * server-derived, cache-scope-bound values. The evaluator never reads
	 * ambient token claims or caller headers.
	 */
	readonly trustedPresets?: {
		readonly descriptors: readonly ReplicaSurfaceTrustedPresetDescriptor[];
		readonly values: readonly DistributedTrustedPreset[];
	};
};

export type ReplicaOrderComparison =
	| { readonly result: 'less' | 'equal' | 'greater' }
	| {
			readonly result: 'unknown';
			readonly reason: ReplicaQueryPlanReason;
	  };

export type ReplicaPaginationChange =
	| { readonly kind: 'insert' }
	| { readonly kind: 'delete' }
	| { readonly kind: 'reorder' }
	| { readonly kind: 'stable_update' };

export type ReplicaPaginationMaintenanceDecision =
	| { readonly decision: 'local' }
	| {
			readonly decision: 'revalidate';
			readonly reason: ReplicaQueryPlanReason;
	  };

export type FilterCatalog = {
	readonly fields: ReadonlyMap<string, ReplicaFilterFieldArtifact>;
	readonly relationships: ReadonlyMap<string, ReplicaRelationshipArtifact>;
};

export type ResolvedInput =
	| { readonly kind: 'omitted' }
	| { readonly kind: 'value'; readonly value: ReplicaValue }
	| { readonly kind: 'unknown'; readonly reason: ReplicaQueryPlanReason };

export type ResolvedOperand =
	| {
			readonly kind: 'value';
			readonly value: ReplicaValue;
			readonly literalKind?: ReplicaFilterLiteral['kind'];
			readonly source: 'literal' | 'trusted_preset';
	  }
	| { readonly kind: 'unknown'; readonly reason: ReplicaQueryPlanReason };

export type OrderDirection =
	| 'asc'
	| 'asc_nulls_first'
	| 'asc_nulls_last'
	| 'desc'
	| 'desc_nulls_first'
	| 'desc_nulls_last';
