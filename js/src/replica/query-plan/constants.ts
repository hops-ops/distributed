import type {
	ReplicaFilterEvaluation,
	ReplicaOrderComparison,
	ReplicaPaginationMaintenanceDecision
} from './types.js';

export const FILTER_MATCH = Object.freeze({ result: 'match' as const }) satisfies ReplicaFilterEvaluation;
export const FILTER_NO_MATCH = Object.freeze({ result: 'no_match' as const }) satisfies ReplicaFilterEvaluation;
export const ORDER_EQUAL = Object.freeze({ result: 'equal' as const }) satisfies ReplicaOrderComparison;
export const PAGINATION_LOCAL = Object.freeze({
	decision: 'local' as const
}) satisfies ReplicaPaginationMaintenanceDecision;
