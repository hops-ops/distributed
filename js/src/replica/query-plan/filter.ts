import type { GraphqlVariables } from '../../types.js';

import type {
	ReplicaFilterArtifact,
	ReplicaFilterExpression,
	ReplicaFilterFieldArtifact,
	ReplicaFilterLiteral,
	ReplicaFilterOperator,
	ReplicaRelationshipArtifact,
	ReplicaRelationshipKeyMapping,
	ReplicaValue
} from '../types.js';
import {
	FILTER_MATCH,
	FILTER_NO_MATCH
} from './constants.js';
import {
	recordField,
	resolveInput,
	resolveOperand,
	uniqueStringList
} from './resolve.js';
import type {
	FilterCatalog,
	ReplicaFilterEvaluation,
	ReplicaFilterEvaluationOptions,
	ReplicaQueryPlanPath,
	ReplicaQueryPlanReason,
	ResolvedOperand
} from './types.js';
import {
	isFiniteNumber,
	isInt32,
	isName,
	isPortableNumericCodec,
	isRecord,
	isSafeIntegerNumber,
	isScalarCodecPair,
	reason
} from './util.js';

export function evaluateReplicaFilter(
	artifact: ReplicaFilterArtifact,
	record: Readonly<Record<string, ReplicaValue>>,
	variables: GraphqlVariables = {},
	options: ReplicaFilterEvaluationOptions = {}
): ReplicaFilterEvaluation {
	const catalog = filterCatalog(artifact);
	if ('reason' in catalog) return filterUnknown(catalog.reason);

	const input = resolveInput(artifact.input, variables, ['where']);
	const caller =
		input.kind === 'unknown'
			? filterUnknown(input.reason)
			: input.kind === 'omitted' || input.value === null
				? FILTER_MATCH
				: evaluateCallerWhere(
						input.value,
						record,
						catalog,
						options,
						['where']
					);
	const policy = evaluateRowPolicy(
		artifact,
		record,
		catalog,
		options,
		['rowPolicy']
	);
	return filterAnd([caller, policy]);
}

export function evaluateCallerWhere(
	value: ReplicaValue,
	record: Readonly<Record<string, ReplicaValue>>,
	catalog: FilterCatalog,
	options: ReplicaFilterEvaluationOptions,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	if (value === null) return FILTER_MATCH;
	if (!isRecord(value)) {
		return filterUnknown(
			reason(
				'invalid_filter_input',
				path,
				'where predicates must be objects'
			)
		);
	}
	const predicates: ReplicaFilterEvaluation[] = [];
	for (const [key, predicate] of Object.entries(value)) {
		const predicatePath = [...path, key];
		if (key === '_and' || key === '_or') {
			if (predicate === null) continue;
			const items = Array.isArray(predicate)
				? predicate
				: isRecord(predicate)
					? [predicate]
					: undefined;
			if (items === undefined) {
				predicates.push(
					filterUnknown(
						reason(
							'invalid_filter_input',
							predicatePath,
							`${key} must be an object or list of objects`
						)
					)
				);
				continue;
			}
			// The server treats both empty caller lists as no predicate (TRUE).
			if (items.length === 0) continue;
			const evaluated = items.map((item, index) =>
				evaluateCallerWhere(
					item,
					record,
					catalog,
					options,
					[...predicatePath, index]
				)
			);
			predicates.push(key === '_and' ? filterAnd(evaluated) : filterOr(evaluated));
			continue;
		}
		if (key === '_not') {
			if (predicate === null) {
				predicates.push(FILTER_NO_MATCH);
			} else {
				predicates.push(
					filterNot(
						evaluateCallerWhere(
							predicate,
							record,
							catalog,
							options,
							predicatePath
						)
					)
				);
			}
			continue;
		}

		const field = catalog.fields.get(key);
		if (field !== undefined) {
			predicates.push(
				evaluateCallerField(field, predicate, record, predicatePath)
			);
			continue;
		}
		const relationship = catalog.relationships.get(key);
		if (relationship !== undefined) {
			const resolver = options.resolveRelationship;
			predicates.push(
				resolver === undefined
					? filterUnknown(
							reason(
								'relationship_resolver_required',
								predicatePath,
								`relationship predicate ${key} requires an explicit resolver`
							)
						)
					: resolver({
							source: 'caller',
							relationship,
							predicate,
							record,
							path: Object.freeze(predicatePath)
						})
			);
			continue;
		}
		predicates.push(
			filterUnknown(
				reason(
					'unknown_field',
					predicatePath,
					`where field ${key} is not declared by the artifact`
				)
			)
		);
	}
	return filterAnd(predicates);
}

export function evaluateCallerField(
	field: ReplicaFilterFieldArtifact,
	value: ReplicaValue,
	record: Readonly<Record<string, ReplicaValue>>,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	if (value === null) return FILTER_MATCH;
	if (!isRecord(value)) {
		return filterUnknown(
			reason(
				'invalid_filter_input',
				path,
				`comparison expression for ${field.field} must be an object`
			)
		);
	}
	const predicates: ReplicaFilterEvaluation[] = [];
	for (const [rawOperator, rhs] of Object.entries(value)) {
		const operatorPath = [...path, rawOperator];
		if (
			!isFilterOperator(rawOperator) ||
			!field.operators.includes(rawOperator)
		) {
			predicates.push(
				filterUnknown(
					reason(
						'unsupported_operator',
						operatorPath,
						`operator ${rawOperator} is not declared for ${field.field}`
					)
				)
			);
			continue;
		}
		const left = recordField(record, field.field, operatorPath);
		if (left.kind === 'unknown') {
			predicates.push(filterUnknown(left.reason));
			continue;
		}
		predicates.push(
			evaluateClientOperator(field, rawOperator, left.value, rhs, operatorPath)
		);
	}
	return filterAnd(predicates);
}

export function evaluateClientOperator(
	field: ReplicaFilterFieldArtifact,
	operator: ReplicaFilterOperator,
	left: ReplicaValue,
	rhs: ReplicaValue,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	if (operator === '_is_null') {
		if (rhs !== null && typeof rhs !== 'boolean') {
			return filterUnknown(
				reason(
					'invalid_filter_value',
					path,
					'_is_null must be a boolean or null'
				)
			);
		}
		const wantsNull = rhs === true;
		return (left === null) === wantsNull ? FILTER_MATCH : FILTER_NO_MATCH;
	}
	if (operator === '_in' || operator === '_nin') {
		if (rhs === null) {
			return filterUnknown(
				reason('invalid_filter_value', path, `${operator} requires a list`)
			);
		}
		const values = Array.isArray(rhs) ? rhs : [rhs];
		return evaluateIn(field.codec, left, values, operator === '_nin', path);
	}
	return evaluateComparison(field.codec, operator, left, rhs, path);
}

export function evaluateRowPolicy(
	artifact: ReplicaFilterArtifact,
	record: Readonly<Record<string, ReplicaValue>>,
	catalog: FilterCatalog,
	options: ReplicaFilterEvaluationOptions,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	const policy = artifact.rowPolicy;
	if (policy.kind === 'unrestricted') return FILTER_MATCH;
	if (policy.kind === 'server_only') {
		return filterUnknown(
			reason(
				'server_only_policy',
				path,
				'server-only row policy cannot be evaluated in the replica'
			)
		);
	}
	if (policy.kind !== 'predicate' || !isRecord(policy.expression)) {
		return filterUnknown(
			reason('invalid_artifact', path, 'row policy artifact is malformed')
		);
	}
	return evaluatePolicyExpression(
		policy.expression,
		record,
		catalog,
		options,
		[...path, 'expression']
	);
}

export function evaluatePolicyExpression(
	expression: ReplicaFilterExpression,
	record: Readonly<Record<string, ReplicaValue>>,
	catalog: FilterCatalog,
	options: ReplicaFilterEvaluationOptions,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	if (expression.kind === 'and' || expression.kind === 'or') {
		if (!Array.isArray(expression.value)) {
			return filterUnknown(
				reason('invalid_artifact', path, 'row-policy boolean value must be a list')
			);
		}
		const evaluated = expression.value.map((item, index) =>
			evaluatePolicyExpression(
				item,
				record,
				catalog,
				options,
				[...path, expression.kind, index]
			)
		);
		// Unlike caller `_or: []`, a row-policy Or([]) is SQL FALSE.
		return expression.kind === 'and' ? filterAnd(evaluated) : filterOr(evaluated);
	}
	if (expression.kind === 'not') {
		return filterNot(
			evaluatePolicyExpression(
				expression.value,
				record,
				catalog,
				options,
				[...path, 'not']
			)
		);
	}
	if (expression.kind === 'rel') {
		const field = expression.value.field;
		const relationshipPath = [...path, 'rel', field];
		const relationship = catalog.relationships.get(field);
		if (relationship === undefined) {
			return filterUnknown(
				reason(
					'unknown_relationship',
					relationshipPath,
					`row policy relationship ${field} is not declared by the artifact`
				)
			);
		}
		const resolver = options.resolveRelationship;
		return resolver === undefined
			? filterUnknown(
					reason(
						'relationship_resolver_required',
						relationshipPath,
						`row policy relationship ${field} requires an explicit resolver`
					)
				)
			: resolver({
					source: 'row_policy',
					relationship,
					predicate: expression.value.predicate,
					record,
					path: Object.freeze(relationshipPath)
				});
	}
	if (
		expression.kind !== 'cmp' &&
		expression.kind !== 'in' &&
		expression.kind !== 'is_null'
	) {
		return filterUnknown(
			reason('invalid_artifact', path, 'row policy expression kind is unsupported')
		);
	}

	const column = expression.value.column;
	const field = catalog.fields.get(column);
	const columnPath = [...path, expression.kind, column];
	if (field === undefined) {
		return filterUnknown(
			reason(
				'unknown_field',
				columnPath,
				`row policy field ${column} is not declared by the artifact`
			)
		);
	}
	const left = recordField(record, column, columnPath);
	if (left.kind === 'unknown') return filterUnknown(left.reason);
	if (expression.kind === 'is_null') {
		if (typeof expression.value.is_null !== 'boolean') {
			return filterUnknown(
				reason(
					'invalid_artifact',
					[...columnPath, 'is_null'],
					'row-policy is_null must be a boolean'
				)
			);
		}
		return (left.value === null) === expression.value.is_null
			? FILTER_MATCH
			: FILTER_NO_MATCH;
	}
	if (expression.kind === 'cmp') {
		const operator = `_${expression.value.op}` as ReplicaFilterOperator;
		if (
			!isFilterOperator(operator) ||
			!isOperatorScalarCompatible(field.scalar, operator)
		) {
			return filterUnknown(
				reason(
					'invalid_artifact',
					[...columnPath, 'op'],
					`row-policy operator ${expression.value.op} cannot target ${field.scalar}`
				)
			);
		}
		const operand = resolveOperand(
			expression.value.rhs,
			field,
			options,
			[...columnPath, 'rhs']
		);
		if (operand.kind === 'unknown') return filterUnknown(operand.reason);
		const literalValidity = validateRowPolicyLiteral(
			field,
			operand,
			[...columnPath, 'rhs'],
			operator
		);
		if (literalValidity !== undefined) return filterUnknown(literalValidity);
		return evaluateComparison(
			field.codec,
			`_${expression.value.op}` as ReplicaFilterOperator,
			left.value,
			operand.value,
			columnPath
		);
	}
	if (expression.kind === 'in') {
		if (
			typeof expression.value.negated !== 'boolean' ||
			!Array.isArray(expression.value.values)
		) {
			return filterUnknown(
				reason(
					'invalid_artifact',
					columnPath,
					'row-policy IN values must be a list and negated must be a boolean'
				)
			);
		}
		const values: ReplicaValue[] = [];
		for (const [index, operand] of expression.value.values.entries()) {
			const operandPath = [...columnPath, 'values', index];
			const resolved = resolveOperand(operand, field, options, operandPath);
			if (resolved.kind === 'unknown') return filterUnknown(resolved.reason);
			const literalValidity = validateRowPolicyLiteral(
				field,
				resolved,
				operandPath
			);
			if (literalValidity !== undefined) return filterUnknown(literalValidity);
			values.push(resolved.value);
		}
		return evaluateIn(
			field.codec,
			left.value,
			values,
			expression.value.negated,
			columnPath
		);
	}
	return filterUnknown(
		reason('invalid_artifact', path, 'row policy expression kind is unsupported')
	);
}

export function evaluateComparison(
	codec: string,
	operator: ReplicaFilterOperator,
	left: ReplicaValue,
	right: ReplicaValue,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	if (
		operator === '_like' ||
		operator === '_ilike' ||
		operator === '_icontains' ||
		operator === '_contains' ||
		operator === '_contained_in' ||
		operator === '_has_key'
	) {
		return filterUnknown(
			reason(
				'unsupported_operator',
				path,
				`${operator} has no certified cross-dialect replica evaluator`
			)
		);
	}
	if (left === null || right === null) {
		return filterUnknown(
			reason('sql_null', path, 'SQL comparison with null is unknown')
		);
	}
	const validity = validateComparableValues(codec, left, right, path);
	if (validity !== undefined) return filterUnknown(validity);
	if (operator === '_eq' || operator === '_neq') {
		if (codec === 'string' && left !== right) {
			return filterUnknown(
				reason(
					'collation_not_portable',
					path,
					'non-identical strings require an explicit server collation contract'
				)
			);
		}
		const equal = left === right;
		return equal === (operator === '_eq') ? FILTER_MATCH : FILTER_NO_MATCH;
	}
	if (
		operator !== '_gt' &&
		operator !== '_gte' &&
		operator !== '_lt' &&
		operator !== '_lte'
	) {
		return filterUnknown(
			reason('unsupported_operator', path, `operator ${operator} is not portable`)
		);
	}
	if (!isPortableNumericCodec(codec)) {
		return filterUnknown(
			reason(
				'unsupported_operator',
				path,
				`${operator} is portable only for certified numeric codecs`
			)
		);
	}
	const a = left as number;
	const b = right as number;
	const matches =
		operator === '_gt'
			? a > b
			: operator === '_gte'
				? a >= b
				: operator === '_lt'
					? a < b
					: a <= b;
	return matches ? FILTER_MATCH : FILTER_NO_MATCH;
}

export function evaluateIn(
	codec: string,
	left: ReplicaValue,
	values: readonly ReplicaValue[],
	negated: boolean,
	path: ReplicaQueryPlanPath
): ReplicaFilterEvaluation {
	// Server compilation special-cases empty lists before evaluating the column.
	if (values.length === 0) return negated ? FILTER_MATCH : FILTER_NO_MATCH;
	if (left === null) {
		return filterUnknown(
			reason('sql_null', path, 'SQL IN comparison with a null column is unknown')
		);
	}
	// GraphQL rejects a type-invalid list before SQL runs. Validate every
	// non-null candidate before allowing a later equality to establish truth.
	// SQL NULL is different: a concrete equality may still dominate UNKNOWN.
	for (const [index, candidate] of values.entries()) {
		if (candidate === null) continue;
		const validity = validateComparableValues(
			codec,
			left,
			candidate,
			[...path, index]
		);
		if (validity !== undefined) return filterUnknown(validity);
	}
	let firstUnknown: ReplicaQueryPlanReason | undefined;
	for (const [index, candidate] of values.entries()) {
		if (candidate === null) {
			firstUnknown ??= reason(
				'sql_null',
				[...path, index],
				'SQL IN list containing null can produce an unknown result'
			);
			continue;
		}
		if (left === candidate) return negated ? FILTER_NO_MATCH : FILTER_MATCH;
		if (codec === 'string') {
			firstUnknown ??= reason(
				'collation_not_portable',
				[...path, index],
				'non-identical strings in SQL IN require an explicit server collation contract'
			);
		}
	}
	if (firstUnknown !== undefined) return filterUnknown(firstUnknown);
	return negated ? FILTER_MATCH : FILTER_NO_MATCH;
}

export function validateComparableValues(
	codec: string,
	left: ReplicaValue,
	right: ReplicaValue,
	path: ReplicaQueryPlanPath
): ReplicaQueryPlanReason | undefined {
	if (codec === 'string') {
		return typeof left === 'string' && typeof right === 'string'
			? undefined
			: reason(
					'invalid_filter_value',
					path,
					'string codec requires string values'
				);
	}
	if (codec === 'boolean') {
		return typeof left === 'boolean' && typeof right === 'boolean'
			? undefined
			: reason(
					'invalid_filter_value',
					path,
					'boolean codec requires boolean values'
				);
	}
	if (codec === 'int32') {
		return isInt32(left) && isInt32(right)
			? undefined
			: reason(
					'invalid_filter_value',
					path,
					'int32 codec requires signed 32-bit integer values'
				);
	}
	if (codec === 'float64') {
		return isFiniteNumber(left) && isFiniteNumber(right)
			? undefined
			: reason(
					'invalid_filter_value',
					path,
					'float64 codec requires finite number values'
				);
	}
	if (codec === 'json_number_precision_limited') {
		return isSafeIntegerNumber(left) && isSafeIntegerNumber(right)
			? undefined
			: reason(
					'invalid_filter_value',
					path,
					'BigInt codec requires JavaScript-safe integer values'
				);
	}
	return reason(
		'unsupported_codec',
		path,
		`codec ${codec} has no certified replica comparison semantics`
	);
}

/**
 * Protocol v1 orders strings by their unsigned UTF-8 bytes. The GraphQL SQL
 * compiler emits the matching binary collation for every textual ORDER BY,
 * making optimistic index maintenance identical on SQLite and PostgreSQL.
 */

export function validateRowPolicyLiteral(
	field: ReplicaFilterFieldArtifact,
	operand: Extract<ResolvedOperand, { readonly kind: 'value' }>,
	path: ReplicaQueryPlanPath,
	operator?: ReplicaFilterOperator
): ReplicaQueryPlanReason | undefined {
	if (operand.source === 'trusted_preset') return undefined;
	const literalKind = operand.literalKind;
	if (literalKind === undefined) {
		return reason(
			'invalid_artifact',
			path,
			'row-policy literal is missing its tagged kind'
		);
	}
	if (literalKind === 'null') return undefined;
	const expected: readonly ReplicaFilterLiteral['kind'][] | undefined =
		field.scalar === 'ID' ||
		field.scalar === 'String' ||
		field.scalar === 'Timestamptz'
			? ['string']
			: field.scalar === 'Boolean'
				? ['bool']
				: field.scalar === 'Int' || field.scalar === 'BigInt'
					? ['i64']
					: field.scalar === 'Float'
						? ['f64', 'i64']
						: field.scalar === 'JSON'
							? operator === '_has_key'
								? ['string']
								: ['json']
							: undefined;
	if (expected?.includes(literalKind)) return undefined;
	return reason(
		'invalid_artifact',
		path,
		expected === undefined
			? `scalar ${field.scalar} has no non-null row-policy literal encoding`
			: `row-policy literal kind ${literalKind} cannot target ${field.scalar}; expected ${expected.join(' or ')}`
	);
}

export function filterCatalog(
	artifact: ReplicaFilterArtifact
):
	| FilterCatalog
	| {
			readonly reason: ReplicaQueryPlanReason;
	  } {
	const fields = new Map<string, ReplicaFilterFieldArtifact>();
	for (const [index, field] of artifact.fields.entries()) {
		if (
			!isName(field.field) ||
			!isScalarCodecPair(field.scalar, field.codec) ||
			typeof field.nullable !== 'boolean' ||
			!Array.isArray(field.operators) ||
			fields.has(field.field)
		) {
			return {
				reason: reason(
					'invalid_artifact',
					['filter', 'fields', index],
					'filter fields must have unique names, exact scalar-codec pairs, nullability, and operators'
				)
			};
		}
		const operators = new Set<ReplicaFilterOperator>();
		for (const [operatorIndex, operator] of field.operators.entries()) {
			if (
				!isFilterOperator(operator) ||
				!isOperatorScalarCompatible(field.scalar, operator) ||
				operators.has(operator)
			) {
				return {
					reason: reason(
						'invalid_artifact',
						['filter', 'fields', index, 'operators', operatorIndex],
						'filter operators must be unique and compatible with the field scalar'
					)
				};
			}
			operators.add(operator);
		}
		fields.set(field.field, field);
	}
	const relationships = new Map<string, ReplicaRelationshipArtifact>();
	for (const [index, value] of artifact.relationships.entries()) {
		const parsed = relationshipArtifact(value, [
			'filter',
			'relationships',
			index
		]);
		if ('reason' in parsed) return parsed;
		const relationship = parsed.value;
		if (relationships.has(relationship.field)) {
			return {
				reason: reason(
					'invalid_artifact',
					['filter', 'relationships', index],
					'filter relationship descriptors must have unique fields'
				)
			};
		}
		relationships.set(relationship.field, relationship);
	}
	return { fields, relationships };
}

export function relationshipArtifact(
	value: unknown,
	path: ReplicaQueryPlanPath
):
	| { readonly value: ReplicaRelationshipArtifact }
	| { readonly reason: ReplicaQueryPlanReason } {
	if (
		!isRecord(value) ||
		!isName(value.field) ||
		!isName(value.targetModel) ||
		(value.kind !== 'has_many' &&
			value.kind !== 'belongs_to' &&
			value.kind !== 'many_to_many') ||
		(value.maintenance !== 'local' && value.maintenance !== 'revalidate')
	) {
		return {
			reason: reason(
				'invalid_artifact',
				path,
				'relationship descriptor has invalid field, target, kind, or maintenance'
			)
		};
	}
	const dependencies = uniqueStringList(value.dependencies);
	if (dependencies === undefined || dependencies.length === 0) {
		return {
			reason: reason(
				'invalid_artifact',
				[...path, 'dependencies'],
				'relationship dependencies must be a non-empty unique string list'
			)
		};
	}
	const keyMapping = relationshipKeyMapping(
		value.keyMapping,
		dependencies,
		[...path, 'keyMapping']
	);
	if ('reason' in keyMapping) return keyMapping;
	const localMapping =
		keyMapping.value.kind === 'direct' || keyMapping.value.kind === 'through';
	if (!localMapping && value.maintenance !== 'revalidate') {
		return {
			reason: reason(
				'invalid_artifact',
				[...path, 'maintenance'],
				'opaque and embedded mappings must use revalidation maintenance'
			)
		};
	}
	return {
		value: Object.freeze({
			field: value.field,
			targetModel: value.targetModel,
			kind: value.kind,
			keyMapping: keyMapping.value,
			maintenance: value.maintenance,
			dependencies
		})
	};
}

export function relationshipKeyMapping(
	value: unknown,
	dependencies: readonly string[],
	path: ReplicaQueryPlanPath
):
	| { readonly value: ReplicaRelationshipKeyMapping }
	| { readonly reason: ReplicaQueryPlanReason } {
	if (!isRecord(value) || !isName(value.kind)) {
		return {
			reason: reason(
				'invalid_artifact',
				path,
				'relationship key mapping must declare a supported kind'
			)
		};
	}
	if (value.kind === 'embedded') {
		return { value: Object.freeze({ kind: 'embedded' as const }) };
	}
	const local = uniqueStringList(value.local);
	const remote = uniqueStringList(value.remote);
	if (
		local === undefined ||
		remote === undefined ||
		local.length === 0 ||
		local.length !== remote.length
	) {
		return {
			reason: reason(
				'invalid_artifact',
				path,
				'relationship key lists must be unique, non-empty, and equally sized'
			)
		};
	}
	if (value.kind === 'direct') {
		return {
			value: Object.freeze({ kind: 'direct' as const, local, remote })
		};
	}
	const sourceForeignKey = uniqueStringList(value.sourceForeignKey);
	const targetForeignKey = uniqueStringList(value.targetForeignKey);
	if (
		value.kind === 'through' &&
		isName(value.table) &&
		dependencies.includes(value.table) &&
		sourceForeignKey !== undefined &&
		targetForeignKey !== undefined &&
		sourceForeignKey.length === local.length &&
		targetForeignKey.length === remote.length
	) {
		return {
			value: Object.freeze({
				kind: 'through' as const,
				local,
				remote,
				table: value.table,
				sourceForeignKey,
				targetForeignKey
			})
		};
	}
	if (
		value.kind === 'through_opaque' &&
		isName(value.dependency) &&
		dependencies.includes(value.dependency)
	) {
		return {
			value: Object.freeze({
				kind: 'through_opaque' as const,
				local,
				remote,
				dependency: value.dependency
			})
		};
	}
	return {
		reason: reason(
			'invalid_artifact',
			path,
			'relationship key mapping is incomplete or references an undeclared dependency'
		)
	};
}

export function filterAnd(
	values: readonly ReplicaFilterEvaluation[]
): ReplicaFilterEvaluation {
	let firstUnknown: ReplicaQueryPlanReason | undefined;
	for (const value of values) {
		if (value.result === 'no_match') return FILTER_NO_MATCH;
		if (value.result === 'unknown') firstUnknown ??= value.reason;
	}
	return firstUnknown === undefined ? FILTER_MATCH : filterUnknown(firstUnknown);
}

export function filterOr(
	values: readonly ReplicaFilterEvaluation[]
): ReplicaFilterEvaluation {
	let firstUnknown: ReplicaQueryPlanReason | undefined;
	for (const value of values) {
		if (value.result === 'match') return FILTER_MATCH;
		if (value.result === 'unknown') firstUnknown ??= value.reason;
	}
	return firstUnknown === undefined ? FILTER_NO_MATCH : filterUnknown(firstUnknown);
}

export function filterNot(value: ReplicaFilterEvaluation): ReplicaFilterEvaluation {
	if (value.result === 'unknown') return value;
	return value.result === 'match' ? FILTER_NO_MATCH : FILTER_MATCH;
}

export function filterUnknown(reasonValue: ReplicaQueryPlanReason): ReplicaFilterEvaluation {
	return Object.freeze({ result: 'unknown' as const, reason: reasonValue });
}

export function isFilterOperator(value: string): value is ReplicaFilterOperator {
	return (
		value === '_eq' ||
		value === '_neq' ||
		value === '_gt' ||
		value === '_gte' ||
		value === '_lt' ||
		value === '_lte' ||
		value === '_in' ||
		value === '_nin' ||
		value === '_is_null' ||
		value === '_like' ||
		value === '_ilike' ||
		value === '_icontains' ||
		value === '_contains' ||
		value === '_contained_in' ||
		value === '_has_key'
	);
}

export function isOperatorScalarCompatible(
	scalar: string,
	operator: ReplicaFilterOperator
): boolean {
	if (operator === '_like' || operator === '_ilike' || operator === '_icontains') {
		return scalar === 'String';
	}
	if (
		operator === '_contains' ||
		operator === '_contained_in' ||
		operator === '_has_key'
	) {
		return scalar === 'JSON';
	}
	return true;
}
