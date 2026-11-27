import type { GraphqlVariables } from '../../types.js';
import type {
	ReplicaOrderArtifact,
	ReplicaOrderFieldArtifact,
	ReplicaValue
} from '../types.js';
import { ORDER_EQUAL } from './constants.js';
import { recordField, resolveInput } from './resolve.js';
import { compareUtf8 } from '../../lib/compare-utf8.js';
import type {
	OrderDirection,
	ReplicaOrderComparison,
	ReplicaQueryPlanPath,
	ReplicaQueryPlanReason
} from './types.js';
import {
	isName,
	isPortableNumericCodec,
	isRecord,
	isScalarCodecPair,
	reason
} from './util.js';
import { validateComparableValues } from './filter.js';

export function compareReplicaOrder(
	artifact: ReplicaOrderArtifact,
	left: Readonly<Record<string, ReplicaValue>>,
	right: Readonly<Record<string, ReplicaValue>>,
	variables: GraphqlVariables = {}
): ReplicaOrderComparison {
	const fields = new Map<string, ReplicaOrderFieldArtifact>();
	for (const [index, field] of artifact.fields.entries()) {
		if (
			!isName(field.field) ||
			!isScalarCodecPair(field.scalar, field.codec) ||
			typeof field.nullable !== 'boolean' ||
			fields.has(field.field)
		) {
			return orderUnknown(
				reason(
					'invalid_artifact',
					['order', 'fields', index],
					'order fields must have unique names, exact scalar-codec pairs, and nullability'
				)
			);
		}
		fields.set(field.field, field);
	}
	const tieBreakerFields = new Set<string>();
	for (const [index, tieBreaker] of artifact.tieBreakers.entries()) {
		if (
			!isName(tieBreaker.field) ||
			!isScalarCodecPair(tieBreaker.scalar, tieBreaker.codec) ||
			tieBreaker.nullable !== false ||
			tieBreakerFields.has(tieBreaker.field)
		) {
			return orderUnknown(
				reason(
					'invalid_artifact',
					['order', 'tieBreakers', index],
					'order tie-breakers must have unique fields and exact scalar-codec pairs'
				)
			);
		}
		tieBreakerFields.add(tieBreaker.field);
	}

	const input = resolveInput(artifact.input, variables, ['order_by']);
	if (input.kind === 'unknown') return orderUnknown(input.reason);
	const entries: readonly ReplicaValue[] =
		input.kind === 'omitted' || input.value === null
			? []
			: Array.isArray(input.value)
				? input.value
				: isRecord(input.value)
					? [input.value]
					: [];
	if (
		input.kind === 'value' &&
		input.value !== null &&
		!Array.isArray(input.value) &&
		!isRecord(input.value)
	) {
		return orderUnknown(
			reason(
				'invalid_order_input',
				['order_by'],
				'order_by must be an object or a list of objects'
			)
		);
	}

	for (const [index, entry] of entries.entries()) {
		const path: ReplicaQueryPlanPath = ['order_by', index];
		if (!isRecord(entry)) {
			return orderUnknown(
				reason(
					'invalid_order_input',
					path,
					'each order_by entry must be an object'
				)
			);
		}
		const pairs = Object.entries(entry);
		if (pairs.length === 0) continue;
		if (pairs.length > 1) {
			return orderUnknown(
				reason(
					'ambiguous_order_entry',
					path,
					'each order_by entry may declare at most one field'
				)
			);
		}
		const [fieldName, rawDirection] = pairs[0]!;
		const field = fields.get(fieldName);
		if (field === undefined) {
			return orderUnknown(
				reason(
					'unknown_field',
					[...path, fieldName],
					`order field ${fieldName} is not declared by the artifact`
				)
			);
		}
		if (!isOrderDirection(rawDirection)) {
			return orderUnknown(
				reason(
					'invalid_order_direction',
					[...path, fieldName],
					`order direction for ${fieldName} is not supported`
				)
			);
		}
		if (field.nullable && !hasExplicitNullPlacement(rawDirection)) {
			return orderUnknown(
				reason(
					'implicit_null_order',
					[...path, fieldName],
					`nullable order field ${fieldName} requires explicit null placement`
				)
			);
		}
		const compared = compareOrderField(
			field.field,
			field.codec,
			field.nullable,
			rawDirection,
			left,
			right,
			[...path, fieldName]
		);
		if (compared.result !== 'equal') return compared;
	}

	for (const [index, tieBreaker] of artifact.tieBreakers.entries()) {
		const compared = compareOrderField(
			tieBreaker.field,
			tieBreaker.codec,
			false,
			'asc',
			left,
			right,
			['order', 'tieBreakers', index, tieBreaker.field]
		);
		if (compared.result !== 'equal') return compared;
	}
	if (artifact.tieBreakers.length === 0) {
		return orderUnknown(
			reason(
				'invalid_artifact',
				['order', 'tieBreakers'],
				'equal order values require a complete identity tie-breaker'
			)
		);
	}
	return ORDER_EQUAL;
}

export function compareOrderField(
	field: string,
	codec: string,
	nullable: boolean,
	direction: OrderDirection,
	left: Readonly<Record<string, ReplicaValue>>,
	right: Readonly<Record<string, ReplicaValue>>,
	path: ReplicaQueryPlanPath
): ReplicaOrderComparison {
	const a = recordField(left, field, path);
	if (a.kind === 'unknown') return orderUnknown(a.reason);
	const b = recordField(right, field, path);
	if (b.kind === 'unknown') return orderUnknown(b.reason);
	if ((a.value === null || b.value === null) && !nullable) {
		return orderUnknown(
			reason(
				'invalid_order_value',
				path,
				`non-null order field ${field} contains null`
			)
		);
	}
	if (a.value === null || b.value === null) {
		if (a.value === null && b.value === null) return ORDER_EQUAL;
		if (!hasExplicitNullPlacement(direction)) {
			return orderUnknown(
				reason(
					'implicit_null_order',
					path,
					`nullable order field ${field} requires explicit null placement`
				)
			);
		}
		const nullFirst = direction.endsWith('_nulls_first');
		return orderResult((a.value === null) === nullFirst ? -1 : 1);
	}

	const validity = validateComparableValues(codec, a.value, b.value, path);
	if (validity !== undefined) return orderUnknown(validity);
	let comparison: -1 | 0 | 1;
	if (isPortableNumericCodec(codec)) {
		comparison =
			(a.value as number) < (b.value as number)
				? -1
				: (a.value as number) > (b.value as number)
					? 1
					: 0;
	} else if (codec === 'boolean') {
		comparison =
			a.value === b.value ? 0 : a.value === false && b.value === true ? -1 : 1;
	} else if (codec === 'string') {
		comparison = compareUtf8(
			a.value as string,
			b.value as string
		);
	} else {
		return orderUnknown(
			reason(
				'unsupported_codec',
				path,
				`codec ${codec} has no certified replica comparator`
			)
		);
	}
	if (comparison === 0) return ORDER_EQUAL;
	return orderResult(direction.startsWith('desc') ? invert(comparison) : comparison);
}

export function orderUnknown(reasonValue: ReplicaQueryPlanReason): ReplicaOrderComparison {
	return Object.freeze({ result: 'unknown' as const, reason: reasonValue });
}

export function orderResult(comparison: -1 | 1): ReplicaOrderComparison {
	return Object.freeze({
		result: comparison < 0 ? ('less' as const) : ('greater' as const)
	});
}

export function isOrderDirection(value: ReplicaValue): value is OrderDirection {
	return (
		value === 'asc' ||
		value === 'asc_nulls_first' ||
		value === 'asc_nulls_last' ||
		value === 'desc' ||
		value === 'desc_nulls_first' ||
		value === 'desc_nulls_last'
	);
}

export function hasExplicitNullPlacement(direction: OrderDirection): boolean {
	return direction.includes('_nulls_');
}

export function invert(value: -1 | 1): -1 | 1 {
	return value === -1 ? 1 : -1;
}
