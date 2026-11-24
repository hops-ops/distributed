import type { GraphqlVariables } from '../../types.js';
import type { DistributedTrustedPreset } from '../../protocol.js';
import type {
	ReplicaArgumentValue,
	ReplicaFilterFieldArtifact,
	ReplicaFilterOperand,
	ReplicaSurfaceTrustedPresetDescriptor,
	ReplicaValue
} from '../types.js';
import { resolveReplicaArgumentValue } from '../identity/keys.js';
import type {
	ReplicaFilterEvaluationOptions,
	ReplicaQueryPlanPath,
	ReplicaQueryPlanReason,
	ResolvedInput,
	ResolvedOperand
} from './types.js';
import {
	isFiniteNumber,
	isName,
	isRecord,
	isReplicaValue,
	reason
} from './util.js';

export function resolveInput(
	input: ReplicaArgumentValue | undefined,
	variables: GraphqlVariables,
	path: ReplicaQueryPlanPath
): ResolvedInput {
	if (input === undefined) return { kind: 'omitted' };
	try {
		const value = resolveReplicaArgumentValue(input, variables);
		return value === undefined
			? { kind: 'omitted' }
			: { kind: 'value', value };
	} catch (error) {
		return {
			kind: 'unknown',
			reason: reason(
				'invalid_argument_value',
				path,
				error instanceof Error
					? error.message
					: 'plan input could not be resolved'
			)
		};
	}
}

export function resolveOperand(
	operand: ReplicaFilterOperand,
	field: ReplicaFilterFieldArtifact,
	options: ReplicaFilterEvaluationOptions,
	path: ReplicaQueryPlanPath
): ResolvedOperand {
	if (operand.kind === 'claim') {
		return resolveTrustedPresetOperand(
			operand.value.header,
			field,
			options,
			path
		);
	}
	if (operand.kind !== 'lit' || !isRecord(operand.value)) {
		return {
			kind: 'unknown',
			reason: reason('invalid_artifact', path, 'row-policy operand is malformed')
		};
	}
	const literal = operand.value;
	if (literal.kind === 'null') {
		return {
			kind: 'value',
			value: null,
			literalKind: literal.kind,
			source: 'literal'
		};
	}
	if (
		literal.kind === 'string' &&
		typeof literal.value === 'string'
	) {
		return {
			kind: 'value',
			value: literal.value,
			literalKind: literal.kind,
			source: 'literal'
		};
	}
	if (literal.kind === 'bool' && typeof literal.value === 'boolean') {
		return {
			kind: 'value',
			value: literal.value,
			literalKind: literal.kind,
			source: 'literal'
		};
	}
	if (
		(literal.kind === 'i64' && Number.isSafeInteger(literal.value)) ||
		(literal.kind === 'f64' && isFiniteNumber(literal.value))
	) {
		return {
			kind: 'value',
			value: literal.value,
			literalKind: literal.kind,
			source: 'literal'
		};
	}
	if (literal.kind === 'json' && isReplicaValue(literal.value)) {
		return {
			kind: 'value',
			value: literal.value,
			literalKind: literal.kind,
			source: 'literal'
		};
	}
	return {
		kind: 'unknown',
		reason: reason(
			'invalid_artifact',
			path,
			'row-policy literal is not a portable JSON value'
		)
	};
}

export function resolveTrustedPresetOperand(
	name: string,
	field: ReplicaFilterFieldArtifact,
	options: ReplicaFilterEvaluationOptions,
	path: ReplicaQueryPlanPath
): ResolvedOperand {
	const inventory = options.trustedPresets;
	if (inventory === undefined) {
		return {
			kind: 'unknown',
			reason: reason(
				'claim_operand',
				path,
				`claim ${name} has no authoritative scope-bound preset inventory`
			)
		};
	}
	const descriptors = new Map<string, ReplicaSurfaceTrustedPresetDescriptor>();
	for (const [index, descriptor] of inventory.descriptors.entries()) {
		if (
			typeof descriptor?.name !== 'string' ||
			descriptor.name.length === 0 ||
			typeof descriptor.codec !== 'string' ||
			descriptors.has(descriptor.name)
		) {
			return {
				kind: 'unknown',
				reason: reason(
					'claim_inventory',
					[...path, 'descriptors', index],
					'trusted preset descriptor inventory is malformed or duplicated'
				)
			};
		}
		descriptors.set(descriptor.name, descriptor);
	}
	const values = new Map<string, DistributedTrustedPreset>();
	for (const [index, preset] of inventory.values.entries()) {
		if (
			typeof preset?.name !== 'string' ||
			preset.name.length === 0 ||
			typeof preset.codec !== 'string' ||
			values.has(preset.name)
		) {
			return {
				kind: 'unknown',
				reason: reason(
					'claim_inventory',
					[...path, 'values', index],
					'trusted preset value inventory is malformed or duplicated'
				)
			};
		}
		values.set(preset.name, preset);
	}
	if (
		descriptors.size !== values.size ||
		[...descriptors].some(
			([presetName, descriptor]) =>
				values.get(presetName)?.codec !== descriptor.codec
		)
	) {
		return {
			kind: 'unknown',
			reason: reason(
				'claim_inventory',
				path,
				'trusted preset descriptors and scope-bound values do not form an exact union'
			)
		};
	}
	const descriptor = descriptors.get(name);
	const preset = values.get(name);
	if (descriptor === undefined || preset === undefined) {
		return {
			kind: 'unknown',
			reason: reason(
				'claim_operand',
				path,
				`claim ${name} is absent from the selected client surface`
			)
		};
	}
	if (descriptor.codec !== field.codec) {
		return {
			kind: 'unknown',
			reason: reason(
				'claim_operand',
				path,
				`claim ${name} codec ${descriptor.codec} cannot target ${field.field}:${field.codec}`
			)
		};
	}
	if (!isReplicaValue(preset.value)) {
		return {
			kind: 'unknown',
			reason: reason(
				'claim_inventory',
				path,
				`claim ${name} is not a portable replica value`
			)
		};
	}
	return {
		kind: 'value',
		value: preset.value,
		source: 'trusted_preset'
	};
}

export function recordField(
	record: Readonly<Record<string, ReplicaValue>>,
	field: string,
	path: ReplicaQueryPlanPath
):
	| { readonly kind: 'value'; readonly value: ReplicaValue }
	| { readonly kind: 'unknown'; readonly reason: ReplicaQueryPlanReason } {
	if (!Object.prototype.hasOwnProperty.call(record, field)) {
		return {
			kind: 'unknown',
			reason: reason(
				'missing_field',
				path,
				`record does not contain canonical field ${field}`
			)
		};
	}
	return { kind: 'value', value: record[field]! };
}

export function uniqueStringList(value: unknown): readonly string[] | undefined {
	if (!Array.isArray(value) || value.some((entry) => !isName(entry))) {
		return undefined;
	}
	const values = value as string[];
	if (new Set(values).size !== values.length) return undefined;
	return Object.freeze([...values]);
}
