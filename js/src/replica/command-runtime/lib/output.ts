import {
	type DistributedCommandMetadata
} from '../../../protocol.js';
import {
	type ReplicaCommandArtifact
} from '../../commands.js';
import type {
	ReplicaValue
} from '../../types.js';
import {
	MAX_OUTPUT_DEPTH
} from '../constants.js';
import { ReplicaCommandRuntimeError } from '../errors.js';
import { compareCodeUnits } from '../../../lib/compare-code-units.js';
import { isPlainRecord } from '../../../lib/is-plain-record.js';
import { isUnsignedInteger, unsignedIntegerMaximum } from '../../../unsigned-integer.js';
import {
	comparePropertyKeys,
	outputInvalid
} from './util.js';
export function projectionExpectationFingerprint(
	value: DistributedCommandMetadata['expects'][number]
): string {
	return tupleFingerprint([
		value.projection,
		value.model,
		value.scopeToken
	]);
}

export function projectionObservationFingerprint(
	value: DistributedCommandMetadata['observations'][number]
): string {
	return tupleFingerprint([
		value.causationId,
		value.projection,
		value.model,
		value.scopeToken
	]);
}

export function recordRevisionFingerprint(
	value: DistributedCommandMetadata['records'][number]
): string {
	return tupleFingerprint([
		value.model,
		value.scopeToken,
		value.incarnation,
		value.revision,
		value.tombstone ? '1' : '0',
		...(value.path ?? [])
	]);
}

export function tupleFingerprint(parts: readonly string[]): string {
	return parts.map((part) => `${part.length}:${part}`).join('');
}

export function sameStringMultiset(
	left: readonly string[],
	right: readonly string[]
): boolean {
	if (left.length !== right.length) return false;
	const sortedLeft = [...left].sort(compareCodeUnits);
	const sortedRight = [...right].sort(compareCodeUnits);
	return sortedLeft.every((value, index) => value === sortedRight[index]);
}

export function isStringSubset(
	subset: readonly string[],
	superset: readonly string[]
): boolean {
	const remaining = new Map<string, number>();
	for (const value of superset) {
		remaining.set(value, (remaining.get(value) ?? 0) + 1);
	}
	for (const value of subset) {
		const count = remaining.get(value) ?? 0;
		if (count === 0) return false;
		remaining.set(value, count - 1);
	}
	return true;
}

export function commandOutput<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	data: Readonly<Record<string, unknown>> | null | undefined,
	field: string
): unknown {
	if (
		data === undefined ||
		data === null ||
		!Object.prototype.hasOwnProperty.call(data, field) ||
		Reflect.ownKeys(data).some((key) => key !== field)
	) {
		throw new ReplicaCommandRuntimeError(
			'REPLICA_COMMAND_PROTOCOL_INVALID'
		);
	}
	return cloneOutputShape(artifact.output, data[field], `data.${field}`, 0);
}

export function cloneOutputShape(
	shape: ReplicaCommandArtifact<unknown, unknown>['output'],
	value: unknown,
	path: string,
	depth: number
): unknown {
	if (depth > MAX_OUTPUT_DEPTH) outputInvalid(path);
	if (shape.kind !== 'object') outputInvalid(path);
	if (!isPlainRecord(value)) outputInvalid(path);
	const known = new Set(shape.definition.fields.map(({ name }) => name));
	for (const key of Reflect.ownKeys(value)) {
		if (typeof key !== 'string' || !known.has(key)) outputInvalid(`${path}.${String(key)}`);
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !('value' in descriptor)) {
			outputInvalid(`${path}.${key}`);
		}
	}
	const output: Record<string, unknown> = {};
	for (const field of shape.definition.fields) {
		const present =
			Object.prototype.hasOwnProperty.call(value, field.name) &&
			value[field.name] !== undefined;
		if (!present) {
			outputInvalid(`${path}.${field.name}`);
		}
		const fieldValue = value[field.name];
		if (fieldValue === null) {
			if (!field.nullable) outputInvalid(`${path}.${field.name}`);
			output[field.name] = null;
			continue;
		}
		const cloneItem = (item: unknown, itemPath: string): unknown =>
			field.nested === undefined
				? cloneOutputScalar(field.codec, item, itemPath)
				: cloneOutputShape(
						{ kind: 'object', definition: field.nested },
						item,
						itemPath,
						depth + 1
					);
		if (field.list) {
			if (!Array.isArray(fieldValue)) outputInvalid(`${path}.${field.name}`);
			output[field.name] = Object.freeze(
				fieldValue.map((item, index) => {
					if (item === null) {
						if (!field.itemNullable) {
							outputInvalid(`${path}.${field.name}[${index}]`);
						}
						return null;
					}
					return cloneItem(item, `${path}.${field.name}[${index}]`);
				})
			);
		} else {
			output[field.name] = cloneItem(fieldValue, `${path}.${field.name}`);
		}
	}
	return Object.freeze(output);
}

export function cloneOutputScalar(
	codec: string | undefined,
	value: unknown,
	path: string
): ReplicaValue {
	const unsignedMaximum = unsignedIntegerMaximum(codec);
	if (unsignedMaximum !== undefined) {
		if (!isUnsignedInteger(value, unsignedMaximum)) outputInvalid(path);
		return value;
	}
	switch (codec) {
		case 'string':
		case 'string_unvalidated_timestamp':
		case 'base64':
			if (typeof value !== 'string') outputInvalid(path);
			return value;
		case 'boolean':
			if (typeof value !== 'boolean') outputInvalid(path);
			return value;
		case 'int32':
			if (
				typeof value !== 'number' ||
				!Number.isInteger(value) ||
				value < -2_147_483_648 ||
				value > 2_147_483_647
			) {
				outputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'json_number_precision_limited':
			if (typeof value !== 'number' || !Number.isInteger(value)) {
				outputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'float64':
			if (typeof value !== 'number' || !Number.isFinite(value)) {
				outputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'json':
			return cloneOutputJson(value, path, new Set(), 0);
		default:
			outputInvalid(`${path}.codec`);
	}
}

export function cloneOutputJson(
	value: unknown,
	path: string,
	active: Set<object>,
	depth: number
): ReplicaValue {
	if (depth > MAX_OUTPUT_DEPTH) outputInvalid(path);
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	) {
		return value;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) outputInvalid(path);
		return Object.is(value, -0) ? 0 : value;
	}
	if (typeof value !== 'object' || active.has(value)) outputInvalid(path);
	active.add(value);
	if (Array.isArray(value)) {
		const output = Object.freeze(
			value.map((item, index) =>
				cloneOutputJson(item, `${path}[${index}]`, active, depth + 1)
			)
		);
		active.delete(value);
		return output;
	}
	if (!isPlainRecord(value)) outputInvalid(path);
	const output: Record<string, ReplicaValue> = {};
	for (const key of Reflect.ownKeys(value).sort(comparePropertyKeys)) {
		if (typeof key !== 'string') outputInvalid(path);
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (
			descriptor === undefined ||
			!('value' in descriptor) ||
			descriptor.value === undefined
		) {
			outputInvalid(`${path}.${key}`);
		}
		output[key] = cloneOutputJson(
			descriptor.value,
			`${path}.${key}`,
			active,
			depth + 1
		);
	}
	active.delete(value);
	return Object.freeze(output);
}
