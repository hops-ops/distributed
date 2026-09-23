import type { ReplicaClientSurface, ReplicaValue } from '../types.js';
import { isUnsignedInteger, unsignedIntegerMaximum } from '../../unsigned-integer.js';
import { createReplicaCommandId } from '../command-id.js';
import { isPlainRecord } from '../../lib/is-plain-record.js';
import {
	MAX_TYPE_DEPTH
} from './constants.js';
import type {
	ReplicaCommandGenerators,
	ReplicaCommandInputDefault,
	ReplicaCommandRevalidationPlan,
	ReplicaCommandEffectRelationship,
	ReplicaCommandScalarCodec,
	ReplicaCommandTypeDefinition,
	ReplicaCommandTypeField,
	ReplicaTrustedPresetDescriptor
} from './types.js';
import {
	artifactInvalid,
	createUlid,
	defineEnumerable,
	inputInvalid,
	requiredString,
	samePath,
	validateUlid,
	validateUuidV7
} from './util.js';

export function cloneClientSurface(surface: ReplicaClientSurface): ReplicaClientSurface {
	return surface.kind === 'role'
		? Object.freeze({ kind: 'role' as const, name: surface.name })
		: Object.freeze({
				kind: 'application' as const,
				name: surface.name,
				eligible_roles: Object.freeze([...surface.eligible_roles]),
				schema_roles: Object.freeze([...surface.schema_roles])
			});
}

export function cloneTrustedPresetDescriptors(
	value: readonly ReplicaTrustedPresetDescriptor[]
): readonly ReplicaTrustedPresetDescriptor[] {
	return Object.freeze(
		value.map(({ name, codec }) => Object.freeze({ name, codec }))
	);
}

export function cloneDefinitionValue(
	definition: ReplicaCommandTypeDefinition,
	value: unknown,
	path: readonly string[],
	defaults: readonly ReplicaCommandInputDefault[],
	generators: ReplicaCommandGenerators | undefined,
	label: string
): Readonly<Record<string, unknown>> {
	if (!isPlainRecord(value)) inputInvalid(label);
	const ownKeys = Reflect.ownKeys(value);
	if (ownKeys.some((key) => typeof key !== 'string')) inputInvalid(label);
	const known = new Set(definition.fields.map((field) => field.name));
	for (const key of ownKeys as string[]) {
		if (!known.has(key)) inputInvalid(`${label}.${key}`);
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !('value' in descriptor)) {
			inputInvalid(`${label}.${key}`);
		}
	}

	const result: Record<string, unknown> = {};
	for (const field of definition.fields) {
		const fieldPath = [...path, field.name];
		const fieldLabel = `${label}.${field.name}`;
		const present =
			Object.prototype.hasOwnProperty.call(value, field.name) &&
			value[field.name] !== undefined;
		let fieldValue: unknown;
		if (present) {
			fieldValue = value[field.name];
		} else {
			const declared = defaults.find((entry) =>
				samePath(entry.path, fieldPath)
			);
			if (declared !== undefined) {
				fieldValue = generateDefault(declared, generators, fieldLabel);
			} else if (field.nullable) {
				continue;
			} else {
				inputInvalid(fieldLabel);
			}
		}
		defineEnumerable(
			result,
			field.name,
			cloneFieldValue(
				field,
				fieldValue,
				fieldPath,
				defaults,
				generators,
				fieldLabel
			)
		);
	}
	return Object.freeze(result);
}

export function cloneFieldValue(
	field: ReplicaCommandTypeField,
	value: unknown,
	path: readonly string[],
	defaults: readonly ReplicaCommandInputDefault[],
	generators: ReplicaCommandGenerators | undefined,
	label: string
): unknown {
	if (value === null) {
		if (!field.nullable) inputInvalid(label);
		return null;
	}
	if (field.list) {
		if (!Array.isArray(value)) inputInvalid(label);
		return Object.freeze(
			value.map((item, index) => {
				if (item === null) {
					if (!field.itemNullable) inputInvalid(`${label}[${index}]`);
					return null;
				}
				return cloneNonNullFieldValue(
					field,
					item,
					path,
					defaults,
					generators,
					`${label}[${index}]`
				);
			})
		);
	}
	return cloneNonNullFieldValue(
		field,
		value,
		path,
		defaults,
		generators,
		label
	);
}

export function cloneNonNullFieldValue(
	field: ReplicaCommandTypeField,
	value: unknown,
	path: readonly string[],
	defaults: readonly ReplicaCommandInputDefault[],
	generators: ReplicaCommandGenerators | undefined,
	label: string
): unknown {
	if (field.nested !== undefined) {
		return cloneDefinitionValue(
			field.nested,
			value,
			path,
			defaults,
			generators,
			label
		);
	}
	return cloneScalar(field.codec, value, label);
}

export function cloneScalar(
	codec: ReplicaCommandScalarCodec | undefined,
	value: unknown,
	path: string
): ReplicaValue {
	const unsignedMaximum = unsignedIntegerMaximum(codec);
	if (unsignedMaximum !== undefined) {
		if (!isUnsignedInteger(value, unsignedMaximum)) inputInvalid(path);
		return value;
	}
	switch (codec) {
		case 'string':
		case 'string_unvalidated_timestamp':
		case 'base64':
			if (typeof value !== 'string') inputInvalid(path);
			return value;
		case 'boolean':
			if (typeof value !== 'boolean') inputInvalid(path);
			return value;
		case 'int32':
			if (
				typeof value !== 'number' ||
				!Number.isInteger(value) ||
				value < -2_147_483_648 ||
				value > 2_147_483_647
			) {
				inputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'json_number_precision_limited':
			if (typeof value !== 'number' || !Number.isInteger(value)) {
				inputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'float64':
			if (typeof value !== 'number' || !Number.isFinite(value)) {
				inputInvalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'json':
			return cloneJson(value, path);
		default:
			artifactInvalid(`${path}.codec`);
	}
}

export function cloneJson(
	value: unknown,
	path: string,
	active: Set<object> = new Set(),
	depth = 0
): ReplicaValue {
	if (depth > MAX_TYPE_DEPTH) inputInvalid(path);
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	) {
		return value;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) inputInvalid(path);
		return Object.is(value, -0) ? 0 : value;
	}
	if (Array.isArray(value)) {
		if (active.has(value)) inputInvalid(path);
		active.add(value);
		const result = Object.freeze(
			value.map((item, index) =>
				cloneJson(item, `${path}[${index}]`, active, depth + 1)
			)
		);
		active.delete(value);
		return result;
	}
	if (!isPlainRecord(value)) inputInvalid(path);
	if (active.has(value)) inputInvalid(path);
	active.add(value);
	const keys = Reflect.ownKeys(value);
	if (keys.some((key) => typeof key !== 'string')) inputInvalid(path);
	const result: Record<string, ReplicaValue> = {};
	for (const key of (keys as string[]).sort()) {
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (
			descriptor === undefined ||
			!('value' in descriptor) ||
			descriptor.value === undefined
		) {
			inputInvalid(`${path}.${key}`);
		}
		defineEnumerable(
			result,
			key,
			cloneJson(descriptor.value, `${path}.${key}`, active, depth + 1)
		);
	}
	active.delete(value);
	return Object.freeze(result);
}

export function cloneRevalidation(
	plan: ReplicaCommandRevalidationPlan
): ReplicaCommandRevalidationPlan {
	return Object.freeze({
		version: 1 as const,
		required: plan.required,
		dependencies: Object.freeze([...plan.dependencies]),
		models: Object.freeze([...plan.models]),
		relationships: Object.freeze(
			plan.relationships.map((relationship, index) =>
				cloneRelationship(
					relationship,
					`artifact.revalidation.relationships[${index}]`
				)
			)
		)
	});
}

export function cloneRelationship(
	relationship: ReplicaCommandEffectRelationship,
	path: string
): ReplicaCommandEffectRelationship {
	return Object.freeze({
		sourceModel: requiredString(
			relationship.sourceModel,
			`${path}.sourceModel`
		),
		field: requiredString(relationship.field, `${path}.field`),
		targetModel: requiredString(
			relationship.targetModel,
			`${path}.targetModel`
		)
	});
}


export function generateDefault(
	entry: ReplicaCommandInputDefault,
	generators: ReplicaCommandGenerators | undefined,
	path: string
): string {
	switch (entry.generator) {
			case 'uuid_v7':
				return validateUuidV7(
					(generators?.uuidV7 ?? createReplicaCommandId)(),
					path,
					'REPLICA_COMMAND_INPUT_INVALID'
				);
		case 'ulid':
			return validateUlid(
				(generators?.ulid ?? createUlid)(),
				path
			);
		default:
			artifactInvalid(`${path}.generator`);
	}
}
