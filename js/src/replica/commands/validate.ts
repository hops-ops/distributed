import type { ReplicaClientSurface } from '../types.js';
import { isPlainRecord } from '../../lib/is-plain-record.js';
import { oncePerIdentity } from '../../lib/once-per-identity.js';
import {
	GRAPHQL_NAME,
	MAX_TYPE_DEPTH
} from './constants.js';
import type {
	ReplicaCommandArtifact,
	ReplicaCommandEffectExpression,
	ReplicaCommandEffectRelationship,
	ReplicaCommandInputDefaults,
	ReplicaCommandShape,
	ReplicaCommandTypeDefinition,
	ReplicaTrustedPresetDescriptor,
	TrustedPresetReference
} from './types.js';
import {
	artifactInvalid,
	hash,
	isSupportedCodec,
	nonempty,
	requiredString
} from './util.js';
import {
	parseReplicaTrustedPresetDescriptors,
	trustedPresetName
} from './presets.js';
import { fieldAtPath } from './resolve.js';
import { ProjectionDeltaValidationError, validateCommandProjectionArtifact } from '../projection-delta/validate.js';
import type { ProjectionPreviewValue, ReplicaCommandProjection } from '../projection-delta/types.js';

/** A command artifact is an immutable generated constant, validated once. */
export function validateArtifact<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	allowTrustedPresets: boolean
): readonly ReplicaTrustedPresetDescriptor[] {
	return oncePerIdentity(
		allowTrustedPresets ? validatedWithPresets : validatedWithoutPresets,
		artifact as ReplicaCommandArtifact<unknown, unknown>,
		(value) => validateCommandArtifact(value, allowTrustedPresets)
	);
}

type ValidatedPresets = readonly ReplicaTrustedPresetDescriptor[];
const validatedWithPresets = new WeakMap<object, ValidatedPresets>();
const validatedWithoutPresets = new WeakMap<object, ValidatedPresets>();

function validateCommandArtifact(
	artifact: ReplicaCommandArtifact<unknown, unknown>,
	allowTrustedPresets: boolean
): readonly ReplicaTrustedPresetDescriptor[] {
	if (artifact.version !== 2) artifactInvalid('artifact.version');
	nonempty(artifact.name, 'artifact.name');
	if (!GRAPHQL_NAME.test(artifact.mutationField)) {
		artifactInvalid('artifact.mutationField');
	}
	nonempty(artifact.document, 'artifact.document');
	hash(artifact.operationHash, 'artifact.operationHash');
	if (artifact.protocol.version !== 1) artifactInvalid('artifact.protocol.version');
	hash(artifact.protocol.schemaHash, 'artifact.protocol.schemaHash');
	hash(artifact.protocol.protocolHash, 'artifact.protocol.protocolHash');
	validateClientSurface(artifact.protocol.surface, 'artifact.protocol.surface');
	if (artifact.protocol.operation !== artifact.operationHash) {
		artifactInvalid('artifact.protocol.operation');
	}
	const surfaceTrustedPresets = parseReplicaTrustedPresetDescriptors(
		artifact.protocol.trustedPresets,
		'artifact.protocol.trustedPresets'
	);
	const trustedPresets = parseReplicaTrustedPresetDescriptors(
		artifact.trustedPresets ?? [],
		'artifact.trustedPresets'
	);
	const surfaceByName = new Map(
		surfaceTrustedPresets.map((descriptor) => [
			descriptor.name,
			descriptor.codec
		] as const)
	);
	for (let index = 0; index < trustedPresets.length; index += 1) {
		const descriptor = trustedPresets[index]!;
		if (surfaceByName.get(descriptor.name) !== descriptor.codec) {
			artifactInvalid(`artifact.trustedPresets[${index}]`);
		}
	}
	validateShape(artifact.input, 'artifact.input', 0, new Set());
	validateShape(artifact.output, 'artifact.output', 0, new Set());
	if (artifact.output.kind === 'none') artifactInvalid('artifact.output.kind');
	validateDefaults(artifact.input, artifact.inputDefaults);
	if (
		artifact.consistency !== 'succeeded' &&
		artifact.consistency !== 'eventual' &&
		artifact.consistency !== 'atomic'
	) {
		artifactInvalid('artifact.consistency');
	}
	if (artifact.projection !== undefined) {
		try {
			validateProjectionCapabilities(
				validateCommandProjectionArtifact(artifact.projection),
				artifact
			);
		} catch (error) {
			if (error instanceof ProjectionDeltaValidationError) {
				artifactInvalid(error.path);
			}
			throw error;
		}
	}
	validateDirectProjection(artifact);
	validateRevalidation(artifact);
	validateTrustedPresetReferences(
		artifact,
		trustedPresets,
		allowTrustedPresets
	);
	return trustedPresets;
}

export function validateClientSurface(
	surface: ReplicaClientSurface,
	path: string
): void {
	if (surface === null || typeof surface !== 'object') artifactInvalid(path);
	requiredString(surface.name, `${path}.name`);
	if (surface.kind === 'role') return;
	if (
		surface.kind !== 'application' ||
		!Array.isArray(surface.eligible_roles) ||
		!Array.isArray(surface.schema_roles) ||
		surface.eligible_roles.length === 0 ||
		surface.schema_roles.length === 0
	) {
		artifactInvalid(path);
	}
	const eligibleRoles = new Set<string>();
	for (let index = 0; index < surface.eligible_roles.length; index += 1) {
		const role = requiredString(
			surface.eligible_roles[index],
			`${path}.eligible_roles[${index}]`
		);
		if (eligibleRoles.has(role)) artifactInvalid(`${path}.eligible_roles[${index}]`);
		if (index > 0 && surface.eligible_roles[index - 1]! >= role) {
			artifactInvalid(`${path}.eligible_roles[${index}]`);
		}
		eligibleRoles.add(role);
	}
	const schemaRoles = new Set<string>();
	for (let index = 0; index < surface.schema_roles.length; index += 1) {
		const role = requiredString(
			surface.schema_roles[index],
			`${path}.schema_roles[${index}]`
		);
		if (schemaRoles.has(role)) artifactInvalid(`${path}.schema_roles[${index}]`);
		if (index > 0 && surface.schema_roles[index - 1]! >= role) {
			artifactInvalid(`${path}.schema_roles[${index}]`);
		}
		if (!eligibleRoles.has(role)) artifactInvalid(`${path}.schema_roles[${index}]`);
		schemaRoles.add(role);
	}
}

export function validateTrustedPresetReferences<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	descriptors: readonly ReplicaTrustedPresetDescriptor[],
	allowTrustedPresets: boolean
): void {
	const references = collectTrustedPresetReferences(artifact);
	const declared = new Set(descriptors.map(({ name }) => name));
	for (const reference of references) {
		if (!declared.has(reference.name)) artifactInvalid(reference.path);
	}
	const referenced = new Set(references.map(({ name }) => name));
	for (let index = 0; index < descriptors.length; index += 1) {
		if (!referenced.has(descriptors[index]!.name)) {
			artifactInvalid(`artifact.trustedPresets[${index}]`);
		}
	}
	if (!allowTrustedPresets && references.length !== 0) {
		artifactInvalid(references[0]!.path);
	}
}

export function collectTrustedPresetReferences<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>
): readonly TrustedPresetReference[] {
	const out: TrustedPresetReference[] = [];
	const expression = (
		value: ReplicaCommandEffectExpression,
		path: string
	): void => {
		if (value.kind === 'trusted_preset') {
			out.push(Object.freeze({ name: value.name, path }));
		}
	};
	if (artifact.projection !== undefined) {
		for (let index = 0; index < artifact.projection.preview.operations.length; index += 1) {
			const mutation = artifact.projection.preview.operations[index]!.mutation;
			collectPreviewMutationPresets(
				mutation,
				`artifact.projection.preview.operations[${index}].mutation`,
				out
			);
		}
	}
	if (artifact.directProjection?.partition !== undefined) {
		expression(
			artifact.directProjection.partition,
			'artifact.directProjection.partition'
		);
	}
	return Object.freeze(out);
}

function collectPreviewMutationPresets(
	mutation: ReplicaCommandProjection['preview']['operations'][number]['mutation'],
	path: string,
	out: TrustedPresetReference[]
): void {
	const value = (expression: ProjectionPreviewValue, valuePath: string): void => {
		switch (expression.kind) {
			case 'trusted_preset':
				out.push(Object.freeze({ name: expression.name, path: valuePath }));
				break;
			case 'list':
				expression.values.forEach((item, index) =>
					value(item, `${valuePath}.values[${index}]`)
				);
				break;
			case 'object':
				expression.fields.forEach((field, index) =>
					value(field.value, `${valuePath}.fields[${index}].value`)
				);
				break;
			case 'transform':
				expression.arguments.forEach((item, index) =>
					value(item, `${valuePath}.arguments[${index}]`)
				);
				break;
			default:
				break;
		}
	};
	const scope = (
		candidate: import('../projection-delta/types.js').ProjectionPreviewScope,
		scopePath: string
	): void => {
		if (candidate.partition.kind === 'expression') {
			value(candidate.partition.expression, `${scopePath}.partition.expression`);
		}
		candidate.key.forEach((field, index) =>
			value(field.value, `${scopePath}.key[${index}].value`)
		);
	};
	switch (mutation.op) {
		case 'upsert':
		case 'patch':
			scope(mutation.scope, `${path}.scope`);
			(mutation.op === 'upsert' ? mutation.fields : mutation.set).forEach(
				(field, index) => value(field.value, `${path}.fields[${index}].value`)
			);
			break;
		case 'delete':
			scope(mutation.scope, `${path}.scope`);
			break;
		case 'link':
		case 'unlink':
			scope(mutation.source, `${path}.source`);
			scope(mutation.target, `${path}.target`);
			break;
		case 'invalidate_relationship':
			scope(mutation.source, `${path}.source`);
			break;
		case 'invalidate_model':
			if (mutation.partition?.kind === 'expression') {
				value(mutation.partition.expression, `${path}.partition.expression`);
			}
			break;
	}
}

function validateProjectionCapabilities<TInput, TOutput>(
	projection: ReplicaCommandProjection,
	artifact: ReplicaCommandArtifact<TInput, TOutput>
): void {
	const models = new Set(artifact.revalidation.models);
	const relationships = new Set(
		artifact.revalidation.relationships.map(
			(item) =>
				`${item.sourceModel}\0${item.field}\0${item.targetModel}`
		)
	);
	const requireModel = (model: string, path: string): void => {
		if (!models.has(model)) artifactInvalid(path);
	};
	for (let index = 0; index < projection.preview.operations.length; index += 1) {
		const mutation = projection.preview.operations[index]!.mutation;
		const path = `artifact.projection.preview.operations[${index}].mutation`;
		switch (mutation.op) {
			case 'upsert':
			case 'patch':
			case 'delete':
				requireModel(mutation.scope.model, `${path}.scope.model`);
				break;
			case 'link':
			case 'unlink':
				requireModel(mutation.source.model, `${path}.source.model`);
				requireModel(mutation.target.model, `${path}.target.model`);
				if (
					!relationships.has(
						`${mutation.source.model}\0${mutation.relationship}\0${mutation.target.model}`
					)
				) artifactInvalid(`${path}.relationship`);
				break;
			case 'invalidate_model':
				requireModel(mutation.model, `${path}.model`);
				break;
			case 'invalidate_relationship':
				requireModel(mutation.source.model, `${path}.source.model`);
				if (
					![...relationships].some((relationship) =>
						relationship.startsWith(
							`${mutation.source.model}\0${mutation.relationship}\0`
						)
					)
				) artifactInvalid(`${path}.relationship`);
				break;
		}
	}
}

export function validateShape(
	shape: ReplicaCommandShape,
	path: string,
	depth: number,
	active: Set<ReplicaCommandTypeDefinition>
): void {
	if (depth > MAX_TYPE_DEPTH) artifactInvalid(path);
	switch (shape.kind) {
		case 'none':
			return;
		case 'object':
			validateDefinition(shape.definition, `${path}.definition`, depth, active);
			return;
		default:
			artifactInvalid(`${path}.kind`);
	}
}

export function validateDefinition(
	definition: ReplicaCommandTypeDefinition,
	path: string,
	depth: number,
	active: Set<ReplicaCommandTypeDefinition>
): void {
	if (depth > MAX_TYPE_DEPTH || active.has(definition)) artifactInvalid(path);
	if (!GRAPHQL_NAME.test(definition.name) || !Array.isArray(definition.fields)) {
		artifactInvalid(path);
	}
	active.add(definition);
	const names = new Set<string>();
	for (let index = 0; index < definition.fields.length; index += 1) {
		const field = definition.fields[index]!;
		const fieldPath = `${path}.fields[${index}]`;
		if (!GRAPHQL_NAME.test(field.name) || !GRAPHQL_NAME.test(field.typeName)) {
			artifactInvalid(fieldPath);
		}
		if (names.has(field.name)) artifactInvalid(`${fieldPath}.name`);
		names.add(field.name);
		if (
			typeof field.nullable !== 'boolean' ||
			typeof field.list !== 'boolean' ||
			typeof field.itemNullable !== 'boolean' ||
			(field.itemNullable && !field.list) ||
			(field.codec === undefined) === (field.nested === undefined)
		) {
			artifactInvalid(fieldPath);
		}
		if (field.codec !== undefined && !isSupportedCodec(field.codec)) {
			artifactInvalid(`${fieldPath}.codec`);
		}
		if (field.nested !== undefined) {
			if (field.nested.name !== field.typeName) {
				artifactInvalid(`${fieldPath}.nested.name`);
			}
			validateDefinition(field.nested, `${fieldPath}.nested`, depth + 1, active);
		}
	}
	active.delete(definition);
}

export function validateDefaults(
	shape: ReplicaCommandShape,
	defaults: ReplicaCommandInputDefaults | undefined
): void {
	if (defaults === undefined) return;
	if (defaults.version !== 1 || !Array.isArray(defaults.defaults)) {
		artifactInvalid('artifact.inputDefaults');
	}
	if (shape.kind !== 'object' && defaults.defaults.length > 0) {
		artifactInvalid('artifact.inputDefaults.defaults');
	}
	const paths = new Set<string>();
	for (let index = 0; index < defaults.defaults.length; index += 1) {
		const entry = defaults.defaults[index]!;
		const path = `artifact.inputDefaults.defaults[${index}]`;
		if (
			!Array.isArray(entry.path) ||
			entry.path.length === 0 ||
			entry.path.some(
				(segment: unknown) =>
					typeof segment !== 'string' || !GRAPHQL_NAME.test(segment)
			) ||
			(entry.generator !== 'uuid_v7' && entry.generator !== 'ulid')
		) {
			artifactInvalid(path);
		}
		const key = entry.path.join('\u0000');
		if (paths.has(key)) artifactInvalid(`${path}.path`);
		paths.add(key);
		if (shape.kind === 'object') {
			const field = fieldAtPath(shape.definition, entry.path, path);
			if (
				field.nullable ||
				field.list ||
				field.nested !== undefined ||
				field.codec !== 'string' ||
				(field.typeName !== 'ID' && field.typeName !== 'String')
			) {
				artifactInvalid(path);
			}
		}
	}
}

export function validateDirectProjection<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>
): void {
	const direct = artifact.directProjection;
	if (artifact.consistency === 'atomic' && direct === undefined) {
		artifactInvalid('artifact.directProjection');
	}
	if (artifact.consistency !== 'atomic' && direct !== undefined) {
		artifactInvalid('artifact.directProjection');
	}
	if (direct === undefined) return;
	if (direct.topology.version !== 1) {
		artifactInvalid('artifact.directProjection.topology.version');
	}
	nonempty(direct.topology.name, 'artifact.directProjection.topology.name');
	hash(direct.topology.digest, 'artifact.directProjection.topology.digest');
	nonempty(direct.model, 'artifact.directProjection.model');
	nonempty(direct.changeEpoch, 'artifact.directProjection.changeEpoch');
	if (
		!Array.isArray(direct.identityFields) ||
		direct.identityFields.length === 0 ||
		artifact.output.kind !== 'object'
	) {
		artifactInvalid('artifact.directProjection.identityFields');
	}
	const identityFields = new Set<string>();
	for (let index = 0; index < direct.identityFields.length; index += 1) {
		const fieldName = direct.identityFields[index]!;
		const path = `artifact.directProjection.identityFields[${index}]`;
		if (
			typeof fieldName !== 'string' ||
			!GRAPHQL_NAME.test(fieldName) ||
			identityFields.has(fieldName)
		) {
			artifactInvalid(path);
		}
		identityFields.add(fieldName);
		const outputField = artifact.output.definition.fields.find(
			(field) => field.name === fieldName
		);
		if (
			outputField === undefined ||
			outputField.nullable ||
			outputField.list ||
			outputField.codec === undefined ||
			outputField.nested !== undefined
		) {
			artifactInvalid(path);
		}
	}
	if (direct.partition !== undefined) {
		validateExpressionArtifact(
			direct.partition,
			artifact.input,
			'artifact.directProjection.partition'
		);
	}
}

export function validateRevalidation<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>
): void {
	const plan = artifact.revalidation;
	if (
		plan.version !== 1 ||
		typeof plan.required !== 'boolean' ||
		!Array.isArray(plan.dependencies) ||
		!Array.isArray(plan.models) ||
		!Array.isArray(plan.relationships)
	) {
		artifactInvalid('artifact.revalidation');
	}
	for (const [index, dependency] of plan.dependencies.entries()) {
		nonempty(dependency, `artifact.revalidation.dependencies[${index}]`);
	}
	for (const [index, model] of plan.models.entries()) {
		nonempty(model, `artifact.revalidation.models[${index}]`);
	}
	for (const [index, relationship] of plan.relationships.entries()) {
		validateRelationshipArtifact(
			relationship,
			`artifact.revalidation.relationships[${index}]`
		);
	}
	if (
		((artifact.projection?.preview.operations.length ?? 0) === 0 &&
			artifact.consistency !== 'atomic') &&
		!plan.required
	) {
		artifactInvalid('artifact.revalidation.required');
	}
}

export function validateExpressionArtifact(
	expression: ReplicaCommandEffectExpression,
	input: ReplicaCommandShape,
	path: string
): void {
	if (expression === null || typeof expression !== 'object') {
		artifactInvalid(path);
	}
	switch (expression.kind) {
		case 'input':
			if (
				!Array.isArray(expression.path) ||
				expression.path.length === 0 ||
				expression.path.some(
					(segment: unknown) =>
						typeof segment !== 'string' || !GRAPHQL_NAME.test(segment)
				)
			) {
				artifactInvalid(`${path}.path`);
			}
			if (input.kind !== 'object') artifactInvalid(`${path}.path`);
			fieldAtPath(input.definition, expression.path, path);
			return;
		case 'constant':
			validateArtifactJson(expression.value, `${path}.value`, new Set(), 0);
			return;
		case 'null':
			return;
		case 'trusted_preset':
			trustedPresetName(expression.name, `${path}.name`);
			return;
		case undefined:
		default:
			artifactInvalid(`${path}.kind`);
	}
}

export function validateRelationshipArtifact(
	relationship: ReplicaCommandEffectRelationship,
	path: string
): void {
	if (relationship === null || typeof relationship !== 'object') {
		artifactInvalid(path);
	}
	requiredString(relationship.sourceModel, `${path}.sourceModel`);
	requiredString(relationship.field, `${path}.field`);
	requiredString(relationship.targetModel, `${path}.targetModel`);
}

export function validateArtifactJson(
	value: unknown,
	path: string,
	active: Set<object>,
	depth: number
): void {
	if (depth > MAX_TYPE_DEPTH) artifactInvalid(path);
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	) {
		return;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) artifactInvalid(path);
		return;
	}
	if (typeof value !== 'object') artifactInvalid(path);
	if (active.has(value)) artifactInvalid(path);
	active.add(value);
	if (Array.isArray(value)) {
		for (let index = 0; index < value.length; index += 1) {
			validateArtifactJson(value[index], `${path}[${index}]`, active, depth + 1);
		}
		active.delete(value);
		return;
	}
	if (!isPlainRecord(value)) artifactInvalid(path);
	for (const key of Reflect.ownKeys(value)) {
		if (typeof key !== 'string') artifactInvalid(path);
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (
			descriptor === undefined ||
			!('value' in descriptor) ||
			descriptor.value === undefined
		) {
			artifactInvalid(`${path}.${key}`);
		}
		validateArtifactJson(
			descriptor.value,
			`${path}.${key}`,
			active,
			depth + 1
		);
	}
	active.delete(value);
}
