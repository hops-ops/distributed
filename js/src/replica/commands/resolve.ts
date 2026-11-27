import type { ReplicaValue } from '../types.js';
import { isPlainRecord } from '../../lib/is-plain-record.js';
import {
	GRAPHQL_NAME
} from './constants.js';
import type {
	ReplicaCommandDirectProjection,
	ReplicaCommandEffectExpression,
	ReplicaCommandGenerators,
	ReplicaCommandInputDefault,
	ReplicaCommandShape,
	ReplicaCommandTypeDefinition,
	ReplicaCommandTypeField,
	ReplicaMatchedTrustedPresetInventory,
	ReplicaPreparedCommand
} from './types.js';
import {
	artifactInvalid,
	inputInvalid
} from './util.js';
import {
	cloneDefinitionValue,
	cloneJson
} from './clone.js';

export function materializeInput(
	shape: ReplicaCommandShape,
	input: unknown,
	defaults: readonly ReplicaCommandInputDefault[],
	generators: ReplicaCommandGenerators | undefined
): unknown {
	switch (shape.kind) {
		case 'none':
			if (input !== undefined) inputInvalid('input');
			return undefined;
		case 'object':
			return cloneDefinitionValue(
				shape.definition,
				input,
				[],
				defaults,
				generators,
				'input'
			);
	}
}

export function resolveExpression(
	expression: ReplicaCommandEffectExpression,
	input: unknown,
	path: string,
	trustedPresets: ReplicaMatchedTrustedPresetInventory | undefined
): ReplicaValue {
	switch (expression.kind) {
		case 'input':
			return resolveInputPath(input, expression.path, path);
		case 'constant':
			return cloneJson(expression.value, `${path}.value`);
		case 'null':
			return null;
		case 'trusted_preset':
			if (trustedPresets === undefined) artifactInvalid(path);
			return trustedPresets.resolve(expression.name);
		default:
			artifactInvalid(`${path}.kind`);
	}
}

export function resolveInputPath(
	input: unknown,
	segments: readonly string[],
	path: string
): ReplicaValue {
	if (
		!Array.isArray(segments) ||
		segments.length === 0 ||
		segments.some((segment) => !GRAPHQL_NAME.test(segment))
	) {
		artifactInvalid(`${path}.path`);
	}
	let current = input;
	for (const segment of segments) {
		if (
			!isPlainRecord(current) ||
			!Object.prototype.hasOwnProperty.call(current, segment)
		) {
			inputInvalid(`input.${segments.join('.')}`);
		}
		current = current[segment];
	}
	if (current === undefined) inputInvalid(`input.${segments.join('.')}`);
	return current as ReplicaValue;
}

export function resolveDirectProjection(
	direct: ReplicaCommandDirectProjection | undefined,
	input: unknown,
	trustedPresets: ReplicaMatchedTrustedPresetInventory | undefined
): ReplicaPreparedCommand<unknown>['directProjection'] | undefined {
	if (direct === undefined) return undefined;
	const partition =
		direct.partition === undefined
			? undefined
			: resolveExpression(
					direct.partition,
					input,
					'artifact.directProjection.partition',
					trustedPresets
				);
	return Object.freeze({
		topology: Object.freeze({ ...direct.topology }),
		model: direct.model,
		identityFields: Object.freeze([...direct.identityFields]),
		...(partition === undefined ? {} : { partition })
	});
}

export function fieldAtPath(
	definition: ReplicaCommandTypeDefinition,
	segments: readonly string[],
	path: string
): ReplicaCommandTypeField {
	let current = definition;
	for (let index = 0; index < segments.length; index += 1) {
		const field = current.fields.find(({ name }) => name === segments[index]);
		if (field === undefined) artifactInvalid(`${path}.path`);
		if (index + 1 === segments.length) return field;
		if (field.list || field.nested === undefined) {
			artifactInvalid(`${path}.path`);
		}
		current = field.nested;
	}
	artifactInvalid(`${path}.path`);
}

