import { cacheIndexKey } from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import type {
	ReplicaArgumentValue,
	ReplicaArgumentsArtifact,
	ReplicaCoverageArtifact,
	ReplicaIdentity,
	ReplicaIndexCoverage,
	ReplicaModelArtifact,
	ReplicaValue
} from '../types.js';
import { assertName } from '../../lib/assert-name.js';
import { freezeRecord } from '../../lib/freeze-record.js';
import { canonicalJson } from '../../lib/canonical-json.js';
import { cloneJsonValue } from './clone.js';

export function replicaRecordKey(
	model: ReplicaModelArtifact,
	identity: ReplicaIdentity
): string {
	assertName(model.id, 'model id');
	if (!Array.isArray(model.identityFields) || model.identityFields.length === 0) {
		throw new TypeError(`model ${model.id} must declare at least one identity field`);
	}
	const parts = Array.isArray(identity) ? identity : [identity];
	if (parts.length !== model.identityFields.length) {
		throw new TypeError(
			`model ${model.id} identity expected ${model.identityFields.length} part(s), received ${parts.length}`
		);
	}
	return `record:${encodeURIComponent(model.id)}:${canonicalJson(
		parts.map((part) => cloneJsonValue(part))
	)}`;
}

export function replicaIndexKey(input: {
	readonly parent?: string;
	readonly field: string;
	readonly arguments?: Readonly<Record<string, ReplicaValue>>;
}): string {
	return cacheIndexKey({
		...(input.parent === undefined ? {} : { parent: input.parent }),
		field: input.field,
		arguments: input.arguments ?? {}
	});
}

export function resolveArguments(
	artifact: ReplicaArgumentsArtifact | undefined,
	variables: GraphqlVariables,
	coverage?: ReplicaCoverageArtifact
): Readonly<Record<string, ReplicaValue>> {
	const entries: Array<readonly [string, ReplicaValue]> = [];
	for (const [argument, source] of Object.entries(artifact ?? {})) {
		assertName(argument, 'GraphQL argument');
		const value = resolveReplicaArgumentValue(source, variables);
		if (value !== undefined) entries.push([argument, value]);
	}
	const resolved = freezeRecord(entries);
	return coverage?.kind === 'offset'
		? effectiveOffsetArguments(resolved, coverage)
		: resolved;
}

export function resolveReplicaArgumentValue(
	source: ReplicaArgumentValue,
	variables: GraphqlVariables,
	position: 'argument' | 'object' | 'list' = 'argument'
): ReplicaValue | undefined {
	if (source.kind === 'literal') return cloneJsonValue(source.value);
	if (source.kind === 'variable') {
		assertName(source.name, 'GraphQL variable');
		if (
			!Object.prototype.hasOwnProperty.call(variables, source.name) ||
			variables[source.name] === undefined
		) {
			return position === 'list' ? null : undefined;
		}
		return cloneJsonValue(variables[source.name]);
	}
	if (source.kind === 'list') {
		if (!Array.isArray(source.items)) {
			throw new TypeError('GraphQL list argument source must contain an items array');
		}
		return Object.freeze(
			source.items.map(
				(item) => resolveReplicaArgumentValue(item, variables, 'list') ?? null
			)
		);
	}
	if (source.kind === 'object') {
		if (
			source.fields === null ||
			typeof source.fields !== 'object' ||
			Array.isArray(source.fields)
		) {
			throw new TypeError('GraphQL object argument source must contain a fields object');
		}
		const entries: Array<readonly [string, ReplicaValue]> = [];
		for (const [field, value] of Object.entries(source.fields)) {
			assertName(field, 'GraphQL input field');
			const resolved = resolveReplicaArgumentValue(value, variables, 'object');
			if (resolved !== undefined) entries.push([field, resolved]);
		}
		return freezeRecord(entries);
	}
	throw new TypeError('unsupported argument source');
}

export function coverageFromArtifact(
	artifact: ReplicaCoverageArtifact | undefined,
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	returned: number
): ReplicaIndexCoverage {
	if (artifact === undefined || artifact.kind === 'complete') {
		return Object.freeze({ kind: 'complete' });
	}
	if (artifact.kind === 'offset') {
		const window = effectiveOffsetWindow(argumentsValue, artifact);
		return Object.freeze({
			kind: 'offset' as const,
			offset: window.offset,
			...(window.limit === undefined ? {} : { limit: window.limit }),
			returned
		});
	}
	return Object.freeze({
		kind: 'cursor' as const,
		...cacheArgument(argumentsValue, artifact.afterArgument, 'after'),
		...cacheArgument(argumentsValue, artifact.beforeArgument, 'before'),
		...numericCoverageArgument(argumentsValue, artifact.firstArgument, 'first'),
		...numericCoverageArgument(argumentsValue, artifact.lastArgument, 'last')
	});
}

export type OffsetCoverageArtifact = Extract<ReplicaCoverageArtifact, { readonly kind: 'offset' }>;

export function effectiveOffsetArguments(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	artifact: OffsetCoverageArtifact
): Readonly<Record<string, ReplicaValue>> {
	const window = effectiveOffsetWindow(argumentsValue, artifact);
	const entries = new Map(Object.entries(argumentsValue));
	if (artifact.offsetArgument !== undefined) {
		assertName(artifact.offsetArgument, 'offset pagination argument');
		entries.set(artifact.offsetArgument, window.offset);
	}
	if (artifact.limitArgument !== undefined) {
		assertName(artifact.limitArgument, 'limit pagination argument');
		if (window.limit === undefined) entries.delete(artifact.limitArgument);
		else entries.set(artifact.limitArgument, window.limit);
	}
	return freezeRecord([...entries.entries()]);
}

export function effectiveOffsetWindow(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	artifact: OffsetCoverageArtifact
): { readonly offset: number; readonly limit?: number } {
	const configuredDefault = coverageBound(artifact.defaultLimit, 'defaultLimit');
	const configuredMax = coverageBound(artifact.maxLimit, 'maxLimit');
	if (
		configuredDefault !== undefined &&
		configuredMax !== undefined &&
		configuredDefault > configuredMax
	) {
		throw new TypeError('offset coverage defaultLimit must not exceed maxLimit');
	}
	const offset =
		serverNonNegativeIntegerArgument(argumentsValue, artifact.offsetArgument) ?? 0;
	const requested = serverNonNegativeIntegerArgument(
		argumentsValue,
		artifact.limitArgument
	);
	const selected = requested ?? configuredDefault;
	const limit =
		selected === undefined || configuredMax === undefined
			? selected
			: Math.min(selected, configuredMax);
	return {
		offset,
		...(limit === undefined ? {} : { limit })
	};
}

/**
 * Distributed's SQL compiler treats an omitted, explicit-null, or negative
 * nullable Int as absent before applying the server default. Other values
 * cannot appear in a successful GraphQL response and remain fail-fast here.
 */
export function serverNonNegativeIntegerArgument(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	argument: string | undefined
): number | undefined {
	if (
		argument === undefined ||
		!Object.prototype.hasOwnProperty.call(argumentsValue, argument)
	) {
		return undefined;
	}
	const value = argumentsValue[argument];
	if (value === null) return undefined;
	if (!Number.isSafeInteger(value)) {
		throw new TypeError(`pagination argument ${argument} must be an integer or null`);
	}
	if ((value as number) < 0) return undefined;
	return (value as number) === 0 ? 0 : (value as number);
}

export function coverageBound(value: number | undefined, field: string): number | undefined {
	if (value === undefined) return undefined;
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new TypeError(`offset coverage ${field} must be a non-negative safe integer`);
	}
	return value;
}

export function cacheArgument<K extends 'after' | 'before'>(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	argument: string | undefined,
	key: K
): Partial<Record<K, ReplicaValue>> {
	if (argument === undefined || !Object.prototype.hasOwnProperty.call(argumentsValue, argument)) {
		return {};
	}
	return { [key]: argumentsValue[argument]! } as Partial<Record<K, ReplicaValue>>;
}

export function numericCoverageArgument<K extends 'first' | 'last'>(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	argument: string | undefined,
	key: K
): Partial<Record<K, number>> {
	const value = numericArgument(argumentsValue, argument);
	return value === undefined ? {} : ({ [key]: value } as Partial<Record<K, number>>);
}

export function numericArgument(
	argumentsValue: Readonly<Record<string, ReplicaValue>>,
	argument: string | undefined
): number | undefined {
	if (argument === undefined || !Object.prototype.hasOwnProperty.call(argumentsValue, argument)) {
		return undefined;
	}
	const value = argumentsValue[argument];
	if (!Number.isSafeInteger(value) || (value as number) < 0) {
		throw new TypeError(`pagination argument ${argument} must be a non-negative integer`);
	}
	return value as number;
}

/** True when `recordKey` is a key {@link replicaRecordKey} builds for `model`. */
export function recordKeyMatchesModel(recordKey: string, model: string): boolean {
	return recordKey.startsWith(`record:${encodeURIComponent(model)}:`);
}
