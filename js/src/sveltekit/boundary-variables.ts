import {
	canonicalizeOperationVariables,
	type ReplicaOperationArtifact
} from '../replica/index.js';
import type { GraphqlVariables } from '../types.js';
import type { ReplicaVariableInputRef } from '../replica/types.js';
import {
	decodeSearchScalar,
	normalizeSearchParamSource,
	type DistributedSearchParamSource
} from './search-variables.js';

export const DISTRIBUTED_BOUNDARY_BINDING_VERSION = 2;
const GRAPHQL_NAME = /^[_A-Za-z][_0-9A-Za-z]*$/;
const MAX_BINDING_VARIABLES = 128;
const MAX_PATH_SEGMENTS = 16;
const MAX_LITERAL_DEPTH = 32;
const MAX_LITERAL_VALUES = 4_096;
const HOSTILE_KEYS = new Set(['__proto__', 'prototype', 'constructor']);

type SearchScalarFor<TValue> =
	NonNullable<TValue> extends string ? 'String' | 'ID' :
	NonNullable<TValue> extends number ? 'Int' | 'Float' :
	NonNullable<TValue> extends boolean ? 'Boolean' : never;

type SearchSourceFor<TValue> = unknown extends TValue
	? DistributedSearchParamSource
	: NonNullable<TValue> extends readonly (infer TItem)[]
		? DistributedSearchParamSource<SearchScalarFor<TItem>, 'all'> & { mode: 'all' }
		: DistributedSearchParamSource<SearchScalarFor<TValue>, 'first'>;

export type DistributedBoundaryVariableSource<TValue = unknown> =
	| Readonly<{ kind: 'route_param'; name: string }>
	| SearchSourceFor<TValue>
	| Readonly<{ kind: 'trusted_session'; path: readonly string[] }>
	| Readonly<{ kind: 'constant'; value: TValue }>
	| Readonly<{ kind: 'forwarded_prop'; path: readonly string[] }>
	| Readonly<{ kind: 'omit' }>;

export type DistributedBoundaryVariableSources<
	TVariables extends GraphqlVariables
> = Readonly<{
	[K in keyof TVariables]?: DistributedBoundaryVariableSource<TVariables[K]>;
}>;

export type DistributedBoundaryVariableContext<
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
> = Readonly<{
	params: Readonly<Record<string, string | undefined>>;
	search:
		| URLSearchParams
		| Readonly<Record<string, string | readonly string[] | undefined>>;
	session: TSession | null;
	props: TProps;
}>;

export type DistributedBoundaryBinding<
	TVariables extends GraphqlVariables,
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
> = Readonly<{
	version: 2;
	id: string;
	artifactId: string;
	sources: DistributedBoundaryVariableSources<TVariables>;
	resolve(
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): TVariables;
	canonicalBytes(
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): string;
}>;

export type DistributedBoundaryPlan = Readonly<{
	operation: string;
	route: string;
	kind: 'layout' | 'page';
	sourcePath?: string;
	discovery: 'component' | 'route_document' | 'explicit';
}>;

export type DistributedBoundaryOperation<
	TData = unknown,
	TVariables extends GraphqlVariables = GraphqlVariables,
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
> = Readonly<{
	plan: DistributedBoundaryPlan;
	artifact: ReplicaOperationArtifact<TData, TVariables>;
	binding: DistributedBoundaryBinding<TVariables, TSession, TProps>;
}>;

/**
 * Define one closed, inspectable variable binding for every boundary lifecycle.
 * The operation artifact remains the sole owner of coercion and cache identity.
 */
export function defineDistributedBoundaryBinding<
	TData,
	TVariables extends GraphqlVariables,
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	sources: DistributedBoundaryVariableSources<TVariables>
): DistributedBoundaryBinding<TVariables, TSession, TProps> {
	const validated = validateSources(artifact, sources);
	const id = `boundary-v${DISTRIBUTED_BOUNDARY_BINDING_VERSION}:${fnv1a64(
		`${artifact.id}\n${stableJson(validated)}`
	)}`;
	const resolve = (
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): TVariables =>
		resolveDistributedBoundaryVariables(artifact, validated, context);
	return Object.freeze({
		version: DISTRIBUTED_BOUNDARY_BINDING_VERSION,
		id,
		artifactId: artifact.id,
		sources: validated,
		resolve,
		canonicalBytes(context): string {
			return JSON.stringify(resolve(context));
		}
	});
}

export function defineDistributedBoundaryOperation<
	TData,
	TVariables extends GraphqlVariables,
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
>(
	plan: DistributedBoundaryPlan,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	binding: DistributedBoundaryBinding<TVariables, TSession, TProps>
): DistributedBoundaryOperation<TData, TVariables, TSession, TProps> {
	if (binding.version !== DISTRIBUTED_BOUNDARY_BINDING_VERSION) {
		throw new TypeError('Distributed boundary binding version is unsupported; regenerate the boundary plan');
	}
	if (binding.artifactId !== artifact.id) {
		throw new TypeError('Distributed boundary binding belongs to a different operation artifact');
	}
	if (
		plan === null ||
		typeof plan !== 'object' ||
		!GRAPHQL_NAME.test(plan.operation) ||
		!plan.route.startsWith('/') ||
		(plan.kind !== 'layout' && plan.kind !== 'page') ||
		!['component', 'route_document', 'explicit'].includes(plan.discovery)
	) {
		throw new TypeError('Distributed boundary operation plan is invalid');
	}
	return Object.freeze({
		plan: Object.freeze({ ...plan }),
		artifact,
		binding
	});
}

export function resolveDistributedBoundaryVariables<
	TData,
	TVariables extends GraphqlVariables,
	TSession,
	TProps
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	sources: DistributedBoundaryVariableSources<TVariables>,
	context: DistributedBoundaryVariableContext<TSession, TProps>
): TVariables {
	if (context === null || typeof context !== 'object') {
		throw new TypeError('Distributed boundary variable context is required');
	}
	const entries: Array<[string, unknown]> = [];
	for (const [variable, source] of Object.entries(sources).sort(([left], [right]) =>
		left.localeCompare(right)
	)) {
		const value = resolveSource(source as DistributedBoundaryVariableSource, context);
		if (value !== OMITTED) entries.push([variable, value]);
	}
	return canonicalizeOperationVariables(
		artifact,
		Object.fromEntries(entries) as TVariables
	);
}

const OMITTED = Symbol('distributed.boundary.omitted');

function resolveSource<TSession, TProps>(
	source: DistributedBoundaryVariableSource,
	context: DistributedBoundaryVariableContext<TSession, TProps>
): unknown {
	switch (source.kind) {
		case 'omit':
			return OMITTED;
		case 'constant':
			return source.value;
		case 'route_param': {
			const value = ownValue(context.params, source.name);
			return value === undefined ? OMITTED : value;
		}
		case 'search_param': {
			const search = context.search;
			const value = isSearchParams(search) ? search.getAll(source.name) : ownValue(search, source.name);
			const values = value === undefined ? [] : Array.isArray(value) ? value : [value];
			if (values.length === 0) return OMITTED;
			return source.mode === 'all'
				? values.map((entry) => decodeSearchScalar(entry, source.scalar))
				: decodeSearchScalar(values[0], source.scalar);
		}
		case 'trusted_session': {
			const value = readPath(context.session, source.path, 'trusted session');
			return value === undefined ? OMITTED : value;
		}
		case 'forwarded_prop': {
			const value = readPath(context.props, source.path, 'forwarded prop');
			return value === undefined ? OMITTED : value;
		}
	}
}

function validateSources<TData, TVariables extends GraphqlVariables>(
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	sources: DistributedBoundaryVariableSources<TVariables>
): DistributedBoundaryVariableSources<TVariables> {
	if (sources === null || typeof sources !== 'object' || Array.isArray(sources)) {
		throw new TypeError('Distributed boundary variable sources must be an object');
	}
	const definitions = artifact.variableCodec?.variables;
	const defaults = artifact.variableCodec?.defaults;
	if (definitions === null || typeof definitions !== 'object' || Array.isArray(definitions)) {
		throw new TypeError('Distributed boundary artifact has no variable codec');
	}
	if (defaults === null || typeof defaults !== 'object' || Array.isArray(defaults)) {
		throw new TypeError('Distributed boundary artifact has no variable defaults');
	}
	const names = Object.keys(sources);
	if (names.length > MAX_BINDING_VARIABLES) {
		throw new TypeError(`Distributed boundary binding exceeds ${MAX_BINDING_VARIABLES} variables`);
	}
	const entries: Array<[string, DistributedBoundaryVariableSource]> = [];
	for (const name of names.sort()) {
		if (!GRAPHQL_NAME.test(name) || !Object.hasOwn(definitions, name)) {
			throw new TypeError(`Distributed boundary binding names unknown variable ${name}`);
		}
		const source = ownValue(sources, name);
		entries.push([name, validateSource(source, name, definitions[name]!)]);
	}
	for (const [name, definition] of Object.entries(definitions)) {
		const required =
			definition !== null &&
			typeof definition === 'object' &&
			'nullable' in definition &&
			definition.nullable === false;
		if (required && !Object.hasOwn(defaults, name) && !Object.hasOwn(sources, name)) {
			throw new TypeError(
				`Distributed boundary binding is missing required variable ${name}; use an explicit binding, parent/boundary query, client-only execution, or a better read root`
			);
		}
	}
	return Object.freeze(Object.fromEntries(entries)) as DistributedBoundaryVariableSources<TVariables>;
}

function variableGraphqlType(definition: ReplicaVariableInputRef): string {
	if (definition.kind === 'list') return `[${variableGraphqlType(definition.item)}]`;
	return definition.kind === 'scalar' ? definition.scalar : '';
}

function validateSource(
	value: unknown,
	variable: string,
	definition: ReplicaVariableInputRef
): DistributedBoundaryVariableSource {
	const source = exactRecord(value, `binding source ${variable}`);
	const kind = ownValue(source, 'kind');
	if (typeof kind !== 'string') {
		throw new TypeError(`Distributed boundary source ${variable} has no kind`);
	}
	switch (kind) {
		case 'omit':
			exactKeys(source, ['kind'], variable);
			return Object.freeze({ kind: 'omit' });
		case 'route_param':
			exactKeys(source, ['kind', 'name'], variable);
			return Object.freeze({ kind: 'route_param', name: safeName(source.name, variable) });
		case 'search_param': {
			return normalizeSearchParamSource(source, variable, variableGraphqlType(definition));
		}
		case 'trusted_session':
			exactKeys(source, ['kind', 'path'], variable);
			return Object.freeze({ kind: 'trusted_session', path: safePath(source.path, variable) });
		case 'forwarded_prop':
			exactKeys(source, ['kind', 'path'], variable);
			return Object.freeze({ kind: 'forwarded_prop', path: safePath(source.path, variable) });
		case 'constant':
			exactKeys(source, ['kind', 'value'], variable);
			return Object.freeze({
				kind: 'constant',
				value: freezeJson(stableValue(source.value))
			});
		default:
			throw new TypeError(
				`Distributed boundary source ${variable} is unsupported; use an explicit binding, parent/boundary query, client-only execution, or a better read root`
			);
	}
}

function safePath(value: unknown, variable: string): readonly string[] {
	if (
		!Array.isArray(value) ||
		value.length === 0 ||
		value.length > MAX_PATH_SEGMENTS ||
		value.some((part) => typeof part !== 'string' || !GRAPHQL_NAME.test(part) || HOSTILE_KEYS.has(part))
	) {
		throw new TypeError(`Distributed boundary source ${variable} has an invalid path`);
	}
	return Object.freeze([...value]) as readonly string[];
}

function safeName(value: unknown, variable: string): string {
	if (typeof value !== 'string' || value.length === 0 || value.length > 512) {
		throw new TypeError(`Distributed boundary source ${variable} has an invalid name`);
	}
	return value;
}

function readPath(value: unknown, path: readonly string[], label: string): unknown {
	let current = value;
	for (const segment of path) {
		if (current === null || typeof current !== 'object') return undefined;
		const descriptor = Object.getOwnPropertyDescriptor(current, segment);
		if (descriptor === undefined) return undefined;
		if (!('value' in descriptor)) {
			throw new TypeError(`Distributed boundary ${label} path contains an accessor`);
		}
		current = descriptor.value;
	}
	return current;
}

function ownValue(value: object, key: string): unknown {
	const descriptor = Object.getOwnPropertyDescriptor(value, key);
	if (descriptor === undefined) return undefined;
	if (!('value' in descriptor)) {
		throw new TypeError('Distributed boundary input contains an accessor');
	}
	return descriptor.value;
}

function isSearchParams(value: unknown): value is URLSearchParams {
	return (
		value !== null &&
		typeof value === 'object' &&
		typeof (value as URLSearchParams).get === 'function' &&
		typeof (value as URLSearchParams).getAll === 'function'
	);
}

function exactRecord(value: unknown, label: string): Record<string, unknown> {
	if (
		value === null ||
		typeof value !== 'object' ||
		Array.isArray(value) ||
		(Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
	) {
		throw new TypeError(`Distributed ${label} must be a plain object`);
	}
	return value as Record<string, unknown>;
}

function exactKeys(
	value: Record<string, unknown>,
	allowed: readonly string[],
	variable: string,
	optional: readonly string[] = []
): void {
	const permitted = new Set(allowed);
	for (const key of Object.keys(value)) {
		if (!permitted.has(key)) {
			throw new TypeError(`Distributed boundary source ${variable} contains unknown field ${key}`);
		}
	}
	for (const key of allowed) {
		if (!optional.includes(key) && !Object.hasOwn(value, key)) {
			throw new TypeError(`Distributed boundary source ${variable} is missing field ${key}`);
		}
	}
}

function stableJson(value: unknown): string {
	return JSON.stringify(stableValue(value));
}

function stableValue(value: unknown): unknown {
	let visited = 0;
	const active = new Set<object>();
	const visit = (current: unknown, depth: number): unknown => {
		visited += 1;
		if (visited > MAX_LITERAL_VALUES || depth > MAX_LITERAL_DEPTH) {
			throw new TypeError('Distributed boundary constant exceeds structural limits');
		}
		if (
			current === null ||
			typeof current === 'string' ||
			typeof current === 'boolean'
		) return current;
		if (typeof current === 'number' && Number.isFinite(current)) return current;
		if (typeof current !== 'object') {
			throw new TypeError('Distributed boundary constant is not JSON-compatible');
		}
		if (active.has(current)) throw new TypeError('Distributed boundary constant is cyclic');
		active.add(current);
		try {
			if (Array.isArray(current)) return current.map((entry) => visit(entry, depth + 1));
			const record = exactRecord(current, 'boundary constant');
			return Object.fromEntries(
				Object.keys(record).sort().map((key) => {
					if (HOSTILE_KEYS.has(key)) {
						throw new TypeError('Distributed boundary constant contains a hostile object key');
					}
					return [key, visit(ownValue(record, key), depth + 1)];
				})
			);
		} finally {
			active.delete(current);
		}
	};
	return visit(value, 0);
}

function fnv1a64(value: string): string {
	let hash = 0xcbf29ce484222325n;
	for (const byte of new TextEncoder().encode(value)) {
		hash ^= BigInt(byte);
		hash = BigInt.asUintN(64, hash * 0x100000001b3n);
	}
	return hash.toString(16).padStart(16, '0');
}

function freezeJson(value: unknown): unknown {
	if (value === null || typeof value !== 'object') return value;
	if (Array.isArray(value)) {
		for (const entry of value) freezeJson(entry);
		return Object.freeze(value);
	}
	for (const entry of Object.values(value as Record<string, unknown>)) freezeJson(entry);
	return Object.freeze(value);
}
