import type { GraphqlVariables } from '../types.js';
import type {
	DistributedBoundaryVariableSource,
	DistributedBoundaryVariableSources
} from './boundary-variables.js';
import type {
	DistributedSearchParamSource,
	DistributedSearchScalar
} from './search-variables.js';

const GRAPHQL_ISLAND_BINDINGS = Symbol.for(
	'@hops-ops/distributed/graphql-island-bindings'
);
const HOSTILE_KEYS = new Set(['__proto__', 'prototype', 'constructor']);

/**
 * Define the exceptional variables for one colocated GraphQL island.
 *
 * Save the default export as `<document>.bindings.js`. Route parameters with
 * the same name and GraphQL variable defaults need no sidecar entry.
 */
export function defineGraphqlIslandBindings<
	TVariables extends GraphqlVariables = GraphqlVariables
>(
	sources: DistributedBoundaryVariableSources<TVariables>
): DistributedBoundaryVariableSources<TVariables> {
	if (
		sources === null ||
		typeof sources !== 'object' ||
		Array.isArray(sources) ||
		(Object.getPrototypeOf(sources) !== Object.prototype &&
			Object.getPrototypeOf(sources) !== null)
	) {
		throw new TypeError('GraphQL island bindings must be an object');
	}
	const bindings: Record<string | symbol, unknown> = {};
	for (const key of Object.keys(sources)) {
		if (HOSTILE_KEYS.has(key)) {
			throw new TypeError(`GraphQL island binding name ${key} is unsafe`);
		}
		const descriptor = Object.getOwnPropertyDescriptor(sources, key);
		if (descriptor === undefined || !('value' in descriptor)) {
			throw new TypeError(`GraphQL island binding ${key} must be a data property`);
		}
		bindings[key] = descriptor.value;
	}
	Object.defineProperty(bindings, GRAPHQL_ISLAND_BINDINGS, {
		value: true,
		enumerable: false
	});
	return Object.freeze(bindings) as DistributedBoundaryVariableSources<TVariables>;
}

/** @internal Validate a discovered sidecar without trusting its prototype. */
export function isGraphqlIslandBindings(
	value: unknown
): value is DistributedBoundaryVariableSources<GraphqlVariables> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
	const descriptor = Object.getOwnPropertyDescriptor(value, GRAPHQL_ISLAND_BINDINGS);
	return descriptor !== undefined && 'value' in descriptor && descriptor.value === true;
}

export function routeParam(name: string): DistributedBoundaryVariableSource {
	return Object.freeze({ kind: 'route_param', name });
}

export function searchParam<TScalar extends DistributedSearchScalar>(
	name: string,
	scalar: TScalar
): DistributedSearchParamSource<TScalar, 'first'> & { mode: 'first' };
export function searchParam<TScalar extends DistributedSearchScalar, TMode extends 'first' | 'all'>(
	name: string,
	scalar: TScalar,
	mode: TMode
): DistributedSearchParamSource<TScalar, TMode> & { mode: TMode };
export function searchParam(
	name: string,
	scalar: DistributedSearchScalar,
	mode: 'first' | 'all' = 'first'
): DistributedSearchParamSource & { mode: 'first' | 'all' } {
	return Object.freeze({ kind: 'search_param', name, scalar, mode });
}

export function sessionClaim(...path: string[]): DistributedBoundaryVariableSource {
	return Object.freeze({ kind: 'trusted_session', path: Object.freeze(path) });
}

export function forwardedProp(...path: string[]): DistributedBoundaryVariableSource {
	return Object.freeze({ kind: 'forwarded_prop', path: Object.freeze(path) });
}

export function constant<TValue>(
	value: TValue
): DistributedBoundaryVariableSource<TValue> {
	return Object.freeze({ kind: 'constant', value });
}

export function omitVariable(): DistributedBoundaryVariableSource {
	return Object.freeze({ kind: 'omit' });
}
