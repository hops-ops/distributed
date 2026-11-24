import type { GraphqlVariables } from '../../types.js';
import type { ReplicaValue } from '../types.js';
import { canonicalJson } from '../../lib/canonical-json.js';
import { freezeRecord } from '../../lib/freeze-record.js';

export function canonicalVariables(variables: GraphqlVariables): string {
	return canonicalJson(cloneJsonObject(variables));
}

export function cloneJsonObject(
	value: GraphqlVariables
): Readonly<Record<string, ReplicaValue>> {
	const entries: Array<readonly [string, ReplicaValue]> = [];
	for (const [key, entry] of Object.entries(value)) {
		if (entry === undefined) continue;
		entries.push([key, cloneJsonValue(entry)]);
	}
	return freezeRecord(entries);
}

export function cloneJsonValue(value: unknown, ancestors = new Set<object>()): ReplicaValue {
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean' ||
		typeof value === 'number'
	) {
		if (typeof value === 'number' && !Number.isFinite(value)) {
			throw new TypeError('replica values must contain only finite numbers');
		}
		return value;
	}
	if (typeof value !== 'object') {
		throw new TypeError('replica values must be JSON-compatible');
	}
	if (ancestors.has(value)) throw new TypeError('replica values must not contain cycles');
	ancestors.add(value);
	let cloned: ReplicaValue;
	if (Array.isArray(value)) {
		cloned = Object.freeze(value.map((entry) => cloneJsonValue(entry, ancestors)));
	} else {
		const prototype = Object.getPrototypeOf(value);
		if (prototype !== Object.prototype && prototype !== null) {
			throw new TypeError('replica objects must be plain JSON objects');
		}
		cloned = freezeRecord(
			Object.entries(value).flatMap(([key, entry]) =>
				entry === undefined ? [] : [[key, cloneJsonValue(entry, ancestors)] as const]
			)
		);
	}
	ancestors.delete(value);
	return cloned;
}
