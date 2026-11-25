import { compareUtf8 } from '../lib/compare-utf8.js';
import type { GraphqlVariables } from '../types.js';
import type { ReplicaValue, ReplicaWindowArtifact, ReplicaWindowKey } from './types.js';

/**
 * Keyset helpers for framework window adapters (package-internal).
 *
 * A `@window` operation orders rows by `window.keys`: the `order_by` columns
 * and then the primary key, which makes every row's key tuple unique. Rows
 * compare by those values exactly as the server orders them (strings by
 * UTF-8 bytes, numbers numerically), so a merged list sorts like one server
 * page and two rows are the same row exactly when their tuples are equal.
 */

function keyValue(key: ReplicaWindowKey, row: unknown): string | number | undefined {
	if (row === null || typeof row !== 'object') return undefined;
	const value = (row as Readonly<Record<string, unknown>>)[key.responseKey];
	if (key.codec === 'string') return typeof value === 'string' ? value : undefined;
	return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

/** The row's keyset values, or `undefined` when a key is missing. */
export function replicaWindowKeyValues(
	window: ReplicaWindowArtifact,
	row: unknown
): readonly (string | number)[] | undefined {
	const values: (string | number)[] = [];
	for (const key of window.keys) {
		const value = keyValue(key, row);
		if (value === undefined) return undefined;
		values.push(value);
	}
	return values;
}

/**
 * Negative when `left` comes first in window order. Both tuples must come
 * from `replicaWindowKeyValues` for the same window.
 */
export function compareReplicaWindowKeys(
	window: ReplicaWindowArtifact,
	left: readonly (string | number)[],
	right: readonly (string | number)[]
): number {
	for (const [index, key] of window.keys.entries()) {
		const a = left[index]!;
		const b = right[index]!;
		const order =
			typeof a === 'string' && typeof b === 'string'
				? compareUtf8(a, b)
				: Math.sign((a as number) - (b as number));
		if (order !== 0) return key.direction === 'desc' ? -order : order;
	}
	return 0;
}

/** Variables selecting the rows after `values` in window order. */
export function replicaWindowCursorVariables(
	window: ReplicaWindowArtifact,
	values: readonly (string | number)[]
): GraphqlVariables {
	return Object.fromEntries(
		window.keys.map((key, index) => [key.variable, values[index] as ReplicaValue])
	);
}

/**
 * Validate a page size against the compiler's contract. A page anchored at
 * the head's newest row asks for the rest of the head plus one page, so two
 * pages must fit in the server's max limit.
 */
export function replicaWindowPageSize(
	window: ReplicaWindowArtifact,
	pageSize: number | undefined
): number {
	const size = pageSize ?? window.pageSize;
	if (!Number.isSafeInteger(size) || size < 1 || size * 2 - 1 > window.maxLimit) {
		throw new RangeError(
			`window pageSize must be an integer from 1 to ${Math.floor((window.maxLimit + 1) / 2)}`
		);
	}
	return size;
}
