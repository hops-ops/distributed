import type { ReplicaValue } from '../types.js';
import { UTF8_ENCODER } from './constants.js';
import type {
	ReplicaQueryPlanPath,
	ReplicaQueryPlanReason,
	ReplicaQueryPlanReasonCode
} from './types.js';

export function reason(
	code: ReplicaQueryPlanReasonCode,
	path: ReplicaQueryPlanPath,
	message: string
): ReplicaQueryPlanReason {
	return Object.freeze({
		code,
		path: Object.freeze([...path]),
		message
	});
}

export function isScalarCodecPair(scalar: unknown, codec: unknown): boolean {
	if (typeof scalar !== 'string' || typeof codec !== 'string') return false;
	return (
		`${scalar}:${codec}` === 'ID:string' ||
		`${scalar}:${codec}` === 'String:string' ||
		`${scalar}:${codec}` === 'Bytea:base64' ||
		`${scalar}:${codec}` ===
			'Timestamptz:string_unvalidated_timestamp' ||
		`${scalar}:${codec}` === 'Boolean:boolean' ||
		`${scalar}:${codec}` === 'Int:int32' ||
		`${scalar}:${codec}` === 'Float:float64' ||
		`${scalar}:${codec}` ===
			'BigInt:json_number_precision_limited' ||
		`${scalar}:${codec}` === 'JSON:json'
	);
}

export function isPortableNumericCodec(codec: string): boolean {
	return (
		codec === 'int32' ||
		codec === 'float64' ||
		codec === 'json_number_precision_limited'
	);
}

export function isSafeIntegerNumber(value: ReplicaValue): value is number {
	return typeof value === 'number' && Number.isSafeInteger(value);
}

export function isInt32(value: ReplicaValue): value is number {
	return (
		Number.isInteger(value) &&
		(value as number) >= -2_147_483_648 &&
		(value as number) <= 2_147_483_647
	);
}

export function isFiniteNumber(value: unknown): value is number {
	return typeof value === 'number' && Number.isFinite(value);
}

export function isName(value: unknown): value is string {
	return typeof value === 'string' && value.length > 0;
}

export function isRecord(value: ReplicaValue | unknown): value is {
	readonly [key: string]: ReplicaValue;
} {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function isReplicaValue(value: unknown, ancestors = new Set<object>()): value is ReplicaValue {
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean' ||
		isFiniteNumber(value)
	) {
		return true;
	}
	if (typeof value !== 'object' || ancestors.has(value)) return false;
	ancestors.add(value);
	const valid = Array.isArray(value)
		? value.every((entry) => isReplicaValue(entry, ancestors))
		: (Object.getPrototypeOf(value) === Object.prototype ||
				Object.getPrototypeOf(value) === null) &&
			Object.values(value).every((entry) => isReplicaValue(entry, ancestors));
	ancestors.delete(value);
	return valid;
}

/**
 * Text compares by its UTF-8 bytes, for equality, `IN`, `<`/`>` and
 * `ORDER BY` alike. The GraphQL SQL compiler pins the matching binary
 * collation, so this answer is the server's on SQLite and on PostgreSQL.
 * Equal bytes means identical strings, so equality needs only `===`.
 */
export function compareUtf8(left: string, right: string): -1 | 0 | 1 {
	if (left === right) return 0;
	const leftBytes = UTF8_ENCODER.encode(left);
	const rightBytes = UTF8_ENCODER.encode(right);
	const length = Math.min(leftBytes.length, rightBytes.length);
	for (let index = 0; index < length; index += 1) {
		const a = leftBytes[index]!;
		const b = rightBytes[index]!;
		if (a !== b) return a < b ? -1 : 1;
	}
	return leftBytes.length < rightBytes.length ? -1 : 1;
}
