import type {
	ReplicaAuthoritativeScope,
	ReplicaClientSurface
} from '../../types.js';
import {
	MAX_TRANSPORT_RETRIES
} from '../constants.js';
import { ReplicaCommandRuntimeError } from '../errors.js';
import { compareCodeUnits } from '../../../lib/compare-code-units.js';
import { isPlainRecord } from '../../../lib/is-plain-record.js';

export function normalizeRetries(value: number | undefined): number {
	if (value === undefined) return 0;
	if (!Number.isSafeInteger(value) || value < 0 || value > MAX_TRANSPORT_RETRIES) {
		throw new TypeError(
			`transportRetries must be an integer between 0 and ${MAX_TRANSPORT_RETRIES}`
		);
	}
	return value;
}

export function cloneScope(scope: ReplicaAuthoritativeScope): ReplicaAuthoritativeScope {
	return Object.freeze({
		protocolVersion: 1,
		schemaHash: scope.schemaHash,
		authorizationGeneration: scope.authorizationGeneration,
		cacheScope: scope.cacheScope
	});
}

export function sameSurface(
	left: ReplicaClientSurface | undefined,
	right: ReplicaClientSurface
): boolean {
	if (
		left === undefined ||
		left.kind !== right.kind ||
		left.name !== right.name
	) {
		return false;
	}
	return (
		left.kind === 'role' ||
		(right.kind === 'application' &&
			left.eligible_roles.length === right.eligible_roles.length &&
			left.eligible_roles.every(
				(role, index) => role === right.eligible_roles[index]
			) &&
			left.schema_roles.length === right.schema_roles.length &&
			left.schema_roles.every(
				(role, index) => role === right.schema_roles[index]
			))
	);
}

export function cloneSurface(surface: ReplicaClientSurface): ReplicaClientSurface {
	return surface.kind === 'role'
		? Object.freeze({ kind: 'role', name: surface.name })
		: Object.freeze({
				kind: 'application',
				name: surface.name,
				eligible_roles: Object.freeze([...surface.eligible_roles]),
				schema_roles: Object.freeze([...surface.schema_roles])
			});
}

export function linkAbortSignals(
	signals: readonly (AbortSignal | undefined)[]
): Readonly<{
	signal: AbortSignal | undefined;
	dispose(): void;
}> {
	const sources = [
		...new Set(
			signals.filter(
				(signal): signal is AbortSignal => signal !== undefined
			)
		)
	];
	if (sources.length === 0) {
		return Object.freeze({
			signal: undefined,
			dispose(): void {}
		});
	}
	if (sources.length === 1) {
		return Object.freeze({
			signal: sources[0],
			dispose(): void {}
		});
	}
	const controller = new AbortController();
	const listeners = new Map<AbortSignal, () => void>();
	let disposed = false;
	const dispose = (): void => {
		if (disposed) return;
		disposed = true;
		for (const [source, listener] of listeners) {
			source.removeEventListener('abort', listener);
		}
		listeners.clear();
	};
	const abort = (signal: AbortSignal) => {
		if (!controller.signal.aborted) {
			controller.abort(signal.reason);
		}
		dispose();
	};
	for (const source of sources) {
		if (source.aborted) {
			abort(source);
			break;
		}
		const listener = () => abort(source);
		listeners.set(source, listener);
		source.addEventListener('abort', listener, { once: true });
		// Close the check/register race if a source aborted synchronously.
		if (source.aborted) {
			listener();
			break;
		}
	}
	return Object.freeze({ signal: controller.signal, dispose });
}

export function waitForCommandOperation<T>(
	operation: Promise<T> | T,
	signal: AbortSignal | undefined
): Promise<T> {
	const result = Promise.resolve(operation);
	if (signal === undefined) return result;
	return new Promise<T>((resolve, reject) => {
		let settled = false;
		const finish = (complete: () => void): void => {
			if (settled) return;
			settled = true;
			signal.removeEventListener('abort', onAbort);
			complete();
		};
		const onAbort = (): void => {
			finish(() =>
				reject(
					signal.reason ??
						new ReplicaCommandRuntimeError('REPLICA_COMMAND_ABORTED')
				)
			);
		};
		signal.addEventListener('abort', onAbort, { once: true });
		void result.then(
			(value) => finish(() => resolve(value)),
			(error: unknown) => finish(() => reject(error))
		);
		// Close the check/register race if the signal aborted synchronously.
		if (signal.aborted) onAbort();
	});
}

export function outputInvalid(path: string): never {
	throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_PROTOCOL_INVALID', {
		cause: new TypeError(`invalid command output at ${path}`)
	});
}

export function comparePropertyKeys(left: PropertyKey, right: PropertyKey): number {
	return compareCodeUnits(String(left), String(right));
}

export { compareCodeUnits, isPlainRecord };
