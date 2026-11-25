import { isPlainRecord } from '../../../lib/is-plain-record.js';
import { createReplicaCommandId } from '../../command-id.js';
import { generateDefault } from '../../commands/clone.js';
import type { ReplicaCommandInputDefault } from '../../commands/types.js';
import type { ReplicaCommandCall, ReplicaCommandCallOptions } from '../types.js';

/**
 * Start one command call: fix its command id and generate its declared
 * input defaults now, so the caller holds both synchronously, then dispatch
 * with exactly those values.
 *
 * Only absent top-level defaults are generated here; the dispatch validates
 * the whole input as before and rejects the returned promise when it is
 * invalid.
 */
export function sendCommand(
	defaults: readonly ReplicaCommandInputDefault[] | undefined,
	hasInput: boolean,
	input: unknown,
	callOptions: ReplicaCommandCallOptions<unknown>,
	dispatch: (
		input: unknown,
		callOptions: ReplicaCommandCallOptions<unknown>
	) => Promise<unknown>
): ReplicaCommandCall<unknown, unknown> {
	let commandId: string;
	let sentInput: unknown;
	try {
		commandId = callOptions.commandId ?? createReplicaCommandId();
		sentInput = hasInput
			? withGeneratedDefaults(defaults ?? [], input, callOptions)
			: undefined;
	} catch (error) {
		return Object.freeze(
			Object.assign(Promise.reject(error), {
				commandId: callOptions.commandId ?? '',
				input: hasInput ? input : undefined
			})
		) as ReplicaCommandCall<unknown, unknown>;
	}
	const promise = dispatch(sentInput, { ...callOptions, commandId });
	return Object.freeze(
		Object.assign(promise, { commandId, input: sentInput })
	) as ReplicaCommandCall<unknown, unknown>;
}

function withGeneratedDefaults(
	defaults: readonly ReplicaCommandInputDefault[],
	input: unknown,
	callOptions: ReplicaCommandCallOptions<unknown>
): unknown {
	if (defaults.length === 0 || !isPlainRecord(input)) return input;
	let filled: Record<string, unknown> | undefined;
	for (const entry of defaults) {
		if (entry.path.length !== 1) continue;
		const field = entry.path[0]!;
		const present =
			Object.prototype.hasOwnProperty.call(input, field) &&
			(input as Record<string, unknown>)[field] !== undefined;
		if (present) continue;
		filled ??= { ...(input as Record<string, unknown>) };
		filled[field] = generateDefault(entry, callOptions.generators, `input.${field}`);
	}
	return filled === undefined ? input : Object.freeze(filled);
}
