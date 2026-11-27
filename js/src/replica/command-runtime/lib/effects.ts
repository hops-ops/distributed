import type { ReplicaPreparedCommand } from '../../commands/types.js';
import type {
	PreparedProjectionOperation,
	PreparedProjectionScope,
	ReplicaPureFunction
} from '../../projection-delta/types.js';
import { replicaRecordKey } from '../../identity/keys.js';
import type { ReplicaIndexSemanticChange } from '../../index-maintenance/types.js';
import type {
	ReplicaIdentity,
	ReplicaModelArtifact,
	ReplicaOptimisticWriter,
	ReplicaValue
} from '../../types.js';

export type PureReduceHost = Readonly<{
	/** Read live cache fields for a known record (pre-layer). Fail-closed on miss. */
	readRecord?: (
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity
	) => Readonly<Record<string, ReplicaValue>> | undefined;
	pureFunctions?: Readonly<Record<string, ReplicaPureFunction>>;
}>;

/**
 * Expand pure reducers against known cache rows, then apply ordinary ops.
 * Missing row / unknown pure / pure null → skip that reduce (no invent).
 */
export function applyOptimisticEffects(
	writer: ReplicaOptimisticWriter,
	effects: readonly PreparedProjectionOperation[],
	host: PureReduceHost = {}
): void {
	const expanded = expandReduceKnownRecord(effects, host);
	for (const effect of expanded) {
		switch (effect.kind) {
			case 'upsert':
			case 'patch': {
				const model = modelFromKey(effect.scope);
				const fields = fieldsFromEffect(effect.scope, effect.fields);
				const unset =
					effect.kind === 'upsert'
						? effect.replace.filter((field) => !Object.hasOwn(effect.fields, field))
						: effect.unset;
				writer.writeRecord(model, identityFromKey(effect.scope), {
					fields,
					...(unset.length === 0 ? {} : { unset }),
					...(effect.kind === 'patch' ? { ifPresent: true } : {})
				});
				break;
			}
			case 'delete':
				writer.tombstoneRecord(
					modelFromKey(effect.scope),
					identityFromKey(effect.scope)
				);
				break;
			case 'link':
			case 'unlink':
			case 'invalidate_model':
			case 'invalidate_relationship':
			case 'reduce_known_record':
				// Task 8 consumes the exact semantic context. Guessing a to-one
				// record link for a to-many relationship would corrupt truth.
				// reduce_known_record is expanded above.
				break;
		}
	}
}

function expandReduceKnownRecord(
	effects: readonly PreparedProjectionOperation[],
	host: PureReduceHost
): readonly PreparedProjectionOperation[] {
	const out: PreparedProjectionOperation[] = [];
	for (const effect of effects) {
		if (effect.kind !== 'reduce_known_record') {
			out.push(effect);
			continue;
		}
		const pure = host.pureFunctions?.[effect.fn];
		const read = host.readRecord;
		if (pure === undefined || read === undefined) {
			continue;
		}
		const model = modelFromKey(effect.scope);
		const identity = identityFromKey(effect.scope);
		const current = read(model, identity);
		if (current === undefined) {
			continue;
		}
		let next: Readonly<Record<string, ReplicaValue>> | null;
		try {
			next = pure(current, effect.args);
		} catch {
			continue;
		}
		if (next === null) {
			continue;
		}
		const fields: Record<string, ReplicaValue> = Object.create(null) as Record<
			string,
			ReplicaValue
		>;
		for (const field of effect.assign) {
			if (!Object.prototype.hasOwnProperty.call(next, field)) {
				continue;
			}
			fields[field] = next[field] as ReplicaValue;
		}
		if (Object.keys(fields).length === 0) {
			continue;
		}
		out.push(
			Object.freeze({
				kind: 'patch' as const,
				scope: effect.scope,
				fields: Object.freeze(fields),
				unset: Object.freeze([]) as readonly string[],
				ifPresent: true as const
			})
		);
	}
	return Object.freeze(out);
}

export function preparedSemanticChanges<TInput, TOutput>(
	prepared: ReplicaPreparedCommand<TInput, TOutput>
): readonly ReplicaIndexSemanticChange[] {
	const dependencies = Object.freeze([...prepared.revalidation.dependencies]);
	const changes: ReplicaIndexSemanticChange[] = [];
	for (const effect of prepared.optimistic.operations) {
		switch (effect.kind) {
			case 'upsert':
			case 'patch':
			case 'delete':
			case 'reduce_known_record':
				// DistributedReplica captures ordinary writer mutations into the
				// same layer context. Supplying them again would double-apply the
				// semantic record operation. Pure reduce expands to patch first.
				break;
			case 'link':
			case 'unlink': {
				const source = modelFromKey(
					effect.source
				);
				const target = modelFromKey(
					effect.target
				);
				changes.push(
					Object.freeze({
						kind: effect.kind,
						sourceModel: effect.source.model,
						field: effect.relationship,
						targetModel: effect.target.model,
						sourceKey: replicaRecordKey(
							source,
							identityFromKey(effect.source)
						),
						targetKey: replicaRecordKey(
							target,
							identityFromKey(effect.target)
						),
						dependencies
					})
				);
				break;
			}
			case 'invalidate_model':
			case 'invalidate_relationship':
				changes.push(
					Object.freeze({
						kind: 'invalidate',
						dependencies
					})
				);
				break;
		}
	}
	return Object.freeze(changes);
}

/**
 * Exact record identities whose aggregate-facing command dispatches must retain
 * invocation order. Optimistic layers still apply immediately; only transport
 * waits for an earlier command touching the same modeled record to settle.
 */
export function preparedDispatchKeys<TInput, TOutput>(
	prepared: ReplicaPreparedCommand<TInput, TOutput>
): readonly string[] {
	const keys = new Set<string>();
	const addScope = (scope: PreparedProjectionScope): void => {
		keys.add(
			replicaRecordKey(modelFromKey(scope), identityFromKey(scope))
		);
	};

	for (const effect of prepared.optimistic.operations) {
		switch (effect.kind) {
			case 'upsert':
			case 'patch':
			case 'delete':
			case 'reduce_known_record':
				addScope(effect.scope);
				break;
			case 'link':
			case 'unlink':
				addScope(effect.source);
				addScope(effect.target);
				break;
			case 'invalidate_relationship':
				addScope(effect.source);
				break;
			case 'invalidate_model':
				/*
				 * A model invalidation has no compiler-proven aggregate
				 * identity. It cannot safely participate in a record queue.
				 */
				break;
		}
	}

	return Object.freeze([...keys].sort());
}

export function modelFromKey(
	scope: PreparedProjectionScope
): ReplicaModelArtifact {
	return Object.freeze({
		id: scope.model,
		identityFields: Object.freeze(scope.key.map(({ field }) => field))
	});
}

export function identityFromKey(
	key: PreparedProjectionScope
): readonly ReplicaValue[] {
	return Object.freeze(key.key.map(({ value }) => value));
}

export function fieldsFromEffect(
	scope: PreparedProjectionScope,
	fields: Readonly<Record<string, ReplicaValue>>
): Readonly<Record<string, ReplicaValue>> {
	const result: Record<string, ReplicaValue> = Object.create(null) as Record<
		string,
		ReplicaValue
	>;
	for (const field of [
		{ field: '__typename', value: scope.model },
		...scope.key,
		...Object.entries(fields).map(([field, value]) => ({ field, value }))
	]) {
		Object.defineProperty(result, field.field, {
			enumerable: true,
			configurable: false,
			writable: false,
			value: field.value
		});
	}
	return Object.freeze(result);
}
