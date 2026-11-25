import {
	parseGraphqlResponseExtensions,
	type DistributedCommandMetadata,
	type DistributedProtocolEnvelope
} from '../../protocol.js';
import { sameScope } from '../distributed-replica/hydration.js';
import { compareCodeUnits } from '../../lib/compare-code-units.js';
import { matchReplicaTrustedPresetInventory } from '../commands/presets.js';
import { prepareReplicaCommandWithTrustedPresets } from '../commands/prepare.js';
import { verifyReplicaCommandReceipt } from '../commands/receipt.js';
import type {
	ReplicaCommandArtifact,
	ReplicaCommandRevalidationPlan,
	ReplicaPreparedCommand
} from '../commands/types.js';
import type { ReplicaResultEnvelope } from '../types.js';
import {
	applyOptimisticEffects,
	preparedDispatchKeys,
	preparedSemanticChanges
} from './lib/effects.js';
import {
	attachAuthorityAbort,
	callerProjectedPromise,
	confirmDirectProjection,
	failTrackedProjection,
	monitorPendingProjection,
	pendingProjection,
	settleProjectionFailure,
	settleProjectionSuccess,
	settleTrackedProjection
} from './lib/projection.js';
import {
	cloneScope,
	linkAbortSignals,
	normalizeRetries,
	waitForCommandOperation
} from './lib/util.js';
import { commandOutput, projectionExpectationFingerprint } from './lib/output.js';
import {
	commandStatusArtifact,
	commandSurfaceContract,
	normalizeInventory
} from './lib/inventory.js';
import {
	commandStatusRequest,
	commandTransportRequest,
	dispatchPrepared,
	graphqlCommandRejection,
	requireCommandEnvelope,
	requireCommandRejectionEnvelope
} from './lib/transport.js';
import { defineBoundCommand, freezeCommandTree } from './lib/binding.js';
import { sendCommand } from './lib/sent.js';
import { requireStatusEnvelope, validateStatusProgression } from './lib/status.js';
import { ReplicaCommandRuntimeError } from './errors.js';
import {
	replicaCommandAuthority,
	replicaCommandFreshness,
	replicaCommandProjectionDelta,
	replicaCommandReadRecord,
	replicaResultObservation,
	replicaCommandAwaitsFetch,
	replicaCommandLayerHeld
} from './symbols.js';
import type {
	CapturedAuthority,
	CommandEntry,
	CommandStatusTracker,
	PendingProjection,
	ReplicaBoundCommands,
	ReplicaCommandAuthorityHost,
	ReplicaCommandAuthoritySnapshot,
	ReplicaCommandCallOptions,
	ReplicaCommandProjectedOutcome,
	ReplicaCommandReceipt,
	ReplicaCommandRecoveryReceipt,
	ReplicaCommandRuntime,
	ReplicaCommandRuntimeOptions,
	ReplicaCommandStatus,
	ReplicaCommandTransport,
	ReplicaCommandTransportResult,
	SemanticReplica
} from './types.js';
import { operationsFromProjectionDelta } from '../projection-delta/resolve.js';
import { validateProjectionMetadataAuthority } from '../projection-delta/validate.js';
import type {
	ProjectionCapabilityArm,
	ProjectionCapabilityMutation,
	ProjectionDelta,
	ProjectionDeltaMutation,
	ProjectionDeltaRecoveryTarget,
	ProjectionDeltaScope,
	PreparedProjectionOperation,
	ReplicaCommandProjection
} from '../projection-delta/types.js';

function hasCompleteProjectionObservations(
	metadata: DistributedCommandMetadata
): boolean {
	if (metadata.expects.length === 0 || metadata.observations.length === 0) {
		return false;
	}
	const observed = new Set(
		metadata.observations
			.filter((observation) => observation.causationId === metadata.causationId)
			.map(projectionExpectationFingerprint)
	);
	return metadata.expects.every((expectation) =>
		observed.has(projectionExpectationFingerprint(expectation))
	);
}

function assertActualProjectionCapabilities(
	contract: ReplicaCommandProjection,
	delta: ProjectionDelta
): void {
	for (let ordinal = 0; ordinal < delta.occurrences.length; ordinal += 1) {
		const references = new Set<number>();
		for (const item of [...delta.operations, ...delta.recoveries]) {
			if (item.occurrence_ordinal !== ordinal) continue;
			for (const reference of item.projection_refs) references.add(reference);
		}
		let selectedEvents:
			| Map<string, ProjectionCapabilityArm['event']>
			| undefined;
		for (const reference of references) {
			const operations = delta.operations.filter(
				(item) =>
					item.occurrence_ordinal === ordinal &&
					item.projection_refs.includes(reference)
			);
			const recoveries = delta.recoveries.filter(
				(item) =>
					item.occurrence_ordinal === ordinal &&
					item.projection_refs.includes(reference)
			);
			const candidates = contract.capabilities.arms.filter(
				(arm) =>
					arm.projection_ref === reference &&
					operations.every(({ mutation }) =>
						capabilityAllowsMutation(arm, mutation)
					) &&
					recoveries.every(({ target }) =>
						capabilityAllowsRecovery(arm, target)
					)
			);
			if (candidates.length === 0) {
				throw new Error(
					'actual projection delta is outside every generated event arm'
				);
			}
			const candidateEvents = new Map(
				candidates.map(({ event }) => [eventIdentity(event), event])
			);
			if (selectedEvents === undefined) {
				selectedEvents = candidateEvents;
				continue;
			}
			for (const identity of selectedEvents.keys()) {
				if (!candidateEvents.has(identity)) selectedEvents.delete(identity);
			}
			if (selectedEvents.size === 0) {
				throw new Error(
					'actual projection refs disagree on their generated event arm'
				);
			}
		}
	}
}

function eventIdentity(event: ProjectionCapabilityArm['event']): string {
	return JSON.stringify([event.id, event.name, event.version]);
}

function capabilityAllowsMutation(
	arm: ProjectionCapabilityArm,
	mutation: ProjectionDeltaMutation
): boolean {
	switch (mutation.op) {
		case 'upsert': {
			const capabilities = recordCapabilities(arm, mutation.scope);
			const replace = capabilities[0]?.replace;
			return (
				capabilities.some(({ upsert }) => upsert) &&
				replace !== undefined &&
				capabilities.every((capability) =>
					sameStrings(capability.replace, replace)
				) &&
				sameStrings(mutation.replace, replace) &&
				sameStrings(
					mutation.fields.map(({ field }) => field),
					replace
				) &&
				sameStrings(
					unionNames(capabilities.flatMap(({ fields }) => fields)),
					replace
				)
			);
		}
		case 'patch': {
			const capabilities = recordCapabilities(arm, mutation.scope);
			const allowed = new Set(
				capabilities
					.filter(({ patch }) => patch)
					.flatMap(({ fields }) => fields)
			);
			return (
				allowed.size !== 0 &&
				[...mutation.set.map(({ field }) => field), ...mutation.unset].every(
					(field) => allowed.has(field)
				)
			);
		}
		case 'delete':
			return recordCapabilities(arm, mutation.scope).some(
				({ delete: allowed }) => allowed
			);
		case 'link':
		case 'unlink':
			return arm.mutations.some(
				(capability) =>
					capability.kind === 'relationship' &&
					capability.relationship === mutation.relationship &&
					(mutation.op === 'link' ? capability.link : capability.unlink) &&
					capabilityAllowsScope(
						arm,
						mutation.source,
						capability.source_model,
						capability.source_key
					) &&
					capabilityAllowsScope(
						arm,
						mutation.target,
						capability.target_model,
						capability.target_key
					)
			);
		case 'invalidate_model':
			return (
				capabilityAllowsPartition(arm, mutation.partition) &&
				arm.mutations.some(
					(capability) =>
						capability.kind === 'model' &&
						capability.model === mutation.model
				)
			);
		case 'invalidate_relationship':
			return arm.mutations.some(
				(capability) =>
					capability.kind === 'relationship' &&
					capability.relationship === mutation.relationship &&
					capabilityAllowsScope(
						arm,
						mutation.source,
						capability.source_model,
						capability.source_key
					)
			);
	}
}

function capabilityAllowsRecovery(
	arm: ProjectionCapabilityArm,
	target: ProjectionDeltaRecoveryTarget
): boolean {
	switch (target.kind) {
		case 'record':
			return recordCapabilities(arm, target.scope).length !== 0;
		case 'relationship':
			return arm.mutations.some(
				(capability) =>
					capability.kind === 'relationship' &&
					capability.relationship === target.relationship &&
					capabilityAllowsScope(
						arm,
						target.source,
						capability.source_model,
						capability.source_key
					)
			);
		case 'model':
			return (
				capabilityAllowsPartition(arm, target.partition) &&
				arm.mutations.some(
					(capability) =>
						capability.kind === 'model' &&
						capability.model === target.model
				)
			);
	}
}

function recordCapabilities(
	arm: ProjectionCapabilityArm,
	scope: ProjectionDeltaScope
): readonly Extract<ProjectionCapabilityMutation, { readonly kind: 'record' }>[] {
	if (!capabilityAllowsPartition(arm, scope.partition)) return [];
	return arm.mutations.filter(
		(capability): capability is Extract<
			ProjectionCapabilityMutation,
			{ readonly kind: 'record' }
		> =>
			capability.kind === 'record' &&
			capability.model === scope.model &&
			sameStrings(
				capability.key,
				scope.key.map(({ field }) => field)
			)
	);
}

function capabilityAllowsScope(
	arm: ProjectionCapabilityArm,
	scope: ProjectionDeltaScope,
	model: string,
	key: readonly string[]
): boolean {
	return (
		capabilityAllowsPartition(arm, scope.partition) &&
		scope.model === model &&
		sameStrings(
			key,
			scope.key.map(({ field }) => field)
		)
	);
}

function capabilityAllowsPartition(
	arm: ProjectionCapabilityArm,
	partition: ProjectionDeltaScope['partition'] | undefined
): boolean {
	return arm.partition.kind === 'unit'
		? partition?.kind === 'unit'
		: partition === undefined || partition.kind === 'opaque';
}

function unionNames(values: readonly string[]): readonly string[] {
	return Object.freeze([...new Set(values)].sort(compareCodeUnits));
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
	return (
		left.length === right.length &&
		left.every((value, index) => value === right[index])
	);
}

function actualProjectionRevalidation(
	base: ReplicaCommandRevalidationPlan,
	delta: ProjectionDelta
): ReplicaCommandRevalidationPlan {
	const models = new Set(base.models);
	const relationships = new Map(
		base.relationships.map((relationship) => [
			JSON.stringify([
				relationship.sourceModel,
				relationship.field,
				relationship.targetModel
			]),
			relationship
		])
	);
	const addRelationship = (
		sourceModel: string,
		field: string,
		targetModel: string
	): void => {
		const relationship = Object.freeze({ sourceModel, field, targetModel });
		relationships.set(
			JSON.stringify([sourceModel, field, targetModel]),
			relationship
		);
	};
	for (const { mutation } of delta.operations) {
		switch (mutation.op) {
			case 'upsert':
			case 'patch':
			case 'delete':
				models.add(mutation.scope.model);
				break;
			case 'link':
			case 'unlink':
				models.add(mutation.source.model);
				models.add(mutation.target.model);
				addRelationship(
					mutation.source.model,
					mutation.relationship,
					mutation.target.model
				);
				break;
			case 'invalidate_model':
				models.add(mutation.model);
				break;
			case 'invalidate_relationship':
				models.add(mutation.source.model);
				break;
		}
	}
	for (const { target } of delta.recoveries) {
		switch (target.kind) {
			case 'record':
				models.add(target.scope.model);
				break;
			case 'relationship':
				models.add(target.source.model);
				break;
			case 'model':
				models.add(target.model);
				break;
		}
	}
	return Object.freeze({
		version: 1 as const,
		required: true,
		dependencies: Object.freeze([...base.dependencies]),
		models: Object.freeze([...models].sort(compareCodeUnits)),
		relationships: Object.freeze(
			[...relationships.values()].sort((left, right) =>
				compareCodeUnits(
					JSON.stringify([
						left.sourceModel,
						left.field,
						left.targetModel
					]),
					JSON.stringify([
						right.sourceModel,
						right.field,
						right.targetModel
					])
				)
			)
		)
	});
}

export function createReplicaCommandRuntime<
	const TEntries extends Readonly<Record<string, CommandEntry>>
>(
	replica: ReplicaCommandAuthorityHost,
	transport: ReplicaCommandTransport,
	entries: TEntries,
	options: ReplicaCommandRuntimeOptions = {}
): ReplicaCommandRuntime<TEntries> {
	const inventory = normalizeInventory(entries);
	if (options.diagnostics !== undefined) {
		for (const { artifact } of inventory) {
			try {
				options.diagnostics.command(artifact);
			} catch (error) {
				options.onBackgroundError?.(error);
			}
		}
	}
	const contract = commandSurfaceContract(
		inventory.map(({ artifact }) => artifact),
		options.status?.protocol.trustedPresets
	);
	const statusArtifact =
		options.status === undefined
			? undefined
			: commandStatusArtifact(options.status, contract);
	const replicaRevalidate =
		typeof replica.revalidate === 'function'
			? replica.revalidate.bind(replica)
			: undefined;
	if (statusArtifact !== undefined && transport.status === undefined) {
		throw new TypeError(
			'generated command status artifact requires transport.status'
		);
	}
	if (
		replicaRevalidate === undefined &&
		inventory.some(
			({ artifact }) =>
				artifact.projection !== undefined ||
				artifact.revalidation.required
		)
	) {
		throw new TypeError(
			'generated modeled projection or required revalidation plan requires replica.revalidate'
		);
	}
	const registration = replica[replicaCommandAuthority]?.(contract);
	/*
	 * Per-command state. Every entry ends with its command's optimistic layer:
	 *
	 * - `pending`: an accepted command whose `projected` promise waits for a
	 *   proving frame. Deleted when that promise settles (proof, terminal
	 *   status, scope change, dispose).
	 * - `unmanagedLayers`: a layer with no `pending` controller that this
	 *   runtime still has to retire or roll back (in flight, recovery, required
	 *   revalidation). Deleted when the runtime retires or rejects it, or by
	 *   `sweep` once the replica no longer holds the layer.
	 * - `appliedProjections`: the actual projection delta applied to a layer,
	 *   kept so a replayed status or live envelope is recognised instead of
	 *   applied twice, and so revalidation covers the models that delta
	 *   touched. Deleted by `sweep` once the replica no longer holds the layer.
	 *   A revalidation reads its plan when it starts, so a sweep never drops
	 *   the plan of a revalidation already under way.
	 * - `dispatchTails`: the last dispatch per record key. Deleted when that
	 *   dispatch settles.
	 */
	const pending = new Map<string, PendingProjection>();
	const appliedProjections = new Map<
		ReplicaPreparedCommand<unknown, unknown>,
		Readonly<{
			canonical: string;
			revalidation: ReplicaCommandRevalidationPlan | undefined;
		}>
	>();
	const dispatchTails = new Map<string, Promise<void>>();
	const unmanagedLayers = new Set<string>();
	const runtimeAbort = new AbortController();
	let disposed = false;

	/*
	 * A layer can leave the replica without this runtime retiring it: a frame
	 * proves its receipt, or a scope change empties the cache. A host without
	 * the layer question keeps its entries until the runtime is disposed.
	 */
	const layerHeld = (commandId: string): boolean =>
		replica[replicaCommandLayerHeld]?.(commandId) ?? true;
	/*
	 * Drop the entries of commands whose layer is gone. Runs at the end of
	 * every entry point (dispatch, status read, observed result, background
	 * revalidation), after the synchronous work that may still read them.
	 */
	const sweep = (): void => {
		for (const commandId of unmanagedLayers) {
			if (!layerHeld(commandId)) unmanagedLayers.delete(commandId);
		}
		for (const prepared of appliedProjections.keys()) {
			if (!layerHeld(prepared.commandId)) appliedProjections.delete(prepared);
		}
	};
	let sweptGeneration: AbortSignal | undefined;
	/*
	 * A closed authorization generation empties the replica right after it
	 * aborts this signal, so sweep once that purge has run.
	 */
	const sweepWhenGenerationCloses = (signal: AbortSignal | undefined): void => {
		if (signal === undefined || signal === sweptGeneration) return;
		sweptGeneration = signal;
		signal.addEventListener('abort', () => queueMicrotask(sweep), {
			once: true
		});
	};

	const reserveDispatch = (
		prepared: ReplicaPreparedCommand<unknown, unknown>
	): Readonly<{ wait: Promise<void>; release(): void }> => {
		const keys = preparedDispatchKeys(prepared);
		if (keys.length === 0) {
			return Object.freeze({
				wait: Promise.resolve(),
				release(): void {}
			});
		}
		const predecessors = keys.flatMap((key) => {
			const predecessor = dispatchTails.get(key);
			return predecessor === undefined ? [] : [predecessor];
		});
		let resolve!: () => void;
		const tail = new Promise<void>((settled) => {
			resolve = settled;
		});
		for (const key of keys) dispatchTails.set(key, tail);
		let released = false;
		return Object.freeze({
			wait: Promise.all(predecessors).then(() => undefined),
			release(): void {
				if (released) return;
				released = true;
				for (const key of keys) {
					if (dispatchTails.get(key) === tail) dispatchTails.delete(key);
				}
				resolve();
			}
		});
	};

	type ValidatedActualProjection = Readonly<{
		canonical?: string;
		operations?: readonly PreparedProjectionOperation[];
		revalidation?: ReplicaCommandRevalidationPlan;
		requiresRevalidation: boolean;
	}>;

	const validateActualProjection = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		metadata: import('../../protocol.js').DistributedCommandMetadata,
		authority: CapturedAuthority
	): ValidatedActualProjection => {
		if (prepared.projection === undefined) {
			if (metadata.projection !== undefined || metadata.expects.length !== 0) {
				throw new Error('unmodeled command returned a projection delta');
			}
			return Object.freeze({ requiresRevalidation: false });
		}
		const actual = metadata.projection;
		if (actual === undefined) {
			throw new Error('modeled command omitted its projection delta');
		}
		const canonical = validateProjectionMetadataAuthority(
			actual,
			prepared.projection.contract,
			{
				surface: prepared.transport.protocol.surface,
				schemaHash: prepared.transport.protocol.schemaHash,
				protocolHash: prepared.transport.protocol.protocolHash,
				authorizationGeneration:
					authority.scope.authorizationGeneration,
				cacheScope: authority.scope.cacheScope,
				causationId: metadata.causationId
			}
		);
		const expected = actual.obligations.map((obligation) => ({
			projection:
				actual.delta.projections[obligation.projectionRef]!.program_id,
			model: obligation.model,
			scopeToken: obligation.scopeToken
		}));
		if (
			expected.length !== metadata.expects.length ||
			expected.some((item, index) => {
				const candidate = metadata.expects[index];
				return (
					candidate === undefined ||
					candidate.projection !== item.projection ||
					candidate.model !== item.model ||
					candidate.scopeToken !== item.scopeToken
				);
			})
		) {
			throw new Error('projection obligations do not match command expectations');
		}
		const previous = appliedProjections.get(prepared);
		if (previous !== undefined) {
			if (previous.canonical !== canonical) {
				throw new Error('projection delta changed during command replay');
			}
			return Object.freeze({
				requiresRevalidation: actual.revalidate
			});
		}
		if (!layerHeld(prepared.commandId)) {
			// The layer already retired: there is nothing left to replace.
			return Object.freeze({ requiresRevalidation: actual.revalidate });
		}
		assertActualProjectionCapabilities(
			prepared.projection.contract,
			actual.delta
		);
		const operations = operationsFromProjectionDelta(actual.delta.operations);
		const seam = replica[replicaCommandProjectionDelta];
		if (seam === undefined) {
			throw new Error('replica does not implement projection delta replacement');
		}
		return Object.freeze({
			canonical,
			operations,
			revalidation:
				actual.revalidate
					? actualProjectionRevalidation(
							prepared.revalidation,
							actual.delta
						)
					: undefined,
			requiresRevalidation: actual.revalidate
		});
	};

	const commitActualProjection = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		validated: ValidatedActualProjection
	): boolean => {
		if (
			validated.canonical === undefined ||
			validated.operations === undefined
		) {
			return validated.requiresRevalidation;
		}
		const seam = replica[replicaCommandProjectionDelta]!;
		if (validated.revalidation !== undefined) replica[replicaCommandFreshness]?.(prepared.commandId, validated.revalidation);
		const actualPrepared = Object.freeze({
			...prepared,
			optimistic: Object.freeze({
				version: 1 as const,
				operations: validated.operations,
				fallback: 'revalidate' as const
			})
		});
		seam.call(
			replica,
			prepared.commandId,
			(writer) => applyOptimisticEffects(writer, validated.operations!),
			preparedSemanticChanges(actualPrepared)
		);
		appliedProjections.set(
			prepared,
			Object.freeze({
				canonical: validated.canonical,
				revalidation: validated.revalidation
			})
		);
		return validated.requiresRevalidation;
	};

	const applyActualProjection = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		metadata: import('../../protocol.js').DistributedCommandMetadata,
		authority: CapturedAuthority
	): boolean =>
		commitActualProjection(
			prepared,
			validateActualProjection(prepared, metadata, authority)
		);

	const validateProjectionForState = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		metadata: import('../../protocol.js').DistributedCommandMetadata,
		authority: CapturedAuthority
	): ValidatedActualProjection => {
		switch (metadata.state) {
			case 'succeeded':
			case 'succeeded_pending_projection':
			case 'atomic':
				return validateActualProjection(prepared, metadata, authority);
			case 'in_progress':
			case 'rejected':
			case 'projection_failed':
			case 'expired':
			case 'unknown':
				return Object.freeze({ requiresRevalidation: false });
		}
	};

	const rejectUnmanagedLayer = (commandId: string): boolean => {
		unmanagedLayers.delete(commandId);
		return replica.rejectOptimisticLayer(commandId);
	};

	const retireLayerAfterAuthoritativeRevalidation = (
		commandId: string
	): boolean => {
		if (!replica.markOptimisticLayerAccepted(commandId)) {
			// The authoritative result may already have retired this layer.
			unmanagedLayers.delete(commandId);
			return false;
		}
		replica.confirmOptimisticLayer(commandId, () => undefined);
		unmanagedLayers.delete(commandId);
		return true;
	};

	const authoritySnapshot = (): ReplicaCommandAuthoritySnapshot =>
		registration?.read() ?? {
			generation: replica.authorizationGeneration,
			scope: replica.scope,
			trustedPresets: Object.freeze([])
		};

	const currentAuthority = (): CapturedAuthority => {
		if (disposed) {
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_DISPOSED');
		}
		try {
			options.lifecycle?.assertDispatchOpen();
		} catch (error) {
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_RELOADING', {
				cause: error
			});
		}
		const snapshot = authoritySnapshot();
		const scope = snapshot.scope;
		if (
			scope === undefined ||
			scope.protocolVersion !== contract.protocolVersion ||
			scope.schemaHash !== contract.schemaHash
		) {
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_AUTHORITY_UNAVAILABLE'
			);
		}
		const matched = matchReplicaTrustedPresetInventory(
			contract.trustedPresets,
			snapshot.trustedPresets
		);
		return Object.freeze({
			generation: snapshot.generation,
			scope: cloneScope(scope),
			trustedPresets: matched.values,
			...(snapshot.signal === undefined ? {} : { signal: snapshot.signal })
		});
	};

	const stillCurrent = (captured: CapturedAuthority): boolean => {
		const current = authoritySnapshot();
		return (
			!disposed &&
			current.generation === captured.generation &&
			current.scope !== undefined &&
			sameScope(current.scope, captured.scope)
		);
	};

	const revalidationPlan = (
		prepared: ReplicaPreparedCommand<unknown, unknown>
	) => appliedProjections.get(prepared)?.revalidation ?? prepared.revalidation;
	const awaitsFetch = (
		prepared: ReplicaPreparedCommand<unknown, unknown>
	): boolean =>
		replica[replicaCommandAwaitsFetch]?.(
			prepared.commandId,
			revalidationPlan(prepared)
		) ?? false;

	const revalidate = async (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		authoritySignal?: AbortSignal
	): Promise<boolean> => {
		if (replicaRevalidate === undefined) return false;
		const revalidationSignals = linkAbortSignals([
			authoritySignal,
			runtimeAbort.signal
		]);
		try {
			await waitForCommandOperation(
				replicaRevalidate(revalidationPlan(prepared)),
				revalidationSignals.signal
			);
			return true;
		} catch (error) {
			if (!disposed && authoritySignal?.aborted !== true) {
				options.onBackgroundError?.(error);
			}
			throw error;
		} finally {
			revalidationSignals.dispose();
		}
	};
	const revalidateInBackground = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		authority?: CapturedAuthority
	): void => {
		void revalidate(prepared, authority?.signal).catch(() => undefined);
	};
	const revalidateAndConfirmUnmanagedInBackground = (
		prepared: ReplicaPreparedCommand<unknown, unknown>,
		authority: CapturedAuthority
	): void => {
		void revalidate(prepared, authority.signal)
			.then((refreshed) => {
				if (
					refreshed &&
					stillCurrent(authority) &&
					unmanagedLayers.has(prepared.commandId)
				) {
					/*
					 * A generated required revalidation is the canonical base
					 * fence for commands without a finite observation contract.
					 * Retire their accepted overlay only after that fetch settles.
					 */
					retireLayerAfterAuthoritativeRevalidation(
						prepared.commandId
					);
				}
				sweep();
			})
			.catch(() => undefined);
	};
	const revalidateDispositionAndRetireInBackground = (
		controller: PendingProjection,
		onFailure?: () => void
	): void => {
		void revalidate(controller.prepared, controller.authority.signal)
			.then((refreshed) => {
				if (
					refreshed &&
					!controller.settled &&
					stillCurrent(controller.authority) &&
					pending.get(controller.commandId) === controller
				) {
					retireLayerAfterAuthoritativeRevalidation(
						controller.commandId
					);
					settleProjectionSuccess(controller);
					pending.delete(controller.commandId);
					sweep();
				} else if (!refreshed) onFailure?.();
			})
			.catch(() => onFailure?.());
	};

	const commitStatusProgression = (
		tracker: CommandStatusTracker,
		status: ReplicaCommandStatus
	): void => {
		tracker.state = status.state;
		if (status.metadata !== undefined) {
			tracker.metadata = status.metadata;
			if (tracker.pending !== undefined) {
				tracker.pending.metadata = status.metadata;
			}
		}
	};

	const statusReader = <TInput, TOutput>(
		prepared: ReplicaPreparedCommand<TInput, TOutput>,
		authority: CapturedAuthority,
		tracker: CommandStatusTracker
	): (() => Promise<ReplicaCommandStatus>) => {
		const read = async (): Promise<ReplicaCommandStatus> => {
			if (disposed) {
				throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_DISPOSED', {
					commandId: prepared.commandId
				});
			}
			if (statusArtifact === undefined || transport.status === undefined) {
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_STATUS_UNAVAILABLE',
					{ commandId: prepared.commandId }
				);
			}
			if (!stillCurrent(authority)) {
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_SCOPE_INVALIDATED',
					{ commandId: prepared.commandId }
				);
			}
			let result: ReplicaCommandTransportResult;
			const statusSignals = linkAbortSignals([
				authority.signal,
				runtimeAbort.signal
			]);
			try {
				result = await waitForCommandOperation(
					transport.status(
						commandStatusRequest(
							statusArtifact,
							prepared.commandId,
							statusSignals.signal
						)
					),
					statusSignals.signal
				);
			} catch (error) {
				if (disposed) {
					throw new ReplicaCommandRuntimeError(
						'REPLICA_COMMAND_DISPOSED',
						{ commandId: prepared.commandId, cause: error }
					);
				}
				throw new ReplicaCommandRuntimeError(
					authority.signal?.aborted
						? 'REPLICA_COMMAND_SCOPE_INVALIDATED'
						: 'REPLICA_COMMAND_TRANSPORT_AMBIGUOUS',
					{ commandId: prepared.commandId, cause: error }
				);
			} finally {
				statusSignals.dispose();
			}
			if (!stillCurrent(authority)) {
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_SCOPE_INVALIDATED',
					{ commandId: prepared.commandId }
				);
			}
			let status: ReplicaCommandStatus;
			let statusRequiresRevalidation = false;
			try {
				status = requireStatusEnvelope(
					result,
					statusArtifact,
					prepared,
					authority,
					contract
				);
				validateStatusProgression(tracker.state, tracker.metadata, status);
				if (status.metadata !== undefined) {
					if (
						tracker.pending !== undefined &&
						status.metadata.causationId !==
							tracker.pending.causationId
					) {
						throw new Error(
							'command status changed pending causation identity'
						);
					}
					if (
						status.metadata.projectionDisposition === 'revalidate'
					) {
						/*
						 * This is current-envelope authority to refetch, never
						 * authority to validate or apply the omitted old-scope
						 * delta.
						 */
						statusRequiresRevalidation = true;
					} else if (prepared.consistency !== 'atomic') {
						// Atomic rows are confirmed on the mutation response,
						// not via async projection-delta status envelopes.
						const validated = validateProjectionForState(
							prepared as ReplicaPreparedCommand<unknown, unknown>,
							status.metadata,
							authority
						);
						statusRequiresRevalidation = commitActualProjection(
							prepared as ReplicaPreparedCommand<unknown, unknown>,
							validated
						);
					}
				}
			} catch (error) {
				const failure = new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_PROTOCOL_INVALID',
					{ commandId: prepared.commandId, cause: error }
				);
				rejectUnmanagedLayer(prepared.commandId);
				revalidateInBackground(prepared, authority);
				failTrackedProjection(tracker, pending, failure);
				throw failure;
			}
			commitStatusProgression(tracker, status);
			switch (status.state) {
				case 'rejected':
					rejectUnmanagedLayer(prepared.commandId);
					failTrackedProjection(
						tracker,
						pending,
						new ReplicaCommandRuntimeError(
							'REPLICA_COMMAND_REJECTED',
							{
								commandId: prepared.commandId,
								state: status.state
							}
						)
					);
					break;
				case 'projection_failed':
				case 'expired':
					rejectUnmanagedLayer(prepared.commandId);
					revalidateInBackground(prepared, authority);
					failTrackedProjection(
						tracker,
						pending,
						new ReplicaCommandRuntimeError(
							'REPLICA_COMMAND_PROJECTION_FAILED',
							{
								commandId: prepared.commandId,
								state: status.state
							}
						)
					);
					break;
				case 'succeeded':
				case 'succeeded_pending_projection':
				case 'atomic': {
					const metadata = status.metadata!;
					if (metadata.projectionDisposition === 'revalidate') {
						if (metadata.state === 'succeeded_pending_projection') {
							/*
							 * Historical evidence is still pending. Refresh
							 * canonical current-scope queries opportunistically,
							 * but retain optimism and keep polling until the
							 * server reports a terminal state.
							 */
							revalidateInBackground(prepared, authority);
							break;
						}
						let refreshed = false;
						try {
							refreshed = await revalidate(
								prepared,
								authority.signal
							);
						} catch (error) {
							if (disposed) {
								throw new ReplicaCommandRuntimeError(
									'REPLICA_COMMAND_DISPOSED',
									{
										commandId: prepared.commandId,
										cause: error
									}
								);
							}
							if (
								authority.signal?.aborted ||
								!stillCurrent(authority)
							) {
								throw new ReplicaCommandRuntimeError(
									'REPLICA_COMMAND_SCOPE_INVALIDATED',
									{
										commandId: prepared.commandId,
										cause: error
									}
								);
							}
							// The retained monitor will retry this exact
							// current-scope revalidation on the next status read.
						}
						if (refreshed && stillCurrent(authority)) {
							const controller = tracker.pending;
							if (
								controller !== undefined &&
								!controller.settled &&
								pending.get(prepared.commandId) === controller
							) {
								retireLayerAfterAuthoritativeRevalidation(
									prepared.commandId
								);
								settleTrackedProjection(tracker, pending);
							} else if (
								unmanagedLayers.has(prepared.commandId)
							) {
								retireLayerAfterAuthoritativeRevalidation(
									prepared.commandId
								);
							}
						}
						break;
					}
					const remainsPending = replica.markOptimisticLayerAccepted(
						prepared.commandId,
						metadata
					);
					const exactDelta =
						!prepared.revalidation.required &&
						!statusRequiresRevalidation;
					const proven =
						metadata.state === 'atomic' ||
						hasCompleteProjectionObservations(metadata);
					if (!remainsPending) {
						unmanagedLayers.delete(prepared.commandId);
						if (tracker.pending !== undefined) {
							settleTrackedProjection(tracker, pending);
						}
					} else if (
						exactDelta &&
						proven &&
						!awaitsFetch(prepared)
					) {
						/*
						 * An exact delta already shows the server's effect, and
						 * every row it shows is one the base holds (or an open
						 * live stream will deliver it). The proof settles the
						 * projected wait; the accepted layer stays until a later
						 * comparable authoritative result seals it, without
						 * racing sibling commands with a command-triggered query.
						 */
						settleTrackedProjection(tracker, pending);
					} else if (
						metadata.state === 'atomic' ||
						(exactDelta && proven) ||
						(metadata.state === 'succeeded' &&
							metadata.expects.length === 0 &&
							!exactDelta)
					) {
						/*
						 * Status is causal truth, but it carries no canonical
						 * query payload. `atomic` fences asynchronous projection;
						 * `succeeded` is terminal when the generated contract has
						 * no confirmations; a proven exact delta whose layer
						 * shows a row the base lacks can get that row only from
						 * a fetch when no live stream covers it (`@load` only, or
						 * no live delivery). In every case only a query started
						 * after the proof may retire optimism.
						 */
						let refreshed = false;
						try {
							refreshed = await revalidate(
								prepared,
								authority.signal
							);
						} catch (error) {
							if (disposed) {
								throw new ReplicaCommandRuntimeError(
									'REPLICA_COMMAND_DISPOSED',
									{
										commandId: prepared.commandId,
										cause: error
									}
								);
							}
							if (
								authority.signal?.aborted ||
								!stillCurrent(authority)
							) {
								throw new ReplicaCommandRuntimeError(
									'REPLICA_COMMAND_SCOPE_INVALIDATED',
									{
										commandId: prepared.commandId,
										cause: error
									}
								);
							}
							// Keep the accepted layer and let the background
							// status monitor retry the exact generated plan.
						}
						if (disposed) {
							throw new ReplicaCommandRuntimeError(
								'REPLICA_COMMAND_DISPOSED',
								{ commandId: prepared.commandId }
							);
						}
						if (
							authority.signal?.aborted ||
							!stillCurrent(authority)
						) {
							throw new ReplicaCommandRuntimeError(
								'REPLICA_COMMAND_SCOPE_INVALIDATED',
								{ commandId: prepared.commandId }
							);
						}
						const controller = tracker.pending;
						if (refreshed) {
							if (
								controller !== undefined &&
								!controller.settled &&
								pending.get(prepared.commandId) === controller
							) {
								retireLayerAfterAuthoritativeRevalidation(
									prepared.commandId
								);
								settleTrackedProjection(tracker, pending);
							} else if (
								unmanagedLayers.has(prepared.commandId)
							) {
								/*
								 * Recovery receipts and commands without a
								 * finite projection awaitable have no
								 * PendingProjection controller. The same
								 * post-status canonical fetch still owns
								 * retirement of their retained overlay.
								 */
								retireLayerAfterAuthoritativeRevalidation(
									prepared.commandId
								);
							}
						}
					}
					break;
				}
				case 'unknown':
				case 'in_progress':
					break;
			}
			return status;
		};
		return () => {
			if (tracker.inFlight !== undefined) return tracker.inFlight;
			let current: Promise<ReplicaCommandStatus>;
			current = read().finally(() => {
				if (tracker.inFlight === current) tracker.inFlight = undefined;
				sweep();
			});
			tracker.inFlight = current;
			return current;
		};
	};

	const invoke = async <TInput, TOutput>(
		artifact: ReplicaCommandArtifact<TInput, TOutput>,
		input: TInput,
		callOptions: ReplicaCommandCallOptions<TOutput>
	): Promise<ReplicaCommandReceipt<TOutput>> => {
		try {
			return await dispatchCommand(artifact, input, callOptions);
		} finally {
			sweep();
		}
	};

	const dispatchCommand = async <TInput, TOutput>(
		artifact: ReplicaCommandArtifact<TInput, TOutput>,
		input: TInput,
		callOptions: ReplicaCommandCallOptions<TOutput>
	): Promise<ReplicaCommandReceipt<TOutput>> => {
		const authority = currentAuthority();
		sweepWhenGenerationCloses(authority.signal);
		const prepared = prepareReplicaCommandWithTrustedPresets(
			artifact,
			input,
			authority.trustedPresets,
			callOptions
		);
		const transportRetries = normalizeRetries(callOptions.transportRetries);
		const semanticChanges = preparedSemanticChanges(prepared);
		const requestSignals = linkAbortSignals([
			authority.signal,
			callOptions.signal,
			runtimeAbort.signal
		]);
		const request = commandTransportRequest(prepared, requestSignals.signal);
		if (request.signal?.aborted) {
			requestSignals.dispose();
			throw new ReplicaCommandRuntimeError(
				stillCurrent(authority)
					? 'REPLICA_COMMAND_ABORTED'
					: 'REPLICA_COMMAND_SCOPE_INVALIDATED',
				{ commandId: prepared.commandId, cause: request.signal.reason }
			);
		}
		try {
			const pureHost = {
				readRecord: replica[replicaCommandReadRecord]?.bind(replica),
				pureFunctions: options.pureFunctions
			};
			replica[replicaCommandFreshness]?.(prepared.commandId, prepared.revalidation);
			(replica as SemanticReplica).createOptimisticLayer(
				prepared.commandId,
				(writer) =>
					applyOptimisticEffects(
						writer,
						prepared.optimistic.operations,
						pureHost
					),
				semanticChanges
			);
		} catch (error) {
			requestSignals.dispose();
			throw error;
		}
		unmanagedLayers.add(prepared.commandId);

		const statusTracker: CommandStatusTracker = {
			state: undefined,
			metadata: undefined,
			inFlight: undefined
		};
		const readStatus = statusReader(prepared, authority, statusTracker);
		const recovery: ReplicaCommandRecoveryReceipt<TOutput> = Object.freeze({
			commandId: prepared.commandId,
			status: readStatus
		});
		const recoveryForRetainedLayer = (
			revalidateOnRollback = false
		): ReplicaCommandRecoveryReceipt<TOutput> | undefined => {
			if (statusArtifact !== undefined) return recovery;
			/*
			 * A manual status-less runtime cannot make an ambiguous layer
			 * reachable again. Roll it back and let generated revalidation
			 * restore canonical truth instead of leaking optimism forever.
			 */
			rejectUnmanagedLayer(prepared.commandId);
			if (revalidateOnRollback) {
				revalidateInBackground(prepared, authority);
			}
			return undefined;
		};
		const dispatchReservation = reserveDispatch(prepared);
		let result: ReplicaCommandTransportResult;
		let dispatchAttempted = false;
		try {
			await dispatchReservation.wait;
			result = await dispatchPrepared(
				transport,
				request,
				transportRetries,
				() => {
					dispatchAttempted = true;
				}
			);
		} catch (error) {
			if (disposed) {
				rejectUnmanagedLayer(prepared.commandId);
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_DISPOSED',
					{ commandId: prepared.commandId, cause: error }
				);
			}
			if (!dispatchAttempted) {
				rejectUnmanagedLayer(prepared.commandId);
				throw new ReplicaCommandRuntimeError(
					stillCurrent(authority)
						? 'REPLICA_COMMAND_ABORTED'
						: 'REPLICA_COMMAND_SCOPE_INVALIDATED',
					{ commandId: prepared.commandId, cause: error }
				);
			}
			if (!stillCurrent(authority)) {
				rejectUnmanagedLayer(prepared.commandId);
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_SCOPE_INVALIDATED',
					{ commandId: prepared.commandId, cause: error }
				);
			}
			throw new ReplicaCommandRuntimeError(
				request.signal?.aborted
					? 'REPLICA_COMMAND_ABORTED'
					: 'REPLICA_COMMAND_TRANSPORT_AMBIGUOUS',
				{
					commandId: prepared.commandId,
					cause: error,
					recovery: recoveryForRetainedLayer(true)
				}
			);
		} finally {
			dispatchReservation.release();
			requestSignals.dispose();
		}

		if (!stillCurrent(authority)) {
			rejectUnmanagedLayer(prepared.commandId);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_SCOPE_INVALIDATED',
				{ commandId: prepared.commandId }
			);
		}

		// The HTTP lifecycle gate rejects before dispatch and before a receipt
		// exists. Classify only its exact failure shape, never a success/receipt.
		if (result.status === 503 && result.data == null && result.extensions === undefined &&
			result.errors?.length === 1 && result.errors[0].extensions?.code === 'APPLICATION_RELOADING') {
			rejectUnmanagedLayer(prepared.commandId);
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_RELOADING', {
				commandId: prepared.commandId
			});
		}

		const rejection = graphqlCommandRejection(result);
		if (rejection !== undefined) {
			try {
				const rejectionEnvelope = requireCommandRejectionEnvelope(
					result,
					prepared,
					authority
				);
				matchReplicaTrustedPresetInventory(
					contract.trustedPresets,
					rejectionEnvelope.trustedPresets
				);
			} catch (error) {
				revalidateInBackground(prepared, authority);
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_PROTOCOL_INVALID',
					{
						commandId: prepared.commandId,
						cause: error,
						recovery: recoveryForRetainedLayer()
					}
				);
			}
			rejectUnmanagedLayer(prepared.commandId);
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_REJECTED', {
				commandId: prepared.commandId,
				cause: new Error(rejection.message)
			});
		}

		let distributed: DistributedProtocolEnvelope;
		try {
			distributed = requireCommandEnvelope(result, prepared, authority);
			matchReplicaTrustedPresetInventory(
				contract.trustedPresets,
				distributed.trustedPresets
			);
		} catch (error) {
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROTOCOL_INVALID',
				{
					commandId: prepared.commandId,
					cause: error,
					recovery: recoveryForRetainedLayer()
				}
			);
		}

		const metadata = distributed.command!;
		if (metadata.state === 'rejected') {
			rejectUnmanagedLayer(prepared.commandId);
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_REJECTED', {
				commandId: prepared.commandId,
				state: metadata.state
			});
		}
		if (metadata.state === 'expired') {
			rejectUnmanagedLayer(prepared.commandId);
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROJECTION_FAILED',
				{ commandId: prepared.commandId, state: metadata.state }
			);
		}
		if (metadata.state === 'unknown' || metadata.state === 'in_progress') {
			statusTracker.state = metadata.state;
			statusTracker.metadata = metadata;
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_OUTCOME_PENDING',
				{
					commandId: prepared.commandId,
					state: metadata.state,
					recovery: recoveryForRetainedLayer(true)
				}
			);
		}
		if (metadata.state === 'projection_failed') {
			rejectUnmanagedLayer(prepared.commandId);
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROJECTION_FAILED',
				{ commandId: prepared.commandId, state: metadata.state }
			);
		}
		if ((result.errors?.length ?? 0) > 0) {
			rejectUnmanagedLayer(prepared.commandId);
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROTOCOL_INVALID',
				{ commandId: prepared.commandId }
			);
		}

		let output: TOutput;
		try {
			output = commandOutput(
				artifact,
				result.data,
				prepared.transport.mutationField
			) as TOutput;
		} catch (error) {
			rejectUnmanagedLayer(prepared.commandId);
			revalidateInBackground(prepared, authority);
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROTOCOL_INVALID',
				{
					commandId: prepared.commandId,
					cause: error,
					...(statusArtifact === undefined ? {} : { recovery })
				}
			);
		}
		/*
		 * Ship contract: Atomic seals from the atomic GraphQL row +
		 * direct `records` (confirmDirectProjection). Eventual applies
		 * projection-delta when present. Both strategies start from the same
		 * compiler-derived event→mutation preview — different response proof by
		 * design.
		 */
		let actualRequiresRevalidation = false;
		if (prepared.consistency !== 'atomic') {
			try {
				actualRequiresRevalidation = applyActualProjection(
					prepared as ReplicaPreparedCommand<unknown, unknown>,
					metadata,
					authority
				);
			} catch (error) {
				rejectUnmanagedLayer(prepared.commandId);
				revalidateInBackground(prepared, authority);
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_PROTOCOL_INVALID',
					{
						commandId: prepared.commandId,
						cause: error,
						...(statusArtifact === undefined ? {} : { recovery })
					}
				);
			}
		}
		let projected:
			| Promise<ReplicaCommandProjectedOutcome<TOutput>>
			| undefined;
		if (prepared.consistency === 'atomic') {
			if (metadata.state !== 'atomic') {
				statusTracker.state = metadata.state;
				statusTracker.metadata = metadata;
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_OUTCOME_PENDING',
					{
						commandId: prepared.commandId,
						state: metadata.state,
						recovery: recoveryForRetainedLayer(true)
					}
				);
			}
			try {
				confirmDirectProjection(replica, prepared, output, metadata);
				unmanagedLayers.delete(prepared.commandId);
			} catch (error) {
				rejectUnmanagedLayer(prepared.commandId);
				revalidateInBackground(prepared, authority);
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_PROTOCOL_INVALID',
					{
						commandId: prepared.commandId,
						cause: error,
						...(statusArtifact === undefined ? {} : { recovery })
					}
				);
			}
			statusTracker.state = metadata.state;
			statusTracker.metadata = metadata;
			projected = Promise.resolve(
				Object.freeze({
					commandId: prepared.commandId,
					state: 'atomic' as const,
					result: output,
					metadata
				})
			);
		} else {
			statusTracker.state = metadata.state;
			statusTracker.metadata = metadata;
			const remainsPending = replica.markOptimisticLayerAccepted(
				prepared.commandId,
				metadata
			);
			if (metadata.expects.length > 0) {
				const controller = pendingProjection(
					authority,
					metadata,
					prepared as ReplicaPreparedCommand<unknown, unknown>,
					statusTracker
				);
				statusTracker.pending = controller;
				if (!remainsPending) {
					unmanagedLayers.delete(prepared.commandId);
					settleProjectionSuccess(controller);
				} else {
					/*
					 * Receipt/status observations prove durable causality, not
					 * that canonical query data was atomically ingested. Keep
					 * the layer until a canonical frame retires it, or until
					 * a query started after a complete proof settles.
					 */
					pending.set(prepared.commandId, controller);
					unmanagedLayers.delete(prepared.commandId);
					attachAuthorityAbort(controller, () =>
						pending.delete(prepared.commandId)
					);
					const monitor = () => {
						if (
							statusArtifact !== undefined &&
							replicaRevalidate !== undefined
						) {
							monitorPendingProjection(
								controller,
								readStatus,
								() =>
									pending.get(prepared.commandId) === controller,
								options.onBackgroundError
							);
						}
					};
					if (
						replicaRevalidate !== undefined &&
						hasCompleteProjectionObservations(metadata) &&
						awaitsFetch(prepared)
					) {
						/*
						 * The receipt already proves the projection committed,
						 * but it carries no canonical data, and the layer shows
						 * a row the base lacks that no live stream will deliver.
						 * Fetch canonical data started after this proof, then
						 * retire. The status monitor retries a failed fetch.
						 */
						revalidateDispositionAndRetireInBackground(
							controller,
							monitor
						);
					} else {
						monitor();
					}
				}
				projected = callerProjectedPromise(
					controller,
					callOptions.signal
				) as Promise<ReplicaCommandProjectedOutcome<TOutput>>;
			}
		}

		if (prepared.revalidation.required || actualRequiresRevalidation) {
			revalidateAndConfirmUnmanagedInBackground(prepared, authority);
		}

		const receiptValue = {
			commandId: prepared.commandId,
			state: metadata.state as ReplicaCommandReceipt<TOutput>['state'],
			result: output,
			metadata,
			status: readStatus,
			...(projected === undefined ? {} : { projected })
		};
		const receipt = Object.freeze(
			receiptValue
		) as ReplicaCommandReceipt<TOutput>;
		try {
			await callOptions.onSucceeded?.(receipt);
		} catch (error) {
			options.onBackgroundError?.(error);
		}
		return receipt;
	};

	const commands: Record<string, unknown> = Object.create(null) as Record<
		string,
		unknown
	>;
	for (const { key, artifact } of inventory) {
		const defaults = artifact.inputDefaults?.defaults;
		const send = (
			input: unknown,
			callOptions: ReplicaCommandCallOptions<unknown>
		) =>
			sendCommand(
				defaults,
				artifact.input.kind !== 'none',
				input,
				callOptions,
				(sent, options) => invoke(artifact, sent, options)
			);
		defineBoundCommand(
			commands,
			key,
			artifact.input.kind === 'none'
				? (callOptions: ReplicaCommandCallOptions<unknown> = {}) =>
						send(undefined, callOptions)
				: (
						input: unknown,
						callOptions: ReplicaCommandCallOptions<unknown> = {}
					) => send(input, callOptions)
		);
	}
	freezeCommandTree(commands);

	const observeResult = (envelope: ReplicaResultEnvelope<unknown>): void => {
		if (disposed) return;
		try {
			settlePendingProjections(envelope);
		} finally {
			// The frame may have retired layers this runtime does not track.
			sweep();
		}
	};
	const settlePendingProjections = (
		envelope: ReplicaResultEnvelope<unknown>
	): void => {
		if (pending.size === 0) return;
		let distributed: DistributedProtocolEnvelope | undefined;
		try {
			distributed = parseGraphqlResponseExtensions(envelope.extensions)
				?.distributed;
		} catch (error) {
			options.onBackgroundError?.(error);
			return;
		}
		if (distributed === undefined) return;
		for (const [commandId, controller] of pending) {
			if (!stillCurrent(controller.authority)) {
				settleProjectionFailure(
					controller,
					new ReplicaCommandRuntimeError(
						'REPLICA_COMMAND_SCOPE_INVALIDATED',
						{ commandId }
					)
				);
				pending.delete(commandId);
				continue;
			}
			if (
				distributed.schemaHash !== controller.authority.scope.schemaHash ||
				distributed.authorizationGeneration !==
					controller.authority.scope.authorizationGeneration ||
				distributed.cacheScope !== controller.authority.scope.cacheScope
			) {
				continue;
			}
			const command = distributed.command;
			if (command?.commandId === commandId) {
				try {
					verifyReplicaCommandReceipt(controller.prepared, command);
					if (command.causationId !== controller.causationId) {
						throw new Error(
							'live command changed pending causation identity'
						);
					}
					const status = Object.freeze({
						commandId,
						state: command.state,
						metadata: command
					});
					validateStatusProgression(
						controller.tracker.state,
						controller.tracker.metadata,
						status
					);
					if (command.projectionDisposition !== 'revalidate') {
						const validated = validateProjectionForState(
							controller.prepared,
							command,
							controller.authority
						);
						commitActualProjection(controller.prepared, validated);
					}
					commitStatusProgression(controller.tracker, status);
				} catch (error) {
					replica.rejectOptimisticLayer(commandId);
					revalidateInBackground(
						controller.prepared,
						controller.authority
					);
					settleProjectionFailure(
						controller,
						new ReplicaCommandRuntimeError(
							'REPLICA_COMMAND_PROTOCOL_INVALID',
							{ commandId, cause: error }
						)
					);
					pending.delete(commandId);
					continue;
				}
				if (command.state === 'projection_failed') {
					replica.rejectOptimisticLayer(commandId);
					revalidateInBackground(
						controller.prepared,
						controller.authority
					);
					settleProjectionFailure(
						controller,
						new ReplicaCommandRuntimeError(
							'REPLICA_COMMAND_PROJECTION_FAILED',
							{ commandId, state: command.state }
						)
					);
					pending.delete(commandId);
					continue;
				}
				if (
					command.state === 'rejected' ||
					command.state === 'expired'
				) {
					replica.rejectOptimisticLayer(commandId);
					revalidateInBackground(
						controller.prepared,
						controller.authority
					);
					settleProjectionFailure(
						controller,
						new ReplicaCommandRuntimeError(
							command.state === 'rejected'
								? 'REPLICA_COMMAND_REJECTED'
								: 'REPLICA_COMMAND_PROJECTION_FAILED',
							{ commandId, state: command.state }
						)
					);
					pending.delete(commandId);
					continue;
				}
				if (command.projectionDisposition === 'revalidate') {
					if (
						command.state === 'succeeded_pending_projection'
					) {
						revalidateInBackground(
							controller.prepared,
							controller.authority
						);
					} else {
						revalidateDispositionAndRetireInBackground(controller);
					}
					continue;
				}
			}
			/*
			 * DistributedReplica is the authority on whether this frame's
			 * snapshot/observations were admissible. This callback runs only
			 * after that exact frame committed.
			 *
			 * Settle only once the overlay is retired. A frame without a
			 * command payload (a query refetch, perhaps one that raced the
			 * receipt) proves nothing about this command. Settling on it would
			 * stop the status monitor while the accepted layer remains, and
			 * with no live delivery nothing else would ever retire the layer:
			 * its row would stay pending. The command-state paths above still
			 * settle or reject `projected` (status regression can reject it).
			 */
			const remainsPending =
				replica.markOptimisticLayerAccepted(commandId);
			if (!remainsPending) {
				settleProjectionSuccess(controller);
				pending.delete(commandId);
			}
		}
	};
	const observationRegistration =
		replica[replicaResultObservation]?.(observeResult);

	return Object.freeze({
		commands: commands as ReplicaBoundCommands<TEntries>,
		observeResult,
		pendingCommandIds(): readonly string[] {
			return Object.freeze(
				[...new Set([...pending.keys(), ...unmanagedLayers])].sort()
			);
		},
		dispose(): void {
			if (disposed) return;
			disposed = true;
			runtimeAbort.abort(
				new ReplicaCommandRuntimeError('REPLICA_COMMAND_DISPOSED')
			);
			observationRegistration?.dispose();
			registration?.dispose();
			for (const commandId of unmanagedLayers) {
				replica.rejectOptimisticLayer(commandId);
			}
			unmanagedLayers.clear();
			for (const [commandId, controller] of pending) {
				replica.rejectOptimisticLayer(commandId);
				settleProjectionFailure(
					controller,
					new ReplicaCommandRuntimeError('REPLICA_COMMAND_DISPOSED', {
						commandId
					})
				);
			}
			pending.clear();
			appliedProjections.clear();
		},
		/**
		 * Package-internal: how many entries each per-command structure holds.
		 * The retention test compares it with a baseline.
		 */
		_sizes(): Readonly<Record<string, number>> {
			return Object.freeze({
				pendingProjections: pending.size,
				unmanagedLayers: unmanagedLayers.size,
				appliedProjections: appliedProjections.size,
				dispatchTails: dispatchTails.size
			});
		}
	});
}
