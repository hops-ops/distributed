import type { CacheEngine } from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import type {
	DistributedOpaqueString,
	DistributedProtocolEnvelope,
	DistributedTrustedPreset
} from '../../protocol.js';
import { matchReplicaTrustedPresetInventory } from '../commands.js';
import type {
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticLayerInput
} from '../diagnostics.js';
import type { ReplicaIndexPlanRegistration } from '../index-maintenance.js';
import { validateReplicaOperationBinding as validatedArtifactBinding } from '../operation-binding.js';
import type {
	ReplicaOperationArtifact,
	ReplicaWriteSource
} from '../types.js';
import { protocolInvalid } from './clocks.js';
import { EMPTY_CACHE_SNAPSHOT, EMPTY_TRUSTED_PRESETS } from './constants.js';
import {
	canonicalTrustedPresets,
	trustedPresetDescriptorFingerprint,
	trustedPresetInventoryFingerprint
} from './helpers.js';
import type {
	AnonymousRecordProtocolClock,
	OperationProtocolGroup,
	OptimisticReceiptState,
	ProjectedRecordFence,
	ProtocolGeneration,
	QueryState,
	RecordProtocolClock,
	RegisteredCommandAuthorityContract
} from './types.js';
import type { ReplicaWatchState } from './watch.js';

/**
 * Host for protocol generation staging, purge, and authorization closeout.
 */
export type ProtocolHost = {
	readonly engine: CacheEngine;
	readonly queryStates: Map<string, QueryState>;
	readonly operationProtocols: Map<string, OperationProtocolGroup>;
	readonly operationGenerations: Map<string, number>;
	readonly recordClocks: Map<string, RecordProtocolClock>;
	readonly recordKeysByScope: Map<DistributedOpaqueString, string>;
	readonly projectedRecordFences: Map<string, ProjectedRecordFence>;
	readonly membershipFences: Map<
		string,
		Map<string, Set<string>>
	>;
	readonly deferredMembershipConfirms: Set<string>;
	readonly anonymousRecordClocks: Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>;
	readonly optimisticReceipts: Map<string, OptimisticReceiptState>;
	readonly diagnosticLayers: Map<string, ReplicaDiagnosticLayerInput> | undefined;
	readonly indexPlanRegistrations: Map<string, ReplicaIndexPlanRegistration>;
	readonly watches: Map<
		string,
		Set<ReplicaWatchState<unknown, GraphqlVariables>>
	>;
	readonly diagnostics: { readonly enabled: boolean };
	getProtocolGeneration(): ProtocolGeneration | undefined;
	setProtocolGeneration(value: ProtocolGeneration | undefined): void;
	getProtocolGenerationSequence(): number;
	bumpProtocolGenerationSequence(): void;
	disposeAuthorizationGeneration(): void;
	getTrustedPresets(): readonly DistributedTrustedPreset[];
	setTrustedPresets(value: readonly DistributedTrustedPreset[]): void;
	getCommandAuthorityContract():
		| RegisteredCommandAuthorityContract
		| undefined;
	setProjectionGeneration(value: number): void;
	setNextIndexRevision(value: string): void;
	setDiagnosticLayerSequence(value: number): void;
	clearIndexMaintenance(): void;
	abortAuthorization(): void;
	closeActiveTransports(): void;
	syncDiagnostics(): void;
	emitState(key: string, allowFetch: boolean): void;
	diagnosticEvent(event: ReplicaDiagnosticEventInput): void;
	/** Register the index plans of every rendered operation again. */
	restoreIndexPlans(): void;
};

export function validateProtocolBinding<
	TData,
	TVariables extends GraphqlVariables
>(
	host: ProtocolHost,
	artifact: ReplicaOperationArtifact<TData, TVariables>,
	envelope: DistributedProtocolEnvelope,
	source: ReplicaWriteSource
): void {
	const binding = artifact.protocol;
	if (binding === undefined) {
		protocolInvalid('extensions.distributed');
	}
	if (binding.version !== 1) {
		throw new TypeError('replica artifact protocol version is unsupported');
	}
	if (binding.schemaHash !== envelope.schemaHash) {
		purgeProtocolGeneration(host);
		protocolInvalid('extensions.distributed.schemaHash');
	}
	const expectedOperation =
		source === 'live' ? artifact.live?.id : binding.operation;
	if (
		expectedOperation === undefined ||
		envelope.operation !== expectedOperation
	) {
		protocolInvalid('extensions.distributed.operation');
	}
}

export function stageProtocolGeneration(
	host: ProtocolHost,
	envelope: DistributedProtocolEnvelope
): ProtocolGeneration {
	const next: ProtocolGeneration = {
		protocolVersion: 1,
		cacheScope: envelope.cacheScope,
		schemaHash: envelope.schemaHash,
		authorizationGeneration: envelope.authorizationGeneration
	};
	const current = host.getProtocolGeneration();
	if (current === undefined) return next;
	if (
		current.cacheScope === next.cacheScope &&
		current.schemaHash === next.schemaHash &&
		current.authorizationGeneration === next.authorizationGeneration
	) {
		return current;
	}
	purgeProtocolGeneration(host);
	return next;
}

export function stageTrustedPresets<
	TData,
	TVariables extends GraphqlVariables
>(
	host: ProtocolHost,
	incoming: readonly DistributedTrustedPreset[],
	nextGeneration: ProtocolGeneration,
	artifact: ReplicaOperationArtifact<TData, TVariables>
): readonly DistributedTrustedPreset[] {
	const presets = canonicalTrustedPresets(incoming);
	try {
		const binding = validatedArtifactBinding(artifact);
		const operationDescriptors = binding.trustedPresets;
		const commandDescriptors =
			host.getCommandAuthorityContract()?.trustedPresets;
		if (
			commandDescriptors !== undefined &&
			trustedPresetDescriptorFingerprint(operationDescriptors) !==
				trustedPresetDescriptorFingerprint(commandDescriptors)
		) {
			throw new TypeError(
				'operation and command trusted preset contracts differ'
			);
		}
		matchReplicaTrustedPresetInventory(operationDescriptors, presets);
		if (
			host.getProtocolGeneration() === nextGeneration &&
			trustedPresetInventoryFingerprint(host.getTrustedPresets()) !==
				trustedPresetInventoryFingerprint(presets)
		) {
			throw new TypeError(
				'trusted presets changed within one authoritative cache scope'
			);
		}
		return presets;
	} catch {
		if (host.getProtocolGeneration() !== undefined) {
			purgeProtocolGeneration(host);
		}
		protocolInvalid('extensions.distributed.trustedPresets');
	}
}

export function purgeProtocolGeneration(host: ProtocolHost): void {
	closeAuthorizationGeneration(host);
	host.queryStates.clear();
	host.operationProtocols.clear();
	host.operationGenerations.clear();
	host.recordClocks.clear();
	host.recordKeysByScope.clear();
	host.projectedRecordFences.clear();
	host.membershipFences.clear();
	host.deferredMembershipConfirms.clear();
	host.anonymousRecordClocks.clear();
	host.optimisticReceipts.clear();
	host.diagnosticLayers?.clear();
	host.setDiagnosticLayerSequence(0);
	host.clearIndexMaintenance();
	host.indexPlanRegistrations.clear();
	host.setTrustedPresets(EMPTY_TRUSTED_PRESETS);
	host.setProjectionGeneration(0);
	host.setNextIndexRevision('0');
	host.setProtocolGeneration(undefined);
	host.engine.restore(EMPTY_CACHE_SNAPSHOT);
	host.syncDiagnostics();
	// Watches and retained operations outlive the generation; their plans do not.
	host.restoreIndexPlans();
	for (const key of host.watches.keys()) host.emitState(key, true);
	if (host.diagnostics.enabled) {
		host.diagnosticEvent(
			Object.freeze({
				kind: 'scope',
				action: 'invalidated',
				generation: host.getProtocolGenerationSequence()
			})
		);
	}
}

export function closeAuthorizationGeneration(host: ProtocolHost): void {
	host.bumpProtocolGenerationSequence();
	host.disposeAuthorizationGeneration();
	host.abortAuthorization();
	host.closeActiveTransports();
}
