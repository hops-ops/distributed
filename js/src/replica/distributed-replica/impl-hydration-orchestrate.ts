import {
	createCacheEngine,
	type CacheEngine,
	type CacheEngineSnapshot
} from '../../internal/cache-engine.js';
import type { GraphqlVariables } from '../../types.js';
import {
	compareDistributedDecimal,
	type DistributedDecimalString,
	type DistributedOpaqueString,
	type DistributedTrustedPreset
} from '../../protocol.js';
import { matchReplicaTrustedPresetInventory } from '../commands.js';
import type { ReplicaDiagnosticEventInput } from '../diagnostics.js';
import { replicaIndexKey, resolveArguments } from '../identity.js';
import type { ReplicaIndexPlanRegistration } from '../index-maintenance.js';
import {
	runtimeRoot,
	type RuntimeObjectBranch,
	type RuntimeObjectSelection,
	type RuntimeRootSelection
} from '../selection.js';
import type {
	ReplicaAuthoritativeScope,
	ReplicaDehydratedState
} from '../types.js';
import { freezeRecordClock } from './clocks.js';
import { trustedPresetInventoryFingerprint } from './helpers.js';
import {
	hydrationMetadataConsistent,
	parseAuthoritativeScope,
	parseReplicaHydration,
	serializeOperationProtocolGroup
} from './hydration.js';
import type {
	AnonymousRecordProtocolClock,
	OperationProtocolGroup,
	OptimisticReceiptState,
	ProtocolGeneration,
	QueryState,
	RecordProtocolClock,
	RegisteredCommandAuthorityContract,
	RenderedOperation,
	ReplicaArtifactBinding,
	ReplicaDehydratedPayloadV1
} from './types.js';
import type { ReplicaDiagnosticLayerInput } from '../diagnostics.js';

/**
 * Host for dehydrate / hydrate orchestration.
 */
export type HydrationHost = {
	readonly engine: CacheEngine;
	readonly operationProtocols: Map<string, OperationProtocolGroup>;
	readonly operationGenerations: Map<string, number>;
	readonly recordClocks: Map<string, RecordProtocolClock>;
	readonly recordKeysByScope: Map<DistributedOpaqueString, string>;
	readonly anonymousRecordClocks: Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>;
	readonly optimisticReceipts: Map<string, OptimisticReceiptState>;
	readonly diagnosticLayers: Map<string, ReplicaDiagnosticLayerInput> | undefined;
	readonly renderedOperations: Map<string, RenderedOperation>;
	readonly readOperationKeys: Set<string>;
	readonly watchRenderCounts: Map<string, number>;
	readonly indexPlanRegistrations: Map<string, ReplicaIndexPlanRegistration>;
	readonly queryStates: Map<string, QueryState>;
	readonly diagnostics: { readonly enabled: boolean };
	getProtocolGeneration(): ProtocolGeneration | undefined;
	setProtocolGeneration(value: ProtocolGeneration | undefined): void;
	getProtocolGenerationSequence(): number;
	getTrustedPresets(): readonly DistributedTrustedPreset[];
	setTrustedPresets(value: readonly DistributedTrustedPreset[]): void;
	getNextIndexRevision(): string;
	setNextIndexRevision(value: string): void;
	getArtifactBinding(): ReplicaArtifactBinding | undefined;
	setArtifactBinding(value: ReplicaArtifactBinding | undefined): void;
	getCommandAuthorityContract():
		| RegisteredCommandAuthorityContract
		| undefined;
	setDiagnosticLayerSequence(value: number): void;
	operationGeneration(key: string): number;
	closeActiveTransports(): void;
	closeAuthorizationGeneration(): void;
	resumeLiveWatches(): void;
	refreshWatches(): void;
	syncDiagnostics(): void;
	diagnosticEvent(event: ReplicaDiagnosticEventInput): void;
	refreshIndexMaintenance(): void;
	finishDehydration(): void;
};

export function reachableConfirmedState(host: HydrationHost): {
	readonly cache: CacheEngineSnapshot;
	readonly recordKeys: ReadonlySet<string>;
	readonly clockRecordKeys: ReadonlySet<string>;
	readonly models: ReadonlySet<string>;
} {
	const snapshot = host.engine.extract();
	const indexes = new Map(snapshot.indexes.map((index) => [index.key, index]));
	const records = new Map(snapshot.records.map((record) => [record.key, record]));
	const indexKeys = new Set<string>();
	const recordKeys = new Set<string>();
	const clockRecordKeys = new Set<string>();
	const recordFields = new Map<string, Set<string>>();
	const models = new Set<string>();

	const rememberSelection = (selection: RuntimeObjectSelection): void => {
		if (selection.storage.kind === 'normalized') {
			models.add(selection.storage.model);
		}
		for (const member of selection.members) {
			if (member.kind === 'branch') rememberSelection(member.selection);
		}
	};
	const rememberRecord = (
		recordKey: string,
		selection: RuntimeObjectSelection
	): void => {
		recordKeys.add(recordKey);
		clockRecordKeys.add(recordKey);
		let selected = recordFields.get(recordKey);
		if (selected === undefined) {
			selected = new Set();
			recordFields.set(recordKey, selected);
		}
		for (const member of selection.members) {
			if (member.kind === 'scalar') selected.add(member.field);
		}
	};
	const visitObject = (
		selection: RuntimeObjectSelection,
		recordKey: string,
		variables: GraphqlVariables
	): void => {
		rememberRecord(recordKey, selection);
		for (const member of selection.members) {
			if (member.kind !== 'branch') continue;
			visitBranch(member, recordKey, variables);
		}
	};
	const visitBranch = (
		selection: RuntimeRootSelection | RuntimeObjectBranch,
		parent: string | undefined,
		variables: GraphqlVariables
	): void => {
		const key = replicaIndexKey({
			...(parent === undefined ? {} : { parent }),
			field: selection.field,
			arguments: resolveArguments(
				selection.arguments,
				variables,
				selection.coverage
			)
		});
		const index = indexes.get(key);
		if (index === undefined) return;
		indexKeys.add(key);
		for (const recordKey of index.records) {
			visitObject(selection.selection, recordKey, variables);
		}
	};

	for (const [key, rendered] of host.renderedOperations) {
		for (const rootArtifact of rendered.artifact.roots) {
			const root = runtimeRoot(rootArtifact);
			rememberSelection(root.selection);
			visitBranch(root, undefined, rendered.variables);
		}
		const group = host.operationProtocols.get(key);
		for (const state of [group?.query, group?.live]) {
			for (const recordKey of state?.pathRecords.values() ?? []) {
				clockRecordKeys.add(recordKey);
			}
		}
	}

	return Object.freeze({
		cache: Object.freeze({
			version: 1 as const,
			records: Object.freeze(
				[...recordKeys]
					.sort()
					.flatMap((key) => {
						const record = records.get(key);
						if (record === undefined) return [];
						const selected = recordFields.get(key) ?? new Set();
						return [
							Object.freeze({
								...record,
								fields: Object.freeze(
									Object.fromEntries(
										Object.entries(record.fields).filter(([field]) =>
											selected.has(field)
										)
									)
								),
								links: Object.freeze({})
							})
						];
					})
			),
			indexes: Object.freeze(
				[...indexKeys]
					.sort()
					.flatMap((key) => {
						const index = indexes.get(key);
						return index === undefined ? [] : [index];
					})
			)
		}),
		recordKeys,
		clockRecordKeys,
		models
	});
}

export function finishDehydration(host: HydrationHost): void {
	let plansChanged = false;
	for (const key of host.readOperationKeys) {
		if (!host.watchRenderCounts.has(key)) {
			host.renderedOperations.delete(key);
			const registration = host.indexPlanRegistrations.get(key);
			if (registration !== undefined) {
				host.indexPlanRegistrations.delete(key);
				registration.dispose();
				plansChanged = true;
			}
		}
	}
	host.readOperationKeys.clear();
	if (plansChanged) host.refreshIndexMaintenance();
}

export function dehydrateReplica(host: HydrationHost): ReplicaDehydratedState {
	const scope = host.getProtocolGeneration();
	if (scope === undefined) {
		throw new Error(
			'cannot dehydrate replica before the server establishes an authoritative scope'
		);
	}
	const reachable = reachableConfirmedState(host);
	const reachableIndexKeys = new Set(
		reachable.cache.indexes.map((index) => index.key)
	);
	const operationKeys = new Set(host.renderedOperations.keys());
	for (const [key, group] of host.operationProtocols) {
		const governsReachableState = [group.query, group.live].some(
			(state) =>
				state !== undefined &&
				(
					[...state.indexKeys].some((indexKey) =>
						reachableIndexKeys.has(indexKey)
					) ||
					[...state.pathRecords.values()].some((recordKey) =>
						reachable.clockRecordKeys.has(recordKey)
					)
				)
		);
		if (governsReachableState) operationKeys.add(key);
	}
	const operations = [...operationKeys]
		.sort()
		.flatMap((key) => {
			const group = host.operationProtocols.get(key);
			if (group === undefined) return [];
			return [
				serializeOperationProtocolGroup(
					key,
					group,
					host.operationGeneration(key)
				)
			];
		});
	const recordClocks = [...host.recordClocks]
		.filter(([key]) => reachable.clockRecordKeys.has(key))
		.sort(([left], [right]) => left.localeCompare(right))
		.map(([key, clock]) =>
			Object.freeze([key, freezeRecordClock(clock)] as const)
		);
	const anonymousRecordClocks = [...host.anonymousRecordClocks]
		.filter(([, value]) => reachable.models.has(value.model))
		.sort(([left], [right]) => left.localeCompare(right))
		.map(([scopeToken, value]) =>
			Object.freeze([
				scopeToken,
				Object.freeze({
					model: value.model,
					clock: freezeRecordClock(value.clock)
				})
			] as const)
		);
	const payload: ReplicaDehydratedPayloadV1 = Object.freeze({
		cache: reachable.cache,
		operations: Object.freeze(operations),
		recordClocks: Object.freeze(recordClocks),
		anonymousRecordClocks: Object.freeze(anonymousRecordClocks),
		trustedPresets: host.getTrustedPresets(),
		nextIndexRevision: host.getNextIndexRevision()
	});
	const state = Object.freeze({
		version: 1 as const,
		scope: Object.freeze({
			protocolVersion: 1 as const,
			schemaHash: scope.schemaHash,
			authorizationGeneration: scope.authorizationGeneration,
			cacheScope: scope.cacheScope
		}),
		payload
	});
	host.finishDehydration();
	return state;
}

export function hydrateReplica(
	host: HydrationHost,
	state: ReplicaDehydratedState,
	authoritativeScope: ReplicaAuthoritativeScope,
	mode: 'merge' | 'reauthorize' = 'merge'
): boolean {
	const rejected = (
		reason:
			| 'invalid'
			| 'scope-mismatch'
			| 'artifact-mismatch'
			| 'active-scope-mismatch'
			| 'metadata-mismatch'
	): false => {
		if (host.diagnostics.enabled) {
			host.diagnosticEvent(
				Object.freeze({ kind: 'hydration', action: 'rejected', reason })
			);
		}
		return false;
	};
	const parsed = parseReplicaHydration(state);
	const expectedScope = parseAuthoritativeScope(authoritativeScope);
	if (parsed === undefined || expectedScope === undefined) {
		return rejected('invalid');
	}
	if (
		parsed.scope.protocolVersion !== expectedScope.protocolVersion ||
		parsed.scope.schemaHash !== expectedScope.schemaHash ||
		parsed.scope.authorizationGeneration !==
			expectedScope.authorizationGeneration ||
		parsed.scope.cacheScope !== expectedScope.cacheScope
	) {
		return rejected('scope-mismatch');
	}
	const binding = host.getArtifactBinding();
	if (
		binding !== undefined &&
		binding.schemaHash !== parsed.scope.schemaHash
	) {
		return rejected('artifact-mismatch');
	}
	const current = host.getProtocolGeneration();
	if (
		current !== undefined &&
		(
			current.protocolVersion !== parsed.scope.protocolVersion ||
			current.schemaHash !== parsed.scope.schemaHash ||
			current.authorizationGeneration !==
				parsed.scope.authorizationGeneration ||
			current.cacheScope !== parsed.scope.cacheScope
		)
	) {
		return rejected('active-scope-mismatch');
	}
	if (mode === 'reauthorize' && current === undefined) {
		return rejected('active-scope-mismatch');
	}
	const preserveLocalCommandState = current !== undefined;

	// Validate the private engine payload before closing transports or changing
	// any live state. `restore` parses fully before its own transaction.
	try {
		createCacheEngine().restore(parsed.cache);
		const expectedTrustedPresets =
			binding?.trustedPresets !== undefined
				? binding.trustedPresets
				: host.getCommandAuthorityContract()?.trustedPresets;
		if (expectedTrustedPresets !== undefined) {
			matchReplicaTrustedPresetInventory(
				expectedTrustedPresets,
				parsed.trustedPresets
			);
		}
		if (
			preserveLocalCommandState &&
			trustedPresetInventoryFingerprint(host.getTrustedPresets()) !==
				trustedPresetInventoryFingerprint(parsed.trustedPresets)
		) {
			throw new TypeError(
				'trusted presets changed within one authoritative cache scope'
			);
		}
	} catch {
		return rejected('invalid');
	}
	if (!hydrationMetadataConsistent(parsed)) {
		return rejected('metadata-mismatch');
	}

	if (mode === 'reauthorize') {
		// The independent scope authorizes continued use of the current replica.
		// The accompanying snapshot may predate an intervening command: do not
		// merge its records, clocks, indexes, or freshness evidence into that state.
		host.closeActiveTransports();
		host.queryStates.clear();
		host.resumeLiveWatches();
		host.refreshWatches();
		host.syncDiagnostics();
		return true;
	}

	if (preserveLocalCommandState) {
		// Warm same-scope re-hydrate (soft nav / second SSR seed): keep confirmed
		// records and indexes the route seed omitted. Seed keys upsert; purge only
		// happens on auth/scope change (closeAuthorizationGeneration path).
		host.closeActiveTransports();
		host.queryStates.clear();
		for (const [key, group] of parsed.operationProtocols) {
			host.operationProtocols.set(key, group);
		}
		for (const [key, generation] of parsed.operationGenerations) {
			host.operationGenerations.set(key, generation);
		}
		for (const [key, clock] of parsed.recordClocks) {
			host.recordClocks.set(key, clock);
		}
		for (const [scopeToken, key] of parsed.recordKeysByScope) {
			host.recordKeysByScope.set(scopeToken, key);
		}
		for (const [scopeToken, clock] of parsed.anonymousRecordClocks) {
			host.anonymousRecordClocks.set(scopeToken, clock);
		}
		host.setTrustedPresets(parsed.trustedPresets);
		if (
			compareDistributedDecimal(
				parsed.nextIndexRevision as DistributedDecimalString,
				host.getNextIndexRevision() as DistributedDecimalString
			) > 0
		) {
			host.setNextIndexRevision(parsed.nextIndexRevision);
		}
		host.setProtocolGeneration(parsed.scope);
		host.setArtifactBinding(
			Object.freeze({
				version: 1,
				schemaHash: parsed.scope.schemaHash,
				...(binding?.surfaceIdentity !== undefined
					? { surfaceIdentity: binding.surfaceIdentity }
					: {}),
				...(binding?.trustedPresets !== undefined
					? { trustedPresets: binding.trustedPresets }
					: {})
			})
		);
		host.engine.mergeConfirmed(parsed.cache);
	} else {
		host.closeAuthorizationGeneration();
		host.queryStates.clear();
		host.operationProtocols.clear();
		for (const [key, group] of parsed.operationProtocols) {
			host.operationProtocols.set(key, group);
		}
		host.operationGenerations.clear();
		for (const [key, generation] of parsed.operationGenerations) {
			host.operationGenerations.set(key, generation);
		}
		host.recordClocks.clear();
		for (const [key, clock] of parsed.recordClocks) {
			host.recordClocks.set(key, clock);
		}
		host.recordKeysByScope.clear();
		for (const [scopeToken, key] of parsed.recordKeysByScope) {
			host.recordKeysByScope.set(scopeToken, key);
		}
		host.anonymousRecordClocks.clear();
		for (const [scopeToken, clock] of parsed.anonymousRecordClocks) {
			host.anonymousRecordClocks.set(scopeToken, clock);
		}
		host.optimisticReceipts.clear();
		host.diagnosticLayers?.clear();
		host.setDiagnosticLayerSequence(0);
		host.setTrustedPresets(parsed.trustedPresets);
		host.setNextIndexRevision(parsed.nextIndexRevision);
		host.setProtocolGeneration(parsed.scope);
		host.setArtifactBinding(
			Object.freeze({
				version: 1,
				schemaHash: parsed.scope.schemaHash,
				...(binding?.surfaceIdentity !== undefined
					? { surfaceIdentity: binding.surfaceIdentity }
					: {}),
				...(binding?.trustedPresets !== undefined
					? { trustedPresets: binding.trustedPresets }
					: {})
			})
		);
		host.engine.restore(parsed.cache);
	}
	host.resumeLiveWatches();
	host.syncDiagnostics();
	if (host.diagnostics.enabled) {
		host.diagnosticEvent(
			Object.freeze({
				kind: 'hydration',
				action: 'accepted',
				reason: preserveLocalCommandState ? 'same-scope-merge' : 'accepted'
			})
		);
		host.diagnosticEvent(
			Object.freeze({
				kind: 'scope',
				action: 'established',
				generation: host.getProtocolGenerationSequence(),
				schemaHash: parsed.scope.schemaHash
			})
		);
	}
	return true;
}
