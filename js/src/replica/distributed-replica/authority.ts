import type { GraphqlVariables } from '../../types.js';
import type {
	DistributedProtocolEnvelope,
	DistributedTrustedPreset
} from '../../protocol.js';
import type {
	ReplicaCommandAuthoritySnapshot,
	ReplicaCommandSurfaceContract
} from '../command-runtime/types.js';
import { matchReplicaTrustedPresetInventory } from '../commands/presets.js';
import { validateReplicaOperationBinding } from '../operation-binding.js';
import type {
	ReplicaAuthoritativeScope,
	ReplicaOperationArtifact,
	ReplicaWriteSource
} from '../types.js';
import { protocolInvalid } from './clocks.js';
import { EMPTY_TRUSTED_PRESETS } from './constants.js';
import {
	canonicalTrustedPresets,
	trustedPresetDescriptorFingerprint,
	trustedPresetInventoryFingerprint,
	validatedCommandAuthorityContract
} from './helpers.js';
import type {
	ProtocolGeneration,
	RegisteredCommandAuthorityContract,
	ReplicaArtifactBinding
} from './types.js';

/** A scope and its trusted presets, staged by a result and not yet adopted. */
type StagedAuthority = {
	readonly scope: ProtocolGeneration;
	readonly trustedPresets: readonly DistributedTrustedPreset[];
};

/**
 * Whose data the replica holds and which generated surface it serves: the
 * authoritative scope the server established, its trusted presets, the
 * artifact and command contracts bound so far, and the local generation
 * counter and abort signal that fence work begun under an earlier scope.
 *
 * `purge` empties the replica. It runs when a result or artifact contradicts
 * the scope in force, before the contradiction is reported.
 */
export class ReplicaAuthority {
	readonly #onDispose: (() => void) | undefined;
	readonly #reportObserverErrors: (errors: unknown[]) => void;
	readonly #purge: () => void;
	#commandContract: RegisteredCommandAuthorityContract | undefined;
	#binding: ReplicaArtifactBinding | undefined;
	#scope: ProtocolGeneration | undefined;
	#trustedPresets: readonly DistributedTrustedPreset[] = EMPTY_TRUSTED_PRESETS;
	#sequence = 0;
	#abort = new AbortController();

	constructor(options: {
		onDispose: (() => void) | undefined;
		reportObserverErrors: (errors: unknown[]) => void;
		purge: () => void;
	}) {
		this.#onDispose = options.onDispose;
		this.#reportObserverErrors = options.reportObserverErrors;
		this.#purge = options.purge;
	}

	get scope(): ProtocolGeneration | undefined {
		return this.#scope;
	}

	/** Counts authorization generations of this replica; never reset. */
	get sequence(): number {
		return this.#sequence;
	}

	get trustedPresets(): readonly DistributedTrustedPreset[] {
		return this.#trustedPresets;
	}

	get protocolHash(): string | undefined {
		return this.#commandContract?.protocolHash;
	}

	publicScope(): ReplicaAuthoritativeScope | undefined {
		const scope = this.#scope;
		return scope === undefined
			? undefined
			: Object.freeze({
					protocolVersion: scope.protocolVersion,
					schemaHash: scope.schemaHash,
					authorizationGeneration: scope.authorizationGeneration,
					cacheScope: scope.cacheScope
				});
	}

	commandSnapshot(): ReplicaCommandAuthoritySnapshot {
		return Object.freeze({
			generation: this.#sequence,
			scope: this.publicScope(),
			trustedPresets: this.#trustedPresets,
			signal: this.#abort.signal
		});
	}

	registerCommands(contract: ReplicaCommandSurfaceContract): void {
		const next = validatedCommandAuthorityContract(contract);
		const current = this.#commandContract;
		if (current !== undefined && current.fingerprint !== next.fingerprint) {
			throw new TypeError(
				'command inventory does not match the active replica client surface'
			);
		}

		const binding = this.#binding;
		if (binding === undefined) {
			this.#binding = Object.freeze({
				version: 1,
				schemaHash: next.schemaHash,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
		} else if (
			binding.version !== 1 ||
			binding.schemaHash !== next.schemaHash ||
			(
				binding.surfaceIdentity !== undefined &&
				binding.surfaceIdentity !== next.surfaceIdentity
			) ||
			(
				binding.trustedPresets !== undefined &&
				trustedPresetDescriptorFingerprint(binding.trustedPresets) !==
					trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			throw new TypeError(
				'command inventory does not match the active replica client surface'
			);
		} else if (
			binding.surfaceIdentity === undefined ||
			binding.trustedPresets === undefined
		) {
			this.#binding = Object.freeze({
				...binding,
				...(binding.surfaceIdentity === undefined
					? { surfaceIdentity: next.surfaceIdentity }
					: {}),
				...(binding.trustedPresets === undefined
					? { trustedPresets: next.trustedPresets }
					: {})
			});
		}

		if (this.#scope !== undefined) {
			try {
				matchReplicaTrustedPresetInventory(
					next.trustedPresets,
					this.#trustedPresets
				);
			} catch (error) {
				this.#purge();
				throw error;
			}
		}
		this.#commandContract = next;
	}

	bindArtifact<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void {
		const next = validateReplicaOperationBinding(artifact);
		if (this.#scope !== undefined) {
			try {
				matchReplicaTrustedPresetInventory(
					next.trustedPresets,
					this.#trustedPresets
				);
			} catch (error) {
				this.#purge();
				throw error;
			}
		}
		const current = this.#binding;
		if (current === undefined) {
			this.#binding = Object.freeze({
				version: next.version,
				schemaHash: next.schemaHash,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
			return;
		}
		if (
			(
				current.surfaceIdentity === undefined ||
				current.trustedPresets === undefined
			) &&
			current.version === next.version &&
			current.schemaHash === next.schemaHash &&
			(
				current.surfaceIdentity === undefined ||
				current.surfaceIdentity === next.surfaceIdentity
			) &&
			(
				current.trustedPresets === undefined ||
				trustedPresetDescriptorFingerprint(current.trustedPresets) ===
					trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			this.#binding = Object.freeze({
				...current,
				surfaceIdentity: next.surfaceIdentity,
				trustedPresets: next.trustedPresets
			});
			return;
		}
		if (
			current.version !== next.version ||
			current.schemaHash !== next.schemaHash ||
			(
				current.surfaceIdentity !== undefined &&
				current.surfaceIdentity !== next.surfaceIdentity
			) ||
			(
				current.trustedPresets !== undefined &&
				trustedPresetDescriptorFingerprint(
					current.trustedPresets
				) !== trustedPresetDescriptorFingerprint(next.trustedPresets)
			)
		) {
			throw new TypeError(
				'replica artifact schema does not match the active replica binding'
			);
		}
	}

	/**
	 * Check a result's envelope against its artifact, then stage the scope and
	 * trusted presets it carries. A result from another scope purges the
	 * replica first; `commit` adopts the staged values once it is admitted.
	 */
	stage<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		envelope: DistributedProtocolEnvelope,
		source: ReplicaWriteSource
	): StagedAuthority {
		const binding = artifact.protocol;
		if (binding === undefined) {
			protocolInvalid('extensions.distributed');
		}
		if (binding.version !== 1) {
			throw new TypeError('replica artifact protocol version is unsupported');
		}
		if (binding.schemaHash !== envelope.schemaHash) {
			this.#purge();
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
		const scope = this.#stageScope(envelope);
		return {
			scope,
			trustedPresets: this.#stageTrustedPresets(
				envelope.trustedPresets,
				scope,
				artifact
			)
		};
	}

	commit(staged: StagedAuthority): void {
		this.#trustedPresets = staged.trustedPresets;
		this.#scope = staged.scope;
	}

	/**
	 * End the authorization generation: fence everything begun under it and
	 * abort its command dispatches. The owner closes the transports next.
	 */
	close(): void {
		this.#sequence += 1;
		try {
			this.#onDispose?.();
		} catch (error) {
			this.#reportObserverErrors([error]);
		}
		this.#abort.abort();
		this.#abort = new AbortController();
	}

	/** Forget the scope; the bound artifact and command contracts stay. */
	reset(): void {
		this.#trustedPresets = EMPTY_TRUSTED_PRESETS;
		this.#scope = undefined;
	}

	/** Why a dehydrated state of `scope` cannot be hydrated here, if so. */
	hydrationRejection(
		scope: ProtocolGeneration,
		trustedPresets: readonly DistributedTrustedPreset[],
		mode: 'merge' | 'reauthorize'
	): 'invalid' | 'artifact-mismatch' | 'active-scope-mismatch' | undefined {
		const binding = this.#binding;
		if (binding !== undefined && binding.schemaHash !== scope.schemaHash) {
			return 'artifact-mismatch';
		}
		const current = this.#scope;
		if (
			current !== undefined &&
			(
				current.protocolVersion !== scope.protocolVersion ||
				current.schemaHash !== scope.schemaHash ||
				current.authorizationGeneration !== scope.authorizationGeneration ||
				current.cacheScope !== scope.cacheScope
			)
		) {
			return 'active-scope-mismatch';
		}
		if (mode === 'reauthorize' && current === undefined) {
			return 'active-scope-mismatch';
		}
		try {
			const expected =
				binding?.trustedPresets !== undefined
					? binding.trustedPresets
					: this.#commandContract?.trustedPresets;
			if (expected !== undefined) {
				matchReplicaTrustedPresetInventory(expected, trustedPresets);
			}
			if (
				current !== undefined &&
				trustedPresetInventoryFingerprint(this.#trustedPresets) !==
					trustedPresetInventoryFingerprint(trustedPresets)
			) {
				throw new TypeError(
					'trusted presets changed within one authoritative cache scope'
				);
			}
		} catch {
			return 'invalid';
		}
		return undefined;
	}

	/** Adopt the scope of an admitted dehydrated state. */
	hydrate(
		scope: ProtocolGeneration,
		trustedPresets: readonly DistributedTrustedPreset[]
	): void {
		const binding = this.#binding;
		this.#trustedPresets = trustedPresets;
		this.#scope = scope;
		this.#binding = Object.freeze({
			version: 1,
			schemaHash: scope.schemaHash,
			...(binding?.surfaceIdentity !== undefined
				? { surfaceIdentity: binding.surfaceIdentity }
				: {}),
			...(binding?.trustedPresets !== undefined
				? { trustedPresets: binding.trustedPresets }
				: {})
		});
	}

	#stageScope(envelope: DistributedProtocolEnvelope): ProtocolGeneration {
		const next: ProtocolGeneration = {
			protocolVersion: 1,
			cacheScope: envelope.cacheScope,
			schemaHash: envelope.schemaHash,
			authorizationGeneration: envelope.authorizationGeneration
		};
		const current = this.#scope;
		if (current === undefined) return next;
		if (
			current.cacheScope === next.cacheScope &&
			current.schemaHash === next.schemaHash &&
			current.authorizationGeneration === next.authorizationGeneration
		) {
			return current;
		}
		this.#purge();
		return next;
	}

	#stageTrustedPresets<TData, TVariables extends GraphqlVariables>(
		incoming: readonly DistributedTrustedPreset[],
		nextScope: ProtocolGeneration,
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): readonly DistributedTrustedPreset[] {
		const presets = canonicalTrustedPresets(incoming);
		try {
			const operationDescriptors =
				validateReplicaOperationBinding(artifact).trustedPresets;
			const commandDescriptors = this.#commandContract?.trustedPresets;
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
				this.#scope === nextScope &&
				trustedPresetInventoryFingerprint(this.#trustedPresets) !==
					trustedPresetInventoryFingerprint(presets)
			) {
				throw new TypeError(
					'trusted presets changed within one authoritative cache scope'
				);
			}
			return presets;
		} catch {
			if (this.#scope !== undefined) this.#purge();
			protocolInvalid('extensions.distributed.trustedPresets');
		}
	}
}
