import type { DistributedTrustedPreset } from '../../protocol.js';
import { createReplicaCommandId } from '../command-id.js';
import type {
	DeepReadonly,
	PrepareReplicaCommandOptions,
	ReplicaCommandArtifact,
	ReplicaCommandVariables,
	ReplicaPreparedCommand
} from './types.js';
import { validateArtifact } from './validate.js';
import { selectReplicaTrustedPresetInventory } from './presets.js';
import {
	materializeInput,
	resolveDirectProjection
} from './resolve.js';
import { prepareCommandProjection } from '../projection-delta/resolve.js';
import { validateCommandProjectionArtifact } from '../projection-delta/validate.js';
import {
	cloneClientSurface,
	cloneRevalidation,
	cloneTrustedPresetDescriptors
} from './clone.js';
import { validateUuidV7 } from './util.js';

export function prepareReplicaCommand<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	input: TInput,
	options: PrepareReplicaCommandOptions = {}
): ReplicaPreparedCommand<TInput, TOutput> {
	return prepareReplicaCommandInternal(artifact, input, options);
}

/**
 * Finalize a command with values obtained from the replica's current
 * authoritative scope generation.
 *
 * The incoming inventory is scope-wide, so values owned by other commands are
 * ignored. Every descriptor consumed by this artifact must nevertheless have
 * one exact name/codec match before defaults, optimism, or transport state is
 * created.
 *
 * This seam is package-internal and deliberately omitted from the public
 * `@hops-ops/distributed/replica` entry point.
 *
 * @internal
 */
export function prepareReplicaCommandWithTrustedPresets<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	input: TInput,
	authoritativePresets: readonly DistributedTrustedPreset[],
	options: PrepareReplicaCommandOptions = {}
): ReplicaPreparedCommand<TInput, TOutput> {
	return prepareReplicaCommandInternal(
		artifact,
		input,
		options,
		authoritativePresets
	);
}

export function prepareReplicaCommandInternal<TInput, TOutput>(
	artifact: ReplicaCommandArtifact<TInput, TOutput>,
	input: TInput,
	options: PrepareReplicaCommandOptions,
	authoritativePresets?: readonly DistributedTrustedPreset[]
): ReplicaPreparedCommand<TInput, TOutput> {
	const descriptors = validateArtifact(
		artifact,
		authoritativePresets !== undefined
	);
	const trustedPresets =
		authoritativePresets === undefined
			? undefined
			: selectReplicaTrustedPresetInventory(descriptors, authoritativePresets);
	const defaults = artifact.inputDefaults?.defaults ?? [];
	const finalizedInput = materializeInput(
		artifact.input,
		input,
		defaults,
		options.generators
	) as DeepReadonly<TInput>;
	const commandId = validateUuidV7(
		options.commandId ?? createReplicaCommandId(),
		'options.commandId',
		'REPLICA_COMMAND_INPUT_INVALID'
	);
	const variables = Object.freeze({
		commandId,
		...(artifact.input.kind === 'none' ? {} : { input: finalizedInput })
	}) as ReplicaCommandVariables<TInput>;
	const projection =
		artifact.projection === undefined
			? undefined
			: prepareCommandProjection(
					validateCommandProjectionArtifact(artifact.projection),
					finalizedInput,
					trustedPresets?.values ?? []
				);
	const operations = projection?.preview ?? Object.freeze([]);
	const directProjection = resolveDirectProjection(
		artifact.directProjection,
		finalizedInput,
		trustedPresets
	);
	const protocol = Object.freeze({
		...artifact.protocol,
		surface: cloneClientSurface(artifact.protocol.surface),
		trustedPresets: cloneTrustedPresetDescriptors(
			artifact.protocol.trustedPresets
		)
	});
	const revalidation = cloneRevalidation(artifact.revalidation);

	return Object.freeze({
		name: artifact.name,
		commandId,
		consistency: artifact.consistency,
		input: finalizedInput,
		transport: Object.freeze({
			mutationField: artifact.mutationField,
			document: artifact.document,
			operationHash: artifact.operationHash,
			protocol,
			variables
		}),
		optimistic: Object.freeze({
			version: 1 as const,
			operations,
			fallback: 'revalidate' as const
		}),
		...(projection === undefined ? {} : { projection }),
		...(directProjection === undefined ? {} : { directProjection }),
		revalidation
	}) as ReplicaPreparedCommand<TInput, TOutput>;
}

/**
 * Match two command-local inventories as exact name/codec sets.
 *
 * Both sides are reparsed instead of trusting TypeScript annotations. Missing,
 * extra, duplicate, unsupported, or codec-mismatched entries fail closed.
 *
 * @internal
 */
