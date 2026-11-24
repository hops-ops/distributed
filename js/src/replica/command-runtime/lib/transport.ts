import {
	parseGraphqlResponseExtensions,
	type DistributedProtocolEnvelope
} from '../../../protocol.js';
import type { GqlError } from '../../../types.js';
import { verifyReplicaCommandReceipt } from '../../commands/receipt.js';
import type { ReplicaPreparedCommand } from '../../commands/types.js';
import { ReplicaCommandRuntimeError } from '../errors.js';
import type {
	CapturedAuthority,
	ReplicaCommandStatusArtifact,
	ReplicaCommandStatusRequest,
	ReplicaCommandTransport,
	ReplicaCommandTransportRequest,
	ReplicaCommandTransportResult
} from '../types.js';
import {
	cloneSurface,
	waitForCommandOperation
} from './util.js';
export function commandTransportRequest<TInput, TOutput>(
	prepared: ReplicaPreparedCommand<TInput, TOutput>,
	signal: AbortSignal | undefined
): ReplicaCommandTransportRequest {
	const surface = prepared.transport.protocol.surface;
	if (surface === undefined) {
		throw new ReplicaCommandRuntimeError(
			'REPLICA_COMMAND_PROTOCOL_INVALID',
			{ commandId: prepared.commandId }
		);
	}
	return Object.freeze({
		operation: 'mutation',
		commandName: prepared.name,
		commandId: prepared.commandId,
		mutationField: prepared.transport.mutationField,
		document: prepared.transport.document,
		operationHash: prepared.transport.operationHash,
		variables: prepared.transport.variables as Readonly<Record<string, unknown>>,
		extensions: Object.freeze({
			distributed: Object.freeze({
				client: Object.freeze({
					surface: cloneSurface(surface),
					schemaHash: prepared.transport.protocol.schemaHash
				})
			})
		}),
		...(signal === undefined ? {} : { signal })
	});
}

export function commandStatusRequest(
	artifact: ReplicaCommandStatusArtifact,
	commandId: string,
	signal: AbortSignal | undefined
): ReplicaCommandStatusRequest {
	return Object.freeze({
		operation: 'status',
		commandId,
		name: artifact.name,
		document: artifact.document,
		operationHash: artifact.operationHash,
		variables: Object.freeze({ commandId }),
		extensions: Object.freeze({
			distributed: Object.freeze({
				client: Object.freeze({
					surface: cloneSurface(artifact.protocol.surface),
					schemaHash: artifact.protocol.schemaHash
				})
			})
		}),
		...(signal === undefined ? {} : { signal })
	});
}

export async function dispatchPrepared(
	transport: ReplicaCommandTransport,
	request: ReplicaCommandTransportRequest,
	retries: number,
	onAttempt: () => void
): Promise<ReplicaCommandTransportResult> {
	let error: unknown;
	for (let attempt = 0; attempt <= retries; attempt += 1) {
		if (request.signal?.aborted) {
			throw request.signal.reason ?? new Error('command request aborted');
		}
		try {
			onAttempt();
			return await waitForCommandOperation(
				transport.dispatch(request),
				request.signal
			);
		} catch (candidate) {
			error = candidate;
			if (request.signal?.aborted) throw candidate;
		}
	}
	throw error;
}

export function requireCommandEnvelope<TInput, TOutput>(
	result: ReplicaCommandTransportResult,
	prepared: ReplicaPreparedCommand<TInput, TOutput>,
	authority: CapturedAuthority
): DistributedProtocolEnvelope {
	const distributed = parseGraphqlResponseExtensions(result.extensions)?.distributed;
	if (
		distributed === undefined ||
		distributed.command === undefined ||
		distributed.operation !== prepared.transport.operationHash ||
		distributed.protocolVersion !== authority.scope.protocolVersion ||
		distributed.schemaHash !== authority.scope.schemaHash ||
		distributed.authorizationGeneration !==
			authority.scope.authorizationGeneration ||
		distributed.cacheScope !== authority.scope.cacheScope
	) {
		throw new Error('command response does not match its generated scope');
	}
	verifyReplicaCommandReceipt(prepared, distributed.command);
	return distributed;
}

/**
 * Domain rejection happens before a command receipt exists, so GraphQL cannot
 * attach `distributed.command`. It still has to prove the exact generated
 * operation and authoritative cache scope before the runtime may classify the
 * response as a normal rejection instead of a protocol failure.
 */
export function requireCommandRejectionEnvelope<TInput, TOutput>(
	result: ReplicaCommandTransportResult,
	prepared: ReplicaPreparedCommand<TInput, TOutput>,
	authority: CapturedAuthority
): DistributedProtocolEnvelope {
	const distributed = parseGraphqlResponseExtensions(result.extensions)?.distributed;
	if (
		!Number.isSafeInteger(result.status) ||
		result.status < 200 ||
		result.status >= 300 ||
		result.data !== null ||
		graphqlCommandRejection(result) === undefined ||
		distributed === undefined ||
		distributed.command !== undefined ||
		distributed.operation !== prepared.transport.operationHash ||
		distributed.protocolVersion !== authority.scope.protocolVersion ||
		distributed.schemaHash !== authority.scope.schemaHash ||
		distributed.authorizationGeneration !==
			authority.scope.authorizationGeneration ||
		distributed.cacheScope !== authority.scope.cacheScope
	) {
		throw new Error('command rejection does not match its generated scope');
	}
	return distributed;
}

export function graphqlCommandRejection(
	result: ReplicaCommandTransportResult
): GqlError | undefined {
	const errors = result.errors;
	if (
		parseGraphqlResponseExtensions(result.extensions)?.distributed?.command !==
			undefined ||
		errors === undefined ||
		errors.length === 0 ||
		!errors.every((error) => error.extensions?.code === 'REJECTED')
	) {
		return undefined;
	}
	return errors[0];
}
