import {
	parseGraphqlResponseExtensions,
	type DistributedCommandMetadata,
	type DistributedCommandState
} from '../../../protocol.js';
import { matchReplicaTrustedPresetInventory } from '../../commands/presets.js';
import { verifyReplicaCommandReceipt } from '../../commands/receipt.js';
import type { ReplicaPreparedCommand } from '../../commands/types.js';
import type {
	CapturedAuthority,
	ReplicaCommandStatus,
	ReplicaCommandStatusArtifact,
	ReplicaCommandSurfaceContract,
	ReplicaCommandTransportResult
} from '../types.js';
import { isPlainRecord } from '../../../lib/is-plain-record.js';
import {
	isStringSubset,
	projectionExpectationFingerprint,
	projectionObservationFingerprint,
	recordRevisionFingerprint,
	sameStringMultiset
} from './output.js';
export function requireStatusEnvelope<TInput, TOutput>(
	result: ReplicaCommandTransportResult,
	artifact: ReplicaCommandStatusArtifact,
	prepared: ReplicaPreparedCommand<TInput, TOutput>,
	authority: CapturedAuthority,
	contract: ReplicaCommandSurfaceContract
): ReplicaCommandStatus {
	if (
		!Number.isSafeInteger(result.status) ||
		result.status < 200 ||
		result.status >= 300 ||
		(result.errors?.length ?? 0) !== 0
	) {
		throw new Error('command status request did not succeed');
	}
	const state = commandStatusOutput(result.data);
	const distributed = parseGraphqlResponseExtensions(result.extensions)?.distributed;
	if (
		distributed === undefined ||
		distributed.operation !== artifact.operationHash ||
		distributed.protocolVersion !== authority.scope.protocolVersion ||
		distributed.schemaHash !== authority.scope.schemaHash ||
		distributed.authorizationGeneration !==
			authority.scope.authorizationGeneration ||
		distributed.cacheScope !== authority.scope.cacheScope ||
		distributed.snapshot !== undefined ||
		distributed.live !== undefined
	) {
		throw new Error('command status response does not match its generated scope');
	}
	matchReplicaTrustedPresetInventory(
		contract.trustedPresets,
		distributed.trustedPresets
	);
	const metadata = distributed.command;
	if (metadata === undefined) {
		if (state !== 'unknown' && state !== 'expired') {
			throw new Error('command status omitted required causal metadata');
		}
		return Object.freeze({
			commandId: prepared.commandId,
			state
		});
	}
	verifyReplicaCommandReceipt(prepared, metadata);
	if (metadata.state !== state) {
		throw new Error('command status data and causal metadata disagree');
	}
	return Object.freeze({
		commandId: prepared.commandId,
		state,
		metadata
	});
}

export function commandStatusOutput(
	data: Readonly<Record<string, unknown>> | null | undefined
): DistributedCommandState {
	if (
		data === undefined ||
		data === null ||
		!isPlainRecord(data) ||
		Reflect.ownKeys(data).length !== 1 ||
		!Object.prototype.hasOwnProperty.call(data, 'commandStatus')
	) {
		throw new Error('command status data has an invalid root shape');
	}
	const value = data.commandStatus;
	if (
		!isPlainRecord(value) ||
		Reflect.ownKeys(value).length !== 1 ||
		!Object.prototype.hasOwnProperty.call(value, 'state')
	) {
		throw new Error('command status data has an invalid result shape');
	}
	switch (value.state) {
		case 'in_progress':
		case 'succeeded':
		case 'succeeded_pending_projection':
		case 'atomic':
		case 'rejected':
		case 'projection_failed':
		case 'expired':
		case 'unknown':
			return value.state;
		default:
			throw new Error('command status data has an invalid state');
	}
}

export function validateStatusProgression(
	previousState: DistributedCommandState | undefined,
	previous: DistributedCommandMetadata | undefined,
	current: ReplicaCommandStatus
): void {
	if (
		previousState !== undefined &&
		!isStatusTransition(previousState, current.state)
	) {
		throw new Error('command status regressed or changed terminal outcome');
	}
	const next = current.metadata;
	if (next === undefined) {
		if (
			(current.state !== 'unknown' && current.state !== 'expired') ||
			(previous !== undefined && current.state === 'unknown')
		) {
			throw new Error('command status lost causal metadata');
		}
		return;
	}
	if (next.state !== current.state) {
		throw new Error('command status metadata has an inconsistent state');
	}
	if (previous === undefined) return;
	if (
		next.commandId !== previous.commandId ||
		next.causationId !== previous.causationId ||
		next.consistency !== previous.consistency
	) {
		throw new Error('command status changed causal identity');
	}
	if (
		previous.projectionDisposition === 'revalidate' &&
		next.projectionDisposition !== 'revalidate'
	) {
		throw new Error('command status lost projection disposition');
	}
	if (next.projectionDisposition === 'revalidate') return;
	if (
		!(
			previous.state === 'in_progress' &&
			previous.expects.length === 0
		) &&
		!sameStringMultiset(
			previous.expects.map(projectionExpectationFingerprint),
			next.expects.map(projectionExpectationFingerprint)
		)
	) {
		throw new Error('command status changed projection expectations');
	}
	if (
		!isStringSubset(
			previous.observations.map(projectionObservationFingerprint),
			next.observations.map(projectionObservationFingerprint)
		) ||
		!isStringSubset(
			previous.records.map(recordRevisionFingerprint),
			next.records.map(recordRevisionFingerprint)
		)
	) {
		throw new Error('command status lost causal evidence');
	}
}

export function isStatusTransition(
	previous: DistributedCommandState,
	next: DistributedCommandState
): boolean {
	switch (previous) {
		case 'unknown':
			return true;
		case 'in_progress':
			return next !== 'unknown';
		case 'succeeded':
			return next === 'succeeded' || next === 'expired';
		case 'succeeded_pending_projection':
			return (
				next === 'succeeded_pending_projection' ||
				next === 'atomic' ||
				next === 'projection_failed' ||
				next === 'expired'
			);
		case 'atomic':
			return next === 'atomic' || next === 'expired';
		case 'rejected':
			return next === 'rejected' || next === 'expired';
		case 'projection_failed':
			return next === 'projection_failed' || next === 'expired';
		case 'expired':
			return next === 'expired';
	}
}
