import {
	type DistributedCommandMetadata
} from '../../../protocol.js';
import type { ReplicaPreparedCommand } from '../../commands/types.js';
import type {
	ReplicaBaseWriter,
	ReplicaModelArtifact,
	ReplicaValue
} from '../../types.js';
import {
	INITIAL_STATUS_POLL_MS,
	MAX_STATUS_POLL_MS
} from '../constants.js';
import { ReplicaCommandRuntimeError } from '../errors.js';
import { replicaCommandDirectProjection } from '../symbols.js';
import type {
	CapturedAuthority,
	CommandStatusTracker,
	PendingProjection,
	ReplicaCommandAuthorityHost,
	ReplicaCommandProjectedOutcome,
	ReplicaCommandStatus
} from '../types.js';
import { isPlainRecord } from '../../../lib/is-plain-record.js';
import {
	cloneOutputJson
} from './output.js';
export function confirmDirectProjection<TInput, TOutput>(
	replica: ReplicaCommandAuthorityHost,
	prepared: ReplicaPreparedCommand<TInput, TOutput>,
	output: TOutput,
	metadata: DistributedCommandMetadata
): void {
	const direct = prepared.directProjection;
	if (direct === undefined || !isPlainRecord(output)) {
		throw new ReplicaCommandRuntimeError(
			'REPLICA_COMMAND_PROTOCOL_INVALID',
			{ commandId: prepared.commandId }
		);
	}
	const identity: ReplicaValue[] = [];
	for (const field of direct.identityFields) {
		const value = output[field];
		if (value === undefined || value === null) {
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_PROTOCOL_INVALID',
				{ commandId: prepared.commandId }
			);
		}
		identity.push(value as ReplicaValue);
	}
	const evidence = metadata.records.filter(
		(record) =>
			record.model === direct.model &&
			!record.tombstone &&
			(
				record.path === undefined ||
				(record.path.length === 1 &&
					record.path[0] === prepared.transport.mutationField)
			)
	);
	if (evidence.length !== 1) {
		throw new ReplicaCommandRuntimeError(
			'REPLICA_COMMAND_PROTOCOL_INVALID',
			{ commandId: prepared.commandId }
		);
	}
	const record = evidence[0]!;
	const model: ReplicaModelArtifact = Object.freeze({
		id: direct.model,
		identityFields: direct.identityFields
	});
	const outputFields = cloneOutputJson(
		output,
		'projected.output',
		new Set(),
		0
	) as Readonly<Record<string, ReplicaValue>>;
	const fields = Object.freeze({
		...outputFields,
		// Query normalization always supplies the generated model identity. Atomic
		// command rows must be canonicalized the same way or a new record remains
		// incomplete until a later query/reload contributes `__typename`.
		__typename: direct.model
	}) as Readonly<Record<string, ReplicaValue>>;
	const confirmProtocolProjection = replica[replicaCommandDirectProjection];
	if (confirmProtocolProjection !== undefined) {
		confirmProtocolProjection.call(replica, prepared.commandId, {
			model,
			identity: Object.freeze(identity),
			evidence: record,
			fields
		});
		return;
	}
	replica.confirmOptimisticLayer(prepared.commandId, (writer: ReplicaBaseWriter) =>
		writer.writeRecord(model, Object.freeze(identity), record.revision, {
			incarnation: record.incarnation,
			fields
		})
	);
}

export function pendingProjection(
	authority: CapturedAuthority,
	metadata: DistributedCommandMetadata,
	prepared: ReplicaPreparedCommand<unknown, unknown>,
	tracker: CommandStatusTracker
): PendingProjection {
	let resolve!: (value: ReplicaCommandProjectedOutcome<unknown>) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<ReplicaCommandProjectedOutcome<unknown>>(
		(resolvePromise, rejectPromise) => {
			resolve = resolvePromise;
			reject = rejectPromise;
		}
	);
	/*
	 * Authority loss or a terminal status can arrive before application code
	 * receives the succeeded receipt. Mark the internal lifecycle promise handled
	 * eagerly while preserving its rejection for every explicit awaiter.
	 */
	void promise.catch(() => undefined);
	const controller: PendingProjection = {
		commandId: metadata.commandId,
		causationId: metadata.causationId,
		authority,
		tracker,
		resolve,
		reject,
		promise,
		prepared,
		metadata,
		settled: false
	};
	return controller;
}

/**
 * Generated causal commands converge without application polling. Durable status
 * is the only completion signal; timers merely schedule reads and never infer a
 * successful outcome.
 */
export function monitorPendingProjection(
	controller: PendingProjection,
	readStatus: () => Promise<ReplicaCommandStatus>,
	retained: () => boolean,
	reportError: ((error: unknown) => void) | undefined
): void {
	if (
		controller.settled ||
		!retained() ||
		projectionAuthorityAborted(controller)
	) {
		return;
	}
	const monitorAbort = new AbortController();
	const stopMonitor = () => monitorAbort.abort();
	controller.stopMonitor = stopMonitor;
	const signals =
		controller.authority.signal === undefined
			? [monitorAbort.signal]
			: [controller.authority.signal, monitorAbort.signal];
	void (async () => {
		try {
			let delay = INITIAL_STATUS_POLL_MS;
			while (
				!controller.settled &&
				retained() &&
				!projectionAuthorityAborted(controller)
			) {
				await waitForProjectionPoll(delay, signals);
				if (
					controller.settled ||
					!retained() ||
					projectionAuthorityAborted(controller)
				) {
					return;
				}
				try {
					await readStatus();
				} catch (error) {
					if (
						controller.settled ||
						!retained() ||
						projectionAuthorityAborted(controller)
					) {
						return;
					}
					reportBackgroundErrorSafely(reportError, error);
				}
				delay = Math.min(delay * 2, MAX_STATUS_POLL_MS);
			}
		} finally {
			if (controller.stopMonitor === stopMonitor) {
				controller.stopMonitor = undefined;
			}
			monitorAbort.abort();
		}
	})().catch((error: unknown) =>
		reportBackgroundErrorSafely(reportError, error)
	);
}

export function reportBackgroundErrorSafely(
	reportError: ((error: unknown) => void) | undefined,
	error: unknown
): void {
	if (reportError === undefined) return;
	try {
		reportError(error);
	} catch {
		// Error reporting is a terminal boundary and must never reject detached work.
	}
}

export function projectionAuthorityAborted(
	controller: PendingProjection
): boolean {
	return controller.authority.signal?.aborted === true;
}

export function waitForProjectionPoll(
	delay: number,
	signals: readonly AbortSignal[]
): Promise<void> {
	if (signals.some((signal) => signal.aborted)) return Promise.resolve();
	return new Promise((resolve) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const finish = () => {
			if (settled) return;
			settled = true;
			if (timer !== undefined) clearTimeout(timer);
			for (const signal of signals) {
				signal.removeEventListener('abort', finish);
			}
			resolve();
		};
		timer = setTimeout(finish, delay);
		/*
		 * This poll owns the unresolved projected lifecycle. Keep Node's timer
		 * referenced until projection settlement or runtime disposal calls
		 * `finish`, which clears the timer and every abort listener.
		 */
		for (const signal of signals) {
			signal.addEventListener('abort', finish, { once: true });
		}
		// Close the check/register race if either signal aborted synchronously.
		if (signals.some((signal) => signal.aborted)) finish();
	});
}

export function settleProjectionSuccess(controller: PendingProjection): void {
	if (controller.settled) return;
	controller.settled = true;
	controller.abort?.();
	controller.stopMonitor?.();
	controller.resolve(
		Object.freeze({
			commandId: controller.commandId,
			state: 'atomic',
			metadata: controller.metadata
		})
	);
}

export function callerProjectedPromise(
	controller: PendingProjection,
	signal: AbortSignal | undefined
): Promise<ReplicaCommandProjectedOutcome<unknown>> {
	if (signal === undefined) return controller.promise;
	const callerSignal = signal;
	const promise = new Promise<ReplicaCommandProjectedOutcome<unknown>>(
		(resolve, reject) => {
			let settled = false;
			function settle(complete: () => void): void {
				if (settled) return;
				settled = true;
				callerSignal.removeEventListener('abort', onAbort);
				complete();
			}
			function onAbort(): void {
				/*
				 * Internal causal settlement wins once selected, even if its
				 * promise callbacks have not run yet. Caller cancellation never
				 * mutates that internal lifecycle.
				 */
				if (controller.settled) return;
				settle(() =>
					reject(
						new ReplicaCommandRuntimeError('REPLICA_COMMAND_ABORTED', {
							commandId: controller.commandId
						})
					)
				);
			}
			callerSignal.addEventListener('abort', onAbort, { once: true });
			void controller.promise.then(
				(value) => settle(() => resolve(value)),
				(error: unknown) => settle(() => reject(error))
			);
			if (callerSignal.aborted) onAbort();
		}
	);
	/*
	 * An AbortSignal can fire during an async `onSucceeded` callback, before the
	 * receipt reaches caller code. Keep that legitimate rejection observable
	 * without creating a process-level unhandled-rejection race.
	 */
	void promise.catch(() => undefined);
	return promise;
}

export function attachAuthorityAbort(
	controller: PendingProjection,
	onSettled: () => void
): void {
	const signal = controller.authority.signal;
	if (signal === undefined) return;
	const onAbort = () => {
		settleProjectionFailure(
			controller,
			new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_SCOPE_INVALIDATED',
				{ commandId: controller.commandId }
			)
		);
		onSettled();
	};
	signal.addEventListener('abort', onAbort, { once: true });
	controller.abort = () => signal.removeEventListener('abort', onAbort);
	if (signal.aborted) onAbort();
}

export function settleProjectionFailure(
	controller: PendingProjection,
	error: unknown
): void {
	if (controller.settled) return;
	controller.settled = true;
	controller.abort?.();
	controller.stopMonitor?.();
	controller.reject(error);
}

export function settleTrackedProjection(
	tracker: CommandStatusTracker,
	pending: Map<string, PendingProjection>
): void {
	const controller = tracker.pending;
	if (controller === undefined || controller.settled) return;
	settleProjectionSuccess(controller);
	pending.delete(controller.commandId);
}

export function failTrackedProjection(
	tracker: CommandStatusTracker,
	pending: Map<string, PendingProjection>,
	error: unknown
): void {
	const controller = tracker.pending;
	if (controller === undefined) return;
	settleProjectionFailure(controller, error);
	pending.delete(controller.commandId);
}
