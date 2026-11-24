import { DIAGNOSTICS_BUNDLE_MARKER } from './types.js';
import type { GraphqlVariables } from '../../types.js';
import type { ReplicaCommandArtifact } from '../commands/types.js';
import type {
	ReplicaIndexCoverage,
	ReplicaOperationArtifact,
	ReplicaValue
} from '../types.js';
import { reportUnhandledError } from '../../lib/report.js';
import type {
	ReplicaCommandArtifactInspection,
	ReplicaDevelopmentCapability,
	ReplicaDiagnosticEvent,
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticFieldValueContext,
	ReplicaDiagnosticFieldValuePolicy,
	ReplicaDiagnosticIndex,
	ReplicaDiagnosticIndexInput,
	ReplicaDiagnosticLayer,
	ReplicaDiagnosticLayerInput,
	ReplicaDiagnosticReasonContext,
	ReplicaDiagnosticReasonPolicy,
	ReplicaDiagnosticReceipt,
	ReplicaDiagnosticReceiptInput,
	ReplicaDiagnosticRecord,
	ReplicaDiagnosticRecordInput,
	ReplicaDiagnostics,
	ReplicaDiagnosticsOptions,
	ReplicaDiagnosticsSnapshot,
	ReplicaDiagnosticScopeInput,
	ReplicaDiagnosticStateInput,
	ReplicaOperationArtifactInspection
} from './types.js';
import {
	inspectReplicaCommandArtifact,
	inspectReplicaOperationArtifact
} from './inspect.js';

const reportUnhandledObserverError = reportUnhandledError;
const DEFAULT_MAX_EVENTS = 200;
const MAX_EVENTS = 10_000;
const MAX_VALUE_DEPTH = 64;
const MAX_REASON_LENGTH = 160;
const developmentCapabilities = new WeakSet<object>();


export function createReplicaDevelopmentCapability(): ReplicaDevelopmentCapability {
	const capability = Object.freeze({ version: 1 as const });
	developmentCapabilities.add(capability);
	return capability as ReplicaDevelopmentCapability;
}

/** Framework-neutral diagnostics store shared by every framework adapter. */
export function createReplicaDiagnostics(
	options: ReplicaDiagnosticsOptions = {}
): ReplicaDiagnostics {
	return new ReplicaDiagnosticsImpl(options);
}

class ReplicaDiagnosticsImpl implements ReplicaDiagnostics {
	readonly includeStructuralIdentities: boolean;
	readonly #maxEvents: number;
	readonly #now: () => number;
	readonly #reportObserverError: (error: AggregateError) => void;
	readonly #fieldValues: ReplicaDiagnosticFieldValuePolicy | undefined;
	readonly #reasons: ReplicaDiagnosticReasonPolicy | undefined;
	readonly #listeners = new Set<(snapshot: ReplicaDiagnosticsSnapshot) => void>();
	readonly #pseudonyms = new Map<string, Map<string, string>>();
	readonly #operations = new Map<string, ReplicaOperationArtifactInspection>();
	readonly #commands = new Map<string, ReplicaCommandArtifactInspection>();
	#scope: ReplicaDiagnosticScopeInput = Object.freeze({
		generation: 0,
		established: false
	});
	/** The latest state; its records and indexes are read by `snapshot`. */
	#state: ReplicaDiagnosticStateInput | undefined;
	#records: readonly ReplicaDiagnosticRecord[] | undefined;
	#indexes: readonly ReplicaDiagnosticIndex[] | undefined;
	#layers: readonly ReplicaDiagnosticLayer[] = Object.freeze([]);
	#receipts: readonly ReplicaDiagnosticReceipt[] = Object.freeze([]);
	#events: ReplicaDiagnosticEvent[] = [];
	#sequence = 0;
	#snapshotCache: ReplicaDiagnosticsSnapshot | undefined;

	constructor(options: ReplicaDiagnosticsOptions) {
		this.#maxEvents = boundedEventLimit(options.maxEvents);
		this.#now = options.now ?? Date.now;
		this.#reportObserverError =
			options.onObserverError ?? reportUnhandledObserverError;
		this.includeStructuralIdentities = validCapability(options.development);
		if (options.fieldValues !== undefined) {
			if (!validCapability(options.fieldValues.capability)) {
				throw new TypeError(
					'diagnostic field values require a development capability'
				);
			}
			if (
				typeof options.fieldValues.allow !== 'function' ||
				typeof options.fieldValues.redact !== 'function'
			) {
				throw new TypeError(
					'diagnostic field values require explicit allow and redact functions'
				);
			}
			this.#fieldValues = options.fieldValues;
		}
		if (options.reasons !== undefined) {
			if (!validCapability(options.reasons.capability)) {
				throw new TypeError(
					'diagnostic reasons require a development capability'
				);
			}
			if (typeof options.reasons.redact !== 'function') {
				throw new TypeError(
					'diagnostic reasons require an explicit redact function'
				);
			}
			this.#reasons = options.reasons;
		}
	}

	redactRecordValue(
		context: ReplicaDiagnosticFieldValueContext,
		value: ReplicaValue
	): ReplicaValue | undefined {
		const policy = this.#fieldValues;
		if (policy === undefined || !policy.allow(context)) return undefined;
		const redacted = policy.redact(value, context);
		return redacted === undefined
			? undefined
			: cloneDiagnosticValue(redacted, 'diagnostic field value');
	}

	update(state: ReplicaDiagnosticStateInput): void {
		validateScope(state.scope);
		if (state.scope.generation !== this.#scope.generation) {
			this.#clearScopedState();
		}
		this.#scope = freezeScope(state.scope);
		this.#state = state;
		this.#records = undefined;
		this.#indexes = undefined;
		this.#layers = Object.freeze(
			state.layers
				.map((layer) => this.#layer(layer))
				.sort((left, right) => left.sequence - right.sequence)
		);
		this.#receipts = Object.freeze(
			state.receipts
				.map((receipt) => this.#receipt(receipt))
				.sort((left, right) => left.commandId.localeCompare(right.commandId))
		);
		this.#snapshotCache = undefined;
		this.#notify();
	}

	event(input: ReplicaDiagnosticEventInput): void {
		const at = this.#now();
		if (!Number.isFinite(at)) {
			throw new TypeError('diagnostic clock must return a finite number');
		}
		const event = this.#event(input, ++this.#sequence, at);
		this.#events.push(event);
		if (this.#events.length > this.#maxEvents) {
			this.#events.splice(0, this.#events.length - this.#maxEvents);
		}
		this.#snapshotCache = undefined;
		this.#notify();
	}

	operation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): void {
		const inspection = inspectReplicaOperationArtifact(artifact);
		this.#operations.set(inspection.id, inspection);
		this.#snapshotCache = undefined;
		this.#notify();
	}

	command<TInput, TOutput>(artifact: ReplicaCommandArtifact<TInput, TOutput>): void {
		const inspection = inspectReplicaCommandArtifact(artifact);
		this.#commands.set(inspection.name, inspection);
		this.#snapshotCache = undefined;
		this.#notify();
	}

	inspectOperation<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>
	): ReplicaOperationArtifactInspection {
		const inspection = inspectReplicaOperationArtifact(artifact);
		this.#operations.set(inspection.id, inspection);
		this.#snapshotCache = undefined;
		this.#notify();
		return inspection;
	}

	inspectCommand<TInput, TOutput>(
		artifact: ReplicaCommandArtifact<TInput, TOutput>
	): ReplicaCommandArtifactInspection {
		const inspection = inspectReplicaCommandArtifact(artifact);
		this.#commands.set(inspection.name, inspection);
		this.#snapshotCache = undefined;
		this.#notify();
		return inspection;
	}

	readonly snapshot = (): ReplicaDiagnosticsSnapshot => {
		const cached = this.#snapshotCache;
		if (cached !== undefined) return cached;
		this.#records ??= Object.freeze(
			(this.#state?.records ?? [])
				.map((record) => this.#record(record))
				.sort((left, right) => left.key.localeCompare(right.key))
		);
		this.#indexes ??= Object.freeze(
			(this.#state?.indexes ?? [])
				.map((index) => this.#index(index))
				.sort((left, right) => left.key.localeCompare(right.key))
		);
		const snapshot = Object.freeze({
			version: 1 as const,
			marker: DIAGNOSTICS_BUNDLE_MARKER,
			mode: this.includeStructuralIdentities ? 'development' : 'redacted',
			sequence: this.#sequence,
			scope: this.#scope,
			records: this.#records,
			indexes: this.#indexes,
			layers: this.#layers,
			receipts: this.#receipts,
			artifacts: Object.freeze({
				operations: Object.freeze(
					[...this.#operations.values()].sort((left, right) =>
						left.id.localeCompare(right.id)
					)
				),
				commands: Object.freeze(
					[...this.#commands.values()].sort((left, right) =>
						left.name.localeCompare(right.name)
					)
				)
			}),
			events: Object.freeze([...this.#events])
		});
		this.#snapshotCache = snapshot;
		return snapshot;
	};

	readonly getSnapshot = (): ReplicaDiagnosticsSnapshot => this.snapshot();

	readonly subscribe = (
		listener: (snapshot: ReplicaDiagnosticsSnapshot) => void
	): (() => void) => {
		if (typeof listener !== 'function') {
			throw new TypeError('diagnostic listener must be a function');
		}
		this.#listeners.add(listener);
		try {
			listener(this.snapshot());
		} catch (error) {
			this.#listeners.delete(listener);
			this.#report([error]);
		}
		return () => this.#listeners.delete(listener);
	};

	clear(): void {
		this.#scope = Object.freeze({ generation: 0, established: false });
		this.#clearScopedState();
		this.#sequence = 0;
		this.#snapshotCache = undefined;
		this.#notify();
	}

	#clearScopedState(): void {
		this.#state = undefined;
		this.#records = undefined;
		this.#indexes = undefined;
		this.#layers = Object.freeze([]);
		this.#receipts = Object.freeze([]);
		this.#events = [];
		this.#operations.clear();
		this.#commands.clear();
		this.#pseudonyms.clear();
		this.#snapshotCache = undefined;
	}

	#record(input: ReplicaDiagnosticRecordInput): ReplicaDiagnosticRecord {
		const values =
			this.#fieldValues === undefined || input.values === undefined
				? undefined
				: freezeValueRecord(input.values, 'diagnostic record values');
		return Object.freeze({
			key: this.#identifier('record', input.key),
			...(input.model === undefined ? {} : { model: input.model }),
			revision: input.revision,
			incarnation: input.incarnation,
			tombstone: input.tombstone,
			...(input.tombstoneRevision === undefined
				? {}
				: { tombstoneRevision: input.tombstoneRevision }),
			presentFields: Object.freeze([...input.presentFields].sort()),
			presentLinks: Object.freeze([...input.presentLinks].sort()),
			...(values === undefined ? {} : { values })
		});
	}

	#index(input: ReplicaDiagnosticIndexInput): ReplicaDiagnosticIndex {
		const rawArguments = input.arguments ?? Object.freeze({});
		const argumentsValue =
			this.includeStructuralIdentities && Object.keys(rawArguments).length > 0
				? freezeValueRecord(rawArguments, 'diagnostic index arguments')
				: undefined;
		const staleReason =
			input.staleReason === undefined
				? undefined
				: this.#reason(
						input.staleReason,
						Object.freeze({
							kind: 'index-stale' as const,
							indexKey: input.key
						}),
						structuralStaleReason(input.staleReason)
					);
		return Object.freeze({
			key: this.#identifier('index', input.key),
			revision: input.revision,
			...(input.staleRevision === undefined
				? {}
				: { staleRevision: input.staleRevision }),
			records: Object.freeze(
				input.records.map((key) => this.#identifier('record', key))
			),
			complete: input.complete,
			deleted: input.deleted,
			...(input.field === undefined ? {} : { field: input.field }),
			...(input.parent === undefined
				? {}
				: { parent: this.#identifier('record', input.parent) }),
			argumentNames: Object.freeze(
				[...(input.argumentNames ?? Object.keys(rawArguments))].sort()
			),
			...(argumentsValue === undefined ? {} : { arguments: argumentsValue }),
			...(input.coverage === undefined
				? {}
				: { coverage: freezeCoverage(input.coverage) }),
			dependencies: Object.freeze([...(input.dependencies ?? [])].sort()),
			...(staleReason === undefined ? {} : { staleReason }),
			nullValue: input.nullValue === true
		});
	}

	#layer(input: ReplicaDiagnosticLayerInput): ReplicaDiagnosticLayer {
		return Object.freeze({
			id: this.#identifier('transaction', input.id),
			sequence: input.sequence,
			state: input.state,
			recordChanges: input.recordChanges,
			indexChanges: input.indexChanges,
			semanticChanges: input.semanticChanges
		});
	}

	#receipt(input: ReplicaDiagnosticReceiptInput): ReplicaDiagnosticReceipt {
		return Object.freeze({
			commandId: this.#identifier('transaction', input.commandId),
			state: input.state,
			...(input.consistency === undefined
				? {}
				: { consistency: input.consistency }),
			expectations: Object.freeze(
				input.expectations
					.map((expectation) =>
						Object.freeze({
							projection: expectation.projection,
							model: expectation.model,
							observed: expectation.observed
						})
					)
					.sort((left, right) =>
						`${left.projection}\0${left.model}`.localeCompare(
							`${right.projection}\0${right.model}`
						)
					)
			)
		});
	}

	#event(
		input: ReplicaDiagnosticEventInput,
		sequence: number,
		at: number
	): ReplicaDiagnosticEvent {
		if (input.kind === 'layer') {
			const reason =
				input.reason === undefined
					? undefined
					: this.#reason(
							input.reason,
							Object.freeze({
								kind: 'layer' as const,
								layerId: input.layer,
								action: input.action
							}),
							structuralLayerReason(input.reason)
						);
			return Object.freeze({
				...input,
				layer: this.#identifier('transaction', input.layer),
				...(reason === undefined ? {} : { reason }),
				sequence,
				at
			});
		}
		if (input.kind === 'receipt') {
			return Object.freeze({
				...input,
				command: this.#identifier('transaction', input.command),
				sequence,
				at
			});
		}
		if (input.kind === 'index-decision') {
			const reason =
				input.reason === undefined
					? undefined
					: this.#reason(
							input.reason,
							Object.freeze({
								kind: 'index-stale' as const,
								indexKey: input.index
							}),
							structuralStaleReason(input.reason)
						);
			return Object.freeze({
				...input,
				index: this.#identifier('index', input.index),
				...(reason === undefined ? {} : { reason }),
				sequence,
				at
			});
		}
		return Object.freeze({ ...input, sequence, at });
	}

	#reason(
		value: string,
		context: ReplicaDiagnosticReasonContext,
		fallback: string
	): string {
		const redacted = this.#reasons?.redact(value, context);
		return redacted === undefined
			? fallback
			: boundedDiagnosticReason(redacted, fallback);
	}

	#identifier(kind: string, value: string): string {
		if (this.includeStructuralIdentities) return value;
		let values = this.#pseudonyms.get(kind);
		if (values === undefined) {
			values = new Map();
			this.#pseudonyms.set(kind, values);
		}
		let identifier = values.get(value);
		if (identifier === undefined) {
			identifier = `${kind}#${values.size + 1}`;
			values.set(value, identifier);
		}
		return identifier;
	}

	#notify(): void {
		if (this.#listeners.size === 0) return;
		const snapshot = this.snapshot();
		const errors: unknown[] = [];
		for (const listener of this.#listeners) {
			try {
				listener(snapshot);
			} catch (error) {
				errors.push(error);
			}
		}
		this.#report(errors);
	}

	#report(errors: unknown[]): void {
		if (errors.length === 0) return;
		try {
			this.#reportObserverError(
				new AggregateError(errors, 'replica diagnostic listener failed')
			);
		} catch (reporterError) {
			queueMicrotask(() => {
				throw new AggregateError(
					[...errors, reporterError],
					'replica diagnostic error reporter failed'
				);
			});
		}
	}
}

function boundedEventLimit(value: number | undefined): number {
	if (value === undefined) return DEFAULT_MAX_EVENTS;
	if (!Number.isSafeInteger(value) || value < 1 || value > MAX_EVENTS) {
		throw new TypeError(
			`diagnostic maxEvents must be an integer between 1 and ${MAX_EVENTS}`
		);
	}
	return value;
}

function structuralStaleReason(value: string): string {
	switch (value) {
		case 'application-stale':
		case 'graphql-error':
		case 'graphql-partial-error':
		case 'incomplete-result':
		case 'record-lifecycle-changed':
		case 'revision-conflict':
			return value;
		default: {
			const queryPlan = /^query-plan:([a-z][a-z0-9_]{0,63})(?::.*)?$/.exec(
				value
			);
			return queryPlan === null
				? 'application-stale'
				: `query-plan:${queryPlan[1]}`;
		}
	}
}

function structuralLayerReason(value: string): string {
	return value === 'retired-earlier-layer' ||
		value === 'rejected-earlier-layer'
		? value
		: 'application-layer-change';
}

function boundedDiagnosticReason(value: string, fallback: string): string {
	if (typeof value !== 'string') {
		throw new TypeError('diagnostic reason redactor must return a string');
	}
	const normalized = value
		.replace(/[\u0000-\u001f\u007f]/g, '\ufffd')
		.trim();
	if (normalized.length === 0) return fallback;
	return [...normalized].slice(0, MAX_REASON_LENGTH).join('');
}

function validCapability(
	capability: ReplicaDevelopmentCapability | undefined
): boolean {
	return (
		capability !== undefined &&
		typeof capability === 'object' &&
		developmentCapabilities.has(capability)
	);
}

function validateScope(scope: ReplicaDiagnosticScopeInput): void {
	if (!Number.isSafeInteger(scope.generation) || scope.generation < 0) {
		throw new TypeError('diagnostic scope generation must be an unsigned integer');
	}
	if (scope.established) {
		if (scope.protocolVersion !== 1 || !nonempty(scope.schemaHash)) {
			throw new TypeError(
				'established diagnostic scope requires protocol and schema hash'
			);
		}
	} else if (scope.protocolVersion !== undefined || scope.schemaHash !== undefined) {
		throw new TypeError(
			'unestablished diagnostic scope cannot carry protocol identity'
		);
	}
}

function freezeScope(scope: ReplicaDiagnosticScopeInput): ReplicaDiagnosticScopeInput {
	return Object.freeze({
		generation: scope.generation,
		established: scope.established,
		...(scope.protocolVersion === undefined
			? {}
			: { protocolVersion: scope.protocolVersion }),
		...(scope.schemaHash === undefined ? {} : { schemaHash: scope.schemaHash })
	});
}

function freezeCoverage(coverage: ReplicaIndexCoverage): ReplicaIndexCoverage {
	return Object.freeze({ ...coverage });
}

function freezeValueRecord(
	value: Readonly<Record<string, ReplicaValue>>,
	description: string,
	depth = 0
): Readonly<Record<string, ReplicaValue>> {
	const entries = Object.entries(value)
		.sort(([left], [right]) => left.localeCompare(right))
		.map(
			([key, entry]) =>
				[
					key,
					cloneDiagnosticValue(entry, `${description}.${key}`, depth + 1)
				] as const
		);
	return Object.freeze(Object.fromEntries(entries));
}

function cloneDiagnosticValue(
	value: ReplicaValue,
	description: string,
	depth = 0
): ReplicaValue {
	if (depth > MAX_VALUE_DEPTH) {
		throw new TypeError(`${description} exceeds diagnostic value depth`);
	}
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean' ||
		(typeof value === 'number' && Number.isFinite(value))
	) {
		return value;
	}
	if (Array.isArray(value)) {
		return Object.freeze(
			value.map((entry, index) =>
				cloneDiagnosticValue(entry, `${description}[${index}]`, depth + 1)
			)
		);
	}
	if (typeof value !== 'object') {
		throw new TypeError(`${description} must be a JSON-compatible value`);
	}
	return freezeValueRecord(
		value as Readonly<Record<string, ReplicaValue>>,
		description,
		depth
	);
}

function nonempty(value: unknown): value is string {
	return typeof value === 'string' && value.length > 0;
}

