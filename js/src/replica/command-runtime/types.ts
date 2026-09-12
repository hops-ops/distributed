import type {
	DistributedCommandMetadata,
	DistributedCommandState,
	DistributedRecordRevision,
	DistributedTrustedPreset
} from '../../protocol.js';
import type { GqlError } from '../../types.js';
import type {
	PrepareReplicaCommandOptions,
	ReplicaCommandArtifact,
	ReplicaPreparedCommand,
	ReplicaTrustedPresetDescriptor
} from '../commands.js';
import type { ReplicaDiagnosticsSink } from '../diagnostics.js';
import type { ReplicaIndexSemanticChange } from '../index-maintenance.js';
import type {
	DistributedReplica,
	ReplicaAuthoritativeScope,
	ReplicaClientSurface,
	ReplicaIdentity,
	ReplicaModelArtifact,
	ReplicaOptimisticWriter,
	ReplicaResultEnvelope,
	ReplicaValue
} from '../types.js';
import type { ReplicaPureFunction } from '../projection-delta/index.js';
import {
	replicaCommandAuthority,
	replicaCommandFreshness,
	replicaCommandDirectProjection,
	replicaCommandProjectionDelta,
	replicaCommandProjectedLifecycle,
	replicaCommandReadRecord,
	replicaResultObservation
} from './symbols.js';

export type ReplicaCommandSurfaceContract = Readonly<{
	protocolVersion: 1;
	schemaHash: string;
	protocolHash: string;
	surface: ReplicaClientSurface;
	trustedPresets: readonly ReplicaTrustedPresetDescriptor[];
}>;

export type ReplicaCommandAuthoritySnapshot = Readonly<{
	generation: number;
	scope: ReplicaAuthoritativeScope | undefined;
	trustedPresets: readonly DistributedTrustedPreset[];
	/** Aborted by the replica when this authorization generation closes. */
	signal?: AbortSignal;
}>;

export type ReplicaCommandAuthorityRegistration = Readonly<{
	read(): ReplicaCommandAuthoritySnapshot;
	dispose(): void;
}>;

export type ReplicaResultObservationRegistration = Readonly<{
	dispose(): void;
}>;

export type ReplicaCommandDirectProjection = Readonly<{
	model: ReplicaModelArtifact;
	identity: ReplicaIdentity;
	evidence: DistributedRecordRevision;
	fields: Readonly<Record<string, ReplicaValue>>;
}>;

export type ReplicaCommandAuthorityHost = DistributedReplica & {
	readonly [replicaCommandFreshness]?: (commandId: string, plan: import('../types.js').ReplicaRevalidationPlan) => void;
	readonly [replicaCommandAuthority]?: (
		contract: ReplicaCommandSurfaceContract
	) => ReplicaCommandAuthorityRegistration;
	readonly [replicaResultObservation]?: (
		observer: (envelope: ReplicaResultEnvelope<unknown>) => void
	) => ReplicaResultObservationRegistration;
	readonly [replicaCommandDirectProjection]?: (
		commandId: string,
		projection: ReplicaCommandDirectProjection
	) => void;
	readonly [replicaCommandProjectionDelta]?: (
		commandId: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[]
	) => boolean;
	readonly [replicaCommandReadRecord]?: (
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity
	) => Readonly<Record<string, ReplicaValue>> | undefined;
};

export type ReplicaCommandTransportRequest = Readonly<{
	operation: 'mutation';
	commandName: string;
	commandId: string;
	mutationField: string;
	document: string;
	operationHash: string;
	variables: Readonly<Record<string, unknown>>;
	extensions: Readonly<Record<string, unknown>>;
	signal?: AbortSignal;
}>;

export type ReplicaCommandStatusArtifact = Readonly<{
	name: string;
	document: string;
	operationHash: string;
	protocol: Readonly<{
		version: 1;
		schemaHash: string;
		protocolHash: string;
		surface: ReplicaClientSurface;
		operation: string;
		/** Exact surface union: command presets plus client-evaluable policy claims. */
		trustedPresets: readonly ReplicaTrustedPresetDescriptor[];
	}>;
}>;

export type ReplicaCommandStatusRequest = Readonly<{
	operation: 'status';
	commandId: string;
	name: string;
	document: string;
	operationHash: string;
	variables: Readonly<{ commandId: string }>;
	extensions: Readonly<Record<string, unknown>>;
	signal?: AbortSignal;
}>;

export type ReplicaCommandTransportResult = Readonly<{
	data?: Readonly<Record<string, unknown>> | null;
	errors?: readonly GqlError[];
	extensions?: unknown;
	status: number;
}>;

export interface ReplicaCommandTransport {
	dispatch(
		request: ReplicaCommandTransportRequest
	): Promise<ReplicaCommandTransportResult>;
	/** Execute the exact compiler-owned command-status operation. */
	status?(
		request: ReplicaCommandStatusRequest
	): Promise<ReplicaCommandTransportResult>;
}

export type ReplicaCommandProjectedOutcome<TOutput> = Readonly<{
	commandId: string;
	state: 'atomic';
	/** Present for same-transaction Atomic<T>, absent for async facts. */
	result?: TOutput;
	metadata?: DistributedCommandMetadata;
}>;

export type ReplicaCommandStatus = Readonly<{
	commandId: string;
	state: DistributedCommandState;
	/**
	 * Durable causal evidence. The server deliberately omits it for the
	 * non-enumerating `unknown` and compact `expired` states.
	 */
	metadata?: DistributedCommandMetadata;
}>;

export type ReplicaCommandReceipt<TOutput> = Readonly<{
	commandId: string;
	state: Extract<
		DistributedCommandState,
		'succeeded' | 'succeeded_pending_projection' | 'atomic'
	>;
	/** Typed application payload returned by the generated mutation. */
	result: TOutput;
	metadata: DistributedCommandMetadata;
	/** One exact generated status read. Calls coalesce while in flight. */
	status(): Promise<ReplicaCommandStatus>;
	/**
	 * Eventual visibility awaitable. It is omitted when no finite projection
	 * contract exists and never resolves because a wall-clock timer elapsed.
	 */
	projected?: Promise<ReplicaCommandProjectedOutcome<TOutput>>;
}>;

export type ReplicaCommandReceiptWithLifecycle = ReplicaCommandReceipt<unknown> &
	Readonly<{
		[replicaCommandProjectedLifecycle]?: Promise<
			ReplicaCommandProjectedOutcome<unknown>
		>;
	}>;

/**
 * Package-private causal lifecycle used by framework adapters.
 *
 * A caller-scoped `receipt.projected` can reject when its AbortSignal fires
 * after acceptance. The underlying command remains globally pending until
 * canonical projection evidence settles this independent promise.
 *
 * @internal
 */

/**
 * Stable recovery handle attached to an ambiguous dispatch error.
 *
 * `status()` never infers success from time. The original immutable prepared
 * mutation remains retained by the runtime/layer; status reports the durable
 * server outcome without manufacturing rollback or confirmation.
 */
export type ReplicaCommandRecoveryReceipt<TOutput = unknown> = Readonly<{
	commandId: string;
	status(): Promise<ReplicaCommandStatus>;
	/** Phantom slot retains the originating generated output type. */
	readonly __output?: TOutput;
}>;

export type ReplicaCommandCallOptions<TOutput> = PrepareReplicaCommandOptions &
	Readonly<{
		/**
		 * Cancels dispatch while the command outcome is unknown. After finite
		 * acceptance it instead bounds only this caller's `receipt.projected`
		 * wait; causal tracking and the optimistic layer continue independently.
		 */
		signal?: AbortSignal;
		/** Exact same prepared request retries after thrown/ambiguous transport. */
		transportRetries?: number;
		onSucceeded?: (
			receipt: ReplicaCommandReceipt<TOutput>
		) => void | Promise<void>;
	}>;

export type ReplicaCommandRuntimeErrorCode =
	| 'REPLICA_COMMAND_ABORTED'
	| 'REPLICA_COMMAND_AUTHORITY_UNAVAILABLE'
	| 'REPLICA_COMMAND_DISPOSED'
	| 'REPLICA_COMMAND_OUTCOME_PENDING'
	| 'REPLICA_COMMAND_PROJECTION_FAILED'
	| 'REPLICA_COMMAND_PROTOCOL_INVALID'
	| 'REPLICA_COMMAND_REJECTED'
	| 'REPLICA_COMMAND_RELOADING'
	| 'REPLICA_COMMAND_SCOPE_INVALIDATED'
	| 'REPLICA_COMMAND_STATUS_UNAVAILABLE'
	| 'REPLICA_COMMAND_TRANSPORT_AMBIGUOUS';

export type AnyCommandArtifact = ReplicaCommandArtifact<unknown, unknown>;
export type CommandEntry =
	| AnyCommandArtifact
	| Readonly<{ artifact: AnyCommandArtifact }>;

export type ArtifactOf<TEntry> = TEntry extends ReplicaCommandArtifact<
	infer TInput,
	infer TOutput
>
	? ReplicaCommandArtifact<TInput, TOutput>
	: TEntry extends Readonly<{
				artifact: ReplicaCommandArtifact<infer TInput, infer TOutput>;
			}>
		? ReplicaCommandArtifact<TInput, TOutput>
		: never;

export type InputOf<TEntry> =
	ArtifactOf<TEntry> extends ReplicaCommandArtifact<infer TInput, unknown>
		? TInput
		: never;

export type OutputOf<TEntry> =
	ArtifactOf<TEntry> extends ReplicaCommandArtifact<unknown, infer TOutput>
		? TOutput
		: never;

export type ReplicaBoundCommand<TInput, TOutput> = [TInput] extends [void]
	? (
			options?: ReplicaCommandCallOptions<TOutput>
		) => Promise<ReplicaCommandReceipt<TOutput>>
	: (
			input: TInput,
			options?: ReplicaCommandCallOptions<TOutput>
		) => Promise<ReplicaCommandReceipt<TOutput>>;

export type ReplicaBoundCommandPath<
	TPath extends string,
	TCommand
> = TPath extends `${infer THead}.${infer TTail}`
	? THead extends ''
		? never
		: Readonly<{
				[TKey in THead]: ReplicaBoundCommandPath<TTail, TCommand>;
			}>
	: TPath extends ''
		? never
		: Readonly<{ [TKey in TPath]: TCommand }>;

export type UnionToIntersection<TValue> = (
	TValue extends unknown ? (value: TValue) => void : never
) extends (value: infer TIntersection) => void
	? TIntersection
	: never;

export type SimplifyCommandTree<TValue> = TValue extends (
	...args: infer _TArguments
) => infer _TResult
	? TValue
	: Readonly<{
			[TKey in keyof TValue]: SimplifyCommandTree<TValue[TKey]>;
		}>;

export type ReplicaBoundCommands<
	TEntries extends Readonly<Record<string, CommandEntry>>
> = SimplifyCommandTree<
	UnionToIntersection<
		{
			[TKey in Extract<keyof TEntries, string>]: ReplicaBoundCommandPath<
				TKey,
				ReplicaBoundCommand<
					InputOf<TEntries[TKey]>,
					OutputOf<TEntries[TKey]>
				>
			>;
		}[Extract<keyof TEntries, string>]
	>
>;

export type ReplicaCommandRuntimeOptions = Readonly<{
	onBackgroundError?: (error: unknown) => void;
	/** Exact generated status operation for delayed/ambiguous recovery. */
	status?: ReplicaCommandStatusArtifact;
	/** Optional shared replica diagnostics sink used only for static artifact inspection. */
	diagnostics?: ReplicaDiagnosticsSink;
	/**
	 * Named pure functions referenced by `artifact.projection.pureReduces`.
	 * Generated clients ship their inventory; apps do not invent board sims ad hoc.
	 */
	pureFunctions?: Readonly<Record<string, ReplicaPureFunction>>;
	/** Framework lifecycle fence checked immediately before command preparation. */
	lifecycle?: Readonly<{ assertDispatchOpen(): void }>;
}>;

export interface ReplicaCommandRuntime<
	TEntries extends Readonly<Record<string, CommandEntry>>
> {
	readonly commands: ReplicaBoundCommands<TEntries>;
	/**
	 * Observe a query/live result only after the replica committed it.
	 *
	 * This completes pending causal awaitables from exact scoped observations;
	 * it does not normalize or confirm cache data itself.
	 */
	observeResult(envelope: ReplicaResultEnvelope<unknown>): void;
	/** Opaque receipt identities safe to carry across a controlled reload. */
	pendingCommandIds(): readonly string[];
	dispose(): void;
}

export type CapturedAuthority = Readonly<{
	generation: number;
	scope: ReplicaAuthoritativeScope;
	trustedPresets: readonly DistributedTrustedPreset[];
	signal?: AbortSignal;
}>;

export type PendingProjection = {
	readonly commandId: string;
	readonly causationId: string;
	readonly authority: CapturedAuthority;
	readonly tracker: CommandStatusTracker;
	readonly resolve: (
		value: ReplicaCommandProjectedOutcome<unknown>
	) => void;
	readonly reject: (error: unknown) => void;
	readonly promise: Promise<ReplicaCommandProjectedOutcome<unknown>>;
	readonly prepared: ReplicaPreparedCommand<unknown, unknown>;
	abort?: () => void;
	stopMonitor?: () => void;
	metadata: DistributedCommandMetadata;
	settled: boolean;
};

export type CommandStatusTracker = {
	state: DistributedCommandState | undefined;
	metadata: DistributedCommandMetadata | undefined;
	inFlight: Promise<ReplicaCommandStatus> | undefined;
	pending?: PendingProjection;
};

export type SemanticReplica = DistributedReplica & {
	createOptimisticLayer(
		id: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges?: readonly ReplicaIndexSemanticChange[]
	): void;
};
