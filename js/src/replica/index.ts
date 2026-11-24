export { createDistributedReplica } from './distributed-replica/index.js';
export { createReplicaDevelopmentCapability, createReplicaDiagnostics } from './diagnostics/event-log.js';
export { inspectReplicaCommandArtifact, inspectReplicaOperationArtifact } from './diagnostics/inspect.js';
export type {
	ReplicaArtifactSourceLocation,
	ReplicaCommandArtifactInspection,
	ReplicaCommandEffectInspection,
	ReplicaDevelopmentCapability,
	ReplicaDiagnosticEvent,
	ReplicaDiagnosticEventInput,
	ReplicaDiagnosticFieldValueContext,
	ReplicaDiagnosticFieldValuePolicy,
	ReplicaDiagnosticIndex,
	ReplicaDiagnosticIndexInput,
	ReplicaDiagnosticLayer,
	ReplicaDiagnosticLayerInput,
	ReplicaDiagnosticReceipt,
	ReplicaDiagnosticReceiptExpectationInput,
	ReplicaDiagnosticReceiptInput,
	ReplicaDiagnosticRecord,
	ReplicaDiagnosticRecordInput,
	ReplicaDiagnosticReasonContext,
	ReplicaDiagnosticReasonPolicy,
	ReplicaDiagnostics,
	ReplicaDiagnosticsOptions,
	ReplicaDiagnosticsSink,
	ReplicaDiagnosticsSnapshot,
	ReplicaDiagnosticScopeInput,
	ReplicaDiagnosticStateInput,
	ReplicaOperationArtifactInspection,
	ReplicaOperationIndexInspection,
	ReplicaOperationInjectedFieldInspection
} from './diagnostics/types.js';
export { createReplicaGraphqlTransport } from './graphql-transport.js';
export { createReplicaUuidV7 } from './command-id.js';
export type {
	ReplicaDocumentTransport,
	ReplicaGraphqlTransport,
	ReplicaGraphqlTransportOptions
} from './graphql-transport.js';
export { canonicalizeOperationVariables } from './identity/codec.js';
export { replicaIndexKey, replicaRecordKey } from './identity/keys.js';
export { prepareReplicaCommand } from './commands/prepare.js';
export { ReplicaCommandContractError } from './commands/errors.js';
export { verifyReplicaCommandReceipt } from './commands/receipt.js';
export { createReplicaCommandRuntime } from './command-runtime/create.js';
export { ReplicaCommandRuntimeError } from './command-runtime/errors.js';
export { createReplicaIndexedDbPersistence } from './persistence/idb.js';
export { REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED } from './persistence/types.js';
export type {
	ReplicaBoundCommand,
	ReplicaBoundCommands,
	ReplicaCommandCallOptions,
	ReplicaCommandProjectedOutcome,
	ReplicaCommandReceipt,
	ReplicaCommandRecoveryReceipt,
	ReplicaCommandRuntime,
	ReplicaCommandRuntimeErrorCode,
	ReplicaCommandRuntimeOptions,
	ReplicaCommandStatus,
	ReplicaCommandStatusArtifact,
	ReplicaCommandStatusRequest,
	ReplicaCommandTransport,
	ReplicaCommandTransportRequest,
	ReplicaCommandTransportResult
} from './command-runtime/types.js';
export type {
	ReplicaIndexedDbFactory,
	ReplicaIndexedDbPersistence,
	ReplicaIndexedDbPersistenceOptions,
	ReplicaPersistenceModelPolicy,
	ReplicaPersistencePolicy
} from './persistence/types.js';
export type {
	ReplicaCommandOutbox,
	ReplicaCommandOutboxCommandPolicy,
	ReplicaCommandOutboxDrainReport,
	ReplicaCommandOutboxEntrySummary,
	ReplicaCommandOutboxOptions,
	ReplicaCommandOutboxOutcome,
	ReplicaCommandOutboxPolicy,
	ReplicaCommandOutboxSettlement
} from './persistence/outbox.js';
export type {
	PrepareReplicaCommandOptions,
	ReplicaCommandArtifact,
	ReplicaCommandContractErrorCode,
	ReplicaCommandDirectProjection,
	ReplicaCommandEffectExpression,
	ReplicaCommandEffectRelationship,
	ReplicaCommandGenerators,
	ReplicaCommandInputDefault,
	ReplicaCommandInputDefaults,
	ReplicaCommandRevalidationPlan,
	ReplicaCommandScalarCodec,
	ReplicaCommandShape,
	ReplicaCommandTypeDefinition,
	ReplicaCommandTypeField,
	ReplicaCommandVariables,
	ReplicaTrustedPresetDescriptor,
	ReplicaPreparedCommand,
	ReplicaReceiptVerification
} from './commands/types.js';
export { createWasmJsonPure } from './projection-delta/wasm-pure.js';
export type {
	CreateWasmJsonPureOptions,
	WasmJsonModule,
	WasmJsonPureHost
} from './projection-delta/wasm-pure.js';
export type {
	PreparedCommandProjection,
	PreparedProjectionOperation,
	ReplicaCommandProjection
} from './projection-delta/types.js';
export { compareReplicaOrder } from './query-plan/order.js';
export { decideReplicaPaginationMaintenance } from './query-plan/pagination.js';
export { evaluateReplicaFilter } from './query-plan/filter.js';
export { createReplicaIndexMaintenanceRegistry } from './index-maintenance/registry.js';
export { formatReplicaIndexStaleReason } from './index-maintenance/engine.js';
export type {
	ReplicaIndexDependencyChange,
	ReplicaIndexMaintenanceDecision,
	ReplicaIndexMaintenanceIndex,
	ReplicaIndexMaintenanceReason,
	ReplicaIndexMaintenanceReasonCode,
	ReplicaIndexMaintenanceRecord,
	ReplicaIndexMaintenanceRegistry,
	ReplicaIndexMaintenanceSnapshot,
	ReplicaIndexPlanRegistration,
	ReplicaIndexRecordChange,
	ReplicaIndexRelationshipChange,
	ReplicaIndexSemanticChange,
	ReplicaIndexSemanticLayer
} from './index-maintenance/types.js';
export type {
	ReplicaFilterEvaluation,
	ReplicaFilterEvaluationOptions,
	ReplicaOrderComparison,
	ReplicaPaginationChange,
	ReplicaPaginationMaintenanceDecision,
	ReplicaQueryPlanPath,
	ReplicaQueryPlanReason,
	ReplicaQueryPlanReasonCode,
	ReplicaRelationshipFilterRequest
} from './query-plan/types.js';
export type {
	DistributedReplicaOptions,
	DistributedReplica,
	ReplicaArgumentsArtifact,
	ReplicaArgumentValue,
	ReplicaAuthoritativeScope,
	ReplicaBaseWriter,
	ReplicaBranchSemantic,
	ReplicaClientSurface,
	ReplicaCoverageArtifact,
	ReplicaDehydratedState,
	ReplicaFilterArtifact,
	ReplicaFilterExpression,
	ReplicaFilterFieldArtifact,
	ReplicaFilterLiteral,
	ReplicaFilterOperand,
	ReplicaFilterOperator,
	ReplicaIdentity,
	ReplicaIndexCoverage,
	ReplicaIndexInspection,
	ReplicaIndexTarget,
	ReplicaIslandMetadata,
	ReplicaIslandOperation,
	ReplicaListValue,
	ReplicaLiveObserver,
	ReplicaLiveState,
	ReplicaLiteralValue,
	ReplicaModelArtifact,
	ReplicaObjectBranch,
	ReplicaObjectMember,
	ReplicaObjectSelection,
	ReplicaObjectValue,
	ReplicaOperationArtifact,
	ReplicaOperationSourceLocation,
	ReplicaOperationProtocol,
	ReplicaOrderArtifact,
	ReplicaOrderFieldArtifact,
	ReplicaOrderTieBreakerArtifact,
	ReplicaOptimisticWriter,
	ReplicaPaginationArtifact,
	ReplicaPaginationDisposition,
	ReplicaProtocolOperationArtifact,
	ReplicaRecordInspection,
	ReplicaRecordPatch,
	ReplicaRevalidationPlan,
	ReplicaRevalidationRelationship,
	ReplicaRevision,
	ReplicaRelationshipArtifact,
	ReplicaRelationshipKeyMapping,
	ReplicaRelationshipKind,
	ReplicaResultEnvelope,
	ReplicaRowPolicyArtifact,
	ReplicaRootSelection,
	ReplicaScalarSelection,
	ReplicaSelectionStorage,
	ReplicaSparse,
	ReplicaSnapshot,
	ReplicaStatus,
	ReplicaTransport,
	ReplicaTransportRequest,
	ReplicaVariableValue,
	ReplicaVariableCodecArtifact,
	ReplicaVariableEnumInputRef,
	ReplicaVariableFilterInputDefinition,
	ReplicaVariableFilterInputField,
	ReplicaVariableFilterInputRelationship,
	ReplicaVariableFilterInputTarget,
	ReplicaVariableInputDefinition,
	ReplicaVariableInputRef,
	ReplicaVariableListInputRef,
	ReplicaVariableNamedInputRef,
	ReplicaVariableOrderInputDefinition,
	ReplicaVariableOrderInputField,
	ReplicaVariableScalarInputRef,
	ReplicaValue,
	ReplicaWatch,
	ReplicaWriteSource,
	WatchReplicaOptions
} from './types.js';

export type {
	ReplicaLazyCommandCatalog,
	ReplicaLazyCommandModule,
	ReplicaLazyCommandRuntime
} from './command-runtime/lazy.js';
