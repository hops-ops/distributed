import type { GqlError, GraphqlVariables } from '../types.js';
import type {
	DistributedCommandMetadata,
	DistributedLiveCursor,
	DistributedTrustedPresetCodec,
	GraphqlResponseExtensions
} from '../protocol.js';
import type { ReplicaDiagnosticsSink } from './diagnostics.js';

export type ReplicaRevision = number | string | bigint;
export type ReplicaValue =
	| null
	| boolean
	| number
	| string
	| readonly ReplicaValue[]
	| { readonly [key: string]: ReplicaValue };

export type ReplicaIndexRecordChange =
	| {
			readonly kind: 'upsert';
			readonly model: string;
			readonly key: string;
			readonly fields: Readonly<Record<string, ReplicaValue>>;
			readonly unset?: readonly string[];
			readonly ifPresent?: boolean;
			readonly dependencies?: readonly string[];
	  }
	| {
			readonly kind: 'delete';
			readonly model: string;
			readonly key: string;
			readonly dependencies?: readonly string[];
	  };

export type ReplicaIndexRelationshipChange = {
	readonly kind: 'link' | 'unlink';
	readonly sourceModel: string;
	readonly field: string;
	readonly targetModel: string;
	readonly sourceKey: string;
	readonly targetKey: string;
	readonly dependencies: readonly string[];
};

export type ReplicaIndexDependencyChange = {
	readonly kind: 'invalidate';
	readonly dependencies: readonly string[];
};

/** Compiler/runtime semantic evidence carried by one optimistic layer. */
export type ReplicaIndexSemanticChange =
	| ReplicaIndexRecordChange
	| ReplicaIndexRelationshipChange
	| ReplicaIndexDependencyChange;

export type ReplicaIndexCoverage =
	| { readonly kind: 'complete' }
	| { readonly kind: 'unknown' }
	| {
			readonly kind: 'offset';
			readonly offset: number;
			readonly limit?: number;
			readonly returned?: number;
			readonly hasNext?: boolean;
	  }
	| {
			readonly kind: 'cursor';
			readonly after?: ReplicaValue;
			readonly before?: ReplicaValue;
			readonly first?: number;
			readonly last?: number;
			readonly start?: ReplicaValue;
			readonly end?: ReplicaValue;
			readonly hasNext?: boolean;
			readonly hasPrevious?: boolean;
	  };

/** Minimal model identity emitted by a generated operation artifact. */
export type ReplicaModelArtifact = {
	readonly id: string;
	readonly identityFields: readonly string[];
};

export type ReplicaLiteralValue = {
	readonly kind: 'literal';
	readonly value: ReplicaValue;
};

export type ReplicaVariableValue = {
	readonly kind: 'variable';
	readonly name: string;
};

export type ReplicaListValue = {
	readonly kind: 'list';
	readonly items: readonly ReplicaArgumentValue[];
};

export type ReplicaObjectValue = {
	readonly kind: 'object';
	readonly fields: Readonly<Record<string, ReplicaArgumentValue>>;
};

export type ReplicaArgumentValue =
	| ReplicaLiteralValue
	| ReplicaVariableValue
	| ReplicaListValue
	| ReplicaObjectValue;
export type ReplicaArgumentsArtifact = Readonly<Record<string, ReplicaArgumentValue>>;

export type ReplicaVariableScalarInputRef = {
	readonly kind: 'scalar';
	readonly scalar: string;
	readonly codec: string;
	readonly nullable: boolean;
};

export type ReplicaVariableEnumInputRef = {
	readonly kind: 'enum';
	readonly name: string;
	readonly values: readonly string[];
	readonly nullable: boolean;
};

export type ReplicaVariableNamedInputRef = {
	readonly kind: 'input';
	readonly name: string;
	readonly nullable: boolean;
	/** Required for filter inputs; omitted for other named inputs. */
	readonly filterBaseDepth?: number;
};

export type ReplicaVariableListInputRef = {
	readonly kind: 'list';
	readonly nullable: boolean;
	/** Most restrictive list-width cap across every use of this variable. */
	readonly maxItems?: number;
	readonly item: ReplicaVariableInputRef;
};

/** Compiler-owned GraphQL input coercion contract for one operation variable. */
export type ReplicaVariableInputRef =
	| ReplicaVariableScalarInputRef
	| ReplicaVariableEnumInputRef
	| ReplicaVariableNamedInputRef
	| ReplicaVariableListInputRef;

export type ReplicaVariableFilterInputField = {
	readonly field: string;
	readonly scalar: string;
	readonly codec: string;
	/** Record-column nullability; comparison operands follow the GraphQL input grammar. */
	readonly nullable: boolean;
	readonly operators: readonly ReplicaFilterOperator[];
};

export type ReplicaVariableFilterInputTarget =
	| { readonly kind: 'input'; readonly name: string }
	| { readonly kind: 'opaque' };

export type ReplicaVariableFilterInputRelationship = {
	readonly field: string;
	readonly target: ReplicaVariableFilterInputTarget;
};

export type ReplicaVariableFilterInputDefinition = {
	readonly kind: 'filter';
	readonly model: string;
	readonly fields: readonly ReplicaVariableFilterInputField[];
	readonly relationships: readonly ReplicaVariableFilterInputRelationship[];
};

export type ReplicaVariableOrderInputField = {
	readonly field: string;
	readonly scalar: string;
	readonly codec: string;
	readonly nullable: boolean;
};

export type ReplicaVariableOrderInputDefinition = {
	readonly kind: 'order';
	readonly model: string;
	readonly fields: readonly ReplicaVariableOrderInputField[];
	readonly values: readonly string[];
};

export type ReplicaVariableInputDefinition =
	| ReplicaVariableFilterInputDefinition
	| ReplicaVariableOrderInputDefinition;

export type ReplicaVariableCodecLimits = {
	readonly maxDepth: number;
	readonly maxBoolWidth: number;
	readonly maxInList: number;
};

/** Exact variable codec emitted beside a generated operation artifact. */
export type ReplicaVariableCodecArtifact = {
	readonly version: 2;
	readonly limits: ReplicaVariableCodecLimits;
	readonly variables: Readonly<Record<string, ReplicaVariableInputRef>>;
	/** Canonical GraphQL defaults, applied before cache identity or transport. */
	readonly defaults: Readonly<Record<string, ReplicaValue>>;
	readonly inputs: Readonly<Record<string, ReplicaVariableInputDefinition>>;
};

export type ReplicaFilterOperator =
	| '_eq'
	| '_neq'
	| '_gt'
	| '_gte'
	| '_lt'
	| '_lte'
	| '_in'
	| '_nin'
	| '_is_null'
	| '_like'
	| '_ilike'
	| '_icontains'
	| '_contains'
	| '_contained_in'
	| '_has_key';

export type ReplicaFilterFieldArtifact = {
	readonly field: string;
	readonly scalar: string;
	readonly codec: string;
	readonly nullable: boolean;
	readonly operators: readonly ReplicaFilterOperator[];
};

export type ReplicaRelationshipKind =
	| 'has_many'
	| 'belongs_to'
	| 'many_to_many';

export type ReplicaRelationshipKeyMapping =
	| {
			readonly kind: 'direct';
			readonly local: readonly string[];
			readonly remote: readonly string[];
	  }
	| {
			readonly kind: 'through';
			readonly local: readonly string[];
			readonly remote: readonly string[];
			readonly table: string;
			readonly sourceForeignKey: readonly string[];
			readonly targetForeignKey: readonly string[];
	  }
	| {
			readonly kind: 'through_opaque';
			readonly local: readonly string[];
			readonly remote: readonly string[];
			readonly dependency: string;
	  }
	| { readonly kind: 'embedded' };

/**
 * Compiler-owned relationship facts. Task 8 consumes this descriptor to
 * resolve predicates and maintain links without rediscovering schema metadata.
 */
export type ReplicaRelationshipArtifact = {
	readonly field: string;
	readonly targetModel: string;
	readonly kind: ReplicaRelationshipKind;
	readonly keyMapping: ReplicaRelationshipKeyMapping;
	readonly maintenance: 'local' | 'revalidate';
	readonly dependencies: readonly string[];
};

export type ReplicaFilterLiteral =
	| { readonly kind: 'string'; readonly value: string }
	| { readonly kind: 'i64'; readonly value: number }
	| { readonly kind: 'f64'; readonly value: number }
	| { readonly kind: 'bool'; readonly value: boolean }
	| { readonly kind: 'json'; readonly value: ReplicaValue }
	| { readonly kind: 'null' };

/** Manifest-v5 row-policy operand, preserved without ambient claim access. */
export type ReplicaFilterOperand =
	| { readonly kind: 'lit'; readonly value: ReplicaFilterLiteral }
	| {
			readonly kind: 'claim';
			readonly value: { readonly header: string };
	  };

/** Manifest-v5 tagged row-policy expression. */
export type ReplicaFilterExpression =
	| {
			readonly kind: 'and' | 'or';
			readonly value: readonly ReplicaFilterExpression[];
	  }
	| { readonly kind: 'not'; readonly value: ReplicaFilterExpression }
	| {
			readonly kind: 'cmp';
			readonly value: {
				readonly column: string;
				readonly op:
					| 'eq'
					| 'neq'
					| 'gt'
					| 'gte'
					| 'lt'
					| 'lte'
					| 'like'
					| 'ilike'
					| 'contains'
					| 'contained_in'
					| 'has_key';
				readonly rhs: ReplicaFilterOperand;
			};
	  }
	| {
			readonly kind: 'in';
			readonly value: {
				readonly column: string;
				readonly values: readonly ReplicaFilterOperand[];
				readonly negated: boolean;
			};
	  }
	| {
			readonly kind: 'is_null';
			readonly value: {
				readonly column: string;
				readonly is_null: boolean;
			};
	  }
	| {
			readonly kind: 'rel';
			readonly value: {
				readonly field: string;
				readonly predicate: ReplicaFilterExpression;
			};
	  };

export type ReplicaRowPolicyArtifact =
	| { readonly kind: 'unrestricted' }
	| { readonly kind: 'server_only' }
	| {
			readonly kind: 'predicate';
			readonly expression: ReplicaFilterExpression;
	  };

export type ReplicaFilterArtifact = {
	/** Exact literal or variable supplying the operation's `where` input. */
	readonly input?: ReplicaArgumentValue;
	readonly fields: readonly ReplicaFilterFieldArtifact[];
	readonly relationships: readonly ReplicaRelationshipArtifact[];
	readonly rowPolicy: ReplicaRowPolicyArtifact;
};

export type ReplicaOrderFieldArtifact = {
	readonly field: string;
	readonly scalar: string;
	readonly codec: string;
	readonly nullable: boolean;
};

export type ReplicaOrderTieBreakerArtifact = {
	readonly field: string;
	readonly scalar: string;
	readonly codec: string;
	/** Generated identity fields are always non-null. */
	readonly nullable: false;
};

export type ReplicaOrderArtifact = {
	/** Exact literal or variable supplying the operation's `order_by` input. */
	readonly input?: ReplicaArgumentValue;
	readonly fields: readonly ReplicaOrderFieldArtifact[];
	/** Server-appended primary-key fields, in their exact comparison order. */
	readonly tieBreakers: readonly ReplicaOrderTieBreakerArtifact[];
};

export type ReplicaPaginationDisposition = 'local' | 'revalidate';

type ReplicaPaginationPolicies = {
	readonly insert: ReplicaPaginationDisposition;
	readonly delete: ReplicaPaginationDisposition;
	readonly reorder: ReplicaPaginationDisposition;
	readonly stableUpdate: ReplicaPaginationDisposition;
};

export type ReplicaPaginationArtifact = ReplicaPaginationPolicies &
	(
		| { readonly kind: 'complete' | 'offset' }
		| {
				/**
				 * Cursor locality is unavailable until the compiler emits a
				 * versioned proof IR understood by the runtime.
				 */
				readonly kind: 'cursor';
		  }
	);

export type ReplicaCoverageArtifact =
	| { readonly kind: 'complete' }
	| {
			readonly kind: 'offset';
			readonly offsetArgument?: string;
			readonly limitArgument?: string;
			readonly defaultLimit?: number;
			readonly maxLimit?: number;
	  }
	| {
			readonly kind: 'cursor';
			readonly afterArgument?: string;
			readonly beforeArgument?: string;
			readonly firstArgument?: string;
			readonly lastArgument?: string;
	  };

export type ReplicaScalarSelection = {
	readonly kind: 'scalar';
	/** GraphQL response key after aliases are applied. */
	readonly responseKey: string;
	/** Canonical schema/read-model field name. */
	readonly field: string;
	/** Exact scalar codec and nullability emitted by the client compiler. */
	readonly codec: string;
	readonly nullable: boolean;
	/** Wire-only identity fields set this false. */
	readonly expose?: boolean;
};

export type ReplicaSelectionStorage =
	| {
			readonly kind: 'normalized';
			readonly model: string;
			readonly identityFields: readonly string[];
	  }
	| { readonly kind: 'embedded' };

export type ReplicaBranchSemantic =
	| 'relationship'
	| 'aggregate'
	| 'aggregate_fields'
	| 'aggregate_nodes';

/** Recursive compiler-owned object algebra used by manifest v7 artifacts. */
export type ReplicaObjectSelection = {
	readonly typename: string;
	readonly storage: ReplicaSelectionStorage;
	readonly members: readonly ReplicaObjectMember[];
};

type ReplicaObjectBranchBase = {
	readonly kind: 'branch';
	readonly responseKey: string;
	readonly field: string;
	readonly cardinality: 'one' | 'many';
	readonly nullable: boolean;
	readonly arguments?: ReplicaArgumentsArtifact;
	readonly dependencies: readonly string[];
	readonly coverage?: ReplicaCoverageArtifact;
	readonly filter?: ReplicaFilterArtifact;
	readonly order?: ReplicaOrderArtifact;
	readonly pagination?: ReplicaPaginationArtifact;
	readonly selection: ReplicaObjectSelection;
};

export type ReplicaObjectBranch =
	| (ReplicaObjectBranchBase & {
			readonly semantic: 'relationship';
			readonly relationship: ReplicaRelationshipArtifact;
	  })
	| (ReplicaObjectBranchBase & {
			readonly semantic: Exclude<ReplicaBranchSemantic, 'relationship'>;
			readonly relationship?: never;
	  });

export type ReplicaObjectMember = ReplicaScalarSelection | ReplicaObjectBranch;

export type ReplicaRootSelection = {
	readonly responseKey: string;
	readonly field: string;
	readonly cardinality: 'one' | 'many';
	readonly nullable: boolean;
	readonly arguments?: ReplicaArgumentsArtifact;
	readonly dependencies: readonly string[];
	readonly coverage?: ReplicaCoverageArtifact;
	readonly filter?: ReplicaFilterArtifact;
	readonly order?: ReplicaOrderArtifact;
	readonly pagination?: ReplicaPaginationArtifact;
	readonly selection: ReplicaObjectSelection;
};

/**
 * Narrow runtime contract expected from generated code.
 *
 * It deliberately does not mirror the Rust client-manifest JSON. The compiler
 * owns lowering the manifest and GraphQL AST into this executable descriptor.
 */
type ReplicaOperationArtifactBase<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
> = {
	readonly id: string;
	readonly document: string;
	/** Normalized compiler-owned provenance for diagnostics and support tooling. */
	readonly source?: ReplicaOperationSourceLocation;
	readonly roots: readonly ReplicaRootSelection[];
	readonly live?: {
		readonly id: string;
		readonly document: string;
	};
	/** Phantom slots preserve generated result/variables types. */
	readonly __result?: TData;
	readonly __variables?: TVariables;
};

export type ReplicaOperationSourceLocation = {
	readonly path: string;
	readonly line: number;
	readonly column: number;
};

export type ReplicaOperationProtocol = {
	/** Exact generated protocol generation for causal delivery context. */
	readonly protocolHash?: string;
	readonly version: 1;
	/** Opaque service schema fingerprint, compared byte-for-byte. */
	readonly schemaHash: string;
	readonly surface: ReplicaClientSurface;
	/** Opaque operation identity, required to match the artifact id exactly. */
	readonly operation: string;
	/**
	 * Exact descriptor union for the selected client surface. Generated
	 * artifacts repeat this small static contract so every query can validate
	 * scope-bound preset values without depending on command registration.
	 */
	readonly trustedPresets: readonly ReplicaSurfaceTrustedPresetDescriptor[];
};

export type ReplicaSurfaceTrustedPresetDescriptor = {
	readonly name: string;
	readonly codec: DistributedTrustedPresetCodec;
};

/** Exact static client surface selected at code-generation time. */
export type ReplicaClientSurface =
	| {
			readonly kind: 'role';
			readonly name: string;
	  }
	| {
			readonly kind: 'application';
			readonly name: string;
			readonly eligible_roles: readonly string[];
			readonly schema_roles: readonly string[];
	  };

/** Compiler-owned causal artifact. Protocol and variable identity are inseparable. */
export type ReplicaProtocolOperationArtifact<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
> = ReplicaOperationArtifactBase<TData, TVariables> & {
	readonly protocol: ReplicaOperationProtocol;
	/** Exact compiler-owned GraphQL variable coercion and identity contract. */
	readonly variableCodec: ReplicaVariableCodecArtifact;
};

export type ReplicaOperationArtifact<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
> = ReplicaProtocolOperationArtifact<TData, TVariables>;

/** Framework-neutral compiler metadata consumed by placement adapters. */
export type ReplicaIslandMetadata = {
	readonly version: 1;
	readonly id: string;
	readonly operation: string;
	readonly operationHash: string;
	readonly modulePath: string;
	readonly exportName: string;
	readonly source: ReplicaOperationSourceLocation;
	readonly directives: {
		readonly load: boolean;
		readonly live: boolean;
	};
	readonly variableSchema: {
		readonly reference: string;
		readonly codecVersion: number;
		readonly variables: readonly {
			readonly name: string;
			readonly graphqlType: string;
			readonly defaultValue?: ReplicaValue;
		}[];
	};
	readonly liveCoverage: {
		readonly requested: boolean;
		readonly finite: boolean;
		readonly kind: string;
		readonly maxItems?: number;
	};
};

/** One adapter-consumable island plan bound to its executable operation. */
export type ReplicaIslandOperation<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
> = {
	readonly plan: ReplicaIslandMetadata;
	readonly artifact: ReplicaOperationArtifact<TData, TVariables>;
};

export type ReplicaWriteSource = 'network' | 'live' | 'ssr' | 'restore' | 'atomic';

export type ReplicaResultEnvelope<TData = unknown> = {
	readonly data?: TData | null;
	readonly errors?: readonly GqlError[];
	/** Strictly parsed Distributed v1 response metadata. */
	readonly extensions?: GraphqlResponseExtensions;
};

export type ReplicaTransportRequest<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
> = {
	readonly operation: 'query' | 'live';
	readonly operationId: string;
	readonly document: string;
	readonly variables: TVariables;
	readonly artifact: ReplicaOperationArtifact<TData, TVariables>;
	/** Generated surface/schema selector forwarded as GraphQL request extensions. */
	readonly extensions?: Readonly<Record<string, unknown>>;
	/**
	 * Authorization-generation cancellation. Transports must forward this to
	 * their underlying HTTP request when they support cancellation.
	 */
	readonly signal?: AbortSignal;
	/** Latest server-issued cursors for the private generated resume extension. */
	readonly resume?: readonly DistributedLiveCursor[];
};

export type ReplicaLiveObserver<TData = unknown> = {
	next(result: ReplicaResultEnvelope<TData>): void;
	error(error: unknown): void;
	complete(): void;
};

export type ReplicaTransport = {
	fetch<
		TData = Record<string, unknown>,
		TVariables extends GraphqlVariables = GraphqlVariables
	>(
		request: ReplicaTransportRequest<TData, TVariables>
	): Promise<ReplicaResultEnvelope<TData>>;
	subscribe?<
		TData = Record<string, unknown>,
		TVariables extends GraphqlVariables = GraphqlVariables
	>(
		request: ReplicaTransportRequest<TData, TVariables>,
		observer: ReplicaLiveObserver<TData>
	): () => void;
};

export type ReplicaStatus = 'loading' | 'ready' | 'stale' | 'error';
export type ReplicaLiveState = 'off' | 'connecting' | 'active' | 'error';

/** The exact runtime shape while some selected cache fields are still absent. */
export type ReplicaSparse<T> = T extends readonly (infer TItem)[]
	? readonly ReplicaSparse<TItem>[]
	: T extends object
		? { readonly [K in keyof T]?: ReplicaSparse<T[K]> }
		: T;

type ReplicaSnapshotState = {
	readonly fetching: boolean;
	readonly errors: readonly GqlError[];
	readonly live: ReplicaLiveState;
};

/**
 * A structurally complete snapshot carries the generated result type even
 * while its freshness is stale or a refresh failed. Only snapshots missing
 * selected fields or index structure expose a deep sparse result.
 */
export type ReplicaSnapshot<TData> = ReplicaSnapshotState &
	(
		| ({
				readonly data: TData;
				readonly complete: true;
		  } & (
				| {
						readonly status: 'ready';
						readonly stale: false;
				  }
				| {
						readonly status: 'stale';
						readonly stale: true;
				  }
				| {
						readonly status: 'error';
						readonly stale: boolean;
				  }
		  ))
		| {
				readonly data: ReplicaSparse<TData>;
				readonly status: 'loading';
				readonly stale: false;
				readonly complete: false;
		  }
		| {
				readonly data: ReplicaSparse<TData>;
				readonly status: 'stale';
				readonly stale: true;
				readonly complete: false;
		  }
		| {
				readonly data: ReplicaSparse<TData>;
				readonly status: 'error';
				readonly stale: boolean;
				readonly complete: false;
		  }
	);

export type ReplicaWatch<TData> = {
	get(): ReplicaSnapshot<TData>;
	subscribe(listener: (snapshot: ReplicaSnapshot<TData>) => void): () => void;
	refresh(): Promise<void>;
	destroy(): void;
};

export type WatchReplicaOptions = {
	readonly live?: boolean;
};

export type DistributedReplicaOptions = {
	readonly transport?: ReplicaTransport;
	readonly onObserverError?: (error: AggregateError) => void;
	/**
	 * Runs after the old generation is fenced and before its transports/state
	 * are purged. Framework adapters use this to release generation-owned views.
	 */
	readonly onAuthorizationGenerationDispose?: () => void;
	/** Opt-in framework-neutral diagnostics; absent in production by default. */
	readonly diagnostics?: ReplicaDiagnosticsSink;
};

/** Server-issued authorization/cache namespace. Client-decoded claims never create one. */
export type ReplicaAuthoritativeScope = {
	readonly protocolVersion: 1;
	readonly schemaHash: string;
	readonly authorizationGeneration: string;
	readonly cacheScope: string;
};

/**
 * Opaque, engine-independent SSR transfer owned by Distributed.
 *
 * `payload` is intentionally not a public cache-engine schema. Applications
 * pass the value back to `hydrate` without interpreting it.
 */
export type ReplicaDehydratedState = {
	readonly version: 1;
	readonly scope: ReplicaAuthoritativeScope;
	readonly payload: unknown;
};

export type ReplicaRecordInspection = {
	readonly key: string;
	readonly revision: string;
	readonly incarnation: string;
	readonly presentFields: readonly string[];
};

export type ReplicaIndexInspection = {
	readonly key: string;
	readonly revision: string;
	readonly staleRevision?: string;
	readonly records: readonly string[];
	readonly complete: boolean;
	readonly field: string;
	readonly parent?: string;
	readonly arguments: Readonly<Record<string, ReplicaValue>>;
	readonly coverage: ReplicaIndexCoverage;
	readonly dependencies: readonly string[];
	readonly staleReason?: string;
	readonly nullValue: boolean;
};

export type ReplicaIdentity = ReplicaValue | readonly ReplicaValue[];

/** Exact relationship identity used to target authoritative query revalidation. */
export type ReplicaRevalidationRelationship = {
	readonly sourceModel: string;
	readonly field: string;
	readonly targetModel: string;
};

/**
 * Compiler-owned inventory used to find active operations affected by a
 * command. An empty inventory conservatively targets every active operation.
 */
export type ReplicaRevalidationPlan = {
	readonly dependencies: readonly string[];
	readonly models: readonly string[];
	readonly relationships: readonly ReplicaRevalidationRelationship[];
};

export type ReplicaIndexTarget = {
	readonly parent?: string;
	readonly field: string;
	readonly arguments?: Readonly<Record<string, ReplicaValue>>;
	readonly coverage?: ReplicaIndexCoverage;
	readonly dependencies?: readonly string[];
	readonly complete?: boolean;
	readonly staleReason?: string;
	readonly nullValue?: boolean;
};

export type ReplicaRecordPatch = {
	readonly fields?: Readonly<Record<string, ReplicaValue>>;
	readonly unset?: readonly string[];
	readonly ifPresent?: boolean;
	readonly links?: Readonly<Record<string, string | readonly string[] | null>>;
};

export interface ReplicaOptimisticWriter {
	writeRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		patch: ReplicaRecordPatch
	): void;
	tombstoneRecord(model: ReplicaModelArtifact, identity: ReplicaIdentity): void;
	writeIndex(target: ReplicaIndexTarget, records: readonly string[]): void;
	deleteIndex(target: ReplicaIndexTarget): void;
}

export interface ReplicaBaseWriter {
	writeRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		revision: ReplicaRevision,
		patch: ReplicaRecordPatch & { readonly incarnation?: ReplicaRevision }
	): boolean;
	tombstoneRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		revision: ReplicaRevision
	): boolean;
	writeIndex(
		target: ReplicaIndexTarget,
		records: readonly string[],
		revision: ReplicaRevision
	): boolean;
	deleteIndex(target: ReplicaIndexTarget, revision: ReplicaRevision): boolean;
}

export interface DistributedReplica {
	/** The current server-issued scope, absent until authoritative evidence arrives. */
	readonly scope: ReplicaAuthoritativeScope | undefined;
	/** Monotonic local fence for authorization changes and server scope changes. */
	readonly authorizationGeneration: number;
	read<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables
	): ReplicaSnapshot<TData>;
	watch<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		options?: WatchReplicaOptions
	): ReplicaWatch<TData>;
	writeResult<TData, TVariables extends GraphqlVariables>(
		artifact: ReplicaOperationArtifact<TData, TVariables>,
		variables: TVariables,
		envelope: ReplicaResultEnvelope<TData>,
		source: ReplicaWriteSource
	): void;
	/**
	 * Force deduplicated authoritative reads for active operations selected by a
	 * compiler-owned dependency/model/relationship inventory.
	 */
	revalidate(plan: ReplicaRevalidationPlan): Promise<void>;
	/**
	 * Proactively closes the current authorization generation.
	 *
	 * Token/session changes may call this before the next server response, but
	 * they cannot choose or restore a cache scope.
	 */
	invalidateAuthorization(): void;
	/** Serialize confirmed state reachable from operations rendered by this replica. */
	dehydrate(): ReplicaDehydratedState;
	/**
	 * Restore a server-rendered payload against the independently authoritative
	 * scope for the current SSR response. Returns false on scope/schema mismatch
	 * and never partially restores malformed state. Persisted state must wait for
	 * a fresh server scope and pass that scope here.
	 *
	 * Cold clients (no active scope) replace from the seed. Warm same-scope
	 * re-hydrate merges seed records/indexes and retains confirmed keys the seed
	 * omitted—soft navigation must not wipe session cache because a route only
	 * dehydrated its page subset. Auth/scope change still purges.
	 */
	hydrate(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope
	): boolean;
	/**
	 * Validate a fresh transfer against independent server authority and the
	 * exact active scope, then restart query/live transports without applying
	 * the transfer's potentially older data. Preserves command/freshness state.
	 * Returns false without mutating state when validation fails.
	 */
	reauthorize(
		state: ReplicaDehydratedState,
		authoritativeScope: ReplicaAuthoritativeScope
	): boolean;
	createOptimisticLayer(
		id: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges?: readonly ReplicaIndexSemanticChange[]
	): void;
	markOptimisticLayerAccepted(
		id: string,
		receipt?: DistributedCommandMetadata
	): boolean;
	confirmOptimisticLayer<T>(
		id: string,
		update: (writer: ReplicaBaseWriter) => T
	): T;
	rejectOptimisticLayer(id: string): boolean;
	tombstoneRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity,
		revision: ReplicaRevision
	): boolean;
	markIndexStale(target: ReplicaIndexTarget, reason: string): boolean;
	retainRecord(model: ReplicaModelArtifact, identity: ReplicaIdentity): void;
	releaseRecord(model: ReplicaModelArtifact, identity: ReplicaIdentity): void;
	gc(): readonly string[];
	inspectRecord(
		model: ReplicaModelArtifact,
		identity: ReplicaIdentity
	): ReplicaRecordInspection | undefined;
	inspectIndex(target: ReplicaIndexTarget): ReplicaIndexInspection | undefined;
}
