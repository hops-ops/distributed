import type { ReplicaValue } from '../types.js';

export const PROJECTION_DELTA_WIRE_VERSION = 1 as const;
export const COMMAND_PROJECTION_METADATA_WIRE_VERSION = 1 as const;
export const COMMAND_PROJECTION_ARTIFACT_VERSION = 2 as const;
export const PROJECTION_PROGRAM_VERSION = 2 as const;
export const PROJECTION_PROGRAM_IR_VERSION = 1 as const;
export const PROJECTION_OPERATION_SEMANTICS_VERSION = 1 as const;

export type ProjectionDeltaSurface =
	| Readonly<{ kind: 'role'; name: string }>
	| Readonly<{
			kind: 'application';
			name: string;
			eligible_roles: readonly string[];
			schema_roles: readonly string[];
		}>;

export type ProjectionDeltaIdentity = Readonly<{
	manifest_version: 2;
	client_protocol_version: 1;
	surface: ProjectionDeltaSurface;
	schema_fingerprint: string;
	protocol_fingerprint: string;
	authorization_generation: string;
	cache_scope_token: string;
	command_causation_id: string;
}>;

export type ProjectionDeltaProjectionIdentity = Readonly<{
	program_id: string;
	binding_id: string;
	program_ir_version: 1;
	operation_semantics_version: 1;
}>;

export type ProjectionDeltaOccurrence = Readonly<{
	causation_id: string;
	ordinal: number;
	occurrence_id: string;
}>;

export type ProjectionDeltaValue =
	| Readonly<{ type: 'null' }>
	| Readonly<{ type: 'boolean'; value: boolean }>
	| Readonly<{ type: 'i64' | 'u64' | 'f64' | 'string'; value: string }>
	| Readonly<{
			type: 'enum';
			value: Readonly<{ enum_type: string; variant: string }>;
	  }>
	| Readonly<{ type: 'list'; value: readonly ProjectionDeltaValue[] }>
	| Readonly<{ type: 'object'; value: readonly ProjectionDeltaField[] }>;

export type ProjectionDeltaField = Readonly<{
	field: string;
	value: ProjectionDeltaValue;
}>;

export type ProjectionDeltaKeyField = Readonly<{
	ordinal: number;
	field: string;
	value: ProjectionDeltaValue;
}>;

export type ProjectionDeltaPartition =
	| Readonly<{ kind: 'unit' }>
	| Readonly<{ kind: 'opaque'; token: string }>;

export type ProjectionDeltaScope = Readonly<{
	partition: ProjectionDeltaPartition;
	model: string;
	key: readonly ProjectionDeltaKeyField[];
}>;

export type ProjectionDeltaMutation =
	| Readonly<{
			op: 'upsert';
			scope: ProjectionDeltaScope;
			fields: readonly ProjectionDeltaField[];
			replace: readonly string[];
	  }>
	| Readonly<{
			op: 'patch';
			scope: ProjectionDeltaScope;
			set: readonly ProjectionDeltaField[];
			unset: readonly string[];
			if_present: true;
	  }>
	| Readonly<{ op: 'delete'; scope: ProjectionDeltaScope }>
	| Readonly<{
			op: 'link' | 'unlink';
			relationship: string;
			source: ProjectionDeltaScope;
			target: ProjectionDeltaScope;
	  }>
	| Readonly<{
			op: 'invalidate_model';
			partition?: ProjectionDeltaPartition;
			model: string;
	  }>
	| Readonly<{
			op: 'invalidate_relationship';
			relationship: string;
			source: ProjectionDeltaScope;
	  }>;

export type ProjectionDeltaOperation = Readonly<{
	occurrence_ordinal: number;
	projection_refs: readonly number[];
	mutation: ProjectionDeltaMutation;
}>;

export type ProjectionDeltaRecoveryTarget =
	| Readonly<{ kind: 'record'; scope: ProjectionDeltaScope }>
	| Readonly<{
			kind: 'relationship';
			relationship: string;
			source: ProjectionDeltaScope;
	  }>
	| Readonly<{
			kind: 'model';
			partition?: ProjectionDeltaPartition;
			model: string;
	  }>;

export type ProjectionDeltaRecovery = Readonly<{
	occurrence_ordinal: number;
	projection_refs: readonly number[];
	condition: 'always' | 'if_record_missing';
	target: ProjectionDeltaRecoveryTarget;
}>;

export type ProjectionDelta = Readonly<{
	wire_version: 1;
	identity: ProjectionDeltaIdentity;
	projections: readonly ProjectionDeltaProjectionIdentity[];
	occurrences: readonly ProjectionDeltaOccurrence[];
	operations: readonly ProjectionDeltaOperation[];
	recoveries: readonly ProjectionDeltaRecovery[];
}>;

export type CommandProjectionObligation = Readonly<{
	projectionRef: number;
	model: string;
	scopeToken: string;
}>;

/**
 * Opaque server-authenticated deployment identity for historical lifecycle
 * checks. Clients validate only its bounded wire shape and never interpret it
 * as authority to apply an old-scope delta.
 */
export type CommandProjectionLifecycleProof = Readonly<{
	projectionRef: number;
	token: string;
}>;

export type CommandProjectionMetadata = Readonly<{
	wireVersion: 1;
	issuedAtUnixMs: number;
	expiresAtUnixMs: number;
	delta: ProjectionDelta;
	lifecycleProofs: readonly CommandProjectionLifecycleProof[];
	obligations: readonly CommandProjectionObligation[];
	revalidate: boolean;
}>;

export type ProjectionPreviewValue =
	| Readonly<{ kind: 'input' | 'generated_default'; path: readonly string[] }>
	| Readonly<{ kind: 'trusted_preset'; name: string; codec: string }>
	| Readonly<{ kind: 'constant'; value: ProjectionDeltaValue }>
	| Readonly<{ kind: 'null' }>
	| Readonly<{ kind: 'list'; values: readonly ProjectionPreviewValue[] }>
	| Readonly<{
			kind: 'object';
			fields: readonly Readonly<{
				name: string;
				value: ProjectionPreviewValue;
			}>[];
	  }>
	| Readonly<{
			kind: 'transform';
			transform: 'string_concat' | 'first_present';
			arguments: readonly ProjectionPreviewValue[];
	  }>;

export type ProjectionPreviewPartition =
	| Readonly<{ kind: 'unit' }>
	| Readonly<{
			kind: 'expression';
			expression: ProjectionPreviewValue;
			requires: 'current_cache_partition';
	  }>;

export type ProjectionPreviewScope = Readonly<{
	partition: ProjectionPreviewPartition;
	model: string;
	key: readonly Readonly<{
		ordinal: number;
		field: string;
		value: ProjectionPreviewValue;
	}>[];
}>;

export type ProjectionPreviewField = Readonly<{
	field: string;
	value: ProjectionPreviewValue;
}>;

export type ProjectionCapabilityPartition =
	| Readonly<{ kind: 'unit' }>
	| Readonly<{ kind: 'opaque'; expression_fingerprint: string }>;

export type ProjectionCapabilityMutation =
	| Readonly<{
			kind: 'record';
			model: string;
			key: readonly string[];
			fields: readonly string[];
			replace: readonly string[];
			upsert: boolean;
			patch: boolean;
			delete: boolean;
	  }>
	| Readonly<{
			kind: 'relationship';
			relationship: string;
			source_model: string;
			source_key: readonly string[];
			target_model: string;
			target_key: readonly string[];
			link: boolean;
			unlink: boolean;
	  }>
	| Readonly<{ kind: 'model'; model: string }>;

export type ProjectionCapabilityArm = Readonly<{
	event: Readonly<{ id: string; name: string; version: number }>;
	projection_ref: number;
	arm: string;
	partition: ProjectionCapabilityPartition;
	mutations: readonly ProjectionCapabilityMutation[];
}>;

/**
 * Pure reduce: load the known cache row for `scope`, run a named pure function
 * with resolved `args`, and patch `assign` fields from the result.
 *
 * Fail-closed: missing row or pure failure → no invent (same as patch ifPresent).
 */
export type ProjectionPreviewPureReduce = Readonly<{
	fn: string;
	/** App lib-relative module (client compiler); optional at runtime. */
	clientModule?: string;
	/** Named export (client compiler); optional at runtime. */
	clientExport?: string;
	scope: ProjectionPreviewScope;
	args: readonly Readonly<{
		name: string;
		value: ProjectionPreviewValue;
	}>[];
	/** Fields taken from the pure result and written onto the known record. */
	assign: readonly string[];
	occurrence_ordinal: number;
	projection_refs: readonly number[];
}>;

export type ProjectionPreviewMutation =
	| Readonly<{
			op: 'upsert';
			scope: ProjectionPreviewScope;
			fields: readonly ProjectionPreviewField[];
			replace: readonly string[];
	  }>
	| Readonly<{
			op: 'patch';
			scope: ProjectionPreviewScope;
			set: readonly ProjectionPreviewField[];
			unset?: readonly string[];
			if_present: true;
	  }>
	| Readonly<{ op: 'delete'; scope: ProjectionPreviewScope }>
	| Readonly<{
			op: 'link' | 'unlink';
			relationship: string;
			source: ProjectionPreviewScope;
			target: ProjectionPreviewScope;
	  }>
	| Readonly<{
			op: 'invalidate_model';
			partition?: ProjectionPreviewPartition;
			model: string;
	  }>
	| Readonly<{
			op: 'invalidate_relationship';
			relationship: string;
			source: ProjectionPreviewScope;
	  }>;

export type ProjectionPreviewRecoveryTarget =
	| Readonly<{ kind: 'record'; scope: ProjectionPreviewScope }>
	| Readonly<{
			kind: 'relationship';
			relationship: string;
			source: ProjectionPreviewScope;
	  }>
	| Readonly<{
			kind: 'model';
			partition?: ProjectionPreviewPartition;
			model: string;
	  }>;

export type ReplicaCommandProjection = Readonly<{
	version: 2;
	deltaWireVersion: 1;
	projectionProgramVersion: 2;
	operationSemanticsVersion: 1;
	projections: readonly Readonly<{
		programId: string;
		bindingId: string;
		programIrVersion: 1;
		operationSemanticsVersion: 1;
	}>[];
	eventSet: readonly Readonly<{
		id: string;
		name: string;
		version: number;
	}>[];
	capabilities: Readonly<{
		version: 1;
		arms: readonly ProjectionCapabilityArm[];
	}>;
	preview: Readonly<{
		version: 1;
		occurrences: readonly Readonly<{
			ordinal: number;
			event: Readonly<{ id: string; name: string; version: number }>;
		}>[];
		operations: readonly Readonly<{
			occurrence_ordinal: number;
			projection_refs: readonly number[];
			mutation: ProjectionPreviewMutation;
		}>[];
		recoveries: readonly Readonly<{
			occurrence_ordinal: number;
			projection_refs: readonly number[];
			condition: 'always' | 'if_record_missing';
			target: ProjectionPreviewRecoveryTarget;
		}>[];
	}>;
	/**
	 * Optional pure reducers over known cache rows. Expanded at optimistic
	 * apply time (needs the live record); not ordinary preview expressions.
	 */
	pureReduces?: readonly ProjectionPreviewPureReduce[];
	fallback: 'revalidate';
}>;

export type PreparedProjectionOperation =
	| Readonly<{
			kind: 'upsert';
			scope: PreparedProjectionScope;
			fields: Readonly<Record<string, ReplicaValue>>;
			replace: readonly string[];
	  }>
	| Readonly<{
			kind: 'patch';
			scope: PreparedProjectionScope;
			fields: Readonly<Record<string, ReplicaValue>>;
			unset: readonly string[];
			ifPresent: true;
	  }>
	| Readonly<{ kind: 'delete'; scope: PreparedProjectionScope }>
	| Readonly<{
			kind: 'link' | 'unlink';
			relationship: string;
			source: PreparedProjectionScope;
			target: PreparedProjectionScope;
	  }>
	| Readonly<{
			kind: 'invalidate_model';
			model: string;
	  }>
	| Readonly<{
			kind: 'invalidate_relationship';
			relationship: string;
			source: PreparedProjectionScope;
	  }>
	| Readonly<{
			kind: 'reduce_known_record';
			fn: string;
			scope: PreparedProjectionScope;
			args: Readonly<Record<string, ReplicaValue>>;
			assign: readonly string[];
	  }>;

export type PreparedProjectionScope = Readonly<{
	model: string;
	key: readonly Readonly<{ field: string; value: ReplicaValue }>[];
}>;

/** Named pure function: known record + resolved args → fields to patch. */
export type ReplicaPureFunction = (
	record: Readonly<Record<string, ReplicaValue>>,
	args: Readonly<Record<string, ReplicaValue>>
) => Readonly<Record<string, ReplicaValue>> | null;

export type PreparedCommandProjection = Readonly<{
	contract: ReplicaCommandProjection;
	preview: readonly PreparedProjectionOperation[];
	revalidate: boolean;
}>;

export type AppliedProjectionDelta = Readonly<{
	canonical: string;
	revalidate: boolean;
	models: readonly string[];
	relationships: readonly Readonly<{
		sourceModel: string;
		field: string;
		targetModel: string;
	}>[];
}>;
