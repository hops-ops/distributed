/**
 * Versioned framework metadata carried in GraphQL's top-level
 * `extensions.distributed` response envelope.
 *
 * Domain result objects never contain these values. Hidden identities and
 * positions remain opaque strings: the JavaScript client compares or returns
 * them, but never parses them as numbers or reconstructs server scopes.
 */
import {
	parseCommandProjectionMetadata,
	type CommandProjectionMetadata
} from './replica/projection-delta/index.js';
import {
	parseDistributedGenerationEnvelope,
	type DistributedGenerationEnvelope
} from './generation.js';

/** The only Distributed GraphQL protocol version understood by this package. */
export const DISTRIBUTED_PROTOCOL_VERSION = 1 as const;

declare const opaqueDistributedString: unique symbol;
declare const distributedDecimalString: unique symbol;

/** Server-owned identity/token whose contents must not be interpreted by clients. */
export type DistributedOpaqueString = string & {
	readonly [opaqueDistributedString]: true;
};

/** Canonical unsigned u64 decimal carried without JavaScript numeric coercion. */
export type DistributedDecimalString = string & {
	readonly [distributedDecimalString]: true;
};

export type DistributedCommandState =
	| 'in_progress'
	| 'succeeded'
	| 'succeeded_pending_projection'
	| 'atomic'
	| 'rejected'
	| 'projection_failed'
	| 'expired'
	| 'unknown';

export type DistributedCommandConsistency =
	| 'succeeded'
	| 'eventual'
	| 'atomic';

/** Current-scope handling for authenticated historical projector evidence. */
export type DistributedProjectionDisposition = 'revalidate';

/** Closed wire codecs for server-derived, client-visible trusted presets. */
export type DistributedTrustedPresetCodec =
	| 'string'
	| 'string_unvalidated_timestamp'
	| 'base64'
	| 'boolean'
	| 'int32'
	| 'float64'
	| 'json_number_precision_limited'
	| 'json';

/** JSON value accepted by the Distributed protocol parser. */
export type DistributedProtocolValue =
	| null
	| boolean
	| number
	| string
	| readonly DistributedProtocolValue[]
	| { readonly [key: string]: DistributedProtocolValue };

/**
 * One server-derived value valid only under its containing envelope's exact
 * authoritative cache scope.
 */
export type DistributedTrustedPreset = Readonly<{
	name: string;
	codec: DistributedTrustedPresetCodec;
	value: DistributedProtocolValue;
}>;

/** One finite, server-resolved projection obligation for a command. */
export type DistributedProjectionExpectation = Readonly<
	Record<string, unknown> & {
		projection: string;
		model: string;
		scopeToken: DistributedOpaqueString;
	}
>;

/** Comparable record evidence within one exact opaque record scope. */
export type DistributedRecordRevision = Readonly<
	Record<string, unknown> & {
		path?: readonly string[];
		model: string;
		scopeToken: DistributedOpaqueString;
		incarnation: DistributedDecimalString;
		revision: DistributedDecimalString;
		tombstone: boolean;
	}
>;

/** Exact observation of one command causation in one projection obligation. */
export type DistributedProjectionObservation = Readonly<
	Record<string, unknown> & {
		causationId: DistributedOpaqueString;
		projection: string;
		model: string;
		scopeToken: DistributedOpaqueString;
	}
>;

/** Opaque resumable position for one named projection scope. */
export type DistributedLiveCursor = Readonly<
	Record<string, unknown> & {
		projection: string;
		position: DistributedDecimalString;
		token: DistributedOpaqueString;
	}
>;

/** Comparable checkpoint for one exact query-index projection member. */
export type DistributedIndexRevision = Readonly<
	Record<string, unknown> & {
		projection: string;
		scopeToken: DistributedOpaqueString;
		position: DistributedDecimalString;
		resume?: DistributedLiveCursor;
	}
>;

/**
 * Record and causal-index evidence for one exact operation instance.
 *
 * The GraphQL payload itself is the authoritative query snapshot. These flags
 * describe whether the server could additionally prove every normalized record
 * clock and expose a safely comparable projection vector. Row-filtered
 * authorization commonly makes `indexesComparable` false without making the
 * authorized payload partial.
 */
export type DistributedQuerySnapshot = Readonly<
	Record<string, unknown> & {
		scopeToken: DistributedOpaqueString;
		recordsComplete: boolean;
		indexesComparable: boolean;
		records: readonly DistributedRecordRevision[];
		indexes: readonly DistributedIndexRevision[];
		observations: readonly DistributedProjectionObservation[];
	}
>;

/** Per-frame delivery mode, reset decision, and resumable cursors. */
export type DistributedLiveMetadata = Readonly<
	Record<string, unknown> & {
		mode: 'snapshot' | 'resumable';
		reset: boolean;
		cursors: readonly DistributedLiveCursor[];
	}
>;

/** Private GraphQL request extension consumed by generated live operations. */
export type DistributedLiveResumeExtensions = Readonly<{
	distributed: Readonly<{
		resume: Readonly<{
			cursors: readonly DistributedLiveCursor[];
		}>;
	}>;
}>;

/** Durable receipt/status metadata for one idempotent command identity. */
export type DistributedCommandMetadata = Readonly<
	Record<string, unknown> & {
		commandId: DistributedOpaqueString;
		causationId: DistributedOpaqueString;
		state: DistributedCommandState;
		consistency: DistributedCommandConsistency;
		projectionDisposition?: DistributedProjectionDisposition;
		expects: readonly DistributedProjectionExpectation[];
		/** Defaults to an empty array when omitted by the compact wire format. */
		observations: readonly DistributedProjectionObservation[];
		/** Defaults to an empty array when omitted by the compact wire format. */
		records: readonly DistributedRecordRevision[];
		/** Exact modeled projection delta and opaque observation obligations. */
		projection?: CommandProjectionMetadata;
	}
>;

/** Canonical contents of top-level `extensions.distributed`. */
export type DistributedProtocolEnvelope = Readonly<
	Record<string, unknown> & {
		protocolVersion: typeof DISTRIBUTED_PROTOCOL_VERSION;
		schemaHash: string;
		authorizationGeneration: string;
		cacheScope: DistributedOpaqueString;
		generation?: DistributedGenerationEnvelope;
		operation?: string;
		command?: DistributedCommandMetadata;
		snapshot?: DistributedQuerySnapshot;
		live?: DistributedLiveMetadata;
		/** Defaults to an empty array when omitted by the compact wire format. */
		trustedPresets: readonly DistributedTrustedPreset[];
	}
>;

/** GraphQL top-level extensions with a validated Distributed envelope. */
export type GraphqlResponseExtensions = Readonly<
	Record<string, unknown> & {
		distributed?: DistributedProtocolEnvelope;
	}
>;

export type DistributedProtocolErrorCode =
	| 'DISTRIBUTED_PROTOCOL_INVALID'
	| 'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED';

/** Safe parse failure that reports structure, never hidden server values. */
export class DistributedProtocolError extends Error {
	readonly code: DistributedProtocolErrorCode;
	readonly path: string;

	constructor(code: DistributedProtocolErrorCode, path: string) {
		super(
			code === 'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED'
				? 'Unsupported Distributed GraphQL protocol version'
				: `Invalid Distributed GraphQL protocol envelope at ${path}`
		);
		this.name = 'DistributedProtocolError';
		this.code = code;
		this.path = path;
	}
}

const COMMAND_STATES = new Set<DistributedCommandState>([
	'in_progress',
	'succeeded',
	'succeeded_pending_projection',
	'atomic',
	'rejected',
	'projection_failed',
	'expired',
	'unknown'
]);

const COMMAND_CONSISTENCIES = new Set<DistributedCommandConsistency>([
	'succeeded',
	'eventual',
	'atomic'
]);

const MAX_PUBLIC_NAME_LENGTH = 512;
const MAX_OPAQUE_STRING_LENGTH = 16_384;
const MAX_EVIDENCE_ITEMS = 4_096;
const MAX_LIVE_RESUME_CURSORS = 64;
const MAX_PATH_SEGMENTS = 256;
const MAX_TRUSTED_PRESETS = 4_096;
const MAX_TRUSTED_PRESET_NAME_LENGTH = 128;
const MAX_TRUSTED_PRESET_VALUE_DEPTH = 64;
const MAX_UNSIGNED_64 = '18446744073709551615';
const MAX_SAFE_INTEGER = 9_007_199_254_740_991;
const CANONICAL_BASE64 =
	/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/**
 * Parse GraphQL's optional top-level `extensions` object.
 *
 * Non-Distributed extension keys are preserved. When `distributed` is present,
 * its required version/scope/schema fields and known receipt/evidence fields are
 * validated. A malformed or incompatible envelope throws
 * {@link DistributedProtocolError}; transports convert that into a fail-closed
 * GraphQL result/error rather than exposing the accompanying data as trusted.
 */
export function parseGraphqlResponseExtensions(
	value: unknown
): GraphqlResponseExtensions | undefined {
	if (value === undefined) return undefined;
	const extensions = record(value, 'extensions');
	if (extensions.distributed === undefined) {
		return Object.freeze({ ...extensions });
	}

	return Object.freeze({
		...extensions,
		distributed: parseDistributedProtocolEnvelope(extensions.distributed)
	});
}

/** Parse one `extensions.distributed` value independently of a transport. */
export function parseDistributedProtocolEnvelope(
	value: unknown
): DistributedProtocolEnvelope {
	const envelope = record(value, 'extensions.distributed');
	if (envelope.protocolVersion !== DISTRIBUTED_PROTOCOL_VERSION) {
		if (
			typeof envelope.protocolVersion === 'number' &&
			Number.isInteger(envelope.protocolVersion)
		) {
			throw new DistributedProtocolError(
				'DISTRIBUTED_PROTOCOL_VERSION_UNSUPPORTED',
				'extensions.distributed.protocolVersion'
			);
		}
		invalid('extensions.distributed.protocolVersion');
	}

	const schemaHash = publicString(
		envelope.schemaHash,
		'extensions.distributed.schemaHash'
	);
	const authorizationGeneration = publicString(
		envelope.authorizationGeneration,
		'extensions.distributed.authorizationGeneration'
	);
	const cacheScope = opaqueString(
		envelope.cacheScope,
		'extensions.distributed.cacheScope'
	);
	const generation =
		envelope.generation === undefined
			? undefined
			: parseDistributedGenerationEnvelope(
					envelope.generation,
					'extensions.distributed.generation'
				);
	const operation =
		envelope.operation === undefined
			? undefined
			: publicString(
					envelope.operation,
					'extensions.distributed.operation'
				);
	const command =
		envelope.command === undefined
			? undefined
			: parseCommand(envelope.command);
	const snapshot =
		envelope.snapshot === undefined
			? undefined
			: parseSnapshot(envelope.snapshot);
	const live =
		envelope.live === undefined
			? undefined
			: parseLive(envelope.live);
	validateLiveSnapshot(snapshot, live);
	const trustedPresets = parseDistributedTrustedPresetInventory(
		envelope.trustedPresets
	);

	return Object.freeze({
		...envelope,
		protocolVersion: DISTRIBUTED_PROTOCOL_VERSION,
		schemaHash,
		authorizationGeneration,
		cacheScope,
		...(generation === undefined ? {} : { generation }),
		...(operation === undefined ? {} : { operation }),
		...(command === undefined ? {} : { command }),
		...(snapshot === undefined ? {} : { snapshot }),
		...(live === undefined ? {} : { live }),
		trustedPresets
	}) as DistributedProtocolEnvelope;
}

/**
 * Parse and deeply freeze one scope-wide trusted-preset value inventory.
 *
 * This is exported for the framework-neutral replica's internal command seam.
 * Applications receive the same values through
 * {@link parseDistributedProtocolEnvelope}; they must never supply this
 * inventory as command authority.
 *
 * @internal
 */
export function parseDistributedTrustedPresetInventory(
	value: unknown,
	path = 'extensions.distributed.trustedPresets'
): readonly DistributedTrustedPreset[] {
	if (value === undefined) return Object.freeze([]);
	if (!Array.isArray(value) || value.length > MAX_TRUSTED_PRESETS) {
		invalid(path);
	}
	const names = new Set<string>();
	const presets = value.map((candidate, index) => {
		const itemPath = `${path}[${index}]`;
		const item = record(candidate, itemPath);
		const name = trustedPresetName(item.name, `${itemPath}.name`);
		if (names.has(name)) invalid(`${itemPath}.name`);
		names.add(name);
		const codec = trustedPresetCodec(item.codec, `${itemPath}.codec`);
		return Object.freeze({
			name,
			codec,
			value: parseTrustedPresetValue(
				item.value,
				codec,
				`${itemPath}.value`
			)
		});
	});
	return Object.freeze(presets);
}

/**
 * Build the private request extension for a generated live resume.
 *
 * The server remains authoritative and verifies every token. This client-side
 * check only prevents malformed, duplicate, or unbounded state from leaving
 * the replica.
 *
 * @internal
 */
export function distributedLiveResumeExtensions(
	value: readonly DistributedLiveCursor[]
): DistributedLiveResumeExtensions {
	if (
		!Array.isArray(value) ||
		value.length === 0 ||
		value.length > MAX_LIVE_RESUME_CURSORS
	) {
		invalid('request.extensions.distributed.resume.cursors');
	}
	const cursors = Object.freeze(
		value.map((cursor, index) =>
			parseLiveCursor(
				cursor,
				`request.extensions.distributed.resume.cursors[${index}]`
			)
		)
	);
	assertUnique(
		cursors.map((cursor) => cursor.projection),
		'request.extensions.distributed.resume.cursors'
	);
	return Object.freeze({
		distributed: Object.freeze({
			resume: Object.freeze({ cursors })
		})
	});
}

function parseCommand(value: unknown): DistributedCommandMetadata {
	const command = record(value, 'extensions.distributed.command');
	const state = enumString(
		command.state,
		COMMAND_STATES,
		'extensions.distributed.command.state'
	);
	const consistency = enumString(
		command.consistency,
		COMMAND_CONSISTENCIES,
		'extensions.distributed.command.consistency'
	);
	if (!Array.isArray(command.expects)) {
		invalid('extensions.distributed.command.expects');
	}
	if (command.expects.length > MAX_EVIDENCE_ITEMS) {
		invalid('extensions.distributed.command.expects');
	}

	const expects = command.expects.map((expectation, index) => {
		const path = `extensions.distributed.command.expects[${index}]`;
		const item = record(expectation, path);
		return Object.freeze({
			...item,
			projection: publicString(item.projection, `${path}.projection`),
			model: publicString(item.model, `${path}.model`),
			scopeToken: opaqueString(item.scopeToken, `${path}.scopeToken`)
		}) as DistributedProjectionExpectation;
	});
	const observations = parseOptionalEvidenceArray(
		command.observations,
		'extensions.distributed.command.observations',
		parseProjectionObservation
	);
	const records = parseOptionalEvidenceArray(
		command.records,
		'extensions.distributed.command.records',
		parseRecordRevision
	);
	const projection =
		command.projection === undefined
			? undefined
			: parseCommandProjectionMetadata(command.projection);
	const projectionDisposition =
		command.projectionDisposition === undefined
			? undefined
			: command.projectionDisposition === 'revalidate'
				? 'revalidate'
				: invalid(
						'extensions.distributed.command.projectionDisposition'
					);
	const commandId = opaqueString(
		command.commandId,
		'extensions.distributed.command.commandId'
	);
	const causationId = opaqueString(
		command.causationId,
		'extensions.distributed.command.causationId'
	);
	const expectationKeys = new Set(expects.map(projectionExpectationKey));
	const seenObservations = new Set<string>();
	for (const observation of observations) {
		const key = projectionObservationKey(observation);
		if (
			observation.causationId !== causationId ||
			!expectationKeys.has(projectionExpectationKey(observation)) ||
			seenObservations.has(key)
		) {
			invalid('extensions.distributed.command.observations');
		}
		seenObservations.add(key);
	}
	if (
		projectionDisposition === 'revalidate' &&
		(projection !== undefined ||
			expects.length !== 0 ||
			observations.length !== 0 ||
			records.length !== 0 ||
			![
				'succeeded',
				'succeeded_pending_projection',
				'atomic',
				'projection_failed'
			].includes(state))
	) {
		invalid('extensions.distributed.command.projectionDisposition');
	}

	return Object.freeze({
		...command,
		commandId,
		causationId,
		state,
		consistency,
		...(projectionDisposition === undefined
			? {}
			: { projectionDisposition }),
		expects: Object.freeze(expects),
		observations,
		records,
		...(projection === undefined ? {} : { projection })
	}) as DistributedCommandMetadata;
}

function parseSnapshot(value: unknown): DistributedQuerySnapshot {
	const path = 'extensions.distributed.snapshot';
	const snapshot = record(value, path);
	if (typeof snapshot.recordsComplete !== 'boolean') {
		invalid(`${path}.recordsComplete`);
	}
	if (typeof snapshot.indexesComparable !== 'boolean') {
		invalid(`${path}.indexesComparable`);
	}
	const records = parseRequiredEvidenceArray(
		snapshot.records,
		`${path}.records`,
		parseRecordRevision
	);
	const indexes = parseRequiredEvidenceArray(
		snapshot.indexes,
		`${path}.indexes`,
		parseIndexRevision
	);
	const observations = parseRequiredEvidenceArray(
		snapshot.observations,
		`${path}.observations`,
		parseProjectionObservation
	);
	assertUnique(
		indexes.map((index) => index.projection),
		`${path}.indexes`
	);
	if (
		indexes.filter((index) => index.resume !== undefined).length >
		MAX_LIVE_RESUME_CURSORS
	) {
		invalid(`${path}.indexes`);
	}
	if (
		!snapshot.indexesComparable &&
		(indexes.length > 0 || observations.length > 0)
	) {
		invalid(`${path}.indexesComparable`);
	}

	return Object.freeze({
		...snapshot,
		scopeToken: opaqueString(snapshot.scopeToken, `${path}.scopeToken`),
		recordsComplete: snapshot.recordsComplete,
		indexesComparable: snapshot.indexesComparable,
		records,
		indexes,
		observations
	}) as DistributedQuerySnapshot;
}

function parseLive(value: unknown): DistributedLiveMetadata {
	const path = 'extensions.distributed.live';
	const live = record(value, path);
	if (live.mode !== 'snapshot' && live.mode !== 'resumable') invalid(`${path}.mode`);
	if ('supported' in live) invalid(`${path}.supported`);
	if (typeof live.reset !== 'boolean') invalid(`${path}.reset`);
	if (
		!Array.isArray(live.cursors) ||
		live.cursors.length > MAX_LIVE_RESUME_CURSORS
	) {
		invalid(`${path}.cursors`);
	}
	const cursors = parseRequiredEvidenceArray(
		live.cursors,
		`${path}.cursors`,
		parseLiveCursor
	);
	assertUnique(
		cursors.map((cursor) => cursor.projection),
		`${path}.cursors`
	);
	if (live.mode === 'snapshot' && (!live.reset || cursors.length !== 0)) {
		invalid(path);
	}
	return Object.freeze({
		...live,
		mode: live.mode,
		reset: live.reset,
		cursors
	}) as DistributedLiveMetadata;
}

function validateLiveSnapshot(
	snapshot: DistributedQuerySnapshot | undefined,
	live: DistributedLiveMetadata | undefined
): void {
	if (live === undefined) return;
	if (snapshot === undefined) invalid('extensions.distributed.snapshot');
	if (live.mode === 'snapshot') {
		if (snapshot.indexesComparable) invalid('extensions.distributed.snapshot.indexesComparable');
		return;
	}
	if (!snapshot.indexesComparable) {
		invalid('extensions.distributed.snapshot.indexesComparable');
	}
	if (
		live.cursors.length === 0 ||
		snapshot.indexes.length !== live.cursors.length
	) {
		invalid('extensions.distributed.live.cursors');
	}
	const indexes = new Map(
		snapshot.indexes.map((index) => [index.projection, index])
	);
	for (const cursor of live.cursors) {
		const index = indexes.get(cursor.projection);
		if (
			index === undefined ||
			index.position !== cursor.position ||
			index.resume === undefined ||
			index.resume.token !== cursor.token
		) {
			invalid('extensions.distributed.live.cursors');
		}
	}
}

function parseRecordRevision(
	value: unknown,
	path: string
): DistributedRecordRevision {
	const item = record(value, path);
	let responsePath: readonly string[] | undefined;
	if (item.path !== undefined) {
		if (
			!Array.isArray(item.path) ||
			item.path.length === 0 ||
			item.path.length > MAX_PATH_SEGMENTS
		) {
			invalid(`${path}.path`);
		}
		responsePath = Object.freeze(
			item.path.map((segment, index) =>
				publicString(segment, `${path}.path[${index}]`)
			)
		);
	}
	if (typeof item.tombstone !== 'boolean') invalid(`${path}.tombstone`);
	return Object.freeze({
		...item,
		...(responsePath === undefined ? {} : { path: responsePath }),
		model: publicString(item.model, `${path}.model`),
		scopeToken: opaqueString(item.scopeToken, `${path}.scopeToken`),
		incarnation: canonicalDecimal(item.incarnation, `${path}.incarnation`),
		revision: canonicalDecimal(item.revision, `${path}.revision`),
		tombstone: item.tombstone
	}) as DistributedRecordRevision;
}

function parseProjectionObservation(
	value: unknown,
	path: string
): DistributedProjectionObservation {
	const item = record(value, path);
	return Object.freeze({
		...item,
		causationId: opaqueString(item.causationId, `${path}.causationId`),
		projection: publicString(item.projection, `${path}.projection`),
		model: publicString(item.model, `${path}.model`),
		scopeToken: opaqueString(item.scopeToken, `${path}.scopeToken`)
	}) as DistributedProjectionObservation;
}

function parseIndexRevision(
	value: unknown,
	path: string
): DistributedIndexRevision {
	const item = record(value, path);
	const projection = publicString(item.projection, `${path}.projection`);
	const position = canonicalDecimal(item.position, `${path}.position`);
	const resume =
		item.resume === undefined
			? undefined
			: parseLiveCursor(item.resume, `${path}.resume`);
	if (
		resume !== undefined &&
		(resume.projection !== projection || resume.position !== position)
	) {
		invalid(`${path}.resume`);
	}
	return Object.freeze({
		...item,
		projection,
		scopeToken: opaqueString(item.scopeToken, `${path}.scopeToken`),
		position,
		...(resume === undefined ? {} : { resume })
	}) as DistributedIndexRevision;
}

function parseLiveCursor(value: unknown, path: string): DistributedLiveCursor {
	const item = record(value, path);
	return Object.freeze({
		...item,
		projection: publicString(item.projection, `${path}.projection`),
		position: canonicalDecimal(item.position, `${path}.position`),
		token: opaqueString(item.token, `${path}.token`)
	}) as DistributedLiveCursor;
}

function parseRequiredEvidenceArray<T>(
	value: unknown,
	path: string,
	parse: (value: unknown, path: string) => T
): readonly T[] {
	if (!Array.isArray(value) || value.length > MAX_EVIDENCE_ITEMS) invalid(path);
	return Object.freeze(value.map((item, index) => parse(item, `${path}[${index}]`)));
}

function parseOptionalEvidenceArray<T>(
	value: unknown,
	path: string,
	parse: (value: unknown, path: string) => T
): readonly T[] {
	return value === undefined
		? Object.freeze([])
		: parseRequiredEvidenceArray(value, path, parse);
}

function projectionExpectationKey(
	value: Pick<
		DistributedProjectionExpectation | DistributedProjectionObservation,
		'projection' | 'model' | 'scopeToken'
	>
): string {
	return JSON.stringify([value.projection, value.model, value.scopeToken]);
}

function projectionObservationKey(value: DistributedProjectionObservation): string {
	return JSON.stringify([
		value.causationId,
		value.projection,
		value.model,
		value.scopeToken
	]);
}

function assertUnique(values: readonly string[], path: string): void {
	if (new Set(values).size !== values.length) invalid(path);
}

function record(value: unknown, path: string): Record<string, unknown> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		invalid(path);
	}
	const prototype = Object.getPrototypeOf(value);
	if (prototype !== Object.prototype && prototype !== null) invalid(path);
	return value as Record<string, unknown>;
}

function publicString(value: unknown, path: string): string {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value.length > MAX_PUBLIC_NAME_LENGTH
	) {
		invalid(path);
	}
	return value;
}

function trustedPresetName(value: unknown, path: string): string {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value.length > MAX_TRUSTED_PRESET_NAME_LENGTH ||
		value.trim() !== value ||
		/[\u0000-\u001f\u007f-\u009f]/.test(value)
	) {
		invalid(path);
	}
	return value;
}

/** @internal */
export function isDistributedTrustedPresetCodec(
	value: unknown
): value is DistributedTrustedPresetCodec {
	return (
		value === 'string' ||
		value === 'string_unvalidated_timestamp' ||
		value === 'base64' ||
		value === 'boolean' ||
		value === 'int32' ||
		value === 'float64' ||
		value === 'json_number_precision_limited' ||
		value === 'json'
	);
}

function trustedPresetCodec(
	value: unknown,
	path: string
): DistributedTrustedPresetCodec {
	if (!isDistributedTrustedPresetCodec(value)) invalid(path);
	return value;
}

function parseTrustedPresetValue(
	value: unknown,
	codec: DistributedTrustedPresetCodec,
	path: string
): DistributedProtocolValue {
	switch (codec) {
		case 'string':
		case 'string_unvalidated_timestamp':
			if (typeof value !== 'string') invalid(path);
			return value;
		case 'base64':
			if (typeof value !== 'string' || !CANONICAL_BASE64.test(value)) {
				invalid(path);
			}
			return value;
		case 'boolean':
			if (typeof value !== 'boolean') invalid(path);
			return value;
		case 'int32':
			if (
				typeof value !== 'number' ||
				!Number.isInteger(value) ||
				Object.is(value, -0) ||
				value < -2_147_483_648 ||
				value > 2_147_483_647
			) {
				invalid(path);
			}
			return value;
		case 'json_number_precision_limited':
			if (
				typeof value !== 'number' ||
				!Number.isInteger(value) ||
				Object.is(value, -0) ||
				value < -MAX_SAFE_INTEGER ||
				value > MAX_SAFE_INTEGER
			) {
				invalid(path);
			}
			return value;
		case 'float64':
			if (typeof value !== 'number' || !Number.isFinite(value)) {
				invalid(path);
			}
			return Object.is(value, -0) ? 0 : value;
		case 'json':
			return cloneProtocolValue(value, path);
	}
}

function cloneProtocolValue(
	value: unknown,
	path: string,
	active: Set<object> = new Set(),
	depth = 0
): DistributedProtocolValue {
	if (depth > MAX_TRUSTED_PRESET_VALUE_DEPTH) invalid(path);
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	) {
		return value;
	}
	if (typeof value === 'number') {
		if (!Number.isFinite(value)) invalid(path);
		return Object.is(value, -0) ? 0 : value;
	}
	if (typeof value !== 'object' || active.has(value)) invalid(path);
	active.add(value);
	if (Array.isArray(value)) {
		if (Object.getPrototypeOf(value) !== Array.prototype) invalid(path);
		const ownKeys = Reflect.ownKeys(value);
		if (
			ownKeys.some(
				(key) =>
					key !== 'length' &&
					(typeof key !== 'string' ||
						!/^(0|[1-9][0-9]*)$/.test(key) ||
						Number(key) >= value.length)
			)
		) {
			invalid(path);
		}
		const result: DistributedProtocolValue[] = [];
		for (let index = 0; index < value.length; index += 1) {
			const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
			if (
				descriptor === undefined ||
				!('value' in descriptor) ||
				descriptor.value === undefined
			) {
				invalid(`${path}[${index}]`);
			}
			result.push(
				cloneProtocolValue(
					descriptor.value,
					`${path}[${index}]`,
					active,
					depth + 1
				)
			);
		}
		active.delete(value);
		return Object.freeze(result);
	}
	const prototype = Object.getPrototypeOf(value);
	if (prototype !== Object.prototype && prototype !== null) invalid(path);
	const keys = Reflect.ownKeys(value);
	if (keys.some((key) => typeof key !== 'string')) invalid(path);
	const result: Record<string, DistributedProtocolValue> = {};
	for (const key of (keys as string[]).sort()) {
		const descriptor = Object.getOwnPropertyDescriptor(value, key);
		if (
			descriptor === undefined ||
			!('value' in descriptor) ||
			descriptor.value === undefined
		) {
			invalid(`${path}.${key}`);
		}
		Object.defineProperty(result, key, {
			value: cloneProtocolValue(
				descriptor.value,
				`${path}.${key}`,
				active,
				depth + 1
			),
			enumerable: true,
			configurable: false,
			writable: false
		});
	}
	active.delete(value);
	return Object.freeze(result);
}

function opaqueString(value: unknown, path: string): DistributedOpaqueString {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value.length > MAX_OPAQUE_STRING_LENGTH
	) {
		invalid(path);
	}
	return value as DistributedOpaqueString;
}

function canonicalDecimal(
	value: unknown,
	path: string
): DistributedDecimalString {
	if (
		typeof value !== 'string' ||
		!/^(0|[1-9][0-9]*)$/.test(value) ||
		value.length > MAX_UNSIGNED_64.length ||
		(value.length === MAX_UNSIGNED_64.length && value > MAX_UNSIGNED_64)
	) {
		invalid(path);
	}
	return value as DistributedDecimalString;
}

/**
 * Compare two already-validated protocol decimals without converting either
 * through JavaScript's numeric types.
 */
export function compareDistributedDecimal(
	left: DistributedDecimalString,
	right: DistributedDecimalString
): -1 | 0 | 1 {
	if (left.length !== right.length) return left.length < right.length ? -1 : 1;
	return left === right ? 0 : left < right ? -1 : 1;
}

function enumString<T extends string>(
	value: unknown,
	values: ReadonlySet<T>,
	path: string
): T {
	if (typeof value !== 'string' || !values.has(value as T)) invalid(path);
	return value as T;
}

function invalid(path: string): never {
	throw new DistributedProtocolError('DISTRIBUTED_PROTOCOL_INVALID', path);
}
