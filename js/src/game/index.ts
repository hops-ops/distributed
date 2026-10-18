/**
 * Browser client SDK for `distributed_game` authoritative zones.
 *
 * - Codec: `PROTOCOL.md` v1 byte-exact (golden-fixture tested).
 * - `GameConnection`: one zone WebSocket — handshake, reconnect, clock sync.
 * - `EntityReplica`: schema-driven entity store with interpolation buffers.
 * - `Predictor`: client prediction + per-field reconciliation policies.
 * - `GameClient`: authority + observer zones, merged render, handoff.
 */
export {
	MAX_BLOB_LEN,
	GameDecodeError,
	WireReader,
	WireWriter,
	frameStream,
	unframeStream,
	toHex,
	fromHex,
	type GameDecodeErrorCode
} from './wire.js';
export {
	MAX_FIELDS,
	TAU,
	canonicalizeField,
	decodeField,
	decodeSchema,
	defineGameSchema,
	dequantizeAngle,
	dequantizeLinear,
	encodeField,
	encodeSchema,
	quantizeAngle,
	quantizeLinear,
	type FieldTypeName,
	type FieldValue,
	type GameFieldDef,
	type GameKindDef,
	type GameSchema
} from './schema.js';
export {
	GAME_PROTOCOL_VERSION,
	GameClientMirror,
	GameErrorCode,
	GameKickCode,
	GameMessageTag,
	decodeClientMessage,
	decodeServerMessage,
	encodeClientMessage,
	encodeServerMessage,
	maskHas,
	type DeltaMessage,
	type EntityCreate,
	type EntityKinds,
	type EntityUpdate,
	type ErrorMessage,
	type EventMessage,
	type FreezeMessage,
	type GameClientMessage,
	type GameRole,
	type GameServerMessage,
	type HandoffMessage,
	type HelloMessage,
	type InputMessage,
	type KickMessage,
	type LeaveMessage,
	type MirrorEntity,
	type NoticeFields,
	type PingMessage,
	type PongMessage,
	type ResumeMessage,
	type WelcomeMessage
} from './messages.js';
export { ClockSync, type ClockSample, type ClockSyncOptions } from './clock.js';
export {
	GameConnection,
	GameConnectionClosedError,
	defaultScheduler,
	type GameCloseInfo,
	type GameCloseReason,
	type GameConnectionEvents,
	type GameConnectionOptions,
	type GameConnectionState,
	type GameReconnectOptions,
	type GameScheduler,
	type GameSocket,
	type GameSocketFactory,
	type GameTicketRequest,
	type GameTicketSource,
	type GameWelcomeInfo
} from './connection.js';
export {
	EntityReplica,
	angleDelta,
	defaultInterpolationMode,
	interpolateValue,
	wrapAngle,
	type EntityFields,
	type EntityReplicaOptions,
	type InterpolationMode,
	type InterpolationPolicy,
	type ReplicaEntity
} from './replica.js';
export {
	Predictor,
	type Correction,
	type CorrectionPolicies,
	type CorrectionPolicy,
	type PendingInput,
	type PredictFn,
	type PredictorOptions
} from './prediction.js';
export {
	GameClient,
	type ControlledEntityResolver,
	type GameClientEvents,
	type GameClientOptions,
	type GamePredictionOptions,
	type GameZoneView,
	type RenderedEntity,
	type ZoneRole
} from './client.js';
