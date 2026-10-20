/**
 * Message codecs (`PROTOCOL.md` §6–§7) and the reference client mirror
 * (§7.2, matching Rust `protocol::ClientMirror`).
 */
import {
	decodeField,
	decodeSchema,
	encodeField,
	encodeSchema,
	type FieldValue,
	type GameSchema
} from './schema.js';
import { GameDecodeError, WireReader, WireWriter } from './wire.js';

/** Wire protocol version spoken by this client. */
export const GAME_PROTOCOL_VERSION = 1;

/** Message type bytes (§6.1). */
export const GameMessageTag = Object.freeze({
	HELLO: 0x01,
	INPUT: 0x02,
	PING: 0x03,
	LEAVE: 0x04,
	WELCOME: 0x80,
	DELTA: 0x81,
	EVENT: 0x82,
	PONG: 0x83,
	FREEZE: 0x84,
	RESUME: 0x85,
	HANDOFF: 0x86,
	KICK: 0x87,
	ERROR: 0x88,
	REDIRECT: 0x89
});

/** Kick codes. */
export const GameKickCode = Object.freeze({
	BY_ZONE: 1,
	REPLACED: 2,
	SHUTDOWN: 3,
	RATE_LIMITED: 4,
	ZONE_DISPOSED: 5,
	SLOW_CONSUMER: 6,
	/** Not session-terminal: reload content, then reconnect with the token. */
	RELOAD: 7
});

/** Error codes. */
export const GameErrorCode = Object.freeze({
	BAD_MESSAGE: 1,
	UNAUTHORIZED: 2,
	REJECTED: 3,
	ZONE_FULL: 4,
	VERSION_MISMATCH: 5,
	RATE_LIMITED: 6,
	NOT_FOUND: 7,
	RECONNECT_FAILED: 8
});

export type HelloMessage = Readonly<{
	type: 'Hello';
	protocolVersion: number;
	contentVersion: string;
	capabilities: number;
	ticket: string;
	reconnectToken: string;
}>;
export type InputMessage = Readonly<{
	type: 'Input';
	seq: number;
	clientTick: number;
	payload: Uint8Array;
}>;
export type PingMessage = Readonly<{ type: 'Ping'; clientTime: number }>;
export type LeaveMessage = Readonly<{ type: 'Leave' }>;
export type GameClientMessage = HelloMessage | InputMessage | PingMessage | LeaveMessage;

export type GameRole = 'player' | 'observer';

export type WelcomeMessage = Readonly<{
	type: 'Welcome';
	sessionId: string;
	zoneId: string;
	role: GameRole;
	tickRate: number;
	patchRate: number;
	serverTick: number;
	serverTime: number;
	reconnectToken: string;
	contentVersion: string;
	schema: GameSchema;
}>;
export type EntityCreate = Readonly<{
	id: number;
	kind: number;
	fields: readonly FieldValue[];
}>;
export type EntityUpdate = Readonly<{
	id: number;
	mask: number;
	fields: readonly FieldValue[];
}>;
export type DeltaMessage = Readonly<{
	type: 'Delta';
	serverTick: number;
	ackSeq: number;
	removes: readonly number[];
	creates: readonly EntityCreate[];
	updates: readonly EntityUpdate[];
}>;
export type EventMessage = Readonly<{ type: 'Event'; typeId: number; payload: Uint8Array }>;
export type PongMessage = Readonly<{ type: 'Pong'; clientTime: number; serverTime: number }>;
export type NoticeFields = Readonly<{ reason: string; version: string; notes: string }>;
export type FreezeMessage = NoticeFields & Readonly<{ type: 'Freeze' }>;
export type ResumeMessage = NoticeFields & Readonly<{ type: 'Resume' }>;
export type HandoffMessage = Readonly<{
	type: 'Handoff';
	targetZone: string;
	transferTicket: string;
}>;
export type KickMessage = Readonly<{ type: 'Kick'; code: number; message: string }>;
export type ErrorMessage = Readonly<{ type: 'Error'; code: number; message: string }>;
/** This host does not own `zoneId`; resend the same Hello to `url`. */
export type RedirectMessage = Readonly<{ type: 'Redirect'; zoneId: string; url: string }>;
export type GameServerMessage =
	| WelcomeMessage
	| DeltaMessage
	| EventMessage
	| PongMessage
	| FreezeMessage
	| ResumeMessage
	| HandoffMessage
	| KickMessage
	| ErrorMessage
	| RedirectMessage;

/** What the Delta codec needs: the Welcome schema and the kind of each known id. */
export type EntityKinds = Readonly<{
	schema: GameSchema | undefined;
	kindOf(id: number): number | undefined;
}>;

const NO_KINDS: EntityKinds = { schema: undefined, kindOf: () => undefined };

/** Encode one client message (one WebSocket binary frame). */
export function encodeClientMessage(message: GameClientMessage): Uint8Array {
	const w = new WireWriter();
	switch (message.type) {
		case 'Hello':
			w.u8(GameMessageTag.HELLO)
				.varuint(message.protocolVersion)
				.string(message.contentVersion)
				.varuint(message.capabilities)
				.string(message.ticket)
				.string(message.reconnectToken);
			break;
		case 'Input':
			w.u8(GameMessageTag.INPUT)
				.varuint(message.seq)
				.varuint(message.clientTick)
				.bytes(message.payload);
			break;
		case 'Ping':
			w.u8(GameMessageTag.PING).f64(message.clientTime);
			break;
		case 'Leave':
			w.u8(GameMessageTag.LEAVE);
			break;
	}
	return w.finish();
}

/** Decode one client message. */
export function decodeClientMessage(bytes: Uint8Array): GameClientMessage {
	const r = new WireReader(bytes);
	let message: GameClientMessage;
	const tag = r.u8();
	switch (tag) {
		case GameMessageTag.HELLO:
			message = {
				type: 'Hello',
				protocolVersion: r.varuint(),
				contentVersion: r.string(),
				capabilities: r.varuint(),
				ticket: r.string(),
				reconnectToken: r.string()
			};
			break;
		case GameMessageTag.INPUT:
			message = { type: 'Input', seq: r.varuint(), clientTick: r.varuint(), payload: r.bytes() };
			break;
		case GameMessageTag.PING:
			message = { type: 'Ping', clientTime: r.f64() };
			break;
		case GameMessageTag.LEAVE:
			message = { type: 'Leave' };
			break;
		default:
			throw new GameDecodeError('UNKNOWN_MESSAGE', `message type 0x${tag.toString(16)}`);
	}
	r.finish();
	return message;
}

/** Encode one server message (used by test servers and tooling). */
export function encodeServerMessage(
	message: GameServerMessage,
	kinds: EntityKinds = NO_KINDS
): Uint8Array {
	const w = new WireWriter();
	switch (message.type) {
		case 'Welcome':
			w.u8(GameMessageTag.WELCOME)
				.string(message.sessionId)
				.string(message.zoneId)
				.u8(message.role === 'player' ? 0 : 1)
				.varuint(message.tickRate)
				.varuint(message.patchRate)
				.varuint(message.serverTick)
				.f64(message.serverTime)
				.string(message.reconnectToken)
				.string(message.contentVersion);
			encodeSchema(w, message.schema);
			break;
		case 'Delta':
			encodeDelta(w, message, kinds);
			break;
		case 'Event':
			w.u8(GameMessageTag.EVENT).varuint(message.typeId).bytes(message.payload);
			break;
		case 'Pong':
			w.u8(GameMessageTag.PONG).f64(message.clientTime).f64(message.serverTime);
			break;
		case 'Freeze':
		case 'Resume':
			w.u8(message.type === 'Freeze' ? GameMessageTag.FREEZE : GameMessageTag.RESUME)
				.string(message.reason)
				.string(message.version)
				.string(message.notes);
			break;
		case 'Handoff':
			w.u8(GameMessageTag.HANDOFF).string(message.targetZone).string(message.transferTicket);
			break;
		case 'Kick':
			w.u8(GameMessageTag.KICK).varuint(message.code).string(message.message);
			break;
		case 'Error':
			w.u8(GameMessageTag.ERROR).varuint(message.code).string(message.message);
			break;
		case 'Redirect':
			w.u8(GameMessageTag.REDIRECT).string(message.zoneId).string(message.url);
			break;
	}
	return w.finish();
}

/** Decode one server message. Deltas need `kinds` (schema + known ids). */
export function decodeServerMessage(
	bytes: Uint8Array,
	kinds: EntityKinds = NO_KINDS
): GameServerMessage {
	const r = new WireReader(bytes);
	let message: GameServerMessage;
	const tag = r.u8();
	switch (tag) {
		case GameMessageTag.WELCOME: {
			const sessionId = r.string();
			const zoneId = r.string();
			const roleByte = r.u8();
			if (roleByte > 1) throw new GameDecodeError('INVALID', 'role');
			message = {
				type: 'Welcome',
				sessionId,
				zoneId,
				role: roleByte === 0 ? 'player' : 'observer',
				tickRate: r.varuint(),
				patchRate: r.varuint(),
				serverTick: r.varuint(),
				serverTime: r.f64(),
				reconnectToken: r.string(),
				contentVersion: r.string(),
				schema: decodeSchema(r)
			};
			break;
		}
		case GameMessageTag.DELTA:
			message = decodeDelta(r, kinds);
			break;
		case GameMessageTag.EVENT:
			message = { type: 'Event', typeId: r.varuint(), payload: r.bytes() };
			break;
		case GameMessageTag.PONG:
			message = { type: 'Pong', clientTime: r.f64(), serverTime: r.f64() };
			break;
		case GameMessageTag.FREEZE:
		case GameMessageTag.RESUME:
			message = {
				type: tag === GameMessageTag.FREEZE ? 'Freeze' : 'Resume',
				reason: r.string(),
				version: r.string(),
				notes: r.string()
			};
			break;
		case GameMessageTag.HANDOFF:
			message = { type: 'Handoff', targetZone: r.string(), transferTicket: r.string() };
			break;
		case GameMessageTag.KICK:
			message = { type: 'Kick', code: r.varuint(), message: r.string() };
			break;
		case GameMessageTag.ERROR:
			message = { type: 'Error', code: r.varuint(), message: r.string() };
			break;
		case GameMessageTag.REDIRECT:
			message = { type: 'Redirect', zoneId: r.string(), url: r.string() };
			break;
		default:
			throw new GameDecodeError('UNKNOWN_MESSAGE', `message type 0x${tag.toString(16)}`);
	}
	r.finish();
	return message;
}

function requireSchema(kinds: EntityKinds): GameSchema {
	if (kinds.schema === undefined) throw new GameDecodeError('INVALID', 'delta needs a schema');
	return kinds.schema;
}

function kindLookup(id: number, created: Map<number, number>, kinds: EntityKinds): number {
	const kind = created.get(id) ?? kinds.kindOf(id);
	if (kind === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${id}`);
	return kind;
}

function encodeDelta(w: WireWriter, d: DeltaMessage, kinds: EntityKinds): void {
	const schema = requireSchema(kinds);
	w.u8(GameMessageTag.DELTA).varuint(d.serverTick).varuint(d.ackSeq);
	w.varuint(d.removes.length);
	for (const id of d.removes) w.varuint(id);
	const created = new Map<number, number>();
	w.varuint(d.creates.length);
	for (const c of d.creates) {
		const kind = schema.byId.get(c.kind);
		if (kind === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${c.id}`);
		if (kind.fields.length !== c.fields.length) {
			throw new GameDecodeError('INVALID', 'create must carry every field');
		}
		w.varuint(c.id).varuint(c.kind);
		kind.fields.forEach((def, i) => encodeField(w, def, c.fields[i]));
		created.set(c.id, c.kind);
	}
	w.varuint(d.updates.length);
	for (const u of d.updates) {
		const kind = schema.byId.get(kindLookup(u.id, created, kinds));
		if (kind === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${u.id}`);
		w.varuint(u.id).varuint(u.mask);
		let next = 0;
		kind.fields.forEach((def, i) => {
			if (maskHas(u.mask, i)) {
				if (next >= u.fields.length) {
					throw new GameDecodeError('INVALID', 'mask/field count mismatch');
				}
				encodeField(w, def, u.fields[next++]);
			}
		});
	}
}

/** Bit `i` of a u32 mask (safe for bit 31). */
export function maskHas(mask: number, index: number): boolean {
	return ((mask >>> index) & 1) === 1;
}

function decodeDelta(r: WireReader, kinds: EntityKinds): DeltaMessage {
	const schema = requireSchema(kinds);
	const serverTick = r.varuint();
	const ackSeq = r.varuint();
	const removes: number[] = [];
	for (let n = r.varuint(); n > 0; n -= 1) removes.push(r.varuint());
	const created = new Map<number, number>();
	const creates: EntityCreate[] = [];
	for (let n = r.varuint(); n > 0; n -= 1) {
		const id = r.varuint();
		const kindId = r.varuint();
		if (kindId > 0xffff) throw new GameDecodeError('INVALID', 'kind');
		const kind = schema.byId.get(kindId);
		if (kind === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${id}`);
		const fields = kind.fields.map((def) => decodeField(r, def));
		created.set(id, kindId);
		creates.push({ id, kind: kindId, fields });
	}
	const updates: EntityUpdate[] = [];
	for (let n = r.varuint(); n > 0; n -= 1) {
		const id = r.varuint();
		const kind = schema.byId.get(kindLookup(id, created, kinds));
		if (kind === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${id}`);
		const mask = r.varuint();
		if (kind.fields.length < 32 && Math.floor(mask / 2 ** kind.fields.length) !== 0) {
			throw new GameDecodeError('INVALID', 'mask names a field the kind lacks');
		}
		const fields: FieldValue[] = [];
		kind.fields.forEach((def, i) => {
			if (maskHas(mask, i)) fields.push(decodeField(r, def));
		});
		updates.push({ id, mask, fields });
	}
	return { type: 'Delta', serverTick, ackSeq, removes, creates, updates };
}

export type MirrorEntity = { kind: number; fields: FieldValue[] };

/**
 * Normative client-side state (§7.2): decodes frames and applies Deltas.
 * Mirrors Rust `ClientMirror`; higher layers (replica, prediction) build on
 * the decoded messages it returns.
 */
export class GameClientMirror implements EntityKinds {
	welcome: WelcomeMessage | undefined;
	readonly entities = new Map<number, MirrorEntity>();
	serverTick = 0;
	ackSeq = 0;

	get schema(): GameSchema | undefined {
		return this.welcome?.schema;
	}

	kindOf(id: number): number | undefined {
		return this.entities.get(id)?.kind;
	}

	/** Decode a frame and apply it. A Welcome resets all entities. */
	apply(frame: Uint8Array): GameServerMessage {
		const message = decodeServerMessage(frame, this);
		this.applyDecoded(message);
		return message;
	}

	applyDecoded(message: GameServerMessage): void {
		if (message.type === 'Welcome') {
			this.welcome = message;
			this.entities.clear();
			this.serverTick = message.serverTick;
			this.ackSeq = 0;
		} else if (message.type === 'Delta') {
			for (const id of message.removes) this.entities.delete(id);
			for (const c of message.creates) {
				this.entities.set(c.id, { kind: c.kind, fields: [...c.fields] });
			}
			for (const u of message.updates) {
				const entity = this.entities.get(u.id);
				if (entity === undefined) throw new GameDecodeError('UNKNOWN_ENTITY', `entity ${u.id}`);
				let next = 0;
				for (let i = 0; i < entity.fields.length; i += 1) {
					if (maskHas(u.mask, i)) entity.fields[i] = u.fields[next++];
				}
			}
			this.serverTick = message.serverTick;
			this.ackSeq = message.ackSeq;
		}
	}
}
