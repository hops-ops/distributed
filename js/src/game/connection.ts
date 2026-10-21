/**
 * One WebSocket connection to one zone (`PROTOCOL.md` §1, §6, §8).
 *
 * Handles the Hello/Welcome handshake, reconnect-token resumption inside the
 * server's grace window with fallback to a freshly fetched ticket, Ping/Pong
 * clock sync, sequenced inputs and typed server events. The transport and
 * timers are injectable so tests drive it with an in-memory socket.
 */
import { ClockSync, type ClockSyncOptions } from './clock.js';
import { Emitter } from './emitter.js';
import {
	encodeClientMessage,
	GAME_CLOSE_RESTARTED,
	GAME_PROTOCOL_VERSION,
	GameClientMirror,
	GameErrorCode,
	GameKickCode,
	type DeltaMessage,
	type ErrorMessage,
	type EventMessage,
	type FreezeMessage,
	type GameClientMessage,
	type GameRole,
	type GameServerMessage,
	type HandoffMessage,
	type KickMessage,
	type RedirectMessage,
	type ResumeMessage,
	type WelcomeMessage
} from './messages.js';

/** Minimal WebSocket surface (the browser `WebSocket` satisfies it). */
export interface GameSocket {
	binaryType?: string;
	send(data: Uint8Array): void;
	close(code?: number, reason?: string): void;
	onopen: ((event?: unknown) => void) | null;
	onmessage: ((event: { data: unknown }) => void) | null;
	onclose: ((event?: { code?: number; reason?: string }) => void) | null;
	onerror: ((event?: unknown) => void) | null;
}

export type GameSocketFactory = (url: string) => GameSocket;

/** Clock and timers (injectable for deterministic tests). */
export type GameScheduler = Readonly<{
	now(): number;
	setTimeout(callback: () => void, ms: number): unknown;
	clearTimeout(handle: unknown): void;
}>;

export const defaultScheduler: GameScheduler = Object.freeze({
	now: () => globalThis.performance?.now() ?? Date.now(),
	setTimeout: (callback: () => void, ms: number) => globalThis.setTimeout(callback, ms),
	clearTimeout: (handle: unknown) =>
		globalThis.clearTimeout(handle as ReturnType<typeof globalThis.setTimeout>)
});

/** Why the app is being asked for a ticket. */
export type GameTicketRequest = Readonly<{
	zoneId: string;
	role: GameRole;
	reason: 'join' | 'rejoin';
}>;

/**
 * Supplies a signed admission ticket. Called for the first join (unless
 * `initialTicket` is given) and again whenever a reconnect token can no
 * longer be used, so it should fetch a fresh ticket (tickets are single use).
 */
export type GameTicketSource = (request: GameTicketRequest) => string | Promise<string>;

export type GameReconnectOptions = Readonly<{
	/** Server reconnect grace window (default 15000 ms, matching the server). */
	graceMs?: number;
	/** Give up after this many consecutive failed attempts (default 10). */
	maxAttempts?: number;
	/** Delay before attempt `n` (1-based). Default 250·2^(n-1) ms capped at 5 s. */
	backoffMs?: (attempt: number) => number;
}>;

export type GameConnectionOptions = Readonly<{
	/** WebSocket URL of the zone endpoint, e.g. `wss://host/zones/z1`. */
	url: string;
	zoneId: string;
	/** Expected role (informational; the ticket decides). Default `player`. */
	role?: GameRole;
	ticket: GameTicketSource;
	/** Ticket for the first Hello only (e.g. a Handoff transfer ticket). */
	initialTicket?: string;
	contentVersion?: string;
	capabilities?: number;
	socket?: GameSocketFactory;
	scheduler?: GameScheduler;
	/** `false` disables automatic reconnect. */
	reconnect?: false | GameReconnectOptions;
	/** Ping cadence while open (default 1000 ms; first ping right after Welcome). */
	pingIntervalMs?: number;
	clock?: ClockSyncOptions;
	/** Consecutive Redirects followed before giving up (default 3). */
	maxRedirects?: number;
	/**
	 * Called when the server requires a content reload (`Kick(RELOAD)` or a
	 * handshake `Error(VERSION_MISMATCH)`). Load content for `version` and
	 * return the content version now running; the connection then resumes
	 * (reconnect token within the grace window, else a fresh ticket). Without
	 * it the connection waits in `reload-required` until
	 * `resumeAfterReload()` is called.
	 */
	reloadContent?: (request: GameReloadRequest) => string | Promise<string>;
	onListenerError?: (error: unknown) => void;
}>;

export type GameReloadRequest = Readonly<{
	/**
	 * Content version to load: the last Resume's version, else the last
	 * Welcome's `content_version` (undefined if never welcomed).
	 */
	version: string | undefined;
	reason: 'kick' | 'version-mismatch';
	message: string;
}>;

export type GameConnectionState =
	| 'idle'
	| 'connecting'
	| 'open'
	| 'reconnecting'
	| 'reload-required'
	| 'closed';

export type GameCloseReason =
	| 'client'
	| 'kick'
	| 'handoff'
	| 'rejected'
	| 'reconnect-exhausted'
	| 'too-many-redirects'
	| 'connect-failed';

export type GameCloseInfo = Readonly<{
	reason: GameCloseReason;
	code?: number;
	message?: string;
}>;

/** Cumulative transport counters of one connection (all sockets). */
export type GameConnectionStats = Readonly<{
	/** Server → client bytes / messages. */
	bytesIn: number;
	messagesIn: number;
	/** Bytes / count of Delta messages (replication traffic). */
	deltaBytesIn: number;
	deltasIn: number;
	/** Client → server bytes / messages. */
	bytesOut: number;
	messagesOut: number;
}>;

export type GameWelcomeInfo = Readonly<{
	/** True when this Welcome resumed the session with a reconnect token. */
	resumed: boolean;
	/** True for any Welcome after the first on this connection object. */
	reconnected: boolean;
}>;

export type GameConnectionEvents = {
	welcome: [WelcomeMessage, GameWelcomeInfo];
	delta: [DeltaMessage];
	event: [EventMessage];
	freeze: [FreezeMessage];
	resume: [ResumeMessage];
	handoff: [HandoffMessage];
	kick: [KickMessage];
	error: [ErrorMessage];
	redirect: [RedirectMessage];
	reloadRequired: [GameReloadRequest];
	pong: [{ rtt: number; offset: number; jitter: number }];
	state: [GameConnectionState];
	disconnect: [{ code?: number; reason?: string }];
	protocolError: [unknown];
	close: [GameCloseInfo];
};

type Attempt = {
	socket: GameSocket;
	mode: 'fresh' | 'resume';
	welcomed: boolean;
	ticket: string;
	token: string;
	redirect?: string;
	reload?: GameReloadRequest;
};

const defaultBackoff = (attempt: number): number => Math.min(5000, 250 * 2 ** (attempt - 1));

export class GameConnection {
	readonly zoneId: string;
	readonly clock: ClockSync;
	readonly mirror = new GameClientMirror();

	readonly #options: GameConnectionOptions;
	readonly #scheduler: GameScheduler;
	readonly #socketFactory: GameSocketFactory;
	readonly #emitter: Emitter<GameConnectionEvents>;
	readonly #reconnect: Required<GameReconnectOptions> | undefined;
	readonly #pingIntervalMs: number;

	#state: GameConnectionState = 'idle';
	readonly #baseUrl: string;
	#url: string;
	#contentVersion: string;
	#redirects = 0;
	#lastResumeVersion: string | undefined;
	#attempt: Attempt | undefined;
	#initialTicket: string | undefined;
	#reconnectToken = '';
	#sessionId: string | undefined;
	#welcomes = 0;
	#disconnectedAt: number | undefined;
	#failedAttempts = 0;
	#pingTimer: unknown;
	#retryTimer: unknown;
	#terminal: GameCloseInfo | undefined;
	#seq = 0;
	#frozen = false;
	#stats = { bytesIn: 0, messagesIn: 0, deltaBytesIn: 0, deltasIn: 0, bytesOut: 0, messagesOut: 0 };
	#connectWaiters: { resolve: (w: WelcomeMessage) => void; reject: (e: unknown) => void }[] = [];

	constructor(options: GameConnectionOptions) {
		this.#options = options;
		this.zoneId = options.zoneId;
		this.#baseUrl = options.url;
		this.#url = options.url;
		this.#contentVersion = options.contentVersion ?? '';
		this.#initialTicket = options.initialTicket;
		this.#scheduler = options.scheduler ?? defaultScheduler;
		const factory =
			options.socket ??
			((url: string) => {
				const Ctor = (globalThis as { WebSocket?: new (url: string) => GameSocket }).WebSocket;
				if (Ctor === undefined) throw new TypeError('WebSocket is unavailable in this runtime');
				return new Ctor(url);
			});
		this.#socketFactory = factory;
		this.#emitter = new Emitter(options.onListenerError);
		this.#reconnect =
			options.reconnect === false
				? undefined
				: {
						graceMs: options.reconnect?.graceMs ?? 15000,
						maxAttempts: options.reconnect?.maxAttempts ?? 10,
						backoffMs: options.reconnect?.backoffMs ?? defaultBackoff
					};
		this.#pingIntervalMs = options.pingIntervalMs ?? 1000;
		this.clock = new ClockSync(options.clock);
	}

	/** URL of the current (possibly redirected) zone host. */
	get url(): string {
		return this.#url;
	}

	/** Content version sent in Hello. */
	get contentVersion(): string {
		return this.#contentVersion;
	}

	get state(): GameConnectionState {
		return this.#state;
	}

	get welcome(): WelcomeMessage | undefined {
		return this.mirror.welcome;
	}

	get sessionId(): string | undefined {
		return this.#sessionId;
	}

	get role(): GameRole | undefined {
		return this.mirror.welcome?.role;
	}

	/** Reconnect token from the latest Welcome (rotated on every Welcome). */
	get reconnectToken(): string {
		return this.#reconnectToken;
	}

	/** True between Freeze and Resume. */
	get frozen(): boolean {
		return this.#frozen;
	}

	/** Last input sequence number sent on the current session. */
	get lastSeq(): number {
		return this.#seq;
	}

	/**
	 * Cumulative byte/message counters (HUDs, bandwidth graphs). Sample it
	 * periodically and diff for rates.
	 */
	get stats(): GameConnectionStats {
		return { ...this.#stats };
	}

	get scheduler(): GameScheduler {
		return this.#scheduler;
	}

	on<K extends keyof GameConnectionEvents>(
		type: K,
		listener: (...args: GameConnectionEvents[K]) => void
	): () => void {
		return this.#emitter.on(type, listener);
	}

	/** Local monotonic time. */
	now(): number {
		return this.#scheduler.now();
	}

	/** Estimated server clock (ms) now. */
	serverNow(): number {
		return this.clock.serverNow(this.#scheduler.now());
	}

	/** Server time a tick belongs to, from the Welcome's tick/time anchor. */
	tickTime(tick: number): number {
		const w = this.mirror.welcome;
		if (w === undefined || w.tickRate === 0) return this.serverNow();
		// Ticks are the low 32 bits: interpret the difference as int32.
		const diff = (tick - w.serverTick) | 0;
		return w.serverTime + (diff * 1000) / w.tickRate;
	}

	/** Server tick (u32, wrapping) whose state belongs to server time `serverTime`. */
	tickAt(serverTime: number): number {
		const w = this.mirror.welcome;
		if (w === undefined) return 0;
		const ticks = Math.floor(((serverTime - w.serverTime) * w.tickRate) / 1000);
		return (w.serverTick + ticks) >>> 0;
	}

	/** Estimated server tick at the current server time (wraps at 2^32). */
	estimatedServerTick(): number {
		const w = this.mirror.welcome;
		if (w === undefined) return 0;
		const ticks = Math.floor(((this.serverNow() - w.serverTime) * w.tickRate) / 1000);
		return (w.serverTick + ticks) >>> 0;
	}

	/** Open the connection; resolves with the first Welcome. */
	connect(): Promise<WelcomeMessage> {
		if (this.#state === 'closed') {
			return Promise.reject(new Error(`game connection ${this.zoneId} is closed`));
		}
		const promise = new Promise<WelcomeMessage>((resolve, reject) => {
			if (this.#state === 'open' && this.mirror.welcome !== undefined) {
				resolve(this.mirror.welcome);
				return;
			}
			this.#connectWaiters.push({ resolve, reject });
		});
		if (this.#state === 'idle') {
			this.#setState('connecting');
			void this.#open('fresh');
		}
		return promise;
	}

	/**
	 * Send one input. Returns its sequence number, or `undefined` when it was
	 * not sent (not open, observer, or frozen).
	 */
	sendInput(payload: Uint8Array, clientTick = this.estimatedServerTick()): number | undefined {
		if (!this.canSendInput()) return undefined;
		this.#seq += 1;
		this.#send({ type: 'Input', seq: this.#seq, clientTick: clientTick >>> 0, payload });
		return this.#seq;
	}

	canSendInput(): boolean {
		return this.#state === 'open' && this.mirror.welcome?.role === 'player' && !this.#frozen;
	}

	/** Send a Ping now (also sent automatically). */
	ping(): void {
		if (this.#state !== 'open') return;
		this.#send({ type: 'Ping', clientTime: this.#scheduler.now() });
	}

	/**
	 * Close for good. With `leave` (default) a graceful Leave is sent first so
	 * the server removes the player immediately instead of holding it for the
	 * reconnect grace window.
	 */
	close(options: { leave?: boolean } = {}): void {
		if (this.#state === 'closed') return;
		if ((options.leave ?? true) && this.#state === 'open') this.#send({ type: 'Leave' });
		this.#finish({ reason: 'client' });
	}

	/**
	 * Content version presented in the next Hello (reconnects, reloads).
	 * For clients that patch content live (e.g. after a Resume with a new
	 * version) without a reload round-trip; the server learns it on the
	 * next Hello.
	 */
	setContentVersion(contentVersion: string): void {
		this.#contentVersion = contentVersion;
	}

	/**
	 * Continue after a content reload (state `reload-required`): sends the
	 * new `contentVersion` in Hello, with the reconnect token while the grace
	 * window lasts, else with a fresh ticket.
	 */
	resumeAfterReload(contentVersion: string): void {
		if (this.#state !== 'reload-required') return;
		this.#contentVersion = contentVersion;
		this.#setState(this.#welcomes === 0 ? 'connecting' : 'reconnecting');
		void this.#open(this.#withinGrace() ? 'resume' : 'fresh');
	}

	/** Simulate an abrupt transport loss (tests, "airplane mode" toggles). */
	dropTransport(): void {
		this.#attempt?.socket.close(4000, 'dropped by client');
	}

	#setState(state: GameConnectionState): void {
		if (this.#state === state) return;
		this.#state = state;
		this.#emitter.emit('state', state);
	}

	#send(message: GameClientMessage): void {
		const socket = this.#attempt?.socket;
		if (socket === undefined) return;
		try {
			const bytes = encodeClientMessage(message);
			socket.send(bytes);
			this.#stats.bytesOut += bytes.byteLength;
			this.#stats.messagesOut += 1;
		} catch {
			// A failing send surfaces as a close; reconnect logic handles it.
		}
	}

	async #open(
		mode: 'fresh' | 'resume',
		reuse?: { ticket: string; token: string }
	): Promise<void> {
		let ticket = reuse?.ticket ?? '';
		if (mode === 'fresh' && reuse === undefined) {
			try {
				if (this.#initialTicket !== undefined) {
					// Kept until a Welcome: an unanswered Hello did not consume it.
					ticket = this.#initialTicket;
				} else {
					ticket = await this.#options.ticket({
						zoneId: this.zoneId,
						role: this.#options.role ?? 'player',
						reason: this.#welcomes === 0 ? 'join' : 'rejoin'
					});
				}
			} catch (error) {
				if (this.#state === 'closed') return;
				this.#attemptFailed(error);
				return;
			}
			if (this.#state === 'closed') return;
		}
		const token = reuse?.token ?? (mode === 'resume' ? this.#reconnectToken : '');
		if (reuse === undefined && mode === 'fresh') this.#url = this.#baseUrl;
		let socket: GameSocket;
		try {
			socket = this.#socketFactory(this.#url);
		} catch (error) {
			this.#attemptFailed(error);
			return;
		}
		const attempt: Attempt = { socket, mode, welcomed: false, ticket, token };
		this.#attempt = attempt;
		try {
			socket.binaryType = 'arraybuffer';
		} catch {
			// Some fakes expose a read-only binaryType.
		}
		socket.onopen = () => {
			if (this.#attempt !== attempt) return;
			this.#send({
				type: 'Hello',
				protocolVersion: GAME_PROTOCOL_VERSION,
				contentVersion: this.#contentVersion,
				capabilities: this.#options.capabilities ?? 0,
				ticket,
				reconnectToken: token
			});
		};
		socket.onmessage = (event) => {
			if (this.#attempt !== attempt) return;
			const bytes = toBytes(event.data);
			if (bytes === undefined) return;
			this.#stats.bytesIn += bytes.byteLength;
			this.#stats.messagesIn += 1;
			this.#receive(attempt, bytes);
		};
		socket.onerror = () => undefined;
		socket.onclose = (event) => {
			if (this.#attempt !== attempt) return;
			this.#attempt = undefined;
			this.#onSocketClosed(attempt, event?.code, event?.reason);
		};
	}

	#receive(attempt: Attempt, bytes: Uint8Array): void {
		let message: GameServerMessage;
		try {
			message = this.mirror.apply(bytes);
		} catch (error) {
			// Deltas are relative to everything sent before: after a decode
			// failure the mirror is unusable. Drop and resynchronize.
			this.#emitter.emit('protocolError', error);
			attempt.socket.close(1002, 'protocol error');
			return;
		}
		switch (message.type) {
			case 'Welcome':
				this.#onWelcome(attempt, message);
				break;
			case 'Delta':
				this.#stats.deltaBytesIn += bytes.byteLength;
				this.#stats.deltasIn += 1;
				this.#emitter.emit('delta', message);
				break;
			case 'Event':
				this.#emitter.emit('event', message);
				break;
			case 'Pong': {
				const sample = this.clock.observe(
					message.clientTime,
					message.serverTime,
					this.#scheduler.now()
				);
				if (sample !== undefined) {
					this.#emitter.emit('pong', {
						rtt: this.clock.rtt,
						offset: this.clock.offset,
						jitter: this.clock.jitter
					});
				}
				break;
			}
			case 'Freeze':
				this.#frozen = true;
				this.#emitter.emit('freeze', message);
				break;
			case 'Resume':
				this.#frozen = false;
				this.#lastResumeVersion = message.version;
				this.#emitter.emit('resume', message);
				break;
			case 'Handoff':
				this.#terminal = { reason: 'handoff' };
				this.#emitter.emit('handoff', message);
				break;
			case 'Kick':
				if (message.code === GameKickCode.RELOAD) {
					// Not session-terminal: the player is held for the grace window.
					attempt.reload = {
						version: this.#lastResumeVersion ?? this.mirror.welcome?.contentVersion,
						reason: 'kick',
						message: message.message
					};
				} else {
					this.#terminal = { reason: 'kick', code: message.code, message: message.message };
				}
				this.#emitter.emit('kick', message);
				break;
			case 'Redirect':
				attempt.redirect = message.url;
				this.#emitter.emit('redirect', message);
				break;
			case 'Error':
				this.#onError(attempt, message);
				if (!attempt.welcomed && this.#attempt === attempt) {
					// A refused Hello ends the attempt: nothing else follows it.
					// Do not wait for the server's close (a host may never send
					// one, e.g. a restarted zone refusing a void token).
					this.#attempt = undefined;
					attempt.socket.close(1000, 'refused');
					this.#onSocketClosed(attempt, 1008, message.message);
				}
				break;
		}
	}

	#onWelcome(attempt: Attempt, welcome: WelcomeMessage): void {
		const reconnected = this.#welcomes > 0;
		this.#welcomes += 1;
		attempt.welcomed = true;
		this.#redirects = 0;
		this.#initialTicket = undefined;
		this.#lastResumeVersion = undefined;
		this.#failedAttempts = 0;
		this.#disconnectedAt = undefined;
		this.#reconnectToken = welcome.reconnectToken;
		this.#sessionId = welcome.sessionId;
		this.#seq = 0;
		this.#frozen = false;
		this.clock.reset();
		this.clock.seed(welcome.serverTime, this.#scheduler.now());
		this.#setState('open');
		this.#startPing();
		this.#emitter.emit('welcome', welcome, { resumed: attempt.mode === 'resume', reconnected });
		const waiters = this.#connectWaiters;
		this.#connectWaiters = [];
		for (const waiter of waiters) waiter.resolve(welcome);
	}

	#onError(attempt: Attempt, error: ErrorMessage): void {
		this.#emitter.emit('error', error);
		if (attempt.welcomed) return; // post-join errors are advisory (§6.3)
		if (attempt.mode === 'resume' && error.code === GameErrorCode.RECONNECT_FAILED) {
			// Grace window elapsed or token unknown: the server closes; the
			// close handler falls back to a fresh ticket.
			this.#reconnectToken = '';
			return;
		}
		if (error.code === GameErrorCode.VERSION_MISMATCH) {
			attempt.reload = {
				version: this.#lastResumeVersion ?? this.mirror.welcome?.contentVersion,
				reason: 'version-mismatch',
				message: error.message
			};
			return;
		}
		this.#terminal = { reason: 'rejected', code: error.code, message: error.message };
	}

	#onSocketClosed(attempt: Attempt, code?: number, reason?: string): void {
		this.#stopPing();
		if (this.#state === 'closed') return;
		this.#emitter.emit('disconnect', { code, reason });
		if (this.#terminal !== undefined) {
			this.#finish(this.#terminal);
			return;
		}
		if (attempt.welcomed) {
			this.#disconnectedAt = this.#scheduler.now();
			this.#failedAttempts = 0;
		}
		if (code === GAME_CLOSE_RESTARTED) {
			// The zone restarted or moved: its sessions are gone. Rejoin now
			// with a fresh ticket instead of presenting a void token.
			this.#reconnectToken = '';
		}
		if (attempt.redirect !== undefined) {
			this.#redirects += 1;
			if (this.#redirects > (this.#options.maxRedirects ?? 3)) {
				this.#finish({ reason: 'too-many-redirects', message: attempt.redirect });
				return;
			}
			this.#url = attempt.redirect;
			// Same Hello: the ticket was not consumed by the redirecting host.
			void this.#open(attempt.mode, { ticket: attempt.ticket, token: attempt.token });
			return;
		}
		if (attempt.reload !== undefined) {
			this.#awaitReload(attempt.reload);
			return;
		}
		if (this.#welcomes === 0 && this.#reconnect === undefined) {
			this.#finish({ reason: 'connect-failed', code, message: reason });
			return;
		}
		this.#attemptFailed(new Error(`socket closed (${code ?? 'no code'})`), attempt.welcomed);
	}

	#attemptFailed(error: unknown, justDropped = false): void {
		if (this.#state === 'closed') return;
		if (this.#reconnect === undefined) {
			this.#finish({
				reason: this.#welcomes === 0 ? 'connect-failed' : 'reconnect-exhausted',
				message: String(error)
			});
			return;
		}
		if (!justDropped) this.#failedAttempts += 1;
		if (this.#failedAttempts >= this.#reconnect.maxAttempts) {
			this.#finish({
				reason: this.#welcomes === 0 ? 'connect-failed' : 'reconnect-exhausted',
				message: String(error)
			});
			return;
		}
		this.#setState(this.#welcomes === 0 ? 'connecting' : 'reconnecting');
		const delay = justDropped ? 0 : this.#reconnect.backoffMs(this.#failedAttempts);
		this.#retryTimer = this.#scheduler.setTimeout(() => {
			this.#retryTimer = undefined;
			if (this.#state === 'closed') return;
			void this.#open(this.#withinGrace() ? 'resume' : 'fresh');
		}, delay);
	}

	#withinGrace(): boolean {
		return (
			this.#reconnectToken !== '' &&
			this.#disconnectedAt !== undefined &&
			this.#scheduler.now() - this.#disconnectedAt < (this.#reconnect?.graceMs ?? 15000)
		);
	}

	#awaitReload(request: GameReloadRequest): void {
		this.#setState('reload-required');
		this.#emitter.emit('reloadRequired', request);
		const reload = this.#options.reloadContent;
		if (reload === undefined) return;
		Promise.resolve()
			.then(() => reload(request))
			.then(
				(version) => this.resumeAfterReload(version),
				(error) => this.#finish({ reason: 'connect-failed', message: `reload failed: ${String(error)}` })
			);
	}

	#startPing(): void {
		this.#stopPing();
		this.ping();
		const tick = () => {
			this.#pingTimer = this.#scheduler.setTimeout(() => {
				if (this.#state !== 'open') return;
				this.ping();
				tick();
			}, this.#pingIntervalMs);
		};
		tick();
	}

	#stopPing(): void {
		if (this.#pingTimer !== undefined) {
			this.#scheduler.clearTimeout(this.#pingTimer);
			this.#pingTimer = undefined;
		}
	}

	#finish(info: GameCloseInfo): void {
		if (this.#state === 'closed') return;
		this.#stopPing();
		if (this.#retryTimer !== undefined) {
			this.#scheduler.clearTimeout(this.#retryTimer);
			this.#retryTimer = undefined;
		}
		const attempt = this.#attempt;
		this.#attempt = undefined;
		if (attempt !== undefined) {
			try {
				attempt.socket.close(1000, info.reason);
			} catch {
				// already closed
			}
		}
		this.#setState('closed');
		const waiters = this.#connectWaiters;
		this.#connectWaiters = [];
		const failure = new GameConnectionClosedError(this.zoneId, info);
		for (const waiter of waiters) waiter.reject(failure);
		this.#emitter.emit('close', info);
	}
}

/** Rejection of `connect()` when the connection closed before a Welcome. */
export class GameConnectionClosedError extends Error {
	readonly info: GameCloseInfo;
	constructor(zoneId: string, info: GameCloseInfo) {
		super(
			`game connection to ${zoneId} closed: ${info.reason}${
				info.message === undefined ? '' : ` (${info.message})`
			}`
		);
		this.name = 'GameConnectionClosedError';
		this.info = info;
	}
}

function toBytes(data: unknown): Uint8Array | undefined {
	if (data instanceof Uint8Array) return data;
	if (data instanceof ArrayBuffer) return new Uint8Array(data);
	if (ArrayBuffer.isView(data)) {
		return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
	}
	return undefined; // text frames are not part of the protocol
}
