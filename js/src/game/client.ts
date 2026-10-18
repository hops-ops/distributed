/**
 * Multi-zone game client: one authoritative connection (the zone that owns
 * the player) plus observer connections to neighbouring zones, merged into
 * one rendered world tagged by zone. Follows Handoff messages by connecting
 * to the target zone with the transfer ticket and swapping authority without
 * blanking the rendered world.
 */
import {
	GameConnection,
	type GameCloseInfo,
	type GameConnectionOptions,
	type GameReconnectOptions,
	type GameScheduler,
	type GameSocketFactory,
	type GameTicketSource,
	defaultScheduler
} from './connection.js';
import { Emitter } from './emitter.js';
import type {
	DeltaMessage,
	ErrorMessage,
	EventMessage,
	FreezeMessage,
	HandoffMessage,
	KickMessage,
	ResumeMessage,
	WelcomeMessage
} from './messages.js';
import { Predictor, type Correction, type PredictorOptions } from './prediction.js';
import {
	EntityReplica,
	type EntityFields,
	type InterpolationPolicy,
	type ReplicaEntity
} from './replica.js';

export type ZoneRole = 'authority' | 'observer' | 'retired';

/** One zone known to the client. */
export type GameZoneView = Readonly<{
	zoneId: string;
	role: ZoneRole;
	connection: GameConnection;
	replica: EntityReplica;
}>;

export type RenderedEntity = Readonly<{
	/** `${zoneId}:${id}` — entity ids are only unique within a zone. */
	key: string;
	zoneId: string;
	id: number;
	kind: number;
	kindName: string;
	fields: EntityFields;
	/** From the authoritative zone (vs. observed neighbour / retired). */
	authoritative: boolean;
	/** The locally controlled, predicted entity. */
	predicted: boolean;
	stale: boolean;
}>;

export type ControlledEntityResolver = (
	context: Readonly<{ zoneId: string; welcome: WelcomeMessage; replica: EntityReplica }>
) => number | undefined;

export type GamePredictionOptions<TInput> = PredictorOptions<TInput> &
	Readonly<{
		/** Encode an input to its game-defined payload bytes. */
		encodeInput(input: TInput): Uint8Array;
		/**
		 * Which entity the player controls in a zone. Re-evaluated on every
		 * authoritative Delta until it returns an id; `setControlledEntity`
		 * overrides it (e.g. from a game Event).
		 */
		controlledEntity?: ControlledEntityResolver;
	}>;

export type GameClientOptions<TInput> = Readonly<{
	/** WebSocket URL for a zone, e.g. `(z) => \`wss://game.example/zones/${z}\``. */
	url: (zoneId: string) => string;
	/** Fresh player ticket for a zone (joins and post-grace rejoins). */
	ticket: GameTicketSource;
	/** Observer ticket for a zone. Defaults to `ticket` with `role: 'observer'`. */
	observerTicket?: GameTicketSource;
	contentVersion?: string;
	capabilities?: number;
	socket?: GameSocketFactory;
	scheduler?: GameScheduler;
	reconnect?: false | GameReconnectOptions;
	pingIntervalMs?: number;
	/**
	 * Render delay behind the server clock. Default: two patch intervals of
	 * the zone (100 ms at 20 Hz).
	 */
	interpolationDelayMs?: number | ((zone: GameZoneView) => number);
	interpolation?: InterpolationPolicy;
	staleGraceMs?: number;
	historyMs?: number;
	prediction?: GamePredictionOptions<TInput>;
	/** After a handoff, keep the previous zone rendered this long once the target delivered its first Delta. Default 500 ms. */
	handoffRetainMs?: number;
	/** Default input dt (ms) when `sendInput` omits it. Default: 1000 / tickRate. */
	inputDtMs?: number;
	onListenerError?: (error: unknown) => void;
}>;

export type GameClientEvents = {
	authority: [string, WelcomeMessage];
	welcome: [string, WelcomeMessage];
	delta: [string, DeltaMessage];
	event: [string, EventMessage];
	freeze: [string, FreezeMessage];
	resume: [string, ResumeMessage];
	kick: [string, KickMessage];
	error: [string, ErrorMessage];
	handoff: [{ from: string; to: string; message: HandoffMessage }];
	handoffComplete: [{ from: string; to: string }];
	correction: [Correction];
	zoneClosed: [string, GameCloseInfo];
};

type Zone = {
	zoneId: string;
	role: ZoneRole;
	connection: GameConnection;
	replica: EntityReplica;
	offs: (() => void)[];
	firstDeltaSeen: boolean;
	dropTimer?: unknown;
};

export class GameClient<TInput = unknown> {
	readonly #options: GameClientOptions<TInput>;
	readonly #scheduler: GameScheduler;
	readonly #emitter: Emitter<GameClientEvents>;
	readonly #zones = new Map<string, Zone>();
	readonly #retired = new Map<string, Zone>();
	/** Connections superseded by a new one sharing their replica; not rendered. */
	readonly #priors = new Set<Zone>();
	readonly #predictor: Predictor<TInput> | undefined;
	#authority: Zone | undefined;
	#controlledOverride: number | undefined;
	#closed = false;

	constructor(options: GameClientOptions<TInput>) {
		this.#options = options;
		this.#scheduler = options.scheduler ?? defaultScheduler;
		this.#emitter = new Emitter(options.onListenerError);
		this.#predictor =
			options.prediction === undefined ? undefined : new Predictor(options.prediction);
	}

	on<K extends keyof GameClientEvents>(
		type: K,
		listener: (...args: GameClientEvents[K]) => void
	): () => void {
		return this.#emitter.on(type, listener);
	}

	/** The authoritative zone, if any. */
	get authority(): GameZoneView | undefined {
		return this.#authority;
	}

	get predictor(): Predictor<TInput> | undefined {
		return this.#predictor;
	}

	/** Every zone currently rendered (authority, observers, retired). */
	zones(): GameZoneView[] {
		return [...this.#zones.values(), ...this.#retired.values()];
	}

	zone(zoneId: string): GameZoneView | undefined {
		return this.#zones.get(zoneId) ?? this.#retired.get(zoneId);
	}

	/** Join `zoneId` as the player's authoritative zone. */
	connect(zoneId: string, initialTicket?: string): Promise<WelcomeMessage> {
		this.#assertOpen();
		const previous = this.#authority;
		if (previous?.zoneId === zoneId) return previous.connection.connect();
		const zone = this.#adopt(zoneId, 'authority', initialTicket);
		this.#authority = zone;
		if (previous !== undefined) this.#retire(previous, false);
		return zone.connection.connect();
	}

	/** Observe a neighbouring zone (read-only; merged into `render`). */
	observe(zoneId: string): Promise<WelcomeMessage> {
		this.#assertOpen();
		const existing = this.#zones.get(zoneId);
		if (existing !== undefined) return existing.connection.connect();
		return this.#adopt(zoneId, 'observer').connection.connect();
	}

	/** Stop observing a zone. */
	unobserve(zoneId: string): void {
		const zone = this.#zones.get(zoneId);
		if (zone === undefined || zone.role !== 'observer') return;
		this.#drop(zone);
	}

	/** Override which entity is controlled (e.g. announced by a game Event). */
	setControlledEntity(id: number | undefined): void {
		this.#controlledOverride = id;
		this.#bindControlled(true);
	}

	/**
	 * Send an input on the authoritative connection and apply it to the
	 * prediction. Returns the sequence number, or undefined if not sent.
	 */
	sendInput(input: TInput, dtMs?: number): number | undefined {
		const authority = this.#authority;
		const prediction = this.#options.prediction;
		if (authority === undefined || prediction === undefined) return undefined;
		const connection = authority.connection;
		if (!connection.canSendInput()) return undefined;
		const seq = connection.sendInput(prediction.encodeInput(input));
		if (seq === undefined) return undefined;
		const tickRate = connection.welcome?.tickRate || 30;
		this.#predictor!.record(seq, input, dtMs ?? this.#options.inputDtMs ?? 1000 / tickRate);
		return seq;
	}

	/** Send raw input bytes (no prediction). */
	sendRawInput(payload: Uint8Array): number | undefined {
		return this.#authority?.connection.sendInput(payload);
	}

	/** Render delay in effect for a zone. */
	interpolationDelay(zone: GameZoneView): number {
		const configured = this.#options.interpolationDelayMs;
		if (typeof configured === 'number') return configured;
		if (typeof configured === 'function') return configured(zone);
		const patchRate = zone.connection.welcome?.patchRate || 20;
		return 2 * (1000 / patchRate);
	}

	/**
	 * The merged world to draw now: every zone sampled at its own
	 * `serverNow - interpolationDelay`, the controlled entity at its predicted
	 * (present) state with correction smoothing.
	 */
	render(): RenderedEntity[] {
		const out: RenderedEntity[] = [];
		const now = this.#scheduler.now();
		const predictor = this.#predictor;
		const controlled =
			predictor?.active === true && this.#authority !== undefined ? predictor.entityId : undefined;
		for (const zone of this.zones()) {
			const renderTime = zone.connection.serverNow() - this.interpolationDelay(zone);
			const authoritative = zone === this.#authority;
			for (const entity of zone.replica.sample(renderTime)) {
				const isControlled = authoritative && entity.id === controlled && !entity.stale;
				out.push(
					rendered(zone.zoneId, entity, authoritative, isControlled, isControlled ? predictor!.current(now) : undefined)
				);
			}
		}
		return out;
	}

	/** Close every connection (graceful Leave on the authority). */
	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		for (const zone of [...this.#zones.values(), ...this.#retired.values(), ...this.#priors]) {
			this.#drop(zone);
		}
		this.#authority = undefined;
	}

	#assertOpen(): void {
		if (this.#closed) throw new Error('GameClient is closed');
	}

	#connectionOptions(zoneId: string, role: 'authority' | 'observer', initialTicket?: string): GameConnectionOptions {
		const o = this.#options;
		const observerTicket: GameTicketSource =
			o.observerTicket ?? ((request) => o.ticket({ ...request, role: 'observer' }));
		return {
			url: o.url(zoneId),
			zoneId,
			role: role === 'authority' ? 'player' : 'observer',
			ticket: role === 'authority' ? o.ticket : observerTicket,
			initialTicket,
			contentVersion: o.contentVersion,
			capabilities: o.capabilities,
			socket: o.socket,
			scheduler: this.#scheduler,
			reconnect: o.reconnect,
			pingIntervalMs: o.pingIntervalMs,
			onListenerError: o.onListenerError
		};
	}

	/**
	 * Create the zone entry for `zoneId` in `role`. An existing view of the
	 * same zone (observer, or retired after a handoff) hands its replica over
	 * so already-rendered entities continue without a gap.
	 */
	#adopt(zoneId: string, role: 'authority' | 'observer', initialTicket?: string): Zone {
		const prior = this.#zones.get(zoneId) ?? this.#retired.get(zoneId);
		const replica =
			prior?.replica ??
			new EntityReplica({
				interpolation: this.#options.interpolation,
				staleGraceMs: this.#options.staleGraceMs,
				historyMs: this.#options.historyMs
			});
		const connection = new GameConnection(this.#connectionOptions(zoneId, role, initialTicket));
		const zone: Zone = { zoneId, role, connection, replica, offs: [], firstDeltaSeen: false };
		if (prior !== undefined) {
			// The prior connection keeps feeding the shared replica until the
			// new one is welcomed, then it is closed.
			this.#zones.delete(zoneId);
			this.#retired.delete(zoneId);
			if (prior.dropTimer !== undefined) this.#scheduler.clearTimeout(prior.dropTimer);
			const off = connection.on('welcome', () => {
				off();
				this.#closeConnection(prior);
			});
			prior.role = 'retired';
			this.#priors.add(prior);
		}
		this.#zones.set(zoneId, zone);
		this.#wire(zone);
		return zone;
	}

	#wire(zone: Zone): void {
		const c = zone.connection;
		zone.offs.push(
			c.on('welcome', (welcome) => {
				zone.replica.reset(welcome.schema, c.serverNow());
				zone.firstDeltaSeen = false;
				if (zone === this.#authority) {
					this.#predictor?.resetSession();
					this.#bindControlled(true);
					this.#emitter.emit('authority', zone.zoneId, welcome);
				}
				this.#emitter.emit('welcome', zone.zoneId, welcome);
			}),
			c.on('delta', (delta) => this.#onDelta(zone, delta)),
			c.on('event', (event) => this.#emitter.emit('event', zone.zoneId, event)),
			c.on('freeze', (m) => this.#emitter.emit('freeze', zone.zoneId, m)),
			c.on('resume', (m) => this.#emitter.emit('resume', zone.zoneId, m)),
			c.on('kick', (m) => this.#emitter.emit('kick', zone.zoneId, m)),
			c.on('error', (m) => this.#emitter.emit('error', zone.zoneId, m)),
			c.on('handoff', (m) => this.#onHandoff(zone, m)),
			c.on('close', (info) => {
				this.#emitter.emit('zoneClosed', zone.zoneId, info);
			})
		);
	}

	#onDelta(zone: Zone, delta: DeltaMessage): void {
		zone.replica.applyDelta(delta, zone.connection.tickTime(delta.serverTick));
		if (!zone.firstDeltaSeen) {
			zone.firstDeltaSeen = true;
			if (zone === this.#authority) this.#scheduleRetiredDrop();
		}
		if (zone === this.#authority && this.#predictor !== undefined) {
			this.#bindControlled(false);
			const id = this.#predictor.entityId;
			if (id !== undefined) {
				const entity = zone.replica.get(id);
				if (entity !== undefined && !this.#predictor.active) {
					this.#predictor.bind(id, zone.replica.schema?.byId.get(entity.kind), entity.fields);
				}
				const correction = this.#predictor.reconcile(
					delta.ackSeq,
					entity?.fields,
					this.#scheduler.now()
				);
				if (correction !== undefined && Object.keys(correction.resolved).length > 0) {
					this.#emitter.emit('correction', correction);
				}
			}
		}
		this.#emitter.emit('delta', zone.zoneId, delta);
	}

	#bindControlled(force: boolean): void {
		const predictor = this.#predictor;
		const authority = this.#authority;
		if (predictor === undefined || authority === undefined) return;
		const welcome = authority.connection.welcome;
		let id = this.#controlledOverride;
		if (id === undefined && welcome !== undefined) {
			id = this.#options.prediction?.controlledEntity?.({
				zoneId: authority.zoneId,
				welcome,
				replica: authority.replica
			});
		}
		if (!force && id === predictor.entityId) return;
		const entity = id === undefined ? undefined : authority.replica.get(id);
		predictor.bind(
			id,
			entity === undefined ? undefined : authority.replica.schema?.byId.get(entity.kind),
			entity?.fields
		);
	}

	#onHandoff(zone: Zone, message: HandoffMessage): void {
		if (zone !== this.#authority) return;
		const from = zone.zoneId;
		const to = message.targetZone;
		this.#emitter.emit('handoff', { from, to, message });
		this.#controlledOverride = undefined;
		this.#predictor?.bind(undefined);
		const target = this.#adopt(to, 'authority', message.transferTicket);
		this.#authority = target;
		// Keep rendering the old zone until the new authority has content.
		this.#retire(zone, true);
		const off = target.connection.on('delta', () => {
			off();
			this.#emitter.emit('handoffComplete', { from, to });
		});
		void target.connection.connect().catch(() => undefined);
	}

	#retire(zone: Zone, keepRendering: boolean): void {
		if (this.#zones.get(zone.zoneId) === zone) this.#zones.delete(zone.zoneId);
		if (!keepRendering) {
			this.#drop(zone);
			return;
		}
		zone.role = 'retired';
		this.#retired.set(zone.zoneId, zone);
		if (this.#authority?.firstDeltaSeen) this.#scheduleRetiredDrop();
	}

	#scheduleRetiredDrop(): void {
		const retain = this.#options.handoffRetainMs ?? 500;
		for (const zone of this.#retired.values()) {
			if (zone.dropTimer !== undefined) continue;
			zone.dropTimer = this.#scheduler.setTimeout(() => {
				zone.dropTimer = undefined;
				if (this.#retired.get(zone.zoneId) === zone) this.#drop(zone);
			}, retain);
		}
	}

	#closeConnection(zone: Zone): void {
		for (const off of zone.offs) off();
		zone.offs = [];
		zone.connection.close({ leave: zone.role !== 'retired' });
		this.#priors.delete(zone);
		if (this.#retired.get(zone.zoneId) === zone) this.#retired.delete(zone.zoneId);
	}

	#drop(zone: Zone): void {
		if (zone.dropTimer !== undefined) this.#scheduler.clearTimeout(zone.dropTimer);
		this.#closeConnection(zone);
		if (this.#zones.get(zone.zoneId) === zone) this.#zones.delete(zone.zoneId);
	}
}

function rendered(
	zoneId: string,
	entity: ReplicaEntity,
	authoritative: boolean,
	predicted: boolean,
	predictedFields: EntityFields | undefined
): RenderedEntity {
	return {
		key: `${zoneId}:${entity.id}`,
		zoneId,
		id: entity.id,
		kind: entity.kind,
		kindName: entity.kindName,
		fields: predictedFields === undefined ? entity.fields : { ...entity.fields, ...predictedFields },
		authoritative,
		predicted: predicted && predictedFields !== undefined,
		stale: entity.stale
	};
}
