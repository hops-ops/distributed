/**
 * Svelte 5 runes adapter for the UI *around* a game: connection status,
 * RTT, current zone, prediction readiness and typed event subscriptions
 * (decoded with the generated `EVENTS` codecs) for a generated
 * `create<Name>Client` or a plain `GameClient`.
 *
 * Every getter is reactive in runes mode (`createSubscriber`), so templates
 * and `$derived` read it directly; outside an effect it is a plain read. The
 * render loop stays outside Svelte: it reads `state.client.render()` from its
 * own `requestAnimationFrame`, never through reactivity.
 */
import { onMount } from 'svelte';
import { createSubscriber } from 'svelte/reactivity';

import { onGameEvent } from '../../game/typed.js';
import type {
	GameEventCodecs,
	GameClient,
	GameConnection,
	GameConnectionState,
	GameTicketSource,
	WelcomeMessage
} from '../../game/index.js';

/** A generated client (`create<Name>Client`) or a bare `GameClient`. */
export type GameStateSource =
	| GameClient<any>
	| Readonly<{
			client: GameClient<any>;
			prediction?: Readonly<{ ready: Promise<unknown> }> | undefined;
	  }>;

export type GameStatus = GameConnectionState;

/** Latest value of one typed event; `current` is reactive. */
export type GameLatest<T> = Readonly<{ readonly current: T | undefined }>;

/** Newest-first bounded list of one typed event; `items` is reactive. */
export type GameEventList<T> = Readonly<{ readonly items: readonly T[] }>;

export type GameStateOptions<TEvents> = Readonly<{
	/**
	 * The surface's event codecs (generated `EVENTS`): types `latest`,
	 * `events` and `on` by event name.
	 */
	events?: GameEventCodecs<TEvents>;
	/** Report errors thrown while connecting (default: kept in `error`). */
	onConnectError?: (error: unknown) => void;
}>;

/** A value whose reads subscribe the active Svelte effect. */
class Cell<T> {
	#value: T;
	readonly #updates = new Set<() => void>();
	readonly #subscribe = createSubscriber((update) => {
		this.#updates.add(update);
		return () => {
			this.#updates.delete(update);
		};
	});

	constructor(value: T) {
		this.#value = value;
	}

	get(): T {
		this.#subscribe();
		return this.#value;
	}

	peek(): T {
		return this.#value;
	}

	set(value: T): void {
		if (Object.is(value, this.#value)) return;
		this.#value = value;
		for (const update of [...this.#updates]) update();
	}
}

type AnySource = GameStateSource;

function clientOf(source: AnySource): GameClient<any> {
	return 'client' in source && typeof source.client === 'object' ? source.client : (source as GameClient<any>);
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/**
 * Reactive state for one game client. Create it during component setup;
 * the client itself is built lazily by `connect` (so nothing runs during
 * SSR) or adopted from `source`.
 */
export class GameState<TSource extends AnySource = AnySource, TEvents = Record<string, never>> {
	readonly #factory: () => TSource;
	readonly #options: GameStateOptions<TEvents>;
	readonly #source = new Cell<TSource | undefined>(undefined);
	readonly #status = new Cell<GameStatus>('idle');
	readonly #zone = new Cell<string | undefined>(undefined);
	readonly #rtt = new Cell<number | undefined>(undefined);
	readonly #jitter = new Cell<number | undefined>(undefined);
	readonly #predictionReady = new Cell(false);
	readonly #error = new Cell<string | undefined>(undefined);
	readonly #attachers: Array<(source: TSource) => () => void> = [];
	readonly #disposers: Array<() => void> = [];
	#connectionOffs: Array<() => void> = [];
	#disposed = false;

	constructor(source: TSource | (() => TSource), options: GameStateOptions<TEvents> = {}) {
		this.#factory = typeof source === 'function' ? (source as () => TSource) : () => source;
		this.#options = options;
	}

	/** The generated client (or `GameClient`) once created. */
	get source(): TSource | undefined {
		return this.#source.get();
	}

	/** The underlying `GameClient` once created (render loops read `render()` from it). */
	get client(): GameClient<any> | undefined {
		const source = this.#source.get();
		return source === undefined ? undefined : clientOf(source);
	}

	/** Authority connection state: `idle` → `connecting` → `open` (→ `reconnecting` …). */
	get status(): GameStatus {
		return this.#status.get();
	}

	/** True while the authority connection is open. */
	get connected(): boolean {
		return this.#status.get() === 'open';
	}

	/** Zone the player is in (follows handoffs). */
	get zone(): string | undefined {
		return this.#zone.get();
	}

	/** Round-trip time of the authority connection (ms), after the first pong. */
	get rttMs(): number | undefined {
		return this.#rtt.get();
	}

	/** RTT jitter estimate (ms). */
	get jitterMs(): number | undefined {
		return this.#jitter.get();
	}

	/** True once the compiled prediction step is loaded (false without prediction). */
	get predictionReady(): boolean {
		return this.#predictionReady.get();
	}

	/** Last refusal, kick or connection failure, cleared by the next Welcome. */
	get error(): string | undefined {
		return this.#error.get();
	}

	/** Latest value of a typed event. */
	latest<K extends keyof TEvents & string>(name: K): GameLatest<TEvents[K]> {
		const cell = new Cell<TEvents[K] | undefined>(undefined);
		this.#subscribeEvent(name, (value) => cell.set(value));
		return Object.freeze({
			get current() {
				return cell.get();
			}
		});
	}

	/** The last `limit` values of a typed event, newest first. */
	events<K extends keyof TEvents & string>(
		name: K,
		options: Readonly<{ limit?: number }> = {}
	): GameEventList<TEvents[K]> {
		const limit = options.limit ?? 20;
		if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError('events limit must be >= 1');
		const cell = new Cell<readonly TEvents[K][]>(Object.freeze([]));
		this.#subscribeEvent(name, (value) => {
			cell.set(Object.freeze([value, ...cell.peek()].slice(0, limit)));
		});
		return Object.freeze({
			get items() {
				return cell.get();
			}
		});
	}

	/** Subscribe a callback to a typed event (unsubscribed by `dispose`). */
	on<K extends keyof TEvents & string>(name: K, handler: (value: TEvents[K], zoneId: string) => void): () => void {
		return this.#subscribeEvent(name, handler);
	}

	/** Create the client if needed and join `zone`. */
	async connect(zone: string): Promise<WelcomeMessage> {
		if (this.#disposed) throw new Error('GameState is disposed');
		const source = this.#ensureSource();
		const client = clientOf(source);
		this.#error.set(undefined);
		this.#status.set('connecting');
		const pending = client.connect(zone);
		this.#watchConnection(client.zone(zone)?.connection);
		try {
			return await pending;
		} catch (error) {
			if (!this.#disposed) {
				this.#error.set(describe(error));
				this.#status.set(client.zone(zone)?.connection.state ?? 'closed');
			}
			this.#options.onConnectError?.(error);
			throw error;
		}
	}

	/** Close every connection and drop every subscription (component destroy / navigation). */
	dispose(): void {
		if (this.#disposed) return;
		this.#disposed = true;
		for (const off of this.#connectionOffs) off();
		this.#connectionOffs = [];
		for (const dispose of this.#disposers.splice(0)) dispose();
		const source = this.#source.peek();
		if (source !== undefined) clientOf(source).close();
		this.#status.set('closed');
	}

	#ensureSource(): TSource {
		const existing = this.#source.peek();
		if (existing !== undefined) return existing;
		const source = this.#factory();
		const client = clientOf(source);
		this.#disposers.push(
			client.on('authority', (zoneId) => {
				this.#zone.set(zoneId);
				this.#watchConnection(client.zone(zoneId)?.connection);
			}),
			client.on('welcome', (zoneId) => {
				if (client.authority?.zoneId === zoneId) this.#error.set(undefined);
			}),
			client.on('error', (zoneId, message) => {
				if (client.authority === undefined || client.authority.zoneId === zoneId) {
					this.#error.set(message.message || `error ${message.code}`);
				}
			}),
			client.on('kick', (_zoneId, message) => {
				this.#error.set(message.message || `kicked (${message.code})`);
			})
		);
		const prediction = 'client' in source ? source.prediction : undefined;
		prediction?.ready.then(
			() => this.#predictionReady.set(true),
			(error: unknown) => this.#error.set(`prediction unavailable: ${describe(error)}`)
		);
		for (const attach of this.#attachers) this.#disposers.push(attach(source));
		this.#source.set(source);
		return source;
	}

	#watchConnection(connection: GameConnection | undefined): void {
		if (connection === undefined) return;
		for (const off of this.#connectionOffs) off();
		this.#status.set(connection.state);
		this.#connectionOffs = [
			connection.on('state', (state) => this.#status.set(state)),
			connection.on('pong', ({ rtt, jitter }) => {
				this.#rtt.set(Math.round(rtt));
				this.#jitter.set(Math.round(jitter));
			})
		];
	}

	#subscribeEvent<K extends keyof TEvents & string>(
		name: K,
		handler: (value: TEvents[K], zoneId: string) => void
	): () => void {
		const codecs = this.#options.events;
		if (codecs === undefined || codecs[name] === undefined) {
			throw new TypeError(`unknown game event \`${name}\`: pass the generated EVENTS as \`events\``);
		}
		let off: (() => void) | undefined;
		let cancelled = false;
		const attach = (source: TSource): (() => void) => {
			if (cancelled) return () => undefined;
			off = onGameEvent(clientOf(source), codecs, name, handler);
			return () => off?.();
		};
		const existing = this.#source.peek();
		if (existing === undefined) {
			this.#attachers.push(attach);
		} else {
			this.#disposers.push(attach(existing));
		}
		return () => {
			cancelled = true;
			off?.();
		};
	}
}

/** Reactive state for `source` (or a factory, called on the first `connect`). */
export function createGameState<TSource extends AnySource, TEvents = Record<string, never>>(
	source: TSource | (() => TSource),
	options?: GameStateOptions<TEvents>
): GameState<TSource, TEvents> {
	return new GameState(source, options);
}

export type UseGameOptions<TEvents> = GameStateOptions<TEvents> &
	Readonly<{
		/** Zone to join on mount (e.g. from the page's server `load`). */
		zone: string | (() => string);
	}>;

/**
 * `createGameState` bound to the component lifecycle: the client is created
 * and joins `zone` on mount (browser only) and is closed on destroy, which
 * includes navigating away. Call it during component initialisation.
 *
 * ```svelte
 * <script lang="ts">
 *   const game = useGame(() => createMobpitClient({ url, ticket: gameTicketSource() }), {
 *     zone: data.zone,
 *     events: EVENTS
 *   });
 *   const profile = game.latest('profile');
 * </script>
 * <p>{game.status} · {game.rttMs ?? '–'} ms · XP {profile.current?.xp ?? 0}</p>
 * ```
 */
export function useGame<TSource extends AnySource, TEvents = Record<string, never>>(
	factory: () => TSource,
	options: UseGameOptions<TEvents>
): GameState<TSource, TEvents> {
	const state = new GameState(factory, options);
	onMount(() => {
		const zone = typeof options.zone === 'function' ? options.zone() : options.zone;
		state.connect(zone).catch(() => undefined);
		return () => state.dispose();
	});
	return state;
}

export type GameTicketSourceOptions = Readonly<{
	/** Same-origin ticket route (`createGameTicketHandler`). Default `/api/game/ticket`. */
	endpoint?: string;
	fetch?: typeof fetch;
}>;

/**
 * `GameClient` ticket source backed by a `createGameTicketHandler` route:
 * POSTs `{ zone, role }` with the session cookie; identity comes from the
 * server session, never from this request.
 */
export function gameTicketSource(options: GameTicketSourceOptions = {}): GameTicketSource {
	const endpoint = options.endpoint ?? '/api/game/ticket';
	return async ({ zoneId, role }) => {
		const request = options.fetch ?? globalThis.fetch;
		const response = await request(endpoint, {
			method: 'POST',
			credentials: 'same-origin',
			cache: 'no-store',
			headers: { 'content-type': 'application/json', accept: 'application/json' },
			body: JSON.stringify({ zone: zoneId, role })
		});
		let body: { ticket?: unknown; error?: unknown; message?: unknown } = {};
		try {
			body = (await response.json()) as typeof body;
		} catch {
			// Non-JSON error page; reported by status below.
		}
		if (!response.ok || typeof body.ticket !== 'string') {
			const reason = typeof body.message === 'string' ? body.message : `HTTP ${response.status}`;
			throw new Error(`game ticket refused: ${reason}`);
		}
		return body.ticket;
	};
}
