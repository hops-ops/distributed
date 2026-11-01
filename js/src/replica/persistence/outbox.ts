/**
 * Durable offline command outbox.
 *
 * A transport decorator for the replica command runtime: an exact prepared
 * command request is written to IndexedDB *before* it is sent, and removed
 * only once the server returned a definitive answer. Entries that could not
 * be sent (offline, network failure, 5xx, reload gate) stay durable and are
 * drained strictly in enqueue order later — after a reconnect or a page
 * reload — reusing the original command id, so the server's command
 * receipts deduplicate any resend of a command it already processed.
 *
 * A live caller (the runtime's pending dispatch) keeps waiting until its
 * command gets a definitive server answer, so the runtime's normal receipt,
 * rejection and optimistic-layer handling apply unchanged; it can stop
 * waiting through its AbortSignal. Commands replayed after a reload have no
 * live caller and settle through `onSettled`.
 *
 * Only the immutable transport request is stored. Optimistic layers and
 * unconfirmed cache state are never persisted (the confirmed-only policy is
 * unchanged); after a reload the replica learns the effects of drained
 * commands through normal revalidation.
 *
 * Durability is opt-in per command and fails closed like model policy: a
 * command is written only when its policy is explicitly
 * `{ durable: true, sensitive: false }`. Everything else passes straight
 * through to the network transport.
 */
import { parseGraphqlResponseExtensions } from '../../protocol.js';
import type {
	ReplicaCommandTransport,
	ReplicaCommandTransportRequest,
	ReplicaCommandTransportResult
} from '../command-runtime/types.js';
import type { ReplicaAuthoritativeScope } from '../types.js';

export type ReplicaCommandOutboxCommandPolicy = Readonly<{
	/** Persist and replay this command when it cannot be sent right away. */
	durable: boolean;
	/** Sensitive command inputs are never written to durable storage. */
	sensitive: boolean;
}>;

export type ReplicaCommandOutboxPolicy = Readonly<{
	/** Keyed by generated command name (`request.commandName`). */
	commands: Readonly<Record<string, ReplicaCommandOutboxCommandPolicy>>;
}>;

export type ReplicaCommandOutboxOutcome = 'accepted' | 'rejected' | 'failed';

export type ReplicaCommandOutboxSettlement = Readonly<{
	commandId: string;
	commandName: string;
	/**
	 * `accepted`: the server holds a receipt (any non-rejected command state).
	 * `rejected`: the domain rejected it. `failed`: a non-retryable protocol
	 * or request error (malformed envelope, 4xx).
	 */
	outcome: ReplicaCommandOutboxOutcome;
	result: ReplicaCommandTransportResult;
	/** True when an in-memory caller was still awaiting this dispatch. */
	live: boolean;
	/** Send attempts including this one. */
	attempts: number;
}>;

export type ReplicaCommandOutboxDrainReport = Readonly<{
	settled: readonly ReplicaCommandOutboxSettlement[];
	/** Entries still queued for this scope (drain stops at the first unsendable). */
	remaining: number;
	/** Why draining stopped early, if it did. */
	stoppedBy?: 'offline' | 'transport-error' | 'retryable-response';
}>;

export type ReplicaCommandOutboxEntrySummary = Readonly<{
	commandId: string;
	commandName: string;
	enqueuedAt: number;
	attempts: number;
}>;

export type ReplicaCommandOutboxOptions = Readonly<{
	/** The network transport the outbox decorates. */
	transport: ReplicaCommandTransport;
	/**
	 * Current authoritative scope. Entries are bound to protocol version,
	 * schema hash and cache scope and only ever drained under the same
	 * values, so a queued command never replays for another principal or
	 * an incompatible schema. `undefined` disables durability.
	 */
	scope: () => ReplicaAuthoritativeScope | undefined;
	policy: ReplicaCommandOutboxPolicy;
	/** Defaults to `navigator.onLine` (true when unavailable). */
	isOnline?: () => boolean;
	/** Every definitive outcome, including commands replayed after reload. */
	onSettled?: (settlement: ReplicaCommandOutboxSettlement) => void;
}>;

export interface ReplicaCommandOutbox {
	/** Pass to `createReplicaCommandRuntime` in place of the network transport. */
	readonly transport: ReplicaCommandTransport;
	/** Send queued commands of the current scope in order. Call on reconnect. */
	drain(): Promise<ReplicaCommandOutboxDrainReport>;
	/** Queued commands of the current scope, oldest first. */
	pending(): Promise<readonly ReplicaCommandOutboxEntrySummary[]>;
	/** Remove one queued command without sending it (a live caller rejects). */
	discard(commandId: string): Promise<void>;
	/** Remove every queued command bound to `scope` (e.g. on sign-out). */
	discardScope(scope: ReplicaAuthoritativeScope): Promise<void>;
}

/** Storage seam (IndexedDB in production). */
export interface ReplicaCommandOutboxStore {
	getAll(): Promise<readonly unknown[]>;
	get(commandId: string): Promise<unknown | undefined>;
	put(entry: StoredOutboxEntry): Promise<void>;
	delete(commandId: string): Promise<void>;
}

type StoredRequest = Omit<ReplicaCommandTransportRequest, 'signal'>;

export type StoredOutboxEntry = Readonly<{
	formatVersion: 1;
	commandId: string;
	order: number;
	scopeKey: string;
	request: StoredRequest;
	enqueuedAt: number;
	attempts: number;
}>;

export const OUTBOX_STORE_NAME = 'command-outbox';

type Waiter = {
	resolve: (result: ReplicaCommandTransportResult) => void;
	reject: (error: unknown) => void;
};

/**
 * Outbox binding key. It deliberately excludes the authorization
 * generation: a token refresh for the same cache scope must not strand
 * queued commands, and the server binds the principal from the verified
 * session at send time, never from the stored request.
 */
export function outboxScopeKey(scope: ReplicaAuthoritativeScope): string {
	return JSON.stringify([
		'distributed-command-outbox',
		1,
		scope.protocolVersion,
		scope.schemaHash,
		scope.cacheScope
	]);
}

function defaultIsOnline(): boolean {
	const nav = (globalThis as { navigator?: { onLine?: boolean } }).navigator;
	return nav?.onLine ?? true;
}

function parseEntry(raw: unknown): StoredOutboxEntry | undefined {
	if (raw === null || typeof raw !== 'object') return undefined;
	const entry = raw as Record<string, unknown>;
	const request = entry.request as Record<string, unknown> | null | undefined;
	if (
		entry.formatVersion !== 1 ||
		typeof entry.commandId !== 'string' ||
		typeof entry.order !== 'number' ||
		typeof entry.scopeKey !== 'string' ||
		typeof entry.enqueuedAt !== 'number' ||
		typeof entry.attempts !== 'number' ||
		request === undefined ||
		request === null ||
		typeof request !== 'object' ||
		request.operation !== 'mutation' ||
		request.commandId !== entry.commandId ||
		typeof request.commandName !== 'string' ||
		typeof request.document !== 'string' ||
		typeof request.operationHash !== 'string' ||
		typeof request.mutationField !== 'string'
	) {
		return undefined;
	}
	return entry as unknown as StoredOutboxEntry;
}

/**
 * Classify a transport result. `retry` keeps the command queued: the
 * server took no durable decision (5xx, 408/429, reload gate, `unknown`).
 */
export function classifyOutboxResult(
	result: ReplicaCommandTransportResult
): ReplicaCommandOutboxOutcome | 'retry' {
	let state: string | undefined;
	try {
		state = parseGraphqlResponseExtensions(result.extensions)?.distributed?.command?.state;
	} catch {
		return 'failed';
	}
	if (state !== undefined) {
		if (state === 'rejected') return 'rejected';
		if (state === 'unknown') return 'retry';
		return 'accepted';
	}
	const errors = result.errors ?? [];
	if (errors.length > 0 && errors.every((e) => e.extensions?.code === 'REJECTED')) {
		return 'rejected';
	}
	if (result.status >= 500 || result.status === 408 || result.status === 429) return 'retry';
	if (errors.some((e) => e.extensions?.code === 'APPLICATION_RELOADING')) return 'retry';
	return 'failed';
}

export class ReplicaCommandOutboxImpl implements ReplicaCommandOutbox {
	readonly transport: ReplicaCommandTransport;
	readonly #store: ReplicaCommandOutboxStore;
	readonly #options: ReplicaCommandOutboxOptions;
	readonly #waiters = new Map<string, Waiter>();
	#lastOrder: number | undefined;
	#queue: Promise<unknown> = Promise.resolve();

	constructor(store: ReplicaCommandOutboxStore, options: ReplicaCommandOutboxOptions) {
		if (typeof options?.transport?.dispatch !== 'function') {
			throw new TypeError('command outbox requires a transport');
		}
		if (typeof options.scope !== 'function') {
			throw new TypeError('command outbox requires a scope() provider');
		}
		if (options.policy === undefined || typeof options.policy.commands !== 'object') {
			throw new TypeError('command outbox requires an explicit command policy');
		}
		this.#store = store;
		this.#options = options;
		const inner = options.transport;
		this.transport = Object.freeze({
			dispatch: (request: ReplicaCommandTransportRequest) => this.#dispatch(request),
			...(inner.status === undefined ? {} : { status: inner.status.bind(inner) })
		});
	}

	drain(): Promise<ReplicaCommandOutboxDrainReport> {
		return this.#serialize(() => this.#drainNow());
	}

	async pending(): Promise<readonly ReplicaCommandOutboxEntrySummary[]> {
		const scope = this.#options.scope();
		if (scope === undefined) return [];
		const entries = await this.#serialize(() => this.#entries(outboxScopeKey(scope)));
		return entries.map((e) => ({
			commandId: e.commandId,
			commandName: e.request.commandName,
			enqueuedAt: e.enqueuedAt,
			attempts: e.attempts
		}));
	}

	discard(commandId: string): Promise<void> {
		return this.#serialize(async () => {
			await this.#store.delete(commandId);
			this.#rejectWaiter(commandId, new Error('queued command discarded before delivery'));
		});
	}

	discardScope(scope: ReplicaAuthoritativeScope): Promise<void> {
		return this.#serialize(async () => {
			for (const entry of await this.#entries(outboxScopeKey(scope))) {
				await this.#store.delete(entry.commandId);
				this.#rejectWaiter(entry.commandId, new Error('queued command discarded with its scope'));
			}
		});
	}

	#rejectWaiter(commandId: string, error: unknown): void {
		const waiter = this.#waiters.get(commandId);
		if (waiter === undefined) return;
		this.#waiters.delete(commandId);
		waiter.reject(error);
	}

	#durable(request: ReplicaCommandTransportRequest): boolean {
		const policy = this.#options.policy.commands[request.commandName];
		return policy?.durable === true && policy.sensitive === false;
	}

	#online(): boolean {
		return (this.#options.isOnline ?? defaultIsOnline)();
	}

	async #dispatch(request: ReplicaCommandTransportRequest): Promise<ReplicaCommandTransportResult> {
		const scope = this.#options.scope();
		if (scope === undefined || !this.#durable(request)) {
			return this.#options.transport.dispatch(request);
		}
		if (this.#waiters.has(request.commandId)) {
			throw new Error(`command ${request.commandId} is already awaiting delivery`);
		}
		const result = new Promise<ReplicaCommandTransportResult>((resolve, reject) => {
			this.#waiters.set(request.commandId, { resolve, reject });
		});
		// Persist before any network attempt; a crash after this point
		// replays the command on the next drain.
		try {
			await this.#serialize(() => this.#enqueue(request, outboxScopeKey(scope)));
		} catch (error) {
			this.#waiters.delete(request.commandId);
			throw error;
		}
		const signal = request.signal;
		if (signal !== undefined) {
			const onAbort = () => {
				void this.#serialize(async () => {
					if (!this.#waiters.has(request.commandId)) return; // settled already
					const stored = parseEntry(await this.#store.get(request.commandId));
					// Never sent: cancel it. Sent without an answer: the outcome
					// is unknown, so keep it; a replay is deduplicated.
					if (stored !== undefined && stored.attempts === 0) {
						await this.#store.delete(request.commandId);
					}
					this.#rejectWaiter(
						request.commandId,
						signal.reason ?? new Error('command request aborted')
					);
				});
			};
			if (signal.aborted) onAbort();
			else signal.addEventListener('abort', onAbort, { once: true });
		}
		if (this.#online()) void this.drain().catch(() => undefined);
		return result;
	}

	async #enqueue(request: ReplicaCommandTransportRequest, scopeKey: string): Promise<void> {
		const existing = parseEntry(await this.#store.get(request.commandId));
		if (existing !== undefined) return; // runtime transport retry of the same command
		if (this.#lastOrder === undefined) {
			const all = await this.#store.getAll();
			this.#lastOrder = all.reduce<number>((max, raw) => {
				const entry = parseEntry(raw);
				return entry === undefined ? max : Math.max(max, entry.order);
			}, 0);
		}
		this.#lastOrder += 1;
		const { signal: _signal, ...stored } = request;
		await this.#store.put(
			Object.freeze({
				formatVersion: 1 as const,
				commandId: request.commandId,
				order: this.#lastOrder,
				scopeKey,
				request: structuredClone(stored) as StoredRequest,
				enqueuedAt: Date.now(),
				attempts: 0
			})
		);
	}

	async #entries(scopeKey: string): Promise<StoredOutboxEntry[]> {
		const out: StoredOutboxEntry[] = [];
		for (const raw of await this.#store.getAll()) {
			const entry = parseEntry(raw);
			if (entry === undefined) {
				// Corrupt entries are never replayed.
				const id = (raw as { commandId?: unknown } | null)?.commandId;
				if (typeof id === 'string') await this.#store.delete(id);
				continue;
			}
			if (entry.scopeKey === scopeKey) out.push(entry);
		}
		return out.sort((a, b) => a.order - b.order);
	}

	async #drainNow(): Promise<ReplicaCommandOutboxDrainReport> {
		const scope = this.#options.scope();
		if (scope === undefined) return { settled: [], remaining: 0 };
		const entries = await this.#entries(outboxScopeKey(scope));
		const settled: ReplicaCommandOutboxSettlement[] = [];
		for (let index = 0; index < entries.length; index += 1) {
			const remaining = entries.length - index;
			if (!this.#online()) return { settled, remaining, stoppedBy: 'offline' };
			const entry = entries[index];
			const attempts = entry.attempts + 1;
			// Record the attempt first: an abort after this point keeps the
			// entry because the server may already have processed it.
			await this.#store.put({ ...entry, attempts });
			let result: ReplicaCommandTransportResult;
			try {
				result = await this.#options.transport.dispatch(Object.freeze({ ...entry.request }));
			} catch {
				// Outcome unknown (offline, connection reset): keep it queued.
				return { settled, remaining, stoppedBy: 'transport-error' };
			}
			const outcome = classifyOutboxResult(result);
			if (outcome === 'retry') {
				// No durable server decision yet: keep it queued, in order.
				return { settled, remaining, stoppedBy: 'retryable-response' };
			}
			await this.#store.delete(entry.commandId);
			const waiter = this.#waiters.get(entry.commandId);
			this.#waiters.delete(entry.commandId);
			const settlement: ReplicaCommandOutboxSettlement = Object.freeze({
				commandId: entry.commandId,
				commandName: entry.request.commandName,
				outcome,
				result,
				live: waiter !== undefined,
				attempts
			});
			settled.push(settlement);
			// The live caller gets the exact result; the runtime turns a
			// rejection into REPLICA_COMMAND_REJECTED and rolls back its layer.
			waiter?.resolve(result);
			try {
				this.#options.onSettled?.(settlement);
			} catch {
				// Observer failures never stall the outbox.
			}
		}
		return { settled, remaining: 0 };
	}

	#serialize<T>(work: () => Promise<T>): Promise<T> {
		const run = this.#queue.then(work, work);
		this.#queue = run.catch(() => undefined);
		return run;
	}
}
