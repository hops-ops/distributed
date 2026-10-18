import type {
	ReplicaAuthoritativeScope,
	ReplicaDehydratedState
} from '../types.js';
import type {
	ReplicaIndexedDbFactory,
	ReplicaIndexedDbPersistence,
	ReplicaIndexedDbPersistenceOptions
} from './types.js';
import {
	OUTBOX_STORE_NAME,
	ReplicaCommandOutboxImpl,
	type ReplicaCommandOutbox,
	type ReplicaCommandOutboxOptions,
	type StoredOutboxEntry
} from './outbox.js';
import {
	DATABASE_VERSION,
	DEFAULT_DATABASE_NAME,
	STORE_NAME,
	filterReplicaState,
	normalizePolicy,
	parseAuthoritativeScope,
	parseReplicaState,
	parseStoredEntry,
	persistenceIdentity,
	sameScope,
	type NormalizedPolicy,
	type StoredReplicaEntry
} from './state.js';

export class IndexedDbConfirmedStatePersistence
	implements ReplicaIndexedDbPersistence
{
	readonly supportsOfflineCommandOutbox = true as const;
	readonly #factory: ReplicaIndexedDbFactory;
	readonly #databaseName: string;
	readonly #policy: NormalizedPolicy;
	#databasePromise: Promise<IDBDatabase> | undefined;
	#closed = false;

	constructor(
		factory: ReplicaIndexedDbFactory,
		databaseName: string,
		policy: NormalizedPolicy
	) {
		this.#factory = factory;
		this.#databaseName = databaseName;
		this.#policy = policy;
	}

	async save(state: ReplicaDehydratedState): Promise<boolean> {
		this.#assertOpen();
		const parsed = parseReplicaState(state);
		const filtered = filterReplicaState(parsed, this.#policy);
		const identity = persistenceIdentity(parsed.scope);
		if (filtered === undefined) {
			// A newly restrictive policy must not leave a previously durable copy.
			await this.#delete(identity);
			return false;
		}
		const entry: StoredReplicaEntry = Object.freeze({
			formatVersion: 1 as const,
			identity,
			storedAt: Date.now(),
			state: filtered
		});
		await this.#write(entry);
		return true;
	}

	async restore(
		authoritativeScope: ReplicaAuthoritativeScope
	): Promise<ReplicaDehydratedState | undefined> {
		this.#assertOpen();
		const scope = parseAuthoritativeScope(authoritativeScope);
		const identity = persistenceIdentity(scope);
		const raw = await this.#read(identity);
		if (raw === undefined) return undefined;

		try {
			const entry = parseStoredEntry(raw);
			const parsed = parseReplicaState(entry.state);
			if (
				entry.identity !== identity ||
				!sameScope(parsed.scope, scope)
			) {
				throw new TypeError('persisted replica scope does not match its key');
			}
			const filtered = filterReplicaState(parsed, this.#policy);
			if (filtered === undefined) {
				await this.#delete(identity);
				return undefined;
			}
			// Reapply the current policy to the durable copy as well as the value
			// returned to memory. This removes data made memory-only by a newer
			// manifest instead of leaving it at rest under an older policy.
			await this.#write(
				Object.freeze({
					formatVersion: 1 as const,
					identity,
					storedAt: entry.storedAt,
					state: filtered
				})
			);
			return filtered;
		} catch {
			// Never consume a questionable snapshot. Deletion is best-effort so an
			// IndexedDB failure cannot turn corrupt data into accepted state.
			try {
				await this.#delete(identity);
			} catch {
				// A later restore will reject the same entry again.
			}
			return undefined;
		}
	}

	async discard(authoritativeScope: ReplicaAuthoritativeScope): Promise<void> {
		this.#assertOpen();
		await this.#delete(
			persistenceIdentity(parseAuthoritativeScope(authoritativeScope))
		);
	}

	commandOutbox(options: ReplicaCommandOutboxOptions): ReplicaCommandOutbox {
		this.#assertOpen();
		const run = <T>(
			mode: IDBTransactionMode,
			operation: (store: IDBObjectStore) => Promise<T>
		) => this.#transaction(mode, operation, OUTBOX_STORE_NAME);
		return new ReplicaCommandOutboxImpl(
			{
				getAll: () =>
					run('readonly', (store) => requestResult(store.getAll() as IDBRequest<unknown[]>)),
				get: (commandId) =>
					run('readonly', (store) => requestResult(store.get(commandId) as IDBRequest<unknown>)),
				put: async (entry: StoredOutboxEntry) => {
					await run('readwrite', async (store) => {
						await requestResult(store.put(entry));
					});
				},
				delete: async (commandId) => {
					await run('readwrite', async (store) => {
						await requestResult(store.delete(commandId));
					});
				}
			},
			options
		);
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		const pending = this.#databasePromise;
		if (pending !== undefined) {
			void pending.then(
				(database) => database.close(),
				() => undefined
			);
		}
	}

	async #read(identity: string): Promise<unknown | undefined> {
		return this.#transaction('readonly', (store) =>
			requestResult(store.get(identity))
		);
	}

	async #write(entry: StoredReplicaEntry): Promise<void> {
		await this.#transaction('readwrite', async (store) => {
			await requestResult(store.put(entry));
		});
	}

	async #delete(identity: string): Promise<void> {
		await this.#transaction('readwrite', async (store) => {
			await requestResult(store.delete(identity));
		});
	}

	async #transaction<T>(
		mode: IDBTransactionMode,
		operation: (store: IDBObjectStore) => Promise<T>,
		storeName: string = STORE_NAME
	): Promise<T> {
		const database = await this.#database();
		this.#assertOpen();
		const transaction = database.transaction(storeName, mode);
		const completion = transactionResult(transaction);
		try {
			const result = await operation(transaction.objectStore(storeName));
			await completion;
			return result;
		} catch (error) {
			try {
				transaction.abort();
			} catch {
				// It may already have completed or aborted.
			}
			await completion.catch(() => undefined);
			throw error;
		}
	}

	#database(): Promise<IDBDatabase> {
		this.#assertOpen();
		this.#databasePromise ??= openDatabase(
			this.#factory,
			this.#databaseName
		);
		return this.#databasePromise;
	}

	#assertOpen(): void {
		if (this.#closed) {
			throw new Error('replica IndexedDB persistence is closed');
		}
	}
}

/**
 * Create opt-in confirmed-state persistence. Merely creating a replica never
 * opens IndexedDB; applications must explicitly create and call this adapter.
 */
export function createReplicaIndexedDbPersistence(
	options: ReplicaIndexedDbPersistenceOptions = {}
): ReplicaIndexedDbPersistence {
	const factory =
		options.indexedDB ??
		(globalThis as { indexedDB?: ReplicaIndexedDbFactory }).indexedDB;
	if (factory === undefined || typeof factory.open !== 'function') {
		throw new TypeError('IndexedDB is unavailable in this runtime');
	}
	const databaseName = options.databaseName ?? DEFAULT_DATABASE_NAME;
	if (typeof databaseName !== 'string' || databaseName.length === 0) {
		throw new TypeError('replica persistence databaseName must be non-empty');
	}
	return new IndexedDbConfirmedStatePersistence(
		factory,
		databaseName,
		normalizePolicy(options.policy)
	);
}

export function openDatabase(
	factory: ReplicaIndexedDbFactory,
	name: string
): Promise<IDBDatabase> {
	return new Promise((resolve, reject) => {
		let settled = false;
		const request = factory.open(name, DATABASE_VERSION);
		request.onupgradeneeded = () => {
			const database = request.result;
			if (!database.objectStoreNames.contains(STORE_NAME)) {
				database.createObjectStore(STORE_NAME, { keyPath: 'identity' });
			}
			if (!database.objectStoreNames.contains(OUTBOX_STORE_NAME)) {
				database.createObjectStore(OUTBOX_STORE_NAME, { keyPath: 'commandId' });
			}
		};
		request.onsuccess = () => {
			if (settled) {
				request.result.close();
				return;
			}
			settled = true;
			resolve(request.result);
		};
		request.onerror = () => {
			if (settled) return;
			settled = true;
			reject(request.error ?? new Error('failed to open replica IndexedDB'));
		};
		request.onblocked = () => {
			if (settled) return;
			settled = true;
			reject(new Error('replica IndexedDB upgrade is blocked'));
		};
	});
}

export function requestResult<T>(request: IDBRequest<T>): Promise<T> {
	return new Promise((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () =>
			reject(request.error ?? new Error('replica IndexedDB request failed'));
	});
}

export function transactionResult(transaction: IDBTransaction): Promise<void> {
	return new Promise((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onabort = () =>
			reject(transaction.error ?? new Error('replica IndexedDB transaction aborted'));
		transaction.onerror = () =>
			reject(transaction.error ?? new Error('replica IndexedDB transaction failed'));
	});
}
