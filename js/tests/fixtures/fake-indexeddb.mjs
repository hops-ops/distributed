/**
 * Minimal in-memory IndexedDB (open/upgrade, multiple keyPath stores, get,
 * getAll, put, delete, transaction completion) that persists across
 * "reloads" — i.e. across persistence instances sharing one factory.
 */
export class FakeIndexedDb {
	#databases = new Map();
	opens = 0;

	open(name, version) {
		const request = fakeRequest();
		this.opens += 1;
		queueMicrotask(() => {
			let database = this.#databases.get(name);
			const upgrade = database === undefined || database.version < version;
			if (database === undefined) {
				database = { version, stores: new Map() };
				this.#databases.set(name, database);
			}
			database.version = version;
			request.result = new FakeDatabase(database);
			if (upgrade) request.onupgradeneeded?.({ target: request });
			queueMicrotask(() => request.onsuccess?.({ target: request }));
		});
		return request;
	}

	/** Raw store contents (Map key → value). */
	store(name, storeName) {
		return this.#databases.get(name)?.stores.get(storeName)?.entries ?? new Map();
	}
}

class FakeDatabase {
	constructor(state) {
		this.state = state;
		this.objectStoreNames = { contains: (name) => this.state.stores.has(name) };
	}

	createObjectStore(name, options = {}) {
		const store = { keyPath: options.keyPath, entries: new Map() };
		this.state.stores.set(name, store);
		return store;
	}

	transaction(name) {
		const store = this.state.stores.get(name);
		if (store === undefined) throw new Error(`missing object store ${name}`);
		return new FakeTransaction(store);
	}

	close() {}
}

class FakeTransaction {
	#pending = 0;
	completed = false;
	error = null;

	constructor(store) {
		this.store = store;
	}

	objectStore() {
		const { entries, keyPath } = this.store;
		return {
			get: (key) => this.#request(() => clone(entries.get(key))),
			getAll: () => this.#request(() => [...entries.values()].map(clone)),
			put: (value) =>
				this.#request(() => {
					const copy = structuredClone(value);
					entries.set(copy[keyPath], copy);
					return copy[keyPath];
				}),
			delete: (key) =>
				this.#request(() => {
					entries.delete(key);
					return undefined;
				})
		};
	}

	abort() {
		if (this.completed) throw new Error('transaction already completed');
		this.completed = true;
		this.onabort?.({ target: this });
	}

	#request(work) {
		const request = fakeRequest();
		this.#pending += 1;
		queueMicrotask(() => {
			if (this.completed) return;
			try {
				request.result = work();
				request.onsuccess?.({ target: request });
			} catch (error) {
				request.error = error;
				this.error = error;
				request.onerror?.({ target: request });
				this.onerror?.({ target: this });
				return;
			}
			this.#pending -= 1;
			queueMicrotask(() => {
				if (this.completed || this.#pending > 0) return;
				this.completed = true;
				this.oncomplete?.({ target: this });
			});
		});
		return request;
	}
}

function clone(value) {
	return value === undefined ? undefined : structuredClone(value);
}

function fakeRequest() {
	return {
		result: undefined,
		error: null,
		onsuccess: null,
		onerror: null,
		onupgradeneeded: null,
		onblocked: null
	};
}
