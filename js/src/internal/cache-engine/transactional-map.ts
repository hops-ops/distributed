const ABSENT: unique symbol = Symbol('absent');

/**
 * A map whose committed root is never written while a transaction is open.
 *
 * A transaction's writes land in a delta that holds only the keys it touched;
 * reads see the delta over the root, so every untouched key is shared with the
 * root by reference. `replace` swaps in a whole new root for the rest of the
 * transaction and keeps the old one. `rollback` drops the delta and any
 * replacement, leaving the root the transaction started from. `commit` folds
 * the delta into the root, in place: nothing else holds the root, and the
 * transaction that could have needed the old version is over.
 *
 * Values are immutable: a write replaces the value under a key, never edits
 * it, so a value read before a transaction is still exact after a rollback.
 *
 * Iteration keeps `Map` order: root keys in place (with their new value, if
 * written), then keys the transaction added.
 */
export class TransactionalMap<K, V> {
	#root: Map<K, V>;
	#replacement: Map<K, V> | undefined;
	#delta: Map<K, V | typeof ABSENT> | undefined;

	constructor(root: Map<K, V> = new Map()) {
		this.#root = root;
	}

	begin(): void {
		if (this.#delta !== undefined) throw new Error('transaction already open');
		this.#delta = new Map();
	}

	commit(): void {
		const delta = this.#delta;
		if (delta === undefined) throw new Error('no open transaction');
		const root = (this.#root = this.#replacement ?? this.#root);
		for (const [key, value] of delta) {
			if (value === ABSENT) root.delete(key);
			else root.set(key, value);
		}
		this.#replacement = undefined;
		this.#delta = undefined;
	}

	rollback(): void {
		this.#replacement = undefined;
		this.#delta = undefined;
	}

	/** Replace every entry for the rest of the transaction. */
	replace(root: Map<K, V>): void {
		this.#open().clear();
		this.#replacement = root;
	}

	get(key: K): V | undefined {
		const delta = this.#delta;
		if (delta === undefined || !delta.has(key)) return this.#base().get(key);
		const value = delta.get(key)!;
		return value === ABSENT ? undefined : value;
	}

	has(key: K): boolean {
		const delta = this.#delta;
		if (delta === undefined || !delta.has(key)) return this.#base().has(key);
		return delta.get(key) !== ABSENT;
	}

	/** True when this transaction wrote `key`: its value is not shared with the root. */
	written(key: K): boolean {
		return this.#delta?.has(key) === true;
	}

	set(key: K, value: V): void {
		this.#open().set(key, value);
	}

	delete(key: K): void {
		this.#open().set(key, ABSENT);
	}

	*entries(): IterableIterator<[K, V]> {
		const delta = this.#delta;
		const base = this.#base();
		if (delta === undefined || delta.size === 0) {
			yield* base;
			return;
		}
		for (const [key, value] of base) {
			if (!delta.has(key)) {
				yield [key, value];
				continue;
			}
			const written = delta.get(key)!;
			if (written !== ABSENT) yield [key, written];
		}
		for (const [key, value] of delta) {
			if (value !== ABSENT && !base.has(key)) yield [key, value];
		}
	}

	*keys(): IterableIterator<K> {
		for (const [key] of this.entries()) yield key;
	}

	[Symbol.iterator](): IterableIterator<[K, V]> {
		return this.entries();
	}

	#base(): Map<K, V> {
		return this.#replacement ?? this.#root;
	}

	#open(): Map<K, V | typeof ABSENT> {
		if (this.#delta === undefined) {
			throw new Error('cache state written outside a transaction');
		}
		return this.#delta;
	}
}
