type AnyListener = (...args: unknown[]) => void;

/** Tiny typed event emitter; listener errors never break the network loop. */
export class Emitter<TEvents extends Record<string, unknown[]>> {
	readonly #listeners = new Map<keyof TEvents, Set<AnyListener>>();
	readonly #onListenerError: (error: unknown) => void;

	constructor(onListenerError?: (error: unknown) => void) {
		this.#onListenerError =
			onListenerError ??
			((error) => {
				queueMicrotask(() => {
					throw error;
				});
			});
	}

	on<K extends keyof TEvents>(type: K, listener: (...args: TEvents[K]) => void): () => void {
		let set = this.#listeners.get(type);
		if (set === undefined) {
			set = new Set();
			this.#listeners.set(type, set);
		}
		set.add(listener as unknown as AnyListener);
		return () => {
			set.delete(listener as unknown as AnyListener);
		};
	}

	once<K extends keyof TEvents>(type: K, listener: (...args: TEvents[K]) => void): () => void {
		const off = this.on(type, (...args) => {
			off();
			listener(...args);
		});
		return off;
	}

	emit<K extends keyof TEvents>(type: K, ...args: TEvents[K]): void {
		const set = this.#listeners.get(type);
		if (set === undefined) return;
		for (const listener of [...set]) {
			try {
				(listener as unknown as (...a: TEvents[K]) => void)(...args);
			} catch (error) {
				this.#onListenerError(error);
			}
		}
	}

	clear(): void {
		this.#listeners.clear();
	}
}
