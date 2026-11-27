import type { ReplicaPendingRows, ReplicaWindowArtifact } from '../replica/index.js';
import {
	compareReplicaWindowKeys,
	replicaWindowCursorVariables,
	replicaWindowKeyValues
} from '../replica/window.js';
import type { GraphqlVariables } from '../types.js';
import type { SveltekitQuerySnapshot, SveltekitQueryStore } from './replica.js';

export type UseSveltekitWindowOptions = Readonly<{
	/** Rows per page; defaults to the operation's `limit` default. */
	pageSize?: number;
	/** Keep the head page live. Defaults to true when the operation is `@live`. */
	live?: boolean;
}>;

/**
 * One ordered, de-duplicated list: the live head page followed by every page
 * `fetchMore()` retained, in the operation's `order_by`.
 */
export type SveltekitWindowSnapshot<TRow> = Readonly<{
	rows: readonly TRow[];
	/** Rows exist after the last one; `fetchMore()` loads the next page. */
	hasMore: boolean;
	fetchingMore: boolean;
	/** Why the last `fetchMore()` failed; the next call retries. */
	fetchMoreError: Error | undefined;
	/** `pending.has(row)`: the row is a preview the server has not returned. */
	pending: ReplicaPendingRows;
}> &
	Pick<
		SveltekitQuerySnapshot<unknown>,
		'complete' | 'status' | 'stale' | 'live' | 'loading' | 'errors' | 'error'
	>;

export interface SveltekitWindowStore<TRow> {
	/** Rows per page, and the head's size. */
	readonly pageSize: number;
	get(): SveltekitWindowSnapshot<TRow>;
	subscribe(listener: (snapshot: SveltekitWindowSnapshot<TRow>) => void): () => void;
	/** Load the next page after the last row. Never rejects; see `fetchMoreError`. */
	fetchMore(): Promise<void>;
	/** Refetch the head page. */
	refetch(): Promise<void>;
	destroy(): void;
}

export type SveltekitWindowSource<TData> = (
	variables: GraphqlVariables,
	live: boolean
) => SveltekitQueryStore<TData>;

type Keys = readonly (string | number)[];

type Page<TData> = {
	readonly store: SveltekitQueryStore<TData>;
	/** Exclusive: the page holds the rows after this key, in window order. */
	readonly after: Keys;
	readonly limit: number;
	unsubscribe: (() => void) | undefined;
};

/**
 * The live head is the newest `pageSize` rows and slides as rows arrive.
 * Retained pages are keyset pages, so arrivals never shift them; what keeps
 * the list contiguous is where the first page starts.
 *
 * - The first `fetchMore()` anchors after the head's newest server row and
 *   asks for the rest of the head plus one page, so it already holds every
 *   row the head may later drop.
 * - Each later `fetchMore()` continues after the last retained row.
 * - Once the head has slid past the anchor (`pageSize` arrivals), the rows
 *   between are in no page; the window then drops its retained pages rather
 *   than show a gap, and `fetchMore()` anchors again.
 * - A page shorter than its limit is the end of the result.
 */
export class SveltekitWindowStoreImpl<TData, TRow> implements SveltekitWindowStore<TRow> {
	readonly #window: ReplicaWindowArtifact;
	readonly #rootKey: string;
	readonly #open: SveltekitWindowSource<TData>;
	readonly #variables: GraphqlVariables;
	readonly #pageSize: number;
	readonly #head: SveltekitQueryStore<TData>;
	readonly #listeners = new Set<(snapshot: SveltekitWindowSnapshot<TRow>) => void>();
	#pages: Page<TData>[] = [];
	#ended = false;
	/** Bumped when retained pages are dropped; a stale page is discarded. */
	#generation = 0;
	#inFlight: Promise<void> | undefined;
	#fetchMoreError: Error | undefined;
	#unsubscribeHead: (() => void) | undefined;
	#snapshot: SveltekitWindowSnapshot<TRow> | undefined;
	#destroyed = false;

	constructor(
		window: ReplicaWindowArtifact,
		rootKey: string,
		open: SveltekitWindowSource<TData>,
		variables: GraphqlVariables,
		pageSize: number,
		headVariables: GraphqlVariables,
		live: boolean
	) {
		this.#window = window;
		this.#rootKey = rootKey;
		this.#open = open;
		this.#variables = variables;
		this.#pageSize = pageSize;
		this.#head = open(headVariables, live);
	}

	get pageSize(): number {
		return this.#pageSize;
	}

	get(): SveltekitWindowSnapshot<TRow> {
		if (this.#unsubscribeHead === undefined) this.#snapshot = this.#compute();
		return (this.#snapshot ??= this.#compute());
	}

	subscribe(listener: (snapshot: SveltekitWindowSnapshot<TRow>) => void): () => void {
		if (this.#destroyed) throw new Error('Distributed SvelteKit window is destroyed');
		if (typeof listener !== 'function') {
			throw new TypeError('Distributed SvelteKit window listener must be a function');
		}
		if (this.#listeners.size === 0) this.#activate();
		this.#listeners.add(listener);
		listener(this.get());
		let active = true;
		return () => {
			if (!active) return;
			active = false;
			this.#listeners.delete(listener);
			if (this.#listeners.size === 0) this.#deactivate();
		};
	}

	refetch(): Promise<void> {
		return this.#head.refetch();
	}

	fetchMore(): Promise<void> {
		if (this.#destroyed) return Promise.resolve();
		this.#inFlight ??= this.#loadPage().finally(() => {
			this.#inFlight = undefined;
			this.#publish();
		});
		this.#publish();
		return this.#inFlight;
	}

	destroy(): void {
		if (this.#destroyed) return;
		this.#destroyed = true;
		this.#deactivate();
		this.#dropPages();
		this.#head.destroy();
		this.#listeners.clear();
	}

	async #loadPage(): Promise<void> {
		const generation = this.#generation;
		const head = this.#head.get();
		const headRows = this.#rowsOf(head);
		let after: Keys | undefined;
		let limit: number;
		if (this.#pages.length === 0) {
			if (!this.#headHasMore(head, headRows)) return;
			// The newest row the server returned; previews carry a client clock.
			const index = headRows.findIndex((row) => !head.pending.has(row));
			after = index < 0 ? undefined : replicaWindowKeyValues(this.#window, headRows[index]);
			limit = headRows.length - index - 1 + this.#pageSize;
		} else {
			if (this.#ended) return;
			const last = this.#pages.at(-1)!;
			after = replicaWindowKeyValues(this.#window, this.#rowsOf(last.store.get()).at(-1));
			limit = this.#pageSize;
		}
		if (after === undefined) return;

		let store: SveltekitQueryStore<TData> | undefined;
		this.#fetchMoreError = undefined;
		try {
			store = this.#open(
				{
					...this.#variables,
					...replicaWindowCursorVariables(this.#window, after),
					[this.#window.limitVariable]: limit
				},
				false
			);
			const page: Page<TData> = { store, after, limit, unsubscribe: undefined };
			if (this.#unsubscribeHead !== undefined) this.#watchPage(page);
			await store.refetch();
			const snapshot = store.get();
			if (!snapshot.complete) {
				throw new Error(snapshot.error?.message ?? 'window page did not load');
			}
			if (this.#destroyed || generation !== this.#generation) {
				page.unsubscribe?.();
				store.destroy();
				return;
			}
			this.#pages.push(page);
			this.#ended = this.#rowsOf(snapshot).length < limit;
		} catch (error) {
			store?.destroy();
			this.#fetchMoreError = error instanceof Error ? error : new Error(String(error));
		}
	}

	#activate(): void {
		this.#unsubscribeHead = this.#head.subscribe(() => this.#headChanged());
		for (const page of this.#pages) this.#watchPage(page);
	}

	#deactivate(): void {
		this.#unsubscribeHead?.();
		this.#unsubscribeHead = undefined;
		for (const page of this.#pages) {
			page.unsubscribe?.();
			page.unsubscribe = undefined;
		}
	}

	#watchPage(page: Page<TData>): void {
		page.unsubscribe = page.store.subscribe(() => this.#publish());
	}

	#headChanged(): void {
		const anchor = this.#pages[0]?.after;
		if (anchor !== undefined) {
			const head = this.#head.get();
			const headRows = this.#rowsOf(head);
			const last = replicaWindowKeyValues(this.#window, headRows.at(-1));
			if (
				head.complete &&
				headRows.length >= this.#pageSize &&
				last !== undefined &&
				compareReplicaWindowKeys(this.#window, last, anchor) < 0
			) {
				this.#dropPages();
			}
		}
		this.#publish();
	}

	#dropPages(): void {
		this.#generation += 1;
		for (const page of this.#pages) {
			page.unsubscribe?.();
			page.store.destroy();
		}
		this.#pages = [];
		this.#ended = false;
	}

	#publish(): void {
		if (this.#destroyed) return;
		const next = this.#compute();
		if (this.#snapshot !== undefined && sameSnapshot(this.#snapshot, next)) return;
		this.#snapshot = next;
		for (const listener of [...this.#listeners]) listener(next);
	}

	#headHasMore(head: SveltekitQuerySnapshot<TData>, headRows: readonly TRow[]): boolean {
		return head.complete && headRows.length >= this.#pageSize;
	}

	#rowsOf(snapshot: SveltekitQuerySnapshot<TData>): readonly TRow[] {
		const rows = (snapshot.data as Readonly<Record<string, unknown>> | null | undefined)?.[
			this.#rootKey
		];
		return Array.isArray(rows) ? (rows as readonly TRow[]) : [];
	}

	#compute(): SveltekitWindowSnapshot<TRow> {
		const head = this.#head.get();
		const headRows = this.#rowsOf(head);
		const pages = this.#pages.map((page) => page.store.get());
		const seen = new Set<string>();
		const keyed: { readonly row: TRow; readonly keys: Keys }[] = [];
		// The head comes first, so a row both hold is the head's live object.
		for (const rows of [headRows, ...pages.map((page) => this.#rowsOf(page))]) {
			for (const row of rows) {
				const keys = replicaWindowKeyValues(this.#window, row);
				if (keys === undefined) continue;
				const identity = JSON.stringify(keys);
				if (seen.has(identity)) continue;
				seen.add(identity);
				keyed.push({ row, keys });
			}
		}
		keyed.sort((left, right) => compareReplicaWindowKeys(this.#window, left.keys, right.keys));
		const rows = keyed.map(({ row }) => row);
		const previous = this.#snapshot;
		const sources = [head, ...pages];
		return Object.freeze({
			rows:
				previous !== undefined && sameRows(previous.rows, rows) ? previous.rows : rows,
			hasMore:
				this.#pages.length === 0 ? this.#headHasMore(head, headRows) : !this.#ended,
			fetchingMore: this.#inFlight !== undefined,
			fetchMoreError: this.#fetchMoreError,
			pending:
				previous !== undefined &&
				previous.pending instanceof WindowPending &&
				previous.pending.sameSources(sources)
					? previous.pending
					: new WindowPending(sources),
			complete: head.complete && pages.every((page) => page.complete),
			status: head.status,
			stale: head.stale,
			live: head.live,
			loading: head.loading,
			errors: head.errors,
			error: head.error
		}) as SveltekitWindowSnapshot<TRow>;
	}
}

class WindowPending implements ReplicaPendingRows {
	readonly #sources: readonly ReplicaPendingRows[];

	constructor(snapshots: readonly SveltekitQuerySnapshot<unknown>[]) {
		this.#sources = snapshots.map((snapshot) => snapshot.pending);
	}

	has(row: unknown): boolean {
		return this.#sources.some((pending) => pending.has(row));
	}

	get size(): number {
		return this.#sources.reduce((total, pending) => total + pending.size, 0);
	}

	sameSources(snapshots: readonly SveltekitQuerySnapshot<unknown>[]): boolean {
		return (
			snapshots.length === this.#sources.length &&
			snapshots.every((snapshot, index) => snapshot.pending === this.#sources[index])
		);
	}
}

function sameRows<TRow>(left: readonly TRow[], right: readonly TRow[]): boolean {
	return left.length === right.length && left.every((row, index) => row === right[index]);
}

function sameSnapshot<TRow>(
	left: SveltekitWindowSnapshot<TRow>,
	right: SveltekitWindowSnapshot<TRow>
): boolean {
	return (
		left.rows === right.rows &&
		left.hasMore === right.hasMore &&
		left.fetchingMore === right.fetchingMore &&
		left.fetchMoreError === right.fetchMoreError &&
		left.pending === right.pending &&
		left.complete === right.complete &&
		left.status === right.status &&
		left.stale === right.stale &&
		left.live === right.live &&
		left.loading === right.loading &&
		left.errors === right.errors
	);
}
