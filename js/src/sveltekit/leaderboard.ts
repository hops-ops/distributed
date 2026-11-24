import {
	backoffDelay,
	defaultScheduler,
	unrefTimer,
	type Scheduler
} from '../lib/backoff.js';
import {
	LIVE_FAILURE_RETRY_BASE_MS,
	LIVE_FAILURE_RETRY_JITTER,
	LIVE_FAILURE_RETRY_MAX_MS
} from '../replica/distributed-replica/constants.js';
import { requestGraphql, type FetchLike } from '../request.js';
import type { GqlAuth, GqlError, GraphqlVariables } from '../types.js';
import type { ReplicaDocumentTransport } from '../replica/graphql-transport.js';
import { useDistributedSvelteKitClient } from './context.js';

/**
 * A generated leaderboard operation (`leaderboards.ts`, re-exported by
 * `$distributed`), e.g. `XpLeaderboardLeaderboard`.
 */
export type DistributedLeaderboardOperation = Readonly<{
	leaderboard: string;
	operation: string;
	document: string;
	/** The `@live` subscription twin, or null (`…Rank(member)` is never live). */
	liveDocument: string | null;
}>;

export type DistributedLeaderboardLive = 'off' | 'connecting' | 'active' | 'error';

/** The Svelte store contract (`$store` in components). */
export type DistributedLeaderboardStore<T> = Readonly<{
	subscribe(run: (value: T) => void): () => void;
}>;

/** A readable store: `start` runs on the first subscriber, its stop on the last. */
function readable<T>(
	value: T,
	start: (set: (value: T) => void) => (() => void) | undefined
): DistributedLeaderboardStore<T> {
	let current = value;
	const subscribers = new Set<(value: T) => void>();
	let stop: (() => void) | undefined;
	const set = (next: T): void => {
		current = next;
		for (const run of [...subscribers]) run(current);
	};
	return Object.freeze({
		subscribe(run: (value: T) => void): () => void {
			subscribers.add(run);
			if (subscribers.size === 1) stop = start(set);
			run(current);
			return () => {
				if (!subscribers.delete(run) || subscribers.size > 0) return;
				stop?.();
				stop = undefined;
			};
		}
	});
}

/** One leaderboard result as a component reads it. */
export type DistributedLeaderboardSnapshot<TData> = Readonly<{
	/** The latest result (`undefined` until the first one arrives). */
	data: TData | undefined;
	/** The latest GraphQL or transport error, if any. */
	error: string | null;
	live: DistributedLeaderboardLive;
}>;

export type UseDistributedLeaderboardOptions<TData> = Readonly<{
	/** The server-loaded result (`loadDistributedLeaderboard`), so SSR renders it. */
	initial?: TData;
	/** Subscribe to the live document in the browser (default true). */
	live?: boolean;
	/** Whether this runs in a browser (default: `typeof window !== 'undefined'`). */
	browser?: boolean;
	/** Timer source of the live reopen backoff (tests). */
	scheduler?: Scheduler;
	/** Jitter source of the live reopen backoff (tests). */
	random?: () => number;
}>;

function message(errors: readonly GqlError[] | undefined): string | null {
	if (errors === undefined || errors.length === 0) return null;
	return errors.map((error) => error.message).join('; ');
}

/**
 * Server-side load of a leaderboard operation (SvelteKit `load`), with the
 * same endpoint and credential the generated client uses. Leaderboard rows
 * are server-ranked and never enter the replica cache, so they are passed to
 * the component as page data and handed to {@link useDistributedLeaderboard}
 * as `initial`. Throws on a GraphQL or HTTP error.
 */
export async function loadDistributedLeaderboard<
	TData,
	TVariables extends GraphqlVariables = GraphqlVariables
>(
	operation: DistributedLeaderboardOperation,
	variables: TVariables,
	options: Readonly<{ url: string; auth: GqlAuth; fetch?: FetchLike }>
): Promise<TData> {
	const result = await requestGraphql<TData, TVariables>(
		options.url,
		operation.document,
		options.auth,
		variables,
		options.fetch === undefined ? {} : { fetch: options.fetch }
	);
	const error = message(result.errors);
	if (error !== null || result.data === undefined || result.data === null) {
		throw new Error(
			`leaderboard ${operation.leaderboard} ${operation.operation}: ${error ?? `HTTP ${result.status}`}`
		);
	}
	return result.data;
}

/**
 * A readable store of one leaderboard operation for the nearest generated
 * client (call during component initialization). It starts from `initial`
 * (or runs the query in the browser), then follows the operation's `@live`
 * subscription over the client's WebSocket; every push replaces the whole
 * result (server-ranked rows are never merged into the replica). A stream
 * that loses its connection is reopened after a jittered backoff (1 s,
 * doubling to 30 s); its first frame is the current board.
 */
export function useDistributedLeaderboard<
	TData,
	TVariables extends GraphqlVariables = GraphqlVariables
>(
	operation: DistributedLeaderboardOperation,
	variables: TVariables,
	options: UseDistributedLeaderboardOptions<TData> = {}
): DistributedLeaderboardStore<DistributedLeaderboardSnapshot<TData>> {
	return createDistributedLeaderboardStore(
		useDistributedSvelteKitClient().transport,
		operation,
		variables,
		options
	);
}

/** {@link useDistributedLeaderboard} over an explicit transport. */
export function createDistributedLeaderboardStore<
	TData,
	TVariables extends GraphqlVariables = GraphqlVariables
>(
	transport: ReplicaDocumentTransport,
	operation: DistributedLeaderboardOperation,
	variables: TVariables,
	options: UseDistributedLeaderboardOptions<TData> = {}
): DistributedLeaderboardStore<DistributedLeaderboardSnapshot<TData>> {
	const inBrowser = options.browser ?? typeof window !== 'undefined';
	const initial: DistributedLeaderboardSnapshot<TData> = Object.freeze({
		data: options.initial,
		error: null,
		live: 'off'
	});
	return readable(initial, (set) => {
		if (!inBrowser) return undefined;
		let current = initial;
		const update = (next: Partial<DistributedLeaderboardSnapshot<TData>>): void => {
			current = Object.freeze({ ...current, ...next });
			set(current);
		};
		const controller = new AbortController();
		const scheduler = options.scheduler ?? defaultScheduler;
		let close = (): void => undefined;
		let retry: unknown;
		let failures = 0;
		// Results carry no version. A live frame is always newer than the
		// initial query, which was sent before the first stream opened.
		let liveDelivered = false;
		if (options.initial === undefined) {
			transport
				.query<TData, TVariables>(operation.document, variables, controller.signal)
				.then((result) => {
					if (controller.signal.aborted || liveDelivered) return;
					update({
						...(result.data === undefined || result.data === null
							? {}
							: { data: result.data }),
						error: message(result.errors)
					});
				})
				.catch((error: unknown) => {
					if (!controller.signal.aborted) update({ error: String(error) });
				});
		}
		const liveDocument = operation.liveDocument;
		/*
		 * The server answers a subscription with the current board and then
		 * every change, so a reopened stream needs no HTTP refresh and no
		 * ordering against one: only the open stream writes.
		 */
		const open = (): void => {
			if (liveDocument === null) return;
			let streaming = true;
			update({ live: 'connecting' });
			const unsubscribe = transport.live<TData, TVariables>(liveDocument, variables, {
				next(result) {
					if (!streaming) return;
					const error = message(result.errors);
					if (result.data === undefined || result.data === null) {
						update({ error, live: error === null ? current.live : 'error' });
						return;
					}
					failures = 0;
					liveDelivered = true;
					update({ data: result.data, error, live: 'active' });
				},
				error(error) {
					if (!streaming) return;
					update({ error: String(error), live: 'error' });
					if ((error as { retryable?: unknown } | null)?.retryable !== true) return;
					close();
					failures += 1;
					retry = scheduler.setTimeout(
						() => {
							retry = undefined;
							open();
						},
						backoffDelay(failures, {
							baseMs: LIVE_FAILURE_RETRY_BASE_MS,
							maxMs: LIVE_FAILURE_RETRY_MAX_MS,
							jitter: LIVE_FAILURE_RETRY_JITTER,
							...(options.random === undefined ? {} : { random: options.random })
						})
					);
					unrefTimer(retry);
				},
				complete() {
					if (streaming) update({ live: 'off' });
				}
			});
			close = () => {
				streaming = false;
				unsubscribe();
			};
		};
		if (options.live !== false) open();
		return () => {
			controller.abort();
			if (retry !== undefined) scheduler.clearTimeout(retry);
			close();
		};
	});
}
