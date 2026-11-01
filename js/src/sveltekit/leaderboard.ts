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
 * result (server-ranked rows are never merged into the replica).
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
		let close = (): void => undefined;
		if (options.initial === undefined) {
			transport
				.query<TData, TVariables>(operation.document, variables, controller.signal)
				.then((result) => {
					if (controller.signal.aborted) return;
					// A live result that arrived first is newer: keep it.
					if (current.data !== undefined && current.live === 'active') return;
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
		if (options.live !== false && operation.liveDocument !== null) {
			update({ live: 'connecting' });
			close = transport.live<TData, TVariables>(operation.liveDocument, variables, {
				next(result) {
					const error = message(result.errors);
					if (result.data === undefined || result.data === null) {
						update({ error, live: error === null ? current.live : 'error' });
						return;
					}
					update({ data: result.data, error, live: 'active' });
				},
				error(error) {
					update({ error: String(error), live: 'error' });
				},
				complete() {
					update({ live: 'off' });
				}
			});
		}
		return () => {
			controller.abort();
			close();
		};
	});
}
