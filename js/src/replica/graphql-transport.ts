import { requestGraphql, type FetchLike } from '../request.js';
import type { GqlAuth, GraphqlVariables } from '../types.js';
import type { Scheduler } from '../lib/backoff.js';
import {
	createGraphqlWsConnection,
	type GraphqlWsConnection,
	type WebSocketConstructor
} from '../websocket.js';
import type {
	ReplicaCommandStatusRequest,
	ReplicaCommandTransport,
	ReplicaCommandTransportRequest,
	ReplicaCommandTransportResult
} from './command-runtime/types.js';
import type {
	ReplicaLiveObserver,
	ReplicaResultEnvelope,
	ReplicaTransport,
	ReplicaTransportRequest
} from './types.js';

export type ReplicaGraphqlTransportOptions = Readonly<{
	/** GraphQL HTTP endpoint. Same-origin paths and absolute URLs are supported. */
	getUrl: () => string;
	/** One injected credential source shared by HTTP, command, and WebSocket work. */
	getAuth: () => GqlAuth | Promise<GqlAuth>;
	/** Request-scoped SvelteKit fetch on the server, or a test/browser override. */
	fetch?: FetchLike;
	/** Browser/test WebSocket constructor override. */
	webSocket?: WebSocketConstructor;
	/** Clock and timers of the WebSocket redial delay (tests). */
	scheduler?: Scheduler;
	/** Jitter source of the WebSocket redial delay (tests). */
	random?: () => number;
}>;

/**
 * Documents that never enter the replica cache (server-ranked leaderboards):
 * the same endpoint, credentials and WebSocket as replica work, without a
 * replica artifact.
 */
export type ReplicaDocumentTransport = {
	/** Run one query document. */
	query<
		TData = Record<string, unknown>,
		TVariables extends GraphqlVariables = GraphqlVariables
	>(
		document: string,
		variables: TVariables,
		signal?: AbortSignal
	): Promise<ReplicaResultEnvelope<TData>>;
	/** Run one subscription document; returns its close function. */
	live<
		TData = Record<string, unknown>,
		TVariables extends GraphqlVariables = GraphqlVariables
	>(
		document: string,
		variables: TVariables,
		observer: ReplicaLiveObserver<TData>
	): () => void;
};

/**
 * One concrete GraphQL transport for generated replica reads, live operations,
 * command dispatch, command-status recovery, and replica-free documents.
 *
 * The transport is intentionally framework-neutral. Framework adapters inject
 * their request/session lifecycle but never duplicate GraphQL protocol logic.
 */
export type ReplicaGraphqlTransport = ReplicaTransport &
	ReplicaCommandTransport &
	ReplicaDocumentTransport;

export function createReplicaGraphqlTransport(
	options: ReplicaGraphqlTransportOptions
): ReplicaGraphqlTransport {
	if (typeof options.getUrl !== 'function') {
		throw new TypeError('replica GraphQL transport requires getUrl');
	}
	if (typeof options.getAuth !== 'function') {
		throw new TypeError('replica GraphQL transport requires getAuth');
	}

	// Every live operation and live document of this transport shares it.
	const connection = createGraphqlWsConnection({
		getUrl: options.getUrl,
		getAuth: options.getAuth,
		...(options.webSocket === undefined ? {} : { webSocket: options.webSocket }),
		...(options.scheduler === undefined ? {} : { scheduler: options.scheduler }),
		...(options.random === undefined ? {} : { random: options.random })
	});

	const execute = async (
		document: string,
		variables: Readonly<Record<string, unknown>>,
		extensions: Readonly<Record<string, unknown>>,
		signal?: AbortSignal
	): Promise<ReplicaCommandTransportResult> => {
		const auth = await options.getAuth();
		const result = await requestGraphql(
			options.getUrl(),
			document,
			auth,
			variables,
			{
				...(options.fetch === undefined ? {} : { fetch: options.fetch }),
				extensions,
				...(signal === undefined ? {} : { signal })
			}
		);
		return Object.freeze({
			...(result.data === undefined ? {} : { data: result.data }),
			...(result.errors === undefined ? {} : { errors: result.errors }),
			...(result.extensions === undefined
				? {}
				: { extensions: result.extensions }),
			status: result.status
		});
	};

	return Object.freeze({
		async fetch<
			TData = Record<string, unknown>,
			TVariables extends GraphqlVariables = GraphqlVariables
		>(
			request: ReplicaTransportRequest<TData, TVariables>
		): Promise<ReplicaResultEnvelope<TData>> {
			const auth = await options.getAuth();
			const result = await requestGraphql<TData, TVariables>(
				options.getUrl(),
				request.document,
				auth,
				request.variables,
				{
					...(options.fetch === undefined ? {} : { fetch: options.fetch }),
					...(request.extensions === undefined
						? {}
						: { extensions: request.extensions }),
					...(request.signal === undefined
						? {}
						: { signal: request.signal })
				}
			);
			return Object.freeze({
				...(result.data === undefined ? {} : { data: result.data }),
				...(result.errors === undefined ? {} : { errors: result.errors }),
				...(result.extensions === undefined
					? {}
					: { extensions: result.extensions })
			});
		},

		subscribe<
			TData = Record<string, unknown>,
			TVariables extends GraphqlVariables = GraphqlVariables
		>(
			request: ReplicaTransportRequest<TData, TVariables>,
			observer: ReplicaLiveObserver<TData>
		): () => void {
			return subscribeReplicaOperation(connection, request, observer);
		},

		async query<
			TData = Record<string, unknown>,
			TVariables extends GraphqlVariables = GraphqlVariables
		>(
			document: string,
			variables: TVariables,
			signal?: AbortSignal
		): Promise<ReplicaResultEnvelope<TData>> {
			const auth = await options.getAuth();
			const result = await requestGraphql<TData, TVariables>(
				options.getUrl(),
				document,
				auth,
				variables,
				{
					...(options.fetch === undefined ? {} : { fetch: options.fetch }),
					...(signal === undefined ? {} : { signal })
				}
			);
			return Object.freeze({
				...(result.data === undefined ? {} : { data: result.data }),
				...(result.errors === undefined ? {} : { errors: result.errors }),
				...(result.extensions === undefined
					? {}
					: { extensions: result.extensions })
			});
		},

		live<
			TData = Record<string, unknown>,
			TVariables extends GraphqlVariables = GraphqlVariables
		>(
			document: string,
			variables: TVariables,
			observer: ReplicaLiveObserver<TData>
		): () => void {
			return subscribeReplicaOperation<TData, TVariables>(
				connection,
				{ document, variables },
				observer
			);
		},

		dispatch(
			request: ReplicaCommandTransportRequest
		): Promise<ReplicaCommandTransportResult> {
			return execute(
				request.document,
				request.variables,
				request.extensions,
				request.signal
			);
		},

		status(
			request: ReplicaCommandStatusRequest
		): Promise<ReplicaCommandTransportResult> {
			return execute(
				request.document,
				request.variables,
				request.extensions,
				request.signal
			);
		}
	});
}

type LiveRequest<TVariables extends GraphqlVariables> = {
	readonly document: string;
	readonly variables: TVariables;
	readonly extensions?: ReplicaTransportRequest<unknown, TVariables>['extensions'];
	readonly resume?: ReplicaTransportRequest<unknown, TVariables>['resume'];
	readonly signal?: AbortSignal;
};

function subscribeReplicaOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	connection: GraphqlWsConnection,
	request: LiveRequest<TVariables>,
	observer: ReplicaLiveObserver<TData>
): () => void {
	if (request.signal?.aborted) return () => undefined;
	let closed = false;
	const end = (): boolean => {
		if (closed) return false;
		closed = true;
		request.signal?.removeEventListener('abort', close);
		return true;
	};
	const unsubscribe = connection.subscribe<TData, TVariables>(
		request.document,
		{
			onNext: (result) => {
				if (!closed) observer.next(result);
			},
			onError: (error) => {
				if (end()) observer.error(error);
			},
			onComplete: () => {
				if (end()) observer.complete();
			}
		},
		{
			variables: request.variables,
			...(request.extensions === undefined
				? {}
				: { extensions: request.extensions }),
			...(request.resume === undefined ? {} : { resume: request.resume })
		}
	);
	function close(): void {
		if (end()) unsubscribe();
	}
	request.signal?.addEventListener('abort', close, { once: true });
	return close;
}
