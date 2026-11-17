import { requestGraphql, type FetchLike } from '../request.js';
import type { GqlAuth, GraphqlVariables } from '../types.js';
import {
	subscribe as subscribeGraphqlWs,
	type WebSocketConstructor
} from '../websocket.js';
import type {
	ReplicaCommandStatusRequest,
	ReplicaCommandTransport,
	ReplicaCommandTransportRequest,
	ReplicaCommandTransportResult
} from './command-runtime.js';
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
			return subscribeReplicaOperation(options, request, observer);
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
				options,
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
	options: ReplicaGraphqlTransportOptions,
	request: LiveRequest<TVariables>,
	observer: ReplicaLiveObserver<TData>
): () => void {
	let unsubscribe = (): void => undefined;
	let closed = false;
	const close = (): void => {
		if (closed) return;
		closed = true;
		request.signal?.removeEventListener('abort', close);
		unsubscribe();
	};
	const fail = (error: unknown): void => {
		if (closed) return;
		try {
			observer.error(error);
		} finally {
			close();
		}
	};
	if (request.signal?.aborted) {
		return close;
	}
	request.signal?.addEventListener('abort', close, { once: true });

	void Promise.resolve()
		.then(() => options.getAuth())
		.then((auth) => {
			if (closed || request.signal?.aborted) return;
			unsubscribe = subscribeGraphqlWs<TData, TVariables>(
				request.document,
				auth,
				{
					onNext: (result) => {
						if (!closed) observer.next(result);
					},
					onError: fail,
					onComplete: () => {
						if (closed) return;
						try {
							observer.complete();
						} finally {
							close();
						}
					}
				},
				{
					httpUrl: options.getUrl(),
					variables: request.variables,
					...(request.extensions === undefined
						? {}
						: { extensions: request.extensions }),
					...(request.resume === undefined
						? {}
						: { resume: request.resume }),
					...(options.webSocket === undefined
						? {}
						: { webSocket: options.webSocket })
				}
			);
			if (closed || request.signal?.aborted) unsubscribe();
		})
		.catch((error: unknown) => {
			fail(error);
		});

	return close;
}
