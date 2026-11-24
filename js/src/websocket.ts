/** Minimal multiplexed `graphql-transport-ws` client with injectable browser primitives. */
import { wsConnectionInitPayload } from './auth-headers.js';
import {
	backoffDelay,
	defaultScheduler,
	unrefTimer,
	type Scheduler
} from './lib/backoff.js';
import { documentToString, type GqlDocument } from './document.js';
import {
	DistributedProtocolError,
	distributedLiveResumeExtensions,
	parseGraphqlResponseExtensions,
	type DistributedLiveCursor,
	type GraphqlResponseExtensions
} from './protocol.js';
import type {
	GqlAuth,
	GqlError,
	GraphqlVariables
} from './types.js';

/** Constructor accepted by the WebSocket transport (native or test implementation). */
export type WebSocketConstructor = typeof globalThis.WebSocket;

/** GraphQL execution payload delivered by a subscription. */
export type GqlWsResult<TData = unknown> = {
	data?: TData | null;
	errors?: GqlError[];
	/** Validated top-level GraphQL extensions for this live frame. */
	extensions?: GraphqlResponseExtensions;
};

export type GqlWsHandlers<TData = unknown> = {
	onNext: (result: GqlWsResult<TData>) => void;
	onError?: (error: unknown) => void;
	onComplete?: () => void;
};

export type SubscribeOptions<
	TVariables extends GraphqlVariables = GraphqlVariables
> = {
	/** Variables for the subscription operation. */
	variables?: TVariables;
	/**
	 * Latest server-issued cursors for a generated live operation.
	 * @internal Application code must not synthesize resume tokens.
	 */
	resume?: readonly DistributedLiveCursor[];
	/**
	 * Framework-owned GraphQL request extensions.
	 * @internal Generated transports, not application code, supply this value.
	 */
	extensions?: Readonly<Record<string, unknown>>;
};

function browserLocation(): Location | undefined {
	return typeof window === 'undefined' ? undefined : window.location;
}

/** Build a same-origin WebSocket URL in a browser, or retain the path on a server. */
export function graphqlWsUrl(path = '/graphql/ws'): string {
	const location = browserLocation();
	if (!location) return path;

	const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
	const normalizedPath = path.startsWith('/') ? path : `/${path}`;
	return `${protocol}//${location.host}${normalizedPath}`;
}

/** Map an HTTP GraphQL URL to its `graphql-transport-ws` endpoint. */
export function httpUrlToWsUrl(httpUrl: string): string {
	const location = browserLocation();
	const base = location?.href ?? 'http://127.0.0.1/';

	try {
		const url = new URL(httpUrl, base);
		const path = url.pathname.replace(/\/$/, '');
		const wsPath = path.endsWith('/ws') ? path : `${path}/ws`;

		if (httpUrl.startsWith('/')) {
			return location
				? graphqlWsUrl(wsPath)
				: `ws://127.0.0.1${wsPath.startsWith('/') ? wsPath : `/${wsPath}`}`;
		}

		url.protocol = url.protocol === 'https:' || url.protocol === 'wss:' ? 'wss:' : 'ws:';
		url.pathname = wsPath;
		url.search = '';
		url.hash = '';
		return url.toString();
	} catch {
		return location ? graphqlWsUrl('/graphql/ws') : 'ws://127.0.0.1/graphql/ws';
	}
}

/** GraphQL error codes with which a gateway ends an operation that a fresh subscription can replace. */
const REOPENABLE_OPERATION_CODES: ReadonlySet<unknown> = new Set([
	'LIVE_RESET_REQUIRED',
	'FRESHNESS_SCOPE_CHANGED'
]);

/**
 * Why a subscription ended without the server completing it.
 *
 * `retryable` is the classification of what the server sent: a lost or
 * recycled socket, an expired token and a gateway reset can be outlived by
 * a later subscription; a refused credential, a protocol violation and a
 * rejected operation cannot.
 */
export class GraphqlWsError extends Error {
	readonly retryable: boolean;
	/** WebSocket close code, when the socket closed. */
	readonly code: number | undefined;

	constructor(message: string, retryable: boolean, code?: number) {
		super(message);
		this.name = 'GraphqlWsError';
		this.retryable = retryable;
		this.code = code;
	}
}

export type GraphqlWsConnectionOptions = Readonly<{
	/** HTTP GraphQL URL used to derive the `.../ws` endpoint (default `/graphql`). */
	getUrl?: () => string;
	/** Credential for `connection_init`, read once per subscription. */
	getAuth?: () => GqlAuth | Promise<GqlAuth>;
	/** Override the runtime's global WebSocket constructor. */
	webSocket?: WebSocketConstructor;
	/** Clock and timers of the redial delay (tests). */
	scheduler?: Scheduler;
	/** Jitter source of the redial delay (tests). */
	random?: () => number;
}>;

export type GraphqlWsConnection = Readonly<{
	/**
	 * Run one subscription and return its idempotent unsubscribe function.
	 *
	 * Every subscription made with the same endpoint and credential shares
	 * one socket, opened by the first and closed shortly after the last. A
	 * subscription ends with exactly one of `onComplete` (the server
	 * completed it) or `onError` (a {@link GraphqlWsError}, or the reason its
	 * credential or frame was unusable); it is never resubscribed here, so
	 * its owner decides whether and with which cursors to subscribe again.
	 */
	subscribe<TData = unknown, TVariables extends GraphqlVariables = GraphqlVariables>(
		document: GqlDocument<TData, TVariables>,
		handlers: GqlWsHandlers<TData>,
		options?: SubscribeOptions<TVariables>
	): () => void;
}>;

/** Redial delay after a lost socket: 1 s doubling to 30 s, less up to half. */
const REDIAL = Object.freeze({ baseMs: 1_000, maxMs: 30_000, jitter: 0.5 });
/**
 * How long an open socket outlives its last subscription. Owners replace
 * subscriptions (a reopened stream, a navigation) by ending one and starting
 * the next; that must not cost a new socket and handshake each time.
 */
const IDLE_CLOSE_MS = 1_000;

type Subscription = {
	readonly id: string;
	readonly payload: Readonly<Record<string, unknown>>;
	readonly handlers: GqlWsHandlers<unknown>;
	link: Link | undefined;
	closed: boolean;
};

/** One socket and the subscriptions that share its `connection_init` identity. */
type Link = {
	readonly key: string;
	readonly url: string;
	readonly init: Record<string, string>;
	readonly subscriptions: Map<string, Subscription>;
	socket: WebSocket | undefined;
	acknowledged: boolean;
	/** Pending redial of a socket that was lost. */
	dial: unknown;
	/** Pending close of an open socket without subscriptions. */
	idle: unknown;
};

/**
 * One multiplexed `graphql-transport-ws` connection.
 *
 * It owns the sockets: one per endpoint and credential, carrying every
 * subscription made with them under its own operation id. A socket that is
 * lost ends all of its subscriptions with a retryable {@link GraphqlWsError};
 * however many of them subscribe again, the endpoint is redialed once, after
 * a jittered exponential delay that resets when a result arrives.
 */
export function createGraphqlWsConnection(
	options: GraphqlWsConnectionOptions = {}
): GraphqlWsConnection {
	const scheduler = options.scheduler ?? defaultScheduler;
	const links = new Map<string, Link>();
	let nextId = 0;
	let failures = 0;
	let redialAt = 0;

	const detach = (link: Link): void => {
		if (link.dial !== undefined) scheduler.clearTimeout(link.dial);
		link.dial = undefined;
		if (link.idle !== undefined) scheduler.clearTimeout(link.idle);
		link.idle = undefined;
		const socket = link.socket;
		link.socket = undefined;
		link.acknowledged = false;
		if (links.get(link.key) === link) links.delete(link.key);
		if (socket === undefined) return;
		socket.onopen = null;
		socket.onmessage = null;
		socket.onerror = null;
		socket.onclose = null;
		try {
			socket.close();
		} catch {
			// Already closed or never opened; the handlers are detached.
		}
	};

	const send = (link: Link, message: unknown): void => {
		try {
			link.socket?.send(JSON.stringify(message));
		} catch {
			// A failing send surfaces as the socket's error or close.
		}
	};

	/** A subscription left: an unused socket closes, an open one after a pause. */
	const release = (link: Link): void => {
		if (link.subscriptions.size > 0) return;
		if (!link.acknowledged) {
			detach(link);
			return;
		}
		if (link.idle !== undefined) return;
		link.idle = scheduler.setTimeout(() => {
			link.idle = undefined;
			if (link.subscriptions.size === 0) detach(link);
		}, IDLE_CLOSE_MS);
		unrefTimer(link.idle);
	};

	/** End one subscription that the server did not end. */
	const drop = (subscription: Subscription): void => {
		if (subscription.closed) return;
		subscription.closed = true;
		const link = subscription.link;
		if (link === undefined || !link.subscriptions.delete(subscription.id)) return;
		if (link.acknowledged) send(link, { type: 'complete', id: subscription.id });
		release(link);
	};

	/** The socket is gone or unusable: end every subscription on it. */
	const lose = (link: Link, error: GraphqlWsError): void => {
		const ended = [...link.subscriptions.values()];
		link.subscriptions.clear();
		detach(link);
		failures += 1;
		redialAt =
			scheduler.now() +
			backoffDelay(failures, {
				...REDIAL,
				...(options.random === undefined ? {} : { random: options.random })
			});
		const thrown: unknown[] = [];
		for (const subscription of ended) {
			subscription.closed = true;
			try {
				subscription.handlers.onError?.(error);
			} catch (failure) {
				thrown.push(failure);
			}
		}
		if (thrown.length > 0) throw thrown[0];
	};

	const receive = (link: Link, data: unknown): void => {
		let message: { type?: unknown; id?: unknown; payload?: unknown };
		try {
			message = JSON.parse(String(data)) as typeof message;
		} catch {
			return;
		}
		if (message === null || typeof message !== 'object') return;
		switch (message.type) {
			case 'connection_ack':
				if (link.acknowledged) return;
				link.acknowledged = true;
				for (const subscription of link.subscriptions.values()) {
					send(link, {
						type: 'subscribe',
						id: subscription.id,
						payload: subscription.payload
					});
				}
				return;
			case 'ping':
				send(link, {
					type: 'pong',
					...(message.payload === undefined ? {} : { payload: message.payload })
				});
				return;
			case 'connection_error':
				lose(
					link,
					new GraphqlWsError(
						`GraphQL WebSocket connection refused: ${describe(message.payload)}`,
						false
					)
				);
				return;
			default:
				break;
		}
		const subscription =
			typeof message.id === 'string'
				? link.subscriptions.get(message.id)
				: undefined;
		if (subscription === undefined) return;
		switch (message.type) {
			case 'next': {
				let result: GqlWsResult<unknown>;
				try {
					result = parseNextPayload<unknown>(message.payload);
				} catch (error) {
					drop(subscription);
					subscription.handlers.onError?.(error);
					return;
				}
				failures = 0;
				subscription.handlers.onNext(result);
				return;
			}
			case 'error':
				subscription.closed = true;
				link.subscriptions.delete(subscription.id);
				release(link);
				subscription.handlers.onError?.(operationError(message.payload));
				return;
			case 'complete':
				subscription.closed = true;
				link.subscriptions.delete(subscription.id);
				release(link);
				subscription.handlers.onComplete?.();
				return;
			default:
				return;
		}
	};

	const open = (link: Link): void => {
		if (link.socket !== undefined || link.dial !== undefined) return;
		const wait = redialAt - scheduler.now();
		if (wait > 0) {
			link.dial = scheduler.setTimeout(() => {
				link.dial = undefined;
				open(link);
			}, wait);
			return;
		}
		const WebSocketImpl = options.webSocket ?? globalThis.WebSocket;
		let socket: WebSocket;
		try {
			socket = new WebSocketImpl(link.url, 'graphql-transport-ws');
		} catch (error) {
			lose(link, new GraphqlWsError(describe(error), false));
			return;
		}
		link.socket = socket;
		socket.onopen = () => {
			if (link.socket !== socket) return;
			send(link, { type: 'connection_init', payload: link.init });
		};
		socket.onmessage = (event) => {
			if (link.socket === socket) receive(link, event.data);
		};
		socket.onerror = () => {
			if (link.socket !== socket) return;
			lose(link, new GraphqlWsError('GraphQL WebSocket failed', true));
		};
		socket.onclose = (event) => {
			if (link.socket !== socket) return;
			lose(link, closeError(event));
		};
	};

	const attach = (subscription: Subscription, auth: GqlAuth): void => {
		const location = browserLocation();
		const getUrl = options.getUrl;
		const url = new URL(
			getUrl === undefined ? graphqlWsUrl('/graphql/ws') : httpUrlToWsUrl(getUrl()),
			location?.href ?? 'ws://127.0.0.1/'
		).toString();
		const init = wsConnectionInitPayload(auth);
		const key = JSON.stringify([url, init]);
		let link = links.get(key);
		if (link === undefined) {
			link = {
				key,
				url,
				init,
				subscriptions: new Map(),
				socket: undefined,
				acknowledged: false,
				dial: undefined,
				idle: undefined
			};
			links.set(key, link);
		}
		link.subscriptions.set(subscription.id, subscription);
		subscription.link = link;
		if (link.idle !== undefined) scheduler.clearTimeout(link.idle);
		link.idle = undefined;
		if (link.acknowledged) {
			send(link, {
				type: 'subscribe',
				id: subscription.id,
				payload: subscription.payload
			});
		} else {
			open(link);
		}
	};

	return Object.freeze({
		subscribe<TData, TVariables extends GraphqlVariables>(
			document: GqlDocument<TData, TVariables>,
			handlers: GqlWsHandlers<TData>,
			subscribeOptions: SubscribeOptions<TVariables> = {}
		): () => void {
			if (typeof (options.webSocket ?? globalThis.WebSocket) !== 'function') {
				throw new Error(
					'GraphQL subscriptions require a WebSocket implementation in this runtime; pass { webSocket } in the options'
				);
			}
			const extensions = mergeRequestExtensions(
				subscribeOptions.extensions,
				subscribeOptions.resume === undefined ||
					subscribeOptions.resume.length === 0
					? undefined
					: distributedLiveResumeExtensions(subscribeOptions.resume)
			);
			nextId += 1;
			const subscription: Subscription = {
				// Never reused, so a frame of an ended operation matches nothing.
				id: String(nextId),
				payload: Object.freeze({
					query: documentToString(document),
					variables: subscribeOptions.variables ?? {},
					...(extensions === undefined ? {} : { extensions })
				}),
				handlers: handlers as GqlWsHandlers<unknown>,
				link: undefined,
				closed: false
			};
			void Promise.resolve()
				.then(() => options.getAuth?.() ?? {})
				.then((auth) => {
					if (!subscription.closed) attach(subscription, auth);
				})
				.catch((error: unknown) => {
					if (subscription.closed) return;
					drop(subscription);
					handlers.onError?.(error);
				});
			return () => drop(subscription);
		}
	});
}

/**
 * Classify a close by what the servers send (`src/graphql/http.rs`,
 * `src/gateway`): 1002 is a refused `connection_init` or a protocol
 * violation, and 44xx a refused credential or request, except the expiry of
 * a token that was accepted (4401 `token expired`) and an init timeout
 * (4408). Everything else (a lost network, a restart, the connection
 * lifetime, 1013 `LIVE_RESET_REQUIRED`) is outlived by redialing.
 */
function closeError(event: { code?: number; reason?: string }): GraphqlWsError {
	const code = event.code ?? 1006;
	const reason = event.reason ?? '';
	const refused =
		code === 1002 ||
		(code >= 4400 &&
			code <= 4499 &&
			code !== 4408 &&
			!(code === 4401 && reason === 'token expired'));
	return new GraphqlWsError(
		`GraphQL WebSocket closed (${code}${reason ? `: ${reason}` : ''})`,
		!refused,
		code
	);
}

/** An `error` message ends one operation with GraphQL errors. */
function operationError(payload: unknown): GraphqlWsError {
	const errors = Array.isArray(payload) ? (payload as readonly unknown[]) : [payload];
	const codes = errors.map((error) =>
		error !== null && typeof error === 'object'
			? (error as { extensions?: { code?: unknown } }).extensions?.code
			: undefined
	);
	return new GraphqlWsError(
		errors.map(describe).join('; ') || 'subscription error',
		codes.length > 0 && codes.every((code) => REOPENABLE_OPERATION_CODES.has(code))
	);
}

function describe(value: unknown): string {
	if (value instanceof Error) return value.message;
	if (typeof value === 'string') return value;
	if (
		value !== null &&
		typeof value === 'object' &&
		typeof (value as { message?: unknown }).message === 'string'
	) {
		return (value as { message: string }).message;
	}
	return value === undefined ? 'unknown error' : String(value);
}

function mergeRequestExtensions(
	base: Readonly<Record<string, unknown>> | undefined,
	resume: Readonly<Record<string, unknown>> | undefined
): Readonly<Record<string, unknown>> | undefined {
	if (base === undefined) return resume;
	if (resume === undefined) return base;
	const baseDistributed = requestExtensionRecord(
		base.distributed,
		'request.extensions.distributed'
	);
	const resumeDistributed = requestExtensionRecord(
		resume.distributed,
		'request.extensions.distributed'
	);
	for (const key of Object.keys(resumeDistributed)) {
		if (Object.hasOwn(baseDistributed, key)) {
			throw new DistributedProtocolError(
				'DISTRIBUTED_PROTOCOL_INVALID',
				`request.extensions.distributed.${key}`
			);
		}
	}
	return Object.freeze({
		...base,
		distributed: Object.freeze({
			...baseDistributed,
			...resumeDistributed
		})
	});
}

function requestExtensionRecord(
	value: unknown,
	path: string
): Readonly<Record<string, unknown>> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		throw new DistributedProtocolError(
			'DISTRIBUTED_PROTOCOL_INVALID',
			path
		);
	}
	return value as Readonly<Record<string, unknown>>;
}

function parseNextPayload<TData>(value: unknown): GqlWsResult<TData> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		throw new DistributedProtocolError(
			'DISTRIBUTED_PROTOCOL_INVALID',
			'websocket.next.payload'
		);
	}
	const payload = value as Record<string, unknown>;
	if (payload.errors !== undefined && !Array.isArray(payload.errors)) {
		throw new DistributedProtocolError(
			'DISTRIBUTED_PROTOCOL_INVALID',
			'websocket.next.payload.errors'
		);
	}
	const extensions = parseGraphqlResponseExtensions(payload.extensions);
	return {
		...(payload as GqlWsResult<TData>),
		...(extensions === undefined ? {} : { extensions })
	};
}
