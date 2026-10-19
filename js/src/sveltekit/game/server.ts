/**
 * Server-only SvelteKit helpers for `distributed_game` zones:
 * `createGameTicketHandler` turns the app's verified session into a
 * single-use zone admission ticket (`POST` route), and `mintGameTicket`
 * signs one directly. Import only from `+server.ts` / `+page.server.ts` /
 * `$lib/server`: the handler holds the zone servers' shared secret.
 */
import {
	MAX_GAME_TICKET_TTL_SECONDS,
	MIN_GAME_TICKET_SECRET_BYTES,
	mintGameTicket,
	type GameTicketRole,
	type GameTicketSecret
} from './ticket.js';
import { createGameTicketRateLimiter, type GameTicketRateLimiter } from './rate-limit.js';

export {
	GAME_TICKET_RATE_LIMIT_SCRIPT,
	GameTicketRateLimiterUnavailable,
	createGameTicketRateLimiter,
	createRedisGameTicketRateLimiter,
	type GameTicketRateLimiter,
	type GameTicketRedisClient,
	type GameTicketRedisEval,
	type RedisGameTicketRateLimiterOptions
} from './rate-limit.js';

export {
	MAX_GAME_TICKET_TTL_SECONDS,
	MIN_GAME_TICKET_SECRET_BYTES,
	mintGameTicket,
	type GameTicketRole,
	type GameTicketSecret,
	type MintGameTicketOptions,
	type MintedGameTicket
} from './ticket.js';

const MAX_BODY_BYTES = 1024;
const BODY_KEYS = new Set(['zone', 'role']);
const ROLES: readonly GameTicketRole[] = ['player', 'observer'];

/** The subset of a SvelteKit `RequestEvent` the handler reads. */
export type GameTicketRequestEvent = Readonly<{
	request: Request;
	url: URL;
}>;

/** Session shape read by the default `principal` / `name` (Auth.js-compatible). */
export type GameTicketSessionLike = {
	user?: {
		id?: string | null;
		name?: string | null;
		username?: string | null;
	} | null;
} | null | undefined;

export type GameTicketZoneContext<TEvent, TSession> = Readonly<{
	event: TEvent;
	session: TSession;
	/** Verified principal from the session. */
	principal: string;
	/** Zone the browser is connecting to (a hint; undefined for a page load). */
	requested: string | undefined;
	role: GameTicketRole;
}>;

export type GameTicketRateLimit =
	| false
	| Readonly<{ max: number; windowMs: number }>
	| GameTicketRateLimiter;

export type CreateGameTicketHandlerOptions<TEvent extends GameTicketRequestEvent, TSession> = Readonly<{
	/**
	 * The zone servers' shared HS256 secret (≥ 32 bytes), or a function read
	 * on every request (e.g. from `$env/dynamic/private`). Missing → 503.
	 */
	secret: GameTicketSecret | (() => GameTicketSecret | undefined);
	/**
	 * The app's verified session — the same source the GraphQL side uses
	 * (`createDistributedSvelteKitServer({ getSession })`), e.g.
	 * `({ locals }) => locals.auth()`.
	 */
	getSession(event: TEvent): TSession | null | undefined | Promise<TSession | null | undefined>;
	/** Principal from the verified session. Default `session.user.id`. */
	principal?(session: TSession, event: TEvent): string | null | undefined;
	/** Display name for the ticket. Default `session.user.name ?? username`. */
	name?(session: TSession, event: TEvent): string | null | undefined;
	/**
	 * The zone this principal may join, decided on the server (ownership,
	 * party, last position…). `requested` is only a hint from the browser:
	 * returning another zone answers 409 with the server's choice; returning
	 * undefined answers 403.
	 */
	zone(
		context: GameTicketZoneContext<TEvent, TSession>
	): string | null | undefined | Promise<string | null | undefined>;
	/** Roles a browser may ask for. Default both (`observer` owns no player). */
	roles?: readonly GameTicketRole[];
	/** Ticket lifetime in seconds (1..=300). Default 60. */
	ttlSeconds?: number;
	/**
	 * Per-principal limit. Default 10 tickets per minute in memory (per
	 * process); `createRedisGameTicketRateLimiter` shares one budget across
	 * instances; `false` disables. A limiter that throws or rejects answers
	 * 503 `rate_limit_unavailable` (fail closed).
	 */
	rateLimit?: GameTicketRateLimit;
	/** Extra origins allowed besides the request URL's own (e.g. a dev proxy). */
	allowedOrigins?: readonly string[];
	/** Clock in Unix milliseconds (tests). */
	now?(): number;
}>;

export type ResolvedGameTicketZone<TSession> = Readonly<{
	principal: string;
	zone: string;
	session: TSession;
}>;

/** A SvelteKit `RequestHandler` (export it as `POST`) plus a load-time resolver. */
export type GameTicketHandler<TEvent extends GameTicketRequestEvent, TSession> = ((
	event: TEvent
) => Promise<Response>) &
	Readonly<{
		/**
		 * Principal and server-chosen zone for a page `load` (no ticket, no
		 * rate limit); undefined without a session or zone.
		 */
		resolve(event: TEvent): Promise<ResolvedGameTicketZone<TSession> | undefined>;
	}>;

/** Response body of a successful ticket request. */
export type GameTicketResponse = Readonly<{
	ticket: string;
	zone: string;
	role: GameTicketRole;
	/** Unix milliseconds. */
	expiresAt: number;
}>;

class TicketRefusal extends Error {
	readonly status: number;
	readonly code: string;
	readonly extra: Readonly<Record<string, unknown>>;

	constructor(status: number, code: string, message: string, extra: Readonly<Record<string, unknown>> = {}) {
		super(message);
		this.status = status;
		this.code = code;
		this.extra = extra;
	}
}

function json(status: number, body: unknown): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: {
			'content-type': 'application/json; charset=utf-8',
			'cache-control': 'no-store',
			'x-content-type-options': 'nosniff'
		}
	});
}

function limiterFrom(option: GameTicketRateLimit | undefined): GameTicketRateLimiter | undefined {
	if (option === false) return undefined;
	if (option === undefined) return createGameTicketRateLimiter(10, 60_000);
	if ('take' in option && typeof option.take === 'function') return option;
	const { max, windowMs } = option as Readonly<{ max: number; windowMs: number }>;
	return createGameTicketRateLimiter(max, windowMs);
}

function defaultPrincipal(session: unknown): string | undefined {
	const id = (session as GameTicketSessionLike)?.user?.id;
	return typeof id === 'string' && id.length > 0 ? id : undefined;
}

function defaultName(session: unknown): string | undefined {
	const user = (session as GameTicketSessionLike)?.user;
	const name = user?.name ?? user?.username;
	return typeof name === 'string' && name.trim().length > 0 ? name : undefined;
}

function checkOrigin(request: Request, url: URL, allowed: ReadonlySet<string>): void {
	const origin = request.headers.get('origin');
	if (origin === null || (origin !== url.origin && !allowed.has(origin))) {
		throw new TicketRefusal(403, 'cross_origin', 'Ticket requests must come from this site.');
	}
	// Browsers that send Fetch Metadata say so explicitly.
	const site = request.headers.get('sec-fetch-site');
	if (site !== null && site !== 'same-origin' && !allowed.has(origin)) {
		throw new TicketRefusal(403, 'cross_origin', 'Ticket requests must come from this site.');
	}
}

async function readBody(request: Request): Promise<{ zone?: string; role?: GameTicketRole }> {
	const length = request.headers.get('content-length');
	if (length !== null && Number(length) > MAX_BODY_BYTES) {
		throw new TicketRefusal(413, 'body_too_large', 'Ticket request body is too large.');
	}
	if (request.body === null) return {};
	const reader = request.body.getReader();
	const chunks: Uint8Array[] = [];
	let total = 0;
	for (;;) {
		const { value, done } = await reader.read();
		if (done) break;
		total += value.byteLength;
		if (total > MAX_BODY_BYTES) {
			await reader.cancel().catch(() => undefined);
			throw new TicketRefusal(413, 'body_too_large', 'Ticket request body is too large.');
		}
		chunks.push(value);
	}
	if (total === 0) return {};
	const contentType = request.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase();
	if (contentType !== 'application/json') {
		throw new TicketRefusal(415, 'unsupported_media_type', 'Send the ticket request as JSON.');
	}
	const bytes = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
	} catch {
		throw new TicketRefusal(400, 'invalid_body', 'Ticket request body is not valid JSON.');
	}
	if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
		throw new TicketRefusal(400, 'invalid_body', 'Ticket request body must be an object.');
	}
	const body = parsed as Record<string, unknown>;
	for (const key of Object.keys(body)) {
		// Identity, ownership and rewards are never caller input.
		if (!BODY_KEYS.has(key)) {
			throw new TicketRefusal(400, 'unexpected_field', `Unexpected ticket request field \`${key}\`.`);
		}
	}
	const result: { zone?: string; role?: GameTicketRole } = {};
	if (body.zone !== undefined) {
		if (typeof body.zone !== 'string' || body.zone.length === 0 || body.zone.length > 128) {
			throw new TicketRefusal(400, 'invalid_zone', 'zone must be a short string.');
		}
		result.zone = body.zone;
	}
	if (body.role !== undefined) {
		if (body.role !== 'player' && body.role !== 'observer') {
			throw new TicketRefusal(400, 'invalid_role', 'role must be player or observer.');
		}
		result.role = body.role;
	}
	return result;
}

/**
 * A SvelteKit `POST` handler that mints a `distributed_game` admission
 * ticket for the **verified session's** principal and the zone **the server
 * chooses**. The request body may only carry `{ zone?, role? }` hints
 * (unknown fields → 400); identity, zone ownership and rewards are never
 * accepted from it. Every response is `cache-control: no-store`.
 *
 * ```ts
 * // src/lib/server/game.ts
 * export const tickets = createGameTicketHandler({
 *   secret: () => env.GAME_TICKET_SECRET,
 *   getSession: ({ locals }) => locals.auth(),
 *   zone: ({ principal }) => zoneOwnedBy(principal)
 * });
 * // src/routes/api/game/ticket/+server.ts
 * export const POST = tickets;
 * ```
 */
export function createGameTicketHandler<TEvent extends GameTicketRequestEvent, TSession = unknown>(
	options: CreateGameTicketHandlerOptions<TEvent, TSession>
): GameTicketHandler<TEvent, TSession> {
	if (options === null || typeof options !== 'object') {
		throw new TypeError('createGameTicketHandler requires options');
	}
	if (typeof options.getSession !== 'function' || typeof options.zone !== 'function') {
		throw new TypeError('createGameTicketHandler requires getSession and zone');
	}
	const ttlSeconds = options.ttlSeconds ?? 60;
	if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 1 || ttlSeconds > MAX_GAME_TICKET_TTL_SECONDS) {
		throw new TypeError(`ttlSeconds must be 1..=${MAX_GAME_TICKET_TTL_SECONDS}`);
	}
	const roles = new Set(options.roles ?? ROLES);
	for (const role of roles) {
		if (!ROLES.includes(role)) throw new TypeError(`unknown game ticket role ${String(role)}`);
	}
	const allowedOrigins = new Set(
		(options.allowedOrigins ?? []).map((origin) => {
			const parsed = new URL(origin);
			if (parsed.origin !== origin) {
				throw new TypeError(`allowedOrigins entry ${origin} must be a bare origin`);
			}
			return origin;
		})
	);
	const limiter = limiterFrom(options.rateLimit);
	const now = options.now ?? Date.now;
	const principalOf = options.principal ?? defaultPrincipal;
	const nameOf = options.name ?? defaultName;

	const authenticate = async (event: TEvent): Promise<{ session: TSession; principal: string } | undefined> => {
		let session: TSession | null | undefined;
		try {
			session = await options.getSession(event);
		} catch {
			return undefined;
		}
		if (session === null || session === undefined) return undefined;
		const principal = principalOf(session, event);
		if (typeof principal !== 'string' || principal.length === 0) return undefined;
		return { session, principal };
	};

	const chooseZone = async (
		event: TEvent,
		session: TSession,
		principal: string,
		requested: string | undefined,
		role: GameTicketRole
	): Promise<string | undefined> => {
		const zone = await options.zone({ event, session, principal, requested, role });
		return typeof zone === 'string' && zone.length > 0 ? zone : undefined;
	};

	const handler = async (event: TEvent): Promise<Response> => {
		try {
			if (event.request.method !== 'POST') {
				throw new TicketRefusal(405, 'method_not_allowed', 'Use POST.');
			}
			checkOrigin(event.request, event.url, allowedOrigins);
			const verified = await authenticate(event);
			if (verified === undefined) {
				throw new TicketRefusal(401, 'unauthenticated', 'Sign in to join the game.');
			}
			if (limiter !== undefined) {
				let allowed: boolean;
				try {
					allowed = await limiter.take(verified.principal, now());
				} catch {
					// Store unreachable / timed out: fail closed.
					throw new TicketRefusal(
						503,
						'rate_limit_unavailable',
						'Tickets are temporarily unavailable; try again shortly.'
					);
				}
				if (allowed !== true) {
					throw new TicketRefusal(429, 'rate_limited', 'Too many ticket requests; wait a moment.');
				}
			}
			const body = await readBody(event.request);
			const role = body.role ?? 'player';
			if (!roles.has(role)) {
				throw new TicketRefusal(403, 'role_not_allowed', `Role ${role} is not allowed here.`);
			}
			const zone = await chooseZone(event, verified.session, verified.principal, body.zone, role);
			if (zone === undefined) {
				throw new TicketRefusal(403, 'no_zone', 'There is no zone for you to join.');
			}
			if (body.zone !== undefined && body.zone !== zone) {
				throw new TicketRefusal(409, 'zone_mismatch', 'Join the zone the server assigned.', { zone });
			}
			let secret: GameTicketSecret | undefined;
			try {
				secret = typeof options.secret === 'function' ? options.secret() : options.secret;
			} catch {
				secret = undefined;
			}
			const secretLength =
				typeof secret === 'string' ? new TextEncoder().encode(secret).byteLength : secret?.byteLength ?? 0;
			if (secret === undefined || secretLength < MIN_GAME_TICKET_SECRET_BYTES) {
				throw new TicketRefusal(503, 'not_configured', 'The game connection is not configured.');
			}
			const name = nameOf(verified.session, event) ?? undefined;
			const minted = await mintGameTicket({
				secret,
				principal: verified.principal,
				zone,
				role,
				...(name === undefined ? {} : { name }),
				ttlSeconds,
				now: now()
			});
			const response: GameTicketResponse = {
				ticket: minted.ticket,
				zone: minted.zone,
				role: minted.role,
				expiresAt: minted.expiresAt
			};
			return json(200, response);
		} catch (error) {
			if (error instanceof TicketRefusal) {
				return json(error.status, { error: error.code, message: error.message, ...error.extra });
			}
			if (error instanceof TypeError) {
				// Claim values the zone server would refuse (e.g. an oversize principal).
				return json(400, { error: 'invalid_claims', message: 'The ticket could not be issued.' });
			}
			throw error;
		}
	};

	return Object.assign(handler, {
		async resolve(event: TEvent): Promise<ResolvedGameTicketZone<TSession> | undefined> {
			const verified = await authenticate(event);
			if (verified === undefined) return undefined;
			const zone = await chooseZone(event, verified.session, verified.principal, undefined, 'player');
			return zone === undefined ? undefined : Object.freeze({ ...verified, zone });
		}
	});
}
