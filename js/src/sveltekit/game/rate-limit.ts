/**
 * Per-principal limiters for `createGameTicketHandler`: the in-memory
 * default (one process) and a Redis-backed one shared by every instance
 * behind a load balancer. No Redis package is imported: the caller passes
 * its own node-redis or ioredis client (or a plain `eval` function).
 */

/**
 * Sliding-window limiter; `take` resolves false when `key` is over budget.
 * A `take` that throws or rejects makes the ticket handler answer 503
 * (`rate_limit_unavailable`): an unreachable limiter fails closed.
 */
export type GameTicketRateLimiter = Readonly<{
	take(key: string, now: number): boolean | Promise<boolean>;
}>;

/** In-memory sliding window per key (one process; use a shared store behind a load balancer). */
export function createGameTicketRateLimiter(max: number, windowMs: number): GameTicketRateLimiter {
	checkBudget(max, windowMs);
	const starts = new Map<string, number[]>();
	return Object.freeze({
		take(key: string, now: number): boolean {
			const since = now - windowMs;
			if (starts.size > 10_000) {
				for (const [existing, times] of starts) {
					if (times.every((at) => at <= since)) starts.delete(existing);
				}
			}
			const recent = (starts.get(key) ?? []).filter((at) => at > since);
			if (recent.length >= max) {
				starts.set(key, recent);
				return false;
			}
			recent.push(now);
			starts.set(key, recent);
			return true;
		}
	});
}

function checkBudget(max: number, windowMs: number): void {
	if (!Number.isSafeInteger(max) || max < 1 || !Number.isFinite(windowMs) || windowMs <= 0) {
		throw new TypeError('game ticket rate limit needs max >= 1 and windowMs > 0');
	}
}

/**
 * The Redis commands the limiter sends, structurally: node-redis v4/v5
 * (`sendCommand(['EVAL', …])`) and ioredis (`call('EVAL', …)`) both satisfy
 * it without an adapter.
 */
export type GameTicketRedisClient =
	| Readonly<{ sendCommand(args: string[]): Promise<unknown> }>
	| Readonly<{ call(command: string, ...args: string[]): Promise<unknown> }>;

/** Runs `script` with `keys` / `args` atomically (Redis `EVAL` semantics). */
export type GameTicketRedisEval = (script: string, keys: string[], args: string[]) => Promise<unknown>;

export type RedisGameTicketRateLimiterOptions = Readonly<{
	/** A connected node-redis or ioredis client (or anything with `sendCommand` / `call`). */
	client?: GameTicketRedisClient;
	/** Instead of `client`: run the script yourself (cluster clients, other stores with Lua). */
	eval?: GameTicketRedisEval;
	/** Key prefix; the principal is appended. Default `distributed:game-ticket:`. */
	prefix?: string;
	/** Tickets per principal per window, across every instance. */
	limit: number;
	/** Window length in milliseconds. */
	windowMs: number;
	/** A store call slower than this counts as a store error. Default 1000 ms. */
	timeoutMs?: number;
	/**
	 * What a store error (unreachable, timeout, script error) means:
	 * `'closed'` (default) rejects, so the handler answers 503
	 * `rate_limit_unavailable`; `'open'` lets the request through unlimited.
	 */
	onUnavailable?: 'closed' | 'open';
	/** Called with every store error (log / metric). Never throws into the request. */
	onError?(error: unknown): void;
}>;

/** Thrown (fail-closed) when the shared store cannot answer. */
export class GameTicketRateLimiterUnavailable extends Error {
	constructor(cause: unknown) {
		super('game ticket rate limiter store unavailable', { cause });
		this.name = 'GameTicketRateLimiterUnavailable';
	}
}

/**
 * Sliding-window log in one sorted set per principal: drop entries older
 * than the window, count, add when under the limit, expire the key with the
 * window. One `EVAL`, so concurrent instances cannot both take the last slot.
 * `now` comes from the instance clock (keep instances NTP-synchronized).
 */
export const GAME_TICKET_RATE_LIMIT_SCRIPT = `local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
if redis.call('ZCARD', key) >= limit then
  redis.call('PEXPIRE', key, window)
  return 0
end
redis.call('ZADD', key, now, ARGV[4])
redis.call('PEXPIRE', key, window)
return 1`;

function evalWith(options: RedisGameTicketRateLimiterOptions): GameTicketRedisEval {
	if (typeof options.eval === 'function') {
		if (options.client !== undefined) throw new TypeError('pass either client or eval, not both');
		return options.eval;
	}
	const client = options.client as Record<string, unknown> | undefined;
	if (client === undefined || client === null || typeof client !== 'object') {
		throw new TypeError('createRedisGameTicketRateLimiter needs a client or eval');
	}
	const command = (script: string, keys: string[], args: string[]): string[] => [
		script,
		String(keys.length),
		...keys,
		...args
	];
	if (typeof client.sendCommand === 'function') {
		const send = client.sendCommand as (args: string[]) => Promise<unknown>;
		return (script, keys, args) => send.call(client, ['EVAL', ...command(script, keys, args)]);
	}
	if (typeof client.call === 'function') {
		const call = client.call as (name: string, ...args: string[]) => Promise<unknown>;
		return (script, keys, args) => call.call(client, 'EVAL', ...command(script, keys, args));
	}
	throw new TypeError('the Redis client needs sendCommand (node-redis) or call (ioredis)');
}

let sequence = 0;

/**
 * A ticket limiter shared by every SvelteKit instance through Redis (or any
 * store that runs Redis Lua). Same semantics as the in-memory limiter:
 * `limit` tickets per principal in any `windowMs`, refused requests are not
 * counted.
 *
 * ```ts
 * import { createClient } from 'redis';
 * const redis = await createClient({ url: env.REDIS_URL }).connect();
 * createGameTicketHandler({ …, rateLimit: createRedisGameTicketRateLimiter({ client: redis, limit: 10, windowMs: 60_000 }) });
 * ```
 */
export function createRedisGameTicketRateLimiter(options: RedisGameTicketRateLimiterOptions): GameTicketRateLimiter {
	if (options === null || typeof options !== 'object') {
		throw new TypeError('createRedisGameTicketRateLimiter requires options');
	}
	checkBudget(options.limit, options.windowMs);
	if (!Number.isSafeInteger(options.windowMs)) throw new TypeError('windowMs must be whole milliseconds');
	const run = evalWith(options);
	const prefix = options.prefix ?? 'distributed:game-ticket:';
	const timeoutMs = options.timeoutMs ?? 1_000;
	if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError('timeoutMs must be > 0');
	const failOpen = options.onUnavailable === 'open';
	if (options.onUnavailable !== undefined && options.onUnavailable !== 'open' && options.onUnavailable !== 'closed') {
		throw new TypeError("onUnavailable must be 'closed' or 'open'");
	}
	const instance = Math.random().toString(36).slice(2, 10);
	const limit = String(options.limit);
	const windowMs = String(options.windowMs);

	const unavailable = (error: unknown): boolean => {
		try {
			options.onError?.(error);
		} catch {
			// A logging hook never decides the request.
		}
		if (failOpen) return true;
		throw new GameTicketRateLimiterUnavailable(error);
	};

	return Object.freeze({
		async take(key: string, now: number): Promise<boolean> {
			// Unique per take, so two instances at the same millisecond both count.
			const member = `${now}:${instance}:${(sequence = (sequence + 1) % Number.MAX_SAFE_INTEGER)}`;
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				const reply = await Promise.race([
					run(GAME_TICKET_RATE_LIMIT_SCRIPT, [`${prefix}${key}`], [String(now), windowMs, limit, member]),
					new Promise<never>((_, reject) => {
						timer = setTimeout(() => reject(new Error(`rate limit store timed out after ${timeoutMs} ms`)), timeoutMs);
					})
				]);
				const allowed = Number(reply);
				if (allowed !== 0 && allowed !== 1) throw new Error(`unexpected rate limit reply ${String(reply)}`);
				return allowed === 1;
			} catch (error) {
				return unavailable(error);
			} finally {
				if (timer !== undefined) clearTimeout(timer);
			}
		}
	});
}
