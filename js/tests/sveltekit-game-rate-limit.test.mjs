/**
 * Shared ticket rate limiting for `createGameTicketHandler`:
 * - async `take` is awaited; two handler instances over one fake Redis
 *   enforce one combined budget; the window slides; store errors and
 *   timeouts fail closed (503) unless `onUnavailable: 'open'`;
 * - node-redis (`sendCommand`) and ioredis (`call`) shapes both work.
 *
 * Opt-in against a real Redis: `DISTRIBUTED_REDIS_E2E=1 npm test`
 * (`REDIS_URL`, default `redis://127.0.0.1:6379`; a tiny RESP client below,
 * no Redis package). Skipped otherwise.
 */
import assert from 'node:assert/strict';
import { connect } from 'node:net';
import { randomUUID } from 'node:crypto';
import test from 'node:test';

import {
	GAME_TICKET_RATE_LIMIT_SCRIPT,
	createGameTicketHandler,
	createRedisGameTicketRateLimiter
} from '@hops-ops/distributed/sveltekit/game/server';

const SECRET = 'rate-limit-fixture-secret-0123456789abcdef';
const ORIGIN = 'https://game.example';
const REDIS_E2E = process.env.DISTRIBUTED_REDIS_E2E === '1';

/**
 * In-process stand-in for Redis running GAME_TICKET_RATE_LIMIT_SCRIPT: the
 * same sorted-set steps, executed atomically (single-threaded JS). Keyed by
 * the script text so a changed script fails loudly here.
 */
function fakeRedis() {
	const sets = new Map();
	const store = {
		calls: 0,
		failing: undefined,
		hang: false,
		sets,
		async eval(script, keys, args) {
			store.calls += 1;
			if (store.failing) throw store.failing;
			if (store.hang) return new Promise(() => {});
			assert.equal(script, GAME_TICKET_RATE_LIMIT_SCRIPT);
			const [key] = keys;
			const [now, window, limit, member] = [Number(args[0]), Number(args[1]), Number(args[2]), args[3]];
			const entries = (sets.get(key) ?? []).filter((entry) => entry.score > now - window);
			sets.set(key, entries);
			if (entries.length >= limit) return 0;
			entries.push({ score: now, member });
			return 1;
		}
	};
	return store;
}

/** A node-redis-shaped client (`sendCommand(['EVAL', script, n, ...keys, ...args])`). */
const nodeRedisShape = (store) => ({
	async sendCommand(args) {
		assert.equal(args[0], 'EVAL');
		const n = Number(args[2]);
		return store.eval(args[1], args.slice(3, 3 + n), args.slice(3 + n));
	}
});

/** An ioredis-shaped client (`call('EVAL', script, n, ...keys, ...args)`). */
const ioredisShape = (store) => ({
	async call(command, script, n, ...rest) {
		assert.equal(command, 'EVAL');
		return store.eval(script, rest.slice(0, Number(n)), rest.slice(Number(n)));
	}
});

function instance(rateLimit, clock) {
	const handler = createGameTicketHandler({
		secret: SECRET,
		getSession: (event) => event.locals.session,
		zone: () => 'pit-1',
		rateLimit,
		now: () => clock.now
	});
	return (principal = 'u1') =>
		handler({
			request: new Request(`${ORIGIN}/api/game/ticket`, { method: 'POST', headers: { origin: ORIGIN } }),
			url: new URL(`${ORIGIN}/api/game/ticket`),
			locals: { session: { user: { id: principal } } }
		});
}

async function status(promise, expected, error) {
	const response = await promise;
	const body = await response.json();
	assert.equal(response.status, expected, JSON.stringify(body));
	assert.equal(response.headers.get('cache-control'), 'no-store');
	if (error !== undefined) assert.equal(body.error, error);
	return body;
}

test('two handler instances over one store share one budget per principal', async () => {
	const store = fakeRedis();
	const clock = { now: 1_700_000_000_000 };
	// Separate limiter objects (as in separate processes), one store.
	const a = instance(createRedisGameTicketRateLimiter({ client: nodeRedisShape(store), limit: 3, windowMs: 60_000 }), clock);
	const b = instance(createRedisGameTicketRateLimiter({ client: ioredisShape(store), limit: 3, windowMs: 60_000 }), clock);
	await status(a(), 200);
	await status(b(), 200);
	await status(a(), 200);
	await status(b(), 429, 'rate_limited');
	await status(a(), 429, 'rate_limited');
	// Another principal has its own budget; the key carries the prefix.
	await status(b('u2'), 200);
	assert.deepEqual([...store.sets.keys()].sort(), ['distributed:game-ticket:u1', 'distributed:game-ticket:u2']);
	// Refusals are not counted: the 3 recorded takes are all successes.
	assert.equal(store.sets.get('distributed:game-ticket:u1').length, 3);
});

test('the window slides: a slot frees exactly when its oldest take leaves the window', async () => {
	const store = fakeRedis();
	const clock = { now: 1_000_000 };
	const limiter = createRedisGameTicketRateLimiter({ eval: store.eval, prefix: 't:', limit: 2, windowMs: 10_000 });
	const call = instance(limiter, clock);
	await status(call(), 200); // t = 0
	clock.now += 4_000;
	await status(call(), 200); // t = 4 s
	await status(call(), 429, 'rate_limited');
	clock.now += 5_999; // t = 9.999 s: the first take is still inside
	await status(call(), 429, 'rate_limited');
	clock.now += 1; // t = 10 s: it left
	await status(call(), 200);
	await status(call(), 429, 'rate_limited');
	clock.now += 10_000;
	await status(call(), 200);
	await status(call(), 200);
});

test('store errors fail closed with 503 (or open when configured) and reach onError', async () => {
	const store = fakeRedis();
	const clock = { now: 5_000 };
	const errors = [];
	const closed = instance(
		createRedisGameTicketRateLimiter({ client: nodeRedisShape(store), limit: 5, windowMs: 1_000, onError: (e) => errors.push(e) }),
		clock
	);
	store.failing = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:6379'), { code: 'ECONNREFUSED' });
	const body = await status(closed(), 503, 'rate_limit_unavailable');
	assert.doesNotMatch(body.message, /ECONNREFUSED|6379/, 'store details stay on the server');
	assert.equal(errors.length, 1);

	const open = instance(
		createRedisGameTicketRateLimiter({ client: nodeRedisShape(store), limit: 1, windowMs: 1_000, onUnavailable: 'open' }),
		clock
	);
	await status(open(), 200);
	await status(open(), 200, undefined); // unlimited while the store is down

	// A hung store is a store error after timeoutMs.
	store.failing = undefined;
	store.hang = true;
	const slow = instance(createRedisGameTicketRateLimiter({ eval: store.eval, limit: 5, windowMs: 1_000, timeoutMs: 20 }), clock);
	await status(slow(), 503, 'rate_limit_unavailable');

	// A garbage reply is a store error too, not "allowed".
	const odd = instance(createRedisGameTicketRateLimiter({ eval: async () => 'OK', limit: 5, windowMs: 1_000 }), clock);
	await status(odd(), 503, 'rate_limit_unavailable');

	// Any custom async limiter: awaited; rejection → 503; resolve(false) → 429.
	await status(instance({ take: async () => false }, clock)(), 429, 'rate_limited');
	await status(instance({ take: async () => { throw new Error('down'); } }, clock)(), 503, 'rate_limit_unavailable');
	await status(instance({ take: () => { throw new Error('down'); } }, clock)(), 503, 'rate_limit_unavailable');
});

test('createRedisGameTicketRateLimiter validates its options', () => {
	const store = fakeRedis();
	assert.throws(() => createRedisGameTicketRateLimiter({ limit: 1, windowMs: 1 }), /client or eval/);
	assert.throws(() => createRedisGameTicketRateLimiter({ client: {}, limit: 1, windowMs: 1 }), /sendCommand/);
	assert.throws(() => createRedisGameTicketRateLimiter({ eval: store.eval, limit: 0, windowMs: 1 }), /max >= 1/);
	assert.throws(() => createRedisGameTicketRateLimiter({ eval: store.eval, limit: 1, windowMs: 1.5 }), /whole milliseconds/);
	assert.throws(() => createRedisGameTicketRateLimiter({ eval: store.eval, limit: 1, windowMs: 1, onUnavailable: 'maybe' }), /onUnavailable/);
	assert.throws(
		() => createRedisGameTicketRateLimiter({ eval: store.eval, client: nodeRedisShape(store), limit: 1, windowMs: 1 }),
		/either client or eval/
	);
});

/** Minimal RESP2 client: enough for `sendCommand(['EVAL', …])` (integer / error / bulk replies). */
async function respClient(url) {
	const { hostname, port } = new URL(url);
	const socket = connect({ host: hostname || '127.0.0.1', port: Number(port || 6379) });
	await new Promise((resolve, reject) => {
		socket.once('connect', resolve);
		socket.once('error', reject);
	});
	let buffer = '';
	const waiting = [];
	socket.on('data', (chunk) => {
		buffer += chunk.toString('utf8');
		for (;;) {
			const end = buffer.indexOf('\r\n');
			if (end < 0 || waiting.length === 0) return;
			const line = buffer.slice(0, end);
			const kind = line[0];
			if (kind === '$') {
				const length = Number(line.slice(1));
				if (length < 0) {
					buffer = buffer.slice(end + 2);
					waiting.shift().resolve(null);
					continue;
				}
				if (buffer.length < end + 2 + length + 2) return;
				const value = buffer.slice(end + 2, end + 2 + length);
				buffer = buffer.slice(end + 2 + length + 2);
				waiting.shift().resolve(value);
				continue;
			}
			buffer = buffer.slice(end + 2);
			const next = waiting.shift();
			if (kind === ':') next.resolve(Number(line.slice(1)));
			else if (kind === '+') next.resolve(line.slice(1));
			else next.reject(new Error(line.slice(1)));
		}
	});
	return {
		sendCommand(args) {
			const frame = `*${args.length}\r\n${args.map((a) => `$${Buffer.byteLength(a)}\r\n${a}\r\n`).join('')}`;
			return new Promise((resolve, reject) => {
				waiting.push({ resolve, reject });
				socket.write(frame);
			});
		},
		close: () => socket.end()
	};
}

test('real Redis: the Lua script shares one sliding budget', { skip: !REDIS_E2E, timeout: 30_000 }, async () => {
	const client = await respClient(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379');
	try {
		const prefix = `distributed-test:${randomUUID()}:`;
		const clock = { now: Date.now() };
		const a = instance(createRedisGameTicketRateLimiter({ client, prefix, limit: 2, windowMs: 2_000 }), clock);
		const b = instance(createRedisGameTicketRateLimiter({ client, prefix, limit: 2, windowMs: 2_000 }), clock);
		await status(a(), 200);
		await status(b(), 200);
		await status(a(), 429, 'rate_limited');
		await status(b('other'), 200);
		clock.now += 2_001;
		await status(b(), 200);
		// The key expires with the window (no unbounded growth).
		const ttl = await client.sendCommand(['PTTL', `${prefix}u1`]);
		assert.ok(ttl > 0 && ttl <= 2_000, `pttl ${ttl}`);
	} finally {
		client.close();
	}
});
