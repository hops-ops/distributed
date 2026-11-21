/**
 * SvelteKit integration for `distributed_game` zones:
 * - `mintGameTicket` reproduces `distributed_game/tests/fixtures/sveltekit_tickets.json`
 *   byte for byte; the Rust `net_e2e` test admits those tickets on a real
 *   `GameServer` and refuses the tampered / expired / wrong-zone / replayed
 *   ones (`UPDATE_GOLDEN=1` rewrites the fixture);
 * - `createGameTicketHandler` rejection paths (session, origin, rate limit,
 *   caller-supplied identity, zone choice, secret);
 * - `gameTicketSource`;
 * - the runes adapter (`createGameState` reactive in a compiled Svelte
 *   module under the browser runtime; `useGame` inert during SSR).
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';

import { compile, compileModule } from 'svelte/compiler';
import { render } from 'svelte/server';

import {
	TRUSTED_FIELD_DENYLIST,
	createGameTicketHandler,
	mintGameTicket
} from '@hops-ops/distributed/sveltekit/game/server';
import { createGameState, gameTicketSource } from '@hops-ops/distributed/sveltekit/game';

const FIXTURE = new URL('../../distributed_game/tests/fixtures/sveltekit_tickets.json', import.meta.url);
const SECRET = 'sveltekit-fixture-secret-0123456789abcdef';
const DENYLIST = new URL('../../distributed_game/tests/fixtures/trusted_field_denylist.json', import.meta.url);
const TMP = fileURLToPath(new URL('./.tmp-sveltekit-game/', import.meta.url));

const decodeClaims = (ticket) => JSON.parse(Buffer.from(ticket.split('.')[1], 'base64url').toString('utf8'));
const signatureValid = (ticket, secret) => {
	const [h, p, s] = ticket.split('.');
	return createHmac('sha256', secret).update(`${h}.${p}`).digest('base64url') === s;
};

async function fixtureTickets() {
	// exp = 4_000_000_000 (year 2096): far future; exp = 300: long expired.
	const future = (4_000_000_000 - 300) * 1000;
	const valid = await mintGameTicket({
		secret: SECRET, principal: 'session-user-1', zone: 'z-sveltekit', name: 'Ana',
		ttlSeconds: 300, now: future, nonce: 'fixture-valid'
	});
	const observer = await mintGameTicket({
		secret: SECRET, principal: 'session-user-2', zone: 'z-sveltekit', role: 'observer',
		ttlSeconds: 300, now: future, nonce: 'fixture-observer'
	});
	const expired = await mintGameTicket({
		secret: SECRET, principal: 'session-user-1', zone: 'z-sveltekit',
		ttlSeconds: 300, now: 0, nonce: 'fixture-expired'
	});
	const wrongZone = await mintGameTicket({
		secret: SECRET, principal: 'session-user-1', zone: 'z-elsewhere',
		ttlSeconds: 300, now: future, nonce: 'fixture-wrong-zone'
	});
	// Same signature, payload rewritten to another principal.
	const [h, , s] = (await mintGameTicket({
		secret: SECRET, principal: 'session-user-1', zone: 'z-sveltekit',
		ttlSeconds: 300, now: future, nonce: 'fixture-tampered'
	})).ticket.split('.');
	const forgedClaims = Buffer.from(JSON.stringify({
		sub: 'admin', zone: 'z-sveltekit', exp: 4_000_000_000, nonce: 'fixture-tampered', role: 'player'
	})).toString('base64url');
	const extended = await mintGameTicket({
		secret: SECRET, principal: 'session-user-3', zone: 'z-sveltekit',
		ttlSeconds: 300, now: future, nonce: 'fixture-extended',
		extensions: { match_id: 'm-7', team: 2 }
	});
	const otherKey = await mintGameTicket({
		secret: 'a-different-secret-0123456789abcdef!!', principal: 'session-user-1', zone: 'z-sveltekit',
		ttlSeconds: 300, now: future, nonce: 'fixture-other-key'
	});
	return {
		secret: SECRET,
		zone: 'z-sveltekit',
		valid: { ticket: valid.ticket, principal: 'session-user-1', name: 'Ana', exp: 4_000_000_000, nonce: 'fixture-valid' },
		observer: { ticket: observer.ticket, principal: 'session-user-2' },
		extended: { ticket: extended.ticket, principal: 'session-user-3', ext: { match_id: 'm-7', team: 2 } },
		expired: expired.ticket,
		wrongZone: wrongZone.ticket,
		tampered: `${h}.${forgedClaims}.${s}`,
		otherKey: otherKey.ticket
	};
}

test('mintGameTicket is the PROTOCOL.md §8 format the Rust GameServer admits (fixture)', async () => {
	const tickets = await fixtureTickets();
	if (process.env.UPDATE_GOLDEN === '1') {
		writeFileSync(FIXTURE, `${JSON.stringify(tickets, null, 2)}\n`);
	}
	assert.deepEqual(tickets, JSON.parse(readFileSync(FIXTURE, 'utf8')), 'fixture drift: UPDATE_GOLDEN=1');
	const header = JSON.parse(Buffer.from(tickets.valid.ticket.split('.')[0], 'base64url').toString());
	assert.deepEqual(header, { alg: 'HS256', typ: 'JWT' });
	assert.deepEqual(decodeClaims(tickets.valid.ticket), {
		sub: 'session-user-1', zone: 'z-sveltekit', exp: 4_000_000_000, nonce: 'fixture-valid', role: 'player', name: 'Ana'
	});
	assert.ok(signatureValid(tickets.valid.ticket, SECRET));
	assert.ok(!signatureValid(tickets.tampered, SECRET));
	assert.equal(decodeClaims(tickets.observer.ticket).role, 'observer');
});

test('extension claims: server-only, deny list shared with the Rust derives', async () => {
	const fixture = JSON.parse(readFileSync(DENYLIST, 'utf8'));
	assert.deepEqual([...TRUSTED_FIELD_DENYLIST], [...fixture.identity, ...fixture.reward, ...fixture.economy]);
	const secret = SECRET;
	const minted = await mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: { match_id: 'm', team: 1 } });
	assert.deepEqual(decodeClaims(minted.ticket).ext, { match_id: 'm', team: 1 });
	const none = await mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: {} });
	assert.equal('ext' in decodeClaims(none.ticket), false);
	for (const name of ['userId', 'Gold', 'pay_out', 'XP', 'sub', 'owner-id']) {
		await assert.rejects(
			mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: { [name]: 1 } }),
			/identity, a reward or an economy value/,
			name
		);
	}
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: ['m'] }), /plain JSON object/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: { at: new Date() } }), /plain JSON object/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: { 'match id': 1 } }), /identifier/);
	await assert.rejects(
		mintGameTicket({ secret, principal: 'p', zone: 'z', extensions: { blob: 'x'.repeat(2100) } }),
		/exceed 2048 bytes/
	);
});

test('ticket handler: claims() adds server-side extensions; the browser body cannot', async () => {
	const contexts = [];
	const { call } = harness({
		claims: (context) => {
			contexts.push(context);
			return { match_id: `match-of-${context.principal}`, team: 2 };
		}
	});
	const body = await expectStatus(call({ body: { zone: 'pit-u1' } }), 200);
	assert.deepEqual(decodeClaims(body.ticket).ext, { match_id: 'match-of-u1', team: 2 });
	assert.equal(contexts[0].zone, 'pit-u1');
	assert.equal(contexts[0].principal, 'u1');

	// Claims are never request fields, whatever they are called.
	for (const field of ['ext', 'claims', 'extensions', 'match_id', 'team']) {
		const fresh = harness({ claims: () => ({ team: 1 }) });
		await expectStatus(fresh.call({ body: { zone: 'pit-u1', [field]: { team: 9 } } }), 400, 'unexpected_field');
	}
	// No callback: no ext claim at all.
	const plain = await expectStatus(harness().call({}), 200);
	assert.equal('ext' in decodeClaims(plain.ticket), false);
	// A callback returning a denied name is refused, not signed.
	await expectStatus(harness({ claims: () => ({ gold: 100 }) }).call({}), 400, 'invalid_claims');
});

test('mintGameTicket rejects short secrets and malformed claims', async () => {
	await assert.rejects(mintGameTicket({ secret: 'short', principal: 'p', zone: 'z' }), /at least 32 bytes/);
	const secret = SECRET;
	await assert.rejects(mintGameTicket({ secret, principal: '', zone: 'z' }), /principal/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p\n', zone: 'z' }), /principal/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'x'.repeat(129) }), /zone/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'z', ttlSeconds: 301 }), /ttlSeconds/);
	await assert.rejects(mintGameTicket({ secret, principal: 'p', zone: 'z', role: 'admin' }), /role/);
	const minted = await mintGameTicket({ secret, principal: 'p', zone: 'z', name: ' \u0000Bo\u0007b ', now: 1_000_000 });
	assert.equal(decodeClaims(minted.ticket).name, 'Bob');
	assert.equal(minted.expiresAt, (1_000 + 60) * 1000);
	const a = await mintGameTicket({ secret, principal: 'p', zone: 'z' });
	const b = await mintGameTicket({ secret, principal: 'p', zone: 'z' });
	assert.notEqual(a.nonce, b.nonce, 'fresh nonce per ticket (single use)');
});

const ORIGIN = 'https://game.example';

function harness(overrides = {}) {
	let clock = 1_700_000_000_000;
	const zones = [];
	const handler = createGameTicketHandler({
		secret: SECRET,
		getSession: (event) => event.locals.session,
		zone: (context) => {
			zones.push(context);
			return context.principal === 'homeless' ? undefined : `pit-${context.principal}`;
		},
		rateLimit: { max: 3, windowMs: 60_000 },
		now: () => clock,
		...overrides
	});
	const call = (init = {}) => {
		const headers = new Headers(init.headers ?? {});
		if (init.origin !== null) headers.set('origin', init.origin ?? ORIGIN);
		if (init.body !== undefined && !headers.has('content-type')) headers.set('content-type', 'application/json');
		const body = init.body === undefined ? undefined : typeof init.body === 'string' ? init.body : JSON.stringify(init.body);
		return handler({
			request: new Request(`${ORIGIN}/api/game/ticket`, { method: init.method ?? 'POST', headers, body }),
			url: new URL(`${ORIGIN}/api/game/ticket`),
			locals: { session: init.session === undefined ? { user: { id: 'u1', name: 'Ana' } } : init.session }
		});
	};
	return { handler, call, zones, advance: (ms) => (clock += ms) };
}

async function expectStatus(promise, status, error) {
	const response = await promise;
	assert.equal(response.status, status);
	assert.equal(response.headers.get('cache-control'), 'no-store');
	const body = await response.json();
	if (error !== undefined) assert.equal(body.error, error);
	return body;
}

test('ticket handler: principal from the verified session, zone chosen by the server', async () => {
	const { call, zones } = harness();
	const body = await expectStatus(call({ body: { zone: 'pit-u1' } }), 200);
	assert.equal(body.zone, 'pit-u1');
	assert.equal(body.role, 'player');
	assert.equal(body.expiresAt, (1_700_000_000 + 60) * 1000);
	assert.ok(signatureValid(body.ticket, SECRET));
	const claims = decodeClaims(body.ticket);
	assert.equal(claims.sub, 'u1');
	assert.equal(claims.zone, 'pit-u1');
	assert.equal(claims.name, 'Ana');
	assert.equal(claims.exp, 1_700_000_000 + 60);
	assert.equal(zones.at(-1).requested, 'pit-u1');
	// An empty body asks for the server's zone; observers are allowed by default.
	assert.equal((await expectStatus(call({}), 200)).zone, 'pit-u1');
	const observer = await expectStatus(call({ body: { role: 'observer' } }), 200);
	assert.equal(decodeClaims(observer.ticket).role, 'observer');
});

test('ticket handler rejects: no session, cross-origin, rate limit, caller-supplied identity', async () => {
	const { call, advance } = harness();
	await expectStatus(call({ session: null }), 401, 'unauthenticated');
	await expectStatus(call({ session: { user: { id: '' } } }), 401, 'unauthenticated');
	await expectStatus(call({ origin: 'https://evil.example' }), 403, 'cross_origin');
	await expectStatus(call({ origin: null }), 403, 'cross_origin');
	await expectStatus(call({ headers: { 'sec-fetch-site': 'cross-site' } }), 403, 'cross_origin');
	await expectStatus(call({ method: 'GET' }), 405, 'method_not_allowed');

	// Identity, ownership and rewards are not request fields.
	for (const field of ['principal', 'sub', 'name', 'xp', 'owner']) {
		const fresh = harness();
		await expectStatus(fresh.call({ body: { zone: 'pit-u1', [field]: 'admin' } }), 400, 'unexpected_field');
	}
	const fresh = harness();
	await expectStatus(fresh.call({ body: 'not json' }), 400, 'invalid_body');
	await expectStatus(fresh.call({ body: '{"zone":"pit-u1"}', headers: { 'content-type': 'text/plain' } }), 415, 'unsupported_media_type');
	await expectStatus(harness().call({ body: { zone: 'x'.repeat(2000) } }), 413, 'body_too_large');
	await expectStatus(harness().call({ body: { role: 'admin' } }), 400, 'invalid_role');

	// A zone the server did not choose: 409 with the server's choice.
	const mismatch = await expectStatus(harness().call({ body: { zone: 'pit-someone-else' } }), 409, 'zone_mismatch');
	assert.equal(mismatch.zone, 'pit-u1');
	await expectStatus(harness().call({ session: { user: { id: 'homeless' } } }), 403, 'no_zone');

	// 3 per minute per principal (the limit counts refused bodies too).
	await expectStatus(call({}), 200);
	await expectStatus(call({}), 200);
	await expectStatus(call({}), 200);
	await expectStatus(call({}), 429, 'rate_limited');
	await expectStatus(call({ session: { user: { id: 'u2' } } }), 200); // per principal
	advance(60_001);
	await expectStatus(call({}), 200);
});

test('ticket handler: secret, roles and origins are server configuration', async () => {
	await expectStatus(harness({ secret: () => undefined }).call({}), 503, 'not_configured');
	await expectStatus(harness({ secret: 'too-short' }).call({}), 503, 'not_configured');
	await expectStatus(harness({ roles: ['player'] }).call({ body: { role: 'observer' } }), 403, 'role_not_allowed');
	const proxied = harness({ allowedOrigins: ['http://localhost:5573'] });
	await expectStatus(proxied.call({ origin: 'http://localhost:5573' }), 200);
	assert.throws(() => createGameTicketHandler({ secret: SECRET, getSession: () => null, zone: () => 'z', ttlSeconds: 0 }), /ttlSeconds/);
	assert.throws(() => createGameTicketHandler({ secret: SECRET, getSession: () => null, zone: () => 'z', allowedOrigins: ['http://a/b'] }), /bare origin/);
	assert.throws(() => createGameTicketHandler({ secret: SECRET, getSession: () => null }), /zone/);
	// A throwing session source is "no session", not a 500.
	await expectStatus(harness({ getSession: () => { throw new Error('jwt'); } }).call({}), 401, 'unauthenticated');
});

test('ticket handler resolve(): the same principal and zone for a page load', async () => {
	const { handler } = harness();
	const event = (session) => ({
		request: new Request(`${ORIGIN}/pit`),
		url: new URL(`${ORIGIN}/pit`),
		locals: { session }
	});
	const resolved = await handler.resolve(event({ user: { id: 'u7' } }));
	assert.equal(resolved.principal, 'u7');
	assert.equal(resolved.zone, 'pit-u7');
	assert.equal(await handler.resolve(event(null)), undefined);
	assert.equal(await handler.resolve(event({ user: { id: 'homeless' } })), undefined);
});

test('gameTicketSource posts only the zone and role, and surfaces refusals', async () => {
	const requests = [];
	const ok = gameTicketSource({
		endpoint: '/t',
		fetch: async (url, init) => {
			requests.push({ url, init });
			return new Response(JSON.stringify({ ticket: 'T', zone: 'z' }), { status: 200 });
		}
	});
	assert.equal(await ok({ zoneId: 'z', role: 'player' }), 'T');
	assert.equal(requests[0].url, '/t');
	assert.equal(requests[0].init.method, 'POST');
	assert.equal(requests[0].init.credentials, 'same-origin');
	assert.deepEqual(JSON.parse(requests[0].init.body), { zone: 'z', role: 'player' });
	const refused = gameTicketSource({
		fetch: async () => new Response(JSON.stringify({ error: 'unauthenticated', message: 'Sign in' }), { status: 401 })
	});
	await assert.rejects(refused({ zoneId: 'z', role: 'player' }), /Sign in/);
	const broken = gameTicketSource({ fetch: async () => new Response('<html>', { status: 502 }) });
	await assert.rejects(broken({ zoneId: 'z', role: 'player' }), /HTTP 502/);
});

test('runes adapter: reactive status, RTT, zone, prediction, typed events; dispose closes', () => {
	// Svelte's reactivity is a no-op under Node's default (server) export
	// condition, so the reactive half runs in a child with the browser runtime.
	mkdirSync(TMP, { recursive: true });
	try {
		const effects = compileModule(
			`export function track(read, log) {
				return $effect.root(() => { $effect(() => { log.push(read()); }); });
			}`,
			{ generate: 'client', filename: 'track.svelte.js' }
		);
		writeFileSync(`${TMP}/track.mjs`, effects.js.code);
		writeFileSync(`${TMP}/run.mjs`, RUNES_SCRIPT);
		const output = execFileSync(process.execPath, ['--conditions=browser', `${TMP}/run.mjs`], {
			encoding: 'utf8',
			cwd: fileURLToPath(new URL('..', import.meta.url))
		});
		assert.match(output, /RUNES OK/);
	} finally {
		rmSync(TMP, { recursive: true, force: true });
	}
});

const RUNES_SCRIPT = `
import assert from 'node:assert/strict';
import { flushSync } from 'svelte';
import { createGameState } from '@hops-ops/distributed/sveltekit/game';
import { track } from './track.mjs';

class Emitter {
	listeners = new Map();
	on(type, fn) { const set = this.listeners.get(type) ?? new Set(); set.add(fn); this.listeners.set(type, set); return () => set.delete(fn); }
	emit(type, ...args) { for (const fn of [...(this.listeners.get(type) ?? [])]) fn(...args); }
}
class FakeConnection extends Emitter {
	state = 'idle';
	set(state) { this.state = state; this.emit('state', state); }
}
class FakeClient extends Emitter {
	zones = new Map(); authority = undefined; closed = false; connects = [];
	zone(id) { return this.zones.get(id); }
	connect(id) {
		const connection = new FakeConnection();
		this.zones.set(id, { zoneId: id, connection });
		this.connects.push(id);
		connection.set('connecting');
		return new Promise((resolve) => { this.welcome = () => {
			connection.set('open');
			this.authority = { zoneId: id, connection };
			this.emit('welcome', id, {});
			this.emit('authority', id, {});
			resolve({ zoneId: id });
		}; });
	}
	close() { this.closed = true; for (const z of this.zones.values()) z.connection.set('closed'); }
}

const EVENTS = {
	profile: { id: 2, name: 'profile', decode: (p) => ({ xp: p[0] }), encode: () => new Uint8Array() },
	kill: { id: 3, name: 'kill', decode: (p) => ({ mob: p[0] }), encode: () => new Uint8Array() }
};
const client = new FakeClient();
let resolvePrediction;
const prediction = { ready: new Promise((r) => (resolvePrediction = r)) };
let created = 0;
const game = createGameState(() => { created += 1; return { client, prediction }; }, { events: EVENTS });
const profile = game.latest('profile');
const kills = game.events('kill', { limit: 2 });
assert.equal(created, 0, 'the client is built lazily (never during SSR)');

const log = [];
const stop = track(() => [game.status, game.zone, game.rttMs, game.predictionReady, profile.current?.xp, kills.items.map((k) => k.mob).join(',')].join('|'), log);
flushSync();
assert.equal(log.at(-1), 'idle|||false||');

const joined = game.connect('pit-1');
flushSync();
assert.equal(created, 1);
assert.equal(log.at(-1), 'connecting|||false||');
client.welcome();
await joined;
flushSync();
assert.equal(log.at(-1), 'open|pit-1||false||');

client.zone('pit-1').connection.emit('pong', { rtt: 41.6, offset: 0, jitter: 2.2 });
resolvePrediction();
await prediction.ready; await Promise.resolve();
flushSync();
assert.equal(log.at(-1), 'open|pit-1|42|true||');

client.emit('event', 'pit-1', { typeId: 2, payload: new Uint8Array([25]) });
client.emit('event', 'pit-1', { typeId: 3, payload: new Uint8Array([7]) });
client.emit('event', 'pit-1', { typeId: 3, payload: new Uint8Array([8]) });
client.emit('event', 'pit-1', { typeId: 3, payload: new Uint8Array([9]) });
client.emit('event', 'pit-1', { typeId: 99, payload: new Uint8Array([1]) });
flushSync();
assert.equal(log.at(-1), 'open|pit-1|42|true|25|9,8', 'latest + bounded newest-first list');

// Errors from the authority zone are surfaced.
client.emit('error', 'pit-1', { code: 4, message: 'unauthorized' });
flushSync();
assert.equal(game.error, 'unauthorized');

// Unchanged values do not re-run effects.
const before = log.length;
client.zone('pit-1').connection.emit('pong', { rtt: 42.1, offset: 0, jitter: 2.4 });
flushSync();
assert.equal(log.length, before);

assert.throws(() => game.latest('nope'), /unknown game event/);

game.dispose();
flushSync();
assert.equal(client.closed, true);
assert.equal(log.at(-1).split('|')[0], 'closed');
// Subscriptions are gone after dispose.
const settled = log.length;
client.emit('event', 'pit-1', { typeId: 2, payload: new Uint8Array([99]) });
flushSync();
assert.equal(log.length, settled);
await assert.rejects(game.connect('pit-2'), /disposed/);
stop();
console.log('RUNES OK');
`;

test('useGame does nothing during SSR (no client, no connection)', async () => {
	const dir = `${TMP.replace(/\/$/, '')}-ssr`;
	mkdirSync(dir, { recursive: true });
	try {
		const component = compile(
			`<script>
				import { useGame } from '@hops-ops/distributed/sveltekit/game';
				const game = useGame(() => { throw new Error('factory must not run on the server'); }, { zone: 'pit-1' });
			</script>
			<p>{game.status}|{game.zone ?? '-'}|{game.connected}</p>`,
			{ generate: 'server', filename: 'Game.svelte' }
		);
		const file = `${dir}/Game.mjs`;
		writeFileSync(file, component.js.code);
		const { default: Game } = await import(pathToFileURL(file).href);
		const { body } = render(Game);
		assert.match(body, /idle\|-\|false/);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test('ticket handler: wake() grants observers the wake claim; players and the browser never get it', async () => {
	const asked = [];
	const { call } = harness({
		rateLimit: false,
		wake: (context) => {
			asked.push(context);
			return context.zone === 'pit-u1';
		}
	});
	const granted = await expectStatus(call({ body: { role: 'observer' } }), 200);
	assert.equal(decodeClaims(granted.ticket).wake, true);
	assert.equal(asked.at(-1).principal, 'u1');
	assert.equal(asked.at(-1).role, 'observer');
	const player = await expectStatus(call({ body: {} }), 200);
	assert.equal('wake' in decodeClaims(player.ticket), false, 'players never carry wake');
	assert.equal(asked.length, 1, 'not asked for players');
	await expectStatus(call({ body: { role: 'observer', wake: true } }), 400);
	const denied = await expectStatus(harness({ rateLimit: false, wake: () => false }).call({ body: { role: 'observer' } }), 200);
	assert.equal('wake' in decodeClaims(denied.ticket), false);
	const none = await expectStatus(harness({ rateLimit: false }).call({ body: { role: 'observer' } }), 200);
	assert.equal('wake' in decodeClaims(none.ticket), false, 'default: never');
	await assert.rejects(mintGameTicket({ secret: SECRET, principal: 'p', zone: 'z', wake: true }), /only observer/);
});
