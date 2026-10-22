/**
 * Headless SvelteKit demo (production build, real `game-demos mobpit`
 * services, real `mobpit-service`, Mob Pit's rooms on a multi-node celld):
 * dev session sign-in → the page joins the entrance room with a
 * session-minted ticket → keyboard movement → auto-hunt kills a mob → the
 * celld Crawler cell commits it, its domain event goes through the cell
 * outbox and celld's Queue to the services' relay, onto the bus, and is
 * projected into `player_stats` → the `@live` GraphQL queries (MyStats,
 * bound to the session; Leaderboard) update without a reload, for the
 * killer and for a second signed-in player → both players walk through
 * doorways into other rooms (one on another celld node: the debug overlay
 * shows each room's node) → the room they left goes dormant, then asleep
 * on the live minimap (`zone_activities` @live) → walking back restores it
 * (toast, minimap active) → a player who reloads, or closes the browser and
 * signs in again, is the same player in the same room at the same place →
 * the SSR HTML after a reload has the settled
 * numbers → the network panel's latency slider raises the RTT on every room
 * (and the server ghost trails the predicted player) → no page errors. The leaderboard is the services' Redis XP board,
 * served by mobpit-service's GraphQL board roots (SSR-seeded, `@live` from
 * the board change notices on the bus). Needs REDIS_URL.
 *
 * celld: a 3-node fleet in production mode on one RustFS bucket (docker
 * compose, `client/tests/helpers/fleet.mjs`) when docker and the aws CLI
 * are available (required with MOBPIT_FLEET_REQUIRED=1); otherwise a
 * single `celld dev` and the cross-node check is skipped.
 *
 * Ports: GAME_DEMOS_PORT (default 2967), SVELTEKIT_PORT (default 5573),
 * MOBPIT_SERVICE_PORT (default GAME_DEMOS_PORT + 2). Needs the lifecycle
 * build (`distributed build examples/game/server`, which runs `vite build`
 * with the generated `$distributed` client), the binaries
 * (GAME_DEMOS_BIN / MOBPIT_SERVICE_BIN, else `cargo run`), and celld
 * (CELLD_BIN, with the built `examples/game/cells` worker).
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

import { chromium } from 'playwright';

import { startCelld } from '../../client/tests/helpers/celld.mjs';
import { startFleet } from '../../client/tests/helpers/fleet.mjs';

const APP = fileURLToPath(new URL('..', import.meta.url));
const SERVER_DIR = fileURLToPath(new URL('../../server/', import.meta.url));
const GAME_PORT = Number(process.env.GAME_DEMOS_PORT ?? 2967);
const UI_PORT = Number(process.env.SVELTEKIT_PORT ?? 5573);
const SERVICE_PORT = Number(process.env.MOBPIT_SERVICE_PORT ?? GAME_PORT + 2);
const ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const GRAPHQL = `http://127.0.0.1:${SERVICE_PORT}/graphql`;

/** The SvelteKit server's API signing key (private) and the service's JWKS (public). */
function apiKeys() {
	const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'P-256' });
	const kid = randomBytes(8).toString('hex');
	const priv = { ...privateKey.export({ format: 'jwk' }), kid, alg: 'ES256' };
	const pub = { ...publicKey.export({ format: 'jwk' }), kid, alg: 'ES256', use: 'sig' };
	return { signingJwk: JSON.stringify(priv), jwks: JSON.stringify({ keys: [pub] }) };
}

function start(command, args, options, ready) {
	const child = spawn(command, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
	let output = '';
	const up = new Promise((resolve, reject) => {
		const onData = (chunk) => {
			output += chunk;
			if (ready.test(output)) resolve();
		};
		child.stdout.on('data', onData);
		child.stderr.on('data', onData);
		child.on('exit', (code) => reject(new Error(`${command} exited with ${code}:\n${output}`)));
	});
	return {
		up,
		output: () => output,
		async stop() {
			if (child.exitCode !== null || child.signalCode !== null) return;
			const exited = new Promise((resolve) => child.on('exit', resolve));
			child.kill('SIGTERM');
			const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
			await exited;
			clearTimeout(timer);
		}
	};
}

/** Largest frame-to-frame move (m) of the own world position / view, ≈ one tick of walking. */
const MAX_JUMP = 0.25;

/**
 * Walk `page`'s player through the doorway on `side`; resolves with the new
 * room once the handoff completed. Asserts the world stays continuous: the
 * own world position (= the view centre) never moves more than a tick's
 * walk between frames, across the doorway too (room-local positions would
 * jump by a whole room).
 */
async function walkThrough(page, side) {
	const goal = { east: [21, 1.7], west: [-21, 1.7], north: [1.7, 21], south: [1.7, -21] }[side];
	const room = () => page.evaluate(() => document.querySelector('canvas')?.dataset.room);
	const world = () => page.evaluate(() => document.querySelector('canvas')?.dataset.world?.split(',').map(Number));
	const deaths = () => page.evaluate(() => Number(document.querySelector('canvas')?.dataset.deaths ?? 0));
	const from = await room();
	for (let attempt = 1; ; attempt += 1) {
		// Measured only on a walk you survive: a death respawns you at a
		// spawn point (a cut by design, not a doorway jump).
		await page.waitForFunction(() => document.querySelector('canvas')?.dataset.dead === 'false', null, { timeout: 15_000 });
		const died = await deaths();
		await page.evaluate(() => (document.querySelector('canvas').dataset.jump = '0'));
		const before = await world();
		const arrived = await walkInto(page, goal, room, from);
		// The new room's first snapshot, then the held prediction resumes there.
		await page.waitForSelector(`[data-testid=zone]:text-is("${arrived}")`, { timeout: 10_000 });
		await page.waitForTimeout(500);
		if ((await deaths()) !== died) {
			assert.ok(attempt < 3, `died on every walk ${side} out of ${from}`);
			console.error(`walk ${side} out of ${from}: died on the way (attempt ${attempt}); walking again`);
			if (arrived !== from) {
				// Back (unmeasured), then the measured walk again.
				const back = { east: [-21, 1.7], west: [21, 1.7], north: [1.7, -21], south: [1.7, 21] }[side];
				await page.waitForFunction(() => document.querySelector('canvas')?.dataset.dead === 'false', null, { timeout: 15_000 });
				assert.equal(await walkInto(page, back, room, arrived), from);
				await page.waitForSelector(`[data-testid=zone]:text-is("${from}")`, { timeout: 10_000 });
			}
			continue;
		}
		const jump = Number(await page.evaluate(() => document.querySelector('canvas').dataset.jump));
		const after = await world();
		const [dx, dy] = { east: [1, 0], west: [-1, 0], north: [0, 1], south: [0, -1] }[side];
		console.error(`doorway ${from} → ${arrived}: largest frame move ${jump} m, world ${before} → ${after}`);
		if (jump > MAX_JUMP) console.error(`JUMP DEBUG ${await page.evaluate(() => JSON.stringify({ jump: document.querySelector('canvas').dataset.jumpDebug, failed: document.querySelector('canvas').dataset.handoffFailed, closed: document.querySelector('canvas').dataset.zoneClosed }))}`);
		assert.ok(jump <= MAX_JUMP, `${from} → ${arrived}: the view jumped ${jump} m in one frame`);
		// Moved on in the doorway's direction, a few metres, not a room.
		const along = (after[0] - before[0]) * dx + (after[1] - before[1]) * dy;
		assert.ok(along > 0 && Math.hypot(after[0] - before[0], after[1] - before[1]) < 40, `${before} → ${after}`);
		return arrived;
	}
}

async function walkInto(page, goal, room, from) {
	const held = new Set();
	const hold = async (keys) => {
		for (const k of [...held]) if (!keys.includes(k)) (await page.keyboard.up(k), held.delete(k));
		for (const k of keys) if (!held.has(k)) (await page.keyboard.down(k), held.add(k));
	};
	try {
		for (const start = Date.now(); Date.now() - start < 60_000; ) {
			const now = await room();
			if (now !== from) return now;
			const me = await page.evaluate(() => document.querySelector('canvas')?.dataset.me);
			const [x, y] = (me ?? '0,0').split(',').map(Number);
			const keys = [];
			if (goal[0] - x > 0.3) keys.push('KeyD');
			if (goal[0] - x < -0.3) keys.push('KeyA');
			if (goal[1] - y > 0.3) keys.push('KeyW');
			if (goal[1] - y < -0.3) keys.push('KeyS');
			await hold(keys);
			await page.waitForTimeout(80);
		}
		throw new Error(`walking ${side} out of ${from} timed out`);
	} finally {
		await hold([]);
	}
}

/** Walk `page`'s player to room-local `goal` in its room (keyboard). */
async function walkTo(page, goal) {
	const held = new Set();
	const hold = async (keys) => {
		for (const k of [...held]) if (!keys.includes(k)) (await page.keyboard.up(k), held.delete(k));
		for (const k of keys) if (!held.has(k)) (await page.keyboard.down(k), held.add(k));
	};
	try {
		for (const start = Date.now(); Date.now() - start < 30_000; ) {
			const me = await page.evaluate(() => document.querySelector('canvas')?.dataset.me);
			const [x, y] = (me ?? '0,0').split(',').map(Number);
			if (Math.hypot(goal[0] - x, goal[1] - y) < 0.6) return;
			const keys = [];
			if (goal[0] - x > 0.3) keys.push('KeyD');
			if (goal[0] - x < -0.3) keys.push('KeyA');
			if (goal[1] - y > 0.3) keys.push('KeyW');
			if (goal[1] - y < -0.3) keys.push('KeyS');
			await hold(keys);
			await page.waitForTimeout(60);
		}
		throw new Error(`walking to ${goal} timed out`);
	} finally {
		await hold([]);
	}
}

/** Rooms the page keeps awake (proximity wake). */
const nearRooms = (page) => page.evaluate(() => (document.querySelector('[data-testid=near]')?.dataset.rooms ?? '').split(' ').filter(Boolean));

/** The minimap's view of `zone` (the zone status read model, live). */
const roomStatus = (page, zone) => page.getAttribute(`[data-testid="room-${zone}"]`, 'data-status');

test('sign in, join, move, kill, walk through rooms on celld, live read models for two players', { timeout: 600_000 }, async () => {
	// The leaderboard is the Redis board the services write and
	// mobpit-service serves through its GraphQL board roots.
	const redisUrl = process.env.REDIS_URL;
	assert.ok(redisUrl, 'REDIS_URL is required: the SvelteKit leaderboard is the Redis board (docker compose up -d redis)');
	const redisPrefix = `mobpit-sveltekit-${randomBytes(6).toString('hex')}`;
	const dataDir = mkdtempSync(join(tmpdir(), 'mobpit-sveltekit-'));
	const busUrl = `sqlite:${join(dataDir, 'bus.db')}`;
	const keys = apiKeys();
	const bin = process.env.GAME_DEMOS_BIN;
	// The rooms and the Crawler cells run on celld; its outbox Queue relays
	// the domain events to the services, which publish them onto the bus.
	const roomVars = { ZONE_HIBERNATE_AFTER_MS: '2500' };
	const fleet = await startFleet({ relayPort: GAME_PORT, vars: roomVars });
	const celld = fleet ?? (await startCelld({ relayPort: GAME_PORT, vars: { ...roomVars, CELLD_ZONE_NODE: 'celld-dev' } }));
	const roomsUrls = (fleet?.urls ?? [celld.url]).map((u) => u.replace(/^http/, 'ws')).join(',');
	const secret = celld.ticketSecret;
	const gameEnv = { ...process.env, ...celld.env, REDIS_URL: redisUrl, MOBPIT_REDIS_PREFIX: redisPrefix };
	const gameArgs = ['mobpit', '--port', String(GAME_PORT), '--data-dir', join(dataDir, 'local'), '--projection-bus', busUrl];
	const game = bin
		? start(bin, gameArgs, { env: gameEnv }, /LISTENING/)
		: start('cargo', ['run', '-q', '--', ...gameArgs], { cwd: SERVER_DIR, env: gameEnv }, /LISTENING/);
	const serviceEnv = {
		...process.env,
		BIND: `127.0.0.1:${SERVICE_PORT}`,
		DATABASE_URL: `sqlite:${join(dataDir, 'service.db')}?mode=rwc`,
		MOBPIT_BUS_URL: busUrl,
		REDIS_URL: redisUrl,
		MOBPIT_REDIS_PREFIX: redisPrefix,
		MOBPIT_API_ISSUER: 'mobpit-sveltekit',
		MOBPIT_API_AUDIENCE: 'mobpit-service',
		MOBPIT_API_JWKS: keys.jwks,
		MOBPIT_PROTOCOL_KEY: randomBytes(32).toString('hex'),
		MOBPIT_UI_ORIGINS: ORIGIN
	};
	const serviceBin = process.env.MOBPIT_SERVICE_BIN;
	const service = serviceBin
		? start(serviceBin, [], { env: serviceEnv }, /LISTENING/)
		: start('cargo', ['run', '-q', '-p', 'mobpit_service', '--bin', 'mobpit-service'], { cwd: SERVER_DIR, env: serviceEnv }, /LISTENING/);
	const ui = start(process.execPath, ['build'], {
		cwd: APP,
		env: {
			...process.env,
			PORT: String(UI_PORT),
			HOST: '127.0.0.1',
			ORIGIN,
			AUTH_SECRET: randomBytes(32).toString('hex'),
			GAME_DEV_AUTH: '1',
			GAME_TICKET_SECRET: secret,
			GAME_ZONES_URL: roomsUrls,
			// The ticket handler reads the player's last room (presence cell).
			CELLD_URL: celld.env.CELLD_URL,
			DISTRIBUTED_INTERNAL_SECRET: celld.env.DISTRIBUTED_INTERNAL_SECRET,
			MOBPIT_API_SIGNING_JWK: keys.signingJwk,
			MOBPIT_GRAPHQL_URL: GRAPHQL
		}
	}, /Listening on/);
	let browser;
	try {
		await Promise.all([game.up, service.up, ui.up]);

		// The read model refuses requests without a verified Bearer (no
		// identity headers are trusted).
		const forged = await fetch(GRAPHQL, {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'x-user-id': 'dev-ana', 'x-roles': 'user' },
			body: JSON.stringify({ query: '{ player_stats { player_id xp } }' })
		});
		assert.equal(forged.status, 401);

		// No session → no ticket; a body cannot name the principal.
		const anonymous = await fetch(`${ORIGIN}/api/game/ticket`, {
			method: 'POST', headers: { origin: ORIGIN, 'content-type': 'application/json' }, body: '{"zone":"pit.0.0"}'
		});
		assert.equal(anonymous.status, 401);
		assert.equal(anonymous.headers.get('cache-control'), 'no-store');

		browser = await chromium.launch();
		const errors = [];
		const signIn = async (username) => {
			const context = await browser.newContext();
			const p = await context.newPage();
			p.on('pageerror', (error) => errors.push(`${username} pageerror: ${error.message}`));
			p.on('console', (message) => {
				// A failed resource is reported with its URL below.
				if (message.type() === 'error' && !message.text().startsWith('Failed to load resource')) {
					errors.push(`${username} console: ${message.text()}`);
				}
			});
			p.on('response', (response) => {
				// The framework's browser reload lifecycle polls this `distributed
				// dev` side channel in every build; a production server answers 404
				// ("no lifecycle"), which the client treats as such.
				if (response.status() === 404 && new URL(response.url()).pathname === '/__distributed/lifecycle') return;
				if (response.status() >= 400) errors.push(`${username} ${response.status()} ${response.url()}`);
			});
			await p.goto(ORIGIN);
			await p.fill('input[name=username]', username);
			await p.click('[data-testid=signin]');
			await p.waitForURL(`${ORIGIN}/pit`);
			assert.equal(await p.textContent('[data-testid=user]'), username);
			return p;
		};
		const page = await signIn('ana');

		// With the session cookie: a ticket for the entrance (or a room the
		// player rejoins); a smuggled principal, something that is not a room
		// or a cross-site origin is refused.
		const cookie = (await page.context().cookies()).map((c) => `${c.name}=${c.value}`).join('; ');
		const ticket = (body, origin = ORIGIN) =>
			fetch(`${ORIGIN}/api/game/ticket`, { method: 'POST', headers: { cookie, origin, 'content-type': 'application/json' }, body: JSON.stringify(body) });
		const minted = await ticket({ zone: 'pit.0.0' });
		assert.equal(minted.status, 200);
		const claims = JSON.parse(Buffer.from((await minted.json()).ticket.split('.')[1], 'base64url').toString());
		assert.equal(claims.sub, 'dev-ana');
		assert.equal((await ticket({ zone: 'pit.0.0', principal: 'admin' })).status, 400);
		assert.equal((await ticket({ zone: 'pit-2' })).status, 409);
		assert.equal((await ticket({ zone: 'pit.3.-1' })).status, 200, 'rejoining a room');
		assert.equal((await ticket({ zone: 'pit.0.0' }, 'http://evil.example')).status, 403);

		// HUD: connected to the entrance room, RTT measured, WASM prediction loaded.
		await page.waitForSelector('[data-testid=status]:text-is("open")', { timeout: 30_000 });
		assert.equal(await page.textContent('[data-testid=zone]'), 'pit.0.0');
		await page.waitForFunction(() => /^\d+$/.test(document.querySelector('[data-testid=rtt]')?.textContent ?? ''), null, { timeout: 10_000 });
		await page.waitForSelector('[data-testid=prediction]:text-is("wasm")', { timeout: 10_000 });

		// Movement (keyboard → predicted + authoritative position).
		await page.waitForFunction(() => document.querySelector('canvas')?.dataset.me !== undefined, null, { timeout: 10_000 });
		const start = await page.evaluate(() => document.querySelector('canvas').dataset.me.split(',').map(Number));
		await page.keyboard.down('KeyD');
		await page.waitForTimeout(700);
		await page.keyboard.up('KeyD');
		const moved = await page.evaluate(() => document.querySelector('canvas').dataset.me.split(',').map(Number));
		assert.ok(moved[0] > start[0] + 0.5 || Math.hypot(moved[0] - start[0], moved[1] - start[1]) > 0.5, `moved ${start} → ${moved}`);

		// Jump (Q): the predicted height rises and comes back to the floor.
		await page.keyboard.down('KeyQ');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.z ?? 0) > 0.5, null, { timeout: 5_000 });
		await page.keyboard.up('KeyQ');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.z ?? 1) === 0, null, { timeout: 5_000 });

		// Network panel: the latency slider sets the rooms' simulated network
		// (dev route → the cells worker's dev settings → every room on celld
		// delays its sockets); the RTT rises, the server ghost trails the
		// predicted player while walking, and it all drops back.
		const rtt = () => page.textContent('[data-testid=rtt]').then(Number);
		// The RTT is smoothed (1/8 per 500 ms ping) and seeded by the first
		// sample, which can be slow while the page boots: let it settle to
		// a loopback value before taking the baseline.
		await page
			.waitForFunction(() => Number(document.querySelector('[data-testid=rtt]')?.textContent) < 40, null, { timeout: 15_000 })
			.catch(async (e) => {
				throw new Error(`base rtt stayed ${await rtt()}: ${e}`);
			});
		const baseRtt = await rtt();
		assert.equal((await fetch(`${ORIGIN}/api/dev/netsim`)).status, 401, 'signed-out: no network control');
		await page.locator('[data-testid=latency]').fill('100');
		await page.waitForSelector('[data-testid=netsim-status]:text-is("applied to every room")', { timeout: 5_000 });
		await page.waitForFunction((b) => Number(document.querySelector("[data-testid=rtt]")?.textContent) > b + 120, baseRtt, { timeout: 20_000 }).catch(async (e) => { throw new Error(`rtt stayed ${await rtt()} (base ${baseRtt}): ${e}`); });
		await page.keyboard.down('KeyA');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.ghostGap ?? 0) > 0.3, null, { timeout: 5_000 });
		await page.keyboard.up('KeyA');
		console.error(`network panel: rtt ${baseRtt} → ${await rtt()} ms with 100 ms one way, ghost gap seen > 0.3 m`);
		await page.locator('[data-testid=latency]').fill('0');
		await page.waitForFunction((b) => Number(document.querySelector('[data-testid=rtt]')?.textContent) < b + 60, baseRtt, { timeout: 15_000 });
		assert.ok(Number(await page.textContent('[data-testid=corrections]')) >= 0);

		// The world above the room (its hood's cell, the borough's weather):
		// nobody felled a boss yet, so the storm darkens the view.
		await page.waitForSelector('[data-testid=weather]:text-is("storm")', { timeout: 15_000 });
		assert.equal(await page.textContent('[data-testid=hood-tier]'), '0');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.dark ?? 0) > 0.3, null, { timeout: 5_000 });

		// Attack: the server's swing reaches the client and the cone is drawn.
		await page.keyboard.down('Space');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.swings ?? 0) > 0, null, { timeout: 5_000 });
		await page.keyboard.up('Space');

		// A second signed-in player watches the same leaderboard.
		let bob = await signIn('bob');
		await bob.waitForSelector('[data-testid=leaderboard]', { state: 'attached' });
		assert.equal(await bob.locator('[data-testid=leaderboard] li', { hasText: 'ana' }).count(), 0);

		// Settled fact: auto-hunt kills a mob; the cell commits it (XP decided
		// by the cell); the domain event is projected into player_stats and the
		// @live queries push it — no reload, no zone profile/board events.
		assert.equal(await page.textContent('[data-testid=xp]'), '0');
		const navigations = [];
		page.on('framenavigated', (frame) => frame === page.mainFrame() && navigations.push(frame.url()));
		await page.check('[data-testid=autohunt]');
		await page.waitForFunction(() => Number(document.querySelector('[data-testid=xp]')?.textContent) > 0, null, { timeout: 90_000 });
		await page.waitForSelector('.saved', { timeout: 10_000 });
		await page.waitForSelector('[data-testid=leaderboard] li.me', { timeout: 15_000 });
		await page.uncheck('[data-testid=autohunt]');
		await page.waitForFunction(() => document.querySelector('[data-testid=unsaved]')?.textContent === '0', null, { timeout: 15_000 });
		// Every kill is saved; let the projection and the (debounced) live
		// push catch up: the shown numbers stop changing.
		let xp = -1;
		let kills = -1;
		for (let stable = 0, start = Date.now(); stable < 3; ) {
			assert.ok(Date.now() - start < 20_000, 'read model settles');
			await page.waitForTimeout(500);
			const now = [Number(await page.textContent('[data-testid=xp]')), Number(await page.textContent('[data-testid=kills]'))];
			stable = now[0] === xp && now[1] === kills ? stable + 1 : 0;
			[xp, kills] = now;
		}
		assert.ok([10, 25, 60].some((tier) => xp >= tier) && kills >= 1, `xp ${xp} kills ${kills}`);
		assert.deepEqual(navigations, [], 'the read model updated without a reload');

		// The other player's leaderboard (the Redis board via the GraphQL
		// board roots) updated live too: the zone server's board change
		// notice crossed the bus to mobpit-service, which pushed it.
		assert.equal(await bob.getAttribute('[data-testid=leaderboard]', 'data-live'), 'active');
		await bob.waitForFunction(
			(expected) => [...document.querySelectorAll('[data-testid=leaderboard] li')].some((li) => li.textContent === expected),
			`ana — ${xp} XP`,
			{ timeout: 15_000 }
		);

		// Bow: auto-hunt turns ana to the nearest mob; R looses arrows; the
		// room rewinds the mobs to what she saw (lag compensation) and its
		// verdicts come back (green markers). First person turns the view.
		await page.check('[data-testid=autohunt]');
		await page.check('[data-testid=first-person]');
		for (let i = 0; i < 30; i += 1) {
			if (Number(await page.evaluate(() => document.querySelector('canvas')?.dataset.confirmed ?? 0)) > 0) break;
			await page.keyboard.press('KeyR');
			await page.waitForTimeout(450);
		}
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.confirmed ?? 0) > 0, null, { timeout: 10_000 });
		const bow = await page.evaluate(() => ({ ...document.querySelector('canvas').dataset }));
		console.error(`bow: ${bow.shots} arrows, ${bow.resolved} verdicts, ${bow.confirmed} confirmed hits, ${bow.agreed} as seen`);
		await page.uncheck('[data-testid=first-person]');
		await page.uncheck('[data-testid=autohunt]');

		// Rooms. The overlay shows which celld node runs the entrance (celld
		// chose it; the game only shows it).
		await page.waitForSelector('[data-testid=room-pit\\.0\\.0][data-status=active]', { timeout: 15_000 });
		const nodeOf = async (p, zone) => {
			await p.waitForFunction((z) => document.querySelector('[data-testid=overlay]')?.textContent?.includes(z) && document.querySelector('[data-testid=node]')?.textContent !== 'unknown', zone, { timeout: 20_000 });
			return p.textContent('[data-testid=node]');
		};
		const entranceNode = await nodeOf(page, 'pit.0.0');
		// Ana walks east, Bob north: the entrance empties.
		assert.equal(await walkThrough(page, 'east'), 'pit.1.0');
		await page.waitForSelector('[data-testid=zone]:text-is("pit.1.0")', { timeout: 10_000 });
		const eastNode = await nodeOf(page, 'pit.1.0');
		assert.equal(await walkThrough(bob, 'north'), 'pit.0.1');
		const northNode = await nodeOf(bob, 'pit.0.1');
		// Next door: Bob watches the 8 rooms around pit.0.1 (passively) and
		// sees the mobs of the awake ones — pit.1.0, where Ana is.
		await bob.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.nextDoorMobs ?? 0) > 0, null, { timeout: 15_000 });
		await bob.waitForFunction(() => Number(document.querySelector('[data-testid=watching]')?.textContent) >= 1, null, { timeout: 15_000 });
		// Bob reloads, then closes the browser and signs in again: the same
		// player, back in pit.0.1 where he stood (the room kept his body
		// through the reload; the ticket handler chose his last room).
		const here = (p) => p.evaluate(() => document.querySelector('canvas')?.dataset.me?.split(',').map(Number));
		const sameSpot = async (p, what, at) => {
			await p.waitForSelector('[data-testid=zone]:text-is("pit.0.1")', { timeout: 20_000 });
			await p.waitForSelector('[data-testid=status]:text-is("open")', { timeout: 20_000 });
			await p.waitForFunction(() => document.querySelector('canvas')?.dataset.me !== undefined, null, { timeout: 10_000 });
			await p.waitForTimeout(300);
			const now = await here(p);
			console.error(`${what}: ${at} → ${now}`);
			assert.ok(Math.hypot(now[0] - at[0], now[1] - at[1]) < 0.3, `${what}: same place ${at} → ${now}`);
			assert.equal(await p.textContent('[data-testid=user]'), 'bob');
		};
		await bob.waitForTimeout(300);
		const bobAt = await here(bob);
		await bob.reload();
		await sameSpot(bob, 'bob reloads', bobAt);
		await bob.context().close();
		bob = await signIn('bob');
		await sameSpot(bob, 'bob signs in again in a new browser', bobAt);
		console.error(`rooms: pit.0.0 on ${entranceNode}, pit.1.0 on ${eastNode}, pit.0.1 on ${northNode}`);
		if (fleet !== null) {
			assert.ok(fleet.nodes.includes(entranceNode) && fleet.nodes.includes(eastNode), `${entranceNode} ${eastNode}`);
			assert.notEqual(eastNode, entranceNode, 'the doorway led into a room on another celld node');
		}
		// Both step away from the doorways they came through (standing in one
		// keeps the room behind it awake: "wake rooms in reach").
		assert.deepEqual(await nearRooms(page), ['pit.0.0'], 'Ana at the doorway keeps the entrance awake');
		await walkTo(page, [3, 3]);
		await walkTo(bob, [2, 1]);
		await page.waitForFunction(() => document.querySelector('[data-testid=near]')?.dataset.rooms === '', null, { timeout: 10_000 });
		// The left room goes dormant, then asleep, live on both minimaps —
		// although Bob, next door, watches it (a passive watcher keeps no
		// room awake).
		await page.waitForSelector('[data-testid=room-pit\\.0\\.0][data-status=hibernated]', { timeout: 30_000 });
		await bob.waitForSelector('[data-testid=room-pit\\.0\\.0][data-status=hibernated]', { timeout: 10_000 });
		assert.equal(await roomStatus(page, 'pit.1.0'), 'active');
		// Ana walks back: the entrance wakes from its snapshot.
		assert.equal(await walkThrough(page, 'west'), 'pit.0.0');
		await page.waitForSelector('[data-testid=toasts] .toast:has-text("restored from hibernation")', { timeout: 15_000 });
		await page.waitForSelector('[data-testid=room-pit\\.0\\.0][data-status=active]', { timeout: 15_000 });
		await bob.waitForSelector('[data-testid=room-pit\\.0\\.0][data-status=active]', { timeout: 15_000 });
		// Proximity wake (on by default): walking up to the west doorway wakes
		// the room behind it, which never ran, before Ana gets there; N turns
		// it off and the room is let go.
		assert.equal(await page.isChecked('[data-testid=wake]'), true, 'wake rooms in reach: on by default');
		await walkTo(page, [-3, 3]);
		await walkTo(page, [-13, 1.7]);
		await page.waitForFunction(() => (document.querySelector('[data-testid=near]')?.dataset.rooms ?? '').split(' ').includes('pit.-1.0'), null, { timeout: 10_000 });
		await page.waitForSelector('[data-testid="room-pit.-1.0"][data-status=active]', { timeout: 20_000 });
		assert.equal(await page.textContent('[data-testid=zone]'), 'pit.0.0', 'woken before the doorway');
		console.error(`proximity wake: pit.-1.0 woke with Ana 7 m from its doorway (kept awake: ${await nearRooms(page)})`);
		await page.keyboard.press('KeyN');
		assert.equal(await page.isChecked('[data-testid=wake]'), false);
		assert.equal(await page.textContent('[data-testid=near]'), 'off');
		await page.waitForSelector('[data-testid="room-pit.-1.0"][data-status=hibernated]', { timeout: 30_000 });
		await page.keyboard.press('KeyN');
		assert.equal(await page.isChecked('[data-testid=wake]'), true);
		// A kill in the restored room settles and the read model follows live.
		const before = Number(await page.textContent('[data-testid=kills]'));
		await page.check('[data-testid=autohunt]');
		await page.waitForFunction((k) => Number(document.querySelector('[data-testid=kills]')?.textContent) > k, before, { timeout: 90_000 });
		await page.uncheck('[data-testid=autohunt]');
		await page.waitForFunction(() => document.querySelector('[data-testid=unsaved]')?.textContent === '0', null, { timeout: 15_000 });
		xp = -1;
		kills = -1;
		for (let stable = 0, start = Date.now(); stable < 3; ) {
			assert.ok(Date.now() - start < 20_000, 'read model settles again');
			await page.waitForTimeout(500);
			const now = [Number(await page.textContent('[data-testid=xp]')), Number(await page.textContent('[data-testid=kills]'))];
			stable = now[0] === xp && now[1] === kills ? stable + 1 : 0;
			[xp, kills] = now;
		}
		await bob.waitForFunction(
			(expected) => [...document.querySelectorAll('[data-testid=leaderboard] li')].some((li) => li.textContent === expected),
			`ana — ${xp} XP`,
			{ timeout: 15_000 }
		);

		// World events: the entrance's borough runs its own (scheduled by its
		// cell); the boroughs read model lists the running one live, and the
		// room shows it to its players.
		// (One with time to spare, so the room's view is checked while it runs.)
		await page.waitForFunction(
			() => Number(/(\d+) s left/.exec(document.querySelector('[data-testid=events] li[data-borough="0.0"]')?.textContent ?? '')?.[1] ?? 0) >= 8,
			null,
			{ timeout: 90_000 }
		);
		assert.equal(await page.getAttribute('[data-testid=events]', 'data-live'), 'active');
		console.error(`world event (read model, @live): ${await page.textContent('[data-testid=events] li[data-borough="0.0"]')}`);
		await page.waitForSelector('[data-testid=world-event]', { timeout: 10_000 });

		// Live content update (dev admin): every room freezes with the change
		// notes, switches to the pack and resumes; the page patches live.
		assert.equal(await page.getAttribute('[data-testid=content]', 'data-version'), '');
		const notes = 'Frostfall: slower, tougher mobs.';
		const started = await page.evaluate(
			(n) => fetch('/api/dev/rollout', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ version: 'frostfall', notes: n }) }).then((r) => r.status),
			notes
		);
		assert.equal(started, 200);
		await page.waitForSelector('[data-testid=frozen]', { timeout: 10_000 });
		assert.match(await page.textContent('[data-testid=frozen]'), /Frostfall: slower, tougher mobs\./);
		await page.waitForSelector('[data-testid=frozen]', { state: 'detached', timeout: 15_000 });
		await page.waitForFunction(() => document.querySelector('[data-testid=content]')?.getAttribute('data-version') === 'frostfall', null, { timeout: 5_000 });
		assert.equal(await page.textContent('[data-testid=world-content]'), 'Frostfall');
		assert.equal(await page.$('[data-testid=stale]'), null, 'patched live: not stale');
		console.error(`content: rolled out ${await page.textContent('[data-testid=content]')} (froze with the notes, resumed)`);

		// SSR after a reload: the settled numbers are in the server HTML.
		const html = await page.evaluate(() => fetch('/pit', { headers: { accept: 'text/html' } }).then((r) => r.text()));
		assert.match(html, new RegExp(`data-testid="kills"[^>]*>${kills}<`));
		assert.match(html, new RegExp(`data-testid="xp"[^>]*>${xp}<`));
		assert.match(html, /ana — \d+ XP/);
		await page.reload();
		assert.equal(Number(await page.textContent('[data-testid=xp]')), xp, 'hydrated from the SSR seed');

		// Navigation away disposes the client (no errors, socket closed).
		await page.click('header a');
		await page.waitForURL(`${ORIGIN}/`);
		await page.waitForTimeout(300);
		assert.deepEqual(errors, []);
	} finally {
		await browser?.close();
		await ui.stop();
		await service.stop();
		await game.stop();
		await celld.stop();
		rmSync(dataDir, { recursive: true, force: true });
	}
});
