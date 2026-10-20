/**
 * Headless SvelteKit demo (production build, real `game-demos mobpit` and
 * real `mobpit-service`): dev session sign-in → the page joins the
 * server-chosen zone with a session-minted ticket → keyboard movement →
 * auto-hunt kills a mob → the cell commits it, its domain event is drained to
 * the bus and projected into `player_stats` → the `@live` GraphQL queries
 * (MyStats, bound to the session; Leaderboard) update without a reload, for
 * the killer and for a second signed-in player → the SSR HTML after a reload
 * has the settled numbers → no page errors.
 *
 * Ports: GAME_DEMOS_PORT (default 2967), SVELTEKIT_PORT (default 5573),
 * MOBPIT_SERVICE_PORT (default GAME_DEMOS_PORT + 2). Needs the lifecycle
 * build (`distributed build examples/game/server`, which runs `vite build`
 * with the generated `$distributed` client) and the binaries
 * (GAME_DEMOS_BIN / MOBPIT_SERVICE_BIN, else `cargo run`).
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

test('sign in, join, move, kill, live read model updates for two players', { timeout: 600_000 }, async () => {
	const secret = randomBytes(32).toString('hex');
	const dataDir = mkdtempSync(join(tmpdir(), 'mobpit-sveltekit-'));
	const busUrl = `sqlite:${join(dataDir, 'bus.db')}`;
	const keys = apiKeys();
	const bin = process.env.GAME_DEMOS_BIN;
	const gameArgs = ['mobpit', '--port', String(GAME_PORT), '--data-dir', join(dataDir, 'cells'), '--projection-bus', busUrl];
	const game = bin
		? start(bin, gameArgs, { env: { ...process.env, DISTRIBUTED_GAME_SECRET: secret } }, /LISTENING/)
		: start('cargo', ['run', '-q', '--', ...gameArgs], { cwd: SERVER_DIR, env: { ...process.env, DISTRIBUTED_GAME_SECRET: secret } }, /LISTENING/);
	const serviceEnv = {
		...process.env,
		BIND: `127.0.0.1:${SERVICE_PORT}`,
		DATABASE_URL: `sqlite:${join(dataDir, 'service.db')}?mode=rwc`,
		MOBPIT_BUS_URL: busUrl,
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
			GAME_ZONES_URL: `ws://127.0.0.1:${GAME_PORT}`,
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
			method: 'POST', headers: { origin: ORIGIN, 'content-type': 'application/json' }, body: '{"zone":"pit-1"}'
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

		// With the session cookie: a ticket for the server's zone; a smuggled
		// principal, a foreign zone or a cross-site origin is refused.
		const cookie = (await page.context().cookies()).map((c) => `${c.name}=${c.value}`).join('; ');
		const ticket = (body, origin = ORIGIN) =>
			fetch(`${ORIGIN}/api/game/ticket`, { method: 'POST', headers: { cookie, origin, 'content-type': 'application/json' }, body: JSON.stringify(body) });
		const minted = await ticket({ zone: 'pit-1' });
		assert.equal(minted.status, 200);
		const claims = JSON.parse(Buffer.from((await minted.json()).ticket.split('.')[1], 'base64url').toString());
		assert.equal(claims.sub, 'dev-ana');
		assert.equal((await ticket({ zone: 'pit-1', principal: 'admin' })).status, 400);
		assert.equal((await ticket({ zone: 'pit-2' })).status, 409);
		assert.equal((await ticket({ zone: 'pit-1' }, 'http://evil.example')).status, 403);

		// HUD: connected to the server-chosen zone, RTT measured, WASM prediction loaded.
		await page.waitForSelector('[data-testid=status]:text-is("open")', { timeout: 30_000 });
		assert.equal(await page.textContent('[data-testid=zone]'), 'pit-1');
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

		// Attack: the server's swing reaches the client and the cone is drawn.
		await page.keyboard.down('Space');
		await page.waitForFunction(() => Number(document.querySelector('canvas')?.dataset.swings ?? 0) > 0, null, { timeout: 5_000 });
		await page.keyboard.up('Space');

		// A second signed-in player watches the same leaderboard.
		const bob = await signIn('bob');
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

		// The other player's leaderboard updated live too.
		await bob.waitForFunction(
			(expected) => [...document.querySelectorAll('[data-testid=leaderboard] li')].some((li) => li.textContent === expected),
			`ana — ${xp} XP`,
			{ timeout: 15_000 }
		);

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
		rmSync(dataDir, { recursive: true, force: true });
	}
});
