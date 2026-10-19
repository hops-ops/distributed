/**
 * Headless SvelteKit demo (production build, real `game-demos mobpit`):
 * dev session sign-in → the page joins the server-chosen zone with a
 * session-minted ticket → keyboard movement → auto-hunt kills a mob → the
 * settled fact shows up in the live read model and in the SSR read model
 * after a reload → no page errors. Ports: GAME_DEMOS_PORT (default 2967) and
 * SVELTEKIT_PORT (default 5573). Needs `npm run build` and the server binary
 * (GAME_DEMOS_BIN, else `cargo run`).
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
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
const ORIGIN = `http://127.0.0.1:${UI_PORT}`;

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

test('sign in, join, move, kill, settled read model updates', { timeout: 600_000 }, async () => {
	const secret = randomBytes(32).toString('hex');
	const dataDir = mkdtempSync(join(tmpdir(), 'mobpit-sveltekit-'));
	const bin = process.env.GAME_DEMOS_BIN;
	const gameArgs = ['mobpit', '--port', String(GAME_PORT), '--dev', '--data-dir', dataDir];
	const game = bin
		? start(bin, gameArgs, { env: { ...process.env, DISTRIBUTED_GAME_SECRET: secret } }, /LISTENING/)
		: start('cargo', ['run', '-q', '--', ...gameArgs], { cwd: SERVER_DIR, env: { ...process.env, DISTRIBUTED_GAME_SECRET: secret } }, /LISTENING/);
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
			GAME_SERVER_HTTP: `http://127.0.0.1:${GAME_PORT}`
		}
	}, /Listening on/);
	let browser;
	try {
		await Promise.all([game.up, ui.up]);

		// No session → no ticket; a body cannot name the principal.
		const anonymous = await fetch(`${ORIGIN}/api/game/ticket`, {
			method: 'POST', headers: { origin: ORIGIN, 'content-type': 'application/json' }, body: '{"zone":"pit-1"}'
		});
		assert.equal(anonymous.status, 401);
		assert.equal(anonymous.headers.get('cache-control'), 'no-store');

		browser = await chromium.launch();
		const page = await browser.newPage();
		const errors = [];
		page.on('pageerror', (error) => errors.push(`pageerror: ${error.message}`));
		page.on('console', (message) => message.type() === 'error' && errors.push(`console: ${message.text()}`));

		await page.goto(ORIGIN);
		await page.fill('input[name=username]', 'ana');
		await page.click('[data-testid=signin]');
		await page.waitForURL(`${ORIGIN}/pit`);
		assert.equal(await page.textContent('[data-testid=user]'), 'ana');

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

		// Settled fact: auto-hunt kills a mob; the cell commit settles; the
		// live read model (profile + board pushes) shows it.
		assert.equal(await page.textContent('[data-testid=xp]'), '0');
		await page.check('[data-testid=autohunt]');
		await page.waitForFunction(() => Number(document.querySelector('[data-testid=xp]')?.textContent) > 0, null, { timeout: 90_000 });
		await page.waitForSelector('.saved', { timeout: 10_000 });
		await page.waitForSelector('[data-testid=settled] li.me', { timeout: 10_000 });
		await page.uncheck('[data-testid=autohunt]');
		await page.waitForFunction(() => document.querySelector('[data-testid=unsaved]')?.textContent === '0', null, { timeout: 15_000 });
		const xp = Number(await page.textContent('[data-testid=xp]'));
		const kills = Number(await page.textContent('[data-testid=kills]'));
		assert.ok([10, 25, 60].some((tier) => xp >= tier) && kills >= 1, `xp ${xp} kills ${kills}`);

		// SSR read model after a reload: the settled numbers are in the HTML.
		const html = await page.evaluate(() => fetch('/pit', { headers: { accept: 'text/html' } }).then((r) => r.text()));
		assert.match(html, new RegExp(`data-testid="kills"[^>]*>${kills}<`));

		// Navigation away disposes the client (no errors, socket closed).
		await page.click('header a');
		await page.waitForURL(`${ORIGIN}/`);
		await page.waitForTimeout(300);
		assert.deepEqual(errors, []);
	} finally {
		await browser?.close();
		await ui.stop();
		await game.stop();
		rmSync(dataDir, { recursive: true, force: true });
	}
});
