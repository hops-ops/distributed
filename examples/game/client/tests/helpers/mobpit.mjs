/**
 * Headless Mob Pit helpers: a server on its own `celld dev` (the Crawler
 * and presence cells), a throwaway data dir and a scripted hunter
 * that walks to the nearest mob, faces it and swings until `done()` is true.
 */
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BUTTON, isMob, isPlayer, mobFields, playerFields } from '../../src/generated/mobpit/index.ts';
import { freePort, startCelld } from './celld.mjs';
import { sleep, startDemo } from './demo-server.mjs';

export function tempDataDir() {
	return mkdtempSync(join(tmpdir(), 'mobpit-js-'));
}

/**
 * celld + `game-demos <demo>` (mobpit / leaderboard) on a fixed port that
 * celld's outbox relay targets. `restart()` restarts only the game server
 * (same port, data dir and cells); `stop()` stops both.
 */
export async function startPit(demo = 'mobpit') {
	const port = await freePort();
	const celld = await startCelld({ relayPort: port });
	const env = { ...celld.env, MOBPIT_DATA_DIR: tempDataDir() };
	let server;
	try {
		server = await startDemo(demo, { env, port });
	} catch (error) {
		await celld.stop();
		throw error;
	}
	return {
		celld,
		get httpUrl() {
			return server.httpUrl;
		},
		async restart() {
			await server.stop();
			server = await startDemo(demo, { env, port });
		},
		async stop() {
			await server.stop();
			await celld.stop();
		}
	};
}

/** Hunt mobs (one command frame per 30 Hz tick) until `done()` or timeout. */
export async function hunt(pit, done, timeoutMs = 45_000) {
	const { client } = pit;
	const deadline = Date.now() + timeoutMs;
	while (!done()) {
		if (Date.now() > deadline) throw new Error('hunt timed out');
		const rendered = client.render();
		const meEntity = rendered.find((e) => e.id === pit.hello()?.entity && isPlayer(e));
		const me = meEntity && playerFields(meEntity);
		const mobs = rendered.filter((e) => isMob(e) && !e.stale).map(mobFields);
		let input = { mx: 0, my: 0, buttons: 0 };
		if (me !== undefined && mobs.length > 0 && !me.dead) {
			const target = mobs
				.map((m) => ({ m, d: Math.hypot(m.x - me.x, m.y - me.y) }))
				.sort((a, b) => a.d - b.d)[0];
			const dx = target.m.x - me.x;
			const dy = target.m.y - me.y;
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			input = { mx: Math.round(dx * k), my: Math.round(dy * k), buttons: near ? BUTTON.attack : 0 };
		}
		client.sendInput(input);
		await sleep(1000 / 30);
	}
}
