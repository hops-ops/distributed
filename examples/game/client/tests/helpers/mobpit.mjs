/**
 * Headless Mob Pit helpers: a throwaway data dir and a scripted hunter that
 * walks to the nearest mob, faces it and swings until `done()` is true.
 */
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BUTTON_ATTACK } from '../../src/mobpit/client.ts';
import { sleep } from './demo-server.mjs';

export function tempDataDir() {
	return mkdtempSync(join(tmpdir(), 'mobpit-js-'));
}

/** Hunt mobs (one command frame per 30 Hz tick) until `done()` or timeout. */
export async function hunt(pit, done, timeoutMs = 45_000) {
	const { client } = pit;
	const deadline = Date.now() + timeoutMs;
	while (!done()) {
		if (Date.now() > deadline) throw new Error('hunt timed out');
		const rendered = client.render();
		const me = rendered.find((e) => e.id === pit.hello()?.entity && e.kindName === 'player');
		const mobs = rendered.filter((e) => e.kindName === 'mob' && !e.stale);
		let input = { mx: 0, my: 0, buttons: 0 };
		if (me !== undefined && mobs.length > 0 && !me.fields.dead) {
			const target = mobs
				.map((m) => ({ m, d: Math.hypot(m.fields.x - me.fields.x, m.fields.y - me.fields.y) }))
				.sort((a, b) => a.d - b.d)[0];
			const dx = target.m.fields.x - me.fields.x;
			const dy = target.m.fields.y - me.fields.y;
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			input = { mx: Math.round(dx * k), my: Math.round(dy * k), buttons: near ? BUTTON_ATTACK : 0 };
		}
		client.sendInput(input);
		await sleep(1000 / 30);
	}
}
