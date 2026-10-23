/**
 * Headless Mob Pit soak (opt-in: `MOBPIT_SOAK=1`): rooms keep their pace on
 * an aged celld state.
 *
 * A dev state that lived a few days (hundreds of cells, ~90k stored
 * objects) cost rooms 166 dropped ticks per 30 s against 0 on a fresh one,
 * and its restarts took minutes. This ages a state quickly — `ROOMS` rooms
 * woken once, then `CHURN` rooms snapshotting every `AGE_PERSIST_MS` —
 * restarts celld on it (SIGKILL, as the demo stops it) with the worker's
 * real settings and plays: each room must take its player within
 * `MAX_JOIN_MS` (logged: the restart cost) and drop at most
 * `MAX_DROPPED` of its ticks with a late p99 under `MAX_LATE_P99_MS`. It
 * logs the store's growth while played (objects per room-hour).
 *
 * The default (~19k objects, ~600 cells) passes. Around 300 rooms + 16 ×
 * 300 s of churn (~70k objects, ~1,400 cells) a restarted `celld dev`
 * recovers rooms at ~18 a minute and refuses players for minutes: that is
 * the size to stay under (see the dev state notes in examples/game/README.md).
 *
 * Env: MOBPIT_SOAK_ROOMS (default 150), MOBPIT_SOAK_AGE_S (default 120),
 * MOBPIT_SOAK_CHURN (default 4), MOBPIT_SOAK_PLAY_S (default 60),
 * MOBPIT_SOAK_BOTS (played rooms, default 4), MOBPIT_SOAK_PERSIST_MS
 * (snapshot interval while played; default the worker's),
 * MOBPIT_SOAK_KEEP=1 (keep the
 * aged state), MOBPIT_SOAK_FLEET=N (a local N-node fleet on RustFS instead
 * of `celld dev`; needs docker and the aws CLI).
 */
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { startPit, startPitFleet } from './helpers/mobpit.mjs';

const SOAK = process.env.MOBPIT_SOAK === '1';
const env = (key, fallback) => Number(process.env[key] ?? fallback);
const ROOMS = env('MOBPIT_SOAK_ROOMS', 150);
const AGE_S = env('MOBPIT_SOAK_AGE_S', 120);
const CHURN = env('MOBPIT_SOAK_CHURN', 4);
const PLAY_S = env('MOBPIT_SOAK_PLAY_S', 60);
const BOTS = env('MOBPIT_SOAK_BOTS', 4);
const FLEET = env('MOBPIT_SOAK_FLEET', 0);
/** Snapshot interval while aging: many durable writes, fast. */
const AGE_PERSIST_MS = '50';
/** Share of a room's ticks it may drop while played. */
const MAX_DROPPED = 0.01;
const MAX_LATE_P99_MS = 50;
/**
 * How long a room may take to take a player after the restart. A crashed
 * `celld dev` recovers every cell before rooms answer (~80 s at ~730
 * cells on a loaded laptop), so this only catches the pathological.
 */
const MAX_JOIN_MS = 180_000;

test('rooms keep their pace on a long-lived celld state', { skip: !SOAK && 'set MOBPIT_SOAK=1', timeout: 1_800_000 }, async () => {
	const server = FLEET > 0
		? await startPitFleet('mobpit', { ZONE_PERSIST_MS: AGE_PERSIST_MS }, FLEET)
		: await startPit('mobpit', { ZONE_PERSIST_MS: AGE_PERSIST_MS });
	assert.ok(server !== null, 'MOBPIT_SOAK_FLEET needs docker, the aws CLI and CELLD_BIN');
	const clients = [];
	const info = (zone) => new MobPitApi(server.httpUrl, server.roomsUrl).roomInfo(zone);

	async function enter(zone, name) {
		const api = new DevApi({ base: server.httpUrl, name: () => name, storage: memoryStorage() });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		await pit.client.connect(zone);
		await pit.ready;
		await until(() => pit.me()?.predicted === true, 20_000, `${name} in ${zone}`);
		return pit;
	}

	/** `enter`, retried (a room failing while celld recovers) up to `MAX_JOIN_MS`. */
	async function joinRetrying(zone, name) {
		const t0 = Date.now();
		for (let attempts = 1; ; attempts += 1) {
			try {
				return { pit: await enter(zone, name), ms: Date.now() - t0, attempts };
			} catch (error) {
				clients.pop()?.close();
				if (Date.now() - t0 > MAX_JOIN_MS) throw error;
				await sleep(1000);
			}
		}
	}

	/** Wander (a slow circle, 30 Hz command frames) until `until` (ms epoch). */
	async function wander(pit, untilMs, phase = 0) {
		for (let i = 0; Date.now() < untilMs; i += 1) {
			const a = phase + i / 45;
			pit.client.sendInput({ mx: Math.round(Math.cos(a) * 90), my: Math.round(Math.sin(a) * 90), buttons: 0 });
			await sleep(1000 / 30);
		}
	}

	try {
		// Age: wake many rooms once (cells, activity objects, snapshots) …
		const started = Date.now();
		const zones = Array.from({ length: ROOMS }, (_, i) => `pit.${(i % 20) - 10}.${Math.floor(i / 20) - 8}`);
		for (let i = 0; i < zones.length; i += BOTS * 2) {
			await Promise.all(
				zones.slice(i, i + BOTS * 2).map(async (zone, k) => {
					const pit = await enter(zone, `Ager${k}`);
					await wander(pit, Date.now() + 1500, k);
					pit.client.close();
				})
			);
		}
		// … then keep a few rooms busy, snapshotting every AGE_PERSIST_MS.
		const busy = await Promise.all(Array.from({ length: CHURN }, (_, k) => enter(`pit.${20 + (k % 4)}.${20 + Math.floor(k / 4)}`, `Churn${k}`)));
		await Promise.all(busy.map((pit, k) => wander(pit, Date.now() + AGE_S * 1000, k)));
		for (const pit of busy) pit.client.close();
		const objects = FLEET > 0 ? '?' : storeObjects(server.celld.project);
		const cells = FLEET > 0 ? '?' : readdirSync(join(server.celld.project, '.celld/dev/runtime')).length;
		console.log(`aged in ${Math.round((Date.now() - started) / 1000)} s: ${ROOMS} rooms woken, ${cells} cells, ${objects} stored objects`);

		// The aged state with the worker's own settings: celld restarted on
		// it (dev), or the worker deployed again (fleet; rooms pick it up as
		// they wake).
		const persist = process.env.MOBPIT_SOAK_PERSIST_MS;
		if (FLEET > 0) {
			server.redeploy(persist === undefined ? {} : { ZONE_PERSIST_MS: persist });
		} else {
			setVar(server.celld.project, 'ZONE_PERSIST_MS', persist);
			await server.restartCelld();
		}
		await sleep(30_000);

		// Play: one bot per room; measure each room's ticks over the window.
		const playZones = Array.from({ length: BOTS }, (_, k) => `pit.${-7 + k * 3}.0`);
		const joined = await Promise.all(playZones.map((zone, k) => joinRetrying(zone, `Soak${k}`)));
		for (const [k, j] of joined.entries()) console.log(`${playZones[k]}: joined after ${j.ms} ms (${j.attempts} attempts)`);
		for (const [k, j] of joined.entries()) assert.ok(j.ms <= MAX_JOIN_MS, `${playZones[k]} took ${j.ms} ms to take a player`);
		const players = joined.map((j) => j.pit);
		await sleep(3000);
		const before = await Promise.all(playZones.map(info));
		const storedBefore = FLEET > 0 ? undefined : storeObjects(server.celld.project);
		const t0 = Date.now();
		await Promise.all(players.map((pit, k) => wander(pit, t0 + PLAY_S * 1000, k)));
		const after = await Promise.all(playZones.map(info));
		const seconds = (Date.now() - t0) / 1000;
		if (typeof storedBefore === 'number') {
			const grown = storeObjects(server.celld.project) - storedBefore;
			console.log(`store grew by ${grown} objects in ${seconds.toFixed(0)} s of ${BOTS} rooms played (${((grown / BOTS / seconds) * 3600).toFixed(0)} per room-hour)`);
		}
		const rows = playZones.map((zone, k) => {
			const dropped = after[k].droppedTicks - before[k].droppedTicks;
			const ticks = after[k].hz * seconds;
			return { zone, dropped, share: dropped / ticks, lateP99: after[k].lateMs.p99, same: after[k].activation === before[k].activation };
		});
		for (const r of rows) {
			console.log(`${r.zone}: dropped ${r.dropped} (${(r.share * 100).toFixed(2)} %) · late p99 ${r.lateP99.toFixed(1)} ms`);
		}
		for (const r of rows) {
			assert.ok(r.same, `${r.zone} restarted while measured`);
			assert.ok(r.share <= MAX_DROPPED, `${r.zone} dropped ${r.dropped} ticks in ${seconds.toFixed(0)} s`);
			assert.ok(r.lateP99 <= MAX_LATE_P99_MS, `${r.zone} late p99 ${r.lateP99} ms`);
		}
	} finally {
		for (const c of clients) c.close();
		await server.stop();
		// The aged state is hundreds of MB.
		if (server.celld !== undefined && process.env.MOBPIT_SOAK_KEEP !== '1') rmSync(server.celld.project, { recursive: true, force: true });
	}
});

/** Set (or with `undefined` remove) a worker var in the project's `.dev.vars`. */
function setVar(project, key, value) {
	const path = join(project, '.dev.vars');
	const lines = readFileSync(path, 'utf8').split('\n').filter((l) => l !== '' && !l.startsWith(`${key}=`));
	if (value !== undefined) lines.push(`${key}=${value}`);
	writeFileSync(path, `${lines.join('\n')}\n`);
}

/** Objects in `celld dev`'s local store (logged; `?` when unreadable). */
function storeObjects(project) {
	try {
		const { DatabaseSync } = process.getBuiltinModule('node:sqlite');
		const db = new DatabaseSync(join(project, '.celld/dev/objects.sqlite3'), { readOnly: true, timeout: 10_000 });
		const { n } = db.prepare('select count(*) as n from objects').get();
		db.close();
		return n;
	} catch {
		return '?';
	}
}
