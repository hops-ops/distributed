/**
 * Headless Mob Pit (Node): the rooms on their own `celld dev` (each room a
 * Durable Object), the services binary (`game-demos mobpit`), a scripted
 * player:
 *
 * join the entrance room → profile read from the celld Crawler cell → kill
 * a mob → the fact settles ("saved") with server-decided XP → walk through
 * a doorway into the next room, continuously (one world: the own world
 * position never jumps across the handoff) → the left room goes dormant, then
 * hibernated (zone status read model) → walk back → the room wakes from its
 * snapshot with the mobs killed there still dead → restart the services:
 * the profile is still there.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { isMob, isPit, pitFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, until } from './helpers/demo-server.mjs';
import { hunt, largestStep, startPit, walkThroughDoor } from './helpers/mobpit.mjs';

const XP = new Set([10, 25, 60]);
/** One 30 Hz command frame of walking (5 m/s ≈ 0.167 m) plus float slack. */
const STEP = 0.2;

/** The own world position across a doorway: from room `a` into room `b`, never more than a step at once. */
function assertContinuous(trace, a, b) {
	const last = trace.findLast((s) => s.room === a);
	const first = trace.find((s) => s.room === b && s.predicted);
	assert.ok(last && first, `trace covers ${a} → ${b}`);
	// Room-local positions would be a whole room (40 m) apart.
	const across = Math.hypot(first.x - last.x, first.y - last.y);
	console.error(`doorway ${a} → ${b}: ${across.toFixed(3)} m across, largest step ${largestStep(trace).toFixed(3)} m, ${trace.length} samples`);
	assert.ok(across <= STEP, `${a} → ${b}: ${JSON.stringify(last)} → ${JSON.stringify(first)} (${across.toFixed(3)} m)`);
	assert.ok(largestStep(trace) <= STEP, `largest step ${largestStep(trace).toFixed(3)} m`);
}

test('rooms on celld: kills settle, doorways hand over, left rooms sleep and restore', { timeout: 600_000 }, async () => {
	const storage = memoryStorage();
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Hunter', storage });
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		const welcome = await pit.client.connect('pit.0.0');
		assert.deepEqual(welcome.schema.kinds.map((k) => k.name), ['player', 'mob', 'pillar', 'pit']);
		const hello = await pit.ready;
		assert.deepEqual([hello.roomX, hello.roomY, hello.restored], [0, 0, false]);
		await until(() => pit.profile() !== undefined, 10_000, 'profile from the cell');
		assert.equal(pit.profile().source, 'cell');
		assert.equal(pit.profile().kills, 0);
		// The room's own seeded contents, computed by the same Rust as WASM.
		const layout = pit.pures.pitRoom({ seed: hello.seed, x: 0, y: 0 });
		const mobsHere = () => pit.client.render().filter((e) => isMob(e) && e.zoneId === 'pit.0.0' && !e.stale).length;
		await until(() => mobsHere() === layout.mobs.length, 5_000, 'mobs of the generated layout');

		// A kill settles in the Crawler cell.
		const saved = [];
		const kills = [];
		pit.on('saved', (s) => saved.push(s));
		pit.on('kill', (k) => k.killer === pit.hello().entity && kills.push(k));
		await hunt(pit, () => saved.length >= 1);
		assert.ok(saved[0].ok && XP.has(saved[0].xp), JSON.stringify(saved[0]));
		assert.match(saved[0].key, /^crawler\.record_kill:pit\.0\.0:\d+:m\d+:\d+$/, '{command}:{room}:{boot_epoch}:m{mob}:{generation}');
		await until(() => pit.unsaved.size === 0 && saved.length === kills.length, 10_000, 'settlements');
		const pitStats = () => {
			const entity = pit.client.render().find((e) => isPit(e) && e.zoneId === pit.room());
			return entity && pitFields(entity);
		};
		await until(() => (pitStats()?.kills ?? 0) >= kills.length, 5_000, 'room kill count');
		const roomKills = pitStats().kills;

		// Through the east doorway: the room hands the player to pit.1.0,
		// at the same world point (no jump, no slide back across the room).
		const east = await walkThroughDoor(pit, [1, 0]);
		assert.equal(east.room, 'pit.1.0');
		assertContinuous(east.trace, 'pit.0.0', 'pit.1.0');
		await until(() => pit.hello()?.roomX === 1, 10_000, 'hello of the next room');
		// The left room empties: dormant, then hibernated.
		const status = async (zone) => (await pitApi.rooms()).rooms.find((r) => r.zone === zone)?.status;
		await eventually(async () => (await status('pit.0.0')) === 'hibernated', 30_000, 'pit.0.0 hibernated');
		assert.equal((await pitApi.roomInfo('pit.0.0')).awake, false);
		assert.equal(await status('pit.1.0'), 'active');

		// Back west: the room wakes from its snapshot, the dead stay dead.
		const west = await walkThroughDoor(pit, [-1, 0]);
		assert.equal(west.room, 'pit.0.0');
		assertContinuous(west.trace, 'pit.1.0', 'pit.0.0');
		await until(() => pit.hello()?.roomX === 0, 10_000, 'hello of the entrance again');
		assert.equal(pit.hello().restored, true, 'restored from hibernation');
		assert.equal(pit.hello().roomKills, roomKills);
		const restored = await eventually(async () => {
			const row = (await pitApi.rooms()).rooms.find((r) => r.zone === 'pit.0.0');
			return row?.status === 'active' && row.restored && row;
		}, 15_000, 'pit.0.0 active again, restored');
		assert.ok(restored.activations >= 2);

		// The services restart; the profile is read back from the cell.
		const before = pit.profile();
		pit.client.close();
		await server.restart();
		const again = createMobPitClient({ api: new DevApi({ base: server.httpUrl, storage }), roomsBase: server.roomsUrl });
		clients.push(again.client);
		await again.client.connect('pit.0.0');
		await until(() => again.profile() !== undefined, 10_000, 'profile after restart');
		assert.equal(again.profile().kills, before.kills);
		assert.equal(again.profile().xp, before.xp);
		const crawlers = await eventually(
			async () => {
				const { crawlers } = await new MobPitApi(server.httpUrl).crawlers();
				return crawlers.length === 1 && crawlers[0].kills === before.kills && crawlers;
			},
			15_000,
			'crawler directory'
		);
		assert.equal(crawlers[0].kills, before.kills);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
