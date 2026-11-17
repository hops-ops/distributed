/**
 * Headless Mob Pit: an open room side (no wall) is crossed anywhere along
 * it, not just at its doorway. A scripted player joins a room whose east
 * side is open, walks out through it 9 m or more off the doorway and is
 * handed over to the next room with no jump in its world position.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { PIT_WORLD } from '../src/generated/mobpit/index.ts';
import { createMobPitClient } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { memoryStorage, until } from './helpers/demo-server.mjs';
import { largestStep, startPit, walkThroughDoor, walkTo } from './helpers/mobpit.mjs';

/** One 30 Hz command frame of walking (5 m/s ≈ 0.167 m) plus float slack. */
const STEP = 0.2;

/** A lane `y` from the room's middle out through the east side that clears every ramp, platform and obstacle. */
function clearLane(layout) {
	const r = PIT_WORLD.playerRadius + 0.4;
	const boxes = [...layout.ramps, ...layout.platforms];
	return [12, -12, 15, -15, 9, -9, 17, -17].find(
		(y) =>
			boxes.every((b) => y < b.y0 - r || y > b.y1 + r || b.x1 < 0) &&
			layout.obstacles.every((o) => Math.abs(o.y - y) > o.r + r || o.x < 0)
	);
}

test('an open side hands a player over anywhere along it', { timeout: 300_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const api = new DevApi({ base: server.httpUrl, name: () => 'Rover', storage: memoryStorage() });
		const pit = createMobPitClient({ api, roomsBase: server.roomsUrl });
		clients.push(pit.client);
		await pit.pures.ready;
		const seed = PIT_WORLD.defaultSeed;
		// A room near the entrance, open to the east, with no holes and a clear lane.
		let room;
		let lane;
		for (let d = 2; d < 8 && room === undefined; d += 1) {
			for (const [x, y] of [[d, 0], [0, d], [-d, 0], [0, -d], [d, 1], [1, d], [-d, 1], [d, -1]]) {
				const layout = pit.pures.pitRoom({ seed, x, y });
				const y0 = (layout.open & 1) !== 0 && layout.holes.length === 0 ? clearLane(layout) : undefined;
				if (y0 !== undefined) {
					room = [x, y];
					lane = y0;
					break;
				}
			}
		}
		assert.ok(room !== undefined, 'a room open to the east near the entrance');
		const zone = `pit.${room[0]}.${room[1]}`;
		console.error(`open east side: ${zone}, lane y = ${lane}`);
		await pit.client.connect(zone);
		await pit.ready;
		await until(() => pit.me() !== undefined && pit.room() === zone, 10_000, 'joined');
		const layout = pit.pures.pitRoom({ seed, x: room[0], y: room[1] });
		// Up the clear cross to the lane, then east along it through the open side.
		await walkTo(pit, layout.obstacles, [1.7, lane], undefined, 30_000);
		const { room: next, trace } = await walkThroughDoor(pit, [1, 0], 30_000, lane);
		assert.equal(next, `pit.${room[0] + 1}.${room[1]}`);
		const last = trace.findLast((s) => s.room === zone);
		const first = trace.find((s) => s.room === next && s.predicted);
		assert.ok(last && first, 'trace covers the crossing');
		const ry = last.y - room[1] * 40;
		assert.ok(Math.abs(ry) > PIT_WORLD.doorHalf + 5, `crossed ${ry.toFixed(2)} m off the doorway`);
		const across = Math.hypot(first.x - last.x, first.y - last.y);
		console.error(`open side ${zone} → ${next}: ${across.toFixed(3)} m across at y ${ry.toFixed(2)}, largest step ${largestStep(trace).toFixed(3)} m`);
		assert.ok(across <= STEP, `${across.toFixed(3)} m across`);
		assert.ok(largestStep(trace) <= STEP, `largest step ${largestStep(trace).toFixed(3)} m`);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
