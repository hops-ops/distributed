/**
 * Headless Mob Pit live content update (Node): the rooms on their own
 * `celld dev`, the services binary, scripted players.
 *
 * An admin rolls a new content pack out to every live room through the
 * world's rollout cell: the rooms freeze (change notes, inputs held),
 * switch to the pack and resume. A normal client patches live; an old one
 * (pinned) keeps its pack, cannot leave through any doorway but the
 * entrance's and is reloaded there. A fresh join on old content is told to
 * reload; a room that never ran wakes on the live version before admitting.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { playerFields } from '../src/generated/mobpit/index.ts';
import { createMobPitClient, MobPitApi, parseRoom } from '../src/mobpit/client.ts';
import { DevApi } from '../src/shared/dev-api.ts';
import { eventually, memoryStorage, sleep, until } from './helpers/demo-server.mjs';
import { ADMIN_SECRET, startPit, walkThroughDoor } from './helpers/mobpit.mjs';

/** Walk towards side `[dx, dy]` of the current room for `ms` (no handoff expected). */
async function pushAgainst(pit, [dx, dy], ms) {
	const deadline = Date.now() + ms;
	while (Date.now() < deadline) {
		const at = pit.position();
		const [rx, ry] = parseRoom(pit.room());
		let input = { mx: 0, my: 0, buttons: 0 };
		if (at !== undefined) {
			const tx = rx * 40 + (dx !== 0 ? dx * 24 : 1.7);
			const ty = ry * 40 + (dy !== 0 ? dy * 24 : 1.7);
			const len = Math.max(Math.hypot(tx - at.x, ty - at.y), 1e-3);
			input = { mx: Math.round(((tx - at.x) / len) * 127), my: Math.round(((ty - at.y) / len) * 127), buttons: 0 };
		}
		pit.client.sendInput(input);
		await sleep(1000 / 30);
	}
}

test('Mob Pit live content: freeze → apply → resume, old clients held until the entrance', { timeout: 600_000 }, async () => {
	const server = await startPit('mobpit');
	const clients = [];
	try {
		const pitApi = new MobPitApi(server.httpUrl, server.roomsUrl);
		const liveVersion = async () => (await pitApi.content()).state?.version ?? '';
		const join = async (name, zone, options = {}) => {
			const api = new DevApi({ base: server.httpUrl, name: () => name, storage: memoryStorage() });
			const pit = createMobPitClient({ api, roomsBase: server.roomsUrl, neighbours: false, liveVersion, ...options });
			clients.push(pit.client);
			await pit.client.connect(zone);
			await pit.ready;
			await until(() => pit.client.predictor?.active, 5_000, `${name} bound`);
			return pit;
		};
		const before = await pitApi.content();
		assert.equal(before.state, null, 'no rollout yet: the built-in pack');
		assert.deepEqual(
			before.packs.map((p) => p.version),
			['', 'frostfall', 'emberfall']
		);

		const ana = await join('Ana', 'pit.0.0');
		const bo = await join('Bo', 'pit.0.0', { pinContent: true });
		// Bo (an old build) walks into the next room before the update.
		const east = await walkThroughDoor(bo, [1, 0]);
		assert.equal(east.room, 'pit.1.0');

		// Only the admin rolls out, and only known packs.
		await assert.rejects(pitApi.rollout('wrong', { version: 'frostfall' }), /HTTP 401/);
		await assert.rejects(pitApi.rollout(ADMIN_SECRET, { version: 'nope' }), /HTTP 400/);
		const notes = 'Frostfall is here: slower, tougher mobs.';
		await pitApi.rollout(ADMIN_SECRET, { version: 'frostfall', notes, freezeMs: 2_500 });
		await assert.rejects(pitApi.rollout(ADMIN_SECRET, { version: 'emberfall' }), /in progress/, 'one rollout at a time');

		// Every live room freezes with the notes; inputs are held.
		await until(() => ana.content().frozen !== undefined && bo.content().frozen !== undefined, 10_000, 'both rooms frozen');
		assert.equal(ana.content().frozen.notes, notes);
		assert.equal(ana.content().frozen.version, 'frostfall');
		assert.equal(ana.client.sendInput({ mx: 127, my: 0, buttons: 0 }), undefined, 'no inputs while frozen');
		for (const room of ['pit.0.0', 'pit.1.0']) {
			const info = await pitApi.roomInfo(room);
			assert.equal(info.content.freeze, 'frozen', `${room} frozen`);
		}

		// …then resume on the new pack.
		await until(() => ana.content().frozen === undefined && bo.content().frozen === undefined, 15_000, 'both rooms resumed');
		assert.equal(ana.content().lastResume.version, 'frostfall');
		assert.equal(ana.content().version, 'frostfall', 'a normal client patches live');
		assert.equal(ana.content().pack.name, 'Frostfall');
		assert.equal(ana.content().stale, false);
		assert.equal(bo.content().version, '', 'the old client keeps its pack');
		assert.equal(bo.content().stale, true);
		for (const room of ['pit.0.0', 'pit.1.0']) {
			assert.equal((await pitApi.roomInfo(room)).content.version, 'frostfall', `${room} runs frostfall`);
		}
		const after = await eventually(async () => {
			const c = await pitApi.content();
			return c.state?.phase === 'running' && c;
		}, 10_000, 'rollout settled');
		assert.equal(after.state.version, 'frostfall');

		// The old client may not leave through another doorway…
		await pushAgainst(bo, [1, 0], 13_000);
		assert.equal(bo.room(), 'pit.1.0', 'held in its room');
		assert.equal(bo.content().staleNotice?.gate, 'pit.0.0', 'told to head to the entrance');
		// …but the entrance lets it through and reloads it there.
		const back = await walkThroughDoor(bo, [-1, 0], 45_000);
		assert.equal(back.room, 'pit.0.0');
		await until(() => bo.client.contentVersion === 'frostfall', 10_000, 'reloaded at the entrance');
		assert.deepEqual(
			bo.content().reloads.map((r) => r.version),
			['frostfall']
		);
		assert.equal(bo.content().stale, false);
		await until(() => bo.client.predictor?.active && bo.room() === 'pit.0.0', 10_000, 'back in control after the reload');
		const me = playerFields(bo.me());
		assert.ok(Math.abs(me.x) < 20 && Math.abs(me.y) < 20, 'the same body, in the entrance');

		// A fresh join on old content is told to reload, then gets in.
		const cy = await join('Cy', 'pit.0.0', { contentVersion: '' });
		assert.equal(cy.client.contentVersion, 'frostfall');
		assert.deepEqual(
			cy.content().reloads.map((r) => r.version),
			['frostfall']
		);

		// A room that never ran wakes on the live version before admitting.
		const north = await walkThroughDoor(ana, [0, 1]);
		assert.equal(north.room, 'pit.0.1');
		assert.equal(ana.content().worldVersion, 'frostfall', 'the woken room welcomed on frostfall');
		assert.equal((await pitApi.roomInfo('pit.0.1')).content.version, 'frostfall');
		console.error(`rollout frostfall: rooms froze with notes, resumed; old client reloaded at the entrance after ${bo.content().reloads.length} reload`);
	} finally {
		for (const c of clients) c.close();
		await server.stop();
	}
});
