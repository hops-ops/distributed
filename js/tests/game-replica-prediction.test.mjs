import assert from 'node:assert/strict';
import test from 'node:test';

import { EntityReplica, Predictor, TAU, angleDelta } from '@hops-ops/distributed/game';

import { ARENA_SCHEMA, avatar, delta } from './fixtures/game-harness.mjs';

const close = (actual, expected, eps = 1e-3, message) =>
	assert.ok(Math.abs(actual - expected) <= eps, `${message ?? ''} expected ${expected}, got ${actual}`);

test('replica interpolates lerp, shortest-path angle and step fields', () => {
	const replica = new EntityReplica();
	replica.reset(ARENA_SCHEMA, 0);
	replica.applyDelta(delta({ creates: [avatar(7, 0, 10, { yaw: 6.0, hp: 100, name: 'a' })] }), 1000);
	replica.applyDelta(
		delta({
			serverTick: 103,
			updates: [{ id: 7, mask: 0b11111, fields: [100, 20, 0.2, 50, 'b'] }]
		}),
		1100
	);

	const mid = replica.sampleEntity(7, 1050);
	close(mid.fields.x, 50, 1e-3, 'x lerp');
	close(mid.fields.y, 15, 1e-3, 'y lerp');
	// 6.0 → 0.2 crosses 2π: halfway is ≈ 6.2416 (wrapped), not 3.1.
	const expectedYaw = (6.0 + angleDelta(6.0, 0.2) / 2 + TAU) % TAU;
	close(mid.fields.yaw, expectedYaw, 1e-3, 'yaw shortest arc');
	assert.ok(mid.fields.yaw > 6, 'angle interpolation took the short way');
	assert.equal(mid.fields.hp, 100, 'u16 steps (holds older value)');
	assert.equal(mid.fields.name, 'a');

	// Clamp outside the buffer: no extrapolation.
	close(replica.sampleEntity(7, 5000).fields.x, 100);
	close(replica.sampleEntity(7, 0).fields.x, 0);
	assert.equal(replica.sampleEntity(7, 1100).fields.hp, 50);
	assert.equal(replica.get(7).fields.hp, 50, 'get() is the latest authoritative state');
});

test('replica inserts hold samples for quiet intervals and honours overrides', () => {
	const replica = new EntityReplica({ interpolation: { avatar: { hp: 'lerp', x: 'step' } } });
	replica.reset(ARENA_SCHEMA, 0);
	replica.applyDelta(delta({ creates: [avatar(1, 0, 0, { hp: 0 })] }), 0);
	replica.applyDelta(delta({ serverTick: 101 }), 50); // entity unchanged at t=50
	replica.applyDelta(delta({ serverTick: 102 }), 100); // still unchanged at t=100
	replica.applyDelta(
		delta({ serverTick: 103, updates: [{ id: 1, mask: 0b1001, fields: [10, 100] }] }),
		150
	);
	// It was still at y=0/hp=0 at t=100, so t=125 is halfway from the hold sample.
	assert.deepEqual(
		replica.history(1).map((h) => h.time),
		[0, 100, 150]
	);
	const s = replica.sampleEntity(1, 125);
	close(s.fields.hp, 50, 1e-9, 'hp lerp override');
	assert.equal(s.fields.x, 0, 'x step override');
	close(replica.sampleEntity(1, 50).fields.hp, 0, 1e-9);
});

test('welcome marks entities stale; re-create revives them with history', () => {
	const replica = new EntityReplica({ staleGraceMs: 200 });
	replica.reset(ARENA_SCHEMA, 0);
	replica.applyDelta(delta({ creates: [avatar(1, 0), avatar(2, 5)] }), 1000);
	replica.reset(ARENA_SCHEMA, 1050);
	assert.equal(replica.get(1), undefined);
	assert.equal(replica.sample(1050).length, 2, 'stale entities keep rendering');
	assert.ok(replica.sample(1050).every((e) => e.stale));

	replica.applyDelta(delta({ creates: [avatar(1, 10)] }), 1100);
	assert.equal(replica.get(1).fields.x, 10);
	close(replica.sampleEntity(1, 1050).fields.x, 5, 1e-3, 'history kept across the welcome');
	assert.equal(replica.sampleEntity(2, 1100).stale, true);
	replica.applyDelta(delta({ serverTick: 110 }), 1300);
	assert.equal(replica.sampleEntity(2, 1300), undefined, 'stale entity dropped after grace');
	assert.equal(replica.size, 1);
});

/** Scripted 1D mover: input is a velocity (units/s). */
const moverPredict = (state, input, dtMs) => ({ x: state.x + (input * dtMs) / 1000 });

function boundPredictor(policy) {
	const predictor = new Predictor({
		predict: moverPredict,
		corrections: { avatar: { x: policy, hp: { mode: 'replace' } } }
	});
	const kind = ARENA_SCHEMA.byId.get(1);
	predictor.bind(7, kind, { x: 0, y: 0, yaw: 0, hp: 100, name: 'p' });
	return predictor;
}

test('prediction replays unacked inputs over authoritative state', () => {
	const predictor = boundPredictor({ mode: 'snap' });
	for (let seq = 1; seq <= 5; seq += 1) predictor.record(seq, 10, 100); // +1 per input
	close(predictor.predicted().x, 5, 1e-9);

	// Server consumed 1..3 and agrees exactly: no correction.
	const agree = predictor.reconcile(3, { x: 3, y: 0, yaw: 0, hp: 100, name: 'p' }, 0);
	assert.equal(agree.replayed, 2);
	assert.deepEqual(agree.errors, {});
	close(predictor.predicted().x, 5, 1e-9);
	assert.deepEqual(predictor.pending.map((p) => p.seq), [4, 5]);
});

test('inputs recorded before the entity is known are replayed when it binds', () => {
	const predictor = new Predictor({ predict: moverPredict });
	predictor.bind(7);
	predictor.record(1, 10, 100);
	predictor.record(2, 10, 100);
	assert.equal(predictor.active, false);
	predictor.bind(7, ARENA_SCHEMA.byId.get(1), { x: 0, y: 0, yaw: 0, hp: 100, name: 'p' });
	close(predictor.predicted().x, 2, 1e-9, 'both replayed');
	// Another entity: the inputs were not its.
	predictor.bind(8, ARENA_SCHEMA.byId.get(1), { x: 0, y: 0, yaw: 0, hp: 100, name: 'p' });
	close(predictor.predicted().x, 0, 1e-9);
	assert.deepEqual(predictor.pending, []);
});

test('ease blends the divergence out over time while snap jumps', () => {
	const authoritative = { x: 1, y: 0, yaw: 0, hp: 80, name: 'p' }; // server hit a wall
	const eased = boundPredictor({ mode: 'ease', durationMs: 200 });
	const snapped = boundPredictor({ mode: 'snap' });
	for (const p of [eased, snapped]) {
		for (let seq = 1; seq <= 4; seq += 1) p.record(seq, 10, 100);
	}
	// Ack 3: rebased = 1 (authoritative) + 1 (replayed seq 4) = 2; we showed 4.
	const ce = eased.reconcile(3, authoritative, 1000);
	const cs = snapped.reconcile(3, authoritative, 1000);
	close(ce.errors.x, 2, 1e-9);
	assert.equal(ce.resolved.x, 'ease');
	assert.equal(cs.resolved.x, 'snap');

	close(snapped.current(1000).x, 2, 1e-9, 'snap shows the rebased value at once');
	close(eased.current(1000).x, 4, 1e-9, 'ease starts from what was shown');
	close(eased.current(1100).x, 3, 1e-9, 'half the error left halfway through');
	close(eased.current(1200).x, 2, 1e-9, 'converged after durationMs');
	close(eased.current(5000).x, 2, 1e-9);
	assert.deepEqual(eased.visualError(5000), {});

	// `replace` fields are server-owned: never replayed, shown as-is.
	assert.equal(eased.current(1000).hp, 80);
	assert.equal(ce.resolved.hp, 'replace');
});

test('ease with snapAbove: a small error eases, a teleport cuts', () => {
	const near = boundPredictor({ mode: 'ease', durationMs: 200, snapAbove: 3 });
	const far = boundPredictor({ mode: 'ease', durationMs: 200, snapAbove: 3 });
	for (const p of [near, far]) {
		for (let seq = 1; seq <= 4; seq += 1) p.record(seq, 10, 100);
	}
	// Shown 4. A wall (rebased 2: error 2) eases; a respawn at -10 (rebased
	// -9: error 13) cuts at once.
	const cn = near.reconcile(3, { x: 1, y: 0, yaw: 0, hp: 100, name: 'p' }, 1000);
	const cf = far.reconcile(3, { x: -10, y: 0, yaw: 0, hp: 100, name: 'p' }, 1000);
	assert.equal(cn.resolved.x, 'ease');
	close(near.current(1000).x, 4, 1e-9, 'small: eased from what was shown');
	assert.equal(cf.resolved.x, 'snap');
	close(cf.errors.x, 13, 1e-9);
	close(far.current(1000).x, -9, 1e-9, 'teleport: shown at once, no slide');
	assert.equal(far.current(1000).hp, 100);
	assert.deepEqual(far.visualError(1000), {});
});

test('a new correction while easing starts from the currently displayed value', () => {
	const p = boundPredictor({ mode: 'ease', durationMs: 100 });
	p.record(1, 10, 1000); // predicted x = 10
	p.reconcile(0, { x: 0, y: 0, yaw: 0, hp: 100, name: 'p' }, 0); // still pending: 10
	p.reconcile(1, { x: 4, y: 0, yaw: 0, hp: 100, name: 'p' }, 0); // server says 4
	close(p.current(50).x, 7, 1e-9);
	p.reconcile(1, { x: 0, y: 0, yaw: 0, hp: 100, name: 'p' }, 50); // server corrects again
	close(p.current(50).x, 7, 1e-9, 'no visual pop');
	close(p.current(150).x, 0, 1e-9);
});

test('a step recorded with its time is drawn as motion over its interval', () => {
	const p = boundPredictor({ mode: 'ease', durationMs: 100 });
	p.record(1, 10, 100, 0); // predicted x = 1, drawn from 0 over 100 ms
	close(p.predicted().x, 1, 1e-9);
	close(p.current(0).x, 0, 1e-9);
	close(p.current(50).x, 0.5, 1e-9);
	close(p.current(100).x, 1, 1e-9);
	close(p.current(500).x, 1, 1e-9, 'holds the prediction past the interval');
	p.record(2, 10, 100, 100);
	close(p.current(100).x, 1, 1e-9, 'no pop at the next step');
	close(p.current(150).x, 1.5, 1e-9);
	// Without a time the prediction is drawn as is (no blend).
	const plain = boundPredictor({ mode: 'ease', durationMs: 100 });
	plain.record(1, 10, 100);
	close(plain.current(0).x, 1, 1e-9);
});

test('a Delta mid-step keeps the motion going', () => {
	const p = boundPredictor({ mode: 'ease', durationMs: 100 });
	p.record(1, 10, 100, 0);
	p.record(2, 10, 100, 100); // predicted 2, drawn from 1 over 100..200
	const agree = p.reconcile(1, { x: 1, y: 0, yaw: 0, hp: 100, name: 'p' }, 150);
	assert.deepEqual(agree.errors, {});
	close(p.current(150).x, 1.5, 1e-9, 'no stall or pop');
	close(p.current(175).x, 1.75, 1e-9, 'still moving at the step rate');
	close(p.current(200).x, 2, 1e-9);
});

test('a correction mid-step eases only the error, not the motion', () => {
	const p = boundPredictor({ mode: 'ease', durationMs: 100 });
	p.record(1, 10, 100, 0);
	p.record(2, 10, 100, 100);
	// Server: x 0.8 after input 1, so the prediction becomes 1.8.
	const c = p.reconcile(1, { x: 0.8, y: 0, yaw: 0, hp: 100, name: 'p' }, 150);
	close(c.errors.x, 0.2, 1e-9);
	close(p.current(150).x, 1.5, 1e-9, 'no visual pop');
	close(p.current(200).x, 1.9, 1e-9, 'motion done, half the error left');
	close(p.current(250).x, 1.8, 1e-9);
});
