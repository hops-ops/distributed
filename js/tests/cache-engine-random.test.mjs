import assert from 'node:assert/strict';
import test from 'node:test';
import * as engineModule from '../dist/internal/cache-engine.js';
import { RECORDS, randomWalk } from './fixtures/cache-engine-random.mjs';

const SEEDS = 40;
const STEPS = 150;

/** A failed batch or replacement leaves every read and watcher as it was. */
test('a failed write changes nothing', () => {
	let rolledBack = 0;
	for (let seed = 1; seed <= SEEDS; seed += 1) {
		let previous;
		randomWalk(engineModule, seed, STEPS, {
			onStep({ entry, before }) {
				const at = `seed ${seed} step ${entry.step} (${entry.kind})`;
				if (before !== undefined) {
					rolledBack += 1;
					assert.equal(typeof entry.outcome, 'string', `${at} must fail`);
					assert.deepEqual(entry.snapshot, before, `${at}: confirmed state changed`);
					if (previous !== undefined) {
						assert.deepEqual(entry.visible, previous.visible, `${at}: a read changed`);
						assert.deepEqual(entry.notified, previous.notified, `${at}: a watcher ran`);
					}
				}
				previous = entry;
			}
		});
	}
	assert.ok(rolledBack > 100, `only ${rolledBack} rollbacks were exercised`);
});

/**
 * Putting the confirmed state back as it already is rebuilds everything the
 * engine derives from it and keeps the layers and their floors. If what was
 * maintained step by step was right, nothing a reader sees changes.
 */
test('rebuilding from the confirmed state changes nothing a reader sees', () => {
	for (let seed = 1; seed <= SEEDS; seed += 1) {
		for (const steps of [STEPS / 3, STEPS]) {
			let last;
			randomWalk(engineModule, seed, steps, {
				onStep(state) {
					last = state;
				}
			});
			const { entry, engine, selectors, names } = last;
			engine.restoreConfirmed(entry.snapshot);
			assert.deepEqual(engine.extract(), entry.snapshot);
			selectors.forEach(([, selector], index) => {
				assert.deepEqual(
					engine.read(selector),
					entry.visible[index],
					`seed ${seed} after ${steps} steps: ${names[index]}`
				);
			});
		}
	}
});

test('a watcher is notified only when its value changed', () => {
	for (let seed = 1; seed <= SEEDS; seed += 1) {
		let previous;
		randomWalk(engineModule, seed, STEPS, {
			onStep({ entry, names }) {
				if (previous !== undefined) {
					entry.notified.forEach((count, index) => {
						const changed = !isDeepEqual(
							entry.watched[index],
							previous.watched[index]
						);
						assert.ok(
							count - previous.notified[index] <= 1,
							`seed ${seed} step ${entry.step}: ${names[index]} notified twice`
						);
						if (count === previous.notified[index]) {
							assert.equal(
								changed,
								false,
								`seed ${seed} step ${entry.step}: ${names[index]} changed silently`
							);
						}
					});
				}
				previous = entry;
			}
		});
	}
});

function isDeepEqual(left, right) {
	try {
		assert.deepEqual(left, right);
		return true;
	} catch {
		return false;
	}
}

/**
 * What a reader hands out is the engine's own frozen value: the same object
 * until that key changes, and never changed in place.
 */
test('values handed out are frozen and keep their identity until their key changes', () => {
	for (let seed = 1; seed <= 10; seed += 1) {
		let previous;
		randomWalk(engineModule, seed, STEPS, {
			onStep({ entry, engine, names }) {
				const at = `seed ${seed} step ${entry.step}`;
				const copies = structuredClone(entry.visible);
				// The last selector builds its own array; the rest return the engine's.
				entry.visible.slice(0, -1).forEach((value, index) => {
					if (value !== null && typeof value === 'object') {
						assert.ok(Object.isFrozen(value), `${at}: ${names[index]} is not frozen`);
					}
				});
				// The first selectors are `reader.record(key)`, one per record.
				RECORDS.forEach((key, index) => {
					const record = entry.visible[index];
					assert.equal(
						engine.read((reader) => reader.record(key)),
						record,
						`${at}: two reads of ${key} gave different objects`
					);
					if (record === undefined) return;
					assert.ok(Object.isFrozen(record.fields) && Object.isFrozen(record.links));
					assert.throws(() => {
						'use strict';
						record.fields.title = 'changed by a reader';
					}, TypeError);
				});
				if (previous !== undefined) {
					// A value handed out earlier still says what it said then.
					assert.deepEqual(previous.visible, previous.copies, `${at}: an old value changed`);
				}
				previous = { visible: entry.visible, copies };
			}
		});
	}
});
