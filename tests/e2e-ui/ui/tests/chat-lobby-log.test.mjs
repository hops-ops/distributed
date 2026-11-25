/**
 * Unit tests for lobby chat log geometry.
 * Exercises the real `$lib/chat/lobby-log` module used by `/chat`.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const uiRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const lobbyLogPath = path.join(uiRoot, 'src/lib/chat/lobby-log.ts');

const {
	nearBottom,
	nearTop,
	needsHistoryFill,
	pinScrollBottom,
	preserveScrollAfterPrepend,
	distanceFromBottom,
	distanceFromTop,
	scrollRange
} = await import(pathToFileURL(lobbyLogPath).href);

test('column-reverse positive model: scrollTop 0 is newest; max is oldest', () => {
	assert.equal(pinScrollBottom(), 0);
	const bottom = { scrollTop: 0, scrollHeight: 800, clientHeight: 400 };
	assert.equal(distanceFromBottom(bottom), 0);
	assert.ok(nearBottom(bottom));
	assert.equal(nearTop(bottom), false);

	const top = { scrollTop: 400, scrollHeight: 800, clientHeight: 400 };
	assert.equal(distanceFromTop(top), 0);
	assert.equal(nearTop(top), true);
	assert.equal(nearBottom(top), false);
});

test('column-reverse Chromium model: negative scrollTop toward oldest', () => {
	// Matches Chromium: wheel "up" → scrollTop -100, min = -(scrollHeight-clientHeight).
	const bottom = { scrollTop: 0, scrollHeight: 1836, clientHeight: 200 };
	assert.equal(scrollRange(bottom), 1636);
	assert.equal(distanceFromBottom(bottom), 0);
	assert.ok(nearBottom(bottom));
	assert.equal(nearTop(bottom), false, 'must not load history at newest edge');

	const mid = { scrollTop: -100, scrollHeight: 1836, clientHeight: 200 };
	assert.equal(distanceFromBottom(mid), 100);
	assert.equal(
		nearBottom(mid),
		false,
		'negative scroll must detach stick-to-bottom (this was the scroll-up bug)'
	);
	assert.equal(nearTop(mid), false);

	const top = { scrollTop: -1636, scrollHeight: 1836, clientHeight: 200 };
	assert.equal(distanceFromTop(top), 0);
	assert.equal(nearTop(top), true);
	assert.equal(nearBottom(top), false);
});

test('no overflow is not nearTop (fill path uses needsHistoryFill instead)', () => {
	const fit = { scrollTop: 0, scrollHeight: 200, clientHeight: 400 };
	assert.equal(nearTop(fit), false);
	assert.equal(needsHistoryFill(fit), true);
	assert.equal(
		needsHistoryFill({ scrollTop: 0, scrollHeight: 800, clientHeight: 400 }),
		false
	);
});

test('preserveScrollAfterPrepend: positive model adds delta; negative keeps bottom-relative', () => {
	assert.equal(preserveScrollAfterPrepend(120, 500, 700), 320);
	assert.equal(preserveScrollAfterPrepend(0, 500, 500), 0);
	// Chromium reverse: stay at same bottom-relative offset when older rows prepend.
	assert.equal(preserveScrollAfterPrepend(-200, 800, 1000), -200);
});
