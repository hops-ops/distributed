/**
 * Per-delta cost of the replica cache as the cache grows.
 *
 *   npm run build && node scripts/bench-cache-scaling.mjs [sizes...]
 *       [--scenario=update|insert|pending] [--runs=N] [--json]
 *
 * For each cache size: fill the replica, watch 50 lists live, apply 100 live
 * frames that each change one record, and report wall time per frame with the
 * engine's work counts. `update` retitles a record, `insert` appends a new
 * one, and `pending` retitles while an optimistic layer is waiting. Wall time is machine- and
 * load-dependent; the counts are not, and `tests/cache-scaling.test.mjs`
 * asserts on them.
 */
import { performance } from 'node:perf_hooks';
import { cacheEngineWork } from '../dist/internal/cache-engine.js';
import { Game, scalingReplica } from '../tests/fixtures/cache-scaling.mjs';

const args = process.argv.slice(2);
const json = args.includes('--json');
const sizes = args.filter((arg) => /^\d+$/.test(arg)).map(Number);
if (sizes.length === 0) sizes.push(1_000, 10_000, 30_000);
const DELTAS = 100;
const WATCHERS = 50;
const SCENARIO = args.find((arg) => arg.startsWith('--scenario='))?.slice(11) ?? 'update';
const RUNS = Number(args.find((arg) => arg.startsWith('--runs='))?.slice(7) ?? 5);

const quantile = (sorted, q) =>
	sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
const median = (values) => quantile([...values].sort((a, b) => a - b), 0.5);

async function run(records) {
	const fillStarted = performance.now();
	const harness = await scalingReplica({ records, watchers: WATCHERS });
	const fillMs = performance.now() - fillStarted;
	// Warm the JIT on the same path before measuring.
	const kind = SCENARIO === 'insert' ? 'insert' : 'update';
	for (let n = 0; n < 10; n += 1) harness.prepare(n, kind).apply();
	if (SCENARIO === 'pending') {
		harness.replica.createOptimisticLayer('pending-command', (writer) => {
			writer.writeRecord(Game, 'game-49-0', { fields: { title: 'optimistic' } });
		});
	}
	harness.notifications.fill(0);
	const before = { ...cacheEngineWork };
	const times = [];
	for (let n = 10; n < 10 + DELTAS; n += 1) {
		const delta = harness.prepare(n, kind);
		const started = performance.now();
		delta.apply();
		times.push(performance.now() - started);
	}
	const work = Object.fromEntries(
		Object.entries(cacheEngineWork).map(([name, value]) => [
			name,
			(value - before[name]) / DELTAS
		])
	);
	times.sort((a, b) => a - b);
	return {
		records: harness.records,
		fillMs,
		totalMs: times.reduce((sum, value) => sum + value, 0),
		medianMs: quantile(times, 0.5),
		p95Ms: quantile(times, 0.95),
		notifications: harness.notifications.reduce((sum, value) => sum + value, 0) / DELTAS,
		work
	};
}

const rows = [];
for (const size of sizes) {
	const runs = [];
	for (let attempt = 0; attempt < RUNS; attempt += 1) runs.push(await run(size));
	rows.push({
		records: runs[0].records,
		fillMs: median(runs.map((result) => result.fillMs)),
		totalMs: median(runs.map((result) => result.totalMs)),
		medianMs: median(runs.map((result) => result.medianMs)),
		p95Ms: median(runs.map((result) => result.p95Ms)),
		notifications: runs[0].notifications,
		work: runs[0].work
	});
}

if (json) {
	console.log(JSON.stringify(rows, null, 2));
} else {
	console.log(
		`${SCENARIO}: ${DELTAS} single-record live frames, ${WATCHERS} watchers, median of ${RUNS} runs (ms)`
	);
	console.table(
		rows.map((row) => ({
			records: row.records,
			'fill ms': row.fillMs.toFixed(0),
			'total ms': row.totalMs.toFixed(1),
			'median/delta': row.medianMs.toFixed(3),
			'p95/delta': row.p95Ms.toFixed(3),
			'notified/delta': row.notifications,
			...Object.fromEntries(
				Object.entries(row.work).map(([name, value]) => [`${name}/delta`, value])
			)
		}))
	);
	if (rows.length > 1) {
		const first = rows[0];
		const last = rows.at(-1);
		const exponent =
			Math.log(last.medianMs / first.medianMs) / Math.log(last.records / first.records);
		console.log(
			`scaling exponent (median/delta vs records, ${first.records} -> ${last.records}): ${exponent.toFixed(2)}`
		);
	}
}
