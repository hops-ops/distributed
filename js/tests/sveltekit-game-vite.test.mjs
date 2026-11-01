/**
 * The Vite plugin's `games` option: `game-manifest` → `game-client` →
 * `game-wasm` on start, on Rust changes and on build, a `--check` mode for
 * CI, and games-only apps outside the application lifecycle. A fake
 * `distributed` executable records argv and writes deterministic outputs.
 */
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { distributedSvelteKit } from '../dist/sveltekit/vite.js';
import {
	checkDistributedGames,
	distributedSvelteKit as distributedSvelteKitPublic,
	generateDistributedGames
} from '../dist/sveltekit/public-vite.js';

const FAKE = String.raw`
import { appendFileSync, mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
const args = process.argv.slice(2);
appendFileSync(process.env.FAKE_LOG, JSON.stringify(args) + '\n');
const opt = (name) => args[args.indexOf(name) + 1];
const [command] = args;
if (command === 'game-manifest') {
	const surface = readFileSync(join(process.env.FAKE_RUST, 'src', 'lib.rs'), 'utf8');
	process.stdout.write(JSON.stringify({ name: 'pit', schemaVersion: 3, ...(surface.includes('predict') ? { predict: { export: 'pit_step' } } : {}), source: surface }));
} else if (command === 'game-client') {
	const surface = JSON.parse(readFileSync(opt('--manifest'), 'utf8'));
	const out = opt('--out');
	const text = '// generated (' + opt('--import-extension') + ')\nexport const SOURCE = ' + JSON.stringify(surface.source) + ';\n';
	if (args.includes('--check')) {
		const current = existsSync(join(out, 'index.ts')) ? readFileSync(join(out, 'index.ts'), 'utf8') : '';
		if (current !== text) { process.stderr.write('drift in ' + out); process.exit(1); }
	} else {
		mkdirSync(out, { recursive: true });
		writeFileSync(join(out, 'index.ts'), text);
	}
} else if (command === 'game-wasm') {
	const out = opt('--out');
	mkdirSync(join(out, 'wasm'), { recursive: true });
	writeFileSync(join(out, 'wasm', 'pit.wasm'), readFileSync(join(opt('--manifest-path'), '..', 'src', 'lib.rs')));
} else {
	process.exit(2);
}
`;

async function fixture() {
	const root = await mkdtemp(join(tmpdir(), 'distributed-game-vite-'));
	const app = join(root, 'app');
	const rust = join(root, 'game');
	await mkdir(join(rust, 'src'), { recursive: true });
	await mkdir(join(app, 'src'), { recursive: true });
	await writeFile(join(rust, 'Cargo.toml'), '[package]\nname = "game"\n');
	await writeFile(join(rust, 'src', 'lib.rs'), '// zone v1 with predict\n');
	const fake = join(root, 'fake-distributed.mjs');
	await writeFile(fake, FAKE);
	const log = join(root, 'argv.log');
	await writeFile(log, '');
	process.env.FAKE_LOG = log;
	process.env.FAKE_RUST = rust;
	const options = (game = {}) => ({
		cwd: app,
		command: process.execPath,
		commandArgs: [fake],
		games: [
			{
				manifest: {
					args: ['game-manifest', '--manifest-path', join(rust, 'Cargo.toml'), '--entrypoint', 'game::pit::game_surface']
				},
				out: 'src/lib/generated/pit',
				...game
			}
		]
	});
	const calls = async () =>
		(await readFile(log, 'utf8')).trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
	return { root, app, rust, options, calls, out: join(app, 'src/lib/generated/pit') };
}

test('generateDistributedGames runs game-manifest → game-client → game-wasm, then is a no-op', async () => {
	const f = await fixture();
	try {
		assert.equal(await generateDistributedGames(f.options()), true);
		const argv = await f.calls();
		assert.deepEqual(argv.map((a) => a[0]), ['game-manifest', 'game-client', 'game-wasm']);
		assert.deepEqual(argv[1].slice(3), ['--out', f.out, '--import-extension', 'ts']);
		assert.equal(argv[2][argv[2].indexOf('--manifest-path') + 1], join(f.rust, 'Cargo.toml'));
		assert.match(await readFile(join(f.out, 'index.ts'), 'utf8'), /zone v1/);
		assert.ok(existsSync(join(f.out, 'wasm', 'pit.wasm')));
		assert.equal(await generateDistributedGames(f.options()), false, 'unchanged outputs');

		// CI check: current → ok; drift → fails without writing; never builds WASM.
		await checkDistributedGames(f.options());
		await writeFile(join(f.rust, 'src', 'lib.rs'), '// zone v2 with predict\n');
		await assert.rejects(checkDistributedGames(f.options()), /drift/);
		assert.match(await readFile(join(f.out, 'index.ts'), 'utf8'), /zone v1/);
		assert.ok(!(await f.calls()).slice(6).some((a) => a[0] === 'game-wasm'));
	} finally {
		await rm(f.root, { recursive: true, force: true });
	}
});

test('wasm: auto skips surfaces without a step, false never compiles', async () => {
	const f = await fixture();
	try {
		await writeFile(join(f.rust, 'src', 'lib.rs'), '// zone without a step\n');
		await generateDistributedGames(f.options());
		assert.deepEqual((await f.calls()).map((a) => a[0]), ['game-manifest', 'game-client']);
		await writeFile(join(f.rust, 'src', 'lib.rs'), '// zone with predict\n');
		await generateDistributedGames(f.options({ wasm: false, importExtension: 'js' }));
		const argv = await f.calls();
		assert.deepEqual(argv.slice(2).map((a) => a[0]), ['game-manifest', 'game-client']);
		assert.equal(argv[3].at(-1), 'js');
	} finally {
		await rm(f.root, { recursive: true, force: true });
	}
});

test('invalid game configuration fails before running anything', async () => {
	const f = await fixture();
	try {
		await assert.rejects(
			generateDistributedGames({ ...f.options(), games: [{ manifest: { args: ['client-manifest'] }, out: 'x' }] }),
			/must begin with game-manifest/
		);
		await assert.rejects(
			generateDistributedGames({ ...f.options(), games: [{ manifest: 'surface.json', out: '../outside' }] }),
			/must stay within project root/
		);
		await assert.rejects(
			generateDistributedGames({
				...f.options(),
				games: [
					{ manifest: 'a.json', out: 'src/gen' },
					{ manifest: 'b.json', out: 'src/gen/b' }
				]
			}),
			/must not overlap/
		);
		await assert.rejects(
			generateDistributedGames({ ...f.options(), games: [{ manifest: 'a.json', out: 'x', importExtension: 'mjs' }] }),
			/importExtension/
		);
		assert.deepEqual(await f.calls(), []);
	} finally {
		await rm(f.root, { recursive: true, force: true });
	}
});

test('plugin: generates on start, regenerates on a Rust change and reloads; games-only needs no lifecycle', async () => {
	const f = await fixture();
	const previous = process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	delete process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	try {
		// The public entry accepts a games-only configuration under plain Vite.
		const plugin = distributedSvelteKitPublic(f.options({ watch: [join(f.root, 'missing-is-ignored')] }));
		await plugin.configResolved({ root: f.app, command: 'serve', logger: { error: () => undefined } });
		assert.match(await readFile(join(f.out, 'index.ts'), 'utf8'), /zone v1/);
		const watched = [];
		const sent = [];
		const server = {
			watcher: { add: (paths) => watched.push(...[paths].flat()) },
			ws: { send: (payload) => sent.push(payload) },
			moduleGraph: { getModuleById: () => undefined, invalidateModule: () => undefined },
			httpServer: { once: () => undefined }
		};
		plugin.configureServer(server);
		assert.ok(watched.includes(join(f.rust, 'src')));
		assert.ok(watched.includes(join(f.rust, 'Cargo.toml')));

		// Unrelated files are not game inputs.
		assert.equal(await plugin.handleHotUpdate({ file: join(f.app, 'src', 'app.ts'), server }), undefined);
		assert.equal(await plugin.handleHotUpdate({ file: join(f.rust, 'target', 'x.rs'), server }), undefined);

		await writeFile(join(f.rust, 'src', 'lib.rs'), '// zone v2 with predict\n');
		assert.deepEqual(await plugin.handleHotUpdate({ file: join(f.rust, 'src', 'lib.rs'), server }), []);
		assert.match(await readFile(join(f.out, 'index.ts'), 'utf8'), /zone v2/);
		assert.deepEqual(sent, [{ type: 'full-reload', path: '*' }]);

		// A change that generates the same outputs does not reload.
		await plugin.handleHotUpdate({ file: join(f.rust, 'Cargo.toml'), server });
		assert.equal(sent.length, 1);

		// A failing compile is reported to the overlay, not thrown into Vite.
		process.env.FAKE_RUST = join(f.root, 'nowhere');
		assert.deepEqual(await plugin.handleHotUpdate({ file: join(f.rust, 'src', 'lib.rs'), server }), []);
		assert.equal(sent.at(-1).type, 'error');
		await plugin.closeBundle();
	} finally {
		if (previous !== undefined) process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE = previous;
		await rm(f.root, { recursive: true, force: true });
	}
});

test('dev: a failing Rust change never takes the server down (watcher, HMR, startup); builds still fail', async () => {
	const f = await fixture();
	const previous = process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	delete process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	const logged = [];
	const logger = { error: (message) => logged.push(message) };
	const server = () => {
		const sent = [];
		const listeners = [];
		return {
			sent,
			connect: () => listeners.forEach((listener) => listener()),
			watcher: { add: () => undefined },
			ws: { send: (payload) => sent.push(payload), on: (event, listener) => event === 'connection' && listeners.push(listener) },
			moduleGraph: { getModuleById: () => undefined, invalidateModule: () => undefined },
			httpServer: { once: () => undefined }
		};
	};
	const rustFile = join(f.rust, 'src', 'lib.rs');
	try {
		const plugin = distributedSvelteKitPublic(f.options());
		await plugin.configResolved({ root: f.app, command: 'serve', logger });
		const dev = server();
		plugin.configureServer(dev);

		// Vite's dev watcher awaits `watchChange` without a catch: it must resolve.
		process.env.FAKE_RUST = join(f.root, 'nowhere');
		await plugin.watchChange(rustFile);
		assert.equal(logged.length, 1);
		assert.match(logged[0], /game client generation failed[\s\S]*Distributed compiler command failed/);
		assert.equal(dev.sent.at(-1).type, 'error');
		assert.match(await readFile(join(f.out, 'index.ts'), 'utf8'), /zone v1/, 'last good client kept');
		// HMR path: same (overlay + console, no throw).
		assert.deepEqual(await plugin.handleHotUpdate({ file: rustFile, server: dev }), []);
		assert.equal(logged.length, 2);

		// Fixed: regenerates, reloads, and a newly opened overlay sees no stale error.
		process.env.FAKE_RUST = f.rust;
		await writeFile(rustFile, '// zone v3 with predict\n');
		await plugin.handleHotUpdate({ file: rustFile, server: dev });
		assert.deepEqual(dev.sent.at(-1), { type: 'full-reload', path: '*' });
		const before = dev.sent.length;
		dev.connect();
		assert.equal(dev.sent.length, before);
		await plugin.closeBundle();

		// A broken surface at dev start: the server still starts; the error is
		// logged and replayed to overlays that connect afterwards.
		process.env.FAKE_RUST = join(f.root, 'nowhere');
		const late = distributedSvelteKitPublic(f.options());
		await late.configResolved({ root: f.app, command: 'serve', logger });
		assert.equal(logged.length, 3);
		const lateServer = server();
		late.configureServer(lateServer);
		lateServer.connect();
		assert.equal(lateServer.sent.at(-1).type, 'error');
		await late.closeBundle();

		// Builds fail: one-shot build and `vite build --watch` rebuilds.
		const build = distributedSvelteKit(f.options());
		await assert.rejects(build.configResolved({ root: f.app, command: 'build', logger }), /Distributed compiler command failed/);
		process.env.FAKE_RUST = f.rust;
		const watching = distributedSvelteKit(f.options());
		await watching.configResolved({ root: f.app, command: 'build', logger });
		process.env.FAKE_RUST = join(f.root, 'nowhere');
		await assert.rejects(watching.watchChange(rustFile), /Distributed compiler command failed/);
		await watching.closeBundle();
		assert.equal(logged.length, 3, 'build failures are thrown, not just logged');
	} finally {
		process.env.FAKE_RUST = f.rust;
		if (previous !== undefined) process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE = previous;
		await rm(f.root, { recursive: true, force: true });
	}
});

test('plugin build: one generation per build process (SvelteKit reloads the config)', async () => {
	const f = await fixture();
	try {
		const first = distributedSvelteKit(f.options());
		const second = distributedSvelteKit(f.options());
		await first.configResolved({ root: f.app, command: 'build' });
		await second.configResolved({ root: f.app, command: 'build' });
		assert.equal((await f.calls()).filter((a) => a[0] === 'game-client').length, 1);
		await first.closeBundle();
		await second.closeBundle();
	} finally {
		await rm(f.root, { recursive: true, force: true });
	}
});

test('games-free configuration still requires the lifecycle and a surface', async () => {
	const previous = process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	delete process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE;
	try {
		assert.throws(() => distributedSvelteKitPublic({ clients: [] }), /distributed dev/);
		const plugin = distributedSvelteKit({ games: [] });
		await assert.rejects(plugin.configResolved({ root: tmpdir() }), /at least one client or game surface/);
	} finally {
		if (previous !== undefined) process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE = previous;
	}
});
