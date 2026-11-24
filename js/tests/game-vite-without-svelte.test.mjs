/**
 * `@hops-ops/distributed/game/vite` is for any Vite app, and `svelte` is an
 * optional peer. The built package is copied to a directory whose only
 * `node_modules` holds the package's own `dependencies`, which is what an
 * install without the optional peers provides, and the entry is loaded there
 * in a fresh process.
 */
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

const run = promisify(execFile);

async function load(entry, script) {
	const root = await mkdtemp(join(tmpdir(), 'distributed-no-svelte-'));
	try {
		await cp(fileURLToPath(new URL('../dist', import.meta.url)), join(root, 'dist'), {
			recursive: true
		});
		const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
		for (const name of Object.keys(manifest.dependencies ?? {})) {
			const link = join(root, 'node_modules', name);
			await mkdir(dirname(link), { recursive: true });
			await symlink(fileURLToPath(new URL(`../node_modules/${name}`, import.meta.url)), link);
		}
		const url = pathToFileURL(join(root, 'dist', entry)).href;
		return await run(
			process.execPath,
			['--input-type=module', '-e', `const entry = await import(${JSON.stringify(url)});${script}`],
			{ cwd: root }
		);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

test('game/vite loads and builds its plugin where svelte cannot be resolved', async () => {
	const { stdout } = await load(
		'game/vite.js',
		`const plugin = entry.distributedGames({ games: [{ name: 'arena', manifest: 'server/Cargo.toml', out: 'src/generated/arena' }] });
		 console.log(JSON.stringify([plugin.name, typeof entry.generateDistributedGames, typeof entry.checkDistributedGames]));`
	);
	const [name, generate, check] = JSON.parse(stdout);
	assert.equal(typeof name, 'string');
	assert.deepEqual([generate, check], ['function', 'function']);
});

test('the copy really cannot resolve svelte', async () => {
	// Guards the test above: a module that imports `svelte` statically must fail
	// to load from the same place.
	await assert.rejects(load('sveltekit/context.js', ''), /Cannot find package 'svelte'/);
});
