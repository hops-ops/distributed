/**
 * What the browser entry points pull into an application bundle: no Node,
 * Svelte compiler or GraphQL printer code, and a size budget for SvelteKit.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { build } from 'esbuild';

const bundle = (entry) =>
	build({
		entryPoints: [entry],
		bundle: true,
		format: 'esm',
		platform: 'browser',
		metafile: true,
		write: false,
		logLevel: 'silent'
	});

test('browser entry points carry no Node, compiler or GraphQL printer code', async () => {
	for (const entry of ['src/index.ts', 'src/replica/index.ts', 'src/sveltekit/index.ts']) {
		const inputs = Object.keys((await bundle(entry)).metafile.inputs);
		assert.deepEqual(
			inputs.filter(
				(path) =>
					path.includes('svelte/compiler') ||
					path.includes('node_modules/graphql/') ||
					path.includes('/src/vite/') ||
					path.includes('/sveltekit/vite.ts') ||
					path.includes('/sveltekit/islands/boundaries.ts') ||
					path.startsWith('node:')
			),
			[],
			entry
		);
	}
});

test('the SvelteKit browser bundle stays within its budget', async () => {
	const { outputFiles } = await bundle('src/sveltekit/index.ts');
	// Unminified. ~696 KB after the React adapter, offline persistence and the
	// GraphQL printer were cut (2026-11-25; ~722 KB before).
	assert.ok(
		outputFiles[0].contents.byteLength < 725_000,
		`SvelteKit browser bundle is ${outputFiles[0].contents.byteLength} bytes`
	);
});
