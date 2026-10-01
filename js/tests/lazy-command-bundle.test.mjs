import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { build } from 'esbuild';

// Bundle the compiler's exact fixture, not a hand-written approximation of it.
test('generated lazy factory keeps command definitions and runtime outside the initial browser closure', async () => {
	const root = fileURLToPath(new URL('../', import.meta.url));
	const fixture = path.resolve(
		root,
		'../distributed_cli/tests/fixtures/generated-lazy-commands.ts'
	);
	const result = await build({
		entryPoints: [fixture],
		bundle: true,
		splitting: true,
		format: 'esm',
		platform: 'browser',
		minify: true,
		write: false,
		metafile: true,
		outdir: path.join(root, '.bundle-test'),
		alias: {
			'@hops-ops/distributed/replica/lazy': path.join(
				root,
				'dist/replica/lazy.js'
			),
			'@hops-ops/distributed/replica': path.join(root, 'dist/replica/index.js')
		},
		plugins: [
			{
				name: 'fixture-status',
				setup(plugin) {
					plugin.onResolve({ filter: /^\.\/protocol\.js$/ }, (args) =>
						args.importer.includes('/tests/fixtures/')
							? { path: 'status', namespace: 'fixture' }
							: undefined
					);
					plugin.onLoad({ filter: /.*/, namespace: 'fixture' }, () => ({
						contents: 'export const COMMAND_STATUS = {};'
					}));
				}
			}
		]
	});
	const outputs = result.metafile.outputs;
	const initial = Object.keys(outputs).find(
		(file) => outputs[file].entryPoint === path.relative(process.cwd(), fixture)
	);
	assert.ok(initial);
	const seen = new Set();
	function visit(file) {
		if (seen.has(file)) return;
		seen.add(file);
		for (const dependency of outputs[file].imports) {
			if (dependency.kind !== 'dynamic-import' && !dependency.external)
				visit(dependency.path);
		}
	}
	visit(initial);
	const deferredInputs = ['generated-commands.ts', 'command-runtime/create.js'];
	for (const suffix of deferredInputs) {
		assert.ok(
			Object.values(outputs).some((output) =>
				Object.entries(output.inputs).some(
					([name, info]) => name.endsWith(suffix) && info.bytesInOutput > 0
				)
			),
			`${suffix} must remain available`
		);
		assert.ok(
			[...seen].every((file) =>
				Object.entries(outputs[file].inputs).every(
					([name, info]) => !name.endsWith(suffix) || info.bytesInOutput === 0
				)
			),
			`${suffix} must be deferred`
		);
	}
});
