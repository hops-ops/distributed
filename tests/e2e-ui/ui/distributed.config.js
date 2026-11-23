// @ts-nocheck — config script read by Node (Vite plugin, lifecycle, CI).
//
// `distributed.clients.json` declares this application's GraphQL clients.
// `distributed build|dev` validates that file and owns the generated output;
// this file only says where the Rust application and the CLI are.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const app = dirname(fileURLToPath(import.meta.url));
const repo = resolve(app, '../../..');
const server = resolve(app, '..');

const inventory = JSON.parse(readFileSync(resolve(app, 'distributed.clients.json'), 'utf8'));

const manifest = [
	'client-manifest',
	'--manifest-path',
	resolve(server, 'Cargo.toml'),
	'--package',
	'e2e-service',
	'--distributed-path',
	repo
];

/** GraphQL clients from `distributed.clients.json` + how to export their surface. */
export const distributedClients = inventory.clients.map((client) => ({
	module: client.module,
	manifest: {
		args: client.manifest_entrypoint
			? [...manifest, '--entrypoint', client.manifest_entrypoint]
			: manifest
	},
	surface: client.surface,
	documents: client.documents,
	out: client.output
}));

/** @type {import('@hops-ops/distributed/sveltekit/vite').DistributedSvelteKitViteOptions} */
export const distributedViteOptions = {
	cwd: app,
	// The CLI from this checkout (an installed `distributed` works the same;
	// under `distributed build|dev` the lifecycle's own executable is used).
	command: 'cargo',
	commandArgs: ['run', '--quiet', '--manifest-path', resolve(repo, 'Cargo.toml'), '-p', 'distributed_cli', '--bin', 'distributed', '--'],
	clients: distributedClients
};
