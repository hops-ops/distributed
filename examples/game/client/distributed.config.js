// @ts-nocheck — config script read by Node (Vite plugin, scripts, run.sh).
//
// `games`: the Mob Pit zone surface. Its typed client and WebAssembly are
// build outputs generated from the Rust definition by the Vite plugin (dev
// start, Rust changes, build) and by `scripts/games.mjs` before `tsc`;
// `src/generated/` is gitignored so it can never drift from the Rust surface.
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const app = dirname(fileURLToPath(import.meta.url));
const repo = resolve(app, '../../..');
const server = resolve(app, '../server');

/** @type {import('@hops-ops/distributed/game/vite').DistributedGamesViteOptions} */
export const distributedGamesOptions = {
	cwd: app,
	// The CLI from this checkout (an installed `distributed` works the same).
	command: 'cargo',
	commandArgs: ['run', '--quiet', '--manifest-path', resolve(repo, 'Cargo.toml'), '-p', 'distributed_cli', '--bin', 'distributed', '--'],
	games: [
		{
			manifest: {
				args: ['game-manifest', '--manifest-path', resolve(server, 'Cargo.toml'), '--entrypoint', 'game_demos::mobpit::game_surface']
			},
			out: 'src/generated/mobpit',
			// The predicted step (`pit_step`) lives in the sim crate.
			watch: [resolve(server, 'sim')]
		}
	]
};
