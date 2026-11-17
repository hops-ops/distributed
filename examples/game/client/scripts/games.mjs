// Generate the game clients (+ WASM) from the Rust surfaces; `--check` then
// verifies generation is deterministic (a fresh run matches what was just
// written). Generated clients are never committed.
import { checkDistributedGames, generateDistributedGames } from '@hops-ops/distributed/game/vite';
import { distributedGamesOptions } from '../distributed.config.js';

await generateDistributedGames(distributedGamesOptions);
if (process.argv.includes('--check')) {
	await checkDistributedGames(distributedGamesOptions);
	console.log('game client generation is deterministic');
}
