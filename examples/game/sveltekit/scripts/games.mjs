// Generate the game clients (+ WASM) from the Rust surfaces; `--check` then
// verifies generation is deterministic (a fresh run matches what was just
// written). Generated clients are never committed.
import { checkDistributedGames, generateDistributedGames } from '@hops-ops/distributed/sveltekit/vite';
import { distributedViteOptions } from '../distributed.config.js';

await generateDistributedGames(distributedViteOptions);
if (process.argv.includes('--check')) {
	await checkDistributedGames(distributedViteOptions);
	console.log('game client generation is deterministic');
}
