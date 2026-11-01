// CI: generated game clients match the Rust surfaces (never writes).
import { checkDistributedGames } from '@hops-ops/distributed/sveltekit/vite';
import { distributedViteOptions } from '../distributed.config.js';

await checkDistributedGames(distributedViteOptions);
console.log('game clients are current');
