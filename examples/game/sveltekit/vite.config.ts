import { sveltekit } from '@sveltejs/kit/vite';
import { distributedSvelteKit } from '@hops-ops/distributed/sveltekit/vite';
import { defineConfig } from 'vite';

import { distributedViteOptions } from './distributed.config.js';

export default defineConfig({
	plugins: [distributedSvelteKit(distributedViteOptions), sveltekit()],
	server: { port: Number(process.env.PORT ?? 5573), strictPort: true }
});
