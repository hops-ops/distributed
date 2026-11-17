// Multi-page Vite app: `/` lists the demos, `/<demo>/` is one demo.
// Add a demo: create `<demo>/index.html` + `src/<demo>/main.ts` and list it
// in DEMOS. In dev, game traffic is proxied to the Rust `game-demos` server
// (GAME_DEMOS_SERVER, default http://127.0.0.1:2567), whose gateway routes
// Mob Pit's rooms (`/zones/{room}`) on to celld.
import { resolve } from 'node:path';
import { distributedGames } from '@hops-ops/distributed/game/vite';
import { defineConfig } from 'vite';

import { distributedGamesOptions } from './distributed.config.js';

const DEMOS = ['mobpit', 'leaderboard'];
const server = process.env.GAME_DEMOS_SERVER ?? 'http://127.0.0.1:2567';

export default defineConfig({
	// Generates src/generated/<surface>/ (typed client + WASM) from the Rust
	// surfaces on dev start, Rust changes and build.
	// GAME_CODEGEN=external: something else generates them (the local hops
	// pod's Node container has no cargo; its services container does it).
	plugins: process.env.GAME_CODEGEN === 'external' ? [] : [distributedGames(distributedGamesOptions)],
	server: {
		port: Number(process.env.GAME_DEMOS_CLIENT_PORT ?? 5173),
		strictPort: true,
		proxy: {
			'/zones': { target: server, ws: true },
			'/dev': server,
			'/metrics': server
		},
		// The SDK is a symlinked local package (file:../../../js).
		fs: { allow: [resolve(__dirname, '..'), resolve(__dirname, '../../../js')] }
	},
	build: {
		chunkSizeWarningLimit: 2000,
		rollupOptions: {
			input: Object.fromEntries([
				['index', resolve(__dirname, 'index.html')],
				...DEMOS.map((demo) => [demo, resolve(__dirname, `${demo}/index.html`)])
			])
		}
	}
});
