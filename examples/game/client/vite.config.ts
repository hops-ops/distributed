// Multi-page Vite app: `/` lists the demos, `/<demo>/` is one demo.
// Add a demo: create `<demo>/index.html` + `src/<demo>/main.ts` and list it
// in DEMOS. In dev, game traffic is proxied to the Rust `game-demos` server
// (GAME_DEMOS_SERVER, default http://127.0.0.1:2567); Mob Pit's rooms
// (`/rooms/{room}`) to celld, which runs them (GAME_DEMOS_ROOMS, default
// http://127.0.0.1:2570).
import { resolve } from 'node:path';
import { defineConfig } from 'vite';

const DEMOS = ['mobpit', 'leaderboard', 'gallery', 'freeze'];
const server = process.env.GAME_DEMOS_SERVER ?? 'http://127.0.0.1:2567';
const rooms = process.env.GAME_DEMOS_ROOMS ?? 'http://127.0.0.1:2570';

export default defineConfig({
	server: {
		port: Number(process.env.GAME_DEMOS_CLIENT_PORT ?? 5173),
		strictPort: true,
		proxy: {
			'/zones': { target: server, ws: true },
			'/rooms': { target: rooms, ws: true },
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
