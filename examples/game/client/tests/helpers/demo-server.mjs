/**
 * Start a `game-demos <demo>` server for a Node test (port 0 unless given,
 * dev routes on)
 * and stop it afterwards. Uses GAME_DEMOS_BIN when set (run.sh builds it),
 * else `cargo run` in ../server (compiles on first use).
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const SERVER_DIR = fileURLToPath(new URL('../../../server/', import.meta.url));

export async function startDemo(demo, { env = {}, port = 0 } = {}) {
	const bin = process.env.GAME_DEMOS_BIN;
	const args = [demo, '--port', String(port), '--dev'];
	const child = bin
		? spawn(bin, args, { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'inherit'] })
		: spawn('cargo', ['run', '-q', '--', ...args], {
				cwd: SERVER_DIR,
				env: { ...process.env, ...env },
				stdio: ['ignore', 'pipe', 'inherit']
			});
	const httpUrl = await new Promise((resolve, reject) => {
		let buffered = '';
		child.stdout.on('data', (chunk) => {
			buffered += chunk;
			const match = /LISTENING (http:\/\/\S+)/.exec(buffered);
			if (match) resolve(match[1]);
		});
		child.on('exit', (code) => reject(new Error(`game-demos ${demo} exited with ${code}`)));
	});
	return {
		httpUrl,
		async stop() {
			if (child.exitCode !== null) return;
			const exited = new Promise((resolve) => child.on('exit', resolve));
			child.kill('SIGTERM');
			const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
			await exited;
			clearTimeout(timer);
		}
	};
}

export async function until(predicate, timeoutMs = 10_000, what = 'condition') {
	const start = Date.now();
	while (!predicate()) {
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

/** Poll an async `probe` until it returns a truthy value (returned). */
export async function eventually(probe, timeoutMs = 10_000, what = 'condition') {
	const start = Date.now();
	for (;;) {
		const value = await probe();
		if (value) return value;
		if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** In-memory Storage for DevApi in Node. */
export function memoryStorage() {
	const map = new Map();
	return { getItem: (k) => map.get(k) ?? null, setItem: (k, v) => map.set(k, String(v)) };
}
