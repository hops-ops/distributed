/**
 * Start a multi-host demo (`game-demos <demo> --host-id … --peer …`) for a
 * Node test: one process per host on free ephemeral ports, a shared ticket
 * secret, dev routes on. Uses GAME_DEMOS_BIN when set, else `cargo run`.
 */
import { spawn } from 'node:child_process';
import { createServer, Socket } from 'node:net';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const SERVER_DIR = fileURLToPath(new URL('../../../server/', import.meta.url));

function freePort() {
	return new Promise((resolve, reject) => {
		const server = createServer();
		server.unref();
		server.on('error', reject);
		server.listen(0, '127.0.0.1', () => {
			const { port } = server.address();
			server.close(() => resolve(port));
		});
	});
}

/**
 * @param {string} demo
 * @param {{ hosts?: string[], hibernateAfterMs?: number, env?: Record<string, string> }} options
 */
export async function startCluster(demo, { hosts = ['A', 'B'], hibernateAfterMs = 4000, env = {} } = {}) {
	const ports = [];
	for (const _ of hosts) ports.push(await freePort());
	const stateDir = mkdtempSync(join(tmpdir(), `game-demos-${demo}-`));
	const secret = `test-secret-${process.pid}-${Date.now()}`;
	const bin = process.env.GAME_DEMOS_BIN;
	const children = [];
	const urls = {};
	try {
		await Promise.all(
			hosts.map(async (id, i) => {
				const args = [demo, '--dev', '--port', String(ports[i]), '--host-id', id, '--secret', secret];
				args.push('--state-dir', stateDir, '--hibernate-after-ms', String(hibernateAfterMs));
				hosts.forEach((peer, j) => {
					if (j !== i) args.push('--peer', `${peer}=127.0.0.1:${ports[j]}`);
				});
				const options = { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'inherit'] };
				const child = bin
					? spawn(bin, args, options)
					: spawn('cargo', ['run', '-q', '--', ...args], { ...options, cwd: SERVER_DIR });
				children.push(child);
				urls[id] = await new Promise((resolve, reject) => {
					let buffered = '';
					child.stdout.on('data', (chunk) => {
						buffered += chunk;
						const match = /LISTENING (http:\/\/\S+)/.exec(buffered);
						if (match) resolve(match[1]);
					});
					child.on('exit', (code) => reject(new Error(`game-demos ${demo} host ${id} exited with ${code}`)));
				});
			})
		);
	} catch (error) {
		for (const child of children) child.kill('SIGKILL');
		rmSync(stateDir, { recursive: true, force: true });
		throw error;
	}
	return {
		/** HTTP origin per host id. */
		urls,
		/** First host (what the Vite proxy points at in `run.sh`). */
		httpUrl: urls[hosts[0]],
		async stop() {
			await Promise.all(
				children.map(async (child) => {
					if (child.exitCode !== null) return;
					const exited = new Promise((resolve) => child.on('exit', resolve));
					child.kill('SIGTERM');
					const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
					await exited;
					clearTimeout(timer);
				})
			);
			rmSync(stateDir, { recursive: true, force: true });
			// Node's WebSocket (undici) keeps a socket whose closing handshake
			// the server started (Redirect) paused until a ~60 s timeout, which
			// would hold the test process open: drop what still points at us.
			for (const handle of process._getActiveHandles()) {
				if (handle instanceof Socket && ports.includes(handle.remotePort)) handle.destroy();
			}
		}
	};
}
