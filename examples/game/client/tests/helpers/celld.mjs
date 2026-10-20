/**
 * `celld dev` serving Mob Pit's cells (`examples/game/cells`) for one Node
 * test: its own temp project, port and internal secret; `stop()` kills the
 * process tree this helper started (nothing else).
 *
 * celld comes from CELLD_BIN (examples/game/run.sh sets it; there is no
 * native cell mode to fall back to). The worker must be built
 * (`worker-build --release` in examples/game/cells; run.sh does it), and
 * CELLD_ESBUILD is passed through for celld's bundler.
 */
import { spawn, execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { cpSync, existsSync, mkdtempSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const CELLS = fileURLToPath(new URL('../../../cells/', import.meta.url));
const RELAY_PATH = '/internal/celld-queue/relay';

/** A free local port (bound, then released). */
export function freePort() {
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
 * Start celld. `relayPort`: the demo server port its outbox Queue relays
 * domain events to (`/internal/celld-queue/relay`).
 */
export async function startCelld({ relayPort } = {}) {
	const bin = process.env.CELLD_BIN;
	if (!bin) throw new Error('CELLD_BIN is not set: Mob Pit\'s cells run on celld (0.6.2+); point CELLD_BIN at the celld binary');
	if (!existsSync(join(CELLS, 'build/worker/shim.mjs'))) {
		throw new Error(`the Mob Pit cells worker is not built: run \`worker-build --release\` in ${CELLS}`);
	}
	const project = mkdtempSync(join(tmpdir(), 'mobpit-celld-js-'));
	cpSync(join(CELLS, 'build'), join(project, 'build'), { recursive: true });
	cpSync(join(CELLS, 'wrangler.jsonc'), join(project, 'wrangler.jsonc'));
	const secret = `test-only-${randomBytes(24).toString('hex')}`;
	let vars = `DISTRIBUTED_INTERNAL_SECRET=${secret}\n`;
	if (relayPort) vars += `CELLD_QUEUE_RELAY_URL=http://127.0.0.1:${relayPort}${RELAY_PATH}\n`;
	writeFileSync(join(project, '.dev.vars'), vars);
	const port = await freePort();
	const child = spawn(bin, ['dev', project, '--host', '127.0.0.1', '--port', String(port), '--no-watch', '--logs'], {
		env: { ...process.env, RUST_LOG: 'warn' },
		stdio: ['ignore', 'pipe', 'pipe']
	});
	let output = '';
	child.stdout.on('data', (chunk) => (output += chunk));
	child.stderr.on('data', (chunk) => (output += chunk));
	const url = `http://127.0.0.1:${port}`;
	const deadline = Date.now() + 90_000;
	for (;;) {
		if (child.exitCode !== null) throw new Error(`celld dev exited with ${child.exitCode}:\n${output}`);
		try {
			if ((await fetch(`${url}/health`)).ok) break;
		} catch {
			// not listening yet
		}
		if (Date.now() > deadline) throw new Error(`celld dev did not become ready:\n${output}`);
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	return {
		url,
		secret,
		project,
		/** Env for `game-demos mobpit`. */
		env: { CELLD_URL: url, DISTRIBUTED_INTERNAL_SECRET: secret },
		output: () => output,
		async stop() {
			if (child.exitCode !== null || child.signalCode !== null) return;
			const exited = new Promise((resolve) => child.on('exit', resolve));
			killTree(child.pid);
			await exited;
		}
	};
}

/** SIGKILL `root` and every descendant (`celld dev` starts its node in a
 * session of its own, so the tree is collected while the root is stopped). */
function killTree(root) {
	try {
		process.kill(root, 'SIGSTOP');
	} catch {
		return;
	}
	const pairs = execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8' })
		.trim()
		.split('\n')
		.map((line) => line.trim().split(/\s+/).map(Number));
	const doomed = [root];
	for (let i = 0; i < doomed.length; i++) {
		for (const [pid, ppid] of pairs) if (ppid === doomed[i]) doomed.push(pid);
	}
	for (const pid of doomed.reverse()) {
		try {
			process.kill(pid, 'SIGKILL');
		} catch {
			// already gone
		}
	}
}
