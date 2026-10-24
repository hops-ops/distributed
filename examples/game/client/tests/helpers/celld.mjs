/**
 * `celld dev` serving the demos' cells (`examples/game/cells`) for one Node
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
 * domain events to (`/internal/celld-queue/relay`); `vars`: extra worker
 * vars (e.g. `ZONE_HIBERNATE_AFTER_MS` for Mob Pit's rooms). The rooms'
 * ticket secret (`GAME_TICKET_SECRET`) is generated here and returned as
 * `ticketSecret` (and in `env` as `DISTRIBUTED_GAME_SECRET`, what
 * `game-demos mobpit` signs room tickets with).
 */
export async function startCelld({ relayPort, vars: extra = {} } = {}) {
	const bin = process.env.CELLD_BIN;
	if (!bin) throw new Error('CELLD_BIN is not set: the demos\' cells run on celld (the version in .celld-version); point CELLD_BIN at the celld binary');
	if (!existsSync(join(CELLS, 'build/worker/shim.mjs'))) {
		throw new Error(`the demos' cells worker is not built: run \`worker-build --release\` in ${CELLS}`);
	}
	const project = mkdtempSync(join(tmpdir(), 'game-demo-celld-js-'));
	cpSync(join(CELLS, 'build'), join(project, 'build'), { recursive: true });
	cpSync(join(CELLS, 'wrangler.jsonc'), join(project, 'wrangler.jsonc'));
	const secret = `test-only-${randomBytes(24).toString('hex')}`;
	const ticketSecret = randomBytes(32).toString('hex');
	let vars = `DISTRIBUTED_INTERNAL_SECRET=${secret}\nGAME_TICKET_SECRET=${ticketSecret}\n`;
	if (relayPort) vars += `CELLD_QUEUE_RELAY_URL=http://127.0.0.1:${relayPort}${RELAY_PATH}\n`;
	for (const [key, value] of Object.entries(extra)) vars += `${key}=${value}\n`;
	writeFileSync(join(project, '.dev.vars'), vars);
	const port = await freePort();
	const url = `http://127.0.0.1:${port}`;
	let child;
	let output = '';
	// Start (or start again: same project, port and state) and wait until healthy.
	const up = async () => {
		child = spawn(bin, ['dev', project, '--host', '127.0.0.1', '--port', String(port), '--no-watch', '--logs'], {
			// One node: no ownership balancing (its sampler walks every cell).
			env: { CELLD_REBALANCE_INTERVAL_MS: '0', ...process.env, RUST_LOG: 'warn' },
			stdio: ['ignore', 'pipe', 'pipe']
		});
		const started = child;
		started.stdout.on('data', (chunk) => (output += chunk));
		started.stderr.on('data', (chunk) => (output += chunk));
		const deadline = Date.now() + 90_000;
		for (;;) {
			if (started.exitCode !== null) throw new Error(`celld dev exited with ${started.exitCode}:\n${output}`);
			try {
				if ((await fetch(`${url}/health`)).ok) break;
			} catch {
				// not listening yet
			}
			if (Date.now() > deadline) throw new Error(`celld dev did not become ready:\n${output}`);
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
	};
	const stop = async () => {
		if (child.exitCode !== null || child.signalCode !== null) return;
		const exited = new Promise((resolve) => child.on('exit', resolve));
		killTree(child.pid);
		await exited;
	};
	await up();
	return {
		url,
		secret,
		ticketSecret,
		project,
		/** Env for `game-demos mobpit`. */
		env: { CELLD_URL: url, DISTRIBUTED_INTERNAL_SECRET: secret, DISTRIBUTED_GAME_SECRET: ticketSecret },
		output: () => output,
		/** Kill celld (SIGKILL, like a crash) and start it again on the same state and port. */
		async restart() {
			await stop();
			await up();
		},
		stop
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

/**
 * Objects in `celld dev`'s local store, or only those under `prefix`
 * (e.g. `cells/PitRoom:`); `?` when unreadable.
 */
export function storeObjects(project, prefix = '') {
	try {
		const { DatabaseSync } = process.getBuiltinModule('node:sqlite');
		const db = new DatabaseSync(join(project, '.celld/dev/objects.sqlite3'), { readOnly: true, timeout: 10_000 });
		const { n } = db.prepare('select count(*) as n from objects where substr(key, 1, length(?1)) = ?1').get(prefix);
		db.close();
		return n;
	} catch {
		return '?';
	}
}
