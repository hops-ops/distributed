/**
 * A local multi-node celld fleet for one Node test: one RustFS container
 * (docker compose, a project and port of its own) holding the fleet
 * bucket, the demos' cells worker (rooms + cells) deployed to it with
 * `celld deploy`, and N nodes in production mode. `stop()` stops the nodes
 * it started and removes the container (`docker compose down -v`).
 *
 * Returns null when docker, the aws CLI or celld is missing, unless
 * `MOBPIT_FLEET_REQUIRED=1` (then it throws).
 */
import { spawn, spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { freePort } from './celld.mjs';

const CELLS = fileURLToPath(new URL('../../../cells/', import.meta.url));
const RELAY_PATH = '/internal/celld-queue/relay';

function skip(reason) {
	if (process.env.MOBPIT_FLEET_REQUIRED === '1') throw new Error(`fleet required: ${reason}`);
	console.error(`no celld fleet: ${reason}`);
	return null;
}

async function waitFor(url, ms) {
	const deadline = Date.now() + ms;
	for (;;) {
		try {
			if ((await fetch(url)).ok) return;
		} catch {
			// not up yet
		}
		if (Date.now() > deadline) throw new Error(`${url} not ready`);
		await new Promise((r) => setTimeout(r, 250));
	}
}

export async function startFleet({ relayPort, nodes = 3, vars = {} } = {}) {
	const bin = process.env.CELLD_BIN;
	if (!bin) return skip('CELLD_BIN is not set');
	for (const tool of ['docker', 'aws']) {
		if (spawnSync(tool, ['--version']).status !== 0) return skip(`${tool} is not available`);
	}
	const dir = mkdtempSync(join(tmpdir(), 'mobpit-fleet-js-'));
	const s3Port = await freePort();
	const project = `mobpit-fleet-js-${process.pid}-${s3Port}`;
	const aws = {
		...process.env,
		AWS_ACCESS_KEY_ID: 'fleetjs',
		AWS_SECRET_ACCESS_KEY: 'fleetjssecret123',
		AWS_REGION: 'us-east-1',
		S3_ENDPOINT: `http://127.0.0.1:${s3Port}`
	};
	const compose = join(dir, 's3-compose.yaml');
	writeFileSync(
		compose,
		`services:\n  s3:\n    image: rustfs/rustfs:1.0.3\n    command: ["/data"]\n    environment:\n      RUSTFS_ADDRESS: "0.0.0.0:9000"\n      RUSTFS_ACCESS_KEY: ${aws.AWS_ACCESS_KEY_ID}\n      RUSTFS_SECRET_KEY: ${aws.AWS_SECRET_ACCESS_KEY}\n    ports: ["127.0.0.1:${s3Port}:9000"]\n`
	);
	const children = [];
	const down = () => {
		for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
		spawnSync('docker', ['compose', '-p', project, '-f', compose, 'down', '-v'], { stdio: 'ignore' });
	};
	try {
		const up = spawnSync('docker', ['compose', '-p', project, '-f', compose, 'up', '-d'], { encoding: 'utf8' });
		if (up.status !== 0) throw new Error(`docker compose up: ${up.stderr}`);
		const deadline = Date.now() + 30_000;
		for (;;) {
			const mb = spawnSync('aws', ['--endpoint-url', aws.S3_ENDPOINT, 's3', 'mb', 's3://mobpit-fleet'], { env: aws, encoding: 'utf8' });
			if (mb.status === 0 || /BucketAlready/.test(mb.stderr)) break;
			if (Date.now() > deadline) {
				// Docker ran the container but its port is not reachable here.
				down();
				return skip(`RustFS unreachable on 127.0.0.1:${s3Port}: ${mb.stderr.trim()}`);
			}
			await new Promise((r) => setTimeout(r, 500));
		}
		const secret = `test-only-${randomBytes(24).toString('hex')}`;
		const ticketSecret = randomBytes(32).toString('hex');
		const ports = [];
		for (let i = 0; i < nodes; i++) ports.push({ name: `node-${'abcdef'[i]}`, pub: await freePort(), op: await freePort() });
		const worker = join(dir, 'worker');
		mkdirSync(worker, { recursive: true });
		cpSync(join(CELLS, 'build'), join(worker, 'build'), { recursive: true });
		const config = JSON.parse(
			readFileSync(join(CELLS, 'wrangler.jsonc'), 'utf8')
				.split('\n')
				.filter((l) => !l.trimStart().startsWith('//'))
				.join('\n')
		);
		const base = {
			...config.vars,
			DISTRIBUTED_INTERNAL_SECRET: secret,
			GAME_TICKET_SECRET: ticketSecret,
			CELLD_QUEUE_RELAY_URL: `http://127.0.0.1:${relayPort}${RELAY_PATH}`,
			CELLD_ZONE_NODES: ports.map((p) => `${p.name}=127.0.0.1:${p.op}`).join(',')
		};
		// `celld deploy` the worker with `extra` vars over the base ones.
		const deploy = (extra) => {
			config.vars = { ...base, ...extra };
			writeFileSync(join(worker, 'wrangler.json'), JSON.stringify(config));
			const run = spawnSync(bin, ['deploy', worker, '--bucket', 's3://mobpit-fleet/fleet'], { env: aws, encoding: 'utf8' });
			if (run.status !== 0) throw new Error(`celld deploy: ${run.stdout}${run.stderr}`);
		};
		deploy(vars);
		for (const p of ports) {
			const ndir = join(dir, p.name);
			mkdirSync(ndir, { recursive: true });
			const child = spawn(
				bin,
				['--bucket', 's3://mobpit-fleet/fleet', '--listen', `127.0.0.1:${p.pub}`, '--internal-listen', `127.0.0.1:${p.op}`, '--advertise', `127.0.0.1:${p.op}`],
				{
					env: { ...aws, CELLD_NODE: p.name, CELLD_WATCH: join(ndir, 'watch'), CELLD_READY_FLEET_GATE_MS: '0', RUST_LOG: 'warn' },
					stdio: 'ignore'
				}
			);
			children.push(child);
		}
		await Promise.all(ports.map((p) => waitFor(`http://127.0.0.1:${p.pub}/health`, 90_000)));
		const urls = ports.map((p) => `http://127.0.0.1:${p.pub}`);
		return {
			urls,
			url: urls[0],
			nodes: ports.map((p) => p.name),
			ticketSecret,
			env: { CELLD_URL: urls[0], DISTRIBUTED_INTERNAL_SECRET: secret, DISTRIBUTED_GAME_SECRET: ticketSecret },
			/** Deploy the worker again with these vars over the base ones (not over earlier `vars`). */
			redeploy: deploy,
			async stop() {
				down();
				rmSync(dir, { recursive: true, force: true });
			}
		};
	} catch (error) {
		down();
		throw error;
	}
}
