/**
 * One compiler per project: a PID lock file shared by every plugin instance
 * of this process through a global coordinator, which also serializes runs.
 */
import { open, mkdir, readFile, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import { isMissing } from '../lib/node-fs.js';
import { validateNearestExistingParent } from './compiler-process.js';

const COMPILER_LOCK = join('.svelte-kit', 'distributed', 'compiler.lock');
const COMPILER_COORDINATORS = Symbol.for(
	'@hops-ops/distributed/sveltekit/compiler-coordinators/v1'
);

type CompilerCoordinator = {
	path: string;
	references: number;
	ready: Promise<void>;
	tail: Promise<void>;
	startupRuns: Map<string, Promise<void>>;
	closing?: Promise<void>;
};

export type CompilerLockLease = {
	coordinator: CompilerCoordinator;
	released: boolean;
};

function compilerCoordinatorRegistry(): Map<string, CompilerCoordinator> {
	const globalScope = globalThis as unknown as Record<symbol, unknown>;
	const existing = globalScope[COMPILER_COORDINATORS];
	if (existing instanceof Map) {
		return existing as Map<string, CompilerCoordinator>;
	}
	const registry = new Map<string, CompilerCoordinator>();
	globalScope[COMPILER_COORDINATORS] = registry;
	return registry;
}

export async function acquireCompilerLock(cwd: string): Promise<CompilerLockLease> {
	const path = join(cwd, COMPILER_LOCK);
	const registry = compilerCoordinatorRegistry();
	for (;;) {
		const existing = registry.get(path);
		if (existing?.closing !== undefined) {
			await existing.closing;
			continue;
		}
		const coordinator =
			existing ??
			({
				path,
				references: 0,
				ready: acquirePhysicalCompilerLock(cwd, path),
				tail: Promise.resolve(),
				startupRuns: new Map()
			} satisfies CompilerCoordinator);
		if (existing === undefined) registry.set(path, coordinator);
		coordinator.references += 1;
		try {
			await coordinator.ready;
			return { coordinator, released: false };
		} catch (error) {
			coordinator.references -= 1;
			if (
				coordinator.references === 0 &&
				registry.get(path) === coordinator
			) {
				registry.delete(path);
			}
			throw error;
		}
	}
}

async function acquirePhysicalCompilerLock(
	cwd: string,
	path: string
): Promise<void> {
	await validateNearestExistingParent(cwd, dirname(path));
	await mkdir(dirname(path), { recursive: true });
	await validateNearestExistingParent(cwd, path);
	for (let attempt = 0; attempt < 2; attempt += 1) {
		try {
			const handle = await open(path, 'wx', 0o600);
			try {
				await handle.writeFile(
					`${JSON.stringify({ pid: process.pid, version: 1 })}\n`,
					'utf8'
				);
			} finally {
				await handle.close();
			}
			return;
		} catch (error) {
			if (!isAlreadyExists(error)) throw error;
			let owner: unknown;
			try {
				owner = JSON.parse(await readFile(path, 'utf8')) as unknown;
			} catch {
				owner = undefined;
			}
			const pid =
				owner !== null &&
				typeof owner === 'object' &&
				'pid' in owner &&
				typeof owner.pid === 'number'
					? owner.pid
					: undefined;
			if (pid !== undefined && processExists(pid)) {
				throw new Error(
					`Distributed SvelteKit compiler already owns ${cwd} (pid ${pid})`
				);
			}
			await unlink(path).catch((unlinkError: unknown) => {
				if (!isMissing(unlinkError)) throw unlinkError;
			});
		}
	}
	throw new Error(`could not acquire Distributed SvelteKit compiler lock ${path}`);
}

export function withCompilerLock<T>(
	lease: CompilerLockLease,
	operation: () => Promise<T>
): Promise<T> {
	if (lease.released) {
		return Promise.reject(
			new Error('Distributed SvelteKit compiler lock lease is already released')
		);
	}
	const coordinator = lease.coordinator;
	const run = coordinator.tail.catch(() => undefined).then(operation);
	coordinator.tail = run.then(
		() => undefined,
		() => undefined
	);
	return run;
}

export function withCompilerStartup(
	lease: CompilerLockLease,
	/** Identity of the startup run; plugins configured alike share one run. */
	key: string,
	operation: () => Promise<void>
): Promise<void> {
	if (lease.released) {
		return Promise.reject(
			new Error('Distributed SvelteKit compiler lock lease is already released')
		);
	}
	const coordinator = lease.coordinator;
	const existing = coordinator.startupRuns.get(key);
	if (existing !== undefined) return existing;
	const run = withCompilerLock(lease, operation);
	const cached = run.catch((error: unknown) => {
		if (coordinator.startupRuns.get(key) === cached) {
			coordinator.startupRuns.delete(key);
		}
		throw error;
	});
	coordinator.startupRuns.set(key, cached);
	return cached;
}

export function requireCompilerLock(
	lease: CompilerLockLease | undefined
): CompilerLockLease {
	if (lease === undefined) {
		throw new Error('Distributed SvelteKit compiler lock is not initialized');
	}
	return lease;
}

export async function releaseCompilerLock(lease: CompilerLockLease): Promise<void> {
	if (lease.released) return;
	lease.released = true;
	const coordinator = lease.coordinator;
	await coordinator.tail.catch(() => undefined);
	coordinator.references -= 1;
	if (coordinator.references > 0) return;
	if (coordinator.references < 0) {
		throw new Error('Distributed SvelteKit compiler lock reference underflow');
	}
	const registry = compilerCoordinatorRegistry();
	if (registry.get(coordinator.path) !== coordinator) return;
	const closing = unlink(coordinator.path)
		.catch((error: unknown) => {
			if (!isMissing(error)) throw error;
		})
		.finally(() => {
			if (registry.get(coordinator.path) === coordinator) {
				registry.delete(coordinator.path);
			}
		});
	coordinator.closing = closing;
	await closing;
}

function processExists(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (
			error !== null &&
			typeof error === 'object' &&
			'code' in error &&
			error.code === 'EPERM'
		);
	}
}

function isAlreadyExists(error: unknown): boolean {
	return (
		error !== null &&
		typeof error === 'object' &&
		'code' in error &&
		error.code === 'EEXIST'
	);
}
