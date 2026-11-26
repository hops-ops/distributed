/**
 * Running the `distributed` compiler: the command to invoke, bounded child
 * processes without a shell, cancellation, and the path checks every
 * compiler-owned output passes.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, lstatSync, realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';

import { isWithin } from '../lib/node-fs.js';
import type { CompilerCommand, CompilerManifestSource } from './types.js';

const MAX_COMMAND_OUTPUT_BYTES = 16 * 1024 * 1024;
const LIFECYCLE_CLI_EXECUTABLE_ENV = 'DISTRIBUTED_LIFECYCLE_CLI_EXECUTABLE';


/**
 * Resolve the compiler invocation. Inside the `distributed` lifecycle it is
 * always the trusted initiating executable, never the configured command.
 */
export function resolveCompilerCommand(
	cwd: string,
	options: Readonly<{ command?: string; commandArgs?: readonly string[] }>
): CompilerCommand {
	const lifecycleExecutable = process.env[LIFECYCLE_CLI_EXECUTABLE_ENV];
	const lifecycleRoot = process.env.DISTRIBUTED_LIFECYCLE_ROOT;
	const lifecycleStage = process.env.DISTRIBUTED_LIFECYCLE_STAGE;
	const hasLifecycleStage = lifecycleRoot !== undefined || lifecycleStage !== undefined;
	const hasLifecycleOwnership =
		process.env.DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE === '1';
	const lifecycleContext = hasLifecycleStage || hasLifecycleOwnership;
	const validLifecyclePath = (value: string | undefined): value is string =>
		value !== undefined &&
		value.length > 0 &&
		value === value.trim() &&
		isAbsolute(value) &&
		!/[\u0000-\u001f\u007f]/.test(value);
	let command: string;
	if (lifecycleContext) {
		if (!validLifecyclePath(lifecycleExecutable)) {
			throw new TypeError(
				'Distributed lifecycle requires an absolute initiating executable identity'
			);
		}
		if (
			hasLifecycleStage &&
			(!validLifecyclePath(lifecycleRoot) || !validLifecyclePath(lifecycleStage))
		) {
			throw new TypeError(
				'Distributed lifecycle requires absolute root and stage identities'
			);
		}
		command = lifecycleExecutable;
	} else {
		command = options.command ?? 'distributed';
	}
	command = command.trim();
	if (command.length === 0) {
		throw new TypeError('Distributed SvelteKit command must not be empty');
	}
	const commandArgs = (options.commandArgs ?? []).map((argument, index) => {
		if (typeof argument !== 'string') {
			throw new TypeError(
				`Distributed commandArgs[${index}] must be a string`
			);
		}
		return argument;
	});
	return Object.freeze({
		cwd,
		command,
		// A lifecycle-owned compile must be an invocation of the trusted
		// initiating executable itself. A configured standalone prefix (most
		// commonly `cargo run ... --`) belongs to the app's direct Vite mode;
		// carrying it across would make the initiating binary parse launcher
		// arguments as its own command and can select a different binary.
		commandArgs: Object.freeze(lifecycleContext ? [] : commandArgs)
	});
}

/**
 * Run one compiler pass outside Vite: SIGINT/SIGTERM cancel it, and no child
 * outlives it.
 */
export async function runCancellable<T>(
	run: (children: Set<ChildProcess>, signal: AbortSignal) => Promise<T>
): Promise<T> {
	const children = new Set<ChildProcess>();
	const cancellation = new AbortController();
	const cancel = (): void => {
		cancellation.abort();
		for (const child of children) killChild(child, 'SIGTERM');
	};
	process.once('SIGINT', cancel);
	process.once('SIGTERM', cancel);
	try {
		return await run(children, cancellation.signal);
	} finally {
		process.removeListener('SIGINT', cancel);
		process.removeListener('SIGTERM', cancel);
		for (const child of children) killChild(child, 'SIGKILL');
	}
}

/**
 * Stop a plugin's compiler: abort, terminate its children (killed after
 * 1.5 s), and wait up to 3 s for its in-flight runs to settle.
 */
export async function stopCompiler(
	cancellation: AbortController,
	children: ReadonlySet<ChildProcess>,
	runs: readonly (Promise<unknown> | undefined)[]
): Promise<void> {
	cancellation.abort();
	for (const child of children) killChild(child, 'SIGTERM');
	const force = setTimeout(() => {
		for (const child of children) killChild(child, 'SIGKILL');
	}, 1_500);
	force.unref();
	await Promise.race([
		Promise.all(runs.map((run) => run?.catch(() => undefined))),
		new Promise<void>((resolvePromise) => {
			const bounded = setTimeout(resolvePromise, 3_000);
			bounded.unref();
		})
	]);
	clearTimeout(force);
}

export function nonempty(value: unknown, label: string): string {
	if (typeof value !== 'string' || value.trim().length === 0) {
		throw new TypeError(`${label} must be a non-empty string`);
	}
	return value;
}

export function containedPath(root: string, value: string, label: string): string {
	const target = resolve(root, nonempty(value, label));
	if (!isWithin(root, target)) {
		throw new TypeError(`${label} must stay within project root ${root}`);
	}
	return target;
}

export function manifestWatchRoots(
	cwd: string,
	manifest: CompilerManifestSource
): string[] {
	const manifestPath =
		typeof manifest === 'string'
			? resolve(cwd, manifest)
			: (() => {
					const index = manifest.args.indexOf('--manifest-path');
					return index === -1 || manifest.args[index + 1] === undefined
						? undefined
						: resolve(cwd, manifest.args[index + 1]);
				})();
	if (manifestPath === undefined) return [];
	const root = dirname(manifestPath);
	return [manifestPath, join(root, 'src'), join(root, 'crates')]
		.filter((candidate) => existsSync(candidate))
		.sort();
}

export async function runCommand(
	integration: CompilerCommand,
	args: readonly string[],
	children: Set<ChildProcess>,
	signal: AbortSignal
): Promise<{ stdout: string; stderr: string }> {
	throwIfAborted(signal);
	const argv = [...integration.commandArgs, ...args];
	return await new Promise((resolvePromise, rejectPromise) => {
		const child = spawn(integration.command, argv, {
			cwd: integration.cwd,
			env: process.env,
			shell: false,
			detached: process.platform !== 'win32',
			stdio: ['ignore', 'pipe', 'pipe']
		});
		children.add(child);
		const stdout: Buffer[] = [];
		const stderr: Buffer[] = [];
		let bytes = 0;
		let overflow = false;
		let forceKill: ReturnType<typeof setTimeout> | undefined;
		const cancel = (): void => {
			killChild(child, 'SIGTERM');
			forceKill ??= setTimeout(() => killChild(child, 'SIGKILL'), 1_500);
			forceKill.unref();
		};
		signal.addEventListener('abort', cancel, { once: true });
		const collect = (target: Buffer[]) => (chunk: Buffer | string): void => {
			if (overflow) return;
			const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
			bytes += value.length;
			if (bytes > MAX_COMMAND_OUTPUT_BYTES) {
				overflow = true;
				cancel();
				return;
			}
			target.push(value);
		};
		child.stdout?.on('data', collect(stdout));
		child.stderr?.on('data', collect(stderr));
		child.once('error', (error) => {
			children.delete(child);
			signal.removeEventListener('abort', cancel);
			if (forceKill !== undefined) clearTimeout(forceKill);
			rejectPromise(
				commandFailure(integration, argv, '', error.message)
			);
		});
		child.once('close', (code, signal) => {
			children.delete(child);
			cancellationCleanup();
			const out = Buffer.concat(stdout).toString('utf8');
			const err = Buffer.concat(stderr).toString('utf8');
			if (overflow) {
				rejectPromise(
					commandFailure(
						integration,
						argv,
						err,
						`output exceeded ${MAX_COMMAND_OUTPUT_BYTES} bytes`
					)
				);
			} else if (code !== 0) {
				rejectPromise(
					commandFailure(
						integration,
						argv,
						err,
						`exit ${code ?? 'unknown'}${signal === null ? '' : ` (${signal})`}`
					)
				);
			} else {
				resolvePromise({ stdout: out, stderr: err });
			}
		});

		function cancellationCleanup(): void {
			signal.removeEventListener('abort', cancel);
			if (forceKill !== undefined) clearTimeout(forceKill);
		}
	});
}

function commandFailure(
	integration: CompilerCommand,
	argv: readonly string[],
	stderr: string,
	summary: string
): Error {
	const renderedArgv = JSON.stringify([integration.command, ...argv]);
	const details = [
		`Distributed compiler command failed: ${summary}`,
		`cwd: ${integration.cwd}`,
		`argv: ${renderedArgv}`,
		stderr.trim().length === 0 ? undefined : `stderr:\n${stderr.trim()}`
	].filter((value): value is string => value !== undefined);
	return new Error(details.join('\n'));
}

export function plannedPhysicalPath(
	canonicalRoot: string,
	lexicalRoot: string,
	target: string
): string {
	const rel = relative(lexicalRoot, target);
	let lexical = lexicalRoot;
	if (rel !== '') {
		for (const component of rel.split(sep)) {
			lexical = join(lexical, component);
			if (!existsSync(lexical)) break;
			const metadata = lstatSync(lexical);
			if (metadata.isSymbolicLink()) {
				throw new Error(
					`Distributed path ${target} contains symlink component ${lexical}`
				);
			}
		}
	}

	let existing = target;
	const suffix: string[] = [];
	while (!existsSync(existing)) {
		const parent = dirname(existing);
		if (parent === existing) {
			throw new Error(
				`Distributed path ${target} has no existing parent within ${canonicalRoot}`
			);
		}
		suffix.unshift(basename(existing));
		existing = parent;
	}
	const canonicalExisting = realpathSync(existing);
	if (!isWithin(canonicalRoot, canonicalExisting)) {
		throw new Error(
			`Distributed path ${target} resolves outside project root ${canonicalRoot}`
		);
	}
	const planned = resolve(canonicalExisting, ...suffix);
	if (!isWithin(canonicalRoot, planned)) {
		throw new Error(
			`Distributed path ${target} resolves outside project root ${canonicalRoot}`
		);
	}
	return planned;
}

export function physicalPathKey(path: string): string {
	return process.platform === 'win32' || process.platform === 'darwin'
		? path.toLocaleLowerCase('en-US')
		: path;
}

export async function validateNearestExistingParent(
	root: string,
	target: string
): Promise<void> {
	const canonicalRoot = realpathSync(root);
	plannedPhysicalPath(canonicalRoot, root, target);
}

export function killChild(
	child: ChildProcess,
	signal: NodeJS.Signals
): void {
	if (child.pid === undefined) return;
	if (process.platform !== 'win32') {
		try {
			process.kill(-child.pid, signal);
			return;
		} catch {
			// The child may have exited or failed to become its own process group.
		}
	}
	child.kill(signal);
}

export function throwIfAborted(signal: AbortSignal): void {
	if (!signal.aborted) return;
	const error = new Error('Distributed SvelteKit compiler was cancelled');
	error.name = 'AbortError';
	throw error;
}

export function portablePath(path: string): string {
	return path.replaceAll('\\', '/');
}

export function viteError(error: unknown): Readonly<{
	message: string;
	stack?: string;
}> {
	if (error instanceof Error) {
		return {
			message: error.message,
			...(error.stack === undefined ? {} : { stack: error.stack })
		};
	}
	return { message: String(error) };
}
