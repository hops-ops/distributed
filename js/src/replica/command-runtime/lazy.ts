import { matchReplicaTrustedPresetInventory } from '../commands.js';
import { SHA256 } from './constants.js';
import { ReplicaCommandRuntimeError } from './errors.js';
import { defineBoundCommand, freezeCommandTree } from './lib/binding.js';
import {
	commandStatusArtifact,
	commandSurfaceContract,
	normalizeInventory
} from './lib/inventory.js';
import {
	cloneScope,
	cloneSurface,
	linkAbortSignals,
	sameScope,
	sameSurface
} from './lib/util.js';
import { replicaCommandAuthority } from './symbols.js';
import type { ReplicaResultEnvelope } from '../types.js';
import type {
	CommandEntry,
	ReplicaBoundCommands,
	ReplicaCommandAuthorityHost,
	ReplicaCommandCallOptions,
	ReplicaCommandRuntime,
	ReplicaCommandRuntimeOptions,
	ReplicaCommandStatusArtifact,
	ReplicaCommandSurfaceContract,
	ReplicaCommandTransport
} from './types.js';

/** Small compiler-owned inventory; definitions and pure implementations stay in the lazy chunk. */
export type ReplicaLazyCommandCatalog = Readonly<{
	commands: Readonly<
		Record<string, Readonly<{ operationHash: string; hasInput: boolean }>>
	>;
	status: ReplicaCommandStatusArtifact;
}>;

export type ReplicaLazyCommandModule<
	TEntries extends Readonly<Record<string, CommandEntry>>
> = Readonly<{
	entries: TEntries;
	pureFunctions?: ReplicaCommandRuntimeOptions['pureFunctions'];
}>;

export type ReplicaLazyCommandRuntime<
	TEntries extends Readonly<Record<string, CommandEntry>>
> = ReplicaCommandRuntime<TEntries> & Readonly<{ preload(): Promise<void> }>;

/**
 * Register authority synchronously, then load one shared command runtime on demand.
 * Only immutable code may be cached by the loader; runtime state belongs to this client.
 */
export function createLazyReplicaCommandRuntime<
	TEntries extends Readonly<Record<string, CommandEntry>>
>(
	replica: ReplicaCommandAuthorityHost,
	transport: ReplicaCommandTransport,
	catalog: ReplicaLazyCommandCatalog,
	load: () => Promise<ReplicaLazyCommandModule<TEntries>>,
	options: Omit<ReplicaCommandRuntimeOptions, 'status'> = {}
): ReplicaLazyCommandRuntime<TEntries> {
	const protocol = catalog.status.protocol;
	if (protocol.surface === undefined)
		throw new TypeError('lazy commands require a client surface');
	const contract: ReplicaCommandSurfaceContract = Object.freeze({
		protocolVersion: 1,
		schemaHash: protocol.schemaHash,
		protocolHash: protocol.protocolHash,
		surface: cloneSurface(protocol.surface),
		trustedPresets: Object.freeze(
			protocol.trustedPresets.map((value) => Object.freeze({ ...value }))
		)
	});
	const status = commandStatusArtifact(catalog.status, contract);
	if (transport.status === undefined)
		throw new TypeError(
			'generated command status artifact requires transport.status'
		);
	const hashes = Object.freeze(
		Object.fromEntries(
			Object.entries(catalog.commands).map(([key, value]) => [
				key,
				Object.freeze({ ...value })
			])
		)
	);
	const commands = Object.create(null) as Record<string, unknown>;
	if (Object.keys(hashes).length === 0)
		throw new TypeError('lazy command inventory must not be empty');
	for (const [name, descriptor] of Object.entries(hashes)) {
		if (
			typeof descriptor.hasInput !== 'boolean' ||
			!SHA256.test(descriptor.operationHash)
		)
			throw new TypeError('invalid lazy command operation hash');
		defineBoundCommand(
			commands,
			name,
			descriptor.hasInput
				? (input: unknown, callOptions = {}) => invoke(name, input, callOptions)
				: (callOptions = {}) => invoke(name, undefined, callOptions)
		);
	}
	freezeCommandTree(commands);
	const registration = replica[replicaCommandAuthority]?.(contract);
	const lifetime = new AbortController();
	let disposed = false;
	let runtime: ReplicaCommandRuntime<TEntries> | undefined;
	let loading: Promise<void> | undefined;
	let startTail: Promise<void> | undefined;
	// Reserve preparation order before importing. A warm call must not overtake
	// earlier cold calls; release after dispatch starts, not after its receipt.
	const reserveStart = () => {
		const previous = startTail;
		let resolve!: () => void;
		const tail = new Promise<void>((done) => {
			resolve = done;
		});
		startTail = tail;
		return {
			previous,
			release() {
				const complete = () => {
					resolve();
					if (startTail === tail) startTail = undefined;
				};
				if (previous === undefined) complete();
				else void previous.then(complete);
			}
		};
	};
	const readAuthority = () =>
		registration?.read() ?? {
			generation: replica.authorizationGeneration,
			scope: replica.scope,
			trustedPresets: []
		};
	const assertAlive = () => {
		if (disposed)
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_DISPOSED');
	};
	const preload = (): Promise<void> => {
		try {
			assertAlive();
		} catch (error) {
			return Promise.reject(error);
		}
		if (runtime !== undefined) return Promise.resolve();
		if (loading !== undefined) return loading;
		loading = Promise.all([Promise.resolve().then(load), import('./create.js')])
			.then(([module, implementation]) => {
				assertAlive();
				const inventory = normalizeInventory(module.entries);
				if (
					inventory.length !== Object.keys(hashes).length ||
					inventory.some(
						({ key, artifact }) =>
							!Object.hasOwn(hashes, key) ||
							artifact.name !== key ||
							artifact.operationHash !== hashes[key].operationHash ||
							(artifact.input.kind !== 'none') !== hashes[key].hasInput
					)
				)
					throw new TypeError(
						'loaded command inventory does not match its catalog'
					);
				const actual = commandSurfaceContract(
					inventory.map(({ artifact }) => artifact),
					status.protocol.trustedPresets
				);
				if (
					actual.schemaHash !== contract.schemaHash ||
					actual.protocolHash !== contract.protocolHash ||
					!sameSurface(actual.surface, contract.surface) ||
					JSON.stringify(actual.trustedPresets) !==
						JSON.stringify(status.protocol.trustedPresets)
				)
					throw new TypeError(
						'loaded command surface does not match its catalog'
					);
				// The real runtime retains all existing validation, dispatch scheduling,
				// result observation, optimistic layers, status readers and recovery.
				runtime = implementation.createReplicaCommandRuntime(
					replica,
					transport,
					module.entries,
					{
						...options,
						pureFunctions: {
							...module.pureFunctions,
							...options.pureFunctions
						},
						status
					}
				);
			})
			.finally(() => {
				loading = undefined;
			});
		return loading;
	};

	async function invoke(
		name: string,
		input: unknown,
		callOptions: ReplicaCommandCallOptions<unknown>
	) {
		assertAlive();
		try {
			options.lifecycle?.assertDispatchOpen();
		} catch (cause) {
			throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_RELOADING', {
				cause
			});
		}
		const captured = readAuthority();
		if (
			captured.scope === undefined ||
			captured.scope.schemaHash !== contract.schemaHash ||
			captured.scope.protocolVersion !== contract.protocolVersion
		) {
			throw new ReplicaCommandRuntimeError(
				'REPLICA_COMMAND_AUTHORITY_UNAVAILABLE'
			);
		}
		matchReplicaTrustedPresetInventory(
			contract.trustedPresets,
			captured.trustedPresets
		);
		const scope = cloneScope(captured.scope);
		const current = () => {
			assertAlive();
			const next = readAuthority();
			if (
				captured.signal?.aborted ||
				next.generation !== captured.generation ||
				next.scope === undefined ||
				!sameScope(next.scope, scope)
			) {
				throw new ReplicaCommandRuntimeError(
					'REPLICA_COMMAND_SCOPE_INVALIDATED'
				);
			}
		};
		const signals = linkAbortSignals([
			captured.signal,
			callOptions.signal,
			lifetime.signal
		]);
		const reservation = reserveStart();
		try {
			current();
			if (signals.signal?.aborted)
				throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_ABORTED');
			// Snapshot before the new asynchronous boundary, just as eager preparation
			// consumes the caller's input before its first await.
			const savedInput =
				runtime === undefined || reservation.previous !== undefined
					? structuredClone(input)
					: input;
			const savedOptions = {
				...callOptions,
				...(callOptions.generators === undefined
					? {}
					: { generators: { ...callOptions.generators } })
			};
			if (runtime === undefined) await waitForLoad(preload(), signals.signal);
			if (reservation.previous !== undefined)
				await waitForLoad(reservation.previous, signals.signal);
			current();
			if (signals.signal?.aborted)
				throw new ReplicaCommandRuntimeError('REPLICA_COMMAND_ABORTED');
			let command: unknown = runtime!.commands;
			for (const part of name.split('.'))
				command = (command as Record<string, unknown>)[part];
			return hashes[name].hasInput
				? (
						command as (
							input: unknown,
							options: ReplicaCommandCallOptions<unknown>
						) => unknown
					)(savedInput, savedOptions)
				: (command as (options: ReplicaCommandCallOptions<unknown>) => unknown)(
						savedOptions
					);
		} catch (error) {
			current();
			throw error;
		} finally {
			reservation.release();
			signals.dispose();
		}
	}

	return Object.freeze({
		commands: commands as ReplicaBoundCommands<TEntries>,
		preload,
		observeResult: (envelope: ReplicaResultEnvelope<unknown>) =>
			runtime?.observeResult(envelope),
		pendingCommandIds: () => runtime?.pendingCommandIds() ?? Object.freeze([]),
		dispose() {
			if (disposed) return;
			disposed = true;
			lifetime.abort();
			try {
				runtime?.dispose();
			} finally {
				registration?.dispose();
			}
		}
	});
}

/** A cancelled caller releases its wait immediately; other callers still share the import. */
function waitForLoad(
	load: Promise<void>,
	signal: AbortSignal | undefined
): Promise<void> {
	if (signal === undefined) return load;
	return new Promise((resolve, reject) => {
		const abort = () =>
			reject(new ReplicaCommandRuntimeError('REPLICA_COMMAND_ABORTED'));
		if (signal.aborted) abort();
		else signal.addEventListener('abort', abort, { once: true });
		load
			.then(resolve, reject)
			.finally(() => signal.removeEventListener('abort', abort));
	});
}
