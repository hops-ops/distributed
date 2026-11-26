import { getContext, setContext } from 'svelte';

import type {
	ReplicaLiveConnection,
	ReplicaOperationArtifact,
	ReplicaSnapshot,
	ReplicaWindowOperationArtifact
} from '../replica/index.js';
import type { GraphqlVariables } from '../types.js';
import {
	defineDistributedBoundaryBinding,
	defineDistributedBoundaryOperation,
	type DistributedBoundaryOperation,
	type DistributedBoundaryPlan,
	type DistributedBoundaryVariableSources
} from './boundary-variables.js';
import type {
	DistributedSvelteKitBoundaryInstance,
	SveltekitBoundaryRetention
} from './boundary-lifecycle.js';
import type {
	DistributedSvelteKitClient,
	SveltekitBoundOperation,
	SveltekitWindowOperation
} from './replica.js';

const DISTRIBUTED_CLIENT_CONTEXT = Symbol(
	'@hops-ops/distributed/sveltekit/client'
);

type AnyDistributedSvelteKitClient = DistributedSvelteKitClient<unknown>;

/**
 * Install one client in the current Svelte component tree.
 *
 * The context key is module-global, but the client value is not: Svelte owns
 * it per component tree/request. A nested elevated layout may deliberately
 * replace the nearest user-safe client.
 */
export function provideDistributedSvelteKitClient<TCommands>(
	client: DistributedSvelteKitClient<TCommands>
): DistributedSvelteKitClient<TCommands> {
	if (
		client === null ||
		typeof client !== 'object' ||
		typeof client.operation !== 'function'
	) {
		throw new TypeError(
			'provideDistributedSvelteKitClient requires a Distributed SvelteKit client'
		);
	}
	setContext(
		DISTRIBUTED_CLIENT_CONTEXT,
		client as AnyDistributedSvelteKitClient
	);
	return client;
}

/**
 * Resolve the nearest client during component initialization.
 *
 * This intentionally calls Svelte `getContext` each time. Generated modules
 * may contain static operation wrappers, but never capture a client or command
 * tree at module evaluation time.
 */
export function useDistributedSvelteKitClient<
	TCommands = Readonly<Record<never, never>>
>(): DistributedSvelteKitClient<TCommands> {
	const client = getContext<AnyDistributedSvelteKitClient | undefined>(
		DISTRIBUTED_CLIENT_CONTEXT
	);
	if (client === undefined) {
		throw new Error(
			'Distributed client is missing from the active Svelte component tree; call generated provideDistributed(...) in a parent layout'
		);
	}
	return client as DistributedSvelteKitClient<TCommands>;
}

/**
 * The nearest client's live connection as a Svelte store: `reconnecting`
 * while any watched live operation is reopening a lost stream. Render one
 * notice from it; queries keep their data and report no error meanwhile.
 */
export function useDistributedLiveConnection(): Readonly<{
	subscribe(run: (connection: ReplicaLiveConnection) => void): () => void;
}> {
	const replica = useDistributedSvelteKitClient().replica;
	return Object.freeze({
		subscribe(run: (connection: ReplicaLiveConnection) => void): () => void {
			run(replica.liveConnection);
			return replica.subscribeLiveConnection(run);
		}
	});
}

/** Resolve the nearest generated command surface without a global proxy. */
export function useDistributedSvelteKitCommands<TCommands>(): TCommands {
	return useDistributedSvelteKitClient<TCommands>().commands;
}

/** Retain the generated selections owned by the nearest page/layout instance. */
export function retainDistributedSvelteKitBoundary<TSession, TProps>(
	instance: DistributedSvelteKitBoundaryInstance,
	context: import('./boundary-variables.js').DistributedBoundaryVariableContext<
		TSession,
		TProps
	>
): SveltekitBoundaryRetention {
	return useDistributedSvelteKitClient<unknown>().retainBoundary(instance, context);
}

/**
 * Define one SSR-safe generated operation wrapper.
 *
 * The wrapper stores only immutable compiler output. `use()` and `read()`
 * resolve the nearest tree-local client when the component calls them.
 */
export function defineDistributedSvelteKitOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaWindowOperationArtifact<TData, TVariables>
): SveltekitWindowOperation<TData, TVariables>;
export function defineDistributedSvelteKitOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>
): SveltekitBoundOperation<TData, TVariables>;
export function defineDistributedSvelteKitOperation<
	TData,
	TVariables extends GraphqlVariables
>(
	artifact: ReplicaOperationArtifact<TData, TVariables>
): SveltekitBoundOperation<TData, TVariables> {
	const use = ((...args: unknown[]) => {
		const operation =
			useDistributedSvelteKitClient<unknown>().operation(artifact);
		const invoke = operation.use as (
			...values: unknown[]
		) => ReturnType<SveltekitBoundOperation<TData, TVariables>['use']>;
		return invoke(...args);
	}) as SveltekitBoundOperation<TData, TVariables>['use'];

	const useWindow =
		artifact.window === undefined
			? undefined
			: (...args: unknown[]) => {
					const operation = useDistributedSvelteKitClient<unknown>().operation(
						artifact as ReplicaWindowOperationArtifact<TData, TVariables>
					);
					return (operation.useWindow as (...values: unknown[]) => unknown)(...args);
				};

	return Object.freeze({
		artifact,
		use,
		...(useWindow === undefined ? {} : { useWindow }),
		read(variables: TVariables): ReplicaSnapshot<TData> {
			return useDistributedSvelteKitClient<unknown>()
				.operation(artifact)
				.read(variables);
		},
		prefetch(variables: TVariables): Promise<void> {
			return useDistributedSvelteKitClient<unknown>()
				.operation(artifact)
				.prefetch(variables);
		},
		boundary<
			TSession = unknown,
			TProps = Readonly<Record<string, unknown>>
		>(
			plan: DistributedBoundaryPlan,
			sources: DistributedBoundaryVariableSources<TVariables>
		): DistributedBoundaryOperation<TData, TVariables, TSession, TProps> {
			return defineDistributedBoundaryOperation(
				plan,
				artifact,
				defineDistributedBoundaryBinding<
					TData,
					TVariables,
					TSession,
					TProps
				>(artifact, sources)
			);
		}
	});
}
