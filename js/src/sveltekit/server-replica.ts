import {
	createDistributedReplica,
	createReplicaGraphqlTransport,
	type ReplicaDehydratedState,
	type ReplicaOperationArtifact,
	type ReplicaWatch
} from '../replica/index.js';
import type { FetchLike } from '../request.js';
import type { GqlAuth, GraphqlVariables } from '../types.js';
import { authFromPageData, type PageGraphqlData } from './auth.js';
import {
	DISTRIBUTED_BOUNDARY_BINDING_VERSION,
	resolveDistributedBoundaryVariables,
	type DistributedBoundaryOperation,
	type DistributedBoundaryVariableContext
} from './boundary-variables.js';
import { boundaryOperationIdentity } from './operation-identity.js';
import type {
	SveltekitDistributedPageData,
	SveltekitReplicaAuthority,
	SveltekitReplicaHydration
} from './replica.js';

export type SveltekitServerLoadEventLike<TLocals = unknown> = Readonly<{
	locals: TLocals;
	route?: Readonly<{ id?: string | null }>;
	url?: URL;
	params?: Readonly<Record<string, string | undefined>>;
	parent?(): Promise<Readonly<Record<string, unknown>>>;
	request?: Readonly<{ signal: AbortSignal }>;
	fetch?: FetchLike;
	/**
	 * SvelteKit client-side navigation and hover preload (`__data.json`).
	 * Document SSR is `false`/`undefined`; those navigations must not wait on
	 * a fresh GraphQL replica — the browser replica already owns the cache.
	 */
	isDataRequest?: boolean;
}>;

export type CreateDistributedSvelteKitServerOptions<
	TSession extends NonNullable<PageGraphqlData['session']>,
	TEvent extends SveltekitServerLoadEventLike = SveltekitServerLoadEventLike
> = Readonly<{
	/** One executable binding per promoted page/layout island. */
	boundaries: readonly DistributedBoundaryOperation<
		unknown,
		GraphqlVariables,
		TSession,
		Readonly<Record<string, unknown>>
	>[];
	getSession(event: TEvent): Promise<TSession | null>;
	getRole(
		session: TSession | null,
		event: TEvent
	): string | null | undefined;
	getAuth?(pageData: PageGraphqlData, event: TEvent): GqlAuth;
	/** Private API origin or same-origin `/graphql`; defaults to `/graphql`. */
	getUrl?(event: TEvent): string;
	/** Maximum simultaneous SSR island refreshes. Defaults to 8, maximum 32. */
	maxConcurrency?: number;
}>;

export type DistributedSvelteKitServer<TEvent> = Readonly<{
	load(event: TEvent): Promise<SveltekitDistributedPageData & {
		gqlError: string | null;
	}>;
}>;

/**
 * One app-level root layout loader for every compiler-discovered `@load`
 * operation. Each invocation creates a fresh replica and never shares SSR data
 * across requests.
 */
export function createDistributedSvelteKitServer<
	TSession extends NonNullable<PageGraphqlData['session']>,
	TEvent extends SveltekitServerLoadEventLike = SveltekitServerLoadEventLike
>(
	options: CreateDistributedSvelteKitServerOptions<TSession, TEvent>
): DistributedSvelteKitServer<TEvent> {
	const boundaries = validateBoundaryOperations(options.boundaries);
	const maxConcurrency = validateConcurrency(options.maxConcurrency ?? 8);
	return Object.freeze({
		async load(event: TEvent) {
			const session = await options.getSession(event);
			const accessToken = session?.accessToken ?? null;
			const engineRole = options.getRole(session, event) ?? null;
			const pageData: PageGraphqlData = {
				session,
				accessToken,
				engineRole
			};
			// Data requests may rotate the cookie again after an auth refresh, or
			// observe a credential renewed by a concurrent request. A credential
			// without fresh server authority cannot preserve the browser's scope.
			// Resolve the same bounded route selections for document and data loads;
			// warm same-scope hydration merges without replacing newer command state.
			const auth =
				options.getAuth?.(pageData, event) ?? authFromPageData(pageData);
			const routeId = routeIdentity(event);
			const selectedBoundaries = boundaries
				.filter(({ plan }) =>
					plan.kind === 'page'
						? plan.route === routeId
						: layoutOwnsRoute(plan.route, routeId)
				)
				.sort(compareBoundaryOperations);
			if (selectedBoundaries.length === 0) {
				return {
					...pageData,
					gqlError: null
				};
			}

			const requestSignal = event.request?.signal;
			const fetchImpl = event.fetch;
			const requestFetch: FetchLike | undefined =
				fetchImpl === undefined || requestSignal === undefined
					? fetchImpl
					: ((input, init) => {
							const transportSignal = init?.signal;
							const signal =
								transportSignal === undefined || transportSignal === null
									? requestSignal
									: AbortSignal.any([requestSignal, transportSignal]);
							return fetchImpl(input, { ...init, signal });
						});
			const transport = createReplicaGraphqlTransport({
				getUrl: () => options.getUrl?.(event) ?? '/graphql',
				getAuth: () => auth,
				...(requestFetch === undefined ? {} : { fetch: requestFetch })
			});
			const replica = createDistributedReplica({ transport });
			type Scheduled = Readonly<{
				identity: string;
				operation: string;
				artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>;
				variables: GraphqlVariables;
			}>;
			type Execution = Scheduled & Readonly<{
				watch: ReplicaWatch<unknown>;
				failure: string | null;
			}>;
			const activeWatches = new Set<ReplicaWatch<unknown>>();
			const executions: Execution[] = [];
			let aborted = requestSignal?.aborted ?? false;
			const abort = (): void => {
				aborted = true;
				for (const watch of activeWatches) watch.destroy();
			};
			requestSignal?.addEventListener('abort', abort, { once: true });
			try {
				const needsParent = selectedBoundaries.some(({ binding }) =>
					Object.values(binding.sources).some(
						(source) => source?.kind === 'forwarded_prop'
					)
				);
				const props =
					needsParent && event.parent !== undefined
						? await settleWithRequestAbort(() => event.parent!(), requestSignal)
						: Object.freeze({});
				if (aborted) throw requestAborted();
				const boundaryContext: DistributedBoundaryVariableContext<
					TSession,
					Readonly<Record<string, unknown>>
				> = Object.freeze({
					params: event.params ?? Object.freeze({}),
					search: event.url?.searchParams ?? new URLSearchParams(),
					session,
					props
				});
				const scheduled = new Map<string, Scheduled>();
				for (const binding of selectedBoundaries) {
					if (aborted) throw requestAborted();
					const variables = resolveDistributedBoundaryVariables(
						binding.artifact,
						binding.binding.sources,
						boundaryContext
					);
					const identity = boundaryOperationIdentity(binding.artifact, variables);
					if (scheduled.has(identity)) continue;
					scheduled.set(identity, {
						identity,
						operation: binding.plan.operation,
						artifact: binding.artifact,
						variables
					});
				}
				const completed = await mapBounded(
					[...scheduled.values()],
					maxConcurrency,
					async (item): Promise<Execution> => {
						if (aborted) throw requestAborted();
						const watch = replica.watch(item.artifact, item.variables, { live: false });
						activeWatches.add(watch);
						let failure: string | null = null;
						try {
							await watch.refresh();
						} catch {
							failure = 'Distributed GraphQL island refresh failed';
						}
						if (watch.get().errors.length > 0) {
							failure = 'Distributed GraphQL island refresh failed';
						}
						return Object.freeze({ ...item, watch, failure });
					}
				);
				executions.push(...completed);
				if (aborted) throw requestAborted();
				const errors = executions.flatMap(({ failure }) =>
					failure === null ? [] : [failure]
				);
				for (const { artifact, variables, watch } of executions) {
					// Preserve exact rendered-operation reachability after the
					// temporary watch is released.
					replica.read(artifact, variables);
					watch.destroy();
					activeWatches.delete(watch);
				}
				const transfer =
					replica.scope === undefined
						? undefined
						: hydrationTransfer(
								replica.dehydrate(),
								selectedBoundaries.map(({ plan }) => plan.operation),
								selectedBoundaries.map(({ binding }) => binding.id)
							);
				return {
					...pageData,
					...(transfer === undefined
						? {}
						: {
								distributed: transfer.hydration,
								distributedAuthority: transfer.authority
							}),
					gqlError:
						errors[0] ??
						(transfer === undefined
							? 'Distributed GraphQL response did not establish an authoritative cache scope'
							: null)
				};
			} finally {
				requestSignal?.removeEventListener('abort', abort);
				for (const watch of activeWatches) watch.destroy();
				activeWatches.clear();
			}
		}
	});
}

function hydrationTransfer(
	state: ReplicaDehydratedState,
	operations: readonly string[],
	bindings: readonly string[] = []
): Readonly<{
	hydration: SveltekitReplicaHydration;
	authority: SveltekitReplicaAuthority;
}> {
	return Object.freeze({
		hydration: Object.freeze({
			version: 1,
			state,
			operations: Object.freeze([...operations]),
			...(bindings.length === 0
				? {}
				: { bindings: Object.freeze([...new Set(bindings)].sort()) })
		}),
		authority: Object.freeze({
			version: 1,
			scope: state.scope
		})
	});
}

function validateBoundaryOperations<TSession>(
	value: readonly DistributedBoundaryOperation<
		unknown,
		GraphqlVariables,
		TSession,
		Readonly<Record<string, unknown>>
	>[]
): readonly DistributedBoundaryOperation<
	unknown,
	GraphqlVariables,
	TSession,
	Readonly<Record<string, unknown>>
>[] {
	if (!Array.isArray(value)) {
		throw new TypeError('createDistributedSvelteKitServer boundaries must be an array');
	}
	const identities = new Set<string>();
	return Object.freeze(
		value.map((operation, index) => {
			if (
				operation === null ||
				typeof operation !== 'object' ||
				operation.binding?.version !== DISTRIBUTED_BOUNDARY_BINDING_VERSION ||
				operation.binding.artifactId !== operation.artifact?.id
			) {
				throw new TypeError(`invalid Distributed boundary operation at index ${index}`);
			}
			const route = normalizeRoute(operation.plan.route);
			const identity = `${operation.plan.kind}\u0000${route}\u0000${operation.plan.operation}`;
			if (identities.has(identity)) {
				throw new TypeError(
					`duplicate Distributed boundary operation ${operation.plan.operation} at ${route}`
				);
			}
			identities.add(identity);
			return Object.freeze({
				...operation,
				plan: Object.freeze({ ...operation.plan, route })
			});
		})
	);
}

function compareBoundaryOperations(
	left: DistributedBoundaryOperation,
	right: DistributedBoundaryOperation
): number {
	const leftDepth = left.plan.route.split('/').filter(Boolean).length;
	const rightDepth = right.plan.route.split('/').filter(Boolean).length;
	return (
		leftDepth - rightDepth ||
		left.plan.kind.localeCompare(right.plan.kind) ||
		left.plan.route.localeCompare(right.plan.route) ||
		left.plan.operation.localeCompare(right.plan.operation) ||
		left.binding.id.localeCompare(right.binding.id)
	);
}

function layoutOwnsRoute(layout: string, route: string): boolean {
	const owner = normalizeRoute(layout);
	const selected = normalizeRoute(route);
	return owner === '/' || selected === owner || selected.startsWith(`${owner}/`);
}

function validateConcurrency(value: number): number {
	if (!Number.isSafeInteger(value) || value < 1 || value > 32) {
		throw new TypeError('Distributed SvelteKit maxConcurrency must be an integer from 1 through 32');
	}
	return value;
}

async function mapBounded<TInput, TResult>(
	values: readonly TInput[],
	concurrency: number,
	map: (value: TInput, index: number) => Promise<TResult>
): Promise<TResult[]> {
	const results = new Array<TResult>(values.length);
	let next = 0;
	const worker = async (): Promise<void> => {
		while (next < values.length) {
			const index = next;
			next += 1;
			results[index] = await map(values[index]!, index);
		}
	};
	await Promise.all(
		Array.from({ length: Math.min(concurrency, values.length) }, () => worker())
	);
	return results;
}

function requestAborted(): Error {
	const error = new Error('Distributed SvelteKit request was aborted');
	error.name = 'AbortError';
	return error;
}

async function settleWithRequestAbort<T>(
	start: () => Promise<T>,
	signal: AbortSignal | undefined
): Promise<T> {
	if (signal === undefined) return await start();
	if (signal.aborted) throw requestAborted();
	let rejectAbort: ((reason: Error) => void) | undefined;
	const aborted = new Promise<never>((_resolve, reject) => {
		rejectAbort = reject;
	});
	const abort = (): void => rejectAbort?.(requestAborted());
	signal.addEventListener('abort', abort, { once: true });
	try {
		if (signal.aborted) throw requestAborted();
		return await Promise.race([start(), aborted]);
	} finally {
		signal.removeEventListener('abort', abort);
		rejectAbort = undefined;
	}
}

function routeIdentity(event: SveltekitServerLoadEventLike): string {
	const route = event.route?.id;
	if (typeof route === 'string' && route.length > 0) return normalizeRoute(route);
	if (event.url !== undefined) return normalizeRoute(event.url.pathname);
	throw new Error(
		'Distributed SvelteKit route loading requires event.route.id or event.url.pathname'
	);
}

function normalizeRoute(value: string): string {
	const route = nonEmpty(value, 'route');
	if (!route.startsWith('/')) {
		throw new TypeError('Distributed route must start with /');
	}
	if (route.length === 1) return route;
	return route.replace(/\/+$/, '');
}

function nonEmpty(value: unknown, label: string): string {
	if (typeof value !== 'string' || value.trim().length === 0) {
		throw new TypeError(`Distributed ${label} must be a non-empty string`);
	}
	return value.trim();
}
