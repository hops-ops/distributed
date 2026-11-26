import type { DistributedReplica, ReplicaWatch } from '../replica/index.js';
import type { GraphqlVariables } from '../types.js';
import type {
	DistributedBoundaryOperation,
	DistributedBoundaryVariableContext
} from './boundary-variables.js';
import { boundaryOperationIdentity } from './operation-identity.js';

const MAX_BOUNDARY_INSTANCES = 4_096;
const MAX_INSTANCE_ID_BYTES = 512;
const MAX_LOCATION_PATHNAME_BYTES = 8_192;
const MAX_LOCATION_SEGMENTS = 256;

export type DistributedSvelteKitBoundaryInstance = Readonly<{
	/** Opaque identity for one mounted SvelteKit page or layout instance. */
	id: string;
	route: string;
	kind: 'layout' | 'page';
}>;

export type DistributedSvelteKitBoundaryLocation = Readonly<{
	/** Opaque identity for one mounted page or layout instance. */
	id: string;
	pathname: string;
	kind: 'layout' | 'page';
}>;

export type DistributedSvelteKitLocationContext<
	TSession = unknown,
	TProps = Readonly<Record<string, unknown>>
> = Omit<DistributedBoundaryVariableContext<TSession, TProps>, 'params'>;

export type SveltekitBoundaryLifecycleDiagnostic = Readonly<{
	action:
		| 'acquire'
		| 'retain'
		| 'release'
		| 'scope-dispose'
		| 'final-unsubscribe';
	boundary: string;
	operation?: string;
	live?: boolean;
	owners: number;
}>;

export type SveltekitBoundaryRetention = Readonly<{
	release(): void;
}>;

type ResolvedBoundary = Readonly<{
	operation: DistributedBoundaryOperation;
	variables: GraphqlVariables;
	identity: string;
	live: boolean;
}>;

type RetainedBoundary = {
	signature: string;
	boundary: string;
	owners: number;
	watches: readonly Readonly<{
		watch: ReplicaWatch<unknown>;
		identity: string;
		operation: string;
		live: boolean;
	}>[];
};

export class DistributedSvelteKitBoundaryController {
	readonly #replica: DistributedReplica;
	readonly #operations: readonly DistributedBoundaryOperation[];
	readonly #diagnostic?: (event: SveltekitBoundaryLifecycleDiagnostic) => void;
	readonly #instances = new Map<string, RetainedBoundary>();
	readonly #identityOwners = new Map<string, number>();
	#destroyed = false;

	constructor(
		replica: DistributedReplica,
		operations: readonly DistributedBoundaryOperation[],
		diagnostic?: (event: SveltekitBoundaryLifecycleDiagnostic) => void
	) {
		this.#replica = replica;
		this.#operations = operations;
		this.#diagnostic = diagnostic;
	}

	retain<TSession, TProps>(
		instance: DistributedSvelteKitBoundaryInstance,
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): SveltekitBoundaryRetention {
		if (this.#destroyed) {
			throw new Error('Distributed SvelteKit boundary controller is destroyed');
		}
		const validated = validateInstance(instance);
		const resolved = this.#resolve(validated, context);
		const signature = JSON.stringify(
			resolved.map(({ identity }) => identity)
		);
		const existing = this.#instances.get(validated.id);
		if (existing !== undefined) {
			if (
				existing.signature !== signature ||
				existing.boundary !== validated.boundary
			) {
				throw new Error(
					'Distributed SvelteKit boundary instance changed ownership while retained'
				);
			}
			existing.owners += 1;
			this.#emit({
				action: 'retain',
				boundary: existing.boundary,
				owners: existing.owners
			});
			return this.#lease(validated.id, existing);
		}
		if (this.#instances.size >= MAX_BOUNDARY_INSTANCES) {
			throw new Error(
				`Distributed SvelteKit cannot retain more than ${MAX_BOUNDARY_INSTANCES} boundary instances`
			);
		}

		const watches: Array<RetainedBoundary['watches'][number]> = [];
		try {
			for (const item of resolved) {
				const watch = this.#replica.watch(
					item.operation.artifact,
					item.variables,
					{ live: item.live }
				);
				watches.push(
					Object.freeze({
						watch,
						identity: item.identity,
						operation: item.operation.plan.operation,
						live: item.live
					})
				);
			}
		} catch (error) {
			for (const { watch } of watches) watch.destroy();
			throw error;
		}

		const retained: RetainedBoundary = {
			signature,
			boundary: validated.boundary,
			owners: 1,
			watches: Object.freeze(watches)
		};
		this.#instances.set(validated.id, retained);
		for (const item of watches) {
			const owners = (this.#identityOwners.get(item.identity) ?? 0) + 1;
			this.#identityOwners.set(item.identity, owners);
			this.#emit({
				action: 'acquire',
				boundary: retained.boundary,
				operation: item.operation,
				live: item.live,
				owners
			});
		}
		return this.#lease(validated.id, retained);
	}

	/** Retain the nearest generated page/layout boundary at a browser location. */
	retainLocation<TSession, TProps>(
		location: DistributedSvelteKitBoundaryLocation,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): SveltekitBoundaryRetention {
		const matched = matchNearestBoundary(
			this.#operations,
			location.pathname,
			location.kind
		);
		if (matched === undefined) {
			return Object.freeze({ release: () => undefined });
		}
		return this.retain(
			{
				id: location.id,
				route: matched.route,
				kind: location.kind
			},
			Object.freeze({ ...context, params: matched.params })
		);
	}

	/** Warm every generated page and owning layout selection for one target URL. */
	async prefetchLocation<TSession, TProps>(
		pathname: string,
		context: DistributedSvelteKitLocationContext<TSession, TProps>
	): Promise<void> {
		if (this.#destroyed) {
			throw new Error('Distributed SvelteKit boundary controller is destroyed');
		}
		const matches = matchLocationBoundaries(this.#operations, pathname);
		const scheduled = new Map<string, ResolvedBoundary>();
		for (const matched of matches) {
			for (const item of this.#resolve(
				{
					id: 'prefetch',
					route: matched.route,
					kind: matched.kind,
					boundary: `${matched.kind}:${matched.route}`
				},
				Object.freeze({ ...context, params: matched.params })
			)) {
				scheduled.set(item.identity, item);
			}
		}
		await Promise.all(
			[...scheduled.values()].map(async (item) => {
				const snapshot = this.#replica.read(
					item.operation.artifact,
					item.variables
				);
				if (snapshot.complete && !snapshot.stale) return;
				const watch = this.#replica.watch(
					item.operation.artifact,
					item.variables,
					{ live: false }
				);
				try {
					await watch.refresh();
				} finally {
					watch.destroy();
				}
			})
		);
	}

	/** Close every old-scope owner while keeping the controller reusable. */
	disposeScope(): void {
		if (this.#destroyed) return;
		this.#disposeInstances(true);
	}

	destroy(): void {
		if (this.#destroyed) return;
		this.#destroyed = true;
		this.#disposeInstances(false);
	}

	#resolve<TSession, TProps>(
		instance: ReturnType<typeof validateInstance>,
		context: DistributedBoundaryVariableContext<TSession, TProps>
	): readonly ResolvedBoundary[] {
		const selected = this.#operations.filter(
			({ plan }) =>
				plan.kind === instance.kind && normalizeRoute(plan.route) === instance.route
		);
		if (selected.length === 0) {
			throw new Error(
				`Distributed SvelteKit boundary plan has no ${instance.boundary} selection`
			);
		}
		return Object.freeze(
			selected.map((operation) => {
				const variables = operation.binding.resolve(
					context as DistributedBoundaryVariableContext<
						unknown,
						Readonly<Record<string, unknown>>
					>
				);
				return Object.freeze({
					operation,
					variables,
					identity: boundaryOperationIdentity(operation.artifact, variables),
					live: operation.artifact.live !== undefined
				});
			})
		);
	}

	#lease(instanceId: string, retained: RetainedBoundary): SveltekitBoundaryRetention {
		let released = false;
		return Object.freeze({
			release: (): void => {
				if (released) return;
				released = true;
				if (this.#instances.get(instanceId) !== retained) return;
				retained.owners -= 1;
				this.#emit({
					action: 'release',
					boundary: retained.boundary,
					owners: retained.owners
				});
				if (retained.owners > 0) return;
				this.#instances.delete(instanceId);
				this.#releaseWatches(retained);
			}
		});
	}

	#disposeInstances(scope: boolean): void {
		for (const [instanceId, retained] of [...this.#instances]) {
			this.#instances.delete(instanceId);
			if (scope) {
				this.#emit({
					action: 'scope-dispose',
					boundary: retained.boundary,
					owners: 0
				});
			}
			this.#releaseWatches(retained);
		}
	}

	#releaseWatches(retained: RetainedBoundary): void {
		for (const item of retained.watches) {
			item.watch.destroy();
			const owners = (this.#identityOwners.get(item.identity) ?? 1) - 1;
			if (owners > 0) {
				this.#identityOwners.set(item.identity, owners);
				continue;
			}
			this.#identityOwners.delete(item.identity);
			if (item.live) {
				this.#emit({
					action: 'final-unsubscribe',
					boundary: retained.boundary,
					operation: item.operation,
					live: true,
					owners: 0
				});
			}
		}
	}

	#emit(event: SveltekitBoundaryLifecycleDiagnostic): void {
		try {
			this.#diagnostic?.(Object.freeze(event));
		} catch {
			// Diagnostics are observational and cannot alter lifecycle ownership.
		}
	}
}

function validateInstance(instance: DistributedSvelteKitBoundaryInstance): Readonly<{
	id: string;
	route: string;
	kind: 'layout' | 'page';
	boundary: string;
}> {
	if (instance === null || typeof instance !== 'object') {
		throw new TypeError('Distributed SvelteKit boundary instance is required');
	}
	const id = typeof instance.id === 'string' ? instance.id.trim() : undefined;
	if (
		typeof id !== 'string' ||
		id.length === 0 ||
		new TextEncoder().encode(id).byteLength > MAX_INSTANCE_ID_BYTES
	) {
		throw new TypeError('Distributed SvelteKit boundary instance id is invalid');
	}
	if (instance.kind !== 'layout' && instance.kind !== 'page') {
		throw new TypeError('Distributed SvelteKit boundary instance kind is invalid');
	}
	const route = normalizeRoute(instance.route);
	return Object.freeze({
		id,
		route,
		kind: instance.kind,
		boundary: `${instance.kind}:${route}`
	});
}

function normalizeRoute(value: string): string {
	if (typeof value !== 'string' || !value.startsWith('/')) {
		throw new TypeError('Distributed SvelteKit boundary route must start with /');
	}
	const normalized = value.length === 1 ? value : value.replace(/\/+$/, '');
	return normalized.length === 0 ? '/' : normalized;
}

type MatchedBoundary = Readonly<{
	route: string;
	kind: 'layout' | 'page';
	params: Readonly<Record<string, string | undefined>>;
	specificity: number;
}>;

function matchNearestBoundary(
	operations: readonly DistributedBoundaryOperation[],
	pathname: string,
	kind: 'layout' | 'page'
): MatchedBoundary | undefined {
	const matches = matchLocationBoundaries(operations, pathname).filter(
		(candidate) => candidate.kind === kind
	);
	if (matches.length === 0) return undefined;
	const mostSpecific = matches[0]!;
	if (
		matches[1] !== undefined &&
		matches[1].specificity === mostSpecific.specificity
	) {
		throw new Error(
			`Distributed SvelteKit boundary plan is ambiguous for this ${kind} location`
		);
	}
	return mostSpecific;
}

function matchLocationBoundaries(
	operations: readonly DistributedBoundaryOperation[],
	pathname: string
): readonly MatchedBoundary[] {
	const routes = new Map<
		string,
		Readonly<{ route: string; kind: 'layout' | 'page' }>
	>();
	for (const { plan } of operations) {
		routes.set(`${plan.kind}\u0000${plan.route}`, {
			route: plan.route,
			kind: plan.kind
		});
	}
	const matches: MatchedBoundary[] = [];
	for (const candidate of routes.values()) {
		const matched = matchRoutePattern(
			candidate.route,
			pathname,
			candidate.kind === 'layout'
		);
		if (matched !== undefined) {
			matches.push(Object.freeze({ ...candidate, ...matched }));
		}
	}
	return Object.freeze(
		matches.sort(
			(left, right) =>
				right.specificity - left.specificity ||
				left.kind.localeCompare(right.kind) ||
				left.route.localeCompare(right.route)
		)
	);
}

function matchRoutePattern(
	pattern: string,
	pathname: string,
	prefix: boolean
): Readonly<{
	params: Readonly<Record<string, string | undefined>>;
	specificity: number;
}> | undefined {
	const route = normalizeRoute(pattern);
	const path = normalizePathname(pathname);
	const routeSegments = route
		.split('/')
		.filter(
			(segment) => segment.length > 0 && !/^\(.+\)$/.test(segment)
		);
	const pathSegments = path.split('/').filter((segment) => segment.length > 0);
	const params: Record<string, string | undefined> = Object.create(null);
	let pathIndex = 0;
	let specificity = 0;
	for (let routeIndex = 0; routeIndex < routeSegments.length; routeIndex += 1) {
		const segment = routeSegments[routeIndex]!;
		// SvelteKit matcher functions are application code. The browser adapter
		// cannot execute or guess them from a generated route pattern.
		if (segment.startsWith('[') && segment.includes('=')) return undefined;
		const rest = /^\[\[?\.\.\.([^\]=]+)(?:=[^\]]+)?\]?\]$/.exec(segment);
		if (rest !== null) {
			const values = pathSegments.slice(pathIndex).map(decodePathSegment);
			if (values.some((value) => value === undefined)) return undefined;
			params[rest[1]!] =
				values.length === 0 ? undefined : (values as string[]).join('/');
			pathIndex = pathSegments.length;
			break;
		}
		const optional = /^\[\[([^\]=]+)(?:=[^\]]+)?\]\]$/.exec(segment);
		if (optional !== null) {
			const remainingRequired = routeSegments
				.slice(routeIndex + 1)
				.filter((part) => !/^\[\[/.test(part)).length;
			if (pathSegments.length - pathIndex > remainingRequired) {
				const value = decodePathSegment(pathSegments[pathIndex++]!);
				if (value === undefined) return undefined;
				params[optional[1]!] = value;
			} else {
				params[optional[1]!] = undefined;
			}
			continue;
		}
		const dynamic = /^\[([^\]=]+)(?:=[^\]]+)?\]$/.exec(segment);
		if (dynamic !== null) {
			if (pathSegments[pathIndex] === undefined) return undefined;
			const value = decodePathSegment(pathSegments[pathIndex++]!);
			if (value === undefined) return undefined;
			params[dynamic[1]!] = value;
			continue;
		}
		if (pathSegments[pathIndex] === undefined) return undefined;
		const actual = decodePathSegment(pathSegments[pathIndex++]!);
		if (actual === undefined || actual !== segment) return undefined;
		specificity += 1;
	}
	if (!prefix && pathIndex !== pathSegments.length) return undefined;
	return Object.freeze({
		params: Object.freeze(params),
		specificity: specificity * 1_000 + routeSegments.length
	});
}

function normalizePathname(value: string): string {
	if (
		typeof value !== 'string' ||
		!value.startsWith('/') ||
		new TextEncoder().encode(value).byteLength > MAX_LOCATION_PATHNAME_BYTES ||
		value.split('/').length - 1 > MAX_LOCATION_SEGMENTS
	) {
		throw new TypeError(
			'Distributed SvelteKit location pathname is invalid or exceeds adapter limits'
		);
	}
	const withoutQuery = value.split(/[?#]/u, 1)[0]!;
	return withoutQuery.length === 1
		? withoutQuery
		: withoutQuery.replace(/\/+$/, '');
}

function decodePathSegment(value: string): string | undefined {
	try {
		return decodeURIComponent(value);
	} catch {
		return undefined;
	}
}
