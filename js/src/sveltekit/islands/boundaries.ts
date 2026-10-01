import { createHash } from 'node:crypto';
import { lstat, readFile, readdir, realpath } from 'node:fs/promises';
import {
	dirname,
	extname,
	isAbsolute,
	join,
	posix,
	relative,
	resolve,
	sep
} from 'node:path';
import { pathToFileURL } from 'node:url';

import { parse } from 'svelte/compiler';

import {
	DISTRIBUTED_BOUNDARY_BINDING_VERSION,
	type DistributedBoundaryVariableSource
} from '../boundary-variables.js';
import { normalizeSearchParamSource } from '../search-variables.js';
import { isGraphqlIslandBindings } from '../island-bindings.js';

const BOUNDARY_PLAN_VERSION = 2;
const VARIABLE_CODEC_VERSION = 2;
const MAX_COMPONENTS = 4_096;
const MAX_ISLANDS = 4_096;
const MAX_COMPONENT_BYTES = 2 * 1024 * 1024;
const MAX_BINDINGS_BYTES = 256 * 1024;
const MAX_GRAPH_EDGES = 32_768;

type AstNode = Readonly<Record<string, unknown>> & {
	type?: string;
	start?: number;
	end?: number;
	loc?: Readonly<{
		start?: Readonly<{ line?: number; column?: number }>;
	}>;
};

export type DistributedIslandInventory = Readonly<{
	version: number;
	schemaFingerprint: string;
	protocolFingerprint: string;
	surface: unknown;
	islands: readonly DistributedIslandPlanInput[];
}>;

export type DistributedIslandPlanInput = Readonly<{
	version: number;
	id: string;
	operation: string;
	operationHash: string;
	modulePath: string;
	exportName: string;
	source: Readonly<{ path: string; line: number; column: number }>;
	directives: Readonly<{ load: boolean; live: boolean }>;
	liveCoverage: Readonly<{
		requested: boolean;
		finite: boolean;
		kind: string;
		maxItems?: number;
	}>;
	variableSchema: Readonly<{
		reference: string;
		codecVersion: number;
		variables: readonly Readonly<{
			name: string;
			graphqlType: string;
			defaultValue?: unknown;
		}>[];
	}>;
}>;

export type DistributedSvelteKitBoundaryOccurrence = Readonly<{
	islandId: string;
	operation: string;
	modulePath: string;
	exportName: string;
	component: string;
	graphqlSource: string;
	reason: 'route_document' | 'static_component_import' | 'explicit';
	conservative: boolean;
	directives: Readonly<{ load: boolean; live: boolean }>;
	liveCoverage: DistributedIslandPlanInput['liveCoverage'];
	binding: Readonly<{
		version: 2;
		id: string;
		discovery: 'route_param' | 'empty' | 'explicit';
		sources: Readonly<Record<string, DistributedBoundaryVariableSource>>;
	}>;
}>;

export type DistributedSvelteKitBoundary = Readonly<{
	id: string;
	route: string;
	kind: 'layout' | 'page';
	source: string;
	islands: readonly DistributedSvelteKitBoundaryOccurrence[];
}>;

export type DistributedSvelteKitBoundaryPlan = Readonly<{
	version: number;
	module: string;
	schemaFingerprint: string;
	protocolFingerprint: string;
	boundaries: readonly DistributedSvelteKitBoundary[];
	unplaced: readonly Readonly<{
		islandId: string;
		operation: string;
		graphqlSource: string;
	}>[];
}>;

export type DistributedSvelteKitBoundaryAnalysisClient = Readonly<{
	module: string;
	inventory: DistributedIslandInventory;
	explicitBoundaries?: readonly DistributedSvelteKitBoundaryRegistration[];
}>;

export type DistributedSvelteKitBoundaryRegistration = Readonly<{
	operation: string;
	route: string;
	kind: 'layout' | 'page';
	variables?: Readonly<Record<string, DistributedBoundaryVariableSource>>;
}>;

export type DistributedSvelteKitBoundaryAnalysisOptions = Readonly<{
	cwd: string;
	routesDir?: string;
	libDir?: string;
	aliases?: Readonly<Record<string, string>>;
	clients: readonly DistributedSvelteKitBoundaryAnalysisClient[];
}>;

/** Validate a persisted adapter plan before check/dev treats it as coherent. */
export function validateDistributedSvelteKitBoundaryPlan(
	value: unknown,
	module?: string
): DistributedSvelteKitBoundaryPlan {
	if (
		value === null ||
		typeof value !== 'object' ||
		Array.isArray(value) ||
		(value as { version?: unknown }).version !== BOUNDARY_PLAN_VERSION ||
		typeof (value as { module?: unknown }).module !== 'string' ||
		(module !== undefined && (value as { module: string }).module !== module) ||
		!Array.isArray((value as { boundaries?: unknown }).boundaries) ||
		!Array.isArray((value as { unplaced?: unknown }).unplaced)
	) {
		throw new Error(
			`[distributed.island.boundary_plan_invalid] ${module ?? '<unknown>'} boundaries.json is not version ${BOUNDARY_PLAN_VERSION}`
		);
	}
	return value as DistributedSvelteKitBoundaryPlan;
}

type ComponentModule = Readonly<{
	path: string;
	imports: readonly StaticImport[];
	dynamicImports: readonly DynamicImport[];
}>;

type StaticImport = Readonly<{
	specifier: string;
	line: number;
	column: number;
}>;

type DynamicImport = StaticImport & Readonly<{ opaque: boolean }>;

type BoundaryRoot = Readonly<{
	id: string;
	route: string;
	kind: 'layout' | 'page';
	path: string;
}>;

/**
 * Analyze Svelte component reachability without evaluating application
 * components. Colocated, bounded GraphQL binding sidecars are the only app
 * modules evaluated. The returned plan is deterministic and contains
 * project-relative paths only.
 */
export async function analyzeDistributedSvelteKitBoundaries(
	options: DistributedSvelteKitBoundaryAnalysisOptions
): Promise<readonly DistributedSvelteKitBoundaryPlan[]> {
	const cwd = resolve(options.cwd);
	const routesDir = contained(cwd, options.routesDir ?? 'src/routes', 'routesDir');
	const libDir = contained(cwd, options.libDir ?? 'src/lib', 'libDir');
	const aliases = new Map<string, string>([
		['$lib', libDir],
		...Object.entries(options.aliases ?? {}).map(([key, value]) => [
			aliasKey(key),
			contained(cwd, value, `alias ${key}`)
		] as const)
	]);
	const components = await loadComponents(cwd, [
		...new Set([routesDir, libDir, ...aliases.values()])
	]);
	const roots = boundaryRoots(cwd, routesDir, components);
	const sourceOwners = new Map<string, string>();
	for (const client of options.clients) {
		validateInventory(client.module, client.inventory);
		for (const island of client.inventory.islands.filter(
			(candidate) => candidate.directives.load
		)) {
			const source = portableSource(island.source.path);
			const previous = sourceOwners.get(source);
			if (previous !== undefined && previous !== client.module) {
				throw diagnostic(
					'distributed.island.cross_surface',
					source,
					island.source.line,
					island.source.column,
					`operation ${island.operation} is owned by both ${previous} and ${client.module}; split the GraphQL source by authorization surface`
				);
			}
			sourceOwners.set(source, client.module);
		}
	}

	const plans: DistributedSvelteKitBoundaryPlan[] = [];
	const colocatedBindings = await loadColocatedBindings(cwd, sourceOwners.keys());
	for (const client of [...options.clients].sort((left, right) =>
		left.module.localeCompare(right.module)
	)) {
		const loadIslands = client.inventory.islands
			.filter((island) => island.directives.load)
			.map((island) => ({ island, source: portableSource(island.source.path) }))
			.sort((left, right) =>
				left.source.localeCompare(right.source) ||
				left.island.operation.localeCompare(right.island.operation)
			);
		const componentIslands = new Map<string, typeof loadIslands>();
		const routeIslands = new Map<string, typeof loadIslands>();
		const islandByOperation = new Map(
			loadIslands.map((entry) => [entry.island.operation, entry] as const)
		);
		type ExplicitEntry = Readonly<{
			entry: typeof loadIslands[number];
			registration: DistributedSvelteKitBoundaryRegistration;
		}>;
		const explicitByBoundary = new Map<string, ExplicitEntry[]>();
		const explicitByIdentity = new Map<string, DistributedSvelteKitBoundaryRegistration>();
		const explicitlyPlaced = new Set<string>();
		for (const registration of client.explicitBoundaries ?? []) {
			const entry = islandByOperation.get(registration.operation);
			if (entry === undefined) {
				throw new Error(
					`[distributed.island.explicit_operation_missing] ${client.module} explicit boundary references unknown @load operation ${registration.operation}`
				);
			}
			const id = `${registration.kind}:${normalizeRoute(registration.route)}`;
			const bucket = explicitByBoundary.get(id) ?? [];
			const identity = `${id}\u0000${registration.operation}`;
			if (explicitByIdentity.has(identity)) {
				throw new Error(
					`[distributed.island.explicit_duplicate] ${client.module} repeats ${registration.operation} at ${registration.route}`
				);
			}
			bucket.push(Object.freeze({ entry, registration }));
			explicitByBoundary.set(id, bucket);
			explicitByIdentity.set(identity, registration);
			explicitlyPlaced.add(entry.island.id);
		}
		for (const entry of loadIslands) {
			const target = islandOwnerComponent(cwd, entry.source);
			if (target.kind === 'component') {
				const bucket = componentIslands.get(target.path) ?? [];
				const conflicting = bucket.find((candidate) => candidate.source !== entry.source);
				if (conflicting !== undefined) {
					throw diagnostic(
						'distributed.island.sibling_conflict',
						entry.source,
						entry.island.source.line,
						entry.island.source.column,
						`operation ${entry.island.operation} conflicts with sibling source ${conflicting.source}; keep one GraphQL sibling or register an explicit boundary`
					);
				}
				bucket.push(entry);
				componentIslands.set(target.path, bucket);
			} else {
				const bucket = routeIslands.get(target.key) ?? [];
				bucket.push(entry);
				routeIslands.set(target.key, bucket);
			}
		}

		for (const component of componentIslands.keys()) {
			const needsComponent = componentIslands
				.get(component)!
				.some(({ island }) => !explicitlyPlaced.has(island.id));
			if (!needsComponent) continue;
			if (!components.has(component)) {
				const entry = componentIslands.get(component)![0]!;
				throw diagnostic(
					'distributed.island.component_missing',
					entry.source,
					entry.island.source.line,
					entry.island.source.column,
					`operation ${entry.island.operation} requires sibling ${projectPath(cwd, component)}; add the component, register an explicit boundary, or mark it client-only`
				);
			}
		}

		const boundaries: DistributedSvelteKitBoundary[] = [];
		const placed = new Set<string>();
		for (const root of roots) {
			const occurrences: DistributedSvelteKitBoundaryOccurrence[] = [];
			const explicitEntries = explicitByBoundary.get(root.id) ?? [];
			const explicitAtBoundary = new Set(
				explicitEntries.map(({ entry }) => entry.island.id)
			);
			for (const { entry, registration } of explicitEntries) {
				occurrences.push(
					occurrence(
						cwd,
						entry,
						root,
						root.path,
						'explicit',
						false,
						bindingSources(entry, registration.variables, colocatedBindings)
					)
				);
				placed.add(entry.island.id);
			}
			for (
				const entry of routeIslands.get(
					routeBoundaryKey(root.kind, dirname(root.path))
				) ?? []
			) {
				occurrences.push(
					occurrence(
						cwd,
						entry,
						root,
						root.path,
						'route_document',
						false,
						bindingSources(
							entry,
							explicitByIdentity.get(`${root.id}\u0000${entry.island.operation}`)?.variables,
							colocatedBindings
						)
					)
				);
				placed.add(entry.island.id);
			}
			const dynamicComponentIslands = new Map(
				[...componentIslands.entries()]
					.map(([path, entries]) => [
						path,
						entries.filter(({ island }) => !explicitAtBoundary.has(island.id))
					] as const)
					.filter(([, entries]) => entries.length > 0)
			);
			const reachable = traverse(root, components, aliases, dynamicComponentIslands, cwd);
			for (const component of reachable) {
				for (const entry of componentIslands.get(component) ?? []) {
					occurrences.push(
						occurrence(
							cwd,
							entry,
							root,
							component,
							'static_component_import',
							true,
							bindingSources(
								entry,
								explicitByIdentity.get(`${root.id}\u0000${entry.island.operation}`)?.variables,
								colocatedBindings
							)
						)
					);
					placed.add(entry.island.id);
				}
			}
			const deduplicated = [...new Map(
				occurrences.map((entry) => [entry.islandId, entry] as const)
			).values()].sort(compareOccurrences);
			if (deduplicated.length > 0) {
				boundaries.push(Object.freeze({
					id: root.id,
					route: root.route,
					kind: root.kind,
					source: projectPath(cwd, root.path),
					islands: Object.freeze(deduplicated)
				}));
			}
		}
		const unplaced = loadIslands
			.filter(({ island }) => !placed.has(island.id))
			.map(({ island, source }) => Object.freeze({
				islandId: island.id,
				operation: island.operation,
				graphqlSource: source
			}));
		plans.push(Object.freeze({
			version: BOUNDARY_PLAN_VERSION,
			module: client.module,
			schemaFingerprint: client.inventory.schemaFingerprint,
			protocolFingerprint: client.inventory.protocolFingerprint,
			boundaries: Object.freeze(boundaries),
			unplaced: Object.freeze(unplaced)
		}));
	}
	return Object.freeze(plans);
}

function bindingSources(
	entry: Readonly<{ source: string; island: DistributedIslandPlanInput }>,
	explicit: Readonly<Record<string, DistributedBoundaryVariableSource>> | undefined,
	colocated: ReadonlyMap<string, Readonly<Record<string, DistributedBoundaryVariableSource>>>
): Readonly<Record<string, DistributedBoundaryVariableSource>> | undefined {
	const sidecar = colocated.get(entry.source);
	if (explicit !== undefined && sidecar !== undefined) {
		throw diagnostic(
			'distributed.island.variable_binding_conflict',
			entry.source,
			entry.island.source.line,
			entry.island.source.column,
			`operation ${entry.island.operation} has both centralized and colocated variable bindings; remove the screen behavior from distributed.config and keep ${entry.source}.bindings.js`
		);
	}
	return sidecar ?? explicit;
}

async function loadColocatedBindings(
	cwd: string,
	sources: Iterable<string>
): Promise<ReadonlyMap<string, Readonly<Record<string, DistributedBoundaryVariableSource>>>> {
	const canonicalRoot = await realpath(cwd);
	const bindings = new Map<
		string,
		Readonly<Record<string, DistributedBoundaryVariableSource>>
	>();
	for (const source of [...new Set(sources)].sort()) {
		const path = contained(cwd, `${source}.bindings.js`, 'GraphQL island bindings');
		let metadata;
		try {
			metadata = await lstat(path);
		} catch (error) {
			if (isMissing(error)) continue;
			throw error;
		}
		if (!metadata.isFile() || metadata.isSymbolicLink()) {
			throw new Error(
				`[distributed.island.bindings_invalid] ${source}.bindings.js must be a regular project-local file`
			);
		}
		if (metadata.size > MAX_BINDINGS_BYTES) {
			throw new Error(
				`[distributed.island.bindings_too_large] ${source}.bindings.js exceeds ${MAX_BINDINGS_BYTES} bytes`
			);
		}
		const canonicalPath = await realpath(path);
		if (!isWithin(canonicalRoot, canonicalPath)) {
			throw new Error(
				`[distributed.island.bindings_invalid] ${source}.bindings.js must stay within the project root`
			);
		}
		const bytes = await readFile(canonicalPath);
		const url = pathToFileURL(canonicalPath);
		url.searchParams.set(
			'distributed-binding',
			createHash('sha256').update(bytes).digest('hex')
		);
		const loaded = await import(url.href);
		const value = ownDataValue(loaded, 'default');
		if (!isGraphqlIslandBindings(value)) {
			throw new Error(
				`[distributed.island.bindings_invalid] ${source}.bindings.js must default-export defineGraphqlIslandBindings({...})`
			);
		}
		bindings.set(
			source,
			value as Readonly<Record<string, DistributedBoundaryVariableSource>>
		);
	}
	return bindings;
}

function occurrence(
	cwd: string,
	entry: { island: DistributedIslandPlanInput; source: string },
	root: BoundaryRoot,
	component: string,
	reason: DistributedSvelteKitBoundaryOccurrence['reason'],
	conservative: boolean,
	explicitSources?: Readonly<Record<string, DistributedBoundaryVariableSource>>
): DistributedSvelteKitBoundaryOccurrence {
	if (
		root.kind === 'layout' &&
		entry.island.directives.live &&
		!entry.island.liveCoverage.finite
	) {
		throw diagnostic(
			'distributed.island.layout_live_unbounded',
			entry.source,
			entry.island.source.line,
			entry.island.source.column,
			`operation ${entry.island.operation} is live for layout ${root.route} without finite coverage; add a compiler-proved limit, move it to a page, register a bounded boundary-owned query, or mark it client-only`
		);
	}
	const binding = boundaryBinding(entry, root, explicitSources);
	return Object.freeze({
		islandId: entry.island.id,
		operation: entry.island.operation,
		modulePath: entry.island.modulePath,
		exportName: entry.island.exportName,
		component: projectPath(cwd, component),
		graphqlSource: entry.source,
		reason,
		conservative,
		directives: entry.island.directives,
		liveCoverage: entry.island.liveCoverage,
		binding
	});
}

function boundaryBinding(
	entry: { island: DistributedIslandPlanInput; source: string },
	root: BoundaryRoot,
	explicitSources?: Readonly<Record<string, DistributedBoundaryVariableSource>>
): DistributedSvelteKitBoundaryOccurrence['binding'] {
	if (
		typeof entry.island.operationHash !== 'string' ||
		entry.island.operationHash.length === 0 ||
		!Array.isArray(entry.island.variableSchema?.variables)
	) {
		throw diagnostic(
			'distributed.island.binding_inventory_invalid',
			entry.source,
			entry.island.source.line,
			entry.island.source.column,
			`operation ${entry.island.operation} has invalid variable-binding inventory; regenerate the framework-neutral client`
		);
	}
	const variables = [...entry.island.variableSchema.variables].sort((left, right) =>
		left.name.localeCompare(right.name)
	);
	const allowed = new Set(variables.map(({ name }) => name));
	for (const name of Object.keys(explicitSources ?? {})) {
		if (!allowed.has(name)) {
			throw diagnostic(
				'distributed.island.variable_unknown',
				entry.source,
				entry.island.source.line,
				entry.island.source.column,
				`operation ${entry.island.operation} binding names unknown variable ${name}; use an explicit binding, parent/boundary query, client-only execution, or a better read root`
			);
		}
	}
	const routeParams = new Set(routeParameterNames(root.route));
	const sources: Array<[string, DistributedBoundaryVariableSource]> = [];
	let hasExplicit = false;
	let hasRoute = false;
	for (const variable of variables) {
		const explicit = ownDataValue(explicitSources, variable.name);
		if (explicit !== undefined) {
			let normalized: DistributedBoundaryVariableSource;
			try {
				normalized = normalizeBindingSource(explicit, variable.name, variable.graphqlType);
			} catch {
				throw diagnostic(
					'distributed.island.variable_source_invalid',
					entry.source,
					entry.island.source.line,
					entry.island.source.column,
					`operation ${entry.island.operation} variable ${variable.name} has an unsupported or unsafe explicit variable source at boundary ${root.route}; use a route/search parameter, trusted-session path, constant, forwarded prop, omission, parent/boundary query, client-only execution, or a better read root`
				);
			}
			sources.push([variable.name, normalized]);
			hasExplicit = true;
		} else if (routeParams.has(variable.name)) {
			sources.push([
				variable.name,
				Object.freeze({ kind: 'route_param', name: variable.name })
			]);
			hasRoute = true;
		} else if (Object.hasOwn(variable, 'defaultValue') || !variable.graphqlType.endsWith('!')) {
			sources.push([variable.name, Object.freeze({ kind: 'omit' })]);
		} else {
			throw diagnostic(
				'distributed.island.variable_unprovable',
				entry.source,
				entry.island.source.line,
				entry.island.source.column,
				`operation ${entry.island.operation} variable ${variable.name} is not boundary-visible at ${root.route}; use an explicit binding, parent/boundary query, client-only execution, or a better read root`
			);
		}
	}
	const sourceRecord = Object.freeze(Object.fromEntries(sources));
	return Object.freeze({
		version: DISTRIBUTED_BOUNDARY_BINDING_VERSION,
		id: `boundary-v${DISTRIBUTED_BOUNDARY_BINDING_VERSION}:${fnv1a64(`${entry.island.operationHash}\n${stableJson(sourceRecord)}`)}`,
		discovery: hasExplicit ? 'explicit' : hasRoute ? 'route_param' : 'empty',
		sources: sourceRecord
	});
}

function routeParameterNames(route: string): readonly string[] {
	const names: string[] = [];
	for (const segment of route.split('/')) {
		const match = /^\[\[?(?:\.\.\.)?([_A-Za-z][_0-9A-Za-z]*)(?:=[^\]]+)?\]?\]$/.exec(segment);
		if (match?.[1] !== undefined) names.push(match[1]);
	}
	return names;
}

function normalizeBindingSource(
	value: unknown,
	variable: string,
	graphqlType: string
): DistributedBoundaryVariableSource {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		throw new TypeError(`Distributed boundary variable ${variable} source must be an object`);
	}
	const record = value as Record<string, unknown>;
	const kind = ownDataValue(record, 'kind');
	switch (kind) {
		case 'omit':
			return Object.freeze({ kind: 'omit' });
		case 'route_param': {
			const name = ownDataValue(record, 'name');
			if (typeof name !== 'string' || name.length === 0) {
				throw new TypeError(`Distributed boundary variable ${variable} source name is invalid`);
			}
			return Object.freeze({ kind: 'route_param', name });
		}
		case 'search_param': {
			return normalizeSearchParamSource(record, variable, graphqlType);
		}
		case 'trusted_session':
		case 'forwarded_prop': {
			const path = ownDataValue(record, 'path');
			if (
				!Array.isArray(path) ||
				path.length === 0 ||
				path.length > 16 ||
				path.some((part) =>
					typeof part !== 'string' ||
					!/^[_A-Za-z][_0-9A-Za-z]*$/.test(part) ||
					['__proto__', 'prototype', 'constructor'].includes(part)
				)
			) {
				throw new TypeError(`Distributed boundary variable ${variable} source path is invalid`);
			}
			return Object.freeze({ kind, path: Object.freeze([...path]) });
		}
		case 'constant':
			return Object.freeze({
				kind: 'constant',
				value: freezeStable(stableValue(ownDataValue(record, 'value')))
			});
		default:
			throw new TypeError(
				`Distributed boundary variable ${variable} source is unsupported; use an explicit binding, parent/boundary query, client-only execution, or a better read root`
			);
	}
}

function traverse(
	root: BoundaryRoot,
	components: ReadonlyMap<string, ComponentModule>,
	aliases: ReadonlyMap<string, string>,
	componentIslands: ReadonlyMap<string, readonly unknown[]>,
	cwd: string
): readonly string[] {
	const reached = new Set<string>();
	const active = new Set<string>();
	const parent = new Map<string, string>();
	const visiting: Array<Readonly<{ path: string; exit: boolean }>> = [
		{ path: root.path, exit: false }
	];
	let edges = 0;
	while (visiting.length > 0) {
		const frame = visiting.pop()!;
		const path = frame.path;
		if (frame.exit) {
			active.delete(path);
			continue;
		}
		if (reached.has(path)) continue;
		reached.add(path);
		active.add(path);
		visiting.push({ path, exit: true });
		const component = components.get(path);
		if (component === undefined) continue;
		const targets: string[] = [];
		for (const imported of component.imports) {
			edges += 1;
			if (edges > MAX_GRAPH_EDGES) {
				throw diagnostic(
					'distributed.island.graph_unbounded',
					projectPath(cwd, path),
					imported.line,
					imported.column,
					`boundary ${root.route} exceeds ${MAX_GRAPH_EDGES} static component edges; split the boundary or register a bounded explicit boundary`
				);
			}
			const target = resolveComponentImport(path, imported, aliases, components, cwd);
			if (target === undefined) continue;
			if (active.has(target)) {
				const cycle = [target];
				let cursor = path;
				while (cursor !== target) {
					cycle.push(cursor);
					const previous = parent.get(cursor);
					if (previous === undefined) break;
					cursor = previous;
				}
				if (cycle.some((candidate) => componentIslands.has(candidate))) {
					throw diagnostic(
						'distributed.island.component_cycle',
						projectPath(cwd, path),
						imported.line,
						imported.column,
						`boundary ${root.route} reaches an @load island through a cyclic component graph; break the cycle, register an explicit boundary-owned query, or mark it client-only`
					);
				}
				continue;
			}
			if (!reached.has(target)) {
				parent.set(target, path);
				targets.push(target);
			}
		}
		for (const target of targets.reverse()) {
			visiting.push({ path: target, exit: false });
		}
		for (const imported of component.dynamicImports) {
			if (imported.opaque) {
				if (componentIslands.size > 0) {
					throw diagnostic(
						'distributed.island.dynamic_opaque',
						projectPath(cwd, path),
						imported.line,
						imported.column,
						`boundary ${root.route} has opaque dynamic component reachability while @load islands remain unplaced; use a static import, explicit boundary registration, or client-only execution`
					);
				}
				continue;
			}
			const target = resolveComponentImport(path, imported, aliases, components, cwd);
			if (target !== undefined && componentIslands.has(target)) {
				throw diagnostic(
					'distributed.island.dynamic_load',
					projectPath(cwd, path),
					imported.line,
					imported.column,
					`boundary ${root.route} dynamically reaches an @load island; use a static import, explicit boundary registration, or client-only execution`
				);
			}
		}
	}
	return [...reached].sort();
}

function resolveComponentImport(
	from: string,
	imported: StaticImport,
	aliases: ReadonlyMap<string, string>,
	components: ReadonlyMap<string, ComponentModule>,
	cwd: string
): string | undefined {
	const specifier = imported.specifier;
	let candidate: string | undefined;
	if (specifier.startsWith('.')) {
		candidate = resolve(dirname(from), specifier);
	} else {
		const alias = [...aliases.entries()]
			.sort((left, right) => right[0].length - left[0].length)
			.find(([key]) => specifier === key || specifier.startsWith(`${key}/`));
		if (alias !== undefined) {
			candidate = resolve(alias[1], specifier.slice(alias[0].length).replace(/^\//, ''));
		} else if (specifier.startsWith('$') && looksLikeComponent(specifier)) {
			throw diagnostic(
				'distributed.island.alias_unresolved',
				projectPath(cwd, from),
				imported.line,
				imported.column,
				`component alias ${specifier.split('/')[0]} is unresolved; add it to distributedSvelteKit aliases or use a resolvable static import`
			);
		}
	}
	if (candidate === undefined) return undefined;
	if (!isWithin(cwd, candidate)) {
		throw diagnostic(
			'distributed.island.import_escape',
			projectPath(cwd, from),
			imported.line,
			imported.column,
			'component import escapes the project root; use a project-local component or client-only execution'
		);
	}
	const attempts = extname(candidate) === '.svelte'
		? [candidate]
		: [`${candidate}.svelte`, join(candidate, 'index.svelte')];
	for (const attempt of attempts) {
		if (components.has(attempt)) return attempt;
	}
	if (looksLikeComponent(specifier)) {
		throw diagnostic(
			'distributed.island.import_unresolved',
			projectPath(cwd, from),
			imported.line,
			imported.column,
			`component import ${specifier} is unresolved; fix the static import, register an explicit boundary, or mark it client-only`
		);
	}
	return undefined;
}

function looksLikeComponent(specifier: string): boolean {
	return specifier.endsWith('.svelte') || /\/[A-Z][^/]*$/.test(specifier);
}

async function loadComponents(
	cwd: string,
	roots: readonly string[]
): Promise<ReadonlyMap<string, ComponentModule>> {
	const paths = new Set<string>();
	for (const root of roots) await collectSvelteFiles(root, cwd, paths);
	if (paths.size > MAX_COMPONENTS) {
		throw new Error(
			`[distributed.island.graph_unbounded] Svelte project exceeds ${MAX_COMPONENTS} components; narrow routesDir/libDir`
		);
	}
	const components = new Map<string, ComponentModule>();
	for (const path of [...paths].sort()) {
		const metadata = await lstat(path);
		if (!metadata.isFile() || metadata.isSymbolicLink()) continue;
		if (metadata.size > MAX_COMPONENT_BYTES) {
			throw diagnostic(
				'distributed.island.component_too_large',
				projectPath(cwd, path),
				1,
				1,
				`component exceeds ${MAX_COMPONENT_BYTES} bytes; split it or use client-only execution`
			);
		}
		const source = await readFile(path, 'utf8');
		components.set(path, parseComponent(path, source, cwd));
	}
	return components;
}

async function collectSvelteFiles(
	root: string,
	cwd: string,
	paths: Set<string>
): Promise<void> {
	let entries;
	try {
		entries = await readdir(root, { withFileTypes: true });
	} catch (error) {
		if (isMissing(error)) return;
		throw error;
	}
	for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
		const path = join(root, entry.name);
		if (!isWithin(cwd, path) || entry.isSymbolicLink()) continue;
		if (entry.isDirectory()) await collectSvelteFiles(path, cwd, paths);
		else if (entry.isFile() && entry.name.endsWith('.svelte')) paths.add(path);
	}
}

function parseComponent(path: string, source: string, cwd: string): ComponentModule {
	let ast: AstNode;
	try {
		ast = parse(source, { filename: projectPath(cwd, path), modern: true }) as unknown as AstNode;
	} catch (error) {
		throw new Error(
			`[distributed.island.svelte_parse] ${projectPath(cwd, path)}: ${error instanceof Error ? error.message : String(error)}`
		);
	}
	const imports: StaticImport[] = [];
	const dynamicImports: DynamicImport[] = [];
	walkAst(ast, (node) => {
		if (node.type === 'ImportDeclaration') {
			const value = literalValue(node.source);
			if (value !== undefined) imports.push(located(value, node));
		} else if (node.type === 'ImportExpression') {
			const value = literalValue(node.source);
			dynamicImports.push({
				...located(value ?? '<dynamic>', node),
				opaque: value === undefined
			});
		}
	});
	return Object.freeze({
		path,
		imports: Object.freeze(imports),
		dynamicImports: Object.freeze(dynamicImports)
	});
}

function walkAst(value: unknown, visit: (node: AstNode) => void): void {
	const stack: unknown[] = [value];
	const seen = new WeakSet<object>();
	while (stack.length > 0) {
		const current = stack.pop();
		if (current === null || typeof current !== 'object') continue;
		if (seen.has(current)) continue;
		seen.add(current);
		if (!Array.isArray(current)) visit(current as AstNode);
		for (const child of Array.isArray(current)
			? current
			: Object.values(current as Record<string, unknown>)) {
			if (child !== null && typeof child === 'object') stack.push(child);
		}
	}
}

function literalValue(value: unknown): string | undefined {
	if (value === null || typeof value !== 'object') return undefined;
	const candidate = value as { type?: string; value?: unknown };
	return candidate.type === 'Literal' && typeof candidate.value === 'string'
		? candidate.value
		: undefined;
}

function located(specifier: string, node: AstNode): StaticImport {
	return Object.freeze({
		specifier,
		line: node.loc?.start?.line ?? 1,
		column: (node.loc?.start?.column ?? 0) + 1
	});
}

function boundaryRoots(
	cwd: string,
	routesDir: string,
	components: ReadonlyMap<string, ComponentModule>
): readonly BoundaryRoot[] {
	return [...components.keys()]
		.filter((path) => path.startsWith(`${routesDir}${sep}`) || path === routesDir)
		.flatMap((path): BoundaryRoot[] => {
			const name = posix.basename(portable(path));
			const kind = boundaryComponentKind(name);
			if (kind === undefined) return [];
			const routeDirectory = portable(relative(routesDir, dirname(path)));
			const route = routeDirectory === '' ? '/' : `/${routeDirectory}`;
			return [{
				id: `${kind}:${route}`,
				route,
				kind,
				path
			}];
		})
		.sort((left, right) =>
			left.route.localeCompare(right.route) ||
			left.kind.localeCompare(right.kind) ||
			projectPath(cwd, left.path).localeCompare(projectPath(cwd, right.path))
		);
}

function boundaryComponentKind(name: string): BoundaryRoot['kind'] | undefined {
	const match = /^\+(page|layout)(?:@[^/]*)?\.svelte$/.exec(name);
	return match?.[1] === 'page' || match?.[1] === 'layout'
		? match[1]
		: undefined;
}

function routeBoundaryKey(kind: BoundaryRoot['kind'], directory: string): string {
	return `${kind}\u0000${directory}`;
}

function islandOwnerComponent(
	cwd: string,
	source: string
):
	| Readonly<{ kind: 'component'; path: string }>
	| Readonly<{ kind: 'route'; key: string }> {
	const absolute = contained(cwd, source, 'island source');
	const suffix = extname(absolute);
	const base = absolute.slice(0, -suffix.length);
	const name = posix.basename(portable(base));
	const routeDocument = /^\+(page|layout)(?:\.[_A-Za-z][_0-9A-Za-z-]*)?$/.exec(name);
	if (routeDocument !== null) {
		return {
			kind: 'route',
			key: routeBoundaryKey(
				routeDocument[1] === 'page' ? 'page' : 'layout',
				dirname(base)
			)
		};
	}
	return { kind: 'component', path: `${base}.svelte` };
}

function validateInventory(module: string, inventory: DistributedIslandInventory): void {
	if (
		inventory === null ||
		typeof inventory !== 'object' ||
		inventory.version !== 1 ||
		!Array.isArray(inventory.islands) ||
		inventory.islands.length > MAX_ISLANDS ||
		typeof inventory.schemaFingerprint !== 'string' ||
		typeof inventory.protocolFingerprint !== 'string'
	) {
		throw new Error(
			`[distributed.island.inventory_invalid] ${module} islands.json is not version 1`
		);
	}
	for (const island of inventory.islands) {
		const source = island?.source;
		const variableSchema = island?.variableSchema;
		if (
			island?.version !== 1 ||
			typeof island.id !== 'string' ||
			typeof island.operation !== 'string' ||
			typeof island.modulePath !== 'string' ||
			!island.modulePath.startsWith('operations/') ||
			!island.modulePath.endsWith('.ts') ||
			typeof island.exportName !== 'string' ||
			typeof source?.path !== 'string' ||
			!Number.isSafeInteger(source.line) ||
			source.line < 1 ||
			!Number.isSafeInteger(source.column) ||
			source.column < 1 ||
			island.directives === null ||
			typeof island.directives !== 'object' ||
			typeof island.directives.load !== 'boolean' ||
			typeof island.directives.live !== 'boolean' ||
			island.liveCoverage === null ||
			typeof island.liveCoverage !== 'object' ||
			variableSchema === null ||
			typeof variableSchema !== 'object' ||
			Array.isArray(variableSchema) ||
			typeof variableSchema.reference !== 'string' ||
			!Number.isSafeInteger(variableSchema.codecVersion) ||
			variableSchema.codecVersion !== VARIABLE_CODEC_VERSION ||
			!variableSchema.reference.endsWith(
				`#variable-codec-v${VARIABLE_CODEC_VERSION}`
			) ||
			!Array.isArray(variableSchema.variables) ||
			variableSchema.variables.some(
				(variable: unknown) =>
					variable === null ||
					typeof variable !== 'object' ||
					Array.isArray(variable) ||
					typeof (variable as { name?: unknown }).name !== 'string' ||
					typeof (variable as { graphqlType?: unknown }).graphqlType !== 'string'
			) ||
			typeof island.liveCoverage.requested !== 'boolean' ||
			typeof island.liveCoverage.finite !== 'boolean' ||
			typeof island.liveCoverage.kind !== 'string' ||
			(
				island.liveCoverage.maxItems !== undefined &&
				(!Number.isSafeInteger(island.liveCoverage.maxItems) ||
					island.liveCoverage.maxItems < 0)
			)
		) {
			throw new Error(
				`[distributed.island.version_unsupported] ${typeof source?.path === 'string' ? source.path : module}:${Number.isSafeInteger(source?.line) ? source.line : 1}:${Number.isSafeInteger(source?.column) ? source.column : 1}: island metadata is not version 1; regenerate the framework-neutral client and boundary plan`
			);
		}
	}
}

function portableSource(path: string): string {
	if (
		typeof path !== 'string' ||
		path.length === 0 ||
		isAbsolute(path) ||
		path.includes('\\') ||
		path.split('/').some((part) => part === '' || part === '.' || part === '..')
	) {
		throw new Error('[distributed.island.source_invalid] island source path is not portable');
	}
	return path;
}

function compareOccurrences(
	left: DistributedSvelteKitBoundaryOccurrence,
	right: DistributedSvelteKitBoundaryOccurrence
): number {
	return (
		left.operation.localeCompare(right.operation) ||
		left.component.localeCompare(right.component) ||
		left.graphqlSource.localeCompare(right.graphqlSource)
	);
}

function aliasKey(value: string): string {
	if (!/^\$[A-Za-z0-9_-]+$/.test(value)) {
		throw new TypeError(`Distributed SvelteKit alias ${value} must be a single $name segment`);
	}
	return value;
}

function normalizeRoute(value: string): string {
	if (
		typeof value !== 'string' ||
		!value.startsWith('/') ||
		value.includes('\\') ||
		value.includes('?') ||
		value.includes('#') ||
		value.split('/').some((part) => part === '.' || part === '..')
	) {
		throw new TypeError('Distributed explicit boundary route must be a normalized SvelteKit route id');
	}
	return value.length > 1 && value.endsWith('/') ? value.slice(0, -1) : value;
}

function contained(root: string, value: string, label: string): string {
	const path = resolve(root, value);
	if (!isWithin(root, path)) throw new TypeError(`${label} must stay within project root`);
	return path;
}

function isWithin(root: string, target: string): boolean {
	const path = relative(root, target);
	return path === '' || (!path.startsWith(`..${sep}`) && path !== '..' && !isAbsolute(path));
}

function projectPath(cwd: string, path: string): string {
	return portable(relative(cwd, path));
}

function portable(path: string): string {
	return path.split(sep).join('/');
}

function diagnostic(
	code: string,
	path: string,
	line: number,
	column: number,
	message: string
): Error {
	return new Error(`[${code}] ${path}:${line}:${column}: ${message}`);
}

function ownDataValue(value: unknown, key: string): unknown {
	if (value === null || typeof value !== 'object') return undefined;
	const descriptor = Object.getOwnPropertyDescriptor(value, key);
	if (descriptor === undefined) return undefined;
	if (!('value' in descriptor)) {
		throw new TypeError('Distributed boundary binding contains an accessor');
	}
	return descriptor.value;
}

function stableJson(value: unknown): string {
	return JSON.stringify(stableValue(value));
}

function stableValue(value: unknown): unknown {
	const active = new Set<object>();
	let visited = 0;
	const visit = (current: unknown, depth: number): unknown => {
		visited += 1;
		if (visited > 4_096 || depth > 32) {
			throw new TypeError('Distributed boundary binding exceeds structural limits');
		}
		if (
			current === null ||
			typeof current === 'string' ||
			typeof current === 'boolean'
		) return current;
		if (typeof current === 'number' && Number.isFinite(current)) return current;
		if (typeof current !== 'object') {
			throw new TypeError('Distributed boundary binding is not JSON-compatible');
		}
		if (active.has(current)) throw new TypeError('Distributed boundary binding is cyclic');
		active.add(current);
		try {
			if (Array.isArray(current)) return current.map((entry) => visit(entry, depth + 1));
			if (
				Object.getPrototypeOf(current) !== Object.prototype &&
				Object.getPrototypeOf(current) !== null
			) {
				throw new TypeError('Distributed boundary binding must contain plain objects');
			}
			return Object.fromEntries(
				Object.keys(current).sort().map((key) => {
					if (['__proto__', 'prototype', 'constructor'].includes(key)) {
						throw new TypeError('Distributed boundary binding contains a hostile object key');
					}
					return [key, visit(ownDataValue(current, key), depth + 1)];
				})
			);
		} finally {
			active.delete(current);
		}
	};
	return visit(value, 0);
}

function freezeStable(value: unknown): unknown {
	if (value === null || typeof value !== 'object') return value;
	for (const entry of Array.isArray(value)
		? value
		: Object.values(value as Record<string, unknown>)) {
		freezeStable(entry);
	}
	return Object.freeze(value);
}

function fnv1a64(value: string): string {
	let hash = 0xcbf29ce484222325n;
	for (const byte of new TextEncoder().encode(value)) {
		hash ^= BigInt(byte);
		hash = BigInt.asUintN(64, hash * 0x100000001b3n);
	}
	return hash.toString(16).padStart(16, '0');
}

function isMissing(error: unknown): boolean {
	return error !== null && typeof error === 'object' && 'code' in error && error.code === 'ENOENT';
}
