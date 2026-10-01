import type {
	DistributedReplica,
	ReplicaDehydratedState
} from '../replica/index.js';

const STATE_ENDPOINT = '/__distributed/lifecycle';
const CAPSULE_KEY = '@hops-ops/distributed/reload-capsule/v1';
const GENERATION_META = 'distributed-generation';
const MAX_CAPSULE_BYTES = 1024 * 1024;
const MAX_STATE_DEPTH = 32;
const PARTICIPANT_ID = /^[A-Za-z0-9_-]{16,128}$/;
const SECRET_KEY = /(?:authorization|cookie|password|secret|token|credential)/i;
const AUTH_QUERY_KEY = /^(?:code|samlresponse|session|state)$/i;
const STATE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const RESTORE_TIMEOUT_MS = 3_000;
const CAPSULE_MIN_LIFETIME_MS = 30_000;
// Preparation is followed by a bounded process-readiness transaction. Keep
// the capsule alive through the CLI's 90-second aggregate readiness budget,
// restoration, and ordinary browser scheduling delay.
const CAPSULE_ACTIVATION_GRACE_MS = 120_000;

export type DistributedReloadStateDeclaration = Readonly<{
	/** Stable application-owned partition name. */
	key: string;
	/** Changes when the serialized representation becomes incompatible. */
	fingerprint: string;
	capture(): unknown;
	restore(value: unknown): void | Promise<void>;
}>;

export type DistributedReloadOptions = Readonly<{
	/** Compiler-owned surface key; generated clients provide this automatically. */
	key: string;
	/** Explicitly declared serializable application state. Nothing else is captured. */
	state?: readonly DistributedReloadStateDeclaration[];
	/** Recover ambiguous receipts by ID; commands are never replayed by the framework. */
	recoverPendingCommands?: (commandIds: readonly string[]) => void | Promise<void>;
}>;

type LifecycleGeneration = Readonly<{
	generationId: string;
	releaseId: string;
	topologyId: string;
	compatibilityId: string;
}>;

type LifecycleDevState = Readonly<{
	schemaVersion: 1;
	phase: 'active' | 'preparing';
	active: LifecycleGeneration;
	pending?: LifecycleGeneration;
	transitionId?: string;
	deadlineUnixMs?: number;
}>;

type ReloadParticipant = Readonly<{
	key: string;
	prepare(): Readonly<{
		replica?: ReplicaDehydratedState;
		pendingCommandIds: readonly string[];
		state: readonly Readonly<{
			key: string;
			fingerprint: string;
			value: unknown;
		}>[];
	}>;
	/** Return false when restoration is valid but must be retried later. */
	restore(value: ReloadParticipantCapsule, compatible: boolean): boolean | Promise<boolean>;
}>;

type ReloadParticipantCapsule = Readonly<{
	key: string;
	replica?: ReplicaDehydratedState;
	pendingCommandIds: readonly string[];
	state: readonly Readonly<{
		key: string;
		fingerprint: string;
		value: unknown;
	}>[];
}>;

type ReloadCapsule = Readonly<{
	version: 1;
	transitionId: string;
	from: LifecycleGeneration;
	to: LifecycleGeneration;
	location: string;
	createdAtUnixMs: number;
	expiresAtUnixMs: number;
	phase: 'prepared' | 'restoring';
	participants: readonly ReloadParticipantCapsule[];
}>;

export interface DistributedReloadLifecycle {
	/** Let a prepared coherent transition own reload after a dev socket reconnect. */
	deferDevTransportReload(): Promise<void>;
	assertDispatchOpen(): void;
	register(participant: ReloadParticipant): () => void;
	destroy(): void;
}

/** Validate one explicitly declared application-state partition before capture. */
export function validateDistributedReloadState(value: unknown, path = 'reloadState'): unknown {
	assertSafeSerializable(value, path, 0, new Set(), true);
	const encoded = JSON.stringify(value);
	if (new TextEncoder().encode(encoded).length > MAX_CAPSULE_BYTES) {
		throw new TypeError(`${path} exceeds 1 MiB`);
	}
	return value;
}

/** Preserve browser location only when it cannot copy an auth callback secret. */
export function validateDistributedReloadLocation(location: URL): string {
	const parameters = [location.searchParams];
	if (location.hash.length > 1) {
		parameters.push(new URLSearchParams(location.hash.slice(1)));
	}
	for (const key of parameters.flatMap((candidate) => [...candidate.keys()])) {
		if (SECRET_KEY.test(key) || AUTH_QUERY_KEY.test(key)) {
			throw new TypeError(`reload location parameter ${key} is auth-secret-like`);
		}
	}
	return `${location.pathname}${location.search}${location.hash}`;
}

/** Register one generated client with the shared browser reload transaction. */
export function registerDistributedReloadClient(
	replica: DistributedReplica,
	runtime: Readonly<{ pendingCommandIds?(): readonly string[]; preload?(): Promise<void> }> | undefined,
	options: DistributedReloadOptions
): () => void {
	const state = new Map(
		(options.state ?? []).map((declaration) => {
			identity(declaration.key, 'reload state key');
			identity(declaration.fingerprint, 'reload state fingerprint');
			if (!STATE_KEY.test(declaration.key) || declaration.key.includes('..')) {
				throw new TypeError('Distributed reload state keys must be distinct portable names');
			}
			return [declaration.key, declaration] as const;
		})
	);
	if (state.size !== (options.state ?? []).length) {
		throw new TypeError('duplicate Distributed reload state declaration');
	}
	return distributedReloadLifecycle().register(Object.freeze({
		key: identity(options.key, 'reload participant key'),
		prepare() {
			const application = [...state.values()].map((declaration) => {
				const value = declaration.capture();
				validateDistributedReloadState(value, `reloadState.${declaration.key}`);
				return Object.freeze({
					key: declaration.key,
					fingerprint: declaration.fingerprint,
					value
				});
			});
			const dehydrated = replica.scope === undefined ? undefined : replica.dehydrate();
			return Object.freeze({
				...(dehydrated === undefined ? {} : { replica: dehydrated }),
				pendingCommandIds: runtime?.pendingCommandIds?.() ?? Object.freeze([]),
				state: Object.freeze(application)
			});
		},
		async restore(saved, compatible) {
			// A client-only application learns its replica authority from its first
			// query. Retain the capsule until that authority exists instead of
			// treating a skipped compatible hydrate as successful restoration.
			if (compatible && saved.replica !== undefined && replica.scope === undefined) {
				return false;
			}
			const replicaCaptured = saved.replica !== undefined;
			let replicaRestored = saved.replica === undefined;
			if (compatible && saved.replica !== undefined && replica.scope !== undefined) {
				replicaRestored = replica.hydrate(saved.replica, replica.scope);
			}
			for (const candidate of saved.state) {
				const declaration = state.get(candidate.key);
				if (declaration?.fingerprint === candidate.fingerprint) {
					await declaration.restore(candidate.value);
				}
			}
			if (saved.pendingCommandIds.length > 0 && options.recoverPendingCommands !== undefined) {
				await runtime?.preload?.();
				await options.recoverPendingCommands(saved.pendingCommandIds);
			}
			window.dispatchEvent(
				new CustomEvent('distributed:reload-restored', {
					detail: Object.freeze({
						key: options.key,
						replicaCaptured,
						replicaRestored,
						pendingCommandIds: saved.pendingCommandIds
					})
				})
			);
			return true;
		}
	}));
}

let sharedLifecycle: DistributedReloadLifecycle | undefined;
// Schedule from the completed request, leaving a full idle window between
// heartbeats. Besides reducing background work, this preserves browser tooling
// that defines network-idle as 500 ms without an in-flight request.
const LIFECYCLE_POLL_INTERVAL_MS = 1_000;

/** Browser singleton used by every generated SvelteKit surface in one page. */
export function distributedReloadLifecycle(): DistributedReloadLifecycle {
	if (sharedLifecycle === undefined) {
		try {
			sharedLifecycle = createDistributedReloadLifecycle();
		} catch {
			// Storage and Web Crypto are optional browser capabilities. Losing
			// them disables coherent reload state transfer, not the application.
			sharedLifecycle = inertLifecycle();
		}
	}
	return sharedLifecycle;
}

function createDistributedReloadLifecycle(): DistributedReloadLifecycle {
	if (typeof window === 'undefined') return inertLifecycle();
	const participants = new Map<string, ReloadParticipant>();
	const participantId = browserParticipantId();
	let blocked = false;
	let destroyed = false;
	let preparing: string | undefined;
	let reloadRequested = false;
	let devTransportDisconnected = false;
	const deferredTransports = new Set<() => void>();
	let loadedGenerationId = documentGenerationId();
	let timer: ReturnType<typeof setTimeout> | undefined;
	let restoration = Promise.resolve();
	const queueRestoration = (active?: LifecycleGeneration): Promise<void> => {
		const requested = restoration.then(() =>
			restoreAvailableParticipants(participants, active)
		);
		// Keep the queue usable after a storage/restore failure while returning
		// the real result to the caller that owns this attempt.
		restoration = requested.catch(() => undefined);
		return requested;
	};

	const poll = async (): Promise<void> => {
		if (destroyed) return;
		try {
			const response = await fetch(STATE_ENDPOINT, {
				headers: { 'x-distributed-participant': participantId },
				cache: 'no-store',
				credentials: 'same-origin'
			});
			if (response.status === 404) {
				blocked = false;
				return;
			}
			if (!response.ok) throw new Error(`lifecycle state returned ${response.status}`);
			const state = parseLifecycleState(await response.json());
			// The Vite lifecycle integration stamps the generation that served this
			// document into its HTML. Fall back to the first observed active state for
			// consumers that mount the lifecycle client without that integration.
			loadedGenerationId ??= state.active.generationId;
			if (state.phase === 'preparing' && state.pending !== undefined && state.transitionId !== undefined) {
				blocked = true;
				if (preparing !== state.transitionId) {
					preparing = state.transitionId;
					window.dispatchEvent(
						new CustomEvent('distributed:reload-preparing', {
							detail: Object.freeze({
								transitionId: state.transitionId,
								fromGenerationId: state.active.generationId,
								toGenerationId: state.pending.generationId
							})
						})
					);
					await prepareReload(state, participantId, participants);
				}
				return;
			}
			if (
				state.active.generationId !== loadedGenerationId ||
				(preparing !== undefined && devTransportDisconnected)
			) {
				blocked = true;
				if (!reloadRequested) {
					reloadRequested = true;
					// A rejected replacement can restart the previous API too. Restore
					// the prepared state into that verified rollback generation.
					markCapsuleRestoring(
						state.active.generationId === loadedGenerationId ? state.active : undefined
					);
					window.location.reload();
				}
				return;
			}
			preparing = undefined;
			blocked = false;
			await queueRestoration(state.active);
		} catch {
			// A missing/failed lifecycle side channel cannot authorize dispatch
			// during an already-observed transition.
			if (preparing !== undefined) blocked = true;
		} finally {
			if (!destroyed) timer = setTimeout(() => void poll(), LIFECYCLE_POLL_INTERVAL_MS);
		}
	};

	void poll();
	return Object.freeze({
		deferDevTransportReload(): Promise<void> {
			if (preparing === undefined && !reloadRequested) return Promise.resolve();
			devTransportDisconnected = true;
			// Vite awaits its disconnect listeners before its automatic reload.
			// The lifecycle poll keeps running and owns the one navigation after
			// activation (or verified rollback); page teardown releases this wait.
			return new Promise((resolve) => deferredTransports.add(resolve));
		},
		assertDispatchOpen(): void {
			if (blocked) throw new Error('coherent application reload is in progress');
		},
		register(participant: ReloadParticipant): () => void {
			participants.set(participant.key, participant);
			void queueRestoration().catch(() => undefined);
			return () => {
				if (participants.get(participant.key) === participant) {
					participants.delete(participant.key);
				}
			};
		},
		destroy(): void {
			destroyed = true;
			if (timer !== undefined) clearTimeout(timer);
			participants.clear();
			for (const resolve of deferredTransports) resolve();
			deferredTransports.clear();
		}
	});
}

async function prepareReload(
	state: LifecycleDevState,
	participantId: string,
	participants: ReadonlyMap<string, ReloadParticipant>
): Promise<void> {
	try {
		const now = Date.now();
		const capsule: ReloadCapsule = Object.freeze({
			version: 1,
			transitionId: state.transitionId!,
			from: state.active,
			to: state.pending!,
			location: validateDistributedReloadLocation(new URL(window.location.href)),
			createdAtUnixMs: now,
			expiresAtUnixMs: Math.max(
				now + CAPSULE_MIN_LIFETIME_MS,
				state.deadlineUnixMs! + CAPSULE_ACTIVATION_GRACE_MS
			),
			phase: 'prepared',
			participants: Object.freeze(
				[...participants.values()]
					.sort((left, right) => left.key.localeCompare(right.key))
					.map((participant) => Object.freeze({ key: participant.key, ...participant.prepare() }))
			)
		});
		storeCapsule(capsule);
		await acknowledge(state.transitionId!, participantId, true);
	} catch (error) {
		const message = error instanceof Error ? error.message : 'unknown preparation failure';
		console.error('Distributed reload preparation failed:', message);
		window.dispatchEvent(new CustomEvent('distributed:reload-prepare-failed', {
			detail: Object.freeze({ transitionId: state.transitionId!, message })
		}));
		await acknowledge(state.transitionId!, participantId, false);
	}
}

async function acknowledge(
	transitionId: string,
	participantId: string,
	ok: boolean
): Promise<void> {
	const response = await fetch(STATE_ENDPOINT, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		credentials: 'same-origin',
		body: JSON.stringify({ transitionId, participantId, ok })
	});
	if (!response.ok) throw new Error(`lifecycle acknowledgement returned ${response.status}`);
}

async function restoreAvailableParticipants(
	participants: ReadonlyMap<string, ReloadParticipant>,
	active?: LifecycleGeneration
): Promise<void> {
	const capsule = readCapsule();
	if (
		capsule === undefined ||
		capsule.phase !== 'restoring' ||
		(active !== undefined && capsule.to.generationId !== active.generationId)
	) return;
	const remaining: ReloadParticipantCapsule[] = [];
	const compatible = capsule.from.compatibilityId === capsule.to.compatibilityId;
	for (const saved of capsule.participants) {
		const participant = participants.get(saved.key);
		if (participant === undefined) {
			remaining.push(saved);
			continue;
		}
		try {
			const restored = await withDeadline(
				Promise.resolve(participant.restore(saved, compatible)),
				Math.min(RESTORE_TIMEOUT_MS, Math.max(1, capsule.expiresAtUnixMs - Date.now()))
			);
			if (!restored) remaining.push(saved);
		} catch {
			// Incompatible partitions are intentionally dropped; the mounted
			// operation stores perform their ordinary authoritative fetch.
		}
	}
	if (remaining.length === 0) {
		sessionStorage.removeItem(CAPSULE_KEY);
	} else {
		storeCapsule(Object.freeze({ ...capsule, participants: Object.freeze(remaining) }));
	}
}

function withDeadline<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
	return new Promise((resolvePromise, reject) => {
		const timer = setTimeout(
			() => reject(new Error('Distributed reload restoration timed out')),
			timeoutMs
		);
		operation.then(
			(value) => {
				clearTimeout(timer);
				resolvePromise(value);
			},
			(error) => {
				clearTimeout(timer);
				reject(error);
			}
		);
	});
}

function storeCapsule(capsule: ReloadCapsule): void {
	assertSafeSerializable(capsule, 'reloadCapsule', 0, new Set(), false);
	const encoded = JSON.stringify(capsule);
	if (new TextEncoder().encode(encoded).length > MAX_CAPSULE_BYTES) {
		throw new TypeError('Distributed reload capsule exceeds 1 MiB');
	}
	sessionStorage.setItem(CAPSULE_KEY, encoded);
}

function readCapsule(): ReloadCapsule | undefined {
	const encoded = sessionStorage.getItem(CAPSULE_KEY);
	if (encoded === null || new TextEncoder().encode(encoded).length > MAX_CAPSULE_BYTES) return undefined;
	try {
		const value = JSON.parse(encoded) as ReloadCapsule;
		if (value.version !== 1 || value.expiresAtUnixMs < Date.now()) {
			sessionStorage.removeItem(CAPSULE_KEY);
			return undefined;
		}
		return value;
	} catch {
		sessionStorage.removeItem(CAPSULE_KEY);
		return undefined;
	}
}

function markCapsuleRestoring(rollback?: LifecycleGeneration): void {
	const capsule = readCapsule();
	if (capsule !== undefined) storeCapsule(Object.freeze({ ...capsule, to: rollback ?? capsule.to, phase: 'restoring' }));
}

function parseLifecycleState(value: unknown): LifecycleDevState {
	const state = object(value, 'lifecycle');
	if (state.schemaVersion !== 1 || (state.phase !== 'active' && state.phase !== 'preparing')) {
		throw new TypeError('invalid Distributed lifecycle state');
	}
	const active = parseLifecycleGeneration(state.active, 'lifecycle.active');
	const pending = state.pending === undefined
		? undefined
		: parseLifecycleGeneration(state.pending, 'lifecycle.pending');
	const transitionId = state.transitionId === undefined
		? undefined
		: identity(state.transitionId, 'lifecycle.transitionId');
	const deadlineUnixMs = state.deadlineUnixMs === undefined
		? undefined
		: finiteInteger(state.deadlineUnixMs, 'lifecycle.deadlineUnixMs');
	if (state.phase === 'preparing' && (pending === undefined || transitionId === undefined || deadlineUnixMs === undefined)) {
		throw new TypeError('preparing lifecycle state is incomplete');
	}
	return Object.freeze({
		schemaVersion: 1,
		phase: state.phase,
		active,
		...(pending === undefined ? {} : { pending }),
		...(transitionId === undefined ? {} : { transitionId }),
		...(deadlineUnixMs === undefined ? {} : { deadlineUnixMs })
	});
}

function parseLifecycleGeneration(value: unknown, path: string): LifecycleGeneration {
	const generation = object(value, path);
	return Object.freeze({
		generationId: identity(generation.generationId, `${path}.generationId`),
		releaseId: identity(generation.releaseId, `${path}.releaseId`),
		topologyId: identity(generation.topologyId, `${path}.topologyId`),
		compatibilityId: identity(generation.compatibilityId, `${path}.compatibilityId`)
	});
}

function browserParticipantId(): string {
	const key = '@hops-ops/distributed/reload-participant/v1';
	const existing = sessionStorage.getItem(key);
	if (existing !== null && PARTICIPANT_ID.test(existing)) return existing;
	// `crypto.randomUUID()` is restricted to secure contexts, while
	// `getRandomValues()` is intentionally available on ordinary HTTP origins.
	// Local GitOps and LAN development commonly serve Vite through an HTTP
	// service hostname, so requiring randomUUID would silently disable the
	// lifecycle client there.
	const bytes = crypto.getRandomValues(new Uint8Array(16));
	const created = [...bytes]
		.map((byte) => byte.toString(16).padStart(2, '0'))
		.join('');
	sessionStorage.setItem(key, created);
	return created;
}

function documentGenerationId(): string | undefined {
	if (typeof document === 'undefined') return undefined;
	const candidate = document
		.querySelector(`meta[name="${GENERATION_META}"]`)
		?.getAttribute('content');
	if (candidate === null || candidate === undefined) return undefined;
	try {
		return identity(candidate, 'document generation');
	} catch {
		return undefined;
	}
}

function inertLifecycle(): DistributedReloadLifecycle {
	return Object.freeze({
		async deferDevTransportReload(): Promise<void> {},
		assertDispatchOpen(): void {},
		register(): () => void {
			return () => undefined;
		},
		destroy(): void {}
	});
}

function object(value: unknown, path: string): Record<string, unknown> {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) {
		throw new TypeError(`${path} must be an object`);
	}
	return value as Record<string, unknown>;
}

function identity(value: unknown, path: string): string {
	if (typeof value !== 'string' || value.length === 0 || value.length > 512 || value !== value.trim() || /[\u0000-\u001f\u007f]/.test(value)) {
		throw new TypeError(`${path} must be a bounded stable identity`);
	}
	return value;
}

function finiteInteger(value: unknown, path: string): number {
	if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
		throw new TypeError(`${path} must be a non-negative safe integer`);
	}
	return value;
}

function assertSafeSerializable(
	value: unknown,
	path: string,
	depth: number,
	seen: Set<object>,
	rejectSecretKeys: boolean
): void {
	if (depth > MAX_STATE_DEPTH) throw new TypeError(`${path} exceeds maximum depth`);
	if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
	if (typeof value === 'number' && Number.isFinite(value)) return;
	if (typeof value !== 'object') throw new TypeError(`${path} is not JSON serializable`);
	if (seen.has(value)) throw new TypeError(`${path} contains a cycle`);
	seen.add(value);
	if (Array.isArray(value)) {
		for (let index = 0; index < value.length; index += 1) {
			assertSafeSerializable(value[index], `${path}[${index}]`, depth + 1, seen, rejectSecretKeys);
		}
	} else {
		for (const [key, child] of Object.entries(value)) {
			if (rejectSecretKeys && SECRET_KEY.test(key)) throw new TypeError(`${path}.${key} is secret-like state`);
			assertSafeSerializable(child, `${path}.${key}`, depth + 1, seen, rejectSecretKeys);
		}
	}
	seen.delete(value);
}
