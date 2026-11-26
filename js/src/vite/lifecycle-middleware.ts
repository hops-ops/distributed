/**
 * The `distributed dev` lifecycle side of a Vite server: the generation hint
 * in the page head, the `/__distributed/lifecycle` control endpoint, and the
 * active-generation pointers. Every function is inert outside the lifecycle
 * (`DISTRIBUTED_LIFECYCLE_DIR` unset).
 */
import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { lstat, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { isAbsolute, join, resolve } from 'node:path';

import { isWithin } from '../lib/node-fs.js';
import type {
	LifecycleHtmlTag,
	ViteHotContextLike,
	ViteMiddlewareRequest,
	ViteMiddlewareResponse,
	ViteServerLike
} from './types.js';

const SHA256_ID = /^sha256:[0-9a-f]{64}$/;
const GENERATION_META = 'distributed-generation';
const MAX_LIFECYCLE_STATE_BYTES = 1024 * 1024;

export function localFrameworkDist(root: string): string | undefined {
	try {
		const candidate = join(
			root,
			'node_modules',
			'@hops-ops',
			'distributed',
			'dist'
		);
		return existsSync(candidate) ? realpathSync(candidate) : undefined;
	} catch {
		return undefined;
	}
}

export function suppressFrameworkHotUpdate(
	context: ViteHotContextLike,
	frameworkDist: string | undefined
): never[] | undefined {
	if (
		process.env.DISTRIBUTED_LIFECYCLE_DIR === undefined ||
		frameworkDist === undefined ||
		!isWithin(frameworkDist, canonicalExistingPath(context.file))
	) return undefined;
	for (const module of context.modules ?? []) {
		context.server.moduleGraph.invalidateModule(module);
	}
	return [];
}

export function canonicalExistingPath(value: string): string {
	try {
		return realpathSync(value);
	} catch {
		return resolve(value);
	}
}

const CONTROL_ID = /^[A-Za-z0-9_:-]{16,128}$/;
const MAX_ACK_BYTES = 4096;

export function lifecycleGenerationMeta(): LifecycleHtmlTag[] {
	const configured = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	if (configured === undefined || !isAbsolute(configured)) return [];
	try {
		const encoded = readFileSync(join(resolve(configured), 'dev.json'));
		if (encoded.byteLength > MAX_LIFECYCLE_STATE_BYTES) return [];
		const state = JSON.parse(encoded.toString('utf8')) as {
			active?: { generationId?: unknown };
		};
		const generationId = state.active?.generationId;
		if (
			typeof generationId !== 'string' ||
			generationId.length === 0 ||
			generationId.length > 512 ||
			generationId !== generationId.trim() ||
			/[\u0000-\u001f\u007f]/.test(generationId)
		) return [];
		return [Object.freeze({
			tag: 'meta',
			attrs: Object.freeze({ name: GENERATION_META, content: generationId }),
			injectTo: 'head-prepend'
		})];
	} catch {
		// A lifecycle file is atomically replaced. Missing or malformed state
		// simply omits the hint; the browser falls back to its first poll.
		return [];
	}
}

export function configureLifecycleServer(server: ViteServerLike): void {
	const configured = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	if (configured === undefined || !isAbsolute(configured)) return;
	const lifecycleRoot = resolve(configured);
	server.middlewares?.use('/__distributed/lifecycle', (request, response) => {
		void handleLifecycleRequest(lifecycleRoot, request, response).catch(() => {
			if (response.statusCode < 400) response.statusCode = 500;
			response.end();
		});
	});
}

async function handleLifecycleRequest(
	lifecycleRoot: string,
	request: ViteMiddlewareRequest,
	response: ViteMiddlewareResponse
): Promise<void> {
	response.setHeader('cache-control', 'no-store');
	if (request.method === 'GET') {
		const participant = singleHeader(request.headers['x-distributed-participant']);
		if (participant !== undefined && CONTROL_ID.test(participant)) {
			await writeParticipantHeartbeat(lifecycleRoot, participant).catch(
				() => undefined
			);
		}
		const state = await readLifecycleState(lifecycleRoot);
		if (state === undefined) {
			response.statusCode = 404;
			response.end();
			return;
		}
		response.statusCode = 200;
		response.setHeader('content-type', 'application/json');
		response.end(state);
		return;
	}
	if (request.method === 'POST') {
		const contentType = singleHeader(request.headers['content-type']);
		if (contentType?.split(';', 1)[0]?.trim().toLowerCase() !== 'application/json') {
			response.statusCode = 415;
			response.end();
			return;
		}
		if (!sameOriginLifecycleRequest(request)) {
			response.statusCode = 403;
			response.end();
			return;
		}
		const body = JSON.parse(await readRequestBody(request, MAX_ACK_BYTES)) as Record<string, unknown>;
		if (
			!CONTROL_ID.test(String(body.transitionId ?? '')) ||
			!CONTROL_ID.test(String(body.participantId ?? '')) ||
			typeof body.ok !== 'boolean'
		) {
			response.statusCode = 400;
			response.end();
			return;
		}
		const stateSource = await readLifecycleState(lifecycleRoot);
		const state = stateSource === undefined ? undefined : JSON.parse(stateSource) as Record<string, unknown>;
		if (state?.phase !== 'preparing' || state.transitionId !== body.transitionId) {
			response.statusCode = 409;
			response.end();
			return;
		}
		await writeControlJson(
			join(lifecycleRoot, 'dev-control', 'acks', String(body.transitionId)),
			`${String(body.participantId)}.json`,
			{ ok: body.ok }
		);
		response.statusCode = 204;
		response.end();
		return;
	}
	response.statusCode = 405;
	response.end();
}

function writeParticipantHeartbeat(root: string, participant: string): Promise<void> {
	return writeControlJson(
		join(root, 'dev-control', 'participants'),
		`${participant}.json`,
		{ seenAtUnixMs: Date.now() }
	);
}

async function readLifecycleState(root: string): Promise<string | undefined> {
	const path = join(root, 'dev.json');
	let metadata;
	try {
		metadata = await lstat(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
		throw error;
	}
	if (metadata.isSymbolicLink() || !metadata.isFile() || metadata.size > MAX_LIFECYCLE_STATE_BYTES) {
		throw new Error('invalid Distributed lifecycle state file');
	}
	return readFile(path, 'utf8');
}

async function writeControlJson(
	directory: string,
	name: string,
	value: Readonly<Record<string, unknown>>
): Promise<void> {
	await mkdir(directory, { recursive: true });
	const temporary = join(
		directory,
		`.${name}.${process.pid}.${Date.now()}.${randomUUID()}`
	);
	await writeFile(temporary, `${JSON.stringify(value)}\n`, { flag: 'wx', mode: 0o600 });
	await rename(temporary, join(directory, name));
}

function sameOriginLifecycleRequest(request: ViteMiddlewareRequest): boolean {
	const origin = singleHeader(request.headers.origin);
	const host = singleHeader(request.headers.host);
	if (origin === undefined || host === undefined) return false;
	try {
		const parsed = new URL(origin);
		const forwarded = singleHeader(request.headers['x-forwarded-proto']);
		const socket = (request as Readonly<{
			socket?: Readonly<{ encrypted?: boolean }>;
		}>).socket;
		const protocol =
			forwarded === 'http' || forwarded === 'https'
				? `${forwarded}:`
				: socket?.encrypted === true
					? 'https:'
					: 'http:';
		return (
			parsed.protocol === protocol &&
			parsed.host === host &&
			parsed.origin === origin
		);
	} catch {
		return false;
	}
}

function singleHeader(value: string | readonly string[] | undefined): string | undefined {
	return typeof value === 'string' ? value : value?.[0];
}

function readRequestBody(
	request: ViteMiddlewareRequest,
	maximum: number
): Promise<string> {
	return new Promise((resolvePromise, reject) => {
		const chunks: Uint8Array[] = [];
		let bytes = 0;
		let exceeded = false;
		request.on('data', (chunk) => {
			if (exceeded) return;
			bytes += chunk.byteLength;
			if (bytes > maximum) {
				exceeded = true;
				chunks.length = 0;
				reject(new Error('lifecycle acknowledgement exceeds bound'));
				return;
			}
			chunks.push(chunk);
		});
		request.on('error', reject);
		request.on('end', () => {
			if (!exceeded) resolvePromise(Buffer.concat(chunks).toString('utf8'));
		});
	});
}

export function lifecycleActivePointer(): string | undefined {
	const root = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	return root !== undefined && isAbsolute(root)
		? canonicalExistingPath(join(resolve(root), 'active.json'))
		: undefined;
}

export function activeLifecycleGenerationRoot(): string {
	const lifecycleRoot = process.env.DISTRIBUTED_LIFECYCLE_DIR;
	const projectRoot = process.env.DISTRIBUTED_LIFECYCLE_PROJECT_ROOT;
	if (
		lifecycleRoot === undefined ||
		projectRoot === undefined ||
		!isAbsolute(lifecycleRoot) ||
		!isAbsolute(projectRoot)
	) {
		throw new Error(
			'Distributed lifecycle-owned client resolution requires absolute lifecycle and project roots'
		);
	}
	const requestedGeneration = process.env.DISTRIBUTED_LIFECYCLE_GENERATION_ID;
	let generation: unknown = requestedGeneration;
	if (generation === undefined) {
		const pointer = readFileSync(join(resolve(lifecycleRoot), 'active.json'));
		if (pointer.byteLength > MAX_LIFECYCLE_STATE_BYTES) {
			throw new Error('Distributed active lifecycle pointer exceeds 1 MiB');
		}
		const parsed = JSON.parse(pointer.toString('utf8')) as { generation_id?: unknown };
		generation = parsed.generation_id;
	}
	if (
		typeof generation !== 'string' ||
		!SHA256_ID.test(generation)
	) {
		throw new Error('Distributed active lifecycle pointer has an invalid generation ID');
	}
	return join(resolve(lifecycleRoot), 'generations', generation);
}
