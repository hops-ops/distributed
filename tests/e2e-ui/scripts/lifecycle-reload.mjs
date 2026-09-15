import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { waitForProjectedTodo } from './lifecycle-command-proof.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const frameworkRoot = resolve(root, '../../js');
const application = resolve(root, 'crates/todo-domain/src/commands/force_archive.rs');
const clientPage = resolve(root, 'ui/src/routes/todos/+page.svelte');
const framework = resolve(frameworkRoot, 'src/sveltekit/lifecycle.ts');
const lifecycleFile = resolve(root, '.distributed/lifecycle/dev.json');
const baseURL = process.env.PUBLIC_ORIGIN || process.env.E2E_UI_ORIGIN || 'http://localhost:8791';
const apiURL = process.env.E2E_API_ORIGIN || 'http://127.0.0.1:8791';
const timeoutMs = 120_000;
const lifecycleBuildTimeoutMs = 300_000;
const preparingEvents = [];
const participantStorageKey = '@hops-ops/distributed/reload-participant/v1';
const participantIdPattern = /^[A-Za-z0-9_-]{16,128}$/;
const kubeWorkload = process.env.DISTRIBUTED_LIFECYCLE_KUBE_WORKLOAD;
const kubeContext = process.env.DISTRIBUTED_LIFECYCLE_KUBE_CONTEXT;
const kubeNamespace = process.env.DISTRIBUTED_LIFECYCLE_KUBE_NAMESPACE || 'default';
const kubeContainer = process.env.DISTRIBUTED_LIFECYCLE_KUBE_CONTAINER || 'application';

function childPath(base, path) {
	const child = relative(base, path);
	return child !== '' && child !== '..' && !child.startsWith(`..${sep}`) && !isAbsolute(child)
		? child
		: undefined;
}

function remotePath(path) {
	const projectChild = childPath(root, path);
	if (projectChild) return `/workspace/tests/e2e-ui/${projectChild.split(sep).join('/')}`;
	const frameworkChild = childPath(frameworkRoot, path);
	if (frameworkChild) return `/workspace/js/${frameworkChild.split(sep).join('/')}`;
	throw new Error(`lifecycle fixture path escapes its declared roots: ${path}`);
}

function kubectlExec(command, input) {
	const args = [];
	if (kubeContext) args.push('--context', kubeContext);
	args.push(
		'-n',
		kubeNamespace,
		'exec',
		kubeWorkload,
		'-c',
		kubeContainer,
		...(input === undefined ? [] : ['-i']),
		'--',
		...command
	);
	const result = spawnSync('kubectl', args, {
		encoding: 'utf8',
		input,
		maxBuffer: 4 * 1024 * 1024
	});
	if (result.status !== 0) {
		throw new Error(
			`kubectl exec failed (${result.status ?? 'signal'}): ${(result.stderr || '').trim()}`
		);
	}
	return result.stdout;
}

const fixtureIO = kubeWorkload
	? {
			read(path) {
				return kubectlExec(['cat', remotePath(path)]);
			},
			write(path, source) {
				kubectlExec(['tee', remotePath(path)], source);
			},
			pollMs: 250
		}
	: {
			read(path) {
				return readFileSync(path, 'utf8');
			},
			write(path, source) {
				writeFileSync(path, source);
			},
			pollMs: 50
		};
const original = fixtureIO.read(application);
const frameworkOriginal = fixtureIO.read(framework);
const clientOriginal = fixtureIO.read(clientPage);

async function waitFor(predicate, label, timeout = timeoutMs) {
	const started = Date.now();
	while (Date.now() - started < timeout) {
		const value = await predicate();
		if (value) return value;
		await new Promise((resolvePromise) => setTimeout(resolvePromise, fixtureIO.pollMs));
	}
	throw new Error(`timed out waiting for ${label}`);
}

async function lifecycleState() {
	try {
		return JSON.parse(fixtureIO.read(lifecycleFile));
	} catch {
		return undefined;
	}
}

function activeAdminCommands(state) {
	const generationId = state?.active?.generationId;
	if (!/^sha256:[0-9a-f]{64}$/.test(generationId ?? '')) {
		throw new Error('active lifecycle generation ID is invalid');
	}
	return resolve(
		root,
		'.distributed/lifecycle/generations',
		generationId,
		'ui/src/lib/generated/admin/commands.ts'
	);
}

async function waitForBrowserParticipant(page) {
	const participantId = await waitFor(
		() => page.evaluate((key) => sessionStorage.getItem(key), participantStorageKey),
		'browser lifecycle participant ID'
	);
	assert.match(participantId, participantIdPattern);
	const heartbeatFile = resolve(
		root,
		'.distributed/lifecycle/dev-control/participants',
		`${participantId}.json`
	);
	await waitFor(() => {
		try {
			const heartbeat = JSON.parse(fixtureIO.read(heartbeatFile));
			return Number.isFinite(heartbeat.seenAtUnixMs) &&
				Date.now() - heartbeat.seenAtUnixMs < 5_000;
		} catch {
			return false;
		}
	}, 'fresh browser lifecycle participant heartbeat');
}

async function transition(page, path, source, expectedReplicaRestore, assertGate = false) {
	const before = await lifecycleState();
	assert.equal(before.phase, 'active');
	await page.evaluate(() => {
		globalThis.__distributedReloadEvents = [];
	});
	preparingEvents.length = 0;
	const navigations = [];
	const onNavigation = (frame) => {
		if (frame === page.mainFrame()) navigations.push(frame.url());
	};
	page.on('framenavigated', onNavigation);
	let mutationRequests = 0;
	const documentRequests = [];
	const onRequest = (request) => {
		if (request.resourceType() === 'document') documentRequests.push(request.url());
		if ((request.postData() ?? '').includes('todos_create')) mutationRequests += 1;
	};
	page.on('request', onRequest);
	fixtureIO.write(path, source);

	if (assertGate) {
		try {
			await waitFor(
				async () => (await lifecycleState())?.phase === 'preparing',
				'pending lifecycle generation',
				lifecycleBuildTimeoutMs
			);
		} catch (error) {
			throw new Error(
				`${error.message}; last lifecycle state=${JSON.stringify(await lifecycleState())}`,
				{ cause: error }
			);
		}
		await waitFor(() => preparingEvents.length > 0, 'browser command gate');
		await page.locator('#todo-title').fill(`must-not-dispatch-${Date.now()}`);
		await page.getByRole('button', { name: /^add$/i }).click();
		await page.getByText(/paused during a coherent application reload/i).waitFor();
		assert.equal(mutationRequests, 0, 'reload gate must reject before GraphQL dispatch');
		const readable = await fetch(`${apiURL}/graphql`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ query: 'query ReloadReadProbe { __typename }' })
		});
		assert.equal(readable.status, 200, 'pending API must continue serving GraphQL reads');
		const direct = await fetch(`${apiURL}/graphql`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ query: 'mutation ReloadGateProbe { __typename }' })
		});
		assert.equal(direct.status, 503, 'pending API must reject direct GraphQL dispatch');
		assert.equal(
			(await direct.json()).errors[0].extensions.code,
			'APPLICATION_RELOADING'
		);
	}

	const restored = await waitFor(
		async () => {
			if (page.isClosed()) return undefined;
			return page.evaluate(() => {
				const state = globalThis;
				return state.__distributedReloadEvents?.at(-1);
			}).catch(() => undefined);
		},
		'controlled browser reload restoration',
		lifecycleBuildTimeoutMs
	).catch(async (error) => {
		const browserState = await page.evaluate(() => {
			const capsule = JSON.parse(sessionStorage.getItem('@hops-ops/distributed/reload-capsule/v1') || 'null');
			return {
				generation: document.querySelector('meta[name="distributed-generation"]')?.getAttribute('content'),
				capsulePhase: capsule?.phase,
				capsuleTarget: capsule?.to?.generationId,
				restoredEvents: globalThis.__distributedReloadEvents?.length
			};
		}).catch(() => undefined);
		throw new Error(`${error.message}; browser=${JSON.stringify(browserState)}`, { cause: error });
	});
	assert.equal(restored.replicaCaptured, true, 'authenticated replica must participate');
	assert.equal(restored.replicaRestored, expectedReplicaRestore);
	assert.equal(
		documentRequests.length,
		1,
		`coherent transition must request exactly one document: frames=${JSON.stringify(navigations)} documents=${JSON.stringify(documentRequests)}`
	);
	assert.equal(new URL(page.url()).hash, '#reload-proof');
	assert.equal(
		await page.evaluate(() => globalThis.__distributedReloadState?.value),
		'preserve-me'
	);
	const after = await lifecycleState();
	assert.equal(after.phase, 'active');
	assert.notEqual(after.active.generationId, before.active.generationId);
	if (expectedReplicaRestore) {
		assert.equal(after.active.compatibilityId, before.active.compatibilityId);
	} else {
		assert.notEqual(after.active.compatibilityId, before.active.compatibilityId);
	}
	page.off('framenavigated', onNavigation);
	page.off('request', onRequest);
	// Readability alone does not prove the retained API reopened its write gate.
	const title = `after-reload-${Date.now()}`;
	const response = page.waitForResponse((result) => result.request().method() === 'POST' &&
		(result.request().postData() ?? '').includes('todos_create'));
	await page.locator('#todo-title').fill(title);
	await page.getByRole('button', { name: /^add$/i }).click();
	const mutation = await response;
	assert.equal(mutation.status(), 200, 'active generation must accept commands');
	const body = await mutation.json();
	assert.ok(!body.errors?.length);
	assert.equal(body.extensions.distributed.generation.generationId, after.active.generationId);
	assert.equal(body.extensions.distributed.generation.releaseId, after.active.releaseId);
	assert.ok(body.extensions.distributed.command, 'actual command receipt required');
	const todo = body.data.todos_create;
	assert.equal(todo.title, title);
	assert.equal(typeof todo.todo_id, 'string');
	const requestHeaders = await mutation.request().allHeaders();
	const headers = Object.fromEntries(
		['authorization', 'x-user-id', 'x-roles']
			.filter((name) => requestHeaders[name] !== undefined)
			.map((name) => [name, requestHeaders[name]])
	);
	const attempts = await waitForProjectedTodo(async (remainingMs) => {
		const result = await page.request.post(mutation.url(), {
			headers,
			timeout: remainingMs,
			data: {
				query: `query ReloadTodoProof($id: String!) {
					todos(where: { todo_id: { _eq: $id } }, limit: 1) {
						todo_id owner_id title status
					}
				}`,
				variables: { id: todo.todo_id }
			}
		});
		assert.equal(result.status(), 200, 'authoritative Todo query must succeed');
		return result.json();
	}, todo, timeoutMs);
	console.log(`lifecycle-reload: ${relative(root, path)} command accepted and projected (${attempts} queries)`);
	// @load is not @live. Reloading before projection can seed an empty result
	// that never changes, even though the projector subsequently commits the row.
	// Now discard browser optimism and prove fresh SSR + hydration independently.
	await page.reload({waitUntil: 'domcontentloaded'});
	await page.locator('[data-todo-id]').filter({hasText: title}).waitFor({timeout: timeoutMs});
	console.log(`lifecycle-reload: ${relative(root, path)} fresh page confirmed ${todo.todo_id}`);
	await waitFor(() => page.evaluate(() => globalThis.__distributedReloadState !== undefined), 'hydration after command proof');
	await page.evaluate(() => { globalThis.__distributedReloadState.value = 'preserve-me'; });
	await waitForBrowserParticipant(page);
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
	storageState: process.env.E2E_RELOAD_STORAGE_STATE || resolve(root, 'e2e/.auth/alice.json')
});
await context.addInitScript(() => {
	globalThis.__distributedReloadEvents = [];
	addEventListener('distributed:reload-restored', (event) => {
		globalThis.__distributedReloadEvents.push(event.detail);
	});
	addEventListener('distributed:reload-preparing', (event) => {
		void globalThis.__distributedRecordReloadPreparing(event.detail);
	});
});
const page = await context.newPage();
await page.exposeFunction('__distributedRecordReloadPreparing', (detail) => {
	preparingEvents.push(detail);
});
page.on('console', (message) => {
	if (message.type() === 'error' || message.text().includes('Distributed reload')) {
		console.error(`browser console ${message.type()}: ${message.text()}`);
	}
});
page.on('pageerror', (error) => console.error(`browser page error: ${error.message}`));
const baseline = await lifecycleState();
assert.equal(baseline?.phase, 'active');
try {
	await page.goto(`${baseURL}/todos#reload-proof`, { waitUntil: 'domcontentloaded' });
	await page.getByRole('heading', { name: /todos/i }).waitFor();
	await waitFor(
		() => page.evaluate(() => globalThis.__distributedReloadState !== undefined),
		'client hydration'
	);
	await waitForBrowserParticipant(page);
	await page.evaluate(() => {
		globalThis.__distributedReloadState.value = 'preserve-me';
	});
	for (const suffix of ['first', 'second']) {
		await transition(page, clientPage, `${clientOriginal}\n<!-- client-only reload ${suffix} -->\n`, true);
		assert.deepEqual((await lifecycleState()).members.api, baseline.members.api,
			'client-only reload must retain the same verified API instance');
	}

	const compatible = `${original}\n// lifecycle-compatible source-only rebuild\n`;
	await transition(page, application, compatible, true, true);

	const frameworkCompatible = `${frameworkOriginal}\n// lifecycle-compatible framework rebuild\n`;
	await transition(page, framework, frameworkCompatible, true);

	const incompatible = compatible.replace(
		'field: "todos_force_archive",',
		'field: "todos_force_archive_reload",'
	);
	assert.notEqual(incompatible, compatible, 'incompatible fixture edit must apply');
	await transition(page, application, incompatible, false);
	console.log('lifecycle-reload: client-only + application + framework + incompatible transitions accept commands');
} finally {
	fixtureIO.write(application, original);
	fixtureIO.write(framework, frameworkOriginal);
	fixtureIO.write(clientPage, clientOriginal);
	await waitFor(async () => {
		const state = await lifecycleState();
		return state?.phase === 'active' &&
			state.active.generationId === baseline.active.generationId &&
			!fixtureIO.read(activeAdminCommands(state)).includes('todos_force_archive_reload');
	}, 'baseline source and generated-client restoration', lifecycleBuildTimeoutMs);
	await context.close();
	await browser.close();
}
