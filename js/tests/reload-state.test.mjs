import assert from 'node:assert/strict';
import test from 'node:test';

import {
	distributedReloadLifecycle,
	registerDistributedReloadClient,
	validateDistributedReloadLocation,
	validateDistributedReloadState
} from '../dist/sveltekit/index.js';

test('reload state accepts finite declared JSON and rejects secret-like keys', () => {
	const state = { panel: 'todos', filters: ['open'], scroll: 42 };
	assert.equal(validateDistributedReloadState(state), state);
	for (const key of ['accessToken', 'password', 'authorization', 'session_cookie']) {
		assert.throws(() => validateDistributedReloadState({ [key]: 'must-not-cross' }));
	}
});

test('reload location preserves ordinary routing and rejects auth callback material', () => {
	assert.equal(
		validateDistributedReloadLocation(new URL('https://app.test/todos?status=open#mine')),
		'/todos?status=open#mine'
	);
	for (const query of ['code=oauth-code', 'access_token=jwt', 'session=value', 'state=csrf']) {
		assert.throws(() =>
			validateDistributedReloadLocation(new URL(`https://app.test/callback?${query}`))
		);
	}
	assert.throws(() =>
		validateDistributedReloadLocation(
			new URL('https://app.test/callback#access_token=jwt&token_type=bearer')
		)
	);
});

test('reload state rejects cycles, excessive depth, and values over one MiB', () => {
	const cyclic = {};
	cyclic.self = cyclic;
	assert.throws(() => validateDistributedReloadState(cyclic));
	let deep = 'leaf';
	for (let index = 0; index < 34; index += 1) deep = { next: deep };
	assert.throws(() => validateDistributedReloadState(deep));
	assert.throws(() => validateDistributedReloadState('x'.repeat(1024 * 1024)));
});

test('reload waits for replica authority and a resumed stale document reloads', async () => {
	const capsuleKey = '@hops-ops/distributed/reload-capsule/v1';
	const values = new Map();
	const previous = {
		window: globalThis.window,
		document: globalThis.document,
		sessionStorage: globalThis.sessionStorage,
		CustomEvent: globalThis.CustomEvent,
		fetch: globalThis.fetch
	};
	let activeGeneration = 'generation-b';
	let reloads = 0;
	let hydrations = 0;
	let scope;
	const generation = (generationId) => ({
		generationId,
		releaseId: `release-${generationId}`,
		topologyId: 'topology-1',
		compatibilityId: 'compatible-1'
	});
	values.set(capsuleKey, JSON.stringify({
		version: 1,
		transitionId: 'transition-restore-1',
		from: generation('generation-a'),
		to: generation('generation-b'),
		location: '/todos',
		createdAtUnixMs: Date.now(),
		expiresAtUnixMs: Date.now() + 150_000,
		phase: 'restoring',
		participants: [{
			key: 'public-surface',
			replica: { records: [] },
			pendingCommandIds: ['pending-command'],
			state: []
		}]
	}));
	globalThis.sessionStorage = {
		getItem: (key) => values.get(key) ?? null,
		setItem: (key, value) => values.set(key, String(value)),
		removeItem: (key) => values.delete(key)
	};
	globalThis.document = {
		querySelector: () => ({ getAttribute: () => 'generation-b' })
	};
	globalThis.window = {
		location: {
			href: 'http://localhost:5180/todos',
			reload: () => { reloads += 1; }
		},
		dispatchEvent() {}
	};
	globalThis.CustomEvent = class {
		constructor(type, options) {
			this.type = type;
			this.detail = options?.detail;
		}
	};
	globalThis.fetch = async () => ({
		status: 200,
		ok: true,
		json: async () => ({
			schemaVersion: 1,
			phase: 'active',
			active: generation(activeGeneration)
		})
	});
	const waitFor = async (condition, message) => {
		const deadline = Date.now() + 3_500;
		while (!condition()) {
			if (Date.now() >= deadline) throw new Error(message);
			await new Promise((resolve) => setTimeout(resolve, 25));
		}
	};
	const replica = {
		get scope() { return scope; },
		dehydrate() { return { records: [] }; },
		hydrate() {
			hydrations += 1;
			return true;
		}
	};
	const recoveryOrder = [];
	const unregister = registerDistributedReloadClient(replica, {
		async preload() { recoveryOrder.push('preload'); },
	}, {
		key: 'public-surface',
		async recoverPendingCommands(ids) {
			assert.deepEqual(ids, ['pending-command']);
			recoveryOrder.push('recover');
		}
	});
	try {
		await new Promise((resolve) => setTimeout(resolve, 50));
		assert.equal(hydrations, 0);
		assert.notEqual(values.get(capsuleKey), undefined);

		scope = { tenant: 'tenant-1', roles: [] };
		await waitFor(() => hydrations === 1, 'replica restoration was not retried');
		assert.equal(values.get(capsuleKey), undefined);
		assert.deepEqual(recoveryOrder, ['preload', 'recover']);

		activeGeneration = 'generation-c';
		await waitFor(() => reloads === 1, 'stale document did not reload after activation');
	} finally {
		unregister();
		distributedReloadLifecycle().destroy();
		for (const [key, value] of Object.entries(previous)) {
			if (value === undefined) delete globalThis[key];
			else globalThis[key] = value;
		}
	}
});
