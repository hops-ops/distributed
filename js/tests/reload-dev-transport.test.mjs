import assert from 'node:assert/strict';
import test from 'node:test';

for (const outcome of ['activation', 'rollback', 'unplanned disconnect']) {
	test(`dev reconnect ownership during ${outcome}`, async () => {
		// Each case models a fresh browser page with its own lifecycle singleton.
		const { distributedReloadLifecycle, registerDistributedReloadClient } =
			await import(`../dist/sveltekit/lifecycle.js?${outcome}`);
		const previous = Object.fromEntries(
			['window', 'document', 'sessionStorage', 'CustomEvent', 'fetch']
				.map((key) => [key, globalThis[key]])
		);
		const values = new Map();
		const capsuleKey = '@hops-ops/distributed/reload-capsule/v1';
		const generation = (generationId) => ({
			generationId, releaseId: `release-${generationId}`,
			topologyId: 'topology', compatibilityId: 'compatible'
		});
		let state = {
			schemaVersion: 1, phase: 'preparing', active: generation('old'),
			pending: generation('next'), transitionId: 'transition-reconnect',
			deadlineUnixMs: Date.now() + 10_000
		};
		if (outcome === 'unplanned disconnect') state = { schemaVersion: 1, phase: 'active', active: generation('old') };
		let reloads = 0;
		globalThis.sessionStorage = {
			getItem: (key) => values.get(key) ?? null,
			setItem: (key, value) => values.set(key, String(value)),
			removeItem: (key) => values.delete(key)
		};
		globalThis.document = { querySelector: () => ({ getAttribute: () => 'old' }) };
		globalThis.window = {
			location: { href: 'http://localhost:8791/todos', reload() { reloads++; } },
			dispatchEvent() {}
		};
		globalThis.CustomEvent = class { constructor(type, options) { this.type = type; this.detail = options?.detail; } };
		globalThis.fetch = async (_url, options) => options?.method === 'POST'
			? { status: 204, ok: true }
			: { status: 200, ok: true, json: async () => state };
		const waitFor = async (condition) => {
			const deadline = Date.now() + 3_000;
			while (!condition()) {
				assert.ok(Date.now() < deadline, 'lifecycle did not reach expected state');
				await new Promise((resolve) => setTimeout(resolve, 10));
			}
		};
		const unregister = registerDistributedReloadClient(
			{ scope: undefined, dehydrate: () => ({}), hydrate: () => false },
			undefined, { key: 'surface' }
		);
		const lifecycle = distributedReloadLifecycle();
		try {
			if (outcome === 'unplanned disconnect') {
				await lifecycle.deferDevTransportReload();
				lifecycle.assertDispatchOpen();
				assert.equal(reloads, 0);
				assert.equal(values.has(capsuleKey), false);
				return;
			}
			await waitFor(() => values.has(capsuleKey));
			let viteResumed = false;
			const deferred = lifecycle.deferDevTransportReload().then(() => { viteResumed = true; });
			await Promise.resolve();
			assert.equal(viteResumed, false);
			assert.throws(() => lifecycle.assertDispatchOpen(), /reload/);
			const target = outcome === 'activation' ? 'next' : 'old';
			state = { schemaVersion: 1, phase: 'active', active: generation(target) };
			await waitFor(() => reloads === 1);
			const capsule = JSON.parse(values.get(capsuleKey));
			assert.equal(capsule.phase, 'restoring');
			assert.equal(capsule.to.generationId, target);
			assert.equal(viteResumed, false, 'Vite must not race the controlled navigation');
			lifecycle.destroy();
			await deferred;
		} finally {
			unregister();
			lifecycle.destroy();
			for (const [key, value] of Object.entries(previous)) {
				if (value === undefined) delete globalThis[key];
				else globalThis[key] = value;
			}
		}
	});
}
