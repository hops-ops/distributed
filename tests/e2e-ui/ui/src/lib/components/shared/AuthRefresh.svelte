<script lang="ts">
	import { browser } from '$app/environment';
	import { invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import type { SveltekitDistributedPageData } from '@hops-ops/distributed/sveltekit';

	let { onRefresh }: { onRefresh: (data: SveltekitDistributedPageData) => void } = $props();

	const MIN_REFRESH_DELAY_MS = 5_000;
	const RETRY_DELAY_MS = 30_000;
	const FALLBACK_REFRESH_SKEW_MS = 60_000;

	let refreshTimer: number | undefined;
	let retryTimer: number | undefined;

	function traceRefresh(event: Readonly<Record<string, unknown>>) {
		const maxEvents = 64;
		const diagnostics = globalThis as typeof globalThis & {
			__captureReplicaDiagnostics?: unknown;
			__distributedRefreshTrace?: unknown;
		};
		if (diagnostics.__captureReplicaDiagnostics !== true) return;
		if (!Array.isArray(diagnostics.__distributedRefreshTrace)) {
			diagnostics.__distributedRefreshTrace = [];
		}
		const trace = diagnostics.__distributedRefreshTrace as unknown[];
		trace.push({
			time: performance.now(),
			...event
		});
		if (trace.length > maxEvents) trace.splice(0, trace.length - maxEvents);
	}

	function withSessionSource<T>(origin: string, action: () => T): T {
		const diagnostics = globalThis as typeof globalThis & {
			__captureReplicaDiagnostics?: unknown;
			__distributedSessionSourceOrigin?: unknown;
		};
		if (diagnostics.__captureReplicaDiagnostics !== true) return action();
		const previous = diagnostics.__distributedSessionSourceOrigin;
		diagnostics.__distributedSessionSourceOrigin = origin;
		try {
			return action();
		} finally {
			diagnostics.__distributedSessionSourceOrigin = previous;
		}
	}

	function credentialOrdinal(value: unknown): number | undefined {
		const diagnostics = globalThis as typeof globalThis & {
			__distributedSessionCredentialOrdinal?: unknown;
		};
		return typeof diagnostics.__distributedSessionCredentialOrdinal === 'function'
			? (diagnostics.__distributedSessionCredentialOrdinal as (value: unknown) => number | undefined)(value)
			: undefined;
	}

	function clearTimers() {
		if (refreshTimer !== undefined) window.clearTimeout(refreshTimer);
		if (retryTimer !== undefined) window.clearTimeout(retryTimer);
		refreshTimer = undefined;
		retryTimer = undefined;
	}

	async function refreshSession() {
		try {
			traceRefresh({ kind: 'refresh-start' });
			const response = await fetch('/api/auth/refresh', {
				method: 'POST',
				credentials: 'same-origin',
				headers: {
					accept: 'application/json',
					'content-type': 'application/json'
				},
				body: JSON.stringify({
					id: page.route.id,
					path: page.url.pathname + page.url.search,
					params: page.params
				})
			});

			if (response.ok) {
				const result = await response.json();
				traceRefresh({
					kind: 'refresh-response',
					status: response.status,
					hasPageData: Boolean(result.pageData),
					hasDistributed: result.pageData?.distributed !== undefined,
					hasAuthority: result.pageData?.distributedAuthority !== undefined,
					credentialOrdinal: credentialOrdinal(result.pageData)
				});
				if (result.pageData) {
					withSessionSource('refresh-response', () => onRefresh(result.pageData));
				}
				if (result.pageData) traceRefresh({ kind: 'refresh-seed-applied' });
			}

			if (response.ok || response.status === 401) {
				traceRefresh({ kind: 'invalidate-all-start', status: response.status });
				await invalidateAll();
				traceRefresh({ kind: 'invalidate-all-complete', status: response.status });
				return;
			}
		} catch (error) {
			console.error('Background auth refresh failed:', error);
		}

		retryTimer = window.setTimeout(refreshSession, RETRY_DELAY_MS);
	}

	$effect(() => {
		if (!browser) return;

		clearTimers();

		const session = page.data.session;
		if (!session?.user || !session.hasRefreshToken) return;

		const refreshAt =
			typeof session.refreshAfter === 'number'
				? session.refreshAfter * 1000
				: new Date(session.expires ?? 0).getTime() - FALLBACK_REFRESH_SKEW_MS;
		const delay = Math.max(MIN_REFRESH_DELAY_MS, refreshAt - Date.now());

		refreshTimer = window.setTimeout(refreshSession, delay);

		return clearTimers;
	});
</script>
