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

	function clearTimers() {
		if (refreshTimer !== undefined) window.clearTimeout(refreshTimer);
		if (retryTimer !== undefined) window.clearTimeout(retryTimer);
		refreshTimer = undefined;
		retryTimer = undefined;
	}

	async function refreshSession() {
		try {
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
				if (result.pageData) onRefresh(result.pageData);
			}

			if (response.ok || response.status === 401) {
				await invalidateAll();
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
