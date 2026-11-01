<script lang="ts">
	import { browser } from '$app/environment';
	import { page } from '$app/state';
	import { onDestroy, untrack } from 'svelte';
	import {
		createPageDataSessionSource,
		type SveltekitDistributedPageData,
		type SveltekitReplicaHydration
	} from '@hops-ops/distributed/sveltekit';
	import { DISTRIBUTED_BOUNDARY_OPERATIONS, provideDistributed } from '$distributed';

	let { data, children } = $props();

	// One `$distributed` client for this authorization lifecycle: SSR seed
	// first, then HTTP + the `@live` WebSocket straight to mobpit-service with
	// the session's Bearer (no cookies).
	const initial = untrack(() => data);
	const pageData = createPageDataSessionSource<SveltekitDistributedPageData>(initial);
	let applied: SveltekitReplicaHydration | undefined = initial.distributed;
	const client = provideDistributed({
		boundaries: DISTRIBUTED_BOUNDARY_OPERATIONS,
		session: pageData.session,
		browser,
		...(initial.graphqlUrl ? { url: initial.graphqlUrl } : {}),
		...(initial.distributed !== undefined && initial.distributedAuthority !== undefined
			? { hydration: initial.distributed, authority: initial.distributedAuthority }
			: {})
	});

	let hydrationTimer: ReturnType<typeof setTimeout> | undefined;
	$effect(() => {
		pageData.set(data);
		if (data.distributed === undefined || data.distributedAuthority === undefined || data.distributed === applied) return;
		applied = data.distributed;
		const seed = data.distributed;
		const authority = data.distributedAuthority;
		// Apply a navigation seed after the session fence (same-scope merge).
		if (hydrationTimer !== undefined) clearTimeout(hydrationTimer);
		hydrationTimer = setTimeout(() => client.hydrate(seed, authority), 0);
	});

	$effect(() => {
		if (!browser || !data.session?.user) return;
		const retained = client.retainLocation(
			{ id: 'active-page', pathname: page.url.pathname, kind: 'page' },
			{ search: page.url.searchParams, session: data.session, props: data }
		);
		return () => retained.release();
	});

	onDestroy(() => {
		if (hydrationTimer !== undefined) clearTimeout(hydrationTimer);
		client.destroy();
	});
</script>

<header>
	<a href="/">Mob Pit · SvelteKit</a>
	{#if data.session?.user}
		<span data-testid="user">{data.session.user.name}</span>
		<form method="POST" action="/signout"><button>Sign out</button></form>
	{/if}
</header>
{#if data.gqlError}<p class="gql-error" data-testid="gql-error">{data.gqlError}</p>{/if}
{@render children()}

<style>
	:global(body) { margin: 0; background: #0d0b10; color: #e6e1ea; font: 14px/1.4 system-ui, sans-serif; }
	header { display: flex; gap: 1rem; align-items: center; padding: 0.5rem 1rem; border-bottom: 1px solid #2b2533; }
	header a { color: inherit; font-weight: 600; text-decoration: none; margin-right: auto; }
	button { background: #2b2533; color: inherit; border: 1px solid #4a4155; border-radius: 4px; padding: 0.25rem 0.6rem; cursor: pointer; }
	.gql-error { color: #ff6b6b; padding: 0 1rem; }
</style>
