<script lang="ts">
	import { page } from '$app/state';
	import { useDistributedSvelteKitClient } from '@hops-ops/distributed/sveltekit';
	import { DISTRIBUTED_BOUNDARY_OPERATIONS, MyStats } from '$distributed';

	// Island: promoted to the page's `@load` (SSR seed), then `@live`.
	// `$playerId` is resolved by the generated boundary binding
	// (MyStats.graphql.bindings.js → `sessionClaim('user', 'id')`) from the
	// session the server put in page data — the same resolution SSR used, so
	// the hydrated seed is reused. No prop, URL or input supplies it.
	type Boundary = (typeof DISTRIBUTED_BOUNDARY_OPERATIONS)[number];
	type MyStatsBoundary = Extract<Boundary, { artifact: typeof MyStats.artifact }>;
	const boundary = DISTRIBUTED_BOUNDARY_OPERATIONS.find(
		(operation): operation is MyStatsBoundary => operation.artifact === MyStats.artifact
	);
	if (boundary === undefined) throw new Error('MyStats is not a generated boundary operation');
	const stats = useDistributedSvelteKitClient()
		.boundary(boundary)
		.use({ params: page.params, search: page.url.searchParams, session: page.data.session ?? null, props: page.data });
	const mine = $derived($stats.complete ? $stats.data.player_stats[0] : undefined);
</script>

<div>
	XP <b data-testid="xp">{mine?.xp ?? 0}</b> · kills <b data-testid="kills">{mine?.kills ?? 0}</b> ·
	level <b data-testid="level">{mine?.level ?? 1}</b>
</div>
