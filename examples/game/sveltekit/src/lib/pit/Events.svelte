<script lang="ts">
	import { Events } from '$distributed';

	// Island: promoted to the page's `@load` (SSR), then live.
	const events = Events.use();
	type Row = Readonly<{ borough: string; event: string; event_until_ms: number }>;
	const rows = $derived<readonly Row[]>($events.complete ? $events.data.boroughs : []);
	let now = $state(Date.now());
	$effect(() => {
		const timer = setInterval(() => (now = Date.now()), 1000);
		return () => clearInterval(timer);
	});
</script>

<ul data-testid="events" data-live={$events.live}>
	{#each rows as row (row.borough)}
		<li data-borough={row.borough}>
			<b>{row.event}</b> in borough {row.borough} · {Math.max(0, Math.round((row.event_until_ms - now) / 1000))} s left
		</li>
	{:else}
		<li class="none">no world event right now</li>
	{/each}
</ul>

<style>
	ul { margin: 0; padding-left: 1rem; }
	.none { color: #8793a6; list-style: none; margin-left: -1rem; }
</style>
