<script lang="ts">
	import { gameTicketSource, useGame } from '@hops-ops/distributed/sveltekit/game';
	import { createMobpitClient, EVENTS } from '$lib/generated/mobpit/index.ts';
	import Leaderboard from '$lib/pit/Leaderboard.svelte';
	import MyStats from '$lib/pit/MyStats.svelte';
	import { startPitLoop } from '$lib/pit/loop';

	let { data } = $props();

	// Client built on mount (browser only), joins the server-chosen zone,
	// closed on navigation. Tickets come from /api/game/ticket (session).
	const game = useGame(
		() =>
			createMobpitClient({
				url: (zone) => `${data.zonesUrl}/zones/${encodeURIComponent(zone)}`,
				ticket: gameTicketSource(),
				pingIntervalMs: 500,
				inputDtMs: 1000 / 30
			}),
		{ zone: () => data.zone, events: EVENTS }
	);
	const hello = game.latest('hello');
	const kills = game.events('kill', { limit: 20 });
	const saved = game.events('saved', { limit: 20 });
	game.on('hello', (h) => game.client?.setControlledEntity(h.entity));

	let canvas = $state<HTMLCanvasElement>();
	let autoHunt = $state(false);
	$effect(() => {
		const client = game.client;
		if (client === undefined || canvas === undefined) return;
		return startPitLoop(canvas, { client, hello: () => hello.current, autoHunt: () => autoHunt });
	});

	// Kills the zone reported that the cell has not acknowledged yet.
	const unsaved = $derived(
		kills.items.filter((k) => k.killer === hello.current?.entity && !saved.items.some((s) => s.key === k.key)).length
	);
</script>

<main>
	<canvas bind:this={canvas} width="640" height="640"></canvas>
	<aside>
		<section class="hud" data-testid="hud">
			<div>status <b data-testid="status">{game.status}</b></div>
			<div>zone <b data-testid="zone">{game.zone ?? '–'}</b></div>
			<div>rtt <b data-testid="rtt">{game.rttMs ?? '–'}</b> ms</div>
			<div>prediction <b data-testid="prediction">{game.predictionReady ? 'wasm' : 'loading'}</b></div>
			{#if game.error}<div class="error" data-testid="error">{game.error}</div>{/if}
			<label><input type="checkbox" bind:checked={autoHunt} data-testid="autohunt" /> auto-hunt</label>
			<small>WASD move · Space attack · E dash</small>
		</section>
		<section data-testid="settled">
			<h2>Settled</h2>
			<!-- Settled progress: mobpit-service's player_stats read model
			     (projected from the Crawler cells' domain events): SSR, then @live. -->
			<MyStats />
			<div>saving <b data-testid="unsaved">{unsaved}</b></div>
			<Leaderboard me={data.principal} initial={data.board} />
			{#each saved.items.slice(0, 3) as s (s.key)}
				<div class="saved">+{s.xp} XP saved ✓ <small>{s.waitedMs} ms</small></div>
			{/each}
		</section>
	</aside>
</main>

<style>
	main { display: flex; gap: 1rem; padding: 1rem; flex-wrap: wrap; }
	canvas { background: #120f14; border: 1px solid #2b2533; max-width: 100%; }
	aside { min-width: 16rem; display: grid; gap: 1rem; align-content: start; }
	section { border: 1px solid #2b2533; border-radius: 6px; padding: 0.75rem; }
	h2 { margin: 0 0 0.5rem; font-size: 1rem; }
	.error { color: #ff6b6b; }
	.saved { color: #9ad16b; }
</style>
