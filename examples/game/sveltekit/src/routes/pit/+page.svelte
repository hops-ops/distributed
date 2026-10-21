<script lang="ts">
	import { gameTicketSource, useGame } from '@hops-ops/distributed/sveltekit/game';
	import { createMobpitClient, EVENTS } from '$lib/generated/mobpit/index.ts';
	import Leaderboard from '$lib/pit/Leaderboard.svelte';
	import MyStats from '$lib/pit/MyStats.svelte';
	import Rooms from '$lib/pit/Rooms.svelte';
	import { pitHandoffFrame, startPitLoop } from '$lib/pit/loop';

	let { data } = $props();

	// Client built on mount (browser only), joins the server-chosen room,
	// closed on navigation. Tickets come from /api/game/ticket (session).
	// Rooms are celld objects: any entry point reaches any room (celld
	// forwards); the page rotates over the entry points it was given.
	let entry = 0;
	const game = useGame(
		() =>
			createMobpitClient({
				url: (zone) => `${data.roomsUrls[entry++ % data.roomsUrls.length]}/rooms/${encodeURIComponent(zone)}`,
				ticket: gameTicketSource(),
				pingIntervalMs: 500,
				inputDtMs: 1000 / 30,
				// One continuous world: keep predicting through doorways.
				prediction: { handoffFrame: pitHandoffFrame }
			}),
		{ zone: () => data.zone, events: EVENTS }
	);
	const hello = game.latest('hello');
	const kills = game.events('kill', { limit: 20 });
	const saved = game.events('saved', { limit: 20 });
	game.on('hello', (h) => {
		game.client?.setControlledEntity(h.entity);
		if (h.restored) toast(`pit.${h.roomX}.${h.roomY}: restored from hibernation (${h.roomKills} kills here so far)`);
	});

	// Toasts (room restores, sleeps).
	let toasts = $state<{ id: number; text: string }[]>([]);
	let toastSeq = 0;
	function toast(text: string) {
		const id = ++toastSeq;
		toasts = [...toasts, { id, text }];
		setTimeout(() => (toasts = toasts.filter((t) => t.id !== id)), 6000);
	}

	// Debug overlay: the room object's own view (which celld node runs it —
	// celld decides; this only shows it).
	let debug = $state(true);
	type RoomInfo = { zone: string; node: string; isolate: string; activation: string; wakes: number; restored: boolean; players: number; hz: number; lateMs: { p50: number; p99: number } };
	let info = $state<RoomInfo | undefined>();
	$effect(() => {
		const zone = game.zone;
		if (!debug || zone === undefined) return;
		const http = (data.roomsUrls[0] ?? '').replace(/^ws/, 'http');
		let stop = false;
		const poll = async () => {
			while (!stop) {
				try {
					info = await (await fetch(`${http}/rooms/${encodeURIComponent(zone)}/info`)).json();
				} catch {
					// the room's node is moving; keep the last view
				}
				await new Promise((r) => setTimeout(r, 1000));
			}
		};
		void poll();
		return () => (stop = true);
	});

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
			<div>room <b data-testid="zone">{game.zone ?? '–'}</b></div>
			<div>rtt <b data-testid="rtt">{game.rttMs ?? '–'}</b> ms</div>
			<div>prediction <b data-testid="prediction">{game.predictionReady ? 'wasm' : 'loading'}</b></div>
			{#if game.error}<div class="error" data-testid="error">{game.error}</div>{/if}
			<label><input type="checkbox" bind:checked={autoHunt} data-testid="autohunt" /> auto-hunt</label>
			<small>WASD move · Space attack · E dash · walk through a doorway (the gaps in the walls) to the next room</small>
		</section>
		<section data-testid="rooms">
			<h2>Rooms</h2>
			<!-- The zone status read model (mobpit-service zone_activities): SSR, then @live. -->
			<Rooms here={game.zone} {debug} ontoast={toast} />
			<label><input type="checkbox" bind:checked={debug} data-testid="debug" /> debug overlay</label>
			{#if debug && info && info.zone === game.zone}
				<div class="debug" data-testid="overlay" data-node={info.node}>
					celld object {info.zone}<br />
					node <b data-testid="node">{info.node || 'unknown'}</b> · isolate {info.isolate}<br />
					activation {info.activation} · wakes {info.wakes} · {info.restored ? 'restored' : 'fresh'}<br />
					{info.hz} Hz · late p50 {info.lateMs.p50.toFixed(1)} / p99 {info.lateMs.p99.toFixed(1)} ms
				</div>
			{/if}
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
<div class="toasts" data-testid="toasts">
	{#each toasts as t (t.id)}<div class="toast">{t.text}</div>{/each}
</div>

<style>
	.toasts { position: fixed; top: 0.75rem; left: 50%; transform: translateX(-50%); display: grid; gap: 0.4rem; pointer-events: none; }
	.toast { background: #1f3a4d; color: #e8f4ff; padding: 0.5rem 0.9rem; border-radius: 6px; }
	.debug { font: 0.75rem/1.4 monospace; color: #9aa4b2; margin-top: 0.5rem; }
	main { display: flex; gap: 1rem; padding: 1rem; flex-wrap: wrap; }
	canvas { background: #120f14; border: 1px solid #2b2533; max-width: 100%; }
	aside { min-width: 16rem; display: grid; gap: 1rem; align-content: start; }
	section { border: 1px solid #2b2533; border-radius: 6px; padding: 0.75rem; }
	h2 { margin: 0 0 0.5rem; font-size: 1rem; }
	.error { color: #ff6b6b; }
	.saved { color: #9ad16b; }
</style>
