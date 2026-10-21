<script lang="ts">
	import { watchZones, type WatchState } from '@hops-ops/distributed/game';
	import { gameTicketSource, useGame } from '@hops-ops/distributed/sveltekit/game';
	import { createMobpitClient, EVENTS, PIT_WORLD, WEATHER } from '$lib/generated/mobpit/index.ts';
	import Leaderboard from '$lib/pit/Leaderboard.svelte';
	import MyStats from '$lib/pit/MyStats.svelte';
	import Rooms from '$lib/pit/Rooms.svelte';
	import Events from '$lib/pit/Events.svelte';
	import { neighbourRooms, pitHandoffFrame, startPitLoop } from '$lib/pit/loop';

	let { data } = $props();

	// Client built on mount (browser only), joins the server-chosen room,
	// closed on navigation. Tickets come from /api/game/ticket (session).
	// Rooms are celld objects: any entry point reaches any room (celld
	// forwards); the page rotates over the entry points it was given.
	let entry = 0;
	const game = useGame(
		() => {
			const typed = createMobpitClient({
				url: (zone) => `${data.roomsUrls[entry++ % data.roomsUrls.length]}/rooms/${encodeURIComponent(zone)}`,
				ticket: gameTicketSource(),
				pingIntervalMs: 500,
				inputDtMs: 1000 / 30,
				// One continuous world: keep predicting through doorways.
				prediction: { handoffFrame: pitHandoffFrame }
			});
			// The predicted step's context is the room (its seeded obstacles).
			let seed = PIT_WORLD.defaultSeed;
			const setRoom = () => {
				const m = /^pit\.(-?\d+)\.(-?\d+)$/.exec(typed.client.authority?.zoneId ?? '');
				if (m !== null) typed.prediction?.setContext({ seed, x: Number(m[1]), y: Number(m[2]) });
			};
			typed.client.on('authority', setRoom);
			typed.on('hello', (h) => {
				seed = h.seed;
				setRoom();
			});
			return typed;
		},
		{ zone: () => data.zone, events: EVENTS }
	);
	const hello = game.latest('hello');
	// The tiers above the own room (hood, borough, city); neighbours' are ignored.
	let world = $state<import('$lib/generated/mobpit/index.ts').WorldTiers | undefined>();
	game.on('world', (w, zoneId) => {
		if (zoneId === game.zone) world = w;
	});
	game.on('bossDown', (b, zoneId) => {
		if (zoneId === game.zone) toast(`${b.by} felled the boss — the hood is now tier ${b.hoodTier}`);
	});
	const saved = game.events('saved', { limit: 20 });
	// Your kills not settled yet. Entity ids are per room, so only kills in
	// your own room can be yours (the rooms next door are watched too).
	let mine = $state<string[]>([]);
	game.on('kill', (k, zoneId) => {
		if (zoneId === game.zone && k.killer === hello.current?.entity) mine = [k.key, ...mine].slice(0, 20);
	});
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
	let ghost = $state(true);
	let predict = $state(true);
	let firstPerson = $state(false);
	let compensated = $state(true);
	$effect(() => {
		const client = game.client;
		if (client === undefined || canvas === undefined) return;
		return startPitLoop(canvas, {
			client,
			hello: () => hello.current,
			autoHunt: () => autoHunt,
			ghost: () => ghost,
			world: () => world,
			firstPerson: () => firstPerson,
			compensated: () => compensated
		});
	});
	$effect(() => game.client?.setPredictionEnabled(predict));

	// Network panel: prediction / reconciliation readout and the rooms'
	// network simulation (dev only: every room on celld delays its sockets).
	let net = $state({ pending: 0, ack: 0, corrections: 0, last: 0 });
	$effect(() => {
		const client = game.client;
		if (client === undefined) return;
		let corrections = 0;
		let last = 0;
		const off = client.on('correction', (c) => {
			corrections += 1;
			last = Math.hypot(c.errors.x ?? 0, c.errors.y ?? 0);
		});
		const timer = setInterval(() => {
			const p = client.predictor;
			net = { pending: p?.pending.length ?? 0, ack: p?.lastAck ?? 0, corrections, last };
		}, 250);
		return () => {
			clearInterval(timer);
			off();
		};
	});
	let sim = $state({ latencyMs: 0, jitterMs: 0, loss: 0 });
	let simStatus = $state('');
	$effect(() => {
		fetch('/api/dev/netsim')
			.then(async (r) => (r.ok ? (sim = await r.json()) : (simStatus = `dev routes off (${r.status})`)))
			.catch(() => (simStatus = 'unavailable'));
	});
	let simTimer: ReturnType<typeof setTimeout> | undefined;
	function pushSim() {
		clearTimeout(simTimer);
		simTimer = setTimeout(async () => {
			const r = await fetch('/api/dev/netsim', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(sim) });
			simStatus = r.ok ? 'applied to every room' : `failed (${r.status})`;
		}, 150);
	}

	// Kills the zone reported that the cell has not acknowledged yet.
	const unsaved = $derived(mine.filter((key) => !saved.items.some((s) => s.key === key)).length);

	// The 8 rooms next door, watched as passive observers: their mobs are
	// drawn, a sleeping room is not woken (it shows nothing).
	let around = $state<Record<string, WatchState>>({});
	$effect(() => {
		const client = game.client;
		if (client === undefined) return;
		const watch = watchZones(client, neighbourRooms);
		const timer = setInterval(() => (around = Object.fromEntries(watch.states())), 500);
		return () => {
			clearInterval(timer);
			watch.stop();
		};
	});
	const watching = $derived(Object.values(around).filter((s) => s === 'watching').length);
	const asleep = $derived(Object.values(around).filter((s) => s === 'asleep').length);
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
			<small>WASD move · Space attack · E dash · Q jump · walk through a doorway (the gaps in the walls) to the next room</small>
		</section>
		<section data-testid="ranged">
			<h2>Bow</h2>
			<label><input type="checkbox" bind:checked={firstPerson} data-testid="first-person" /> first person (click to look; the view turns with you)</label>
			<label><input type="checkbox" bind:checked={compensated} data-testid="compensated" /> lag compensation</label>
			<small>R / click shoots where you aim (the mouse). Yellow: where you saw it land; green / red: the room's verdict.</small>
		</section>
		<section data-testid="world">
			<h2>World</h2>
			{#if world}
				<div>hood <b>{world.hoodX},{world.hoodY}</b> · tier <b data-testid="hood-tier">{world.hoodTier}</b> · boss <b data-testid="boss">{world.bossDown ? `down (${world.lastBy})` : 'standing'}</b></div>
				<div>borough <b>{world.boroughX},{world.boroughY}</b> · <b data-testid="weather">{WEATHER[world.weather]}</b> · {world.boroughBossesDown}/9 bosses down</div>
				<div data-testid="banner">{world.banner}</div>
				{#if world.event}
					<div class="event" data-testid="world-event">{world.event} in this borough</div>
				{/if}
			{:else}
				<div>…</div>
			{/if}
		</section>
		<section data-testid="world-events">
			<h2>World events</h2>
			<!-- The boroughs read model (mobpit-service): SSR, then @live. -->
			<Events />
		</section>
		<section data-testid="network">
			<h2>Network</h2>
			<label><input type="checkbox" bind:checked={predict} data-testid="predict" /> client prediction</label>
			<label><input type="checkbox" bind:checked={ghost} data-testid="ghost" /> server ghost</label>
			<div>pending inputs <b data-testid="pending">{net.pending}</b> · ack <b>{net.ack}</b></div>
			<div>corrections <b data-testid="corrections">{net.corrections}</b> (last {net.last.toFixed(3)} m)</div>
			<div>interest ring <b>{hello.current?.viewRadius ?? '–'}</b> m</div>
			<div>next door <b data-testid="watching">{watching}</b> watched · <b data-testid="asleep">{asleep}</b> asleep</div>
			<label>latency <input type="range" min="0" max="300" step="5" bind:value={sim.latencyMs} oninput={pushSim} data-testid="latency" /> {sim.latencyMs} ms</label>
			<label>jitter <input type="range" min="0" max="100" step="1" bind:value={sim.jitterMs} oninput={pushSim} /> {sim.jitterMs} ms</label>
			<label>loss <input type="range" min="0" max="0.2" step="0.01" bind:value={sim.loss} oninput={pushSim} /> {Math.round(sim.loss * 100)}%</label>
			<small data-testid="netsim-status">{simStatus}</small>
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
	section label { display: block; }
	h2 { margin: 0 0 0.5rem; font-size: 1rem; }
	.error { color: #ff6b6b; }
	.saved { color: #9ad16b; }
	.event { color: #e0a8ff; font-weight: 600; }
</style>
