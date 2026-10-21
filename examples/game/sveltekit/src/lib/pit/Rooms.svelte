<script lang="ts">
	import { Rooms } from '$distributed';

	let {
		here,
		debug = true,
		ontoast
	}: { here: string | undefined; debug?: boolean; ontoast?: (text: string) => void } = $props();

	// Island: promoted to the page's `@load` (SSR), then live.
	const rooms = Rooms.use();
	type Row = Readonly<{ zone_id: string; status: string; players: number; restored: boolean; activations: number; node: string }>;
	const rows = $derived<readonly Row[]>($rooms.complete ? $rooms.data.zone_activities : []);
	const byZone = $derived(new Map(rows.map((r) => [r.zone_id, r])));

	const parse = (zone: string | undefined): [number, number] | undefined => {
		const m = zone === undefined ? null : /^pit\.(-?\d+)\.(-?\d+)$/.exec(zone);
		return m === null ? undefined : [Number(m[1]), Number(m[2])];
	};
	const center = $derived(parse(here) ?? [0, 0]);
	const SPAN = [-2, -1, 0, 1, 2];
	const grid = $derived(
		SPAN.toReversed().map((dy) =>
			SPAN.map((dx) => {
				const zone = `pit.${center[0] + dx}.${center[1] + dy}`;
				return { zone, row: byZone.get(zone), current: dx === 0 && dy === 0 };
			})
		)
	);

	// Toasts on live transitions: a room went to sleep; a room woke from its snapshot.
	let seen = new Map<string, Row>();
	$effect(() => {
		const next = new Map<string, Row>();
		for (const row of rows) {
			const before = seen.get(row.zone_id);
			if (before !== undefined && before.status !== row.status) {
				if (row.status === 'active' && row.restored && row.activations > before.activations) {
					ontoast?.(`${row.zone_id} woke up from hibernation — its mobs are back`);
				} else if (row.status === 'hibernated') {
					ontoast?.(`${row.zone_id} went to sleep`);
				}
			}
			next.set(row.zone_id, row);
		}
		seen = next;
	});
</script>

<div class="map" data-testid="minimap">
	{#each grid as line, j (j)}
		{#each line as cell (cell.zone)}
			<div
				class="room {cell.row?.status ?? 'cold'}"
				class:current={cell.current}
				data-testid="room-{cell.zone}"
				data-status={cell.row?.status ?? 'cold'}
				data-node={cell.row?.node ?? ''}
				title="{cell.zone}: {cell.row?.status ?? 'never ran'}"
			>
				{#if cell.row && cell.row.players > 0}<b>{cell.row.players}</b>{/if}
				{#if debug && cell.row?.node}<small>{cell.row.node.replace(/^node-/, '')}</small>{/if}
			</div>
		{/each}
	{/each}
</div>
<div class="legend"><span class="active">■ active</span> <span class="dormant">■ dormant</span> <span class="hibernated">■ asleep</span> <span class="cold">■ never ran</span></div>

<style>
	.map { display: grid; grid-template-columns: repeat(5, 2.2rem); gap: 3px; }
	.room { height: 2.2rem; border-radius: 3px; font-size: 0.7rem; color: #0e1116; display: flex; flex-direction: column; align-items: center; justify-content: center; }
	.room.current { outline: 2px solid #fff; }
	.active { background: #4caf50; color: #4caf50; }
	.dormant { background: #f2c94c; color: #f2c94c; }
	.hibernated { background: #6c7aa0; color: #6c7aa0; }
	.cold { background: #20252e; color: #556070; }
	.room.active, .room.dormant, .room.hibernated, .room.cold { color: #0e1116; }
	.legend { font-size: 0.7rem; margin-top: 0.25rem; background: none; }
	.legend span { background: none; }
</style>
