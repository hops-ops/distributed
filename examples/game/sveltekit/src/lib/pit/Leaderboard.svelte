<script lang="ts">
	import { untrack } from 'svelte';
	import { useDistributedLeaderboard } from '@hops-ops/distributed/sveltekit';
	import { Leaderboard, XpLeaderboardLeaderboard, type XpLeaderboardData } from '$distributed';

	let { me, initial }: { me: string; initial?: XpLeaderboardData } = $props();

	// The Redis XP board through the GraphQL board roots: server-ranked,
	// SSR-seeded by the page load, then `@live` (the zone server's projector
	// publishes board changes on the bus; mobpit-service pushes them).
	const board = useDistributedLeaderboard<XpLeaderboardData>(
		XpLeaderboardLeaderboard,
		{ first: 5 },
		untrack(() => (initial === undefined ? {} : { initial }))
	);
	const rows = $derived($board.data?.xpLeaderboard ?? []);

	// Names from the read model (island: SSR + `@live`).
	const names = Leaderboard.use();
	const nameOf = $derived(
		new Map(($names.complete ? $names.data.player_stats : []).map((row) => [row.player_id, row.name]))
	);
</script>

<ol data-testid="leaderboard" data-live={$board.live}>
	{#each rows as row (row.member)}
		<li class:me={row.member === me}>{nameOf.get(row.member) ?? row.member} — {row.score} XP</li>
	{/each}
</ol>
{#if $board.error}<p class="error" data-testid="leaderboard-error">{$board.error}</p>{/if}

<style>
	ol { padding-left: 1.2rem; }
	.me { color: #ffd166; }
	.error { color: #ef476f; }
</style>
