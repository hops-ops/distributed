<script lang="ts">
	import { Leaderboard } from '$distributed';

	let { me }: { me: string } = $props();

	// Island: promoted to the page's `@load` (SSR), then live.
	const board = Leaderboard.use();
	const rows = $derived($board.complete ? $board.data.player_stats : []);
</script>

<ol data-testid="leaderboard">
	{#each rows as row (row.player_id)}
		<li class:me={row.player_id === me}>{row.name} — {row.xp} XP</li>
	{/each}
</ol>

<style>
	ol { padding-left: 1.2rem; }
	.me { color: #ffd166; }
</style>
