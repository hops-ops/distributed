<script lang="ts">
	/**
	 * Blob Game — URL-routed selection from one generated replica operation.
	 *
	 * - URL (`/blob` | `/blob/{gameId}`) selects which game is active.
	 * - Board and history derive from `BlobGames.use()`.
	 * - Commands are Atomic: thin input (`game_id` + `direction`). Known-row pure
	 *   optimism runs `blob.simulate_move` via blob-domain WASM; Atomic seal is
	 *   still server authority.
	 */
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { BlobGames, ensurePureFunctionsReady, useCommands } from '$distributed';
	import { parseBoard, TILE, type Direction } from '$lib/blob/board';
	import { Button } from '$lib/components/shared/ui';
	import SelectedBlobGame from '$lib/components/blob/SelectedBlobGame.svelte';
	import { AppPage, InlineAlert, PageHeader } from '$lib/components/product';
	import { HowItsBuilt } from '$lib/components/walkthrough';
	import { blobWalkthrough } from '$lib/walkthrough';

	let { data } = $props();
	let actionError = $state<string | null>(null);
	/** A new game has no row to mark pending until its atomic row lands. */
	let starting = $state(false);
	let hydrated = $state(false);

	const routeGameId = $derived(page.params.gameId ?? null);

	function gamePath(gameId: string) {
		return `/blob/${encodeURIComponent(gameId)}`;
	}

	async function navigateToGame(gameId: string, replace = false) {
		if (routeGameId === gameId) return;
		await goto(gamePath(gameId), { replaceState: replace, noScroll: true, keepFocus: true });
	}

	const query = BlobGames.use();
	const commands = useCommands();
	const games = $derived($query.complete ? $query.data.blob_games : []);
	const selected = $derived(
		routeGameId ? (games.find((game) => game.game_id === routeGameId) ?? null) : null
	);
	const board = $derived.by(() => {
		if (!selected) return [];
		try {
			return parseBoard(selected.map_json || '[]');
		} catch {
			return [];
		}
	});

	const cols = $derived(board[0]?.length ?? 0);
	const hasBoard = $derived(board.length > 0 && cols > 0);
	const score = $derived(Number(selected?.score ?? 0));
	const playerDead = $derived(!!selected?.player_dead);
	const levelComplete = $derived(!!selected?.current_level_completed);
	const currentLevel = $derived(Number(selected?.current_level ?? 0));
	const status = $derived(selected?.status ?? 'active');
	/** The selected game shows a move or level the server has not sealed yet. */
	const selectedPending = $derived(selected !== null && $query.pending.has(selected));

	function tileClass(t: number): string {
		switch (t) {
			case TILE.player:
				return 'tile-player';
			case TILE.hole:
				return 'tile-hole';
			case TILE.unvisited:
				return 'tile-unvisited';
			case TILE.visited:
				return 'tile-visited';
			case TILE.deadBySuicide:
				return 'tile-dead-suicide';
			case TILE.deadByHole:
				return 'tile-dead-hole';
			default:
				return 'tile-unknown';
		}
	}

	function tileLabel(t: number): string {
		if (t === TILE.player) return '●';
		if (t === TILE.deadByHole || t === TILE.deadBySuicide) return '✕';
		return '';
	}

	async function startGame() {
		if (starting) return;
		starting = true;
		actionError = null;
		try {
			// `game_id` is the command's `uuid_v7()` default.
			const call = commands.blob.start({});
			await call;
			// Atomic response row is already in the replica before resolve.
			await navigateToGame(call.input.game_id, true);
		} catch (e) {
			actionError = e instanceof Error ? e.message : 'Start failed';
		} finally {
			starting = false;
		}
	}

	function canMove(direction: Direction): boolean {
		let playerRow = -1;
		let playerCol = -1;
		for (const [rowIndex, row] of board.entries()) {
			const colIndex = row.indexOf(TILE.player);
			if (colIndex >= 0) {
				playerRow = rowIndex;
				playerCol = colIndex;
				break;
			}
		}
		if (playerRow < 0 || playerCol < 0) return true;
		return (
			(direction !== 'up' || playerRow > 0) &&
			(direction !== 'down' || playerRow < board.length - 1) &&
			(direction !== 'left' || playerCol > 0) &&
			(direction !== 'right' || playerCol < cols - 1)
		);
	}

	async function move(direction: Direction) {
		if (!selected || playerDead || levelComplete || !hasBoard) return;
		// Edge no-op: don't dispatch a predictably rejected command.
		if (!canMove(direction)) {
			actionError = null;
			return;
		}
		actionError = null;
		try {
			await commands.blob.move({
				game_id: selected.game_id,
				direction
			});
		} catch (error) {
			actionError = error instanceof Error ? error.message : 'Move failed';
		}
	}

	async function nextLevel() {
		if (!selected || playerDead || !levelComplete || selectedPending) return;
		actionError = null;
		try {
			await commands.blob.start_level({ game_id: selected.game_id });
		} catch (error) {
			actionError = error instanceof Error ? error.message : 'Next level failed';
		}
	}

	function selectGame(id: string) {
		if (id === routeGameId) return;
		actionError = null;
		void navigateToGame(id);
	}

	function onKey(e: KeyboardEvent) {
		const map: Record<string, Direction> = {
			ArrowUp: 'up',
			ArrowDown: 'down',
			ArrowLeft: 'left',
			ArrowRight: 'right',
			w: 'up',
			s: 'down',
			a: 'left',
			d: 'right',
			W: 'up',
			S: 'down',
			A: 'left',
			D: 'right'
		};
		const dir = map[e.key];
		if (dir) {
			e.preventDefault();
			void move(dir);
		}
	}

	onMount(() => {
		hydrated = true;
		// Warm generated WASM pure hosts (blob.simulate_move) before pure-reduce optimism.
		void ensurePureFunctionsReady().catch(() => {
			/* fail-closed pure until reload */
		});
		const testWindow = window as Window & {
			__distributedBlobRefetch?: () => Promise<void>;
		};
		testWindow.__distributedBlobRefetch = () => query.refetch();
		window.addEventListener('keydown', onKey);
		return () => {
			window.removeEventListener('keydown', onKey);
			delete testWindow.__distributedBlobRefetch;
		};
	});
</script>

<svelte:head>
	<title>Blob Game · e2e-ui</title>
</svelte:head>

<AppPage>
	{#key routeGameId}
		<SelectedBlobGame />
	{/key}
	<div class="blob-page" data-blob-hydrated={hydrated ? '1' : '0'}>
		<div class="blob-title-row">
			<PageHeader
				kicker="URL select · generated replica · atomic commands"
				title="Blob Game"
			>
				Board and history render from the same generated <code>BlobGames</code>
				operation. Moves use the same client optimism path as todos/chat
				(domain-owned <code>blob.simulate_move</code> pure + projection mapping);
				the handler applies the same mutation IR and seals the atomic row before
				await resolves.
			</PageHeader>
			<Button
				type="button"
				variant="ink"
				data-testid="blob-new-game"
				onclick={() => void startGame()}
				disabled={!hydrated}
			>
				New game
			</Button>
		</div>

		{#if data.gqlError}
			<InlineAlert label="SSR GraphQL">{data.gqlError}</InlineAlert>
		{/if}
		{#if actionError}
			<InlineAlert label="Command">{actionError}</InlineAlert>
		{/if}

		{#if routeGameId && $query.loading && !selected && !data.gqlError}
			<div class="blob-empty">
				<p class="blob-empty-copy">Loading game…</p>
			</div>
		{:else if routeGameId && !hasBoard && !data.gqlError}
			<div class="blob-empty">
				<p class="blob-empty-copy">
					Game <code>{routeGameId}</code> not found (or not yours).
				</p>
				<Button href="/blob" variant="ink">All games</Button>
			</div>
		{:else if !hasBoard}
			<div class="blob-empty">
				<p class="blob-empty-copy">No game selected. Start one to play.</p>
				<Button
					type="button"
					variant="ink"
					data-testid="blob-start-game"
					onclick={() => void startGame()}
					disabled={starting || !hydrated}
				>
					Start game
				</Button>
			</div>
		{:else}
			<div class="blob-stage">
				<div class="blob-hud">
					<div class="hud-stat">
						<span class="hud-label">Score</span>
						<strong class="hud-value">{score}</strong>
					</div>
					<div class="hud-stat">
						<span class="hud-label">Level</span>
						<strong class="hud-value">{currentLevel}</strong>
					</div>
					<div class="hud-stat">
						<span class="hud-label">Status</span>
						<strong class="hud-value hud-status-{status}">{status}</strong>
					</div>
					{#if playerDead}
						<span class="hud-banner dead">You died — start a new game</span>
					{:else if levelComplete}
						<span class="hud-banner win">Level complete</span>
						<Button
							type="button"
							variant="ink"
							onclick={() => void nextLevel()}
							disabled={selectedPending}
						>
							Next level
						</Button>
					{/if}
				</div>

				<div class="blob-board" style="--cols: {cols}" role="grid" aria-label="Blob game board">
					{#each board as row, r}
						{#each row as cell, c}
							<div class="cell {tileClass(cell)}" role="gridcell" aria-label="r{r} c{c}">
								{tileLabel(cell)}
							</div>
						{/each}
					{/each}
				</div>

				<div class="blob-legend" aria-hidden="true">
					<span><i class="swatch tile-player"></i> You</span>
					<span><i class="swatch tile-unvisited"></i> Unvisited</span>
					<span><i class="swatch tile-visited"></i> Visited</span>
					<span><i class="swatch tile-hole"></i> Hole</span>
					<span><i class="swatch tile-dead-hole"></i> Death</span>
				</div>

				<div class="blob-pad" aria-label="Move controls">
					<button
						type="button"
						class="pad-btn"
						onclick={() => void move('up')}
						disabled={playerDead || levelComplete}>↑</button
					>
					<div class="pad-row">
						<button
							type="button"
							class="pad-btn"
							onclick={() => void move('left')}
							disabled={playerDead || levelComplete}>←</button
						>
						<button
							type="button"
							class="pad-btn"
							onclick={() => void move('down')}
							disabled={playerDead || levelComplete}>↓</button
						>
						<button
							type="button"
							class="pad-btn"
							onclick={() => void move('right')}
							disabled={playerDead || levelComplete}>→</button
						>
					</div>
				</div>
			</div>
		{/if}

		{#if games.length > 0}
			<section class="blob-history">
				<h2>Your games</h2>
				<ul>
					{#each games as g (g.game_id)}
						<li>
							<a
								href={gamePath(g.game_id)}
								class="history-item"
								class:active={g.game_id === routeGameId}
								onclick={(e) => {
									e.preventDefault();
									selectGame(g.game_id);
								}}
							>
								<span class="history-id">{g.game_id.slice(0, 14)}…</span>
								<span>score {g.score}</span>
								<span class="hud-status-{g.status}">{g.status}</span>
							</a>
						</li>
					{/each}
				</ul>
			</section>
		{/if}
	</div>
</AppPage>

<HowItsBuilt demo={blobWalkthrough} />

<style>
	.blob-page {
		position: relative;
	}
	.blob-title-row {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-start;
		justify-content: space-between;
		gap: 1rem;
	}
	.blob-empty {
		display: flex;
		flex-direction: column;
		align-items: center;
		text-align: center;
		gap: 1rem;
		padding: 2rem 1rem 2.5rem;
		border: 1px dashed var(--wf-line-strong, #cdcabe);
		border-radius: 12px;
		background: var(--wf-bg-elevated, #fff);
	}
	.blob-empty-copy {
		margin: 0;
		max-width: 28rem;
		font-size: 0.92rem;
		color: var(--wf-ink-soft, #5c5c56);
		line-height: 1.45;
	}
	.blob-stage {
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 1.1rem;
	}
	.blob-hud {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: center;
		gap: 0.75rem 1.25rem;
		width: 100%;
		padding: 0.85rem 1rem;
		border-radius: 10px;
		background: var(--wf-bg-elevated, #fff);
		border: 1px solid var(--wf-line, #e2e0d9);
	}
	.hud-stat {
		display: flex;
		flex-direction: column;
		gap: 0.1rem;
		min-width: 4.5rem;
	}
	.hud-label {
		font-size: 0.68rem;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--wf-ink-muted, #8a8a82);
	}
	.hud-value {
		font-size: 1.15rem;
		font-variant-numeric: tabular-nums;
	}
	.hud-status-active {
		color: var(--wf-accent, #3d5a80);
	}
	.hud-status-dead {
		color: var(--wf-danger, #b33a3a);
	}
	.hud-status-level_complete {
		color: var(--wf-success, #2f6f4e);
	}
	.hud-banner {
		font-weight: 700;
		font-size: 0.9rem;
		padding: 0.35rem 0.65rem;
		border-radius: 6px;
	}
	.hud-banner.dead {
		background: rgba(179, 58, 58, 0.12);
		color: var(--wf-danger, #b33a3a);
	}
	.hud-banner.win {
		background: rgba(47, 111, 78, 0.12);
		color: var(--wf-success, #2f6f4e);
	}
	.blob-board {
		display: grid;
		grid-template-columns: repeat(var(--cols), minmax(2.5rem, 3rem));
		gap: 4px;
		padding: 0.65rem;
		background: #141412;
		border-radius: 12px;
		box-shadow: 0 16px 40px rgba(0, 0, 0, 0.18);
		width: max-content;
		max-width: 100%;
		contain: layout style;
	}
	.cell {
		aspect-ratio: 1;
		min-width: 2.5rem;
		min-height: 2.5rem;
		display: flex;
		align-items: center;
		justify-content: center;
		border-radius: 5px;
		font-size: 1rem;
		font-weight: 700;
	}
	.tile-player {
		background: linear-gradient(145deg, #7eb6ff, #4a8fe0);
		color: #0a1628;
		box-shadow: 0 0 0 2px rgba(126, 182, 255, 0.45);
	}
	.tile-hole {
		background: radial-gradient(circle at 40% 35%, #2a2a28 0%, #0a0a09 70%);
		box-shadow: inset 0 2px 6px rgba(0, 0, 0, 0.6);
	}
	.tile-unvisited {
		background: #3f6b45;
	}
	.tile-visited {
		background: #243528;
	}
	.tile-dead-suicide,
	.tile-dead-hole {
		background: #a33a3a;
		color: #fff;
	}
	.tile-unknown {
		background: #444;
	}
	.blob-legend {
		display: flex;
		flex-wrap: wrap;
		justify-content: center;
		gap: 0.75rem 1.1rem;
		font-size: 0.78rem;
		color: var(--wf-ink-soft, #5c5c56);
	}
	.blob-legend span {
		display: inline-flex;
		align-items: center;
		gap: 0.35rem;
	}
	.swatch {
		display: inline-block;
		width: 0.85rem;
		height: 0.85rem;
		border-radius: 3px;
	}
	.blob-pad {
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 0.4rem;
	}
	.pad-row {
		display: flex;
		gap: 0.4rem;
	}
	.pad-btn {
		width: 3rem;
		height: 3rem;
		border-radius: 8px;
		border: 1px solid var(--wf-line-strong, #cdcabe);
		background: #fff;
		font-size: 1.2rem;
		cursor: pointer;
		touch-action: manipulation;
	}
	.pad-btn:active:not(:disabled) {
		transform: scale(0.96);
	}
	.pad-btn:disabled {
		opacity: 0.45;
		cursor: not-allowed;
	}
	.blob-history {
		margin-top: 2.5rem;
		padding-top: 1.5rem;
		border-top: 1px solid var(--wf-line, #e2e0d9);
	}
	.blob-history h2 {
		margin: 0 0 0.75rem;
		font-size: 0.95rem;
	}
	.blob-history ul {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: 0.35rem;
	}
	.history-item {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem 1rem;
		width: 100%;
		text-align: left;
		appearance: none;
		border: 1px solid var(--wf-line, #e2e0d9);
		background: #fff;
		border-radius: 8px;
		padding: 0.55rem 0.75rem;
		font: inherit;
		font-size: 0.85rem;
		cursor: pointer;
		text-decoration: none;
		color: inherit;
	}
	.history-item.active {
		border-color: var(--wf-accent, #3d5a80);
		background: var(--wf-accent-soft, rgba(61, 90, 128, 0.08));
		font-weight: 600;
	}
	.history-id {
		font-family: var(--wf-mono, ui-monospace, monospace);
		font-size: 0.8rem;
	}
	@media (max-width: 480px) {
		.blob-board {
			grid-template-columns: repeat(var(--cols), minmax(2rem, 1fr));
			width: 100%;
		}
		.cell {
			min-width: 0;
			min-height: 0;
		}
	}
</style>
