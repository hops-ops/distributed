<script lang="ts">
	/**
	 * Lobby chat — one generated `@load @live @window` operation.
	 *
	 * `useWindow()` keeps the newest page live and loads older pages by keyset
	 * cursor on scroll-up; `rows` is already one ordered, de-duplicated list
	 * (newest first), so the page only reverses it for display. The log uses
	 * `column-reverse` so SSR/first paint already shows the newest end (no JS
	 * scroll jump).
	 */
	import { tick, untrack } from 'svelte';

	import {
		ChatMessages as UserChatMessages,
		useCommands,
		type Operation_ChatMessages_Data
	} from '$distributed';
	import { ChatMessages as PublicChatMessages } from '$distributed/public';
	import {
		nearBottom,
		nearTop,
		needsHistoryFill,
		pinScrollBottom,
		preserveScrollAfterPrepend
	} from '$lib/chat/lobby-log';
	import { Button } from '$lib/components/shared/ui';
	import { AppPage, InlineAlert, PageHeader } from '$lib/components/product';
	import { HowItsBuilt } from '$lib/components/walkthrough';
	import { chatWalkthrough } from '$lib/walkthrough';
	import { isOwnAuthor, sessionDisplayName, sessionPrincipalId } from '$lib/session';

	type ChatMsg = Operation_ChatMessages_Data['chat_messages'][number];

	const LOBBY_ROOM = 'lobby';

	let { data } = $props();
	/** Guest uses e2e-ui-public; signed-in uses portable e2e-ui (root client). Fixed at mount. */
	const signedIn = untrack(() => !!data.session?.user);
	const ChatMessages = signedIn ? UserChatMessages : PublicChatMessages;

	let sendError = $state<string | null>(null);
	let logEl: HTMLDivElement | undefined = $state();
	let draft = $state('');
	let busy = $state(false);
	/** Auto-pin to bottom only while the user is already near the end. */
	let stickToBottom = $state(true);

	/** Same principal the API stamps as author_id (access-token sub). */
	const me = $derived(
		sessionPrincipalId(data.session, data.accessToken ?? data.session?.accessToken)
	);
	const displayName = $derived(sessionDisplayName(data.session));

	/** The live head uses the variables SSR seeded, so mounting fetches nothing. */
	const lobby = ChatMessages.useWindow();
	const PAGE_SIZE = lobby.pageSize;
	const commands = signedIn ? useCommands() : null;
	/** Chronological for display; the window is newest first. */
	const messages = $derived([...$lobby.rows].reverse());

	function metricsOf(el: HTMLDivElement) {
		return {
			scrollTop: el.scrollTop,
			scrollHeight: el.scrollHeight,
			clientHeight: el.clientHeight
		};
	}

	async function scrollBottom() {
		await tick();
		if (logEl && stickToBottom) logEl.scrollTop = pinScrollBottom();
	}

	$effect(() => {
		// Depend on message list + element bind so pin / fill run after DOM.
		messages;
		logEl;
		if (stickToBottom) void scrollBottom();
		// The first page may not fill the panel — pull history until scrollable.
		// A failed load waits for scroll or Retry instead of looping.
		if (
			logEl &&
			$lobby.hasMore &&
			!$lobby.fetchingMore &&
			$lobby.fetchMoreError === undefined &&
			needsHistoryFill(metricsOf(logEl))
		) {
			void loadOlder();
		}
	});

	function shortId(id: string) {
		if (!id) return '?';
		if (id.length <= 10) return id;
		return id.slice(0, 6) + '…';
	}

	function formatWhen(raw: string) {
		if (!raw) return '';
		// Server may emit unix millis as a decimal string (no chrono in fixture).
		const asNum = /^\d{11,16}$/.test(raw.trim()) ? Number(raw) : NaN;
		const d = Number.isFinite(asNum) ? new Date(asNum) : new Date(raw);
		if (Number.isNaN(d.getTime())) return raw;
		return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
	}

	function messageIsMine(m: ChatMsg): boolean {
		return isOwnAuthor(m.author_id, me, {
			authorUserId: m.author?.user_id,
			displayName: m.author?.display_name
		});
	}

	/** Prefer AuthUsers.display_name; fall back to email, then a short id. */
	function authorName(m: ChatMsg): string {
		const joined = (m as { author?: { display_name?: string; email?: string } | null })
			.author;
		const name = joined?.display_name?.trim();
		if (name) return name;
		const email = joined?.email?.trim();
		if (email) return email.split('@')[0] || email;
		// Optimistic own rows may lack the author edge until projection lands.
		if (messageIsMine(m)) return displayName;
		return shortId(m.author_id);
	}

	/**
	 * iMessage-style: only the latest consecutive own message shows a status
	 * under the bubble (not every message).
	 */
	function isStatusFooterMessage(index: number): boolean {
		const m = messages[index];
		if (!m || !messageIsMine(m)) return false;
		const next = messages[index + 1];
		return next === undefined || !messageIsMine(next);
	}

	/**
	 * Sent while the row is still this client's preview; Delivered once the
	 * server's row replaced it. A failed send rolls the row back entirely.
	 */
	function deliveryLabel(m: ChatMsg): 'sent' | 'delivered' {
		return $lobby.pending.has(m) ? 'sent' : 'delivered';
	}

	function onLogScroll() {
		if (!logEl) return;
		const m = metricsOf(logEl);
		// Detach as soon as the user leaves the newest edge so live updates
		// cannot re-pin and cancel a scroll-up for history.
		stickToBottom = nearBottom(m);
		if (nearTop(m)) void loadOlder();
	}

	async function loadOlder() {
		const current = lobby.get();
		if (current.fetchingMore || !current.hasMore) return;
		// Reading older history leaves the newest edge.
		stickToBottom = false;
		const el = logEl;
		const prevTop = el?.scrollTop ?? 0;
		const prevHeight = el?.scrollHeight ?? 0;
		await lobby.fetchMore();
		await tick();
		// Content grew at the visual top; keep the same messages in view.
		if (el) el.scrollTop = preserveScrollAfterPrepend(prevTop, prevHeight, el.scrollHeight);
	}

	async function onSend(e: Event) {
		e.preventDefault();
		const body = draft.trim();
		if (!body || busy || !commands) return;
		sendError = null;
		busy = true;
		// Clear only the submitted draft. Anything typed while this command is
		// pending belongs to the next message and must survive its completion.
		draft = '';
		// Own sends should land in view.
		stickToBottom = true;
		try {
			// `message_id` (uuid_v7) and `created_at` (now) are command defaults:
			// the preview row shows this client's clock until the server's row,
			// stamped with the host clock, replaces it and the order settles.
			await commands.chat.post({ body, room_id: LOBBY_ROOM });
		} catch (error) {
			if (!draft.trim()) draft = body;
			sendError = error instanceof Error ? error.message : 'send failed';
		} finally {
			busy = false;
		}
	}
</script>

<AppPage>
	<PageHeader kicker="Room · {LOBBY_ROOM}" title="Lobby">
		{#snippet meta()}
			<div class="ch-status" data-state={$lobby.live}>
				<span class="ch-pulse" aria-hidden="true"></span>
				{#if $lobby.live === 'active'}
					Live
				{:else if $lobby.live === 'connecting'}
					Connecting…
				{:else if $lobby.live === 'error'}
					Offline
				{:else}
					Idle
				{/if}
			</div>
		{/snippet}
		{#if signedIn}
			Newest {PAGE_SIZE} messages stay live. Scroll up for older history. Signed in as
			<strong>{displayName}</strong>.
		{:else}
			Reading as <strong>anonymous</strong> (e2e-ui-public). Sign in to post.
		{/if}
		<a class="ch-info-link" data-testid="chat-child-link" href="/chat/about">
			About this room
		</a>
	</PageHeader>

	{#if data.gqlError}
		<InlineAlert label="SSR GraphQL">{data.gqlError}</InlineAlert>
	{/if}
	{#if $lobby.error}
		<InlineAlert label="Subscription">
			{$lobby.error.message}
			<button type="button" class="ch-link-btn" onclick={() => void lobby.refetch()}>Retry</button>
		</InlineAlert>
	{/if}
	{#if $lobby.fetchMoreError}
		<InlineAlert label="History">
			{$lobby.fetchMoreError.message}
			<button type="button" class="ch-link-btn" onclick={() => void loadOlder()}>Retry</button>
		</InlineAlert>
	{/if}
	{#if sendError}
		<InlineAlert label="Mutation">{sendError}</InlineAlert>
	{/if}

	<div class="ch-shell">
		<!--
			column-reverse: main-start is the visual bottom, so scrollTop=0 (the
			browser default, including SSR HTML) already shows the newest end.
			No hide-until-JS pin required.
		-->
		<div
			class="ch-log"
			bind:this={logEl}
			onscroll={onLogScroll}
			role="log"
			aria-live="polite"
			aria-relevant="additions"
			data-chat-page-size={PAGE_SIZE}
			data-has-more-history={$lobby.hasMore ? '1' : '0'}
			data-loading-history={$lobby.fetchingMore ? '1' : '0'}
		>
			<div class="ch-log-stack">
				{#if $lobby.fetchingMore}
					<div class="ch-history-hint" aria-live="polite">Loading earlier messages…</div>
				{:else if $lobby.hasMore && messages.length > 0}
					<button
						type="button"
						class="ch-history-hint ch-history-load"
						onclick={() => void loadOlder()}
						data-testid="chat-load-earlier"
					>
						Scroll up or click for earlier messages
					</button>
				{:else if !$lobby.hasMore && messages.length > 0}
					<div class="ch-history-hint">Beginning of lobby history</div>
				{/if}

				{#if messages.length === 0}
					<div class="ch-empty">
						<div class="ch-empty-icon" aria-hidden="true">◇</div>
						<p>No messages yet. Say hello to the lobby.</p>
					</div>
				{:else}
					{#each messages as m, i (m.message_id)}
						{@const mine = messageIsMine(m)}
						{@const authorLabel = mine ? 'You' : authorName(m)}
						{@const showStatus = isStatusFooterMessage(i)}
						{@const delivery = showStatus ? deliveryLabel(m) : null}
						<div class="ch-msg-block" class:mine style="--i: {i}">
							<article class="ch-msg" class:mine>
								<header class="ch-msg-meta">
									<span class="ch-author" title={m.author_id}>{authorLabel}</span>
									<time class="ch-when" datetime={m.created_at}>{formatWhen(m.created_at)}</time>
								</header>
								<p class="ch-body">{m.body}</p>
							</article>
							{#if showStatus}
								<p class="ch-status-footer" data-state={delivery}>
									{delivery === 'sent' ? 'Sent' : 'Delivered'}
								</p>
							{/if}
						</div>
					{/each}
				{/if}
			</div>
		</div>

		{#if signedIn}
			<form class="ch-composer" onsubmit={onSend}>
				<label class="ch-sr" for="chat-body">Message</label>
				<input
					id="chat-body"
					class="ch-input"
					name="body"
					placeholder="Message the lobby…"
					required
					autocomplete="off"
					bind:value={draft}
				/>
				<Button type="submit" variant="ink" disabled={!draft.trim() || busy}>
					Send
					<svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
						<path
							d="M5 12h14M13 6l6 6-6 6"
							stroke="currentColor"
							stroke-width="2.4"
							stroke-linecap="round"
							stroke-linejoin="round"
						/>
					</svg>
				</Button>
			</form>
		{:else}
			<div class="ch-composer ch-composer-guest" data-testid="chat-guest-cta">
				<p class="ch-guest-copy">
					Lobby is readable without signing in (anonymous GraphQL). Posting requires a session.
				</p>
				<Button variant="ink" href="/signin?callbackUrl=/chat">Sign in to post</Button>
			</div>
		{/if}
	</div>
</AppPage>

<HowItsBuilt demo={chatWalkthrough} />

<style>
	/* Lobby shell + messages — page-local only */
	.ch-shell {
		--edge: var(--wf-line, #e2e0d9);
		--ink: var(--wf-ink, #1c1c1a);
		--ink-soft: var(--wf-ink-soft, #5c5c56);
		--accent: var(--wf-accent, #3d5a80);
		--surface: var(--wf-bg-elevated, #fff);
		--bubble: #f0efe9;
		--bubble-mine: var(--wf-ink, #1c1c1a);

		display: flex;
		flex-direction: column;
		/* Fixed height so the log scrolls, not the document window. */
		height: min(calc(100dvh - 13.5rem), 42rem);
		min-height: 18rem;
		border-radius: var(--df-radius-lg, 10px);
		border: 1px solid var(--edge);
		background: var(--surface);
		overflow: hidden;
	}

	.ch-status {
		display: inline-flex;
		align-items: center;
		gap: 0.4rem;
		padding: 0.35rem 0.7rem;
		border-radius: 999px;
		font-size: 0.72rem;
		font-weight: 600;
		letter-spacing: 0.04em;
		text-transform: uppercase;
		background: transparent;
		border: 1px solid var(--wf-line, #e2e0d9);
		color: var(--wf-ink-soft, #5c5c56);
	}

	.ch-info-link {
		margin-left: 0.45rem;
		color: var(--wf-accent, #3d5a80);
		font-weight: 650;
		text-decoration-thickness: 1px;
		text-underline-offset: 0.18em;
	}

	.ch-status[data-state='active'] {
		color: var(--wf-success, #2f6f4e);
		background: rgba(47, 111, 78, 0.1);
		border-color: rgba(47, 111, 78, 0.22);
	}

	.ch-status[data-state='error'] {
		color: var(--wf-danger, #b33a3a);
		background: rgba(179, 58, 58, 0.08);
		border-color: rgba(179, 58, 58, 0.22);
	}

	.ch-status[data-state='connecting'] {
		color: var(--wf-accent, #3d5a80);
		background: var(--wf-accent-soft, rgba(61, 90, 128, 0.08));
		border-color: var(--wf-line, #e2e0d9);
	}

	.ch-pulse {
		width: 0.45rem;
		height: 0.45rem;
		border-radius: 50%;
		background: currentColor;
	}

	.ch-status[data-state='active'] .ch-pulse {
		box-shadow: 0 0 0 0 rgba(47, 111, 78, 0.45);
		animation: ch-pulse 1.6s ease infinite;
	}

	@keyframes ch-pulse {
		70% {
			box-shadow: 0 0 0 7px transparent;
		}
		100% {
			box-shadow: 0 0 0 0 transparent;
		}
	}

	.ch-link-btn {
		margin-left: 0.75rem;
		border: none;
		background: transparent;
		color: inherit;
		font: inherit;
		font-weight: 600;
		text-decoration: underline;
		cursor: pointer;
	}

	/*
	 * column-reverse makes main-start the visual bottom. Default scrollTop=0
	 * (SSR + first paint) already shows newest messages — no JS pin flash.
	 */
	.ch-log {
		flex: 1;
		min-height: 0;
		overflow-y: auto;
		padding: 1.1rem 1rem 0.75rem;
		display: flex;
		flex-direction: column-reverse;
		scroll-behavior: auto;
	}

	.ch-log-stack {
		display: flex;
		flex-direction: column;
		gap: 0.55rem;
		/* Grow from the bottom when short; scroll when tall. */
		min-height: min-content;
		width: 100%;
	}

	.ch-history-hint {
		align-self: center;
		padding: 0.25rem 0.6rem;
		font-size: 0.72rem;
		font-weight: 500;
		letter-spacing: 0.02em;
		color: var(--ink-soft);
		opacity: 0.85;
	}

	.ch-history-load {
		border: 1px dashed var(--edge);
		border-radius: 999px;
		background: transparent;
		cursor: pointer;
		font: inherit;
	}

	.ch-history-load:hover {
		opacity: 1;
		border-color: var(--accent);
		color: var(--accent);
	}

	.ch-empty {
		margin: 2rem auto;
		text-align: center;
		color: var(--ink-soft);
		padding: 2rem 1rem;
	}

	.ch-empty-icon {
		font-size: 1.35rem;
		opacity: 0.35;
		margin-bottom: 0.5rem;
	}

	/* iMessage-like block: bubble + optional status under the last own message */
	.ch-msg-block {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		max-width: min(88%, 28rem);
		align-self: flex-start;
	}

	.ch-msg-block.mine {
		align-self: flex-end;
		align-items: flex-end;
	}

	.ch-msg {
		width: 100%;
		padding: 0.6rem 0.8rem 0.7rem;
		border-radius: 10px 10px 10px 4px;
		background: var(--bubble);
		border: 1px solid var(--edge);
	}

	.ch-msg.mine {
		border-radius: 10px 10px 4px 10px;
		background: var(--bubble-mine);
		border-color: transparent;
		color: #f6f5f2;
	}

	.ch-msg-meta {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 0.75rem;
		margin-bottom: 0.2rem;
	}

	.ch-author {
		font-size: 0.7rem;
		font-weight: 600;
		letter-spacing: 0.04em;
		text-transform: none;
		opacity: 0.75;
	}

	.ch-when {
		font-size: 0.68rem;
		font-variant-numeric: tabular-nums;
		opacity: 0.5;
	}

	/* iMessage: small gray caption under the last bubble you sent */
	.ch-status-footer {
		margin: 0.15rem 0.35rem 0;
		padding: 0;
		font-size: 0.68rem;
		font-weight: 400;
		letter-spacing: 0.01em;
		color: var(--ink-soft);
		opacity: 0.85;
		font-variant-numeric: tabular-nums;
	}

	.ch-status-footer[data-state='sent'] {
		opacity: 0.7;
	}

	.ch-status-footer[data-state='delivered'] {
		opacity: 0.9;
	}

	.ch-body {
		margin: 0;
		font-size: 0.95rem;
		line-height: 1.45;
		white-space: pre-wrap;
		word-break: break-word;
	}

	.ch-composer {
		flex-shrink: 0;
		display: flex;
		gap: 0.5rem;
		padding: 0.75rem;
		border-top: 1px solid var(--edge);
		background: rgba(28, 28, 26, 0.02);
	}

	.ch-composer-guest {
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem 1rem;
	}

	.ch-guest-copy {
		margin: 0;
		flex: 1;
		min-width: 12rem;
		font-size: 0.88rem;
		line-height: 1.45;
		color: var(--ink-soft);
	}

	.ch-input {
		flex: 1;
		min-width: 0;
		border: 1px solid var(--edge);
		border-radius: var(--wf-radius, 6px);
		padding: 0.65rem 0.85rem;
		font: inherit;
		font-size: 0.95rem;
		background: #fff;
		color: var(--ink);
		outline: none;
		transition:
			border-color 0.15s ease,
			box-shadow 0.15s ease;
	}

	.ch-input:focus {
		border-color: var(--accent);
		box-shadow: 0 0 0 3px var(--wf-accent-soft, rgba(61, 90, 128, 0.12));
	}

	.ch-sr {
		position: absolute;
		width: 1px;
		height: 1px;
		padding: 0;
		margin: -1px;
		overflow: hidden;
		clip: rect(0, 0, 0, 0);
		border: 0;
	}
</style>
