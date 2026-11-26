/**
 * Lobby chat log geometry (pure). Paging, ordering and de-duplication belong
 * to the generated `ChatMessages.useWindow()`.
 *
 * The scroller uses `flex-direction: column-reverse`. Browser scrollTop models:
 *
 * - **Chromium (negative):** scrollTop === 0 is newest (visual bottom);
 *   scrollTop decreases toward `-(scrollHeight - clientHeight)` at the oldest edge.
 * - **Positive model (some engines):** scrollTop === 0 is newest; increases toward
 *   `+(scrollHeight - clientHeight)` at the oldest edge.
 *
 * Helpers normalize both so near-bottom / near-top and scroll preserve work.
 */

/** Detach stick-to-bottom once the user leaves the newest edge. */
export const NEAR_BOTTOM_PX = 48;

/** Trigger history fetch when this close to the oldest edge. */
export const LOAD_TOP_PX = 80;

export type ChatLogMetrics = Readonly<{
	scrollTop: number;
	scrollHeight: number;
	clientHeight: number;
}>;

/** Scrollable range in px (always ≥ 0). */
export function scrollRange(m: ChatLogMetrics): number {
	return Math.max(0, m.scrollHeight - m.clientHeight);
}

/**
 * How far the viewport is from the newest edge (visual bottom).
 * Works for negative (Chromium reverse) and positive scrollTop models.
 */
export function distanceFromBottom(m: ChatLogMetrics): number {
	if (m.scrollTop < 0) return -m.scrollTop;
	return Math.max(0, m.scrollTop);
}

/**
 * How far the viewport is from the oldest edge (visual top).
 */
export function distanceFromTop(m: ChatLogMetrics): number {
	const range = scrollRange(m);
	if (m.scrollTop < 0) {
		// At oldest when scrollTop === -range.
		return Math.max(0, range + m.scrollTop);
	}
	// Positive model: at oldest when scrollTop === range.
	return Math.max(0, range - m.scrollTop);
}

export function nearBottom(
	m: ChatLogMetrics,
	thresholdPx: number = NEAR_BOTTOM_PX
): boolean {
	return distanceFromBottom(m) < thresholdPx;
}

export function nearTop(m: ChatLogMetrics, thresholdPx: number = LOAD_TOP_PX): boolean {
	if (scrollRange(m) <= 0) return false;
	return distanceFromTop(m) < thresholdPx;
}

/** Content does not fill the scroller — load older until it does or history ends. */
export function needsHistoryFill(m: ChatLogMetrics): boolean {
	return m.scrollHeight <= m.clientHeight + 1;
}

/** Pin newest end under column-reverse (both scrollTop models). */
export function pinScrollBottom(): number {
	return 0;
}

/**
 * After prepending older rows (stack grows at the visual top), keep the same
 * messages under the viewport.
 *
 * - Negative model (Chromium): scrollTop is bottom-relative; keep it.
 * - Positive model: add the height delta to scrollTop.
 */
export function preserveScrollAfterPrepend(
	prevScrollTop: number,
	prevScrollHeight: number,
	nextScrollHeight: number
): number {
	const delta = Math.max(0, nextScrollHeight - prevScrollHeight);
	if (prevScrollTop < 0) return prevScrollTop;
	return prevScrollTop + delta;
}
