import { test, expect } from '@playwright/test';

test.describe('blob game (alice)', () => {
	test('start a game, paint board, move, show in history', async ({ page }) => {
		await page.goto('/blob');
		await expect(page.getByRole('heading', { name: /blob game/i })).toBeVisible();

		// Wait for client hydration so Start/New game handlers are attached.
		await expect(page.locator('[data-blob-hydrated="1"]')).toBeVisible({ timeout: 15_000 });
		await expect(page.getByTestId('blob-start-game')).toBeEnabled({ timeout: 10_000 });

		// Empty state — no fake grid pretending to be a game
		await expect(page.getByText(/no game selected/i)).toBeVisible();
		await expect(page.locator('.blob-board')).toHaveCount(0);

		const start = page.getByTestId('blob-start-game');
		const [resp] = await Promise.all([
			page.waitForResponse(
				(r) =>
					r.url().includes('/graphql') &&
					r.request().method() === 'POST' &&
					(r.request().postData() ?? '').includes('blob_games_start'),
				{ timeout: 20_000 }
			),
			start.click()
		]);
		expect(resp.ok(), `blob_games_start HTTP ${resp.status()}`).toBeTruthy();

		// Real board from command payload
		const board = page.locator('.blob-board');
		await expect(board).toBeVisible({ timeout: 15_000 });
		await expect(page).toHaveURL(/\/blob\/[^/]+$/);
		await expect(board.locator('.cell').first()).toBeVisible();
		await expect(page.locator('.hud-value').first()).toBeVisible();

		// Score starts at 0
		await expect(page.locator('.blob-hud')).toContainText(/score/i);

		// Generated levels start at the top-left. A known boundary is a local
		// no-op: no rejected command and no framework error should escape.
		let wallMoveRequests = 0;
		const countWallMoves = (request: import('@playwright/test').Request) => {
			if (
				request.url().includes('/graphql') &&
				(request.postData() ?? '').includes('blob_games_move')
			) {
				wallMoveRequests += 1;
			}
		};
		page.on('request', countWallMoves);
		await page.keyboard.press('ArrowUp');
		await page.waitForTimeout(250);
		page.off('request', countWallMoves);
		expect(wallMoveRequests, 'a board-edge move should not reach GraphQL').toBe(0);
		await expect(page.locator('.inline-alert')).toHaveCount(0);

		const navigations: string[] = [];
		page.on('framenavigated', (frame) => {
			if (frame === page.mainFrame()) navigations.push(frame.url());
		});
		await page.evaluate(() => {
			const samples = [document.querySelectorAll('.blob-board').length];
			const observer = new MutationObserver(() => {
				samples.push(document.querySelectorAll('.blob-board').length);
			});
			observer.observe(document.querySelector('.blob-page')!, {
				childList: true,
				subtree: true,
				characterData: true
			});
			Object.assign(globalThis, {
				__distributedBlobContinuitySamples: samples,
				__distributedBlobContinuityObserver: observer
			});
		});

		let releaseFirstMove!: () => void;
		const firstMoveRelease = new Promise<void>((resolve) => {
			releaseFirstMove = resolve;
		});
		let firstMoveBlocked!: () => void;
		const firstMoveIsBlocked = new Promise<void>((resolve) => {
			firstMoveBlocked = resolve;
		});
		let dispatchedMoves = 0;
		const moveResponses: import('@playwright/test').Response[] = [];
		page.on('response', (response) => {
			if ((response.request().postData() ?? '').includes('blob_games_move')) {
				moveResponses.push(response);
			}
		});
		await page.route('**/graphql', async (route) => {
			if (!(route.request().postData() ?? '').includes('blob_games_move')) {
				await route.continue();
				return;
			}
			dispatchedMoves += 1;
			if (dispatchedMoves === 1) {
				firstMoveBlocked();
				// Hold before route.fetch: the server has not seen the first move,
				// and the runtime must retain later local layers while serializing
				// same-game transport behind it.
				await firstMoveRelease;
			}
			const response = await route.fetch();
			await route.fulfill({ response });
		});

		// The first local pure moves right while its request is held before the
		// server. A second legal move must stack immediately on that optimistic
		// board even though its transport remains ordered behind the first.
		await page.keyboard.press('ArrowRight');
		await firstMoveIsBlocked;
		await expect(board.locator('.tile-player')).toHaveAttribute(
			'aria-label',
			'r0 c1',
			{ timeout: 200 }
		);
		const rightClass =
			(await page
				.locator('.blob-board .cell[aria-label="r0 c2"]')
				.getAttribute('class')) ?? '';
		const secondMove = rightClass.includes('tile-hole')
			? ({ key: 'ArrowDown', label: 'r1 c1' } as const)
			: ({ key: 'ArrowRight', label: 'r0 c2' } as const);
		await expect(
			page.locator(`.blob-board .cell[aria-label="${secondMove.label}"]`)
		).not.toHaveClass(/tile-hole/);
		await page.keyboard.press(secondMove.key);
		await expect(board.locator('.tile-player')).toHaveAttribute(
			'aria-label',
			secondMove.label,
			{ timeout: 200 }
		);
		expect(
			dispatchedMoves,
			'later same-game transport must wait without blocking its optimistic layer'
		).toBe(1);
		await expect(page.getByTestId('blob-new-game')).toBeEnabled();
		for (const button of await page.locator('.pad-btn').all()) {
			await expect(button).toBeEnabled();
		}
		releaseFirstMove();
		await expect
			.poll(() => moveResponses.length, { timeout: 20_000 })
			.toBe(2);
		for (const response of moveResponses) {
			expect(response.ok(), `blob_games_move HTTP ${response.status()}`).toBeTruthy();
		}
		await expect(board.locator('.tile-player')).toHaveAttribute(
			'aria-label',
			secondMove.label
		);
		await page.unrouteAll({ behavior: 'wait' });
		await expect(board).toBeVisible();
		await expect(page.locator('.inline-alert, .blob-empty')).toHaveCount(0);

		// History lists this game
		await expect(page.getByRole('heading', { name: /your games/i })).toBeVisible({
			timeout: 15_000
		});
		await expect(page.locator('.history-item').first()).toBeVisible({ timeout: 15_000 });

		// Soft URL may include game id (replaceState) or stay /blob
		await page.waitForTimeout(300);
		expect(page.url()).toMatch(/\/blob/);
		const samples = await page.evaluate(() => {
			const state = globalThis as typeof globalThis & {
				__distributedBlobContinuitySamples: number[];
				__distributedBlobContinuityObserver: MutationObserver;
			};
			state.__distributedBlobContinuityObserver.disconnect();
			return state.__distributedBlobContinuitySamples;
		});
		expect(navigations, 'move commands must not navigate or reload the page').toEqual([]);
		expect(
			Math.min(...samples),
			'stale-while-revalidate must never replace the known board with an empty view'
		).toBeGreaterThan(0);
	});

	test('new game replaces a selected game without a page reload', async ({ page }) => {
		await page.goto('/blob');
		await expect(page.locator('[data-blob-hydrated="1"]')).toBeVisible({
			timeout: 15_000
		});
		await expect(page.getByTestId('blob-start-game')).toBeEnabled({ timeout: 10_000 });

		const startResponse = page.waitForResponse(
			(response) =>
				(response.request().postData() ?? '').includes('blob_games_start'),
			{ timeout: 20_000 }
		);
		await page.getByTestId('blob-start-game').click();
		expect((await startResponse).ok()).toBeTruthy();
		await expect(page.locator('.blob-board')).toBeVisible({ timeout: 15_000 });
		const firstGameUrl = page.url();

		const continuityToken = `blob-new-game-${Date.now()}`;
		await page.evaluate((token) => {
			Object.assign(globalThis, { __distributedBlobNewGameToken: token });
		}, continuityToken);
		const newGameResponse = page.waitForResponse(
			(response) =>
				(response.request().postData() ?? '').includes('blob_games_start'),
			{ timeout: 20_000 }
		);
		await page.getByTestId('blob-new-game').click();
		expect((await newGameResponse).ok()).toBeTruthy();

		await expect(page).not.toHaveURL(firstGameUrl);
		await expect(page.locator('.blob-board .tile-player')).toHaveAttribute(
			'aria-label',
			'r0 c0',
			{ timeout: 1_000 }
		);
		await expect(page.locator('.blob-empty')).toHaveCount(0);
		expect(
			await page.evaluate(
				() =>
					(globalThis as typeof globalThis & {
						__distributedBlobNewGameToken?: string;
					}).__distributedBlobNewGameToken
			),
			'New game must preserve the current document while changing routes'
		).toBe(continuityToken);
	});

	// A revalidation that started before an Atomic move must not roll it back
	// with later record evidence: that is replica behaviour, proved
	// deterministically by js/tests/replica-protocol.test.mjs ("a revalidation
	// started before an Atomic row cannot roll it back with later evidence")
	// without a page-level refetch hook.
});
