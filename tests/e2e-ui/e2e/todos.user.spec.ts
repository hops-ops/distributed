import { test, expect } from '@playwright/test';

async function visibleTodoOrders(page: import('@playwright/test').Page) {
	return page.locator('.list').evaluateAll((lists) =>
		lists.map((list) =>
			[...list.querySelectorAll<HTMLElement>('[data-todo-id]')].map(
				(item) => item.dataset.todoId ?? ''
			)
		)
	);
}

type TodoOrderFrame = {
	open: string[];
	done: string[];
};

async function startTodoOrderTrace(page: import('@playwright/test').Page) {
	await page.evaluate(() => {
		const state = globalThis as typeof globalThis & {
			__todoOrderTrace?: {
				frames: TodoOrderFrame[];
				frame: number;
			};
		};
		const frames: TodoOrderFrame[] = [];
		const orderFor = (name: string) => {
			const panel = [...document.querySelectorAll<HTMLElement>('.panel')].find(
				(candidate) =>
					candidate.querySelector('h2')?.textContent?.trim().toLowerCase() ===
					name
			);
			return panel
				? [...panel.querySelectorAll<HTMLElement>('[data-todo-id]')].map(
						(item) => item.dataset.todoId ?? ''
					)
				: [];
		};
		const sample = () => {
			const order = { open: orderFor('open'), done: orderFor('done') };
			const previous = frames.at(-1);
			if (
				previous === undefined ||
				JSON.stringify(previous) !== JSON.stringify(order)
			) {
				frames.push(order);
			}
			state.__todoOrderTrace!.frame = requestAnimationFrame(sample);
		};
		state.__todoOrderTrace = {
			frames,
			frame: requestAnimationFrame(sample)
		};
	});
}

async function stopTodoOrderTrace(page: import('@playwright/test').Page) {
	return page.evaluate(() => {
		const state = globalThis as typeof globalThis & {
			__todoOrderTrace?: {
				frames: TodoOrderFrame[];
				frame: number;
			};
		};
		const trace = state.__todoOrderTrace;
		if (trace === undefined) return [];
		cancelAnimationFrame(trace.frame);
		delete state.__todoOrderTrace;
		return trace.frames;
	});
}

function expectBinarySorted(orders: string[][]) {
	for (const ids of orders) {
		expect(ids, `todo ids must stay in generated binary order: ${ids.join(', ')}`).toEqual(
			[...ids].sort()
		);
	}
}

function isBinarySorted(ids: string[]) {
	return JSON.stringify(ids) === JSON.stringify([...ids].sort());
}

function validTodoTransitionFrame(frame: TodoOrderFrame, todoId: string) {
	const occurrences =
		frame.open.filter((id) => id === todoId).length +
		frame.done.filter((id) => id === todoId).length;
	return isBinarySorted(frame.open) && isBinarySorted(frame.done) && occurrences === 1;
}

function todoIsIn(frame: TodoOrderFrame, todoId: string, panel: 'open' | 'done') {
	return frame[panel].includes(todoId);
}

function waitForTodoCommand(
	page: import('@playwright/test').Page,
	mutation: 'todos_create' | 'todos_complete' | 'todos_reopen' | 'todos_archive'
) {
	return page.waitForResponse(
		(response) =>
			response.url().includes('/graphql') &&
			(response.request().postData() ?? '').includes(mutation),
		{ timeout: 20_000 }
	);
}

async function expectTodoCommandSuccess(
	response: import('@playwright/test').Response,
	mutation: string
) {
	const responseBody = await response.text();
	expect(
		response.ok(),
		`${mutation} HTTP ${response.status()}: ${responseBody.slice(0, 400)}`
	).toBeTruthy();
	expect(
		responseBody,
		`${mutation} GraphQL errors: ${responseBody.slice(0, 600)}`
	).not.toMatch(/"errors"\s*:\s*\[/);
}

test.describe('todos (alice)', () => {
	test('create, complete, reopen, archive', async ({ page }) => {
		const title = `e2e todo ${Date.now()}`;

		await page.goto('/todos');
		await expect(page.getByRole('heading', { name: /todos/i })).toBeVisible();

		// Create
		await page.locator('#todo-title').fill(title);
		const createResponse = waitForTodoCommand(page, 'todos_create');
		await page.getByRole('button', { name: /^add$/i }).click();
		expect((await createResponse).ok(), 'todos_create must succeed').toBeTruthy();
		const openItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) })
			.locator('.item', { hasText: title });
		await expect(openItem).toBeVisible({ timeout: 30_000 });

		// Complete
		const completeResponse = waitForTodoCommand(page, 'todos_complete');
		await openItem.getByRole('button', { name: /^done$/i }).click();
		const doneItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^done$/i }) })
			.locator('.item', { hasText: title });
		await expect(doneItem).toBeVisible({ timeout: 15_000 });
		expect((await completeResponse).ok(), 'todos_complete must succeed').toBeTruthy();

		// Reopen (prefer the text button; wait until not busy)
		const reopen = doneItem.getByRole('button', { name: 'Reopen', exact: true });
		await expect(reopen).toBeEnabled({ timeout: 10_000 });
		const reopenResponse = waitForTodoCommand(page, 'todos_reopen');
		await reopen.click();
		const openAgain = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) })
			.locator('.item', { hasText: title });
		await expect(openAgain).toBeVisible({ timeout: 15_000 });
		await reopenResponse;

		// Archive
		const archive = openAgain.getByRole('button', { name: 'Archive', exact: true });
		await expect(archive).toBeEnabled({ timeout: 10_000 });
		const archiveResponse = waitForTodoCommand(page, 'todos_archive');
		await archive.click();
		const archiveDetails = page.locator('details.archive');
		await expect(archiveDetails).toBeVisible({ timeout: 15_000 });
		await archiveDetails.locator('summary').click();
		await expect(archiveDetails.locator('.item', { hasText: title })).toBeVisible({
			timeout: 10_000
		});
		// Optimistic visibility is deliberately immediate. Do not tear down the
		// browser runtime until the durable command response has arrived.
		await archiveResponse;

		// Persist across navigation (async projectors may lag a beat)
		await expect
			.poll(
				async () => {
					await page.goto('/todos');
					const again = page.locator('details.archive');
					if (!(await again.isVisible().catch(() => false))) return false;
					await again.locator('summary').click();
					return again.locator('.item', { hasText: title }).isVisible();
				},
				{ timeout: 40_000 }
			)
			.toBeTruthy();
	});

	test('empty title cannot submit', async ({ page }) => {
		await page.goto('/todos');
		const add = page.getByRole('button', { name: /^add$/i });
		await expect(add).toBeDisabled();
	});

	test('commands preserve rendered cache while settling', async ({ page }) => {
		await page.goto('/todos');
		await expect(page.getByRole('heading', { name: /todos/i })).toBeVisible();

		// Establish one row that must remain visible while later commands update.
		const baseline = `continuity baseline ${Date.now()}`;
		await page.locator('#todo-title').fill(baseline);
		const baselineCreateResponse = waitForTodoCommand(page, 'todos_create');
		await page.getByRole('button', { name: /^add$/i }).click();
		await expect(page.locator('.item', { hasText: baseline })).toBeVisible();
		await expectTodoCommandSuccess(
			await baselineCreateResponse,
			'baseline todos_create'
		);

		const navigations: string[] = [];
		page.on('framenavigated', (frame) => {
			if (frame === page.mainFrame()) navigations.push(frame.url());
		});
		await page.evaluate(() => {
			const samples = [document.querySelectorAll('.item').length];
			const observer = new MutationObserver(() => {
				samples.push(document.querySelectorAll('.item').length);
			});
			observer.observe(document.querySelector('.board')!, {
				childList: true,
				subtree: true,
				characterData: true
			});
			Object.assign(globalThis, {
				__distributedContinuitySamples: samples,
				__distributedContinuityObserver: observer
			});
		});

		const changed = `continuity changed ${Date.now()}`;
		await page.locator('#todo-title').fill(changed);
		const changedCreateResponse = waitForTodoCommand(page, 'todos_create');
		await page.getByRole('button', { name: /^add$/i }).click();
		const openItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) })
			.locator('.item', { hasText: changed });
		await expect(openItem).toBeVisible();
		await expectTodoCommandSuccess(
			await changedCreateResponse,
			'changed todos_create'
		);
		const completeResponse = waitForTodoCommand(page, 'todos_complete');
		await openItem.getByRole('button', { name: /^done$/i }).click();
		await expect(
			page
				.locator('.panel')
				.filter({ has: page.getByRole('heading', { name: /^done$/i }) })
				.locator('.item', { hasText: changed })
		).toBeVisible();
		await expectTodoCommandSuccess(
			await completeResponse,
			'continuity todos_complete'
		);

		const samples = await page.evaluate(() => {
			const state = globalThis as typeof globalThis & {
				__distributedContinuitySamples: number[];
				__distributedContinuityObserver: MutationObserver;
			};
			state.__distributedContinuityObserver.disconnect();
			return state.__distributedContinuitySamples;
		});
		expect(navigations, 'commands must not navigate or reload the page').toEqual([]);
		expect(
			Math.min(...samples),
			'stale-while-revalidate must never replace known rows with an empty view'
		).toBeGreaterThan(0);
	});

	test('optimistic and authoritative states keep the same generated order', async ({
		page
	}) => {
		await page.goto('/todos');
		await expect(page.getByRole('heading', { name: /todos/i })).toBeVisible();

		await page.route('**/graphql', async (route) => {
			const body = route.request().postData() ?? '';
			if (
				!body.includes('todos_create') &&
				!body.includes('todos_complete') &&
				!body.includes('todos_reopen') &&
				!body.includes('todos_archive')
			) {
				await route.continue();
				return;
			}
			const response = await route.fetch();
			await new Promise((resolve) => setTimeout(resolve, 1_500));
			await route.fulfill({ response });
		});
		let completeRequests = 0;
		page.on('request', (request) => {
			if ((request.postData() ?? '').includes('todos_complete')) {
				completeRequests += 1;
			}
		});

		const title = `ordered optimism ${Date.now()}`;
		await page.locator('#todo-title').fill(title);
		const createResponse = page.waitForResponse(
			(response) =>
				(response.request().postData() ?? '').includes('todos_create')
		);
		await page.getByRole('button', { name: /^add$/i }).click();
		const openItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) })
			.locator('.item', { hasText: title });
		// The sourced transition supplies `status = Open`; the Todo projection
		// turns that event value into the upsert that must paint before the held
		// Eventual response.
		await expect(openItem).toBeVisible({ timeout: 1_000 });
		await expect(openItem).toHaveAttribute('aria-busy', 'true');
		await expect(openItem).toHaveClass(/item-pending/);
		await expect(openItem.locator('.pending-state')).toHaveText('Saving…');
		expect(
			await openItem.locator('button:disabled').count(),
			'a pending optimistic Todo must not expose actions before the server row replaces it'
		).toBe(3);
		await expectTodoCommandSuccess(await createResponse, 'ordered todos_create');
		await expect(openItem).toBeVisible();
		await expect(openItem).toHaveAttribute('aria-busy', 'false');
		await expect(openItem.locator('.pending-state')).toHaveCount(0);
		expect(
			await page.locator('.board button:disabled').count(),
			'Todo controls must unlock once the projected row replaces the preview'
		).toBe(0);
		expectBinarySorted(await visibleTodoOrders(page));
		const todoId = await openItem.getAttribute('data-todo-id');
		expect(todoId).not.toBeNull();
		await startTodoOrderTrace(page);
		const completeResponse = page.waitForResponse(
			(response) =>
				(response.request().postData() ?? '').includes('todos_complete')
		);
		await openItem.getByRole('button', { name: /^done$/i }).click();
		const doneItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^done$/i }) })
			.locator('.item', { hasText: title });
		// The projection maps the transition's `Completed` constant to a loaded-row
		// patch, so filtered membership changes before the wire is released.
		await expect(doneItem).toBeVisible({ timeout: 1_000 });
		expect(
			await page.locator('.board button:disabled').count(),
			'routine command concurrency guards must not flash Todo row controls disabled'
		).toBe(0);
		await expectTodoCommandSuccess(
			await completeResponse,
			'ordered todos_complete'
		);
		await expect(doneItem).toBeVisible();
		expectBinarySorted(await visibleTodoOrders(page));
		expect(completeRequests, 'complete must reach the server once').toBe(1);
		await expect(doneItem).toBeVisible();
		expectBinarySorted(await visibleTodoOrders(page));
		await page.waitForTimeout(750);
		const completeOrderFrames = await stopTodoOrderTrace(page);
		expect(
			completeOrderFrames.every((order) => validTodoTransitionFrame(order, todoId!)),
			`complete rendered an intermediate non-generated order: ${JSON.stringify(completeOrderFrames)}`
		).toBe(true);
		expect(
			completeOrderFrames.some((order) => todoIsIn(order, todoId!, 'done')),
			`complete never rendered the Todo in Done: ${JSON.stringify(completeOrderFrames)}`
		).toBe(true);

		await startTodoOrderTrace(page);
		const reopenResponse = page.waitForResponse((response) =>
			(response.request().postData() ?? '').includes('todos_reopen')
		);
		await doneItem.getByRole('button', { name: /^reopen$/i }).click();
		const reopenedItem = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) })
			.locator('.item', { hasText: title });
		await expect(reopenedItem).toBeVisible({ timeout: 1_000 });
		expect(
			await page.locator('.board button:disabled').count(),
			'routine command concurrency guards must not flash Todo row controls disabled'
		).toBe(0);
		await expectTodoCommandSuccess(await reopenResponse, 'ordered todos_reopen');
		await expect(reopenedItem).toBeVisible();
		await page.waitForTimeout(750);
		const reopenOrderFrames = await stopTodoOrderTrace(page);
		expect(
			reopenOrderFrames.every((order) => validTodoTransitionFrame(order, todoId!)),
			`reopen rendered an intermediate non-generated order: ${JSON.stringify(reopenOrderFrames)}`
		).toBe(true);
		expect(
			reopenOrderFrames.some((order) => todoIsIn(order, todoId!, 'open')),
			`reopen never rendered the Todo in Open: ${JSON.stringify(reopenOrderFrames)}`
		).toBe(true);
		await expect(reopenedItem).toBeVisible();
		expectBinarySorted(await visibleTodoOrders(page));

		// Repeat after both earlier commands have converged so the invariant also
		// covers a row that is fully authoritative before the next transition.
		await startTodoOrderTrace(page);
		const authoritativeCompleteResponse = page.waitForResponse((response) =>
			(response.request().postData() ?? '').includes('todos_complete')
		);
		await reopenedItem.getByRole('button', { name: /^done$/i }).click();
		await expect(doneItem).toBeVisible({ timeout: 1_000 });
		await expectTodoCommandSuccess(
			await authoritativeCompleteResponse,
			'authoritative todos_complete'
		);
		await page.waitForTimeout(750);
		const authoritativeCompleteFrames = await stopTodoOrderTrace(page);
		expect(
			authoritativeCompleteFrames.every((order) =>
				validTodoTransitionFrame(order, todoId!)
			),
			`authoritative complete rendered an intermediate order: ${JSON.stringify(authoritativeCompleteFrames)}`
		).toBe(true);
		expect(
			authoritativeCompleteFrames.some((order) => todoIsIn(order, todoId!, 'done')),
			`authoritative complete never rendered the Todo in Done: ${JSON.stringify(authoritativeCompleteFrames)}`
		).toBe(true);

		await startTodoOrderTrace(page);
		const authoritativeReopenResponse = page.waitForResponse((response) =>
			(response.request().postData() ?? '').includes('todos_reopen')
		);
		await doneItem.getByRole('button', { name: /^reopen$/i }).click();
		await expect(reopenedItem).toBeVisible({ timeout: 1_000 });
		await authoritativeReopenResponse;
		await page.waitForTimeout(750);
		const authoritativeReopenFrames = await stopTodoOrderTrace(page);
		expect(
			authoritativeReopenFrames.every((order) =>
				validTodoTransitionFrame(order, todoId!)
			),
			`authoritative reopen rendered an intermediate order: ${JSON.stringify(authoritativeReopenFrames)}`
		).toBe(true);
		expect(
			authoritativeReopenFrames.some((order) => todoIsIn(order, todoId!, 'open')),
			`authoritative reopen never rendered the Todo in Open: ${JSON.stringify(authoritativeReopenFrames)}`
		).toBe(true);
		await expect(reopenedItem).toBeVisible();
		expectBinarySorted(await visibleTodoOrders(page));

		const archiveResponse = page.waitForResponse((response) =>
			(response.request().postData() ?? '').includes('todos_archive')
		);
		await reopenedItem.getByRole('button', { name: /^archive$/i }).click();
		const archivedItem = page.locator('.archive .item', { hasText: title });
		await expect(archivedItem).toBeAttached({ timeout: 1_000 });
		await page.locator('details.archive').evaluate((details) => {
			(details as HTMLDetailsElement).open = true;
		});
		await expect(archivedItem).toBeVisible({ timeout: 1_000 });
		await archiveResponse;
		await expect(archivedItem).toBeVisible();
		await page.unrouteAll({ behavior: 'wait' });
	});

	test('rapid independent complete and reopen commands do not refetch or regress', async ({
		page
	}) => {
		await page.goto('/todos');
		await expect(page.getByRole('heading', { name: /todos/i })).toBeVisible();

		const prefix = `rapid transitions ${Date.now()}`;
		const titles = Array.from({ length: 6 }, (_, index) => `${prefix} ${index + 1}`);
		const todoIds: string[] = [];
		for (const title of titles) {
			await page.locator('#todo-title').fill(title);
			const response = waitForTodoCommand(page, 'todos_create');
			await page.getByRole('button', { name: /^add$/i }).click();
			await expectTodoCommandSuccess(await response, 'setup todos_create');
			const item = page.locator('.item', { hasText: title });
			await expect(item).toBeVisible();
			const todoId = await item.getAttribute('data-todo-id');
			expect(todoId).not.toBeNull();
			todoIds.push(todoId!);
		}
		await page.waitForLoadState('networkidle');

		let transitionQueries = 0;
		let transitionResponses = 0;
		page.on('request', (request) => {
			const body = request.postData() ?? '';
			if (body.includes('query Todos')) transitionQueries += 1;
		});
		page.on('response', (response) => {
			const body = response.request().postData() ?? '';
			if (body.includes('todos_complete') || body.includes('todos_reopen')) {
				transitionResponses += 1;
			}
		});
		await page.route('**/graphql', async (route) => {
			const body = route.request().postData() ?? '';
			if (!body.includes('todos_complete') && !body.includes('todos_reopen')) {
				await route.continue();
				return;
			}
			const response = await route.fetch();
			await new Promise((resolve) => setTimeout(resolve, 350));
			await route.fulfill({ response });
		});

		const openPanel = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^open$/i }) });
		const donePanel = page
			.locator('.panel')
			.filter({ has: page.getByRole('heading', { name: /^done$/i }) });

		await startTodoOrderTrace(page);
		for (const title of titles) {
			await openPanel
				.locator('.item', { hasText: title })
				.getByRole('button', { name: /^done$/i })
				.click();
		}
		for (const title of titles) {
			const item = donePanel.locator('.item', { hasText: title });
			await expect(item).toBeVisible({ timeout: 1_000 });
			await item.getByRole('button', { name: /^reopen$/i }).click();
		}

		for (const title of titles) {
			await expect(openPanel.locator('.item', { hasText: title })).toBeVisible({
				timeout: 1_000
			});
		}
		await expect
			.poll(() => transitionResponses, { timeout: 20_000 })
			.toBe(titles.length * 2);
		await page.waitForTimeout(750);

		const frames = await stopTodoOrderTrace(page);
		const allDoneFrame = frames.findIndex((frame) =>
			todoIds.every((todoId) => todoIsIn(frame, todoId, 'done'))
		);
		const fullyReopenedFrame = frames.findIndex(
			(frame, index) =>
				index > allDoneFrame &&
				todoIds.every((todoId) => todoIsIn(frame, todoId, 'open'))
		);
		for (const title of titles) {
			await expect(openPanel.locator('.item', { hasText: title })).toBeVisible();
			await expect(donePanel.locator('.item', { hasText: title })).toHaveCount(0);
		}
		expect(
			transitionQueries,
			'exact successful Todo deltas must not launch a full-list revalidation'
		).toBe(0);
		expect(allDoneFrame, `rapid transitions never reached Done: ${JSON.stringify(frames)}`).toBeGreaterThanOrEqual(
			0
		);
		expect(
			fullyReopenedFrame,
			`rapid transitions never fully reopened: ${JSON.stringify(frames)}`
		).toBeGreaterThan(allDoneFrame);
		expect(
			frames
				.slice(fullyReopenedFrame)
				.every((frame) => todoIds.every((todoId) => todoIsIn(frame, todoId, 'open'))),
			`a stale result regressed a fully reopened Todo: ${JSON.stringify(frames)}`
		).toBe(true);
		expect(
			frames.every(
				(frame) =>
					isBinarySorted(frame.open) &&
					isBinarySorted(frame.done) &&
					todoIds.every((todoId) => validTodoTransitionFrame(frame, todoId))
			),
			`rapid transitions rendered an invalid generated order: ${JSON.stringify(frames)}`
		).toBe(true);

		await page.unrouteAll({ behavior: 'wait' });
	});
});
