/**
 * Browser proof that a command paints optimistically.
 *
 * Hold the GraphQL mutation before it reaches the server. If the UI updates
 * before dispatch is released, the paint came from the client optimistic
 * layer — not a response or subscription frame.
 */
import type { Page, Request, Response } from '@playwright/test';
import { expect } from '@playwright/test';

export type MutationNeedle =
	| 'chat_messages_post'
	| 'todos_create'
	| 'todos_complete'
	| 'todos_reopen'
	| 'todos_archive'
	| 'todos_rename'
	| 'blob_games_start'
	| 'blob_games_move'
	| 'blob_games_start_level';

export type HoldMutationOptions = {
	/** Substring matched against the GraphQL POST body. */
	readonly needle: MutationNeedle | string;
	/** How long to hold the fulfilled mutation response (ms). */
	readonly holdMs?: number;
	/**
	 * Max wait for the optimistic paint. Must be strictly less than holdMs so
	 * the assert cannot pass from the delayed wire response.
	 */
	readonly assertWithinMs?: number;
};

const DEFAULT_HOLD_MS = 1_500;
const DEFAULT_ASSERT_MS = 1_000;

function mutationMatches(postData: string | null, needle: string): boolean {
	return (postData ?? '').includes(needle);
}

export type HeldGraphqlMutation = {
	/** The response to the first request this route held, once it is fulfilled. */
	readonly response: Promise<Response>;
	/** Remove the route. Await `response` first: see holdGraphqlMutation. */
	readonly dispose: () => Promise<void>;
};

/**
 * Install a GraphQL route that continues non-matching requests and delays
 * matching mutation dispatch by holdMs.
 *
 * `response` is the held request's own response, not any matching one: a
 * mutation the page sent before the route existed can answer after it is
 * installed. Dispose only after `response`: Playwright releases a held request
 * to the network when the route is removed while another request is still in
 * the handler (its fallback re-syncs interception), and the held handler's
 * `route.fulfill` then throws "Route is already handled!" despite
 * `unrouteAll({ behavior: 'wait' })`.
 */
export async function holdGraphqlMutation(
	page: Page,
	needle: string,
	holdMs: number = DEFAULT_HOLD_MS
): Promise<HeldGraphqlMutation> {
	let held: Request | undefined;
	const response = page.waitForResponse((candidate) => candidate.request() === held, {
		timeout: holdMs + 20_000
	});
	await page.route('**/graphql', async (route) => {
		if (!mutationMatches(route.request().postData(), needle)) {
			await route.continue();
			return;
		}
		held ??= route.request();
		// Hold before route.fetch so neither the command response nor a server
		// projection/subscription can produce the state under assertion.
		await new Promise((resolve) => setTimeout(resolve, holdMs));
		const fetched = await route.fetch();
		await route.fulfill({ response: fetched });
	});
	return {
		response,
		dispose: async () => {
			await page.unrouteAll({ behavior: 'wait' });
		}
	};
}

export type OptimisticPaintOptions = HoldMutationOptions & {
	/** Trigger the command (click/send/etc). */
	readonly act: () => Promise<void>;
	/** Assert the optimistic UI state (use short timeout internally or via assertWithinMs). */
	readonly assertOptimistic: () => Promise<void>;
	/** Optional assert after the held response arrives. */
	readonly assertConverged?: () => Promise<void>;
};

/**
 * Run act under a held mutation and require assertOptimistic before the
 * command reaches the server.
 */
export async function expectOptimisticPaint(
	page: Page,
	options: OptimisticPaintOptions
): Promise<Response> {
	const holdMs = options.holdMs ?? DEFAULT_HOLD_MS;
	const assertWithinMs = options.assertWithinMs ?? DEFAULT_ASSERT_MS;
	if (assertWithinMs >= holdMs) {
		throw new Error(
			`assertWithinMs (${assertWithinMs}) must be < holdMs (${holdMs}) to prove paint-before-wire`
		);
	}

	const { response: pending, dispose } = await holdGraphqlMutation(
		page,
		options.needle,
		holdMs
	);

	try {
		await options.act();
		// Bound the optimistic assert so a later server result cannot satisfy it.
		await expect
			.poll(async () => {
				try {
					await options.assertOptimistic();
					return true;
				} catch {
					return false;
				}
			}, { timeout: assertWithinMs, intervals: [50, 100, 150, 200] })
			.toBe(true);

		const response = await pending;
		if (options.assertConverged !== undefined) {
			await options.assertConverged();
		}
		return response;
	} finally {
		await dispose();
	}
}

/** Convenience: expect a locator visible within the optimism window. */
export async function expectVisibleSoon(
	locator: { waitFor: (opts: { state: 'visible'; timeout: number }) => Promise<unknown> },
	timeoutMs: number
): Promise<void> {
	await locator.waitFor({ state: 'visible', timeout: timeoutMs });
}
