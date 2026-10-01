import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';

// Eventual command receipts prove the aggregate commit, not projector completion.
// Query outside the browser replica before asking a fresh SSR page to prove it.
export async function waitForProjectedTodo(query, expected, timeoutMs = 120_000) {
	const deadline = Date.now() + timeoutMs;
	let attempts = 0;
	while (Date.now() < deadline) {
		const body = await query(Math.max(1, deadline - Date.now()));
		attempts += 1;
		assert.ok(!body.errors?.length, 'authoritative Todo query must not return GraphQL errors');
		assert.ok(Array.isArray(body.data?.todos), 'authoritative Todo query must return rows');
		const row = body.data.todos.find((todo) => todo.todo_id === expected.todo_id);
		if (row) {
			assert.deepEqual(row, expected, 'projected Todo must match the committed command payload');
			return attempts;
		}
		await delay(Math.min(50, Math.max(0, deadline - Date.now())));
	}
	throw new Error(`Todo ${expected.todo_id} was accepted but not projected after ${attempts} queries`);
}
