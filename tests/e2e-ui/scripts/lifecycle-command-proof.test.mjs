import assert from 'node:assert/strict';
import { test } from 'node:test';
import { waitForProjectedTodo } from './lifecycle-command-proof.mjs';

const todo = { todo_id: 'proof-1', owner_id: 'alice', title: 'Persisted', status: 'open' };

test('an empty SSR-era read is retried until the exact authoritative row exists', async () => {
	let calls = 0;
	const attempts = await waitForProjectedTodo(async () => ({
		data: { todos: ++calls < 3 ? [] : [todo] }
	}), todo);
	assert.equal(attempts, 3);
});

test('matching title on another aggregate does not prove this command', async () => {
	await assert.rejects(waitForProjectedTodo(async () => ({
		data: { todos: [{ ...todo, todo_id: 'other' }] }
	}), todo, 20), /accepted but not projected/);
});

test('a stalled projector fails instead of falling back to the receipt or optimism', async () => {
	await assert.rejects(waitForProjectedTodo(async () => ({ data: { todos: [] } }), todo, 20),
		/accepted but not projected/);
});

test('a wrong persisted value fails rather than merely checking row presence', async () => {
	await assert.rejects(waitForProjectedTodo(async () => ({
		data: { todos: [{ ...todo, title: 'Wrong' }] }
	}), todo), /must match the committed command payload/);
});

test('GraphQL failures are not retried as eventual projection lag', async () => {
	await assert.rejects(waitForProjectedTodo(async () => ({ errors: [{ message: 'Denied' }] }), todo),
		/must not return GraphQL errors/);
});
