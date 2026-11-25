import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

import {
	generatedPath,
	readGenerated,
	uiRoot
} from './lifecycle-generation.mjs';
const boundaryPlan = (surface) => {
	const source = readGenerated(surface, 'boundaries.ts');
	const match = /DISTRIBUTED_BOUNDARY_PLAN = (\{[\s\S]*?\}) as const;/.exec(
		source
	);
	assert.ok(match, `${surface} must emit an inspectable boundary plan`);
	return JSON.parse(match[1]);
};

test('normal and elevated command inventories cannot be mixed', () => {
	const user = readGenerated('user', 'commands.ts');
	const admin = readGenerated('admin', 'commands.ts');

	assert.match(user, /"name": "todo\.create"/);
	assert.match(user, /"name": "chat\.post"/);
	assert.match(user, /"name": "blob\.start"/);
	assert.match(user, /"name": "blob\.move"/);
	assert.doesNotMatch(user, /"name": "todo\.force_archive"/);
	assert.match(user, /"kind": "application"/);
	assert.match(user, /"name": "e2e-ui"/);
	assert.match(user, /"eligible_roles": \[\s*"admin",\s*"user"\s*\]/);
	assert.match(user, /"schema_roles": \[\s*"user"\s*\]/);

	assert.match(admin, /"name": "todo\.force_archive"/);
	assert.match(admin, /"name": "e2e-ui-admin"/);
	assert.match(admin, /"eligible_roles": \[\s*"admin"\s*\]/);
	assert.match(admin, /"schema_roles": \[\s*"admin"\s*\]/);
});

test('generated clients use one island boundary plan with no route registry', () => {
	for (const surface of ['user', 'public', 'admin']) {
		assert.equal(
			fs.existsSync(
				generatedPath(surface, 'routes.ts')
			),
			false,
			`${surface} must not regenerate routes.ts`
		);
		const generated = readGenerated(surface, 'sveltekit.ts');
		assert.match(generated, /DISTRIBUTED_BOUNDARY_OPERATIONS/);
		assert.doesNotMatch(generated, /DISTRIBUTED_ROUTE_OPERATIONS/);
	}

	const user = boundaryPlan('user');
	const blob = user.boundaries.find(
		(boundary) => boundary.id === 'page:/blob/[[gameId]]'
	);
	const selected = blob.islands.find(
		(island) => island.operation === 'SelectedBlobGame'
	);
	assert.equal(selected.reason, 'static_component_import');
	assert.equal(selected.component, 'src/lib/components/blob/SelectedBlobGame.svelte');
	assert.deepEqual(selected.binding.sources, {
		gameId: { kind: 'route_param', name: 'gameId' }
	});

	const chat = user.boundaries
		.filter((boundary) => boundary.route === '/chat')
		.map((boundary) => ({
			id: boundary.id,
			operation: boundary.islands[0].operation,
			binding: boundary.islands[0].binding.id,
			live: boundary.islands[0].directives.live
		}));
	assert.deepEqual(chat, [
		{
			id: 'layout:/chat',
			operation: 'ChatMessages',
			binding: chat[0].binding,
			live: true
		}
	]);
	assert.deepEqual(
		user.boundaries
			.find((boundary) => boundary.id === 'layout:/chat')
		.islands[0].binding.sources,
		{
			cursor_created_at: { kind: 'omit' },
			cursor_message_id: { kind: 'omit' },
			limit: { kind: 'omit' }
		}
	);

	const publicPlan = boundaryPlan('public');
	assert.equal(
		publicPlan.boundaries[0].islands[0].graphqlSource,
		'src/routes/chat/+layout.public.graphql',
		'authorization surfaces own distinct GraphQL sources'
	);
});

test('application source contains no superseded route or variable switches', () => {
	const roots = [
		path.join(uiRoot, 'src/routes'),
		path.join(uiRoot, 'src/lib/components')
	];
	const forbidden = /DISTRIBUTED_ROUTE_OPERATIONS|matchDistributedRoute|registerDistributedRoute/;
	const pending = [...roots];
	while (pending.length > 0) {
		const current = pending.pop();
		for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
			const target = path.join(current, entry.name);
			if (entry.isDirectory()) pending.push(target);
			else if (/\.(?:svelte|ts|js|mjs)$/.test(entry.name)) {
				assert.doesNotMatch(fs.readFileSync(target, 'utf8'), forbidden);
			}
		}
	}
});
