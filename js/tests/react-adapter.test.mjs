import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
	act,
	createElement
} from 'react';
import { renderToString } from 'react-dom/server';
import { create as createTestRenderer } from 'react-test-renderer';
import { build } from 'esbuild';

import {
	DistributedProvider,
	useDistributedQuery
} from '../dist/react/index.js';
import {
	createDistributedReplica
} from '../dist/replica/index.js';
import {
	assertReplicaAdapterConformance,
	OpenTodosArtifact,
	REACT_FIXTURE_SCHEMA,
	TodoByIdArtifact,
	TodoModel,
	TodosArtifact,
	todoFrame
} from './fixtures/adapter-conformance.mjs';
import { ReactTodoApp } from './fixtures/react-todo-app.mjs';

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

async function flushMicrotasks() {
	for (let iteration = 0; iteration < 8; iteration += 1) {
		await Promise.resolve();
	}
}

async function mountReactQuery({
	replica,
	artifact,
	variables,
	options
}) {
	let current;

	function Probe() {
		current = useDistributedQuery(artifact, variables, options);
		return createElement('output', {
			'data-status': current.status,
			'data-fetching': String(current.fetching)
		});
	}

	let renderer;
	await act(async () => {
		renderer = createTestRenderer(
			createElement(
				DistributedProvider,
				{ replica },
				createElement(Probe)
			)
		);
		await flushMicrotasks();
	});

	return {
		getSnapshot() {
			assert.ok(current, 'React query must render a snapshot');
			return current;
		},
		async settle(action) {
			await act(async () => {
				action?.();
				await flushMicrotasks();
			});
		},
		refetch() {
			return current.refresh();
		},
		async dispose() {
			await act(async () => {
				renderer.unmount();
				await flushMicrotasks();
			});
		}
	};
}

function noOpCommands() {
	return Object.freeze({
		todo: Object.freeze({
			complete: async () => Object.freeze({ state: 'succeeded' })
		})
	});
}

function seedTodoReplica(
	replica,
	{
		id = 'todo-1',
		title,
		cacheScope,
		position = '1'
	}
) {
	const row = { id, title, status: 'open', completed: false };
	replica.writeResult(
		OpenTodosArtifact,
		{},
		todoFrame(OpenTodosArtifact, [row], { cacheScope, position }),
		'ssr'
	);
	replica.writeResult(
		TodoByIdArtifact,
		{ id },
		todoFrame(TodoByIdArtifact, [row], {
			cacheScope,
			position: String(Number(position) + 1)
		}),
		'ssr'
	);
}

function renderTodoApp(replica, selectedId = 'todo-1') {
	return renderToString(
		createElement(
			DistributedProvider,
			{ replica },
			createElement(ReactTodoApp, {
				commands: noOpCommands(),
				selectedId
			})
		)
	);
}

test('React passes the shared replica adapter conformance contract', async () => {
	await assertReplicaAdapterConformance({ mount: mountReactQuery });
});

test('minimal React fixture shares normalized detail/list state and generated command shape', async () => {
	const replica = createDistributedReplica();
	seedTodoReplica(replica, {
		title: 'Shared record',
		cacheScope: 'cache:fixture'
	});
	const commandCalls = [];
	const commands = {
		todo: {
			async complete(input) {
				commandCalls.push(input);
				replica.createOptimisticLayer('fixture-command', (writer) => {
					writer.writeRecord(TodoModel, input.todoId, {
						fields: {
							title: 'Optimistic in both views',
							status: 'done',
							completed: true
						}
					});
				});
				return { state: 'succeeded_pending_projection' };
			}
		}
	};

	let renderer;
	await act(async () => {
		renderer = createTestRenderer(
			createElement(
				DistributedProvider,
				{ replica },
				createElement(ReactTodoApp, { commands })
			)
		);
		await flushMicrotasks();
	});

	const listTitles = () =>
		renderer.root
			.findAll(
				(node) =>
					typeof node.props?.['data-testid'] === 'string' &&
					node.props['data-testid'].startsWith('list-')
			)
			.map((node) => node.children.join(''));
	const detailTitle = () =>
		renderer.root.findByProps({ 'data-testid': 'todo-detail' }).children.join('');
	assert.deepEqual(listTitles(), ['Shared record']);
	assert.equal(detailTitle(), 'Shared record');

	const button = renderer.root.findByType('button');
	await act(async () => {
		await button.props.onClick();
		await flushMicrotasks();
	});
	assert.deepEqual(commandCalls, [{ todoId: 'todo-1' }]);
	assert.deepEqual(
		listTitles(),
		[],
		'the optimistic status change must remove the record from the open filter'
	);
	assert.equal(detailTitle(), 'Optimistic in both views');

	await act(async () => {
		replica.confirmOptimisticLayer('fixture-command', (writer) => {
			writer.writeRecord(TodoModel, 'todo-1', '3', {
				fields: {
					title: 'Projected in both views',
					status: 'done',
					completed: true
				}
			});
			writer.writeIndex(
				{
					field: 'todos',
					arguments: {},
					coverage: { kind: 'complete' },
					dependencies: ['todos'],
					complete: true
				},
				[],
				'3'
			);
		});
		await flushMicrotasks();
	});
	assert.deepEqual(listTitles(), []);
	assert.equal(detailTitle(), 'Projected in both views');

	await act(async () => {
		renderer.unmount();
		await flushMicrotasks();
	});
});

test('React SSR uses the core request snapshot for hydration and rejects scope mismatch', async () => {
	const serverReplica = createDistributedReplica();
	seedTodoReplica(serverReplica, {
		title: 'Server request A',
		cacheScope: 'cache:ssr-a'
	});
	const serverHtml = renderTodoApp(serverReplica);
	const dehydrated = serverReplica.dehydrate();

	const browserReplica = createDistributedReplica();
	assert.equal(
		browserReplica.hydrate(
			JSON.parse(JSON.stringify(dehydrated)),
			dehydrated.scope
		),
		true
	);
	const hydrationHtml = renderTodoApp(browserReplica);
	assert.equal(hydrationHtml, serverHtml);
	assert.match(hydrationHtml, /Server request A/);

	const mismatchedReplica = createDistributedReplica();
	assert.equal(
		mismatchedReplica.hydrate(dehydrated, {
			...dehydrated.scope,
			cacheScope: 'cache:other-user'
		}),
		false
	);
	const mismatchedHtml = renderTodoApp(mismatchedReplica);
	assert.doesNotMatch(mismatchedHtml, /Server request A/);
	assert.match(mismatchedHtml, /Loading/);
});

test('concurrent React server renders keep replicas and authorization scopes isolated', async () => {
	const first = createDistributedReplica();
	const second = createDistributedReplica();
	seedTodoReplica(first, {
		id: 'todo-a',
		title: 'Request A only',
		cacheScope: 'cache:request-a'
	});
	seedTodoReplica(second, {
		id: 'todo-b',
		title: 'Request B only',
		cacheScope: 'cache:request-b'
	});

	const [firstHtml, secondHtml] = await Promise.all([
		Promise.resolve().then(() => renderTodoApp(first, 'todo-a')),
		Promise.resolve().then(() => renderTodoApp(second, 'todo-b'))
	]);
	assert.match(firstHtml, /Request A only/);
	assert.doesNotMatch(firstHtml, /Request B only/);
	assert.match(secondHtml, /Request B only/);
	assert.doesNotMatch(secondHtml, /Request A only/);
	assert.equal(first.dehydrate().scope.cacheScope, 'cache:request-a');
	assert.equal(second.dehydrate().scope.cacheScope, 'cache:request-b');
});

test('React hook requires an explicit provider', () => {
	function MissingProvider() {
		useDistributedQuery(TodosArtifact);
		return null;
	}
	assert.throws(
		() => renderToString(createElement(MissingProvider)),
		/inside a DistributedProvider/
	);
});

test('React and SvelteKit subpaths remain bundle-isolated and React is optional', async () => {
	const packageJson = JSON.parse(
		await readFile(new URL('../package.json', import.meta.url), 'utf8')
	);
	assert.match(packageJson.peerDependencies.react, /18/);
	assert.equal(packageJson.peerDependenciesMeta.react.optional, true);

	const [reactBundle, svelteBundle] = await Promise.all([
		build({
			entryPoints: ['src/react/index.ts'],
			bundle: true,
			format: 'esm',
			platform: 'browser',
			external: ['react'],
			metafile: true,
			write: false,
			logLevel: 'silent'
		}),
		build({
			entryPoints: ['src/sveltekit/index.ts'],
			bundle: true,
			format: 'esm',
			platform: 'browser',
			metafile: true,
			write: false,
			logLevel: 'silent'
		})
	]);
	const reactInputs = Object.keys(reactBundle.metafile.inputs);
	const svelteInputs = Object.keys(svelteBundle.metafile.inputs);
	assert.equal(
		reactInputs.some((path) => path.includes('/sveltekit/')),
		false,
		'React bundle must not pull in the SvelteKit adapter'
	);
	assert.equal(
		svelteInputs.some((path) => path.includes('/react/')),
		false,
		'SvelteKit bundle must not pull in the React adapter'
	);
	assert.equal(
		svelteInputs.some((path) => path.includes('node_modules/react/')),
		false,
		'SvelteKit bundle must not require the optional React peer'
	);
	assert.equal(
		svelteInputs.some(
			(path) =>
				path.includes('svelte/compiler') ||
				path.includes('/sveltekit/vite.ts') ||
				path.includes('/sveltekit/islands/boundaries.ts') ||
				path.startsWith('node:')
		),
		false,
		'Node-only component analysis must stay out of the browser SvelteKit subpath'
	);
	assert.ok(
		reactBundle.outputFiles[0].contents.byteLength < 45_000,
		'React adapter browser bundle exceeded its framework-isolation budget'
	);
	assert.ok(
		svelteBundle.outputFiles[0].contents.byteLength < 720_000,
		'SvelteKit adapter browser bundle pulled in an unexpected Node/compiler dependency'
	);
});

test('React fixture protocol remains on the exact generated surface', () => {
	assert.equal(TodosArtifact.protocol.schemaHash, REACT_FIXTURE_SCHEMA);
	assert.deepEqual(TodosArtifact.protocol.surface, {
		kind: 'role',
		name: 'user'
	});
});
