import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import {
	mkdtemp,
	mkdir,
	readFile,
	rm,
	stat,
	writeFile
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { build } from 'esbuild';

const execFileAsync = promisify(execFile);
const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const nodeCommand = process.execPath;
const removedSubpaths = Object.freeze([
	'@hops-ops/distributed/cache',
	'@hops-ops/distributed/commands',
	'@hops-ops/distributed/codegen',
	'@hops-ops/distributed/internal/cache-engine'
]);

async function run(command, args, cwd) {
	try {
		return await execFileAsync(command, args, {
			cwd,
			env: process.env,
			maxBuffer: 10 * 1024 * 1024
		});
	} catch (error) {
		const stdout =
			typeof error.stdout === 'string' ? error.stdout.trim() : '';
		const stderr =
			typeof error.stderr === 'string' ? error.stderr.trim() : '';
		const output = [stdout, stderr].filter(Boolean).join('\n');
		throw new Error(
			`Command failed: ${command} ${args.join(' ')}${
				output.length === 0 ? '' : `\n${output}`
			}`,
			{ cause: error }
		);
	}
}

function collectPackageTargets(value, targets) {
	if (typeof value === 'string') {
		if (value.startsWith('./')) targets.add(value.slice(2));
		else if (!value.startsWith('.') && !isAbsolute(value)) targets.add(value);
		return;
	}
	if (Array.isArray(value)) {
		for (const entry of value) collectPackageTargets(entry, targets);
		return;
	}
	if (value !== null && typeof value === 'object') {
		for (const entry of Object.values(value)) {
			collectPackageTargets(entry, targets);
		}
	}
}

function parsePackResult(stdout) {
	try {
		return JSON.parse(stdout.trim());
	} catch (error) {
		throw new Error(`npm pack did not return valid JSON:\n${stdout.trim()}`, {
			cause: error
		});
	}
}

function inspectPackageContract(packageJson) {
	assert.deepEqual(Object.keys(packageJson.exports).sort(), [
		'.',
		'./diagnostics',
		'./package.json',
		'./react',
		'./replica',
		'./replica/lazy',
		'./sveltekit',
		'./sveltekit/vite'
	]);
	assert.equal(
		packageJson.bin,
		undefined,
		'the npm package must not expose a package-owned generator'
	);
	for (const subpath of ['./cache', './commands', './codegen']) {
		assert.equal(
			Object.hasOwn(packageJson.exports, subpath),
			false,
			`${subpath} must not remain public`
		);
	}
	assert.equal(
		packageJson.peerDependenciesMeta?.react?.optional,
		true,
		'React must remain an optional peer'
	);
	assert.equal(
		packageJson.peerDependenciesMeta?.svelte?.optional,
		true,
		'Svelte must remain an optional peer'
	);
}

function inspectPackResult(packResult, packageJson) {
	assert.equal(packResult.length, 1, 'npm pack must produce exactly one tarball');
	const [packed] = packResult;
	assert.equal(packed.name, packageJson.name);
	assert.equal(packed.version, packageJson.version);
	assert.ok(Array.isArray(packed.files), 'npm pack must report its file manifest');

	const packedPaths = new Set(packed.files.map((file) => file.path));
	const requiredPaths = new Set(['README.md', 'package.json']);
	collectPackageTargets(packageJson.exports, requiredPaths);
	for (const requiredPath of requiredPaths) {
		assert.ok(packedPaths.has(requiredPath), `tarball is missing ${requiredPath}`);
	}
	assert.ok(
		[...packedPaths].some((path) => path.startsWith('dist/')),
		'tarball must contain built declarations and JavaScript'
	);

	const forbiddenSegments = new Set([
		'src',
		'tests',
		'type-tests',
		'scripts',
		'node_modules'
	]);
	const forbiddenPaths = [...packedPaths].filter((path) =>
		path.split('/').some((segment) => forbiddenSegments.has(segment))
	);
	assert.deepEqual(
		forbiddenPaths,
		[],
		`tarball contains private paths: ${forbiddenPaths.join(', ')}`
	);
	return { packed, packedPaths };
}

const consumerTypeSource = `
import {
  DISTRIBUTED_PROTOCOL_VERSION,
  documentToString,
  parseDistributedProtocolEnvelope,
  requestGraphql,
  type GqlAuth,
  type GqlDocument
} from '@hops-ops/distributed';
import {
  createDistributedReplica,
  createReplicaGraphqlTransport,
  createReplicaIndexedDbPersistence,
  type ReplicaOperationArtifact,
  type ReplicaSnapshot,
  type ReplicaSparse
} from '@hops-ops/distributed/replica';
import {
  createReplicaDiagnostics,
  type ReplicaDiagnosticsSnapshot
} from '@hops-ops/distributed/diagnostics';
// @ts-expect-error The removed document-cache type must stay absent.
import type { QueryCache } from '@hops-ops/distributed';
// @ts-expect-error The removed handwritten cache target must stay absent.
import type { CacheTarget } from '@hops-ops/distributed';
// @ts-expect-error The removed manual list reconciliation policy must stay absent.
import type { ListMergeSpec } from '@hops-ops/distributed';
// @ts-expect-error The removed pilot client factory must stay absent.
import { createGraphqlClient } from '@hops-ops/distributed';
// @ts-expect-error The removed resource wrapper must stay absent.
import { defineResource } from '@hops-ops/distributed';
// @ts-expect-error The removed pilot client type must stay absent.
import type { GraphqlClient } from '@hops-ops/distributed';
// @ts-expect-error The removed resource abstraction must stay absent.
import type { GraphqlResource } from '@hops-ops/distributed';
// @ts-expect-error The removed document-store abstraction must stay absent.
import type { DocumentStore } from '@hops-ops/distributed';
// @ts-expect-error The removed raw command definition must stay absent.
import type { CommandDefinition } from '@hops-ops/distributed';
// @ts-expect-error The removed manual command policy must stay absent.
import type { CommandPolicy } from '@hops-ops/distributed';

type TodosData = { todos: readonly { id: string; title: string }[] };
type TodosVariables = Readonly<{ limit: number }>;

const operation: ReplicaOperationArtifact<TodosData, TodosVariables> = {
  id: 'query:todos',
  document: 'query Todos($limit: Int!) { todos(limit: $limit) { id title } }',
  protocol: {
    version: 1,
    schemaHash: \`sha256:\${'a'.repeat(64)}\`,
    surface: { kind: 'role', name: 'user' },
    operation: 'query:todos',
    trustedPresets: []
  },
  variableCodec: {
    version: 2,
    limits: { maxDepth: 8, maxBoolWidth: 32, maxInList: 64 },
    variables: {
      limit: {
        kind: 'scalar',
        scalar: 'Int',
        codec: 'int32',
        nullable: false
      }
    },
    defaults: {},
    inputs: {}
  },
  roots: [{
    responseKey: 'todos',
    field: 'todos',
    cardinality: 'many',
    nullable: false,
    arguments: { limit: { kind: 'variable', name: 'limit' } },
    dependencies: ['todos'],
    selection: {
      typename: 'todo',
      storage: {
        kind: 'normalized',
        model: 'Todo',
        identityFields: ['id']
      },
      members: [
        {
          kind: 'scalar',
          responseKey: 'id',
          field: 'id',
          codec: 'ID',
          nullable: false
        },
        {
          kind: 'scalar',
          responseKey: 'title',
          field: 'title',
          codec: 'String',
          nullable: false
        }
      ]
    }
  }]
};

const auth: GqlAuth = { accessToken: 'token' };
const document: GqlDocument<TodosData, TodosVariables> = operation.document;
const diagnostics = createReplicaDiagnostics();
const diagnosticSnapshot: ReplicaDiagnosticsSnapshot = diagnostics.getSnapshot();
const transport = createReplicaGraphqlTransport({
  getUrl: () => '/graphql',
  getAuth: () => auth
});
const replica = createDistributedReplica({ transport, diagnostics });
const snapshot: ReplicaSnapshot<TodosData> = replica.read(operation, { limit: 10 });
const sparse: ReplicaSparse<TodosData> = {};
if (snapshot.complete) snapshot.data.todos.map((todo) => todo.title);

void [
  DISTRIBUTED_PROTOCOL_VERSION,
  documentToString(document),
  parseDistributedProtocolEnvelope,
  requestGraphql,
  createReplicaIndexedDbPersistence,
  diagnosticSnapshot,
  sparse
];
`;

const consumerRuntimeSource = `
import assert from 'node:assert/strict';
import * as rootSurface from '@hops-ops/distributed';
import * as lazySurface from '@hops-ops/distributed/replica/lazy';
assert.deepEqual(Object.keys(lazySurface), ['createLazyReplicaCommandRuntime']);
import * as replicaSurface from '@hops-ops/distributed/replica';
import * as diagnosticsSurface from '@hops-ops/distributed/diagnostics';
import {
  requestGraphql
} from '@hops-ops/distributed';
import {
  createDistributedReplica
} from '@hops-ops/distributed/replica';
import {
  createReplicaDiagnostics
} from '@hops-ops/distributed/diagnostics';

const schemaHash = \`sha256:\${'a'.repeat(64)}\`;
const operation = Object.freeze({
  id: 'query:todos',
  document: 'query Todos { todos { id title } }',
  protocol: Object.freeze({
    version: 1,
    schemaHash,
    surface: Object.freeze({ kind: 'role', name: 'user' }),
    operation: 'query:todos',
    trustedPresets: Object.freeze([])
  }),
  variableCodec: Object.freeze({
    version: 2,
    limits: Object.freeze({
      maxDepth: 8,
      maxBoolWidth: 32,
      maxInList: 64
    }),
    variables: Object.freeze({}),
    defaults: Object.freeze({}),
    inputs: Object.freeze({})
  }),
  roots: Object.freeze([Object.freeze({
    responseKey: 'todos',
    field: 'todos',
    cardinality: 'many',
    nullable: false,
    dependencies: Object.freeze(['todos']),
    selection: Object.freeze({
      typename: 'todo',
      storage: Object.freeze({
        kind: 'normalized',
        model: 'Todo',
        identityFields: Object.freeze(['id'])
      }),
      members: Object.freeze([
        Object.freeze({
          kind: 'scalar',
          responseKey: 'id',
          field: 'id',
          codec: 'ID',
          nullable: false
        }),
        Object.freeze({
          kind: 'scalar',
          responseKey: 'title',
          field: 'title',
          codec: 'String',
          nullable: false
        })
      ])
    })
  })])
});

const diagnostics = createReplicaDiagnostics();
const replica = createDistributedReplica({ diagnostics });
replica.writeResult(operation, {}, {
  data: { todos: [{ id: 'todo-1', title: 'packed' }] },
  extensions: {
    distributed: {
      protocolVersion: 1,
      schemaHash,
      authorizationGeneration: 'auth-1',
      cacheScope: 'scope:user',
      operation: operation.id,
      trustedPresets: [],
      snapshot: {
        scopeToken: 'snapshot:todos',
        recordsComplete: true,
        indexesComparable: true,
        records: [{
          path: ['todos', '0'],
          model: 'Todo',
          scopeToken: 'record:todo-1',
          incarnation: '1',
          revision: '1',
          tombstone: false
        }],
        indexes: [{
          projection: 'todos',
          scopeToken: 'index:todos',
          position: '1'
        }],
        observations: []
      }
    }
  }
}, 'network');
assert.deepEqual(replica.read(operation, {}).data, {
  todos: [{ id: 'todo-1', title: 'packed' }]
});
assert.equal(diagnostics.getSnapshot().records.length, 1);

const result = await requestGraphql(
  'https://example.test/graphql',
  'query Health { health }',
  {},
  {},
  {
    fetch: async (_input, init) => {
      const body = JSON.parse(String(init.body));
      assert.equal(body.query, 'query Health { health }');
      return new Response(JSON.stringify({ data: { health: 'ok' } }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      });
    }
  }
);
assert.equal(result.data.health, 'ok');

assert.deepEqual(Object.keys(rootSurface).sort(), [
  'DISTRIBUTED_PROTOCOL_VERSION',
  'DistributedProtocolError',
  'applyWsDevHeaderParams',
  'authIdentityKey',
  'buildAuthHeaders',
  'compareDistributedDecimal',
  'distributedLiveResumeExtensions',
  'documentToString',
  'graphqlWsUrl',
  'httpUrlToWsUrl',
  'jwtPayloadSub',
  'parseDistributedGenerationEnvelope',
  'parseDistributedProtocolEnvelope',
  'parseGraphqlResponseExtensions',
  'requestGraphql',
  'subscribe',
  'wsConnectionInitPayload'
]);
assert.deepEqual(Object.keys(replicaSurface).sort(), [
  'MUTATION_CACHE_VISIBILITY_FULL',
  'MUTATION_CACHE_VISIBILITY_UNAUTHORIZED',
  'REPLICA_OFFLINE_COMMAND_OUTBOX_SUPPORTED',
  'ReplicaCommandContractError',
  'ReplicaCommandRuntimeError',
  'canonicalizeOperationVariables',
  'compareReplicaOrder',
  'createDistributedReplica',
  'createReplicaCommandRuntime',
  'createReplicaDevelopmentCapability',
  'createReplicaDiagnostics',
  'createReplicaGraphqlTransport',
  'createReplicaIndexMaintenanceRegistry',
  'createReplicaIndexedDbPersistence',
  'createReplicaUuidV7',
  'createWasmJsonPure',
  'decideReplicaPaginationMaintenance',
  'evaluateReplicaFilter',
  'formatReplicaIndexStaleReason',
  'inspectReplicaCommandArtifact',
  'inspectReplicaOperationArtifact',
  'lowerMutationCache',
  'prepareReplicaCommand',
  'replicaIndexKey',
  'replicaRecordKey',
  'verifyReplicaCommandReceipt'
]);
assert.deepEqual(Object.keys(diagnosticsSurface).sort(), [
  'createReplicaDevelopmentCapability',
  'createReplicaDiagnostics',
  'inspectReplicaCommandArtifact',
  'inspectReplicaOperationArtifact'
]);
for (const subpath of ${JSON.stringify(removedSubpaths)}) {
  await assert.rejects(
    import(subpath),
    (error) => error?.code === 'ERR_PACKAGE_PATH_NOT_EXPORTED',
    \`\${subpath} must remain private\`
  );
}
`;

const svelteTypeSource = `
import {
  createDistributedSvelteKit,
  createDistributedSvelteKitServer,
  createPageDataSessionSource,
  defineDistributedSvelteKitOperation,
  provideDistributedSvelteKitClient,
  useDistributedSvelteKitClient,
  useDistributedSvelteKitCommands
} from '@hops-ops/distributed/sveltekit';
import {
  distributedGraphqlProxy,
  distributedSvelteKit,
  distributedSvelteKitAliases,
  validateDistributedSvelteKitBoundaryPlan
} from '@hops-ops/distributed/sveltekit/vite';
import type {
  ReplicaOperationArtifact
} from '@hops-ops/distributed/replica';
// @ts-expect-error Manual client generation is lifecycle-internal.
import { generateDistributedSvelteKit } from '@hops-ops/distributed/sveltekit/vite';
// @ts-expect-error Manual generated-client drift checks are lifecycle-internal.
import { checkDistributedSvelteKit } from '@hops-ops/distributed/sveltekit/vite';
// @ts-expect-error The browser lifecycle side channel is lifecycle-internal.
import { distributedLifecycle } from '@hops-ops/distributed/sveltekit/vite';
// @ts-expect-error The removed Svelte pilot query helper must stay absent.
import { createUseGraphql } from '@hops-ops/distributed/sveltekit';
// @ts-expect-error The removed Svelte pilot load helper must stay absent.
import { createLoadQuery } from '@hops-ops/distributed/sveltekit';
// @ts-expect-error Node-only proxy helpers must not leak from the browser entry.
import { distributedGraphqlProxy as leakedProxy } from '@hops-ops/distributed/sveltekit';

type TodosData = { todos: readonly { id: string; title: string }[] };
type TodosVariables = Readonly<{ limit: number }>;
type Commands = {
  readonly todo: {
    readonly create: (input: { readonly title: string }) => Promise<unknown>
  }
};
declare const operation: ReplicaOperationArtifact<TodosData, TodosVariables>;

const pageData = createPageDataSessionSource({
  session: null,
  accessToken: null,
  engineRole: null
});
const client = createDistributedSvelteKit<Commands>({
  boundaries: [],
  browser: false,
  session: pageData.session
});
const Todos = defineDistributedSvelteKitOperation(operation);
Todos.read({ limit: 10 });
provideDistributedSvelteKitClient(client);
useDistributedSvelteKitClient<Commands>().operation(operation);
useDistributedSvelteKitCommands<Commands>().todo.create({ title: 'typed' });
client.retainBoundary(
  { id: 'page-instance', route: '/todos', kind: 'page' },
  { params: {}, search: new URLSearchParams(), session: null, props: {} }
).release();
client.retainLocation(
  { id: 'active-page', pathname: '/todos', kind: 'page' },
  { search: new URLSearchParams(), session: null, props: {} }
).release();
void client.prefetchLocation('/todos', {
  search: new URLSearchParams(), session: null, props: {}
});

createDistributedSvelteKitServer({
  boundaries: [],
  getSession: async () => null,
  getRole: () => 'user'
});

const compiler = {
  clients: [{
    module: '$distributed',
    manifest: 'target/distributed-client.json',
    role: 'user',
    documents: ['src/**/*.graphql'],
    out: 'src/lib/generated/distributed'
  }]
} as const;
const plugin = distributedSvelteKit(compiler);
const aliases = distributedSvelteKitAliases(compiler);
const proxy = distributedGraphqlProxy('http://127.0.0.1:8791');

void [
  plugin,
  aliases,
  proxy,
  validateDistributedSvelteKitBoundaryPlan,
  generateDistributedSvelteKit,
  checkDistributedSvelteKit,
  distributedLifecycle,
  leakedProxy
];
client.destroy();
`;

const svelteRuntimeSource = `
import assert from 'node:assert/strict';
import * as sveltekitSurface from '@hops-ops/distributed/sveltekit';
import * as sveltekitViteSurface from '@hops-ops/distributed/sveltekit/vite';
import {
  createDistributedSvelteKit,
  createPageDataSessionSource
} from '@hops-ops/distributed/sveltekit';
import {
  distributedGraphqlProxy
} from '@hops-ops/distributed/sveltekit/vite';

assert.deepEqual(Object.keys(sveltekitSurface).sort(), [
  'DistributedSvelteKitBoundaryController',
  'authFromPageData',
  'bindSveltekitOperation',
  'constant',
  'createDistributedSvelteKit',
  'createDistributedSvelteKitServer',
  'createPageDataSessionSource',
  'defineDistributedBoundaryBinding',
  'defineDistributedBoundaryOperation',
  'defineDistributedSvelteKitOperation',
  'defineGraphqlIslandBindings',
  'distributedReloadLifecycle',
  'forwardedProp',
  'omitVariable',
  'parseDistributedGenerationEnvelope',
  'provideDistributedSvelteKitClient',
  'registerDistributedReloadClient',
  'resolveDistributedBoundaryVariables',
  'retainDistributedSvelteKitBoundary',
  'routeParam',
  'searchParam',
  'sessionClaim',
  'sessionSourceFromPageData',
  'useDistributedSvelteKitClient',
  'useDistributedSvelteKitCommands',
  'validateDistributedReloadLocation',
  'validateDistributedReloadState'
]);
assert.deepEqual(Object.keys(sveltekitViteSurface).sort(), [
  'analyzeDistributedSvelteKitBoundaries',
  'distributedGraphqlProxy',
  'distributedSvelteKit',
  'distributedSvelteKitAliases',
  'validateDistributedSvelteKitBoundaryPlan'
]);
assert.equal(typeof sveltekitViteSurface.distributedSvelteKit, 'function');

const pageData = createPageDataSessionSource({
  session: null,
  accessToken: null,
  engineRole: null
});
assert.deepEqual(pageData.get(), {
  session: null,
  accessToken: null,
  engineRole: null
});
pageData.set({
  session: { user: { sub: 'user-1' } },
  accessToken: 'token',
  engineRole: 'user'
});
assert.equal(pageData.session.getAuth().accessToken, 'token');

const client = createDistributedSvelteKit({
  boundaries: [],
  browser: false,
  session: pageData.session
});
assert.equal(typeof client.operation, 'function');
client.destroy();

assert.deepEqual(distributedGraphqlProxy('http://127.0.0.1:8791'), {
  '/graphql': {
    target: 'http://127.0.0.1:8791',
    changeOrigin: true,
    ws: true
  }
});
`;

const reactTypeSource = `
import { createElement } from 'react';
import {
  DistributedProvider,
  useDistributedQuery
} from '@hops-ops/distributed/react';
import type {
  DistributedReplica,
  ReplicaOperationArtifact
} from '@hops-ops/distributed/replica';

type TodosData = { todos: readonly { id: string; title: string }[] };
declare const replica: DistributedReplica;
declare const Todos: ReplicaOperationArtifact<
  TodosData,
  Readonly<Record<never, never>>
>;

function App() {
  const todos = useDistributedQuery(Todos);
  if (todos.complete) todos.data.todos.map((todo) => todo.title);
  void todos.refresh;
  return null;
}

createElement(DistributedProvider, { replica }, createElement(App));
`;

const reactRuntimeSource = `
import assert from 'node:assert/strict';
import * as reactSurface from '@hops-ops/distributed/react';
import {
  DistributedProvider,
  useDistributedQuery,
  useDistributedReplica
} from '@hops-ops/distributed/react';

assert.equal(typeof DistributedProvider, 'function');
assert.equal(typeof useDistributedQuery, 'function');
assert.equal(typeof useDistributedReplica, 'function');
assert.deepEqual(Object.keys(reactSurface).sort(), [
  'DistributedProvider',
  'useDistributedQuery',
  'useDistributedReplica'
]);
`;

const tsconfig = (types) =>
	`${JSON.stringify(
		{
			compilerOptions: {
				target: 'ES2022',
				module: 'NodeNext',
				moduleResolution: 'NodeNext',
				lib: ['ES2022', 'DOM'],
				types,
				strict: true,
				noEmit: true,
				verbatimModuleSyntax: true
			},
			include: ['consumer.ts']
		},
		null,
		2
	)}\n`;

async function installConsumer(
	directory,
	name,
	tarballPath,
	typescriptVersion,
	extraPackages = []
) {
	await writeFile(
		join(directory, 'package.json'),
		`${JSON.stringify(
			{
				name,
				private: true,
				type: 'module'
			},
			null,
			2
		)}\n`
	);
	await run(
		npmCommand,
		[
			'install',
			'--ignore-scripts',
			'--no-audit',
			'--no-fund',
			'--save-exact',
			tarballPath,
			`typescript@${typescriptVersion}`,
			...extraPackages
		],
		directory
	);
}

async function typecheck(directory, types) {
	await writeFile(join(directory, 'tsconfig.json'), tsconfig(types));
	const tscPath = join(
		directory,
		'node_modules',
		'typescript',
		'bin',
		'tsc'
	);
	await run(nodeCommand, [tscPath, '--project', 'tsconfig.json'], directory);
}

async function bundleInputs(directory, contents) {
	const result = await build({
		absWorkingDir: directory,
		stdin: {
			contents,
			resolveDir: directory,
			sourcefile: 'bundle-smoke.js'
		},
		bundle: true,
		format: 'esm',
		platform: 'browser',
		target: 'es2022',
		treeShaking: true,
		write: false,
		metafile: true,
		logLevel: 'silent'
	});
	return Object.keys(result.metafile.inputs).map((path) =>
		path.replaceAll('\\', '/')
	);
}

async function smokePack() {
	const packageJson = JSON.parse(
		await readFile(join(packageRoot, 'package.json'), 'utf8')
	);
	inspectPackageContract(packageJson);
	const typescriptVersion = packageJson.devDependencies?.typescript;
	assert.ok(typescriptVersion, 'TypeScript must be a declared dev dependency');

	const temporaryRoot = await mkdtemp(join(tmpdir(), 'distributed-pack-smoke-'));
	try {
		const packDirectory = join(temporaryRoot, 'pack');
		const consumerDirectory = join(temporaryRoot, 'consumer');
		const svelteConsumerDirectory = join(temporaryRoot, 'svelte-consumer');
		const reactConsumerDirectory = join(temporaryRoot, 'react-consumer');
		await Promise.all([
			mkdir(packDirectory),
			mkdir(consumerDirectory),
			mkdir(svelteConsumerDirectory),
			mkdir(reactConsumerDirectory)
		]);

		const { stdout } = await run(
			npmCommand,
			['pack', '--json', '--silent', '--pack-destination', packDirectory],
			packageRoot
		);
		const { packed, packedPaths } = inspectPackResult(
			parsePackResult(stdout),
			packageJson
		);
		const tarballPath = isAbsolute(packed.filename)
			? packed.filename
			: resolve(packDirectory, packed.filename);
		assert.ok(
			tarballPath.startsWith(`${resolve(packDirectory)}${sep}`),
			'npm pack returned a path outside the temporary pack directory'
		);
		assert.ok((await stat(tarballPath)).size > 0, 'tarball must not be empty');

		await installConsumer(
			consumerDirectory,
			'distributed-pack-smoke-consumer',
			tarballPath,
			typescriptVersion
		);
		await writeFile(
			join(consumerDirectory, 'consumer.ts'),
			consumerTypeSource
		);
		await writeFile(
			join(consumerDirectory, 'runtime.mjs'),
			consumerRuntimeSource
		);
		await typecheck(consumerDirectory, []);
		await run(nodeCommand, ['runtime.mjs'], consumerDirectory);
		for (const peer of ['react', 'svelte']) {
			await assert.rejects(
				stat(join(consumerDirectory, 'node_modules', peer)),
				(error) => error?.code === 'ENOENT',
				`${peer} must remain optional for root/replica consumers`
			);
		}

		const baseInputs = await bundleInputs(
			consumerDirectory,
			`
				import '@hops-ops/distributed';
				import '@hops-ops/distributed/replica';
				import '@hops-ops/distributed/diagnostics';
			`
		);
		assert.equal(
			baseInputs.some((path) => /node_modules\/react(?:\/|$)/.test(path)),
			false,
			'framework-neutral entry points must not pull React into browser bundles'
		);
		assert.equal(
			baseInputs.some((path) => /node_modules\/svelte(?:\/|$)/.test(path)),
			false,
			'framework-neutral entry points must not pull Svelte into browser bundles'
		);
		assert.equal(
			baseInputs.some((path) => path.includes('/dist/sveltekit/')),
			false,
			'framework-neutral entry points must not pull the SvelteKit adapter'
		);

		await installConsumer(
			svelteConsumerDirectory,
			'distributed-svelte-pack-smoke-consumer',
			tarballPath,
			typescriptVersion,
			[`svelte@${packageJson.devDependencies.svelte}`]
		);
		await writeFile(
			join(svelteConsumerDirectory, 'consumer.ts'),
			svelteTypeSource
		);
		await writeFile(
			join(svelteConsumerDirectory, 'runtime.mjs'),
			svelteRuntimeSource
		);
		await typecheck(svelteConsumerDirectory, []);
		await run(nodeCommand, ['runtime.mjs'], svelteConsumerDirectory);
		await stat(
			join(
				svelteConsumerDirectory,
				'node_modules',
				'@hops-ops',
				'distributed',
				'dist',
				'sveltekit',
				'lifecycle-compiler.js'
			)
		);
		await assert.rejects(
			stat(join(svelteConsumerDirectory, 'node_modules', 'react')),
			(error) => error?.code === 'ENOENT',
			'the SvelteKit entry point must not install React'
		);

		const svelteInputs = await bundleInputs(
			svelteConsumerDirectory,
			`import '@hops-ops/distributed/sveltekit';`
		);
		assert.equal(
			svelteInputs.some((path) => /node_modules\/svelte(?:\/|$)/.test(path)),
			true,
			'the SvelteKit entry point must bind the installed Svelte peer'
		);
		assert.equal(
			svelteInputs.some((path) => /node_modules\/react(?:\/|$)/.test(path)),
			false,
			'the SvelteKit entry point must not pull React'
		);
		assert.equal(
			svelteInputs.some((path) => path.endsWith('/dist/sveltekit/vite.js')),
			false,
			'the browser SvelteKit entry point must not pull the Node-only Vite integration'
		);

		await installConsumer(
			reactConsumerDirectory,
			'distributed-react-pack-smoke-consumer',
			tarballPath,
			typescriptVersion,
			[
				`react@${packageJson.devDependencies.react}`,
				`@types/react@${packageJson.devDependencies['@types/react']}`
			]
		);
		await writeFile(
			join(reactConsumerDirectory, 'consumer.ts'),
			reactTypeSource
		);
		await writeFile(
			join(reactConsumerDirectory, 'runtime.mjs'),
			reactRuntimeSource
		);
		await typecheck(reactConsumerDirectory, ['react']);
		await run(nodeCommand, ['runtime.mjs'], reactConsumerDirectory);
		await assert.rejects(
			stat(join(reactConsumerDirectory, 'node_modules', 'svelte')),
			(error) => error?.code === 'ENOENT',
			'the React entry point must not install Svelte'
		);

		const reactInputs = await bundleInputs(
			reactConsumerDirectory,
			`import '@hops-ops/distributed/react';`
		);
		assert.equal(
			reactInputs.some((path) => /node_modules\/react(?:\/|$)/.test(path)),
			true,
			'the React entry point must bind the installed peer'
		);
		assert.equal(
			reactInputs.some((path) => path.includes('/dist/sveltekit/')),
			false,
			'the React entry point must not pull the SvelteKit adapter'
		);

		console.log(
			`Pack smoke passed for ${packageJson.name}@${packageJson.version} (${packedPaths.size} files).`
		);
	} finally {
		await rm(temporaryRoot, { recursive: true, force: true });
	}
}

await smokePack();
