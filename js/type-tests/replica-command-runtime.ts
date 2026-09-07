import { createLazyReplicaCommandRuntime } from '@hops-ops/distributed/replica/lazy';
import {
	createReplicaCommandRuntime,
	type ReplicaCommandStatusArtifact,
	type DistributedReplica,
	type ReplicaCommandArtifact,
	type ReplicaCommandTransport
} from '@hops-ops/distributed/replica';

type CreateInput = {
	readonly id: string;
};

type CreateOutput = {
	readonly ok: boolean;
};

declare const replica: DistributedReplica;
declare const transport: ReplicaCommandTransport;
declare const createArtifact: ReplicaCommandArtifact<CreateInput, CreateOutput>;
declare const pingArtifact: ReplicaCommandArtifact<void, { readonly pong: true }>;

const runtime = createReplicaCommandRuntime(replica, transport, {
	create: createArtifact,
	ping: { artifact: pingArtifact }
});

runtime.commands.create({ id: 'todo-1' }).then((receipt) => {
	const ok: boolean = receipt.result.ok;
	return ok;
});
runtime.commands.ping().then((receipt) => {
	const pong: true = receipt.result.pong;
	return pong;
});
runtime.commands.create({ id: 'todo-1' }).then((receipt) =>
	receipt.status().then((status) => {
		const state: string = status.state;
		const metadata = status.metadata;
		return { state, metadata };
	})
);

const nestedRuntime = createReplicaCommandRuntime(replica, transport, {
	'todo.create': createArtifact,
	'todo.ping': { artifact: pingArtifact }
});
nestedRuntime.commands.todo.create({ id: 'todo-1' });
nestedRuntime.commands.todo.ping();
// @ts-expect-error Dotted inventory keys are nested callable namespaces.
nestedRuntime.commands['todo.create'];

// @ts-expect-error Generated input remains required.
runtime.commands.create();
// @ts-expect-error A void-input command accepts options, not domain input.
runtime.commands.ping({ id: 'todo-1' });
// @ts-expect-error Result types cannot bleed between generated commands.
runtime.commands.create({ id: 'todo-1' }).then((receipt) => receipt.result.pong);


declare const status: ReplicaCommandStatusArtifact;
const lazy = createLazyReplicaCommandRuntime(replica, transport, {
 commands: { 'todo.create': { operationHash: 'hash', hasInput: true }, 'todo.ping': { operationHash: 'hash', hasInput: false } }, status
}, async () => ({ entries: { 'todo.create': createArtifact, 'todo.ping': pingArtifact } }));
lazy.commands.todo.create({ id: 'todo-1' }).then(receipt => { const ok: boolean = receipt.result.ok; return ok; });
lazy.commands.todo.ping({ commandId: 'id' }).then(receipt => { const pong: true = receipt.result.pong; return pong; });
lazy.preload();
// @ts-expect-error Lazy commands preserve required input.
lazy.commands.todo.create();
// @ts-expect-error No-input commands still accept only options.
lazy.commands.todo.ping({ id: 'todo-1' });
// @ts-expect-error Lazy output inference stays specific to each command.
lazy.commands.todo.create({ id: 'todo-1' }).then(receipt => receipt.result.pong);
