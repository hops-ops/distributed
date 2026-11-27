/** Dedicated entry point keeps eager command exports out of a lazy client's initial import graph. */
export { createLazyReplicaCommandRuntime } from './command-runtime/lazy.js';
export type {
	ReplicaLazyCommandCatalog,
	ReplicaLazyCommandModule,
	ReplicaLazyCommandRuntime
} from './command-runtime/lazy.js';
