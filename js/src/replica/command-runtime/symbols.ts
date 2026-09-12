/**
 * Package-private authority handshake implemented by DistributedReplica.
 *
 * This symbol is intentionally not exported from the public replica barrel.
 * Generated command binders can therefore consume server-derived preset values,
 * while application code cannot pass an ambient caller-owned inventory.
 *
 * @internal
 */
export const replicaCommandAuthority = Symbol('distributed.replica.command-authority');
/**
 * Package-private post-commit observation channel implemented by
 * DistributedReplica. Generated command runtimes register here so framework
 * adapters never guess replica commit ordering.
 *
 * @internal
 */
export const replicaResultObservation = Symbol(
	'distributed.replica.result-observation'
);
/**
 * Package-private direct-projection commit implemented by DistributedReplica.
 *
 * The replica must advance its protocol record clock in the same confirmation
 * path as the base-cache write. Otherwise an older query response can be
 * mistaken for an incomplete write after the cache correctly rejects it.
 *
 * @internal
 */
export const replicaCommandDirectProjection = Symbol(
	'distributed.replica.command-direct-projection'
);
/** @internal Atomic preview-to-actual optimistic layer replacement. */
export const replicaCommandProjectionDelta = Symbol(
	'distributed.replica.command-projection-delta'
);
/** @internal Package-private; intentionally absent from the public barrel. */
export const replicaCommandProjectedLifecycle = Symbol(
	'distributed.replica.command-projected-lifecycle'
);
/**
 * Package-private live-record field read for pure reduce optimism.
 * Returns undefined when the record is not present (fail closed).
 *
 * @internal
 */
export const replicaCommandReadRecord = Symbol(
	'distributed.replica.command-read-record'
);

/** @internal Generated dependency registration before dispatch/optimism. */
export const replicaCommandFreshness = Symbol('distributed.replica.command-freshness');
