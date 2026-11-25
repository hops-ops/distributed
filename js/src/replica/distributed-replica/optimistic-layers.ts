import type {
	BaseCacheWriter,
	CacheEngine
} from '../../internal/cache-engine.js';
import {
	DistributedProtocolError,
	type DistributedCommandMetadata,
	type DistributedProjectionObservation
} from '../../protocol.js';
import { replicaRecordKey } from '../identity/keys.js';
import type { ReplicaIndexSemanticChange } from '../index-maintenance/types.js';
import type {
	ReplicaBaseWriter,
	ReplicaOptimisticWriter,
	ReplicaRevalidationPlan
} from '../types.js';
import { protocolInvalid } from './clocks.js';
import type { DiagnosticsReporter } from './diagnostics.js';
import { baseWriter, indexKeyFromTarget } from './helpers.js';
import {
	assertReplicaOptimisticLayerId,
	captureReplicaOptimisticUpdate,
	cloneOptimisticReceipt,
	expectationKey,
	optimisticReceiptState,
	replayReplicaOptimisticUpdate,
	sameReceipt
} from './optimistic.js';
import type { OptimisticReceiptState } from './types.js';

type OptimisticReceiptPlan = {
	readonly updates: Map<string, OptimisticReceiptState>;
	readonly satisfied: string[];
};

/**
 * Everything the replica keeps per pending command: its causal receipt, the
 * index memberships its projection delta inserted, a confirmation deferred
 * behind those memberships, and its freshness plan. The cache engine holds
 * the layers themselves.
 */
export class OptimisticLayers {
	readonly #engine: CacheEngine;
	readonly #diagnostics: DiagnosticsReporter;
	readonly #receipts = new Map<string, OptimisticReceiptState>();
	/**
	 * Record keys inserted by Eventual projection-delta. A later complete
	 * query/live index that omits them is behind command confirmation and
	 * must not shrink the visible list. Atomic rows use projected-record
	 * fences only — they have no @live that would otherwise clear this map.
	 */
	readonly #membershipFences = new Map<string, Map<string, Set<string>>>();
	readonly #deferredConfirms = new Set<string>();
	readonly #freshnessPlans = new Map<
		string,
		{ generation: number; plan: ReplicaRevalidationPlan }
	>();

	constructor(engine: CacheEngine, diagnostics: DiagnosticsReporter) {
		this.#engine = engine;
		this.#diagnostics = diagnostics;
	}

	receipt(id: string): OptimisticReceiptState | undefined {
		return this.#receipts.get(id);
	}

	create(
		id: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[]
	): void {
		assertReplicaOptimisticLayerId(id);
		if (this.#engine.optimisticLayerState(id) !== undefined) {
			throw new Error(`optimistic layer already exists: ${id}`);
		}
		if (typeof update !== 'function') {
			throw new TypeError('optimistic layer update must be a function');
		}
		const captured = captureReplicaOptimisticUpdate(
			id,
			update,
			semanticChanges
		);
		this.#engine.createOptimisticLayer(
			id,
			(writer) => replayReplicaOptimisticUpdate(writer, captured.operations),
			captured.context
		);
		if (!this.#diagnostics.enabled) return;
		const recordChanges = captured.operations.filter(
			(operation) =>
				operation.kind === 'write-record' ||
				operation.kind === 'tombstone-record'
		).length;
		this.#diagnostics.layerCreated(
			id,
			recordChanges,
			captured.operations.length - recordChanges,
			recordChanges + semanticChanges.length
		);
	}

	/**
	 * Atomically replace a layer's record operations and semantic index
	 * context with a command's projection delta, remembering the index
	 * memberships it inserts.
	 */
	replace(
		id: string,
		update: (writer: ReplicaOptimisticWriter) => void,
		semanticChanges: readonly ReplicaIndexSemanticChange[]
	): boolean {
		assertReplicaOptimisticLayerId(id);
		const captured = captureReplicaOptimisticUpdate(
			id,
			(writer) => update(this.#capturingWriter(id, writer)),
			semanticChanges
		);
		return this.#engine.replaceOptimisticLayer(
			id,
			(_reader, writer) =>
				replayReplicaOptimisticUpdate(writer, captured.operations),
			captured.context
		);
	}

	markAccepted(id: string, receipt?: DistributedCommandMetadata): boolean {
		if (receipt !== undefined && receipt.commandId !== id) {
			throw new TypeError('optimistic layer id must equal the causal command id');
		}
		const accepted = this.#engine.markOptimisticLayerAccepted(id);
		if (!accepted) return false;
		this.#diagnostics.layerAccepted(id, true);
		if (receipt === undefined) {
			this.#diagnostics.receiptAccepted(id, undefined);
			this.#diagnostics.sync();
			return true;
		}
		const next = optimisticReceiptState(receipt);
		const current = this.#receipts.get(id);
		if (current !== undefined && !sameReceipt(current, next)) {
			throw new DistributedProtocolError(
				'DISTRIBUTED_PROTOCOL_INVALID',
				'extensions.distributed.command'
			);
		}
		this.#receipts.set(id, next);
		this.#diagnostics.receiptAccepted(id, next);
		this.#diagnostics.sync();
		return true;
	}

	/**
	 * Confirm a command's layer. While a later complete index has yet to echo
	 * a membership the layer inserted, the layer stays and only the base
	 * write happens; `flushDeferredConfirms` retires it afterwards.
	 */
	confirm<T>(id: string, update: (writer: ReplicaBaseWriter) => T): T {
		if (this.#hasMembershipFence(id)) {
			this.#deferredConfirms.add(id);
			return this.#engine.batch((writer) => update(baseWriter(writer)));
		}
		return this.confirmWithCacheWriter(id, (writer) =>
			update(baseWriter(writer))
		);
	}

	/** Confirmation for protocol code that must atomically seal indexes. */
	confirmWithCacheWriter<T>(
		id: string,
		update: (writer: BaseCacheWriter) => T
	): T {
		const result = this.#engine.confirmOptimisticLayer(id, update);
		this.#diagnostics.retireLayer(
			id,
			'retired',
			'atomic',
			this.#receipts.get(id)
		);
		this.#forget(id);
		this.#diagnostics.sync();
		return result;
	}

	reject(id: string): boolean {
		this.#clearMembershipFences(id);
		const rejected = this.#engine.rejectOptimisticLayer(id);
		if (rejected) {
			this.#diagnostics.retireLayer(
				id,
				'rejected',
				'rejected',
				this.#receipts.get(id)
			);
		}
		this.#forget(id);
		if (rejected) this.#diagnostics.sync();
		return rejected;
	}

	planReceipts(
		command: DistributedCommandMetadata | undefined,
		observations: readonly DistributedProjectionObservation[],
		satisfactionAdmissible: boolean
	): OptimisticReceiptPlan {
		const updates = new Map<string, OptimisticReceiptState>();
		if (
			command !== undefined &&
			this.#engine.optimisticLayerState(command.commandId) !== undefined
		) {
			const proposed = optimisticReceiptState(command);
			const current = this.#receipts.get(command.commandId);
			if (current !== undefined && !sameReceipt(current, proposed)) {
				protocolInvalid('extensions.distributed.command');
			}
			updates.set(
				command.commandId,
				cloneOptimisticReceipt(current ?? proposed)
			);
		}
		for (const [id, receipt] of this.#receipts) {
			if (!updates.has(id)) {
				updates.set(id, cloneOptimisticReceipt(receipt));
			}
		}
		for (const receipt of updates.values()) {
			for (const observation of observations) {
				if (observation.causationId !== receipt.causationId) continue;
				const key = expectationKey(observation);
				if (receipt.expectations.has(key)) receipt.observed.add(key);
			}
		}
		const satisfied = satisfactionAdmissible
			? [...updates]
					.filter(
						([, receipt]) =>
							receipt.expectations.size > 0 &&
							[...receipt.expectations.keys()].every((key) =>
								receipt.observed.has(key)
							)
					)
					.map(([id]) => id)
			: [];
		return { updates, satisfied };
	}

	/**
	 * Keep the receipt progress of a committed result. Layers in
	 * `plan.satisfied` were confirmed by that transaction and retire here.
	 */
	commitReceipts(plan: OptimisticReceiptPlan): void {
		for (const [id, receipt] of plan.updates) {
			if (plan.satisfied.includes(id)) {
				this.#diagnostics.retireLayer(id, 'retired', 'atomic', receipt);
				this.#forget(id);
			} else {
				this.#accept(id, receipt);
			}
		}
	}

	/** A result without a snapshot carries only a command receipt. */
	applyReceiptOnly(command: DistributedCommandMetadata | undefined): void {
		if (command === undefined) return;
		const plan = this.planReceipts(command, command.observations, true);
		for (const [id, receipt] of plan.updates) this.#accept(id, receipt);
	}

	#accept(id: string, receipt: OptimisticReceiptState): void {
		this.#receipts.set(id, receipt);
		this.#engine.markOptimisticLayerAccepted(id);
		this.#diagnostics.layerAccepted(id, false);
		this.#diagnostics.receiptAccepted(id, receipt);
	}

	/**
	 * A base writer that refuses a complete index omitting a membership a
	 * pending command inserted, and releases the memberships an accepted index
	 * echoes. Indexes in `ownedElsewhere` stay with their independent owner.
	 */
	guardIndexWriter(
		writer: BaseCacheWriter,
		ownedElsewhere?: ReadonlySet<string>
	): BaseCacheWriter {
		const skip = (key: string): boolean => ownedElsewhere?.has(key) === true;
		return {
			recordClock: (key) => writer.recordClock(key),
			writeRecord: (write) => writer.writeRecord(write),
			tombstoneRecord: (key, revision, incarnation) =>
				writer.tombstoneRecord(key, revision, incarnation),
			discardRecord: (key) => writer.discardRecord(key),
			writeIndex: (write) => {
				if (skip(write.key)) return false;
				const recordsForIndex = this.#membershipFences.get(write.key);
				if (write.complete === true && recordsForIndex !== undefined) {
					const visible =
						this.#engine.read(
							(reader) => reader.index(write.key)?.records
						) ?? [];
					for (const recordKey of recordsForIndex.keys()) {
						if (
							visible.includes(recordKey) &&
							!write.records.includes(recordKey)
						) {
							return false;
						}
					}
				}
				const wrote = writer.writeIndex(write);
				if (wrote && recordsForIndex !== undefined) {
					for (const recordKey of write.records) {
						recordsForIndex.delete(recordKey);
					}
					if (recordsForIndex.size === 0) {
						this.#membershipFences.delete(write.key);
					}
				}
				return wrote;
			},
			markIndexStale: (key, reason, revision) =>
				!skip(key) && writer.markIndexStale(key, reason, revision),
			deleteIndex: (key, revision) =>
				!skip(key) && writer.deleteIndex(key, revision)
		};
	}

	/** Retire the layers whose deferred confirmation is no longer fenced. */
	flushDeferredConfirms(): void {
		for (const commandId of [...this.#deferredConfirms]) {
			if (this.#hasMembershipFence(commandId)) continue;
			this.#deferredConfirms.delete(commandId);
			if (this.#engine.optimisticLayerState(commandId) === undefined) {
				continue;
			}
			this.confirmWithCacheWriter(commandId, () => undefined);
		}
	}

	registerFreshness(
		commandId: string,
		plan: ReplicaRevalidationPlan,
		generation: number
	): void {
		this.#pruneFreshness(generation);
		if (!this.#freshnessPlans.has(commandId) && this.#freshnessPlans.size >= 256) {
			throw new Error('too many pending causal commands');
		}
		this.#freshnessPlans.set(commandId, { generation, plan });
	}

	/** The freshness plans of commands still pending in this generation. */
	pendingFreshness(generation: number): ReplicaRevalidationPlan[] {
		this.#pruneFreshness(generation);
		return [...this.#freshnessPlans.values()].map((entry) => entry.plan);
	}

	#pruneFreshness(generation: number): void {
		for (const [id, entry] of this.#freshnessPlans) {
			if (
				entry.generation !== generation ||
				this.#engine.optimisticLayerState(id) === undefined
			) {
				this.#freshnessPlans.delete(id);
			}
		}
	}

	/** Forget receipts only: a cold hydration replaces the cache under them. */
	clearReceipts(): void {
		this.#receipts.clear();
	}

	/** Forget everything scoped to the authorization generation. */
	clear(): void {
		this.#receipts.clear();
		this.#membershipFences.clear();
		this.#deferredConfirms.clear();
		this.#freshnessPlans.clear();
	}

	/**
	 * A layer left the cache (confirmed, proven or rejected): drop its
	 * receipt, any confirmation deferred behind its memberships, and its
	 * freshness plan. The memberships themselves stay until a complete index
	 * echoes them, since that index is what they fence.
	 */
	#forget(id: string): void {
		this.#receipts.delete(id);
		this.#deferredConfirms.delete(id);
		this.#freshnessPlans.delete(id);
	}

	sizes(): Record<string, number> {
		return {
			membershipFences: this.#membershipFences.size,
			deferredMembershipConfirms: this.#deferredConfirms.size,
			freshnessPlans: this.#freshnessPlans.size,
			optimisticReceipts: this.#receipts.size
		};
	}

	#capturingWriter(
		commandId: string,
		writer: ReplicaOptimisticWriter
	): ReplicaOptimisticWriter {
		const touchedRecords = new Set<string>();
		return {
			writeRecord: (model, identity, patch) => {
				touchedRecords.add(replicaRecordKey(model, identity));
				writer.writeRecord(model, identity, patch);
			},
			tombstoneRecord: (model, identity) => {
				const recordKey = replicaRecordKey(model, identity);
				touchedRecords.delete(recordKey);
				this.#clearMembershipFences(commandId, recordKey);
				writer.tombstoneRecord(model, identity);
			},
			writeIndex: (target, records) => {
				const indexKey = indexKeyFromTarget(target);
				for (const recordKey of records) {
					if (!touchedRecords.has(recordKey)) continue;
					let recordsForIndex = this.#membershipFences.get(indexKey);
					if (recordsForIndex === undefined) {
						recordsForIndex = new Map();
						this.#membershipFences.set(indexKey, recordsForIndex);
					}
					let owners = recordsForIndex.get(recordKey);
					if (owners === undefined) {
						owners = new Set();
						recordsForIndex.set(recordKey, owners);
					}
					owners.add(commandId);
				}
				writer.writeIndex(target, records);
			},
			deleteIndex: (target) => {
				const indexKey = indexKeyFromTarget(target);
				const recordsForIndex = this.#membershipFences.get(indexKey);
				if (recordsForIndex !== undefined) {
					for (const [recordKey, owners] of recordsForIndex) {
						owners.delete(commandId);
						if (owners.size === 0) recordsForIndex.delete(recordKey);
					}
					if (recordsForIndex.size === 0) {
						this.#membershipFences.delete(indexKey);
					}
				}
				writer.deleteIndex(target);
			}
		};
	}

	#hasMembershipFence(commandId: string): boolean {
		for (const recordsForIndex of this.#membershipFences.values()) {
			for (const owners of recordsForIndex.values()) {
				if (owners.has(commandId)) return true;
			}
		}
		return false;
	}

	/** Release a command's memberships: all of them, or those of one record. */
	#clearMembershipFences(commandId: string, onlyRecord?: string): void {
		for (const [indexKey, recordsForIndex] of this.#membershipFences) {
			for (const [recordKey, owners] of recordsForIndex) {
				if (onlyRecord !== undefined && recordKey !== onlyRecord) continue;
				owners.delete(commandId);
				if (owners.size === 0) recordsForIndex.delete(recordKey);
			}
			if (recordsForIndex.size === 0) {
				this.#membershipFences.delete(indexKey);
			}
		}
	}
}
