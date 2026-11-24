import { compareCodeUnits } from '../../lib/compare-code-units.js';
import type { BaseCacheWriter } from '../../internal/cache-engine.js';
import {
	compareDistributedDecimal,
	type DistributedOpaqueString,
	type DistributedRecordRevision
} from '../../protocol.js';
import { recordKeyMatchesModel } from '../identity/keys.js';
import type { ReplicaValue } from '../types.js';
import {
	compareEvidenceToProjectedFence,
	compareProjectedRecordFields,
	compareRecordClock,
	freezeRecordClock,
	protocolInvalid,
	responsePathKey,
	sameRecordClock,
	sameRecordRevision
} from './clocks.js';
import { MAX_ANONYMOUS_RECORD_CLOCKS } from './constants.js';
import type {
	AnonymousRecordProtocolClock,
	ParsedReplicaHydration,
	ProjectedRecordFence,
	RecordProtocolClock,
	ReplicaDehydratedPayloadV1
} from './types.js';

/**
 * The causal clock of every record the replica has evidence for: by record
 * key, by the server's scope token, for rows not yet seen by key
 * ("anonymous"), and the write fence a command's direct projection holds
 * until a query acknowledges it.
 */
export class RecordClocks {
	readonly #clocks = new Map<string, RecordProtocolClock>();
	readonly #keysByScope = new Map<DistributedOpaqueString, string>();
	readonly #anonymous = new Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>();
	readonly #projectedFences = new Map<string, ProjectedRecordFence>();
	#projectionGeneration = 0;

	/** Counts direct projections; a request compares it with the fence's. */
	get projectionGeneration(): number {
		return this.#projectionGeneration;
	}

	/** Start admitting the record evidence of one result. */
	evidence(): RecordEvidence {
		return new RecordEvidence(
			this.#clocks,
			this.#keysByScope,
			this.#anonymous,
			this.#projectedFences
		);
	}

	/**
	 * The command result is authoritative before an asynchronous read model
	 * necessarily catches up. Retain its complete row and causal clock as a
	 * write fence until a query acknowledges it or a newer record/tombstone
	 * supersedes it.
	 */
	project(
		recordKey: string,
		fields: Readonly<Record<string, ReplicaValue>>,
		evidence: DistributedRecordRevision
	): void {
		this.#projectionGeneration += 1;
		this.#projectedFences.set(
			recordKey,
			Object.freeze({
				fields: Object.freeze({ ...fields }),
				clock: Object.freeze({
					scopeToken: evidence.scopeToken,
					incarnation: evidence.incarnation,
					revision: evidence.revision,
					tombstone: false
				}),
				projectionGeneration: this.#projectionGeneration
			})
		);
	}

	projectedFences(): IterableIterator<[string, ProjectedRecordFence]> {
		return this.#projectedFences.entries();
	}

	/** Drop the clocks, scopes and fences of collected records. */
	forget(recordKeys: readonly string[]): void {
		const gone = new Set(recordKeys);
		for (const key of recordKeys) {
			this.#clocks.delete(key);
			this.#projectedFences.delete(key);
		}
		for (const [scopeToken, key] of this.#keysByScope) {
			if (gone.has(key)) this.#keysByScope.delete(scopeToken);
		}
	}

	clear(): void {
		this.#clocks.clear();
		this.#keysByScope.clear();
		this.#projectedFences.clear();
		this.#anonymous.clear();
		this.#projectionGeneration = 0;
	}

	/** Replace the clocks with a dehydrated state's (cold hydration). */
	restore(parsed: ParsedReplicaHydration): void {
		this.#clocks.clear();
		for (const [key, clock] of parsed.recordClocks) {
			this.#clocks.set(key, clock);
		}
		this.#keysByScope.clear();
		for (const [scopeToken, key] of parsed.recordKeysByScope) {
			this.#keysByScope.set(scopeToken, key);
		}
		this.#anonymous.clear();
		for (const [scopeToken, clock] of parsed.anonymousRecordClocks) {
			this.#anonymous.set(scopeToken, clock);
		}
	}

	/**
	 * Merge a same-scope dehydrated state. Returns the records whose current
	 * clock is ahead of (or foreign to) the incoming one: they keep their row.
	 */
	merge(parsed: ParsedReplicaHydration): ReadonlySet<string> {
		const retained = new Set<string>();
		for (const [key, clock] of parsed.recordClocks) {
			const previous = this.#clocks.get(key);
			if (
				previous !== undefined && (
					previous.scopeToken !== clock.scopeToken ||
					compareRecordClock(previous, clock) > 0
				)
			) {
				retained.add(key);
				continue;
			}
			this.#clocks.set(key, clock);
		}
		for (const [scopeToken, key] of parsed.recordKeysByScope) {
			if (!retained.has(key)) this.#keysByScope.set(scopeToken, key);
		}
		for (const [scopeToken, clock] of parsed.anonymousRecordClocks) {
			const previous = this.#anonymous.get(scopeToken);
			if (
				previous !== undefined &&
				compareRecordClock(previous.clock, clock.clock) > 0
			) continue;
			this.#anonymous.set(scopeToken, clock);
		}
		return retained;
	}

	dehydrate(
		recordKeys: ReadonlySet<string>,
		models: ReadonlySet<string>
	): Pick<ReplicaDehydratedPayloadV1, 'recordClocks' | 'anonymousRecordClocks'> {
		return {
			recordClocks: Object.freeze(
				[...this.#clocks]
					.filter(([key]) => recordKeys.has(key))
					.sort(([left], [right]) => compareCodeUnits(left, right))
					.map(([key, clock]) =>
						Object.freeze([key, freezeRecordClock(clock)] as const)
					)
			),
			anonymousRecordClocks: Object.freeze(
				[...this.#anonymous]
					.filter(([, value]) => models.has(value.model))
					.sort(([left], [right]) => compareCodeUnits(left, right))
					.map(([scopeToken, value]) =>
						Object.freeze([
							scopeToken,
							Object.freeze({
								model: value.model,
								clock: freezeRecordClock(value.clock)
							})
						] as const)
					)
			)
		};
	}

	sizes(): Record<string, number> {
		return {
			recordClocks: this.#clocks.size,
			recordKeysByScope: this.#keysByScope.size,
			projectedRecordFences: this.#projectedFences.size,
			anonymousRecordClocks: this.#anonymous.size
		};
	}
}

/**
 * The record evidence of one result, admitted against the clocks without
 * changing them. `commit` applies it once the cache transaction succeeded;
 * a result that throws leaves the clocks as they were.
 */
class RecordEvidence {
	readonly #clocks: Map<string, RecordProtocolClock>;
	readonly #keysByScope: Map<DistributedOpaqueString, string>;
	readonly #anonymous: Map<DistributedOpaqueString, AnonymousRecordProtocolClock>;
	readonly #projectedFences: Map<string, ProjectedRecordFence>;
	readonly #pendingClocks = new Map<string, RecordProtocolClock>();
	readonly #pendingScopes = new Map<DistributedOpaqueString, string>();
	readonly #pendingAnonymous = new Map<
		DistributedOpaqueString,
		AnonymousRecordProtocolClock
	>();
	readonly #consumedAnonymous = new Set<DistributedOpaqueString>();
	readonly #releasedFences = new Map<string, ProjectedRecordFence>();

	constructor(
		clocks: Map<string, RecordProtocolClock>,
		keysByScope: Map<DistributedOpaqueString, string>,
		anonymous: Map<DistributedOpaqueString, AnonymousRecordProtocolClock>,
		projectedFences: Map<string, ProjectedRecordFence>
	) {
		this.#clocks = clocks;
		this.#keysByScope = keysByScope;
		this.#anonymous = anonymous;
		this.#projectedFences = projectedFences;
	}

	/**
	 * Admit the evidence of a selected row. Returns whether the row's fields
	 * may be written.
	 */
	row(
		recordKey: string,
		evidence: DistributedRecordRevision,
		fields: Readonly<Record<string, ReplicaValue>>,
		responseProjectionGeneration: number | undefined
	): boolean {
		const projectedFence = this.#projectedFences.get(recordKey);
		const projectedDisposition =
			projectedFence === undefined
				? undefined
				: compareProjectedRecordFields(projectedFence.fields, fields);
		const projectedClockComparison =
			projectedFence === undefined
				? undefined
				: compareEvidenceToProjectedFence(evidence, projectedFence);
		const newerPostProjectionRow =
			projectedFence !== undefined &&
			projectedDisposition === 'conflict' &&
			projectedClockComparison === 1 &&
			responseProjectionGeneration !== undefined &&
			responseProjectionGeneration >= projectedFence.projectionGeneration;
		/*
		 * Snapshot bodies can race: SQL may be read before a command
		 * while response evidence is stamped after it. Conflicting
		 * pre-command responses therefore cannot override a projected
		 * fence, even when their evidence revision is numerically later.
		 * A causally newer row from an HTTP request or live frame that
		 * began after the command is an atomic supersession and may
		 * release the fence without first echoing the projected body.
		 */
		if (projectedFence !== undefined) {
			const exactEcho =
				projectedDisposition === 'complete' &&
				(
					projectedClockComparison === 0 ||
					projectedClockComparison === 1
				);
			const newerPartial =
				projectedDisposition === 'partial' &&
				projectedClockComparison === 1;
			if (exactEcho || newerPartial || newerPostProjectionRow) {
				this.#releasedFences.set(recordKey, projectedFence);
			}
		}
		return this.resolve(
			recordKey,
			evidence,
			projectedDisposition === 'conflict' && !newerPostProjectionRow
		);
	}

	/**
	 * Compare one record's evidence with its clock. Returns whether the
	 * evidence is current, so the row (or tombstone) it proves may be written.
	 */
	resolve(
		recordKey: string,
		evidence: DistributedRecordRevision,
		projectedConflict = false
	): boolean {
		if (!recordKeyMatchesModel(recordKey, evidence.model)) {
			protocolInvalid('extensions.distributed.snapshot.records.model');
		}
		const scopedKey =
			this.#pendingScopes.get(evidence.scopeToken) ??
			this.#keysByScope.get(evidence.scopeToken);
		if (scopedKey !== undefined && scopedKey !== recordKey) {
			protocolInvalid('extensions.distributed.snapshot.records.scopeToken');
		}
		this.#pendingScopes.set(evidence.scopeToken, recordKey);

		const incoming: RecordProtocolClock = {
			scopeToken: evidence.scopeToken,
			incarnation: evidence.incarnation,
			revision: evidence.revision,
			tombstone: evidence.tombstone
		};
		const pending = this.#pendingClocks.get(recordKey);
		if (pending !== undefined) {
			if (!sameRecordClock(pending, incoming)) {
				protocolInvalid('extensions.distributed.snapshot.records');
			}
			return true;
		}
		let current = this.#clocks.get(recordKey);
		const anonymous =
			this.#pendingAnonymous.get(evidence.scopeToken) ??
			this.#anonymous.get(evidence.scopeToken);
		if (anonymous !== undefined) {
			if (anonymous.model !== evidence.model) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.model'
				);
			}
			this.#consumedAnonymous.add(evidence.scopeToken);
			if (current === undefined) {
				current = anonymous.clock;
			} else {
				if (current.scopeToken !== anonymous.clock.scopeToken) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.scopeToken'
					);
				}
				const anonymousComparison = compareRecordClock(
					anonymous.clock,
					current
				);
				if (
					anonymousComparison === 0 &&
					anonymous.clock.tombstone !== current.tombstone
				) {
					protocolInvalid(
						'extensions.distributed.snapshot.records.tombstone'
					);
				}
				if (anonymousComparison > 0) current = anonymous.clock;
			}
		}
		if (current === undefined) {
			this.#pendingClocks.set(recordKey, incoming);
			return true;
		}
		if (current.scopeToken !== incoming.scopeToken) {
			protocolInvalid('extensions.distributed.snapshot.records.scopeToken');
		}
		if (projectedConflict) {
			this.#pendingClocks.set(recordKey, current);
			return false;
		}
		const comparison = compareRecordClock(incoming, current);
		if (comparison < 0) {
			this.#pendingClocks.set(recordKey, current);
			return false;
		}
		if (comparison === 0) {
			if (current.tombstone !== incoming.tombstone) {
				protocolInvalid('extensions.distributed.snapshot.records.tombstone');
			}
			this.#pendingClocks.set(recordKey, incoming);
			return true;
		}
		if (
			current.tombstone &&
			!incoming.tombstone &&
			compareDistributedDecimal(
				incoming.incarnation,
				current.incarnation
			) <= 0
		) {
			protocolInvalid('extensions.distributed.snapshot.records.incarnation');
		}
		this.#pendingClocks.set(recordKey, incoming);
		return true;
	}

	/**
	 * Apply tombstone evidence. A tombstone names its record by scope token or
	 * by the response path the operation last resolved; one for a record never
	 * seen by key is kept as an anonymous clock.
	 */
	tombstones(
		writer: BaseCacheWriter,
		evidenceItems: readonly DistributedRecordRevision[],
		pathRecords: ReadonlyMap<string, string>,
		consumedPaths: Set<string>
	): void {
		for (const evidence of evidenceItems) {
			const encodedPath =
				evidence.path === undefined
					? undefined
					: responsePathKey(evidence.path);
			const recordKey =
				this.#keysByScope.get(evidence.scopeToken) ??
				(encodedPath === undefined
					? undefined
					: pathRecords.get(encodedPath));
			if (recordKey === undefined) {
				this.#retainAnonymous(evidence);
				continue;
			}
			if (encodedPath !== undefined) consumedPaths.add(encodedPath);
			if (this.#resolveAgainstFence(recordKey, evidence)) {
				writer.tombstoneRecord(
					recordKey,
					evidence.revision,
					evidence.incarnation
				);
			}
		}
	}

	/**
	 * Apply change-log evidence that names no response path. It advances the
	 * record's clock and discards the cached row unless a selected row of the
	 * same result already certified that revision.
	 */
	pathless(
		writer: BaseCacheWriter,
		evidenceItems: readonly DistributedRecordRevision[],
		pathEvidence: ReadonlyMap<string, DistributedRecordRevision>,
		consumedPaths: ReadonlySet<string>
	): void {
		for (const evidence of evidenceItems) {
			const recordKey =
				this.#pendingScopes.get(evidence.scopeToken) ??
				this.#keysByScope.get(evidence.scopeToken);
			if (recordKey === undefined) {
				this.#retainAnonymous(evidence);
				continue;
			}
			if (!recordKeyMatchesModel(recordKey, evidence.model)) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.model'
				);
			}
			let certifiedByPath = false;
			for (const [path, candidate] of pathEvidence) {
				if (
					consumedPaths.has(path) &&
					candidate.scopeToken === evidence.scopeToken &&
					sameRecordRevision(candidate, evidence)
				) {
					certifiedByPath = true;
					break;
				}
			}
			if (certifiedByPath) continue;
			if (this.#resolveAgainstFence(recordKey, evidence)) {
				// A change-log upsert proves only that a newer row exists. It
				// advances the tuple fence, but cannot certify any cached field.
				writer.discardRecord(recordKey);
			}
		}
	}

	/** Make the admitted evidence the replica's clocks. */
	commit(): void {
		for (const [recordKey, clock] of this.#pendingClocks) {
			this.#clocks.set(recordKey, clock);
			this.#keysByScope.set(clock.scopeToken, recordKey);
		}
		for (const [scopeToken, recordKey] of this.#pendingScopes) {
			this.#keysByScope.set(scopeToken, recordKey);
		}
		for (const [scopeToken, clock] of this.#pendingAnonymous) {
			if (!this.#consumedAnonymous.has(scopeToken)) {
				this.#anonymous.set(scopeToken, clock);
			}
		}
		for (const scopeToken of this.#consumedAnonymous) {
			this.#anonymous.delete(scopeToken);
		}
		for (const [recordKey, fence] of this.#releasedFences) {
			if (this.#projectedFences.get(recordKey) === fence) {
				this.#projectedFences.delete(recordKey);
			}
		}
	}

	/** Evidence without a row: only a causally newer one passes the fence. */
	#resolveAgainstFence(
		recordKey: string,
		evidence: DistributedRecordRevision
	): boolean {
		const projectedFence = this.#projectedFences.get(recordKey);
		const newer =
			projectedFence !== undefined &&
			compareEvidenceToProjectedFence(evidence, projectedFence) === 1;
		if (newer) this.#releasedFences.set(recordKey, projectedFence);
		return this.resolve(
			recordKey,
			evidence,
			projectedFence !== undefined && !newer
		);
	}

	#retainAnonymous(evidence: DistributedRecordRevision): void {
		const incoming: AnonymousRecordProtocolClock = {
			model: evidence.model,
			clock: {
				scopeToken: evidence.scopeToken,
				incarnation: evidence.incarnation,
				revision: evidence.revision,
				tombstone: evidence.tombstone
			}
		};
		const current =
			this.#pendingAnonymous.get(evidence.scopeToken) ??
			this.#anonymous.get(evidence.scopeToken);
		if (current === undefined) {
			let retained = this.#anonymous.size;
			for (const scopeToken of this.#pendingAnonymous.keys()) {
				if (!this.#anonymous.has(scopeToken)) retained += 1;
			}
			if (retained >= MAX_ANONYMOUS_RECORD_CLOCKS) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.capacity'
				);
			}
			this.#pendingAnonymous.set(evidence.scopeToken, incoming);
			return;
		}
		if (current.model !== incoming.model) {
			protocolInvalid('extensions.distributed.snapshot.records.model');
		}
		const comparison = compareRecordClock(
			incoming.clock,
			current.clock
		);
		if (comparison < 0) return;
		if (comparison === 0) {
			if (current.clock.tombstone !== incoming.clock.tombstone) {
				protocolInvalid(
					'extensions.distributed.snapshot.records.tombstone'
				);
			}
			return;
		}
		if (
			current.clock.tombstone &&
			!incoming.clock.tombstone &&
			compareDistributedDecimal(
				incoming.clock.incarnation,
				current.clock.incarnation
			) <= 0
		) {
			protocolInvalid(
				'extensions.distributed.snapshot.records.incarnation'
			);
		}
		this.#pendingAnonymous.set(evidence.scopeToken, incoming);
	}
}
