/**
 * Generic entity replica driven by Deltas, with per-entity snapshot history
 * for interpolated rendering at `serverNow - interpolationDelay`.
 *
 * Entities are decoded with the Welcome schema; no generated code. Each
 * entity keeps a short time-ordered snapshot buffer on its zone's server
 * timeline (tick → time via the Welcome anchor). Sampling between two
 * snapshots interpolates per field: `lerp` (linear), `angle` (shortest arc,
 * wrapped to [0, 2π)) or `step` (hold the older value).
 */
import { maskHas, type DeltaMessage } from './messages.js';
import { TAU, type FieldValue, type GameKindDef, type GameSchema } from './schema.js';

export type InterpolationMode = 'lerp' | 'angle' | 'step';

/** Per kind name → per field name → mode. Unlisted fields use type defaults. */
export type InterpolationPolicy = Readonly<
	Record<string, Readonly<Record<string, InterpolationMode>>>
>;

export type EntityReplicaOptions = Readonly<{
	interpolation?: InterpolationPolicy;
	/** Snapshot history kept per entity (server ms). Default 1000. */
	historyMs?: number;
	/** Hard cap on snapshots per entity. Default 64. */
	maxSnapshots?: number;
	/**
	 * After a Welcome (reconnect, authority swap) previously known entities
	 * stay rendered as stale until re-created or until this much server
	 * time passed. Default 1000 ms.
	 */
	staleGraceMs?: number;
}>;

/** One entity's fields keyed by schema field name. */
export type EntityFields = Readonly<Record<string, FieldValue>>;

export type ReplicaEntity = Readonly<{
	id: number;
	kind: number;
	kindName: string;
	fields: EntityFields;
	/** Known before the latest Welcome and not yet re-created. */
	stale: boolean;
}>;

type Snapshot = { time: number; values: FieldValue[] };

type Entry = {
	id: number;
	kind: GameKindDef;
	modes: InterpolationMode[];
	snapshots: Snapshot[];
	stale: boolean;
	staleSince: number;
};

/** Default interpolation for a field type. */
export function defaultInterpolationMode(type: string): InterpolationMode {
	if (type === 'f32' || type === 'quant_f32') return 'lerp';
	if (type === 'angle') return 'angle';
	return 'step';
}

/** Shortest signed angular difference `b - a`, in (-π, π]. */
export function angleDelta(a: number, b: number): number {
	let d = (b - a) % TAU;
	if (d > Math.PI) d -= TAU;
	else if (d <= -Math.PI) d += TAU;
	return d;
}

/** Wrap radians into [0, 2π). */
export function wrapAngle(a: number): number {
	const w = a % TAU;
	return w < 0 ? w + TAU : w;
}

export function interpolateValue(
	mode: InterpolationMode,
	a: FieldValue,
	b: FieldValue,
	t: number
): FieldValue {
	if (typeof a !== 'number' || typeof b !== 'number' || mode === 'step') return t >= 1 ? b : a;
	if (mode === 'angle') return wrapAngle(a + angleDelta(a, b) * t);
	return a + (b - a) * t;
}

export class EntityReplica {
	readonly #options: Required<Omit<EntityReplicaOptions, 'interpolation'>> & {
		interpolation: InterpolationPolicy;
	};
	#schema: GameSchema | undefined;
	readonly #entries = new Map<number, Entry>();
	#lastDeltaTime: number | undefined;
	#serverTick = 0;
	#ackSeq = 0;

	constructor(options: EntityReplicaOptions = {}) {
		this.#options = {
			interpolation: options.interpolation ?? {},
			historyMs: options.historyMs ?? 1000,
			maxSnapshots: Math.max(2, options.maxSnapshots ?? 64),
			staleGraceMs: options.staleGraceMs ?? 1000
		};
	}

	/**
	 * Render delay (ms) behind the server clock this replica is sampled at.
	 * Maintained by `GameClient`; games include it (or the matching view
	 * tick) in input payloads so the server can lag-compensate.
	 */
	interpolationDelayMs: number | undefined;

	get schema(): GameSchema | undefined {
		return this.#schema;
	}

	get serverTick(): number {
		return this.#serverTick;
	}

	get ackSeq(): number {
		return this.#ackSeq;
	}

	get size(): number {
		return this.#entries.size;
	}

	/**
	 * A Welcome arrived: adopt its schema and mark everything stale. Stale
	 * entities keep rendering (holding their last value) so a reconnect or
	 * authority swap does not blank the world; a re-create of the same id
	 * and kind revives the entity with its history intact.
	 */
	reset(schema: GameSchema, time: number): void {
		this.#schema = schema;
		this.#lastDeltaTime = undefined;
		this.#ackSeq = 0;
		for (const entry of this.#entries.values()) {
			const kind = schema.byId.get(entry.kind.id);
			if (kind === undefined || !sameKindShape(kind, entry.kind)) {
				this.#entries.delete(entry.id);
				continue;
			}
			if (!entry.stale) {
				entry.stale = true;
				entry.staleSince = time;
			}
		}
	}

	/** Remove every entity. */
	clear(): void {
		this.#entries.clear();
		this.#lastDeltaTime = undefined;
	}

	/** Apply one decoded Delta stamped with its server time. */
	applyDelta(delta: DeltaMessage, time: number): void {
		const schema = this.#schema;
		if (schema === undefined) throw new Error('EntityReplica.applyDelta before reset(schema)');
		const previous = this.#lastDeltaTime;
		for (const id of delta.removes) {
			const entry = this.#entries.get(id);
			if (entry !== undefined && !entry.stale) this.#entries.delete(id);
		}
		for (const create of delta.creates) {
			const kind = schema.byId.get(create.kind);
			if (kind === undefined) continue;
			const existing = this.#entries.get(create.id);
			if (existing !== undefined && existing.stale && existing.kind.id === create.kind) {
				existing.stale = false;
				this.#hold(existing, previous);
				this.#push(existing, time, [...create.fields]);
				continue;
			}
			this.#entries.set(create.id, {
				id: create.id,
				kind,
				modes: this.#modes(kind),
				snapshots: [{ time, values: [...create.fields] }],
				stale: false,
				staleSince: 0
			});
		}
		for (const update of delta.updates) {
			const entry = this.#entries.get(update.id);
			if (entry === undefined || entry.stale) continue;
			const last = entry.snapshots[entry.snapshots.length - 1];
			const values = [...last.values];
			let next = 0;
			for (let i = 0; i < values.length; i += 1) {
				if (maskHas(update.mask, i)) values[i] = update.fields[next++];
			}
			this.#hold(entry, previous);
			this.#push(entry, time, values);
		}
		for (const entry of this.#entries.values()) {
			if (entry.stale && time - entry.staleSince > this.#options.staleGraceMs) {
				this.#entries.delete(entry.id);
			}
		}
		this.#lastDeltaTime = time;
		this.#serverTick = delta.serverTick;
		this.#ackSeq = delta.ackSeq;
	}

	/** Drop stale entities whose grace elapsed at server time `time`. */
	pruneStale(time: number, graceMs = this.#options.staleGraceMs): void {
		for (const entry of this.#entries.values()) {
			if (entry.stale && time - entry.staleSince > graceMs) this.#entries.delete(entry.id);
		}
	}

	has(id: number): boolean {
		const entry = this.#entries.get(id);
		return entry !== undefined && !entry.stale;
	}

	/** Latest authoritative state of a live entity. */
	get(id: number): ReplicaEntity | undefined {
		const entry = this.#entries.get(id);
		if (entry === undefined || entry.stale) return undefined;
		return view(entry, entry.snapshots[entry.snapshots.length - 1].values);
	}

	/** Latest authoritative state of every live (non-stale) entity. */
	entities(): ReplicaEntity[] {
		const out: ReplicaEntity[] = [];
		for (const entry of this.#entries.values()) {
			if (!entry.stale) out.push(view(entry, entry.snapshots[entry.snapshots.length - 1].values));
		}
		return out;
	}

	/** Interpolated state of one entity at server time `renderTime`. */
	sampleEntity(id: number, renderTime: number): ReplicaEntity | undefined {
		const entry = this.#entries.get(id);
		return entry === undefined ? undefined : view(entry, sampleEntry(entry, renderTime));
	}

	/** Interpolated state of every entity (stale ones hold their last value). */
	sample(renderTime: number): ReplicaEntity[] {
		const out: ReplicaEntity[] = [];
		for (const entry of this.#entries.values()) {
			out.push(view(entry, sampleEntry(entry, renderTime)));
		}
		return out;
	}

	/** Snapshot times of an entity (diagnostics/tests). */
	history(id: number): readonly { time: number; fields: EntityFields }[] {
		const entry = this.#entries.get(id);
		if (entry === undefined) return [];
		return entry.snapshots.map((s) => ({ time: s.time, fields: fieldsOf(entry.kind, s.values) }));
	}

	#modes(kind: GameKindDef): InterpolationMode[] {
		const overrides = this.#options.interpolation[kind.name] ?? {};
		return kind.fields.map((f) => overrides[f.name] ?? defaultInterpolationMode(f.type));
	}

	/**
	 * Unchanged fields are not sent, so an entity absent from the previous
	 * Delta was still at its last value then. Insert that "hold" sample so
	 * interpolation does not smear a late change over the quiet interval.
	 */
	#hold(entry: Entry, previous: number | undefined): void {
		if (previous === undefined) return;
		const last = entry.snapshots[entry.snapshots.length - 1];
		if (last.time < previous) this.#push(entry, previous, [...last.values]);
	}

	#push(entry: Entry, time: number, values: FieldValue[]): void {
		const snapshots = entry.snapshots;
		const last = snapshots[snapshots.length - 1];
		if (last !== undefined && time <= last.time) {
			last.values = values;
		} else {
			snapshots.push({ time, values });
		}
		const horizon = time - this.#options.historyMs;
		while (
			snapshots.length > 2 &&
			(snapshots.length > this.#options.maxSnapshots || snapshots[1].time <= horizon)
		) {
			snapshots.shift();
		}
	}
}

function sameKindShape(a: GameKindDef, b: GameKindDef): boolean {
	return (
		a.fields.length === b.fields.length &&
		a.fields.every((f, i) => f.name === b.fields[i].name && f.type === b.fields[i].type)
	);
}

function sampleEntry(entry: Entry, renderTime: number): FieldValue[] {
	const snaps = entry.snapshots;
	const last = snaps[snaps.length - 1];
	if (entry.stale || renderTime >= last.time) return last.values;
	if (renderTime <= snaps[0].time) return snaps[0].values;
	let i = snaps.length - 2;
	while (i > 0 && snaps[i].time > renderTime) i -= 1;
	const a = snaps[i];
	const b = snaps[i + 1];
	const t = (renderTime - a.time) / (b.time - a.time);
	return a.values.map((value, f) => interpolateValue(entry.modes[f], value, b.values[f], t));
}

function fieldsOf(kind: GameKindDef, values: readonly FieldValue[]): EntityFields {
	const fields: Record<string, FieldValue> = {};
	kind.fields.forEach((f, i) => {
		fields[f.name] = values[i];
	});
	return fields;
}

function view(entry: Entry, values: readonly FieldValue[]): ReplicaEntity {
	return {
		id: entry.id,
		kind: entry.kind.id,
		kindName: entry.kind.name,
		fields: fieldsOf(entry.kind, values),
		stale: entry.stale
	};
}
