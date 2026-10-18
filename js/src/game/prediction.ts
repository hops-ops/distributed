/**
 * Client-side prediction and server reconciliation for the locally
 * controlled entity (`PROTOCOL.md` §7.1 `ack_seq`).
 *
 * Every sent input is applied immediately through the app's `predict` and
 * kept until the server acknowledges it. On each authoritative Delta the
 * predictor rebases: authoritative state + replay of inputs with
 * `seq > ack_seq`. Any difference between what was displayed and the rebased
 * prediction is resolved per field:
 *
 * - `ease`    – adopt the rebased value but carry the visual error as an
 *               offset that decays linearly to zero over `durationMs`.
 * - `snap`    – adopt the rebased value immediately.
 * - `replace` – the field is server-owned: show the authoritative value as
 *               is, never replaying inputs over it (hp, score, …).
 */
import { angleDelta, wrapAngle, type EntityFields } from './replica.js';
import { canonicalizeField, type FieldValue, type GameKindDef } from './schema.js';

export type CorrectionPolicy =
	| Readonly<{ mode: 'ease'; durationMs?: number }>
	| Readonly<{ mode: 'snap' }>
	| Readonly<{ mode: 'replace' }>;

/** Per kind name → per field name → policy. */
export type CorrectionPolicies = Readonly<
	Record<string, Readonly<Record<string, CorrectionPolicy>>>
>;

export type PredictFn<TInput> = (
	state: EntityFields,
	input: TInput,
	dtMs: number
) => Readonly<Record<string, FieldValue>>;

export type PredictorOptions<TInput> = Readonly<{
	predict: PredictFn<TInput>;
	corrections?: CorrectionPolicies;
	/** Policy for fields not listed. Default `{ mode: 'ease', durationMs: 100 }`. */
	defaultCorrection?: CorrectionPolicy;
	/** Inputs kept unacknowledged before the oldest is dropped. Default 256. */
	maxPending?: number;
	/**
	 * Round every predicted step through the field's wire quantization.
	 * Enable only when the server simulation itself canonicalizes state each
	 * tick (`FieldType::canonicalize`); otherwise the server keeps full
	 * precision and per-step rounding would drift. Default false.
	 */
	canonicalize?: boolean;
	/** Differences at or below this are ignored (no correction). Default 1e-6. */
	epsilon?: number;
}>;

export type PendingInput<TInput> = Readonly<{
	seq: number;
	input: TInput;
	dtMs: number;
}>;

/** Divergence observed by one reconciliation (predicted − rebased). */
export type Correction = Readonly<{
	ackSeq: number;
	replayed: number;
	/** Per numeric field: previous prediction minus rebased prediction. */
	errors: Readonly<Record<string, number>>;
	/** Fields whose prediction diverged and how each was resolved. */
	resolved: Readonly<Record<string, CorrectionPolicy['mode']>>;
}>;

type Offset = { value: number; start: number; durationMs: number; angle: boolean };

const DEFAULT_CORRECTION: CorrectionPolicy = Object.freeze({ mode: 'ease', durationMs: 100 });

export class Predictor<TInput> {
	readonly #options: PredictorOptions<TInput>;
	#kind: GameKindDef | undefined;
	#entityId: number | undefined;
	#pending: PendingInput<TInput>[] = [];
	#authoritative: Record<string, FieldValue> | undefined;
	#predicted: Record<string, FieldValue> | undefined;
	#offsets = new Map<string, Offset>();
	#lastAck = 0;

	constructor(options: PredictorOptions<TInput>) {
		this.#options = options;
	}

	get entityId(): number | undefined {
		return this.#entityId;
	}

	get kind(): GameKindDef | undefined {
		return this.#kind;
	}

	get pending(): readonly PendingInput<TInput>[] {
		return this.#pending;
	}

	get lastAck(): number {
		return this.#lastAck;
	}

	/** True when an entity is bound and its authoritative state is known. */
	get active(): boolean {
		return this.#predicted !== undefined;
	}

	/**
	 * Bind (or rebind) the controlled entity. Clears pending inputs: they
	 * belonged to the previous binding or session.
	 */
	bind(entityId: number | undefined, kind?: GameKindDef, authoritative?: EntityFields): void {
		this.#entityId = entityId;
		this.#kind = entityId === undefined ? undefined : kind;
		this.#pending = [];
		this.#offsets.clear();
		this.#lastAck = 0;
		this.#authoritative = authoritative === undefined ? undefined : { ...authoritative };
		this.#predicted = authoritative === undefined ? undefined : { ...authoritative };
	}

	/** New session (Welcome): sequence numbers restart, pending inputs are void. */
	resetSession(): void {
		this.#pending = [];
		this.#lastAck = 0;
	}

	/**
	 * Record an input that was sent with `seq` and apply it to the
	 * prediction. Inputs recorded before authoritative state is known are
	 * kept and replayed once it arrives.
	 */
	record(seq: number, input: TInput, dtMs: number): void {
		this.#pending.push({ seq, input, dtMs });
		const max = this.#options.maxPending ?? 256;
		if (this.#pending.length > max) this.#pending.splice(0, this.#pending.length - max);
		if (this.#predicted !== undefined) {
			this.#predicted = this.#step(this.#predicted, input, dtMs);
		}
	}

	/**
	 * Rebase onto authoritative state after a Delta. `authoritative` is the
	 * entity's latest server fields (or undefined if it is not visible).
	 */
	reconcile(ackSeq: number, authoritative: EntityFields | undefined, now: number): Correction | undefined {
		if (ackSeq > this.#lastAck) this.#lastAck = ackSeq;
		this.#pending = this.#pending.filter((p) => p.seq > this.#lastAck);
		if (authoritative === undefined || this.#kind === undefined) return undefined;
		const before = this.#predicted === undefined ? undefined : this.current(now);
		const previous = this.#predicted;
		this.#authoritative = { ...authoritative };

		let rebased: Record<string, FieldValue> = { ...authoritative };
		for (const p of this.#pending) rebased = this.#step(rebased, p.input, p.dtMs);

		const errors: Record<string, number> = {};
		const resolved: Record<string, CorrectionPolicy['mode']> = {};
		const epsilon = this.#options.epsilon ?? 1e-6;
		for (const field of this.#kind.fields) {
			const name = field.name;
			const policy = this.#policy(name);
			const angle = field.type === 'angle';
			if (policy.mode === 'replace') {
				rebased[name] = authoritative[name];
				this.#offsets.delete(name);
			}
			if (previous === undefined || before === undefined) continue;
			const oldValue = previous[name];
			const newValue = rebased[name];
			if (typeof oldValue === 'number' && typeof newValue === 'number') {
				const error = angle ? -angleDelta(oldValue, newValue) : oldValue - newValue;
				if (Math.abs(error) > epsilon) {
					errors[name] = error;
					resolved[name] = policy.mode;
				}
				if (policy.mode === 'ease') {
					const shown = before[name];
					const visual =
						typeof shown === 'number'
							? angle
								? -angleDelta(shown, newValue)
								: shown - newValue
							: 0;
					const duration = policy.durationMs ?? 100;
					if (Math.abs(visual) > epsilon && duration > 0) {
						this.#offsets.set(name, { value: visual, start: now, durationMs: duration, angle });
					} else {
						this.#offsets.delete(name);
					}
				} else {
					this.#offsets.delete(name);
				}
			} else if (!sameValue(oldValue, newValue)) {
				resolved[name] = policy.mode === 'ease' ? 'snap' : policy.mode;
			}
		}
		this.#predicted = rebased;
		return { ackSeq: this.#lastAck, replayed: this.#pending.length, errors, resolved };
	}

	/** Latest authoritative fields seen for the controlled entity. */
	authoritative(): EntityFields | undefined {
		return this.#authoritative;
	}

	/** Prediction without visual smoothing. */
	predicted(): EntityFields | undefined {
		return this.#predicted;
	}

	/** Prediction with decaying correction offsets applied (what to render). */
	current(now: number): EntityFields | undefined {
		const predicted = this.#predicted;
		if (predicted === undefined) return undefined;
		if (this.#offsets.size === 0) return predicted;
		const out: Record<string, FieldValue> = { ...predicted };
		for (const [name, offset] of this.#offsets) {
			const k = 1 - (now - offset.start) / offset.durationMs;
			const value = predicted[name];
			if (k <= 0 || typeof value !== 'number') {
				this.#offsets.delete(name);
				continue;
			}
			const shifted = value + offset.value * Math.min(1, k);
			out[name] = offset.angle ? wrapAngle(shifted) : shifted;
		}
		return out;
	}

	/** Remaining visual correction per field (diagnostics/tests). */
	visualError(now: number): Readonly<Record<string, number>> {
		const out: Record<string, number> = {};
		for (const [name, offset] of this.#offsets) {
			const k = 1 - (now - offset.start) / offset.durationMs;
			if (k > 0) out[name] = offset.value * Math.min(1, k);
		}
		return out;
	}

	#policy(field: string): CorrectionPolicy {
		const kindName = this.#kind?.name ?? '';
		return (
			this.#options.corrections?.[kindName]?.[field] ??
			this.#options.defaultCorrection ??
			DEFAULT_CORRECTION
		);
	}

	#step(state: Record<string, FieldValue>, input: TInput, dtMs: number): Record<string, FieldValue> {
		const next = { ...state, ...this.#options.predict(state, input, dtMs) };
		const kind = this.#kind;
		if (kind !== undefined) {
			for (const field of kind.fields) {
				if (this.#policy(field.name).mode === 'replace') {
					next[field.name] = state[field.name];
				} else if (this.#options.canonicalize ?? false) {
					next[field.name] = canonicalizeField(field, next[field.name]);
				}
			}
		}
		return next;
	}
}

function sameValue(a: FieldValue | undefined, b: FieldValue | undefined): boolean {
	if (a instanceof Uint8Array && b instanceof Uint8Array) {
		return a.length === b.length && a.every((v, i) => v === b[i]);
	}
	return a === b;
}
