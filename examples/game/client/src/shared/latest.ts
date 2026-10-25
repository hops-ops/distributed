/**
 * The latest value of one typed event, plus a promise for the first one —
 * e.g. a zone's `hello` (own entity id) that arrives right after joining:
 *
 * ```ts
 * const hello = latest<pit.HelloEvent>((h) => typed.on('hello', h));
 * ```
 */
export type Latest<T> = Readonly<{
	/** Latest value; undefined until the first event. */
	get(): T | undefined;
	/** Resolves on the first event. */
	ready: Promise<T>;
}>;

export function latest<T>(
	subscribe: (handler: (value: T) => void) => unknown,
	onValue?: (value: T) => void
): Latest<T> {
	let value: T | undefined;
	let resolve: (v: T) => void = () => undefined;
	const ready = new Promise<T>((r) => (resolve = r));
	subscribe((v) => {
		value = v;
		onValue?.(v);
		resolve(v);
	});
	return { get: () => value, ready };
}
