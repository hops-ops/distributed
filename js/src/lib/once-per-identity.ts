/**
 * Return what `compute` made of this object the first time, computing it
 * only then.
 *
 * For generated artifacts: they are immutable constants, so validating the
 * same object again cannot find anything new. A throw is not remembered, and
 * a value that is not an object is computed every time.
 */
export function oncePerIdentity<T, R>(
	results: WeakMap<object, R>,
	value: T,
	compute: (value: T) => R
): R {
	if (value === null || typeof value !== 'object') return compute(value);
	if (results.has(value)) return results.get(value) as R;
	const result = compute(value);
	results.set(value, result);
	return result;
}
