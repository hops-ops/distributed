/** Explicit URL text decoding; the operation codec still validates the result. */
export type DistributedSearchScalar = 'String' | 'ID' | 'Int' | 'Float' | 'Boolean';

export type DistributedSearchParamSource<
	TScalar extends DistributedSearchScalar = DistributedSearchScalar,
	TMode extends 'first' | 'all' = 'first' | 'all'
> = Readonly<{
	kind: 'search_param';
	name: string;
	scalar: TScalar;
	mode?: TMode;
}>;

/** Shared by build-time generation and runtime registration. */
export function normalizeSearchParamSource(
	record: Record<string, unknown>,
	variable: string,
	graphqlType: string
): DistributedSearchParamSource {
	if (
		(Object.getPrototypeOf(record) !== Object.prototype && Object.getPrototypeOf(record) !== null) ||
		!['kind', 'name', 'scalar'].every((key) => Object.hasOwn(record, key))
	) throw new TypeError(`Distributed boundary search source ${variable} requires own scalar fields`);
	const allowed = new Set(['kind', 'name', 'scalar', 'mode']);
	for (const key of Reflect.ownKeys(record)) {
		const property = Object.getOwnPropertyDescriptor(record, key);
		if (typeof key !== 'string' || !allowed.has(key) || property === undefined || !('value' in property)) {
			throw new TypeError(`Distributed boundary search source ${variable} has an invalid field`);
		}
	}
	const { name, scalar, mode = 'first' } = record;
	if (
		typeof name !== 'string' || name.length === 0 || name.length > 512 ||
		!['String', 'ID', 'Int', 'Float', 'Boolean'].includes(scalar as string) ||
		(mode !== 'first' && mode !== 'all')
	) {
		throw new TypeError(`Distributed boundary search source ${variable} requires a scalar and valid mode`);
	}
	const expected = mode === 'all' ? `[${scalar}]` : scalar;
	if (graphqlType.replaceAll('!', '') !== expected) {
		throw new TypeError(`Distributed boundary search source ${variable} does not match its GraphQL type`);
	}
	return Object.freeze({
		kind: 'search_param', name, scalar: scalar as DistributedSearchScalar, mode
	});
}

export function decodeSearchScalar(
	value: unknown,
	scalar: DistributedSearchScalar
): string | number | boolean {
	// Do not trim, parse partial numbers, infer JSON, or include URL values in errors.
	if (typeof value !== 'string') throw new TypeError('Distributed search scalar requires URL text');
	if (scalar === 'String' || scalar === 'ID') return value;
	if (scalar === 'Boolean') {
		if (value === 'true') return true;
		if (value === 'false') return false;
	} else if (
		(scalar === 'Int' && /^-?(?:0|[1-9][0-9]*)$/.test(value)) ||
		(scalar === 'Float' && /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$/.test(value))
	) {
		const number = Number(value);
		if (Number.isFinite(number)) return number;
	}
	throw new TypeError(`Distributed search scalar is not a valid ${scalar}`);
}
