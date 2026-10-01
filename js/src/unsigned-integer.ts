/** Exact browser-number refinements of Rust unsigned command integers. */
export type UnsignedIntegerCodec = 'uint8' | 'uint16' | 'uint32' | 'uint64_safe_integer';

export function unsignedIntegerMaximum(codec: string | undefined): number | undefined {
	switch (codec) {
		case 'uint8': return 255;
		case 'uint16': return 65_535;
		case 'uint32': return 4_294_967_295;
		case 'uint64_safe_integer': return Number.MAX_SAFE_INTEGER;
		default: return undefined;
	}
}

export function isUnsignedInteger(value: unknown, maximum: number): value is number {
	return typeof value === 'number' && Number.isSafeInteger(value)
		&& !Object.is(value, -0) && value >= 0 && value <= maximum;
}
