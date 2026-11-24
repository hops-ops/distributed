/** 64-bit FNV-1a of the UTF-8 bytes of `value`, as 16 hex digits. */
export function fnv1a64(value: string): string {
	let hash = 0xcbf29ce484222325n;
	for (const byte of new TextEncoder().encode(value)) {
		hash ^= BigInt(byte);
		hash = BigInt.asUintN(64, hash * 0x100000001b3n);
	}
	return hash.toString(16).padStart(16, '0');
}
