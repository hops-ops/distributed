const encoder = /* @__PURE__ */ new TextEncoder();

/**
 * Order two strings by their UTF-8 bytes, which is how the server orders text.
 * Only identical strings compare equal.
 */
export function compareUtf8(left: string, right: string): -1 | 0 | 1 {
	if (left === right) return 0;
	const leftBytes = encoder.encode(left);
	const rightBytes = encoder.encode(right);
	const length = Math.min(leftBytes.length, rightBytes.length);
	for (let index = 0; index < length; index += 1) {
		const a = leftBytes[index]!;
		const b = rightBytes[index]!;
		if (a !== b) return a < b ? -1 : 1;
	}
	return leftBytes.length < rightBytes.length ? -1 : 1;
}
