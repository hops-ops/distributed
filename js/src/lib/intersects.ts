export function intersects(
	left: readonly string[],
	right: ReadonlySet<string>
): boolean {
	return left.some((value) => right.has(value));
}
