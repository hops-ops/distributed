/** Filesystem helpers for build-time (Node) modules only. */
import { isAbsolute, relative, sep } from 'node:path';

/** True when `target` is `root` or lies beneath it. */
export function isWithin(root: string, target: string): boolean {
	const path = relative(root, target);
	return (
		path === '' ||
		(!path.startsWith(`..${sep}`) && path !== '..' && !isAbsolute(path))
	);
}

export function isMissing(error: unknown): boolean {
	return (
		error !== null &&
		typeof error === 'object' &&
		'code' in error &&
		error.code === 'ENOENT'
	);
}
