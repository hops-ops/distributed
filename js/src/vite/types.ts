/**
 * Shapes shared by the Vite modules: compiler invocations and structural Vite
 * and Rollup types (Vite is never a runtime dependency). No Node types, so
 * public declarations that use them load without `@types/node`.
 */

/** The compiler invocation: `command ...commandArgs <args>` in `cwd`. */
export type CompilerCommand = Readonly<{
	cwd: string;
	command: string;
	commandArgs: readonly string[];
}>;

/** A Cargo-backed manifest source: an existing JSON path, or compiler argv. */
export type CompilerManifestSource = string | Readonly<{ args: readonly string[] }>;

export type ViteWebSocketLike = Readonly<{
	send(payload: unknown): void;
	/** Vite's `server.ws.on('connection', …)` (optional in test doubles). */
	on?(event: 'connection', listener: () => void): void;
}>;

export type ViteModuleGraphLike = Readonly<{
	getModuleById(id: string): unknown;
	invalidateModule(module: unknown): void;
}>;

export type ViteMiddlewareRequest = Readonly<{
	method?: string;
	headers: Readonly<Record<string, string | readonly string[] | undefined>>;
	on(event: 'data', listener: (chunk: Uint8Array) => void): void;
	on(event: 'end' | 'error', listener: (value?: unknown) => void): void;
}>;

export type ViteMiddlewareResponse = {
	statusCode: number;
	setHeader(name: string, value: string): void;
	end(body?: string | Uint8Array): void;
};

export type ViteServerLike = Readonly<{
	watcher: Readonly<{ add(paths: string | readonly string[]): void }>;
	ws: ViteWebSocketLike;
	moduleGraph: ViteModuleGraphLike;
	middlewares?: Readonly<{
		use(
			path: string,
			handler: (request: ViteMiddlewareRequest, response: ViteMiddlewareResponse) => void
		): void;
	}>;
	httpServer?:
		| Readonly<{
				once(event: 'close', listener: () => void): void;
		  }>
		| null;
}>;

export type ViteHotContextLike = Readonly<{
	file: string;
	server: ViteServerLike;
	modules?: readonly unknown[];
}>;

export type RollupWatchContextLike = Readonly<{
	addWatchFile(path: string): void;
}>;


export type LifecycleHtmlTag = Readonly<{
	tag: 'meta';
	attrs: Readonly<{ name: string; content: string }>;
	injectTo: 'head-prepend';
}>;
