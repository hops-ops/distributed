import { env } from '$env/dynamic/private';
import { createDistributedSvelteKitServer, type SveltekitServerLoadEventLike } from '@hops-ops/distributed/sveltekit';
import { DISTRIBUTED_BOUNDARY_OPERATIONS } from '$distributed';

type LoadEvent = SveltekitServerLoadEventLike<App.Locals>;
type Session = NonNullable<Awaited<ReturnType<LoadEvent['locals']['auth']>>>;

/** `mobpit-service` GraphQL endpoint as this server reaches it. */
export function graphqlUrl(): string {
	if (env.MOBPIT_GRAPHQL_URL) return env.MOBPIT_GRAPHQL_URL;
	// `distributed dev` hands the UI its API origin.
	if (env.DISTRIBUTED_API_ORIGIN) return `${env.DISTRIBUTED_API_ORIGIN.replace(/\/$/, '')}/graphql`;
	return 'http://127.0.0.1:8791/graphql';
}

/** The same endpoint as the browser reaches it (HTTP + `/graphql/ws` for `@live`). */
export function publicGraphqlUrl(): string {
	return env.MOBPIT_PUBLIC_GRAPHQL_URL || graphqlUrl();
}

/**
 * One root loader for every generated `@load` operation (SSR). The Bearer
 * token comes from the verified session (`session.accessToken`, see
 * `auth.ts`); session-bound variables (`sessionClaim`) read the same session.
 */
export const distributed = createDistributedSvelteKitServer<Session, LoadEvent>({
	boundaries: DISTRIBUTED_BOUNDARY_OPERATIONS,
	getSession: (event) => event.locals.auth(),
	getRole: () => 'user',
	getUrl: graphqlUrl
});
