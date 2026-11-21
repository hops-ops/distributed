import {
	createDistributedSvelteKitServer,
	type SveltekitServerLoadEventLike
} from '@hops-ops/distributed/sveltekit';

import { DISTRIBUTED_BOUNDARY_OPERATIONS } from '$distributed';
import { engineRoleFromGroups } from '$lib/roles';
import { graphqlHttpUrl } from '$lib/server/graphql';

type LoadEvent = SveltekitServerLoadEventLike<App.Locals>;
type Session = NonNullable<
	Awaited<ReturnType<LoadEvent['locals']['auth']>>
>;

/**
 * One root loader owns every compiler-discovered user-safe `@load` operation.
 * A fresh replica is created per request and no GraphQL work runs for routes
 * absent from the generated registry.
 */
export const distributed = createDistributedSvelteKitServer<Session, LoadEvent>({
	boundaries: DISTRIBUTED_BOUNDARY_OPERATIONS,
	getSession: (event) => event.locals.auth(),
	getRole: (session) => engineRoleFromGroups(session?.user?.groups),
	getUrl: graphqlHttpUrl
});
