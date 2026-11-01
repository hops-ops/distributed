import { distributed, publicGraphqlUrl } from '$lib/server/distributed';

/**
 * Session + the SSR seed of every generated `@load` query on the route
 * (`$distributed`). Signed-out visitors run no GraphQL.
 */
export const load = async (event) => {
	const session = await event.locals.auth();
	if (!session?.user) {
		return { session: null, accessToken: null, engineRole: null, gqlError: null, graphqlUrl: null };
	}
	return { ...(await distributed.load(event)), graphqlUrl: publicGraphqlUrl() };
};
