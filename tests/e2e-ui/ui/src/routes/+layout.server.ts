import { distributed } from '$lib/server/distributed';
import type { LayoutServerLoad } from './$types';

/**
 * Unauthenticated traffic skips the portable user GraphQL surface (no Bearer /
 * empty identity cannot open e2e-ui). Lobby chat SSR uses the nested public
 * client under routes/chat when there is no session.
 */
export const load: LayoutServerLoad = (async (event) => {
	const session = await event.locals.auth();
	if (!session?.user) {
		return {
			session: null,
			accessToken: null,
			engineRole: null,
			gqlError: null
		};
	}
	return distributed.load(event);
}) satisfies LayoutServerLoad;
