import { json, type RequestEvent, type RequestHandler } from '@sveltejs/kit';
import { isCurrentSession } from '$lib/server/require-auth';

type CurrentSession = NonNullable<Awaited<ReturnType<App.Locals['auth']>>>;
type PageDataLoader = (event: RequestEvent, session: CurrentSession) => Promise<unknown>;

/** Share auth refresh independently of an application's optional GraphQL loader. */
export function createAuthRefreshHandler(loadPageData?: PageDataLoader): RequestHandler {
	return async (event) => {
		const session = await event.locals.auth();
		if (!isCurrentSession(session)) {
			return json({ authenticated: false, error: session?.error }, { status: 401 });
		}

		const pageData = await loadPageData?.(event, session);
		return json({
			pageData,
			authenticated: true,
			expires: session.expires,
			expiresAt: session.expiresAt,
			refreshAfter: session.refreshAfter,
			hasAccessToken: session.hasAccessToken,
			hasRefreshToken: session.hasRefreshToken,
			hasIdToken: session.hasIdToken,
			error: session.error
		}, { headers: { 'cache-control': 'no-store' } });
	};
}
