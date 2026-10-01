/**
 * Require a signed-in Auth.js session or 303 to /login with callbackUrl.
 * Used from page/layout loads so client-side navigations still gate
 * (hooks alone miss routes with no server load when the root layout is cached).
 */
import { redirect } from '@sveltejs/kit';

type AuthSession = { user?: unknown; error?: string; expiresAt?: number };
type AuthLocals = {
	auth: () => Promise<AuthSession | null>;
};

/** Share the current-session check between protected UI and refresh/API routes. */
export function isCurrentSession(session: AuthSession | null): session is AuthSession & { user: NonNullable<unknown> } {
	return !!session?.user && !session.error &&
		(session.expiresAt === undefined || session.expiresAt > Date.now() / 1000);
}

export async function requireAuth(
	event: { locals: AuthLocals; url: URL },
	options?: { fallbackPath?: string }
): Promise<NonNullable<Awaited<ReturnType<AuthLocals['auth']>>>> {
	const session = await event.locals.auth();
	if (isCurrentSession(session)) {
		return session;
	}

	const path = event.url.pathname + event.url.search;
	const callbackUrl = encodeURIComponent(path || options?.fallbackPath || '/');
	redirect(303, `/login?callbackUrl=${callbackUrl}`);
}
