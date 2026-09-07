import { json } from '@sveltejs/kit';
import { isCurrentSession } from '$lib/server/require-auth';
import type { RequestHandler } from './$types';

export const POST: RequestHandler = async ({ locals }) => {
	const session = await locals.auth();

	if (!isCurrentSession(session)) {
		return json({ authenticated: false, error: session?.error }, { status: 401 });
	}

	return json({
		authenticated: true,
		expires: session.expires,
		expiresAt: session.expiresAt,
		refreshAfter: session.refreshAfter,
		hasAccessToken: session.hasAccessToken,
		hasRefreshToken: session.hasRefreshToken,
		hasIdToken: session.hasIdToken,
		error: session.error
	});
};
