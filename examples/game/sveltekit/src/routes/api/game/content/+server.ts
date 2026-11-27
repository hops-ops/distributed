/**
 * The world's live content version (signed in): what a client reloads to
 * when a room told it its content is out of date without naming a version
 * (a fresh join on old content).
 */
import { error, json } from '@sveltejs/kit';
import { liveContent } from '$lib/server/game';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ locals }) => {
	if (!(await locals.auth())?.user) error(401, 'sign in first');
	return json({ version: (await liveContent())?.version ?? '' });
};
