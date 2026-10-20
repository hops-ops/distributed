import { redirect } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { tickets } from '$lib/server/game';

export const load = async (event) => {
	const joined = await tickets.resolve(event);
	if (joined === undefined) redirect(303, '/');
	return {
		zone: joined.zone,
		zonesUrl: env.GAME_ZONES_URL ?? 'ws://127.0.0.1:2967',
		principal: joined.principal
	};
};
