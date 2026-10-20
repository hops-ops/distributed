import { env } from '$env/dynamic/private';
import type { RequestEvent } from '@sveltejs/kit';
import { createGameTicketHandler } from '@hops-ops/distributed/sveltekit/game/server';

/** Zone admission: principal from the Auth.js session, zone chosen here. */
export const tickets = createGameTicketHandler({
	secret: () => env.GAME_TICKET_SECRET,
	getSession: (event: RequestEvent) => event.locals.auth(),
	// One shared pit; a real game picks the zone the player is in or owns.
	zone: () => env.PIT_ZONE ?? 'pit-1'
});
