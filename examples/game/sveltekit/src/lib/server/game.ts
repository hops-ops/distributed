import { env } from '$env/dynamic/private';
import type { RequestEvent } from '@sveltejs/kit';
import { createGameTicketHandler } from '@hops-ops/distributed/sveltekit/game/server';

/** The room every player starts in. */
export const ENTRANCE = 'pit.0.0';

/** `pit.<x>.<y>` with canonical integers (the rooms' grid). */
export function isRoom(zone: string | undefined): zone is string {
	const m = zone === undefined ? null : /^pit\.(-?\d+)\.(-?\d+)$/.exec(zone);
	return m !== null && m.slice(1).every((v) => String(Number(v)) === v && Math.abs(Number(v)) <= 1_000_000);
}

/**
 * Room admission: principal from the Auth.js session, the room chosen here —
 * the entrance for a fresh join; the room the client asks for when it
 * rejoins one (a room that restarted closes sockets with 1012, a reload).
 * Moving between rooms needs no ticket from here: a doorway hands the player
 * over with a transfer ticket the room itself signs.
 */
export const tickets = createGameTicketHandler({
	secret: () => env.GAME_TICKET_SECRET,
	getSession: (event: RequestEvent) => event.locals.auth(),
	zone: ({ requested }) => (isRoom(requested) ? requested : ENTRANCE)
});

/**
 * Where browsers reach the rooms: celld's public listeners (`ws://…`,
 * comma separated for a local fleet; a load balancer in production). The
 * page rotates over them per connection — an entry point, never a
 * placement: celld forwards every socket to the node that runs the room.
 */
export function roomsUrls(): string[] {
	return (env.GAME_ZONES_URL ?? 'ws://127.0.0.1:2970')
		.split(',')
		.map((u) => u.trim())
		.filter((u) => u.length > 0);
}
