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
 * The room `principal` was last in, from its presence cell on celld
 * (`presence/{principal}`: joins, handoffs and departures, written from the
 * rooms' verified lifecycle facts). Server-only: it uses celld's internal
 * secret (`CELLD_URL`, `DISTRIBUTED_INTERNAL_SECRET`). Undefined when the
 * player never played, or celld is not configured or unreachable.
 */
export async function lastRoom(principal: string): Promise<string | undefined> {
	const base = env.CELLD_URL;
	const secret = env.DISTRIBUTED_INTERNAL_SECRET;
	if (!base || !secret) return undefined;
	try {
		const response = await fetch(`${base.replace(/\/$/, '')}/presence/${encodeURIComponent(principal)}`, {
			headers: { 'x-distributed-internal-secret': secret },
			signal: AbortSignal.timeout(2000)
		});
		if (!response.ok) return undefined;
		const body = (await response.json()) as { current?: { zone?: unknown } | null };
		const zone = body.current?.zone;
		return typeof zone === 'string' && isRoom(zone) ? zone : undefined;
	} catch {
		return undefined;
	}
}

/**
 * Room admission: principal from the Auth.js session, the room chosen here —
 * on a page load the room the player was last in (so a reload, a new tab
 * or a restart brings you back where you were; the room keeps your place),
 * else the entrance; the room the client asks for when it rejoins one (a
 * room that restarted closes sockets with 1012). Moving between rooms needs
 * no ticket from here: a doorway hands the player over with a transfer
 * ticket the room itself signs.
 */
export const tickets = createGameTicketHandler({
	secret: () => env.GAME_TICKET_SECRET,
	// The page also watches the 8 rooms next door (one observer ticket each
	// per doorway, retried with back-off while a room sleeps).
	rateLimit: { max: 120, windowMs: 60_000 },
	getSession: (event: RequestEvent) => event.locals.auth(),
	zone: async ({ requested, principal }) => (isRoom(requested) ? requested : ((await lastRoom(principal)) ?? ENTRANCE))
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
