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

export type BoardRow = { rank: number; member: string; name: string; score: number };
export type SettledFacts = {
	board: { kills: BoardRow[]; xp: BoardRow[] };
	me: { kills: { score: number | null }; xp: { score: number | null } } | null;
};

/**
 * Read model of settled kills for the first render: the Mob Pit
 * leaderboard, a projection of the durable Crawler cells (dev HTTP route of
 * the game server, read server-side with the session's principal). Live
 * updates then arrive as the zone's `profile` and `board` events.
 */
export async function settledFacts(principal: string): Promise<SettledFacts | null> {
	const base = env.GAME_SERVER_HTTP ?? 'http://127.0.0.1:2967';
	try {
		const response = await fetch(`${base}/dev/mobpit/leaderboard?member=${encodeURIComponent(principal)}`, {
			signal: AbortSignal.timeout(2_000)
		});
		if (!response.ok) return null;
		const { snapshot, me } = (await response.json()) as { snapshot: SettledFacts['board']; me: SettledFacts['me'] };
		return { board: { kills: snapshot.kills, xp: snapshot.xp }, me };
	} catch {
		return null;
	}
}
