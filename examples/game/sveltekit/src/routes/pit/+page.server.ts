import { redirect } from '@sveltejs/kit';
import { loadDistributedLeaderboard } from '@hops-ops/distributed/sveltekit';
import { XpLeaderboardLeaderboard, type XpLeaderboardData } from '$distributed';
import { liveContent, roomsUrls, tickets } from '$lib/server/game';
import { graphqlUrl } from '$lib/server/distributed';

export const load = async (event) => {
	const joined = await tickets.resolve(event);
	if (joined === undefined) redirect(303, '/');
	// The XP board is server-ranked (Redis), not a replica query: load it
	// here so the first HTML has it; the component then follows `@live`.
	const session = await event.locals.auth();
	let board: XpLeaderboardData | undefined;
	try {
		board = await loadDistributedLeaderboard<XpLeaderboardData>(XpLeaderboardLeaderboard, { first: 5 }, {
			url: graphqlUrl(),
			auth: { accessToken: session?.accessToken ?? null },
			fetch: event.fetch
		});
	} catch (error) {
		console.error(`leaderboard SSR: ${error}`);
	}
	// The client starts on the world's live content (its packs are built in).
	const content = await liveContent();
	return {
		zone: joined.zone,
		contentVersion: content?.version ?? '',
		roomsUrls: roomsUrls(),
		principal: joined.principal,
		board
	};
};
