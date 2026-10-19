import { defineGraphqlIslandBindings, sessionClaim } from '@hops-ops/distributed/sveltekit';

// The principal comes from the session only (`session.user.id`, the Auth.js
// verified subject) — the same id the zone ticket and the read model use.
export default defineGraphqlIslandBindings({
	playerId: sessionClaim('user', 'id')
});
