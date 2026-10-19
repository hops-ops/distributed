/**
 * Auth.js, the session mechanism the framework's SvelteKit apps use
 * (`tests/e2e-ui`): `locals.auth()` returns the verified session from an
 * encrypted, HttpOnly cookie. Real deployments configure the OIDC provider
 * (`OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`); local dev and the
 * smoke test enable a **dev-only** username sign-in with `GAME_DEV_AUTH=1`.
 * Either way the game ticket route only ever reads `session.user.id`, and
 * `session.accessToken` (the read-model service's Bearer) is bound to the
 * same verified subject.
 */
import { SvelteKitAuth, type SvelteKitAuthConfig } from '@auth/sveltekit';
import Credentials from '@auth/sveltekit/providers/credentials';
import { env } from '$env/dynamic/private';
import { mintApiToken } from '$lib/server/api-token';

const USERNAME = /^[a-z][a-z0-9_-]{2,15}$/;

export const { handle, signIn, signOut } = SvelteKitAuth(async (): Promise<SvelteKitAuthConfig> => ({
	trustHost: true,
	secret: env.AUTH_SECRET,
	session: { strategy: 'jwt' },
	pages: { signIn: '/' },
	providers: [
		...(env.OIDC_ISSUER
			? [
					{
						id: 'oidc',
						name: 'OIDC',
						type: 'oidc' as const,
						issuer: env.OIDC_ISSUER,
						clientId: env.OIDC_CLIENT_ID,
						clientSecret: env.OIDC_CLIENT_SECRET,
						checks: ['pkce' as const, 'state' as const, 'nonce' as const]
					}
				]
			: []),
		...(env.GAME_DEV_AUTH === '1'
			? [
					Credentials({
						id: 'dev',
						name: 'Dev sign-in (local only)',
						credentials: { username: { label: 'Username' } },
						authorize(credentials) {
							const username = String(credentials?.username ?? '').trim().toLowerCase();
							return USERNAME.test(username) ? { id: `dev-${username}`, name: username } : null;
						}
					})
				]
			: [])
	],
	callbacks: {
		jwt({ token, account }) {
			// OIDC: keep the provider's access token for the API (server-side
			// encrypted cookie; only this server reads it).
			if (account?.access_token) token.accessToken = account.access_token;
			return token;
		},
		async session({ session, token }) {
			// The verified subject is the game principal.
			if (session.user && typeof token.sub === 'string') session.user.id = token.sub;
			// The read-model API Bearer: minted here for that subject when this
			// server holds the signing key, else the provider's access token.
			const minted = typeof token.sub === 'string' ? await mintApiToken(token.sub) : undefined;
			const accessToken = minted ?? (typeof token.accessToken === 'string' ? token.accessToken : undefined);
			return accessToken === undefined ? session : { ...session, accessToken };
		}
	}
}));
