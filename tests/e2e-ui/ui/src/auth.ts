import { SvelteKitAuth } from '@auth/sveltekit';
import type { Handle } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { cleanEnvValue } from '$lib/clean-env';
import { oidcAudience, oidcScopes } from '$lib/server/oidc-scopes';

declare module '@auth/sveltekit' {
	interface Session {
		accessToken?: string;
		idToken?: string;
		expiresAt?: number;
		refreshAfter?: number;
		hasAccessToken?: boolean;
		hasRefreshToken?: boolean;
		hasIdToken?: boolean;
		error?: string;
	}

	interface User {
		id?: string;
		groups?: string[];
		username?: string;
		emailVerified?: boolean;
	}
}

/**
 * Where we look for role keys after Zitadel asserts them on the token.
 * Zitadel term is **project roles** (role keys), not IdP "groups" — we map those
 * keys into `session.user.groups` for the rest of the app.
 *
 * Claims (object whose keys are role names, e.g. `{ "admin": { "<orgId>": "…" } }`):
 * - `urn:zitadel:iam:org:project:roles` — current project
 * - `urn:zitadel:iam:org:project:{projectId}:roles` — explicit project id
 * - `urn:zitadel:iam:org:projects:roles` — all projects
 */
const DEFAULT_GROUP_CLAIMS = [
	'groups',
	'roles',
	'urn:zitadel:iam:org:project:roles',
	'urn:zitadel:iam:org:projects:roles'
];
const ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 60;
const TOKEN_AUTH_BASIC = 'client_secret_basic';
const TOKEN_AUTH_POST = 'client_secret_post';
const TOKEN_AUTH_NONE = 'none';

type TokenRecord = Record<string, unknown>;
type SessionCookieSameSite = 'lax' | 'strict';
const DEFAULT_SESSION_COOKIE_SAME_SITE: SessionCookieSameSite = 'lax';

function envFirst(names: string[], fallback = '') {
	for (const name of names) {
		const value = cleanEnvValue(env[name]);
		if (value) return value;
	}

	return fallback;
}

function envCsv(name: string, fallback: string[]) {
	const value = env[name]?.trim();
	if (!value) return fallback;

	const items = value
		.split(',')
		.map((item) => item.trim())
		.filter(Boolean);

	return items.length ? items : fallback;
}

function oidcIssuer() {
	return envFirst(['OIDC_ISSUER', 'ZITADEL_ISSUER']).replace(/\/+$/, '');
}

function oidcClientId() {
	return envFirst(['OIDC_CLIENT_ID', 'ZITADEL_CLIENT_ID']);
}

function oidcClientSecret() {
	return envFirst(['OIDC_CLIENT_SECRET', 'ZITADEL_CLIENT_SECRET']);
}

function oidcTokenAuthMethod() {
	return envFirst(['OIDC_TOKEN_AUTH_METHOD'], TOKEN_AUTH_BASIC).toLowerCase();
}

function authSessionCookieSameSite(): SessionCookieSameSite {
	const value = env.AUTH_SESSION_COOKIE_SAME_SITE?.trim().toLowerCase();
	if (value === 'lax' || value === 'strict') return value;

	if (value) {
		console.warn('AUTH_SESSION_COOKIE_SAME_SITE must be lax or strict; defaulting to lax');
	}

	// Lax (not strict): OIDC return from Zitadel is a top-level nav; lax is enough
	// and avoids edge cases with local multi-origin setups.
	return 'lax';
}

/**
 * Local e2e is plain http://127.0.0.1 — Secure cookies are dropped by the browser
 * and login/session silently fails (every protected page looks "broken").
 * Only enable Secure when AUTH_URL / AUTH_TRUST_SECURE explicitly says https.
 */
function useSecureCookies(): boolean {
	const forced = env.AUTH_USE_SECURE_COOKIES?.trim().toLowerCase();
	if (forced === '1' || forced === 'true' || forced === 'yes') return true;
	if (forced === '0' || forced === 'false' || forced === 'no') return false;

	const authUrl = envFirst(['AUTH_URL', 'ORIGIN', 'PUBLIC_ORIGIN']);
	if (authUrl.startsWith('https://')) return true;
	// Default: local fixture is HTTP
	return false;
}

function providerName() {
	if (env.AUTH_PROVIDER?.trim()) return env.AUTH_PROVIDER.trim();
	if (env.ZITADEL_ISSUER?.trim()) return 'Zitadel';
	return 'OIDC';
}

function decodeJwtPayload(jwt: unknown): Record<string, unknown> | null {
	if (typeof jwt !== 'string') return null;

	const payload = jwt.split('.')[1];
	if (!payload) return null;

	try {
		const normalized = payload.replace(/-/g, '+').replace(/_/g, '/');
		const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=');
		const binary = globalThis.atob(padded);
		const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
		return JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
	} catch {
		return null;
	}
}

function claimString(claims: Record<string, unknown>, key: string) {
	const value = claims[key];
	return typeof value === 'string' && value.trim() ? value : undefined;
}

function claimBool(claims: Record<string, unknown>, key: string) {
	const value = claims[key];
	return typeof value === 'boolean' ? value : undefined;
}

function claimValue(claims: Record<string, unknown>, path: string): unknown {
	if (path in claims) return claims[path];

	return path.split('.').reduce<unknown>((current, segment) => {
		if (current && typeof current === 'object' && segment in current) {
			return (current as Record<string, unknown>)[segment];
		}

		return undefined;
	}, claims);
}

function extractGroups(claims: Record<string, unknown>, groupClaims: string[]) {
	const groups = new Set<string>();

	for (const claim of groupClaims) {
		const value = claimValue(claims, claim);

		if (Array.isArray(value)) {
			for (const item of value) {
				if (typeof item === 'string' && item.trim()) groups.add(item);
			}
		} else if (typeof value === 'string' && value.trim()) {
			groups.add(value);
		} else if (value && typeof value === 'object') {
			// Zitadel project roles: { "admin": { "<projectId>": "..." }, ... }
			for (const key of Object.keys(value)) groups.add(key);
		}
	}

	return [...groups].sort();
}

function groupClaimPaths() {
	const paths = envCsv('OIDC_GROUP_CLAIMS', DEFAULT_GROUP_CLAIMS);
	const aud = oidcAudience() || envFirst(['OIDC_AUDIENCE', 'ZITADEL_PROJECT_ID']);
	if (aud) {
		const projectClaim = `urn:zitadel:iam:org:project:${aud}:roles`;
		const projectIdClaim = `urn:zitadel:iam:org:project:id:${aud}:roles`;
		if (!paths.includes(projectClaim)) paths.push(projectClaim);
		if (!paths.includes(projectIdClaim)) paths.push(projectIdClaim);
	}
	return paths;
}

/**
 * Roles often live on the **access** token (Zitadel `urn:zitadel:iam:org:project:roles`)
 * while the id_token only has profile claims. Merge both payloads so admin grants
 * are not dropped when an id_token is present.
 */
function groupsFromTokens(token: TokenRecord): string[] {
	const paths = groupClaimPaths();
	const groups = new Set<string>();

	for (const jwt of [token.idToken, token.accessToken, token.id_token, token.access_token]) {
		const claims = decodeJwtPayload(jwt);
		if (!claims) continue;
		for (const g of extractGroups(claims, paths)) groups.add(g);
	}

	// Auth.js may have stored profile groups on the JWT cookie (first sign-in).
	const stored = token.groups;
	if (Array.isArray(stored)) {
		for (const item of stored) {
			if (typeof item === 'string' && item.trim()) groups.add(item);
		}
	}

	return [...groups].sort();
}

function userClaims(token: TokenRecord) {
	// Profile fields: prefer id_token, fall back to access token.
	return decodeJwtPayload(token.idToken) ?? decodeJwtPayload(token.accessToken) ?? {};
}

const { handle: authHandle, signIn, signOut } = SvelteKitAuth({
	providers: [
		{
			id: 'oidc',
			name: providerName(),
			type: 'oidc',
			issuer: oidcIssuer(),
			clientId: oidcClientId(),
			clientSecret: oidcClientSecret(),
			// Scope is resolved when this module loads (UI process must have OIDC_* env
			// from `make run` / sourced e2e-ui.env). oidcScopes() always merges Zitadel
			// project audience + roles scopes so bare OIDC_SCOPES=openid still works.
			authorization: {
				params: {
					scope: oidcScopes()
				}
			},
			checks: ['pkce', 'state', 'nonce'],
			profile(profile: Record<string, unknown>) {
				const groupClaims = envCsv('OIDC_GROUP_CLAIMS', DEFAULT_GROUP_CLAIMS);
				const name =
					claimString(profile, 'name') ??
					claimString(profile, 'preferred_username') ??
					claimString(profile, 'email');

				return {
					id: claimString(profile, 'sub') ?? '',
					name,
					email: claimString(profile, 'email'),
					image: claimString(profile, 'picture'),
					groups: extractGroups(profile, groupClaims),
					username: claimString(profile, 'preferred_username'),
					emailVerified: claimBool(profile, 'email_verified')
				};
			}
		} as any
	],
	callbacks: {
		async jwt({ token, account, user, profile }) {
			// Auth.js may assign an internal user UUID. UI and API identity must
			// use the same OIDC subject established by the provider callback.
			if (account) {
				token.sub = account.providerAccountId;
				token.accessToken = account.access_token;
				token.refreshToken = account.refresh_token;
				token.idToken = account.id_token;
				token.expiresAt =
					(account.expires_at as number | undefined) ??
					Math.floor(Date.now() / 1000) + ((account.expires_in as number | undefined) ?? 3600);
			}

			// Profile/userinfo may carry groups even when tokens are still empty.
			const profileGroups = extractGroups(
				(profile as Record<string, unknown> | undefined) ?? {},
				groupClaimPaths()
			);
			if (profileGroups.length) token.groups = profileGroups;
			if (user && Array.isArray(user.groups) && user.groups.length) {
				token.groups = user.groups;
			}

			const expiresAt = typeof token.expiresAt === 'number' ? token.expiresAt : 0;
			const stillFresh =
				expiresAt && Date.now() < (expiresAt - ACCESS_TOKEN_REFRESH_SKEW_SECONDS) * 1000;

			if (!stillFresh && token.refreshToken) {
				try {
					const refreshed = (await refreshAccessToken(token as TokenRecord)) as TokenRecord;
					// Re-extract roles from the new access token.
					const groups = groupsFromTokens(refreshed);
					if (groups.length) refreshed.groups = groups;
					return refreshed;
				} catch (error) {
					console.error('Token refresh failed');
					token.error = 'RefreshAccessTokenError';
					return token;
				}
			}

			// Always refresh groups from current tokens (access token has Zitadel roles).
			const groups = groupsFromTokens(token as TokenRecord);
			if (groups.length) token.groups = groups;

			return token;
		},
		async session({ session, token }) {
			session.accessToken = token.accessToken as string | undefined;
			session.idToken = token.idToken as string | undefined;
			session.expiresAt = token.expiresAt as number | undefined;
			session.refreshAfter = Math.max(
				0,
				((token.expiresAt as number | undefined) ?? 0) - ACCESS_TOKEN_REFRESH_SKEW_SECONDS
			);
			session.hasAccessToken = typeof token.accessToken === 'string' && token.accessToken.length > 0;
			session.hasRefreshToken =
				typeof token.refreshToken === 'string' && token.refreshToken.length > 0;
			session.hasIdToken = typeof token.idToken === 'string' && token.idToken.length > 0;
			session.error = token.error as string | undefined;
			session.user = {
				...session.user,
				id: token.sub as string
			};

			const claims = userClaims(token as TokenRecord);
			// Merge id + access token role claims (Zitadel puts project roles on access).
			session.user.groups = groupsFromTokens(token as TokenRecord);

			const username = claimString(claims, 'preferred_username');
			if (username) session.user.username = username;

			const emailVerified = claimBool(claims, 'email_verified');
			if (emailVerified !== undefined) {
				(session.user as unknown as { emailVerified?: boolean }).emailVerified = emailVerified;
			}

			return session;
		},
		async redirect({ url, baseUrl }) {
			if (url.startsWith('/')) return `${baseUrl}${url}`;
			if (new URL(url).origin === baseUrl) return url;
			return baseUrl;
		}
	},
	pages: {
		signIn: '/',
		error: '/'
	},
	// Cookie overrides and Auth.js defaults share the configured public-origin
	// policy. Local HTTP fixtures remain usable; HTTPS sets Secure throughout.
	useSecureCookies: useSecureCookies(),
	cookies: {
		sessionToken: {
			name: 'authjs.session-token',
			options: {
				httpOnly: true,
				sameSite: authSessionCookieSameSite(),
				path: '/',
				secure: useSecureCookies()
			}
		},
		callbackUrl: {
			name: 'authjs.callback-url',
			options: {
				httpOnly: true,
				sameSite: authSessionCookieSameSite(),
				path: '/',
				secure: useSecureCookies()
			}
		},
		csrfToken: {
			name: 'authjs.csrf-token',
			options: {
				httpOnly: true,
				sameSite: authSessionCookieSameSite(),
				path: '/',
				secure: useSecureCookies()
			}
		},
		pkceCodeVerifier: {
			name: 'authjs.pkce.code_verifier',
			options: {
				httpOnly: true,
				sameSite: authSessionCookieSameSite(),
				path: '/',
				secure: useSecureCookies(),
				maxAge: 60 * 15
			}
		},
		state: {
			name: 'authjs.state',
			options: {
				httpOnly: true,
				sameSite: authSessionCookieSameSite(),
				path: '/',
				secure: useSecureCookies(),
				maxAge: 60 * 15
			}
		}
	},
	jwt: {
		maxAge: 60 * 60
	},
	trustHost: true
});

async function refreshAccessToken(token: TokenRecord) {
	const tokenEndpoint = await oidcTokenEndpoint();
	const tokenAuthMethod = oidcTokenAuthMethod();
	const body = new URLSearchParams({
		grant_type: 'refresh_token',
		refresh_token: String(token.refreshToken)
	});
	const headers: Record<string, string> = {
		'Content-Type': 'application/x-www-form-urlencoded'
	};

	if (tokenAuthMethod === TOKEN_AUTH_BASIC) {
		headers.Authorization = `Basic ${btoa(`${oidcClientId()}:${oidcClientSecret()}`)}`;
	} else if (tokenAuthMethod === TOKEN_AUTH_POST) {
		body.set('client_id', oidcClientId());
		body.set('client_secret', oidcClientSecret());
	} else if (tokenAuthMethod === TOKEN_AUTH_NONE) {
		body.set('client_id', oidcClientId());
	} else {
		throw new Error('OIDC_TOKEN_AUTH_METHOD must be client_secret_basic, client_secret_post, or none');
	}

	const response = await fetch(tokenEndpoint, {
		method: 'POST',
		headers,
		body
	});

	const refreshedTokens = (await response.json()) as {
		access_token?: string;
		refresh_token?: string;
		id_token?: string;
		expires_in?: number;
	};
	if (!response.ok) throw refreshedTokens;

	const expiresIn = typeof refreshedTokens.expires_in === 'number' ? refreshedTokens.expires_in : 3600;

	return {
		...token,
		accessToken: refreshedTokens.access_token,
		refreshToken: refreshedTokens.refresh_token ?? token.refreshToken,
		idToken: refreshedTokens.id_token ?? token.idToken,
		expiresAt: Math.floor(Date.now() / 1000) + expiresIn,
		error: undefined
	};
}

async function oidcTokenEndpoint() {
	const override = envFirst(['OIDC_TOKEN_ENDPOINT']);
	if (override) return override;

	const issuer = oidcIssuer();
	const response = await fetch(`${issuer}/.well-known/openid-configuration`);
	if (!response.ok) {
		throw new Error(`OIDC discovery failed with ${response.status}`);
	}

	const metadata = (await response.json()) as { token_endpoint?: string };
	if (!metadata.token_endpoint) {
		throw new Error('OIDC discovery did not include token_endpoint');
	}

	return metadata.token_endpoint;
}

export { engineRoleFromGroups } from './lib/roles';

// Auth.js session renewal parses Set-Cookie before calling event.cookies.set.
// An absent Secure attribute becomes undefined, which SvelteKit defaults to
// true on HTTP 127.0.0.1. Preserve our explicit policy during that delegation,
// including cookies renewed by locals.auth() on UI/API requests.
export const handle: Handle = ({ event, resolve }) => {
	const setCookie = event.cookies.set.bind(event.cookies);
	event.cookies.set = (name, value, options) => {
		if (name.startsWith('authjs.')) {
			return setCookie(name, value, { ...options, secure: useSecureCookies() });
		}
		return setCookie(name, value, options);
	};
	return authHandle({ event, resolve });
};
export { signIn, signOut };
