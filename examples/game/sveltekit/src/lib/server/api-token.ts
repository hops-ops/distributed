/**
 * Bearer tokens for `mobpit-service` (GraphQL `@live` read model), minted
 * by this server for the **verified session's** subject. The service
 * verifies them (ES256, static JWKS of the public key) and maps `sub` to
 * `x-user-id`; it trusts no identity header. With a real OIDC provider and
 * no signing key, the provider's access token is used instead (see
 * `auth.ts`).
 *
 * `MOBPIT_API_SIGNING_JWK` is the private EC P-256 JWK (`run.sh` and the
 * smoke test generate one per run and hand the public half to the service).
 */
import { env } from '$env/dynamic/private';

export const API_ISSUER_DEFAULT = 'mobpit-sveltekit';
export const API_AUDIENCE_DEFAULT = 'mobpit-service';
/** Lifetime of a minted token; every session read mints a fresh one. */
const TTL_SECONDS = 15 * 60;

let cached: { jwk: string; key: Promise<CryptoKey>; kid: string | undefined } | undefined;

function signingKey(): { key: Promise<CryptoKey>; kid: string | undefined } | undefined {
	const raw = env.MOBPIT_API_SIGNING_JWK;
	if (raw === undefined || raw.trim() === '') return undefined;
	if (cached?.jwk !== raw) {
		const jwk = JSON.parse(raw) as JsonWebKey & { kid?: string };
		cached = {
			jwk: raw,
			kid: jwk.kid,
			key: crypto.subtle.importKey('jwk', jwk, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign'])
		};
	}
	return cached;
}

const b64url = (bytes: Uint8Array | string) =>
	Buffer.from(typeof bytes === 'string' ? new TextEncoder().encode(bytes) : bytes).toString('base64url');

/** A token for `subject`, or undefined when this server has no signing key. */
export async function mintApiToken(subject: string): Promise<string | undefined> {
	const signer = signingKey();
	if (signer === undefined || subject.length === 0) return undefined;
	const now = Math.floor(Date.now() / 1000);
	const header = b64url(JSON.stringify({ alg: 'ES256', typ: 'JWT', ...(signer.kid ? { kid: signer.kid } : {}) }));
	const claims = b64url(
		JSON.stringify({
			iss: env.MOBPIT_API_ISSUER ?? API_ISSUER_DEFAULT,
			aud: env.MOBPIT_API_AUDIENCE ?? API_AUDIENCE_DEFAULT,
			sub: subject,
			roles: ['user'],
			iat: now,
			exp: now + TTL_SECONDS
		})
	);
	// WebCrypto ECDSA signatures are raw r‖s, exactly JWS ES256.
	const signature = await crypto.subtle.sign(
		{ name: 'ECDSA', hash: 'SHA-256' },
		await signer.key,
		new TextEncoder().encode(`${header}.${claims}`)
	);
	return `${header}.${claims}.${b64url(new Uint8Array(signature))}`;
}
