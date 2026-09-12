import { startOidcSignIn } from '$lib/server/oidc-start';
export async function GET(event) { return startOidcSignIn(event); }
