import { createHash } from 'node:crypto';
import { json } from '@sveltejs/kit';
import { isCurrentSession } from '$lib/server/require-auth';

// Exercise concurrent first reads and later consumers against the real Auth.js
// request cookie, including the compatibility alias. Never expose credentials.
export async function GET({ locals }) {
 const sessions = await Promise.all([locals.auth(), locals.auth(), locals.getSession()]);
 sessions.push(await locals.auth(), await locals.getSession());
 const session = sessions[0];
 return json({
  sameSession: sessions.every(value => value === session),
  authenticated: isCurrentSession(session),
  error: session?.error ?? null,
  tokenDigest: session?.accessToken ? createHash('sha256').update(session.accessToken).digest('hex') : null
 });
}
