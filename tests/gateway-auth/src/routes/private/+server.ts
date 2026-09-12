import { json } from '@sveltejs/kit';
import { isCurrentSession } from '$lib/server/require-auth';
export async function GET({ locals }) {
 const session = await locals.auth();
 return isCurrentSession(session) ? json({ subject: session.user.id }) : json({ error: 'unauthorized' }, { status: 401 });
}
