/**
 * **Dev only** (`GAME_DEV_AUTH=1`, signed in): the network simulation every
 * Mob Pit room on celld applies to its sockets (latency, jitter, loss), kept
 * in the cells worker's dev settings object (`zone-dev/netsim`, read by each
 * room within a second). Server-wide: it affects every player.
 */
import { error, json } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import type { RequestHandler } from './$types';

type NetSim = { latencyMs: number; jitterMs: number; loss: number };

function bounded(body: Partial<NetSim>): NetSim {
	const ms = (v: unknown, max: number) => (typeof v === 'number' && Number.isFinite(v) ? Math.min(max, Math.max(0, Math.round(v))) : 0);
	const loss = typeof body.loss === 'number' && Number.isFinite(body.loss) ? Math.min(0.5, Math.max(0, body.loss)) : 0;
	return { latencyMs: ms(body.latencyMs, 2000), jitterMs: ms(body.jitterMs, 2000), loss };
}

async function devSettings(init?: RequestInit): Promise<NetSim> {
	const base = env.CELLD_URL;
	const secret = env.DISTRIBUTED_INTERNAL_SECRET;
	if (!base || !secret) error(503, 'celld is not configured');
	const response = await fetch(`${base.replace(/\/$/, '')}/zone-dev/netsim`, {
		...init,
		headers: { 'x-distributed-internal-secret': secret, 'content-type': 'application/json' },
		signal: AbortSignal.timeout(3000)
	});
	if (!response.ok) error(502, `dev settings: HTTP ${response.status}`);
	return bounded((await response.json()) as Partial<NetSim>);
}

async function guard(locals: App.Locals): Promise<void> {
	if (env.GAME_DEV_AUTH !== '1') error(404, 'not found');
	if (!(await locals.auth())?.user) error(401, 'sign in first');
}

export const GET: RequestHandler = async ({ locals }) => {
	await guard(locals);
	return json(await devSettings());
};

export const POST: RequestHandler = async ({ locals, request }) => {
	await guard(locals);
	const settings = bounded((await request.json()) as Partial<NetSim>);
	return json(await devSettings({ method: 'PUT', body: JSON.stringify(settings) }));
};
