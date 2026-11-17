/**
 * **Dev only** (`GAME_DEV_AUTH=1`, signed in): a live content update of
 * every Mob Pit room. `GET` → the world's rollout state and the packs;
 * `POST {version, notes?, hard?}` starts a rollout on the world's
 * `content-rollout` cell, which freezes every room (the notes on screen),
 * applies the pack and resumes them on its own schedule. The cell runs it
 * for an admin session built here from the verified sign-in
 * (`admin:<user id>`), never for a principal from the body; in this dev
 * demo every signed-in user is an admin.
 */
import { error, json } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { CONTENT_PACKS } from '$lib/generated/mobpit/index.ts';
import { cellFetch, CONTENT_WORLD, liveContent } from '$lib/server/game';
import type { RequestHandler } from './$types';

async function admin(locals: App.Locals): Promise<string> {
	if (env.GAME_DEV_AUTH !== '1') error(404, 'not found');
	const user = (await locals.auth())?.user;
	const id = user?.id ?? user?.name;
	if (!id) error(401, 'sign in first');
	return `admin:${id}`;
}

export const GET: RequestHandler = async ({ locals }) => {
	await admin(locals);
	return json({ world: CONTENT_WORLD, state: await liveContent(), packs: CONTENT_PACKS });
};

export const POST: RequestHandler = async ({ locals, request }) => {
	const principal = await admin(locals);
	const body = (await request.json()) as { version?: unknown; notes?: unknown; hard?: unknown };
	const pack = CONTENT_PACKS.find((p) => p.version === body.version);
	if (pack === undefined) error(400, 'unknown content version');
	const notes = typeof body.notes === 'string' && body.notes.trim() !== '' ? body.notes.trim().slice(0, 500) : pack.notes;
	const response = await cellFetch(`/content-rollout/${CONTENT_WORLD}/content_rollout.start`, {
		method: 'POST',
		headers: {
			'content-type': 'application/json',
			'x-distributed-service-id': 'mobpit-admin',
			'x-distributed-principal-partition': principal,
			'x-user-id': principal
		},
		body: JSON.stringify({
			commandId: commandId(),
			input: { world: CONTENT_WORLD, version: pack.version, notes, freeze_ms: 3000, hard: body.hard === true }
		})
	});
	if (response === undefined) error(503, 'celld is not configured');
	const answer = await response.json().catch(() => ({}));
	if (!response.ok) return json(answer, { status: response.status });
	return json({ version: pack.version, notes });
};

/** A UUIDv7 command id (the cells' command ledger requires v7). */
function commandId(): string {
	const bytes = crypto.getRandomValues(new Uint8Array(16));
	const ms = Date.now();
	for (let i = 0; i < 6; i += 1) bytes[i] = Math.floor(ms / 2 ** (8 * (5 - i))) & 0xff;
	bytes[6] = (bytes[6]! & 0x0f) | 0x70;
	bytes[8] = (bytes[8]! & 0x3f) | 0x80;
	const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
