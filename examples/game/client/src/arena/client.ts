/**
 * Arena networking without rendering: the generated `createArenaClient`
 * (input encoder, correction defaults, typed events) plus the demo's
 * predicted movement step and dev tickets; binds the own capsule from the
 * zone's typed `hello`. Used by `main.ts` (Babylon) and the headless test.
 */
import type { GameSocketFactory } from '@hops-ops/distributed/game';

import * as arena from '../generated/arena/index.ts';
import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { latest } from '../shared/latest.ts';
import { predictPlayer } from './movement.ts';

export const DEFAULT_ZONE = 'arena-1';

export type ArenaClient = arena.ArenaClient &
	Readonly<{
		/** Latest hello (own entity id, view radius); undefined until joined. */
		hello(): arena.HelloEvent | undefined;
		/** Resolves on the first hello. */
		ready: Promise<arena.HelloEvent>;
	}>;

export type ArenaClientOptions = Readonly<{
	api: DevApi;
	/** HTTP origin of the server (Node); default: the page origin. */
	base?: string;
	socket?: GameSocketFactory;
}>;

export function createArenaClient(options: ArenaClientOptions): ArenaClient {
	const typed = arena.createArenaClient({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		// One input = one 30 Hz command frame on the server.
		inputDtMs: 1000 / 30,
		prediction: { predict: (fields, input) => predictPlayer(fields, input) }
	});
	const hello = latest<arena.HelloEvent>(
		(h) => typed.on('hello', h),
		(value) => typed.client.setControlledEntity(value.entity)
	);
	return { ...typed, hello: hello.get, ready: hello.ready };
}
