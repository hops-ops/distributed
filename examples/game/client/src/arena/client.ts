/**
 * Arena networking without rendering: builds the `GameClient` with
 * prediction for the own capsule and binds it from the zone's hello event.
 * Used by `main.ts` (Babylon) and by the headless Node test.
 */
import { GameClient, type GameSocketFactory } from '@hops-ops/distributed/game';

import { zoneUrl, type DevApi } from '../shared/dev-api.ts';
import { onJsonEvent } from '../shared/events.ts';
import { encodeMoveInput, predictPlayer, type MoveInput } from './movement.ts';

export const DEFAULT_ZONE = 'arena-1';
/** Event type ids (server `arena::EVENT_*`). */
export const EVENT_HELLO = 1;
/** Replicated kind names. */
export const KIND = { player: 'player', drone: 'drone', pillar: 'pillar' } as const;

export type ArenaHello = Readonly<{ entity: number; viewRadius: number; half: number }>;

export type ArenaClient = Readonly<{
	client: GameClient<MoveInput>;
	/** Latest hello (own entity id, view radius); undefined until joined. */
	hello(): ArenaHello | undefined;
	/** Resolves on the first hello. */
	ready: Promise<ArenaHello>;
}>;

export type ArenaClientOptions = Readonly<{
	api: DevApi;
	/** HTTP origin of the server (Node); default: the page origin. */
	base?: string;
	socket?: GameSocketFactory;
	/** Ease duration for position corrections (ms). */
	easeMs?: number;
}>;

export function createArenaClient(options: ArenaClientOptions): ArenaClient {
	const ease = { mode: 'ease', durationMs: options.easeMs ?? 150 } as const;
	const snap = { mode: 'snap' } as const;
	const client = new GameClient<MoveInput>({
		url: (zone) => zoneUrl(zone, options.base),
		ticket: options.api.ticketSource(),
		socket: options.socket,
		pingIntervalMs: 500,
		// One input = one 30 Hz command frame on the server.
		inputDtMs: 1000 / 30,
		prediction: {
			encodeInput: (input) => encodeMoveInput(input),
			predict: (fields, input) => predictPlayer(fields, input),
			corrections: {
				[KIND.player]: { x: ease, y: ease, vx: snap, vy: snap, dash: snap, cd: snap, name: { mode: 'replace' } }
			}
		}
	});
	let hello: ArenaHello | undefined;
	let resolveReady: (h: ArenaHello) => void = () => undefined;
	const ready = new Promise<ArenaHello>((resolve) => (resolveReady = resolve));
	onJsonEvent<ArenaHello>(client, EVENT_HELLO, (value) => {
		hello = value;
		client.setControlledEntity(value.entity);
		resolveReady(value);
	});
	return { client, hello: () => hello, ready };
}
