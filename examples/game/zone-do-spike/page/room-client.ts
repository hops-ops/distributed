// Spike gr-p42: a Mob Pit room client over the framework's generic
// GameClient (no generated code, no prediction). Shared by the debug page
// and the headless test (Node 22+ has a global WebSocket).
import { GameClient, decodePayload, encodePayload, type PayloadLayout, type RenderedEntity } from '../../../../js/src/game/index.ts';

const INPUT: PayloadLayout = [['mx', { type: 'i8' }], ['my', { type: 'i8' }], ['buttons', { type: 'u8' }]];
const HELLO: PayloadLayout = [['entity', { type: 'u32' }], ['half', { type: 'f32' }], ['boot', { type: 'short_str' }], ['attackRange', { type: 'f32' }], ['attackArcCos', { type: 'f32' }]];
const KILL: PayloadLayout = [['mob', { type: 'u32' }], ['tier', { type: 'u8' }], ['killer', { type: 'u32' }], ['killerName', { type: 'short_str' }], ['kills', { type: 'u32' }]];
export const ATTACK = 2;

export type Host = { node: string; isolate: string; activation: string; activations: number; restored: boolean; wakeMs: number; hz: number; lateMs: number; lateMaxMs: number; workUs: number; isolateRooms: number };

export type RoomClient = {
	client: GameClient<unknown>;
	welcome: Promise<unknown>;
	hello: () => { entity: number; boot: string } | undefined;
	host: () => Host | undefined;
	entities: () => RenderedEntity[];
	me: () => RenderedEntity | undefined;
	kills: { mob: number; killer: number; killerName: string; kills: number }[];
	input: (mx: number, my: number, buttons: number) => void;
	rtt: () => number | undefined;
	close: () => void;
};

export function connectRoom(base: string, room: string, name: string): RoomClient {
	const ws = base.replace(/^http/, 'ws');
	let hello: { entity: number; boot: string } | undefined;
	const kills: RoomClient['kills'] = [];
	const client = new GameClient<unknown>({
		url: (zone) => `${ws}/rooms/${encodeURIComponent(zone)}`,
		ticket: async ({ zoneId }) => {
			const r = await fetch(`${base}/ticket?room=${encodeURIComponent(zoneId)}&name=${encodeURIComponent(name)}`);
			return ((await r.json()) as { ticket: string }).ticket;
		},
		pingIntervalMs: 500,
		reconnect: false
	});
	client.on('event', (_zone, e) => {
		if (e.typeId === 1) hello = decodePayload(HELLO, e.payload) as never;
		if (e.typeId === 3) kills.push(decodePayload(KILL, e.payload) as never);
	});
	const welcome = client.connect(room);
	const entities = () => client.render();
	const host = (): Host | undefined => entities().find((e) => e.kindName === 'host')?.fields as never;
	return {
		client,
		welcome,
		hello: () => hello,
		host,
		entities,
		me: () => entities().find((e) => e.kindName === 'player' && hello && e.id === hello.entity),
		kills,
		input: (mx, my, buttons) => client.sendRawInput(encodePayload(INPUT, { mx, my, buttons })),
		rtt: () => {
			const z = client.zone(room) as unknown as { connection?: { clock?: { rtt?: number } } } | undefined;
			return z?.connection?.clock?.rtt;
		},
		close: () => client.close()
	};
}
