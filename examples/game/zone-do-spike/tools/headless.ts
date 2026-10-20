// Headless check of one room on celld: join, move, attack until a kill,
// leave, wait for eviction, rejoin (possibly via another node) and check
// the room restored its state in a new activation.
// usage: node headless.mjs <base-url> <room> [rejoin-base-url] [evict-wait-s]
import { connectRoom, ATTACK, type RoomClient } from '../page/room-client.ts';

const [base, room, rejoinBase = base, waitS = '25'] = process.argv.slice(2);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async <T>(f: () => T | undefined | false, ms: number, what: string): Promise<T> => {
	const end = Date.now() + ms;
	for (;;) { const v = f(); if (v) return v; if (Date.now() > end) throw new Error(`timeout: ${what}`); await sleep(20); }
};
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), ...a);

async function join(url: string, name: string) {
	const t0 = performance.now();
	const rc = connectRoom(url, room, name);
	await rc.welcome;
	const welcomeMs = performance.now() - t0;
	await until(() => rc.host()?.node && rc.host()!.node !== '?' && rc.me(), 5000, 'host node + own player');
	return { rc, welcomeMs };
}

async function fight(rc: RoomClient, kills: number) {
	const start = rc.kills.length;
	const end = Date.now() + 60_000;
	let moved = 0;
	const at0 = rc.me()!.fields as any;
	while (rc.kills.length - start < kills) {
		if (Date.now() > end) throw new Error('no kill within 60 s');
		const me = rc.me()?.fields as any;
		const mobs = rc.entities().filter((e) => e.kindName === 'mob').map((e) => e.fields as any);
		if (me && mobs.length) {
			const t = mobs.sort((a, b) => Math.hypot(a.x - me.x, a.y - me.y) - Math.hypot(b.x - me.x, b.y - me.y))[0];
			const [dx, dy] = [t.x - me.x, t.y - me.y]; const d = Math.hypot(dx, dy) || 1;
			const near = d < 1.8;
			rc.input(near ? Math.round(dx / d * 30) : Math.round(dx / d * 127), near ? Math.round(dy / d * 30) : Math.round(dy / d * 127), ATTACK);
			moved = Math.max(moved, Math.hypot(me.x - at0.x, me.y - at0.y));
		}
		await sleep(33);
	}
	return { moved };
}

const { rc, welcomeMs } = await join(base, 'bot-ana');
const h1 = { ...rc.host()! };
log(`joined ${room} via ${base}: node=${h1.node} isolate=${h1.isolate} activation=${h1.activation} #${h1.activations} restored=${h1.restored} welcome=${welcomeMs.toFixed(1)}ms`);
const kills0 = (rc.entities().find((e) => e.kindName === 'pit')?.fields as any)?.kills;
const { moved } = await fight(rc, 1);
await sleep(500);
const k = rc.kills.at(-1)!;
log(`moved ${moved.toFixed(1)} m, killed mob ${k.mob}; my kills in room=${k.kills}; room kills ${kills0} -> ${(rc.entities().find((e) => e.kindName === 'pit')?.fields as any)?.kills}`);
rc.client.close();
log(`left; waiting ${waitS}s for the room to persist, stop and be evicted`);
await sleep(Number(waitS) * 1000);
const r2 = await join(rejoinBase, 'bot-ana');
const h2 = { ...r2.rc.host()! };
const me2 = r2.rc.me()!.fields as any;
const pit2 = r2.rc.entities().find((e) => e.kindName === 'pit')?.fields as any;
log(`rejoined via ${rejoinBase}: node=${h2.node} isolate=${h2.isolate} activation=${h2.activation} #${h2.activations} restored=${h2.restored} wake=${(h2 as any).wake_ms}ms welcome=${r2.welcomeMs.toFixed(1)}ms`);
log(`restored state: my kills=${me2.level} (expected ${k.kills}), room kills=${pit2?.kills}, lifetime ticks=${pit2?.ticks}`);
const ok = h2.activation !== h1.activation && h2.restored && me2.level === k.kills && pit2?.kills >= k.kills;
r2.rc.client.close();
log(ok ? 'PASS' : 'FAIL');
process.exit(ok ? 0 : 1);
