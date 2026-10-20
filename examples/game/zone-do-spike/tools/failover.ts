// Node failure: a room placed on one node (first touch through <owner-base>)
// with a player connected through another node (<entry-base>); the
// operator SIGKILLs the owner when this prints READY. The player then
// rejoins through <entry-base> until a room answers again.
// usage: node failover.mjs <owner-base> <entry-base> <room>
import { connectRoom, ATTACK } from '../page/room-client.ts';
const [ownerBase, entry, room] = process.argv.slice(2);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), ...a);
// First touch through the owner places the room there.
await (await fetch(`${ownerBase}/rooms/${encodeURIComponent(room)}/stats`)).json();
const rc = connectRoom(entry, room, 'survivor');
await rc.welcome;
for (let i = 0; i < 100 && !(rc.host()?.node && rc.host()!.node !== '?'); i++) await sleep(50);
log(`joined ${room} via ${entry}; runs on ${rc.host()?.node}`);
let closedAt = 0;
rc.client.on('zoneClosed', (_z, info) => { closedAt = performance.now(); log('connection closed', JSON.stringify(info)); });
const fight = setInterval(() => {
	const me = rc.me()?.fields as any; const mobs = rc.entities().filter((e) => e.kindName === 'mob').map((e) => e.fields as any);
	if (!me || !mobs.length) return;
	const t = mobs.sort((a, b) => Math.hypot(a.x - me.x, a.y - me.y) - Math.hypot(b.x - me.x, b.y - me.y))[0];
	const [dx, dy] = [t.x - me.x, t.y - me.y]; const d = Math.hypot(dx, dy) || 1; const s = d < 1.8 ? 30 : 127;
	rc.input(Math.round(dx / d * s), Math.round(dy / d * s), ATTACK);
}, 33);
while (rc.kills.length < 3) await sleep(100);
await sleep(6000); // > the room's 5 s persist interval
const before = rc.kills.at(-1)!.kills;
log(`kills before failure: ${before} (persisted at most 5 s ago); READY`);
const t0 = performance.now();
while (!closedAt && performance.now() - t0 < 60_000) await sleep(20);
clearInterval(fight);
const killDetected = closedAt;
let attempt = 0;
for (;;) {
	attempt++;
	try {
		const r2 = connectRoom(entry, room, 'survivor');
		await r2.welcome;
		for (let i = 0; i < 100 && !(r2.host()?.node && r2.host()!.node !== '?' && r2.me()); i++) await sleep(50);
		const h = r2.host()!; const me = r2.me()?.fields as any;
		log(`rejoined after ${((performance.now() - killDetected) / 1000).toFixed(2)} s (${attempt} attempts): node=${h.node} activation #${h.activations} restored=${h.restored}; my kills ${me?.level} (had ${before})`);
		r2.close(); break;
	} catch (e) {
		if (attempt % 10 === 1) log(`attempt ${attempt}: ${String(e).slice(0, 120)}`);
		await sleep(250);
	}
}
process.exit(0);
