// Graceful node stop: create rooms through <owner-base>, leave, wait for
// eviction, print READY (operator stops that node with SIGTERM), then
// rejoin each through <entry-base> and report where it runs now.
import { connectRoom } from '../page/room-client.ts';
const [ownerBase, entry, n, waitS] = process.argv.slice(2);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const tag = Date.now().toString(36);
const rooms = Array.from({ length: Number(n) }, (_, i) => `dr-${tag}-${i}`);
const node = async (base: string, r: string) => ((await (await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats`)).json()) as any);
const before: Record<string, number> = {};
for (const r of rooms) {
	const rc = connectRoom(ownerBase, r, 'd'); await rc.welcome; await sleep(400); rc.close();
	const st = await node(ownerBase, r); before[st.node] = (before[st.node] ?? 0) + 1;
}
console.log('placed', JSON.stringify(before));
await sleep(Number(waitS) * 1000);
console.log('READY'); 
for (let i = 0; i < 200 && !(await fetch(`${ownerBase}/ticket`).then(() => false, () => true)); i++) await sleep(100);
const t0 = performance.now();
const after: Record<string, number> = {}; let restored = 0; const waits: number[] = [];
for (const r of rooms) {
	const t = performance.now();
	for (;;) { try { const rc = connectRoom(entry, r, 'd'); await rc.welcome; rc.close(); break; } catch { await sleep(200); } }
	waits.push(performance.now() - t);
	const st = await node(entry, r); after[st.node] = (after[st.node] ?? 0) + 1; if (st.restored) restored++;
}
console.log('after stop', JSON.stringify({ owners: after, restored, rooms: rooms.length, firstRejoinMs: Math.round(waits[0]), allRejoinedS: +((performance.now() - t0) / 1000).toFixed(1) }));
process.exit(0);
