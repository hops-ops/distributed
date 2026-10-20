// Print one room's replicated host entity and stats.
import { connectRoom } from '../page/room-client.ts';
const [base, room] = process.argv.slice(2);
const t0 = performance.now();
const rc = connectRoom(base, room, 'peek');
await rc.welcome;
const tw = performance.now() - t0;
for (let i = 0; i < 100 && !(rc.host() as any)?.node; i++) await new Promise((r) => setTimeout(r, 20));
await new Promise((r) => setTimeout(r, 1500));
console.log(JSON.stringify({ welcomeMs: +tw.toFixed(1), host: rc.host(), rtt: rc.rtt() }));
rc.client.close(); process.exit(0);
