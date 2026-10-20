// Spike gr-p42 debug page: one Mob Pit room on celld, drawn in 2D, with the
// node / isolate / activation running it.
import { connectRoom, ATTACK } from './room-client.ts';

const params = new URLSearchParams(location.search);
const room = params.get('room') ?? 'pit-1';
const name = params.get('name') ?? `p${Math.floor(Math.random() * 1000)}`;
const rc = connectRoom(location.origin, room, name);
const canvas = document.querySelector('canvas')!;
const g = canvas.getContext('2d')!;
const overlay = document.querySelector('#overlay')!;
const keys = new Set<string>();
addEventListener('keydown', (e) => keys.add(e.key.toLowerCase()));
addEventListener('keyup', (e) => keys.delete(e.key.toLowerCase()));
let status = 'connecting';
rc.welcome.then(() => (status = 'joined'), (e) => (status = `failed: ${e}`));
rc.client.on('zoneClosed', (_z, info) => (status = `closed ${JSON.stringify(info)}`));
setInterval(() => {
	const mx = (keys.has('d') ? 127 : 0) - (keys.has('a') ? 127 : 0);
	const my = (keys.has('s') ? 127 : 0) - (keys.has('w') ? 127 : 0);
	if (status === 'joined') rc.input(mx, my, keys.has(' ') || keys.has('j') ? ATTACK : 0);
}, 1000 / 30);
async function placement() {
	try {
		const r = await fetch(`/placement?rooms=${encodeURIComponent(room)}`);
		const p = await r.json();
		(document.querySelector('#placement') as HTMLElement).textContent =
			`operator view: ${room} owned by ${p.rooms[room] ?? '?'}\n` +
			p.nodes.map((n: any) => `${n.node}: ${n.up ? `${n.resident_cells} cells, cpu ${n.cpu_percent}%` : 'down'}`).join('\n');
	} catch {}
}
setInterval(placement, 2000);
placement();
function frame() {
	const s = canvas.width / 42;
	g.fillStyle = '#14161a';
	g.fillRect(0, 0, canvas.width, canvas.height);
	const P = (x: number, y: number) => [(x + 21) * s, (y + 21) * s] as const;
	const me = rc.hello()?.entity;
	for (const e of rc.entities()) {
		const f = e.fields as Record<string, any>;
		if (e.kindName === 'pillar') { const [x, y] = P(f.x, f.y); g.fillStyle = '#444'; g.beginPath(); g.arc(x, y, f.r * s, 0, 7); g.fill(); }
		if (e.kindName === 'mob') { const [x, y] = P(f.x, f.y); g.fillStyle = ['', '#9ad16b', '#b48cf2', '#ff6b6b'][f.tier] ?? '#9ad16b'; g.beginPath(); g.arc(x, y, (0.4 + f.tier * 0.12) * s, 0, 7); g.fill(); if (f.tele) { g.strokeStyle = '#f80'; g.beginPath(); g.arc(x, y, 2 * s * f.tele / 255, 0, 7); g.stroke(); } }
		if (e.kindName === 'player') { const [x, y] = P(f.x, f.y); g.fillStyle = e.id === me ? '#4cc3ff' : '#ddd'; g.beginPath(); g.arc(x, y, 0.5 * s, 0, 7); g.fill(); g.fillStyle = '#fff'; g.fillText(`${f.name} hp${f.hp} k${f.level}`, x + 8, y - 8); }
	}
	const h = rc.host();
	const pit = rc.entities().find((e) => e.kindName === 'pit')?.fields as any;
	overlay.textContent = [
		`room ${room} as ${name} — ${status}`,
		`celld node   ${h?.node ?? '?'}`,
		`isolate      ${h?.isolate ?? '?'}  (${h?.isolate_rooms ?? '?'} rooms seen by this isolate)`,
		`activation   ${h?.activation ?? '?'}  #${h?.activations ?? '?'}  ${h?.restored ? 'restored from storage' : 'fresh'}  wake ${h?.wake_ms ?? '?'} ms`,
		`tick         ${h?.hz ?? '?'} Hz  late ${h?.late_ms?.toFixed(2)} ms (max/s ${h?.late_max_ms?.toFixed(1)})`,
		`room         kills ${pit?.kills ?? '?'}  lifetime ticks ${pit?.ticks ?? '?'}`,
		`rtt          ${rc.rtt()?.toFixed(1) ?? '?'} ms (via ${location.host})`,
		`WASD move, space/J attack`
	].join('\n');
	requestAnimationFrame(frame);
}
frame();
