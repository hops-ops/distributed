// Spike gr-p42 measurements with raw protocol bots (no rendering).
//
//   node bench.mjs join <base> <room>                 join phase timings
//   node bench.mjs rtt <base> <room> [n]              Ping/Pong RTT through <base>
//   node bench.mjs load <base> <prefix> <rooms> <secs> [hz] [pids]
//       one bot per room (moves + attacks at 30 Hz); all rooms first touched
//       through <base> so celld places them on that node. Reports the
//       rooms' own loop timing (/rooms/<r>/stats), Delta inter-arrival at
//       the bots and the node processes' CPU (ps) while loaded.
import { execFileSync } from 'node:child_process';
import { decodeServerMessage, encodeClientMessage, encodePayload, type PayloadLayout } from '../../../../js/src/game/index.ts';

const INPUT: PayloadLayout = [['mx', { type: 'i8' }], ['my', { type: 'i8' }], ['buttons', { type: 'u8' }]];
const TAG_DELTA = 0x81; // GameMessageTag.DELTA
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const q = (a: number[], p: number) => { if (!a.length) return NaN; const s = [...a].sort((x, y) => x - y); return +s[Math.round(p * (s.length - 1))].toFixed(2); };
const summary = (a: number[]) => ({ n: a.length, p50: q(a, 0.5), p99: q(a, 0.99), max: q(a, 1) });

type Bot = { ws: WebSocket; deltas: number[]; pongs: Map<number, (t: number) => void>; seq: number; timings: Record<string, number> };

async function bot(base: string, room: string, name: string): Promise<Bot> {
	const t0 = performance.now();
	const r = await fetch(`${base}/ticket?room=${encodeURIComponent(room)}&name=${name}`);
	const { ticket } = (await r.json()) as { ticket: string };
	const tTicket = performance.now();
	const ws = new WebSocket(`${base.replace(/^http/, 'ws')}/rooms/${encodeURIComponent(room)}`);
	ws.binaryType = 'arraybuffer';
	const b: Bot = { ws, deltas: [], pongs: new Map(), seq: 0, timings: {} };
	await new Promise<void>((ok, fail) => { ws.onopen = () => ok(); ws.onerror = () => fail(new Error(`ws error ${room}`)); });
	const tOpen = performance.now();
	ws.send(encodeClientMessage({ type: 'Hello', protocolVersion: 1, contentVersion: '', capabilities: 0, ticket, reconnectToken: '' }));
	await new Promise<void>((ok, fail) => {
		let welcomed = false;
		ws.onclose = (e) => { if (!welcomed) fail(new Error(`closed ${e.code} ${e.reason}`)); };
		ws.onmessage = (e) => {
			const bytes = new Uint8Array(e.data as ArrayBuffer);
			const now = performance.now();
			if (bytes[0] === TAG_DELTA) { b.deltas.push(now); if (!b.timings.firstDelta) b.timings.firstDelta = now - t0; return; }
			const m = decodeServerMessage(bytes);
			if (m.type === 'Welcome') { welcomed = true; b.timings.welcome = now - t0; ok(); }
			if (m.type === 'Pong') b.pongs.get(m.clientTime)?.(now);
			if (m.type === 'Error') fail(new Error(`error ${m.code} ${m.message}`));
		};
	});
	b.timings.ticket = tTicket - t0;
	b.timings.open = tOpen - tTicket;
	b.timings.hello = b.timings.welcome - (tOpen - t0);
	return b;
}

function ping(b: Bot): Promise<number> {
	const t = performance.now();
	return new Promise((ok) => { b.pongs.set(t, (now) => { b.pongs.delete(t); ok(now - t); }); b.ws.send(encodeClientMessage({ type: 'Ping', clientTime: t })); });
}

function drive(b: Bot, phase: number) {
	let k = phase;
	return setInterval(() => {
		k++;
		const a = k / 40;
		b.seq++;
		b.ws.send(encodeClientMessage({ type: 'Input', seq: b.seq, clientTick: b.seq, payload: encodePayload(INPUT, { mx: Math.round(Math.cos(a) * 100), my: Math.round(Math.sin(a) * 100), buttons: 2 }) }));
	}, 1000 / 30);
}

function cpu(pids: string[]) {
	if (!pids.length) return [];
	const out = execFileSync('ps', ['-o', 'pid=,%cpu=,rss=', '-p', pids.join(',')]).toString().trim().split('\n');
	return out.map((l) => { const [pid, c, rss] = l.trim().split(/\s+/); return { pid, cpu: +c, rssMb: Math.round(+rss / 1024) }; });
}

const [mode, base, room, ...rest] = process.argv.slice(2);
if (mode === 'join') {
	// Warm the Node HTTP/WebSocket stack (its first use costs 50-500 ms).
	const w = await bot(base, 'warmup', 'w');
	w.ws.close();
	for (const r of room.split(',')) {
		const b = await bot(base, r, 'joiner');
		await sleep(200);
		console.log(JSON.stringify({ room: r, via: base, ...Object.fromEntries(Object.entries(b.timings).map(([k, v]) => [k, +v.toFixed(1)])) }));
		b.ws.close();
	}
} else if (mode === 'wake') {
	// wake <owner-base> <prefix> <n> <wait-s> <other-base>: create n rooms
	// through the owner (fresh activation), leave, wait for eviction, then
	// rejoin each (even through the owner, odd through <other-base>).
	const [n, waitS, other] = rest;
	const w = await bot(base, 'warmup', 'w'); w.ws.close();
	const w2 = await bot(other, 'warmup', 'w'); w2.ws.close();
	const tag = Date.now().toString(36);
	const rooms = Array.from({ length: Number(n) }, (_, i) => `${room}-${tag}-${i}`);
	const fresh: number[] = [];
	const before: Record<string, number> = {}; const after: Record<string, number> = {}; const moved: string[] = [];
	const ownerOf = async (r: string) => ((await (await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats`)).json()) as any).node as string;
	const first: string[] = [];
	for (const r of rooms) { const b = await bot(base, r, 'x'); fresh.push(b.timings.welcome); await sleep(300); const o = await ownerOf(r); first.push(o); before[o] = (before[o] ?? 0) + 1; b.ws.close(); }
	await sleep(Number(waitS) * 1000);
	const viaOwner: number[] = []; const viaOther: number[] = []; const restored: boolean[] = [];
	for (const [i, r] of rooms.entries()) {
		const b = await bot(i % 2 ? other : base, r, 'x');
		(i % 2 ? viaOther : viaOwner).push(b.timings.welcome);
		b.ws.close();
		const st = await (await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats`)).json() as any;
		restored.push(st.restored && st.activations === 2);
		after[st.node] = (after[st.node] ?? 0) + 1;
		if (st.node !== first[i]) moved.push(`${r}:${first[i]}->${st.node}`);
	}
	console.log(JSON.stringify({ fresh: summary(fresh), wakeViaOwner: summary(viaOwner), wakeViaOther: summary(viaOther), restoredAll: restored.every(Boolean), restored: restored.filter(Boolean).length, ownersBefore: before, ownersAfter: after, moved: moved.length, movedRooms: moved.slice(0, 5) }));
} else if (mode === 'rtt') {
	const n = Number(rest[0] ?? 200);
	const b = await bot(base, room, 'pinger');
	const rtts: number[] = [];
	const t0 = performance.now(); const spikes: string[] = [];
	for (let i = 0; i < n; i++) { const r = await ping(b); rtts.push(r); if (r > 15) spikes.push(`${((performance.now() - t0) / 1000).toFixed(2)}s:${r.toFixed(0)}`); await sleep(10); }
	if (process.env.SPIKES) console.log('spikes', spikes.join(' '));
	const st = await (await fetch(`${base}/rooms/${encodeURIComponent(room)}/stats`)).json() as any;
	console.log(JSON.stringify({ room, via: base, owner: st.node, rtt: summary(rtts) }));
	b.ws.close();
} else if (mode === 'crowd') {
	// crowd <base> <room> <players> <secs> [hz] [pids] [bases]: <players> bots
	// in ONE room, joins spread over [bases] (comma list, default <base>), so
	// every bot not on the owner node is forwarded. Reports the room's loop
	// timing, per-bot Delta size/interval/bandwidth, ping and node CPU.
	const [count, secs, hz = '30', pidList = '', baseList = ''] = rest;
	const pids = pidList ? pidList.split(',') : [];
	const bases = baseList ? baseList.split(',') : [base];
	const r = `${room}@${hz}`;
	const bots: Bot[] = [];
	const bytes: number[][] = [];
	const t0 = performance.now();
	for (let i = 0; i < Number(count); i += 10) {
		const batch = await Promise.all(Array.from({ length: Math.min(10, Number(count) - i) }, (_, j) => bot(bases[(i + j) % bases.length], r, `p${i + j}`)));
		for (const b of batch) {
			const sizes: number[] = [];
			bytes.push(sizes);
			const prev = b.ws.onmessage!;
			b.ws.onmessage = (e) => { const d = e.data as ArrayBuffer; if (new Uint8Array(d)[0] === TAG_DELTA) sizes.push(d.byteLength); (prev as any).call(b.ws, e); };
			bots.push(b);
		}
	}
	const joinS = (performance.now() - t0) / 1000;
	const welcome = bots.map((b) => b.timings.welcome);
	const timers = bots.map((b, i) => drive(b, i * 7));
	await sleep(3000);
	await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats?reset=1`).catch(() => undefined);
	for (const b of bots) b.deltas.length = 0;
	for (const s of bytes) s.length = 0;
	const samples: ReturnType<typeof cpu>[] = [];
	const end = Date.now() + Number(secs) * 1000;
	while (Date.now() < end) { await sleep(1000); samples.push(cpu(pids)); }
	const st = (await (await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats`)).json()) as any;
	const rtts: number[] = [];
	for (const b of bots.filter((_, i) => i % 5 === 0)) rtts.push(await ping(b));
	timers.forEach(clearInterval);
	const arrivals = bots.flatMap((b) => b.deltas.slice(1).map((t, i) => t - b.deltas[i]));
	const sizes = bytes.flat();
	const perBotKBps = bytes.map((s) => s.reduce((a, x) => a + x, 0) / 1024 / Number(secs));
	const cpuByPid: Record<string, number[]> = {};
	for (const s of samples) for (const c of s) (cpuByPid[c.pid] ??= []).push(c.cpu);
	console.log(JSON.stringify({
		room: r, players: bots.length, connected: bots.filter((b) => b.ws.readyState === 1).length, hz: Number(hz), secs: Number(secs),
		joinS: +joinS.toFixed(1), welcome: summary(welcome), owner: st.node,
		lateP50: st.late_ms?.p50, lateP99: st.late_ms?.p99, intervalP99: st.interval_ms?.p99, droppedTicks: st.dropped_ticks,
		iterations: st.iterations, expectedIterations: Math.round(Number(hz) * Number(secs)),
		deltaBytes: summary(sizes), deltaArrival: summary(arrivals),
		perBotKBps: summary(perBotKBps), roomOutKBps: +perBotKBps.reduce((a, x) => a + x, 0).toFixed(1),
		pingUnderLoad: summary(rtts),
		cpu: Object.fromEntries(Object.entries(cpuByPid).map(([p, a]) => [p, { avg: +(a.reduce((x, y) => x + y, 0) / a.length).toFixed(1), max: Math.max(...a) }]))
	}));
	for (const b of bots) b.ws.close();
	await sleep(500);
} else if (mode === 'load') {
	const [count, secs, hz = '30', pidList = ''] = rest;
	const pids = pidList ? pidList.split(',') : [];
	const rooms = Array.from({ length: Number(count) }, (_, i) => `${room}-${i}@${hz}`);
	const bots: Bot[] = [];
	const t0 = performance.now();
	for (let i = 0; i < rooms.length; i += 20) {
		bots.push(...(await Promise.all(rooms.slice(i, i + 20).map((r, j) => bot(base, r, `b${i + j}`)))));
	}
	const joinS = (performance.now() - t0) / 1000;
	const timers = bots.map((b, i) => drive(b, i * 7));
	await sleep(3000);
	await Promise.all(rooms.map((r) => fetch(`${base}/rooms/${encodeURIComponent(r)}/stats?reset=1`).catch(() => undefined)));
	for (const b of bots) b.deltas.length = 0;
	const samples: ReturnType<typeof cpu>[] = [];
	const end = Date.now() + Number(secs) * 1000;
	while (Date.now() < end) { await sleep(1000); samples.push(cpu(pids)); }
	const fetched = await Promise.all(rooms.map(async (r) => { try { return await (await fetch(`${base}/rooms/${encodeURIComponent(r)}/stats`)).json() as any; } catch { return undefined; } }));
	const stats = fetched.filter(Boolean);
	if (stats.length < rooms.length) console.log(`stats unavailable for ${rooms.length - stats.length} rooms`);
	const rtts: number[] = [];
	for (const b of bots.slice(0, 10)) rtts.push(await ping(b));
	timers.forEach(clearInterval);
	const late99 = stats.map((s) => s.late_ms.p99);
	const late50 = stats.map((s) => s.late_ms.p50);
	const interval = stats.map((s) => s.interval_ms.p99);
	const dropped = stats.reduce((a, s) => a + s.dropped_ticks, 0);
	const catchUp = stats.reduce((a, s) => a + s.catch_up_iterations, 0);
	const iterations = stats.reduce((a, s) => a + s.iterations, 0);
	const arrivals = bots.flatMap((b) => b.deltas.slice(1).map((t, i) => t - b.deltas[i]));
	const nodes: Record<string, number> = {};
	for (const s of stats) nodes[s.node] = (nodes[s.node] ?? 0) + 1;
	const isolates = new Set(stats.map((s) => s.isolate)).size;
	const cpuByPid: Record<string, number[]> = {};
	for (const s of samples) for (const c of s) (cpuByPid[c.pid] ??= []).push(c.cpu);
	console.log(JSON.stringify({
		rooms: rooms.length, hz: Number(hz), secs: Number(secs), joinS: +joinS.toFixed(1), nodes, isolates,
		lateP50_median: q(late50, 0.5), lateP99_median: q(late99, 0.5), lateP99_worstRoom: q(late99, 1),
		intervalP99_median: q(interval, 0.5), droppedTicks: dropped, catchUpIterations: catchUp, iterations,
		expectedIterations: Math.round(rooms.length * Number(hz) * Number(secs)),
		deltaArrival: summary(arrivals), pingUnderLoad: summary(rtts),
		cpu: Object.fromEntries(Object.entries(cpuByPid).map(([p, a]) => [p, { avg: +(a.reduce((x, y) => x + y, 0) / a.length).toFixed(1), max: Math.max(...a) }])),
		rss: samples.at(-1)
	}));
	for (const b of bots) b.ws.close();
	await sleep(500);
}
process.exit(0);
