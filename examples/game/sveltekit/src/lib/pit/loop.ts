/**
 * The Mob Pit render and input loop: plain TypeScript on its own
 * `requestAnimationFrame`, outside Svelte. It reads `client.render()` every
 * frame and sends one input per server tick; the Svelte page only starts it
 * and reads the slower facts (status, RTT, events) through `useGame`.
 *
 * One continuous world: room `(x, y)` sits at world `(x, y) × ROOM`, the
 * view follows the own player's world position, and the rooms around are
 * drawn from their seeded layout, so walking through a doorway just
 * continues into the next room (the room hands the player over at the same
 * world point).
 */
import { InputPacer, type EntityFields, type GameClient, type RenderedEntity } from '@hops-ops/distributed/game';

import {
	BUTTON,
	createMobpitPures,
	PIT_RULES,
	PIT_WORLD,
	isMob,
	isPillar,
	isPlayer,
	mobFields,
	pillarFields,
	playerFields,
	type HelloEvent,
	type PitInput,
	type WorldTiers
} from '$lib/generated/mobpit/index.ts';

const TIER_COLOR: Record<number, string> = { 1: '#9ad16b', 2: '#b48cf2', 3: '#ff6b6b', 4: '#ff2d55' };
const TIER_RADIUS = Object.fromEntries(PIT_RULES.tiers.map((t) => [t.tier, t]));
const PLAYER_RADIUS = 0.45;
const DOOR_HALF = PIT_WORLD.doorHalf;
const HALF = PIT_WORLD.half;
/** Room edge (m): room `(x, y)` is centred at world `(x, y) × ROOM`, one continuous world. */
const ROOM = PIT_WORLD.roomSize;
/** Metres across the canvas: your room and a strip of each neighbour. */
const VIEW = 2 * HALF + 12;
/** Floor tint per biome (`PIT_WORLD.biomes`). */
const BIOME_COLOR = ['#8a8f99', '#c9a86a', '#3f7d4e', '#7fbf6a'];

/** `pit.<x>.<y>` → `[x, y]`. */
function parseRoom(zone: string | undefined): [number, number] | undefined {
	const m = zone === undefined ? null : /^pit\.(-?\d+)\.(-?\d+)$/.exec(zone);
	return m === null ? undefined : [Number(m[1]), Number(m[2])];
}

/** The 8 rooms around `zone` (watched as passive observers: `watchZones`). */
export function neighbourRooms(zone: string): string[] {
	const at = parseRoom(zone);
	if (at === undefined) return [];
	const out: string[] = [];
	for (let dy = -1; dy <= 1; dy += 1) {
		for (let dx = -1; dx <= 1; dx += 1) if (dx !== 0 || dy !== 0) out.push(`pit.${at[0] + dx}.${at[1] + dy}`);
	}
	return out;
}

/**
 * The own player's predicted fields in the next room's frame (the same world
 * point): the SDK keeps predicting through a doorway handoff (`handoffFrame`).
 */
export function pitHandoffFrame(fields: EntityFields, from: string, to: string): EntityFields | undefined {
	const a = parseRoom(from);
	const b = parseRoom(to);
	const { x, y } = fields as { x?: unknown; y?: unknown };
	if (a === undefined || b === undefined || typeof x !== 'number' || typeof y !== 'number') return undefined;
	return { ...fields, x: x + (a[0] - b[0]) * ROOM, y: y + (a[1] - b[1]) * ROOM };
}

/** World position (m) of an entity: its room's place plus its room-local x / y. */
function worldOf(entity: RenderedEntity): { x: number; y: number } | undefined {
	const room = parseRoom(entity.zoneId);
	const { x, y } = entity.fields as { x?: unknown; y?: unknown };
	if (room === undefined || typeof x !== 'number' || typeof y !== 'number') return undefined;
	return { x: room[0] * ROOM + x, y: room[1] * ROOM + y };
}
/** Swing cone, from the server's rules (reach to a mob's edge, plus the body). */
const SWING_REACH = PIT_RULES.attackRange + PLAYER_RADIUS;
const SWING_HALF = PIT_RULES.attackHalfAngle;

export type PitLoopOptions = Readonly<{
	client: GameClient<PitInput>;
	hello: () => HelloEvent | undefined;
	/** Steer to the nearest mob and swing (demo and headless test). */
	autoHunt: () => boolean;
	/** Draw the server's authoritative copy of you (the ghost). */
	ghost?: () => boolean;
	/** The tiers above the room (borough weather darkens the view). */
	world?: () => WorldTiers | undefined;
}>;

/** Darkness of the view (0..1): the borough's weather, thicker near a boss room. */
const WEATHER_DARK = [0.55, 0.4, 0.22, 0.05];

/** Start drawing on `canvas`; returns the stop function. */
export function startPitLoop(canvas: HTMLCanvasElement, options: PitLoopOptions): () => void {
	const { client } = options;
	const ctx = canvas.getContext('2d')!;
	const keys = new Set<string>();
	const down = (e: KeyboardEvent) => {
		if (['KeyW', 'KeyA', 'KeyS', 'KeyD', 'Space', 'KeyE'].includes(e.code)) e.preventDefault();
		keys.add(e.code);
	};
	const up = (e: KeyboardEvent) => keys.delete(e.code);
	window.addEventListener('keydown', down);
	window.addEventListener('keyup', up);
	const pacer = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });

	const steer = (): PitInput => {
		const hello = options.hello();
		const room = client.authority?.zoneId;
		const rendered = client.render().filter((e) => room === undefined || e.zoneId === room);
		const me = rendered.find((e) => e.id === hello?.entity && isPlayer(e));
		if (options.autoHunt() && me !== undefined) {
			const p = playerFields(me);
			const target = rendered
				.filter((e) => isMob(e) && !e.stale)
				.map(mobFields)
				.map((m) => ({ m, d: Math.hypot(m.x - p.x, m.y - p.y) }))
				.sort((a, b) => a.d - b.d)[0];
			if (target === undefined || p.dead) return { mx: 0, my: 0, buttons: 0 };
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			return {
				mx: Math.round((target.m.x - p.x) * k),
				my: Math.round((target.m.y - p.y) * k),
				buttons: near ? BUTTON.attack : 0
			};
		}
		const axis = (neg: string, pos: string) => (keys.has(pos) ? 127 : 0) - (keys.has(neg) ? 127 : 0);
		return {
			mx: axis('KeyA', 'KeyD'),
			// Screen y grows downwards; world y grows upwards.
			my: axis('KeyS', 'KeyW'),
			buttons: (keys.has('Space') ? BUTTON.attack : 0) | (keys.has('KeyE') ? BUTTON.dash : 0)
		};
	};

	// The rooms around are drawn from their seeded layout (`pit_room`, the
	// Rust generator as WASM): biome floor, walls, doorways, pillars.
	const pures = createMobpitPures();
	/** Pillars are the same in every room: learned from the replicated ones. */
	let pillars: { x: number; y: number; r: number }[] = [];
	let lastMe: { x: number; y: number } | undefined;
	let lastFrame = 0;

	let frame = requestAnimationFrame(function draw(now) {
		pacer.update(now, () => client.sendInput(steer()));
		const hello = options.hello();
		const room = client.authority?.zoneId;
		if (room !== undefined) canvas.dataset.room = room;
		const here = parseRoom(room);
		const rendered = client.render();
		// Yours: predicted, or the SDK's hold of it during a doorway handoff.
		const mine = (e: RenderedEntity) => isPlayer(e) && (e.predicted || (e.id === hello?.entity && e.zoneId === room));
		const meEntity = rendered.find((e) => mine(e) && e.predicted) ?? rendered.find(mine);
		const me = meEntity === undefined ? undefined : worldOf(meEntity);
		// The view follows you through one continuous world.
		const centre = me ?? (here === undefined ? { x: 0, y: 0 } : { x: here[0] * ROOM, y: here[1] * ROOM });
		if (me !== undefined) {
			canvas.dataset.world = `${me.x.toFixed(3)},${me.y.toFixed(3)}`;
			// Largest frame-to-frame move of the view (m), frames ≤ 50 ms
			// apart; the headless test resets it to 0 before a doorway.
			if (lastMe !== undefined && now - lastFrame <= 50) {
				const jump = Math.hypot(me.x - lastMe.x, me.y - lastMe.y);
				if (jump > Number(canvas.dataset.jump ?? 0)) canvas.dataset.jump = jump.toFixed(3);
			}
			lastMe = me;
		}
		lastFrame = now;
		const { width, height } = canvas;
		const scale = Math.min(width, height) / VIEW;
		const at = (x: number, y: number): [number, number] => [width / 2 + (x - centre.x) * scale, height / 2 - (y - centre.y) * scale];
		const circle = (x: number, y: number, r: number, fill: string, stroke?: string) => {
			const [cx, cy] = at(x, y);
			ctx.beginPath();
			ctx.arc(cx, cy, r * scale, 0, Math.PI * 2);
			ctx.fillStyle = fill;
			ctx.fill();
			if (stroke !== undefined) {
				ctx.strokeStyle = stroke;
				ctx.stroke();
			}
		};
		const line = (x0: number, y0: number, x1: number, y1: number) => {
			ctx.beginPath();
			ctx.moveTo(...at(x0, y0));
			ctx.lineTo(...at(x1, y1));
			ctx.stroke();
		};
		ctx.fillStyle = '#0b090d';
		ctx.fillRect(0, 0, width, height);
		const seed = hello?.seed ?? PIT_WORLD.defaultSeed;
		const [cx, cy] = [Math.round(centre.x / ROOM), Math.round(centre.y / ROOM)];
		for (let dy = -1; dy <= 1; dy += 1) {
			for (let dx = -1; dx <= 1; dx += 1) {
				const [rx, ry] = [cx + dx, cy + dy];
				const [ox, oy] = [rx * ROOM, ry * ROOM];
				const biome = pures.loaded() ? pures.pitRoom({ seed, x: rx, y: ry }).biome : undefined;
				ctx.fillStyle = biome === undefined ? '#120f14' : `${BIOME_COLOR[biome] ?? '#333333'}30`;
				ctx.fillRect(...at(ox - HALF, oy + HALF), 2 * HALF * scale, 2 * HALF * scale);
				for (const p of pillars) circle(ox + p.x, oy + p.y, p.r, '#3a3443');
				// Walls with a doorway gap in the middle of each.
				ctx.strokeStyle = '#4b5668';
				ctx.lineWidth = 3;
				for (const s of [-1, 1]) {
					line(ox - HALF, oy + s * HALF, ox - DOOR_HALF, oy + s * HALF);
					line(ox + DOOR_HALF, oy + s * HALF, ox + HALF, oy + s * HALF);
					line(ox + s * HALF, oy - HALF, ox + s * HALF, oy - DOOR_HALF);
					line(ox + s * HALF, oy + DOOR_HALF, ox + s * HALF, oy + HALF);
				}
				ctx.strokeStyle = 'rgba(77,208,225,0.5)';
				ctx.lineWidth = 2;
				for (const s of [-1, 1]) {
					line(ox - DOOR_HALF, oy + s * HALF, ox + DOOR_HALF, oy + s * HALF);
					line(ox + s * HALF, oy - DOOR_HALF, ox + s * HALF, oy + DOOR_HALF);
				}
				if (rx === here?.[0] && ry === here?.[1]) {
					ctx.strokeStyle = 'rgba(255,255,255,0.25)';
					ctx.lineWidth = 1;
					ctx.strokeRect(...at(ox - HALF, oy + HALF), 2 * HALF * scale, 2 * HALF * scale);
				}
			}
		}
		// Interest ring: the room replicates mobs and players inside it only.
		if (me !== undefined && hello !== undefined) {
			const [mx, my] = at(me.x, me.y);
			ctx.beginPath();
			ctx.arc(mx, my, hello.viewRadius * scale, 0, Math.PI * 2);
			ctx.strokeStyle = 'rgba(108,196,255,0.35)';
			ctx.setLineDash([6, 6]);
			ctx.lineWidth = 1.5;
			ctx.stroke();
			ctx.setLineDash([]);
		}
		// The ghost: the room's authoritative copy of you (behind by the round trip).
		const authoritative = client.predictor?.authoritative();
		const ghostZone = client.authority?.zoneId;
		const ghostRoom = parseRoom(ghostZone);
		if (authoritative !== undefined && ghostRoom !== undefined) {
			const g = playerFields({ fields: authoritative });
			const gw = { x: ghostRoom[0] * ROOM + g.x, y: ghostRoom[1] * ROOM + g.y };
			if (me !== undefined) canvas.dataset.ghostGap = Math.hypot(gw.x - me.x, gw.y - me.y).toFixed(3);
			if (options.ghost?.() ?? true) {
				const [gx, gy] = at(gw.x, gw.y);
				ctx.beginPath();
				ctx.arc(gx, gy, PLAYER_RADIUS * scale, 0, Math.PI * 2);
				ctx.strokeStyle = 'rgba(255,255,255,0.8)';
				ctx.setLineDash([3, 3]);
				ctx.stroke();
				ctx.setLineDash([]);
			}
		}
		ctx.lineWidth = 1;
		// Mobs drawn from the rooms next door (watched), for the headless test.
		let nextDoor = 0;
		for (const entity of rendered) {
			const w = worldOf(entity);
			if (w === undefined) continue;
			if (isPillar(entity)) {
				const p = pillarFields(entity);
				if (!pillars.some((q) => q.x === p.x && q.y === p.y)) pillars = [...pillars, { x: p.x, y: p.y, r: p.r }];
			} else if (isMob(entity)) {
				if (entity.stale) continue;
				if (entity.zoneId !== room) nextDoor += 1;
				const m = mobFields(entity);
				const look = TIER_RADIUS[m.tier];
				if (m.tele > 0 && look !== undefined) circle(w.x, w.y, look.tele, 'rgba(255,80,80,0.12)', '#ff5050');
				circle(w.x, w.y, look?.radius ?? 0.5, TIER_COLOR[m.tier] ?? '#9ad16b');
			} else if (isPlayer(entity)) {
				if (entity.stale) continue;
				const p = playerFields(entity);
				const isMe = entity === meEntity;
				if (!isMe && mine(entity)) continue;
				// Own room-local position for the headless test (a DOM attribute, not Svelte state).
				if (isMe) canvas.dataset.me = `${p.x.toFixed(2)},${p.y.toFixed(2)}`;
				if (p.swing > 0 && !p.dead) {
					// The swing cone (blue), fading as `swing` counts down.
					const fade = p.swing / PIT_RULES.swingFrames;
					ctx.beginPath();
					ctx.moveTo(...at(w.x, w.y));
					for (let i = 0; i <= 16; i += 1) {
						const a = p.face - SWING_HALF + (2 * SWING_HALF * i) / 16;
						ctx.lineTo(...at(w.x + Math.cos(a) * SWING_REACH, w.y + Math.sin(a) * SWING_REACH));
					}
					ctx.closePath();
					ctx.fillStyle = `rgba(77,163,255,${0.12 + 0.25 * fade})`;
					ctx.fill();
					ctx.strokeStyle = '#4da3ff';
					ctx.stroke();
					// Frames that drew an own swing, for the headless test.
					if (isMe) canvas.dataset.swings = String(Number(canvas.dataset.swings ?? 0) + 1);
				}
				circle(w.x, w.y, PLAYER_RADIUS, p.dead ? '#555' : isMe ? '#ffd166' : '#6ec3ff');
				const [nx, ny] = at(w.x + Math.cos(p.face) * 0.6, w.y + Math.sin(p.face) * 0.6);
				const [px, py] = at(w.x, w.y);
				ctx.strokeStyle = '#fff';
				ctx.beginPath();
				ctx.moveTo(px, py);
				ctx.lineTo(nx, ny);
				ctx.stroke();
				ctx.fillStyle = '#ddd';
				ctx.font = '11px system-ui';
				ctx.fillText(p.name, px - 16, py - 0.7 * scale);
			}
		}
		canvas.dataset.nextDoorMobs = String(nextDoor);
		// Weather and darkness: the borough's sky, thicker toward bosses.
		if (here !== undefined && pures.loaded()) {
			const world = options.world?.();
			const near = Math.max(0, 1 - pures.pitRoom({ seed, x: here[0], y: here[1] }).bossDistance / 6);
			const dark = Math.min(0.85, (WEATHER_DARK[world?.weather ?? 0] ?? 0.55) * (1 + 0.6 * near));
			ctx.fillStyle = `rgba(8,10,20,${dark.toFixed(3)})`;
			ctx.fillRect(0, 0, width, height);
			canvas.dataset.dark = dark.toFixed(3);
		}
		frame = requestAnimationFrame(draw);
	});

	return () => {
		cancelAnimationFrame(frame);
		window.removeEventListener('keydown', down);
		window.removeEventListener('keyup', up);
	};
}
