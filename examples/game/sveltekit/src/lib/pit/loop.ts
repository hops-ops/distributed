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
import { InputPacer, onGameEvent, type EntityFields, type GameClient, type RenderedEntity } from '@hops-ops/distributed/game';

import {
	BUTTON,
	createMobpitPures,
	EVENTS,
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

/** Obstacle fill by look: column, boulder, tree, crystal. */
const OBSTACLE_COLOR = ['#3a3443', '#4a463f', '#24452b', '#2f5e70'];
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
	/** First person: the view turns with your aim (pointer lock), arrows leave from the eye. */
	firstPerson?: () => boolean;
	/** Resolve arrows with lag compensation (default on). */
	compensated?: () => boolean;
	/** The tint (0xRRGGBB, 0: none) of the content pack this client runs. */
	tint?: () => number;
}>;

/** `color` blended 60 % towards `tint` (0: unchanged). */
function tinted(color: string, tint: number): string {
	if (tint === 0) return color;
	const c = Number.parseInt(color.slice(1), 16);
	const mix = (shift: number) => Math.round(((c >> shift) & 255) * 0.4 + ((tint >> shift) & 255) * 0.6);
	return `#${((mix(16) << 16) | (mix(8) << 8) | mix(0)).toString(16).padStart(6, '0')}`;
}

/** Entry distance of a ray (unit `dir`) into a vertical capsule on the floor. */
function rayCapsule(origin: number[], dir: number[], x: number, y: number, radius: number, height: number): number | undefined {
	const flat = Math.hypot(dir[0]!, dir[1]!);
	if (flat < 1e-6) return undefined;
	const [ux, uy] = [dir[0]! / flat, dir[1]! / flat];
	const [ox, oy] = [x - origin[0]!, y - origin[1]!];
	const along = ox * ux + oy * uy;
	const off2 = ox * ox + oy * oy - along * along;
	if (along <= 0 || off2 > radius * radius) return undefined;
	const entry = (along - Math.sqrt(radius * radius - off2)) / flat;
	const z = origin[2]! + dir[2]! * entry;
	return z >= 0 && z <= height ? entry : undefined;
}

/** Darkness of the view (0..1): the borough's weather, thicker near a boss room. */
const WEATHER_DARK = [0.55, 0.4, 0.22, 0.05];

/** Start drawing on `canvas`; returns the stop function. */
export function startPitLoop(canvas: HTMLCanvasElement, options: PitLoopOptions): () => void {
	const { client } = options;
	const ctx = canvas.getContext('2d')!;
	const keys = new Set<string>();
	const down = (e: KeyboardEvent) => {
		if (['KeyW', 'KeyA', 'KeyS', 'KeyD', 'Space', 'KeyE', 'KeyQ'].includes(e.code)) e.preventDefault();
		keys.add(e.code);
	};
	const up = (e: KeyboardEvent) => keys.delete(e.code);
	window.addEventListener('keydown', down);
	window.addEventListener('keyup', up);
	const pacer = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });

	// ---- ranged: aim with the mouse (first person: pointer lock turns
	// you), R or a click looses an arrow; markers yellow (as you saw it),
	// then green / red with the room's verdict ----
	let yaw = Math.PI / 2;
	let fireQueued = false;
	let lastFire = 0;
	let nextShot = 0;
	type Shot = { zone: string; point: [number, number]; predicted: number | undefined; verdict?: { hit?: number; x: number; y: number }; at: number };
	const shots = new Map<number, Shot>();
	const stats = { shots: 0, confirmed: 0, agreed: 0, resolved: 0 };
	const first = () => options.firstPerson?.() ?? false;
	const offFail = client.on('handoffFailed', (f) => {
		canvas.dataset.handoffFailed = `${canvas.dataset.handoffFailed ?? ''} ${f.from}→${f.to} ${f.stage} ${f.reason};`;
	});
	const offClosed = client.on('zoneClosed', (zone, info) => {
		canvas.dataset.zoneClosed = `${canvas.dataset.zoneClosed ?? ''} ${zone} ${info.reason} ${info.code ?? ''};`;
	});
	const offShots = onGameEvent(client, EVENTS, 'shot', (v) => {
		const shot = shots.get(v.shot);
		if (shot === undefined || shot.verdict !== undefined) return;
		shot.verdict = { hit: v.hit, x: v.x, y: v.y };
		shot.at = performance.now();
		stats.resolved += 1;
		if (v.hit !== undefined) stats.confirmed += 1;
		if (v.hit === shot.predicted) stats.agreed += 1;
	});
	const onMove = (e: MouseEvent) => {
		if (first()) {
			if (document.pointerLockElement === canvas) yaw -= e.movementX * 0.004;
			return;
		}
		// Top-down: aim at the cursor (you are at the centre).
		const r = canvas.getBoundingClientRect();
		const cx = ((e.clientX - r.left) * canvas.width) / r.width - canvas.width / 2;
		const cy = ((e.clientY - r.top) * canvas.height) / r.height - canvas.height / 2;
		if (Math.hypot(cx, cy) > 4) yaw = Math.atan2(-cy, cx);
	};
	const onDown = (e: MouseEvent) => {
		if (e.button !== 0) return;
		if (first() && document.pointerLockElement !== canvas) {
			Promise.resolve(canvas.requestPointerLock?.()).catch(() => undefined);
			return;
		}
		fireQueued = true;
	};
	const onKey = (e: KeyboardEvent) => {
		if (e.code === 'KeyR' && !e.repeat) fireQueued = true;
	};
	canvas.addEventListener('mousemove', onMove);
	document.addEventListener('mousemove', onMove);
	canvas.addEventListener('mousedown', onDown);
	window.addEventListener('keydown', onKey);

	/** The arrow fields of this frame, when one is loosed. */
	const loose = (): Partial<PitInput> => {
		const zone = client.authority;
		const hello = options.hello();
		const now = performance.now();
		if (!fireQueued || zone === undefined || hello === undefined || now - lastFire < (PIT_RULES.fireCooldown * 1000) / 30) return {};
		fireQueued = false;
		const me = client.render().find((e) => e.id === hello.entity && isPlayer(e) && e.zoneId === zone.zoneId);
		if (me === undefined || !zone.connection.canSendInput()) return {};
		const p = playerFields(me);
		// Top-down: level from the hip. First person: from the eye, pitched
		// at the middle of the mob straight ahead (the view has no pitch).
		const fp = first();
		const origin = [p.x, p.y, p.z + (fp ? PIT_RULES.eyeHeight : PIT_RULES.hipHeight)];
		let pitch = 0;
		if (fp) {
			const ahead = client
				.render()
				.filter((e) => isMob(e) && !e.stale && e.zoneId === zone.zoneId)
				.map((e) => mobFields(e))
				.map((m) => ({ m, d: rayCapsule([p.x, p.y, 0.1], [Math.cos(yaw), Math.sin(yaw), 0], m.x, m.y, TIER_RADIUS[m.tier]?.radius ?? 0.5, 10) }))
				.filter((a) => a.d !== undefined)
				.sort((a, b) => a.d! - b.d!)[0];
			if (ahead !== undefined) pitch = Math.atan2((PIT_RULES.mobHeights[ahead.m.tier] ?? 1) / 2 - origin[2]!, ahead.d! + 0.3);
		}
		const dir = [Math.cos(yaw) * Math.cos(pitch), Math.sin(yaw) * Math.cos(pitch), Math.sin(pitch)];
		const context = client.inputContext(zone)!;
		const delayMs = Math.max(0, zone.connection.tickTime(context.clientTick) - (zone.connection.serverNow() - context.interpolationDelayMs));
		let predicted: number | undefined;
		let best = PIT_RULES.range;
		for (const e of client.render()) {
			if (!isMob(e) || e.stale || e.zoneId !== zone.zoneId) continue;
			const m = mobFields(e);
			const d = rayCapsule(origin, dir, m.x, m.y, TIER_RADIUS[m.tier]?.radius ?? 0.5, PIT_RULES.mobHeights[m.tier] ?? 1);
			if (d !== undefined && d < best) {
				best = d;
				predicted = e.id;
			}
		}
		nextShot = (nextShot % 0xffff) + 1;
		shots.set(nextShot, { zone: zone.zoneId, point: [origin[0]! + dir[0]! * best, origin[1]! + dir[1]! * best], predicted, at: now });
		if (shots.size > 64) shots.delete(shots.keys().next().value!);
		stats.shots += 1;
		lastFire = now;
		const compensated = options.compensated?.() ?? true;
		return { buttons: BUTTON.fire | (compensated ? 0 : BUTTON.noComp), shot: nextShot, delayMs: Math.min(0xffff, Math.round(delayMs)), pitch };
	};

	const steer = (): PitInput => {
		const base = steerMove();
		const arrow = loose();
		return {
			...base,
			// First person faces the aim (and the stick turns with it).
			buttons: base.buttons | (arrow.buttons ?? 0) | (first() ? BUTTON.aim : 0),
			yaw,
			pitch: arrow.pitch ?? 0,
			shot: arrow.shot ?? 0,
			delayMs: arrow.delayMs ?? 0
		};
	};

	const steerMove = (): Pick<PitInput, 'mx' | 'my' | 'buttons'> => {
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
			// Face the prey (an arrow loosed meanwhile flies at it).
			yaw = Math.atan2(target.m.y - p.y, target.m.x - p.x);
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			return {
				mx: Math.round((target.m.x - p.x) * k),
				my: Math.round((target.m.y - p.y) * k),
				buttons: near ? BUTTON.attack : 0
			};
		}
		const axis = (neg: string, pos: string) => (keys.has(pos) ? 127 : 0) - (keys.has(neg) ? 127 : 0);
		let [mx, my] = [axis('KeyA', 'KeyD'), axis('KeyS', 'KeyW')];
		if (first()) {
			// W walks where you look, D to its right.
			const [fx, fy] = [Math.cos(yaw), Math.sin(yaw)];
			[mx, my] = [Math.round(fy * mx + fx * my), Math.round(-fx * mx + fy * my)];
		}
		return {
			mx: Math.max(-127, Math.min(127, mx)),
			// Screen y grows downwards; world y grows upwards.
			my: Math.max(-127, Math.min(127, my)),
			buttons: (keys.has('Space') ? BUTTON.attack : 0) | (keys.has('KeyE') ? BUTTON.dash : 0) | (keys.has('KeyQ') ? BUTTON.jump : 0)
		};
	};

	// The rooms around are drawn from their seeded layout (`pit_room`, the
	// Rust generator as WASM): biome floor, walls, doorways, pillars.
	const pures = createMobpitPures();
	/** Pillars are the same in every room: learned from the replicated ones. */
	let pillars: { x: number; y: number; r: number }[] = [];
	let lastMe: { x: number; y: number } | undefined;
	let lastZone: string | undefined;
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
				if (jump > Number(canvas.dataset.jump ?? 0)) {
					canvas.dataset.jump = jump.toFixed(3);
					canvas.dataset.jumpDebug = JSON.stringify({ from: lastMe, to: me, zone: meEntity?.zoneId, lastZone, room, predicted: meEntity?.predicted, fields: meEntity?.fields });
				}
			}
			lastMe = me;
			lastZone = meEntity?.zoneId;
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
		ctx.setTransform(1, 0, 0, 1, 0, 0);
		ctx.fillStyle = '#0b090d';
		ctx.fillRect(0, 0, width, height);
		// First person: the world turns so that your aim points up.
		if (first()) {
			ctx.translate(width / 2, height / 2);
			ctx.rotate(yaw - Math.PI / 2);
			ctx.translate(-width / 2, -height / 2);
		}
		const seed = hello?.seed ?? PIT_WORLD.defaultSeed;
		const [cx, cy] = [Math.round(centre.x / ROOM), Math.round(centre.y / ROOM)];
		for (let dy = -1; dy <= 1; dy += 1) {
			for (let dx = -1; dx <= 1; dx += 1) {
				const [rx, ry] = [cx + dx, cy + dy];
				const [ox, oy] = [rx * ROOM, ry * ROOM];
				const biome = pures.loaded() ? pures.pitRoom({ seed, x: rx, y: ry }).biome : undefined;
				ctx.fillStyle = biome === undefined ? '#120f14' : `${BIOME_COLOR[biome] ?? '#333333'}30`;
				ctx.fillRect(...at(ox - HALF, oy + HALF), 2 * HALF * scale, 2 * HALF * scale);
				// The room's seeded obstacles (what the step collides with).
				const obstacles = pures.loaded() ? pures.pitRoom({ seed, x: rx, y: ry }).obstacles : pillars;
				for (const p of obstacles) circle(ox + p.x, oy + p.y, p.r, OBSTACLE_COLOR[(p as { look?: number }).look ?? 0] ?? '#3a3443');
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
				circle(w.x, w.y, look?.radius ?? 0.5, tinted(TIER_COLOR[m.tier] ?? '#9ad16b', options.tint?.() ?? 0));
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
				// A jump: a shadow on the floor, the body up and bigger.
				if (p.z > 0.01) circle(w.x, w.y, PLAYER_RADIUS, 'rgba(0,0,0,0.35)');
				if (isMe) canvas.dataset.z = p.z.toFixed(2);
				circle(w.x, w.y + p.z * 0.6, PLAYER_RADIUS * (1 + p.z * 0.25), p.dead ? '#555' : isMe ? '#ffd166' : '#6ec3ff');
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
		// Arrows: the aim line, markers (yellow: as you saw it; green / red:
		// the room's verdict), the counters for the headless test.
		if (me !== undefined) {
			const [ax, ay] = at(me.x, me.y);
			const [bx, by] = at(me.x + Math.cos(yaw) * 6, me.y + Math.sin(yaw) * 6);
			ctx.strokeStyle = 'rgba(255,209,102,0.6)';
			ctx.setLineDash([4, 4]);
			ctx.beginPath();
			ctx.moveTo(ax, ay);
			ctx.lineTo(bx, by);
			ctx.stroke();
			ctx.setLineDash([]);
		}
		const tnow = performance.now();
		for (const [id, shot] of shots) {
			const r = parseRoom(shot.zone);
			if (r === undefined) continue;
			const p = shot.verdict ?? { x: shot.point[0], y: shot.point[1], hit: undefined };
			const color = shot.verdict === undefined ? '#ffd166' : shot.verdict.hit !== undefined ? '#3ddc84' : '#ff5252';
			circle(r[0] * ROOM + p.x, r[1] * ROOM + p.y, 0.3, color);
			if (shot.verdict !== undefined && tnow - shot.at > 1200) shots.delete(id);
		}
		canvas.dataset.shots = String(stats.shots);
		canvas.dataset.confirmed = String(stats.confirmed);
		canvas.dataset.agreed = String(stats.agreed);
		canvas.dataset.resolved = String(stats.resolved);
		ctx.setTransform(1, 0, 0, 1, 0, 0);
		if (first()) {
			// Crosshair: ahead of you, straight up the screen.
			ctx.strokeStyle = '#fff';
			ctx.beginPath();
			ctx.moveTo(width / 2 - 8, height / 2 - 6 * scale);
			ctx.lineTo(width / 2 + 8, height / 2 - 6 * scale);
			ctx.moveTo(width / 2, height / 2 - 6 * scale - 8);
			ctx.lineTo(width / 2, height / 2 - 6 * scale + 8);
			ctx.stroke();
		}
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
		window.removeEventListener('keydown', onKey);
		canvas.removeEventListener('mousemove', onMove);
		document.removeEventListener('mousemove', onMove);
		canvas.removeEventListener('mousedown', onDown);
		offShots();
		offFail();
		offClosed();
	};
}
