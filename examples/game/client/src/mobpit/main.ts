/**
 * Mob Pit page: Babylon scene over `createMobPitClient`.
 *
 * What to look at: rooms are celld Durable Objects on a near-infinite
 * seeded grid, laid out as one continuous world (room `(x, y)` at
 * `(x, y) × ROOM_SIZE`): the rooms around yours are drawn from the seeded
 * layout (`pitRoom`, Rust as WASM: biome floor, walls, doorways, pillars),
 * so you see into the next room before you go. Walk through a doorway (the
 * gaps in the middle of each wall) and you are simply in the next room —
 * the room hands you over at the same world point; nothing moves. A
 * floating origin at your room keeps coordinates small. The minimap (bottom right) shows every known room
 * live — active, dormant (just left), hibernated (asleep in storage) — from
 * the `zone_status` read model. Come back to a room after it slept: it
 * wakes from its snapshot with the mobs you left (toast). Kill a mob
 * (Space): the grey "saving…" popup turns into "+XP saved ✓" when the kill
 * settled in your Crawler cell. The debug overlay (O) shows which celld
 * node and isolate run the room — celld decides, the game never does.
 *
 * Network panel (the HUD): latency / jitter / loss sliders apply to every
 * room on celld (each room object delays its sockets' frames both ways);
 * your capsule is predicted (the Rust step as WASM) and reconciled — P
 * turns prediction off to feel the raw round trip, G shows the server's
 * authoritative position as a wireframe ghost, and the readout counts
 * pending inputs and corrections. The ring on the floor is your interest
 * radius: mobs and players beyond it are not replicated to you.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { DynamicTexture } from '@babylonjs/core/Materials/Textures/dynamicTexture';
import { Color3, Color4 } from '@babylonjs/core/Maths/math.color';
import { Scene } from '@babylonjs/core/scene';
import { Vector3 } from '@babylonjs/core/Maths/math.vector';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { TransformNode } from '@babylonjs/core/Meshes/transformNode';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import {
	BUTTON,
	CONTENT_PACKS,
	GATE_ROOM,
	PIT_RULES,
	PIT_WORLD,
	isMob,
	isPillar,
	isPit,
	mobFields,
	PATTERNS,
	WEATHER,
	pitFields,
	playerFields,
	type PitFields,
	type ContentPack,
	type PitInput
} from '../generated/mobpit/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createRing, createStage, material } from '../shared/scene.ts';
import {
	createBoardWatcher,
	createMobPitClient,
	DEFAULT_ZONE,
	MobPitApi,
	parseRoom,
	ROOM_SIZE,
	roomZone,
	TIERS,
	type BoardSnapshot,
	type ShotRecord,
	type RoomInfo,
	type RoomStatus,
	type TiersAround
} from './client.ts';

/** The rooms' floor, generated from the simulation's constants (`game_sim::mobpit::pit_world`). */
const { half: HALF, playerRadius: PLAYER_RADIUS, pillars: PILLARS } = PIT_WORLD;
/** Rooms drawn around yours (2 → 5×5). */
const ROOM_VIEW = 2;
/** Wall thickness and height (m), drawn just outside each room's floor. */
const WALL_T = 0.3;
const WALL_H = 0.6;
/** Swing cone, from the server's rules: reach to a mob's edge, plus the body. */
const { attackRange, attackHalfAngle: SWING_HALF, swingFrames: SWING_FRAMES } = PIT_RULES;
const ATTACK_REACH = attackRange + PLAYER_RADIUS;

const params = new URLSearchParams(location.search);
/** `?zone=` forces a room; otherwise you start where you last were (`/dev/mobpit/start`). */
const forcedZone = params.get('zone') ?? undefined;
const storedName = (() => {
	try {
		return localStorage.getItem('game-demos.name') ?? undefined;
	} catch {
		return undefined;
	}
})();
const name = params.get('name') ?? storedName;

const api = new DevApi({ name: () => name });
const pitApi = new MobPitApi();
// `?old=1` simulates a client built before the next content update: it
// keeps its pack when the rooms move on, and only the entrance reloads it.
const pinContent = params.has('old');
const pit = createMobPitClient({
	api,
	pinContent,
	liveVersion: async () => (await pitApi.content()).state?.version ?? ''
});
const { client } = pit;

const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: HALF, background: '#120f14' });
const { scene } = stage;

// ---- weather and darkness (Boss Ripple's sky, now the borough's) ----
// The borough's weather lifts as its bosses fall (storm → rain → clearing →
// clear); within it, the fog thickens and the light dims the closer the room
// is to a boss room (`boss_distance`, the shared Rust room generator).
// The fog is linear and starts at the player, not at the camera: whatever the
// zoom (or first person), the ground around you stays readable and only the
// distance fades. `clearM` is how far past the player it stays clear, `fadeM`
// how much further until the fog is solid.
scene.fogMode = Scene.FOGMODE_LINEAR;
const SKY: Record<string, { clear: string; clearM: number; fadeM: number; light: number }> = {
	storm: { clear: '#1c2230', clearM: 12, fadeM: 34, light: 0.7 },
	rain: { clear: '#2e3a4a', clearM: 16, fadeM: 40, light: 0.8 },
	clearing: { clear: '#56708c', clearM: 24, fadeM: 50, light: 0.95 },
	clear: { clear: '#8fc3f0', clearM: 36, fadeM: 70, light: 1.1 }
};
const lights = scene.lights.map((l) => ({ light: l, base: l.intensity }));
/** Light tint per biome (highlands, steppe, forest, meadow). */
const BIOME_LIGHT = ['#d6dcea', '#ffe1b3', '#b9dfb4', '#eaf6cf'];
/** Fog and darkness of the current room: its borough's weather × closeness to a boss. */
function roomSky(): { clear: string; clearM: number; fadeM: number; light: number; near: number } {
	const sky = SKY[WEATHER[pit.world()?.weather ?? 0] ?? 'storm'] ?? SKY.storm!;
	const here = parseRoom(pit.room() ?? DEFAULT_ZONE);
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	const distance = here !== undefined && pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: here[0], y: here[1] }).bossDistance : 6;
	// 1 in a boss room, 0 six rooms away. Near a boss the fog closes in to
	// half the distance and the light drops by a fifth, never to black.
	const near = Math.max(0, 1 - distance / 6);
	const close = 1 - 0.5 * near;
	return { clear: sky.clear, clearM: sky.clearM * close, fadeM: sky.fadeM * close, light: sky.light * (1 - 0.2 * near), near };
}
let fogClearM = SKY.storm!.clearM;
let fogFadeM = SKY.storm!.fadeM;
function applySky(dtMs: number): void {
	const target = roomSky();
	const want = Color4.FromHexString(`${target.clear}ff`).scale(1 - 0.25 * target.near);
	const k = 1 - Math.exp(-dtMs / 600);
	scene.clearColor = Color4.Lerp(scene.clearColor, new Color4(want.r, want.g, want.b, 1), k);
	scene.fogColor = new Color3(scene.clearColor.r, scene.clearColor.g, scene.clearColor.b);
	fogClearM += (target.clearM - fogClearM) * k;
	fogFadeM += (target.fadeM - fogFadeM) * k;
	// Measured from the camera, so offset by the camera's distance to the player.
	const toPlayer = stage.view() === 'first' ? 0 : stage.camera.radius;
	scene.fogStart = toPlayer + fogClearM;
	scene.fogEnd = scene.fogStart + fogFadeM;
	const storm = (pit.world()?.weather ?? 0) === 0;
	const flash = storm && Math.random() < 0.004 ? 1.8 : 1;
	for (const { light, base } of lights) light.intensity += (base * target.light * flash - light.intensity) * Math.min(1, k * (flash > 1 ? 20 : 1));
	// The room's biome tints the light.
	const here = parseRoom(pit.room() ?? DEFAULT_ZONE);
	if (here !== undefined && pit.pures.loaded()) {
		const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
		const tint = Color3.FromHexString(BIOME_LIGHT[pit.pures.pitRoom({ seed, x: here[0], y: here[1] }).biome] ?? '#ffffff');
		for (const { light } of lights) light.diffuse = Color3.Lerp(light.diffuse, tint, k);
	}
	canvas.dataset.fog = `${fogClearM.toFixed(1)}/${fogFadeM.toFixed(1)}`;
}

const hud = new Hud({
	title: 'Mob Pit',
	hint: 'WASD move · Space attack · R / click shoot · E dash · Q jump (over a slam) · V first person · C lag compensation · walk through a doorway to the next room · O debug overlay · P prediction · G server ghost. Rooms run on celld; kills are durable facts.',
	client,
	devApi: api
});
let debug = params.get('debug') !== '0';
hud.toggle('Debug overlay (celld node per room)', debug, (on) => (debug = on), 'o');
hud.toggle('Client prediction', true, (on) => client.setPredictionEnabled(on), 'p');
hud.toggle('First-person view (click to look, R / click fires)', false, (on) => stage.setView(on ? 'first' : 'third'), 'v');
hud.toggle('Lag compensation (ranged)', true, (on) => (compensated = on), 'c');
let ghostOn = true;
hud.toggle('Server ghost', true, (on) => (ghostOn = on), 'g');
let corrections = 0;
let lastCorrection = 0;
client.on('correction', (c) => {
	corrections += 1;
	lastCorrection = Math.hypot(c.errors.x ?? 0, c.errors.y ?? 0);
});

// ---- side panels: XP bar, durable facts graph, mini leaderboard ----
const xpPanel = div('pit-xp', document.body);
const xpLabel = div('pit-xp-label', xpPanel);
const xpBar = div('pit-xp-bar', xpPanel);
const xpFill = div('pit-xp-fill', xpBar);
const factsPanel = div('pit-facts', document.body);
factsPanel.innerHTML = `<h2>Durable facts</h2>
	<div class="pit-big"><span data-k="pending">0</span><small> pending</small> <span data-k="saved">0</span><small> saved ✓</small></div>
	<canvas width="260" height="90"></canvas>
	<div class="pit-legend"><span class="c-pending">■ pending facts</span> <span class="c-tick">■ tick interval (ms)</span></div>
	<div class="pit-note" data-k="note"></div>
	<h2>Leaderboard <a href="/leaderboard/" target="_blank">open ↗</a></h2>
	<ol data-k="board"></ol>`;
const graph = factsPanel.querySelector('canvas') as HTMLCanvasElement;
const field = (k: string) => factsPanel.querySelector(`[data-k="${k}"]`) as HTMLElement;
const deathBanner = div('pit-death', document.body);
// The borough's world event (scheduled by its cell): a banner while it runs.
const eventBanner = div('pit-event', document.body);
eventBanner.style.display = 'none';
const EVENT_TEXT: Record<string, string> = {
	invasion: 'INVASION — the mobs of this borough come at you from afar and rise again at once',
	frenzy: 'FRENZY — the mobs of this borough are 40 % faster',
	blessing: 'BLESSING — you heal five times as fast in this borough'
};
deathBanner.style.display = 'none';
// ---- live content: freeze overlay, old-client banner, admin panel ----
const freezeOverlay = div('pit-freeze', document.body);
freezeOverlay.style.display = 'none';
const staleBanner = div('pit-stale', document.body);
staleBanner.style.display = 'none';
client.on('resume', (zone, message) => {
	if (zone !== client.authority?.zoneId || message.version === '' && message.notes === '') return;
	const pack = CONTENT_PACKS.find((p) => p.version === message.version);
	toast(`World updated to ${pack?.name ?? message.version}${message.notes === '' ? '' : ` — ${message.notes}`}`);
});
const adminParam = params.get('admin');
const adminSecret = (() => {
	try {
		if (adminParam !== null) localStorage.setItem('game-demos.mobpit.admin', adminParam);
		return adminParam ?? localStorage.getItem('game-demos.mobpit.admin') ?? undefined;
	} catch {
		return adminParam ?? undefined;
	}
})();
if (adminSecret !== undefined && adminSecret !== '') {
	const panel = div('pit-admin', document.body);
	panel.innerHTML = `<h2>World admin <small>(dev, admin secret)</small></h2>
		<input data-k="notes" placeholder="change notes (default: the pack's)" />
		<label><input type="checkbox" data-k="hard" /> hard (reload old clients now)</label>
		<div data-k="packs"></div><div class="pit-note" data-k="result"></div>`;
	const q = (k: string) => panel.querySelector(`[data-k="${k}"]`) as HTMLInputElement;
	for (const pack of CONTENT_PACKS) {
		const button = document.createElement('button');
		button.textContent = `Roll out ${pack.name}`;
		button.addEventListener('click', () => {
			q('result').textContent = 'rolling out…';
			pitApi
				.rollout(adminSecret, { version: pack.version, notes: q('notes').value.trim() || undefined, hard: q('hard').checked })
				.then(() => (q('result').textContent = `rolling out ${pack.name}: every room freezes, then resumes on it`))
				.catch((error: unknown) => (q('result').textContent = String(error)));
		});
		q('packs').append(button);
	}
}

function div(className: string, parent: HTMLElement): HTMLDivElement {
	const d = document.createElement('div');
	d.className = className;
	parent.append(d);
	return d;
}

// ---- rooms: minimap from the zone status read model, toasts, debug ----
const roomsPanel = div('pit-rooms', document.body);
roomsPanel.innerHTML = `<h2>Rooms <small data-k="here"></small></h2>
	<canvas width="256" height="256" title="Scroll to zoom"></canvas>
	<div class="pit-legend"><span class="c-active">■ active</span> <span class="c-dormant">■ dormant</span> <span class="c-hibernated">■ asleep</span> <span class="c-cold">■ never ran</span> <span class="c-watched">□ watched</span> <span class="c-me">▲ you</span> <span class="c-other">● players</span> <span class="c-mob">● mobs</span></div>
	<div class="pit-debug" data-k="debug"></div>`;
const roomsMap = roomsPanel.querySelector('canvas') as HTMLCanvasElement;
const roomsField = (k: string) => roomsPanel.querySelector(`[data-k="${k}"]`) as HTMLElement;
const toasts = div('pit-toasts', document.body);
const rooms = new Map<string, RoomStatus>();
let roomInfo: RoomInfo | undefined;
const STATUS_COLOR: Record<string, string> = { active: '#4caf50', dormant: '#f2c94c', hibernated: '#6c7aa0' };
const BIOME_COLOR = ['#8a8f99', '#c9a86a', '#3f7d4e', '#7fbf6a'];

function toast(text: string): void {
	const t = div('pit-toast', toasts);
	t.textContent = text;
	setTimeout(() => t.remove(), 5000);
}

async function pollRooms(): Promise<void> {
	for (;;) {
		try {
			const { rooms: rows } = await pitApi.rooms();
			for (const row of rows) {
				const before = rooms.get(row.zone);
				if (before !== undefined && before.status !== row.status) {
					if (row.status === 'active' && row.restored && row.activations > before.activations) {
						toast(`${row.zone} woke up from hibernation — its mobs are back`);
					} else if (row.status === 'hibernated') {
						toast(`${row.zone} went to sleep (snapshot in celld storage)`);
					}
				}
				rooms.set(row.zone, row);
			}
		} catch {
			// The services restarting: keep the last view.
		}
		await new Promise((r) => setTimeout(r, 500));
	}
}
void pollRooms();

async function pollRoomInfo(): Promise<void> {
	for (;;) {
		const here = pit.room();
		if (debug && here !== undefined) roomInfo = await pitApi.roomInfo(here).catch(() => roomInfo);
		await new Promise((r) => setTimeout(r, 1000));
	}
}
void pollRoomInfo();

let tiers: TiersAround | undefined;
async function pollTiers(): Promise<void> {
	for (;;) {
		const here = pit.room();
		if (here !== undefined) tiers = await pitApi.tiers(here).catch(() => tiers);
		await new Promise((r) => setTimeout(r, 2000));
	}
}
void pollTiers();

/** A dot on the minimap, in world metres (room (x, y) is centred on (x, y) × ROOM_SIZE). */
type MiniDot = { wx: number; wy: number; kind: 'me' | 'player' | 'mob'; tier: number; face: number; dead: boolean };
const miniDots: MiniDot[] = [];
let miniMe: MiniDot | undefined;
/** Metres across the minimap; the mouse wheel zooms. */
let miniSpanM = 4 * ROOM_SIZE;
roomsMap.addEventListener(
	'wheel',
	(event) => {
		event.preventDefault();
		miniSpanM = Math.min(12 * ROOM_SIZE, Math.max(1.5 * ROOM_SIZE, miniSpanM * Math.exp(event.deltaY * 0.0015)));
	},
	{ passive: false }
);
const roomLayouts = new Map<string, { biome: number; obstacles: { x: number; y: number; r: number }[] }>();
function roomLayout(seed: number, x: number, y: number): { biome: number; obstacles: { x: number; y: number; r: number }[] } | undefined {
	if (!pit.pures.loaded()) return undefined;
	const key = `${seed}:${x}:${y}`;
	let layout = roomLayouts.get(key);
	if (layout === undefined) {
		const room = pit.pures.pitRoom({ seed, x, y });
		layout = { biome: room.biome, obstacles: room.obstacles.map(({ x, y, r }) => ({ x, y, r })) };
		if (roomLayouts.size > 400) roomLayouts.clear();
		roomLayouts.set(key, layout);
	}
	return layout;
}
const mod = (n: number, m: number) => ((n % m) + m) % m;

/**
 * The minimap: a top-down map that follows you every frame. Rooms show their
 * biome floor, walls with doorway gaps and obstacles; the room's state (zone
 * status read model) is the tile's border; you, other players and mobs are
 * dots at their live positions (your room and the watched rooms next door).
 */
function drawRooms(): void {
	const ctx = roomsMap.getContext('2d');
	const here = parseRoom(pit.room() ?? DEFAULT_ZONE);
	if (ctx === null || here === undefined) return;
	const { width, height } = roomsMap;
	const s = width / miniSpanM;
	// Centre on you (between frames of a handoff: your room's centre).
	const cx = miniMe?.wx ?? here[0] * ROOM_SIZE;
	const cy = miniMe?.wy ?? here[1] * ROOM_SIZE;
	const toMap = (wx: number, wy: number): [number, number] => [width / 2 + (wx - cx) * s, height / 2 - (wy - cy) * s];
	ctx.fillStyle = '#0b0d12';
	ctx.fillRect(0, 0, width, height);
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	const reach = Math.ceil(miniSpanM / ROOM_SIZE / 2) + 1;
	const rx0 = Math.round(cx / ROOM_SIZE);
	const ry0 = Math.round(cy / ROOM_SIZE);
	const door = PIT_WORLD.doorHalf;
	// Node names only say something when the rooms run on more than one node.
	const manyNodes = new Set([...rooms.values()].map((r) => r.node).filter((n) => n !== '')).size > 1;
	ctx.font = '10px monospace';
	for (let ry = ry0 - reach; ry <= ry0 + reach; ry++) {
		for (let rx = rx0 - reach; rx <= rx0 + reach; rx++) {
			const zone = roomZone(rx, ry);
			const row = rooms.get(zone);
			const layout = roomLayout(seed, rx, ry);
			const [x0, y0] = toMap(rx * ROOM_SIZE - HALF, ry * ROOM_SIZE + HALF);
			const size = HALF * 2 * s;
			// Floor: biome colour, dimmer for rooms that never ran.
			ctx.fillStyle = `${BIOME_COLOR[layout?.biome ?? 0] ?? '#333'}${row === undefined ? '33' : '66'}`;
			ctx.fillRect(x0, y0, size, size);
			// Obstacles.
			if (layout !== undefined && s > 0.6) {
				ctx.fillStyle = '#0b0d12aa';
				for (const o of layout.obstacles) {
					const [ox, oy] = toMap(rx * ROOM_SIZE + o.x, ry * ROOM_SIZE + o.y);
					ctx.beginPath();
					ctx.arc(ox, oy, Math.max(1, o.r * s), 0, Math.PI * 2);
					ctx.fill();
				}
			}
			// Walls with a doorway gap in the middle of each.
			ctx.strokeStyle = '#9aa3b2';
			ctx.lineWidth = 1;
			const wall = (ax: number, ay: number, bx: number, by: number) => {
				ctx.beginPath();
				ctx.moveTo(...toMap(rx * ROOM_SIZE + ax, ry * ROOM_SIZE + ay));
				ctx.lineTo(...toMap(rx * ROOM_SIZE + bx, ry * ROOM_SIZE + by));
				ctx.stroke();
			};
			for (const sign of [-1, 1]) {
				wall(-HALF, sign * HALF, -door, sign * HALF);
				wall(door, sign * HALF, HALF, sign * HALF);
				wall(sign * HALF, -HALF, sign * HALF, -door);
				wall(sign * HALF, door, sign * HALF, HALF);
			}
			// Room state as the tile's inner border.
			const status = row === undefined ? undefined : STATUS_COLOR[row.status];
			if (status !== undefined) {
				ctx.strokeStyle = status;
				ctx.lineWidth = 2;
				ctx.strokeRect(x0 + 3, y0 + 3, size - 6, size - 6);
			}
			if (pit.neighbours().get(zone) === 'watching' && !(rx === here[0] && ry === here[1])) {
				ctx.strokeStyle = '#4dd0e1';
				ctx.lineWidth = 1;
				ctx.setLineDash([3, 3]);
				ctx.strokeRect(x0 + 6, y0 + 6, size - 12, size - 12);
				ctx.setLineDash([]);
			}
			if (size > 36) {
				ctx.fillStyle = '#e6e9ef';
				if (row !== undefined && row.players > 0) ctx.fillText(`${row.players}p`, x0 + 6, y0 + 14);
				if (debug && manyNodes && row !== undefined && row.node !== '') ctx.fillText(row.node.replace(/^(celld-|node-)/, ''), x0 + 6, y0 + size - 6);
			}
			// Boss rooms: a red mark (grey while the boss is down) and the hood's tier.
			const hood = tiers?.hoods.find((h) => h.bossRoom[0] === rx && h.bossRoom[1] === ry);
			if (hood !== undefined) {
				const [bx, by] = toMap(rx * ROOM_SIZE, ry * ROOM_SIZE);
				ctx.fillStyle = hood.bossDown ? '#777' : '#ff2d55';
				ctx.beginPath();
				ctx.arc(bx, by, Math.max(4, 3 * s), 0, Math.PI * 2);
				ctx.fill();
				ctx.fillStyle = '#fff';
				ctx.fillText(`T${hood.tier}`, bx + 7, by - 5);
			}
		}
	}
	// Hood (3 × 3 rooms) and borough (9 × 9) borders: the left / bottom wall of
	// every room whose index is a multiple of 3 (9).
	for (let r = rx0 - reach; r <= rx0 + reach; r++) {
		if (mod(r, 3) !== 0) continue;
		const borough = mod(r, 9) === 0;
		const [x] = toMap(r * ROOM_SIZE - HALF, 0);
		ctx.strokeStyle = borough ? '#ffd166' : '#8a6dd1';
		ctx.lineWidth = borough ? 2.5 : 1.25;
		ctx.beginPath();
		ctx.moveTo(x, 0);
		ctx.lineTo(x, height);
		ctx.stroke();
	}
	for (let r = ry0 - reach; r <= ry0 + reach; r++) {
		if (mod(r, 3) !== 0) continue;
		const borough = mod(r, 9) === 0;
		const [, y] = toMap(0, r * ROOM_SIZE - HALF);
		ctx.strokeStyle = borough ? '#ffd166' : '#8a6dd1';
		ctx.lineWidth = borough ? 2.5 : 1.25;
		ctx.beginPath();
		ctx.moveTo(0, y);
		ctx.lineTo(width, y);
		ctx.stroke();
	}
	// Your interest ring.
	const viewRadius = pit.hello()?.viewRadius;
	if (miniMe !== undefined && viewRadius !== undefined) {
		ctx.strokeStyle = '#ffffff22';
		ctx.lineWidth = 1;
		ctx.beginPath();
		ctx.arc(width / 2, height / 2, viewRadius * s, 0, Math.PI * 2);
		ctx.stroke();
	}
	// Mobs, other players, then you on top.
	for (const d of miniDots) {
		const [x, y] = toMap(d.wx, d.wy);
		if (x < -4 || y < -4 || x > width + 4 || y > height + 4) continue;
		ctx.fillStyle = d.kind === 'mob' ? (d.tier >= 4 ? '#ff2d55' : '#ff8a65') : d.dead ? '#666' : '#64b5f6';
		ctx.beginPath();
		ctx.arc(x, y, d.kind === 'mob' ? (d.tier >= 4 ? 3.5 : 2) : 3, 0, Math.PI * 2);
		ctx.fill();
	}
	if (miniMe !== undefined) {
		const [x, y] = toMap(miniMe.wx, miniMe.wy);
		const f = miniMe.face;
		const tip = (a: number, r: number): [number, number] => [x + Math.cos(f + a) * r, y - Math.sin(f + a) * r];
		ctx.fillStyle = miniMe.dead ? '#888' : '#ffffff';
		ctx.strokeStyle = '#0b0d12';
		ctx.lineWidth = 1.5;
		ctx.beginPath();
		ctx.moveTo(...tip(0, 8));
		ctx.lineTo(...tip(2.5, 5));
		ctx.lineTo(...tip(-2.5, 5));
		ctx.closePath();
		ctx.stroke();
		ctx.fill();
	}
}

function renderDebug(): void {
	const el = roomsField('debug');
	roomsField('here').textContent = pit.room() ?? '';
	if (!debug) {
		el.textContent = '';
		return;
	}
	const info = roomInfo;
	if (info === undefined || info.zone !== pit.room()) {
		el.textContent = 'room object: …';
		return;
	}
	const node = info.node === '' ? 'unknown (celld does not tell code its node; set CELLD_ZONE_NODES)' : info.node;
	el.innerHTML = `<b>celld object</b> ${info.zone}<br>node ${node}<br>isolate ${info.isolate} · activation ${info.activation}` +
		`<br>wakes ${info.wakes} · ${info.restored ? 'restored from snapshot' : 'fresh'} · ${info.players} players` +
		`<br>tick ${info.hz} Hz · late p50 ${info.lateMs.p50.toFixed(1)} / p99 ${info.lateMs.p99.toFixed(1)} ms`;
}

// ---- materials and templates ----
const mats = {
	me: material(scene, 'me', '#6cc4ff'),
	other: material(scene, 'other', '#f2a65a'),
	dead: material(scene, 'dead', '#555555', 0.4),
	pillar: material(scene, 'pillar', '#5d5566'),
	blade: material(scene, 'blade', '#e8eef7'),
	swing: material(scene, 'swing', '#4da3ff', 0.3),
	hpBack: material(scene, 'hp-back', '#1b1b1b'),
	hpFill: material(scene, 'hp-fill', '#e04848'),
	tele: material(scene, 'tele', '#ff3b3b', 0.25),
	teleFill: material(scene, 'tele-fill', '#ff3b3b', 0.45),
	ghost: material(scene, 'ghost', '#ffffff', 0.25)
};
mats.ghost.wireframe = true;
for (const m of [mats.swing, mats.tele, mats.teleFill, mats.hpFill]) m.emissiveColor = m.diffuseColor.clone();
mats.blade.emissiveColor = new Color3(0.6, 0.6, 0.7);
const tierMats = new Map<number, StandardMaterial>();
for (const [tier, t] of Object.entries(TIERS)) tierMats.set(Number(tier), material(scene, `mob-${tier}`, t.color));
// The content pack this client runs tints the mobs (its own pack: an old
// client keeps drawing the old one).
let tintedFor: string | undefined;
function tintMobs(pack: ContentPack): void {
	if (tintedFor === pack.version) return;
	tintedFor = pack.version;
	const tint = Color3.FromHexString(`#${pack.tint.toString(16).padStart(6, '0')}`);
	for (const [tier, t] of Object.entries(TIERS)) {
		const base = Color3.FromHexString(t.color);
		tierMats.get(Number(tier))!.diffuseColor = pack.tint === 0 ? base : Color3.Lerp(base, tint, 0.6);
	}
}

// ---- the world: rooms at fixed places, floating origin at your room ----
// The stage's single floor and walls give way to one chunk per room.
for (const mesh of [...scene.meshes]) if (mesh.name === 'floor' || mesh.name === 'wall') mesh.dispose();
type Room = readonly [number, number];
let origin: Room = parseRoom(forcedZone ?? DEFAULT_ZONE) ?? [0, 0];

/** Scene position of a room-local point of room `room`. */
function place(room: Room, x: number, y: number, height = 0): Vector3 {
	return new Vector3((room[0] - origin[0]) * ROOM_SIZE + x, height, (room[1] - origin[1]) * ROOM_SIZE + y);
}

/** Scene position of a room-local point of zone `zoneId` (the origin room if it is not a room). */
function placeIn(zoneId: string, x: number, y: number, height = 0): Vector3 {
	return place(parseRoom(zoneId) ?? origin, x, y, height);
}

/** Room templates, instanced per room: floor per biome, wall segment, doorway gate, pillar. */
const templates = (() => {
	const px = 1024;
	const texture = new DynamicTexture('room-grid', { width: px, height: px }, scene, true);
	const ctx = texture.getContext() as CanvasRenderingContext2D;
	ctx.fillStyle = '#ffffff';
	ctx.fillRect(0, 0, px, px);
	const lines = HALF * 2;
	for (let i = 0; i <= lines; i += 1) {
		const p = (i / lines) * px;
		ctx.strokeStyle = i % 5 === 0 ? '#9aa0aa' : '#c8ccd2';
		ctx.lineWidth = i % 5 === 0 ? 3 : 1.5;
		ctx.beginPath();
		ctx.moveTo(p, 0);
		ctx.lineTo(p, px);
		ctx.moveTo(0, p);
		ctx.lineTo(px, p);
		ctx.stroke();
	}
	texture.update();
	const floors = BIOME_COLOR.map((color, biome) => {
		const m = material(scene, `floor-${biome}`, color);
		m.diffuseTexture = texture;
		m.diffuseColor = Color3.FromHexString(color).scale(0.42);
		m.specularColor = Color3.Black();
		const ground = MeshBuilder.CreateGround(`floor-${biome}`, { width: HALF * 2, height: HALF * 2 }, scene);
		ground.material = m;
		ground.isPickable = false;
		ground.setEnabled(false);
		return ground;
	});
	const wallMat = material(scene, 'room-wall', '#4b5668');
	const segment = HALF - PIT_WORLD.doorHalf;
	const wall = MeshBuilder.CreateBox('room-wall', { width: segment, depth: WALL_T, height: WALL_H }, scene);
	wall.material = wallMat;
	wall.isPickable = false;
	wall.setEnabled(false);
	const doorMat = material(scene, 'door', '#4dd0e1', 0.55);
	doorMat.emissiveColor = Color3.FromHexString('#4dd0e1');
	const gate = MeshBuilder.CreateBox('door', { width: PIT_WORLD.doorHalf * 2, depth: 0.3, height: 0.08 }, scene);
	gate.material = doorMat;
	gate.isPickable = false;
	gate.setEnabled(false);
	const pillar = MeshBuilder.CreateCylinder('room-pillar', { diameter: 1, height: 2.4, tessellation: 24 }, scene);
	pillar.material = mats.pillar;
	pillar.isPickable = false;
	pillar.setEnabled(false);
	// Obstacle looks of the seeded layouts: 0 column · 1 boulder · 2 tree · 3 crystal.
	const boulder = MeshBuilder.CreateSphere('room-boulder', { diameter: 1, segments: 6 }, scene);
	boulder.material = material(scene, 'boulder', '#6e6a62');
	boulder.isPickable = false;
	boulder.setEnabled(false);
	const tree = MeshBuilder.CreateCylinder('room-tree', { diameterTop: 0, diameterBottom: 1.6, height: 3.2, tessellation: 7 }, scene);
	tree.material = material(scene, 'tree', '#2f6b3a');
	tree.isPickable = false;
	tree.setEnabled(false);
	const crystalMat = material(scene, 'crystal', '#7fe3ff', 0.85);
	crystalMat.emissiveColor = Color3.FromHexString('#2a6f88');
	const crystal = MeshBuilder.CreateBox('room-crystal', { size: 1 }, scene);
	crystal.material = crystalMat;
	crystal.isPickable = false;
	crystal.setEnabled(false);
	// Walls take the biome's tone.
	const walls = BIOME_COLOR.map((color, biome) => {
		const w = wall.clone(`room-wall-${biome}`);
		w.material = material(scene, `room-wall-${biome}`, Color3.FromHexString(color).scale(0.55).toHexString());
		w.setEnabled(false);
		return w;
	});
	return { floors, wall, walls, segment, gate, pillar, boulder, tree, crystal };
})();

type Chunk = { room: Room; node: TransformNode; biome: number | undefined; seed: number };
const chunks = new Map<string, Chunk>();

/** One room's static geometry under a node at the room's centre. */
function createChunk(room: Room): Chunk {
	const key = roomZone(room[0], room[1]);
	const node = new TransformNode(`room-${key}`, scene);
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	const biome = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }).biome : undefined;
	const add = (mesh: Mesh, name: string, x: number, y: number, height: number, turn = 0) => {
		const instance = mesh.createInstance(`${key}-${name}`);
		instance.parent = node;
		instance.position.set(x, height, y);
		instance.rotation.y = turn;
		instance.isPickable = false;
		return instance;
	};
	add(templates.floors[biome ?? 0]!, 'floor', 0, 0, 0);
	// Each wall: two segments either side of its doorway gap, just outside
	// the floor (the neighbour's wall stands right behind it).
	const off = PIT_WORLD.doorHalf + templates.segment / 2;
	const out = HALF + WALL_T / 2;
	const wall = templates.walls[biome ?? 0] ?? templates.wall;
	for (const side of [-1, 1]) {
		for (const along of [-1, 1]) {
			add(wall, `wall-h${side}${along}`, along * off, side * out, WALL_H / 2);
			add(wall, `wall-v${side}${along}`, side * out, along * off, WALL_H / 2, Math.PI / 2);
		}
		add(templates.gate, `door-h${side}`, 0, side * HALF, 0.04);
		add(templates.gate, `door-v${side}`, side * HALF, 0, 0.04, Math.PI / 2);
	}
	// The room's seeded obstacles (what the step collides with).
	const obstacles = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }).obstacles : PILLARS.map((p) => ({ ...p, look: 0 }));
	obstacles.forEach(({ x, y, r, look }, i) => {
		if (look === 1) {
			const instance = add(templates.boulder, `obstacle-${i}`, x, y, r * 0.55);
			instance.scaling.set(r * 2, r * 1.3, r * 2);
		} else if (look === 2) {
			const instance = add(templates.tree, `obstacle-${i}`, x, y, 1.6);
			instance.scaling.set(r * 1.25, 1, r * 1.25);
		} else if (look === 3) {
			const instance = add(templates.crystal, `obstacle-${i}`, x, y, r);
			instance.scaling.set(r * 1.4, r * 2, r * 1.4);
			instance.rotation.y = 0.6;
		} else {
			const instance = add(templates.pillar, `pillar-${i}`, x, y, 1.2);
			instance.scaling.set(r * 2, 1, r * 2);
		}
	});
	return { room, node, biome, seed };
}

/** Keep the rooms within `ROOM_VIEW` of the origin built and placed. */
function syncChunks(): void {
	const wanted = new Set<string>();
	for (let dy = -ROOM_VIEW; dy <= ROOM_VIEW; dy += 1) {
		for (let dx = -ROOM_VIEW; dx <= ROOM_VIEW; dx += 1) {
			const room: Room = [origin[0] + dx, origin[1] + dy];
			const key = roomZone(room[0], room[1]);
			wanted.add(key);
			const chunk = chunks.get(key);
			// Built before the world module loaded or the seed was known: rebuild.
			const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
			if (chunk !== undefined && chunk.seed === seed && (chunk.biome !== undefined || !pit.pures.loaded())) continue;
			chunk?.node.dispose();
			chunks.set(key, createChunk(room));
		}
	}
	for (const [key, chunk] of chunks) {
		if (!wanted.has(key)) {
			chunk.node.dispose();
			chunks.delete(key);
			continue;
		}
		chunk.node.position.copyFrom(place(chunk.room, 0, 0));
	}
}

/** Re-centre on `to` without a visible move: the camera shifts with the world. */
function moveOrigin(to: Room): void {
	if (to[0] === origin[0] && to[1] === origin[1]) return;
	const shift = new Vector3((to[0] - origin[0]) * ROOM_SIZE, 0, (to[1] - origin[1]) * ROOM_SIZE);
	origin = to;
	stage.camera.setTarget(stage.camera.target.subtract(shift), false, false, true);
	syncChunks();
}
syncChunks();
pit.pures.ready.then(syncChunks, () => undefined);
pit.on('hello', () => syncChunks());

// Doorway labels of your room: anchors at its doors (the origin room, so
// they never move), named after the room behind each.
const DOORS: { dx: number; dy: number }[] = [
	{ dx: 1, dy: 0 },
	{ dx: -1, dy: 0 },
	{ dx: 0, dy: 1 },
	{ dx: 0, dy: -1 }
];
const doorAnchors = DOORS.map(({ dx, dy }, i) => {
	const anchor = MeshBuilder.CreateBox(`door-anchor-${i}`, { size: 0.01 }, scene);
	anchor.isVisible = false;
	anchor.isPickable = false;
	anchor.position.set(dx * HALF, 0.1, dy * HALF);
	return anchor;
});
let shownRoom = '';

function showRoom(zone: string): void {
	const at = parseRoom(zone);
	if (at === undefined || zone === shownRoom) return;
	shownRoom = zone;
	DOORS.forEach(({ dx, dy }, i) => {
		stage.labels.set(`door-${i}`, doorAnchors[i]!, `→ ${roomZone(at[0] + dx, at[1] + dy)}`, 0.6, 'door');
	});
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	if (pit.pures.loaded()) {
		const room = pit.pures.pitRoom({ seed, x: at[0], y: at[1] });
		hud.row('Room', `${zone} · ${PIT_WORLD.biomes[room.biome] ?? '?'} · ${PATTERNS[room.pattern] ?? '?'} · danger ${room.danger} · ${room.mobs.length} mobs${room.boss ? ' · BOSS ROOM' : ''}`);
	}
}

// ---- network panel: server ghost and interest ring ----
const ghost = MeshBuilder.CreateCapsule('ghost', { radius: PLAYER_RADIUS, height: 1.8 }, scene);
ghost.material = mats.ghost;
ghost.isPickable = false;
ghost.setEnabled(false);
/** Unit ring scaled to the room's interest radius (`hello.viewRadius`). */
const interestRing = createRing(scene, 1, '#6cc4ff');
interestRing.isPickable = false;
interestRing.setEnabled(false);

type PlayerVisual = { kind: 'player'; body: Mesh; blade: Mesh; arc: Mesh; nose: Mesh };
type MobVisual = { kind: 'mob'; body: Mesh; hpBack: Mesh; hpFill: Mesh; ring: Mesh; fill: Mesh; tier: number };
type Visual = PlayerVisual | MobVisual;
const visuals = new Map<string, Visual>();

function flatDisc(id: string, radius: number, mat: StandardMaterial, arc = 1): Mesh {
	const disc = MeshBuilder.CreateDisc(id, { radius, arc, tessellation: 48, sideOrientation: Mesh.DOUBLESIDE }, scene);
	disc.rotation.x = Math.PI / 2;
	disc.material = mat;
	disc.isPickable = false;
	return disc;
}

function createPlayer(entity: RenderedEntity, mine: boolean): PlayerVisual {
	const body = MeshBuilder.CreateCapsule(entity.key, { radius: PLAYER_RADIUS, height: 1.8 }, scene);
	body.material = mine ? mats.me : mats.other;
	const nose = MeshBuilder.CreateBox(`${entity.key}-nose`, { width: 0.35, height: 0.15, depth: 0.2 }, scene);
	nose.material = mats.blade;
	const blade = MeshBuilder.CreateBox(`${entity.key}-blade`, { width: 1.6, height: 0.08, depth: 0.14 }, scene);
	blade.material = mats.blade;
	// Swing wedge: 120° (the server's cone), flat on the floor.
	const arc = flatDisc(`${entity.key}-arc`, ATTACK_REACH, mats.swing, SWING_HALF / Math.PI);
	return { kind: 'player', body, blade, arc, nose };
}

function createMob(entity: RenderedEntity): MobVisual {
	const tier = mobFields(entity).tier || 1;
	const t = TIERS[tier] ?? TIERS[1]!;
	const body =
		tier === 4
			? MeshBuilder.CreateCylinder(entity.key, { diameterTop: t.radius * 0.8, diameterBottom: t.radius * 2, height: 2.8, tessellation: 8 }, scene)
			: tier === 3
			? MeshBuilder.CreateCylinder(entity.key, { diameterTop: 0, diameterBottom: t.radius * 2, height: 2, tessellation: 6 }, scene)
			: MeshBuilder.CreateBox(entity.key, { width: t.radius * 2, depth: t.radius * 2, height: tier === 2 ? 1.5 : 0.9 }, scene);
	body.material = tierMats.get(tier) ?? mats.other;
	const hpBack = MeshBuilder.CreatePlane(`${entity.key}-hpb`, { width: 1.2, height: 0.14 }, scene);
	hpBack.material = mats.hpBack;
	hpBack.billboardMode = Mesh.BILLBOARDMODE_ALL;
	const hpFill = MeshBuilder.CreatePlane(`${entity.key}-hpf`, { width: 1.16, height: 0.1 }, scene);
	hpFill.material = mats.hpFill;
	hpFill.billboardMode = Mesh.BILLBOARDMODE_ALL;
	const ring = flatDisc(`${entity.key}-tele`, t.tele, mats.tele);
	const fill = flatDisc(`${entity.key}-tele-fill`, t.tele, mats.teleFill);
	return { kind: 'mob', body, hpBack, hpFill, ring, fill, tier };
}

function visualFor(entity: RenderedEntity, mine: boolean): Visual {
	let visual = visuals.get(entity.key);
	if (visual !== undefined) return visual;
	if (isMob(entity)) {
		visual = createMob(entity);
	} else {
		visual = createPlayer(entity, mine);
	}
	visuals.set(entity.key, visual);
	return visual;
}

function dispose(visual: Visual): void {
	for (const mesh of Object.values(visual)) if (mesh instanceof Mesh) mesh.dispose();
}

// ---- kill / saved popups (DOM labels anchored to invisible meshes) ----
type Popup = { mesh: Mesh; until: number; zone: string; x: number; y: number };
const popups = new Map<string, Popup>();
let popupSeq = 0;

/** The room a kill happened in, from its fact key (`…:pit.<x>.<y>:…`). */
function killRoom(key: string, fallback: string): string {
	return /:(pit\.-?\d+\.-?\d+):/.exec(key)?.[1] ?? fallback;
}

function popup(key: string, zoneId: string, x: number, y: number, text: string, className: string, ttlMs: number): void {
	let p = popups.get(key);
	if (p === undefined) {
		const mesh = MeshBuilder.CreateBox(`popup-${popupSeq++}`, { size: 0.01 }, scene);
		mesh.isVisible = false;
		p = { mesh, until: 0, zone: killRoom(key, zoneId), x, y };
		mesh.position.copyFrom(placeIn(p.zone, x, y, 1.2));
		popups.set(key, p);
	}
	p.until = performance.now() + ttlMs;
	stage.labels.set(`popup:${key}`, p.mesh, text, 0.6, `pit-pop ${className}`);
}

pit.on('kill', (kill, zoneId) => {
	// Entity ids are per room: a kill next door is never yours.
	const mine = zoneId === pit.room() && kill.killer === pit.hello()?.entity;
	const tier = TIERS[kill.tier]?.name ?? 'Mob';
	if (mine) popup(kill.key, zoneId, kill.x, kill.y, `${tier} slain · saving…`, 'pending', 60_000);
	else popup(kill.key, zoneId, kill.x, kill.y, `${kill.killerName} slew a ${tier}`, 'other', 2000);
});
let savedCount = 0;
pit.on('saved', (saved, zoneId) => {
	if (saved.ok) {
		savedCount += 1;
		const waited = saved.waitedMs >= 200 ? ` (${(saved.waitedMs / 1000).toFixed(1)} s)` : '';
		popup(saved.key, zoneId, saved.x, saved.y, `+${saved.xp} XP saved ✓${waited}`, 'saved', 2500);
	} else {
		popup(saved.key, zoneId, saved.x, saved.y, `save failed: ${saved.error ?? '?'}`, 'failed', 4000);
	}
});
pit.on('bossDown', (b, zoneId) => {
	if (zoneId === pit.room()) toast(`${b.by} felled the boss! The hood is now tier ${b.hoodTier} — its rooms get tougher`);
});
let lastWeather: number | undefined;
let lastEvent = '';
pit.on('world', (w, zoneId) => {
	if (zoneId !== pit.room()) return;
	if (w.event !== lastEvent && w.event !== '') toast(`World event in borough ${w.boroughX},${w.boroughY}: ${w.event}`);
	lastEvent = w.event;
	if (lastWeather !== undefined && w.weather > lastWeather) toast(`The sky over the borough lifts: ${WEATHER[w.weather]}`);
	lastWeather = w.weather;
});
pit.on('died', (d) => {
	deathBanner.textContent = 'You died — respawning…';
	deathBanner.style.display = '';
	setTimeout(() => (deathBanner.style.display = 'none'), d.respawnMs);
});
pit.on('hello', (hello) => {
	if (hello.restored) toast(`You entered ${roomZone(hello.roomX, hello.roomY)}: restored from hibernation (${hello.roomKills} kills here so far)`);
});
const boards = createBoardWatcher(pitApi, 1000);
boards.on((board) => renderBoard(board));

function renderBoard(board: BoardSnapshot): void {
	const me = api.last?.principal;
	const list = field('board');
	list.replaceChildren(
		...board.kills.slice(0, 5).map((row) => {
			const li = document.createElement('li');
			li.textContent = `${row.name} — ${row.score} kills`;
			if (row.member === me) li.className = 'me';
			return li;
		})
	);
}

// ---- durable-facts graph ----
type Sample = { pending: number; tick: number };
const samples: Sample[] = [];
let lastSampleAt = 0;

function drawGraph(): void {
	const ctx = graph.getContext('2d');
	if (ctx === null) return;
	const { width, height } = graph;
	ctx.clearRect(0, 0, width, height);
	ctx.strokeStyle = '#2a3240';
	ctx.beginPath();
	ctx.moveTo(0, height - 0.5);
	ctx.lineTo(width, height - 0.5);
	ctx.stroke();
	const maxPending = Math.max(4, ...samples.map((s) => s.pending));
	const series = (pick: (s: Sample) => number, max: number, color: string) => {
		ctx.strokeStyle = color;
		ctx.lineWidth = 2;
		ctx.beginPath();
		samples.forEach((s, i) => {
			const x = (i / 119) * width;
			const y = height - 2 - (Math.min(pick(s), max) / max) * (height - 6);
			if (i === 0) ctx.moveTo(x, y);
			else ctx.lineTo(x, y);
		});
		ctx.stroke();
	};
	series((s) => s.tick, 100, '#6cc4ff');
	series((s) => s.pending, maxPending, '#f2a65a');
	ctx.fillStyle = '#8793a6';
	ctx.font = '10px monospace';
	ctx.fillText(`pending max ${maxPending}`, 4, 10);
	ctx.fillText('tick 0–100 ms', width - 80, 10);
}

// ---- input ----
const sampler = new InputSampler();
const commandFrames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });

// Ranged: R or a click fires along the view (first person: where you
// look; third person: the camera's heading). The room resolves the arrow
// lag-compensated (C toggles it per shot).
let fireQueued = false;
let lastFireAt = 0;
let compensated = true;
window.addEventListener('keydown', (e) => {
	if (e.code === 'KeyR' && !e.repeat) fireQueued = true;
});
canvas.addEventListener('mousedown', (e) => {
	if (e.button !== 0) return;
	// First person: the first click only captures the mouse.
	if (stage.view() === 'first' && document.pointerLockElement !== canvas) return;
	fireQueued = true;
});

function sendCommandFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	const aim = stage.aim();
	const first = stage.view() === 'first';
	let input: PitInput = {
		mx: toAxis(move.x),
		my: toAxis(move.y),
		buttons: (s.action ? BUTTON.attack : 0) | (s.alt ? BUTTON.dash : 0) | (s.jump ? BUTTON.jump : 0) | (first ? BUTTON.aim : 0),
		yaw: aim.yaw,
		pitch: aim.pitch,
		shot: 0,
		delayMs: 0
	};
	const now = performance.now();
	if (fireQueued && now - lastFireAt >= (PIT_RULES.fireCooldown * 1000) / 30) {
		const shot = pit.aimShot(aim.yaw, aim.pitch, compensated, first);
		if (shot !== undefined) {
			input = { ...input, ...shot.input, buttons: input.buttons | shot.input.buttons };
			lastFireAt = now;
			marker(shot.record);
		}
	}
	fireQueued = false;
	client.sendInput(input);
}

// ---- shot markers: yellow where you saw it land, then green (the room
// confirmed the hit) or red (it missed) ----
const markerMats = {
	pending: material(scene, 'marker-pending', '#ffd166'),
	hit: material(scene, 'marker-hit', '#3ddc84'),
	miss: material(scene, 'marker-miss', '#ff5252')
};
for (const m of Object.values(markerMats)) m.emissiveColor = m.diffuseColor.clone();
function marker(record: ShotRecord): void {
	const ball = MeshBuilder.CreateSphere(`shot-${record.id}`, { diameter: 0.35, segments: 6 }, scene);
	ball.material = markerMats.pending;
	ball.isPickable = false;
	ball.position.copyFrom(placeIn(record.zone, record.point[0], record.point[1], record.point[2]));
	const done = () => setTimeout(() => ball.dispose(), 1200);
	pit.verdict(record.id).then((v) => {
		ball.material = v.hit !== undefined ? markerMats.hit : markerMats.miss;
		ball.position.copyFrom(placeIn(record.zone, v.x, v.y, v.z));
		done();
	});
	setTimeout(() => ball.dispose(), 4000);
}
// A crosshair in first person.
const crosshair = div('pit-crosshair', document.body);
crosshair.textContent = '+';

// ---- frame ----
stage.onFrame((now, dtMs) => {
	applySky(dtMs);
	commandFrames.update(now, sendCommandFrame);
	const here = pit.room();
	const hereRoom = here === undefined ? undefined : parseRoom(here);
	if (hereRoom !== undefined) moveOrigin(hereRoom);
	const hello = pit.hello();
	const authority = client.authority;
	const rendered = client.render();
	miniDots.length = 0;
	const seen = new Set<string>();
	let me: { at: Vector3; hp: number; dead: boolean } | undefined;
	let stats: PitFields | undefined;
	for (const entity of rendered) {
		if (isPit(entity)) {
			if (entity.zoneId === here) stats = pitFields(entity);
			continue;
		}
		// Pillars are static: every room's are drawn with its chunk.
		if (isPillar(entity)) continue;
		// Yours: predicted, or the SDK's hold of it during a doorway handoff.
		const mine =
			entity.predicted || (authority !== undefined && hello !== undefined && entity.zoneId === authority.zoneId && entity.id === hello.entity);
		const at = (x: number, y: number, height: number) => placeIn(entity.zoneId, x, y, height);
		const visual = visualFor(entity, mine);
		seen.add(entity.key);
		const visible = !entity.stale;
		const entityRoom = parseRoom(entity.zoneId);
		if (entityRoom !== undefined && visible) {
			const base = { wx: entityRoom[0] * ROOM_SIZE, wy: entityRoom[1] * ROOM_SIZE };
			if (visual.kind === 'mob') {
				const m = mobFields(entity);
				miniDots.push({ wx: base.wx + m.x, wy: base.wy + m.y, kind: 'mob', tier: visual.tier, face: m.face, dead: false });
			} else {
				const p = playerFields(entity);
				const dot: MiniDot = { wx: base.wx + p.x, wy: base.wy + p.y, kind: mine ? 'me' : 'player', tier: 0, face: p.face, dead: p.dead };
				if (mine) miniMe = dot;
				else miniDots.push(dot);
			}
		}
		if (visual.kind === 'mob') {
			const mob = mobFields(entity);
			const { x, y } = mob;
			const height = visual.tier === 4 ? 1.4 : visual.tier === 3 ? 1 : visual.tier === 2 ? 0.75 : 0.45;
			visual.body.position.copyFrom(at(x, y, height));
			visual.body.rotation.y = -mob.face;
			const hp = mob.hp / 100;
			const top = height * 2 + 0.35;
			visual.hpBack.position.copyFrom(at(x, y, top));
			visual.hpFill.position.copyFrom(at(x, y, top));
			visual.hpFill.scaling.x = Math.max(0.001, hp);
			const tele = mob.tele / 255;
			const winding = mob.state === 2;
			visual.ring.setEnabled(visible && winding);
			visual.fill.setEnabled(visible && winding);
			if (winding) {
				visual.ring.position.copyFrom(at(x, y, 0.03));
				visual.fill.position.copyFrom(at(x, y, 0.04));
				visual.fill.scaling.setAll(Math.max(0.05, tele));
			}
			visual.body.scaling.setAll(winding ? 1 + 0.15 * tele : 1);
			for (const m of [visual.body, visual.hpBack, visual.hpFill]) m.setEnabled(visible);
		} else {
			const player = playerFields(entity);
			const { x, y, z, dead, face, swing } = player;
			visual.body.position.copyFrom(at(x, y, 0.9 + z));
			visual.body.material = dead ? mats.dead : mine ? mats.me : mats.other;
			visual.nose.position.copyFrom(at(x + Math.cos(face) * 0.5, y + Math.sin(face) * 0.5, 1.3 + z));
			visual.nose.rotation.y = -face;
			// Blade sweeps through the swing cone while `swing` counts down to 0.
			const sweep = swing > 0 ? ((SWING_FRAMES - swing) / SWING_FRAMES) * 2 * SWING_HALF - SWING_HALF : -Math.PI / 2.5;
			const a = face + sweep;
			visual.blade.position.copyFrom(at(x + Math.cos(a) * 0.8, y + Math.sin(a) * 0.8, 1.0 + z));
			visual.blade.rotation.y = -a;
			visual.blade.setEnabled(visible && !dead);
			visual.arc.setEnabled(visible && swing > 0);
			if (swing > 0) {
				visual.arc.position.copyFrom(at(x, y, 0.05));
				// Disc arcs start at +x and run counter-clockwise; centre it on `face`.
				visual.arc.rotation.y = -(face - SWING_HALF);
			}
			const label = `${player.name} · L${player.level} · ${player.hp} HP`;
			// First person: you are the camera (no body, no label of your own).
			const hidden = mine && stage.view() === 'first';
			if (hidden) stage.labels.delete(entity.key);
			else stage.labels.set(entity.key, visual.body, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			visual.body.setEnabled(visible && !hidden);
			visual.nose.setEnabled(visible && !dead && !hidden);
			if (mine) me = { at: visual.body.position, hp: player.hp, dead };
		}
	}
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		dispose(visual);
		stage.labels.delete(key);
		visuals.delete(key);
	}
	for (const [key, p] of popups) {
		p.mesh.position.copyFrom(placeIn(p.zone, p.x, p.y, 1.2));
		if (now < p.until) continue;
		p.mesh.dispose();
		stage.labels.delete(`popup:${key}`);
		popups.delete(key);
	}
	if (me !== undefined) stage.follow(me.at.x, me.at.z, dtMs, me.at.y - 0.9);
	crosshair.style.display = stage.view() === 'first' ? '' : 'none';

	// The server's own copy of you (authoritative, behind by the round
	// trip): a wireframe ghost in the room that owns you.
	const authoritative = client.predictor?.authoritative();
	let ghostGap: number | undefined;
	if (authoritative !== undefined && authority !== undefined) {
		const g = playerFields({ fields: authoritative });
		ghost.position.copyFrom(placeIn(authority.zoneId, g.x, g.y, 0.9 + g.z));
		if (me !== undefined) ghostGap = Math.hypot(ghost.position.x - me.at.x, ghost.position.z - me.at.z);
	}
	ghost.setEnabled(ghostOn && authoritative !== undefined && !(stage.view() === 'first' && (ghostGap ?? 0) < 0.8));
	interestRing.setEnabled(me !== undefined && hello !== undefined);
	if (me !== undefined && hello !== undefined) {
		interestRing.position.set(me.at.x, 0, me.at.z);
		interestRing.scaling.setAll(hello.viewRadius);
	}

	// ---- HUD ----
	hud.frame({ entities: rendered.length });
	const pending = stats?.pending ?? 0;
	const tickMs = stats?.tickMs ?? 0;
	const tickMax = stats?.tickMaxMs ?? 0;
	hud.row('You', api.last === undefined ? '—' : `${api.last.name} · ${api.last.principal}`);
	hud.row('HP', me === undefined ? '—' : me.dead ? 'dead' : String(me.hp));
	hud.row('Facts pending (zone)', String(pending));
	hud.row('Facts saved (zone)', String(stats?.committed ?? 0));
	hud.row('My unsaved kills', String(pit.unsaved.size));
	hud.row('Kills in this room', String(stats?.kills ?? 0));
	hud.row('Room tick interval', `${tickMs.toFixed(1)} ms avg · ${tickMax.toFixed(1)} max`);
	const predictor = client.predictor;
	hud.row('Prediction', client.predictionEnabled ? 'on (pit_step as WASM)' : 'off (raw round trip)');
	hud.row('Pending inputs', String(predictor?.pending.length ?? 0));
	hud.row('Ack seq', String(predictor?.lastAck ?? 0));
	hud.row('Corrections', `${corrections} (last ${lastCorrection.toFixed(3)} m)`);
	hud.row('Ghost gap', ghostGap === undefined ? '—' : `${ghostGap.toFixed(2)} m`);
	const around = [...pit.neighbours().values()];
	hud.row('Next door', `${around.filter((s) => s === 'watching').length} rooms watched · ${around.filter((s) => s === 'asleep').length} asleep (not woken)`);
	const w = pit.world();
	hud.row('Hood', w === undefined ? '—' : `${w.hoodX},${w.hoodY} · tier ${w.hoodTier} · boss ${w.bossDown ? `down (${w.lastBy})` : 'standing'}`);
	hud.row('Borough', w === undefined ? '—' : `${w.boroughX},${w.boroughY} · ${WEATHER[w.weather]} · ${w.boroughBossesDown}/9 bosses down`);
	hud.row('City', w === undefined || w.banner === '' ? '—' : w.banner);
	const left = w === undefined || w.event === '' ? 0 : Math.max(0, Math.round((w.eventUntilMs - Date.now()) / 1000));
	hud.row('World event', w === undefined || w.event === '' ? 'none (the borough schedules the next)' : `${w.event} · ${left} s left`);
	eventBanner.style.display = w !== undefined && w.event !== '' ? '' : 'none';
	if (w !== undefined && w.event !== '') eventBanner.textContent = `${EVENT_TEXT[w.event] ?? w.event} · ${left} s`;
	const c = pit.content();
	tintMobs(c.pack);
	hud.row('Content', `${c.pack.name}${pinContent ? ' (old client: no live patch)' : ''} · world ${CONTENT_PACKS.find((p) => p.version === c.worldVersion)?.name ?? c.worldVersion}${c.reloads.length > 0 ? ` · ${c.reloads.length} reload(s)` : ''}`);
	freezeOverlay.style.display = c.frozen === undefined ? 'none' : '';
	if (c.frozen !== undefined) {
		const target = CONTENT_PACKS.find((p) => p.version === c.frozen!.version)?.name ?? c.frozen.version;
		freezeOverlay.innerHTML = `<b>SYSTEM: an administrator has paused the world to update it to ${target}.</b><br/>${c.frozen.notes}`;
	}
	staleBanner.style.display = c.stale ? '' : 'none';
	if (c.stale) {
		const world = CONTENT_PACKS.find((p) => p.version === c.worldVersion)?.name ?? c.worldVersion;
		staleBanner.textContent = `Your client runs ${c.pack.name}; the world runs ${world}. Only the entrance (${GATE_ROOM}) lets you through — it updates you.${c.staleNotice === undefined ? '' : ' (that doorway is closed to you)'}`;
	}
	const r = pit.rangedStats;
	hud.row('Ranged', `${r.shots} shots · ${r.confirmedHits} hits confirmed · ${r.agreed}/${r.resolved} as seen · last rewind ${r.lastAgeMs.toFixed(0)} ms${compensated ? '' : ' (compensation OFF)'}`);
	hud.row('Interest', hello === undefined ? '—' : `${hello.viewRadius} m ring · ${rendered.filter((e) => isMob(e) && !e.stale).length} mobs replicated`);
	const profile = pit.profile();
	if (profile !== undefined) {
		const span = Math.max(1, profile.nextLevelXp - profile.levelXp);
		xpFill.style.width = `${Math.min(100, ((profile.xp - profile.levelXp) / span) * 100).toFixed(1)}%`;
		xpLabel.textContent = `Level ${profile.level} · ${profile.xp} XP · ${profile.kills} kills · next level at ${profile.nextLevelXp} XP (${profile.source === 'cell' ? 'loaded from cell' : 'saved'})`;
	} else {
		xpLabel.textContent = 'Loading your Crawler cell…';
	}
	field('pending').textContent = String(pending);
	field('saved').textContent = String(savedCount);
	field('note').textContent = 'Kills settle in your Crawler cell next to the room on celld; the tick never waits for them.';
	if (here !== undefined) showRoom(here);
	drawRooms();
	if (now - lastSampleAt > 250) {
		renderDebug();
		lastSampleAt = now;
		samples.push({ pending, tick: tickMs });
		if (samples.length > 120) samples.shift();
		drawGraph();
	}
});

declare global {
	interface Window {
		mobpit?: unknown;
	}
}
window.mobpit = { client, api, pitApi, pit, scene, rooms, boards, camera: stage.camera, origin: () => origin };

// The same browser is the same player: back in the room you were last in.
(async () => {
	// Start on the world's live content (the packs are built in).
	const live = await pitApi.content().catch(() => undefined);
	client.setContentVersion(live?.state?.version ?? '');
	const zone = forcedZone ?? (await pitApi.startRoom(api.guest).catch(() => DEFAULT_ZONE));
	origin = parseRoom(zone) ?? origin;
	await client.connect(zone);
})().catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
