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
 *
 * Next door: the 8 rooms around yours are watched (their mobs drawn where
 * they are) without keeping them awake. "Wake rooms in reach" (N, on by
 * default, remembered): walk toward a doorway and the room behind it wakes
 * and stays live until you walk away (minimap: solid cyan border = kept
 * awake, dashed = watched only).
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { DynamicTexture } from '@babylonjs/core/Materials/Textures/dynamicTexture';
import { Color3, Color4 } from '@babylonjs/core/Maths/math.color';
import { Scene } from '@babylonjs/core/scene';
import { Frustum } from '@babylonjs/core/Maths/math.frustum';
import { Matrix, Vector3 } from '@babylonjs/core/Maths/math.vector';
import type { Plane } from '@babylonjs/core/Maths/math.plane';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { VertexData } from '@babylonjs/core/Meshes/mesh.vertexData';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { TransformNode } from '@babylonjs/core/Meshes/transformNode';
import type { AbstractMesh } from '@babylonjs/core/Meshes/abstractMesh';
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
	type PitInput,
	type Platform,
	type Ramp,
	type RoomTerrain
} from '../generated/mobpit/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, PAD, toAxis } from '../shared/input.ts';
import { easeOutCubic, Pool, Trauma, tween } from '../shared/feel.ts';
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
/** Wall thickness and height (m), drawn just outside each room's floor:
 * the box a laser bounces off (`game_sim::mobpit::room_colliders`). */
const WALL_T = PIT_WORLD.wallThickness;
const WALL_H = PIT_WORLD.wallHeight;
/** Swing cone, from the server's rules: reach to a mob's edge, plus the body. */
const { attackRange, attackHalfAngle: SWING_HALF, swingFrames: SWING_FRAMES } = PIT_RULES;
const ATTACK_REACH = attackRange + PLAYER_RADIUS;
/** A player's full health (`PLAYER_HP` in the room, `server/rooms/src/zone.rs`). */
const PLAYER_HP = 100;

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
/** "Wake rooms in reach" (proximity wake), remembered per browser. */
const WAKE_KEY = 'game-demos.mobpit.wake';
const wakeInitial = (() => {
	try {
		return localStorage.getItem(WAKE_KEY) !== 'off';
	} catch {
		return true;
	}
})();
const pit = createMobPitClient({
	api,
	pinContent,
	wake: wakeInitial,
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
	hint: 'WASD move · Space attack · R / click laser (bounces off walls, ramps, the floor) · E dash · Q jump (over a slam, onto ramps) · V first person · C lag compensation · walk through a doorway to the next room · N wake rooms in reach · O debug overlay · P prediction · G server ghost. Controller: left stick move · right stick look · X / RB attack · RT laser · B / LB dash · A jump · R3 first person · View lag compensation. Rooms run on celld; kills are durable facts.',
	client,
	devApi: api
});
let debug = params.get('debug') !== '0';
hud.toggle('Debug overlay (celld node per room)', debug, (on) => (debug = on), 'o');
hud.toggle('Client prediction', true, (on) => client.setPredictionEnabled(on), 'p');
hud.toggle('First-person view (click to look, R / click / RT fires)', false, (on) => stage.setView(on ? 'first' : 'third'), 'v', PAD.r3);
hud.toggle('Lag compensation (ranged)', true, (on) => (compensated = on), 'c', PAD.view);
let ghostOn = true;
hud.toggle('Server ghost', true, (on) => (ghostOn = on), 'g');
let cullOn = true;
hud.toggle('Skip off-screen entities', true, (on) => (cullOn = on), 'k');
let lodOn = true;
hud.toggle('Level of detail (props and mobs)', true, (on) => (lodOn = on), 'l');
hud.toggle(
	'Wake rooms in reach (doorway neighbours you walk up to)',
	wakeInitial,
	(on) => {
		pit.setWakeRooms(on);
		try {
			localStorage.setItem(WAKE_KEY, on ? 'on' : 'off');
		} catch {
			// Not remembered (private mode); still applied.
		}
	},
	'n'
);
let corrections = 0;
let lastCorrection = 0;
client.on('correction', (c) => {
	corrections += 1;
	lastCorrection = Math.hypot(c.errors.x ?? 0, c.errors.y ?? 0);
});

// ---- side panels: XP bar, durable facts graph, mini leaderboard ----
const xpPanel = div('pit-xp', document.body);
// First person: your health in the HUD (your own head and its bar are hidden).
const hpRow = div('pit-hp', xpPanel);
const hpLabel = div('pit-hp-label', hpRow);
const hpFillBar = div('pit-hp-fill', div('pit-hp-bar', hpRow));
hpRow.style.display = 'none';
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
// Top centre, one column: the borough's world event (scheduled by its cell;
// a banner while it runs), then the toasts under it.
const topStack = div('pit-top', document.body);
const eventBanner = div('pit-event', topStack);
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
	<div class="pit-legend"><span class="c-active">■ active</span> <span class="c-dormant">■ dormant</span> <span class="c-hibernated">■ asleep</span> <span class="c-cold">■ never ran</span> <span class="c-watched">┄ watched</span> <span class="c-watched">□ kept awake</span> <span class="c-me">▲ you</span> <span class="c-other">● players</span> <span class="c-mob">● mobs</span></div>
	<div class="pit-debug" data-k="debug"></div>`;
const roomsMap = roomsPanel.querySelector('canvas') as HTMLCanvasElement;
const roomsField = (k: string) => roomsPanel.querySelector(`[data-k="${k}"]`) as HTMLElement;
const toasts = div('pit-toasts', topStack);
const rooms = new Map<string, RoomStatus>();
let roomInfo: RoomInfo | undefined;
/** Recent room info samples (one activation), for the dropped-tick rate. */
const tickSamples: { at: number; activation: string; dropped: number }[] = [];
/** Window of the overlay's dropped-tick count. */
const DROP_WINDOW_MS = 30_000;
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
		if (debug && here !== undefined) {
			roomInfo = await pitApi.roomInfo(here).catch(() => roomInfo);
			if (roomInfo !== undefined && roomInfo.zone === here) {
				const now = performance.now();
				const last = tickSamples.at(-1);
				if (last !== undefined && last.activation !== `${roomInfo.zone}/${roomInfo.activation}`) tickSamples.length = 0;
				tickSamples.push({ at: now, activation: `${roomInfo.zone}/${roomInfo.activation}`, dropped: roomInfo.droppedTicks });
				while (tickSamples.length > 1 && now - tickSamples[0]!.at > DROP_WINDOW_MS) tickSamples.shift();
			}
		}
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
type Box = { x0: number; y0: number; x1: number; y1: number };
type MiniLayout = { biome: number; obstacles: { x: number; y: number; r: number }[]; ramps: readonly Ramp[]; holes: readonly Box[]; platforms: readonly Platform[]; open: number };
const roomLayouts = new Map<string, MiniLayout>();
function roomLayout(seed: number, x: number, y: number): MiniLayout | undefined {
	if (!pit.pures.loaded()) return undefined;
	const key = `${seed}:${x}:${y}`;
	let layout = roomLayouts.get(key);
	if (layout === undefined) {
		const room = pit.pures.pitRoom({ seed, x, y });
		layout = { biome: room.biome, obstacles: room.obstacles.map(({ x, y, r }) => ({ x, y, r })), ramps: room.ramps, holes: room.holes, platforms: room.platforms, open: room.open };
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
 * A watched neighbour has a dashed cyan border; one you keep awake
 * (proximity wake) a solid one.
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
			// Ramps: their footprint, red when too steep to walk.
			if (layout !== undefined && s > 0.6) {
				for (const r of layout.ramps) {
					const [ax, ay] = toMap(rx * ROOM_SIZE + r.x0, ry * ROOM_SIZE + r.y1);
					ctx.fillStyle = r.degrees <= 45 ? '#c9c2b088' : '#d0703f99';
					ctx.fillRect(ax, ay, (r.x1 - r.x0) * s, (r.y1 - r.y0) * s);
				}
				// Holes black, then platforms and stepping stones over them.
				const box = (b: Box, fill: string) => {
					const [ax, ay] = toMap(rx * ROOM_SIZE + b.x0, ry * ROOM_SIZE + b.y1);
					ctx.fillStyle = fill;
					ctx.fillRect(ax, ay, (b.x1 - b.x0) * s, (b.y1 - b.y0) * s);
				};
				for (const h of layout.holes) box(h, '#000000ee');
				for (const p of layout.platforms) box(p, p.height > 0 ? '#b8a88caa' : '#8c7f6acc');
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
			// Open sides (no wall) are left out.
			const open = layout?.open ?? 0;
			for (const sign of [-1, 1]) {
				if ((open & (sign > 0 ? 4 : 8)) === 0) {
					wall(-HALF, sign * HALF, -door, sign * HALF);
					wall(door, sign * HALF, HALF, sign * HALF);
				}
				if ((open & (sign > 0 ? 1 : 2)) === 0) {
					wall(sign * HALF, -HALF, sign * HALF, -door);
					wall(sign * HALF, door, sign * HALF, HALF);
				}
			}
			// Room state as the tile's inner border.
			const status = row === undefined ? undefined : STATUS_COLOR[row.status];
			if (status !== undefined) {
				ctx.strokeStyle = status;
				ctx.lineWidth = 2;
				ctx.strokeRect(x0 + 3, y0 + 3, size - 6, size - 6);
			}
			const watched = pit.neighbours().get(zone);
			if (pit.near().has(zone)) {
				ctx.strokeStyle = '#4dd0e1';
				ctx.lineWidth = 2;
				ctx.strokeRect(x0 + 6, y0 + 6, size - 12, size - 12);
			} else if (watched === 'watching' && !(rx === here[0] && ry === here[1])) {
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
		`<br>tick ${info.hz} Hz · late p50 ${info.lateMs.p50.toFixed(1)} / p99 ${info.lateMs.p99.toFixed(1)} ms` +
		`<br>${tickHealth(info)}`;
}

/**
 * Dropped ticks over the last ~30 s: a room on a degraded celld state shows
 * a rising count (and a late p99 in the hundreds of ms) before play feels
 * it. Red past 1 % of the window's ticks.
 */
function tickHealth(info: RoomInfo): string {
	const first = tickSamples[0];
	const last = tickSamples.at(-1);
	if (first === undefined || last === undefined || last.at - first.at < 2000) {
		return `dropped ticks ${info.droppedTicks} (measuring…)`;
	}
	const span = last.at - first.at;
	const recent = last.dropped - first.dropped;
	const bad = recent > (info.hz * span) / 1000 / 100 || info.lateMs.p99 > 50;
	const color = bad ? '#ff6b6b' : '#7bd88f';
	return `<span style="color:${color}">dropped ticks ${recent} in ${Math.round(span / 1000)} s</span> · ${info.droppedTicks} this activation`;
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
	playerHpFill: material(scene, 'player-hp-fill', '#4caf50'),
	tele: material(scene, 'tele', '#ff3b3b', 0.25),
	teleFill: material(scene, 'tele-fill', '#ff3b3b', 0.45),
	ghost: material(scene, 'ghost', '#ffffff', 0.25)
};
mats.ghost.wireframe = true;
for (const m of [mats.swing, mats.tele, mats.teleFill, mats.hpFill, mats.playerHpFill]) m.emissiveColor = m.diffuseColor.clone();
// Slam warnings lie on the floor: both faces, drawn over it where they meet
// (the floor's triangles and the room's bilinear ground differ by a few cm).
for (const m of [mats.tele, mats.teleFill]) {
	m.backFaceCulling = false;
	m.zOffset = -2;
}
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

// ---- level of detail ----
// Round props and the bigger mobs come in three levels, about 100 %, 30 %
// and 10 % of the full level's triangles; an object's level follows its
// distance from the camera, past LOD_AT[0] the middle one, past LOD_AT[1]
// the coarsest. Around each threshold a 10 % band (±5 %): a coarser level
// only past T × 1.05, a finer one only inside T × 0.95, so an object
// standing near a threshold does not flicker between levels.
const LOD_AT = [18, 45];
const LOD_BAND = 0.05;
type Lod = { node: TransformNode; levels: readonly AbstractMesh[]; level: number };
/** Three template meshes of one shape (level 0 the finest), hidden: objects are instances of them. */
function lodTemplates(name: string, mat: StandardMaterial, make: (name: string, level: 0 | 1 | 2) => Mesh): Mesh[] {
	return ([0, 1, 2] as const).map((l) => {
		const mesh = make(`${name}-lod${l}`, l);
		mesh.material = mat;
		mesh.isPickable = false;
		mesh.setEnabled(false);
		return mesh;
	});
}
/** An object of `templates` (one instance per level under `node`; the finest shown). */
function lodOf(node: TransformNode, templates: readonly Mesh[]): Lod {
	const levels = templates.map((t, l) => {
		const instance = t.createInstance(`${node.name}-lod${l}`);
		instance.parent = node;
		instance.isPickable = false;
		instance.setEnabled(l === 0);
		return instance;
	});
	return { node, levels, level: 0 };
}
/** The level for distance `d` (m) from `level`, with the band round each threshold. */
function lodLevel(level: number, d: number, count: number): number {
	let l = level;
	while (l < count - 1 && d > LOD_AT[l]! * (1 + LOD_BAND)) l += 1;
	while (l > 0 && d < LOD_AT[l - 1]! * (1 - LOD_BAND)) l -= 1;
	return l;
}
/** Objects per level this frame (for the HUD). */
const lodCounts = [0, 0, 0];
function applyLod(lod: Lod, eye: Vector3): void {
	const want = lodOn ? lodLevel(lod.level, Vector3.Distance(eye, lod.node.getAbsolutePosition()), lod.levels.length) : 0;
	lodCounts[want] = (lodCounts[want] ?? 0) + 1;
	if (want === lod.level) return;
	lod.levels[lod.level]!.setEnabled(false);
	lod.levels[want]!.setEnabled(true);
	lod.level = want;
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
	// Columns, boulders and trees have three levels of detail (LOD_DETAIL).
	const pillar = lodTemplates('room-pillar', mats.pillar, (name, l) =>
		MeshBuilder.CreateCylinder(name, { diameter: 1, height: 2.4, tessellation: [40, 12, 4][l] }, scene)
	);
	// Obstacle looks of the seeded layouts: 0 column · 1 boulder · 2 tree · 3 crystal.
	const boulder = lodTemplates('room-boulder', material(scene, 'boulder', '#6e6a62'), (name, l) =>
		MeshBuilder.CreateSphere(name, { diameter: 1, segments: [16, 8, 4][l] }, scene)
	);
	const tree = lodTemplates('room-tree', material(scene, 'tree', '#2f6b3a'), (name, l) =>
		MeshBuilder.CreateCylinder(name, { diameterTop: 0, diameterBottom: 1.6, height: 3.2, tessellation: [40, 12, 4][l] }, scene)
	);
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
	// Ramps: stone you can walk up; rust-red where it is too steep (over
	// about 45°, `walkable` in the room) and you slide off.
	const rampMats = {
		walk: material(scene, 'ramp-walk', '#9a9384'),
		steep: material(scene, 'ramp-steep', '#b5653b')
	};
	for (const m of Object.values(rampMats)) {
		m.backFaceCulling = false;
		m.specularColor = Color3.Black();
	}
	// Holes: an open-topped dark shaft (inside faces only), scaled per hole.
	const shaft = MeshBuilder.CreateBox('room-hole', { size: 1, sideOrientation: Mesh.BACKSIDE }, scene);
	const shaftMat = material(scene, 'room-hole', '#07050a');
	shaftMat.specularColor = Color3.Black();
	shaft.material = shaftMat;
	shaft.isPickable = false;
	shaft.setEnabled(false);
	// Platforms: boxes to jump onto; stepping stones are columns rising out of a hole.
	const platform = MeshBuilder.CreateBox('room-platform', { size: 1 }, scene);
	platform.material = material(scene, 'room-platform', '#8c7f6a');
	platform.isPickable = false;
	platform.setEnabled(false);
	return { floors, wall, walls, segment, gate, pillar, boulder, tree, crystal, rampMats, shaft, platform };
})();

/** Each built room's terrain, for standing mobs on it. */
const terrains = new Map<string, RoomTerrain>();

/** Whether terrain sample `h` lies over a hole (`TERRAIN_HOLE`, -1000). */
const isHole = (h: number) => h < -100;

/** Terrain height at room-local `(x, y)` of zone `zoneId` (bilinear; 0 off the terrain or over a hole). */
function groundAt(zoneId: string, x: number, y: number): number {
	const t = terrains.get(zoneId);
	if (t === undefined) return 0;
	const fx = (x - t.minX) / t.cell;
	const fy = (y - t.minY) / t.cell;
	const i = Math.floor(fx);
	const j = Math.floor(fy);
	if (i < 0 || j < 0 || i + 1 >= t.nx || j + 1 >= t.ny) return 0;
	const at = (a: number, b: number) => t.heights[b * t.nx + a]!;
	const c = [at(i, j), at(i + 1, j), at(i, j + 1), at(i + 1, j + 1)];
	if (c.some(isHole)) return 0;
	const u = fx - i;
	const v = fy - j;
	return (c[0]! * (1 - u) + c[1]! * u) * (1 - v) + (c[2]! * (1 - u) + c[3]! * u) * v;
}

/** A room's ground as one mesh (room-local, scene axes: x, up, y): cells touching a hole are left open. */
function terrainMesh(name: string, t: RoomTerrain, biome: number): Mesh {
	const positions: number[] = [];
	const uvs: number[] = [];
	const indices: number[] = [];
	for (let j = 0; j < t.ny; j += 1) {
		for (let i = 0; i < t.nx; i += 1) {
			const x = t.minX + i * t.cell;
			const y = t.minY + j * t.cell;
			const h = t.heights[j * t.nx + i]!;
			positions.push(x, isHole(h) ? 0 : h, y);
			uvs.push(x / (HALF * 2) + 0.5, y / (HALF * 2) + 0.5);
		}
	}
	for (let j = 0; j + 1 < t.ny; j += 1) {
		for (let i = 0; i + 1 < t.nx; i += 1) {
			const a = j * t.nx + i;
			const corners = [a, a + 1, a + t.nx, a + t.nx + 1];
			if (corners.some((k) => isHole(t.heights[k]!))) continue;
			// Same winding as the ramps' top (front faces up, normals up).
			indices.push(a, a + 1, a + t.nx + 1, a, a + t.nx + 1, a + t.nx);
		}
	}
	const data = new VertexData();
	data.positions = positions;
	data.indices = indices;
	data.uvs = uvs;
	const normals: number[] = [];
	VertexData.ComputeNormals(positions, indices, normals);
	data.normals = normals;
	const mesh = new Mesh(name, scene);
	data.applyToMesh(mesh);
	mesh.material = templates.floors[biome]?.material ?? null;
	mesh.isPickable = false;
	return mesh;
}

/** One ramp of a room as a wedge mesh (room-local, scene axes: x, up, y). */
function rampMesh(name: string, r: Ramp): Mesh {
	// Corner heights: 0 on the low edge, `height` on the high one.
	const corners: [number, number][] = [
		[r.x0, r.y0],
		[r.x1, r.y0],
		[r.x1, r.y1],
		[r.x0, r.y1]
	];
	const top = ([x, y]: [number, number]) => {
		const high = r.uphill === 0 ? x === r.x1 : r.uphill === 1 ? x === r.x0 : r.uphill === 2 ? y === r.y1 : y === r.y0;
		return high ? r.height : 0;
	};
	const positions: number[] = [];
	const indices: number[] = [];
	const quad = (a: number[], b: number[], c: number[], d: number[]) => {
		const base = positions.length / 3;
		positions.push(...a, ...b, ...c, ...d);
		indices.push(base, base + 1, base + 2, base, base + 2, base + 3);
	};
	const up = corners.map((c) => [c[0], top(c), c[1]]);
	const down = corners.map((c) => [c[0], 0, c[1]]);
	quad(up[0]!, up[1]!, up[2]!, up[3]!);
	for (let i = 0; i < 4; i += 1) {
		const j = (i + 1) % 4;
		quad(down[i]!, down[j]!, up[j]!, up[i]!);
	}
	const data = new VertexData();
	data.positions = positions;
	data.indices = indices;
	const normals: number[] = [];
	VertexData.ComputeNormals(positions, indices, normals);
	data.normals = normals;
	const mesh = new Mesh(name, scene);
	data.applyToMesh(mesh);
	mesh.material = r.degrees <= 45 ? templates.rampMats.walk : templates.rampMats.steep;
	mesh.isPickable = false;
	return mesh;
}

type Chunk = { room: Room; node: TransformNode; biome: number | undefined; seed: number; lods: Lod[] };
const chunks = new Map<string, Chunk>();

/** One room's static geometry under a node at the room's centre. */
function createChunk(room: Room): Chunk {
	const key = roomZone(room[0], room[1]);
	const node = new TransformNode(`room-${key}`, scene);
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	const biome = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }).biome : undefined;
	const addAt = (mesh: Mesh, name: string, x: number, y: number, height: number, turn = 0) => {
		const instance = mesh.createInstance(`${key}-${name}`);
		instance.parent = node;
		instance.position.set(x, height, y);
		instance.rotation.y = turn;
		instance.isPickable = false;
		return instance;
	};
	const add = addAt;
	const layout = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }) : undefined;
	if (layout !== undefined && biome !== undefined) {
		const terrain = pit.pures.terrain({ seed, x: room[0], y: room[1] });
		terrains.set(key, terrain);
		terrainMesh(`${key}-terrain`, terrain, biome).parent = node;
		// Each hole: a shaft 4 m deep (falling past 2 m kills).
		layout.holes.forEach((h, i) => {
			const instance = add(templates.shaft, `hole-${i}`, (h.x0 + h.x1) / 2, (h.y0 + h.y1) / 2, -2);
			instance.scaling.set(h.x1 - h.x0, 4, h.y1 - h.y0);
		});
		// Platforms; a stepping stone (height 0) is a column from the bottom of its hole up to the floor.
		layout.platforms.forEach((p, i) => {
			const bottom = p.height > 0 ? 0 : -4;
			const top = p.height > 0 ? p.height : 0;
			const instance = add(templates.platform, `platform-${i}`, (p.x0 + p.x1) / 2, (p.y0 + p.y1) / 2, (bottom + top) / 2);
			instance.scaling.set(p.x1 - p.x0, top - bottom, p.y1 - p.y0);
		});
	} else {
		terrains.delete(key);
		add(templates.floors[biome ?? 0]!, 'floor', 0, 0, 0);
	}
	// Each wall: two segments either side of its doorway gap, just outside
	// the floor (the neighbour's wall stands right behind it).
	const off = PIT_WORLD.doorHalf + templates.segment / 2;
	const out = HALF + WALL_T / 2;
	const wall = templates.walls[biome ?? 0] ?? templates.wall;
	// An open side (no wall, crossed anywhere) has neither wall nor door
	// gate; the corner posts always stand.
	const open = layout?.open ?? 0;
	for (const side of [-1, 1]) {
		const openH = (open & (side > 0 ? 4 : 8)) !== 0;
		const openV = (open & (side > 0 ? 1 : 2)) !== 0;
		for (const along of [-1, 1]) {
			if (!openH) add(wall, `wall-h${side}${along}`, along * off, side * out, WALL_H / 2);
			if (!openV) add(wall, `wall-v${side}${along}`, side * out, along * off, WALL_H / 2, Math.PI / 2);
			const post = add(wall, `post-${side}${along}`, side * out, along * out, WALL_H / 2);
			post.scaling.set(WALL_T / templates.segment, 1, 1);
		}
		if (!openH) add(templates.gate, `door-h${side}`, 0, side * HALF, 0.04);
		if (!openV) add(templates.gate, `door-v${side}`, side * HALF, 0, 0.04, Math.PI / 2);
	}
	const lods: Lod[] = [];
	// The room's seeded obstacles (what the step collides with).
	const obstacles = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }).obstacles : PILLARS.map((p) => ({ ...p, look: 0 }));
	obstacles.forEach(({ x, y, r, look }, i) => {
		// Each stands on the level patch the terrain leaves it (on a hill, not in it).
		const base = groundAt(key, x, y);
		const add = (mesh: Mesh, name: string, x: number, y: number, height: number) => addAt(mesh, name, x, y, base + height);
		// A three-level prop: a node at its place, its levels under it.
		const addLod = (levels: readonly Mesh[], name: string, x: number, y: number, height: number) => {
			const prop = new TransformNode(`${key}-${name}`, scene);
			prop.parent = node;
			prop.position.set(x, base + height, y);
			lods.push(lodOf(prop, levels));
			return prop;
		};
		if (look === 1) {
			const instance = addLod(templates.boulder, `obstacle-${i}`, x, y, r * 0.55);
			instance.scaling.set(r * 2, r * 1.3, r * 2);
		} else if (look === 2) {
			const instance = addLod(templates.tree, `obstacle-${i}`, x, y, 1.6);
			instance.scaling.set(r * 1.25, 1, r * 1.25);
		} else if (look === 3) {
			const instance = add(templates.crystal, `obstacle-${i}`, x, y, r);
			instance.scaling.set(r * 1.4, r * 2, r * 1.4);
			instance.rotation.y = 0.6;
		} else {
			const instance = addLod(templates.pillar, `pillar-${i}`, x, y, 1.2);
			instance.scaling.set(r * 2, 1, r * 2);
		}
	});
	// Its sloped ramps (what the step walks on and the laser bounces off).
	const ramps = pit.pures.loaded() ? pit.pures.pitRoom({ seed, x: room[0], y: room[1] }).ramps : [];
	ramps.forEach((r, i) => {
		rampMesh(`${key}-ramp-${i}`, r).parent = node;
	});
	return { room, node, biome, seed, lods };
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
			terrains.delete(key);
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

type HpBar = { hpBack: Mesh; hpFill: Mesh; /** The fill drawn (eases to the health). */ hpShown: number };
/** `culled`: off screen, every mesh disabled and nothing updated. */
type Culled = {
	culled: boolean;
	/** Health share last frame (a drop is a hit); NaN until seen. */
	lastHp: number;
	/** Hit stop: drawn frozen until then (performance.now ms). */
	stopUntil: number;
};
type PlayerVisual = { kind: 'player'; body: Mesh; blade: Mesh; arc: Mesh; nose: Mesh } & HpBar & Culled;
/**
 * `edge`: last seen near its room's side (it may be crossing); `slide`: it
 * just came across from the room next door and glides from where its old
 * room last drew it.
 */
type MobVisual = {
	kind: 'mob';
	/** A mesh, or (the round tiers) a node over its levels of detail. */
	body: TransformNode;
	lod?: Lod;
	ring: Mesh;
	fill: Mesh;
	tier: number;
	edge: boolean;
	slide?: { from: Vector3; start: number };
	/** The floor its slam warning lies on (fetched when a windup starts there). */
	area?: SlamArea;
} & HpBar &
	Culled;
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
	return { kind: 'player', body, blade, arc, nose, culled: false, lastHp: Number.NaN, stopUntil: 0, ...hpBar(entity.key, mats.playerHpFill) };
}

// ---- slam warnings, draped on the floor ----
// A mob's slam hits anyone within its reach standing on whatever floor is
// under them (terrain, ramp or platform top), as the room's `floor_at`
// says; the warning lies on that floor (the `floor_patch` pure, the room's
// own ground) instead of a flat disc that hills and ramps cut off.
const SLAM_RINGS = 6;
const SLAM_SEGMENTS = 32;
const SLAM_LIFT = 0.05;
type SlamArea = { zone: string; x: number; y: number; radius: number; heights: readonly number[] };
function drapedDisc(id: string, mat: StandardMaterial): Mesh {
	const mesh = new Mesh(id, scene);
	const n = 1 + SLAM_RINGS * SLAM_SEGMENTS;
	const index = (r: number, k: number) => (r === 0 ? 0 : 1 + (r - 1) * SLAM_SEGMENTS + (k % SLAM_SEGMENTS));
	const indices: number[] = [];
	for (let k = 0; k < SLAM_SEGMENTS; k += 1) indices.push(0, index(1, k + 1), index(1, k));
	for (let r = 1; r < SLAM_RINGS; r += 1) {
		for (let k = 0; k < SLAM_SEGMENTS; k += 1) {
			indices.push(index(r, k), index(r, k + 1), index(r + 1, k + 1), index(r, k), index(r + 1, k + 1), index(r + 1, k));
		}
	}
	const data = new VertexData();
	data.positions = new Array<number>(n * 3).fill(0);
	data.normals = Array.from({ length: n * 3 }, (_, i) => (i % 3 === 1 ? 1 : 0));
	data.indices = indices;
	data.applyToMesh(mesh, true);
	mesh.material = mat;
	mesh.isPickable = false;
	// Its bounds change as it is draped: always considered for drawing.
	mesh.alwaysSelectAsActiveMesh = true;
	return mesh;
}
/** The floor under a slam of `radius` at room-local `(x, y)`: fetched once per spot. */
function slamArea(visual: MobVisual, zone: string, x: number, y: number, radius: number): SlamArea {
	const old = visual.area;
	if (old !== undefined && old.zone === zone && old.radius === radius && Math.hypot(old.x - x, old.y - y) < 0.05) return old;
	const room = parseRoom(zone);
	const heights =
		room !== undefined && pit.pures.loaded()
			? pit.pures.floorPatch({ seed: pit.hello()?.seed ?? PIT_WORLD.defaultSeed, roomX: room[0], roomY: room[1], x, y, radius, rings: SLAM_RINGS, segments: SLAM_SEGMENTS })
			: new Array<number>(1 + SLAM_RINGS * SLAM_SEGMENTS).fill(groundAt(zone, x, y));
	visual.area = { zone, x, y, radius, heights };
	drape(visual.ring, visual.area, 1);
	return visual.area;
}
/**
 * Lay `mesh` (a `drapedDisc`) on `area` at `scale` of its radius: each
 * ring's height is read along its ray from the area's rings. Over a hole
 * it sinks in.
 */
function drape(mesh: Mesh, area: SlamArea, scale: number): void {
	const at = (r: number, k: number) => area.heights[r === 0 ? 0 : 1 + (r - 1) * SLAM_SEGMENTS + k]!;
	const height = (f: number, k: number) => {
		const r = Math.min(Math.floor(f), SLAM_RINGS - 1);
		const t = Math.min(f - r, 1);
		const a = at(r, k);
		const b = at(r + 1, k);
		const ha = isHole(a) ? at(0, k) - 3 : a;
		const hb = isHole(b) ? at(0, k) - 3 : b;
		return ha + (hb - ha) * t + SLAM_LIFT;
	};
	const positions = new Float32Array((1 + SLAM_RINGS * SLAM_SEGMENTS) * 3);
	positions[1] = height(0, 0);
	for (let r = 1; r <= SLAM_RINGS; r += 1) {
		const f = r * scale;
		const d = (area.radius * r * scale) / SLAM_RINGS;
		for (let k = 0; k < SLAM_SEGMENTS; k += 1) {
			const a = (Math.PI * 2 * k) / SLAM_SEGMENTS;
			const i = (1 + (r - 1) * SLAM_SEGMENTS + k) * 3;
			positions[i] = d * Math.cos(a);
			positions[i + 1] = height(f, k);
			positions[i + 2] = d * Math.sin(a);
		}
	}
	mesh.updateVerticesData('position', positions);
}

/** Three-level bodies of the round tiers (3: a spike, 4: the boss), made on first use. */
const mobLods = new Map<number, Mesh[]>();
function mobLodTemplates(tier: number): Mesh[] | undefined {
	if (tier !== 3 && tier !== 4) return undefined;
	let levels = mobLods.get(tier);
	if (levels === undefined) {
		const t = TIERS[tier]!;
		const top = tier === 4 ? t.radius * 0.8 : 0;
		const height = tier === 4 ? 2.8 : 2;
		levels = lodTemplates(`mob-tier${tier}`, tierMats.get(tier) ?? mats.other, (name, l) =>
			MeshBuilder.CreateCylinder(name, { diameterTop: top, diameterBottom: t.radius * 2, height, tessellation: [40, 12, 4][l] }, scene)
		);
		mobLods.set(tier, levels);
	}
	return levels;
}

function createMob(entity: RenderedEntity): MobVisual {
	const tier = mobFields(entity).tier || 1;
	const t = TIERS[tier] ?? TIERS[1]!;
	// The round tiers in three levels; a box (grunt, brute) cannot go lower.
	const levels = mobLodTemplates(tier);
	let body: TransformNode;
	let lod: Lod | undefined;
	if (levels !== undefined) {
		body = new TransformNode(entity.key, scene);
		lod = lodOf(body, levels);
	} else {
		const box = MeshBuilder.CreateBox(entity.key, { width: t.radius * 2, depth: t.radius * 2, height: tier === 2 ? 1.5 : 0.9 }, scene);
		box.material = tierMats.get(tier) ?? mats.other;
		body = box;
	}
	const ring = drapedDisc(`${entity.key}-tele`, mats.tele);
	const fill = drapedDisc(`${entity.key}-tele-fill`, mats.teleFill);
	return { kind: 'mob', body, lod, ring, fill, tier, edge: false, culled: false, lastHp: Number.NaN, stopUntil: 0, ...hpBar(entity.key, mats.hpFill) };
}

// ---- mobs crossing between rooms ----
// A mob that follows a player across leaves its room (its entity goes) and
// arrives next door as a new entity a message later. Its mesh is held a
// moment where it vanished at a side; the newcomer of the same tier beside
// it takes the mesh over and glides into place: no blink, no hop.
const CROSS_HOLD_MS = 500;
const CROSS_SLIDE_MS = 150;
/** How near (m) a newcomer must be to a held mob to be it. */
const CROSS_MATCH = 3;
const crossing = new Map<string, { visual: MobVisual; until: number }>();
/** Mobs just killed (their entity goes too, but they are not crossing). */
const killedAt = new Map<string, number>();
function adoptCrossing(entity: RenderedEntity): MobVisual | undefined {
	const m = mobFields(entity);
	const arriving = placeIn(entity.zoneId, m.x, m.y, 0);
	for (const [key, held] of crossing) {
		const at = held.visual.body.position;
		if (held.visual.tier !== (m.tier || 1) || Math.hypot(at.x - arriving.x, at.z - arriving.z) > CROSS_MATCH) continue;
		crossing.delete(key);
		held.visual.slide = { from: at.clone(), start: performance.now() };
		return held.visual;
	}
	return undefined;
}

function visualFor(entity: RenderedEntity, mine: boolean): Visual {
	let visual = visuals.get(entity.key);
	if (visual !== undefined) return visual;
	if (isMob(entity)) {
		visual = adoptCrossing(entity) ?? createMob(entity);
	} else {
		visual = createPlayer(entity, mine);
	}
	visuals.set(entity.key, visual);
	return visual;
}

function dispose(visual: Visual): void {
	// (The bar's fill goes with its back, a body's levels with their node.)
	for (const node of Object.values(visual)) if (node instanceof TransformNode && !node.isDisposed()) node.dispose();
}

const HP_W = 1.2;
const HP_FILL_W = 1.16;
/**
 * A health bar over a head: a billboarded back with the fill as its child,
 * just in front of it (never z-fighting with it) and anchored at its left
 * edge, so it drains right to left.
 */
function hpBar(key: string, fillMat: StandardMaterial): HpBar {
	const hpBack = MeshBuilder.CreatePlane(`${key}-hpb`, { width: HP_W, height: 0.14 }, scene);
	hpBack.material = mats.hpBack;
	hpBack.billboardMode = Mesh.BILLBOARDMODE_ALL;
	hpBack.isPickable = false;
	const hpFill = MeshBuilder.CreatePlane(`${key}-hpf`, { width: HP_FILL_W, height: 0.1 }, scene);
	// Origin at the left edge: scaling x shortens it from the right.
	hpFill.position.x = HP_FILL_W / 2;
	hpFill.bakeCurrentTransformIntoVertices();
	hpFill.material = fillMat;
	hpFill.isPickable = false;
	hpFill.parent = hpBack;
	// A plane faces -z; the billboard turns that toward the camera.
	hpFill.position.set(-HP_FILL_W / 2, 0, -0.01);
	return { hpBack, hpFill, hpShown: Number.NaN };
}

/** Place the bar at `at` and ease its fill toward `share` (0..1). */
function showHp(bar: HpBar, at: Vector3, share: number, dtMs: number): void {
	bar.hpBack.position.copyFrom(at);
	const target = Math.max(0, Math.min(1, share));
	bar.hpShown = Number.isNaN(bar.hpShown) ? target : bar.hpShown + (target - bar.hpShown) * (1 - Math.exp(-dtMs / 90));
	if (Math.abs(target - bar.hpShown) < 1e-3) bar.hpShown = target;
	bar.hpFill.scaling.x = Math.max(1e-3, bar.hpShown);
	bar.hpFill.isVisible = bar.hpShown > 2e-3;
}

// ---- kill / saved popups (DOM labels anchored to invisible meshes) ----
type Popup = { mesh: Mesh; until: number; zone: string; x: number; y: number; born: number };
/** A popup rises this far (m) over POPUP_RISE_MS as it appears. */
const POPUP_RISE = 0.5;
const POPUP_RISE_MS = 300;
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
		p = { mesh, until: 0, zone: killRoom(key, zoneId), x, y, born: performance.now() };
		mesh.position.copyFrom(placeIn(p.zone, x, y, 1.2));
		popups.set(key, p);
	}
	p.until = performance.now() + ttlMs;
	stage.labels.set(`popup:${key}`, p.mesh, text, 0.6, `pit-pop ${className}`);
}

// A laser that came through from the room next door: that room's leg of it.
pit.on('beam', (b, zoneId) => {
	const points = [placeIn(zoneId, b.x, b.y, b.z), ...b.path.map((h) => placeIn(zoneId, h.x, h.y, h.z)), placeIn(zoneId, b.endX, b.endY, b.endZ)];
	beam(points, b.hit !== undefined ? beamMats.hit : beamMats.miss, 0.04, BEAM_MS * 2);
});
pit.on('kill', (kill, zoneId) => {
	// Killed, not crossing: never held at the side.
	const key = `${zoneId}:${kill.mob}`;
	killedAt.set(key, performance.now());
	const held = crossing.get(key);
	if (held !== undefined) {
		dispose(held.visual);
		crossing.delete(key);
	}
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

// Ranged: R, a click or RT fires along the view (first person: where you
// look; third person: the camera's heading). The laser bounces off walls,
// obstacles, ramps and the floor; the room resolves it lag-compensated
// (C toggles it per shot).
let fireQueued = false;
let lastFireAt = 0;
let compensated = true;
window.addEventListener('keydown', (e) => {
	if (e.code === 'KeyR' && !e.repeat) fireQueued = true;
});
// Third person, a left drag orbits the camera: fire on release, and only
// if the mouse stayed put. Pointer events, not mouse events: Babylon
// cancels pointerdown, so Chromium sends no mousedown/mouseup.
const CLICK_SLOP_PX = 5;
let clickFrom: { x: number; y: number } | undefined;
canvas.addEventListener('pointerdown', (e) => {
	if (e.button !== 0) return;
	if (stage.view() === 'first') {
		// The first click only captures the mouse.
		if (document.pointerLockElement === canvas) fireQueued = true;
		return;
	}
	clickFrom = { x: e.clientX, y: e.clientY };
});
window.addEventListener('pointerup', (e) => {
	if (e.button !== 0 || clickFrom === undefined) return;
	if (Math.hypot(e.clientX - clickFrom.x, e.clientY - clickFrom.y) <= CLICK_SLOP_PX) fireQueued = true;
	clickFrom = undefined;
});

function sendCommandFrame(): void {
	const s = sampler.sample();
	// RT held fires at the fire cooldown.
	if (s.fire) fireQueued = true;
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
const beamMats = {
	shot: material(scene, 'laser-beam', '#ff3b5c'),
	hit: material(scene, 'laser-hit', '#3ddc84'),
	miss: material(scene, 'laser-miss', '#ff8a8a')
};
for (const m of Object.values(beamMats)) {
	m.emissiveColor = m.diffuseColor.clone();
	m.disableLighting = true;
}
const sparkMat = material(scene, 'laser-spark', '#ffe28a');
sparkMat.emissiveColor = sparkMat.diffuseColor.clone();
sparkMat.disableLighting = true;
sparkMat.backFaceCulling = false;
/** How long a beam and its impact marks stay (ms). */
const BEAM_MS = 220;
const SPARK_MS = 700;
// Beams, impact marks and shot balls come from pools sized for the peak
// (a shot every 0.4 s: about three in flight, each with a predicted and a
// room beam, up to four marks and a ball that waits for its verdict).
/** Points in every pooled beam: the longest path (origin, LASER_BOUNCES + 1 hits, an end). */
const BEAM_POINTS = 8;
type Beam = { tube: Mesh; born: number; ms: number };
const liveBeams = new Set<Beam>();
let beamSeq = 0;
const beamPool = new Pool<Beam>(
	'beams',
	16,
	() => {
		const path = Array.from({ length: BEAM_POINTS }, (_, i) => new Vector3(i, 0, 0));
		const tube = MeshBuilder.CreateTube(`laser-${beamSeq++}`, { path, radius: 0.05, tessellation: 6, cap: Mesh.CAP_ALL, updatable: true }, scene);
		tube.isPickable = false;
		return { tube, born: 0, ms: 1 };
	},
	(b) => b.tube.setEnabled(false)
);
/** `points` stretched to BEAM_POINTS by splitting its last leg evenly (no zero-length legs). */
function beamPath(points: Vector3[]): Vector3[] {
	const path = points.slice(0, BEAM_POINTS);
	const to = path.pop()!;
	const from = path[path.length - 1]!;
	const parts = BEAM_POINTS - path.length;
	for (let i = 1; i <= parts; i += 1) path.push(Vector3.Lerp(from, to, i / parts));
	return path;
}
/** Draw a beam through `points` (scene space) that fades out. */
function beam(points: Vector3[], mat: StandardMaterial, radius: number, ms: number): void {
	if (points.length < 2) return;
	const b = beamPool.take();
	MeshBuilder.CreateTube('', { path: beamPath(points), radius, instance: b.tube });
	b.tube.refreshBoundingInfo();
	b.tube.material = mat;
	b.tube.visibility = 1;
	b.tube.setEnabled(true);
	b.born = performance.now();
	b.ms = ms;
	liveBeams.add(b);
}
scene.onBeforeRenderObservable.add(() => {
	const now = performance.now();
	for (const b of liveBeams) {
		const t = (now - b.born) / b.ms;
		b.tube.visibility = Math.max(0, 1 - t);
		if (t < 1) continue;
		liveBeams.delete(b);
		beamPool.release(b);
	}
});
let sparkSeq = 0;
const sparkPool = new Pool<Mesh>(
	'sparks',
	24,
	() => {
		const disc = MeshBuilder.CreateDisc(`spark-${sparkSeq++}`, { radius: 0.22, tessellation: 10 }, scene);
		disc.material = sparkMat;
		disc.isPickable = false;
		return disc;
	},
	(disc) => disc.setEnabled(false)
);
/** An impact mark at a bounce, lying on the surface (facing its normal). */
function spark(at: Vector3, normal: Vector3): void {
	const disc = sparkPool.take();
	disc.position.copyFrom(at.add(normal.scale(0.02)));
	disc.lookAt(at.add(normal));
	disc.setEnabled(true);
	setTimeout(() => sparkPool.release(disc), SPARK_MS);
}
let ballSeq = 0;
const ballPool = new Pool<Mesh>(
	'shot-balls',
	12,
	() => {
		const ball = MeshBuilder.CreateSphere(`shot-${ballSeq++}`, { diameter: 0.35, segments: 6 }, scene);
		ball.isPickable = false;
		return ball;
	},
	(ball) => ball.setEnabled(false)
);
/** A room-local normal in scene axes. */
const sceneNormal = (n: readonly [number, number, number]) => new Vector3(n[0], n[2], n[1]);
function marker(record: ShotRecord): void {
	// The beam leaves from just below the aim (in first person one from the
	// eye itself would sit on the crosshair) and follows the predicted
	// bounces, one impact mark per surface it met.
	const [ox, oy, oz] = record.origin;
	const points = [placeIn(record.zone, ox, oy, oz - 0.3), ...record.path.map((p) => placeIn(record.zone, p.point[0], p.point[1], p.point[2]))];
	beam(points, beamMats.shot, 0.05, BEAM_MS);
	record.path.forEach((p, i) => {
		if (p.normal !== undefined) spark(points[i + 1]!, sceneNormal(p.normal));
	});
	const ball = ballPool.take();
	ball.material = markerMats.pending;
	ball.position.copyFrom(points[points.length - 1]!);
	ball.setEnabled(true);
	// The verdict and the give-up timer both put it back (only the first counts).
	let back = false;
	const release = () => {
		if (back) return;
		back = true;
		ballPool.release(ball);
	};
	const done = () => setTimeout(release, 1200);
	pit.verdict(record.id).then((v) => {
		if (!back) {
			ball.material = v.hit !== undefined ? markerMats.hit : markerMats.miss;
			ball.position.copyFrom(placeIn(record.zone, v.x, v.y, v.z));
		}
		// The room's own beam, thin: green if it hit.
		const room = [placeIn(record.zone, ox, oy, oz - 0.3), ...v.path.map((h) => placeIn(record.zone, h.x, h.y, h.z))];
		if (v.path.length === 0) room.push(placeIn(record.zone, v.x, v.y, v.z));
		beam(room, v.hit !== undefined ? beamMats.hit : beamMats.miss, 0.02, BEAM_MS * 2);
		if (v.hit !== undefined) landed(0.3);
		done();
	});
	setTimeout(release, 4000);
}
// ---- off-screen culling ----
// Babylon skips drawing what is off screen, but still computes the world
// matrix and bounds of every enabled mesh, and the frame loop would still
// place and animate it. An entity wholly outside the view (with a margin)
// has every mesh disabled and is not updated at all; it snaps to its
// current state when it comes back.
const CULL_MARGIN = 1;
const viewProjection = new Matrix();
const frustumPlanes: Plane[] = Frustum.GetPlanes(Matrix.IdentityReadOnly);
/** This frame's view (the eye's rotation is already this frame's; its position lags a frame, which the margin covers). */
function updateFrustum(): void {
	const camera = scene.activeCamera;
	if (camera === null) return;
	camera.getViewMatrix(true).multiplyToRef(camera.getProjectionMatrix(true), viewProjection);
	Frustum.GetPlanesToRef(viewProjection, frustumPlanes);
}
function onScreen(centre: Vector3, radius: number): boolean {
	const r = radius + CULL_MARGIN;
	return frustumPlanes.every((plane) => plane.dotCoordinate(centre) > -r);
}
function setCulled(visual: Visual, culled: boolean): void {
	if (visual.culled === culled) return;
	visual.culled = culled;
	// Disabled: off Babylon's active-mesh pass. The bar's fill goes with its
	// back (the frame re-enables only top-level meshes).
	if (culled) for (const node of Object.values(visual)) if (node instanceof TransformNode && node.parent === null) node.setEnabled(false);
	// Back on screen: the bar snaps to the health instead of easing from the old one.
	else visual.hpShown = Number.NaN;
}
/** Per-frame cost of the entity loop and of Babylon's render (CPU), smoothed. */
const cullStats = { drawn: 0, culled: 0, loopMs: 0, renderMs: 0 };
const smooth = (avg: number, sample: number) => avg + (sample - avg) * 0.05;
let renderStart = 0;
scene.onBeforeRenderObservable.add(() => (renderStart = performance.now()));
scene.onAfterRenderObservable.add(() => (cullStats.renderMs = smooth(cullStats.renderMs, performance.now() - renderStart)));

// ---- hit feel: hit stop and screen shake ----
// Drawing only: the rooms simulate everyone at one pace, so a stop freezes
// the drawn attacker and target and they catch up after it. Input is read
// and sent throughout; bars, beams and marks keep moving.
/** How long a hit freezes them (ms): the least, and the most for a big hit. */
const STOP_MS = [50, 100] as const;
/** A hit taking this share of health (or more) stops longest and shakes hardest. */
const BIG_HIT = 0.25;
const trauma = new Trauma({ x: 0.2, y: 0.14, roll: 0.05 });
/** A mob this close to you when you are hit is taken as the one that hit you (m). */
const ATTACKER_NEAR = 5;
const hitSize = (drop: number) => Math.min(1, drop / BIG_HIT);
/** Hit stops so far (for tests and the console). */
let hitStops = 0;
function hitStop(visual: Visual, size: number): void {
	hitStops += 1;
	visual.stopUntil = Math.max(visual.stopUntil, performance.now() + STOP_MS[0] + (STOP_MS[1] - STOP_MS[0]) * size);
}
/** Your drawn self (set by the frame loop). */
let myVisual: PlayerVisual | undefined;
let mySwing = 0;
/** One of your hits landed (`size` 0..1): you pause and the view jolts a little. */
function landed(size: number): void {
	if (myVisual !== undefined) hitStop(myVisual, size);
	trauma.add(0.3);
}
type Hit = { visual: Visual; size: number };
/** Hits seen this frame (health drops), attributed once every entity is placed. */
const hits: Hit[] = [];
function attributeHits(): void {
	for (const { visual, size } of hits) {
		hitStop(visual, size);
		if (visual === myVisual) {
			trauma.add(0.3 + 0.3 * size);
			// The nearest mob is taken as the one that hit you.
			let nearest: Visual | undefined;
			let best = ATTACKER_NEAR;
			for (const other of visuals.values()) {
				if (other.kind !== 'mob' || other.culled) continue;
				const d = Vector3.Distance(other.body.position, visual.body.position);
				if (d < best) [nearest, best] = [other, d];
			}
			if (nearest !== undefined) hitStop(nearest, size);
		} else if (visual.kind === 'mob' && myVisual !== undefined && mySwing > 0) {
			// Struck while your blade is out and within its reach: yours.
			if (Vector3.Distance(visual.body.position, myVisual.body.position) < ATTACK_REACH + 1) landed(size);
		}
	}
	hits.length = 0;
}
/** Note a hit when `visual`'s health share fell since last frame. */
function watchHealth(visual: Visual, share: number): void {
	const drop = visual.lastHp - share;
	if (drop > 1e-6) hits.push({ visual, size: hitSize(drop) });
	visual.lastHp = share;
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
	const loopStart = performance.now();
	updateFrustum();
	// Levels of detail from the camera (its last rendered spot; the band absorbs a frame's move).
	const eye = scene.activeCamera?.globalPosition ?? Vector3.ZeroReadOnly;
	lodCounts.fill(0);
	for (const chunk of chunks.values()) for (const lod of chunk.lods) applyLod(lod, eye);
	let drawn = 0;
	let culled = 0;
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
		if (visual.kind === 'mob') watchHealth(visual, mobFields(entity).hp / 100);
		else {
			const p = playerFields(entity);
			watchHealth(visual, p.dead ? 0 : p.hp / PLAYER_HP);
			if (mine) {
				myVisual = visual;
				mySwing = p.swing;
			}
		}
		// Off screen (never your own): nothing to place or animate.
		if (cullOn && !mine && visible) {
			const f = visual.kind === 'mob' ? mobFields(entity) : playerFields(entity);
			const lift = visual.kind === 'mob' ? groundAt(entity.zoneId, f.x, f.y) : 'z' in f ? f.z : 0;
			const radius = visual.kind === 'mob' ? Math.max(TIERS[visual.tier]?.tele ?? 1, 1.5) : ATTACK_REACH;
			const out = !onScreen(at(f.x, f.y, lift + 1), radius);
			setCulled(visual, out);
			if (out) {
				culled += 1;
				continue;
			}
		} else setCulled(visual, false);
		drawn += 1;
		if (visual.kind === 'mob') {
			const mob = mobFields(entity);
			// Hit stop: held where it was (the bar still drains).
			if (now < visual.stopUntil) {
				showHp(visual, visual.hpBack.position, mob.hp / 100, dtMs);
				continue;
			}
			const { x, y } = mob;
			const ground = groundAt(entity.zoneId, x, y);
			const height = ground + (visual.tier === 4 ? 1.4 : visual.tier === 3 ? 1 : visual.tier === 2 ? 0.75 : 0.45);
			visual.edge = Math.max(Math.abs(x), Math.abs(y)) > ROOM_SIZE / 2 - CROSS_MATCH;
			const slide = visual.slide;
			const k = slide === undefined ? 1 : (now - slide.start) / CROSS_SLIDE_MS;
			if (slide !== undefined && k < 1) visual.body.position.copyFrom(Vector3.Lerp(slide.from, at(x, y, height), easeOutCubic(k)));
			else {
				visual.slide = undefined;
				visual.body.position.copyFrom(at(x, y, height));
			}
			visual.body.rotation.y = -mob.face;
			if (visual.lod !== undefined) applyLod(visual.lod, eye);
			const hp = mob.hp / 100;
			const top = height * 2 - ground + 0.35;
			showHp(visual, at(x, y, top), hp, dtMs);
			const tele = mob.tele / 255;
			const winding = mob.state === 2;
			visual.ring.setEnabled(visible && winding);
			visual.fill.setEnabled(visible && winding);
			if (winding) {
				const area = slamArea(visual, entity.zoneId, x, y, TIERS[visual.tier]?.tele ?? 1);
				visual.ring.position.copyFrom(at(x, y, 0));
				visual.fill.position.copyFrom(at(x, y, 0.01));
				drape(visual.fill, area, Math.max(0.05, tele));
			}
			visual.body.scaling.setAll(winding ? 1 + 0.15 * tele : 1);
			for (const m of [visual.body, visual.hpBack]) m.setEnabled(visible);
		} else {
			const player = playerFields(entity);
			const { x, y, z, dead, face, swing } = player;
			if (now < visual.stopUntil) {
				showHp(visual, visual.hpBack.position, dead ? 0 : player.hp / PLAYER_HP, dtMs);
				if (mine) me = { at: visual.body.position, hp: player.hp, dead };
				continue;
			}
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
			const label = `${player.name} · L${player.level}`;
			// First person: you are the camera (no body, no label or bar of your own).
			const hidden = mine && stage.view() === 'first';
			if (hidden) stage.labels.delete(entity.key);
			else stage.labels.set(entity.key, visual.body, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			const top = 2.05 + z;
			showHp(visual, at(x, y, top), dead ? 0 : player.hp / PLAYER_HP, dtMs);
			visual.hpBack.setEnabled(visible && !hidden);
			visual.body.setEnabled(visible && !hidden);
			visual.nose.setEnabled(visible && !dead && !hidden);
			if (mine) me = { at: visual.body.position, hp: player.hp, dead };
		}
	}
	attributeHits();
	cullStats.drawn = drawn;
	cullStats.culled = culled;
	cullStats.loopMs = smooth(cullStats.loopMs, performance.now() - loopStart);
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		// Gone at a side, not killed: it may be crossing (held a moment).
		if (visual.kind === 'mob' && visual.edge && !visual.culled && !killedAt.has(key)) {
			crossing.set(key, { visual, until: now + CROSS_HOLD_MS });
			visuals.delete(key);
			continue;
		}
		dispose(visual);
		stage.labels.delete(key);
		visuals.delete(key);
	}
	for (const [key, held] of crossing) {
		if (now < held.until) continue;
		dispose(held.visual);
		crossing.delete(key);
	}
	for (const [key, at] of killedAt) if (now - at > 2000) killedAt.delete(key);
	for (const [key, p] of popups) {
		p.mesh.position.copyFrom(placeIn(p.zone, p.x, p.y, 1.2 + tween(0, POPUP_RISE, now - p.born, POPUP_RISE_MS)));
		if (now < p.until) continue;
		p.mesh.dispose();
		stage.labels.delete(`popup:${key}`);
		popups.delete(key);
	}
	if (me !== undefined) stage.follow(me.at.x, me.at.z, dtMs, me.at.y - 0.9);
	stage.shake(trauma.update(dtMs / 1000));
	crosshair.style.display = stage.view() === 'first' ? '' : 'none';
	hpRow.style.display = stage.view() === 'first' && me !== undefined ? '' : 'none';
	if (me !== undefined) {
		const share = me.dead ? 0 : Math.max(0, Math.min(1, me.hp / PLAYER_HP));
		hpLabel.textContent = me.dead ? 'HP — dead' : `HP ${me.hp} / ${PLAYER_HP}`;
		hpFillBar.style.width = `${share * 100}%`;
		hpFillBar.className = `pit-hp-fill${share < 0.3 ? ' low' : share < 0.6 ? ' mid' : ''}`;
	}

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
	hud.row('Entities drawn / off screen', `${cullStats.drawn} / ${cullStats.culled}`);
	hud.row('LOD near / mid / far', `${lodCounts[0]} / ${lodCounts[1]} / ${lodCounts[2]}${lodOn ? '' : ' (off: all full)'}`);
	hud.row('Triangles drawn', String(Math.round(scene.getActiveIndices() / 3)));
	hud.row('Frame CPU', `entities ${cullStats.loopMs.toFixed(2)} ms · render ${cullStats.renderMs.toFixed(2)} ms`);
	hud.row('Ghost gap', ghostGap === undefined ? '—' : `${ghostGap.toFixed(2)} m`);
	const around = [...pit.neighbours().values()];
	hud.row('Next door', `${around.filter((s) => s === 'watching').length} rooms watched · ${around.filter((s) => s === 'asleep').length} asleep (not woken)`);
	const awake = [...pit.near()];
	hud.row('Wake rooms in reach (N)', pit.wakeRooms() ? `on · ${awake.length === 0 ? 'no doorway in reach' : `keeping ${awake.join(', ')} awake`}` : 'off (every neighbour a passive watcher)');
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
window.mobpit = {
	client,
	api,
	pitApi,
	pit,
	scene,
	rooms,
	boards,
	camera: stage.camera,
	origin: () => origin,
	feel: () => ({
		trauma: trauma.value,
		hitStops,
		pools: Object.fromEntries([beamPool, sparkPool, ballPool].map((p) => [p.name, { size: p.size, active: p.active }]))
	})
};

// The same browser is the same player: back in the room you were last in.
(async () => {
	// Start on the world's live content (the packs are built in).
	const live = await pitApi.content().catch(() => undefined);
	client.setContentVersion(live?.state?.version ?? '');
	const zone = forcedZone ?? (await pitApi.startRoom(api.guest).catch(() => DEFAULT_ZONE));
	origin = parseRoom(zone) ?? origin;
	await client.connect(zone);
})().catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
