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
import { Color3 } from '@babylonjs/core/Maths/math.color';
import { Vector3 } from '@babylonjs/core/Maths/math.vector';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { TransformNode } from '@babylonjs/core/Meshes/transformNode';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import {
	BUTTON,
	PIT_RULES,
	PIT_WORLD,
	isMob,
	isPillar,
	isPit,
	mobFields,
	pitFields,
	playerFields,
	type PitFields,
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
	type RoomInfo,
	type RoomStatus
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
const pit = createMobPitClient({ api });
const { client } = pit;

const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: HALF, background: '#120f14' });
const { scene } = stage;

const hud = new Hud({
	title: 'Mob Pit',
	hint: 'WASD move · Space attack · E dash · walk through a doorway to the next room · O debug overlay · P prediction · G server ghost. Rooms run on celld; kills are durable facts.',
	client,
	devApi: api
});
let debug = params.get('debug') !== '0';
hud.toggle('Debug overlay (celld node per room)', debug, (on) => (debug = on), 'o');
hud.toggle('Client prediction', true, (on) => client.setPredictionEnabled(on), 'p');
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
deathBanner.style.display = 'none';

function div(className: string, parent: HTMLElement): HTMLDivElement {
	const d = document.createElement('div');
	d.className = className;
	parent.append(d);
	return d;
}

// ---- rooms: minimap from the zone status read model, toasts, debug ----
const roomsPanel = div('pit-rooms', document.body);
roomsPanel.innerHTML = `<h2>Rooms <small data-k="here"></small></h2>
	<canvas width="210" height="210"></canvas>
	<div class="pit-legend"><span class="c-active">■ active</span> <span class="c-dormant">■ dormant</span> <span class="c-hibernated">■ asleep</span> <span class="c-cold">■ never ran</span></div>
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

function drawRooms(): void {
	const ctx = roomsMap.getContext('2d');
	const here = parseRoom(pit.room() ?? DEFAULT_ZONE);
	if (ctx === null || here === undefined) return;
	const { width } = roomsMap;
	const span = 7;
	const cell = width / span;
	ctx.clearRect(0, 0, width, width);
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	for (let j = 0; j < span; j++) {
		for (let i = 0; i < span; i++) {
			const x = here[0] + i - 3;
			const y = here[1] + (span - 1 - j) - 3;
			const zone = roomZone(x, y);
			const row = rooms.get(zone);
			let color = '#20252e';
			if (row !== undefined) color = STATUS_COLOR[row.status] ?? color;
			else if (pit.pures.loaded()) color = `${BIOME_COLOR[pit.pures.pitRoom({ seed, x, y }).biome] ?? '#333'}55`;
			ctx.fillStyle = color;
			ctx.fillRect(i * cell + 2, j * cell + 2, cell - 4, cell - 4);
			if (x === here[0] && y === here[1]) {
				ctx.strokeStyle = '#ffffff';
				ctx.lineWidth = 2;
				ctx.strokeRect(i * cell + 1, j * cell + 1, cell - 2, cell - 2);
			}
			ctx.fillStyle = '#0e1116';
			ctx.font = '10px monospace';
			if (row !== undefined && row.players > 0) ctx.fillText(`${row.players}p`, i * cell + 5, j * cell + 13);
			if (debug && row !== undefined && row.node !== '') ctx.fillText(row.node.replace(/^node-/, ''), i * cell + 5, j * cell + cell - 6);
		}
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
	return { floors, wall, segment, gate, pillar };
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
	for (const side of [-1, 1]) {
		for (const along of [-1, 1]) {
			add(templates.wall, `wall-h${side}${along}`, along * off, side * out, WALL_H / 2);
			add(templates.wall, `wall-v${side}${along}`, side * out, along * off, WALL_H / 2, Math.PI / 2);
		}
		add(templates.gate, `door-h${side}`, 0, side * HALF, 0.04);
		add(templates.gate, `door-v${side}`, side * HALF, 0, 0.04, Math.PI / 2);
	}
	PILLARS.forEach(({ x, y, r }, i) => {
		const instance = add(templates.pillar, `pillar-${i}`, x, y, 1.2);
		instance.scaling.set(r * 2, 1, r * 2);
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
		hud.row('Room', `${zone} · ${PIT_WORLD.biomes[room.biome] ?? '?'} · danger ${room.danger} · ${room.mobs.length} mobs`);
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
		tier === 3
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
	const mine = kill.killer === pit.hello()?.entity;
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

function sendCommandFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	const input: PitInput = {
		mx: toAxis(move.x),
		my: toAxis(move.y),
		buttons: (s.action ? BUTTON.attack : 0) | (s.alt ? BUTTON.dash : 0)
	};
	client.sendInput(input);
}

// ---- frame ----
stage.onFrame((now, dtMs) => {
	commandFrames.update(now, sendCommandFrame);
	const here = pit.room();
	const hereRoom = here === undefined ? undefined : parseRoom(here);
	if (hereRoom !== undefined) moveOrigin(hereRoom);
	const hello = pit.hello();
	const authority = client.authority;
	const rendered = client.render();
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
		if (visual.kind === 'mob') {
			const mob = mobFields(entity);
			const { x, y } = mob;
			const height = visual.tier === 3 ? 1 : visual.tier === 2 ? 0.75 : 0.45;
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
			const { x, y, dead, face, swing } = player;
			visual.body.position.copyFrom(at(x, y, 0.9));
			visual.body.material = dead ? mats.dead : mine ? mats.me : mats.other;
			visual.nose.position.copyFrom(at(x + Math.cos(face) * 0.5, y + Math.sin(face) * 0.5, 1.3));
			visual.nose.rotation.y = -face;
			// Blade sweeps through the swing cone while `swing` counts down to 0.
			const sweep = swing > 0 ? ((SWING_FRAMES - swing) / SWING_FRAMES) * 2 * SWING_HALF - SWING_HALF : -Math.PI / 2.5;
			const a = face + sweep;
			visual.blade.position.copyFrom(at(x + Math.cos(a) * 0.8, y + Math.sin(a) * 0.8, 1.0));
			visual.blade.rotation.y = -a;
			visual.blade.setEnabled(visible && !dead);
			visual.arc.setEnabled(visible && swing > 0);
			if (swing > 0) {
				visual.arc.position.copyFrom(at(x, y, 0.05));
				// Disc arcs start at +x and run counter-clockwise; centre it on `face`.
				visual.arc.rotation.y = -(face - SWING_HALF);
			}
			const label = `${player.name} · L${player.level} · ${player.hp} HP`;
			stage.labels.set(entity.key, visual.body, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			visual.body.setEnabled(visible);
			visual.nose.setEnabled(visible && !dead);
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
	if (me !== undefined) stage.follow(me.at.x, me.at.z, dtMs);

	// The server's own copy of you (authoritative, behind by the round
	// trip): a wireframe ghost in the room that owns you.
	const authoritative = client.predictor?.authoritative();
	let ghostGap: number | undefined;
	if (authoritative !== undefined && authority !== undefined) {
		const g = playerFields({ fields: authoritative });
		ghost.position.copyFrom(placeIn(authority.zoneId, g.x, g.y, 0.9));
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
	if (now - lastSampleAt > 250) {
		drawRooms();
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
	const zone = forcedZone ?? (await pitApi.startRoom(api.guest).catch(() => DEFAULT_ZONE));
	origin = parseRoom(zone) ?? origin;
	await client.connect(zone);
})().catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
