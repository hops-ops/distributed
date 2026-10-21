/**
 * Mob Pit page: Babylon scene over `createMobPitClient`.
 *
 * What to look at: rooms are celld Durable Objects on a near-infinite
 * seeded grid. Walk through a doorway (the gaps in the middle of each wall)
 * into the next room; the minimap (bottom right) shows every known room
 * live — active, dormant (just left), hibernated (asleep in storage) — from
 * the `zone_status` read model. Come back to a room after it slept: it
 * wakes from its snapshot with the mobs you left (toast). Kill a mob
 * (Space): the grey "saving…" popup turns into "+XP saved ✓" when the kill
 * settled in your Crawler cell. The debug overlay (O) shows which celld
 * node and isolate run the room — celld decides, the game never does.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Color3 } from '@babylonjs/core/Maths/math.color';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { ARENA_FLOOR } from '../generated/arena/constants.ts';
import {
	BUTTON,
	PIT_RULES,
	PIT_WORLD,
	isMob,
	isPillar,
	isPit,
	mobFields,
	pillarFields,
	pitFields,
	playerFields,
	type PitFields,
	type PitInput
} from '../generated/mobpit/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createStage, material, toWorld } from '../shared/scene.ts';
import {
	createBoardWatcher,
	createMobPitClient,
	DEFAULT_ZONE,
	MobPitApi,
	parseRoom,
	roomZone,
	TIERS,
	type BoardSnapshot,
	type RoomInfo,
	type RoomStatus
} from './client.ts';

/** The Arena floor, generated from the simulation's constants (`game_sim::arena::arena_floor`). */
const { half: HALF, playerRadius: PLAYER_RADIUS } = ARENA_FLOOR;
/** Swing cone, from the server's rules: reach to a mob's edge, plus the body. */
const { attackRange, attackHalfAngle: SWING_HALF, swingFrames: SWING_FRAMES } = PIT_RULES;
const ATTACK_REACH = attackRange + PLAYER_RADIUS;

const params = new URLSearchParams(location.search);
const zone = params.get('zone') ?? DEFAULT_ZONE;
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
	hint: 'WASD move · Space attack · E dash · walk through a doorway to the next room · O debug overlay. Rooms run on celld; kills are durable facts.',
	client,
	devApi: api
});
let debug = params.get('debug') !== '0';
hud.toggle('Debug overlay (celld node per room)', debug, (on) => (debug = on), 'o');

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
	teleFill: material(scene, 'tele-fill', '#ff3b3b', 0.45)
};
for (const m of [mats.swing, mats.tele, mats.teleFill, mats.hpFill]) m.emissiveColor = m.diffuseColor.clone();
mats.blade.emissiveColor = new Color3(0.6, 0.6, 0.7);
const tierMats = new Map<number, StandardMaterial>();
for (const [tier, t] of Object.entries(TIERS)) tierMats.set(Number(tier), material(scene, `mob-${tier}`, t.color));

// Doorways: a glowing gate in the middle of each wall, labelled with the
// room behind it; the floor takes the room's biome colour.
const doorMat = material(scene, 'door', '#4dd0e1', 0.55);
doorMat.emissiveColor = Color3.FromHexString('#4dd0e1');
const DOORS: { dx: number; dy: number }[] = [
	{ dx: 1, dy: 0 },
	{ dx: -1, dy: 0 },
	{ dx: 0, dy: 1 },
	{ dx: 0, dy: -1 }
];
const doorMeshes = DOORS.map(({ dx, dy }, i) => {
	const gate = MeshBuilder.CreateBox(
		`door-${i}`,
		{ width: dx === 0 ? PIT_WORLD.doorHalf * 2 : 0.3, depth: dy === 0 ? PIT_WORLD.doorHalf * 2 : 0.3, height: 0.2 },
		scene
	);
	gate.material = doorMat;
	gate.isPickable = false;
	gate.position.copyFrom(toWorld(dx * HALF, dy * HALF, 0.1));
	return gate;
});
const biomeFloor = MeshBuilder.CreateGround('biome', { width: HALF * 2, height: HALF * 2 }, scene);
biomeFloor.position.y = 0.01;
biomeFloor.isPickable = false;
const biomeMat = material(scene, 'biome', '#3f7d4e', 0.18);
biomeFloor.material = biomeMat;
let shownRoom = '';

function showRoom(zone: string): void {
	const at = parseRoom(zone);
	if (at === undefined || zone === shownRoom) return;
	shownRoom = zone;
	DOORS.forEach(({ dx, dy }, i) => {
		stage.labels.set(`door-${i}`, doorMeshes[i]!, `→ ${roomZone(at[0] + dx, at[1] + dy)}`, 0.6, 'door');
	});
	const seed = pit.hello()?.seed ?? PIT_WORLD.defaultSeed;
	if (pit.pures.loaded()) {
		const room = pit.pures.pitRoom({ seed, x: at[0], y: at[1] });
		biomeMat.diffuseColor = Color3.FromHexString(BIOME_COLOR[room.biome] ?? '#3f7d4e');
		hud.row('Room', `${zone} · ${PIT_WORLD.biomes[room.biome] ?? '?'} · danger ${room.danger} · ${room.mobs.length} mobs`);
	}
}

type PlayerVisual = { kind: 'player'; body: Mesh; blade: Mesh; arc: Mesh; nose: Mesh };
type MobVisual = { kind: 'mob'; body: Mesh; hpBack: Mesh; hpFill: Mesh; ring: Mesh; fill: Mesh; tier: number };
type PillarVisual = { kind: 'pillar'; body: Mesh };
type Visual = PlayerVisual | MobVisual | PillarVisual;
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
	} else if (isPillar(entity)) {
		const { r } = pillarFields(entity);
		const body = MeshBuilder.CreateCylinder(entity.key, { diameter: r * 2, height: 2.4, tessellation: 24 }, scene);
		body.material = mats.pillar;
		visual = { kind: 'pillar', body };
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
type Popup = { mesh: Mesh; until: number };
const popups = new Map<string, Popup>();
let popupSeq = 0;

function popup(key: string, x: number, y: number, text: string, className: string, ttlMs: number): void {
	let p = popups.get(key);
	if (p === undefined) {
		const mesh = MeshBuilder.CreateBox(`popup-${popupSeq++}`, { size: 0.01 }, scene);
		mesh.isVisible = false;
		mesh.position.copyFrom(toWorld(x, y, 1.2));
		p = { mesh, until: 0 };
		popups.set(key, p);
	}
	p.until = performance.now() + ttlMs;
	stage.labels.set(`popup:${key}`, p.mesh, text, 0.6, `pit-pop ${className}`);
}

pit.on('kill', (kill) => {
	const mine = kill.killer === pit.hello()?.entity;
	const tier = TIERS[kill.tier]?.name ?? 'Mob';
	if (mine) popup(kill.key, kill.x, kill.y, `${tier} slain · saving…`, 'pending', 60_000);
	else popup(kill.key, kill.x, kill.y, `${kill.killerName} slew a ${tier}`, 'other', 2000);
});
let savedCount = 0;
pit.on('saved', (saved) => {
	if (saved.ok) {
		savedCount += 1;
		const waited = saved.waitedMs >= 200 ? ` (${(saved.waitedMs / 1000).toFixed(1)} s)` : '';
		popup(saved.key, saved.x, saved.y, `+${saved.xp} XP saved ✓${waited}`, 'saved', 2500);
	} else {
		popup(saved.key, saved.x, saved.y, `save failed: ${saved.error ?? '?'}`, 'failed', 4000);
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
	const hello = pit.hello();
	const authority = client.authority;
	const rendered = client.render();
	const seen = new Set<string>();
	let me: { x: number; y: number; hp: number; dead: boolean } | undefined;
	let stats: PitFields | undefined;
	for (const entity of rendered) {
		if (isPit(entity)) {
			stats = pitFields(entity);
			continue;
		}
		const mine = authority !== undefined && hello !== undefined && entity.zoneId === authority.zoneId && entity.id === hello.entity;
		const visual = visualFor(entity, mine);
		seen.add(entity.key);
		const visible = !entity.stale || visual.kind === 'pillar';
		if (visual.kind === 'pillar') {
			const { x, y } = pillarFields(entity);
			visual.body.position.copyFrom(toWorld(x, y, 1.2));
		} else if (visual.kind === 'mob') {
			const mob = mobFields(entity);
			const { x, y } = mob;
			const height = visual.tier === 3 ? 1 : visual.tier === 2 ? 0.75 : 0.45;
			visual.body.position.copyFrom(toWorld(x, y, height));
			visual.body.rotation.y = -mob.face;
			const hp = mob.hp / 100;
			const top = height * 2 + 0.35;
			visual.hpBack.position.copyFrom(toWorld(x, y, top));
			visual.hpFill.position.copyFrom(toWorld(x, y, top));
			visual.hpFill.scaling.x = Math.max(0.001, hp);
			const tele = mob.tele / 255;
			const winding = mob.state === 2;
			visual.ring.setEnabled(visible && winding);
			visual.fill.setEnabled(visible && winding);
			if (winding) {
				visual.ring.position.copyFrom(toWorld(x, y, 0.03));
				visual.fill.position.copyFrom(toWorld(x, y, 0.04));
				visual.fill.scaling.setAll(Math.max(0.05, tele));
			}
			visual.body.scaling.setAll(winding ? 1 + 0.15 * tele : 1);
			for (const m of [visual.body, visual.hpBack, visual.hpFill]) m.setEnabled(visible);
		} else {
			const player = playerFields(entity);
			const { x, y, dead, face, swing } = player;
			visual.body.position.copyFrom(toWorld(x, y, 0.9));
			visual.body.material = dead ? mats.dead : mine ? mats.me : mats.other;
			visual.nose.position.copyFrom(toWorld(x + Math.cos(face) * 0.5, y + Math.sin(face) * 0.5, 1.3));
			visual.nose.rotation.y = -face;
			// Blade sweeps through the swing cone while `swing` counts down to 0.
			const sweep = swing > 0 ? ((SWING_FRAMES - swing) / SWING_FRAMES) * 2 * SWING_HALF - SWING_HALF : -Math.PI / 2.5;
			const a = face + sweep;
			visual.blade.position.copyFrom(toWorld(x + Math.cos(a) * 0.8, y + Math.sin(a) * 0.8, 1.0));
			visual.blade.rotation.y = -a;
			visual.blade.setEnabled(visible && !dead);
			visual.arc.setEnabled(visible && swing > 0);
			if (swing > 0) {
				visual.arc.position.copyFrom(toWorld(x, y, 0.05));
				// Disc arcs start at +x and run counter-clockwise; centre it on `face`.
				visual.arc.rotation.y = -(face - SWING_HALF);
			}
			const label = `${player.name} · L${player.level} · ${player.hp} HP`;
			stage.labels.set(entity.key, visual.body, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			visual.body.setEnabled(visible);
			visual.nose.setEnabled(visible && !dead);
			if (mine) me = { x, y, hp: player.hp, dead };
		}
	}
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		dispose(visual);
		stage.labels.delete(key);
		visuals.delete(key);
	}
	for (const [key, p] of popups) {
		if (now < p.until) continue;
		p.mesh.dispose();
		stage.labels.delete(`popup:${key}`);
		popups.delete(key);
	}
	if (me !== undefined) stage.follow(me.x, me.y, dtMs);

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
	const here = pit.room();
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
window.mobpit = { client, api, pitApi, pit, scene, rooms, boards };

client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
