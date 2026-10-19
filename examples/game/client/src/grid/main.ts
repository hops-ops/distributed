/**
 * Endless Grid page: Babylon scene over `createGridClient`.
 *
 * - terrain, biomes and props of the 5×5 cells around you are generated
 *   locally from the seed (`world.ts`, bit-identical to the server) —
 *   nothing static comes over the wire;
 * - cell borders are tinted by the host that runs the cell; the minimap
 *   (`/dev/grid/map`) shows active / hibernated / cold cells per host;
 * - your walker is predicted; everything else (critters, other walkers,
 *   markers) is interpolated, from your cell and the 8 observed neighbours;
 * - crossing a border hands you off (maybe to the other host); entering a
 *   cell that was hibernated shows a "restored from hibernation" toast.
 *
 * Rendering uses a floating origin at your current cell so coordinates stay
 * small however far you walk.
 */
import { VertexBuffer } from '@babylonjs/core/Buffers/buffer';
import { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Color3, Color4 } from '@babylonjs/core/Maths/math.color';
import { Matrix, Vector3 } from '@babylonjs/core/Maths/math.vector';
import type { AbstractMesh } from '@babylonjs/core/Meshes/abstractMesh';
import type { InstancedMesh } from '@babylonjs/core/Meshes/instancedMesh';
import type { LinesMesh } from '@babylonjs/core/Meshes/linesMesh';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { VertexData } from '@babylonjs/core/Meshes/mesh.vertexData';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { Scene } from '@babylonjs/core/scene';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createStage, material } from '../shared/scene.ts';
import { createGridClient, DEFAULT_ZONE, KIND, type GridHello, type MapCell, type WorldMap } from './client.ts';
import {
	BIOMES,
	BUTTON_MARK,
	BUTTON_SPRINT,
	CELL,
	PLAYER_RADIUS,
	PROP_BUSH,
	PROP_ROCK,
	PROP_TREE,
	biomeOf,
	height,
	moisture,
	parseZone,
	zoneId,
	type Cell,
	type MoveInput
} from './world.ts';
import './grid.css';

const params = new URLSearchParams(location.search);
const startZone = params.get('zone') ?? DEFAULT_ZONE;
const name = params.get('name') ?? undefined;
const VIEW_CELLS = 2;
const RELIEF = 4;
const HOST_COLORS = ['#4ea1ff', '#ff9f43', '#b48cff', '#5fd39a'];

const api = new DevApi({ name: () => name });
const grid = createGridClient({ api });
const { client } = grid;

const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: 1, background: '#a9c7e6' });
const { scene, camera } = stage;
for (const mesh of [...scene.meshes]) if (mesh.name === 'floor' || mesh.name === 'wall') mesh.dispose();
camera.radius = 26;
camera.upperRadiusLimit = 70;
scene.fogMode = Scene.FOGMODE_EXP2;
scene.fogDensity = 0.011;
scene.fogColor = Color3.FromHexString('#a9c7e6');
scene.clearColor = Color4.FromHexString('#a9c7e6ff');

// ---- hosts ----
const hostOrder: string[] = [];
function hostColor(host: string | undefined): string {
	if (host === undefined) return '#8a8f99';
	if (!hostOrder.includes(host)) {
		hostOrder.push(host);
		hostOrder.sort();
	}
	return HOST_COLORS[hostOrder.indexOf(host) % HOST_COLORS.length]!;
}
const owners = new Map<string, string>();

// ---- floating origin ----
let origin: Cell = parseZone(startZone) ?? [0, 0];
function local(cell: Cell, x: number, y: number, up = 0): Vector3 {
	return new Vector3((cell[0] - origin[0]) * CELL + x, terrain(cell, x, y) + up, (cell[1] - origin[1]) * CELL + y);
}
function terrain(cell: Cell, x: number, y: number): number {
	const cx = Math.min(CELL, Math.max(0, x));
	const cy = Math.min(CELL, Math.max(0, y));
	return height(grid.seed(), cell, cx, cy) * RELIEF;
}

// ---- terrain chunks ----
const groundMat = new StandardMaterial('ground', scene);
groundMat.specularColor = Color3.Black();
const BIOME_COLORS = [
	Color3.FromHexString('#8c8579'),
	Color3.FromHexString('#c9b36b'),
	Color3.FromHexString('#3d7638'),
	Color3.FromHexString('#7cae4c')
];

/** Instancing templates; a tree is two instances (trunk + canopy). */
const templates = (() => {
	const trunk = MeshBuilder.CreateCylinder('trunk', { diameter: 0.5, height: 2.2, tessellation: 8 }, scene);
	trunk.position.y = 1.1;
	trunk.bakeCurrentTransformIntoVertices();
	trunk.material = material(scene, 'bark', '#6b4a2f');
	const canopy = MeshBuilder.CreateCylinder('canopy', { diameterTop: 0, diameterBottom: 3, height: 3.6, tessellation: 9 }, scene);
	canopy.position.y = 3.6;
	canopy.bakeCurrentTransformIntoVertices();
	canopy.material = material(scene, 'leaves', '#2f6b34');
	const rock = MeshBuilder.CreateIcoSphere('rock', { radius: 1, subdivisions: 1, flat: true }, scene);
	rock.scaling.y = 0.6;
	rock.bakeCurrentTransformIntoVertices();
	rock.material = material(scene, 'stone', '#6f737a');
	const bush = MeshBuilder.CreateSphere('bush', { diameter: 2, segments: 6 }, scene);
	bush.scaling.y = 0.55;
	bush.bakeCurrentTransformIntoVertices();
	bush.material = material(scene, 'shrub', '#4f8f3a');
	for (const m of [trunk, canopy, rock, bush]) m.setEnabled(false);
	return { [PROP_TREE]: [trunk, canopy], [PROP_ROCK]: [rock], [PROP_BUSH]: [bush] } as Record<number, Mesh[]>;
})();

// Props are not parented to the ground: instanced children of a moved
// parent rendered fully fogged (white) here, so they are placed directly.
type Chunk = { cell: Cell; ground: Mesh; props: { mesh: InstancedMesh; at: Vector3 }[]; border: LinesMesh; owner?: string };
const chunks = new Map<string, Chunk>();

function chunkColors(cell: Cell, ground: Mesh, owner: string | undefined): void {
	const positions = ground.getVerticesData(VertexBuffer.PositionKind)!;
	const colors = new Float32Array((positions.length / 3) * 4);
	const tint = owner === undefined ? undefined : Color3.FromHexString(hostColor(owner));
	const seed = grid.seed();
	for (let i = 0; i < positions.length / 3; i += 1) {
		const lx = positions[i * 3]! + CELL / 2;
		const ly = positions[i * 3 + 2]! + CELL / 2;
		const h = height(seed, cell, lx, ly);
		const b = biomeOf(h, moisture(seed, cell, lx, ly));
		let c = BIOME_COLORS[b]!.scale(0.72 + h * 0.55);
		if (lx % 8 === 0 || ly % 8 === 0) c = Color3.Lerp(c, Color3.FromHexString('#d9c9a3'), 0.28); // trails
		if (tint !== undefined) c = Color3.Lerp(c, tint, 0.1);
		colors.set([c.r, c.g, c.b, 1], i * 4);
	}
	ground.setVerticesData(VertexBuffer.ColorKind, colors);
}

function createChunk(cell: Cell): Chunk {
	const key = zoneId(cell);
	const ground = MeshBuilder.CreateGround(`ground-${key}`, { width: CELL, height: CELL, subdivisions: 16, updatable: true }, scene);
	const positions = ground.getVerticesData(VertexBuffer.PositionKind)!;
	for (let i = 0; i < positions.length / 3; i += 1) {
		positions[i * 3 + 1] = height(grid.seed(), cell, positions[i * 3]! + CELL / 2, positions[i * 3 + 2]! + CELL / 2) * RELIEF;
	}
	const normals: number[] = [];
	VertexData.ComputeNormals(positions, ground.getIndices()!, normals);
	ground.updateVerticesData(VertexBuffer.PositionKind, positions);
	ground.updateVerticesData(VertexBuffer.NormalKind, normals);
	ground.material = groundMat;
	const owner = owners.get(key);
	chunkColors(cell, ground, owner);

	const props: Chunk['props'] = [];
	for (const [i, p] of grid.props(cell).entries()) {
		for (const [j, template] of templates[p.kind]!.entries()) {
			const inst = template.createInstance(`prop-${key}-${i}-${j}`);
			inst.scaling.setAll(p.kind === PROP_TREE ? p.r / 0.55 : p.r);
			inst.rotation.y = (p.variant / 255) * Math.PI * 2;
			props.push({ mesh: inst, at: new Vector3(p.x - CELL / 2, terrain(cell, p.x, p.y) - 0.1, p.y - CELL / 2) });
		}
	}

	const edge: Vector3[] = [];
	const at = (x: number, y: number) => new Vector3(x - CELL / 2, terrain(cell, x, y) + 0.08, y - CELL / 2);
	for (let k = 0; k <= 16; k += 1) edge.push(at(k * 2, 0));
	for (let k = 1; k <= 16; k += 1) edge.push(at(CELL, k * 2));
	for (let k = 15; k >= 0; k -= 1) edge.push(at(k * 2, CELL));
	for (let k = 15; k >= 0; k -= 1) edge.push(at(0, k * 2));
	const border = MeshBuilder.CreateLines(`border-${key}`, { points: edge }, scene);
	border.parent = ground;
	border.color = Color3.FromHexString(hostColor(owner));
	return { cell, ground, props, border, owner };
}

function syncChunks(): void {
	const wanted = new Set<string>();
	for (let dy = -VIEW_CELLS; dy <= VIEW_CELLS; dy += 1) {
		for (let dx = -VIEW_CELLS; dx <= VIEW_CELLS; dx += 1) {
			const cell: Cell = [origin[0] + dx, origin[1] + dy];
			const key = zoneId(cell);
			wanted.add(key);
			if (!chunks.has(key)) chunks.set(key, createChunk(cell));
		}
	}
	for (const [key, chunk] of chunks) {
		if (!wanted.has(key)) {
			for (const prop of chunk.props) prop.mesh.dispose();
			chunk.ground.dispose(false, true);
			chunks.delete(key);
			continue;
		}
		chunk.ground.position.set((chunk.cell[0] - origin[0]) * CELL + CELL / 2, 0, (chunk.cell[1] - origin[1]) * CELL + CELL / 2);
		for (const prop of chunk.props) prop.mesh.position.copyFrom(chunk.ground.position.add(prop.at));
		const owner = owners.get(key);
		if (owner !== chunk.owner) {
			chunk.owner = owner;
			chunk.border.color = Color3.FromHexString(hostColor(owner));
			chunkColors(chunk.cell, chunk.ground, owner);
		}
	}
}

function moveOrigin(to: Cell): void {
	if (to[0] === origin[0] && to[1] === origin[1]) return;
	const shift = new Vector3((to[0] - origin[0]) * CELL, 0, (to[1] - origin[1]) * CELL);
	origin = to;
	camera.setTarget(camera.target.subtract(shift), false, false, true); // keep the orbit angles
	syncChunks();
}

// ---- entities ----
function hueColor(hue: number, s = 0.65, v = 0.95): Color3 {
	const c = new Color3();
	Color3.HSVtoRGBToRef((hue / 255) * 360, s, v, c);
	return c;
}
const mats = new Map<string, StandardMaterial>();
function hueMat(hue: number, kind: string): StandardMaterial {
	const key = `${kind}-${hue}`;
	let m = mats.get(key);
	if (m === undefined) {
		m = new StandardMaterial(key, scene);
		m.diffuseColor = hueColor(hue, kind === 'critter' ? 0.45 : 0.7);
		m.specularColor = new Color3(0.1, 0.1, 0.1);
		mats.set(key, m);
	}
	return m;
}
const meMat = material(scene, 'me', '#ffffff');
meMat.emissiveColor = Color3.FromHexString('#2a4c66');

type Visual = { mesh: AbstractMesh; kind: string };
const visuals = new Map<string, Visual>();
function visualFor(entity: RenderedEntity, mine: boolean): Visual {
	let visual = visuals.get(entity.key);
	const hue = typeof entity.fields.hue === 'number' ? entity.fields.hue : 0;
	if (visual !== undefined) return visual;
	let mesh: AbstractMesh;
	if (entity.kindName === KIND.critter) {
		const body = MeshBuilder.CreateSphere(entity.key, { diameter: 0.7, segments: 8 }, scene);
		body.scaling.set(1.2, 0.7, 0.8);
		body.material = hueMat(hue, 'critter');
		mesh = body;
	} else if (entity.kindName === KIND.marker) {
		const pole = MeshBuilder.CreateCylinder(entity.key, { diameter: 0.08, height: 2.2 }, scene);
		pole.bakeTransformIntoVertices(Matrix.Translation(0, 1.1, 0));
		const flag = MeshBuilder.CreateBox(`${entity.key}-flag`, { width: 0.8, height: 0.5, depth: 0.04 }, scene);
		flag.parent = pole;
		flag.position.set(0.42, 1.9, 0);
		flag.material = hueMat(hue, 'marker');
		mesh = pole;
	} else {
		const capsule = MeshBuilder.CreateCapsule(entity.key, { radius: PLAYER_RADIUS, height: 1.8 }, scene);
		capsule.material = mine ? meMat : hueMat(hue, 'walker');
		mesh = capsule;
	}
	visual = { mesh, kind: entity.kindName };
	visuals.set(entity.key, visual);
	return visual;
}

// ---- HUD, toasts, minimap ----
const hud = new Hud({
	title: 'Endless Grid',
	hint: 'WASD / stick to walk, Space/Shift to sprint, E to drop a marker. Trails (light lines) are always free.',
	client,
	devApi: api
});
const toasts = document.createElement('div');
toasts.className = 'grid-toasts';
document.body.append(toasts);
function toast(text: string, kind = ''): void {
	const el = document.createElement('div');
	el.className = `grid-toast ${kind}`.trim();
	el.textContent = text;
	toasts.append(el);
	setTimeout(() => el.classList.add('out'), 4200);
	setTimeout(() => el.remove(), 4800);
}
// Restores arrive in bursts (walking back wakes a cell and its neighbours):
// one toast per burst.
let restoredBurst: GridHello[] = [];
grid.onRestored((hello) => {
	restoredBurst.push(hello);
	if (restoredBurst.length > 1) return;
	setTimeout(() => {
		const burst = restoredBurst;
		restoredBurst = [];
		const here = burst.find((h) => h.zone === client.authority?.zoneId) ?? burst[0]!;
		const slept = here.hibernatedForMs === null ? '' : ` after ${(here.hibernatedForMs / 1000).toFixed(1)} s`;
		const others = burst.length > 1 ? ` (+${burst.length - 1} neighbour${burst.length > 2 ? 's' : ''})` : '';
		toast(
			`${here.zone} restored from hibernation${slept} on host ${here.host}: ${here.restoredCritters} critters, ${here.restoredMarkers} markers${others}`,
			'restored'
		);
	}, 400);
});
let lastHost: string | undefined;
client.on('handoffComplete', ({ to }) => {
	const host = grid.info(to)?.host;
	if (host !== undefined && lastHost !== undefined && host !== lastHost) toast(`Crossed into ${to} — now on host ${host}`, 'host');
	lastHost = host ?? lastHost;
});
client.on('handoffFailed', ({ to, reason }) => toast(`Handoff to ${to} failed: ${reason}`, 'error'));

const mapBox = document.createElement('div');
mapBox.className = 'grid-map';
const mapCanvas = document.createElement('canvas');
const MAP_R = 6;
const MAP_PX = 15;
mapCanvas.width = mapCanvas.height = (MAP_R * 2 + 1) * MAP_PX;
const legend = document.createElement('div');
legend.className = 'grid-legend';
mapBox.append(mapCanvas, legend);
document.body.append(mapBox);
let worldMap: WorldMap | undefined;

function drawMap(): void {
	const ctx = mapCanvas.getContext('2d');
	if (ctx === null || worldMap === undefined) return;
	const center = grid.cell() ?? origin;
	ctx.clearRect(0, 0, mapCanvas.width, mapCanvas.height);
	const observed = new Set(client.zones().filter((z) => z.role === 'observer').map((z) => z.zoneId));
	for (const cell of worldMap.cells as MapCell[]) {
		const px = (cell.x - center[0] + MAP_R) * MAP_PX;
		const py = (MAP_R - (cell.y - center[1])) * MAP_PX;
		if (px < 0 || py < 0 || px >= mapCanvas.width || py >= mapCanvas.height) continue;
		ctx.globalAlpha = cell.state === 'active' ? 0.9 : cell.state === 'hibernated' ? 0.4 : 0.1;
		ctx.fillStyle = hostColor(cell.owner);
		ctx.fillRect(px + 1, py + 1, MAP_PX - 2, MAP_PX - 2);
		ctx.globalAlpha = 1;
		if (cell.state === 'hibernated') {
			ctx.fillStyle = '#e8f1ff';
			ctx.font = '10px ui-monospace, monospace';
			ctx.fillText('z', px + 4, py + 11);
		}
		if (observed.has(cell.zone)) {
			ctx.strokeStyle = 'rgba(255,255,255,0.45)';
			ctx.strokeRect(px + 1.5, py + 1.5, MAP_PX - 3, MAP_PX - 3);
		}
		if (cell.x === center[0] && cell.y === center[1]) {
			ctx.strokeStyle = '#ffffff';
			ctx.lineWidth = 2;
			ctx.strokeRect(px + 1, py + 1, MAP_PX - 2, MAP_PX - 2);
			ctx.lineWidth = 1;
		}
	}
	legend.replaceChildren(
		...worldMap.hosts.map((h) => {
			const row = document.createElement('div');
			const swatch = document.createElement('span');
			swatch.className = 'swatch';
			swatch.style.background = hostColor(h.id);
			row.append(swatch, `host ${h.id}: ${h.active} active, ${h.hibernated} asleep${h.reachable ? '' : ' (unreachable)'}`);
			return row;
		}),
		Object.assign(document.createElement('small'), { textContent: 'bright = active · dim z = hibernated · faint = cold' })
	);
}

async function pollMap(): Promise<void> {
	try {
		worldMap = await grid.map(grid.cell() ?? origin, MAP_R);
		for (const cell of worldMap.cells) owners.set(cell.zone, cell.owner);
		drawMap();
	} catch {
		legend.textContent = 'world map unavailable (dev routes off?)';
	}
}
setInterval(() => void pollMap(), 1000);
void pollMap();

// ---- input ----
const sampler = new InputSampler();
const commandFrames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });
let lastInput: MoveInput = { mx: 0, my: 0, buttons: 0 };
function sendCommandFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	lastInput = {
		mx: toAxis(move.x),
		my: toAxis(move.y),
		buttons: (s.action ? BUTTON_SPRINT : 0) | (s.alt ? BUTTON_MARK : 0)
	};
	client.sendInput(lastInput);
}

// ---- frame ----
syncChunks();
stage.onFrame((now, dtMs) => {
	commandFrames.update(now, sendCommandFrame);
	const cell = grid.cell();
	if (cell !== undefined) moveOrigin(cell);
	const rendered = client.render();
	const seen = new Set<string>();
	const hello = cell === undefined ? undefined : grid.info(zoneId(cell));
	let me: Vector3 | undefined;
	let markers = 0;
	for (const entity of rendered) {
		const ecell = parseZone(entity.zoneId);
		if (ecell === undefined) continue;
		const mine = entity.predicted || (entity.authoritative && hello?.entity === entity.id);
		const visual = visualFor(entity, mine);
		const x = Number(entity.fields.x ?? 0);
		const y = Number(entity.fields.y ?? 0);
		seen.add(entity.key);
		if (visual.kind === KIND.critter) {
			visual.mesh.position.copyFrom(local(ecell, x, y, 0.3));
			visual.mesh.rotation.y = -Number(entity.fields.yaw ?? 0);
		} else if (visual.kind === KIND.marker) {
			visual.mesh.position.copyFrom(local(ecell, x, y));
			stage.labels.set(entity.key, visual.mesh, `⚑ ${String(entity.fields.label ?? '')}`, 2.6);
			if (entity.authoritative) markers += 1;
		} else {
			visual.mesh.position.copyFrom(local(ecell, x, y, 0.9));
			const label = String(entity.fields.name ?? '');
			stage.labels.set(entity.key, visual.mesh, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			if (mine) me = visual.mesh.position;
		}
		visual.mesh.setEnabled(!entity.stale);
	}
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		visual.mesh.dispose(false, true);
		stage.labels.delete(key);
		visuals.delete(key);
	}
	if (me !== undefined) stage.follow(me.x, me.z, dtMs);

	hud.frame({ entities: rendered.length });
	const observers = client.zones().filter((z) => z.role === 'observer');
	const predicted = client.predictor?.active === true ? client.predictor.predicted() : undefined;
	const px = Number(predicted?.x ?? 16);
	const py = Number(predicted?.y ?? 16);
	hud.row('You', api.last === undefined ? '—' : `${api.last.name} · ${api.last.principal}`);
	hud.row('Cell', cell === undefined ? '—' : `${cell[0]}, ${cell[1]} · local ${px.toFixed(1)}, ${py.toFixed(1)}`);
	hud.row('Host', hello?.host ?? '—');
	hud.row('Biome', cell === undefined ? '—' : BIOMES[biomeOf(height(grid.seed(), cell, px, py), moisture(grid.seed(), cell, px, py))]!);
	hud.row('Neighbours', `${observers.filter((z) => z.connection.state === 'open').length}/8 observed`);
	hud.row('Handoffs', String(grid.handoffs()));
	hud.row('Markers here', String(markers));
	hud.row('Seed', `0x${grid.seed().toString(16)}`);
});

declare global {
	interface Window {
		/** Debug handle (console). */
		grid?: unknown;
	}
}
window.grid = { grid, client, api, scene };

client.connect(startZone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
