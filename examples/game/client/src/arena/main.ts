/**
 * Arena demo page: Babylon scene over `createArenaClient`.
 *
 * - own capsule: predicted (inputs apply immediately), corrections eased;
 *   toggle prediction off (P) to see raw input latency (RTT + interpolation
 *   delay); the ghost (G) shows the latest authoritative server position;
 * - other players and drones: interpolated ~100 ms in the past;
 * - interest ring: drones beyond it are removed by the server (they shrink
 *   away over the last few metres before the edge); players and pillars are
 *   global, so everyone in the room stays visible (dimmed outside the ring);
 * - minimap (top right): everyone's position and the view ring.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Vector3 } from '@babylonjs/core/Maths/math.vector';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import type { Scene } from '@babylonjs/core/scene';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, describeGamepad, InputSampler, toAxis } from '../shared/input.ts';
import { Minimap, type MinimapDot } from '../shared/minimap.ts';
import { createRing, createStage, material, toWorld } from '../shared/scene.ts';
import { droneFields, isDrone, isPillar, pillarFields, playerFields, BUTTON, type MoveInput } from '../generated/arena/index.ts';
import { createArenaClient, DEFAULT_ZONE } from './client.ts';
import { DASH_COOLDOWN, HALF, PLAYER_RADIUS } from './movement.ts';

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
const arena = createArenaClient({ api });
const { client } = arena;

const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: HALF });
const { scene } = stage;

let ghostOn = true;
let corrections = 0;
let lastCorrection = 0;
client.on('correction', (c) => {
	corrections += 1;
	lastCorrection = Math.hypot(c.errors.x ?? 0, c.errors.y ?? 0);
});

const hud = new Hud({
	title: 'Arena',
	hint: 'WASD / stick to move, Space / A to dash. P: prediction, G: ghost, V: first person (click to look). Right stick / drag: camera.',
	client,
	devApi: api
});
hud.toggle('Client prediction', true, (on) => client.setPredictionEnabled(on), 'p');
hud.toggle('Server ghost', true, (on) => (ghostOn = on), 'g');
hud.toggle('First-person view', false, (on) => stage.setView(on ? 'first' : 'third'), 'v');

// ---- meshes ----
const mats = {
	me: material(scene, 'me', '#6cc4ff'),
	other: material(scene, 'other', '#f2a65a'),
	otherFar: material(scene, 'other-far', '#f2a65a', 0.45),
	ghost: material(scene, 'ghost', '#ffffff', 0.25),
	drone: material(scene, 'drone', '#9ad16b'),
	pillar: material(scene, 'pillar', '#7b8798')
};
mats.ghost.wireframe = true;

function capsule(id: string, mat: StandardMaterial): Mesh {
	const mesh = MeshBuilder.CreateCapsule(id, { radius: PLAYER_RADIUS, height: 1.8 }, scene);
	mesh.material = mat;
	return mesh;
}

const droneTemplate = (() => {
	const body = MeshBuilder.CreateSphere('drone-body', { diameter: 0.7, segments: 8 }, scene);
	body.scaling.y = 0.45;
	const nose = MeshBuilder.CreateBox('drone-nose', { width: 0.5, height: 0.12, depth: 0.12 }, scene);
	nose.position.x = 0.35;
	const merged = Mesh.MergeMeshes([body, nose], true)!;
	merged.material = mats.drone;
	merged.setEnabled(false);
	return merged;
})();

type VisualKind = 'player' | 'drone' | 'pillar';
type Visual = { mesh: Mesh | ReturnType<Mesh['createInstance']>; kind: VisualKind };
const visuals = new Map<string, Visual>();
const ghost = capsule('ghost', mats.ghost);
const ring = createRing(scene, 14, '#6cc4ff');
ring.setEnabled(false);

/** Drones shrink away over this many metres inside the view-radius edge. */
const FADE_BAND = 3;
const COLORS = { me: '#6cc4ff', other: '#f2a65a', drone: '#9ad16b', pillar: '#7b8798' };
const minimap = new Minimap({
	half: HALF,
	legend: [
		{ color: COLORS.me, label: 'you' },
		{ color: COLORS.other, label: 'players (global)' },
		{ color: COLORS.drone, label: 'drones (in view only)' }
	]
});

function visualFor(entity: RenderedEntity, mine: boolean): Visual {
	let visual = visuals.get(entity.key);
	if (visual !== undefined) return visual;
	if (isDrone(entity)) {
		visual = { mesh: droneTemplate.createInstance(entity.key), kind: 'drone' };
	} else if (isPillar(entity)) {
		const { r } = pillarFields(entity);
		const mesh = MeshBuilder.CreateCylinder(entity.key, { diameter: r * 2, height: 2, tessellation: 24 }, scene);
		mesh.material = mats.pillar;
		visual = { mesh, kind: 'pillar' };
	} else {
		visual = { mesh: capsule(entity.key, mine ? mats.me : mats.other), kind: 'player' };
	}
	visuals.set(entity.key, visual);
	return visual;
}

// ---- input: one command frame per 30 Hz tick ----
const sampler = new InputSampler();
const commandFrames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });
let lastInput: MoveInput = { mx: 0, my: 0, buttons: 0 };

function sendCommandFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	lastInput = { mx: toAxis(move.x), my: toAxis(move.y), buttons: s.action ? BUTTON.dash : 0 };
	client.sendInput(lastInput);
}

// ---- frame ----
stage.onFrame((now, dtMs) => {
	commandFrames.update(now, sendCommandFrame);
	const hello = arena.hello();
	const authority = client.authority;
	const rendered = client.render();
	const seen = new Set<string>();
	const isMine = (entity: RenderedEntity) =>
		authority !== undefined && hello !== undefined && entity.zoneId === authority.zoneId && entity.id === hello.entity;
	const mineEntity = rendered.find(isMine);
	let me: { x: number; y: number } | undefined = mineEntity === undefined ? undefined : playerFields(mineEntity);
	const viewRadius = hello?.viewRadius ?? 14;
	const dots: MinimapDot[] = [];
	for (const entity of rendered) {
		const mine = isMine(entity);
		const visual = visualFor(entity, mine);
		seen.add(entity.key);
		const distanceTo = (x: number, y: number) => (me === undefined ? 0 : Math.hypot(x - me.x, y - me.y));
		if (visual.kind === 'drone') {
			const { x, y, yaw } = droneFields(entity);
			// Shrink toward the edge so culling at the ring reads as "out of view".
			const s = Math.min(1, Math.max(0.2, (viewRadius - distanceTo(x, y)) / FADE_BAND));
			visual.mesh.position.copyFrom(toWorld(x, y, 1.3));
			visual.mesh.rotation.y = -yaw;
			visual.mesh.scaling.setAll(s);
			dots.push({ x, y, color: COLORS.drone, size: 2 });
		} else if (visual.kind === 'pillar') {
			const { x, y, r } = pillarFields(entity);
			visual.mesh.position.copyFrom(toWorld(x, y, 1));
			dots.push({ x, y, color: COLORS.pillar, size: Math.max(2, r * 2), hollow: true });
		} else {
			// Prediction off (P): the SDK renders the own capsule like everyone
			// else's, interpolated in the past.
			const { x, y, dash, name: label } = playerFields(entity);
			const distance = distanceTo(x, y);
			visual.mesh.position.copyFrom(toWorld(x, y, 0.9));
			const dashing = dash > 0;
			visual.mesh.scaling.copyFromFloats(dashing ? 1.25 : 1, 1, dashing ? 1.25 : 1);
			const outside = !mine && distance > viewRadius;
			if (!mine) (visual.mesh as Mesh).material = outside ? mats.otherFar : mats.other;
			const text = mine ? `${label} (you)` : outside ? `${label} · outside view` : label;
			// First person: the camera is the own capsule, so hide it and its label.
			if (mine && stage.view() === 'first') stage.labels.delete(entity.key);
			else stage.labels.set(entity.key, visual.mesh as Mesh, text, 1.4, mine ? 'me' : '');
			if (mine) me = { x, y };
			dots.push({ x, y, color: mine ? COLORS.me : COLORS.other, size: mine ? 4 : 3.5 });
		}
		visual.mesh.setEnabled((!entity.stale || visual.kind === 'pillar') && !(mine && stage.view() === 'first'));
	}
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		visual.mesh.dispose();
		stage.labels.delete(key);
		visuals.delete(key);
	}

	const authoritative = client.predictor?.authoritative();
	let ghostVisible = ghostOn && authoritative !== undefined;
	if (authoritative !== undefined) {
		const { x: gx, y: gy } = playerFields({ fields: authoritative });
		ghost.position.copyFrom(toWorld(gx, gy, 0.9));
		// First person: the eye sits inside the ghost; show it only once it lags visibly.
		if (stage.view() === 'first' && me !== undefined && Math.hypot(gx - me.x, gy - me.y) < 0.8) ghostVisible = false;
	}
	ghost.setEnabled(ghostVisible);
	if (me !== undefined) {
		stage.follow(me.x, me.y, dtMs);
		ring.setEnabled(true);
		ring.position = new Vector3(me.x, 0, me.y);
		if (hello !== undefined && ring.metadata !== hello.viewRadius) {
			ring.scaling.setAll(hello.viewRadius / 14);
			ring.metadata = hello.viewRadius;
		}
	}

	minimap.draw({ me, viewRadius, dots });
	hud.frame({ entities: rendered.length });
	const predictor = client.predictor;
	const cd = authoritative === undefined ? 0 : playerFields({ fields: authoritative }).cd;
	hud.row('You', api.last === undefined ? '—' : `${api.last.name} · ${api.last.principal}`);
	hud.row('Pending inputs', String(predictor?.pending.length ?? 0));
	hud.row('Ack seq', String(predictor?.lastAck ?? 0));
	hud.row('Corrections', `${corrections} (last ${lastCorrection.toFixed(3)} m)`);
	hud.row('Dash', cd > 0 ? `cooldown ${((cd / DASH_COOLDOWN) * 1.5).toFixed(1)} s` : 'ready');
	hud.row('Stick', `${lastInput.mx}, ${lastInput.my}`);
	hud.row('Gamepad', describeGamepad());
});

declare global {
	interface Window {
		/** Debug handle (console). */
		arena?: unknown;
	}
}
window.arena = { client, api, scene: scene as Scene };

client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
