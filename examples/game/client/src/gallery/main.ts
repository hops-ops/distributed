/**
 * Shooting Gallery page: first-person view from the player's station over
 * `createGalleryClient`.
 *
 * - Click (or Space / gamepad A through the crosshair) fires a hitscan ray.
 *   A yellow marker shows the client-predicted hit immediately; when the
 *   server's verdict arrives it turns green (confirmed) or red (rejected).
 * - C toggles lag compensation (sent per shot). Raise the NetSim latency to
 *   150–250 ms: with compensation the confirmed hits keep up with the
 *   predicted ones, without it most "hits" are rejected.
 * - G shows wireframe ghosts at the newest received target state (what the
 *   server had a one-way latency ago), next to the interpolated targets you
 *   aim at.
 */
import { Color3 } from '@babylonjs/core/Maths/math.color';
import { Matrix, Vector3 } from '@babylonjs/core/Maths/math.vector';
import type { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import type { Scene } from '@babylonjs/core/scene';

import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { InputSampler } from '../shared/input.ts';
import { createStage, material, toWorld } from '../shared/scene.ts';
import { createGalleryClient, DEFAULT_ZONE, KIND, targetPosition, type ShotRecord, type Vec3 } from './client.ts';
import '@babylonjs/core/Culling/ray'; // side effect: scene.createPickingRay
import './gallery.css';

const params = new URLSearchParams(location.search);
const zone = params.get('zone') ?? DEFAULT_ZONE;
const name = params.get('name') ?? undefined;

const api = new DevApi({ name: () => name });
const gallery = createGalleryClient({ api });
const { client } = gallery;

const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: 20, background: '#0b0f14' });
const { scene, camera } = stage;

// First person: an ArcRotateCamera orbiting a point 5 cm away is a look-around camera.
camera.lowerRadiusLimit = 0.05;
camera.upperRadiusLimit = 0.05;
camera.radius = 0.05;
camera.lowerBetaLimit = 1.2;
camera.upperBetaLimit = 1.85;
camera.beta = Math.PI / 2 - 0.05;
camera.alpha = -Math.PI / 2;
camera.angularSensibilityX = 2500;
camera.angularSensibilityY = 2500;
camera.fov = 0.9;
camera.minZ = 0.02;

let compensation = true;
let ghostsOn = true;

const hud = new Hud({
	title: 'Shooting Gallery',
	hint: 'Click a target (or Space/A through the crosshair). Drag to look. C: lag compensation, G: server ghosts.',
	client,
	devApi: api
});
hud.toggle('Lag compensation', true, (on) => (compensation = on), 'c');
hud.toggle('Ghosts at newest server state', true, (on) => (ghostsOn = on), 'g');

const crosshair = document.createElement('div');
crosshair.className = 'crosshair';
document.body.append(crosshair);
const feed = document.createElement('ol');
feed.className = 'shot-feed';
document.body.append(feed);

// ---- meshes ----
const mats = {
	target: material(scene, 'target', '#ff8a3d'),
	flash: material(scene, 'flash', '#ffffff'),
	ghost: material(scene, 'ghost', '#9ad1ff', 0.35),
	station: material(scene, 'station', '#3a4556'),
	me: material(scene, 'me-station', '#6cc4ff'),
	rail: material(scene, 'rail', '#2b3442'),
	predicted: material(scene, 'predicted', '#ffd84a'),
	confirmed: material(scene, 'confirmed', '#59e08c'),
	rejected: material(scene, 'rejected', '#ff5d5d')
};
mats.ghost.wireframe = true;
for (const m of [mats.predicted, mats.confirmed, mats.rejected, mats.flash]) m.emissiveColor = m.diffuseColor.clone();

// Rails / circles (decor matching the server course) and the firing line.
for (const [y, half] of [
	[2, 12],
	[6, 14],
	[16, 16],
	[-2, 10]
] as const) {
	const rail = MeshBuilder.CreateBox('rail', { width: half * 2, height: 0.05, depth: 0.1 }, scene);
	rail.position = toWorld(0, y, 0.03);
	rail.material = mats.rail;
}
for (const [x, y, r] of [
	[-7, 11, 4],
	[7, 11, 3]
] as const) {
	const ring = MeshBuilder.CreateTorus('loop', { diameter: r * 2, thickness: 0.08, tessellation: 48 }, scene);
	ring.position = toWorld(x, y, 0.04);
	ring.material = mats.rail;
}
const line = MeshBuilder.CreateBox('firing-line', { width: 18, height: 0.04, depth: 0.15 }, scene);
line.position = toWorld(0, -13.2, 0.02);
line.material = mats.station;

type TargetVisual = { mesh: Mesh; ghost: Mesh; flash: number; flashUntil: number };
const targets = new Map<string, TargetVisual>();
const shooters = new Map<string, Mesh>();

function targetVisual(key: string, r: number): TargetVisual {
	let v = targets.get(key);
	if (v === undefined) {
		const mesh = MeshBuilder.CreateSphere(key, { diameter: r * 2, segments: 16 }, scene);
		mesh.material = mats.target;
		const ghost = MeshBuilder.CreateSphere(`${key}-ghost`, { diameter: r * 2, segments: 8 }, scene);
		ghost.material = mats.ghost;
		v = { mesh, ghost, flash: -1, flashUntil: 0 };
		targets.set(key, v);
	}
	return v;
}

// ---- shot markers ----
type Marker = { mesh: Mesh; until: number };
const markers: Marker[] = [];

function marker(at: Vec3, mat: StandardMaterial, size: number, ttl: number): Mesh {
	const mesh = MeshBuilder.CreateSphere('marker', { diameter: size, segments: 6 }, scene);
	mesh.position = toWorld(at[0], at[1], at[2]);
	mesh.material = mat;
	markers.push({ mesh, until: performance.now() + ttl });
	return mesh;
}

const pending = new Map<number, Mesh>();

function fire(dir: Vec3): void {
	const shot = gallery.shoot(dir, { compensated: compensation });
	if (shot === undefined) return;
	if (shot.point !== undefined) pending.set(shot.id, marker(shot.point, mats.predicted, 0.35, 4000));
	crosshair.classList.remove('kick');
	void crosshair.offsetWidth;
	crosshair.classList.add('kick');
}

gallery.onVerdict((shot: ShotRecord) => {
	const v = shot.verdict!;
	const predictedMarker = pending.get(shot.id);
	pending.delete(shot.id);
	const confirmed = v.hit !== null;
	if (predictedMarker !== undefined) predictedMarker.material = confirmed && v.hit === shot.predicted ? mats.confirmed : mats.rejected;
	else if (confirmed) {
		const target = gallery.targets().find((t) => t.id === v.hit);
		if (target !== undefined) marker(targetPosition(target), mats.confirmed, 0.3, 1500);
	}
	const li = document.createElement('li');
	const verdict = confirmed ? (shot.predicted === v.hit ? 'CONFIRMED' : `HIT #${v.hit} (unexpected)`) : shot.predicted === undefined ? 'miss' : 'REJECTED';
	li.className = confirmed ? 'ok' : shot.predicted === undefined ? '' : 'bad';
	li.textContent = `#${shot.id} ${verdict} · ${v.compensated ? `rewind ${v.ageMs.toFixed(0)} ms${v.clamped ? ` (clamped from ${v.requestedAgeMs.toFixed(0)})` : ''}` : 'no compensation'}`;
	feed.prepend(li);
	while (feed.children.length > 8) feed.lastChild?.remove();
});

// ---- input ----
const sampler = new InputSampler();
let down: { x: number; y: number } | undefined;
canvas.addEventListener('pointerdown', (e) => (down = { x: e.clientX, y: e.clientY }));
canvas.addEventListener('pointerup', (e) => {
	if (down === undefined) return;
	const moved = Math.hypot(e.clientX - down.x, e.clientY - down.y);
	down = undefined;
	if (moved > 6) return; // a drag looks around
	const rect = canvas.getBoundingClientRect();
	const scale = stage.engine.getHardwareScalingLevel();
	const ray = scene.createPickingRay((e.clientX - rect.left) / scale, (e.clientY - rect.top) / scale, Matrix.Identity(), camera);
	fire([ray.direction.x, ray.direction.z, ray.direction.y]);
});
let actionWasDown = false;

// ---- frame ----
stage.onFrame((now) => {
	const hello = gallery.hello();
	if (hello !== undefined) {
		const eye = toWorld(hello.eye[0], hello.eye[1], hello.eye[2]);
		if (!camera.target.equals(eye)) camera.setTarget(eye, false, false, true); // keep alpha/beta/radius
	}
	const s = sampler.sample();
	if (s.action && !actionWasDown) {
		const forward = camera.getForwardRay(1).direction;
		fire([forward.x, forward.z, forward.y]);
	}
	actionWasDown = s.action;

	const rendered = client.render();
	const authority = client.authority;
	const seen = new Set<string>();
	for (const entity of rendered) {
		seen.add(entity.key);
		if (entity.kindName === KIND.target) {
			const v = targetVisual(entity.key, Number(entity.fields.r ?? 0.5));
			const p = targetPosition(entity);
			v.mesh.position.copyFrom(toWorld(p[0], p[1], p[2]));
			const flash = Number(entity.fields.flash ?? 0);
			if (v.flash >= 0 && flash !== v.flash) v.flashUntil = now + 150;
			v.flash = flash;
			v.mesh.material = now < v.flashUntil ? mats.flash : mats.target;
			const newest = authority?.replica.get(entity.id);
			v.ghost.setEnabled(ghostsOn && newest !== undefined);
			if (newest !== undefined) {
				const f = newest.fields;
				v.ghost.position.copyFrom(toWorld(Number(f.x), Number(f.y), Number(f.z)));
			}
		} else if (entity.kindName === KIND.shooter) {
			let mesh = shooters.get(entity.key);
			const mine = entity.id === hello?.entity;
			if (mesh === undefined) {
				mesh = MeshBuilder.CreateCylinder(entity.key, { diameter: 0.9, height: 0.15, tessellation: 24 }, scene);
				mesh.material = mine ? mats.me : mats.station;
				shooters.set(entity.key, mesh);
			}
			mesh.position.copyFrom(toWorld(Number(entity.fields.x), Number(entity.fields.y), 0.08));
			if (!mine) stage.labels.set(entity.key, mesh, `${String(entity.fields.name)} · ${entity.fields.hits}/${entity.fields.shots}`, 1.9);
		}
	}
	for (const [key, v] of targets) {
		if (seen.has(key)) continue;
		v.mesh.dispose();
		v.ghost.dispose();
		targets.delete(key);
	}
	for (const [key, mesh] of shooters) {
		if (seen.has(key)) continue;
		mesh.dispose();
		stage.labels.delete(key);
		shooters.delete(key);
	}
	for (let i = markers.length - 1; i >= 0; i -= 1) {
		if (now > markers[i].until) {
			markers[i].mesh.dispose();
			markers.splice(i, 1);
		}
	}

	hud.frame({ entities: rendered.length });
	const st = gallery.stats;
	const pct = (n: number, d: number) => (d === 0 ? '—' : `${Math.round((100 * n) / d)}%`);
	hud.row('Interp delay', authority === undefined ? '—' : `${gallery.delay(authority).value.toFixed(0)} ms (target ${gallery.delay(authority).target(authority).toFixed(0)})`);
	hud.row('Compensation', compensation ? `ON (max ${hello?.maxRewindMs ?? '—'} ms)` : 'OFF');
	hud.row('Shots', String(st.shots));
	hud.row('Predicted hits', `${st.predictedHits} (${pct(st.predictedHits, st.shots)})`);
	hud.row('Confirmed hits', `${st.confirmedHits} (${pct(st.confirmedHits, st.resolved)})`);
	hud.row('Client = server', `${st.agreed}/${st.resolved} (${pct(st.agreed, st.resolved)})`);
	hud.row('Rewind age', st.lastAgeMs === 0 ? '—' : `last ${st.lastAgeMs.toFixed(0)} · mean ${st.meanAgeMs.toFixed(0)} ms`);
	hud.row('Clamped shots', String(st.clamped));
});

declare global {
	interface Window {
		/** Debug handle (console). */
		gallery?: unknown;
	}
}
window.gallery = { gallery, client, api, scene: scene as Scene, Vector3, Color3 };

client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
