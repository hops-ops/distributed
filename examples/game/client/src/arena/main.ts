/**
 * Arena demo page: Babylon scene over `createArenaClient`.
 *
 * - own capsule: predicted (inputs apply immediately), corrections eased;
 *   toggle prediction off (P) to see raw input latency (RTT + interpolation
 *   delay); the ghost (G) shows the latest authoritative server position;
 * - other players and drones: interpolated ~100 ms in the past;
 * - interest ring: entities beyond it are removed by the server (drones pop
 *   in/out at the edge); pillars are global entities.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Vector3 } from '@babylonjs/core/Maths/math.vector';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import type { Scene } from '@babylonjs/core/scene';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createRing, createStage, material, toWorld } from '../shared/scene.ts';
import { createArenaClient, DEFAULT_ZONE, KIND } from './client.ts';
import { BUTTON_DASH, DASH_COOLDOWN, HALF, PLAYER_RADIUS, type MoveInput } from './movement.ts';

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
	hint: 'WASD / stick to move, Space / A to dash. P: prediction, G: ghost. Drag to orbit.',
	client,
	devApi: api
});
hud.toggle('Client prediction', true, (on) => client.setPredictionEnabled(on), 'p');
hud.toggle('Server ghost', true, (on) => (ghostOn = on), 'g');

// ---- meshes ----
const mats = {
	me: material(scene, 'me', '#6cc4ff'),
	other: material(scene, 'other', '#f2a65a'),
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

type Visual = { mesh: Mesh | ReturnType<Mesh['createInstance']>; kind: string };
const visuals = new Map<string, Visual>();
const ghost = capsule('ghost', mats.ghost);
const ring = createRing(scene, 14, '#6cc4ff');
ring.setEnabled(false);

function visualFor(entity: RenderedEntity, mine: boolean): Visual {
	let visual = visuals.get(entity.key);
	if (visual !== undefined) return visual;
	switch (entity.kindName) {
		case KIND.drone:
			visual = { mesh: droneTemplate.createInstance(entity.key), kind: KIND.drone };
			break;
		case KIND.pillar: {
			const r = num(entity, 'r');
			const mesh = MeshBuilder.CreateCylinder(entity.key, { diameter: r * 2, height: 2, tessellation: 24 }, scene);
			mesh.material = mats.pillar;
			visual = { mesh, kind: KIND.pillar };
			break;
		}
		default:
			visual = { mesh: capsule(entity.key, mine ? mats.me : mats.other), kind: KIND.player };
	}
	visuals.set(entity.key, visual);
	return visual;
}

function num(entity: RenderedEntity | { fields: RenderedEntity['fields'] }, field: string): number {
	const v = entity.fields[field];
	return typeof v === 'number' ? v : 0;
}

// ---- input: one command frame per 30 Hz tick ----
const sampler = new InputSampler();
const commandFrames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });
let lastInput: MoveInput = { mx: 0, my: 0, buttons: 0 };

function sendCommandFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	lastInput = { mx: toAxis(move.x), my: toAxis(move.y), buttons: s.action ? BUTTON_DASH : 0 };
	client.sendInput(lastInput);
}

// ---- frame ----
stage.onFrame((now, dtMs) => {
	commandFrames.update(now, sendCommandFrame);
	const hello = arena.hello();
	const authority = client.authority;
	const rendered = client.render();
	const seen = new Set<string>();
	let me: { x: number; y: number } | undefined;
	for (const entity of rendered) {
		const mine = authority !== undefined && hello !== undefined && entity.zoneId === authority.zoneId && entity.id === hello.entity;
		// Prediction off (P): the SDK renders the own capsule like everyone
		// else's, interpolated in the past.
		const fields = entity.fields;
		const visual = visualFor(entity, mine);
		const x = num({ fields }, 'x');
		const y = num({ fields }, 'y');
		seen.add(entity.key);
		if (visual.kind === KIND.drone) {
			visual.mesh.position.copyFrom(toWorld(x, y, 1.3));
			visual.mesh.rotation.y = -num(entity, 'yaw');
		} else if (visual.kind === KIND.pillar) {
			visual.mesh.position.copyFrom(toWorld(x, y, 1));
		} else {
			visual.mesh.position.copyFrom(toWorld(x, y, 0.9));
			const dashing = num({ fields }, 'dash') > 0;
			visual.mesh.scaling.copyFromFloats(dashing ? 1.25 : 1, 1, dashing ? 1.25 : 1);
			const label = String(fields.name ?? '');
			stage.labels.set(entity.key, visual.mesh as Mesh, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
			if (mine) me = { x, y };
		}
		visual.mesh.setEnabled(!entity.stale || visual.kind === KIND.pillar);
	}
	for (const [key, visual] of visuals) {
		if (seen.has(key)) continue;
		visual.mesh.dispose();
		stage.labels.delete(key);
		visuals.delete(key);
	}

	const authoritative = client.predictor?.authoritative();
	ghost.setEnabled(ghostOn && authoritative !== undefined);
	if (authoritative !== undefined) ghost.position.copyFrom(toWorld(num({ fields: authoritative }, 'x'), num({ fields: authoritative }, 'y'), 0.9));
	if (me !== undefined) {
		stage.follow(me.x, me.y, dtMs);
		ring.setEnabled(true);
		ring.position = new Vector3(me.x, 0, me.y);
		if (hello !== undefined && ring.metadata !== hello.viewRadius) {
			ring.scaling.setAll(hello.viewRadius / 14);
			ring.metadata = hello.viewRadius;
		}
	}

	hud.frame({ entities: rendered.length });
	const predictor = client.predictor;
	const cd = authoritative === undefined ? 0 : num({ fields: authoritative }, 'cd');
	hud.row('You', api.last === undefined ? '—' : `${api.last.name} · ${api.last.principal}`);
	hud.row('Pending inputs', String(predictor?.pending.length ?? 0));
	hud.row('Ack seq', String(predictor?.lastAck ?? 0));
	hud.row('Corrections', `${corrections} (last ${lastCorrection.toFixed(3)} m)`);
	hud.row('Dash', cd > 0 ? `cooldown ${((cd / DASH_COOLDOWN) * 1.5).toFixed(1)} s` : 'ready');
	hud.row('Stick', `${lastInput.mx}, ${lastInput.my}`);
});

declare global {
	interface Window {
		/** Debug handle (console). */
		arena?: unknown;
	}
}
window.arena = { client, api, scene: scene as Scene };

client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
