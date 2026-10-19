/**
 * Mob Pit page: Babylon scene over `createMobPitClient`.
 *
 * What to look at: kill a mob (Space) and watch the grey "saving…" popup
 * turn into "+XP saved ✓" when the durable cell commit settles. Toggle
 * *Slow storage* (L): every commit now takes 1.5 s — the pending-facts graph
 * climbs while the tick graph stays flat at ~33 ms, because the zone never
 * waits for storage. Kill the server (`kill -9`), restart it, reload: your
 * XP and kills come back from the cell.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Color3 } from '@babylonjs/core/Maths/math.color';
import { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { HALF, PLAYER_RADIUS } from '../arena/floor.ts';
import {
	BUTTON,
	isMob,
	isPillar,
	isPit,
	mobFields,
	pillarFields,
	pitFields,
	playerFields,
	type BoardSnapshot,
	type PitFields,
	type PitInput
} from '../generated/mobpit/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createStage, material, toWorld } from '../shared/scene.ts';
import { createMobPitClient, DEFAULT_ZONE, MobPitApi, TIERS } from './client.ts';

const SLOW_MS = 1500;
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
	hint: 'WASD move · Space attack · E dash · L slow storage. Kills are durable facts; the tick never waits for them.',
	client,
	devApi: api
});
let slow = false;
hud.toggle(`Slow storage (${SLOW_MS} ms per commit)`, false, (on) => {
	slow = on;
	pitApi.setStorage(on ? SLOW_MS : 0).catch((e: unknown) => hud.row('Storage', `failed: ${String(e)}`));
}, 'l');
pitApi
	.storage()
	.then((s) => (slow = s.latencyMs > 0))
	.catch(() => undefined);

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

// ---- materials and templates ----
const mats = {
	me: material(scene, 'me', '#6cc4ff'),
	other: material(scene, 'other', '#f2a65a'),
	dead: material(scene, 'dead', '#555555', 0.4),
	pillar: material(scene, 'pillar', '#5d5566'),
	blade: material(scene, 'blade', '#e8eef7'),
	swing: material(scene, 'swing', '#ffffff', 0.18),
	hpBack: material(scene, 'hp-back', '#1b1b1b'),
	hpFill: material(scene, 'hp-fill', '#e04848'),
	tele: material(scene, 'tele', '#ff3b3b', 0.25),
	teleFill: material(scene, 'tele-fill', '#ff3b3b', 0.45)
};
for (const m of [mats.swing, mats.tele, mats.teleFill, mats.hpFill]) m.emissiveColor = m.diffuseColor.clone();
mats.blade.emissiveColor = new Color3(0.6, 0.6, 0.7);
const tierMats = new Map<number, StandardMaterial>();
for (const [tier, t] of Object.entries(TIERS)) tierMats.set(Number(tier), material(scene, `mob-${tier}`, t.color));

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
	const arc = flatDisc(`${entity.key}-arc`, 2.2 + PLAYER_RADIUS, mats.swing, 1 / 3);
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
pit.on('board', (board) => renderBoard(board));

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
			// Blade sweeps through the 120° cone while `swing` counts down (6 → 0).
			const sweep = swing > 0 ? ((6 - swing) / 6) * (Math.PI * 2 / 3) - Math.PI / 3 : -Math.PI / 2.5;
			const a = face + sweep;
			visual.blade.position.copyFrom(toWorld(x + Math.cos(a) * 0.8, y + Math.sin(a) * 0.8, 1.0));
			visual.blade.rotation.y = -a;
			visual.blade.setEnabled(visible && !dead);
			visual.arc.setEnabled(visible && swing > 0);
			if (swing > 0) {
				visual.arc.position.copyFrom(toWorld(x, y, 0.05));
				// Disc arcs start at +x and run counter-clockwise; centre it on `face`.
				visual.arc.rotation.y = -(face - Math.PI / 3);
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
	const slowMs = stats?.slowMs ?? 0;
	hud.row('You', api.last === undefined ? '—' : `${api.last.name} · ${api.last.principal}`);
	hud.row('HP', me === undefined ? '—' : me.dead ? 'dead' : String(me.hp));
	hud.row('Facts pending (zone)', String(pending));
	hud.row('Facts saved (zone)', String(stats?.committed ?? 0));
	hud.row('My unsaved kills', String(pit.unsaved.size));
	hud.row('Storage latency', slowMs > 0 ? `${slowMs} ms (slow)` : 'normal');
	hud.row('Server tick interval', `${tickMs.toFixed(1)} ms avg · ${tickMax.toFixed(1)} max`);
	hud.row('Tick work', `${stats?.workUs ?? 0} µs`);
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
	field('note').textContent = slow || slowMs > 0
		? `Storage is slow: each commit waits ${slowMs} ms. Pending climbs, the tick does not.`
		: 'Storage is fast: facts settle within a tick or two.';
	if (now - lastSampleAt > 250) {
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
window.mobpit = { client, api, pitApi, pit, scene };

client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
