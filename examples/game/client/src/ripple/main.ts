/**
 * Boss Ripple page: a playable neighborhood (Babylon) next to the live
 * hierarchy (DOM + SVG), both over `createRippleClient`.
 *
 * - fight the boss in your hood (E near it, or "Smite"); its defeat travels
 *   up as tier-up events — watch the pulse climb hood → borough → city →
 *   realm in the tree (each hop is a cross-host admin call with two hosts);
 * - the borough unlocks when all its bosses fell, the city's weather
 *   follows the remaining bosses (it is broadcast to everyone observing
 *   the city — including you, so the sky over your hood changes), and the
 *   realm shows a banner when the city is liberated;
 * - each tier shows its own tick rate and clock (realm 2 Hz … hood 30 Hz).
 */
import { Color3, Color4 } from '@babylonjs/core/Maths/math.color';
import type { AbstractMesh } from '@babylonjs/core/Meshes/abstractMesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { Scene } from '@babylonjs/core/scene';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { ARENA_FLOOR } from '../generated/arena/constants.ts';
import { BITS, isPlayer, playerFields } from '../generated/ripple/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createStage, material, toWorld } from '../shared/scene.ts';
import {
	ACTION_ATTACK,
	ACTION_SMITE,
	allZones,
	createRippleClient,
	DEFAULT_ZONE,
	tierOf,
	type HoodInput,
	type RippleEvent
} from './client.ts';
import './ripple.css';

/** The Arena floor, generated from the simulation's constants (`game_sim::arena::arena_floor`). */
const { half: HALF, playerRadius: PLAYER_RADIUS } = ARENA_FLOOR;

const params = new URLSearchParams(location.search);
let hood = params.get('hood') ?? DEFAULT_ZONE;
if (tierOf(hood) !== 'hood') hood = DEFAULT_ZONE;
const name = params.get('name') ?? undefined;

const api = new DevApi({ name: () => name });
const ripple = createRippleClient({ api });
const { client } = ripple;

// ---- hood scene ----
const canvas = document.getElementById('stage') as HTMLCanvasElement;
const stage = createStage(canvas, { half: HALF });
const { scene } = stage;
scene.fogMode = Scene.FOGMODE_EXP2;
const sky = {
	storm: { clear: '#141821', fog: 0.035, light: 0.35 },
	rain: { clear: '#2a3442', fog: 0.022, light: 0.55 },
	clearing: { clear: '#56708c', fog: 0.012, light: 0.8 },
	clear: { clear: '#8fc3f0', fog: 0.004, light: 1.1 }
} as const;
let weather: keyof typeof sky = 'storm';
const lights = scene.lights.map((l) => ({ light: l, base: l.intensity }));
function applySky(dtMs: number): void {
	const target = sky[weather];
	const want = Color4.FromHexString(`${target.clear}ff`);
	const k = 1 - Math.exp(-dtMs / 400);
	scene.clearColor = Color4.Lerp(scene.clearColor, want, k);
	scene.fogColor = new Color3(scene.clearColor.r, scene.clearColor.g, scene.clearColor.b);
	scene.fogDensity += (target.fog - scene.fogDensity) * k;
	const flash = weather === 'storm' && Math.random() < 0.004 ? 2.5 : 1;
	for (const { light, base } of lights) light.intensity += (base * target.light * flash - light.intensity) * Math.min(1, k * (flash > 1 ? 20 : 1));
}

const mats = {
	me: material(scene, 'me', '#6cc4ff'),
	other: material(scene, 'other', '#f2a65a'),
	pillar: material(scene, 'pillar', '#5d6675'),
	boss: material(scene, 'boss', '#d0453b'),
	down: material(scene, 'down', '#4a4a4a')
};
mats.boss.emissiveColor = Color3.FromHexString('#3a0b08');
for (const [i, { x, y, r }] of ARENA_FLOOR.pillars.entries()) {
	if (i === 0) continue; // the boss stands on the central pillar
	const ruin = MeshBuilder.CreateCylinder(`ruin-${i}`, { diameter: r * 2, height: 1.2 + (i % 3) * 0.8, tessellation: 7 }, scene);
	ruin.position = toWorld(x, y, ruin.getBoundingInfo().boundingBox.extendSize.y);
	ruin.material = mats.pillar;
}
const boss = MeshBuilder.CreateSphere('boss', { diameter: ARENA_FLOOR.pillars[0]!.r * 2, segments: 12 }, scene);
boss.scaling.y = 1.3;
boss.material = mats.boss;
boss.position = toWorld(0, 0, 2.2);

const visuals = new Map<string, AbstractMesh>();
function capsule(entity: RenderedEntity, mine: boolean): AbstractMesh {
	let mesh = visuals.get(entity.key);
	if (mesh === undefined) {
		mesh = MeshBuilder.CreateCapsule(entity.key, { radius: PLAYER_RADIUS, height: 1.8 }, scene);
		mesh.material = mine ? mats.me : mats.other;
		visuals.set(entity.key, mesh);
	}
	return mesh;
}

// ---- HUD, banner ----
const hud = new Hud({
	title: 'Boss Ripple',
	hint: 'WASD to move, Space to dash, E near the boss to attack. Click a neighborhood in the tree to fight there.',
	client,
	devApi: api
});
let smitePending = false;
const smite = document.createElement('button');
smite.className = 'ripple-button';
smite.textContent = 'Smite the boss (demo)';
smite.addEventListener('click', () => (smitePending = true));
hud.root.append(smite);

const banner = document.createElement('div');
banner.className = 'ripple-banner';
document.body.append(banner);
function showBanner(text: string): void {
	banner.textContent = text;
	banner.classList.remove('show');
	void banner.offsetWidth;
	banner.classList.add('show');
}
ripple.onBanner(showBanner);
ripple.onWeather((w) => {
	if (w in sky) weather = w as keyof typeof sky;
});

// ---- hierarchy tree ----
const tree = document.createElement('aside');
tree.className = 'ripple-tree';
const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
svg.classList.add('ripple-edges');
tree.append(svg);
const nodes = new Map<string, { el: HTMLElement; state: HTMLElement; bar?: HTMLElement }>();
for (const zone of allZones()) {
	const tier = tierOf(zone)!;
	const el = document.createElement('div');
	el.className = `ripple-node tier-${tier}`;
	const title = document.createElement('div');
	title.className = 'title';
	title.textContent = zone.split('.').at(-1)!;
	const meta = document.createElement('span');
	meta.className = 'meta';
	title.append(meta);
	const state = document.createElement('div');
	state.className = 'state';
	el.append(title, state);
	let bar: HTMLElement | undefined;
	if (tier === 'hood') {
		const track = document.createElement('div');
		track.className = 'hp';
		bar = document.createElement('div');
		track.append(bar);
		el.append(track);
		el.title = 'click to fight here';
		el.addEventListener('click', () => {
			if (zone === hood) return;
			hood = zone;
			history.replaceState(null, '', `?hood=${encodeURIComponent(zone)}${name === undefined ? '' : `&name=${encodeURIComponent(name)}`}`);
			ripple.enter(zone).catch(() => undefined);
		});
	}
	tree.append(el);
	nodes.set(zone, { el, state, bar });
	meta.dataset.role = 'meta';
}
const log = document.createElement('ol');
log.className = 'ripple-log';
tree.append(log);
document.body.append(tree);

function center(zone: string): { x: number; y: number } | undefined {
	const node = nodes.get(zone)?.el;
	if (node === undefined) return undefined;
	const box = tree.getBoundingClientRect();
	const r = node.getBoundingClientRect();
	return { x: r.left - box.left + 10, y: r.top - box.top + r.height / 2 + tree.scrollTop };
}
function drawEdges(): void {
	svg.setAttribute('width', String(tree.scrollWidth));
	svg.setAttribute('height', String(tree.scrollHeight));
	const lines: string[] = [];
	for (const zone of allZones()) {
		const parent = zone.includes('.') ? zone.slice(0, zone.lastIndexOf('.')) : undefined;
		if (parent === undefined) continue;
		const a = center(parent);
		const b = center(zone);
		if (a === undefined || b === undefined) continue;
		lines.push(`<path d="M${a.x} ${a.y} V${b.y} H${b.x}" />`);
	}
	svg.innerHTML = lines.join('');
}
window.addEventListener('resize', drawEdges);
requestAnimationFrame(drawEdges);

function pulse(event: RippleEvent): void {
	const from = center(event.from);
	const to = center(event.at);
	nodes.get(event.at)?.el.classList.remove('pulse');
	void nodes.get(event.at)?.el.offsetWidth;
	nodes.get(event.at)?.el.classList.add('pulse');
	const item = document.createElement('li');
	item.textContent = `${event.from.split('.').at(-1)} → ${event.at.split('.').at(-1)}: ${event.kind}`;
	log.prepend(item);
	while (log.children.length > 8) log.lastChild?.remove();
	if (from === undefined || to === undefined) return;
	const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
	dot.setAttribute('r', '5');
	dot.classList.add(event.kind.includes('respawn') || event.kind.includes('lost') || event.kind.includes('fallen') ? 'bad' : 'good');
	svg.append(dot);
	const start = performance.now();
	const step = (now: number) => {
		const t = Math.min(1, (now - start) / 600);
		// Up the elbow: first along x back to the parent column, then up.
		const [x, y] = t < 0.5 ? [from.x + (to.x - from.x) * t * 2, from.y] : [to.x, from.y + (to.y - from.y) * (t - 0.5) * 2];
		dot.setAttribute('cx', String(x));
		dot.setAttribute('cy', String(y));
		if (t < 1) requestAnimationFrame(step);
		else dot.remove();
	};
	requestAnimationFrame(step);
}
ripple.onRipple(pulse);
ripple.onBoss((zone, event) => {
	if (zone === hood && event.kind === 'defeated') showBanner(`Boss down! (${event.by ?? 'someone'})`);
});

function refreshTree(): void {
	for (const zone of allZones()) {
		const node = nodes.get(zone)!;
		const view = ripple.tier(zone);
		const hello = view.hello;
		const meta = node.el.querySelector<HTMLElement>('[data-role="meta"]')!;
		meta.textContent = hello === undefined ? ' · …' : ` · host ${hello.host} · ${hello.tickRate} Hz · t${view.clock}`;
		node.el.classList.toggle('offline', !view.connected);
		node.el.classList.toggle('here', zone === client.authority?.zoneId);
		let text = '';
		switch (tierOf(zone)) {
			case 'realm':
				text = `bosses ${view.defeated}/${view.total} · boroughs ${view.cleared}/2 · ${view.liberated ? 'city free' : 'city occupied'}`;
				break;
			case 'city':
				text = `${view.weather} · boroughs cleared ${view.cleared}/2${view.liberated ? ' · LIBERATED' : ''}`;
				break;
			case 'borough':
				text = `bosses down ${view.defeated}/3 · ${view.cleared > 0 ? 'unlocked' : 'locked'}`;
				node.el.classList.toggle('cleared', view.cleared > 0);
				break;
			default: {
				const b = view.boss;
				text = b === undefined ? '…' : b.hp === 0 ? `defeated by ${b.by || '?'}` : `boss ${b.hp}/${b.max}`;
				if (node.bar !== undefined && b !== undefined) node.bar.style.width = `${(100 * b.hp) / Math.max(1, b.max)}%`;
				node.el.classList.toggle('cleared', b?.hp === 0);
			}
		}
		if (node.state.textContent !== text) node.state.textContent = text;
	}
}

// ---- input ----
const sampler = new InputSampler();
const frames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });
function sendFrame(): void {
	const s = sampler.sample();
	const move = cameraRelative(s, stage.cameraForward());
	const input: HoodInput = {
		mx: toAxis(move.x),
		my: toAxis(move.y),
		buttons: s.action ? BITS.buttons.dash : 0,
		action: (s.alt ? ACTION_ATTACK : 0) | (smitePending ? ACTION_SMITE : 0)
	};
	if (client.sendInput(input) !== undefined) smitePending = false;
}

let lastTree = 0;
stage.onFrame((now, dtMs) => {
	frames.update(now, sendFrame);
	applySky(dtMs);
	const authority = client.authority;
	const view = authority === undefined ? undefined : ripple.tier(authority.zoneId);
	const hp = view?.boss?.hp ?? 1;
	boss.material = hp === 0 ? mats.down : mats.boss;
	const s = hp === 0 ? 0.35 : 0.55 + 0.45 * (hp / Math.max(1, view?.boss?.max ?? 1));
	boss.scaling.set(s, 1.3 * s, s);
	boss.position.y = 2.2 * s;
	stage.labels.set('boss', boss, hp === 0 ? `defeated by ${view?.boss?.by ?? ''}` : `Boss ${hp}/${view?.boss?.max ?? ''}`, 2.2);

	const rendered = client.render().filter((e) => e.authoritative && isPlayer(e));
	const seen = new Set<string>();
	for (const entity of rendered) {
		const mine = entity.predicted;
		const mesh = capsule(entity, mine);
		const { x, y, name: label } = playerFields(entity);
		mesh.position.copyFrom(toWorld(x, y, 0.9));
		mesh.setEnabled(!entity.stale);
		stage.labels.set(entity.key, mesh, mine ? `${label} (you)` : label, 1.4, mine ? 'me' : '');
		if (mine) stage.follow(x, y, dtMs);
		seen.add(entity.key);
	}
	for (const [key, mesh] of visuals) {
		if (seen.has(key)) continue;
		mesh.dispose();
		stage.labels.delete(key);
		visuals.delete(key);
	}
	if (now - lastTree > 150) {
		lastTree = now;
		refreshTree();
	}
	hud.frame({ entities: rendered.length });
	hud.row('Hood', authority?.zoneId ?? '—');
	hud.row('Weather', ripple.weather() ?? '—');
	hud.row('Realm banner', ripple.banner() ?? '—');
});

declare global {
	interface Window {
		/** Debug handle (console). */
		ripple?: unknown;
	}
}
window.ripple = { ripple, client, api, scene };

client.connect(hood).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
