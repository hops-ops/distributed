/**
 * The Freeze page: Babylon scene over `createFreezeClient`, plus a dev
 * admin panel (needs the admin secret printed by the server; `?admin=`).
 *
 * Everything that depends on content (pool look, slime color/size,
 * crystals, gem value) is derived from the client's *own* pack, so a
 * pinned old client visibly keeps the old world while the server already
 * runs the new rules.
 */
import type { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { Color3 } from '@babylonjs/core/Maths/math.color';
import type { Mesh } from '@babylonjs/core/Meshes/mesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { InputPacer, type RenderedEntity } from '@hops-ops/distributed/game';

import { HALF, PILLARS, PLAYER_RADIUS } from '../arena/movement.ts';
import { BUTTON, KIND, gemFields, playerFields, ritualFields, slimeFields, type KindName, type MoveInput } from '../generated/freeze/index.ts';
import { DevApi } from '../shared/dev-api.ts';
import { Hud } from '../shared/hud.ts';
import { cameraRelative, InputSampler, toAxis } from '../shared/input.ts';
import { createRing, createStage, material, toWorld } from '../shared/scene.ts';
import { createFreezeClient, DEFAULT_ZONE, FreezeAdmin, ZONES, type Pack, type WorldStatus } from './client.ts';
import './freeze.css';

const params = new URLSearchParams(location.search);
const zone = params.get('zone') ?? DEFAULT_ZONE;
const name = params.get('name') ?? undefined;
let pinned = params.get('old') === '1';

function stored(key: string): string | undefined {
	try {
		return localStorage.getItem(key) ?? undefined;
	} catch {
		return undefined;
	}
}
function store(key: string, value: string): void {
	try {
		localStorage.setItem(key, value);
	} catch {
		// private mode
	}
}

const el = <K extends keyof HTMLElementTagNameMap>(tag: K, className = '', text?: string): HTMLElementTagNameMap[K] => {
	const node = document.createElement(tag);
	if (className !== '') node.className = className;
	if (text !== undefined) node.textContent = text;
	return node;
};

declare global {
	interface Window {
		/** Debug handle (console). */
		freeze?: unknown;
	}
}

async function main(): Promise<void> {
	const api = new DevApi({ name: () => name });
	const freeze = await createFreezeClient({ api, pinned: () => pinned });
	const { client } = freeze;

	const canvas = document.getElementById('stage') as HTMLCanvasElement;
	const stage = createStage(canvas, { half: HALF, background: '#0d1013' });
	const { scene } = stage;

	const hud = new Hud({
		title: 'The Freeze',
		hint: 'WASD to move, Space to dash. Walk through portals. Roll out updates in the admin panel; O: simulate an old client.',
		client
	});
	hud.toggle('Simulate old client (no live patch)', pinned, (on) => (pinned = on), 'o');

	// ---- DOM overlays ----
	const badge = el('div', 'zone-badge');
	const overlay = el('div', 'freeze-overlay');
	const toast = el('div', 'freeze-toast');
	const banner = el('div', 'stale-banner');
	const logBox = el('ol', 'freeze-log');
	document.body.append(badge, overlay, toast, banner, logBox);
	let toastTimer: ReturnType<typeof setTimeout> | undefined;

	function showToast(title: string, lines: readonly string[], kind: string): void {
		toast.replaceChildren(el('strong', '', title), ...lines.map((l) => el('div', '', `• ${l}`)));
		toast.className = `freeze-toast show ${kind}`;
		clearTimeout(toastTimer);
		toastTimer = setTimeout(() => (toast.className = 'freeze-toast'), 7000);
	}

	freeze.onChange((what) => {
		if (what === 'resume') {
			const r = freeze.lastResume()!;
			if (r.aborted) showToast(`SYSTEM: update aborted — the world stays on ${r.version}`, r.notes, 'aborted');
			else if (r.patched || !pinned) showToast(`SYSTEM: update ${r.version} applied. Here's what changed:`, r.notes.slice(1), 'applied');
			else showToast(`SYSTEM: the world now runs ${r.version}`, ['Your old client keeps its content until you enter the Town Hub.', ...r.notes.slice(1)], 'stale');
		}
		if (what === 'pack' && freeze.reloads.length > 0 && freeze.reloads.at(-1)!.version === freeze.pack().version) {
			const p = freeze.pack();
			showToast(`SYSTEM: content reloaded — ${p.name} (${p.version})`, p.notes, 'applied');
		}
		if (what === 'log') {
			const e = freeze.log.at(-1)!;
			const li = el('li', e.kind, `${e.text}${e.stamp === undefined ? '' : ` · ${e.stamp.zone}#${e.stamp.tick} @${e.stamp.contentVersion}`}`);
			logBox.prepend(li);
			while (logBox.children.length > 8) logBox.lastChild?.remove();
		}
	});

	// ---- admin panel ----
	// `?admin=` is remembered, so other tabs (e.g. `?old=1`) can roll out too.
	const adminParam = params.get('admin');
	if (adminParam !== null) store('game-demos.freeze.admin', adminParam);
	const admin = new FreezeAdmin('', adminParam ?? stored('game-demos.freeze.admin') ?? '');
	const panel = el('fieldset', 'admin');
	panel.append(el('legend', '', 'World admin (dev, admin secret)'));
	const secret = el('input', 'secret');
	secret.type = 'password';
	secret.placeholder = 'admin secret (printed by the server)';
	secret.value = admin.secret;
	secret.addEventListener('input', () => {
		admin.secret = secret.value.trim();
		store('game-demos.freeze.admin', admin.secret);
	});
	const buttons = el('div', 'buttons');
	const hardButton = el('button', '', 'Roll out update (Hard)');
	const rollingButton = el('button', '', 'Roll out (Safe room)');
	buttons.append(hardButton, rollingButton);
	const ritualLabel = el('label', 'toggle');
	const ritual = el('input');
	ritual.type = 'checkbox';
	ritualLabel.append(ritual, document.createTextNode(' Ritual in Ember Fields (zone refuses to freeze)'));
	const result = el('small', 'result', '');
	const status = el('div', 'world-status');
	panel.append(secret, buttons, ritualLabel, result, status);
	hud.root.append(panel);

	function needSecret(): boolean {
		if (admin.secret !== '') return false;
		result.textContent = 'Paste the admin secret printed by the server (or open /freeze/?admin=<secret>).';
		secret.focus();
		return true;
	}

	async function roll(mode: 'hard' | 'rolling'): Promise<void> {
		if (needSecret()) return;
		hardButton.disabled = rollingButton.disabled = true;
		result.textContent = `rolling out (${mode})…`;
		try {
			const r = await admin.rollout(mode);
			result.textContent = r.ok
				? `✓ ${String(r.from)} → ${String(r.version)} "${String(r.name)}" in ${String(r.tookMs)} ms`
				: `✗ aborted (${String(r.error)}: ${(r.missing as string[] | undefined)?.join(', ') ?? ''}) — still ${String(r.version)}`;
		} catch (error) {
			result.textContent = String(error);
		} finally {
			hardButton.disabled = rollingButton.disabled = false;
		}
	}
	hardButton.addEventListener('click', () => void roll('hard'));
	rollingButton.addEventListener('click', () => void roll('rolling'));
	ritual.addEventListener('change', () => {
		if (needSecret()) {
			ritual.checked = !ritual.checked;
			return;
		}
		admin.setStuck(ZONES.ember, ritual.checked).then(
			() => (result.textContent = ritual.checked ? 'Ember Fields will refuse the next freeze' : 'ritual ended'),
			(error: unknown) => {
				result.textContent = String(error);
				ritual.checked = !ritual.checked;
			}
		);
	});

	let world: WorldStatus | undefined;
	async function pollWorld(): Promise<void> {
		try {
			world = await admin.world();
			status.replaceChildren(
				...world.hosts.flatMap((h) =>
					(h.zones ?? []).map((z) =>
						el(
							'div',
							`zone-row ${z.state}`,
							`${h.host} · ${z.title.replace(' (safe room)', '')}: ${z.state} ${z.contentVersion} · ${z.players}p · dropped ${z.droppedFrozen}${z.stuck ? ' · RITUAL' : ''}`
						)
					)
				)
			);
			ritual.checked = world.hosts.some((h) => h.zones?.some((z) => z.zone === ZONES.ember && z.stuck));
		} catch {
			status.textContent = 'world status unavailable';
		}
	}
	setInterval(() => void pollWorld(), 1000);
	void pollWorld();

	// ---- content-derived scenery ----
	const mats = {
		me: material(scene, 'me', '#6cc4ff'),
		other: material(scene, 'other', '#f2a65a'),
		pillar: material(scene, 'pillar', '#7b8798'),
		portal: material(scene, 'portal', '#b48cff'),
		gem: material(scene, 'gem', '#fff27a')
	};
	mats.portal.emissiveColor = Color3.FromHexString('#5b3fa8');
	mats.gem.emissiveColor = Color3.FromHexString('#8a7d1e');
	for (const [x, y, r] of PILLARS) {
		const p = MeshBuilder.CreateCylinder('pillar', { diameter: r * 2, height: 2, tessellation: 24 }, scene);
		p.position = toWorld(x, y, 1);
		p.material = mats.pillar;
	}
	let scenery: Mesh[] = [];
	let sceneryKey = '';
	let slimeMat: StandardMaterial = material(scene, 'slime', '#7ed957');

	function buildScenery(pack: Pack, zoneId: string): void {
		for (const m of scenery) m.dispose();
		for (const key of [...portalLabels]) stage.labels.delete(key);
		portalLabels.clear();
		scenery = [];
		const layout = pack.zones[zoneId];
		if (layout === undefined) return;
		const lava = pack.hazard === 'lava';
		const pool = material(scene, `pool-${pack.version}`, lava ? '#ff5a1f' : '#bfe9ff', lava ? 1 : 0.85);
		pool.emissiveColor = Color3.FromHexString(lava ? '#a8320c' : '#3d6f8a');
		for (const p of layout.pools) {
			const disc = MeshBuilder.CreateDisc('pool', { radius: p.r, tessellation: 40 }, scene);
			disc.rotation.x = Math.PI / 2;
			disc.position = toWorld(p.x, p.y, 0.02);
			disc.material = pool;
			scenery.push(disc);
		}
		for (const p of layout.portals) {
			const ring = MeshBuilder.CreateTorus('portal', { diameter: 2.4, thickness: 0.25, tessellation: 32 }, scene);
			ring.rotation.z = Math.PI / 2;
			ring.position = toWorld(p.x, p.y, 1.3);
			ring.material = mats.portal;
			scenery.push(ring);
			const key = `portal:${p.to}`;
			portalLabels.add(key);
			stage.labels.set(key, ring, `→ ${p.title}`, 1.6, 'portal');
		}
		const crystal = material(scene, `crystal-${pack.version}`, pack.crystalColor);
		crystal.emissiveColor = Color3.FromHexString(pack.crystalColor).scale(0.35);
		for (const [x, y] of pack.zones.crystals[pack.crystalLayout] ?? []) {
			const c = MeshBuilder.CreateCylinder('crystal', { diameterTop: 0, diameterBottom: 1.1, height: 2.6, tessellation: 5 }, scene);
			c.position = toWorld(x, y, 1.3);
			c.material = crystal;
			scenery.push(c);
		}
		slimeMat = material(scene, `slime-${pack.version}`, pack.slimeColor);
		for (const v of visuals.values()) if (v.kind === 'slime') v.mesh.material = slimeMat;
	}
	const portalLabels = new Set<string>();

	// ---- entities ----
	type Visual = { mesh: Mesh; kind: KindName };
	const kindOf = (entity: RenderedEntity): KindName =>
		(Object.keys(KIND) as KindName[]).find((k) => KIND[k] === entity.kind) ?? 'player';
	const visuals = new Map<string, Visual>();
	const ritualRing = createRing(scene, 2, '#ff4d6d');
	ritualRing.setEnabled(false);

	function visualFor(entity: RenderedEntity, mine: boolean): Visual {
		let v = visuals.get(entity.key);
		if (v !== undefined) return v;
		let mesh: Mesh;
		const kind = kindOf(entity);
		switch (kind) {
			case 'slime':
				mesh = MeshBuilder.CreateSphere(entity.key, { diameter: 0.9, segments: 10 }, scene);
				mesh.scaling.y = 0.6;
				mesh.material = slimeMat;
				break;
			case 'gem':
				mesh = MeshBuilder.CreatePolyhedron(entity.key, { type: 1, size: 0.35 }, scene);
				mesh.material = mats.gem;
				break;
			case 'ritual':
				mesh = MeshBuilder.CreateBox(entity.key, { size: 0.01 }, scene);
				mesh.isVisible = false;
				break;
			default:
				mesh = MeshBuilder.CreateCapsule(entity.key, { radius: PLAYER_RADIUS, height: 1.8 }, scene);
				mesh.material = mine ? mats.me : mats.other;
		}
		v = { mesh, kind };
		visuals.set(entity.key, v);
		return v;
	}

	// ---- input ----
	const sampler = new InputSampler();
	const frames = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });
	let lastInput: MoveInput = { mx: 0, my: 0, buttons: 0 };
	const sendFrame = () => {
		const s = sampler.sample();
		const move = cameraRelative(s, stage.cameraForward());
		lastInput = { mx: toAxis(move.x), my: toAxis(move.y), buttons: s.action ? BUTTON.dash : 0 };
		freeze.sendInput(lastInput);
	};

	stage.onFrame((now, dtMs) => {
		frames.update(now, sendFrame);
		const hello = freeze.hello();
		const pack = freeze.pack();
		const authority = client.authority;
		const zoneId = hello?.zone ?? authority?.zoneId ?? zone;
		if (sceneryKey !== `${pack.version}:${zoneId}`) {
			sceneryKey = `${pack.version}:${zoneId}`;
			buildScenery(pack, zoneId);
		}
		const rendered = client.render();
		const seen = new Set<string>();
		let me: { x: number; y: number } | undefined;
		let ritualStuck = false;
		for (const entity of rendered) {
			if (authority !== undefined && entity.zoneId !== authority.zoneId) continue;
			const mine = hello !== undefined && entity.id === hello.entity;
			const v = visualFor(entity, mine);
			seen.add(entity.key);
			switch (v.kind) {
				case 'slime': {
					const { x, y } = slimeFields(entity);
					v.mesh.position.copyFrom(toWorld(x, y, 0.3 * pack.slimeScale));
					v.mesh.scaling.set(pack.slimeScale, 0.6 * pack.slimeScale, pack.slimeScale);
					break;
				}
				case 'gem': {
					const { x, y } = gemFields(entity);
					v.mesh.position.copyFrom(toWorld(x, y, 0.8 + 0.15 * Math.sin(now / 300)));
					v.mesh.rotation.y = now / 600;
					break;
				}
				case 'ritual': {
					const { x, y, stuck } = ritualFields(entity);
					v.mesh.position.copyFrom(toWorld(x, y, 0));
					ritualStuck = stuck === 1;
					ritualRing.position.copyFrom(toWorld(x, y, 0));
					if (ritualStuck) stage.labels.set('ritual', v.mesh, 'RITUAL IN PROGRESS — this zone cannot pause', 0.6, 'ritual');
					else stage.labels.delete('ritual');
					break;
				}
				default: {
					const { x, y, name } = playerFields(entity);
					v.mesh.position.copyFrom(toWorld(x, y, 0.9));
					stage.labels.set(entity.key, v.mesh, mine ? `${name} (you)` : name, 1.4, mine ? 'me' : '');
					if (mine) me = { x, y };
				}
			}
			v.mesh.setEnabled(!entity.stale);
		}
		ritualRing.setEnabled(ritualStuck);
		ritualRing.scaling.setAll(1 + 0.08 * Math.sin(now / 150));
		for (const [key, v] of visuals) {
			if (seen.has(key)) continue;
			v.mesh.dispose();
			stage.labels.delete(key);
			visuals.delete(key);
		}
		if (me !== undefined) stage.follow(me.x, me.y, dtMs);

		// Overlays.
		const frozen = freeze.frozen();
		overlay.classList.toggle('show', frozen !== undefined);
		if (frozen !== undefined) {
			const title = frozen.notes.split('\n')[0] ?? '';
			overlay.replaceChildren(
				el('div', 'system', 'SYSTEM'),
				el('div', 'line', 'An administrator has paused the world…'),
				el('div', 'line', `applying update ${frozen.version}${title === '' ? '' : ` — ${title}`}`),
				el('div', 'spinner'),
				el('small', '', `inputs dropped while frozen: ${freeze.droppedWhileFrozen()}`)
			);
		}
		const stale = freeze.stale() && pinned;
		banner.classList.toggle('show', stale);
		if (stale) {
			banner.textContent = hello?.gate
				? `Your client runs ${client.contentVersion}; the world runs ${freeze.worldVersion()}. Leave and re-enter the hub to update.`
				: `Your client runs ${client.contentVersion} (old build); the world runs ${freeze.worldVersion()}. Keep playing — walk into the Town Hub (safe room) to update.`;
		}
		badge.textContent = hello === undefined ? 'connecting…' : `${hello.title} · host ${hello.host} · content ${client.contentVersion} "${pack.name}"`;

		hud.frame({ entities: rendered.length });
		const serverDropped = world?.hosts.reduce((n, h) => n + (h.zones ?? []).reduce((m, z) => m + z.droppedFrozen, 0), 0);
		hud.row('You', api.last === undefined ? '—' : api.last.name);
		hud.row('Client content', `${client.contentVersion} ${pinned ? '(pinned old client)' : '(live patching)'}`);
		hud.row('World content', freeze.worldVersion() ?? '—');
		hud.row('Pools', pack.hazard === 'lava' ? 'lava (burns)' : 'ice (safe)');
		hud.row('Inputs dropped', `client ${freeze.droppedWhileFrozen()} · server ${serverDropped ?? '—'}`);
		hud.row('Reloads', freeze.reloads.map((r) => `${r.version}@${r.zone.replace('freeze-', '')}`).join(', ') || '—');
	});

	window.freeze = { freeze, client, api, admin, scene };

	await client.connect(zone).catch((error: unknown) => hud.row('Status', `failed: ${String(error)}`));
}

main().catch((error: unknown) => {
	document.body.append(el('pre', 'fatal', `The Freeze failed to start: ${String(error)}\n(is the server running? ./run.sh freeze)`));
});
