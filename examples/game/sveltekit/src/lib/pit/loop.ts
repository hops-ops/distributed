/**
 * The Mob Pit render and input loop: plain TypeScript on its own
 * `requestAnimationFrame`, outside Svelte. It reads `client.render()` every
 * frame and sends one input per server tick; the Svelte page only starts it
 * and reads the slower facts (status, RTT, events) through `useGame`.
 */
import { InputPacer, type GameClient } from '@hops-ops/distributed/game';

import {
	BUTTON,
	PIT_RULES,
	isMob,
	isPillar,
	isPlayer,
	mobFields,
	pillarFields,
	playerFields,
	type HelloEvent,
	type PitInput
} from '$lib/generated/mobpit/index.ts';

const TIER_COLOR: Record<number, string> = { 1: '#9ad16b', 2: '#b48cf2', 3: '#ff6b6b' };
const TIER_RADIUS = Object.fromEntries(PIT_RULES.tiers.map((t) => [t.tier, t]));

export type PitLoopOptions = Readonly<{
	client: GameClient<PitInput>;
	hello: () => HelloEvent | undefined;
	/** Steer to the nearest mob and swing (demo and headless test). */
	autoHunt: () => boolean;
}>;

/** Start drawing on `canvas`; returns the stop function. */
export function startPitLoop(canvas: HTMLCanvasElement, options: PitLoopOptions): () => void {
	const { client } = options;
	const ctx = canvas.getContext('2d')!;
	const keys = new Set<string>();
	const down = (e: KeyboardEvent) => {
		if (['KeyW', 'KeyA', 'KeyS', 'KeyD', 'Space', 'KeyE'].includes(e.code)) e.preventDefault();
		keys.add(e.code);
	};
	const up = (e: KeyboardEvent) => keys.delete(e.code);
	window.addEventListener('keydown', down);
	window.addEventListener('keyup', up);
	const pacer = new InputPacer({ tickRate: () => client.authority?.connection.welcome?.tickRate });

	const steer = (): PitInput => {
		const hello = options.hello();
		const rendered = client.render();
		const me = rendered.find((e) => e.id === hello?.entity && isPlayer(e));
		if (options.autoHunt() && me !== undefined) {
			const p = playerFields(me);
			const target = rendered
				.filter((e) => isMob(e) && !e.stale)
				.map(mobFields)
				.map((m) => ({ m, d: Math.hypot(m.x - p.x, m.y - p.y) }))
				.sort((a, b) => a.d - b.d)[0];
			if (target === undefined || p.dead) return { mx: 0, my: 0, buttons: 0 };
			const near = target.d < 1.9;
			const k = (near ? 40 : 127) / Math.max(target.d, 1e-3);
			return {
				mx: Math.round((target.m.x - p.x) * k),
				my: Math.round((target.m.y - p.y) * k),
				buttons: near ? BUTTON.attack : 0
			};
		}
		const axis = (neg: string, pos: string) => (keys.has(pos) ? 127 : 0) - (keys.has(neg) ? 127 : 0);
		return {
			mx: axis('KeyA', 'KeyD'),
			// Screen y grows downwards; world y grows upwards.
			my: axis('KeyS', 'KeyW'),
			buttons: (keys.has('Space') ? BUTTON.attack : 0) | (keys.has('KeyE') ? BUTTON.dash : 0)
		};
	};

	let frame = requestAnimationFrame(function draw(now) {
		pacer.update(now, () => client.sendInput(steer()));
		const half = options.hello()?.half ?? 12;
		const { width, height } = canvas;
		const scale = Math.min(width, height) / (2 * half);
		const at = (x: number, y: number): [number, number] => [width / 2 + x * scale, height / 2 - y * scale];
		const circle = (x: number, y: number, r: number, fill: string, stroke?: string) => {
			const [cx, cy] = at(x, y);
			ctx.beginPath();
			ctx.arc(cx, cy, r * scale, 0, Math.PI * 2);
			ctx.fillStyle = fill;
			ctx.fill();
			if (stroke !== undefined) {
				ctx.strokeStyle = stroke;
				ctx.stroke();
			}
		};
		ctx.fillStyle = '#120f14';
		ctx.fillRect(0, 0, width, height);
		ctx.strokeStyle = '#2b2533';
		ctx.strokeRect(...at(-half, half), 2 * half * scale, 2 * half * scale);
		for (const entity of client.render()) {
			if (isPillar(entity)) {
				const p = pillarFields(entity);
				circle(p.x, p.y, p.r, '#3a3443');
			} else if (isMob(entity)) {
				const m = mobFields(entity);
				const look = TIER_RADIUS[m.tier];
				if (m.tele > 0 && look !== undefined) circle(m.x, m.y, look.tele, 'rgba(255,80,80,0.12)', '#ff5050');
				circle(m.x, m.y, look?.radius ?? 0.5, TIER_COLOR[m.tier] ?? '#9ad16b');
			} else if (isPlayer(entity)) {
				const p = playerFields(entity);
				const mine = entity.id === options.hello()?.entity;
				// Own position for the headless test (a DOM attribute, not Svelte state).
				if (mine) canvas.dataset.me = `${p.x.toFixed(2)},${p.y.toFixed(2)}`;
				circle(p.x, p.y, 0.45, p.dead ? '#555' : mine ? '#ffd166' : '#6ec3ff');
				const [nx, ny] = at(p.x + Math.cos(p.face) * 0.6, p.y + Math.sin(p.face) * 0.6);
				const [cx, cy] = at(p.x, p.y);
				ctx.strokeStyle = '#fff';
				ctx.beginPath();
				ctx.moveTo(cx, cy);
				ctx.lineTo(nx, ny);
				ctx.stroke();
				ctx.fillStyle = '#ddd';
				ctx.font = '11px system-ui';
				ctx.fillText(p.name, cx - 16, cy - 0.7 * scale);
			}
		}
		frame = requestAnimationFrame(draw);
	});

	return () => {
		cancelAnimationFrame(frame);
		window.removeEventListener('keydown', down);
		window.removeEventListener('keyup', up);
	};
}
