/**
 * Shared HUD: connection status, RTT/jitter, server tick, delta bytes/s and
 * entity count for the authoritative zone of a `GameClient`, plus demo rows,
 * toggles and the server-side NetSim controls (`/dev/netsim`).
 *
 * ```ts
 * const hud = new Hud({ title: 'Mob Pit', client, devApi });
 * hud.toggle('Prediction', true, (on) => (predict = on));
 * stage.onFrame(() => hud.frame({ entities: rendered.length }));
 * hud.row('Pending inputs', String(n));
 * ```
 */
import type { GameClient } from '@hops-ops/distributed/game';

import type { DevApi, NetSimSettings } from './dev-api.ts';

export type HudOptions = Readonly<{
	title: string;
	/** One-line description of what to try. */
	hint?: string;
	client: GameClient<any>;
	/** Shows NetSim sliders when given. */
	devApi?: DevApi;
	/** Mount point (default document.body). */
	parent?: HTMLElement;
}>;

const STAT_INTERVAL_MS = 250;

export class Hud {
	readonly root: HTMLElement;
	readonly #client: GameClient<any>;
	readonly #rows = new Map<string, HTMLElement>();
	readonly #table: HTMLElement;
	/** Toggles sit right under the hint, above the stats (always in view). */
	readonly #controls: HTMLElement;
	#lastSample: { at: number; deltaBytes: number; connection: unknown } | undefined;
	#deltaRate = 0;
	#lastStatAt = 0;

	constructor(options: HudOptions) {
		this.#client = options.client;
		this.root = el('aside', 'hud');
		const title = el('h1', '', options.title);
		this.root.append(title);
		if (options.hint !== undefined) this.root.append(el('p', 'hint', options.hint));
		this.#controls = el('div', 'controls');
		this.#table = el('dl', 'stats');
		this.root.append(this.#controls, this.#table);
		for (const name of ['Status', 'Zone', 'RTT', 'Jitter', 'Server tick', 'Delta in', 'Entities']) {
			this.row(name, '—');
		}
		if (options.devApi !== undefined) this.root.append(netSimPanel(options.devApi));
		(options.parent ?? document.body).append(this.root);
	}

	/** Set (or add) a stat row. */
	row(name: string, value: string): void {
		let dd = this.#rows.get(name);
		if (dd === undefined) {
			const dt = el('dt', '', name);
			dd = el('dd');
			this.#table.append(dt, dd);
			this.#rows.set(name, dd);
		}
		if (dd.textContent !== value) dd.textContent = value;
	}

	/** Add a checkbox toggle. */
	toggle(label: string, initial: boolean, onChange: (on: boolean) => void, key?: string): void {
		const wrap = el('label', 'toggle');
		const input = document.createElement('input');
		input.type = 'checkbox';
		input.checked = initial;
		input.addEventListener('change', () => onChange(input.checked));
		wrap.append(input, document.createTextNode(` ${label}`));
		if (key !== undefined) {
			wrap.title = `key: ${key}`;
			wrap.append(el('kbd', '', key.toUpperCase()));
		}
		this.#controls.append(wrap);
		if (key !== undefined) {
			window.addEventListener('keydown', (e) => {
				if (e.key.toLowerCase() === key.toLowerCase() && !e.repeat) {
					input.checked = !input.checked;
					onChange(input.checked);
				}
			});
		}
	}

	/** Call every frame; refreshes the standard rows a few times a second. */
	frame(extra: Readonly<{ entities?: number }> = {}): void {
		const now = performance.now();
		if (now - this.#lastStatAt < STAT_INTERVAL_MS) return;
		this.#lastStatAt = now;
		const zone = this.#client.authority;
		if (zone === undefined) {
			this.row('Status', 'not connected');
			return;
		}
		const connection = zone.connection;
		const stats = connection.stats;
		const sample = this.#lastSample;
		if (sample !== undefined && sample.connection === connection && now > sample.at) {
			const rate = ((stats.deltaBytesIn - sample.deltaBytes) * 1000) / (now - sample.at);
			this.#deltaRate = this.#deltaRate === 0 ? rate : this.#deltaRate * 0.7 + rate * 0.3;
		}
		this.#lastSample = { at: now, deltaBytes: stats.deltaBytesIn, connection };
		const clock = connection.clock;
		this.row('Status', connection.frozen ? 'frozen' : connection.state);
		this.row('Zone', zone.zoneId);
		this.row('RTT', clock.synced ? `${clock.rtt.toFixed(0)} ms` : '—');
		this.row('Jitter', clock.synced ? `${clock.jitter.toFixed(1)} ms` : '—');
		this.row('Server tick', String(zone.replica.serverTick));
		this.row('Delta in', `${formatBytes(this.#deltaRate)}/s · ${stats.deltasIn} deltas`);
		this.row('Entities', String(extra.entities ?? zone.replica.size));
	}
}

function netSimPanel(api: DevApi): HTMLElement {
	const box = el('fieldset', 'netsim');
	box.append(el('legend', '', 'Network simulation (server, all clients)'));
	const settings = { latencyMs: 0, jitterMs: 0, loss: 0 };
	const sliders: Record<keyof NetSimSettings, HTMLInputElement> = {
		latencyMs: slider(box, 'Latency (one way)', 0, 300, 5, (v) => `${v} ms`),
		jitterMs: slider(box, 'Jitter', 0, 100, 1, (v) => `${v} ms`),
		loss: slider(box, 'Loss (retransmit)', 0, 0.2, 0.01, (v) => `${Math.round(v * 100)}%`)
	};
	let timer: ReturnType<typeof setTimeout> | undefined;
	const status = el('small', 'netsim-status', '');
	box.append(status);
	const push = () => {
		clearTimeout(timer);
		timer = setTimeout(() => {
			api
				.setNetSim(settings)
				.then(() => (status.textContent = 'applied'))
				.catch((e: unknown) => (status.textContent = `failed: ${String(e)}`));
		}, 150);
	};
	for (const key of Object.keys(sliders) as (keyof NetSimSettings)[]) {
		sliders[key].addEventListener('input', () => {
			settings[key] = Number(sliders[key].value);
			push();
		});
	}
	api
		.getNetSim()
		.then((current) => {
			for (const key of Object.keys(sliders) as (keyof NetSimSettings)[]) {
				settings[key] = current[key];
				sliders[key].value = String(current[key]);
				sliders[key].dispatchEvent(new Event('sync'));
			}
		})
		.catch(() => (status.textContent = 'dev routes unavailable (start with --dev)'));
	return box;
}

function slider(
	parent: HTMLElement,
	label: string,
	min: number,
	max: number,
	step: number,
	format: (v: number) => string
): HTMLInputElement {
	const row = el('label', 'slider');
	const name = el('span', '', label);
	const value = el('output');
	const input = document.createElement('input');
	input.type = 'range';
	input.min = String(min);
	input.max = String(max);
	input.step = String(step);
	input.value = String(min);
	const show = () => (value.textContent = format(Number(input.value)));
	input.addEventListener('input', show);
	input.addEventListener('sync', show);
	show();
	row.append(name, input, value);
	parent.append(row);
	return input;
}

function el(tag: string, className = '', text?: string): HTMLElement {
	const node = document.createElement(tag);
	if (className !== '') node.className = className;
	if (text !== undefined) node.textContent = text;
	return node;
}

export function formatBytes(n: number): string {
	if (n < 1024) return `${n.toFixed(0)} B`;
	return `${(n / 1024).toFixed(1)} KiB`;
}
