/**
 * Top-down minimap overlay (2D canvas, top-right). Draws the floor bounds,
 * the local view radius and a dot per entity, so it is obvious which
 * entities the server culled by interest management and which are global.
 */

export type MinimapDot = Readonly<{
	x: number;
	y: number;
	color: string;
	/** Dot radius in CSS pixels (default 3). */
	size?: number;
	/** Draw as an outlined ring instead of a filled dot. */
	hollow?: boolean;
}>;

export type MinimapFrame = Readonly<{
	/** Local player position; the view ring is centred here. */
	me?: Readonly<{ x: number; y: number }>;
	/** View radius in world metres (omit to hide the ring). */
	viewRadius?: number;
	dots: readonly MinimapDot[];
}>;

export type MinimapOptions = Readonly<{
	/** Half the floor size in metres: the map shows [-half, half]². */
	half: number;
	/** Canvas size in CSS pixels (default 180). */
	size?: number;
	/** Legend entries shown under the map. */
	legend?: readonly Readonly<{ color: string; label: string }>[];
}>;

export class Minimap {
	readonly #canvas: HTMLCanvasElement;
	readonly #ctx: CanvasRenderingContext2D;
	readonly #half: number;
	readonly #size: number;

	constructor(options: MinimapOptions, parent: HTMLElement = document.body) {
		this.#half = options.half;
		this.#size = options.size ?? 180;
		const root = document.createElement('div');
		root.className = 'minimap';
		this.#canvas = document.createElement('canvas');
		const dpr = globalThis.devicePixelRatio ?? 1;
		this.#canvas.width = Math.round(this.#size * dpr);
		this.#canvas.height = Math.round(this.#size * dpr);
		this.#canvas.style.width = `${this.#size}px`;
		this.#canvas.style.height = `${this.#size}px`;
		root.append(this.#canvas);
		if (options.legend !== undefined && options.legend.length > 0) {
			const legend = document.createElement('ul');
			for (const entry of options.legend) {
				const item = document.createElement('li');
				const swatch = document.createElement('span');
				swatch.style.background = entry.color;
				item.append(swatch, entry.label);
				legend.append(item);
			}
			root.append(legend);
		}
		parent.append(root);
		const ctx = this.#canvas.getContext('2d');
		if (ctx === null) throw new Error('minimap: 2d context unavailable');
		ctx.scale(dpr, dpr);
		this.#ctx = ctx;
	}

	/** World metres → canvas pixels (north-up: +y is drawn upward). */
	#px(x: number, y: number): [number, number] {
		const s = this.#size / (this.#half * 2);
		return [(x + this.#half) * s, (this.#half - y) * s];
	}

	draw(frame: MinimapFrame): void {
		const ctx = this.#ctx;
		const size = this.#size;
		ctx.clearRect(0, 0, size, size);
		ctx.fillStyle = 'rgba(10, 14, 20, 0.75)';
		ctx.fillRect(0, 0, size, size);
		ctx.strokeStyle = 'rgba(255, 255, 255, 0.25)';
		ctx.lineWidth = 1;
		ctx.strokeRect(0.5, 0.5, size - 1, size - 1);
		if (frame.me !== undefined && frame.viewRadius !== undefined) {
			const [cx, cy] = this.#px(frame.me.x, frame.me.y);
			const r = (frame.viewRadius / (this.#half * 2)) * size;
			ctx.beginPath();
			ctx.arc(cx, cy, r, 0, Math.PI * 2);
			ctx.fillStyle = 'rgba(108, 196, 255, 0.08)';
			ctx.fill();
			ctx.strokeStyle = 'rgba(108, 196, 255, 0.6)';
			ctx.stroke();
		}
		for (const dot of frame.dots) {
			const [x, y] = this.#px(dot.x, dot.y);
			ctx.beginPath();
			ctx.arc(x, y, dot.size ?? 3, 0, Math.PI * 2);
			if (dot.hollow === true) {
				ctx.strokeStyle = dot.color;
				ctx.lineWidth = 1.5;
				ctx.stroke();
			} else {
				ctx.fillStyle = dot.color;
				ctx.fill();
			}
		}
	}
}
