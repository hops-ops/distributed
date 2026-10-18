/**
 * Babylon.js stage shared by the demos: engine + scene + light, a gridded
 * floor, a third-person follow camera, DOM name labels and helpers.
 *
 * Coordinates: the server's 2D plane (x, y) maps to Babylon (x, 0, z = y),
 * Y up. Use `toWorld(x, y, height)`.
 */
// Deep imports keep the bundle small (the package root pulls in everything).
import { ArcRotateCamera } from '@babylonjs/core/Cameras/arcRotateCamera';
import { Engine } from '@babylonjs/core/Engines/engine';
import { DirectionalLight } from '@babylonjs/core/Lights/directionalLight';
import { HemisphericLight } from '@babylonjs/core/Lights/hemisphericLight';
import { StandardMaterial } from '@babylonjs/core/Materials/standardMaterial';
import { DynamicTexture } from '@babylonjs/core/Materials/Textures/dynamicTexture';
import { Color3, Color4 } from '@babylonjs/core/Maths/math.color';
import { Matrix, Vector3 } from '@babylonjs/core/Maths/math.vector';
import type { AbstractMesh } from '@babylonjs/core/Meshes/abstractMesh';
import type { LinesMesh } from '@babylonjs/core/Meshes/linesMesh';
import { MeshBuilder } from '@babylonjs/core/Meshes/meshBuilder';
import { Scene } from '@babylonjs/core/scene';
// Side effect: Mesh.prototype.createInstance.
import '@babylonjs/core/Meshes/instancedMesh';

export type Stage = Readonly<{
	engine: Engine;
	scene: Scene;
	camera: ArcRotateCamera;
	labels: LabelLayer;
	/** Ground-plane direction the camera looks along (game x/y), for camera-relative input. */
	cameraForward(): { x: number; y: number };
	/** Ease the camera target toward a game position. */
	follow(x: number, y: number, dtMs: number): void;
	/** Register a per-frame callback (after input, before render). */
	onFrame(callback: (now: number, dtMs: number) => void): void;
}>;

export type StageOptions = Readonly<{
	/** Floor half extent (m). */
	half: number;
	/** Grid line spacing (m). */
	gridStep?: number;
	background?: string;
}>;

export function toWorld(x: number, y: number, height = 0): Vector3 {
	return new Vector3(x, height, y);
}

export function material(scene: Scene, name: string, color: string, alpha = 1): StandardMaterial {
	const m = new StandardMaterial(name, scene);
	m.diffuseColor = Color3.FromHexString(color);
	m.specularColor = new Color3(0.1, 0.1, 0.1);
	m.alpha = alpha;
	return m;
}

export function createStage(canvas: HTMLCanvasElement, options: StageOptions): Stage {
	const engine = new Engine(canvas, true, { preserveDrawingBuffer: true, stencil: true });
	const scene = new Scene(engine);
	scene.clearColor = Color4.FromHexString(`${options.background ?? '#0e1116'}ff`);

	const hemi = new HemisphericLight('sky', new Vector3(0, 1, 0), scene);
	hemi.intensity = 0.65;
	const sun = new DirectionalLight('sun', new Vector3(-0.4, -1, 0.3), scene);
	sun.intensity = 0.6;

	const camera = new ArcRotateCamera('camera', -Math.PI / 2, 0.95, 20, Vector3.Zero(), scene);
	camera.lowerRadiusLimit = 6;
	camera.upperRadiusLimit = 60;
	camera.upperBetaLimit = 1.45;
	camera.wheelDeltaPercentage = 0.02;
	camera.attachControl(canvas, true);
	// Arrow keys move the player, not the camera.
	camera.inputs.removeByType('ArcRotateCameraKeyboardMoveInput');

	createFloor(scene, options.half, options.gridStep ?? 1);

	const labels = new LabelLayer(canvas, scene, camera);
	const frameCallbacks: ((now: number, dtMs: number) => void)[] = [];
	let last = performance.now();
	engine.runRenderLoop(() => {
		const now = performance.now();
		const dt = now - last;
		last = now;
		for (const callback of frameCallbacks) callback(now, dt);
		scene.render();
		labels.update();
	});
	window.addEventListener('resize', () => engine.resize());

	return {
		engine,
		scene,
		camera,
		labels,
		cameraForward() {
			return { x: -Math.cos(camera.alpha), y: -Math.sin(camera.alpha) };
		},
		follow(x, y, dtMs) {
			const k = 1 - Math.exp(-dtMs / 120);
			const t = camera.target;
			camera.setTarget(new Vector3(t.x + (x - t.x) * k, 0, t.z + (y - t.z) * k));
		},
		onFrame(callback) {
			frameCallbacks.push(callback);
		}
	};
}

function createFloor(scene: Scene, half: number, step: number): void {
	const size = half * 2;
	const ground = MeshBuilder.CreateGround('floor', { width: size, height: size }, scene);
	const px = 2048;
	const texture = new DynamicTexture('grid', { width: px, height: px }, scene, true);
	const ctx = texture.getContext() as CanvasRenderingContext2D;
	ctx.fillStyle = '#1a1f27';
	ctx.fillRect(0, 0, px, px);
	const lines = Math.round(size / step);
	for (let i = 0; i <= lines; i += 1) {
		const p = (i / lines) * px;
		const major = i % 5 === 0;
		ctx.strokeStyle = major ? '#3a4556' : '#262d38';
		ctx.lineWidth = major ? 3 : 1.5;
		ctx.beginPath();
		ctx.moveTo(p, 0);
		ctx.lineTo(p, px);
		ctx.moveTo(0, p);
		ctx.lineTo(px, p);
		ctx.stroke();
	}
	texture.update();
	const m = new StandardMaterial('floor', scene);
	m.diffuseTexture = texture;
	m.specularColor = Color3.Black();
	ground.material = m;

	// Low walls on the border.
	const wall = material(scene, 'wall', '#4b5668');
	const h = 0.6;
	const t = 0.3;
	for (const [x, z, w, d] of [
		[0, half + t / 2, size + 2 * t, t],
		[0, -half - t / 2, size + 2 * t, t],
		[half + t / 2, 0, t, size],
		[-half - t / 2, 0, t, size]
	] as const) {
		const box = MeshBuilder.CreateBox('wall', { width: w, depth: d, height: h }, scene);
		box.position = new Vector3(x, h / 2, z);
		box.material = wall;
	}
}

/** A flat ring (e.g. an interest radius) on the floor; move it with `.position`. */
export function createRing(scene: Scene, radius: number, color: string): LinesMesh {
	const points: Vector3[] = [];
	for (let i = 0; i <= 96; i += 1) {
		const a = (i / 96) * Math.PI * 2;
		points.push(new Vector3(Math.cos(a) * radius, 0.03, Math.sin(a) * radius));
	}
	const ring = MeshBuilder.CreateLines('ring', { points }, scene);
	ring.color = Color3.FromHexString(color);
	return ring;
}

/** HTML labels that follow meshes (cheap, crisp, no GUI dependency). */
export class LabelLayer {
	readonly #root: HTMLDivElement;
	readonly #labels = new Map<string, { el: HTMLDivElement; mesh: AbstractMesh; offset: number }>();
	readonly #scene: Scene;
	readonly #camera: ArcRotateCamera;

	constructor(canvas: HTMLCanvasElement, scene: Scene, camera: ArcRotateCamera) {
		this.#scene = scene;
		this.#camera = camera;
		this.#root = document.createElement('div');
		this.#root.className = 'labels';
		canvas.parentElement?.appendChild(this.#root);
	}

	set(key: string, mesh: AbstractMesh, text: string, offset = 2.2, className = ''): void {
		let label = this.#labels.get(key);
		if (label === undefined) {
			const el = document.createElement('div');
			this.#root.appendChild(el);
			label = { el, mesh, offset };
			this.#labels.set(key, label);
		}
		label.mesh = mesh;
		label.offset = offset;
		if (label.el.textContent !== text) label.el.textContent = text;
		const cls = `label ${className}`.trim();
		if (label.el.className !== cls) label.el.className = cls;
	}

	delete(key: string): void {
		this.#labels.get(key)?.el.remove();
		this.#labels.delete(key);
	}

	update(): void {
		const engine = this.#scene.getEngine();
		const width = engine.getRenderWidth();
		const height = engine.getRenderHeight();
		const scale = engine.getHardwareScalingLevel();
		const viewport = this.#camera.viewport.toGlobal(width, height);
		const transform = this.#scene.getTransformMatrix();
		for (const { el, mesh, offset } of this.#labels.values()) {
			const world = mesh.getAbsolutePosition().add(new Vector3(0, offset, 0));
			const p = Vector3.Project(world, Matrix.IdentityReadOnly, transform, viewport);
			const visible = p.z > 0 && p.z < 1 && mesh.isEnabled();
			el.style.display = visible ? '' : 'none';
			if (visible) el.style.transform = `translate(${p.x * scale}px, ${p.y * scale}px) translate(-50%, -100%)`;
		}
	}
}
