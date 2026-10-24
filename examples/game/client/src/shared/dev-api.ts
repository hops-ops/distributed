/**
 * Client side of the demo server's **dev-only** routes (`/dev/ticket`,
 * `/dev/netsim`, see `server/src/common/dev.rs`) plus zone URLs.
 *
 * A real game gets admission tickets from its authenticated lobby; the dev
 * route mints one for a server-generated guest. We keep the server-signed
 * guest token per browser (localStorage when available): a reload, a new
 * tab or a reopened browser is the same player (a second tab takes the
 * player over). `?player=<label>` in the page URL keeps a separate guest
 * per label, to play several players from one browser. Works in browsers
 * (relative URLs) and in Node tests (`base`).
 */
import type { GameTicketSource } from '@hops-ops/distributed/game';

export type DevTicket = Readonly<{
	ticket: string;
	principal: string;
	name: string;
	guest: string;
	zone: string;
	expiresAt: number;
}>;

export type NetSimSettings = Readonly<{ latencyMs: number; jitterMs: number; loss: number }>;

export type DevApiOptions = Readonly<{
	/** HTTP origin of the demo server; '' = same origin (Vite proxy). */
	base?: string;
	/** Display name for new tickets (read on every request). */
	name?: () => string | undefined;
	/** Where the guest token lives between visits (default localStorage when present: one guest per browser). */
	storage?: Pick<Storage, 'getItem' | 'setItem'>;
	/** Guest label (default the page's `?player=`): one guest per label. */
	player?: string;
}>;

const GUEST_KEY = 'game-demos.guest';

function defaultStorage(): Pick<Storage, 'getItem' | 'setItem'> | undefined {
	try {
		return globalThis.localStorage ?? undefined;
	} catch {
		return undefined;
	}
}

function pagePlayer(): string | undefined {
	try {
		return new URLSearchParams(globalThis.location?.search ?? '').get('player') ?? undefined;
	} catch {
		return undefined;
	}
}

export class DevApi {
	readonly base: string;
	readonly #name: () => string | undefined;
	readonly #storage: Pick<Storage, 'getItem' | 'setItem'> | undefined;
	readonly #key: string;
	#guest: string | undefined;
	/** Last ticket response (principal and display name in effect). */
	last: DevTicket | undefined;

	constructor(options: DevApiOptions = {}) {
		this.base = options.base ?? '';
		this.#name = options.name ?? (() => undefined);
		this.#storage = options.storage ?? defaultStorage();
		const player = options.player ?? pagePlayer();
		this.#key = player === undefined || player === '' ? GUEST_KEY : `${GUEST_KEY}.${player}`;
		this.#guest = this.#read() ?? undefined;
	}

	/** The server-signed guest token (undefined before the first ticket). */
	get guest(): string | undefined {
		return this.#guest;
	}

	#read(): string | null | undefined {
		try {
			return this.#storage?.getItem(this.#key);
		} catch {
			return undefined;
		}
	}

	/** Mint a single-use admission ticket for `zone`. */
	async ticket(zone: string, role: 'player' | 'observer' = 'player'): Promise<DevTicket> {
		const response = await this.#json('/dev/ticket', {
			zone,
			role,
			name: this.#name(),
			guest: this.#guest
		});
		const ticket = response as DevTicket;
		this.#guest = ticket.guest;
		try {
			this.#storage?.setItem(this.#key, ticket.guest);
		} catch {
			// Storage full or blocked: this page keeps the guest anyway.
		}
		this.last = ticket;
		return ticket;
	}

	/** `GameClient` / `GameConnection` ticket source backed by `ticket()`. */
	ticketSource(): GameTicketSource {
		return async ({ zoneId, role }) => (await this.ticket(zoneId, role)).ticket;
	}

	async getNetSim(): Promise<NetSimSettings> {
		return (await this.#json('/dev/netsim')) as NetSimSettings;
	}

	/** Server-wide: affects every connected client (dev only). */
	async setNetSim(settings: NetSimSettings): Promise<NetSimSettings> {
		return (await this.#json('/dev/netsim', settings)) as NetSimSettings;
	}

	async #json(path: string, body?: unknown): Promise<unknown> {
		const response = await fetch(`${this.base}${path}`, {
			method: body === undefined ? 'GET' : 'POST',
			headers: body === undefined ? undefined : { 'content-type': 'application/json' },
			body: body === undefined ? undefined : JSON.stringify(body)
		});
		if (!response.ok) {
			throw new Error(`${path}: HTTP ${response.status} ${await response.text()}`);
		}
		return response.json();
	}
}

/**
 * WebSocket URL of a zone. `base` is an http(s) origin (Node tests); in the
 * browser the page's own origin is used (Vite proxies `/zones`).
 */
export function zoneUrl(zone: string, base?: string): string {
	const origin = base ?? globalThis.location.origin;
	return `${origin.replace(/^http/, 'ws')}/zones/${encodeURIComponent(zone)}`;
}
