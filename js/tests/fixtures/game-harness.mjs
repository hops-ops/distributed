/**
 * In-memory transport and deterministic scheduler for game client tests.
 * The fake server side speaks the real binary protocol through the SDK codec.
 */
import {
	GAME_PROTOCOL_VERSION,
	GameFeature,
	decodeClientMessage,
	defineGameSchema,
	encodeServerMessage
} from '@hops-ops/distributed/game';

export const ARENA_SCHEMA = defineGameSchema([
	{
		id: 1,
		name: 'avatar',
		fields: [
			{ name: 'x', type: 'quant_f32', min: -1000, max: 1000, bits: 24 },
			{ name: 'y', type: 'quant_f32', min: -1000, max: 1000, bits: 24 },
			{ name: 'yaw', type: 'angle', bits: 16 },
			{ name: 'hp', type: 'u16' },
			{ name: 'name', type: 'short_str' }
		]
	},
	{
		id: 2,
		name: 'coin',
		fields: [
			{ name: 'x', type: 'quant_f32', min: -1000, max: 1000, bits: 24 },
			{ name: 'y', type: 'quant_f32', min: -1000, max: 1000, bits: 24 }
		]
	}
]);

/** Manual clock + timers. */
export class FakeScheduler {
	time = 0;
	#timers = new Map();
	#next = 1;

	now = () => this.time;

	setTimeout = (callback, ms) => {
		const id = this.#next++;
		this.#timers.set(id, { at: this.time + Math.max(0, ms), callback, id });
		return id;
	};

	clearTimeout = (id) => {
		this.#timers.delete(id);
	};

	/** Advance time, firing due timers in order. */
	async advance(ms) {
		const end = this.time + ms;
		for (;;) {
			await flush();
			let due;
			for (const timer of this.#timers.values()) {
				if (timer.at <= end && (due === undefined || timer.at < due.at || (timer.at === due.at && timer.id < due.id))) {
					due = timer;
				}
			}
			if (due === undefined) break;
			this.#timers.delete(due.id);
			this.time = due.at;
			due.callback();
		}
		this.time = end;
		await flush();
	}
}

export async function flush() {
	for (let i = 0; i < 5; i += 1) await new Promise((resolve) => setImmediate(resolve));
}

/** Server-side end of one fake socket. */
export class ServerConn {
	inbox = [];
	closed = false;
	closeCode;
	#client;
	#kinds = new Map();
	#schema;

	constructor(network, url, client) {
		this.network = network;
		this.url = url;
		this.zoneId = url.split('/').pop();
		this.#client = client;
	}

	get lastHello() {
		return this.inbox.find((m) => m.type === 'Hello');
	}

	inputs() {
		return this.inbox.filter((m) => m.type === 'Input');
	}

	/** Encode and deliver a server message (tracks entity kinds for updates). */
	send(message) {
		if (this.closed) return;
		if (message.type === 'Welcome') {
			this.#schema = message.schema;
			this.#kinds.clear();
		}
		const bytes = encodeServerMessage(message, {
			schema: this.#schema,
			kindOf: (id) => this.#kinds.get(id)
		});
		if (message.type === 'Delta') {
			for (const id of message.removes) this.#kinds.delete(id);
			for (const c of message.creates) this.#kinds.set(c.id, c.kind);
		}
		this.#client.onmessage?.({ data: bytes.buffer });
	}

	/** Server closes the socket. */
	close(code = 1000, reason = '') {
		if (this.closed) return;
		this.closed = true;
		this.closeCode = code;
		this.#client.readyState = 3;
		queueMicrotask(() => this.#client.onclose?.({ code, reason }));
	}

	receive(bytes) {
		const message = decodeClientMessage(new Uint8Array(bytes));
		this.inbox.push(message);
		this.network.onMessage?.(this, message);
	}
}

class FakeClientSocket {
	binaryType = 'blob';
	readyState = 0;
	onopen = null;
	onmessage = null;
	onclose = null;
	onerror = null;

	constructor(network, url) {
		this.server = new ServerConn(network, url, this);
		queueMicrotask(() => {
			if (network.refuse(url)) {
				this.server.closed = true;
				this.readyState = 3;
				this.onclose?.({ code: 1006, reason: 'refused' });
				return;
			}
			this.readyState = 1;
			network.onConnect?.(this.server);
			this.onopen?.({});
		});
	}

	send(data) {
		if (this.readyState !== 1 || this.server.closed) return;
		this.server.receive(data.slice().buffer);
	}

	close(code = 1000, reason = '') {
		if (this.readyState === 3) return;
		this.readyState = 3;
		this.server.closed = true;
		queueMicrotask(() => this.onclose?.({ code, reason }));
	}
}

/**
 * Fake network. `onConnect(serverConn)` / `onMessage(serverConn, message)`
 * script the server; `down` refuses new connections.
 */
export class FakeNetwork {
	connections = [];
	down = false;
	onConnect;
	onMessage;

	factory = (url) => {
		const socket = new FakeClientSocket(this, url);
		this.connections.push(socket.server);
		return socket;
	};

	refuse() {
		return this.down;
	}

	open(zoneId) {
		return this.connections.filter((c) => !c.closed && c.zoneId === zoneId);
	}

	last(zoneId) {
		const list = this.connections.filter((c) => zoneId === undefined || c.zoneId === zoneId);
		return list[list.length - 1];
	}
}

/** A Welcome for the arena schema (protocol 2, every runtime feature). */
export function welcome(overrides = {}) {
	return {
		type: 'Welcome',
		protocolVersion: GAME_PROTOCOL_VERSION,
		features: GameFeature.FOCUS,
		sessionId: 's1',
		zoneId: 'z1',
		role: 'player',
		tickRate: 30,
		patchRate: 20,
		serverTick: 100,
		serverTime: 5000,
		reconnectToken: 'rt-1',
		contentVersion: '',
		schema: ARENA_SCHEMA,
		...overrides
	};
}

export function delta(overrides = {}) {
	return {
		type: 'Delta',
		serverTick: 100,
		ackSeq: 0,
		removes: [],
		creates: [],
		updates: [],
		...overrides
	};
}

export function avatar(id, x, y = 0, extra = {}) {
	return { id, kind: 1, fields: [x, y, extra.yaw ?? 0, extra.hp ?? 100, extra.name ?? 'p'] };
}
