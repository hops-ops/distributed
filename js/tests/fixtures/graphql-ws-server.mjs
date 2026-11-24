/**
 * A real in-process GraphQL endpoint for transport tests: HTTP POST and a
 * `graphql-transport-ws` WebSocket on one port, with no dependency. The test
 * scripts every answer; `until` waits for an event, never for time.
 */
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function frame(opcode, payload) {
	const length = payload.length;
	const header =
		length < 126
			? Buffer.from([0x80 | opcode, length])
			: length < 65_536
				? Buffer.from([0x80 | opcode, 126, length >> 8, length & 0xff])
				: Buffer.concat([
						Buffer.from([0x80 | opcode, 127]),
						(() => {
							const size = Buffer.alloc(8);
							size.writeBigUInt64BE(BigInt(length));
							return size;
						})()
					]);
	return Buffer.concat([header, payload]);
}

/** Server side of one accepted WebSocket. */
class ServerSocket {
	/** Every JSON message the client sent, in order. */
	received = [];
	closed = false;
	#socket;
	#buffer = Buffer.alloc(0);
	#notify;

	constructor(socket, notify) {
		this.#socket = socket;
		this.#notify = notify;
		socket.on('data', (chunk) => this.#read(chunk));
		socket.on('close', () => {
			this.closed = true;
			notify();
		});
		socket.on('error', () => undefined);
	}

	send(message) {
		this.#socket.write(frame(1, Buffer.from(JSON.stringify(message))));
	}

	/** Close with a status code, as a server that ends the connection does. */
	close(code, reason = '') {
		const payload = Buffer.alloc(2 + Buffer.byteLength(reason));
		payload.writeUInt16BE(code);
		payload.write(reason, 2);
		this.#socket.end(frame(8, payload));
	}

	/** Drop the TCP connection with no close frame, as a crash does. */
	destroy() {
		this.#socket.destroy();
	}

	/** The subscribe messages received so far. */
	get subscriptions() {
		return this.received.filter((message) => message.type === 'subscribe');
	}

	#read(chunk) {
		this.#buffer = Buffer.concat([this.#buffer, chunk]);
		for (;;) {
			if (this.#buffer.length < 2) return;
			const opcode = this.#buffer[0] & 0x0f;
			let length = this.#buffer[1] & 0x7f;
			let offset = 2;
			if (length === 126) {
				if (this.#buffer.length < 4) return;
				length = this.#buffer.readUInt16BE(2);
				offset = 4;
			} else if (length === 127) {
				if (this.#buffer.length < 10) return;
				length = Number(this.#buffer.readBigUInt64BE(2));
				offset = 10;
			}
			// Client frames are always masked.
			if (this.#buffer.length < offset + 4 + length) return;
			const mask = this.#buffer.subarray(offset, offset + 4);
			const payload = Buffer.from(
				this.#buffer.subarray(offset + 4, offset + 4 + length)
			);
			for (let index = 0; index < payload.length; index += 1) {
				payload[index] ^= mask[index % 4];
			}
			this.#buffer = this.#buffer.subarray(offset + 4 + length);
			if (opcode === 8) {
				this.#socket.end(frame(8, payload.subarray(0, 2)));
			} else if (opcode === 1) {
				this.received.push(JSON.parse(payload.toString('utf8')));
				this.#notify();
			}
		}
	}
}

/**
 * Start the endpoint. `http(body)` answers one POST (it may return a
 * promise the test resolves later); WebSockets are accepted and left to the
 * test, which reads `sockets[n].received` and calls `send`.
 */
export async function startGraphqlServer({ port = 0, http, notify = () => undefined }) {
	const sockets = [];
	const connections = new Set();
	const requests = [];
	const server = createServer((request, response) => {
		const chunks = [];
		request.on('data', (chunk) => chunks.push(chunk));
		request.on('end', async () => {
			const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
			requests.push(body);
			notify();
			let result;
			try {
				result = await http(body);
			} catch {
				response.destroy();
				return;
			}
			response.writeHead(200, { 'content-type': 'application/json' });
			response.end(JSON.stringify(result));
		});
	});
	server.on('connection', (connection) => {
		connections.add(connection);
		connection.on('close', () => connections.delete(connection));
	});
	server.on('upgrade', (request, socket) => {
		const accept = createHash('sha1')
			.update(request.headers['sec-websocket-key'] + WS_GUID)
			.digest('base64');
		socket.write(
			'HTTP/1.1 101 Switching Protocols\r\n' +
				'Upgrade: websocket\r\n' +
				'Connection: Upgrade\r\n' +
				`Sec-WebSocket-Accept: ${accept}\r\n` +
				`Sec-WebSocket-Protocol: ${request.headers['sec-websocket-protocol']}\r\n\r\n`
		);
		sockets.push(new ServerSocket(socket, notify));
		notify();
	});
	await new Promise((resolve, reject) => {
		server.once('error', reject);
		server.listen(port, '127.0.0.1', resolve);
	});
	return {
		port: server.address().port,
		/** Accepted WebSockets, oldest first. */
		sockets,
		/** Parsed bodies of the HTTP requests received. */
		requests,
		/** Stop like a killed process: every connection is dropped unannounced. */
		async stop() {
			const closed = new Promise((resolve) => server.close(resolve));
			for (const connection of connections) connection.destroy();
			await closed;
		}
	};
}

/**
 * Event-driven waiting: `wake` is called by every source of progress and
 * `until(predicate)` resolves at the first wake after which it holds.
 */
export function createWaiter() {
	const waiting = new Set();
	const wake = () => {
		for (const check of [...waiting]) check();
	};
	const until = (predicate) =>
		new Promise((resolve, reject) => {
			const check = () => {
				let held;
				try {
					held = predicate();
				} catch (error) {
					waiting.delete(check);
					reject(error);
					return;
				}
				if (!held) return;
				waiting.delete(check);
				resolve();
			};
			waiting.add(check);
			check();
		});
	return { wake, until };
}
