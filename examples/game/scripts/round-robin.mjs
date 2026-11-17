#!/usr/bin/env node
/**
 * A dev TCP load balancer in front of a local celld fleet (`run.sh mobpit
 * --fleet`): each new connection goes to the next node, round robin, the
 * way a production load balancer spreads clients over celld's public
 * listeners. It decides nothing about placement: celld places a room on
 * the node that first receives it and forwards every later socket to the
 * owner, wherever it entered.
 *
 *   node scripts/round-robin.mjs <listen port> <host:port>[,<host:port>…]
 *
 * Prints `LISTENING <port>` when ready.
 */
import { connect, createServer } from 'node:net';

const [port, list] = process.argv.slice(2);
const targets = (list ?? '').split(',').filter(Boolean).map((t) => {
	const [host, p] = t.replace(/^\w+:\/\//, '').split(':');
	return { host, port: Number(p) };
});
if (!port || targets.length === 0) {
	console.error('usage: round-robin.mjs <port> <host:port>[,…]');
	process.exit(2);
}
let next = 0;
createServer((client) => {
	const target = targets[next++ % targets.length];
	const upstream = connect(target.port, target.host);
	client.pipe(upstream).pipe(client);
	const close = () => {
		client.destroy();
		upstream.destroy();
	};
	client.on('error', close);
	upstream.on('error', close);
}).listen(Number(port), '127.0.0.1', () => console.log(`LISTENING ${port}`));
