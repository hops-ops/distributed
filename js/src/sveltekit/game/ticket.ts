/**
 * `distributed_game` admission tickets minted on the web server
 * (`distributed_game/PROTOCOL.md` §8): a compact HS256 JWT over
 * `{sub, zone, exp, nonce, role, name?, ext?}`, verified by the zone server's
 * `TicketKey`. WebCrypto only, so it runs in Node 20+, Deno, Bun and Workers;
 * it must only ever run where the shared secret lives (a server route).
 */

/** Smallest accepted HS256 secret (bytes); the Rust demos use 32+ as well. */
export const MIN_GAME_TICKET_SECRET_BYTES = 32;
/** Longest accepted ticket lifetime (tickets are single use and short-lived). */
export const MAX_GAME_TICKET_TTL_SECONDS = 300;

const MAX_PRINCIPAL_BYTES = 256;
const MAX_ZONE_BYTES = 128;
const MAX_NAME_CHARS = 64;
/** Largest accepted `ext` object as compact JSON (the Rust `MAX_EXTENSION_BYTES`). */
export const MAX_GAME_TICKET_EXTENSION_BYTES = 2048;
const EXTENSION_NAME = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;

/**
 * Names an extension claim must never use (identity, rewards, economy):
 * the same list as the Rust `FactCommand` / `TicketExtension` derives
 * (`distributed_game/tests/fixtures/trusted_field_denylist.json`). Matching
 * ignores `_`, `-` and case.
 */
export const TRUSTED_FIELD_DENYLIST: readonly string[] = Object.freeze([
	'principal', 'user', 'user_id', 'owner', 'owner_id', 'account', 'account_id', 'subject', 'sub',
	'xp', 'experience', 'reward', 'rewards',
	'amount', 'coins', 'gold', 'currency', 'price', 'cost', 'balance', 'grant', 'payout', 'credit', 'debit'
]);
const normalizeName = (name: string) => name.replaceAll(/[_-]/g, '').toLowerCase();
const DENIED = new Set(TRUSTED_FIELD_DENYLIST.map(normalizeName));

/** JSON value of an extension claim. */
export type GameTicketClaimValue =
	| string
	| number
	| boolean
	| null
	| readonly GameTicketClaimValue[]
	| { readonly [key: string]: GameTicketClaimValue };

/**
 * Game-declared extension claims (match id, team, party…) signed under the
 * ticket's `ext`. The zone server admits them only when they match its
 * declared `#[derive(TicketExtension)]` schema.
 */
export type GameTicketExtensions = Readonly<Record<string, GameTicketClaimValue>>;
const CONTROL = /[\u0000-\u001f\u007f]/;
const CONTROLS = /[\u0000-\u001f\u007f]/g;
const encoder = new TextEncoder();

export type GameTicketRole = 'player' | 'observer';

export type GameTicketSecret = string | Uint8Array;

export type MintGameTicketOptions = Readonly<{
	/** Shared HS256 secret (the zone server's `TicketKey`); at least 32 bytes. */
	secret: GameTicketSecret;
	/** Verified principal (`sub`): the only identity the zone ever sees. */
	principal: string;
	/** Zone the ticket admits to; must equal the route zone. */
	zone: string;
	/** Default `player`. */
	role?: GameTicketRole;
	/** Display name vouched for by this issuer (never identity). */
	name?: string;
	/**
	 * Extension claims decided **on the server** (never from the browser).
	 * Identity-, reward- and economy-looking names are refused.
	 */
	extensions?: GameTicketExtensions;
	/** Lifetime in seconds (1..=300, default 60). */
	ttlSeconds?: number;
	/** Clock in Unix milliseconds (tests). Default `Date.now()`. */
	now?: number;
	/** Single-use replay key (tests). Default `crypto.randomUUID()`. */
	nonce?: string;
}>;

export type MintedGameTicket = Readonly<{
	ticket: string;
	zone: string;
	role: GameTicketRole;
	/** Unix milliseconds. */
	expiresAt: number;
	nonce: string;
}>;

function secretBytes(secret: GameTicketSecret): Uint8Array {
	const bytes = typeof secret === 'string' ? encoder.encode(secret) : secret;
	if (!(bytes instanceof Uint8Array) || bytes.byteLength < MIN_GAME_TICKET_SECRET_BYTES) {
		throw new TypeError(
			`game ticket secret must be at least ${MIN_GAME_TICKET_SECRET_BYTES} bytes`
		);
	}
	return bytes;
}

function boundedText(value: unknown, label: string, maxBytes: number): string {
	if (
		typeof value !== 'string' ||
		value.length === 0 ||
		value !== value.trim() ||
		CONTROL.test(value) ||
		encoder.encode(value).byteLength > maxBytes
	) {
		throw new TypeError(`game ticket ${label} must be 1..=${maxBytes} bytes of trimmed text`);
	}
	return value;
}

function plainJson(value: unknown, depth: number): boolean {
	if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
	if (typeof value === 'number') return Number.isFinite(value);
	if (depth > 8 || typeof value !== 'object') return false;
	if (Array.isArray(value)) return value.every((item) => plainJson(item, depth + 1));
	const proto = Object.getPrototypeOf(value);
	if (proto !== Object.prototype && proto !== null) return false;
	return Object.values(value).every((item) => plainJson(item, depth + 1));
}

/**
 * Validate extension claims: a plain object of identifier names (not on
 * the deny list) to JSON values, at most `MAX_GAME_TICKET_EXTENSION_BYTES`.
 * Returns undefined for none. Throws `TypeError`.
 */
export function checkGameTicketExtensions(value: unknown): GameTicketExtensions | undefined {
	if (value === undefined || value === null) return undefined;
	if (typeof value !== 'object' || Array.isArray(value) || !plainJson(value, 0)) {
		throw new TypeError('game ticket extensions must be a plain JSON object');
	}
	const names = Object.keys(value);
	if (names.length === 0) return undefined;
	for (const name of names) {
		if (DENIED.has(normalizeName(name))) {
			throw new TypeError(
				`game ticket extension \`${name}\` looks like identity, a reward or an economy value; the zone binds identity from \`sub\` and handlers decide amounts`
			);
		}
		if (!EXTENSION_NAME.test(name)) {
			throw new TypeError(`game ticket extension name \`${name}\` must be an identifier`);
		}
	}
	const json = JSON.stringify(value);
	if (encoder.encode(json).byteLength > MAX_GAME_TICKET_EXTENSION_BYTES) {
		throw new TypeError(`game ticket extensions exceed ${MAX_GAME_TICKET_EXTENSION_BYTES} bytes`);
	}
	return JSON.parse(json) as GameTicketExtensions;
}

/** Unpadded base64url (RFC 7515 §2). */
export function base64url(bytes: Uint8Array): string {
	let binary = '';
	for (const byte of bytes) binary += String.fromCharCode(byte);
	return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '');
}

const keys = new WeakMap<Uint8Array, Promise<CryptoKey>>();
const stringKeys = new Map<string, Promise<CryptoKey>>();

function hmacKey(secret: GameTicketSecret): Promise<CryptoKey> {
	const bytes = secretBytes(secret);
	const cached = typeof secret === 'string' ? stringKeys.get(secret) : keys.get(bytes);
	if (cached !== undefined) return cached;
	const key = globalThis.crypto.subtle.importKey(
		'raw',
		bytes as Uint8Array<ArrayBuffer>,
		{ name: 'HMAC', hash: 'SHA-256' },
		false,
		['sign']
	);
	if (typeof secret === 'string') {
		// Bounded: a process normally holds one or two secrets (rotation).
		if (stringKeys.size >= 8) stringKeys.clear();
		stringKeys.set(secret, key);
	} else {
		keys.set(bytes, key);
	}
	return key;
}

/** Sign an arbitrary claims object as a compact HS256 JWT. */
export async function signHs256(
	secret: GameTicketSecret,
	claims: Readonly<Record<string, unknown>>
): Promise<string> {
	const header = base64url(encoder.encode('{"alg":"HS256","typ":"JWT"}'));
	const payload = base64url(encoder.encode(JSON.stringify(claims)));
	const signingInput = `${header}.${payload}`;
	const signature = await globalThis.crypto.subtle.sign(
		'HMAC',
		await hmacKey(secret),
		encoder.encode(signingInput)
	);
	return `${signingInput}.${base64url(new Uint8Array(signature))}`;
}

/**
 * Mint a single-use admission ticket the zone server's `TicketKey` accepts.
 * Throws `TypeError` on a short secret or malformed claim values.
 */
export async function mintGameTicket(options: MintGameTicketOptions): Promise<MintedGameTicket> {
	const principal = boundedText(options.principal, 'principal', MAX_PRINCIPAL_BYTES);
	const zone = boundedText(options.zone, 'zone', MAX_ZONE_BYTES);
	const role = options.role ?? 'player';
	if (role !== 'player' && role !== 'observer') {
		throw new TypeError('game ticket role must be player or observer');
	}
	const ttl = options.ttlSeconds ?? 60;
	if (!Number.isSafeInteger(ttl) || ttl < 1 || ttl > MAX_GAME_TICKET_TTL_SECONDS) {
		throw new TypeError(`game ticket ttlSeconds must be 1..=${MAX_GAME_TICKET_TTL_SECONDS}`);
	}
	const now = options.now ?? Date.now();
	if (!Number.isFinite(now) || now < 0) throw new TypeError('game ticket clock is invalid');
	const nonce = boundedText(options.nonce ?? globalThis.crypto.randomUUID(), 'nonce', 128);
	let name: string | undefined;
	if (options.name !== undefined) {
		const trimmed = [...options.name.replace(CONTROLS, '').trim()].slice(0, MAX_NAME_CHARS).join('');
		name = trimmed.length === 0 ? undefined : trimmed;
	}
	const ext = checkGameTicketExtensions(options.extensions);
	const exp = Math.floor(now / 1000) + ttl;
	const ticket = await signHs256(options.secret, {
		sub: principal,
		zone,
		exp,
		nonce,
		role,
		...(name === undefined ? {} : { name }),
		...(ext === undefined ? {} : { ext })
	});
	return Object.freeze({ ticket, zone, role, expiresAt: exp * 1000, nonce });
}
