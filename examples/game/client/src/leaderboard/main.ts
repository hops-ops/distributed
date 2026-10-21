/**
 * Leaderboard page: renders the services' top-10 snapshots (a projection of
 * the Crawler cells, polled), reads your rank and neighbours on every
 * change, triggers a rebuild from the cells' event history, and observes
 * the entrance room (no avatar) for a live mini map.
 */
import { ARENA_FLOOR } from '../generated/arena/constants.ts';
import {
	createMobpitClient,
	isMob,
	isPillar,
	isPlayer,
	mobFields,
	pillarFields,
	playerFields
} from '../generated/mobpit/index.ts';
import {
	createBoardWatcher,
	DEFAULT_ZONE,
	MobPitApi,
	roomUrl,
	TIERS,
	type BoardPosition,
	type BoardRow,
	type BoardSnapshot
} from '../mobpit/client.ts';
import { DevApi } from '../shared/dev-api.ts';

/** The Arena floor, generated from the simulation's constants (`game_sim::arena::arena_floor`). */
const { half: HALF } = ARENA_FLOOR;

const params = new URLSearchParams(location.search);
const zone = params.get('zone') ?? DEFAULT_ZONE;
const api = new DevApi();
const pitApi = new MobPitApi();
const watcher = createBoardWatcher(pitApi);
const { client } = createMobpitClient({
	url: (room) => roomUrl(room),
	ticket: api.ticketSource(),
	pingIntervalMs: 1000,
	prediction: false
});

const $ = (k: string) => document.querySelector(`[data-k="${k}"]`) as HTMLElement;
const status = $('status');
let pushes = 0;

function table(el: HTMLElement, rows: readonly BoardRow[], unit: string, me: string | undefined): void {
	if (rows.length === 0) {
		el.innerHTML = '<tr><td>No kills yet — go to the pit.</td></tr>';
		return;
	}
	el.replaceChildren(
		...rows.map((row) => {
			const tr = document.createElement('tr');
			if (row.member === me) tr.className = 'me';
			for (const [text, cls] of [
				[`#${row.rank}`, 'num'],
				[row.name, ''],
				[`${row.score} ${unit}`, 'num']
			] as const) {
				const td = document.createElement('td');
				td.textContent = text;
				if (cls !== '') td.className = cls;
				tr.append(td);
			}
			return tr;
		})
	);
}

function position(label: string, unit: string, p: BoardPosition | undefined, me: string): string {
	if (p === undefined || p.rank === null) return `<div class="pos">${label}: not ranked yet</div>`;
	const around = p.around
		.map((r) => `${r.member === me ? '<b>' : ''}#${r.rank} ${escape(r.name)} ${r.score}${r.member === me ? '</b>' : ''}`)
		.join('<br>');
	return `<div class="pos">${label}: <b>#${p.rank}</b> (${p.score} ${unit})<br><small>${around}</small></div>`;
}

function escape(text: string): string {
	return text.replace(/[&<>"]/g, (c) => `&#${c.charCodeAt(0)};`);
}

async function render(board: BoardSnapshot): Promise<void> {
	pushes += 1;
	const me = api.last?.principal;
	table($('kills'), board.kills, 'kills', me);
	table($('xp'), board.xp, 'XP', me);
	const bg = board.background;
	status.textContent =
		`update #${pushes} · version ${board.version} · store ${board.backend} · ${board.members} ranked` +
		` · background updates: ${bg.applied} applied, ${bg.dropped} dropped, ${bg.failed} failed`;
	status.classList.add('flash');
	setTimeout(() => status.classList.remove('flash'), 120);
	if (me === undefined) return;
	try {
		const { me: mine } = await pitApi.leaderboard(me);
		$('me').innerHTML =
			`<div class="pos">${escape(api.last?.name ?? me)}</div>` +
			position('Kills', 'kills', mine?.kills, me) +
			position('XP', 'XP', mine?.xp, me);
	} catch (error) {
		$('me').textContent = `rank lookup failed: ${String(error)}`;
	}
}

watcher.on((board) => void render(board));

$('rebuild').addEventListener('click', async () => {
	const button = $('rebuild') as HTMLButtonElement;
	button.disabled = true;
	$('report').textContent = 'rebuilding…';
	try {
		const r = await pitApi.rebuild();
		$('report').textContent =
			`${r.events} kill events from ${r.crawlers} cells → ${r.applied} board changes in ${r.tookMs.toFixed(1)} ms (${r.backend})\n` +
			`top 10 identical to before: ${r.identicalTop ? 'yes ✓' : 'no (live board had drifted — now healed)'}\n` +
			`every cell's kills/XP equal its board scores: ${r.matchesCells ? 'yes ✓' : `NO: ${JSON.stringify(r.mismatches)}`}`;
	} catch (error) {
		$('report').textContent = `rebuild failed: ${String(error)}`;
	} finally {
		button.disabled = false;
	}
});

// ---- live minimap of the observed zone ----
const map = $('map') as HTMLCanvasElement;
function drawMap(): void {
	const ctx = map.getContext('2d');
	if (ctx !== null) {
		const s = map.width / (HALF * 2);
		const px = (v: number) => (v + HALF) * s;
		ctx.clearRect(0, 0, map.width, map.height);
		for (const e of client.render()) {
			if (isPillar(e)) {
				const { x, y, r } = pillarFields(e);
				ctx.fillStyle = '#5d5566';
				ctx.beginPath();
				ctx.arc(px(x), map.height - px(y), r * s, 0, Math.PI * 2);
				ctx.fill();
			} else if (isMob(e)) {
				const { x, y, tier } = mobFields(e);
				ctx.fillStyle = TIERS[tier]?.color ?? '#9ad16b';
				ctx.fillRect(px(x) - 3, map.height - px(y) - 3, 6, 6);
			} else if (isPlayer(e)) {
				const { x, y, dead } = playerFields(e);
				ctx.fillStyle = dead ? '#555' : '#f2a65a';
				ctx.beginPath();
				ctx.arc(px(x), map.height - px(y), 4, 0, Math.PI * 2);
				ctx.fill();
			}
		}
	}
	requestAnimationFrame(drawMap);
}
requestAnimationFrame(drawMap);

declare global {
	interface Window {
		leaderboard?: unknown;
	}
}
window.leaderboard = { client, api, pitApi, watcher };

client.observe(zone).catch((error: unknown) => (status.textContent = `failed: ${String(error)}`));
