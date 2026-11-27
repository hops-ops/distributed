/**
 * Browser-safe SvelteKit integration for `distributed_game` zones: the
 * Svelte 5 runes adapter for the UI around a game (`useGame`,
 * `createGameState`) and the ticket source for a `createGameTicketHandler`
 * route (`gameTicketSource`). Server-side minting lives in
 * `@hops-ops/distributed/sveltekit/game/server`.
 */
export {
	GameState,
	createGameState,
	gameTicketSource,
	useGame,
	type GameEventList,
	type GameLatest,
	type GameStateOptions,
	type GameStateSource,
	type GameStatus,
	type GameTicketSourceOptions,
	type UseGameOptions
} from './state.js';
