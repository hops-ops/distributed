/**
 * Demos send low-rate events as UTF-8 JSON (`common::send_json_event` on the
 * server). Subscribe by type id; returns the unsubscribe function.
 */
import type { GameClient } from '@hops-ops/distributed/game';

const decoder = new TextDecoder();

export function onJsonEvent<T>(
	client: Pick<GameClient<unknown>, 'on'>,
	typeId: number,
	handler: (value: T, zoneId: string) => void
): () => void {
	return client.on('event', (zoneId, event) => {
		if (event.typeId !== typeId) return;
		handler(JSON.parse(decoder.decode(event.payload)) as T, zoneId);
	});
}
