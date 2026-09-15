import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

export const artifact = JSON.parse(readFileSync(new URL(
	'../../../distributed_cli/tests/fixtures/unique-key-bridge-operation.json', import.meta.url
), 'utf8'));
export const root = artifact.roots[0];
export const owner = root.selection.members.find(member => member.field === 'owner');

export function frame(position, targetId, targetTitle) {
	const records = [];
	function wire(selection, values, path) {
		records.push({ path, model: selection.storage.model,
			scopeToken: `record:${values.id}`, incarnation: '1', revision: position, tombstone: false });
		return Object.fromEntries(selection.members.map(member => {
			if (member.kind === 'branch') return [member.responseKey, targetId === null ? null :
				wire(member.selection, { id: targetId, title: targetTitle, tenantId: 'tenant-a',
					__typename: 'todo' }, [...path, member.responseKey])];
			assert.ok(Object.hasOwn(values, member.field), member.field);
			return [member.responseKey, values[member.field]];
		}));
	}
	const row = wire(root.selection, { id: 'source-id', title: 'source',
		ownerTitle: targetTitle, tenantId: 'tenant-a', __typename: 'todo' }, ['todos', '0']);
	return { data: { todos: [row] }, extensions: { distributed: {
		protocolVersion: artifact.protocol.version, schemaHash: artifact.protocol.schemaHash,
		authorizationGeneration: 'auth-1', cacheScope: 'unique-key-cache',
		operation: position === '1' ? artifact.id : artifact.live.id,
		...(position === '1' ? {} : { live: { mode: "resumable", reset: false, cursors: [
			{ projection: 'unique-key-projector', position, token: `resume:${position}` }
		] } }),
		snapshot: { scopeToken: 'unique-key-snapshot', recordsComplete: true, indexesComparable: true,
			records, indexes: [{ projection: 'unique-key-projector', scopeToken: 'unique-key-index', position,
				resume: { projection: 'unique-key-projector', position, token: `resume:${position}` } }], observations: [] }
	} } };
}
