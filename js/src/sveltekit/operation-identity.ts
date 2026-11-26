import type { ReplicaOperationArtifact } from '../replica/index.js';
import type { GraphqlVariables } from '../types.js';

/** Build the canonical identity shared by SSR scheduling and browser retention. */
export function boundaryOperationIdentity(
	artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>,
	variables: GraphqlVariables
): string {
	return JSON.stringify([
		artifact.protocol.version,
		artifact.protocol.schemaHash,
		artifact.protocol.surface,
		artifact.id,
		variables
	]);
}
