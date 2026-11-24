/** Replica identity/keys and variable codec; implementation in ./identity/. */
export {
	canonicalVariables,
	canonicalizeOperationVariables,
	cloneJsonObject,
	cloneJsonValue,
	coverageFromArtifact,
	recordKeyMatchesModel,
	replicaIndexKey,
	replicaRecordKey,
	resolveArguments,
	resolveReplicaArgumentValue
} from './identity/index.js';
