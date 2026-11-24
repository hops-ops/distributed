import type { GraphqlVariables } from '../types.js';
import { intersects } from '../lib/intersects.js';
import type {
	ReplicaObjectSelection,
	ReplicaOperationArtifact,
	ReplicaRevalidationPlan
} from './types.js';

type RevalidationInventory = {
	readonly dependencies: ReadonlySet<string>;
	readonly models: ReadonlySet<string>;
	readonly relationships: ReadonlySet<string>;
	readonly global: boolean;
};

/**
 * Compile one validated command inventory into a reusable operation matcher.
 *
 * Adapters only forward generated plans; the replica owns knowledge of active
 * operation artifacts and therefore remains the framework-neutral coordinator.
 */
export function createReplicaRevalidationMatcher(
	plan: ReplicaRevalidationPlan
): (
	artifact: ReplicaOperationArtifact<unknown, GraphqlVariables>
) => boolean {
	const inventory = revalidationInventory(plan);
	return (artifact) =>
		inventory.global ||
		artifact.roots.some(
			(root) =>
				intersects(root.dependencies, inventory.dependencies) ||
				selectionMatches(root.selection, undefined, inventory)
		);
}

function selectionMatches(
	selection: ReplicaObjectSelection,
	parentModel: string | undefined,
	inventory: RevalidationInventory
): boolean {
	const model =
		selection.storage.kind === 'normalized'
			? selection.storage.model
			: parentModel;
	if (
		selection.storage.kind === 'normalized' &&
		inventory.models.has(selection.storage.model)
	) {
		return true;
	}
	for (const member of selection.members) {
		if (member.kind !== 'branch') continue;
		if (intersects(member.dependencies, inventory.dependencies)) return true;
		if (
			member.semantic === 'relationship' &&
			model !== undefined &&
			inventory.relationships.has(
				relationshipKey(model, member.field, member.relationship.targetModel)
			)
		) {
			return true;
		}
		if (selectionMatches(member.selection, model, inventory)) return true;
	}
	return false;
}

function revalidationInventory(
	value: ReplicaRevalidationPlan
): RevalidationInventory {
	if (
		value === null ||
		typeof value !== 'object' ||
		!Array.isArray(value.dependencies) ||
		!Array.isArray(value.models) ||
		!Array.isArray(value.relationships)
	) {
		invalidPlan();
	}
	const dependencies = stringSet(value.dependencies, 'dependencies');
	const models = stringSet(value.models, 'models');
	const relationships = new Set<string>();
	for (const [index, relationship] of value.relationships.entries()) {
		if (
			relationship === null ||
			typeof relationship !== 'object' ||
			!validName(relationship.sourceModel) ||
			!validName(relationship.field) ||
			!validName(relationship.targetModel)
		) {
			invalidPlan(`relationships[${index}]`);
		}
		relationships.add(
			relationshipKey(
				relationship.sourceModel,
				relationship.field,
				relationship.targetModel
			)
		);
	}
	return Object.freeze({
		dependencies,
		models,
		relationships,
		global:
			dependencies.size === 0 &&
			models.size === 0 &&
			relationships.size === 0
	});
}

function stringSet(values: readonly string[], path: string): ReadonlySet<string> {
	const result = new Set<string>();
	for (const [index, value] of values.entries()) {
		if (!validName(value)) invalidPlan(`${path}[${index}]`);
		result.add(value);
	}
	return result;
}

function validName(value: unknown): value is string {
	return typeof value === 'string' && value.length > 0;
}

function relationshipKey(
	sourceModel: string,
	field: string,
	targetModel: string
): string {
	return JSON.stringify([sourceModel, field, targetModel]);
}

function invalidPlan(path?: string): never {
	throw new TypeError(
		`replica revalidation plan${path === undefined ? '' : ` ${path}`} is invalid`
	);
}
