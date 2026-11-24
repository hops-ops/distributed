import {
	isDistributedTrustedPresetCodec
} from '../../../protocol.js';
import type { ReplicaTrustedPresetDescriptor } from '../../commands/types.js';
import {
	SHA256
} from '../constants.js';
import type {
	AnyCommandArtifact,
	CommandEntry,
	ReplicaCommandStatusArtifact,
	ReplicaCommandSurfaceContract
} from '../types.js';
import { compareCodeUnits } from '../../../lib/compare-code-units.js';
import {
	commandNamespaceCollision,
	commandPathSegments
} from './binding.js';
import {
	cloneSurface,
	sameSurface
} from './util.js';
export function normalizeInventory<TEntries extends Readonly<Record<string, CommandEntry>>>(
	entries: TEntries
): readonly { readonly key: string; readonly artifact: AnyCommandArtifact }[] {
	if (entries === null || typeof entries !== 'object' || Array.isArray(entries)) {
		throw new TypeError('replica command inventory must be an object');
	}
	const names = new Set<string>();
	const inventory = Object.entries(entries).map(([key, entry]) => {
		commandPathSegments(key);
		const artifact = 'artifact' in entry ? entry.artifact : entry;
		if (names.has(artifact.name)) {
			throw new TypeError(`duplicate replica command artifact ${artifact.name}`);
		}
		names.add(artifact.name);
		return Object.freeze({ key, artifact });
	});
	const sortedPaths = inventory.map(({ key }) => key).sort(compareCodeUnits);
	for (let index = 1; index < sortedPaths.length; index += 1) {
		const previous = sortedPaths[index - 1]!;
		const current = sortedPaths[index]!;
		if (current.startsWith(`${previous}.`)) {
			commandNamespaceCollision(current);
		}
	}
	return Object.freeze(inventory);
}

export function commandSurfaceContract(
	artifacts: readonly AnyCommandArtifact[],
	surfacePresets: readonly ReplicaTrustedPresetDescriptor[] | undefined
): ReplicaCommandSurfaceContract {
	if (artifacts.length === 0) {
		throw new TypeError('replica command inventory must not be empty');
	}
	const first = artifacts[0]!;
	const protocol = first.protocol;
	if (protocol.surface === undefined) {
		throw new TypeError('generated command protocol requires a client surface');
	}
	const trustedPresets = normalizePresetDescriptors(
		protocol.trustedPresets,
		'artifact.protocol.trustedPresets'
	);
	const commandPresets = new Map<string, ReplicaTrustedPresetDescriptor>();
	for (const artifact of artifacts) {
		if (
			artifact.protocol.version !== 1 ||
			artifact.protocol.schemaHash !== protocol.schemaHash ||
			artifact.protocol.protocolHash !== protocol.protocolHash ||
			!sameSurface(artifact.protocol.surface, protocol.surface) ||
			!samePresetDescriptors(
				normalizePresetDescriptors(
					artifact.protocol.trustedPresets,
					'artifact.protocol.trustedPresets'
				),
				trustedPresets
			)
		) {
			throw new TypeError(
				'replica command inventory spans incompatible client surfaces'
			);
		}
		for (const descriptor of artifact.trustedPresets ?? []) {
			const previous = commandPresets.get(descriptor.name);
			if (previous !== undefined && previous.codec !== descriptor.codec) {
				throw new TypeError(
					`trusted preset ${descriptor.name} has conflicting codecs`
				);
			}
			commandPresets.set(
				descriptor.name,
				Object.freeze({ name: descriptor.name, codec: descriptor.codec })
			);
		}
	}
	if (
		surfacePresets !== undefined &&
		!samePresetDescriptors(
			normalizePresetDescriptors(
				surfacePresets,
				'status.protocol.trustedPresets'
			),
			trustedPresets
		)
	) {
		throw new TypeError(
			'generated command status inventory does not match its client surface'
		);
	}
	const surfaceByName = new Map(
		trustedPresets.map((descriptor) => [descriptor.name, descriptor] as const)
	);
	for (const descriptor of commandPresets.values()) {
		if (surfaceByName.get(descriptor.name)?.codec !== descriptor.codec) {
			throw new TypeError(
				`command trusted preset ${descriptor.name} is absent from the client surface`
			);
		}
	}
	return Object.freeze({
		protocolVersion: 1,
		schemaHash: protocol.schemaHash,
		protocolHash: protocol.protocolHash,
		surface: cloneSurface(protocol.surface),
		trustedPresets
	});
}

export function commandStatusArtifact(
	value: ReplicaCommandStatusArtifact,
	contract: ReplicaCommandSurfaceContract
): ReplicaCommandStatusArtifact {
	if (
		value === null ||
		typeof value !== 'object' ||
		typeof value.name !== 'string' ||
		value.name.trim().length === 0 ||
		typeof value.document !== 'string' ||
		value.document.trim().length === 0 ||
		typeof value.operationHash !== 'string' ||
		!SHA256.test(value.operationHash) ||
		value.protocol === null ||
		typeof value.protocol !== 'object' ||
		value.protocol.version !== 1 ||
		value.protocol.operation !== value.operationHash ||
		value.protocol.schemaHash !== contract.schemaHash ||
		value.protocol.protocolHash !== contract.protocolHash ||
		!sameSurface(value.protocol.surface, contract.surface)
	) {
		throw new TypeError('generated command status artifact is invalid');
	}
	const trustedPresets = normalizePresetDescriptors(
		value.protocol.trustedPresets,
		'status.protocol.trustedPresets'
	);
	if (!samePresetDescriptors(trustedPresets, contract.trustedPresets)) {
		throw new TypeError(
			'generated command status inventory does not match its client surface'
		);
	}
	return Object.freeze({
		name: value.name,
		document: value.document,
		operationHash: value.operationHash,
		protocol: Object.freeze({
			version: 1,
			schemaHash: contract.schemaHash,
			protocolHash: contract.protocolHash,
			surface: cloneSurface(contract.surface),
			operation: value.operationHash,
			trustedPresets
		})
	});
}

export function normalizePresetDescriptors(
	value: readonly ReplicaTrustedPresetDescriptor[],
	path: string
): readonly ReplicaTrustedPresetDescriptor[] {
	if (!Array.isArray(value)) {
		throw new TypeError(`${path} must be an array`);
	}
	const names = new Set<string>();
	const result = value.map((descriptor, index) => {
		if (
			descriptor === null ||
			typeof descriptor !== 'object' ||
			typeof descriptor.name !== 'string' ||
			descriptor.name.length === 0 ||
			descriptor.name.length > 128 ||
			descriptor.name.trim() !== descriptor.name ||
			/[\u0000-\u001f\u007f-\u009f]/.test(descriptor.name) ||
			names.has(descriptor.name) ||
			!isDistributedTrustedPresetCodec(descriptor.codec)
		) {
			throw new TypeError(`${path}[${index}] is invalid`);
		}
		names.add(descriptor.name);
		return Object.freeze({
			name: descriptor.name,
			codec: descriptor.codec
		});
	});
	return Object.freeze(
		result.sort(({ name: left }, { name: right }) =>
			compareCodeUnits(left, right)
		)
	);
}

export function samePresetDescriptors(
	left: readonly ReplicaTrustedPresetDescriptor[],
	right: readonly ReplicaTrustedPresetDescriptor[]
): boolean {
	return (
		left.length === right.length &&
		left.every(
			(descriptor, index) =>
				descriptor.name === right[index]?.name &&
				descriptor.codec === right[index]?.codec
		)
	);
}

