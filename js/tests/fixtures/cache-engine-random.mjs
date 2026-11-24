/**
 * A seeded random walk over one cache engine: base batches (some rolled
 * back), optimistic layers created, replaced, accepted, confirmed and
 * rejected, derived indexes, retention and collection.
 *
 * `randomWalk` returns what every reader and watcher saw after each step, so
 * a caller can check it against a recomputation or against another engine.
 */

export const RECORDS = Object.freeze(
	Array.from({ length: 8 }, (_, index) => `Item:${index}`)
);

function metadata(parent) {
	return {
		...(parent === undefined ? {} : { parent }),
		field: parent === undefined ? 'items' : 'children',
		arguments: {},
		coverage: { kind: 'complete' },
		dependencies: ['items']
	};
}

/** mulberry32 */
function random(seed) {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let t = state;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

const copy = (value) => (value === undefined ? undefined : structuredClone(value));

/**
 * @param engineModule `{ createCacheEngine, cacheIndexKey }`
 * @param options.confirmedIndex how the derived-index reconciler reads one
 *   confirmed index from what the engine hands it
 * @param options.onStep called after every step with the walk's live state
 */
export function randomWalk(engineModule, seed, steps, options = {}) {
	const { createCacheEngine, cacheIndexKey } = engineModule;
	const confirmedIndex =
		options.confirmedIndex ?? ((confirmed, key) => confirmed.index(key));
	const next = random(seed);
	const int = (limit) => Math.floor(next() * limit);
	const pick = (values) => values[int(values.length)];
	const chance = (probability) => next() < probability;
	const subset = (values) => values.filter(() => chance(0.4));

	const indexes = [undefined, RECORDS[0], RECORDS[1]].map((parent) => ({
		key: cacheIndexKey({
			...(parent === undefined ? {} : { parent }),
			field: parent === undefined ? 'items' : 'children',
			arguments: {}
		}),
		metadata: metadata(parent)
	}));
	const indexKeys = indexes.map(({ key }) => key);

	/** Layers add their context's records to the confirmed root index. */
	const reconciler = (confirmed, layers) => {
		const root = confirmedIndex(confirmed, indexKeys[0]);
		if (root === undefined) return [];
		const added = layers.flatMap((layer) => layer.context?.records ?? []);
		if (added.length === 0) return [];
		return [
			{
				kind: 'write',
				write: {
					key: root.key,
					records: [...new Set([...root.records, ...added])],
					complete: true,
					metadata: root.metadata
				}
			}
		];
	};

	const engine = createCacheEngine();
	engine.setDerivedIndexReconciler(reconciler);

	const selectors = [
		...RECORDS.map((key) => [`record ${key}`, (reader) => reader.record(key)]),
		...RECORDS.map((key) => [`meta ${key}`, (reader) => reader.recordMeta(key)]),
		...RECORDS.map((key) => [`title ${key}`, (reader) => reader.field(key, 'title')]),
		...RECORDS.map((key) => [`owner ${key}`, (reader) => reader.link(key, 'owner')]),
		...indexKeys.map((key) => [`index ${key}`, (reader) => reader.index(key)]),
		[
			'titles of the root index',
			(reader) =>
				(reader.index(indexKeys[0])?.records ?? []).map(
					(key) => reader.record(key)?.fields.title ?? null
				)
		]
	];
	const watched = selectors.map(([name, selector]) => {
		const watch = { name, selector, value: engine.read(selector), notified: 0 };
		engine.watch(selector, (value) => {
			watch.value = value;
			watch.notified += 1;
		});
		return watch;
	});

	let revision = 0;
	let layerNumber = 0;
	/** Live layers in sequence order, as data a fresh engine can replay. */
	const layers = [];

	const baseOperations = () =>
		Array.from({ length: 1 + int(3) }, () => {
			const kind = pick([
				'record', 'record', 'record', 'tombstone', 'discard',
				'index', 'index', 'stale', 'deleteIndex'
			]);
			revision += 1;
			if (kind === 'record') {
				return {
					kind,
					write: {
						key: pick(RECORDS),
						revision,
						...(chance(0.8)
							? {
									fields: {
										...(chance(0.8) ? { title: `title ${revision}` } : {}),
										...(chance(0.3) ? { count: int(3) } : {}),
										...(chance(0.2) ? { note: null } : {})
									}
								}
							: {}),
						...(chance(0.3)
							? { links: { owner: chance(0.2) ? null : pick(RECORDS) } }
							: {})
					}
				};
			}
			if (kind === 'tombstone' || kind === 'discard') {
				return { kind, key: pick(RECORDS), revision };
			}
			const index = pick(indexes);
			if (kind === 'index') {
				return {
					kind,
					write: {
						key: index.key,
						revision,
						records: subset(RECORDS),
						complete: chance(0.7),
						metadata: index.metadata
					}
				};
			}
			return { kind, key: index.key, revision };
		});

	const applyBase = (writer, operations) => {
		for (const operation of operations) {
			if (operation.kind === 'record') writer.writeRecord(copy(operation.write));
			else if (operation.kind === 'tombstone') {
				writer.tombstoneRecord(operation.key, operation.revision);
			} else if (operation.kind === 'discard') writer.discardRecord(operation.key);
			else if (operation.kind === 'index') writer.writeIndex(copy(operation.write));
			else if (operation.kind === 'stale') {
				writer.markIndexStale(operation.key, 'random', operation.revision);
			} else writer.deleteIndex(operation.key, operation.revision);
		}
	};

	const layerOperations = () =>
		Array.from({ length: 1 + int(3) }, () => {
			const kind = pick(['record', 'record', 'record', 'tombstone', 'index', 'deleteIndex']);
			if (kind === 'record') {
				return {
					kind,
					write: {
						key: pick(RECORDS),
						...(chance(0.8) ? { fields: { title: `optimistic ${int(1000)}` } } : {}),
						...(chance(0.2) ? { unset: ['count'] } : {}),
						...(chance(0.3) ? { ifPresent: true } : {}),
						...(chance(0.2) ? { links: { owner: pick(RECORDS) } } : {})
					}
				};
			}
			if (kind === 'tombstone') return { kind, key: pick(RECORDS) };
			const index = pick(indexes);
			return kind === 'index'
				? {
						kind,
						write: {
							key: index.key,
							records: subset(RECORDS),
							complete: chance(0.7),
							metadata: index.metadata
						}
					}
				: { kind, key: index.key };
		});

	const applyLayer = (writer, operations) => {
		for (const operation of operations) {
			if (operation.kind === 'record') writer.writeRecord(copy(operation.write));
			else if (operation.kind === 'tombstone') writer.tombstoneRecord(operation.key);
			else if (operation.kind === 'index') writer.writeIndex(copy(operation.write));
			else writer.deleteIndex(operation.key);
		}
	};
	const layerContext = () =>
		chance(0.5) ? { records: subset(RECORDS).slice(0, 2) } : undefined;

	const stepKinds = [
		'batch', 'batch', 'batch', 'batch', 'failingBatch',
		'create', 'create', 'replace', 'failingReplace', 'accept',
		'confirm', 'confirmMany', 'reject',
		'retain', 'release', 'gc', 'discardIndexes'
	];

	const view = (read) => selectors.map(([, selector]) => read(selector));
	const trace = [];
	for (let step = 0; step < steps; step += 1) {
		const kind = pick(stepKinds);
		const before = kind.startsWith('failing') ? engine.extract() : undefined;
		let outcome;
		try {
			if (kind === 'batch' || kind === 'failingBatch') {
				const operations = baseOperations();
				engine.batch((writer) => {
					applyBase(writer, operations);
					if (kind === 'failingBatch') throw new Error('rolled back');
				});
			} else if (kind === 'create') {
				const layer = {
					id: `layer-${(layerNumber += 1)}`,
					operations: layerOperations(),
					context: layerContext(),
					accepted: false
				};
				engine.createOptimisticLayer(
					layer.id,
					(writer) => applyLayer(writer, layer.operations),
					copy(layer.context)
				);
				layers.push(layer);
			} else if (kind === 'replace' || kind === 'failingReplace') {
				if (layers.length === 0) continue;
				const layer = pick(layers);
				const operations = layerOperations();
				const context = layerContext();
				engine.replaceOptimisticLayer(
					layer.id,
					(_reader, writer) => {
						applyLayer(writer, operations);
						if (kind === 'failingReplace') throw new Error('rolled back');
					},
					copy(context)
				);
				layer.operations = operations;
				layer.context = context;
			} else if (kind === 'accept') {
				if (layers.length === 0) continue;
				const layer = pick(layers);
				engine.markOptimisticLayerAccepted(layer.id);
				layer.accepted = true;
			} else if (kind === 'confirm' || kind === 'confirmMany') {
				if (layers.length === 0) continue;
				const confirmed =
					kind === 'confirm' ? [pick(layers)] : layers.filter(() => chance(0.5));
				if (confirmed.length === 0) continue;
				const operations = baseOperations();
				const ids = confirmed.map((layer) => layer.id);
				if (kind === 'confirm') {
					engine.confirmOptimisticLayer(ids[0], (writer) => applyBase(writer, operations));
				} else {
					engine.confirmOptimisticLayers(ids, (writer) => applyBase(writer, operations));
				}
				for (const layer of confirmed) layers.splice(layers.indexOf(layer), 1);
			} else if (kind === 'reject') {
				if (layers.length === 0) continue;
				const layer = pick(layers);
				engine.rejectOptimisticLayer(layer.id);
				layers.splice(layers.indexOf(layer), 1);
			} else if (kind === 'retain') engine.retain(pick(RECORDS));
			else if (kind === 'release') engine.release(pick(RECORDS));
			else if (kind === 'gc') outcome = [...engine.gc()].sort();
			else engine.discardIndexes(subset(indexKeys));
		} catch (error) {
			outcome = `${error.name}: ${error.message}`;
		}
		const entry = {
			step,
			kind,
			outcome,
			visible: view((selector) => engine.read(selector)),
			confirmed: view((selector) => engine.readConfirmed(selector)),
			watched: watched.map((watch) => watch.value),
			notified: watched.map((watch) => watch.notified),
			layerStates: layers.map((layer) => engine.optimisticLayerState(layer.id)),
			snapshot: engine.extract()
		};
		trace.push(entry);
		options.onStep?.({
			entry,
			before,
			engine,
			layers,
			selectors,
			names: selectors.map(([name]) => name)
		});
	}
	return trace;
}
