use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExecutionLimits {
    pub max_depth: u64,
    pub max_complexity: u64,
    pub max_bool_width: u64,
    pub max_in_list: u64,
    pub complexity: ClientComplexityWeights,
}

impl Default for ClientExecutionLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH as u64,
            max_complexity: DEFAULT_MAX_COMPLEXITY as u64,
            max_bool_width: DEFAULT_MAX_BOOL_WIDTH,
            max_in_list: DEFAULT_MAX_IN_LIST,
            complexity: ClientComplexityWeights::current(),
        }
    }
}

impl ClientExecutionLimits {
    #[cfg(feature = "graphql")]
    pub(crate) fn from_runtime(
        max_depth: usize,
        max_complexity: usize,
        max_bool_width: usize,
        max_in_list: usize,
    ) -> Result<Self, ClientManifestError> {
        Ok(Self {
            max_depth: u64::try_from(max_depth).map_err(|_| {
                ClientManifestError("GraphQL max_depth exceeds the client manifest range".into())
            })?,
            max_complexity: u64::try_from(max_complexity).map_err(|_| {
                ClientManifestError(
                    "GraphQL max_complexity exceeds the client manifest range".into(),
                )
            })?,
            max_bool_width: u64::try_from(max_bool_width).map_err(|_| {
                ClientManifestError(
                    "GraphQL max_bool_width exceeds the client manifest range".into(),
                )
            })?,
            max_in_list: u64::try_from(max_in_list).map_err(|_| {
                ClientManifestError("GraphQL max_in_list exceeds the client manifest range".into())
            })?,
            complexity: ClientComplexityWeights::current(),
        })
    }
}

/// Weights and formula of nested query cost (contract version 1): what the
/// engine enforces against `max_complexity`, exported so a client compiler
/// rejects the same operations at build time.
///
/// | Kind | Cost |
/// |---|---|
/// | scalar | `scalar` per leaf field |
/// | object | sum of its fields; an empty selection still costs `scalar` |
/// | belongs_to | `belongs_to` + child selection |
/// | has_many / m2m | `has_many`/`m2m` + `list_fanout` × child selection |
/// | aggregate | `aggregate` + child selection |
/// | list root | `list_root` + `list_fanout` × child selection |
/// | by_pk root | `by_pk` + child selection |
///
/// `list_fanout` models nested row multiplication without using the full
/// `limit` (which defaults to 100 and would make any nest fail). It is a
/// conservative multiplier so deep `has_many` trees grow faster than flat
/// field counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientComplexityWeights {
    pub version: u32,
    pub scalar: u64,
    pub belongs_to: u64,
    pub has_many: u64,
    pub m2m: u64,
    pub aggregate: u64,
    pub list_root: u64,
    pub by_pk: u64,
    /// Multiplier for nested list relationship child selections.
    pub list_fanout: u64,
}

impl ClientComplexityWeights {
    /// The weights this crate's engine enforces and its manifests export.
    pub fn current() -> Self {
        Self {
            version: QUERY_COMPLEXITY_VERSION,
            scalar: 1,
            belongs_to: 2,
            has_many: 10,
            m2m: 12,
            aggregate: 8,
            list_root: 3,
            by_pk: 1,
            // Fan-out factor: deep has_many trees exceed
            // DEFAULT_MAX_COMPLEXITY by about three nested levels while
            // one-level nests remain usable.
            list_fanout: 5,
        }
    }

    /// Cost of an object selection given the cost of each selected field.
    /// A selection with no costed field still costs one scalar.
    pub fn object(&self, fields: impl IntoIterator<Item = u64>) -> u64 {
        let mut fields = fields.into_iter().peekable();
        if fields.peek().is_none() {
            return self.scalar;
        }
        fields.fold(0, u64::saturating_add)
    }

    /// Cost of a relationship field whose selection costs `child`.
    pub fn relationship(&self, kind: ClientRelationshipKind, child: u64) -> u64 {
        match kind {
            ClientRelationshipKind::BelongsTo => self.belongs_to.saturating_add(child),
            ClientRelationshipKind::HasMany => self
                .has_many
                .saturating_add(self.list_fanout.saturating_mul(child)),
            ClientRelationshipKind::ManyToMany => self
                .m2m
                .saturating_add(self.list_fanout.saturating_mul(child)),
        }
    }

    /// Cost of an aggregate (root or `<relationship>_aggregate` field) whose
    /// selection costs `child`.
    pub fn aggregate(&self, child: u64) -> u64 {
        self.aggregate.saturating_add(child)
    }

    /// Cost of an aggregate's `nodes` field whose selection costs `child`.
    pub fn aggregate_nodes(&self, child: u64) -> u64 {
        self.list_fanout.saturating_mul(child).max(self.scalar)
    }

    /// Cost of a root field whose selection costs `child`.
    pub fn root(&self, kind: ClientRootKind, child: u64) -> u64 {
        match kind {
            ClientRootKind::List => self
                .list_root
                .saturating_add(self.list_fanout.saturating_mul(child)),
            ClientRootKind::ByPk => self.by_pk.saturating_add(child),
            ClientRootKind::Aggregate => self.aggregate(child),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weights() -> ClientComplexityWeights {
        // Distinct primes: a formula that reads the wrong weight, or the
        // built-in ones, gives another number.
        ClientComplexityWeights {
            version: QUERY_COMPLEXITY_VERSION,
            scalar: 2,
            belongs_to: 3,
            has_many: 5,
            m2m: 7,
            aggregate: 11,
            list_root: 13,
            by_pk: 17,
            list_fanout: 19,
        }
    }

    #[test]
    fn an_empty_selection_costs_one_scalar_wherever_it_nests() {
        let w = weights();
        let empty = w.object([]);
        assert_eq!(empty, 2);
        assert_eq!(w.object([0]), 0, "a costed field is not an empty selection");
        assert_eq!(w.relationship(ClientRelationshipKind::BelongsTo, empty), 5);
        assert_eq!(w.relationship(ClientRelationshipKind::HasMany, empty), 43);
        assert_eq!(w.root(ClientRootKind::List, empty), 51);
        assert_eq!(w.root(ClientRootKind::Aggregate, empty), 13);
    }

    #[test]
    fn every_cost_uses_the_given_weights() {
        let w = weights();
        let child = w.object([w.scalar, w.scalar]);
        assert_eq!(child, 4);
        assert_eq!(w.relationship(ClientRelationshipKind::BelongsTo, child), 7);
        assert_eq!(w.relationship(ClientRelationshipKind::HasMany, child), 81);
        assert_eq!(
            w.relationship(ClientRelationshipKind::ManyToMany, child),
            83
        );
        assert_eq!(w.aggregate(child), 15);
        assert_eq!(w.aggregate_nodes(child), 76);
        assert_eq!(w.aggregate_nodes(0), 2);
        assert_eq!(w.root(ClientRootKind::List, child), 89);
        assert_eq!(w.root(ClientRootKind::ByPk, child), 21);
        assert_eq!(w.root(ClientRootKind::Aggregate, child), 15);
        assert_eq!(w.root(ClientRootKind::List, u64::MAX), u64::MAX);
    }

    #[test]
    fn default_budget_admits_one_has_many_level_and_rejects_three() {
        let w = ClientComplexityWeights::current();
        let has_many = |inner| {
            w.relationship(
                ClientRelationshipKind::HasMany,
                w.object([w.scalar, w.scalar, inner]),
            )
        };
        let leaf = w.relationship(
            ClientRelationshipKind::HasMany,
            w.object([w.scalar, w.scalar]),
        );
        let flat = w.root(ClientRootKind::List, w.object([w.scalar; 3]));
        let one_nest = w.root(ClientRootKind::List, w.object([w.scalar, leaf]));
        let three_nest = w.root(ClientRootKind::List, w.object([w.scalar, has_many(leaf)]));
        assert!(flat < one_nest && one_nest < three_nest);
        assert!(one_nest < DEFAULT_MAX_COMPLEXITY as u64, "{one_nest}");
        assert!(three_nest > DEFAULT_MAX_COMPLEXITY as u64, "{three_nest}");
    }
}
