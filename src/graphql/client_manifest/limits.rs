use super::*;

/// The engine's query cost limits, exported so a client compiler refuses the
/// operations the engine would refuse. One unit per field; the model is
/// described in `docs/graphql-query-cost.md`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExecutionLimits {
    /// Nesting levels of fields, relationships and `where` filters.
    pub max_depth: u64,
    /// Selected fields in one document, every field once at every level.
    pub max_fields: u64,
    /// Model rows one root field may produce ([`row_cost`]).
    pub max_rows: u64,
    /// Entries in one `_and` / `_or` list.
    pub max_bool_width: u64,
    /// Entries in one `_in` / `_nin` list.
    pub max_in_list: u64,
}

impl Default for ClientExecutionLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH as u64,
            max_fields: DEFAULT_MAX_FIELDS as u64,
            max_rows: DEFAULT_MAX_ROWS,
            max_bool_width: DEFAULT_MAX_BOOL_WIDTH as u64,
            max_in_list: DEFAULT_MAX_IN_LIST as u64,
        }
    }
}

/// How many model rows an operation can produce: the arithmetic the engine
/// checks against `max_rows` and a client compiler mirrors.
///
/// A page of `limit` rows costs `limit × (1 + rows nested under one row)`;
/// one object (a by-pk root, a belongs-to relationship) costs
/// `1 + rows nested under it`; scalars and `aggregate { count }` cost nothing.
/// Sibling costs add. Arithmetic saturates, so an overflowing cost is refused
/// rather than wrapped.
pub mod row_cost {
    /// A page of up to `limit` rows, each carrying `nested` rows.
    pub fn page(limit: u64, nested: u64) -> u64 {
        limit.saturating_mul(nested.saturating_add(1))
    }

    /// One row carrying `nested` rows.
    pub fn one(nested: u64) -> u64 {
        page(1, nested)
    }
}

#[cfg(feature = "graphql")]
impl ClientExecutionLimits {
    pub(crate) fn from_runtime(
        max_depth: usize,
        max_fields: usize,
        max_rows: u64,
        max_bool_width: usize,
        max_in_list: usize,
    ) -> Result<Self, ClientManifestError> {
        let wide = |name: &str, value: usize| {
            u64::try_from(value).map_err(|_| {
                ClientManifestError(format!("GraphQL {name} exceeds the client manifest range"))
            })
        };
        Ok(Self {
            max_depth: wide("max_depth", max_depth)?,
            max_fields: wide("max_fields", max_fields)?,
            max_rows,
            max_bool_width: wide("max_bool_width", max_bool_width)?,
            max_in_list: wide("max_in_list", max_in_list)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::row_cost;
    use crate::graphql::cost::{DEFAULT_LIMIT, DEFAULT_MAX_LIMIT, DEFAULT_MAX_ROWS};

    #[test]
    fn a_chain_of_lists_costs_about_the_product_of_its_limits() {
        // orders(limit: 1000) { items(limit: 1000) { id } }
        assert_eq!(row_cost::page(1000, row_cost::page(1000, 0)), 1_001_000);
        // todo(id) { owner { name } }
        assert_eq!(row_cost::one(row_cost::one(0)), 2);
        assert_eq!(row_cost::page(u64::MAX, 1), u64::MAX);
    }

    #[test]
    fn default_budget_admits_one_level_of_nesting_and_refuses_two() {
        let page = |limit, nested| row_cost::page(limit, nested);
        let max_root_one_nest = page(DEFAULT_MAX_LIMIT, page(DEFAULT_LIMIT, 0));
        let siblings = page(DEFAULT_LIMIT, 19 * page(DEFAULT_LIMIT, 0));
        let two_levels = page(DEFAULT_LIMIT, page(DEFAULT_LIMIT, page(DEFAULT_LIMIT, 0)));
        let max_nest = page(DEFAULT_MAX_LIMIT, page(DEFAULT_MAX_LIMIT, 0));
        assert!(max_root_one_nest <= DEFAULT_MAX_ROWS, "{max_root_one_nest}");
        assert!(siblings <= DEFAULT_MAX_ROWS, "{siblings}");
        assert!(two_levels > DEFAULT_MAX_ROWS, "{two_levels}");
        assert!(max_nest > DEFAULT_MAX_ROWS, "{max_nest}");
    }
}
