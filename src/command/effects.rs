use serde::{Deserialize, Serialize};

/// Portable value expression resolved from a command's canonical input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum EffectExpression {
    Input {
        path: Vec<String>,
    },
    TrustedPreset {
        name: String,
    },
    Constant {
        value: serde_json::Value,
    },
    /// SQL null, emitted only by the type-checked `null()` expression.
    Null,
}
