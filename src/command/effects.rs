use serde::{Deserialize, Serialize};

use super::projection_obligations::CommandProjectionConfirmation;

/// Portable value expression in a command effect.
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
    /// Construction-time serialization failure sentinel (legacy effect macro).
    /// Kept for IR shape stability; no longer constructed after macro removal.
    #[serde(skip)]
    #[allow(dead_code)]
    InvalidConstant {
        error: String,
    },
}

/// One model-field assignment in portable effect IR.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EffectFieldValue {
    pub field: String,
    pub value: EffectExpression,
}

/// A complete, ordered model key in portable effect IR.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EffectKey {
    pub fields: Vec<EffectFieldValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EffectRelationship {
    pub source_model: String,
    pub field: String,
    pub target_model: String,
}

/// Closed portable command-effect operation set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CommandEffect {
    Upsert {
        model: String,
        key: EffectKey,
        fields: Vec<EffectFieldValue>,
    },
    Patch {
        model: String,
        key: EffectKey,
        fields: Vec<EffectFieldValue>,
    },
    Delete {
        model: String,
        key: EffectKey,
    },
    Link {
        relationship: EffectRelationship,
        source: EffectKey,
        target: EffectKey,
    },
    Unlink {
        relationship: EffectRelationship,
        source: EffectKey,
        target: EffectKey,
    },
    InvalidateModel {
        model: String,
    },
    InvalidateRelationship {
        relationship: EffectRelationship,
        source: EffectKey,
    },
}

/// What the client must do when the declared operations cannot prove safety.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandEffectFallback {
    /// No local invention: mark affected selections stale and revalidate.
    Revalidate,
}

/// Version-independent portable effect declaration attached to one command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommandEffects {
    pub operations: Vec<CommandEffect>,
    pub fallback: CommandEffectFallback,
}

impl CommandEffects {
    pub(crate) fn new(operations: impl IntoIterator<Item = CommandEffect>) -> Self {
        Self {
            operations: operations.into_iter().collect(),
            fallback: CommandEffectFallback::Revalidate,
        }
    }

    pub(crate) fn revalidate() -> Self {
        Self::new([])
    }

    pub(crate) fn canonicalize(&mut self) {
        for operation in &mut self.operations {
            match operation {
                CommandEffect::Upsert { fields, .. } | CommandEffect::Patch { fields, .. } => {
                    fields.sort_by(|left, right| left.field.cmp(&right.field));
                }
                CommandEffect::Delete { .. }
                | CommandEffect::Link { .. }
                | CommandEffect::Unlink { .. }
                | CommandEffect::InvalidateModel { .. }
                | CommandEffect::InvalidateRelationship { .. } => {}
            }
        }
    }

    pub(super) fn invalid_constant_error(&self) -> Option<&str> {
        self.operations
            .iter()
            .find_map(|operation| match operation {
                CommandEffect::Upsert { key, fields, .. }
                | CommandEffect::Patch { key, fields, .. } => invalid_key_constant(key)
                    .or_else(|| fields.iter().find_map(invalid_field_constant)),
                CommandEffect::Delete { key, .. } => invalid_key_constant(key),
                CommandEffect::Link { source, target, .. }
                | CommandEffect::Unlink { source, target, .. } => {
                    invalid_key_constant(source).or_else(|| invalid_key_constant(target))
                }
                CommandEffect::InvalidateRelationship { source, .. } => {
                    invalid_key_constant(source)
                }
                CommandEffect::InvalidateModel { .. } => None,
            })
    }
}

pub(super) fn invalid_expression_constant(expression: &EffectExpression) -> Option<&str> {
    match expression {
        EffectExpression::InvalidConstant { error } => Some(error),
        EffectExpression::Input { .. }
        | EffectExpression::TrustedPreset { .. }
        | EffectExpression::Constant { .. }
        | EffectExpression::Null => None,
    }
}

fn invalid_field_constant(field: &EffectFieldValue) -> Option<&str> {
    invalid_expression_constant(&field.value)
}

fn invalid_key_constant(key: &EffectKey) -> Option<&str> {
    key.fields.iter().find_map(invalid_field_constant)
}

pub(super) fn invalid_confirmation_constant(
    confirmation: &CommandProjectionConfirmation,
) -> Option<&str> {
    invalid_key_constant(&confirmation.key).or_else(|| {
        confirmation
            .partition
            .as_ref()
            .and_then(invalid_expression_constant)
    })
}
