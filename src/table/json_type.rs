//! The serialized shape of a JSON value, so a JSON read-model column has an
//! exact type in generated clients instead of `unknown`.
//!
//! `JsonType` follows Serde's serialized form. It is implemented for the
//! scalar, sequence, tuple, map and option types Serde serializes as plain JSON, and
//! derived (`#[derive(JsonType)]`) for named-field structs and unit-only enums.
//! A type whose JSON form it cannot state exactly is not `JsonType`; a
//! `serde_json::Value` column states that its shape is unknown.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

/// The serialized JSON shape of a Rust type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonShape {
    Boolean,
    /// Any JSON number (integers are exact only within JavaScript's safe range).
    Number,
    String,
    /// One of these strings: a unit-only enum.
    Enum {
        values: Vec<String>,
    },
    Array {
        items: Box<JsonShape>,
    },
    /// A fixed-length array: a Rust tuple.
    Tuple {
        items: Vec<JsonShape>,
    },
    /// The value or `null`.
    Nullable {
        value: Box<JsonShape>,
    },
    /// An object keyed by arbitrary strings.
    Map {
        values: Box<JsonShape>,
    },
    /// An object with exactly these fields, in declaration order.
    Object {
        fields: Vec<JsonField>,
    },
    /// Any JSON value.
    Unknown,
}

/// One field of a [`JsonShape::Object`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonField {
    /// The serialized field name.
    pub name: String,
    pub shape: JsonShape,
}

impl JsonShape {
    /// The fields of an object shape; empty for every other shape.
    pub fn fields(&self) -> &[JsonField] {
        match self {
            Self::Object { fields } => fields,
            _ => &[],
        }
    }
}

/// A type with a fixed serialized JSON shape. Derive it with
/// `#[derive(distributed::table::JsonType)]`.
pub trait JsonType {
    fn json_shape() -> JsonShape;
}

macro_rules! json_scalar {
    ($shape:ident: $($ty:ty),+) => {
        $(impl JsonType for $ty {
            fn json_shape() -> JsonShape {
                JsonShape::$shape
            }
        })+
    };
}

json_scalar!(Boolean: bool);
json_scalar!(Number: i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64);
json_scalar!(String: String, str, char);

impl JsonType for serde_json::Value {
    fn json_shape() -> JsonShape {
        JsonShape::Unknown
    }
}

impl<T: JsonType + ?Sized> JsonType for &T {
    fn json_shape() -> JsonShape {
        T::json_shape()
    }
}

impl<T: JsonType + ?Sized> JsonType for Box<T> {
    fn json_shape() -> JsonShape {
        T::json_shape()
    }
}

impl<T: JsonType> JsonType for Option<T> {
    fn json_shape() -> JsonShape {
        JsonShape::Nullable {
            value: Box::new(T::json_shape()),
        }
    }
}

impl<T: JsonType> JsonType for Vec<T> {
    fn json_shape() -> JsonShape {
        JsonShape::Array {
            items: Box::new(T::json_shape()),
        }
    }
}

impl<T: JsonType> JsonType for [T] {
    fn json_shape() -> JsonShape {
        JsonShape::Array {
            items: Box::new(T::json_shape()),
        }
    }
}

macro_rules! json_tuple {
    ($($name:ident),+) => {
        impl<$($name: JsonType),+> JsonType for ($($name,)+) {
            fn json_shape() -> JsonShape {
                JsonShape::Tuple {
                    items: vec![$($name::json_shape()),+],
                }
            }
        }
    };
}

json_tuple!(A);
json_tuple!(A, B);
json_tuple!(A, B, C);
json_tuple!(A, B, C, D);

impl<V: JsonType> JsonType for BTreeMap<String, V> {
    fn json_shape() -> JsonShape {
        JsonShape::Map {
            values: Box::new(V::json_shape()),
        }
    }
}

impl<V: JsonType, S> JsonType for HashMap<String, V, S> {
    fn json_shape() -> JsonShape {
        JsonShape::Map {
            values: Box::new(V::json_shape()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Serialize, Deserialize, crate::table::JsonType)]
    #[serde(rename_all = "snake_case")]
    #[allow(dead_code)]
    enum Tile {
        Hole,
        DeadByHole,
        #[serde(rename = "you")]
        Player,
    }

    #[derive(Serialize, crate::table::JsonType)]
    #[allow(dead_code)]
    struct Board {
        cells: Vec<Vec<Tile>>,
        #[serde(rename = "best")]
        high_score: Option<i64>,
        tags: BTreeMap<String, bool>,
    }

    #[test]
    fn derived_shapes_follow_serde_names() {
        assert_eq!(
            Tile::json_shape(),
            JsonShape::Enum {
                values: vec!["hole".into(), "dead_by_hole".into(), "you".into()]
            }
        );
        let board = Board::json_shape();
        let names: Vec<_> = board.fields().iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["cells", "best", "tags"]);
        assert_eq!(
            board.fields()[0].shape,
            JsonShape::Array {
                items: Box::new(JsonShape::Array {
                    items: Box::new(Tile::json_shape())
                })
            }
        );
        assert_eq!(
            board.fields()[1].shape,
            JsonShape::Nullable {
                value: Box::new(JsonShape::Number)
            }
        );
    }

    #[derive(Clone, Default, Serialize, Deserialize, crate::read_model::ReadModel)]
    #[readmodel(primary_key = ["id"])]
    struct Boards {
        id: String,
        cells: Vec<Vec<Tile>>,
        score: i64,
    }

    #[test]
    fn read_model_json_columns_carry_their_shape() {
        use crate::read_model::RelationalReadModel;
        let schema = Boards::schema();
        let column = |name: &str| {
            schema
                .columns
                .iter()
                .find(|column| column.column_name == name)
                .unwrap()
        };
        assert_eq!(
            column("cells").json_type,
            Some(<Vec<Vec<Tile>>>::json_shape())
        );
        assert_eq!(column("score").json_type, None);
    }

    #[test]
    fn shapes_round_trip_as_tagged_json() {
        let shape = Board::json_shape();
        let json = serde_json::to_value(&shape).unwrap();
        assert_eq!(json["kind"], "object");
        assert_eq!(json["fields"][0]["shape"]["kind"], "array");
        assert_eq!(serde_json::from_value::<JsonShape>(json).unwrap(), shape);
    }
}
