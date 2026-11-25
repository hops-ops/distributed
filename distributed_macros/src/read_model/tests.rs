use super::types::{default_storage_name, to_snake_case};
use super::*;
use syn::DeriveInput;

#[test]
fn direct_relationship_references_need_one_belongs_to() {
    for attributes in [
        "references = \"id\"",
        "belongs_to = \"Object\", foreign_key = \"id\", references = \"id\", references = \"id\"",
        "many_to_many = \"Object\", foreign_key = \"id\", through = \"links\", references = \"id\"",
    ] {
        let input = syn::parse_str(&format!(
            "struct Ref {{ id: String, #[readmodel({attributes})] object: Option<Object> }}"
        ))
        .unwrap();
        assert!(expand_read_model(input).is_err(), "accepted {attributes}");
    }
}

#[test]
fn boxed_belongs_to_rejects_anything_but_one_box_of_the_target() {
    for ty in [
        "Option<Box<Other>>",
        "Option<Box<Box<Child>>>",
        "Option<std::sync::Arc<Child>>",
        "Box<Child>",
        "Vec<Box<Child>>",
    ] {
        let input: DeriveInput = syn::parse_str(&format!(
            "struct Parent {{ id: String, #[readmodel(belongs_to = \"Child\", foreign_key = \"id\")] child: {ty} }}"
        )).unwrap();
        assert!(expand_read_model(input).is_err(), "accepted {ty}");
    }
}

#[test]
fn expand_read_model_rejects_struct_index_without_columns() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("counter_views")]
        #[index]
        struct CounterView {
            #[id]
            id: String,
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("struct index needs columns");

    assert!(
        err.to_string().contains("#[index] requires columns"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_direct_attributes_without_values() {
    let input: DeriveInput = syn::parse_quote! {
        #[collection]
        struct CounterView {
            id: String,
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("direct collection needs a value");

    assert!(
        err.to_string()
            .contains("#[collection] requires a string literal"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_multiple_explicit_id_attributes() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView {
            #[readmodel(id)]
            counter_id: String,
            #[readmodel(id)]
            tenant_id: String,
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("multiple ids should return an error");

    assert!(
        err.to_string()
            .contains("Multiple #[readmodel(id)] fields found"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_unknown_struct_attributes() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(tabel = "counter_views")]
        struct CounterView {
            id: String,
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("unknown struct attribute should fail");

    assert!(
        err.to_string()
            .contains("unknown readmodel struct attribute"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_tuple_structs() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView(String);
    };

    let err = expand_read_model(input).expect_err("tuple struct should return an error");

    assert!(
        err.to_string().contains("requires named fields"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_relationships_without_foreign_key() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "players")]
        struct Player {
            #[readmodel(id)]
            player_id: String,
            #[readmodel(has_many = "PlayerWeapon")]
            weapons: Vec<PlayerWeapon>,
        }
    };

    let err = expand_read_model(input).expect_err("missing relationship key should fail");

    assert!(
        err.to_string().contains("foreign_key"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_duplicate_relationship_foreign_keys() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "players")]
        struct Player {
            #[readmodel(id)]
            player_id: String,
            #[readmodel(has_many = "PlayerWeapon", foreign_key = "player_id", foreign_key = "owner_id")]
            weapons: Vec<PlayerWeapon>,
        }
    };

    let err = expand_read_model(input).expect_err("duplicate relationship foreign key");

    assert!(
        err.to_string()
            .contains("foreign_key declared more than once"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_rejects_duplicate_pending_relationship_through_attrs() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "players")]
        struct Player {
            #[readmodel(id)]
            player_id: String,
            #[readmodel(through = "player_weapon_links", through = "weapon_players", many_to_many = "Weapon")]
            weapons: Vec<Weapon>,
        }
    };

    let err = expand_read_model(input).expect_err("duplicate pending relationship through");

    assert!(
        err.to_string().contains("through declared more than once"),
        "unexpected error: {err}"
    );
}

#[test]
fn snake_case_preserves_multi_char_lowercase_mapping() {
    assert_eq!(to_snake_case("İdView"), "i\u{307}d_view");
}

#[test]
fn default_storage_name_infers_natural_plural_query_names() {
    let actual = ["Todos", "ChatMessages", "BlobGames"].map(default_storage_name);
    let expected = [
        "todos".to_string(),
        "chat_messages".to_string(),
        "blob_games".to_string(),
    ];

    assert_eq!(actual, expected);
}

#[test]
fn default_storage_name_retains_the_singular_append_s_convention() {
    assert_eq!(default_storage_name("CounterView"), "counter_views");
}

#[test]
fn default_storage_name_does_not_assign_meaning_to_the_view_suffix() {
    let actual = ["Todo", "TodoView"].map(default_storage_name);
    let expected = ["todos".to_string(), "todo_views".to_string()];

    assert_eq!(actual, expected);
}

#[test]
fn default_storage_name_treats_ambiguous_terminal_s_as_plural() {
    let actual = ["Status", "Bus"].map(default_storage_name);
    let expected = ["status".to_string(), "bus".to_string()];

    assert_eq!(actual, expected);
}
