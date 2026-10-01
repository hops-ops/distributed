use super::types::{default_storage_name, to_snake_case};
use super::*;
use syn::DeriveInput;

#[test]
fn direct_relationship_references_are_order_independent_and_emitted() {
    for attributes in [
        "references = \"namespace,oid\", belongs_to = \"Object\", foreign_key = \"scope,oid\"",
        "belongs_to = \"Object\", foreign_key = \"scope,oid\", references = \"namespace,oid\"",
    ] {
        let input = syn::parse_str(&format!(
            "struct Ref {{ id: String, #[readmodel({attributes})] object: Option<Object> }}"
        ))
        .unwrap();
        let expanded = expand_read_model(input).unwrap().to_string();
        assert!(
            expanded.contains("references : Some (\"namespace,oid\""),
            "{expanded}"
        );
    }
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
fn boxed_belongs_to_preserves_target_markers_and_checks_inner_type() {
    let input: DeriveInput = syn::parse_quote! {
        struct Parent {
            id: String,
            #[readmodel(belongs_to = "Child", foreign_key = "id")]
            child: Option<std::boxed::Box<Child>>,
        }
    };
    let expanded = expand_read_model(input).unwrap().to_string();
    assert!(expanded.contains("type Target = Child"), "{expanded}");
    assert!(!expanded.contains("type Target = std :: boxed :: Box"));
    assert!(expanded.contains("Box :: new"));
    assert!(expanded.contains("value . as_ref ()"));
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
fn expand_read_model_accepts_named_id_field() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView {
            id: String,
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("impl distributed :: ReadModel for CounterView"));
    assert!(expanded.contains("fn id"));
}

#[test]
fn expand_read_model_accepts_explicit_id_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView {
            id: String,
            #[readmodel(id)]
            counter_id: String,
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("& self . counter_id"));
    assert!(!expanded.contains("& self . id"));
}

#[test]
fn expand_read_model_accepts_declared_name() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(name = "operational.todos", table = "operational_todos")]
        struct OperationalTodos {
            #[readmodel(id)]
            todo_id: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();
    assert!(
        expanded.contains("operational.todos") || expanded.contains("operational . todos"),
        "{expanded}"
    );
    assert!(!expanded.contains("model_name : \"OperationalTodos\""));
    assert!(
        expanded.contains("operational_todos"),
        "default table must come from the Rust type, not the declared name: {expanded}"
    );
}

#[test]
fn expand_read_model_accepts_direct_collection_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[collection("counter_views")]
        struct CounterView {
            #[id]
            counter_id: String,
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("const COLLECTION : & 'static str = \"counter_views\""));
    assert!(expanded.contains("& self . counter_id"));
}

#[test]
fn expand_read_model_accepts_direct_table_and_id_column_attributes() {
    let input: DeriveInput = syn::parse_quote! {
        #[table = "counter_views"]
        struct CounterView {
            #[id("counter_id")]
            id: String,
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("table_name : \"counter_views\""));
    assert!(expanded.contains("column_name : \"counter_id\""));
}

#[test]
fn expand_read_model_accepts_direct_column_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("counter_views")]
        struct CounterView {
            #[id]
            id: String,
            #[column("counter_value")]
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("column_name : \"counter_value\""));
}

#[test]
fn expand_read_model_accepts_direct_index_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("counter_views")]
        struct CounterView {
            #[id]
            id: String,
            #[index("idx_counter_views_value")]
            value: i32,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("name : Some (\"idx_counter_views_value\""));
    assert!(expanded.contains("columns : vec ! [\"value\""));
    assert!(expanded.contains("unique : false"));
}

#[test]
fn expand_read_model_accepts_direct_unique_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("counter_views")]
        struct CounterView {
            #[id]
            id: String,
            #[unique("uq_counter_views_slug")]
            slug: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("name : Some (\"uq_counter_views_slug\""));
    assert!(expanded.contains("columns : vec ! [\"slug\""));
    assert!(expanded.contains("unique : true"));
}

#[test]
fn expand_read_model_accepts_struct_compound_index_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("account_summaries")]
        #[index(name = "idx_account_summaries_owner_created", columns = ["owner", "created_at"])]
        struct AccountSummary {
            #[id("account_id")]
            id: String,
            owner: String,
            #[column("created_at_utc")]
            created_at: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("name : Some (\"idx_account_summaries_owner_created\""));
    assert!(expanded.contains("columns : vec ! [\"owner\" . to_string () , \"created_at_utc\""));
    assert!(expanded.contains("unique : false"));
}

#[test]
fn expand_read_model_accepts_struct_compound_unique_attribute() {
    let input: DeriveInput = syn::parse_quote! {
        #[table("accounts")]
        #[unique(columns = ["tenant_id", "slug"])]
        struct AccountSummary {
            #[id("account_id")]
            id: String,
            tenant_id: String,
            slug: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("name : Some (\"uq_accounts_tenant_id_slug\""));
    assert!(expanded.contains("columns : vec ! [\"tenant_id\""));
    assert!(expanded.contains("\"slug\""));
    assert!(expanded.contains("unique : true"));
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
fn expand_read_model_rejects_missing_id_field_for_document_models() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView {
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("missing id field should return an error");

    assert!(
        err.to_string().contains("field named `id`"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_allows_composite_relational_models_without_string_id() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "player_weapons", primary_key = ["player_id", "weapon_id"])]
        struct PlayerWeapon {
            #[readmodel(foreign_key = "players.player_id", delegated_from = "Player.player_id")]
            player_id: String,
            weapon_id: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("impl distributed :: RelationalReadModel for PlayerWeapon"));
    assert!(!expanded.contains("impl distributed :: ReadModel for PlayerWeapon"));
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
fn expand_read_model_rejects_unknown_field_attributes() {
    let input: DeriveInput = syn::parse_quote! {
        struct CounterView {
            #[readmodel(ide)]
            id: String,
            value: i32,
        }
    };

    let err = expand_read_model(input).expect_err("unknown field attribute should fail");

    assert!(
        err.to_string()
            .contains("unknown readmodel field attribute"),
        "unexpected error: {err}"
    );
}

#[test]
fn expand_read_model_accepts_relationship_metadata_before_relationship_kind() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "players")]
        struct Player {
            #[readmodel(id, column = "player_id")]
            id: String,
            #[readmodel(foreign_key = "player_id", has_many = "PlayerWeapon")]
            weapons: Vec<PlayerWeapon>,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("RelationshipKind :: HasMany"));
    assert!(expanded.contains("foreign_key : Some (\"player_id\""));
}

#[test]
fn expand_read_model_accepts_through_before_many_to_many() {
    let input: DeriveInput = syn::parse_quote! {
        #[readmodel(table = "players")]
        struct Player {
            #[readmodel(id, column = "player_id")]
            id: String,
            #[readmodel(through = "player_weapon_links", foreign_key = "player_id", many_to_many = "Weapon")]
            weapons: Vec<Weapon>,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(expanded.contains("RelationshipKind :: ManyToMany"));
    assert!(expanded.contains("through : Some (\"player_weapon_links\""));
    assert!(expanded.contains("foreign_key : Some (\"player_id\""));
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

#[test]
fn expand_read_model_shares_natural_default_between_document_and_relational_metadata() {
    let input: DeriveInput = syn::parse_quote! {
        struct BlobGames {
            #[id]
            game_id: String,
            #[index]
            owner_id: String,
        }
    };

    let expanded = expand_read_model(input).unwrap().to_string();

    assert!(
        expanded.contains("const COLLECTION : & 'static str = \"blob_games\"")
            && expanded.contains("table_name : \"blob_games\"")
    );
}
