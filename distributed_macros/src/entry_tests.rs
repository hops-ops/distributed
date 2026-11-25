#[cfg(test)]
mod tests {
    use crate::{aggregate, sourced};
    use quote::quote;
    use syn::parse::Parser;

    // ---- digest ----------------------------------------------------------

    // ---- enqueue ---------------------------------------------------------

    // ---- event (parse) ---------------------------------------------------

    #[test]
    fn parse_event_args_rejects_unknown_key() {
        let attr = quote! { "completed", wen = true };
        let err = sourced::parse_event_args
            .parse2(attr)
            .err()
            .expect("unknown key should error");
        assert!(
            err.to_string().contains("unsupported key `wen`"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_event_args_keeps_replay_names_separate_from_domain_names() {
        sourced::parse_event_args
            .parse2(quote! { "private.*", version = 0 })
            .expect("unmarked replay events do not use public domain-event constraints");
    }

    #[test]
    fn parse_event_args_validates_domain_name_and_version() {
        let name_error = sourced::parse_event_args
            .parse2(quote! { "public.*", domain })
            .err()
            .expect("domain event names must be publication-safe");
        assert!(
            name_error.to_string().contains("wildcard"),
            "got: {name_error}"
        );

        let version_error = sourced::parse_event_args
            .parse2(quote! { "todo.completed", version = 0, domain })
            .err()
            .expect("domain event versions must be nonzero");
        assert!(
            version_error.to_string().contains("greater than zero"),
            "got: {version_error}"
        );
    }

    // ---- sourced ---------------------------------------------------------

    #[test]
    fn parse_sourced_args_requires_entity_field() {
        let attr = quote! {};
        let err = sourced::parse_sourced_args
            .parse2(attr)
            .err()
            .expect("missing entity should error");
        assert!(
            err.to_string().contains("requires the entity field name"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_sourced_args_rejects_unknown_key() {
        let attr = quote! { entity, evnts = "Foo" };
        let err = sourced::parse_sourced_args
            .parse2(attr)
            .err()
            .expect("unknown key should error");
        assert!(
            err.to_string().contains("unsupported key `evnts`"),
            "got: {err}"
        );
    }

    #[test]
    fn known_values_need_a_domain_mode() {
        let error = sourced::expand_sourced(
            quote! { entity, aggregate_type = "todo", domain_state = TodoState },
            quote! {
                impl Todo {
                    pub fn complete(&mut self) {
                        self.record_completed().unwrap();
                    }

                    #[event("todo.completed", version = 1, known(status: "completed"))]
                    fn record_completed(&mut self) {
                        self.status = TodoStatus::Completed;
                    }
                }
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("needs one of those"), "{error}");
    }

    // ---- upcasters -------------------------------------------------------

    /// Both `aggregate!` and `#[sourced]` route through the same
    /// `UpcasterDef` parser, so one grammar check covers both entry points.
    #[test]
    fn upcaster_def_parses_full_entry() {
        let input = quote! { ("initialized", 1 => 2, OldPayload => NewPayload, upcast_fn) };
        let def: aggregate::UpcasterDef = syn::parse2(input).unwrap();
        assert_eq!(def.event_name.value(), "initialized");
        assert_eq!(def.from_version.base10_parse::<u64>().unwrap(), 1);
        assert_eq!(def.to_version.base10_parse::<u64>().unwrap(), 2);
    }

    #[test]
    fn upcaster_def_rejects_missing_transform_fn() {
        let input = quote! { ("initialized", 1 => 2, OldPayload => NewPayload) };
        assert!(syn::parse2::<aggregate::UpcasterDef>(input).is_err());
    }

    #[test]
    fn parse_sourced_args_accepts_upcasters() {
        let attr = quote! {
            entity,
            upcasters(("initialized", 1 => 2, OldPayload => NewPayload, upcast_fn))
        };
        let args = sourced::parse_sourced_args.parse2(attr).unwrap();
        assert_eq!(args.upcasters.len(), 1);
    }

    // ---- aggregate -------------------------------------------------------

    fn thin_command(
        name: &str,
        load: proc_macro2::TokenStream,
        roles: proc_macro2::TokenStream,
    ) -> syn::Result<String> {
        crate::command::expand(quote! {
            name: #name,
            transition: domain_commands::Create,
            aggregate: Todo,
            input: TodoCreateInput,
            outcome: Eventual<TodoCreatePayload>,
            shard: |input| input.todo_id.clone(),
            load: #load,
            roles: #roles,
            field: "todos_create",
            guard: authenticated_user,
            invoke: |todo, input, principal| todo.create(&input.todo_id, principal),
            payload: |todo, principal| TodoCreatePayload::new(todo, principal),
        })
        .map(|tokens| tokens.to_string())
    }

    /// The method form and `command!` reject the same names and roles.
    #[test]
    fn expand_command_shares_name_and_role_rules_with_the_method_form() {
        for name in ["create", "Todo.create", "todo..create", "todo.create-now"] {
            let err = thin_command(name, quote!(create), quote!(["user"])).expect_err(name);
            assert!(
                err.to_string().contains("2+ dot-separated snake_case"),
                "{name}: {err}"
            );
        }
        for roles in [quote!([]), quote!([""]), quote!([user])] {
            let err = thin_command("todo.create", quote!(create), roles.clone())
                .expect_err("invalid roles");
            assert!(err.to_string().contains("roles"), "{roles}: {err}");
        }
    }

    #[test]
    fn expand_command_rejects_unknown_key() {
        let input = quote! {
            name: "todo.complete",
            nope: 1,
        };
        let err = crate::command::expand(input).expect_err("unknown key");
        assert!(
            err.to_string().contains("unknown command key `nope`"),
            "got: {err}"
        );
    }
}
