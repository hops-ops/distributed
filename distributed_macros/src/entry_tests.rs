#[cfg(test)]
mod tests {
    use crate::{aggregate, digest, enqueue, sourced};
    use quote::quote;
    use syn::parse::Parser;

    // ---- digest ----------------------------------------------------------

    #[test]
    fn expand_digest_inserts_digest_call() {
        let attr = quote! { "initialized" };
        let item = quote! {
            fn initialize(&mut self, id: String) {
                self.id = id;
            }
        };
        let out = digest::expand_digest(attr, item).unwrap().to_string();
        assert!(out.contains("digest"), "unexpected output: {out}");
        assert!(out.contains("\"initialized\""), "unexpected output: {out}");
    }

    #[test]
    fn parse_digest_args_rejects_unknown_key() {
        let attr = quote! { "x", versoin = 2 };
        let err = digest::parse_digest_args
            .parse2(attr)
            .err()
            .expect("unknown key should error");
        let msg = err.to_string();
        assert!(msg.contains("unsupported key `versoin`"), "got: {msg}");
        assert!(msg.contains("version"), "got: {msg}");
    }

    #[test]
    fn parse_digest_args_accepts_version_and_when() {
        let attr = quote! { entity, "renamed", when = true, version = 2 };
        let args = digest::parse_digest_args.parse2(attr).unwrap();
        assert_eq!(args.entity_field, "entity");
        assert!(args.guard.is_some());
        assert!(args.version.is_some());
    }

    // ---- enqueue ---------------------------------------------------------

    #[test]
    fn parse_enqueue_args_defaults_entity_field() {
        let attr = quote! { "order.initialized" };
        let args = enqueue::parse_enqueue_args.parse2(attr).unwrap();
        assert_eq!(args.emitter_field, "emitter");
        assert_eq!(args.entity_field, "entity");
    }

    #[test]
    fn parse_enqueue_args_overrides_entity_field() {
        let attr = quote! { "order.initialized", entity = state };
        let args = enqueue::parse_enqueue_args.parse2(attr).unwrap();
        assert_eq!(args.entity_field, "state");
    }

    #[test]
    fn expand_enqueue_uses_renamed_entity_field_in_guard() {
        let attr = quote! { "order.initialized", entity = state };
        let item = quote! {
            fn create(&mut self, id: String) {
                self.id = id;
            }
        };
        let out = enqueue::expand_enqueue(attr, item).unwrap().to_string();
        assert!(
            out.contains("self . state . is_replaying"),
            "expected renamed entity guard, got: {out}"
        );
    }

    #[test]
    fn parse_enqueue_args_rejects_unknown_key() {
        let attr = quote! { "x", wen = true };
        let err = enqueue::parse_enqueue_args
            .parse2(attr)
            .err()
            .expect("unknown key should error");
        assert!(
            err.to_string().contains("unsupported key `wen`"),
            "got: {err}"
        );
    }

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
    fn expand_sourced_generates_enum_and_aggregate() {
        let attr = quote! { entity };
        let item = quote! {
            impl Todo {
                #[event("initialized")]
                pub fn initialize(&mut self, id: String) {
                    self.id = id;
                }
            }
        };
        let out = sourced::expand_sourced(attr, item).unwrap().to_string();
        assert!(out.contains("enum TodoEvent"), "got: {out}");
        assert!(
            out.contains("impl distributed :: Aggregate for Todo"),
            "got: {out}"
        );
    }

    #[test]
    fn expand_sourced_places_state_capture_after_transition_body() {
        let attr = quote! {
            entity,
            aggregate_type = "todo",
            domain_state = TodoState,
        };
        let item = quote! {
            impl Todo {
                #[event("todo.completed", version = 1, domain)]
                pub fn complete(&mut self) {
                    self.completed = true;
                }
            }
        };

        let output = sourced::expand_sourced(attr, item).unwrap().to_string();
        let digest = output.find("digest_v").unwrap();
        let transition = output.find("self . completed = true").unwrap();
        let capture = output.find("capture_domain_state").unwrap();

        assert!(digest < transition);
        assert!(transition < capture);
        assert!(output.contains("is_replaying"));
        assert!(output.contains("pub enum TodoCompletedDomainEvent"));
        assert!(output.contains(
            "impl distributed :: domain_event :: DomainEventContract for TodoCompletedDomainEvent"
        ));
        assert!(output.contains("DomainEventBodyContract < TodoState >"));
    }

    #[test]
    fn expand_sourced_transition_exports_unconditional_known_state_values() {
        let attr = quote! {
            entity,
            aggregate_type = "todo",
            domain_state = TodoState,
        };
        let item = quote! {
            impl Todo {
                pub fn complete(&mut self) {
                    self.record_completed().unwrap();
                }

                #[event("todo.completed", version = 1, domain)]
                fn record_completed(&mut self) {
                    self.status = TodoStatus::Completed;
                    self.label = std::string::String::from("done");
                    self.shadowed_label = String::from("not-known");
                    self.assignee_id = None;
                    if self.audit_enabled {
                        self.audit_label = "conditional";
                    }
                }
            }
        };

        let output = sourced::expand_sourced(attr, item).unwrap().to_string();

        assert!(
            output.contains("fn command_event_known_values"),
            "got: {output}"
        );
        assert!(
            output.contains("__command_projection_state_known_values"),
            "got: {output}"
        );
        assert!(output.contains("\"status\""), "got: {output}");
        assert!(output.contains("TodoStatus :: Completed"), "got: {output}");
        assert!(output.contains("\"label\""), "got: {output}");
        assert!(
            output.contains("std :: string :: String :: from (\"done\")"),
            "got: {output}"
        );
        assert!(!output.contains("\"shadowed_label\""), "got: {output}");
        assert!(output.contains("\"assignee_id\""), "got: {output}");
        assert!(
            !output.contains("\"audit_label\""),
            "conditional assignments are not safe preview facts: {output}"
        );
    }

    #[test]
    fn sourced_helpers_ignore_other_receivers_and_nested_item_self() {
        let output = sourced::expand_sourced(
            quote! { entity, aggregate_type = "todo" },
            quote! {
                impl Todo {
                    pub fn unrelated(&mut self, other: &mut Todo) {
                        other.record_completed().unwrap();
                        struct Other;
                        impl Other {
                            fn action(&mut self) { self.record_completed(); }
                        }
                    }
                    #[event("todo.completed", domain = event)]
                    fn record_completed(&mut self) {}
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(!output.contains("pub enum Unrelated"), "{output}");
        assert!(!output.contains("pub mod domain_commands"), "{output}");
    }

    #[test]
    fn expand_sourced_identity_mode_generates_independent_public_descriptor() {
        let attr = quote! {
            entity,
            aggregate_type = "todo",
        };
        let item = quote! {
            impl Todo {
                #[event("todo.renamed", version = 2, domain = event)]
                pub fn rename(&mut self, title: String) {
                    self.title = title;
                }
            }
        };

        let output = sourced::expand_sourced(attr, item).unwrap().to_string();

        assert!(output.contains("pub struct TodoRenamedDomainEvent"));
        assert!(output.contains(
            "impl distributed :: domain_event :: DomainEventContract for TodoRenamedDomainEvent"
        ));
        assert!(output.contains("DomainEventBodyDescriptor :: distributed_json"));
        assert!(output.contains("capture_domain_event"));
        assert!(!output.contains("payload_bytes"));
    }

    #[test]
    fn expand_sourced_deletion_uses_aggregate_sequence_as_incarnation() {
        let attr = quote! {
            entity,
            aggregate_type = "todo",
        };
        let item = quote! {
            impl Todo {
                #[event("todo.purged", domain = deleted)]
                pub fn purge(&mut self) {
                    self.purged = true;
                }
            }
        };

        let output = sourced::expand_sourced(attr, item).unwrap().to_string();

        assert!(output.contains("pub struct TodoDomainIdentity"));
        assert!(output.contains("pub enum TodoPurgedDomainEvent"));
        assert!(output.contains("DomainEventBodyContract < distributed :: DomainDeletion"));
        assert!(output.contains("self . entity . version ()"));
        assert!(output.contains("capture_domain_deletion"));
        assert!(!output.contains("capture_domain_state"));
    }

    #[test]
    fn expand_sourced_custom_mode_type_checks_a_pure_function_pointer() {
        let attr = quote! {
            entity,
            events = "TodoReplayEvent",
            aggregate_type = "todo",
        };
        let item = quote! {
            impl Todo {
                #[event(
                    "todo.completed",
                    domain = with(TodoCompleted, TodoCompleted::capture_after)
                )]
                pub fn complete(&mut self) {
                    self.completed = true;
                }
            }
        };

        let output = sourced::expand_sourced(attr, item).unwrap().to_string();

        assert!(output.contains(
            "fn (& Todo , & TodoReplayEvent) -> TodoCompleted = TodoCompleted :: capture_after"
        ));
        assert!(output.contains("if let Some"));
        assert!(output.contains("capture_domain_event"));
        assert!(output.contains("DomainEventContract > :: EVENT_NAME"));
        assert!(output.contains("DomainEventContract > :: EVENT_VERSION"));
        assert!(output.contains("DomainEventBodyContract < T >"));
        assert!(output.contains("!= < TodoCompleted as distributed :: DomainEvent > :: DESCRIPTOR"));
    }

    #[test]
    fn expand_sourced_rejects_duplicate_event_names() {
        let attr = quote! { entity };
        let item = quote! {
            impl Todo {
                #[event("done")]
                pub fn complete(&mut self) {}
                #[event("done")]
                pub fn finish(&mut self) {}
            }
        };
        let err = sourced::expand_sourced(attr, item).expect_err("duplicate should error");
        assert!(
            err.to_string().contains("duplicate #[event] name `done`"),
            "got: {err}"
        );
    }

    #[test]
    fn expand_sourced_rejects_variant_ident_collisions() {
        let attr = quote! { entity };
        let item = quote! {
            impl Workflow {
                #[event("user.completed")]
                pub fn user_completed(&mut self) {}
                #[event("admin.completed")]
                pub fn admin_completed(&mut self) {}
            }
        };
        let err = sourced::expand_sourced(attr, item).expect_err("variant collision should error");
        let msg = err.to_string();
        assert!(msg.contains("`user.completed`"), "got: {msg}");
        assert!(msg.contains("`admin.completed`"), "got: {msg}");
        assert!(msg.contains("`Completed`"), "got: {msg}");
    }

    #[test]
    fn expand_sourced_rejects_tuple_parameter_pattern() {
        let attr = quote! { entity };
        let item = quote! {
            impl Point {
                #[event("moved")]
                pub fn moved(&mut self, (x, y): (u8, u8)) {
                    self.x = x;
                    self.y = y;
                }
            }
        };
        let err = sourced::expand_sourced(attr, item).expect_err("tuple pattern should error");
        assert!(
            err.to_string()
                .contains("unsupported parameter pattern in #[event] method"),
            "got: {err}"
        );
    }

    #[test]
    fn expand_digest_rejects_wildcard_parameter_pattern() {
        let attr = quote! { "initialized" };
        let item = quote! {
            fn initialize(&mut self, _: String) {}
        };
        let err = digest::expand_digest(attr, item).expect_err("wildcard pattern should error");
        assert!(
            err.to_string()
                .contains("unsupported parameter pattern in #[digest] method"),
            "got: {err}"
        );
    }

    #[test]
    fn expand_enqueue_rejects_struct_parameter_pattern() {
        let attr = quote! { "order.initialized" };
        let item = quote! {
            fn create(&mut self, Payload { id }: Payload) {
                let _ = id;
            }
        };
        let err = enqueue::expand_enqueue(attr, item).expect_err("struct pattern should error");
        assert!(
            err.to_string()
                .contains("unsupported parameter pattern in #[enqueue] method"),
            "got: {err}"
        );
    }

    #[test]
    fn expand_sourced_rejects_event_without_receiver() {
        let attr = quote! { entity };
        let item = quote! {
            impl Todo {
                #[event("initialized")]
                pub fn initialize(id: String) {}
            }
        };
        let err = sourced::expand_sourced(attr, item).expect_err("missing receiver should error");
        assert!(
            err.to_string().contains("must take a `&mut self` receiver"),
            "got: {err}"
        );
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

    #[test]
    fn expand_aggregate_accepts_upcasters_block() {
        let input = quote! {
            Todo, entity {
                "initialized"(id) => initialize,
            }
            upcasters [
                ("initialized", 1 => 2, OldPayload => NewPayload, upcast_fn),
            ]
        };
        let out = aggregate::expand_aggregate(input).unwrap().to_string();
        assert!(out.contains("upcasters"), "got: {out}");
        assert!(out.contains("upcast_fn"), "got: {out}");
    }

    // ---- aggregate -------------------------------------------------------

    #[test]
    fn expand_aggregate_generates_impl() {
        let input = quote! {
            Todo, entity {
                "initialized"(id) => initialize,
            }
        };
        let out = aggregate::expand_aggregate(input).unwrap().to_string();
        assert!(
            out.contains("impl distributed :: Aggregate for Todo"),
            "got: {out}"
        );
        assert!(out.contains("replay_event"), "got: {out}");
    }

    #[test]
    fn expand_portable_command_emits_thin_mount() {
        let input = quote! {
            name: "todo.complete",
            transition: domain_commands::Complete,
            aggregate: Todo,
            input: TodoCompleteInput,
            outcome: Eventual<TodoStatusPayload>,
            shard: |input| input.todo_id.clone(),
            load: required,
            roles: ["user", "admin"],
            field: "todos_complete",
            invoke: |todo, _input, principal| todo.complete(principal),
            payload: |todo| TodoStatusPayload::from_todo(&**todo),
        };
        let out = crate::portable_command::expand(input)
            .expect("expand")
            .to_string();
        assert!(out.contains("struct Complete"), "{out}");
        assert!(out.contains("fn complete"), "{out}");
        assert!(out.contains("todo . complete"), "{out}");
        assert!(out.contains("load_by"), "{out}");
        assert!(out.contains("eventual"), "{out}");
        assert!(out.contains("PortableCommand"), "{out}");
    }

    #[test]
    fn expand_portable_command_preserves_contract_projection_options() {
        let input = quote! {
            name: "chat.post",
            transition: domain_commands::Post,
            aggregate: ChatMessage,
            input: ChatPostInput,
            outcome: Eventual<ChatPostPayload>,
            shard: |input| input.message_id.clone(),
            roles: ["user", "admin"],
            field: "chat_messages_post",
            constructor: post_message,
            authenticated_user_field: (
                ChatMessagePostedDomainEvent,
                ChatMessageState,
                "author_id"
            ),
            preview_reduce_known_record: blob_preview(),
            guard: authenticated_user,
            handle: handle_post,
        };
        let out = crate::portable_command::expand(input)
            .expect("expand")
            .to_string();
        assert!(out.contains("authenticated_user_field"), "{out}");
        assert!(out.contains("fn post_message"), "{out}");
        assert!(out.contains("ChatMessagePostedDomainEvent"), "{out}");
        assert!(out.contains("ChatMessageState"), "{out}");
        assert!(out.contains("author_id"), "{out}");
        assert!(out.contains("preview_reduce_known_record"), "{out}");
        assert!(out.contains("blob_preview"), "{out}");
        assert!(out.contains("declared_in_rust_package"), "{out}");
        assert!(out.contains("CARGO_PKG_NAME"), "{out}");
    }

    #[test]
    fn expand_portable_command_allows_constructor_override_for_keyword_name() {
        let input = quote! {
            name: "blob.move",
            transition: domain_commands::MoveDir,
            aggregate: BlobGame,
            input: BlobMoveInput,
            outcome: Atomic<BlobGames>,
            shard: |input| input.game_id.clone(),
            roles: ["user", "admin"],
            field: "blob_games_move",
            constructor: move_dir,
            guard: authenticated_user,
            handle: handle_move,
        };
        let out = crate::portable_command::expand(input)
            .expect("expand")
            .to_string();
        assert!(out.contains("struct Move"), "{out}");
        assert!(out.contains("fn move_dir"), "{out}");
    }

    #[test]
    fn expand_portable_command_rejects_unknown_key() {
        let input = quote! {
            name: "todo.complete",
            nope: 1,
        };
        let err = crate::portable_command::expand(input).expect_err("unknown key");
        assert!(
            err.to_string()
                .contains("unknown portable_command key `nope`"),
            "got: {err}"
        );
    }
}
