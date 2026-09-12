mod aggregate;
mod application;
mod command;
mod command_input_defaults;
mod command_types;
mod digest;
mod domain_event;
mod domain_state;
mod enqueue;
mod module;
mod mutation;
mod portable_command;
// Event-owning `projection!` authoring removed (mutation projectors cutover).
mod read_model;
mod shared;
mod snapshot;
mod sourced;

use proc_macro::TokenStream;
use syn::DeriveInput;

/// Generate one typed command's portable contract and optional executable
/// mount from the same handler declaration.
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    command::expand(attr.into(), item.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

/// Domain-owned portable command mount (`PCH-DEC-001`).
///
/// Spec sketches used `command!`; that name is the handler attribute
/// [`command`]. This is the function-like form for shard + invoke + Eventual
/// (or a `handle:` escape hatch).
#[proc_macro]
pub fn portable_command(input: TokenStream) -> TokenStream {
    portable_command::expand(input.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

/// Register an explicit logical module.
#[proc_macro]
pub fn module(input: TokenStream) -> TokenStream {
    module::expand(input.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

/// Register an explicit application and its selected modules/surfaces.
#[proc_macro]
pub fn application(input: TokenStream) -> TokenStream {
    application::expand(input.into())
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

/// Attribute macro that automatically queues a local event for emission.
#[proc_macro_attribute]
pub fn enqueue(attr: TokenStream, item: TokenStream) -> TokenStream {
    enqueue::expand_enqueue(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Attribute macro that automatically inserts a digest call at the beginning of a method.
#[proc_macro_attribute]
pub fn digest(attr: TokenStream, item: TokenStream) -> TokenStream {
    digest::expand_digest(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Generates the Aggregate trait impl with replay logic.
#[proc_macro]
pub fn aggregate(input: TokenStream) -> TokenStream {
    aggregate::expand_aggregate(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Generate aggregate replay code and optionally capture typed outward domain events.
///
/// Configure a [`DomainState`] body once with
/// `#[sourced(entity, aggregate_type = "todo", domain_state = TodoState)]`.
/// Then `#[event("todo.completed", domain)]` captures the post-transition
/// state, `domain = event` re-encodes the typed method arguments,
/// `domain = deleted` captures the aggregate identity, and
/// `domain = with(Output, adapter)` invokes a typed post-transition adapter.
/// An unmarked `#[event]` remains aggregate-history-only.
#[proc_macro_attribute]
pub fn sourced(attr: TokenStream, item: TokenStream) -> TokenStream {
    sourced::expand_sourced(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive macro for the `ReadModel` trait.
#[proc_macro_derive(
    ReadModel,
    attributes(readmodel, collection, table, column, id, index, unique)
)]
pub fn derive_read_model(input: TokenStream) -> TokenStream {
    read_model::derive_read_model(input)
}

/// Derive macro that generates a snapshot struct, `fn snapshot()`, and
/// `impl Snapshottable` for an aggregate.
#[proc_macro_derive(Snapshot, attributes(snapshot))]
pub fn derive_snapshot(input: TokenStream) -> TokenStream {
    snapshot::derive_snapshot(input)
}

/// Derive the independently versioned public state-body descriptor.
///
/// Use `#[domain_state(version = N)]`; the semantic outward event name belongs
/// on each domain-marked `#[event(...)]`, not on the state DTO.
#[proc_macro_derive(DomainState, attributes(domain_state, serde))]
pub fn derive_domain_state(input: TokenStream) -> TokenStream {
    domain_state::derive_domain_state(input)
}

/// Derive a typed explicit outward domain-event descriptor.
///
/// Use `#[domain_event(name = "todo.completed", version = N)]` for adapter
/// output DTOs whose public contract differs from aggregate replay data.
#[proc_macro_derive(DomainEvent, attributes(domain_event, serde))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    domain_event::derive_domain_event(input)
}

/// Compile declaration-owned generators for canonical command input fields.
#[proc_macro]
pub fn command_input_defaults(input: TokenStream) -> TokenStream {
    command_input_defaults::expand_input_defaults(input)
}

/// Compile an event-independent read-model mutation program.
///
/// Mutations never name events. Portable handlers bind events to mutation
/// inputs. Generated capabilities are internal; they are **not** GraphQL
/// schema fields.
///
/// GraphQL-looking form (preferred surface):
///
/// ```ignore
/// mutation! {
///     mutation SaveTodo {
///         upsert_todos(object: $input.todo)
///     }
/// }
/// ```
///
/// Classic sugar still works:
///
/// ```ignore
/// mutation! {
///     name: "SaveTodo";
///     version: 1;
///     upsert Todos from input.todo;
/// }
/// ```
#[proc_macro]
pub fn mutation(input: TokenStream) -> TokenStream {
    mutation::expand(input)
}

/// Load a GraphQL-looking mutation document from a path relative to
/// `CARGO_MANIFEST_DIR` (e.g. `src/mutations/save_todo.mutation.graphql`).
///
/// Syntax-only: compiles to the same `MutationProgram` IR as [`mutation!`].
/// Not a public GraphQL schema field.
///
/// ```ignore
/// pub fn SaveTodo() -> Mutation<()> {
///     mutation_file!("src/mutations/save_todo.mutation.graphql")
/// }
/// ```
#[proc_macro]
pub fn mutation_file(input: TokenStream) -> TokenStream {
    mutation::expand_file(input)
}

/// Derive transport-neutral command input metadata from Serde field shapes.
#[proc_macro_derive(CommandInput, attributes(serde))]
pub fn derive_command_input(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match command_types::expand_command_input(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive transport-neutral command output metadata from Serde field shapes.
#[proc_macro_derive(CommandOutput, attributes(serde))]
pub fn derive_command_output(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match command_types::expand_command_output(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[cfg(test)]
mod entry_tests;
