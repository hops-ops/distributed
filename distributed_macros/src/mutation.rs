//! `mutation!` macro: event-independent read-model mutation programs.
//!
//! GraphQL-looking, syntax-only documents (not public GraphQL schema fields):
//!
//! ```ignore
//! mutation! {
//!     mutation SaveTodo {
//!         upsert_todos(object: $input.todo)
//!     }
//! }
//! ```
//!
//! And file loading via [`mutation_file`]:
//!
//! ```ignore
//! mutation_file!("src/mutations/save_todo.mutation.graphql")
//! ```

use std::path::PathBuf;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{braced, parenthesized, Ident, LitInt, LitStr, Path, Result, Token};

pub(crate) fn expand(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as MutationDeclaration);
    expand_declaration(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Load a `.mutation.graphql` document relative to `CARGO_MANIFEST_DIR`.
///
/// The expansion `include_str!`s the file, so the compiler tracks it and an
/// edit to the document rebuilds the crate.
pub(crate) fn expand_file(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let path_lit = syn::parse_macro_input!(input as LitStr);
    let declaration = load_graphql_file(&path_lit).and_then(expand_declaration);
    match declaration {
        Ok(declaration) => quote! {
            {
                const _: &str = ::core::include_str!(
                    ::core::concat!(::core::env!("CARGO_MANIFEST_DIR"), "/", #path_lit)
                );
                #declaration
            }
        }
        .into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn load_graphql_file(path_lit: &LitStr) -> Result<MutationDeclaration> {
    let relative = path_lit.value();
    let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            path_lit.span(),
            "CARGO_MANIFEST_DIR is required to load mutation.graphql files",
        )
    })?;
    let full = PathBuf::from(manifest).join(&relative);
    let source = std::fs::read_to_string(&full).map_err(|error| {
        syn::Error::new(
            path_lit.span(),
            format!(
                "failed to read mutation file `{}` (resolved `{}`): {error}",
                relative,
                full.display()
            ),
        )
    })?;
    parse_graphql_document(&source, path_lit.span())
}

struct MutationDeclaration {
    name: LitStr,
    version: Option<LitInt>,
    operations: Vec<MutationOpSyntax>,
}

type GraphqlFieldBinding = (Ident, Vec<Ident>);

enum MutationOpSyntax {
    /// `upsert_model(object: $input.root)`
    Upsert { model: Path, input_root: Vec<Ident> },
    /// `delete_model_by_pk(field: $input.path, ...)`
    DeleteByPk {
        model: Path,
        keys: Vec<GraphqlFieldBinding>,
    },
    /// `update_model_by_pk(pk_columns: {..}, _set: {..})`
    UpdateByPk {
        model: Path,
        keys: Vec<GraphqlFieldBinding>,
        sets: Vec<GraphqlFieldBinding>,
    },
    /// `insert_model_one(object: $input.root)`
    InsertOne {
        model: Path,
        object_root: Vec<Ident>,
    },
}

impl Parse for MutationDeclaration {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.peek(Ident) && input.fork().parse::<Ident>()? == "mutation" {
            return parse_graphql_token_document(input);
        }
        Err(input.error("expected `mutation Name { upsert_model(object: $input.field) … }`"))
    }
}

/// Parse GraphQL-looking tokens inside `mutation! { mutation Name { ops } }`.
fn parse_graphql_token_document(input: ParseStream<'_>) -> Result<MutationDeclaration> {
    let mutation_kw: Ident = input.parse()?;
    if mutation_kw != "mutation" {
        return Err(syn::Error::new(mutation_kw.span(), "expected `mutation`"));
    }
    let name_ident: Ident = input.parse()?;
    let mut version: Option<u64> = None;
    if input.peek(Token![@]) {
        input.parse::<Token![@]>()?;
        let attr: Ident = input.parse()?;
        if attr != "version" {
            return Err(syn::Error::new(
                attr.span(),
                "only `@version(n)` is supported on GraphQL-looking mutations",
            ));
        }
        let version_content;
        parenthesized!(version_content in input);
        let lit: LitInt = version_content.parse()?;
        version = Some(lit.base10_parse()?);
    }
    let body;
    braced!(body in input);
    let mut operations = Vec::new();
    while !body.is_empty() {
        operations.push(parse_graphql_field_operation(&body)?);
        // Optional trailing commas between field ops.
        if body.peek(Token![,]) {
            body.parse::<Token![,]>()?;
        }
    }
    if operations.is_empty() {
        return Err(syn::Error::new(
            name_ident.span(),
            "GraphQL-looking mutation body requires at least one operation",
        ));
    }
    // Keep the GraphQL operation name as the program identity so projection
    // bindings (`mutation: SaveTodo`) match `mutation SaveTodo { … }`.
    let name = LitStr::new(&name_ident.to_string(), name_ident.span());
    let version_lit = version.map(|v| LitInt::new(&v.to_string(), name_ident.span()));
    Ok(MutationDeclaration {
        name,
        version: version_lit,
        operations,
    })
}

/// Parse a raw `.mutation.graphql` text document.
fn parse_graphql_document(source: &str, span: proc_macro2::Span) -> Result<MutationDeclaration> {
    // Strip line comments (`# …`).
    let mut cleaned = String::new();
    for line in source.lines() {
        let trimmed = line.split('#').next().unwrap_or("").trim_end();
        if !trimmed.is_empty() {
            cleaned.push_str(trimmed);
            cleaned.push('\n');
        }
    }
    let tokens: TokenStream = cleaned.parse().map_err(|error| {
        syn::Error::new(
            span,
            format!("invalid GraphQL-looking mutation tokens: {error}"),
        )
    })?;
    syn::parse2::<MutationDeclaration>(tokens).map_err(|error| {
        syn::Error::new(
            span,
            format!("failed to parse mutation.graphql document: {error}"),
        )
    })
}

/// `upsert_todos(object: $input.todo)` / `delete_todos_by_pk(todo_id: $input.todo_id)`.
/// The suffix is the read-model table name (snake_case), same as the query field.
fn parse_graphql_field_operation(input: ParseStream<'_>) -> Result<MutationOpSyntax> {
    let field: Ident = input.parse()?;
    let field_name = field.to_string();
    let args;
    parenthesized!(args in input);

    if let Some(model) = field_name.strip_prefix("upsert_") {
        let model = model_path_from_name(model, field.span())?;
        let object_root = parse_graphql_object_arg(&args)?;
        return Ok(MutationOpSyntax::Upsert {
            model,
            input_root: object_root,
        });
    }
    if let Some(model) = field_name
        .strip_prefix("insert_")
        .map(|rest| rest.strip_suffix("_one").unwrap_or(rest))
    {
        let model = model_path_from_name(model, field.span())?;
        let object_root = parse_graphql_object_arg(&args)?;
        return Ok(MutationOpSyntax::InsertOne { model, object_root });
    }
    if let Some(model) = field_name
        .strip_prefix("delete_")
        .and_then(|rest| rest.strip_suffix("_by_pk"))
    {
        let model = model_path_from_name(model, field.span())?;
        let keys = parse_graphql_key_args(&args)?;
        return Ok(MutationOpSyntax::DeleteByPk { model, keys });
    }
    if let Some(model) = field_name
        .strip_prefix("update_")
        .and_then(|rest| rest.strip_suffix("_by_pk"))
        .or_else(|| {
            field_name
                .strip_prefix("patch_")
                .and_then(|rest| rest.strip_suffix("_by_pk"))
        })
    {
        let model = model_path_from_name(model, field.span())?;
        let (keys, sets) = parse_graphql_update_args(&args)?;
        return Ok(MutationOpSyntax::UpdateByPk { model, keys, sets });
    }

    Err(syn::Error::new(
        field.span(),
        format!(
            "unsupported GraphQL-looking mutation field `{field_name}`; \
             expected upsert_todos, insert_todos[_one], delete_todos_by_pk, \
             or update_todos_by_pk (snake_case table name, same as the query field)"
        ),
    ))
}

fn model_path_from_name(name: &str, span: proc_macro2::Span) -> Result<Path> {
    if name.is_empty() {
        return Err(syn::Error::new(
            span,
            "model name missing from mutation field",
        ));
    }
    if name.starts_with('_') || name.ends_with('_') || name.contains("__") {
        return Err(syn::Error::new(
            span,
            format!(
                "`{name}` is not a valid read-model name \
                 (expected a snake_case table name or PascalCase model suffix)"
            ),
        ));
    }
    let pascal = snake_to_pascal(name);
    syn::parse_str::<Ident>(&pascal)
        .map(Path::from)
        .map_err(|_| {
            syn::Error::new(
                span,
                format!("`{name}` is not a valid read-model name (expected snake_case table name)"),
            )
        })
}

/// `chat_messages` → `ChatMessages`. A PascalCase suffix stays PascalCase.
fn snake_to_pascal(name: &str) -> String {
    name.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn parse_graphql_object_arg(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    let mut object_root = None;
    while !input.is_empty() {
        let key: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        if key != "object" {
            return Err(syn::Error::new(
                key.span(),
                "expected `object: $input…` argument",
            ));
        }
        object_root = Some(parse_graphql_input_path(input)?);
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
    }
    object_root.ok_or_else(|| input.error("upsert/insert requires `object: $input…`"))
}

fn parse_graphql_key_args(input: ParseStream<'_>) -> Result<Vec<GraphqlFieldBinding>> {
    let mut keys = Vec::new();
    while !input.is_empty() {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let path = parse_graphql_input_path(input)?;
        keys.push((name, path));
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
    }
    if keys.is_empty() {
        return Err(input.error("delete_by_pk requires at least one key argument"));
    }
    Ok(keys)
}

fn parse_graphql_update_args(
    input: ParseStream<'_>,
) -> Result<(Vec<GraphqlFieldBinding>, Vec<GraphqlFieldBinding>)> {
    let mut keys = Vec::new();
    let mut sets = Vec::new();
    while !input.is_empty() {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        if name == "_set" {
            let set_content;
            braced!(set_content in input);
            while !set_content.is_empty() {
                let field: Ident = set_content.parse()?;
                set_content.parse::<Token![:]>()?;
                let path = parse_graphql_input_path(&set_content)?;
                sets.push((field, path));
                if set_content.peek(Token![,]) {
                    set_content.parse::<Token![,]>()?;
                }
            }
        } else {
            let path = parse_graphql_input_path(input)?;
            keys.push((name, path));
        }
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
    }
    if keys.is_empty() || sets.is_empty() {
        return Err(input.error("update_by_pk requires key fields and `_set: { field: $input… }`"));
    }
    Ok((keys, sets))
}

/// `$input.todo`, `input.todo`, or `$todo_id` / bare paths.
fn parse_graphql_input_path(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    if input.peek(Token![$]) {
        input.parse::<Token![$]>()?;
    }
    parse_input_path(input)
}

fn parse_input_path(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    // Accept `input.foo.bar` or bare `foo.bar` paths.
    let first: Ident = input.parse()?;
    let mut path = vec![first];
    while input.peek(Token![.]) {
        input.parse::<Token![.]>()?;
        path.push(input.parse()?);
    }
    // Drop leading `input` segment; mutation IR paths are relative to the input object.
    if path.first().is_some_and(|segment| segment == "input") {
        path.remove(0);
    }
    if path.is_empty() {
        return Err(input.error("input path must select at least one field"));
    }
    Ok(path)
}

fn expand_declaration(decl: MutationDeclaration) -> Result<TokenStream> {
    let version = decl
        .version
        .map(|lit| lit.base10_parse::<u64>())
        .transpose()?
        .unwrap_or(1);
    let steps = decl.operations.iter().map(expand_step);
    let name_lit = decl.name;
    // The declaration is data. `Mutation::program` builds and validates it and
    // returns an error; nothing here can panic.
    Ok(quote! {
        {
            const __STEPS: &[::distributed::MutationStep] = &[#(#steps),*];
            ::distributed::Mutation::<()>::declare(#name_lit, #version, __STEPS)
        }
    })
}

fn path_literals(path: &[Ident]) -> Vec<LitStr> {
    path.iter()
        .map(|ident| LitStr::new(&ident.to_string(), ident.span()))
        .collect()
}

fn expand_inputs(bindings: &[GraphqlFieldBinding]) -> TokenStream {
    let inputs = bindings.iter().map(|(column, path)| {
        let column = LitStr::new(&column.to_string(), column.span());
        let path = path_literals(path);
        quote! {
            ::distributed::MutationInput { column: #column, path: &[#(#path),*] }
        }
    });
    quote! { &[#(#inputs),*] }
}

fn expand_step(operation: &MutationOpSyntax) -> TokenStream {
    let schema = |model: &Path| {
        quote! { <#model as ::distributed::RelationalReadModel>::schema }
    };
    match operation {
        MutationOpSyntax::Upsert { model, input_root } => {
            let model = schema(model);
            let object = path_literals(input_root);
            quote! {
                ::distributed::MutationStep::Upsert { model: #model, object: &[#(#object),*] }
            }
        }
        MutationOpSyntax::InsertOne { model, object_root } => {
            let model = schema(model);
            let object = path_literals(object_root);
            quote! {
                ::distributed::MutationStep::Insert { model: #model, object: &[#(#object),*] }
            }
        }
        MutationOpSyntax::DeleteByPk { model, keys } => {
            let model = schema(model);
            let key = expand_inputs(keys);
            quote! {
                ::distributed::MutationStep::Delete { model: #model, key: #key }
            }
        }
        MutationOpSyntax::UpdateByPk { model, keys, sets } => {
            let model = schema(model);
            let key = expand_inputs(keys);
            let set = expand_inputs(sets);
            quote! {
                ::distributed::MutationStep::Patch { model: #model, key: #key, set: #set }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::snake_to_pascal;

    #[test]
    fn snake_table_names_become_rust_type_names() {
        assert_eq!(snake_to_pascal("todos"), "Todos");
        assert_eq!(snake_to_pascal("chat_messages"), "ChatMessages");
        assert_eq!(snake_to_pascal("blob_games"), "BlobGames");
        assert_eq!(snake_to_pascal("auth_users"), "AuthUsers");
    }

    #[test]
    fn pascal_suffixes_stay_pascal() {
        assert_eq!(snake_to_pascal("Todos"), "Todos");
        assert_eq!(snake_to_pascal("ChatMessages"), "ChatMessages");
    }

    #[test]
    fn malformed_underscore_segments_are_rejected() {
        use proc_macro2::Span;
        assert!(super::model_path_from_name("todos_", Span::call_site()).is_err());
        assert!(super::model_path_from_name("_todos", Span::call_site()).is_err());
        assert!(super::model_path_from_name("chat__messages", Span::call_site()).is_err());
        assert!(super::model_path_from_name("chat_messages", Span::call_site()).is_ok());
        assert!(super::model_path_from_name("ChatMessages", Span::call_site()).is_ok());
    }
}
