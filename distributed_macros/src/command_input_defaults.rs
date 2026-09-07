//! Compile declaration-owned generators for canonical command input fields.
//!
//! Separately authored `command_effects!` / `command_confirmations!` macros were
//! removed: optimistic cache layers come from mutation IR lowering; causal
//! confirmation derives from `.emits` + portable/modeled event handlers.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parenthesized, Ident, Path, Result, Token};

mod keyword {
    syn::custom_keyword!(input);
    syn::custom_keyword!(default);
}

pub fn expand_input_defaults(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match syn::parse::<CommandInputDefaults>(input) {
        Ok(defaults) => match crate::shared::framework_path() {
            Ok(framework) => defaults.expand(framework).into(),
            Err(error) => error.to_compile_error().into(),
        },
        Err(error) => error.to_compile_error().into(),
    }
}

struct CommandInputDefaults {
    input: Path,
    defaults: Vec<InputDefault>,
}

impl Parse for CommandInputDefaults {
    fn parse(stream: ParseStream<'_>) -> Result<Self> {
        stream.parse::<keyword::input>()?;
        stream.parse::<Token![:]>()?;
        let input = stream.parse()?;
        stream.parse::<Token![;]>()?;

        let mut defaults = Vec::new();
        let mut fields = std::collections::BTreeSet::new();
        while !stream.is_empty() {
            stream.parse::<keyword::default>()?;
            let default = InputDefault::parse(stream)?;
            if !fields.insert(default.field.to_string()) {
                return Err(syn::Error::new(
                    default.field.span(),
                    format!("duplicate generated input default `{}`", default.field),
                ));
            }
            defaults.push(default);
            stream.parse::<Token![;]>()?;
        }
        if defaults.is_empty() {
            return Err(stream.error("command_input_defaults! requires at least one default"));
        }
        Ok(Self { input, defaults })
    }
}

impl CommandInputDefaults {
    fn expand(self, framework: TokenStream) -> TokenStream {
        let input = self.input;
        let defaults = self
            .defaults
            .into_iter()
            .map(|default| default.expand(&input, &framework));
        quote! {
            #framework::command::__command_input_defaults::<#input>(
                vec![#(#defaults),*]
            )
        }
    }
}

enum InputDefaultGenerator {
    UuidV7,
    Ulid,
}

struct InputDefault {
    field: Ident,
    generator: InputDefaultGenerator,
}

impl InputDefault {
    fn parse(stream: ParseStream<'_>) -> Result<Self> {
        let input_ident: Ident = stream.parse()?;
        if input_ident != "input" {
            return Err(syn::Error::new(
                input_ident.span(),
                "generated defaults must target `input.field`",
            ));
        }
        stream.parse::<Token![.]>()?;
        let field = stream.parse()?;
        stream.parse::<Token![=]>()?;
        let generator: Ident = stream.parse()?;
        let arguments;
        parenthesized!(arguments in stream);
        if !arguments.is_empty() {
            return Err(arguments.error("input-default generators take no arguments"));
        }
        let generator = match generator.to_string().as_str() {
            "uuid_v7" => InputDefaultGenerator::UuidV7,
            "ulid" => InputDefaultGenerator::Ulid,
            _ => {
                return Err(syn::Error::new(
                    generator.span(),
                    "unknown input-default generator; expected uuid_v7() or ulid()",
                ));
            }
        };
        Ok(Self { field, generator })
    }

    fn expand(self, input: &Path, framework: &TokenStream) -> TokenStream {
        let marker_name = format_ident!(
            "__Distributed{}EffectInputField_{}",
            input.segments.last().unwrap().ident,
            self.field,
            span = self.field.span()
        );
        let marker = marker_path(input, marker_name);
        match self.generator {
            InputDefaultGenerator::UuidV7 => quote! {
                #framework::command::__input_default_uuid_v7::<#input, #marker>()
            },
            InputDefaultGenerator::Ulid => quote! {
                #framework::command::__input_default_ulid::<#input, #marker>()
            },
        }
    }
}

fn marker_path(base: &Path, marker: Ident) -> Path {
    let mut path = base.clone();
    path.segments
        .last_mut()
        .expect("syn paths always contain at least one segment")
        .ident = marker;
    path
}
