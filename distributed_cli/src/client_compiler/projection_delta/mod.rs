//! Compiler-owned command preview: an executable artifact IR whose values
//! remain scoped input/preset expressions until the JavaScript command
//! runtime prepares a command.

mod preview;

pub(crate) use preview::{compile_command_preview, CompiledCommandProjection};
