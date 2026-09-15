# Unsigned command inputs and optimistic projections

Use the Rust type that expresses the domain boundary:

```rust,ignore
#[derive(serde::Deserialize, distributed::CommandInput)]
pub struct UpdateDocumentInput {
    pub document_id: String,
    pub expected_revision: u64,
}
```

`CommandInput` and `CommandOutput` retain unsigned width automatically, including
optional fields and list elements. A field remains GraphQL `BigInt` and TypeScript
`number`. Its generated command codec carries the narrower unsigned contract:

| Rust type | Command codec | Browser range |
| --- | --- | --- |
| `u8` | `uint8` | 0–255 |
| `u16` | `uint16` | 0–65,535 |
| `u32` | `uint32` | 0–4,294,967,295 |
| `u64` | `uint64_safe_integer` | 0–9,007,199,254,740,991 |

`usize` follows the target's pointer width. Fixed-width types are preferable
for portable domain contracts.

For example, an event preview can bind `input.expected_revision` to a typed
`u64` event field. The manifest compiler now proves that assignment from the
unsigned codec, and the generated command prepares the same integer in its
optimistic projection and transport input. No browser annotation or handwritten
optimistic mutation is needed.

Command preparation rejects negative numbers, negative zero, fractions,
non-finite numbers, strings, and numbers above the codec's maximum before any
optimistic effect or dispatch. The same validation applies to generated command
results and trusted presets. Explicit trusted presets targeting U64 projection
slots must declare an unsigned codec; an unrestricted signed `BigInt` preset
cannot establish that proof.

JavaScript numbers cannot represent every `u64` exactly. The browser boundary
deliberately stops at `Number.MAX_SAFE_INTEGER`. Rust command execution retains
the full unsigned range, and projection constants and stored aggregate state
retain their existing full-width U64 representation. Signed command codecs and
SQL read-model scalar codecs are unchanged.

## Regeneration

This changes command contract and protocol fingerprints. Rebuild the service
and generated clients together with `distributed build` or `distributed dev`.
Mixed old/new artifacts fail the existing fingerprint check.

Code that manually constructs `CommandTypeField` or `SurfaceTypeField` must
set `unsigned_integer` to the matching `CommandUnsignedInteger` variant, or
`None` for unrefined fields. Prefer derives so this metadata follows the Rust
type. A refinement on a non-`BigInt` field is rejected.

Typed application manifests preserve `unsigned_integer` in canonical Surface
command input and output contracts, including nested types. Decoding and
re-encoding retains the same contract bytes and fingerprints. Unrefined fields
omit this metadata, preserving their existing canonical representation.
