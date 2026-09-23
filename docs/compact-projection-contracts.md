# Shared projection material in application manifests

Several exact event selectors can apply the same mutation. This is common when
retained state events and current events update the same read model. The
`projection!` authoring form already accepts multiple events per mutation;
application manifests now share identical operation lists and selector body
schemas when that reduces their encoded size. No authoring change is required.

The modeled projection's `program` value uses the explicit
`shared_projection_program_v1` encoding when smaller than the expanded form.
Its `program` retains all original fields and every exact arm, replacing only
`operations` with `operations_ref` and selector `body_schema` with
`body_schema_ref`. These zero-based references address the wrapper's canonical
`operation_sets` and `body_schemas` tables. Operations are interned by their
complete canonical JSON, including ordering, IDs, expressions and effects;
schemas are interned by exact string equality. No historical selector is
removed, merged, treated as current, or exempted from replay validation.

The runtime projection IR, program and binding IDs, server execution and client
projection-program exports are unchanged. The application Surface's artifact
fingerprints change deterministically when shared storage is used. Rebuild the
service and generated clients together. Existing expanded manifests remain accepted
and retain their existing encoding and fingerprints when decoded and re-encoded.

Tools reading the opaque modeled `program` JSON can use
`distributed::application::expand_projection_program_contract(&value)` to read
either representation. Expansion returns exactly the original program JSON.
Unsupported encodings, invalid references, inline/reference ambiguity, duplicate
or unused table entries, and noncanonical table order fail validation.

The complete encoded application manifest is bounded at 16 MiB, matching CLI
manifest ingress. This aggregate budget accommodates complete current and
retained-selector catalogs composed from many independently bounded contracts.
Older framework readers capped at 4 MiB reject larger artifacts; rebuild tooling
and applications together. Existing bytes, fingerprints and encoding versions
are unchanged for manifests that already fit.

Each opaque
modeled value remains bounded at 1 MiB; an expanded program also must fit the
existing 1 MiB budget. The decoder checks repeated byte costs before copying
shared values and validates programs individually, without retaining an expanded
copy of the whole application. Collection, string and depth limits remain
unchanged. Sharing is storage normalization, not a replacement for
retained-history replay.
