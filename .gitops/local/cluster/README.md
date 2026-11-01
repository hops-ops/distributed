# Mob Pit cluster overlay

Packages the Mob Pit Environments need on the machine Cluster:

- `configurations/celld-stack.yaml`: `CelldStack` (celld fleet + RustFS),
  used by `mobpit-system`. Dependencies (provider-helm, provider-kubernetes,
  function-auto-ready) are already part of the machine Cluster, so
  dependency resolution is skipped (the AWS S3 provider is only needed for
  `spec.aws`).

`hops local up` from this checkout layers this directory over the machine
tree; the running Cluster controller then installs it.
