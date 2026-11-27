# e2e-ui Cluster GitOps tree

This is the project-level Cluster desired state. The directory names describe
ownership only; Hops resolves Kubernetes readiness and exact object identity,
then records an inventory before pruning a removed source.

```text
cluster/
  registry/        # local package registry KRM (TLS bytes stay external)
  providers/       # Provider, ProviderConfig, DRC, and provider RBAC
  configurations/  # Crossplane Configuration packages
  functions/       # Crossplane Function packages
  platform/        # local platform KRM (PSQLStack + serving PSQLCluster + AuthStack)
  shared/          # cluster-shared resources
  rbac/            # cluster-level access needed by local packages
```

The Crossplane Helm seed is the only imperative substrate prerequisite.
`../cluster.yaml` declares no `spec.controlPlane`, so hops installs its
default (`crossplane-stable/crossplane` 2.4.0 in hops-cli v0.57.1). Application charts live under their own
`.gitops/local` and `.gitops/deploy` roots.

The environment's identity chart uses the auth-stack `HumanUser` and `Grant`
XRs. The pinned `ghcr.io/hops-ops/auth-stack:v1.9.0` package provides those
APIs, so a clean cluster can reconcile this tree without a source checkout.

The provider service accounts in `rbac/provider-accounts.yaml` are deliberately
cluster-admin because this disposable Cluster may install arbitrary explicit
Helm/Kubernetes platform resources. Use this tree only with a single-user local
cluster, never a shared or production context.
