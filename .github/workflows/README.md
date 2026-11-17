# Distributed shared GitHub Actions workflows

Reusable workflows **for Distributed consumers** (domain crates, services that
depend on the framework). This monorepo’s own PR/main gates stay on the
existing unbounded-tech quality provider plus the integration/* jobs below —
`quality.yaml` is **not** wired into `on-pr-quality` / `on-push-main` here.

## Catalog

| Workflow | `workflow_call` | Audience |
|----------|-----------------|----------|
| [`quality.yaml`](./quality.yaml) | yes | **Consumers:** fmt → clippy → build → test + coverage (sticky PR comment) |
| [`test-all-features.yaml`](./test-all-features.yaml) | yes | **This repo:** workspace `--all-features` |
| [`integration-*.yaml`](./) | yes | **This repo:** broker / DB / CLI / observability / GraphQL identity+OIDC |
| [`integration-e2e-ui.yaml`](./integration-e2e-ui.yaml) | yes | **This repo:** `tests/e2e-ui` offline suite, compose Playwright e2e, and kind local-GitOps Playwright e2e |
| [`integration-celld.yaml`](./integration-celld.yaml) | yes | **This repo:** `tests/e2e-celld` workspace tests, celld storage and cell-alarm proofs (`celld dev`), live celld+NATS |
| [`integration-wasm32-domains.yaml`](./integration-wasm32-domains.yaml) | yes | **This repo:** every domain crate builds for `wasm32-unknown-unknown` (`scripts/check-wasm32-domains.sh`), and a native-only fixture must fail |
| [`integration-js.yaml`](./integration-js.yaml) | yes | **This repo:** install, typecheck, test, build, and packed-consumer smoke test for `js/` |
| [`on-pr-quality.yaml`](./on-pr-quality.yaml) | entry | **This repo** PR gate (not the consumer quality contract) |
| [`on-push-main-version-and-tag.yaml`](./on-push-main-version-and-tag.yaml) | entry | **This repo** main → **vnext** tag |
| [`on-v-tag-publish.yaml`](./on-v-tag-publish.yaml) | entry | **This repo** crates.io + npm + `distributed` binary release |

**Version tagging (anywhere):** `unbounded-tech/workflow-vnext-tag`  
**GitHub Release only (domain crates):** `unbounded-tech/workflow-simple-release`

## Domain crate recipe (private libs)

```yaml
# .github/workflows/on-pr-quality.yaml
name: Rust Quality Pipeline for PRs
on:
  pull_request:
    branches: [main]
permissions:
  contents: read
  pull-requests: write
jobs:
  quality:
    permissions:
      contents: read
      pull-requests: write  # coverage sticky comment on PRs
    uses: hops-ops/distributed/.github/workflows/quality.yaml@main  # or @feat/shared-workflows while landing
    with:
      cargo_build_args: "--verbose --locked"
      cargo_test_args: "--verbose --locked"
```

```yaml
# .github/workflows/on-push-main-version-and-tag.yaml
name: On Push to Main, Version and Tag For Release
on:
  push:
    branches: [main]
permissions:
  contents: write
  packages: write
  pull-requests: write
jobs:
  quality:
    permissions:
      contents: read
      pull-requests: write
    uses: hops-ops/distributed/.github/workflows/quality.yaml@main
    with:
      cargo_build_args: "--verbose --locked"
      cargo_test_args: "--verbose --locked"
  version-and-tag:
    needs: [quality]
    uses: unbounded-tech/workflow-vnext-tag/.github/workflows/workflow.yaml@v1.21.5
    secrets:
      DEPLOY_KEY: ${{ secrets.DEPLOY_KEY }}
    with:
      useDeployKey: true
      rust: true
```

```yaml
# .github/workflows/on-v-tag-release.yaml
name: On Version Tagged, Create GitHub Release
on:
  push:
    tags: ["v*.*.*"]
permissions:
  contents: write
jobs:
  release:
    uses: unbounded-tech/workflow-simple-release/.github/workflows/workflow.yaml@v2.1.3
    with:
      tag: ${{ github.ref_name }}
      name: ${{ github.ref_name }}
```

**Secrets:** write `DEPLOY_KEY` via `vnext generate-deploy-key --owner … --name …`.

**Pinning:** prefer a released tag once cut (`@vX.Y.Z` or git SHA). During development of quality, `@feat/shared-workflows` is fine.

## npm release setup

Version tags publish the package under [`js/`](../../js/) to npm as
`@hops-ops/distributed`. The tag must have the exact form `vX.Y.Z`; the workflow
uses `X.Y.Z` for both `js/package.json` and `js/package-lock.json`, runs the same
package quality gate used on pull requests, and publishes with npm provenance.

Configure an npm **Trusted Publisher** for `@hops-ops/distributed` with these
exact GitHub Actions coordinates:

- Organization or user: `hops-ops`
- Repository: `distributed`
- Workflow filename: `on-v-tag-publish.yaml`
- Environment: `npm`
- Allowed action: `npm publish`

Trusted Publisher configuration is available only after the npm package exists.
One-time bootstrap (already completed for this repo):

1. Publish a throwaway non-release version such as `0.0.0` for
   `@hops-ops/distributed` on the public npm registry.
2. Configure the npm Trusted Publisher using the exact coordinates above
   (`npm trust github @hops-ops/distributed --file on-v-tag-publish.yaml
   --repo hops-ops/distributed --env npm --allow-publish`).
3. Create the GitHub `npm` deployment environment (optional protection rules:
   tag pattern `v*`, required reviewers).

Release preflight validates the tag form and that `@hops-ops/distributed`
exists on the public registry before any npm or crates.io write starts. Tagged
npm releases authenticate through GitHub Actions OIDC. The workflow
intentionally has job-scoped `id-token: write`, uses npm 11.5.1 or newer, and
does **not** require or accept a long-lived `NPM_TOKEN` repository secret.

## Why this monorepo doesn’t call `quality.yaml`

The framework workspace needs a different gate: default-features tests via
unbounded quality (or similar), plus **all-features**, Postgres/NATS/Kafka/…
integrations, CLI/observability, and GraphQL identity/OIDC jobs. Consumer
domain crates are single packages (or small libs) and only need the reusable
quality contract.

## Out of scope (for now)

- crates.io publish reusable (framework still uses `unbounded-tech` publish helpers in `on-v-tag-publish.yaml`)
- image / GitOps promote (service scaffolds via `distributed`)
