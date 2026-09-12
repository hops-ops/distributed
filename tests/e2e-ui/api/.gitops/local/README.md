# e2e-ui coherent application chart

This cluster-development chart renders one Deployment running
`distributed dev` for the complete editable application. The lifecycle owns
the Rust API, SvelteKit UI, generated clients, linked framework JavaScript,
and generation activation. Both Services enter the backend gateway on 8791:
the public UI Service retains port 5180 and the API Service uses 8791. The
gateway forwards UI/auth to same-pod SvelteKit at 127.0.0.1:5180; it never
uses the public UI Service as its upstream. PUBLIC_ORIGIN and Auth.js use
the public UI Service URL, so existing OIDC callbacks stay on that origin.

Hops supplies the source mount or sync delivery. A Node init container places
the pinned Node/npm toolchain beside the Rust toolchain without a custom local
image or chart-level package installer. The linked framework package pins and
installs its required `wasm-pack` compiler, so declared Rust/WASM pures are
built inside the same lifecycle too. Dependency and framework builds are owned
by `distributed dev`, not by chart shell scripts or a second UI Deployment.

OIDC project, demo humans, and web application resources live in the explicit
`ui/.gitops/test-users` chart. When identity is enabled, this chart consumes
the same generation-specific connection Secret for the API audience and the
UI client credentials.

Platform AuthStack, ProviderConfig, and the shared PSQLCluster stay under the
project `.gitops/local/cluster/` tree. Cloud images belong to the independent
`.gitops/deploy` chart.
