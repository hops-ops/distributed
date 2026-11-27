#!/usr/bin/env python3
"""The Worker gateway links portable control contracts, never a native server."""
from pathlib import Path
import subprocess
manifest = Path(__file__).resolve().with_name("Cargo.toml")
output = subprocess.check_output(["cargo", "tree", "--manifest-path", str(manifest), "--locked", "--target", "wasm32-unknown-unknown", "--edges", "normal", "--prefix", "none"], text=True)
packages = {line.split()[0] for line in output.splitlines() if line.strip()}
forbidden = {"async-graphql", "async-graphql-axum", "sqlx", "sqlx-core", "axum", "reqwest", "tonic", "async-nats", "lapin", "rdkafka"}
assert not packages & forbidden, sorted(packages & forbidden)
assert "worker" in packages
# workers-rs itself uses Tokio's feature-free utility types. A native runtime,
# networking or timer feature must never become enabled in the Wasm graph.
features = subprocess.check_output(["cargo", "tree", "--manifest-path", str(manifest), "--locked", "--target", "wasm32-unknown-unknown", "--edges", "normal", "--prefix", "none", "--format", "{p}|{f}"], text=True)
for line in features.splitlines():
    if line.startswith("tokio "):
        assert not line.split("|", 1)[1].strip().replace("(*)", "").strip(), line
print("Worker gateway dependency boundary passed: no native server, SQL, or domain bus")
