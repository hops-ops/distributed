#!/usr/bin/env python3
"""The native UI/auth adapter has no GraphQL/SQL or domain bus dependency."""
from pathlib import Path
import subprocess
manifest = Path(__file__).resolve().with_name("Cargo.toml")
output = subprocess.check_output(["cargo", "tree", "--manifest-path", str(manifest), "--locked", "--edges", "normal", "--prefix", "none"], text=True)
packages = {line.split()[0] for line in output.splitlines() if line.strip()}
forbidden = {"async-graphql", "async-graphql-axum", "sqlx", "sqlx-core", "tonic", "worker", "async-nats", "lapin", "rdkafka"}
assert not packages & forbidden, sorted(packages & forbidden)
print("native UI/auth dependency boundary passed")
