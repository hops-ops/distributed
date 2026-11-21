#!/usr/bin/env python3
"""Fail if the UI/auth consumer starts depending on a native runtime or executor."""
from pathlib import Path
import subprocess
import sys

manifest = Path(__file__).resolve().with_name("Cargo.toml")
for target in (None, "wasm32-unknown-unknown"):
    command = ["cargo", "tree", "--manifest-path", str(manifest), "--locked", "--edges", "normal", "--prefix", "none"]
    command += sys.argv[1:]
    if target:
        command += ["--target", target]
    output = subprocess.check_output(command, text=True)
    packages = {line.split()[0] for line in output.splitlines() if line.strip()}
    forbidden = {"async-graphql", "async-graphql-axum", "axum", "sqlx", "sqlx-core", "tokio", "reqwest", "tonic", "worker"}
    leaked = sorted(packages & forbidden)
    if leaked:
        raise SystemExit(f"{target or 'native'}: forbidden runtime dependencies: {', '.join(leaked)}")
    print(f"{target or 'native'}: portable dependency boundary passed")
