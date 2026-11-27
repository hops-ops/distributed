#!/usr/bin/env python3
"""Turn each test's merged profile into the repository lines it executed.

Usage: export.py WORK_DIR   (run from the repository root; run.sh does this)

Reads WORK_DIR/list.json (`cargo nextest list --message-format json`) and
WORK_DIR/profiles/*/ (one directory per test, written by runner.sh). Writes:

- WORK_DIR/hits/<n>.json: {"binary", "test", "hits": {path: [line, ...]}}
  for every test with a profile, paths relative to the repository root;
- WORK_DIR/instrumented.json: {path: [line, ...]}, every line any exported
  binary instruments;
- WORK_DIR/export_failures.txt: tests whose profile could not be exported.

Needs FOOTPRINTS_LLVM_COV (the toolchain's llvm-cov).
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from multiprocessing import Pool

ROOT = os.getcwd() + "/"
IGNORE = r"(/\.cargo/|/rustc/|/\.rustup/|/target/)"
LLVM_COV = os.environ.get("FOOTPRINTS_LLVM_COV", "")

_extra_objects: list[str] = []
_sent: dict[str, set[int]] = {}  # per worker: instrumented lines already reported


def parse_lcov(text: str, root: str = ROOT) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """(hit lines, instrumented lines) per repository file in an lcov report.
    Files outside `root` and under its `target/` are dropped."""
    hits: dict[str, set[int]] = {}
    instr: dict[str, set[int]] = {}
    cur_hits = cur_instr = None
    for line in text.splitlines():
        if line.startswith("SF:"):
            path = line[3:]
            cur_hits = cur_instr = None
            if path.startswith(root) and not path.startswith(root + "target/"):
                rel = path[len(root):]
                cur_hits = hits.setdefault(rel, set())
                cur_instr = instr.setdefault(rel, set())
        elif cur_instr is not None and line.startswith("DA:"):
            number, count = line[3:].split(",")[:2]
            cur_instr.add(int(number))
            if count != "0":
                cur_hits.add(int(number))
    return {f: h for f, h in hits.items() if h}, instr


def _init(extra: list[str]) -> None:
    global _extra_objects
    _extra_objects = extra


def _export(args: tuple[int, str, str]):
    index, profile_dir, hits_dir = args
    with open(os.path.join(profile_dir, "test")) as f:
        binary_id, test, binary = f.read().split("\n")[:3]
    profdata = os.path.join(profile_dir, "t.profdata")
    if not os.path.exists(profdata):
        return binary_id, test, "no merged profile (killed, or merge failed)", {}
    cmd = [LLVM_COV, "export", "-format=lcov", "-instr-profile", profdata, "-ignore-filename-regex", IGNORE, binary]
    for obj in _extra_objects:
        if obj != binary:
            cmd += ["-object", obj]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return binary_id, test, "llvm-cov export: " + proc.stderr.strip()[:300], {}
    hits, instr = parse_lcov(proc.stdout)
    with open(os.path.join(hits_dir, f"{index}.json"), "w") as f:
        json.dump({"binary": binary_id, "test": test, "hits": {p: sorted(ls) for p, ls in hits.items()}}, f)
    new = {}
    for path, lines in instr.items():
        seen = _sent.setdefault(path, set())
        fresh = lines - seen
        if fresh:
            seen |= fresh
            new[path] = fresh
    return binary_id, test, None, new


def non_test_binaries(listing: dict) -> list[str]:
    meta = listing["rust-build-meta"]
    target = meta["target-directory"]
    return sorted(
        os.path.join(target, b["path"])
        for binaries in meta.get("non-test-binaries", {}).values()
        for b in binaries
    )


def main(work: str) -> int:
    if not LLVM_COV:
        sys.exit("FOOTPRINTS_LLVM_COV is not set")
    with open(os.path.join(work, "list.json")) as f:
        extra = non_test_binaries(json.load(f))
    hits_dir = os.path.join(work, "hits")
    os.makedirs(hits_dir, exist_ok=True)
    dirs = sorted(os.path.dirname(p) for p in glob.glob(os.path.join(work, "profiles", "*", "test")))
    instrumented: dict[str, set[int]] = {}
    failures = []
    jobs = [(i, d, hits_dir) for i, d in enumerate(dirs)]
    with Pool(os.cpu_count(), initializer=_init, initargs=(extra,)) as pool:
        for binary_id, test, error, new in pool.imap_unordered(_export, jobs, chunksize=4):
            if error:
                failures.append(f"{binary_id} {test}: {error}")
            for path, lines in new.items():
                instrumented.setdefault(path, set()).update(lines)
    with open(os.path.join(work, "instrumented.json"), "w") as f:
        json.dump({p: sorted(ls) for p, ls in sorted(instrumented.items())}, f)
    with open(os.path.join(work, "export_failures.txt"), "w") as f:
        f.writelines(line + "\n" for line in sorted(failures))
    print(f"exported {len(dirs) - len(failures)} test profiles, {len(failures)} without coverage")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
