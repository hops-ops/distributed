"""Per-crate line coverage and the TOTAL, from `cargo llvm-cov report --json
--summary-only` (`make coverage-local` writes it).

Usage: python3 -B scripts/coverage/summary.py SUMMARY_JSON [--markdown]

llvm-cov reports files and one total, no crates. A file belongs to the
workspace member whose directory is the longest prefix of its path (the root
package owns what no other member does). The TOTAL is llvm-cov's own.

Exits 1 when the report holds a file the footprint classifier calls test or
example code: the ignore regex (`ignore_regex.py`) and the classifier then
disagree, and the number would count test code.
"""

from __future__ import annotations

import json
import os
import sys
import tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "test-footprints"))

from classify import PROD, class_of, classify, load_sources  # noqa: E402

NOTE = (
    "Production code only: files the footprint classifier "
    "(scripts/test-footprints/classify.py) calls test or example code are "
    "excluded. Inline #[cfg(test)] blocks inside production files still count, "
    "so this sits slightly above the footprint tool's figure; doctests are not "
    "run. distributed_macros and distributed_game_macros are proc-macro crates: "
    "their figure is their expansion during the build plus their own unit tests."
)


def members(root: str) -> dict[str, str]:
    """Workspace member directory ("" for the root) -> package name."""
    def name(directory: str) -> str:
        with open(os.path.join(root, directory, "Cargo.toml"), "rb") as f:
            return tomllib.load(f)["package"]["name"]

    with open(os.path.join(root, "Cargo.toml"), "rb") as f:
        manifest = tomllib.load(f)
    out = {d.rstrip("/"): name(d) for d in manifest["workspace"]["members"]}
    if "package" in manifest:
        out[""] = manifest["package"]["name"]
    return out


def crate_of(rel: str, dirs: dict[str, str]) -> str:
    for directory in sorted(dirs, key=len, reverse=True):
        if directory == "" or rel.startswith(directory + "/"):
            return dirs[directory]
    return "(outside the workspace)"


def summarize(report: dict, root: str) -> tuple[dict[str, list[int]], dict, list[str]]:
    """Per crate [count, covered], llvm-cov's line totals, and the reported
    files the classifier does not call production code."""
    dirs = members(root)
    classes = classify(load_sources(root))
    data = report["data"][0]
    crates: dict[str, list[int]] = {}
    stray = []
    for entry in data["files"]:
        path = os.path.realpath(entry["filename"])
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        if rel.startswith("../"):
            crate = "(outside the workspace)"
        else:
            crate = crate_of(rel, dirs)
            if class_of(rel, classes)[0] != PROD:
                stray.append(rel)
        lines = entry["summary"]["lines"]
        acc = crates.setdefault(crate, [0, 0])
        acc[0] += lines["count"]
        acc[1] += lines["covered"]
    return crates, data["totals"]["lines"], sorted(stray)


def percent(covered: int, count: int) -> str:
    return f"{100.0 * covered / count:.2f}%" if count else "-"


def render(crates: dict[str, list[int]], total: dict, markdown: bool) -> str:
    rows = [(c, n, k) for c, (n, k) in sorted(crates.items())]
    rows.append(("TOTAL", total["count"], total["covered"]))
    if markdown:
        out = ["| Crate | Lines | Missed | Cover |", "|---|---:|---:|---:|"]
        out += [f"| {c} | {n} | {n - k} | {percent(k, n)} |" for c, n, k in rows]
        out[-1] = out[-1].replace("| TOTAL |", "| **TOTAL** |")
        return "\n".join(out) + "\n\n" + NOTE + "\n"
    out = [f"{'crate':<26}{'lines':>9}{'missed':>9}{'cover':>9}"]
    out += [f"{c:<26}{n:>9}{n - k:>9}{percent(k, n):>9}" for c, n, k in rows]
    return "\n".join(out) + "\n\n" + NOTE + "\n"


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if a != "--markdown"]
    if len(args) != 1:
        print("usage: summary.py SUMMARY_JSON [--markdown]", file=sys.stderr)
        return 2
    root = os.path.realpath(os.path.join(HERE, "..", ".."))
    with open(args[0], encoding="utf-8") as f:
        report = json.load(f)
    crates, total, stray = summarize(report, root)
    print(render(crates, total, "--markdown" in argv), end="")
    if stray:
        print("coverage: the report counts test code the ignore regex missed:", file=sys.stderr)
        for rel in stray:
            print("  " + rel, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
