#!/usr/bin/env python3
"""Fail when README.md, AGENTS.md or docs/ point at a repo path that does not exist.

Checked, in every file named below:

- Relative Markdown links `[text](target)`: the target file or directory
  exists, and a `#fragment` names a heading in the target Markdown file
  (GitHub's anchor rules).
- Backticked repo paths such as `src/bus/nats.rs` or `tests/e2e-ui/Makefile`:
  the path exists, from the repository root or from the document's directory.
  A trailing `:line`, `::symbol` or `#fragment` is ignored.

External URLs, placeholders (`<name>`, `…`) and globs are skipped. The
pre-release review reports under docs/reviews/ are dated snapshots that cite
code as it was, so they are not checked.

Usage: scripts/check-doc-paths.py [FILE.md ...]   (from anywhere in the repo)
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GLOBS = ["README.md", "AGENTS.md", "docs/**/*.md"]
EXCLUDED_DIRS = [ROOT / "docs" / "reviews"]

# A backticked token is treated as a repo path when its first segment is one of
# these top-level entries (or it is one of the top-level files).
TOP_LEVEL_DIRS = {
    ".github",
    "distributed_cli",
    "distributed_game",
    "distributed_game_macros",
    "distributed_leaderboard",
    "distributed_macros",
    "docs",
    "examples",
    "js",
    "migrations",
    "scripts",
    "src",
    "tests",
}
# Paths a document names on purpose although they are not in the tree: build
# outputs that CI produces and uploads.
GENERATED_PATHS = {"tests/gateway-worker/artifacts/load-report.json"}

TOP_LEVEL_FILES = {"Cargo.toml", "Cargo.lock", "Makefile", "compose.yaml", "AGENTS.md", "README.md"}

LINK = re.compile(r"(?<!\!)\[(?:[^\]\[]|\[[^\]]*\])*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
IMAGE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)\)")
CODE_SPAN = re.compile(r"`([^`\n]+)`")
FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")


def github_anchor(heading: str, seen: dict[str, int]) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    out = []
    for ch in text:
        if ch in " -":
            out.append("-" if ch == " " else ch)
        elif ch == "_" or ch.isalnum() or unicodedata.category(ch).startswith("M"):
            out.append(ch)
    slug = "".join(out)
    count = seen.get(slug, 0)
    seen[slug] = count + 1
    return slug if count == 0 else f"{slug}-{count}"


_anchor_cache: dict[Path, set[str]] = {}


def anchors(path: Path) -> set[str]:
    if path not in _anchor_cache:
        seen: dict[str, int] = {}
        found: set[str] = set()
        in_fence = False
        for line in path.read_text(encoding="utf-8").splitlines():
            if FENCE.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            match = HEADING.match(line)
            if match:
                found.add(github_anchor(match.group(2), seen))
            for explicit in re.findall(r'<a\s+(?:name|id)="([^"]+)"', line):
                found.add(explicit)
        _anchor_cache[path] = found
    return _anchor_cache[path]


def is_skippable(target: str) -> bool:
    return (
        re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I) is not None  # http:, mailto:, …
        or target.startswith("//")
        or any(mark in target for mark in ("<", ">", "…", "*", "{", "}", "$"))
    )


def check_link(doc: Path, target: str, problems: list[str], where: str) -> None:
    if is_skippable(target):
        return
    path_part, _, fragment = target.partition("#")
    if path_part:
        resolved = (doc.parent / path_part).resolve()
        if path_part.startswith("/"):
            resolved = (ROOT / path_part.lstrip("/")).resolve()
        if not resolved.exists():
            problems.append(f"{where}: link target does not exist: {target}")
            return
    else:
        resolved = doc
    if fragment and resolved.is_file() and resolved.suffix == ".md":
        if fragment not in anchors(resolved):
            problems.append(f"{where}: no heading for #{fragment} in {resolved.relative_to(ROOT)}")


def candidate_path(token: str) -> str | None:
    token = token.strip()
    if " " in token or is_skippable(token):
        return None
    token = re.sub(r"(::[A-Za-z_].*|:\d+(?:[-,]\d+)*|#.*)$", "", token)
    token = token.rstrip(".,;")
    if token.startswith("./"):
        token = token[2:]
    if token.startswith("/"):
        token = token[1:]
    if not token:
        return None
    if token in TOP_LEVEL_FILES:
        return token
    first = token.split("/", 1)[0]
    if "/" in token and first in TOP_LEVEL_DIRS:
        # `src/graphql` style module paths and crate-relative files alike.
        if re.fullmatch(r"[A-Za-z0-9_./@+\[\]-]+", token) and ".." not in token:
            return token
    return None


def check_code_span(doc: Path, token: str, problems: list[str], where: str) -> None:
    path = candidate_path(token)
    if path is None or path in GENERATED_PATHS:
        return
    if (ROOT / path).exists() or (doc.parent / path).exists():
        return
    problems.append(f"{where}: path does not exist: {path}")


def documents(args: list[str]) -> list[Path]:
    if args:
        return [Path(a).resolve() for a in args]
    found: list[Path] = []
    for pattern in DEFAULT_GLOBS:
        found.extend(sorted(ROOT.glob(pattern)))
    return [
        p
        for p in dict.fromkeys(found)
        if p.is_file() and not any(p.is_relative_to(d) for d in EXCLUDED_DIRS)
    ]


def main(argv: list[str]) -> int:
    problems: list[str] = []
    docs = documents(argv[1:])
    for doc in docs:
        in_fence = False
        for number, line in enumerate(doc.read_text(encoding="utf-8").splitlines(), 1):
            where = f"{doc.relative_to(ROOT)}:{number}"
            if FENCE.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for target in IMAGE.findall(line):
                check_link(doc, target, problems, where)
            for target in LINK.findall(line):
                check_link(doc, target, problems, where)
            for token in CODE_SPAN.findall(line):
                check_code_span(doc, token, problems, where)
    for problem in problems:
        print(problem)
    print(
        f"check-doc-paths: {len(docs)} files, {len(problems)} problem(s)",
        file=sys.stderr,
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
