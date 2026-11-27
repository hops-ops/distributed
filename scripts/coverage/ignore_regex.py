"""Print the `--ignore-filename-regex` that drops test code from a coverage
report, derived from the footprint tool's classifier.

Usage: python3 -B scripts/coverage/ignore_regex.py [ROOT]

`scripts/test-footprints/classify.py` decides which files are test code
(`tests/`, `benches/`, `examples/`, `tests.rs`, `*_tests.rs`, `test_*.rs`,
modules declared under `#[cfg(test)]`, ...). This prints one POSIX extended
regex matching exactly the files it classifies as test or example code under
ROOT (default: the repository root), anchored at ROOT's absolute path. A
directory holding no production file is matched as a whole (`tests/`); any
other test file is matched by its full path.

What a file name cannot express stays in the number: an inline
`#[cfg(test)] mod tests { ... }` inside a production file counts as
production lines (the footprint tool subtracts those; this report cannot).
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "test-footprints"))

from classify import PROD, classify, load_sources  # noqa: E402

# Characters with a meaning in a POSIX extended regex (llvm-cov's dialect).
_ERE_SPECIAL = set(".[]()*+?{}|^$\\")


def ere_escape(text: str) -> str:
    return "".join("\\" + c if c in _ERE_SPECIAL else c for c in text)


def excluded(classes: dict[str, tuple[str, frozenset[int]]]) -> list[str]:
    """The fewest paths covering every non-production file: a directory
    (ending in `/`) when no production file lies under it, else the file."""
    prod_dirs = set()
    for path, (kind, _) in classes.items():
        if kind == PROD:
            parts = path.split("/")
            prod_dirs.update("/".join(parts[:i]) for i in range(len(parts)))
    out = set()
    for path, (kind, _) in classes.items():
        if kind == PROD:
            continue
        parts = path.split("/")
        for i in range(1, len(parts)):
            directory = "/".join(parts[:i])
            if directory not in prod_dirs:
                out.add(directory + "/")
                break
        else:
            out.add(path)
    return sorted(out)


def regex(root: str, paths: list[str]) -> str:
    dirs = [ere_escape(p) for p in paths if p.endswith("/")]
    files = [ere_escape(p) for p in paths if not p.endswith("/")]
    alternatives = []
    if dirs:
        alternatives.append("(" + "|".join(dirs) + ")")
    if files:
        alternatives.append("(" + "|".join(files) + ")$")
    if not alternatives:
        raise SystemExit("ignore_regex: no test file found under " + root)
    return "^" + ere_escape(root.rstrip("/")) + "/(" + "|".join(alternatives) + ")"


def main(argv: list[str]) -> int:
    root = os.path.realpath(argv[1] if len(argv) > 1 else os.path.join(HERE, "..", ".."))
    print(regex(root, excluded(classify(load_sources(root)))))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
