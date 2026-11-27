#!/usr/bin/env python3
"""Compare two `make test-footprints` output directories.

Usage: diff.py BASE CURRENT [--allowlist FILE]

Prints the production lines BASE covered that CURRENT does not, the tests
added and removed, and the change in total coverage. Exits 1 when a covered
line was lost, 0 otherwise, 2 on bad input.

Line numbers move when a file is edited (deleting a test in an inline
`#[cfg(test)] mod tests` shifts every line below it). Each output directory
holds the production sources it measured (sources.tar.gz), so a base line is
first mapped to its line in the current source: a line whose text is
unchanged is compared at its new number; a line that was edited or deleted
cannot be compared and is listed apart, without failing the diff.

The allowlist names lines whose coverage depends on timing (see
timing-dependent-lines.txt): `path:line` in BASE's numbering, a tab, and the
line's text, `#` comments allowed. An entry matches only when BASE's source
holds that text (stripped) at that line, so an entry that has moved stops
matching and the line is reported as lost again: stale entries fail loudly
rather than hide a loss elsewhere.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import os
import sys
import tarfile

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analyze import lines_file_name, sort_key, test_id  # noqa: E402


def read_lines(path: str) -> list[str]:
    with open(path) as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def read_sources(out_dir: str) -> dict[str, list[str]]:
    sources = {}
    with tarfile.open(os.path.join(out_dir, "sources.tar.gz")) as tar:
        for member in tar.getmembers():
            if member.isfile():
                sources[member.name] = tar.extractfile(member).read().decode("utf-8", "replace").split("\n")
    return sources


def read_tests(out_dir: str) -> dict[tuple[str, str], str]:
    """(binary, test) -> status."""
    with open(os.path.join(out_dir, "per_test.csv"), newline="") as f:
        return {(row["binary"], row["test"]): row.get("status", "") for row in csv.DictReader(f)}


def totals(out_dir: str) -> tuple[int, int]:
    lines = covered = 0
    with open(os.path.join(out_dir, "uncovered_by_file.csv"), newline="") as f:
        for row in csv.DictReader(f):
            lines += int(row["lines"])
            covered += int(row["covered"])
    return lines, covered


def line_map(old: list[str] | None, new: list[str] | None) -> dict[int, int]:
    """1-based old line -> new line, for every line in an unchanged block."""
    if old is None or new is None:
        return {}
    if old == new:
        return {i: i for i in range(1, len(old) + 1)}
    mapping = {}
    for tag, i1, i2, j1, _ in difflib.SequenceMatcher(None, old, new, autojunk=False).get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                mapping[i1 + k + 1] = j1 + k + 1
    return mapping


def read_allowlist(path: str | None) -> set[tuple[str, int, str]]:
    """(path, line, stripped text) of every allowlisted line."""
    if not path:
        return set()
    entries = set()
    for raw in read_lines(path):
        if raw.lstrip().startswith("#"):
            continue
        location, _, text = raw.partition("\t")
        entries.add((*sort_key(location), text.strip()))
    return entries


def compare(base: str, current: str, allowlist: set[tuple[str, int, str]]) -> dict:
    base_covered = read_lines(os.path.join(base, "covered_lines.txt"))
    current_covered = set(read_lines(os.path.join(current, "covered_lines.txt")))
    base_src, current_src = read_sources(base), read_sources(current)

    maps: dict[str, dict[int, int]] = {}
    lost, moved, changed, allowed = [], {}, [], []
    for line in base_covered:
        path, number = sort_key(line)
        if path not in maps:
            maps[path] = line_map(base_src.get(path), current_src.get(path) or _worktree(path))
        new_number = maps[path].get(number)
        if new_number is None:
            changed.append(line)
            continue
        now = f"{path}:{new_number}"
        if now in current_covered:
            continue
        text = base_src[path][number - 1].strip()
        if (path, number, text) in allowlist:
            allowed.append(now)
            continue
        lost.append(now)
        if new_number != number:
            moved[now] = line

    base_status, current_status = read_tests(base), read_tests(current)
    base_tests, current_tests = set(base_status), set(current_status)
    return {
        "unhealthy": {
            name: sorted(t for t, s in status.items() if s not in ("passed", ""))
            for name, status in (("BASE", base_status), ("CURRENT", current_status))
        },
        "lost": lost, "moved": moved, "changed": changed, "allowed": allowed,
        "removed": sorted(base_tests - current_tests), "added": sorted(current_tests - base_tests),
        "base_totals": totals(base), "current_totals": totals(current),
        "base_tests": base_tests,
    }


def _worktree(path: str) -> list[str] | None:
    """The file as it is now, for a path the current run measured no
    production lines in (it may since hold only test code)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().split("\n")
    except OSError:
        return None


def tests_covering(out_dir: str, tests: set[tuple[str, str]], lines: set[str]) -> dict[str, list[str]]:
    """For each of `lines`, the tests in `out_dir` whose footprint holds it."""
    found: dict[str, list[str]] = {line: [] for line in lines}
    for binary, test in sorted(tests):
        path = os.path.join(out_dir, "lines", lines_file_name(binary, test))
        if os.path.exists(path):
            for line in read_lines(path):
                if line in found:
                    found[line].append(test_id(binary, test))
    return found


def report(base: str, current: str, r: dict) -> str:
    out = []
    p = out.append
    (bl, bc), (cl, cc) = r["base_totals"], r["current_totals"]
    pct = lambda c, n: 100 * c / n if n else 0.0  # noqa: E731
    p(f"footprint diff: {base} -> {current}")
    p(f"production lines covered: {bc}/{bl} ({pct(bc, bl):.2f}%) -> {cc}/{cl} ({pct(cc, cl):.2f}%), "
      f"{cc - bc:+d} lines, {pct(cc, cl) - pct(bc, bl):+.2f} points")
    for name, tests in r["unhealthy"].items():
        if tests:
            p(f"WARNING: {len(tests)} tests in {name} failed or left no coverage; lines only they reach "
              f"can show as lost. Compare runs without failures.")
            for binary, test in tests:
                p(f"  {name}: {test_id(binary, test)}")
    p(f"tests: {len(r['removed'])} removed, {len(r['added'])} added")
    for title, tests in (("removed", r["removed"]), ("added", r["added"])):
        for binary, test in tests:
            p(f"  {title}: {test_id(binary, test)}")
    if r["allowed"]:
        p(f"lost but allowlisted as timing-dependent: {len(r['allowed'])}")
        for line in r["allowed"]:
            p(f"  {line}")
    if r["changed"]:
        p(f"covered before, source line edited or deleted since, not compared: {len(r['changed'])}")
        for line in r["changed"]:
            p(f"  {line} (base numbering)")
    p(f"LOST: production lines covered before and not now: {len(r['lost'])}")
    if r["lost"]:
        base_lines = {r["moved"].get(line, line) for line in r["lost"]}
        owners = tests_covering(base, r["base_tests"], base_lines)
        removed = {test_id(*t) for t in r["removed"]}
        for line in r["lost"]:
            was = r["moved"].get(line)
            where = f" (base {was})" if was else ""
            covering = owners[was or line]
            p(f"  {line}{where}  covered before by: {', '.join(covering) or '?'}")
            if covering and not removed.intersection(covering):
                p("    (no test that covered it was removed: the tests changed, or the line is timing-dependent)")
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("base")
    parser.add_argument("current")
    parser.add_argument("--allowlist")
    args = parser.parse_args(argv)
    for d in (args.base, args.current):
        if not os.path.exists(os.path.join(d, "covered_lines.txt")):
            print(f"{d}: not a test-footprints output directory (no covered_lines.txt)", file=sys.stderr)
            return 2
    if os.path.realpath(args.base) == os.path.realpath(args.current):
        print("BASE and CURRENT are the same directory; copy the baseline aside first", file=sys.stderr)
        return 2
    r = compare(args.base, args.current, read_allowlist(args.allowlist))
    sys.stdout.write(report(args.base, args.current, r))
    return 1 if r["lost"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
