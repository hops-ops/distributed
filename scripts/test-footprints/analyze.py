#!/usr/bin/env python3
"""Per-test production-line footprints, from export.py's output.

Usage: analyze.py WORK_DIR OUT_DIR   (run from the repository root)

Reads WORK_DIR/{list.json,junit.xml,instrumented.json,export_failures.txt,
meta.txt,hits/*.json} and writes the files the testing guide describes into
OUT_DIR: per_test.csv, lines/, identical_groups.txt, cross_binary_pairs.csv,
covered_lines.txt, uncovered_by_file.csv, uncovered_ranges.txt, summary.txt
and sources.tar.gz (the production sources measured, for the diff).
"""

from __future__ import annotations

import collections
import csv
import glob
import hashlib
import heapq
import io
import json
import os
import shutil
import sys
import tarfile
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from classify import EXAMPLE, PROD, class_of, classify, load_sources  # noqa: E402

ROOT = os.getcwd()


# ------------------------------------------------------------- shared names

def test_id(binary: str, test: str) -> str:
    return f"{binary} {test}"


def lines_file_name(binary: str, test: str) -> str:
    """The name of a test's file under lines/: `<binary>__<test>.txt`, with
    `%` and `/` escaped, shortened with a hash past 200 characters."""
    name = f"{binary}__{test}".replace("%", "%25").replace("/", "%2F")
    if len(name) > 200:
        name = name[:180] + "-" + hashlib.sha256(name.encode()).hexdigest()[:16]
    return name + ".txt"


def sort_key(line: str) -> tuple[str, int]:
    path, _, number = line.rpartition(":")
    return path, int(number)


def ranges(numbers: list[int], skip: set[int] = frozenset()) -> list[tuple[int, int]]:
    """Runs of `numbers` (sorted), where a run may step over numbers in `skip`
    and over numbers in neither list."""
    out: list[tuple[int, int]] = []
    for n in numbers:
        if out and not any(m in skip for m in range(out[-1][1] + 1, n)):
            out[-1] = (out[-1][0], n)
        else:
            out.append((n, n))
    return out


# --------------------------------------------------------------- analysis

def footprint_hash(lines: list[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16] if lines else ""


def containers(hits: dict[str, frozenset], binary_of: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """For each test with a non-empty footprint: the smallest other test whose
    footprint contains it, and the smallest such test in another binary.
    An identical footprint counts only when the other test's id sorts first,
    so two identical tests never name each other."""
    count = collections.Counter()
    for h in hits.values():
        count.update(h)
    rarest = {t: min(h, key=lambda line: (count[line], line)) for t, h in hits.items() if h}
    by_line = collections.defaultdict(list)
    wanted = set(rarest.values())
    for t, h in hits.items():
        for line in h & wanted:
            by_line[line].append(t)
    single, cross = {}, {}
    for t, rare in rarest.items():
        h = hits[t]
        for other in sorted(by_line[rare], key=lambda o: (len(hits[o]), o)):
            if other == t or not h <= hits[other] or (hits[other] == h and other > t):
                continue
            single.setdefault(t, other)
            if binary_of[other] != binary_of[t]:
                cross[t] = other
                break
    return single, cross


def greedy_cover(hits: dict[str, frozenset], covered: set) -> list[str]:
    """Tests, in pick order, that together reach every covered line: each pick
    adds the most lines not yet reached (ties to the smaller id)."""
    remaining = set(covered)
    heap = [(-len(h), t) for t, h in hits.items() if h]
    heapq.heapify(heap)
    order = []
    while remaining and heap:
        _, t = heapq.heappop(heap)
        gain = len(hits[t] & remaining)
        if gain == 0:
            continue
        if heap and gain < -heap[0][0]:
            heapq.heappush(heap, (-gain, t))
            continue
        remaining -= hits[t]
        order.append(t)
    return order


def crate_of(path: str) -> str:
    return "distributed" if path.startswith("src/") else path.split("/")[0]


def module_of(path: str) -> str:
    parts = path.split("/")
    if parts[0] == "src":
        return "src/" + parts[1].removesuffix(".rs")
    return parts[0] + "/" + (parts[2].removesuffix(".rs") if len(parts) > 2 else "")


def group_of(binary: str, test: str, layer: str) -> str:
    parts = test.split("::")
    if layer == "unit":
        return binary + " unit/" + "::".join(parts[:2] if len(parts) > 2 else parts[:1])
    return binary + "/" + (parts[0] if len(parts) > 1 else "(root)")


def read_junit(path: str) -> tuple[dict[str, str], dict[str, float]]:
    status, seconds = {}, {}
    if not os.path.exists(path):
        return status, seconds
    for suite in ET.parse(path).getroot().iter("testsuite"):
        for case in suite.iter("testcase"):
            t = test_id(suite.get("name"), case.get("name"))
            seconds[t] = float(case.get("time") or 0)
            failed = case.find("failure") is not None or case.find("error") is not None
            status[t] = "failed" if failed else "passed"
    return status, seconds


# ------------------------------------------------------------------- main

def main(work: str, out: str) -> int:
    with open(os.path.join(work, "list.json")) as f:
        listing = json.load(f)
    kind_of = {b: s["kind"] for b, s in listing["rust-suites"].items()}
    listed = matched = 0
    for suite in listing["rust-suites"].values():
        for case in suite["testcases"].values():
            listed += 1
            matched += case["filter-match"]["status"] == "matches"

    classes = classify(load_sources(ROOT))
    with open(os.path.join(work, "instrumented.json")) as f:
        instrumented_raw = json.load(f)
    prod_lines: dict[str, set[int]] = {}
    example_lines: dict[str, set[int]] = {}
    for path, lines in instrumented_raw.items():
        kind, inline = class_of(path, classes)
        if kind == PROD:
            kept = set(lines) - inline
            if kept:
                prod_lines[path] = kept
        elif kind == EXAMPLE:
            example_lines[path] = set(lines)

    hits: dict[str, frozenset] = {}
    names: dict[str, tuple[str, str]] = {}
    example_hit: set[str] = set()
    for path in glob.glob(os.path.join(work, "hits", "*.json")):
        with open(path) as f:
            record = json.load(f)
        t = test_id(record["binary"], record["test"])
        names[t] = (record["binary"], record["test"])
        hit = set()
        for file, lines in record["hits"].items():
            if file in prod_lines:
                keep = prod_lines[file]
                hit.update(sys.intern(f"{file}:{n}") for n in lines if n in keep)
            elif file in example_lines:
                example_hit.update(f"{file}:{n}" for n in lines)
        hits[t] = frozenset(hit)

    status, seconds = read_junit(os.path.join(work, "junit.xml"))
    for t in status:
        if t not in names:
            binary, _, test = t.partition(" ")
            names[t] = (binary, test)
    binary_of = {t: b for t, (b, _) in names.items()}
    layer_of = {t: {"test": "integration", "bench": "bench"}.get(kind_of.get(b, ""), "unit") for t, b in binary_of.items()}
    no_coverage = sorted(t for t in names if t not in hits)
    failed = sorted(t for t, s in status.items() if s == "failed")

    covered = set().union(*hits.values()) if hits else set()
    count = collections.Counter()
    for h in hits.values():
        count.update(h)
    single, cross = containers(hits, binary_of)
    order = greedy_cover(hits, covered)
    in_cover = set(order)

    # ---- outputs
    if os.path.isdir(os.path.join(out, "lines")):
        shutil.rmtree(os.path.join(out, "lines"))
    os.makedirs(os.path.join(out, "lines"))
    sorted_lines = {}
    for t, h in hits.items():
        sorted_lines[t] = sorted(h, key=sort_key)
        with open(os.path.join(out, "lines", lines_file_name(*names[t])), "w") as f:
            f.writelines(line + "\n" for line in sorted_lines[t])

    with open(os.path.join(out, "per_test.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["binary", "test", "group", "layer", "production_lines", "unique_lines",
                    "single_containing_test", "in_minimal_cover", "seconds", "footprint_hash", "status"])
        for t in sorted(names, key=lambda t: names[t]):
            binary, test = names[t]
            h = hits.get(t, frozenset())
            w.writerow([
                binary, test, group_of(binary, test, layer_of[t]), layer_of[t], len(h),
                sum(1 for line in h if count[line] == 1), single.get(t, ""),
                "yes" if t in in_cover else "no", f"{seconds.get(t, 0):.3f}",
                footprint_hash(sorted_lines.get(t, [])),
                "no-coverage" if t not in hits else status.get(t, "unknown"),
            ])

    by_footprint = collections.defaultdict(list)
    for t, h in hits.items():
        if h:
            by_footprint[h].append(t)
    groups = sorted((sorted(ts) for ts in by_footprint.values() if len(ts) > 1),
                    key=lambda ts: (-len(hits[ts[0]]), ts[0]))
    with open(os.path.join(out, "identical_groups.txt"), "w") as f:
        for ts in groups:
            f.write(f"{len(hits[ts[0]])}\t" + "\t".join(ts) + "\n")

    with open(os.path.join(out, "cross_binary_pairs.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["test", "containing_test"])
        for t in sorted(cross):
            w.writerow([t, cross[t]])

    with open(os.path.join(out, "covered_lines.txt"), "w") as f:
        f.writelines(line + "\n" for line in sorted(covered, key=sort_key))

    covered_by_file = collections.defaultdict(set)
    for line in covered:
        path, _, n = line.rpartition(":")
        covered_by_file[path].add(int(n))
    with open(os.path.join(out, "uncovered_by_file.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file", "lines", "covered"])
        for path in sorted(prod_lines, key=lambda p: (-(len(prod_lines[p]) - len(covered_by_file[p])), p)):
            w.writerow([path, len(prod_lines[path]), len(covered_by_file[path])])
    with open(os.path.join(out, "uncovered_ranges.txt"), "w") as f:
        for path in sorted(prod_lines):
            missing = sorted(prod_lines[path] - covered_by_file[path])
            for start, end in ranges(missing, skip=covered_by_file[path]):
                f.write(f"{path}:{start}-{end}\n")

    with tarfile.open(os.path.join(out, "sources.tar.gz"), "w:gz") as tar:
        for path in sorted(prod_lines):
            tar.add(os.path.join(ROOT, path), arcname=path)

    write_summary(out, work, dict(
        listed=listed, matched=matched, names=names, hits=hits, failed=failed, no_coverage=no_coverage,
        prod_lines=prod_lines, covered=covered, count=count, example_lines=example_lines,
        example_hit=example_hit, layer_of=layer_of, binary_of=binary_of, seconds=seconds,
        order=order, groups=groups, single=single, cross=cross,
    ))
    return 0


def write_summary(out: str, work: str, v: dict) -> None:
    hits, covered, prod_lines, count = v["hits"], v["covered"], v["prod_lines"], v["count"]
    names, layer_of, seconds = v["names"], v["layer_of"], v["seconds"]
    s = io.StringIO()
    p = lambda *a: print(*a, file=s)  # noqa: E731
    pct = lambda a, b: f"{100 * a / b:5.1f}%" if b else "  n/a"  # noqa: E731
    total = sum(len(ls) for ls in prod_lines.values())

    meta_path = os.path.join(work, "meta.txt")
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            p(f.read().rstrip())
    p("\n== TESTS")
    p(f"listed: {v['listed']}   selected to run: {v['matched']}   not run (ignored or filtered): {v['listed'] - v['matched']}")
    p(f"run: {len(names)}   with coverage: {len(hits)}   failed: {len(v['failed'])}   without coverage: {len(v['no_coverage'])}")
    p("\n== PRODUCTION LINES (test files, #[cfg(test)] items and examples excluded)")
    p(f"instrumented: {total}   covered: {len(covered)}   {pct(len(covered), total)}")
    ex_total = sum(len(ls) for ls in v["example_lines"].values())
    p(f"examples, reported apart: instrumented {ex_total}, covered {len(v['example_hit'])}")

    def table(title, key, top=None):
        lines_by, covered_by = collections.Counter(), collections.Counter()
        for path, ls in prod_lines.items():
            lines_by[key(path)] += len(ls)
        for line in covered:
            covered_by[key(line.rpartition(":")[0])] += 1
        p(f"\n== {title}")
        p(f"{'':52s} {'lines':>7s} {'covered':>7s} {'uncov':>6s}    pct")
        rows = sorted(lines_by, key=lambda k: (-(lines_by[k] - covered_by[k]), k))
        for k in rows[:top]:
            p(f"{k:52s} {lines_by[k]:7d} {covered_by[k]:7d} {lines_by[k] - covered_by[k]:6d} {pct(covered_by[k], lines_by[k])}")

    table("BY CRATE", crate_of)
    table("BY MODULE (60 with most uncovered lines)", module_of, top=60)

    p("\n== LAYER SPLIT (covered production lines, by which layer's tests reach them)")
    reach = collections.defaultdict(set)
    for t, h in hits.items():
        reach[layer_of[t]] |= h
    unit, integ = reach["unit"], reach["integration"] | reach["bench"]
    rows = collections.defaultdict(lambda: [0, 0, 0])
    for line in covered:
        crate = crate_of(line.rpartition(":")[0])
        rows[crate][0 if line not in integ else 1 if line not in unit else 2] += 1
    p(f"{'crate':28s} {'unit-only':>10s} {'integ-only':>10s} {'both':>8s}")
    for crate, (a, b, c) in sorted(rows.items()):
        p(f"{crate:28s} {a:10d} {b:10d} {c:8d}")

    p("\n== TESTS PER COVERED LINE")
    for lo, hi in [(1, 1), (2, 2), (3, 5), (6, 10), (11, 49), (50, 10**9)]:
        n = sum(1 for c in count.values() if lo <= c <= hi)
        label = f"{lo}" if lo == hi else (f"{lo}+" if hi > 10**8 else f"{lo}-{hi}")
        p(f"  {label:>6s} tests: {n:7d} lines {pct(n, len(covered))}")

    order = v["order"]
    p("\n== MINIMAL COVER (greedy): fewest tests reaching a share of the covered lines")
    reached, marks = set(), [0.5, 0.8, 0.9, 0.95, 0.99, 1.0]
    for i, t in enumerate(order, 1):
        reached |= hits[t]
        while marks and len(reached) >= marks[0] * len(covered) - 1e-9:
            p(f"  {int(marks.pop(0) * 100):3d}%: {i:5d} tests ({pct(i, len(hits))} of tests with coverage)")
    p(f"tests outside the cover: {len(hits) - len(order)}")

    zero_unique = sum(1 for h in hits.values() if not any(count[line] == 1 for line in h))
    groups, single, cross, binary_of = v["groups"], v["single"], v["cross"], v["binary_of"]
    p("\n== OVERLAP")
    p(f"tests with no production line: {sum(1 for h in hits.values() if not h)}")
    p(f"tests with no unique production line: {zero_unique}")
    p(f"identical-footprint groups: {len(groups)} ({sum(map(len, groups))} tests)")
    p(f"tests wholly inside one other test: {len(single)}")
    p(f"  ... whose smallest container is in another binary: {sum(1 for t, o in single.items() if binary_of[o] != binary_of[t])}")
    p(f"  ... with a container in another binary (cross_binary_pairs.csv): {len(cross)}")

    p(f"\n== FAILED TESTS ({len(v['failed'])})")
    for t in v["failed"]:
        p("  " + t)
    p(f"\n== TESTS WITHOUT COVERAGE DATA ({len(v['no_coverage'])})")
    for t in v["no_coverage"]:
        p("  " + t)
    with open(os.path.join(work, "export_failures.txt")) as f:
        for line in f:
            p("  export: " + line.rstrip())
    p("\n== SLOWEST TESTS (instrumented, one process each)")
    for t, sec in sorted(seconds.items(), key=lambda kv: (-kv[1], kv[0]))[:20]:
        p(f"  {sec:7.1f}s {t}")
    p(f"total test seconds: {sum(seconds.values()):.0f}")
    with open(os.path.join(out, "summary.txt"), "w") as f:
        f.write(s.getvalue())


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
