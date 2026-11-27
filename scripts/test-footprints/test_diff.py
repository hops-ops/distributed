import contextlib
import csv
import io
import os
import sys
import tarfile
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import diff  # noqa: E402
from analyze import containers, greedy_cover, lines_file_name, ranges  # noqa: E402

SRC = ["fn a() {", "    one();", "    two();", "}", "fn b() {", "    three();", "}"]


def write_output(root, name, sources, tests, lines_total=None, failed=None):
    """A minimal output directory: `tests` maps (binary, test) -> covered
    production lines (`path:n`); `sources` maps path -> list of lines."""
    out = os.path.join(root, name)
    os.makedirs(os.path.join(out, "lines"))
    covered = sorted({line for ls in tests.values() for line in ls}, key=diff.sort_key)
    with open(os.path.join(out, "covered_lines.txt"), "w") as f:
        f.writelines(line + "\n" for line in covered)
    with open(os.path.join(out, "per_test.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["binary", "test", "status"])
        for binary, test in sorted(tests):
            w.writerow([binary, test, (failed or {}).get((binary, test), "passed")])
            with open(os.path.join(out, "lines", lines_file_name(binary, test)), "w") as g:
                g.writelines(line + "\n" for line in sorted(tests[(binary, test)], key=diff.sort_key))
    with open(os.path.join(out, "uncovered_by_file.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file", "lines", "covered"])
        for path in sources:
            n = sum(1 for line in covered if line.startswith(path + ":"))
            w.writerow([path, lines_total or len(sources[path]), n])
    with tarfile.open(os.path.join(out, "sources.tar.gz"), "w:gz") as tar:
        for path, lines in sources.items():
            data = "\n".join(lines).encode()
            info = tarfile.TarInfo(path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out


def run(base, current, allowlist=None):
    argv = [base, current] + (["--allowlist", allowlist] if allowlist else [])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = diff.main(argv)
    return code, buf.getvalue()


class DiffTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.tests = {
            ("distributed", "a::covers_one"): ["src/a.rs:2"],
            ("distributed::core", "covers_three"): ["src/a.rs:2", "src/a.rs:6"],
        }

    def tearDown(self):
        self.tmp.cleanup()

    def test_identical_runs_lose_nothing(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, self.tests)
        code, out = run(base, current)
        self.assertEqual(code, 0, out)
        self.assertIn("LOST: production lines covered before and not now: 0", out)

    def test_removing_a_test_with_a_unique_line_fails_and_names_it(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        del self.tests[("distributed::core", "covers_three")]
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, self.tests)
        code, out = run(base, current)
        self.assertEqual(code, 1, out)
        self.assertIn("  src/a.rs:6  covered before by: distributed::core covers_three", out)
        self.assertIn("removed: distributed::core covers_three", out)
        self.assertNotIn("src/a.rs:2 ", out)
        self.assertNotIn("no test that covered it was removed", out)

    def test_a_line_lost_while_its_tests_remain_is_flagged(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        self.tests[("distributed::core", "covers_three")] = ["src/a.rs:2"]
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, self.tests)
        code, out = run(base, current)
        self.assertEqual(code, 1, out)
        self.assertIn("  src/a.rs:6  covered before by: distributed::core covers_three\n"
                      "    (no test that covered it was removed", out)

    def test_removing_a_test_without_unique_lines_passes(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        del self.tests[("distributed", "a::covers_one")]
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, self.tests)
        code, out = run(base, current)
        self.assertEqual(code, 0, out)
        self.assertIn("tests: 1 removed, 0 added", out)

    def test_lines_moved_by_an_edit_above_are_compared_at_their_new_number(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        shifted = ["// new", "// lines"] + SRC
        moved = {k: [f"src/a.rs:{int(l.split(':')[1]) + 2}" for l in v] for k, v in self.tests.items()}
        current = write_output(self.root, "cur", {"src/a.rs": shifted}, moved)
        code, out = run(base, current)
        self.assertEqual(code, 0, out)

    def test_a_moved_line_that_lost_coverage_is_reported_at_its_new_number(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        shifted = ["// new"] + SRC
        current = write_output(self.root, "cur", {"src/a.rs": shifted}, {("distributed", "a::covers_one"): ["src/a.rs:3"]})
        code, out = run(base, current)
        self.assertEqual(code, 1, out)
        self.assertIn("  src/a.rs:7 (base src/a.rs:6)  covered before by: distributed::core covers_three", out)

    def test_a_line_whose_old_number_is_now_covered_by_other_code_is_still_lost(self):
        # Without mapping, line 6 would look covered: after the shift it holds `fn b() {`.
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        shifted = ["// new"] + SRC
        current = write_output(self.root, "cur", {"src/a.rs": shifted}, {("x", "t"): ["src/a.rs:3", "src/a.rs:6"]})
        code, out = run(base, current)
        self.assertEqual(code, 1, out)
        self.assertIn("src/a.rs:7 (base src/a.rs:6)", out)

    def test_an_edited_line_is_listed_apart_and_does_not_fail(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        edited = SRC[:5] + ["    four();"] + SRC[6:]
        current = write_output(self.root, "cur", {"src/a.rs": edited}, {("distributed", "a::covers_one"): ["src/a.rs:2"]})
        code, out = run(base, current)
        self.assertEqual(code, 0, out)
        self.assertIn("source line edited or deleted since, not compared: 1", out)
        self.assertIn("  src/a.rs:6 (base numbering)", out)

    def test_allowlisted_lines_are_reported_but_do_not_fail(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, {("distributed", "a::covers_one"): ["src/a.rs:2"]})
        allow = os.path.join(self.root, "allow.txt")
        with open(allow, "w") as f:
            f.write("# timing\nsrc/a.rs:6\t    three();\n")
        code, out = run(base, current, allow)
        self.assertEqual(code, 0, out)
        self.assertIn("lost but allowlisted as timing-dependent: 1", out)

    def test_an_allowlist_entry_whose_line_moved_or_changed_no_longer_matches(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, {("distributed", "a::covers_one"): ["src/a.rs:2"]})
        allow = os.path.join(self.root, "allow.txt")
        for entry in ("src/a.rs:5\t    three();\n", "src/a.rs:6\t    four();\n", "src/b.rs:6\t    three();\n"):
            with open(allow, "w") as f:
                f.write(entry)
            code, out = run(base, current, allow)
            self.assertEqual(code, 1, entry + out)
            self.assertIn("  src/a.rs:6  covered before by", out)

    def test_coverage_change_is_reported(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, {("distributed", "a::covers_one"): ["src/a.rs:2"]})
        _, out = run(base, current)
        self.assertIn("2/7 (28.57%) -> 1/7 (14.29%), -1 lines, -14.29 points", out)

    def test_failed_tests_are_warned_about(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        current = write_output(self.root, "cur", {"src/a.rs": SRC}, self.tests,
                               failed={("distributed::core", "covers_three"): "failed"})
        code, out = run(base, current)
        self.assertEqual(code, 0, out)
        self.assertIn("WARNING: 1 tests in CURRENT failed or left no coverage", out)
        self.assertIn("  CURRENT: distributed::core covers_three", out)
        self.assertNotIn("in BASE", out)

    def test_same_directory_is_refused(self):
        base = write_output(self.root, "base", {"src/a.rs": SRC}, self.tests)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(diff.main([base, base]), 2)


class AnalysisTest(unittest.TestCase):
    def test_containers_prefer_the_smallest_and_never_pair_identical_tests_both_ways(self):
        hits = {
            "u a": frozenset({1, 2}),
            "u b": frozenset({1, 2}),
            "i big": frozenset({1, 2, 3, 4}),
            "i mid": frozenset({1, 2, 3}),
            "u none": frozenset(),
        }
        binary = {t: t.split()[0] for t in hits}
        single, cross = containers(hits, binary)
        self.assertEqual(single["u b"], "u a")
        self.assertEqual(single["u a"], "i mid")
        self.assertEqual(single["i mid"], "i big")
        self.assertNotIn("i big", single)
        self.assertNotIn("u none", single)
        self.assertEqual(cross, {"u a": "i mid", "u b": "i mid"})

    def test_greedy_cover_reaches_every_line(self):
        hits = {"a": frozenset({1, 2, 3}), "b": frozenset({3, 4}), "c": frozenset({1}), "d": frozenset({4})}
        order = greedy_cover(hits, {1, 2, 3, 4})
        self.assertEqual(order[0], "a")
        self.assertEqual(set().union(*(hits[t] for t in order)), {1, 2, 3, 4})
        self.assertEqual(len(order), 2)

    def test_uncovered_ranges_step_over_lines_without_code_but_not_covered_lines(self):
        self.assertEqual(ranges([3, 4, 7, 9, 12], skip={10}), [(3, 9), (12, 12)])

    def test_lines_file_names_are_flat_and_bounded(self):
        name = lines_file_name("distributed_cli::bin/distributed", "a::b")
        self.assertEqual(name, "distributed_cli::bin%2Fdistributed__a::b.txt")
        self.assertLessEqual(len(lines_file_name("x", "y" * 400)), 204)
        self.assertNotEqual(lines_file_name("x", "y" * 400), lines_file_name("x", "y" * 399 + "z"))


if __name__ == "__main__":
    unittest.main()
