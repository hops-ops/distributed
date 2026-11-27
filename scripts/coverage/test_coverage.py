import os
import re
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ignore_regex import excluded, regex  # noqa: E402  (puts classify on the path)
from summary import render, summarize  # noqa: E402

from classify import classify  # noqa: E402

SOURCES = {
    "src/lib.rs": "#[cfg(test)]\nmod fixture;\npub mod a;\n",
    "src/fixture.rs": "pub fn f() {}\n",
    "src/a.rs": "#[cfg(test)]\nmod tests;\npub fn a() {}\n",
    "src/a/tests.rs": "#[test]\nfn t() {}\n",
    "src/b/mod.rs": "pub fn b() {}\n",
    "src/b/tests.rs": "#[test]\nfn t() {}\n",
    "tests/common/src/lib.rs": "",
    "examples/q/main.rs": "",
    "cli/src/main.rs": "fn main() {}\n",
}


class ExcludedTest(unittest.TestCase):
    def test_directories_without_production_files_and_single_files(self):
        self.assertEqual(
            excluded(classify(SOURCES)),
            ["examples/", "src/a/", "src/b/tests.rs", "src/fixture.rs", "tests/"],
        )


class RegexTest(unittest.TestCase):
    # Python's `re` reads this subset of POSIX ERE the same way llvm-cov does.
    def test_matches_exactly_the_excluded_files(self):
        root = "/w/a.b(1)"
        pattern = re.compile(regex(root, excluded(classify(SOURCES))))
        dropped = {p for p in SOURCES if pattern.search(f"{root}/{p}")}
        self.assertEqual(
            dropped,
            {"src/fixture.rs", "src/a/tests.rs", "src/b/tests.rs", "tests/common/src/lib.rs", "examples/q/main.rs"},
        )
        # The root is literal: `.` and `(` do not match other characters.
        self.assertIsNone(pattern.search("/w/aXb(1)/tests/x.rs"))
        # A file is matched by its whole path, not as a prefix.
        self.assertIsNone(pattern.search(f"{root}/src/b/tests.rs.bak"))


class SummaryTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        files = {
            "Cargo.toml": '[workspace]\nmembers = ["cli", "tests/common"]\n[package]\nname = "root"\n',
            "cli/Cargo.toml": '[package]\nname = "cli"\n',
            "tests/common/Cargo.toml": '[package]\nname = "common"\n',
            **SOURCES,
        }
        for rel, text in files.items():
            path = os.path.join(self.root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(text)
        self.root = os.path.realpath(self.root)

    def report(self, *rels):
        files = [
            {"filename": os.path.join(self.root, rel), "summary": {"lines": {"count": 10, "covered": 7}}}
            for rel in rels
        ]
        n = 10 * len(rels)
        return {"data": [{"files": files, "totals": {"lines": {"count": n, "covered": 7 * len(rels)}}}]}

    def test_groups_files_by_longest_member_prefix(self):
        crates, total, stray = summarize(self.report("src/a.rs", "src/b/mod.rs", "cli/src/main.rs"), self.root)
        self.assertEqual(crates, {"root": [20, 14], "cli": [10, 7]})
        self.assertEqual(total["count"], 30)
        self.assertEqual(stray, [])
        rows = [line.split() for line in render(crates, total, False).splitlines()]
        self.assertIn(["TOTAL", "30", "9", "70.00%"], rows)
        self.assertIn(["cli", "10", "3", "70.00%"], rows)

    def test_reports_test_files_the_regex_missed(self):
        _, _, stray = summarize(self.report("src/a.rs", "src/b/tests.rs", "src/fixture.rs"), self.root)
        self.assertEqual(stray, ["src/b/tests.rs", "src/fixture.rs"])


if __name__ == "__main__":
    unittest.main()
