import os
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from classify import EXAMPLE, PROD, TEST, class_of, classify, mask, path_class  # noqa: E402


def kinds(sources):
    return {path: kind for path, (kind, _) in classify(sources).items()}


def test_lines(sources, path):
    return sorted(classify(sources)[path][1])


class PathClassTest(unittest.TestCase):
    def test_test_directories_and_names(self):
        for path in [
            "tests/core/main.rs",
            "tests/common/src/ids.rs",
            "distributed_cli/tests/support/golden.rs",
            "benches/load.rs",
            "src/metrics/tests.rs",
            "src/sqlx_repo/projection_protocol/postgres_tests.rs",
            "src/a/b_test.rs",
            "src/metrics/test_support.rs",
        ]:
            self.assertEqual(path_class(path), TEST, path)

    def test_examples_are_their_own_class(self):
        self.assertEqual(path_class("examples/quickstart/src/main.rs"), EXAMPLE)
        self.assertEqual(path_class("distributed_game/examples/arena.rs"), EXAMPLE)

    def test_production_names(self):
        for path in ["src/lib.rs", "src/testing_tools.rs", "src/attestation.rs", "src/latest.rs", "src/contest/mod.rs"]:
            self.assertEqual(path_class(path), PROD, path)


class DeclaredModulesTest(unittest.TestCase):
    """The three files the repo declares under a test-only cfg."""

    SOURCES = {
        "src/metrics/mod.rs": "mod registry;\n\n#[cfg(test)]\nmod test_support;\n",
        "src/metrics/registry.rs": "pub fn r() {}\n",
        "src/metrics/test_support.rs": "pub fn helper() {}\n",
        "src/outbox_worker/mod.rs": "pub(crate) mod store;\n#[cfg(test)]\npub(crate) mod testing;\n",
        "src/outbox_worker/testing.rs": "pub fn fake() {}\n",
        "src/outbox_worker/store/mod.rs": "pub fn s() {}\n",
        "src/sqlx_repo/mod.rs": '#[cfg(all(test, feature = "postgres"))]\npub(crate) mod postgres_test_schema;\npub mod x;\n',
        "src/sqlx_repo/postgres_test_schema.rs": "pub fn schema() {}\n",
        "src/sqlx_repo/x.rs": "pub fn x() {}\n",
    }

    def test_declared_files_are_test_code(self):
        k = kinds(self.SOURCES)
        self.assertEqual(k["src/metrics/test_support.rs"], TEST)
        self.assertEqual(k["src/outbox_worker/testing.rs"], TEST)
        self.assertEqual(k["src/sqlx_repo/postgres_test_schema.rs"], TEST)
        self.assertEqual(k["src/metrics/registry.rs"], PROD)
        self.assertEqual(k["src/outbox_worker/store/mod.rs"], PROD)
        self.assertEqual(k["src/sqlx_repo/x.rs"], PROD)

    def test_the_declaration_lines_are_test_lines(self):
        self.assertEqual(test_lines(self.SOURCES, "src/metrics/mod.rs"), [3, 4])
        self.assertEqual(test_lines(self.SOURCES, "src/sqlx_repo/mod.rs"), [1, 2])

    def test_a_declared_directory_covers_its_files(self):
        sources = {
            "src/a.rs": "#[cfg(test)]\nmod fixtures;\n",
            "src/a/fixtures/mod.rs": "mod deep;\n",
            "src/a/fixtures/deep.rs": "fn d() {}\n",
            "src/a/real.rs": "fn r() {}\n",
        }
        k = kinds(sources)
        self.assertEqual(k["src/a/fixtures/mod.rs"], TEST)
        self.assertEqual(k["src/a/fixtures/deep.rs"], TEST)
        self.assertEqual(k["src/a/real.rs"], PROD)

    def test_a_non_root_file_declares_into_its_own_directory(self):
        sources = {"src/b.rs": "#[cfg(test)]\nmod helper;\n", "src/helper.rs": "", "src/b/helper.rs": ""}
        k = kinds(sources)
        self.assertEqual(k["src/b/helper.rs"], TEST)
        self.assertEqual(k["src/helper.rs"], PROD)

    def test_path_attribute(self):
        sources = {
            "src/lib.rs": '#[cfg(test)]\n#[path = "support/golden.rs"]\nmod golden;\n',
            "src/support/golden.rs": "",
        }
        self.assertEqual(kinds(sources)["src/support/golden.rs"], TEST)

    def test_inner_cfg_test_makes_the_whole_file_test_code(self):
        sources = {"src/c.rs": "//! doc\n#![allow(dead_code)]\n#![cfg(test)]\nfn c() {}\n"}
        self.assertEqual(kinds(sources)["src/c.rs"], TEST)

    def test_class_of_falls_back_to_the_name(self):
        self.assertEqual(class_of("tests/x.rs", {}), (TEST, frozenset()))
        self.assertEqual(class_of("src/y.rs", {}), (PROD, frozenset()))


class InlineItemsTest(unittest.TestCase):
    def test_inline_test_module(self):
        src = "fn a() {}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {\n        a();\n    }\n}\nfn b() {}\n"
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), [3, 4, 5, 6, 7, 8, 9])

    def test_cfg_test_items_other_than_modules(self):
        src = "#[cfg(test)]\nuse std::fmt;\nfn p() {}\n#[cfg(test)]\nimpl X {\n    fn f() {}\n}\n"
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), [1, 2, 4, 5, 6, 7])

    def test_attribute_and_item_on_one_line(self):
        src = "fn p() {}\n#[cfg(test)] mod tests { fn t() {} }\nfn q() {}\n"
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), [2])

    def test_attributes_between_cfg_and_item(self):
        src = '#[cfg(test)]\n#[allow(clippy::all)]\n/// doc\nmod tests {\n}\nfn p() {}\n'
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), [1, 2, 3, 4, 5])

    def test_test_only_predicates(self):
        cases = {
            "test": True,
            'all(test, feature = "sqlite")': True,
            'all(feature = "graphql", test)': True,
            'all(test, any(feature = "a", feature = "b"))': True,
            'any(target_arch = "wasm32", test)': False,
            "not(test)": False,
            'feature = "test"': False,
            'all(feature = "x", any(unix, test))': False,
        }
        for predicate, test_only in cases.items():
            src = f"#[cfg({predicate})]\nfn f() {{}}\nfn g() {{}}\n"
            got = test_lines({"src/a.rs": src}, "src/a.rs")
            self.assertEqual(got, [1, 2] if test_only else [], predicate)

    def test_braces_in_strings_chars_and_comments_do_not_end_the_item(self):
        src = (
            "#[cfg(test)]\n"
            "mod tests {\n"
            '    const S: &str = "}}}";\n'
            "    const C: char = '}';\n"
            "    // }\n"
            "    /* } /* nested } */ */\n"
            '    const R: &str = r#"}"#;\n'
            "    fn f<'a>(x: &'a str) -> &'a str { x }\n"
            "}\n"
            "fn prod() {}\n"
        )
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), list(range(1, 10)))

    def test_cfg_in_a_comment_or_string_is_ignored(self):
        src = '// #[cfg(test)]\nfn a() {}\nconst S: &str = "#[cfg(test)] mod x;";\nfn b() {}\n'
        self.assertEqual(test_lines({"src/a.rs": src}, "src/a.rs"), [])

    def test_test_files_have_no_inline_lines(self):
        self.assertEqual(classify({"tests/a.rs": "#[cfg(test)]\nmod t {}\n"})["tests/a.rs"], (TEST, frozenset()))


class MaskTest(unittest.TestCase):
    def test_keeps_offsets_and_newlines(self):
        text = 'let s = "a\nb"; // c\nlet c = \'x\'; /* d\n */ let r = br##"q"##;\n'
        masked = mask(text)
        self.assertEqual(len(masked), len(text))
        self.assertEqual(masked.count("\n"), text.count("\n"))
        self.assertNotIn("a", masked.replace("let", ""))
        self.assertIn("let r =", masked)

    def test_lifetimes_are_not_char_literals(self):
        text = "fn f<'a>(x: &'a str) {}\n"
        self.assertEqual(mask(text), text)


if __name__ == "__main__":
    unittest.main()
