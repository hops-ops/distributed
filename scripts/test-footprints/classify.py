"""Which lines of the repository's Rust sources are production code.

`classify(sources)` is the one place that decides. A file is:

- `example`: under an `examples/` directory. Reported on its own, outside
  production totals.
- `test`: under a `tests/` or `benches/` directory; named `tests.rs`,
  `*_test.rs`, `*_tests.rs` or `test_*.rs`; declared as a module under a
  test-only cfg (`#[cfg(test)] mod x;` makes `x.rs`, `x/mod.rs` and every
  file under `x/` test code, `#[path]` included); or starting with
  `#![cfg(test)]`.
- `prod` otherwise, minus the lines of every item under a test-only cfg
  (`#[cfg(test)] mod tests { ... }`, a `#[cfg(test)] fn`, ...).

A cfg is test-only when it can only hold under `test`: `cfg(test)`, or
`cfg(all(..., test, ...))`. `cfg(any(test, x))` and `cfg(not(test))` are not.

Limits, stated rather than handled: a `mod x;` declared inside an inline
`mod outer { ... }` block resolves as if declared at file level, and macros
that expand to test code are invisible.
"""

from __future__ import annotations

import bisect
import os
import posixpath
import re
from typing import Iterable, Mapping

PROD, TEST, EXAMPLE = "prod", "test", "example"

# Directories never holding sources worth classifying.
SKIP_DIRS = {"target", "node_modules"}


def load_sources(root: str) -> dict[str, str]:
    """Every `.rs` file under `root` (hidden and build directories skipped),
    keyed by its `/`-separated path relative to `root`."""
    sources = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("."))
        for name in filenames:
            if name.endswith(".rs"):
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, root).replace(os.sep, "/")
                with open(full, encoding="utf-8", errors="replace") as f:
                    sources[rel] = f.read()
    return sources


def classify(sources: Mapping[str, str]) -> dict[str, tuple[str, frozenset[int]]]:
    """Classify every file in `sources` (relative path -> text).

    Returns, per path, `(class, test_lines)`: the class is PROD, TEST or
    EXAMPLE, and for a PROD file `test_lines` holds the 1-based line numbers
    of its test-only items (empty for the other classes). A path that
    coverage reports but `sources` lacks can be classified by
    `class_of(path, result)`.
    """
    declared: set[str] = set()  # files and directories (ending in "/")
    inline: dict[str, frozenset[int]] = {}
    for path, text in sources.items():
        if path_class(path) != PROD:
            continue
        lines, modules, whole = _scan(path, text)
        if whole:
            declared.add(path)
        declared.update(modules)
        inline[path] = frozenset(lines)
    result = {}
    for path in sources:
        kind = path_class(path)
        if kind == PROD and _is_declared(path, declared):
            kind = TEST
        result[path] = (kind, inline.get(path, frozenset()) if kind == PROD else frozenset())
    return result


def class_of(path: str, classes: Mapping[str, tuple[str, frozenset[int]]]) -> tuple[str, frozenset[int]]:
    """The classification of `path`, falling back to its name alone."""
    return classes.get(path) or (path_class(path), frozenset())


def path_class(path: str) -> str:
    """The class a path has by its name alone."""
    parts = path.split("/")
    dirs, name = parts[:-1], parts[-1]
    if "examples" in dirs:
        return EXAMPLE
    if "tests" in dirs or "benches" in dirs:
        return TEST
    if name == "tests.rs" or name.endswith(("_test.rs", "_tests.rs")) or name.startswith("test_"):
        return TEST
    return PROD


def _is_declared(path: str, declared: set[str]) -> bool:
    if path in declared:
        return True
    parts = path.split("/")
    return any("/".join(parts[:i]) + "/" in declared for i in range(1, len(parts)))


# ---------------------------------------------------------------- scanning

_CFG = re.compile(r"#\s*\[\s*cfg\s*\(")
_INNER_CFG = re.compile(r"#\s*!\s*\[\s*cfg\s*\(")
_INNER_ATTR = re.compile(r"#\s*!\s*\[")
_ATTR = re.compile(r"#\s*\[")
_PATH_ATTR = re.compile(r'^#\s*\[\s*path\s*=\s*"([^"]*)"\s*\]$', re.S)
_MOD_DECL = re.compile(r"(?:pub\s*(?:\([^)]*\))?\s*)?mod\s+(?:r#)?(\w+)\s*;")
_SPACE = re.compile(r"\s*")
_DELIMS = re.compile(r"[{}()\[\];]")


def _scan(path: str, text: str) -> tuple[set[int], set[str], bool]:
    """Test-only lines of one file, the module paths it declares test-only,
    and whether the whole file is test-only."""
    code = mask(text)
    pos = _SPACE.match(code).end()
    while inner := _INNER_ATTR.match(code, pos):
        close = _close(code, inner.end() - 1)
        if close is None:
            break
        cfg = _INNER_CFG.match(code, pos)
        if cfg:
            cfg_close = _close(code, cfg.end() - 1)
            if cfg_close is not None and _implies_test(code[cfg.end():cfg_close]):
                return set(), set(), True
        pos = _SPACE.match(code, close + 1).end()

    starts = [0] + [i + 1 for i, c in enumerate(code) if c == "\n"]
    line = lambda offset: bisect.bisect_right(starts, offset)  # noqa: E731
    lines: set[int] = set()
    modules: set[str] = set()
    covered_to = -1
    for attr in _CFG.finditer(code):
        if attr.start() < covered_to:
            continue  # inside an item already counted
        close = _close(code, attr.end() - 1)
        if close is None or not _implies_test(code[attr.end():close]):
            continue
        end_attr = _SPACE.match(code, close + 1).end()
        if not code.startswith("]", end_attr):
            continue
        pos, mod_path = _skip_attributes(code, text, end_attr + 1)
        decl = _MOD_DECL.match(code, pos)
        if decl:
            end = decl.end()
            modules.update(_module_paths(path, decl.group(1), mod_path))
        else:
            end = _item_end(code, pos)
        lines.update(range(line(attr.start()), line(max(end - 1, attr.start())) + 1))
        covered_to = end
    return lines, modules, False


def _skip_attributes(code: str, text: str, pos: int) -> tuple[int, str | None]:
    """Skip whitespace and outer attributes; return the item start and the
    value of a `#[path = "..."]` among them."""
    mod_path = None
    while True:
        pos = _SPACE.match(code, pos).end()
        attr = _ATTR.match(code, pos)
        if not attr:
            return pos, mod_path
        close = _close(code, attr.end() - 1)
        if close is None:
            return pos, mod_path
        found = _PATH_ATTR.match(text[pos:close + 1])  # literal text: masking blanks strings
        if found:
            mod_path = found.group(1)
        pos = close + 1


def _module_paths(path: str, name: str, mod_path: str | None) -> set[str]:
    directory = posixpath.dirname(path)
    if mod_path is not None:
        target = posixpath.normpath(posixpath.join(directory, mod_path))
        return {target}
    base_name = posixpath.basename(path)
    is_root = base_name in ("mod.rs", "lib.rs", "main.rs", "build.rs") or posixpath.basename(directory) == "bin"
    base = directory if is_root else posixpath.join(directory, base_name[:-3])
    target = posixpath.normpath(posixpath.join(base, name))
    return {target + ".rs", target + "/"}


def _item_end(code: str, pos: int) -> int:
    """Offset just past the item starting at `pos`: its closing `}` or `;`."""
    braces = others = 0
    for m in _DELIMS.finditer(code, pos):
        c = m.group()
        if c == "{":
            braces += 1
        elif c == "}":
            braces -= 1
            if braces <= 0 and others == 0:
                return m.end()
        elif c in "([":
            others += 1
        elif c in ")]":
            others -= 1
        elif braces == 0 and others == 0:  # ";"
            return m.end()
    return len(code)


def _close(code: str, open_pos: int) -> int | None:
    """Offset of the bracket closing the one at `open_pos`."""
    pairs = {"(": ")", "[": "]", "{": "}"}
    stack = []
    for i in range(open_pos, len(code)):
        c = code[i]
        if c in pairs:
            stack.append(pairs[c])
        elif c in ")]}":
            if not stack or stack.pop() != c:
                return None
            if not stack:
                return i
    return None


def _implies_test(predicate: str) -> bool:
    """Whether a cfg predicate can only hold when `test` is set."""
    p = predicate.strip()
    if p == "test":
        return True
    m = re.match(r"all\s*\(", p)
    if m and p.endswith(")"):
        return any(_implies_test(arg) for arg in _split_args(p[m.end():-1]))
    return False


def _split_args(args: str) -> Iterable[str]:
    depth, start = 0, 0
    for i, c in enumerate(args):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            yield args[start:i]
            start = i + 1
    yield args[start:]


# ----------------------------------------------------------------- masking

_TOKEN = re.compile(
    r"""(?P<line>//)|(?P<block>/\*)|(?P<raw>\b[bc]?r(?P<hashes>\#*)")|(?P<str>\b[bc]"|")"""
    r"""|(?P<char>\bb?'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]+\}|.)|[^\\'\n])')"""
    r"""|(?P<char2>'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]+\}|.)|[^\\'\n])')"""
)
_STR_BODY = re.compile(r'(?:[^"\\]|\\.)*"', re.S)
_BLOCK_EDGE = re.compile(r"/\*|\*/")


def mask(text: str) -> str:
    """`text` with comments and string and character literals blanked to
    spaces, newlines kept, so offsets and line numbers still match."""
    out = []
    pos = 0
    while True:
        m = _TOKEN.search(text, pos)
        if not m:
            out.append(text[pos:])
            return "".join(out)
        start = m.start()
        if m.group("line"):
            end = text.find("\n", start)
            end = len(text) if end < 0 else end
        elif m.group("block"):
            depth, end = 1, m.end()
            while depth:
                edge = _BLOCK_EDGE.search(text, end)
                if not edge:
                    end = len(text)
                    break
                depth += 1 if edge.group() == "/*" else -1
                end = edge.end()
        elif m.group("raw"):
            close = text.find('"' + m.group("hashes"), m.end())
            end = len(text) if close < 0 else close + 1 + len(m.group("hashes"))
        elif m.group("str"):
            body = _STR_BODY.match(text, m.end())
            end = body.end() if body else len(text)
        else:
            end = m.end()
        out.append(text[pos:start])
        out.append(re.sub(r"[^\n]", " ", text[start:end]))
        pos = end
