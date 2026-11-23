//! The one byte-exact comparison for checked-in generated output, shared by
//! the crate's unit tests and its integration tests.

use std::fs;
use std::path::Path;

/// Assert `actual` is byte-identical to `tests/fixtures/<fixture>`.
/// `UPDATE_GOLDEN=1` rewrites the fixture with `actual` instead; review the
/// diff, since downstream TypeScript and JavaScript tests read the same files.
pub(crate) fn assert_golden(fixture: &str, actual: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixture);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        fs::create_dir_all(path.parent().expect("fixture has a directory")).unwrap();
        fs::write(&path, actual).unwrap();
        return;
    }
    let golden = fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        golden, actual,
        "{fixture} drifted; regenerate with UPDATE_GOLDEN=1"
    );
}
