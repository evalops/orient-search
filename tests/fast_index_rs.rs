use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use orient::fast_index::FastIndex;
use orient::repo_index::{
    SearchFilters, search_repo_fast_filtered, search_repo_fast_filtered_with_timeout,
};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn refresh_reuses_unchanged_files_and_picks_up_changed_terms() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );

    let first = FastIndex::build(repo.path()).unwrap();
    assert_eq!(first.search("rotating secret", 10).unwrap(), Vec::new());

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn rotating_secret() {}\n",
    );
    let outcome = FastIndex::refresh(repo.path(), Some(&first)).unwrap();
    assert!(outcome.reused_files >= 1);
    assert!(outcome.refreshed_files >= 1);

    let refreshed = outcome.index;
    let results = refreshed.search("rotating secret", 10).unwrap();
    assert_eq!(results[0].path, "src/auth.rs");
}

#[test]
fn indexed_search_supports_filters_require_all_and_symbol_boosting() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("docs/session.md"),
        "Session manager notes mention session manager many times.\n",
    );
    write(
        &repo.path().join("src/session.ts"),
        "export function issueToken() { return 'token' }\n",
    );

    let index = FastIndex::build(repo.path()).unwrap();
    let results = index
        .search_filtered(
            "SessionManager",
            10,
            &SearchFilters {
                path: Some("src/".to_string()),
                language: Some("rust".to_string()),
                extension: Some("rs".to_string()),
                require_all: true,
            },
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/auth.rs");
    assert!(results[0].reason.contains("symbol:SessionManager"));
    assert!(results[0].snippet.contains("1:"));
}

#[test]
fn fast_search_deduplicates_repeated_worktree_hits() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("one/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );
    write(
        &repo.path().join("two/src/auth.rs"),
        "pub fn issue_token() { let token = \"session\"; }\n",
    );

    let results = search_repo_fast_filtered(
        repo.path(),
        "issue token session",
        10,
        &SearchFilters {
            require_all: true,
            ..SearchFilters::default()
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "one/src/auth.rs");
}

#[test]
fn fast_search_timeout_is_bounded() {
    let repo = tempfile::tempdir().unwrap();
    for index in 0..200 {
        write(
            &repo.path().join(format!("src/file_{index}.rs")),
            "pub fn unrelated_symbol() {}\n",
        );
    }

    let started = Instant::now();
    let results = search_repo_fast_filtered_with_timeout(
        repo.path(),
        "unrelated symbol",
        10,
        &SearchFilters::default(),
        Duration::from_nanos(1),
    )
    .unwrap();

    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(results.len() <= 10);
}
