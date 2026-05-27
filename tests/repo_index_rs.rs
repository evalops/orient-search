use std::fs;
use std::path::Path;

use orient::fast_index::FastIndex;
use orient::repo_index::{RepoIndexer, read_file_range};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn indexes_repo_symbols_search_related_files_and_commands() {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join("src/auth.py"),
        r#"
import json

class SessionManager:
    def issue_token(self, user_id: str) -> str:
        return json.dumps({"sub": user_id})

def verify_token(token: str) -> bool:
    return token.startswith("{")
"#,
    );
    write(
        &temp.path().join("tests/test_auth.py"),
        r#"
from src.auth import SessionManager, verify_token

def test_issue_token_round_trip():
    assert verify_token(SessionManager().issue_token("u_123"))
"#,
    );
    write(
        &temp.path().join("pyproject.toml"),
        "[project]\nname='sample'\n",
    );
    write(
        &temp.path().join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","typecheck":"tsc --noEmit"}}"#,
    );
    write(
        &temp.path().join("pnpm-lock.yaml"),
        "lockfileVersion: '9.0'\n",
    );

    let index = RepoIndexer::new(temp.path()).build().unwrap();

    let symbol = index.find_symbol("SessionManager", 10).remove(0);
    assert_eq!(symbol.path, "src/auth.py");
    assert_eq!(symbol.kind, "class");
    assert!(index.find_symbol("", 10).is_empty());
    assert!(index.find_symbol("SessionManager", 0).is_empty());

    let search = index.search_code("issue token user session", 3);
    assert_eq!(search[0].path, "src/auth.py");
    assert!(search[0].snippet.contains("issue_token"));

    let related: Vec<_> = index
        .related_files("src/auth.py", 10)
        .into_iter()
        .map(|item| item.path)
        .collect();
    assert!(related.contains(&"tests/test_auth.py".to_string()));

    let related_symbols = index.related_symbols(Some("src/auth.py"), Some("session token"), 10);
    assert_eq!(related_symbols[0].symbol.name, "SessionManager");
    assert_eq!(related_symbols[0].symbol.path, "src/auth.py");
    assert!(related_symbols[0].reason.contains("same file"));
    assert!(
        related_symbols
            .iter()
            .any(|item| item.symbol.name == "verify_token")
    );

    let brief = index.repo_brief();
    assert_eq!(brief.language_counts.get("python"), Some(&2));
    assert!(brief.known_commands.contains(&"pytest".to_string()));
    assert!(brief.known_commands.contains(&"pnpm test".to_string()));
    assert!(brief.known_commands.contains(&"pnpm run lint".to_string()));
    assert!(
        brief
            .known_commands
            .contains(&"pnpm run typecheck".to_string())
    );
    assert!(brief.manifest_files.contains(&"pyproject.toml".to_string()));
    assert!(
        brief
            .important_files
            .contains(&"pyproject.toml".to_string())
    );
}

#[test]
fn read_file_range_rejects_paths_outside_repo() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() {}\n",
    );
    write(
        &outside.path().join("outside.rs"),
        "pub fn outside_repo() {}\n",
    );

    let range = read_file_range(repo.path(), "src/auth.rs", 1, 10).unwrap();
    assert_eq!(range.start_line, 1);
    assert!(range.text.contains("issue_token"));

    let error = read_file_range(repo.path(), "../outside.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("repo-relative"));

    let absolute_error = read_file_range(
        repo.path(),
        outside.path().join("outside.rs").to_str().unwrap(),
        1,
        1,
    )
    .unwrap_err()
    .to_string();
    assert!(absolute_error.contains("repo-relative"));
}

#[cfg(unix)]
#[test]
fn read_file_range_rejects_symlink_escape() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &outside.path().join("outside.rs"),
        "pub fn outside_repo() {}\n",
    );
    std::os::unix::fs::symlink(
        outside.path().join("outside.rs"),
        repo.path().join("linked.rs"),
    )
    .unwrap();

    let error = read_file_range(repo.path(), "linked.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("inside repository"));
}

#[cfg(unix)]
#[test]
fn repo_and_persistent_indexes_ignore_symlinked_files() {
    let repo = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn visible_symbol() {}\n",
    );
    write(
        &outside.path().join("secrets.rs"),
        "pub fn leaked_secret() {}\n",
    );
    std::os::unix::fs::symlink(
        outside.path().join("secrets.rs"),
        repo.path().join("src/leaked.rs"),
    )
    .unwrap();

    let live = RepoIndexer::new(repo.path()).build().unwrap();
    assert!(live.search_code("visible symbol", 10)[0].path == "src/lib.rs");
    assert!(live.search_code("leaked secret", 10).is_empty());

    let persistent = FastIndex::build(repo.path()).unwrap();
    assert!(persistent.search("visible symbol", 10).unwrap()[0].path == "src/lib.rs");
    assert!(persistent.search("leaked secret", 10).unwrap().is_empty());
    assert!(
        !persistent
            .files
            .iter()
            .any(|file| file.path == "src/leaked.rs")
    );
}
