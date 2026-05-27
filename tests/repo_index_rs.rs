use std::fs;
use std::path::Path;

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

    let index = RepoIndexer::new(temp.path()).build().unwrap();

    let symbol = index.find_symbol("SessionManager", 10).remove(0);
    assert_eq!(symbol.path, "src/auth.py");
    assert_eq!(symbol.kind, "class");

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
}

#[test]
fn read_file_range_rejects_paths_outside_repo() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub fn issue_token() {}\n",
    );

    let range = read_file_range(repo.path(), "src/auth.rs", 1, 10).unwrap();
    assert_eq!(range.start_line, 1);
    assert!(range.text.contains("issue_token"));

    let error = read_file_range(repo.path(), "../outside.rs", 1, 1)
        .unwrap_err()
        .to_string();
    assert!(error.contains("repo-relative"));
}
