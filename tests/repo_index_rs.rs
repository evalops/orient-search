use std::fs;
use std::path::Path;

use orient::fast_index::FastIndex;
use orient::repo_index::{
    MAX_READ_RANGE_LINES, RepoIndexer, read_file_range, search_repo_fast_filtered,
};

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
        "[project]\nname='sample'\ndependencies=['fastapi>=0.100', 'pydantic']\n",
    );
    write(
        &temp.path().join("package.json"),
        r#"{"scripts":{"test":"vitest run","lint":"eslint .","typecheck":"tsc --noEmit"},"dependencies":{"react":"latest"},"devDependencies":{"typescript":"latest"}}"#,
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
    let test_related: Vec<_> = index
        .related_files("tests/test_auth.py", 10)
        .into_iter()
        .map(|item| item.path)
        .collect();
    assert!(test_related.contains(&"src/auth.py".to_string()));

    let related_symbols = index.related_symbols(Some("src/auth.py"), Some("session token"), 10);
    assert_eq!(related_symbols[0].symbol.name, "SessionManager");
    assert_eq!(related_symbols[0].symbol.path, "src/auth.py");
    assert!(related_symbols[0].reason.contains("same file"));
    assert!(
        related_symbols
            .iter()
            .any(|item| item.symbol.name == "verify_token")
    );
    let test_related_symbols = index.related_symbols(Some("tests/test_auth.py"), None, 10);
    assert!(
        test_related_symbols
            .iter()
            .any(|item| item.symbol.name == "SessionManager" && item.symbol.path == "src/auth.py"),
        "{test_related_symbols:?}"
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
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "pnpm test" && hint.kind == "test" && hint.source == "package.json"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "pnpm run lint" && hint.kind == "lint" && hint.source == "package.json"
    }));
    assert!(brief.command_hints.iter().any(|hint| {
        hint.command == "pytest" && hint.kind == "test" && hint.source == "pyproject.toml"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "fastapi" && hint.kind == "dependency" && hint.source == "pyproject.toml"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "react" && hint.kind == "dependency" && hint.source == "package.json"
    }));
    assert!(brief.dependency_hints.iter().any(|hint| {
        hint.name == "typescript" && hint.kind == "dev_dependency" && hint.source == "package.json"
    }));
    assert!(brief.import_hints.iter().any(|hint| {
        hint.module == "json"
            && hint.kind == "import"
            && hint.source == "src/auth.py"
            && hint.line == 2
    }));
    assert!(brief.import_hints.iter().any(|hint| {
        hint.module == "src.auth"
            && hint.kind == "from"
            && hint.source == "tests/test_auth.py"
            && hint.line == 2
    }));
    assert!(brief.manifest_files.contains(&"pyproject.toml".to_string()));
    assert!(
        brief
            .important_files
            .contains(&"pyproject.toml".to_string())
    );

    let map = index.repo_map(10, 10);
    assert!(
        map.related_files.iter().any(|related| {
            related.source_path == "src/auth.py" && related.path == "tests/test_auth.py"
        }),
        "{:?}",
        map.related_files
    );
    assert!(
        map.related_symbols.iter().any(|related| {
            related.source_path == "src/auth.py"
                && related.symbol.name == "SessionManager"
                && related.reason.contains("same file")
        }),
        "{:?}",
        map.related_symbols
    );
}

#[test]
fn fallback_symbol_filter_finds_deep_private_definition_after_broad_hits() {
    let repo = tempfile::tempdir().unwrap();
    let mut source = String::new();
    for index in 0..40 {
        source.push_str(&format!(
            "// early broad token noise {index}: target symbol name routing\n"
        ));
    }
    source.push_str("fn target_symbol_name() -> bool { true }\n");
    write(&repo.path().join("src/lib.rs"), &source);

    let results = search_repo_fast_filtered(
        repo.path(),
        "symbol:target_symbol_name",
        5,
        &Default::default(),
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "src/lib.rs");
    assert!(results[0].reason.contains("symbol:target_symbol_name"));
    assert!(results[0].snippet.contains("target_symbol_name"));

    let mixed = search_repo_fast_filtered(
        repo.path(),
        "routing symbol:target_symbol_name",
        5,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(mixed.len(), 1);
    assert!(mixed[0].reason.contains("symbol:target_symbol_name"));
    assert!(mixed[0].snippet.contains("target_symbol_name"));
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

    let long_text = (1..=MAX_READ_RANGE_LINES + 10)
        .map(|line| format!("line_{line}\n"))
        .collect::<String>();
    write(&repo.path().join("src/long.rs"), &long_text);
    let capped = read_file_range(repo.path(), "src/long.rs", 1, MAX_READ_RANGE_LINES + 10).unwrap();
    assert_eq!(capped.start_line, 1);
    assert_eq!(capped.end_line, MAX_READ_RANGE_LINES);
    assert!(!capped.text.contains(&format!(
        "{}: line_{}",
        MAX_READ_RANGE_LINES + 1,
        MAX_READ_RANGE_LINES + 1
    )));

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
