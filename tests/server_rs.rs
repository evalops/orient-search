use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn server_handles_json_lines_tool_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let request = serde_json::json!({
        "id": 1,
        "tool": "search_code",
        "arguments": {
            "repo": repo.path(),
            "query": "issue token",
            "limit": 3,
            "extension": "rs",
            "require_all": true,
            "explain": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":1"));
    assert!(stdout.contains("src/auth.rs"));
    assert!(stdout.contains("\"explanation\""));
    assert!(stdout.contains("symbol_exact"));
}

#[test]
fn server_handles_indexed_search_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let index_path = repo.path().join(".orient/index");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let status = Command::new(&binary)
        .args([
            "index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let request = serde_json::json!({
        "id": 2,
        "tool": "indexed_search_code",
        "arguments": {
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "language": "rust",
            "require_all": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":2"));
    assert!(stdout.contains("src/auth.rs"));
}

#[test]
fn server_handles_repo_map_and_read_range_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\nimpl SessionManager {\n    pub fn issue_token() {}\n}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "#[test]\nfn issues_tokens() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let map_request = serde_json::json!({
        "id": "map",
        "tool": "repo_map",
        "arguments": {
            "repo": repo.path(),
            "symbols": 5,
            "tests": 5
        }
    });
    let range_request = serde_json::json!({
        "id": "range",
        "tool": "read_range",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 2
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{range_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"map\""));
    assert!(stdout.contains("SessionManager"));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"range\""));
    assert!(stdout.contains("\"start_line\":2"));
    assert!(stdout.contains("issue_token"));
}
