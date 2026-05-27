use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

#[test]
fn server_reports_tool_manifest_for_agent_wrappers() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let list_request = serde_json::json!({
        "id": "list",
        "tool": "list_tools",
        "arguments": {}
    });
    let manifest_request = serde_json::json!({
        "id": "manifest",
        "tool": "tool_manifest",
        "arguments": {}
    });
    writeln!(child.stdin.as_mut().unwrap(), "{list_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{manifest_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"list\""));
    assert!(stdout.contains("tool_manifest"));
    assert!(stdout.contains("\"id\":\"manifest\""));
    assert!(stdout.contains("\"name\":\"search_code\""));
    assert!(stdout.contains("\"required\":[\"repo\",\"query\"]"));
    assert!(stdout.contains("\"optional\""));
    assert!(stdout.contains("read_shard_range"));
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
fn server_handles_shard_index_search_and_read_requests() {
    let parent = tempfile::tempdir().unwrap();
    let auth_repo = parent.path().join("auth");
    let billing_repo = parent.path().join("billing");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("src/legacy.rs"),
        "pub fn legacy_invoice() -> usize { 1 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = parent.path().join(".orient-shards");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let index_request = serde_json::json!({
        "id": "index-shards",
        "tool": "index_shards",
        "arguments": {
            "repos": [auth_repo, billing_repo],
            "output_dir": shard_dir
        }
    });
    let search_request = serde_json::json!({
        "id": "search-shards",
        "tool": "search_shards",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "repo:billing invoice total",
            "limit": 5,
            "require_all": true,
            "explain": true
        }
    });
    let read_request = serde_json::json!({
        "id": "read-shard-range",
        "tool": "read_shard_range",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "billing/src/billing.rs",
            "start": 1,
            "lines": 1
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{index_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"index-shards\""));
    assert!(stdout.contains("\"shards\":2"));
    assert!(stdout.contains("\"id\":\"search-shards\""));
    assert!(stdout.contains("billing/src/billing.rs"));
    assert!(stdout.contains("shard:billing"));
    assert!(!stdout.contains("auth/src/auth.rs"));
    assert!(stdout.contains("\"id\":\"read-shard-range\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("invoice_total"));
}

#[test]
fn server_handles_shard_refresh_request() {
    let parent = tempfile::tempdir().unwrap();
    let auth_repo = parent.path().join("auth");
    let billing_repo = parent.path().join("billing");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("src/legacy.rs"),
        "pub fn legacy_invoice() -> usize { 1 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = parent.path().join(".orient-shards");

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let status = Command::new(&binary)
        .args([
            "index-shards",
            "--repo",
            auth_repo.to_str().unwrap(),
            "--repo",
            billing_repo.to_str().unwrap(),
            "--output-dir",
            shard_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\npub fn credit_memo() -> usize { 7 }\n",
    );
    fs::rename(
        billing_repo.join("src/legacy.rs"),
        billing_repo.join("src/refunds.rs"),
    )
    .unwrap();
    write(
        &billing_repo.join("src/refunds.rs"),
        "pub fn refund_credit() -> usize { 1 }\n",
    );

    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let refresh_request = serde_json::json!({
        "id": "refresh-shards",
        "tool": "refresh_shards",
        "arguments": {
            "index_dir": shard_dir
        }
    });
    let search_request = serde_json::json!({
        "id": "search-after-refresh",
        "tool": "search_shards",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "repo:billing credit memo",
            "limit": 5,
            "require_all": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{refresh_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"refresh-shards\""));
    assert!(stdout.contains("\"reused_files\""));
    assert!(stdout.contains("\"refreshed_files\""));
    assert!(stdout.contains("\"deleted_files\":1"));
    assert!(stdout.contains("\"id\":\"search-after-refresh\""));
    assert!(stdout.contains("credit_memo"));
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
    let symbols_request = serde_json::json!({
        "id": "symbols",
        "tool": "related_symbols",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{range_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbols_request}").unwrap();
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
    assert!(stdout.contains("\"id\":\"symbols\""));
    assert!(stdout.contains("same file"));
}
