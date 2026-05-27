use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn sample_repo() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join("src/auth.rs"),
        r#"
pub struct SessionManager;

impl SessionManager {
    pub fn issue_token(user_id: &str) -> String {
        format!("token-{user_id}")
    }
}
"#,
    );
    write(&temp.path().join("Cargo.toml"), "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n");
    temp
}

#[test]
fn cli_outputs_repo_brief_as_json() {
    let repo = sample_repo();

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args(["brief", "--repo", repo.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"known_commands\""))
        .stdout(predicate::str::contains("cargo test"));
}

#[test]
fn cli_searches_symbols_and_related_files() {
    let repo = sample_repo();

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args(["search", "--repo", repo.path().to_str().unwrap(), "issue token"])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"));

    let mut symbol = Command::cargo_bin("orient").unwrap();
    symbol
        .args(["symbol", "--repo", repo.path().to_str().unwrap(), "SessionManager"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\":\"struct\""));
}

#[test]
fn cli_reports_jsonl_metrics() {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join(".codex/sample.jsonl"),
        r#"
{"type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"rg auth src\"}","call_id":"c1"}}
{"type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":"Process exited with code 0\nOutput:\nsrc/auth.py"}}
"#,
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args(["metrics", "--root", temp.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"total_calls\":1"))
        .stdout(predicate::str::contains("search_discovery"));
}
