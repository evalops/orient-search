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
    write(
        &temp.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
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
fn cli_outputs_repo_map_and_reads_ranges() {
    let repo = sample_repo();
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issues_tokens() {}\n",
    );

    let mut repo_map = Command::cargo_bin("orient").unwrap();
    repo_map
        .args([
            "repo-map",
            "--repo",
            repo.path().to_str().unwrap(),
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"entrypoints\""))
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("SessionManager"));

    let mut read_range = Command::cargo_bin("orient").unwrap();
    read_range
        .args([
            "read-range",
            "--repo",
            repo.path().to_str().unwrap(),
            "src/auth.rs",
            "--start",
            "3",
            "--lines",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"start_line\":3"))
        .stdout(predicate::str::contains("issue_token"));
}

#[test]
fn cli_searches_symbols_and_related_files() {
    let repo = sample_repo();

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "issue token",
            "--path",
            "src/",
            "--language",
            "rust",
            "--extension",
            "rs",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"));

    let mut symbol = Command::cargo_bin("orient").unwrap();
    symbol
        .args([
            "symbol",
            "--repo",
            repo.path().to_str().unwrap(),
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\":\"struct\""));
}

#[test]
fn cli_builds_and_searches_persistent_index() {
    let repo = sample_repo();
    let index_path = repo.path().join(".orient/index");

    let mut index = Command::cargo_bin("orient").unwrap();
    index
        .args([
            "index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"terms\""));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "issue token",
            "--path",
            "src/",
            "--language",
            "rust",
            "--extension",
            "rs",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("indexed match"));

    write(
        &repo.path().join("src/auth.rs"),
        r#"
pub struct SessionManager;

impl SessionManager {
    pub fn rotate_secret() -> String {
        "secret".to_string()
    }
}
"#,
    );

    let mut refresh = Command::cargo_bin("orient").unwrap();
    refresh
        .args([
            "refresh-index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--index",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"refreshed_files\""));

    let mut refreshed_search = Command::cargo_bin("orient").unwrap();
    refreshed_search
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "rotate secret",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"));
}

#[test]
fn cli_reports_search_benchmarks() {
    let repo = sample_repo();

    let mut fallback = Command::cargo_bin("orient").unwrap();
    fallback
        .args([
            "bench-search",
            "--repo",
            repo.path().to_str().unwrap(),
            "--runs",
            "2",
            "--warmup",
            "1",
            "--fail-p95-ms",
            "1000",
            "issue token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"mode\":\"fallback\""))
        .stdout(predicate::str::contains("\"p95_ms\""));

    let index_path = repo.path().join(".orient/index");
    let mut index = Command::cargo_bin("orient").unwrap();
    index
        .args([
            "index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut indexed = Command::cargo_bin("orient").unwrap();
    indexed
        .args([
            "bench-search",
            "--repo",
            repo.path().to_str().unwrap(),
            "--index",
            index_path.to_str().unwrap(),
            "--runs",
            "2",
            "--warmup",
            "1",
            "--fail-p95-ms",
            "1000",
            "issue token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"mode\":\"indexed\""))
        .stdout(predicate::str::contains("\"p95_ms\""));
}

#[test]
fn cli_benchmark_can_fail_on_p95_threshold() {
    let repo = sample_repo();

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "bench-search",
        "--repo",
        repo.path().to_str().unwrap(),
        "--runs",
        "1",
        "--warmup",
        "0",
        "--fail-p95-ms",
        "0",
        "issue token",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("exceeded threshold"));
}
