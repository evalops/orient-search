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
            "--snippet",
            "block",
            "--explain",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("\"explanation\""));

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

    let mut related_symbols = Command::cargo_bin("orient").unwrap();
    related_symbols
        .args([
            "related-symbols",
            "--repo",
            repo.path().to_str().unwrap(),
            "--path",
            "src/auth.rs",
            "--query",
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("same file"));
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
            "--snippet",
            "symbol",
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
fn cli_builds_and_searches_shard_directory() {
    let auth_repo = sample_repo();
    let billing_repo = tempfile::tempdir().unwrap();
    write(
        &billing_repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.path().join("src/legacy.rs"),
        "pub fn legacy_invoice() -> usize { 1 }\n",
    );
    write(
        &billing_repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "index-shards",
            "--repo",
            auth_repo.path().to_str().unwrap(),
            "--repo",
            billing_repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"shards\":2"))
        .stdout(predicate::str::contains("\"path_terms\""));
    assert!(shard_dir.path().join("manifest.json").exists());

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice total",
            "--require-all",
            "--explain",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing.rs"))
        .stdout(predicate::str::contains("shard:"));

    let billing_name = billing_repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let mut repo_filtered = Command::cargo_bin("orient").unwrap();
    repo_filtered
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice total",
            "--repo",
            &billing_name,
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing.rs"));

    let mut read = Command::cargo_bin("orient").unwrap();
    read.args([
        "read-shard-range",
        "--index-dir",
        shard_dir.path().to_str().unwrap(),
        &format!("{billing_name}/src/billing.rs"),
        "--start",
        "1",
        "--lines",
        "1",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"path\""))
    .stdout(predicate::str::contains("invoice_total"));

    write(
        &billing_repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\npub fn credit_memo() -> usize { 7 }\n",
    );
    fs::rename(
        billing_repo.path().join("src/legacy.rs"),
        billing_repo.path().join("src/refunds.rs"),
    )
    .unwrap();
    write(
        &billing_repo.path().join("src/refunds.rs"),
        "pub fn refund_credit() -> usize { 1 }\n",
    );
    let mut refresh = Command::cargo_bin("orient").unwrap();
    refresh
        .args([
            "refresh-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"reused_files\""))
        .stdout(predicate::str::contains("\"refreshed_files\""))
        .stdout(predicate::str::contains("\"deleted_files\":1"));

    let mut refreshed_search = Command::cargo_bin("orient").unwrap();
    refreshed_search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "credit memo",
            "--repo",
            &billing_name,
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("credit_memo"));

    let mut renamed_search = Command::cargo_bin("orient").unwrap();
    renamed_search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "refund credit",
            "--repo",
            &billing_name,
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/refunds.rs"));
}

#[test]
fn cli_reports_search_benchmarks() {
    let repo = sample_repo();
    let baseline_path = repo.path().join(".orient/fallback-bench.json");

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
            "--write-baseline",
            baseline_path.to_str().unwrap(),
            "issue token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"mode\":\"fallback\""))
        .stdout(predicate::str::contains("\"p95_ms\""));
    assert!(baseline_path.exists());

    let mut baseline_check = Command::cargo_bin("orient").unwrap();
    baseline_check
        .args([
            "bench-search",
            "--repo",
            repo.path().to_str().unwrap(),
            "--runs",
            "2",
            "--warmup",
            "1",
            "--baseline",
            baseline_path.to_str().unwrap(),
            "--max-p95-regression",
            "1000",
            "issue token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"mode\":\"fallback\""));

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

#[test]
fn cli_benchmark_can_fail_against_saved_baseline() {
    let repo = sample_repo();
    let baseline_path = repo.path().join(".orient/too-fast.json");
    write(
        &baseline_path,
        r#"{
  "mode": "fallback",
  "runs": 1,
  "warmup": 0,
  "limit": 10,
  "queries": [
    {
      "query": "issue token",
      "result_count": 1,
      "min_ms": 0.0,
      "p50_ms": 0.0,
      "p95_ms": 0.0,
      "max_ms": 0.0,
      "samples_ms": [0.0]
    }
  ]
}"#,
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "bench-search",
        "--repo",
        repo.path().to_str().unwrap(),
        "--runs",
        "1",
        "--warmup",
        "0",
        "--baseline",
        baseline_path.to_str().unwrap(),
        "--max-p95-regression",
        "0",
        "issue token",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("exceeded baseline"));
}
