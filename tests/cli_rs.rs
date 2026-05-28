use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command as ProcessCommand, Stdio};

use assert_cmd::Command;
use orient::server::{MAX_BATCH_QUERIES, MAX_BATCH_RANGES};
use predicates::prelude::*;

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn git(repo: &Path, args: &[&str]) {
    let status = ProcessCommand::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {:?} failed", args);
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
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n[dependencies]\nserde='1'\n",
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
        .stdout(predicate::str::contains("\"manifest_files\""))
        .stdout(predicate::str::contains("cargo test"));
}

#[test]
fn cli_outputs_tool_manifest() {
    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.arg("tool-manifest")
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"discover_repos\""))
        .stdout(predicate::str::contains("\"name\":\"search_code\""))
        .stdout(predicate::str::contains("\"name\":\"search\""))
        .stdout(predicate::str::contains("\"name\":\"search_auto\""))
        .stdout(predicate::str::contains("\"name\":\"search_auto_batch\""))
        .stdout(predicate::str::contains("\"name\":\"search_query_plan\""))
        .stdout(predicate::str::contains("\"name\":\"search_plan\""))
        .stdout(predicate::str::contains("\"name\":\"indexed_search\""))
        .stdout(predicate::str::contains("\"name\":\"index_plan\""))
        .stdout(predicate::str::contains("\"name\":\"shard_plan\""))
        .stdout(predicate::str::contains("\"name\":\"mcp_manifest\""))
        .stdout(predicate::str::contains("\"name\":\"agent_guide\""))
        .stdout(predicate::str::contains(
            "\"required\":[\"repo\",\"query\"]",
        ))
        .stdout(predicate::str::contains("daemon_status"))
        .stdout(predicate::str::contains("warm_index"))
        .stdout(predicate::str::contains("warm_shards"))
        .stdout(predicate::str::contains("single_warmed_index"))
        .stdout(predicate::str::contains("single_warmed_shard_dir"))
        .stdout(predicate::str::contains("open_range"))
        .stdout(predicate::str::contains("read_ranges"))
        .stdout(predicate::str::contains("open_index_range"))
        .stdout(predicate::str::contains("search_batch"))
        .stdout(predicate::str::contains("search_plan_batch"))
        .stdout(predicate::str::contains("read_index_ranges"))
        .stdout(predicate::str::contains("indexed_search_batch"))
        .stdout(predicate::str::contains("indexed_query_plan_batch"))
        .stdout(predicate::str::contains("open_shard_range"))
        .stdout(predicate::str::contains("read_shard_ranges"))
        .stdout(predicate::str::contains("search_shards_batch"))
        .stdout(predicate::str::contains("shard_query_plan_batch"))
        .stdout(predicate::str::contains("read_shard_range"))
        .stdout(predicate::str::contains("related_shard_files"))
        .stdout(predicate::str::contains("related_shard_symbols"))
        .stdout(predicate::str::contains("context_lines"))
        .stdout(predicate::str::contains("\"arguments\""))
        .stdout(predicate::str::contains("\"input_schema\""))
        .stdout(predicate::str::contains("\"default\":10"))
        .stdout(predicate::str::contains("\"maximum\":100"))
        .stdout(predicate::str::contains("\"maximum\":1000"))
        .stdout(predicate::str::contains("\"maxItems\":32"))
        .stdout(predicate::str::contains("\"maxItems\":64"))
        .stdout(predicate::str::contains("\"type\":\"range|range[]\""));
}

#[test]
fn cli_suppresses_broken_pipe_when_output_consumer_closes() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = ProcessCommand::new(binary)
        .arg("tool-manifest")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut buffer = [0u8; 128];
    let read = stdout.read(&mut buffer).unwrap();
    assert!(read > 0);
    drop(stdout);

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(!stderr.contains("Broken pipe"), "{stderr}");
}

#[test]
fn cli_outputs_mcp_manifest() {
    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.arg("mcp-manifest")
        .assert()
        .success()
        .stdout(predicate::str::contains("\"tools\""))
        .stdout(predicate::str::contains("\"name\":\"search_code\""))
        .stdout(predicate::str::contains("\"name\":\"agent_guide\""))
        .stdout(predicate::str::contains("\"name\":\"agent_instructions\""))
        .stdout(predicate::str::contains("\"inputSchema\""))
        .stdout(predicate::str::contains("\"annotations\""))
        .stdout(predicate::str::contains("\"readOnlyHint\":true"))
        .stdout(predicate::str::contains("\"destructiveHint\":false"))
        .stdout(predicate::str::contains(
            "\"required\":[\"repo\",\"query\"]",
        ))
        .stdout(predicate::str::contains("\"input_schema\"").not());
}

#[test]
fn cli_outputs_agent_guide() {
    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "agent-guide",
        "--repo",
        "/work/repo",
        "--index",
        "/tmp/repo.index",
        "--index-dir",
        "/tmp/orient-shards",
        "--addr",
        "127.0.0.1:9999",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"purpose\""))
    .stdout(predicate::str::contains("no session analytics"))
    .stdout(predicate::str::contains("\"tool\":\"search_shards\""))
    .stdout(predicate::str::contains("\"tool\":\"search_auto\""))
    .stdout(predicate::str::contains("\"tool\":\"search_auto_batch\""))
    .stdout(predicate::str::contains("\"tool\":\"indexed_search_code\""))
    .stdout(predicate::str::contains("\"tool\":\"search_code\""))
    .stdout(predicate::str::contains("instruction_snippet"))
    .stdout(predicate::str::contains(
        "Use Orient as the first local code-discovery step",
    ))
    .stdout(predicate::str::contains("read_request"))
    .stdout(predicate::str::contains("127.0.0.1:9999"))
    .stdout(predicate::str::contains("/work/repo"))
    .stdout(predicate::str::contains("/tmp/repo.index"));
}

#[test]
fn cli_outputs_agent_instructions() {
    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "agent-instructions",
        "--repo",
        "/work/repo",
        "--index",
        "/tmp/repo.index",
        "--index-dir",
        "/tmp/orient-shards",
        "--addr",
        "127.0.0.1:9999",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("## Orient Search"))
    .stdout(predicate::str::contains(
        "Use Orient as the first local code-discovery step",
    ))
    .stdout(predicate::str::contains(
        "orient client-jsonl --addr 127.0.0.1:9999",
    ))
    .stdout(predicate::str::contains(
        "orient ensure-index --repo /work/repo --index /tmp/repo.index",
    ))
    .stdout(predicate::str::contains("search_auto_batch"))
    .stdout(predicate::str::contains("read_batch_request"))
    .stdout(predicate::str::contains("no session analytics"));
}

#[test]
fn cli_search_auto_selects_live_indexed_and_shard_surfaces() {
    let repo = sample_repo();
    let index_path = repo.path().join("orient.index");
    let shard_dir = repo.path().join("shards");

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

    let mut shards = Command::cargo_bin("orient").unwrap();
    shards
        .args([
            "ensure-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut live = Command::cargo_bin("orient").unwrap();
    live.args([
        "search-auto",
        "--repo",
        repo.path().to_str().unwrap(),
        "issue_token",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"surface\":\"fallback\""))
    .stdout(predicate::str::contains("\"tool\":\"search_query_plan\""))
    .stdout(predicate::str::contains("\"tool\":\"repo_map\""))
    .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
    .stdout(predicate::str::contains("\"tool\":\"read_range\""));

    let mut live_default = Command::cargo_bin("orient").unwrap();
    live_default
        .current_dir(repo.path())
        .args(["search-auto", "issue_token"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"surface\":\"fallback\""))
        .stdout(predicate::str::contains("\"tool\":\"search_query_plan\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""));

    let mut indexed = Command::cargo_bin("orient").unwrap();
    indexed
        .args([
            "search-auto",
            "--index",
            index_path.to_str().unwrap(),
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"surface\":\"indexed\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_query_plan\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_repo_map\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_ranges\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""));

    let mut shard = Command::cargo_bin("orient").unwrap();
    shard
        .args([
            "search-auto",
            "--index-dir",
            shard_dir.to_str().unwrap(),
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"surface\":\"shards\""))
        .stdout(predicate::str::contains("\"tool\":\"shard_query_plan\""))
        .stdout(predicate::str::contains("\"tool\":\"shard_repo_map\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_ranges\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_range\""));

    let mut empty_live = Command::cargo_bin("orient").unwrap();
    empty_live
        .args([
            "search-auto",
            "--repo",
            repo.path().to_str().unwrap(),
            "issue_token definitely_missing",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query_plan_result\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"tool\":\"search_code\""));

    let mut diagnosed_live = Command::cargo_bin("orient").unwrap();
    diagnosed_live
        .args([
            "search-auto",
            "--repo",
            repo.path().to_str().unwrap(),
            "--diagnose",
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query_plan_result\""))
        .stdout(predicate::str::contains("\"final_match_count\":1"));

    let mut diagnosed_bad_scope = Command::cargo_bin("orient").unwrap();
    let output = diagnosed_bad_scope
        .args([
            "search-auto",
            "--repo",
            repo.path().to_str().unwrap(),
            "--diagnose",
            "path:not-real lang:rust issue_token",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let plan = &value["query_plan_result"];
    assert_eq!(plan["repair_hints"][0]["kind"], "relax_path_filter");
    assert_eq!(plan["retry_requests"][0]["arguments"]["language"], "rust");
    assert!(plan["retry_requests"][0]["arguments"]["path"].is_null());

    let mut diagnosed_filter_only_scope = Command::cargo_bin("orient").unwrap();
    let output = diagnosed_filter_only_scope
        .args([
            "search-auto",
            "--repo",
            repo.path().to_str().unwrap(),
            "--diagnose",
            "file:not-real.rs lang:rust",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let plan = &value["query_plan_result"];
    assert_eq!(plan["repair_hints"][0]["kind"], "relax_file_filter");
    assert_eq!(plan["retry_requests"][0]["arguments"]["query"], "");
    assert_eq!(plan["retry_requests"][0]["arguments"]["language"], "rust");
    assert!(plan["retry_requests"][0]["arguments"]["file"].is_null());
}

#[test]
fn cli_search_accepts_index_and_shard_targets() {
    let repo = sample_repo();
    let index_path = repo.path().join("orient.index");
    let shard_dir = repo.path().join("shards");

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

    let mut shards = Command::cargo_bin("orient").unwrap();
    shards
        .args([
            "ensure-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut indexed = Command::cargo_bin("orient").unwrap();
    indexed
        .args([
            "search",
            "--index",
            index_path.to_str().unwrap(),
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""))
        .stdout(predicate::str::contains("\"tool\":\"related_index_files\""));

    let mut shard = Command::cargo_bin("orient").unwrap();
    shard
        .args([
            "search",
            "--index-dir",
            shard_dir.to_str().unwrap(),
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_range\""))
        .stdout(predicate::str::contains("\"tool\":\"related_shard_files\""));
}

#[test]
fn cli_search_auto_batch_returns_query_surfaces() {
    let repo = sample_repo();
    let index_path = repo.path().join("orient.index");

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

    let mut batch = Command::cargo_bin("orient").unwrap();
    batch
        .args([
            "search-auto-batch",
            "--index",
            index_path.to_str().unwrap(),
            "issue_token",
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query\":\"issue_token\""))
        .stdout(predicate::str::contains("\"query\":\"SessionManager\""))
        .stdout(predicate::str::contains("\"surface\":\"indexed\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_query_plan\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_repo_map\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_ranges\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""));

    let mut empty_batch = Command::cargo_bin("orient").unwrap();
    empty_batch
        .args([
            "search-auto-batch",
            "--index",
            index_path.to_str().unwrap(),
            "issue_token definitely_missing",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query_plan_result\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_search_code\""));

    let mut diagnosed_batch = Command::cargo_bin("orient").unwrap();
    diagnosed_batch
        .args([
            "search-auto-batch",
            "--index",
            index_path.to_str().unwrap(),
            "--diagnose",
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query_plan_result\""))
        .stdout(predicate::str::contains("\"final_match_count\":1"));

    let mut default_live_batch = Command::cargo_bin("orient").unwrap();
    default_live_batch
        .current_dir(repo.path())
        .args(["search-auto-batch", "issue_token", "SessionManager"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query\":\"issue_token\""))
        .stdout(predicate::str::contains("\"query\":\"SessionManager\""))
        .stdout(predicate::str::contains("\"surface\":\"fallback\""))
        .stdout(predicate::str::contains("\"tool\":\"search_query_plan\""));
}

#[test]
fn cli_rejects_oversized_batches() {
    let repo = sample_repo();
    let mut cmd = Command::cargo_bin("orient").unwrap();
    let mut args = vec!["search-batch", "--repo", repo.path().to_str().unwrap()];
    let queries = (0..=MAX_BATCH_QUERIES)
        .map(|index| format!("query_{index}"))
        .collect::<Vec<_>>();
    args.extend(queries.iter().map(String::as_str));
    cmd.args(args)
        .assert()
        .failure()
        .stderr(predicate::str::contains("max 32"));

    let mut cmd = Command::cargo_bin("orient").unwrap();
    let mut args = vec!["read-ranges", "--repo", repo.path().to_str().unwrap()];
    let paths = (0..=MAX_BATCH_RANGES)
        .map(|_| "src/auth.rs".to_string())
        .collect::<Vec<_>>();
    args.extend(paths.iter().map(String::as_str));
    cmd.args(args)
        .assert()
        .failure()
        .stderr(predicate::str::contains("max 64"));
}

#[test]
fn cli_discovers_repos_for_shard_setup() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("workspace/billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("workspace/auth/package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    write(
        &root.path().join("workspace/git-only/.git"),
        "gitdir: real\n",
    );
    write(
        &root
            .path()
            .join("workspace/node_modules/ignored/Cargo.toml"),
        "[package]\nname='ignored'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("workspace/deep/too/far/Cargo.toml"),
        "[package]\nname='too-far'\nversion='0.1.0'\nedition='2024'\n",
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "discover-repos",
        "--root",
        root.path().to_str().unwrap(),
        "--max-depth",
        "2",
        "--limit",
        "10",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"repos_found\":3"))
    .stdout(predicate::str::contains("\"name\":\"auth\""))
    .stdout(predicate::str::contains("\"name\":\"billing\""))
    .stdout(predicate::str::contains("\"name\":\"git-only\""))
    .stdout(predicate::str::contains("node_modules").not())
    .stdout(predicate::str::contains("too-far").not());
}

#[test]
fn cli_discovery_prioritizes_visible_repos_before_temp_roots() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join(".tmp-platform/Cargo.toml"),
        "[package]\nname='tmp-platform'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root
            .path()
            .join("platform-feature-split-20260527/Cargo.toml"),
        "[package]\nname='platform-feature'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("platform/Cargo.toml"),
        "[package]\nname='platform'\nversion='0.1.0'\nedition='2024'\n",
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "discover-repos",
        "--root",
        root.path().to_str().unwrap(),
        "--max-depth",
        "1",
        "--limit",
        "1",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"name\":\"platform\""))
    .stdout(predicate::str::contains("platform-feature-split").not())
    .stdout(predicate::str::contains(".tmp-platform").not());
}

#[test]
fn cli_discovery_can_group_git_worktree_families() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/project");
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&repo, &["init", "-b", "main"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/evalops/project.git",
        ],
    );
    git(&repo, &["add", "Cargo.toml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "feature/search",
            "../project-feature",
        ],
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    cmd.args([
        "discover-repos",
        "--root",
        root.path().to_str().unwrap(),
        "--max-depth",
        "2",
        "--limit",
        "10",
        "--git-metadata",
        "--tracked-files",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"repos_found\":2"))
    .stdout(predicate::str::contains("\"families\""))
    .stdout(predicate::str::contains("\"checkouts\":2"))
    .stdout(predicate::str::contains("\"worktrees\":1"))
    .stdout(predicate::str::contains("\"clones\":1"))
    .stdout(predicate::str::contains("\"tracked_files\":2"))
    .stdout(predicate::str::contains(
        "https://github.com/evalops/project.git",
    ))
    .stdout(predicate::str::contains("\"git_kind\":\"worktree\""))
    .stdout(predicate::str::contains("\"branch\":\"feature/search\""));
}

#[test]
fn cli_discovery_can_limit_repeated_repo_families() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/project");
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&repo, &["init", "-b", "main"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/evalops/project.git",
        ],
    );
    git(&repo, &["add", "Cargo.toml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "feature/search",
            "../project-feature",
        ],
    );

    let mut cmd = Command::cargo_bin("orient").unwrap();
    let output = cmd
        .args([
            "discover-repos",
            "--root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "2",
            "--limit",
            "10",
            "--family-limit",
            "1",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(report["candidates_found"], 2);
    assert_eq!(report["repos_found"], 1);
    assert_eq!(report["family_limit"], 1);
    assert_eq!(report["repos"][0]["name"], "project");
    assert_eq!(report["families"][0]["checkouts"], 2);
    assert!(
        report["families"][0]["paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap().contains("project-feature"))
    );
}

#[test]
fn cli_discovery_treats_git_roots_as_manifest_boundaries_by_default() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/platform");
    write(
        &repo.join("package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    write(
        &repo.join("packages/ui/package.json"),
        "{\"scripts\":{\"test\":\"vitest\"}}\n",
    );
    git(&repo, &["init", "-b", "main"]);

    let mut default = Command::cargo_bin("orient").unwrap();
    default
        .args([
            "discover-repos",
            "--root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "4",
            "--limit",
            "20",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"repos_found\":1"))
        .stdout(predicate::str::contains("\"name\":\"platform\""))
        .stdout(predicate::str::contains("\"name\":\"ui\"").not());

    let mut nested = Command::cargo_bin("orient").unwrap();
    nested
        .args([
            "discover-repos",
            "--root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "4",
            "--limit",
            "20",
            "--nested-manifests",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"repos_found\":2"))
        .stdout(predicate::str::contains("\"name\":\"platform\""))
        .stdout(predicate::str::contains("\"name\":\"ui\""));
}

#[test]
fn cli_indexes_shards_from_discovered_root() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("workspace/billing/src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &root.path().join("workspace/billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &root.path().join("workspace/auth/src/lib.rs"),
        "pub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &root.path().join("workspace/auth/Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut index = Command::cargo_bin("orient").unwrap();
    index
        .args([
            "index-shards",
            "--discover-root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "2",
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"shards\":2"))
        .stdout(predicate::str::contains("\"posting_entries\""))
        .stdout(predicate::str::contains("\"compressed_posting_bytes\""));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice_total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing/src/lib.rs"));
}

#[test]
fn cli_indexes_only_selected_family_representatives_when_limited() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("workspace/project");
    write(
        &repo.join("src/lib.rs"),
        "pub fn selected_family_repo() -> usize { 1 }\n",
    );
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(&repo, &["init", "-b", "main"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/evalops/project.git",
        ],
    );
    git(&repo, &["add", "Cargo.toml", "src/lib.rs"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "feature/search",
            "../project-feature",
        ],
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut index = Command::cargo_bin("orient").unwrap();
    let output = index
        .args([
            "index-shards",
            "--discover-root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "2",
            "--family-limit",
            "1",
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(result["shards"], 1);
    assert_eq!(result["discovery"][0]["candidates_found"], 2);
    assert_eq!(result["discovery"][0]["selected_repos"], 1);
    assert_eq!(result["discovery"][0]["family_limit"], 1);
    assert_eq!(result["discovery"][0]["top_families"][0]["checkouts"], 2);
}

#[test]
fn cli_ensures_shards_builds_then_refreshes_existing_directory() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("workspace/auth/src/lib.rs"),
        "pub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &root.path().join("workspace/auth/Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "ensure-shards",
            "--discover-root",
            root.path().to_str().unwrap(),
            "--max-depth",
            "2",
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action\":\"build\""))
        .stdout(predicate::str::contains("\"shards\":1"));

    write(
        &root.path().join("workspace/auth/src/refresh.rs"),
        "pub fn refresh_token() -> &'static str { \"token\" }\n",
    );
    let mut refresh = Command::cargo_bin("orient").unwrap();
    refresh
        .args([
            "ensure-shards",
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action\":\"refresh\""))
        .stdout(predicate::str::contains("\"shards\":1"));

    write(
        &root.path().join("workspace/billing/src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &root.path().join("workspace/billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let mut add = Command::cargo_bin("orient").unwrap();
    add.args([
        "ensure-shards",
        "--discover-root",
        root.path().to_str().unwrap(),
        "--max-depth",
        "2",
        "--output-dir",
        shard_dir.path().to_str().unwrap(),
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains("\"action\":\"refresh+add\""))
    .stdout(predicate::str::contains("\"added_shards\":1"))
    .stdout(predicate::str::contains("\"shards\":2"));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice_total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing/src/lib.rs"));
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
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"entrypoints\""))
        .stdout(predicate::str::contains("\"manifest_files\""))
        .stdout(predicate::str::contains("\"command_hints\""))
        .stdout(predicate::str::contains("\"dependency_hints\""))
        .stdout(predicate::str::contains("\"import_hints\""))
        .stdout(predicate::str::contains("\"command\":\"cargo test\""))
        .stdout(predicate::str::contains("\"source\":\"Cargo.toml\""))
        .stdout(predicate::str::contains(
            "\"module\":\"sample::SessionManager\"",
        ))
        .stdout(predicate::str::contains("\"related_files\""))
        .stdout(predicate::str::contains("\"related_symbols\""))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains(
            "\"cli\":\"orient read-ranges --repo",
        ))
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("SessionManager"));

    let mut related = Command::cargo_bin("orient").unwrap();
    related
        .args([
            "related",
            "--repo",
            repo.path().to_str().unwrap(),
            "--path",
            "src/auth.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));

    let mut read_range = Command::cargo_bin("orient").unwrap();
    read_range
        .args([
            "open-range",
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

    let mut compact_read_range = Command::cargo_bin("orient").unwrap();
    compact_read_range
        .args([
            "read-range",
            "--repo",
            repo.path().to_str().unwrap(),
            "src/auth.rs:3:3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"start_line\":3"))
        .stdout(predicate::str::contains("issue_token"));

    let mut oversized_compact_read_range = Command::cargo_bin("orient").unwrap();
    oversized_compact_read_range
        .args([
            "read-range",
            "--repo",
            repo.path().to_str().unwrap(),
            "src/auth.rs:3:1001",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("range lines has 1001, max 1000"));

    let mut named_read_range = Command::cargo_bin("orient").unwrap();
    named_read_range
        .args([
            "read-range",
            "--repo",
            repo.path().to_str().unwrap(),
            "--path",
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

    let mut read_ranges = Command::cargo_bin("orient").unwrap();
    read_ranges
        .args([
            "read-ranges",
            "--repo",
            repo.path().to_str().unwrap(),
            "src/auth.rs",
            "tests/auth_test.rs",
            "--start",
            "1",
            "--lines",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("\"path\":\"tests/auth_test.rs\""));

    let mut read_precise_ranges = Command::cargo_bin("orient").unwrap();
    read_precise_ranges
        .args([
            "read-ranges",
            "--repo",
            repo.path().to_str().unwrap(),
            "--range",
            "src/auth.rs:5:1",
            "tests/auth_test.rs:3:1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"start_line\":5"))
        .stdout(predicate::str::contains("issue_token"))
        .stdout(predicate::str::contains("\"start_line\":3"))
        .stdout(predicate::str::contains("issues_tokens"));
}

#[test]
fn cli_repo_map_detail_controls_import_payload_size() {
    let repo = tempfile::tempdir().unwrap();
    let bulk_imports = (0..40)
        .map(|index| format!("use alpha::Module{index};\n"))
        .collect::<String>();
    write(&repo.path().join("src/bulk.rs"), &bulk_imports);
    write(
        &repo.path().join("src/other.rs"),
        "use beta::Client;\nuse gamma::Config;\npub fn call() {}\n",
    );
    write(&repo.path().join("README.md"), "# sample\n");
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\n",
    );
    write(
        &repo.path().join("tests/other_test.rs"),
        "#[test]\nfn it_works() {}\n",
    );

    let compact = Command::cargo_bin("orient")
        .unwrap()
        .args([
            "repo-map",
            "--repo",
            repo.path().to_str().unwrap(),
            "--symbols",
            "1",
            "--tests",
            "1",
        ])
        .output()
        .unwrap();
    assert!(compact.status.success());
    let compact: serde_json::Value = serde_json::from_slice(&compact.stdout).unwrap();
    assert_eq!(
        compact["brief"]["import_hints"].as_array().unwrap().len(),
        32
    );

    let full = Command::cargo_bin("orient")
        .unwrap()
        .args([
            "repo-map",
            "--repo",
            repo.path().to_str().unwrap(),
            "--symbols",
            "1",
            "--tests",
            "1",
            "--detail",
            "full",
        ])
        .output()
        .unwrap();
    assert!(full.status.success());
    let full: serde_json::Value = serde_json::from_slice(&full.stdout).unwrap();
    assert_eq!(full["brief"]["import_hints"].as_array().unwrap().len(), 42);

    let narrow_reads = Command::cargo_bin("orient")
        .unwrap()
        .args([
            "repo-map",
            "--repo",
            repo.path().to_str().unwrap(),
            "--symbols",
            "5",
            "--tests",
            "5",
            "--read-limit",
            "2",
        ])
        .output()
        .unwrap();
    assert!(narrow_reads.status.success());
    let narrow_reads: serde_json::Value = serde_json::from_slice(&narrow_reads.stdout).unwrap();
    assert_eq!(
        narrow_reads["read_batch_request"]["arguments"]["ranges"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
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
            "--context-lines",
            "6",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("\"line_range\""))
        .stdout(predicate::str::contains("\"read_range\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains("\"related_request\""))
        .stdout(predicate::str::contains("\"tool\":\"related_files\""))
        .stdout(predicate::str::contains("\"cli\":\"orient related --repo"))
        .stdout(predicate::str::contains("\"related_symbols_request\""))
        .stdout(predicate::str::contains("\"tool\":\"related_symbols\""))
        .stdout(predicate::str::contains(
            "\"cli\":\"orient related-symbols --repo",
        ))
        .stdout(predicate::str::contains("\"query\":\"issue token\""))
        .stdout(predicate::str::contains("\"lines\":80"))
        .stdout(predicate::str::contains("\"explanation\""))
        .stdout(predicate::str::contains("\"context\""))
        .stdout(predicate::str::contains("\"total_lines\""));

    let mut alias_search = Command::cargo_bin("orient").unwrap();
    alias_search
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "issue token",
            "--folder",
            "src",
            "--filename",
            "auth.rs",
            "--lang",
            "rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"));

    let mut negative_query_search = Command::cargo_bin("orient").unwrap();
    negative_query_search
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "-lang:md issue token",
            "--limit",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"));

    let mut any_terms = Command::cargo_bin("orient").unwrap();
    any_terms
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "SessionManager nonexistent",
            "--any-terms",
            "--limit",
            "1",
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
        .stdout(predicate::str::contains("same file"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));
}

#[test]
fn cli_search_surfaces_accept_structured_filters() {
    let repo = sample_repo();
    write(
        &repo.path().join("src/generated.rs"),
        "pub struct GeneratedSession;\npub fn issue_token() -> &'static str { \"generated\" }\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_test() {}\n",
    );
    write(&repo.path().join("docs/auth.md"), "issue token docs\n");

    let mut fallback = Command::cargo_bin("orient").unwrap();
    fallback
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "issue token",
            "--file",
            "auth.rs",
            "--dir",
            "src",
            "--lang",
            "rust",
            "--ext",
            "rs",
            "--symbol",
            "SessionManager",
            "--type",
            "functions",
            "--test",
            "false",
            "--exclude-file",
            "generated",
            "--exclude-path",
            "tests",
            "--exclude-lang",
            "markdown",
            "--exclude-ext",
            ".md",
            "--exclude-symbol",
            "GeneratedSession",
            "--exclude-type",
            "enum",
            "--exclude-repo",
            "other-repo",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("generated").not())
        .stdout(predicate::str::contains("tests/auth_test.rs").not())
        .stdout(predicate::str::contains("docs/auth.md").not());

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
        .stdout(predicate::str::contains("\"source_bytes\""))
        .stdout(predicate::str::contains("\"posting_entries\""))
        .stdout(predicate::str::contains("\"compressed_posting_bytes\""));

    let mut indexed = Command::cargo_bin("orient").unwrap();
    indexed
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "issue token",
            "--file",
            "auth.rs",
            "--dir",
            "src",
            "--lang",
            "rust",
            "--ext",
            "rs",
            "--symbol",
            "SessionManager",
            "--type",
            "functions",
            "--test",
            "false",
            "--exclude-file",
            "generated",
            "--exclude-path",
            "tests",
            "--exclude-lang",
            "markdown",
            "--exclude-ext",
            ".md",
            "--exclude-symbol",
            "GeneratedSession",
            "--exclude-type",
            "enum",
            "--exclude-repo",
            "other-repo",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("generated").not())
        .stdout(predicate::str::contains("tests/auth_test.rs").not())
        .stdout(predicate::str::contains("docs/auth.md").not());

    let mut fallback_filter_only = Command::cargo_bin("orient").unwrap();
    fallback_filter_only
        .args([
            "search",
            "--repo",
            repo.path().to_str().unwrap(),
            "file:auth.rs",
            "--explain",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("file_filter"));

    let mut indexed_filter_only = Command::cargo_bin("orient").unwrap();
    indexed_filter_only
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "lang:rust",
            "--test",
            "true",
            "--explain",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("filter_scan"));

    let mut search_plan = Command::cargo_bin("orient").unwrap();
    search_plan
        .args([
            "search-plan",
            "--repo",
            repo.path().to_str().unwrap(),
            "SessionManager definitely_missing",
            "--dir",
            "src",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"active_filters\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("definitely"))
        .stdout(predicate::str::contains("missing"))
        .stdout(predicate::str::contains("drop_missing_terms"));

    let mut search_plan_batch = Command::cargo_bin("orient").unwrap();
    search_plan_batch
        .args([
            "search-plan-batch",
            "--repo",
            repo.path().to_str().unwrap(),
            "SessionManager definitely_missing",
            "issue absentterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager definitely_missing\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"issue absentterm\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("absentterm"))
        .stdout(predicate::str::contains("drop_missing_terms"));

    let mut index_plan = Command::cargo_bin("orient").unwrap();
    index_plan
        .args([
            "index-plan",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager definitely_missing",
            "--dir",
            "src",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"active_filters\""))
        .stdout(predicate::str::contains("\"field\":\"path\""))
        .stdout(predicate::str::contains("\"candidate_rejections\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("definitely"))
        .stdout(predicate::str::contains("missing"))
        .stdout(predicate::str::contains("\"candidate_count\":0"))
        .stdout(predicate::str::contains("\"filtered_candidate_count\":0"))
        .stdout(predicate::str::contains("\"scored_candidate_count\":0"))
        .stdout(predicate::str::contains("\"final_match_count\":0"))
        .stdout(predicate::str::contains("\"repair_hints\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"retry_requests\""))
        .stdout(predicate::str::contains("\"tool\":\"indexed_search_code\""))
        .stdout(predicate::str::contains("\"query\":\"session manager\""))
        .stdout(predicate::str::contains("\"path\":\"src\""));

    let mut generic_index_plan = Command::cargo_bin("orient").unwrap();
    generic_index_plan
        .args([
            "search-plan",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager definitely_missing",
            "--dir",
            "src",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"active_filters\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"retry_requests\""))
        .stdout(predicate::str::contains("\"tool\":\"search\""))
        .stdout(predicate::str::contains(&format!(
            "\"index\":\"{}\"",
            index_path.display()
        )))
        .stdout(predicate::str::contains("\"query\":\"session manager\""));

    let mut index_plan_batch = Command::cargo_bin("orient").unwrap();
    index_plan_batch
        .args([
            "index-plan-batch",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager definitely_missing",
            "issue absentterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager definitely_missing\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"issue absentterm\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("absentterm"))
        .stdout(predicate::str::contains("drop_missing_terms"));

    let mut generic_index_plan_batch = Command::cargo_bin("orient").unwrap();
    generic_index_plan_batch
        .args([
            "search-plan-batch",
            "--index",
            index_path.to_str().unwrap(),
            "--require-all",
            "SessionManager definitely_missing",
            "issue absentterm",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager definitely_missing\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"issue absentterm\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"tool\":\"search\""))
        .stdout(predicate::str::contains("\"index\""));

    let shard_dir = tempfile::tempdir().unwrap();
    let mut build_shards = Command::cargo_bin("orient").unwrap();
    build_shards
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut shard_search = Command::cargo_bin("orient").unwrap();
    shard_search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "issue token",
            "--file",
            "auth.rs",
            "--dir",
            "src",
            "--symbol",
            "SessionManager",
            "--test",
            "false",
            "--exclude-file",
            "generated",
            "--exclude-path",
            "tests",
            "--exclude-language",
            "markdown",
            "--exclude-extension",
            ".md",
            "--exclude-symbol",
            "GeneratedSession",
            "--exclude-repo",
            "other-repo",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("generated").not())
        .stdout(predicate::str::contains("tests/auth_test.rs").not())
        .stdout(predicate::str::contains("docs/auth.md").not());

    let mut shard_filter_only = Command::cargo_bin("orient").unwrap();
    shard_filter_only
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "file:auth.rs",
            "--explain",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("file_filter"));

    let mut bench = Command::cargo_bin("orient").unwrap();
    bench
        .args([
            "bench-search",
            "--repo",
            repo.path().to_str().unwrap(),
            "--index",
            index_path.to_str().unwrap(),
            "--runs",
            "1",
            "--warmup",
            "0",
            "--file",
            "auth.rs",
            "--dir",
            "src",
            "--symbol",
            "SessionManager",
            "--test",
            "false",
            "--exclude-file",
            "generated",
            "issue token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"result_count\":1"));
}

#[test]
fn cli_batches_searches_across_fallback_indexed_and_shards() {
    let repo = sample_repo();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );

    let mut fallback = Command::cargo_bin("orient").unwrap();
    fallback
        .args([
            "search-batch",
            "--repo",
            repo.path().to_str().unwrap(),
            "SessionManager",
            "invoice total",
            "--limit",
            "2",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query\":\"SessionManager\""))
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("\"query\":\"invoice total\""))
        .stdout(predicate::str::contains("src/billing.rs"));

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
            "indexed-search-batch",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager",
            "invoice total",
            "--limit",
            "2",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("src/billing.rs"));

    let mut generic_indexed = Command::cargo_bin("orient").unwrap();
    generic_indexed
        .args([
            "search-batch",
            "--index",
            index_path.to_str().unwrap(),
            "--limit",
            "2",
            "--require-all",
            "SessionManager",
            "invoice total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query\":\"SessionManager\""))
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("\"query\":\"invoice total\""))
        .stdout(predicate::str::contains("src/billing.rs"))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains(&format!(
            "\"index\":\"{}\"",
            index_path.display()
        )));

    let shard_dir = tempfile::tempdir().unwrap();
    let mut build_shards = Command::cargo_bin("orient").unwrap();
    build_shards
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut shards = Command::cargo_bin("orient").unwrap();
    shards
        .args([
            "search-shards-batch",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "SessionManager",
            "invoice total",
            "--limit",
            "2",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("src/billing.rs"));

    let mut generic_shards = Command::cargo_bin("orient").unwrap();
    generic_shards
        .args([
            "search-batch",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--limit",
            "2",
            "--require-all",
            "SessionManager",
            "invoice total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"query\":\"SessionManager\""))
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("\"query\":\"invoice total\""))
        .stdout(predicate::str::contains("src/billing.rs"))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains("\"index_dir\""));

    let mut index_plan_batch = Command::cargo_bin("orient").unwrap();
    index_plan_batch
        .args([
            "index-plan-batch",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager missingterm",
            "invoice absentterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager missingterm\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"invoice absentterm\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("drop_missing_terms"));

    let mut shard_plan_batch = Command::cargo_bin("orient").unwrap();
    shard_plan_batch
        .args([
            "shard-plan-batch",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "SessionManager missingterm",
            "invoice absentterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager missingterm\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"invoice absentterm\""))
        .stdout(predicate::str::contains("\"plans\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("drop_missing_terms"));

    let mut generic_shard_plan_batch = Command::cargo_bin("orient").unwrap();
    generic_shard_plan_batch
        .args([
            "search-plan-batch",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--require-all",
            "SessionManager missingterm",
            "invoice absentterm",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"query\":\"SessionManager missingterm\"",
        ))
        .stdout(predicate::str::contains("\"query\":\"invoice absentterm\""))
        .stdout(predicate::str::contains("\"plans\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"tool\":\"search\""))
        .stdout(predicate::str::contains("\"index_dir\""));
}

#[test]
fn cli_reports_index_and_shard_freshness() {
    let repo = sample_repo();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );
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

    let mut clean = Command::cargo_bin("orient").unwrap();
    clean
        .args(["index-status", "--index", index_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"stale\":false"))
        .stdout(predicate::str::contains("\"source_bytes\""))
        .stdout(predicate::str::contains("\"posting_entries\""))
        .stdout(predicate::str::contains("\"compressed_posting_bytes\""));

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret_now() {}\n",
    );
    fs::remove_file(repo.path().join("src/billing.rs")).unwrap();
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session() {}\n",
    );

    let mut stale = Command::cargo_bin("orient").unwrap();
    stale
        .args(["index-status", "--index", index_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"stale\":true"))
        .stdout(predicate::str::contains(
            "\"changed_paths\":[\"src/auth.rs\"]",
        ))
        .stdout(predicate::str::contains(
            "\"deleted_paths\":[\"src/billing.rs\"]",
        ))
        .stdout(predicate::str::contains(
            "\"added_paths\":[\"src/new_session.rs\"]",
        ));

    let mut stale_index_search = Command::cargo_bin("orient").unwrap();
    stale_index_search
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "new session",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^\\[\\]\\n?$").unwrap());

    let mut refreshed_index_search = Command::cargo_bin("orient").unwrap();
    refreshed_index_search
        .args([
            "indexed-search",
            "--index",
            index_path.to_str().unwrap(),
            "new session",
            "--require-all",
            "--refresh-if-stale",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/new_session.rs"));

    let shard_dir = tempfile::tempdir().unwrap();
    let mut build_shards = Command::cargo_bin("orient").unwrap();
    build_shards
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();
    write(
        &repo.path().join("src/after_shard.rs"),
        "pub fn after_shard() {}\n",
    );

    let mut shard_status = Command::cargo_bin("orient").unwrap();
    shard_status
        .args([
            "shard-status",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"stale\":true"))
        .stdout(predicate::str::contains("\"stale_shards\":1"))
        .stdout(predicate::str::contains("\"source_bytes\""))
        .stdout(predicate::str::contains("\"posting_entries\""))
        .stdout(predicate::str::contains("\"compressed_posting_bytes\""))
        .stdout(predicate::str::contains("src/after_shard.rs"));

    let mut stale_shard_search = Command::cargo_bin("orient").unwrap();
    stale_shard_search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "after shard",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^\\[\\]\\n?$").unwrap());

    let mut refreshed_shard_search = Command::cargo_bin("orient").unwrap();
    refreshed_shard_search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "after shard",
            "--require-all",
            "--refresh-if-stale",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/after_shard.rs"));
}

#[test]
fn cli_builds_and_searches_persistent_index() {
    let repo = sample_repo();
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
    );
    let index_path = repo.path().join(".orient/index");

    let mut ensure_index = Command::cargo_bin("orient").unwrap();
    ensure_index
        .args([
            "ensure-index",
            "--repo",
            repo.path().to_str().unwrap(),
            "--index",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"refreshed_files\""))
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
            "--explain",
            "--context-lines",
            "4",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/auth.rs"))
        .stdout(predicate::str::contains("indexed match"))
        .stdout(predicate::str::contains("\"match_lines\""))
        .stdout(predicate::str::contains("\"read_range\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""))
        .stdout(predicate::str::contains("\"related_request\""))
        .stdout(predicate::str::contains("\"tool\":\"related_index_files\""))
        .stdout(predicate::str::contains("\"related_symbols_request\""))
        .stdout(predicate::str::contains(
            "\"tool\":\"related_index_symbols\"",
        ))
        .stdout(predicate::str::contains("\"query_plan\""))
        .stdout(predicate::str::contains("\"planned_postings\""))
        .stdout(predicate::str::contains("\"context\""));

    fs::remove_file(repo.path().join("src/auth.rs")).unwrap();

    let mut read_index_range = Command::cargo_bin("orient").unwrap();
    read_index_range
        .args([
            "read-index-range",
            "--index",
            index_path.to_str().unwrap(),
            "./src/auth.rs:3:3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("issue_token"));

    let mut generic_read_index_range = Command::cargo_bin("orient").unwrap();
    generic_read_index_range
        .args([
            "read-range",
            "--index",
            index_path.to_str().unwrap(),
            "--path",
            "./src/auth.rs",
            "--start",
            "3",
            "--lines",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("issue_token"));

    let mut read_index_ranges = Command::cargo_bin("orient").unwrap();
    read_index_ranges
        .args([
            "read-index-ranges",
            "--index",
            index_path.to_str().unwrap(),
            "--range",
            "src/auth.rs:5:1",
            "--range",
            "tests/auth_test.rs:3:1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("issue_token"))
        .stdout(predicate::str::contains("\"path\":\"tests/auth_test.rs\""))
        .stdout(predicate::str::contains("issue_token_round_trip"));

    let mut generic_read_index_ranges = Command::cargo_bin("orient").unwrap();
    generic_read_index_ranges
        .args([
            "read-ranges",
            "--index",
            index_path.to_str().unwrap(),
            "--range",
            "src/auth.rs:5:1",
            "--range",
            "tests/auth_test.rs:3:1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("issue_token"))
        .stdout(predicate::str::contains("\"path\":\"tests/auth_test.rs\""))
        .stdout(predicate::str::contains("issue_token_round_trip"));

    let mut index_symbol = Command::cargo_bin("orient").unwrap();
    index_symbol
        .args([
            "index-symbol",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("\"kind\":\"struct\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""));

    let mut generic_index_symbol = Command::cargo_bin("orient").unwrap();
    generic_index_symbol
        .args([
            "symbol",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"src/auth.rs\""))
        .stdout(predicate::str::contains("\"kind\":\"struct\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains(&format!(
            "\"index\":\"{}\"",
            index_path.display()
        )));

    let mut index_symbol_batch = Command::cargo_bin("orient").unwrap();
    index_symbol_batch
        .args([
            "index-symbol-batch",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager",
            "issue_token",
            "--kind",
            "function",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"SessionManager\""))
        .stdout(predicate::str::contains("\"symbols\":[]"))
        .stdout(predicate::str::contains("\"name\":\"issue_token\""))
        .stdout(predicate::str::contains("\"kind\":\"function\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_ranges\""));

    let mut generic_index_symbol_batch = Command::cargo_bin("orient").unwrap();
    generic_index_symbol_batch
        .args([
            "symbol-batch",
            "--index",
            index_path.to_str().unwrap(),
            "SessionManager",
            "issue_token",
            "--kind",
            "function",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"SessionManager\""))
        .stdout(predicate::str::contains("\"symbols\":[]"))
        .stdout(predicate::str::contains("\"name\":\"issue_token\""))
        .stdout(predicate::str::contains("\"kind\":\"function\""))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""));

    let mut index_map = Command::cargo_bin("orient").unwrap();
    index_map
        .args([
            "index-map",
            "--index",
            index_path.to_str().unwrap(),
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"entrypoints\""))
        .stdout(predicate::str::contains("\"manifest_files\""))
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("cargo test"))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_ranges\""));

    let mut generic_index_map = Command::cargo_bin("orient").unwrap();
    generic_index_map
        .args([
            "repo-map",
            "--index",
            index_path.to_str().unwrap(),
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"entrypoints\""))
        .stdout(predicate::str::contains("\"manifest_files\""))
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("cargo test"))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains(&format!(
            "\"index\":\"{}\"",
            index_path.display()
        )));

    let mut related_index = Command::cargo_bin("orient").unwrap();
    related_index
        .args([
            "related-index",
            "--index",
            index_path.to_str().unwrap(),
            "--path",
            "src/auth.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_index_range\""));

    let mut generic_related_index = Command::cargo_bin("orient").unwrap();
    generic_related_index
        .args([
            "related",
            "--index",
            index_path.to_str().unwrap(),
            "--path",
            "src/auth.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tests/auth_test.rs"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""))
        .stdout(predicate::str::contains(&format!(
            "\"index\":\"{}\"",
            index_path.display()
        )));

    let mut related_index_symbols = Command::cargo_bin("orient").unwrap();
    related_index_symbols
        .args([
            "related-index-symbols",
            "--index",
            index_path.to_str().unwrap(),
            "--path",
            "src/auth.rs",
            "--query",
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("same file"));

    let mut generic_related_index_symbols = Command::cargo_bin("orient").unwrap();
    generic_related_index_symbols
        .args([
            "related-symbols",
            "--index",
            index_path.to_str().unwrap(),
            "--path",
            "src/auth.rs",
            "--query",
            "SessionManager",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SessionManager"))
        .stdout(predicate::str::contains("same file"))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));

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
    write(
        &auth_repo.path().join("src/shared.rs"),
        "pub fn shared_name() -> &'static str { \"auth\" }\n",
    );
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
        &billing_repo.path().join("src/shared.rs"),
        "pub fn shared_name() -> &'static str { \"billing\" }\n",
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
            &billing_name.to_ascii_uppercase(),
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing.rs"));

    let mut shard_symbol = Command::cargo_bin("orient").unwrap();
    shard_symbol
        .args([
            "shard-symbol",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice total",
            "--repo",
            &billing_name.to_ascii_uppercase(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )))
        .stdout(predicate::str::contains("\"name\":\"invoice_total\""));

    let mut generic_shard_symbol = Command::cargo_bin("orient").unwrap();
    generic_shard_symbol
        .args([
            "symbol",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice total",
            "--repo-filter",
            &billing_name.to_ascii_uppercase(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )))
        .stdout(predicate::str::contains("\"name\":\"invoice_total\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));

    let mut generic_shard_symbol_batch = Command::cargo_bin("orient").unwrap();
    generic_shard_symbol_batch
        .args([
            "symbol-batch",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "invoice total",
            "--repo-filter",
            &billing_name.to_ascii_uppercase(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"invoice total\""))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )));

    let mut shard_map = Command::cargo_bin("orient").unwrap();
    shard_map
        .args([
            "shard-map",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo",
            &billing_name.to_ascii_uppercase(),
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"name\":\"{billing_name}\""
        )))
        .stdout(predicate::str::contains(&format!(
            "\"entrypoints\":[\"{billing_name}/Cargo.toml\"]"
        )))
        .stdout(predicate::str::contains(&format!(
            "\"manifest_files\":[\"{billing_name}/Cargo.toml\"]"
        )))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_ranges\""))
        .stdout(predicate::str::contains("\"command_hints\""))
        .stdout(predicate::str::contains(&format!(
            "\"source\":\"{billing_name}/Cargo.toml\""
        )))
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )));

    let mut generic_shard_map = Command::cargo_bin("orient").unwrap();
    generic_shard_map
        .args([
            "repo-map",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo-filter",
            &billing_name.to_ascii_uppercase(),
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"name\":\"{billing_name}\""
        )))
        .stdout(predicate::str::contains(&format!(
            "\"entrypoints\":[\"{billing_name}/Cargo.toml\"]"
        )))
        .stdout(predicate::str::contains("\"read_batch_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains("\"index_dir\""))
        .stdout(predicate::str::contains("\"command_hints\""))
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )));

    let mut read = Command::cargo_bin("orient").unwrap();
    read.args([
        "read-shard-range",
        "--index-dir",
        shard_dir.path().to_str().unwrap(),
        &format!("{billing_name}/./src/billing.rs:1:1"),
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains(&format!(
        "\"path\":\"{billing_name}/src/billing.rs\""
    )))
    .stdout(predicate::str::contains("invoice_total"));

    let mut generic_read = Command::cargo_bin("orient").unwrap();
    generic_read
        .args([
            "read-range",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            &format!("{billing_name}/./src/billing.rs"),
            "--start",
            "1",
            "--lines",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )))
        .stdout(predicate::str::contains("invoice_total"));

    let mut read_unqualified = Command::cargo_bin("orient").unwrap();
    read_unqualified
        .args([
            "read-shard-range",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "src/billing.rs",
            "--start",
            "1",
            "--lines",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )))
        .stdout(predicate::str::contains("invoice_total"));

    let mut generic_read_ranges = Command::cargo_bin("orient").unwrap();
    generic_read_ranges
        .args([
            "read-ranges",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--range",
            &format!("{billing_name}/src/billing.rs:1:1"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "\"path\":\"{billing_name}/src/billing.rs\""
        )))
        .stdout(predicate::str::contains("invoice_total"));

    let mut related_unqualified = Command::cargo_bin("orient").unwrap();
    related_unqualified
        .args([
            "related-shard",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "src/billing.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "{billing_name}/src/legacy.rs"
        )))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_range\""));

    let mut generic_related_unqualified = Command::cargo_bin("orient").unwrap();
    generic_related_unqualified
        .args([
            "related",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "src/billing.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(&format!(
            "{billing_name}/src/legacy.rs"
        )))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));

    let mut ambiguous_read = Command::cargo_bin("orient").unwrap();
    ambiguous_read
        .args([
            "read-shard-range",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "src/shared.rs",
            "--start",
            "1",
            "--lines",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ambiguous shard path"))
        .stderr(predicate::str::contains("use '<repo>/<path>'"));

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
fn cli_shard_manifest_records_git_metadata() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/lib.rs"),
        "pub fn unique_branch_token() -> &'static str { \"needle\" }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='shard-project'\nversion='0.1.0'\nedition='2024'\n",
    );
    git(repo.path(), &["init", "-b", "shard-feature-branch"]);
    git(
        repo.path(),
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/evalops/shard-project.git",
        ],
    );
    git(repo.path(), &["add", "."]);
    git(
        repo.path(),
        &[
            "-c",
            "user.name=Orient Tests",
            "-c",
            "user.email=orient@example.com",
            "commit",
            "-m",
            "init",
        ],
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let manifest = fs::read_to_string(shard_dir.path().join("manifest.json")).unwrap();
    assert!(manifest.contains("\"branch\": \"shard-feature-branch\""));
    assert!(manifest.contains("https://github.com/evalops/shard-project.git"));
    assert!(manifest.contains("\"git_kind\": \"clone\""));

    let mut search_by_branch = Command::cargo_bin("orient").unwrap();
    search_by_branch
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "unique branch token",
            "--repo",
            "shard-feature-branch",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/lib.rs"));

    let mut search_by_explicit_branch = Command::cargo_bin("orient").unwrap();
    search_by_explicit_branch
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "unique branch token",
            "--branch",
            "shard-feature-branch",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/lib.rs"));

    let mut search_by_explicit_origin = Command::cargo_bin("orient").unwrap();
    search_by_explicit_origin
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "unique branch token",
            "--origin",
            "evalops/shard-project",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/lib.rs"));

    let mut exclude_branch = Command::cargo_bin("orient").unwrap();
    exclude_branch
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "unique branch token",
            "--exclude-branch",
            "shard-feature-branch",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::eq("[]\n"));

    let mut map_by_origin = Command::cargo_bin("orient").unwrap();
    map_by_origin
        .args([
            "shard-map",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--origin",
            "evalops/shard-project",
            "--symbols",
            "5",
            "--tests",
            "5",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"git\""))
        .stdout(predicate::str::contains(
            "\"branch\":\"shard-feature-branch\"",
        ))
        .stdout(predicate::str::contains(
            "https://github.com/evalops/shard-project.git",
        ));
}

#[test]
fn cli_filters_shard_search_by_nested_repo_alias() {
    let workspace = tempfile::tempdir().unwrap();
    let billing_repo = workspace.path().join("billing");
    let auth_repo = workspace.path().join("auth");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "index-shards",
            "--repo",
            workspace.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"shards\":1"));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "repo:billing invoice total",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"path\":\"billing/src/billing.rs\"",
        ))
        .stdout(predicate::str::contains(
            "\"read_range\":{\"path\":\"billing/src/billing.rs\"",
        ))
        .stdout(predicate::str::contains("invoice_total"))
        .stdout(predicate::str::contains("auth.rs").not());

    let mut shard_plan = Command::cargo_bin("orient").unwrap();
    shard_plan
        .args([
            "shard-plan",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "repo:billing invoice missingterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"billing\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("missingterm"))
        .stdout(predicate::str::contains("\"candidate_count\":0"))
        .stdout(predicate::str::contains("\"filtered_candidate_count\":0"))
        .stdout(predicate::str::contains("\"final_match_count\":0"))
        .stdout(predicate::str::contains("\"repair_hints\""))
        .stdout(predicate::str::contains("drop_missing_terms"))
        .stdout(predicate::str::contains("\"retry_requests\""))
        .stdout(predicate::str::contains("\"tool\":\"search_shards\""))
        .stdout(predicate::str::contains("\"query\":\"invoice\""))
        .stdout(predicate::str::contains("\"path\":\"billing\""))
        .stdout(predicate::str::contains("\"name\":\"auth\"").not());

    let mut generic_shard_plan = Command::cargo_bin("orient").unwrap();
    generic_shard_plan
        .args([
            "search-plan",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "repo:billing invoice missingterm",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"billing\""))
        .stdout(predicate::str::contains("\"missing_terms\""))
        .stdout(predicate::str::contains("missingterm"))
        .stdout(predicate::str::contains("\"retry_requests\""))
        .stdout(predicate::str::contains("\"tool\":\"search\""))
        .stdout(predicate::str::contains("\"index_dir\""))
        .stdout(predicate::str::contains("\"query\":\"invoice\""))
        .stdout(predicate::str::contains("\"name\":\"auth\"").not());

    let mut shard_selection_plan = Command::cargo_bin("orient").unwrap();
    shard_selection_plan
        .args([
            "shard-plan",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "repo:billing branch:missing-branch invoice total",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"__shard_selection__\""))
        .stdout(predicate::str::contains(
            "\"strategy\":\"shard_filter_mismatch\"",
        ))
        .stdout(predicate::str::contains("\"candidate_count\":1"))
        .stdout(predicate::str::contains("\"field\":\"repo\""))
        .stdout(predicate::str::contains("\"field\":\"branch\""))
        .stdout(predicate::str::contains("\"kind\":\"relax_filters\""))
        .stdout(predicate::str::contains("\"tool\":\"search_shards\""))
        .stdout(predicate::str::contains("\"query\":\"invoice total\""));

    let mut shard_symbol = Command::cargo_bin("orient").unwrap();
    shard_symbol
        .args([
            "shard-symbol",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo",
            "billing",
            "invoice_total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"path\":\"billing/src/billing.rs\"",
        ));

    let mut shard_map = Command::cargo_bin("orient").unwrap();
    shard_map
        .args([
            "shard-map",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo",
            "billing",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"aliases\""))
        .stdout(predicate::str::contains("billing"))
        .stdout(predicate::str::contains("cargo test"))
        .stdout(predicate::str::contains("billing/Cargo.toml"))
        .stdout(predicate::str::contains("auth/src/auth.rs").not());

    let mut generic_shard_map = Command::cargo_bin("orient").unwrap();
    generic_shard_map
        .args([
            "repo-map",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo-filter",
            "billing",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"aliases\""))
        .stdout(predicate::str::contains("billing"))
        .stdout(predicate::str::contains("cargo test"))
        .stdout(predicate::str::contains("billing/Cargo.toml"))
        .stdout(predicate::str::contains("\"tool\":\"read_ranges\""))
        .stdout(predicate::str::contains("auth/src/auth.rs").not());

    let mut read = Command::cargo_bin("orient").unwrap();
    read.args([
        "read-shard-range",
        "--index-dir",
        shard_dir.path().to_str().unwrap(),
        "billing/src/billing.rs",
        "--start",
        "1",
        "--lines",
        "1",
    ])
    .assert()
    .success()
    .stdout(predicate::str::contains(
        "\"path\":\"billing/src/billing.rs\"",
    ))
    .stdout(predicate::str::contains("invoice_total"));

    let mut read_many = Command::cargo_bin("orient").unwrap();
    read_many
        .args([
            "read-shard-ranges",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--range",
            "billing/src/billing.rs:1:1",
            "--range",
            "billing/tests/billing_test.rs:3:1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"path\":\"billing/src/billing.rs\"",
        ))
        .stdout(predicate::str::contains("invoice_total"))
        .stdout(predicate::str::contains(
            "\"path\":\"billing/tests/billing_test.rs\"",
        ))
        .stdout(predicate::str::contains("totals_invoice"));

    let mut related = Command::cargo_bin("orient").unwrap();
    related
        .args([
            "related-shard",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "billing/src/billing.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing/tests/billing_test.rs"))
        .stdout(predicate::str::contains("auth/src/auth.rs").not())
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_range\""));

    let mut generic_related = Command::cargo_bin("orient").unwrap();
    generic_related
        .args([
            "related",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "billing/src/billing.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("billing/tests/billing_test.rs"))
        .stdout(predicate::str::contains("auth/src/auth.rs").not())
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));

    let mut related_symbols = Command::cargo_bin("orient").unwrap();
    related_symbols
        .args([
            "related-shard-symbols",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "billing/src/billing.rs",
            "--query",
            "invoice total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"path\":\"billing/src/billing.rs\"",
        ))
        .stdout(predicate::str::contains("invoice_total"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_shard_range\""));

    let mut generic_related_symbols = Command::cargo_bin("orient").unwrap();
    generic_related_symbols
        .args([
            "related-symbols",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--path",
            "billing/src/billing.rs",
            "--query",
            "invoice total",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"path\":\"billing/src/billing.rs\"",
        ))
        .stdout(predicate::str::contains("invoice_total"))
        .stdout(predicate::str::contains("\"read_request\""))
        .stdout(predicate::str::contains("\"tool\":\"read_range\""));
}

#[test]
fn cli_refresh_shards_updates_nested_repo_aliases() {
    let workspace = tempfile::tempdir().unwrap();
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "index-shards",
            "--repo",
            workspace.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
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
        .stdout(predicate::str::contains("\"refreshed_files\""));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "repo:auth issue token",
            "--require-all",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"path\":\"auth/src/auth.rs\""))
        .stdout(predicate::str::contains("issue_token"))
        .stdout(predicate::str::contains("billing/src/billing.rs").not());
}

#[test]
fn cli_refresh_shards_prunes_missing_repo_roots() {
    let workspace = tempfile::tempdir().unwrap();
    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> &'static str { \"token\" }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );
    let billing_repo = workspace.path().join("billing");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &billing_repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let mut build = Command::cargo_bin("orient").unwrap();
    build
        .args([
            "index-shards",
            "--repo",
            auth_repo.to_str().unwrap(),
            "--repo",
            billing_repo.to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"shards\":2"));

    fs::remove_dir_all(&billing_repo).unwrap();

    let mut refresh = Command::cargo_bin("orient").unwrap();
    refresh
        .args([
            "refresh-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"removed_shards\":1"))
        .stdout(predicate::str::contains("\"shards\":1"));

    let mut search = Command::cargo_bin("orient").unwrap();
    search
        .args([
            "search-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "issue_token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("auth/src/auth.rs"));
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

    let shard_dir = tempfile::tempdir().unwrap();
    let mut build_shards = Command::cargo_bin("orient").unwrap();
    build_shards
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut shard_bench = Command::cargo_bin("orient").unwrap();
    shard_bench
        .args([
            "bench-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
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
        .stdout(predicate::str::contains("\"mode\":\"shards_cached\""))
        .stdout(predicate::str::contains("\"p95_ms\""));

    let mut cold_shard_bench = Command::cargo_bin("orient").unwrap();
    cold_shard_bench
        .args([
            "bench-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--cold",
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
        .stdout(predicate::str::contains("\"mode\":\"shards\""))
        .stdout(predicate::str::contains("\"p95_ms\""));

    let mut cached_shard_bench = Command::cargo_bin("orient").unwrap();
    cached_shard_bench
        .args([
            "bench-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
            "--cached",
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
        .stdout(predicate::str::contains("\"mode\":\"shards_cached\""))
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

    let shard_dir = tempfile::tempdir().unwrap();
    let mut build_shards = Command::cargo_bin("orient").unwrap();
    build_shards
        .args([
            "index-shards",
            "--repo",
            repo.path().to_str().unwrap(),
            "--output-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut shard_cmd = Command::cargo_bin("orient").unwrap();
    shard_cmd
        .args([
            "bench-shards",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
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
