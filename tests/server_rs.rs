use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Command, Stdio};

use orient::fast_index::FastIndex;
use orient::server::{ToolRequest, ToolRuntime};

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
    assert!(stdout.contains("read_index_range"));
    assert!(stdout.contains("indexed_repo_map"));
    assert!(stdout.contains("find_index_symbol"));
    assert!(stdout.contains("related_index_files"));
    assert!(stdout.contains("related_index_symbols"));
    assert!(stdout.contains("read_shard_range"));
    assert!(stdout.contains("shard_repo_map"));
    assert!(stdout.contains("find_shard_symbol"));
    assert!(stdout.contains("daemon_status"));
    assert!(stdout.contains("warm_index"));
    assert!(stdout.contains("warm_shards"));
    assert!(stdout.contains("discover_repos"));
}

#[test]
fn runtime_discovers_repos_by_tool_request() {
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
        &root
            .path()
            .join("workspace/node_modules/ignored/Cargo.toml"),
        "[package]\nname='ignored'\nversion='0.1.0'\nedition='2024'\n",
    );

    let mut runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"name\":\"auth\""), "{result}");
    assert!(result.contains("\"name\":\"billing\""), "{result}");
    assert!(!result.contains("node_modules"), "{result}");
}

#[test]
fn runtime_indexes_shards_from_discovered_root() {
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

    let mut runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "discover_root": root.path(),
            "max_depth": 2,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(2));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice_total"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/lib.rs"), "{result}");
}

#[test]
fn runtime_warms_index_by_tool_request() {
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
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let mut runtime = ToolRuntime::default();
    let warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("warm"),
        tool: "warm_index".to_string(),
        arguments: serde_json::json!({
            "index": index_path
        }),
    });
    assert!(warm.error.is_none(), "{:?}", warm.error);
    assert_eq!(warm.result.unwrap()["cached_indexes"], serde_json::json!(1));

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({}),
    });
    let result = status.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert!(
        result["cached_index_paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap().ends_with(".orient/index"))
    );
}

#[test]
fn runtime_reuses_cached_index_after_initial_load() {
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
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let mut runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/auth.rs"),
        "{:?}",
        first.result
    );

    fs::remove_file(&index_path).unwrap();
    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert!(
        serde_json::to_string(&second.result)
            .unwrap()
            .contains("src/auth.rs"),
        "{:?}",
        second.result
    );

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({}),
    });
    assert_eq!(
        status.result.unwrap()["cached_indexes"],
        serde_json::json!(1)
    );
}

#[test]
fn runtime_reuses_cached_shard_index_after_initial_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let mut runtime = ToolRuntime::default();

    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search_args = serde_json::json!({
        "index_dir": shard_dir.path(),
        "query": "invoice total",
        "limit": 3,
        "require_all": true
    });
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: search_args.clone(),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        first.result
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let shard_index = manifest["shards"][0]["index"].as_str().unwrap();
    fs::remove_file(shard_dir.path().join(shard_index)).unwrap();
    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "search_shards".to_string(),
        arguments: search_args,
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    assert!(
        serde_json::to_string(&second.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        second.result
    );
}

#[test]
fn runtime_warms_shards_by_tool_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let mut runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let warm = runtime.dispatch(ToolRequest {
        id: serde_json::json!("warm-shards"),
        tool: "warm_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(warm.error.is_none(), "{:?}", warm.error);
    let result = warm.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert_eq!(result["warmed_indexes"], serde_json::json!(1));
}

#[test]
fn runtime_filters_shard_search_by_nested_repo_alias() {
    let workspace = tempfile::tempdir().unwrap();
    let billing_repo = workspace.path().join("billing");
    let auth_repo = workspace.path().join("auth");
    write(
        &billing_repo.join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
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
    let mut runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "repo:billing invoice total",
            "limit": 5,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/billing.rs"), "{result}");
    assert!(!result.contains("auth/src/auth.rs"), "{result}");

    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: "shard_repo_map".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo": "billing",
            "symbols": 10,
            "tests": 10
        }),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    let result = serde_json::to_string(&map.result).unwrap();
    assert!(result.contains("billing/Cargo.toml"), "{result}");
    assert!(result.contains("cargo test"), "{result}");
    assert!(!result.contains("auth/src/auth.rs"), "{result}");

    let symbol = runtime.dispatch(ToolRequest {
        id: serde_json::json!("symbol"),
        tool: "find_shard_symbol".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo": "billing",
            "name": "invoice_total"
        }),
    });
    assert!(symbol.error.is_none(), "{:?}", symbol.error);
    let result = serde_json::to_string(&symbol.result).unwrap();
    assert!(
        result.contains("\"path\":\"billing/src/billing.rs\""),
        "{result}"
    );

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    let result = serde_json::to_string(&range.result).unwrap();
    assert!(
        result.contains("\"path\":\"billing/src/billing.rs\""),
        "{result}"
    );
    assert!(result.contains("invoice_total"), "{result}");
}

#[test]
fn runtime_refresh_shards_updates_nested_repo_aliases() {
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
    let mut runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [workspace.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let auth_repo = workspace.path().join("auth");
    write(
        &auth_repo.join("src/auth.rs"),
        "pub fn issue_token() -> String { \"token\".to_string() }\n",
    );
    write(
        &auth_repo.join("Cargo.toml"),
        "[package]\nname='auth'\nversion='0.1.0'\nedition='2024'\n",
    );

    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "repo:auth issue token",
            "limit": 5,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("auth/src/auth.rs"), "{result}");
    assert!(!result.contains("billing/src/billing.rs"), "{result}");
}

#[test]
fn tcp_daemon_serves_json_lines_requests() {
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args(["serve-tcp", "--addr", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"status\""));
    assert!(response.contains("\"cached_indexes\":0"));
}

#[test]
fn tcp_daemon_starts_with_warmed_index() {
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
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index",
            index_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();
    assert_eq!(startup_json["cached_indexes"], serde_json::json!(1));

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "indexed_search_code",
        "arguments": {
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/auth.rs"));
}

#[test]
fn tcp_daemon_starts_with_warmed_shards() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let mut runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--index-dir",
            shard_dir.path().to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    let addr = startup_json["addr"].as_str().unwrap();
    assert_eq!(startup_json["cached_indexes"], serde_json::json!(1));

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "search_shards",
        "arguments": {
            "index_dir": shard_dir.path(),
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/billing.rs"));
}

#[test]
fn server_handles_json_lines_tool_request() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
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
        &repo.path().join("tests/auth_test.rs"),
        "use sample::SessionManager;\n#[test]\nfn issue_token_round_trip() {}\n",
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
    let read_request = serde_json::json!({
        "id": "read-index-range",
        "tool": "read_index_range",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "start": 1,
            "lines": 2
        }
    });
    let symbol_request = serde_json::json!({
        "id": "find-index-symbol",
        "tool": "find_index_symbol",
        "arguments": {
            "index": index_path,
            "name": "SessionManager",
            "limit": 5
        }
    });
    let map_request = serde_json::json!({
        "id": "indexed-repo-map",
        "tool": "indexed_repo_map",
        "arguments": {
            "index": index_path,
            "symbols": 5,
            "tests": 5
        }
    });
    let related_request = serde_json::json!({
        "id": "related-index-files",
        "tool": "related_index_files",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "limit": 5
        }
    });
    let related_symbols_request = serde_json::json!({
        "id": "related-index-symbols",
        "tool": "related_index_symbols",
        "arguments": {
            "index": index_path,
            "path": "src/auth.rs",
            "query": "SessionManager",
            "limit": 5
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_symbols_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":2"));
    assert!(stdout.contains("src/auth.rs"));
    assert!(stdout.contains("\"id\":\"read-index-range\""));
    assert!(stdout.contains("\"path\":\"src/auth.rs\""));
    assert!(stdout.contains("issue_token"));
    assert!(stdout.contains("\"id\":\"find-index-symbol\""));
    assert!(stdout.contains("\"kind\":\"struct\""));
    assert!(stdout.contains("\"id\":\"indexed-repo-map\""));
    assert!(stdout.contains("\"entrypoints\""));
    assert!(stdout.contains("\"manifest_files\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"related-index-files\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"related-index-symbols\""));
    assert!(stdout.contains("SessionManager"));
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
            "query": "invoice total",
            "repo": "BILLING",
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
    let symbol_request = serde_json::json!({
        "id": "find-shard-symbol",
        "tool": "find_shard_symbol",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "name": "invoice total",
            "repo": "BILLING",
            "limit": 5
        }
    });
    let map_request = serde_json::json!({
        "id": "shard-repo-map",
        "tool": "shard_repo_map",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "repo": "BILLING",
            "symbols": 5,
            "tests": 5
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{index_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
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
    assert!(stdout.contains("\"id\":\"find-shard-symbol\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("\"name\":\"invoice_total\""));
    assert!(stdout.contains("\"id\":\"shard-repo-map\""));
    assert!(stdout.contains("\"entrypoints\":[\"billing/Cargo.toml\"]"));
    assert!(stdout.contains("\"manifest_files\":[\"billing/Cargo.toml\"]"));
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
