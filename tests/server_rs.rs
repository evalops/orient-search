use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

use orient::fast_index::FastIndex;
use orient::repo_index::{MAX_ATTACHED_CONTEXT_LINES, MAX_READ_RANGE_LINES, MAX_SEARCH_RESULTS};
use orient::server::{
    MAX_BATCH_QUERIES, MAX_BATCH_RANGES, ToolRequest, ToolRuntime, mcp_tool_manifest, tool_manifest,
};
use orient::shards::{build_shards, refresh_shards};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn git(repo: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {:?} failed", args);
}

fn tcp_tool_request(addr: &str, request: serde_json::Value) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    response
}

#[test]
fn tool_manifest_exposes_typed_defaults_and_input_schemas() {
    let manifest = tool_manifest();
    let tools = manifest.as_array().unwrap();
    let search = tools
        .iter()
        .find(|tool| tool["name"] == "search_code")
        .unwrap();
    let search_alias = tools.iter().find(|tool| tool["name"] == "search").unwrap();
    let discover = tools
        .iter()
        .find(|tool| tool["name"] == "discover_repos")
        .unwrap();
    let refresh_index = tools
        .iter()
        .find(|tool| tool["name"] == "refresh_index")
        .unwrap();
    let ensure_index = tools
        .iter()
        .find(|tool| tool["name"] == "ensure_index")
        .unwrap();
    let index_status = tools
        .iter()
        .find(|tool| tool["name"] == "index_status")
        .unwrap();
    let shard_status = tools
        .iter()
        .find(|tool| tool["name"] == "shard_status")
        .unwrap();
    let read_ranges = tools
        .iter()
        .find(|tool| tool["name"] == "read_ranges")
        .unwrap();
    let search_batch = tools
        .iter()
        .find(|tool| tool["name"] == "search_batch")
        .unwrap();
    let indexed_plan_batch = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_query_plan_batch")
        .unwrap();
    let shard_plan_batch = tools
        .iter()
        .find(|tool| tool["name"] == "shard_query_plan_batch")
        .unwrap();

    assert_eq!(search["required"], serde_json::json!(["repo", "query"]));
    assert_eq!(search["input_schema"]["properties"]["limit"]["default"], 10);
    assert_eq!(
        search["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "limit")
            .unwrap()["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        search["input_schema"]["properties"]["snippet"]["enum"],
        serde_json::json!(["short", "medium", "block", "symbol"])
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_path"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["import"]["type"],
        "string"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_import"]["oneOf"][1]["items"]["type"],
        "string"
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "exclude_symbol_kind")
            .unwrap()["type"],
        "string|string[]"
    );
    assert_eq!(
        search["input_schema"]["properties"]["exclude_symbol_kind"]["oneOf"][1]["items"]["type"],
        "string"
    );
    let indexed_search = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_search_code")
        .unwrap();
    let indexed_search_alias = tools
        .iter()
        .find(|tool| tool["name"] == "indexed_search")
        .unwrap();
    let index_plan_alias = tools
        .iter()
        .find(|tool| tool["name"] == "index_plan")
        .unwrap();
    let shard_plan_alias = tools
        .iter()
        .find(|tool| tool["name"] == "shard_plan")
        .unwrap();
    assert_eq!(
        search_alias["required"],
        serde_json::json!(["repo", "query"])
    );
    assert_eq!(
        search_alias["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        indexed_search["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        indexed_search["daemon_default"]["argument"],
        serde_json::json!("index")
    );
    assert_eq!(
        indexed_search["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search["input_schema"]["properties"]["index"]["x-daemon-default"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search["arguments"][0]["daemon_default"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search_alias["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        indexed_search_alias["input_schema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert_eq!(
        index_plan_alias["daemon_default"]["source"],
        serde_json::json!("single_warmed_index")
    );
    assert_eq!(
        shard_plan_alias["daemon_default"]["source"],
        serde_json::json!("single_warmed_shard_dir")
    );
    assert!(search.get("daemon_default").is_none());
    assert_eq!(
        search_batch["required"],
        serde_json::json!(["repo", "queries"])
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["items"]["type"],
        "string"
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["maxItems"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        search_batch["input_schema"]["properties"]["queries"]["minItems"],
        serde_json::json!(1)
    );
    assert_eq!(
        search_batch["arguments"][1]["max_items"],
        serde_json::json!(MAX_BATCH_QUERIES)
    );
    assert_eq!(
        indexed_plan_batch["required"],
        serde_json::json!(["index", "queries"])
    );
    assert_eq!(
        indexed_plan_batch["input_schema"]["properties"]["refresh_if_stale"]["default"],
        false
    );
    assert_eq!(
        shard_plan_batch["required"],
        serde_json::json!(["index_dir", "queries"])
    );
    assert_eq!(
        shard_plan_batch["daemon_default"]["argument"],
        serde_json::json!("index_dir")
    );
    assert_eq!(
        shard_plan_batch["daemon_default"]["source"],
        serde_json::json!("single_warmed_shard_dir")
    );
    assert_eq!(
        shard_status["input_schema"]["properties"]["index_dir"]["x-daemon-default"],
        serde_json::json!("single_warmed_shard_dir")
    );
    assert_eq!(
        discover["input_schema"]["properties"]["limit"]["default"],
        500
    );
    assert!(
        discover["input_schema"]["properties"]["limit"]
            .get("maximum")
            .is_none()
    );
    assert_eq!(
        discover["input_schema"]["properties"]["git_metadata"]["type"],
        "boolean"
    );
    assert_eq!(
        discover["input_schema"]["properties"]["tracked_files"]["default"],
        false
    );
    assert_eq!(
        discover["input_schema"]["properties"]["family_limit"]["default"],
        0
    );
    assert_eq!(
        discover["input_schema"]["properties"]["nested_manifests"]["default"],
        false
    );
    assert_eq!(
        refresh_index["required"],
        serde_json::json!(["repo", "index"])
    );
    assert_eq!(
        ensure_index["required"],
        serde_json::json!(["repo", "index"])
    );
    assert_eq!(index_status["required"], serde_json::json!(["index"]));
    assert_eq!(shard_status["required"], serde_json::json!(["index_dir"]));
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["items"]["properties"]["lines"]["default"],
        80
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["items"]["properties"]["lines"]["maximum"],
        serde_json::json!(MAX_READ_RANGE_LINES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["maxItems"],
        serde_json::json!(MAX_BATCH_RANGES)
    );
    assert_eq!(
        read_ranges["input_schema"]["properties"]["ranges"]["minItems"],
        serde_json::json!(1)
    );
    assert_eq!(read_ranges["arguments"][1]["type"], "range[]");
    assert_eq!(
        read_ranges["arguments"][1]["max_items"],
        serde_json::json!(MAX_BATCH_RANGES)
    );
    assert_eq!(
        search["input_schema"]["properties"]["context_lines"]["maximum"],
        serde_json::json!(MAX_ATTACHED_CONTEXT_LINES)
    );
    assert_eq!(
        search["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "context_lines")
            .unwrap()["maximum"],
        serde_json::json!(MAX_ATTACHED_CONTEXT_LINES)
    );

    let listed = ToolRuntime::default().dispatch(ToolRequest {
        id: serde_json::json!("list"),
        tool: "list_tools".to_string(),
        arguments: serde_json::json!({}),
    });
    assert!(listed.error.is_none(), "{:?}", listed.error);
    let listed_names = listed
        .result
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let manifest_names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(listed_names, manifest_names);
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
    assert!(stdout.contains("\"name\":\"search\""));
    assert!(stdout.contains("\"name\":\"indexed_search\""));
    assert!(stdout.contains("\"name\":\"index_plan\""));
    assert!(stdout.contains("\"name\":\"shard_plan\""));
    assert!(stdout.contains("\"name\":\"mcp_manifest\""));
    assert!(stdout.contains("\"required\":[\"repo\",\"query\"]"));
    assert!(stdout.contains("\"optional\""));
    assert!(stdout.contains("\"arguments\""));
    assert!(stdout.contains("\"input_schema\""));
    assert!(stdout.contains("\"type\":\"integer\""));
    assert!(stdout.contains("\"default\":10"));
    assert!(stdout.contains("\"enum\":[\"short\",\"medium\",\"block\",\"symbol\"]"));
    assert!(stdout.contains("\"type\":\"range[]\""));
    assert!(stdout.contains("exclude_path"));
    assert!(stdout.contains("exclude_symbol"));
    assert!(stdout.contains("open_range"));
    assert!(stdout.contains("read_ranges"));
    assert!(stdout.contains("search_batch"));
    assert!(stdout.contains("open_index_range"));
    assert!(stdout.contains("read_index_ranges"));
    assert!(stdout.contains("indexed_search_batch"));
    assert!(stdout.contains("read_shard_ranges"));
    assert!(stdout.contains("read_index_range"));
    assert!(stdout.contains("index_status"));
    assert!(stdout.contains("indexed_query_plan"));
    assert!(stdout.contains("indexed_query_plan_batch"));
    assert!(stdout.contains("indexed_repo_map"));
    assert!(stdout.contains("shard_query_plan"));
    assert!(stdout.contains("shard_query_plan_batch"));
    assert!(stdout.contains("find_index_symbol"));
    assert!(stdout.contains("related_index_files"));
    assert!(stdout.contains("related_index_symbols"));
    assert!(stdout.contains("open_shard_range"));
    assert!(stdout.contains("read_shard_range"));
    assert!(stdout.contains("related_shard_files"));
    assert!(stdout.contains("related_shard_symbols"));
    assert!(stdout.contains("shard_repo_map"));
    assert!(stdout.contains("find_shard_symbol"));
    assert!(stdout.contains("daemon_status"));
    assert!(stdout.contains("warm_index"));
    assert!(stdout.contains("warm_shards"));
    assert!(stdout.contains("ensure_shards"));
    assert!(stdout.contains("shard_status"));
    assert!(stdout.contains("search_shards_batch"));
    assert!(stdout.contains("discover_repos"));
}

#[test]
fn mcp_manifest_exposes_input_schema_for_adapter_wrappers() {
    let manifest = mcp_tool_manifest();
    let tools = manifest["tools"].as_array().unwrap();
    let search = tools
        .iter()
        .find(|tool| tool["name"] == "search_code")
        .unwrap();
    let mcp_manifest = tools
        .iter()
        .find(|tool| tool["name"] == "mcp_manifest")
        .unwrap();

    assert_eq!(
        search["description"],
        "Search a local repository with the fast fallback path and return ranked snippets."
    );
    assert_eq!(
        search["inputSchema"]["required"],
        serde_json::json!(["repo", "query"])
    );
    assert_eq!(
        search["inputSchema"]["properties"]["limit"]["maximum"],
        serde_json::json!(MAX_SEARCH_RESULTS)
    );
    assert!(search.get("input_schema").is_none());
    assert_eq!(
        mcp_manifest["inputSchema"]["properties"],
        serde_json::json!({})
    );
}

#[test]
fn runtime_rejects_oversized_batches() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let runtime = ToolRuntime::default();
    let too_many_queries = (0..=MAX_BATCH_QUERIES)
        .map(|index| format!("query_{index}"))
        .collect::<Vec<_>>();

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-queries"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": too_many_queries
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 32"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-queries"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": []
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("must not be empty"), "{error}");

    let too_many_ranges = (0..=MAX_BATCH_RANGES)
        .map(|_| {
            serde_json::json!({
                "path": "src/auth.rs",
                "start": 1,
                "lines": 1
            })
        })
        .collect::<Vec<_>>();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-ranges"),
        tool: "read_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": too_many_ranges
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 64"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("empty-ranges"),
        tool: "open_ranges".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "ranges": []
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("must not be empty"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("zero-start"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 0,
            "lines": 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("positive integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("zero-limit"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": 0
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("positive integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("negative-limit"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": -1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("non-negative integer"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-results"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "limit": MAX_SEARCH_RESULTS + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 100"), "{error}");

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-much-context"),
        tool: "search_code".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "SessionManager",
            "context_lines": MAX_ATTACHED_CONTEXT_LINES + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains(&format!("max {MAX_ATTACHED_CONTEXT_LINES}")),
        "{error}"
    );

    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("too-many-lines"),
        tool: "open_range".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 1,
            "lines": MAX_READ_RANGE_LINES + 1
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("max 1000"), "{error}");
}

#[test]
fn runtime_batches_searches_and_query_plans_against_repo_index_and_shards() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let fallback = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fallback-batch"),
        tool: "search_batch".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "queries": ["SessionManager", "invoice total"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(fallback.error.is_none(), "{:?}", fallback.error);
    let result = serde_json::to_string(&fallback.result).unwrap();
    assert!(result.contains("\"query\":\"SessionManager\""), "{result}");
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("\"query\":\"invoice total\""), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-batch"),
        tool: "indexed_search_batch".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "queries": ["SessionManager", "invoice total"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let result = serde_json::to_string(&indexed.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let indexed_plans = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed-plan-batch"),
        tool: "indexed_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "queries": ["SessionManager missingterm", "invoice absentterm"],
            "require_all": true
        }),
    });
    assert!(indexed_plans.error.is_none(), "{:?}", indexed_plans.error);
    let result = serde_json::to_string(&indexed_plans.result).unwrap();
    assert!(
        result.contains("\"query\":\"SessionManager missingterm\""),
        "{result}"
    );
    assert!(
        result.contains("\"query\":\"invoice absentterm\""),
        "{result}"
    );
    assert!(result.contains("\"missing_terms\""), "{result}");
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("absentterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");

    let indexed_plan_alias = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index-plan-alias"),
        tool: "index_plan".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "SessionManager missingterm",
            "require_all": true
        }),
    });
    assert!(
        indexed_plan_alias.error.is_none(),
        "{:?}",
        indexed_plan_alias.error
    );
    let result = serde_json::to_string(&indexed_plan_alias.result).unwrap();
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");

    let shard_dir = tempfile::tempdir().unwrap();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build-shards"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let shards = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-batch"),
        tool: "search_shards_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "queries": ["SessionManager", "invoice total"],
            "limit": 2,
            "require_all": true
        }),
    });
    assert!(shards.error.is_none(), "{:?}", shards.error);
    let result = serde_json::to_string(&shards.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("src/billing.rs"), "{result}");

    let shard_plans = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-batch"),
        tool: "shard_query_plan_batch".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "queries": ["SessionManager missingterm", "invoice absentterm"],
            "require_all": true
        }),
    });
    assert!(shard_plans.error.is_none(), "{:?}", shard_plans.error);
    let result = serde_json::to_string(&shard_plans.result).unwrap();
    assert!(
        result.contains("\"query\":\"SessionManager missingterm\""),
        "{result}"
    );
    assert!(
        result.contains("\"query\":\"invoice absentterm\""),
        "{result}"
    );
    assert!(result.contains("\"plans\""), "{result}");
    assert!(result.contains("\"missing_terms\""), "{result}");
    assert!(result.contains("missingterm"), "{result}");
    assert!(result.contains("absentterm"), "{result}");
    assert!(result.contains("drop_missing_terms"), "{result}");

    let shard_plan_alias = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-plan-alias"),
        tool: "shard_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "SessionManager missingterm",
            "require_all": true
        }),
    });
    assert!(
        shard_plan_alias.error.is_none(),
        "{:?}",
        shard_plan_alias.error
    );
    let result = serde_json::to_string(&shard_plan_alias.result).unwrap();
    assert!(result.contains("missingterm"), "{result}");
}

#[test]
fn runtime_accepts_structured_negative_search_filters() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("generated/auth.rs"),
        "pub struct GeneratedSessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("src/generated_symbol.rs"),
        "pub struct GeneratedSessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let fallback = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fallback"),
        tool: "search".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "query": "issue token",
            "limit": 10,
            "dir": "src",
            "symbol_kind": "function",
            "require_all": true,
            "exclude_path": ["generated"]
        }),
    });
    assert!(fallback.error.is_none(), "{:?}", fallback.error);
    let result = serde_json::to_string(&fallback.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let indexed = runtime.dispatch(ToolRequest {
        id: serde_json::json!("indexed"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 10,
            "dir": "src",
            "symbol_kind": "function",
            "require_all": true,
            "exclude_path": ["generated"],
            "exclude_symbol": "GeneratedSessionManager",
            "exclude_symbol_kind": "enum"
        }),
    });
    assert!(indexed.error.is_none(), "{:?}", indexed.error);
    let result = serde_json::to_string(&indexed.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(!result.contains("generated/auth.rs"), "{result}");
    assert!(!result.contains("src/generated_symbol.rs"), "{result}");

    let filter_only = runtime.dispatch(ToolRequest {
        id: serde_json::json!("filter-only"),
        tool: "indexed_search".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "file:auth.rs",
            "limit": 10,
            "explain": true
        }),
    });
    assert!(filter_only.error.is_none(), "{:?}", filter_only.error);
    let result = serde_json::to_string(&filter_only.result).unwrap();
    assert!(result.contains("src/auth.rs"), "{result}");
    assert!(result.contains("filter_scan"), "{result}");
    assert!(result.contains("file_filter"), "{result}");
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

    let runtime = ToolRuntime::default();
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
fn runtime_discovery_suppresses_nested_manifest_projects_under_git_roots() {
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

    let runtime = ToolRuntime::default();
    let default = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 4,
            "limit": 20
        }),
    });
    assert!(default.error.is_none(), "{:?}", default.error);
    let result = serde_json::to_string(&default.result).unwrap();
    assert!(result.contains("\"repos_found\":1"), "{result}");
    assert!(result.contains("\"name\":\"platform\""), "{result}");
    assert!(!result.contains("\"name\":\"ui\""), "{result}");

    let nested = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 4,
            "limit": 20,
            "nested_manifests": true
        }),
    });
    assert!(nested.error.is_none(), "{:?}", nested.error);
    let result = serde_json::to_string(&nested.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"name\":\"ui\""), "{result}");
}

#[test]
fn runtime_discovers_repo_families_with_git_metadata() {
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10,
            "git_metadata": true,
            "tracked_files": true
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = serde_json::to_string(&response.result).unwrap();
    assert!(result.contains("\"repos_found\":2"), "{result}");
    assert!(result.contains("\"families\""), "{result}");
    assert!(result.contains("\"checkouts\":2"), "{result}");
    assert!(result.contains("\"worktrees\":1"), "{result}");
    assert!(result.contains("\"clones\":1"), "{result}");
    assert!(result.contains("\"tracked_files\":2"), "{result}");
    assert!(result.contains("\"git_kind\":\"worktree\""), "{result}");
    assert!(result.contains("\"branch\":\"feature/search\""), "{result}");
}

#[test]
fn runtime_discovery_can_limit_repeated_repo_families() {
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

    let runtime = ToolRuntime::default();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("discover"),
        tool: "discover_repos".to_string(),
        arguments: serde_json::json!({
            "root": root.path(),
            "max_depth": 2,
            "limit": 10,
            "family_limit": 1
        }),
    });
    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.unwrap();
    assert_eq!(result["candidates_found"], 2);
    assert_eq!(result["repos_found"], 1);
    assert_eq!(result["family_limit"], 1);
    assert_eq!(result["repos"][0]["name"], "project");
    assert_eq!(result["families"][0]["checkouts"], 2);
    assert!(
        result["families"][0]["paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap().contains("project-feature"))
    );
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

    let runtime = ToolRuntime::default();
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
    let build_result = build.result.unwrap();
    assert_eq!(build_result["shards"], serde_json::json!(2));
    assert_eq!(build_result["discovery"][0]["selected_repos"], 2);
    assert_eq!(build_result["discovery"][0]["candidates_found"], 2);

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
fn runtime_shard_repo_map_reports_git_metadata() {
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

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "unique branch token",
            "repo": "shard-feature-branch",
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let search_result = serde_json::to_string(&search.result).unwrap();
    assert!(search_result.contains("src/lib.rs"), "{search_result}");

    let map = runtime.dispatch(ToolRequest {
        id: serde_json::json!("map"),
        tool: "shard_repo_map".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "repo": "shard-project",
            "symbols": 5,
            "tests": 5
        }),
    });
    assert!(map.error.is_none(), "{:?}", map.error);
    let result = serde_json::to_string(&map.result).unwrap();
    assert!(result.contains("\"git\""), "{result}");
    assert!(
        result.contains("\"branch\":\"shard-feature-branch\""),
        "{result}"
    );
    assert!(
        result.contains("https://github.com/evalops/shard-project.git"),
        "{result}"
    );
}

#[test]
fn runtime_indexes_shards_from_multiple_discovered_roots() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    write(
        &left.path().join("platform/src/lib.rs"),
        "pub fn platform_session() {}\n",
    );
    write(
        &left.path().join("platform/Cargo.toml"),
        "[package]\nname='platform'\nversion='0.1.0'\nedition='2024'\n",
    );
    write(
        &right.path().join("maestro/src/lib.rs"),
        "pub fn maestro_session() {}\n",
    );
    write(
        &right.path().join("maestro/Cargo.toml"),
        "[package]\nname='maestro'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("index"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "discover_roots": [left.path(), right.path()],
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
            "query": "maestro_session"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("maestro/src/lib.rs"), "{result}");
}

#[test]
fn runtime_ensures_shards_builds_refreshes_and_warms() {
    let root = tempfile::tempdir().unwrap();
    write(
        &root.path().join("platform/src/lib.rs"),
        "pub fn platform_session() {}\n",
    );
    write(
        &root.path().join("platform/Cargo.toml"),
        "[package]\nname='platform'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = root.path().join(".orient-shards");

    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-build"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "discover_root": root.path(),
            "max_depth": 2,
            "output_dir": shard_dir
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let build_result = build.result.unwrap();
    assert_eq!(build_result["stats"]["action"], serde_json::json!("build"));
    assert_eq!(
        build_result["stats"]["discovery"][0]["selected_repos"],
        serde_json::json!(1)
    );
    assert_eq!(build_result["warmed_indexes"], serde_json::json!(1));
    assert_eq!(build_result["cached_indexes"], serde_json::json!(1));

    write(
        &root.path().join("platform/src/extra.rs"),
        "pub fn extra_platform_session() {}\n",
    );
    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-refresh"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "output_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    let refresh_result = refresh.result.unwrap();
    assert_eq!(
        refresh_result["stats"]["action"],
        serde_json::json!("refresh")
    );
    assert_eq!(refresh_result["warmed_indexes"], serde_json::json!(1));
    assert_eq!(refresh_result["cached_indexes"], serde_json::json!(1));
    assert!(refresh_result["stats"]["refreshed_files"].as_u64().unwrap() >= 1);

    write(
        &root.path().join("billing/src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &root.path().join("billing/Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let add = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure-add"),
        tool: "ensure_shards".to_string(),
        arguments: serde_json::json!({
            "discover_root": root.path(),
            "max_depth": 2,
            "output_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(add.error.is_none(), "{:?}", add.error);
    let add_result = add.result.unwrap();
    assert_eq!(
        add_result["stats"]["action"],
        serde_json::json!("refresh+add")
    );
    assert_eq!(add_result["stats"]["added_shards"], serde_json::json!(1));
    assert_eq!(add_result["stats"]["shards"], serde_json::json!(2));
    assert_eq!(add_result["warmed_indexes"], serde_json::json!(2));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search-added"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "invoice_total"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("billing/src/lib.rs"), "{result}");

    write(
        &root.path().join("platform/src/after_status.rs"),
        "pub fn after_status_session() {}\n",
    );
    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("shard-status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards")
        }),
    });
    assert!(status.error.is_none(), "{:?}", status.error);
    let status = status.result.unwrap();
    assert_eq!(status["stale"], serde_json::json!(true));
    assert_eq!(status["shard_count"], serde_json::json!(2));
    assert_eq!(status["stale_shards"], serde_json::json!(1));
    assert!(status["source_bytes"].as_u64().unwrap() > 0);
    assert!(status["posting_entries"].as_u64().unwrap() > 0);
    assert!(status["compressed_posting_bytes"].as_u64().unwrap() > 0);
    let shard_names = status["shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|shard| shard["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(shard_names, vec!["platform", "billing"]);
    assert!(
        status["shards"][0]["status"]["source_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(status["added_files"], serde_json::json!(1));

    let stale_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale-shard-search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "after_status_session"
        }),
    });
    assert!(stale_search.error.is_none(), "{:?}", stale_search.error);
    assert_eq!(stale_search.result.unwrap(), serde_json::json!([]));

    let refreshed_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh-shard-search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": root.path().join(".orient-shards"),
            "query": "after_status_session",
            "refresh_if_stale": true
        }),
    });
    assert!(
        refreshed_search.error.is_none(),
        "{:?}",
        refreshed_search.error
    );
    let result = serde_json::to_string(&refreshed_search.result).unwrap();
    assert!(result.contains("platform/src/after_status.rs"), "{result}");
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

    let runtime = ToolRuntime::default();
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
    assert_eq!(
        result["cached_index_details"][0]["files"],
        serde_json::json!(2)
    );
    assert_eq!(
        result["cached_index_details"][0]["root"],
        serde_json::json!(repo.path().canonicalize().unwrap().to_string_lossy())
    );
    assert_eq!(
        result["cached_index_details"][0]["symbols"],
        serde_json::json!(2)
    );
    assert!(
        result["cached_index_details"][0]["index"]
            .as_str()
            .unwrap()
            .ends_with(".orient/index")
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

    let runtime = ToolRuntime::default();
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
fn runtime_refresh_index_updates_cached_single_repo_index() {
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

    let runtime = ToolRuntime::default();
    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "rotate secret",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert_eq!(first.result.unwrap(), serde_json::json!([]));

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
    );
    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    assert_eq!(refresh.result.as_ref().unwrap()["refreshed_files"], 1);
    assert_eq!(refresh.result.as_ref().unwrap()["files"], 2);

    let second = runtime.dispatch(ToolRequest {
        id: serde_json::json!("second"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "rotate secret",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(second.error.is_none(), "{:?}", second.error);
    let result = serde_json::to_string(&second.result).unwrap();
    assert!(result.contains("rotate_secret"), "{result}");

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_index_range".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "path": "src/auth.rs",
            "start": 3,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("rotate_secret"),
        "{:?}",
        range.result
    );
}

#[test]
fn runtime_ensure_index_builds_missing_index_and_warms_cache() {
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
    let runtime = ToolRuntime::default();

    let ensure = runtime.dispatch(ToolRequest {
        id: serde_json::json!("ensure"),
        tool: "ensure_index".to_string(),
        arguments: serde_json::json!({
            "repo": repo.path(),
            "index": index_path
        }),
    });
    assert!(ensure.error.is_none(), "{:?}", ensure.error);
    assert!(index_path.exists());
    assert_eq!(ensure.result.as_ref().unwrap()["refreshed_files"], 2);
    assert_eq!(ensure.result.as_ref().unwrap()["files"], 2);

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({}),
    });
    assert!(status.error.is_none(), "{:?}", status.error);
    let status = status.result.unwrap();
    assert_eq!(status["cached_indexes"], serde_json::json!(1));
    assert_eq!(
        status["cached_index_details"][0]["files"],
        serde_json::json!(2)
    );

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("issue_token"), "{result}");
}

#[test]
fn runtime_reports_index_status_for_cached_indexes() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    let runtime = ToolRuntime::default();

    let clean = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "index_status".to_string(),
        arguments: serde_json::json!({ "index": index_path }),
    });
    assert!(clean.error.is_none(), "{:?}", clean.error);
    let clean_result = clean.result.as_ref().unwrap();
    assert_eq!(clean_result["stale"], serde_json::json!(false));
    assert!(clean_result["source_bytes"].as_u64().unwrap() > 0);
    assert!(clean_result["posting_entries"].as_u64().unwrap() > 0);
    assert!(clean_result["compressed_posting_bytes"].as_u64().unwrap() > 0);

    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret_now() {}\n",
    );
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session() {}\n",
    );

    let stale = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "index_status".to_string(),
        arguments: serde_json::json!({ "index": index_path }),
    });
    assert!(stale.error.is_none(), "{:?}", stale.error);
    let result = stale.result.unwrap();
    assert_eq!(result["stale"], serde_json::json!(true));
    assert_eq!(result["changed_paths"], serde_json::json!(["src/auth.rs"]));
    assert_eq!(
        result["added_paths"],
        serde_json::json!(["src/new_session.rs"])
    );
}

#[test]
fn runtime_can_refresh_stale_index_before_search() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    let index_path = repo.path().join(".orient/index");
    FastIndex::build(repo.path())
        .unwrap()
        .save(&index_path)
        .unwrap();
    write(
        &repo.path().join("src/new_session.rs"),
        "pub fn new_session_token() {}\n",
    );
    let runtime = ToolRuntime::default();

    let stale_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("stale"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "new session token",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(stale_search.error.is_none(), "{:?}", stale_search.error);
    assert_eq!(stale_search.result.unwrap(), serde_json::json!([]));

    let refreshed_search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("fresh"),
        tool: "indexed_search_code".to_string(),
        arguments: serde_json::json!({
            "index": index_path,
            "query": "new session token",
            "limit": 3,
            "require_all": true,
            "refresh_if_stale": true
        }),
    });
    assert!(
        refreshed_search.error.is_none(),
        "{:?}",
        refreshed_search.error
    );
    let result = serde_json::to_string(&refreshed_search.result).unwrap();
    assert!(result.contains("src/new_session.rs"), "{result}");
}

#[test]
fn runtime_coalesces_parallel_cold_index_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
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

    let runtime = Arc::new(ToolRuntime::default());
    let mut handles = Vec::new();
    for index in 0..12 {
        let runtime = Arc::clone(&runtime);
        let index_path = index_path.clone();
        handles.push(thread::spawn(move || {
            let query = if index % 2 == 0 {
                "issue token"
            } else {
                "rotate secret"
            };
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "indexed_search_code".to_string(),
                arguments: serde_json::json!({
                    "index": index_path,
                    "query": query,
                    "limit": 3,
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        assert!(
            serde_json::to_string(&response.result)
                .unwrap()
                .contains("src/auth.rs"),
            "{:?}",
            response.result
        );
    }
    assert_eq!(runtime.cached_index_count(), 1);
}

#[test]
fn runtime_reuses_cached_shard_index_after_initial_load() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/billing.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.path().join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();

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
    let shard_name = manifest["shards"][0]["name"].as_str().unwrap();
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

    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        range.result
    );

    let related = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related"),
        tool: "related_shard_files".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "limit": 5
        }),
    });
    assert!(related.error.is_none(), "{:?}", related.error);
    assert!(
        serde_json::to_string(&related.result)
            .unwrap()
            .contains(&format!("{shard_name}/tests/billing_test.rs")),
        "{:?}",
        related.result
    );

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols"),
        tool: "related_shard_symbols".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "query": "invoice total",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    assert!(
        serde_json::to_string(&related_symbols.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        related_symbols.result
    );

    let plan = runtime.dispatch(ToolRequest {
        id: serde_json::json!("plan"),
        tool: "shard_query_plan".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice missingterm",
            "require_all": true
        }),
    });
    assert!(plan.error.is_none(), "{:?}", plan.error);
    let plan_result = serde_json::to_string(&plan.result).unwrap();
    assert!(plan_result.contains("\"missing_terms\""), "{plan_result}");
    assert!(plan_result.contains("missingterm"), "{plan_result}");
    assert!(plan_result.contains("drop_missing_terms"), "{plan_result}");
}

#[test]
fn runtime_reuses_cached_shard_manifest_after_initial_load() {
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
    let runtime = ToolRuntime::default();

    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [repo.path()],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    let shard_dir_canonical = shard_dir.path().canonicalize().unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(shard_dir.path().join("manifest.json")).unwrap()).unwrap();
    let shard_name = manifest["shards"][0]["name"].as_str().unwrap();

    let first = runtime.dispatch(ToolRequest {
        id: serde_json::json!("first"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }),
    });
    assert!(first.error.is_none(), "{:?}", first.error);
    assert!(
        serde_json::to_string(&first.result)
            .unwrap()
            .contains("src/billing.rs"),
        "{:?}",
        first.result
    );

    fs::remove_file(shard_dir.path().join("manifest.json")).unwrap();
    let range = runtime.dispatch(ToolRequest {
        id: serde_json::json!("range"),
        tool: "read_shard_range".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": format!("{shard_name}/src/billing.rs"),
            "start": 1,
            "lines": 1
        }),
    });
    assert!(range.error.is_none(), "{:?}", range.error);
    assert!(
        serde_json::to_string(&range.result)
            .unwrap()
            .contains("invoice_total"),
        "{:?}",
        range.result
    );

    let status = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "daemon_status".to_string(),
        arguments: serde_json::json!({}),
    });
    let result = status.result.unwrap();
    assert_eq!(result["cached_indexes"], serde_json::json!(1));
    assert_eq!(result["cached_shard_manifests"], serde_json::json!(1));
    assert!(
        result["cached_shard_manifest_paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path.as_str().unwrap() == shard_dir_canonical.to_str().unwrap())
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["index_dir"],
        serde_json::json!(shard_dir_canonical.to_str().unwrap())
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["shards"],
        serde_json::json!(1)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["name"],
        serde_json::json!(shard_name)
    );
    assert_eq!(
        result["cached_shard_manifest_details"][0]["repos"][0]["aliases"][0],
        serde_json::json!(shard_name)
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
    let runtime = ToolRuntime::default();
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
    assert_eq!(result["warmed_shards"]["shards"], serde_json::json!(1));
    assert_eq!(
        result["warmed_shards"]["repos"][0]["aliases"][0],
        result["warmed_shards"]["repos"][0]["name"]
    );
}

#[test]
fn shard_manifest_save_replaces_existing_file_without_leaving_temp_files() {
    let workspace = tempfile::tempdir().unwrap();
    let repo = workspace.path().join("billing");
    write(
        &repo.join("src/lib.rs"),
        "pub fn invoice_total() -> usize { 42 }\n",
    );
    write(
        &repo.join("Cargo.toml"),
        "[package]\nname='billing'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();
    build_shards(&[repo.clone()], shard_dir.path()).unwrap();
    let manifest_path = shard_dir.path().join("manifest.json");
    let original_manifest = fs::read_to_string(&manifest_path).unwrap();
    assert!(original_manifest.contains("\"shards\""));

    fs::remove_dir_all(&repo).unwrap();
    let refreshed = refresh_shards(shard_dir.path()).unwrap();
    assert_eq!(refreshed.removed_shards, 1);
    let updated_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(updated_manifest["shards"], serde_json::json!([]));
    let temp_files = fs::read_dir(shard_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
        .collect::<Vec<_>>();
    assert!(temp_files.is_empty(), "{temp_files:?}");
}

#[test]
fn shard_manifest_rejects_unsafe_or_ambiguous_entries() {
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
    let manifest_path = shard_dir.path().join("manifest.json");

    fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "shards": [{
                "name": "bad",
                "root": shard_dir.path(),
                "index": "../escape.orient",
                "aliases": []
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    let error = response.error.unwrap();
    assert!(
        error.contains("invalid shard manifest index path"),
        "{error}"
    );

    fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "shards": [
                {
                    "name": "duplicate",
                    "root": shard_dir.path(),
                    "index": "one.orient",
                    "aliases": []
                },
                {
                    "name": "duplicate",
                    "root": shard_dir.path(),
                    "index": "two.orient",
                    "aliases": []
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let response = runtime.dispatch(ToolRequest {
        id: serde_json::json!("status"),
        tool: "shard_status".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    let error = response.error.unwrap();
    assert!(error.contains("duplicate shard name"), "{error}");
}

#[test]
fn runtime_serves_parallel_warm_shard_searches() {
    let root = tempfile::tempdir().unwrap();
    let mut repos = Vec::new();
    for index in 0..6 {
        let repo = root.path().join(format!("service_{index}"));
        write(
            &repo.join("src/lib.rs"),
            &format!("pub fn shared_search_token_{index}() -> usize {{ {index} }}\n"),
        );
        write(
            &repo.join("Cargo.toml"),
            &format!("[package]\nname='service-{index}'\nversion='0.1.0'\nedition='2024'\n"),
        );
        repos.push(repo);
    }
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(ToolRuntime::default());
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": repos,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(6));

    runtime.warm_shards(shard_dir.path().to_path_buf()).unwrap();
    assert_eq!(runtime.cached_index_count(), 6);

    let mut handles = Vec::new();
    for index in 0..8 {
        let runtime = Arc::clone(&runtime);
        let shard_dir = shard_dir.path().to_path_buf();
        handles.push(thread::spawn(move || {
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "search_shards".to_string(),
                arguments: serde_json::json!({
                    "index_dir": shard_dir,
                    "query": "shared search token",
                    "limit": 5,
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        let result = serde_json::to_string(&response.result).unwrap();
        assert!(result.contains("shared_search_token"), "{result}");
        assert!(result.contains("service_"), "{result}");
    }
    assert_eq!(runtime.cached_index_count(), 6);
}

#[test]
fn runtime_serves_parallel_warm_shard_query_plans() {
    let root = tempfile::tempdir().unwrap();
    let mut repos = Vec::new();
    for index in 0..6 {
        let repo = root.path().join(format!("service_{index}"));
        write(
            &repo.join("src/lib.rs"),
            &format!("pub fn shared_plan_token_{index}() -> usize {{ {index} }}\n"),
        );
        write(
            &repo.join("Cargo.toml"),
            &format!("[package]\nname='service-{index}'\nversion='0.1.0'\nedition='2024'\n"),
        );
        repos.push(repo);
    }
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(ToolRuntime::default());
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": repos,
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(6));

    runtime.warm_shards(shard_dir.path().to_path_buf()).unwrap();
    assert_eq!(runtime.cached_index_count(), 6);

    let mut handles = Vec::new();
    for index in 0..8 {
        let runtime = Arc::clone(&runtime);
        let shard_dir = shard_dir.path().to_path_buf();
        handles.push(thread::spawn(move || {
            runtime.dispatch(ToolRequest {
                id: serde_json::json!(index),
                tool: "shard_query_plan".to_string(),
                arguments: serde_json::json!({
                    "index_dir": shard_dir,
                    "query": "shared plan missingterm",
                    "require_all": true
                }),
            })
        }));
    }

    for handle in handles {
        let response = handle.join().unwrap();
        assert!(response.error.is_none(), "{:?}", response.error);
        let result = serde_json::to_string(&response.result).unwrap();
        assert!(result.contains("\"missing_terms\""), "{result}");
        assert!(result.contains("missingterm"), "{result}");
        assert!(result.contains("drop_missing_terms"), "{result}");
        assert!(result.contains("service_"), "{result}");
    }
    assert_eq!(runtime.cached_index_count(), 6);
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
    let runtime = ToolRuntime::default();
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
    let search_result = search.result.as_ref().unwrap().as_array().unwrap();
    assert_eq!(
        search_result[0]["read_range"]["path"],
        serde_json::json!("billing/src/billing.rs")
    );
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
    assert!(result.contains("\"command_hints\""), "{result}");
    assert!(
        result.contains("\"source\":\"billing/Cargo.toml\""),
        "{result}"
    );
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

    let related = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related"),
        tool: "related_shard_files".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "limit": 5
        }),
    });
    assert!(related.error.is_none(), "{:?}", related.error);
    let result = serde_json::to_string(&related.result).unwrap();
    assert!(result.contains("billing/tests/billing_test.rs"), "{result}");
    assert!(!result.contains("auth/src/auth.rs"), "{result}");

    let related_symbols = runtime.dispatch(ToolRequest {
        id: serde_json::json!("related-symbols"),
        tool: "related_shard_symbols".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "path": "billing/src/billing.rs",
            "query": "invoice total",
            "limit": 5
        }),
    });
    assert!(
        related_symbols.error.is_none(),
        "{:?}",
        related_symbols.error
    );
    let result = serde_json::to_string(&related_symbols.result).unwrap();
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
    let runtime = ToolRuntime::default();
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
fn runtime_refresh_shards_prunes_missing_repo_roots() {
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
    let runtime = ToolRuntime::default();
    let build = runtime.dispatch(ToolRequest {
        id: serde_json::json!("build"),
        tool: "index_shards".to_string(),
        arguments: serde_json::json!({
            "repos": [auth_repo, billing_repo],
            "output_dir": shard_dir.path()
        }),
    });
    assert!(build.error.is_none(), "{:?}", build.error);
    assert_eq!(build.result.unwrap()["shards"], serde_json::json!(2));

    fs::remove_dir_all(workspace.path().join("billing")).unwrap();

    let refresh = runtime.dispatch(ToolRequest {
        id: serde_json::json!("refresh"),
        tool: "refresh_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path()
        }),
    });
    assert!(refresh.error.is_none(), "{:?}", refresh.error);
    let result = refresh.result.unwrap();
    assert_eq!(result["removed_shards"], serde_json::json!(1));
    assert_eq!(result["shards"], serde_json::json!(1));

    let search = runtime.dispatch(ToolRequest {
        id: serde_json::json!("search"),
        tool: "search_shards".to_string(),
        arguments: serde_json::json!({
            "index_dir": shard_dir.path(),
            "query": "issue_token"
        }),
    });
    assert!(search.error.is_none(), "{:?}", search.error);
    let result = serde_json::to_string(&search.result).unwrap();
    assert!(result.contains("auth/src/auth.rs"), "{result}");
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

#[cfg(unix)]
#[test]
fn unix_daemon_serves_json_lines_requests() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();
    let startup_json: serde_json::Value = serde_json::from_str(&startup).unwrap();
    assert_eq!(startup_json["transport"], serde_json::json!("unix"));
    assert_eq!(startup_json["socket"], serde_json::json!(socket));

    let mut stream = UnixStream::connect(&socket).unwrap();
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

#[cfg(unix)]
#[test]
fn unix_client_forwards_json_lines_requests() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();

    let mut client = Command::new(binary)
        .args(["client-jsonl", "--socket", socket.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let request = serde_json::json!({
        "id": "status",
        "tool": "daemon_status",
        "arguments": {}
    });
    writeln!(client.stdin.as_mut().unwrap(), "{request}").unwrap();
    drop(client.stdin.take());
    let output = client.wait_with_output().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"status\""), "{stdout}");
    assert!(stdout.contains("\"cached_indexes\":0"), "{stdout}");
}

#[cfg(unix)]
#[test]
fn unix_daemon_refuses_to_replace_non_socket_path() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    fs::write(&socket, "not a socket").unwrap();
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let output = Command::new(binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing to remove non-socket path"),
        "{stderr}"
    );
    assert_eq!(fs::read_to_string(&socket).unwrap(), "not a socket");
}

#[cfg(unix)]
#[test]
fn unix_daemon_refuses_to_replace_active_socket() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket = socket_dir.path().join("orient.sock");
    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut startup_reader = BufReader::new(stdout);
    let mut startup = String::new();
    startup_reader.read_line(&mut startup).unwrap();

    let output = Command::new(&binary)
        .args(["serve-unix", "--socket", socket.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("refusing to replace active unix socket"),
        "{stderr}"
    );

    let mut stream = UnixStream::connect(&socket).unwrap();
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

    assert!(response.contains("\"id\":\"status\""), "{response}");
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
    assert_eq!(
        startup_json["daemon_status"]["cached_indexes"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(0)
    );

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "indexed_search_code",
        "arguments": {
            "query": "issue token",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let plan_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "plan",
            "tool": "indexed_query_plan",
            "arguments": {
                "query": "SessionManager missingterm",
                "require_all": true
            }
        }),
    );
    let read_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "read",
            "tool": "read_index_range",
            "arguments": {
                "path": "src/auth.rs",
                "start": 1,
                "lines": 2
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/auth.rs"));
    assert!(plan_response.contains("\"id\":\"plan\""));
    assert!(plan_response.contains("\"missing_terms\""));
    assert!(plan_response.contains("missingterm"));
    assert!(read_response.contains("\"id\":\"read\""));
    assert!(read_response.contains("SessionManager"));
}

#[test]
fn tcp_daemon_can_ensure_and_warm_shards_on_startup() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\n",
    );
    write(
        &repo.path().join("Cargo.toml"),
        "[package]\nname='sample'\nversion='0.1.0'\nedition='2024'\n",
    );
    let shard_dir = tempfile::tempdir().unwrap();

    let binary = assert_cmd::cargo::cargo_bin("orient");
    let mut child = Command::new(binary)
        .args([
            "serve-tcp",
            "--addr",
            "127.0.0.1:0",
            "--ensure-shards-dir",
            shard_dir.path().to_str().unwrap(),
            "--repo",
            repo.path().to_str().unwrap(),
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
    let addr = startup_json["addr"].as_str().unwrap().to_string();

    assert_eq!(startup_json["ensured_shards"][0]["action"], "build");
    assert_eq!(
        startup_json["ensured_shards"][0]["shards"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_indexes"],
        serde_json::json!(1)
    );

    let response = tcp_tool_request(
        &addr,
        serde_json::json!({
            "id": "search",
            "tool": "search_shards",
            "arguments": {
                "index_dir": shard_dir.path(),
                "query": "issue token",
                "limit": 3,
                "require_all": true
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("auth.rs"), "{response}");
}

#[test]
fn tcp_daemon_serves_parallel_cached_index_requests() {
    let repo = tempfile::tempdir().unwrap();
    write(
        &repo.path().join("src/auth.rs"),
        "pub struct SessionManager;\npub fn issue_token() {}\npub fn rotate_secret() {}\n",
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
    let addr = startup_json["addr"].as_str().unwrap().to_string();

    let first_addr = addr.clone();
    let first = thread::spawn(move || {
        tcp_tool_request(
            &first_addr,
            serde_json::json!({
                "id": "first",
                "tool": "indexed_search_code",
                "arguments": {
                    "query": "issue token",
                    "limit": 3,
                    "require_all": true
                }
            }),
        )
    });
    let second = thread::spawn(move || {
        tcp_tool_request(
            &addr,
            serde_json::json!({
                "id": "second",
                "tool": "indexed_search_code",
                "arguments": {
                    "query": "rotate secret",
                    "limit": 3,
                    "require_all": true
                }
            }),
        )
    });

    let first_response = first.join().unwrap();
    let second_response = second.join().unwrap();

    child.kill().unwrap();
    let _ = child.wait();

    assert!(first_response.contains("\"id\":\"first\""));
    assert!(first_response.contains("src/auth.rs"));
    assert!(second_response.contains("\"id\":\"second\""));
    assert!(second_response.contains("src/auth.rs"));
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
    let shard_read_path = format!(
        "{}/src/billing.rs",
        repo.path().file_name().unwrap().to_string_lossy()
    );
    let shard_dir = tempfile::tempdir().unwrap();
    let runtime = ToolRuntime::default();
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
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifests"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifest_details"][0]["shards"],
        serde_json::json!(1)
    );
    assert_eq!(
        startup_json["daemon_status"]["cached_shard_manifest_details"][0]["repos"][0]["aliases"][0],
        startup_json["daemon_status"]["cached_shard_manifest_details"][0]["repos"][0]["name"]
    );

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let request = serde_json::json!({
        "id": "search",
        "tool": "search_shards",
        "arguments": {
            "query": "invoice total",
            "limit": 3,
            "require_all": true
        }
    });
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    let read_response = tcp_tool_request(
        addr,
        serde_json::json!({
            "id": "read",
            "tool": "read_shard_range",
            "arguments": {
                "path": shard_read_path,
                "start": 1,
                "lines": 1
            }
        }),
    );

    child.kill().unwrap();
    let _ = child.wait();

    assert!(response.contains("\"id\":\"search\""));
    assert!(response.contains("src/billing.rs"));
    assert!(read_response.contains("\"id\":\"read\""));
    assert!(read_response.contains("invoice_total"));
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
            "explain": true,
            "context_lines": 3
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
    assert!(stdout.contains("\"context\""));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"lines\":80"));
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
    fs::remove_file(repo.path().join("src/auth.rs")).unwrap();

    let mut child = Command::new(binary)
        .arg("serve-jsonl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let request = serde_json::json!({
        "id": 2,
        "tool": "indexed_search",
        "arguments": {
            "index": index_path,
            "query": "issue token",
            "limit": 3,
            "language": "rust",
            "require_all": true,
            "context_lines": 2
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
    let read_ranges_request = serde_json::json!({
        "id": "read-index-ranges",
        "tool": "read_index_ranges",
        "arguments": {
            "index": index_path,
            "ranges": [
                {"path": "src/auth.rs", "start": 1, "lines": 1},
                {"path": "tests/auth_test.rs", "start": 1, "lines": 2}
            ]
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
    let plan_request = serde_json::json!({
        "id": "indexed-query-plan",
        "tool": "indexed_query_plan",
        "arguments": {
            "index": index_path,
            "query": "SessionManager definitely_missing",
            "path": "src"
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_symbols_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{plan_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":2"));
    assert!(stdout.contains("src/auth.rs"));
    assert!(stdout.contains("\"match_lines\""));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"context\""));
    assert!(stdout.contains("\"id\":\"read-index-range\""));
    assert!(stdout.contains("\"path\":\"src/auth.rs\""));
    assert!(stdout.contains("issue_token"));
    assert!(stdout.contains("\"id\":\"read-index-ranges\""));
    assert!(stdout.contains("\"path\":\"tests/auth_test.rs\""));
    assert!(stdout.contains("\"id\":\"find-index-symbol\""));
    assert!(stdout.contains("\"kind\":\"struct\""));
    assert!(stdout.contains("\"id\":\"indexed-repo-map\""));
    assert!(stdout.contains("\"entrypoints\""));
    assert!(stdout.contains("\"manifest_files\""));
    assert!(stdout.contains("\"related_files\""));
    assert!(stdout.contains("\"related_symbols\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"related-index-files\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"related-index-symbols\""));
    assert!(stdout.contains("SessionManager"));
    assert!(stdout.contains("\"id\":\"indexed-query-plan\""));
    assert!(stdout.contains("\"active_filters\""));
    assert!(stdout.contains("\"field\":\"path\""));
    assert!(stdout.contains("\"candidate_rejections\""));
    assert!(stdout.contains("\"missing_terms\""));
    assert!(stdout.contains("definitely"));
    assert!(stdout.contains("missing"));
    assert!(stdout.contains("\"filtered_candidate_count\":0"));
    assert!(stdout.contains("\"final_match_count\":0"));
    assert!(stdout.contains("\"repair_hints\""));
    assert!(stdout.contains("drop_missing_terms"));
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
        &billing_repo.join("tests/billing_test.rs"),
        "use billing::invoice_total;\n#[test]\nfn totals_invoice() {}\n",
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
            "explain": true,
            "context_lines": 2
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
    let read_ranges_request = serde_json::json!({
        "id": "read-shard-ranges",
        "tool": "read_shard_ranges",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "ranges": [
                {"path": "billing/src/billing.rs", "start": 1, "lines": 1},
                {"path": "billing/tests/billing_test.rs", "start": 1, "lines": 2}
            ]
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
    let related_request = serde_json::json!({
        "id": "related-shard-files",
        "tool": "related_shard_files",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "billing/src/billing.rs",
            "limit": 5
        }
    });
    let related_symbols_request = serde_json::json!({
        "id": "related-shard-symbols",
        "tool": "related_shard_symbols",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "path": "billing/src/billing.rs",
            "query": "invoice total",
            "limit": 5
        }
    });
    let plan_request = serde_json::json!({
        "id": "shard-query-plan",
        "tool": "shard_plan",
        "arguments": {
            "index_dir": parent.path().join(".orient-shards"),
            "query": "repo:BILLING invoice missingterm",
            "require_all": true
        }
    });
    writeln!(child.stdin.as_mut().unwrap(), "{index_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{search_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbol_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{map_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{read_ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{related_symbols_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{plan_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"index-shards\""));
    assert!(stdout.contains("\"shards\":2"));
    assert!(stdout.contains("\"id\":\"search-shards\""));
    assert!(stdout.contains("billing/src/billing.rs"));
    assert!(stdout.contains("shard:billing"));
    assert!(stdout.contains("\"read_range\""));
    assert!(stdout.contains("\"lines\":80"));
    assert!(stdout.contains("\"context\""));
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
    assert!(stdout.contains("\"id\":\"read-shard-ranges\""));
    assert!(stdout.contains("\"path\":\"billing/tests/billing_test.rs\""));
    assert!(stdout.contains("\"id\":\"related-shard-files\""));
    assert!(stdout.contains("billing/tests/billing_test.rs"));
    assert!(stdout.contains("\"id\":\"related-shard-symbols\""));
    assert!(stdout.contains("\"path\":\"billing/src/billing.rs\""));
    assert!(stdout.contains("\"id\":\"shard-query-plan\""));
    assert!(stdout.contains("\"name\":\"billing\""));
    assert!(stdout.contains("\"missing_terms\""));
    assert!(stdout.contains("missingterm"));
    assert!(stdout.contains("\"filtered_candidate_count\":0"));
    assert!(stdout.contains("\"final_match_count\":0"));
    assert!(stdout.contains("\"repair_hints\""));
    assert!(stdout.contains("drop_missing_terms"));
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
        "tool": "open_range",
        "arguments": {
            "repo": repo.path(),
            "path": "src/auth.rs",
            "start": 2,
            "lines": 2
        }
    });
    let ranges_request = serde_json::json!({
        "id": "ranges",
        "tool": "open_ranges",
        "arguments": {
            "repo": repo.path(),
            "ranges": [
                {"path": "src/auth.rs", "start": 1, "lines": 1},
                {"path": "tests/auth_test.rs", "start": 1, "lines": 2}
            ]
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
    writeln!(child.stdin.as_mut().unwrap(), "{ranges_request}").unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{symbols_request}").unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"id\":\"map\""));
    assert!(stdout.contains("SessionManager"));
    assert!(stdout.contains("\"command_hints\""));
    assert!(stdout.contains("\"command\":\"cargo test\""));
    assert!(stdout.contains("\"source\":\"Cargo.toml\""));
    assert!(stdout.contains("tests/auth_test.rs"));
    assert!(stdout.contains("\"id\":\"range\""));
    assert!(stdout.contains("\"start_line\":2"));
    assert!(stdout.contains("issue_token"));
    assert!(stdout.contains("\"id\":\"ranges\""));
    assert!(stdout.contains("\"path\":\"src/auth.rs\""));
    assert!(stdout.contains("\"path\":\"tests/auth_test.rs\""));
    assert!(stdout.contains("\"id\":\"symbols\""));
    assert!(stdout.contains("same file"));
}
